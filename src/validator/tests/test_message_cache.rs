/*
* Copyright (C) 2019-2021 TON Labs. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific TON DEV software governing permissions and
* limitations under the License.
*/

use std::mem::swap;
use std::sync::Arc;
use std::time::Duration;
use openssl::rand::rand_bytes;
use rand::{Rng, thread_rng};
use adnl::telemetry::Metric;
use ton_api::ton::ton_node::{RempMessageLevel, RempMessageStatus, rempmessagestatus::RempAccepted};
use ton_block::{BlockIdExt, ShardIdent};
use ton_types::{Result, SliceData, error, UInt256};
use crate::engine_traits::RempDuplicateStatus;
use crate::ext_messages::get_level_and_level_change;
use crate::validator::message_cache::{MessageCache, RmqMessage};
use crate::validator::reliable_message_queue::MessageQueue;

//use crate::test_helper::init_test_log;

fn gen_random_body(_i: i32) -> Result<SliceData> {
    let mut rand_body_buffer: [u8; 64] = [0; 64];
    rand_bytes(&mut rand_body_buffer)?;
    Ok(SliceData::new(rand_body_buffer.to_vec()))
}

struct SimpleCache {
    messages: Vec<(UInt256, UInt256, i32, RempMessageStatus)> // id,uid,mseq,status
}

impl SimpleCache {
    pub fn new() -> Self { SimpleCache { messages: Vec::new() } }
    pub fn get_messages_for_uid(&self, message_uid: &UInt256) -> Vec<UInt256> {
        let mut res = Vec::new();
        for (id,uid,_mseq, _status) in self.messages.iter() {
            if uid == message_uid {
                res.push(id.clone());
            }
        }
        return res;
    }
    pub fn get_statuses_for_uid(&self, message_uid: &UInt256) -> Vec<(UInt256, RempMessageStatus)> {
        let mut res = Vec::new();
        for (id,uid,_,status) in self.messages.iter() {
            if uid == message_uid {
                res.push((id.clone(),status.clone()));
            }
        }
        return res;
    }
    pub fn add_external_message_status(&mut self, new_id: &UInt256, new_uid: &UInt256, new_mseq: i32, new_status: &RempMessageStatus) {
        if let Some(pos) = self.messages.iter().position(|(i,_,_,_)| i == new_id) {
            let rec = self.messages.get_mut(pos).unwrap();
            rec.3 = new_status.clone();
            return;
        }
        self.messages.push((new_id.clone(),new_uid.clone(),new_mseq,new_status.clone()));
    }
    pub fn uid_has_final_status(&self, uid: &UInt256) -> bool {
        self.messages.iter().any(|(_m,u,_seq,s)| u == uid && MessageQueue::is_final_status(s))
    }
    pub fn check_message_duplicates(&self, id: &UInt256) -> RempDuplicateStatus {
        if let Some((_id,uid,_,_)) = self.messages.iter().find(|(i,_,_,_)| i == id) {
            let mut prev_messages = self.get_messages_for_uid(uid);
            prev_messages.sort();
            let mut status = if prev_messages.get(0) == Some(id) {
                RempDuplicateStatus::Fresh(uid.clone())
            }
            else {
                RempDuplicateStatus::Duplicate(BlockIdExt::default(), uid.clone(), prev_messages.get(0).unwrap().clone())
            };

            let prev_collations = self.get_statuses_for_uid(uid);
            for levels in [RempMessageLevel::TonNode_RempShardchain, RempMessageLevel::TonNode_RempMasterchain] {
                for (e_id, e_status) in prev_collations.iter() {
                    if let RempMessageStatus::TonNode_RempAccepted(acc) = e_status {
                        if acc.level == levels {
                            status = RempDuplicateStatus::Duplicate(acc.block_id.clone(), uid.clone(), e_id.clone())
                        }
                    }
                }
            }

            return status;
        }
        else {
            return RempDuplicateStatus::Absent;
        }
    }
    pub fn get_message_status(&self, id: &UInt256) -> Option<RempMessageStatus> {
        self.messages.iter().find(|(i,_u,_seq,_s)| i == id).map(|(_,_,_,s)| s.clone())
    }
    pub fn remove_old_messages(&mut self, new_mseq: i32) {
        self.messages.retain(|(_id,_uid,mseq,_status)| new_mseq-2 < *mseq);
    }
    pub fn all_messages_count(&self) -> usize {
        return self.messages.len();
    }
}

struct MessageCacheTestbench {
    simple_cache: SimpleCache,
    cache: MessageCache,
    rt: tokio::runtime::Runtime
}

impl MessageCacheTestbench {
    pub fn new() -> Result<Self> {
        let rt = tokio::runtime::Runtime::new()?;

        let cache = MessageCache::with_metrics(
            #[cfg(feature = "telemetry")]
                Metric::without_totals("message_cache cache_size_metric", 0)
        );
        //cache.try_set_master_cc_start_time(1,1.into());
        //cache.update_master_cc_ranges(1,Duration::from_secs(1))?;

        let simple_cache = SimpleCache::new();

        Ok(Self { rt, cache, simple_cache })
    }
}

fn do_test_message_cache_add_remove(check_uids: bool) -> Result<()> {
    //init_test_log();
    let tb = MessageCacheTestbench::new()?;
    let max_messages = 50000;
    let max_seqs = 50;
    let messages_per_seq = max_messages/max_seqs;

    let mut bodies = Vec::new();
    for i in 0..(if check_uids { messages_per_seq } else { max_messages }) {
        bodies.push(gen_random_body(i)?);
    }

    tb.cache.try_set_master_cc_start_time(1,1.into(), vec!())?;
    tb.cache.update_master_cc_ranges(1, Duration::from_secs(1))?;

    tb.rt.block_on( async move {
        for i in 0..max_messages {
            let master_seq = i / messages_per_seq + 1;
            let message_no = i % messages_per_seq;
            let start_new_seq = (i % messages_per_seq) == 0;
            let body = bodies.get(message_no as usize)
                .ok_or_else(|| error!("Cannot get body no. {}", i))?;

            let msg = Arc::new(RmqMessage::make_test_message(body)?);
            match tb.cache.check_message_duplicates(&msg.message_id)? {
                RempDuplicateStatus::Absent => (),
                RempDuplicateStatus::Fresh(_) => panic!("Cannot know the message before it is added"),
                RempDuplicateStatus::Duplicate(_, _, _) => panic!("No duplicates in this test")
            }

            if start_new_seq {
                let ms = master_seq as u32;
                tb.cache.try_set_master_cc_start_time(ms as u32,ms.into(), vec!())?;
                let range = tb.cache.update_master_cc_ranges(ms as u32, Duration::from_secs(1))?;
                tb.cache.gc_old_messages(*range.start()).await;
            }

            tb.cache.add_external_message_status(
                &msg.message_id, &msg.message_uid,
                Some(msg.clone()),
                RempMessageStatus::TonNode_RempNew,
                |_old,new| new.clone(),
                master_seq as u32
            ).await?;

            let mut should_be_duplicate = false;
            if check_uids {
                let ids_for_uid = tb.cache.get_messages_for_uid(&msg.message_uid);
                match ids_for_uid.len() {
                    0 => panic!("Msg seq {}, master seq {}, must have at least id {:x} for uid {:x} after message addition",
                                i, master_seq, &msg.message_id, &msg.message_uid),
                    1 => {
                        assert!(ids_for_uid.iter().all(|id| *id == msg.message_id));
                        assert_eq!(master_seq, 1);
                    },
                    2 => {
                        assert!(ids_for_uid.iter().any(|id| *id == msg.message_id));
                        assert_ne!(master_seq, 1);
                        should_be_duplicate = ids_for_uid.iter().min() != Some(&msg.message_id);
                    },
                    ids_count => panic!("Msg seq {}, master seq {}, {} ids for uid {:x}, must be 2 at most",
                        i, master_seq, ids_count, &msg.message_uid
                    )
                }
            }

            match tb.cache.check_message_duplicates(&msg.message_id)? {
                RempDuplicateStatus::Absent => panic!("Message must present after addition"),
                RempDuplicateStatus::Fresh(_) => assert!(!check_uids || !should_be_duplicate),
                RempDuplicateStatus::Duplicate(_, _, dup_id) => if check_uids && !should_be_duplicate {
                    panic!("Msg seq {}, master seq {}, uid {:x} ids {:?}, id {:x} should not be duplicate with {:x}",
                        i, master_seq, msg.message_uid, tb.cache.get_messages_for_uid(&msg.message_uid), msg.message_id, dup_id
                    )
                }
            }
        };
        let ms2 = max_seqs as u32 + 1;
        tb.cache.try_set_master_cc_start_time(ms2,ms2.into(), vec!())?;
        let lwb = tb.cache.update_master_cc_ranges(ms2, Duration::from_secs(1))?;
        assert_eq!(tb.cache.gc_old_messages(*lwb.start()).await.total, (max_messages / max_seqs) as usize);

        let ms3 = ms2+1;
        tb.cache.try_set_master_cc_start_time(ms3,ms3.into(), vec!())?;
        let lwb = tb.cache.update_master_cc_ranges(ms3, Duration::from_secs(1))?;
        tb.cache.gc_old_messages(*lwb.start()).await;
        assert_eq!(tb.cache.gc_old_messages(*lwb.start()).await.total, 0);
        assert_eq!(tb.cache.all_messages_count(), 0);

        Ok(())
    })
}

fn do_test_message_cache_uids_same_block() -> Result<()> {
    let tb = MessageCacheTestbench::new()?;

    let seq = 1;
    tb.cache.try_set_master_cc_start_time(seq,seq.into(), vec!())?;
    tb.cache.update_master_cc_ranges(seq, Duration::from_secs(1))?;

    let mut bodies = Vec::new();
    bodies.push(gen_random_body(0)?);

    tb.rt.block_on( async move {
        let body = bodies.get(0).unwrap();
        let mut msg1 = Arc::new(RmqMessage::make_test_message(body)?);
        let mut msg2 = Arc::new(RmqMessage::make_test_message(body)?);
        if msg1.message_id > msg2.message_id {
            swap(&mut msg1, &mut msg2);
        }

        tb.cache.add_external_message_status(
            &msg2.message_id, &msg2.message_uid,
            Some(msg2.clone()),
            RempMessageStatus::TonNode_RempNew,
            |_old,new| new.clone(),
            seq
        ).await?;

        tb.cache.add_external_message_status(
            &msg1.message_id, &msg1.message_uid,
            Some(msg1.clone()),
            RempMessageStatus::TonNode_RempNew,
            |_old,new| new.clone(),
            seq
        ).await?;

        let dup1 = tb.cache.check_message_duplicates(&msg1.message_id)?;
        let dup2 = tb.cache.check_message_duplicates(&msg2.message_id)?;

        assert_eq!(dup1, RempDuplicateStatus::Fresh(msg1.message_uid.clone()));
        assert_eq!(dup2, RempDuplicateStatus::Duplicate(BlockIdExt::default(), msg2.message_uid.clone(), msg1.message_id.clone()));
        Ok(())
    })
}

fn do_message_cache_simple_random_test() -> Result<()> {
    let mut tb = MessageCacheTestbench::new()?;
    let max_messages = 5000;
    let max_seqs = 50;
    let max_bodies = 743;

    let mut bodies = Vec::new();
    let mut random_thread = thread_rng();
    for i in 0..max_bodies {
        bodies.push(gen_random_body(i)?);
    }

    tb.rt.block_on( async move {
        for msg in 0..max_messages {
            let seq = msg / (max_messages / max_seqs) + 1;
            let body_id = random_thread.gen_range(0, max_bodies);
            let body = bodies.get(body_id as usize).unwrap();
            let msg = Arc::new(RmqMessage::make_test_message(body)?);
            let status = if random_thread.gen_bool(0.3) {
                let acc = RempAccepted {
                    level: if random_thread.gen_bool(0.2) { RempMessageLevel::TonNode_RempMasterchain } else { RempMessageLevel::TonNode_RempShardchain },
                    block_id: BlockIdExt::with_params(ShardIdent::masterchain(),seq,UInt256::rand(),UInt256::rand()),
                    master_id: Default::default()
                };
                RempMessageStatus::TonNode_RempAccepted(acc)
            }
            else {
                RempMessageStatus::TonNode_RempNew
            };

            tb.cache.try_set_master_cc_start_time(seq,seq.into(), vec!())?;
            let lwb = tb.cache.update_master_cc_ranges(seq, Duration::from_secs(1))?;
            tb.cache.gc_old_messages(*lwb.start()).await;
            tb.simple_cache.remove_old_messages(seq as i32);

            if !tb.simple_cache.uid_has_final_status(&msg.message_uid) {
                tb.cache.add_external_message_status(
                    &msg.message_id, &msg.message_uid,
                    Some(msg.clone()), status.clone(),
                    |_old,new| new.clone(),
                    seq
                ).await?;
                tb.simple_cache.add_external_message_status(
                    &msg.message_id, &msg.message_uid,
                    seq as i32, &status
                );
            }

            let mut cache_ids = tb.cache.get_messages_for_uid(&msg.message_uid);
            let mut simple_cache_ids = tb.simple_cache.get_messages_for_uid(&msg.message_uid);
            cache_ids.sort();
            simple_cache_ids.sort();
            assert_eq!(cache_ids, simple_cache_ids);
            assert_ne!(cache_ids.len(), 0);
            assert_ne!(simple_cache_ids.len(), 0);

            let cache_duplicate = tb.cache.check_message_duplicates(&msg.message_id)?;
            let simple_duplicate = tb.simple_cache.check_message_duplicates(&msg.message_id);
            match (cache_duplicate, simple_duplicate) {
                (RempDuplicateStatus::Duplicate(_bc, uc, ic), RempDuplicateStatus::Duplicate(_bs, us, is)) => {
                    let cstat = tb.simple_cache.get_message_status(&ic).unwrap();
                    let sstat = tb.simple_cache.get_message_status(&is).unwrap();
                    assert_eq!(uc, us);
                    assert_eq!(get_level_and_level_change(&cstat), get_level_and_level_change(&sstat));
                    if MessageQueue::is_final_status(&cstat) {
                        assert_eq!(cstat, sstat);
                    }
                },
                (c,s) => assert_eq!(c,s)
            };
        }
        tb.cache.try_set_master_cc_start_time(max_seqs,max_seqs.into(), vec!())?;
        let lwb = tb.cache.update_master_cc_ranges(max_seqs, Duration::from_secs(1))?;
        tb.cache.gc_old_messages(*lwb.start()).await;
        tb.simple_cache.remove_old_messages(max_seqs as i32);

        tb.cache.try_set_master_cc_start_time(max_seqs + 1,(max_seqs+1).into(), vec!())?;
        let lwb = tb.cache.update_master_cc_ranges(max_seqs + 1, Duration::from_secs(1))?;
        tb.cache.gc_old_messages(*lwb.start()).await;

        tb.simple_cache.remove_old_messages(max_seqs as i32 + 1);
        assert_eq!(tb.cache.all_messages_count(), tb.simple_cache.all_messages_count());
        Ok(())
    })
}

#[test]
pub fn test_message_cache_add_remove() -> Result<()> {
    do_test_message_cache_add_remove(false)
}

#[test]
pub fn test_message_cache_uids() -> Result<()> {
    do_test_message_cache_add_remove(true)
}

#[test]
pub fn test_message_cache_uids_same_block() -> Result<()> { do_test_message_cache_uids_same_block() }

#[test]
pub fn test_message_cache_simple_random() -> Result<()> {
    do_message_cache_simple_random_test()
}

#[test]
pub fn test_message_cache_cc_ranges_and_gc() -> Result<()> {
    let tb = MessageCacheTestbench::new()?;

    tb.rt.block_on( async move {
        let msg = Arc::new(RmqMessage::make_test_message(&gen_random_body(100)?)?);

        tb.cache.try_set_master_cc_start_time(1 as u32,1.into(), vec!())?;
        let _range = tb.cache.update_master_cc_ranges(1, Duration::from_secs(1))?;

        tb.cache.add_external_message_status(
            &msg.message_id, &msg.message_uid,
            Some(msg.clone()), RempMessageStatus::TonNode_RempNew,
            |_old,new| new.clone(),
            1
        ).await?;

        tb.cache.try_set_master_cc_start_time(2 as u32,2.into(), vec!())?;
        let range = tb.cache.update_master_cc_ranges(2, Duration::from_secs(1))?;

        assert!(tb.cache.get_message_with_status_cc(&msg.message_id)?.is_some());
        tb.cache.gc_old_messages(*range.start()).await;

        tb.cache.try_set_master_cc_start_time(3 as u32,3.into(), vec!())?;
        let range = tb.cache.update_master_cc_ranges(3, Duration::from_secs(1))?;
        assert!(tb.cache.get_message_with_status_cc(&msg.message_id)?.is_some());

        tb.cache.gc_old_messages(*range.start()).await;
        assert!(tb.cache.get_message_with_status_cc(&msg.message_id)?.is_none());

        Ok(())
    })
}
