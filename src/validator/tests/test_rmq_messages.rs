/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific EVERX DEV software governing permissions and
* limitations under the License.
*/

use std::{collections::HashSet, str::FromStr, sync::Arc, time::Duration};
use std::ops::RangeInclusive;
use std::thread::sleep;

use ton_api::ton::ton_node::{RempMessageLevel, RempMessageStatus, rempmessagestatus::{RempAccepted, RempIgnored}};

use catchain::PublicKey;

use ever_block::{
    Message, Serializable, Deserializable, ExternalInboundMessageHeader, 
    MsgAddressInt, Grams, ShardIdent, ValidatorDescr, SigPubKey, BlockIdExt, 
    MsgAddressExt::AddrNone, UnixTime32, GetRepresentationHash,
    fail, Result, SliceData, UInt256
};

use crate::{
    config::RempConfig,
    engine_traits::{EngineOperations, RempCoreInterface, RempDuplicateStatus},
    validator::{
        message_cache::{RempMessageOrigin, RempMessageWithOrigin},
        reliable_message_queue::{MessageQueue, RmqMessage},
        remp_block_parser::{BlockProcessor, RempMasterBlockIndexingProcessor},
        remp_catchain::{REMP_CATCHAIN_RECORDS_PER_BLOCK, REMP_MAX_BLOCK_PAYLOAD_LEN, RempCatchain, RempCatchainInfo},
        remp_manager::{RempInterfaceQueues, RempManager, RempSessionStats},
        sessions_computing::GeneralSessionInfo,
        validator_utils::{
            get_message_uid, sigpubkey_to_publickey, ValidatorListHash
        }
    }
};

#[cfg(feature = "telemetry")]
use crate::validator::telemetry::RempCoreTelemetry;

#[test]
fn test_rmq_message_serialize() -> Result<()> {
    let pre_message = Message::with_ext_in_header_and_body(
        ExternalInboundMessageHeader {
            src: AddrNone,
            dst: MsgAddressInt::from_str(
                "-1:7777777777777777777777777777777777777777777777777777777777777777"
            ).unwrap(),
            import_fee: Grams::zero()
        },
        SliceData::from_string(
            "e32fb2df7d15a1266c0b4e949607b67f69f2edfa8dc401bec2df9e985e69eecf8e5fb3903cfc3d19d9910077f0e83c4516cf544c9ca65d33a42c71f6b74ed281d54fe4d47282105f230ce104f0bf52cc3adfdb4d5a538f86e33acf55ca68d1480000005f43e9ca6118634e4fdb079a4f00000000603_"
        ).unwrap()
    );
    let message = Arc::new(pre_message);
    let rmq_message_test0: Arc<RmqMessage> = Arc::new(RmqMessage::new(message)?);

    // Testing proper message serialization
    let data: ton_api::ton::bytes = rmq_message_test0.message.write_to_bytes().unwrap();
    let msg_buffer = Message::construct_from_bytes(&data).unwrap();
    assert_eq!(*rmq_message_test0.message, msg_buffer);

    // Testing RempMessageBody structure serialization/deserailization
    let test0_serialized = RmqMessage::serialize_message_body(&rmq_message_test0.as_remp_message_body());
    println!("Serialized message data: {:?}", data);
    println!("Serialized remp_message_body: {:?}", test0_serialized);
    let msg = RmqMessage::deserialize_message_body(&test0_serialized).unwrap();
    println!("Deserialzed message data: {:?}", msg.message());
    let rmq_message = RmqMessage::from_message_body(&msg)?;

    // Testing message construction from header and body
    let mut message_without_params = Message::default();
    message_without_params.set_header(rmq_message.message.header().clone());
    message_without_params.set_body(rmq_message.message.body().unwrap());
    assert_eq!(*rmq_message_test0.message, message_without_params);
    assert_eq!(rmq_message_test0.message_id, rmq_message.message_id,
               "Message ids differ: {:x} /= {:x}", rmq_message_test0.message_id, rmq_message.message_id);
    assert_eq!(rmq_message_test0.message_uid, rmq_message.message_uid,
               "Message uids differ: {:x} /= {:x}", rmq_message_test0.message_uid, rmq_message.message_uid);

    Ok(())
}

#[test]
fn test_rmq_message_id() -> Result<()> {
    let message_id = UInt256::from_str("c80d5e0e0ada0f4c74ecb03c97947e09204c4d989ac720e1754aeec6f01a4acf")?;
    let message_uid = UInt256::from_str("e4089ffa779a932656bd91a9e5b7323f714fb1782c052e8755fb56312be02c83")?;
    let message_bytes = [
        181, 238, 156, 114, 1, 1, 4, 1, 0, 209, 0, 1, 69, 137, 254, 238, 238, 238, 238, 238, 238, 238, 238, 238, 238,
        238, 238, 238, 238, 238, 238, 238, 238, 238, 238, 238, 238, 238, 238, 238, 238, 238, 238, 238, 238, 238, 238,
        12, 1, 1, 225, 138, 38, 33, 171, 175, 201, 185, 142, 85, 15, 44, 87, 151, 128, 86, 56, 56, 18, 59, 118, 138,
        154, 35, 57, 76, 24, 106, 28, 34, 58, 59, 105, 58, 38, 145, 18, 99, 38, 183, 252, 21, 167, 111, 232, 24, 140,
        227, 54, 240, 143, 185, 206, 53, 134, 220, 228, 17, 238, 112, 57, 5, 129, 96, 1, 242, 26, 212, 19, 110, 243, 200,
        152, 241, 181, 102, 21, 208, 252, 108, 146, 59, 40, 199, 102, 184, 223, 73, 204, 64, 183, 116, 173, 179, 54,
        87, 34, 128, 0, 0, 99, 176, 207, 135, 51, 25, 133, 83, 224, 68, 199, 96, 179, 96, 2, 1, 99, 128, 9, 75, 218,
        166, 71, 170, 33, 189, 182, 153, 177, 109, 126, 100, 87, 19, 110, 125, 93, 129, 102, 179, 156, 141, 198, 170,
        248, 140, 147, 62, 90, 201, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 239, 171, 102, 80, 210, 228, 3, 0, 0
    ].to_vec();

    let rmq_message = RmqMessage::from_raw_message(&message_bytes)?;
    assert_eq!(message_id, rmq_message.message_id);
    assert_eq!(message_uid, rmq_message.message_uid);
    assert_eq!(message_id, rmq_message.message.hash()?);
    assert_eq!(message_uid, get_message_uid(&rmq_message.message));
    Ok(())
}

fn make_vector_of_remp_records<F>(f: F) -> Vec<ton_api::ton::ton_node::RempCatchainRecordV2> 
    where F : Fn(usize) -> ton_api::ton::ton_node::RempCatchainRecordV2 
{
    let mut res: Vec<ton_api::ton::ton_node::RempCatchainRecordV2> = Vec::new();
    for i in 0..REMP_CATCHAIN_RECORDS_PER_BLOCK {
        res.push(f (i))
    }
    res
}

#[test]
fn test_rmq_max_payload_constants() -> Result<()> {
    let (header_payload, hp_ids) = RempCatchain::pack_payload(&make_vector_of_remp_records(
        |idx| ton_api::ton::ton_node::RempCatchainRecordV2::TonNode_RempCatchainMessageHeaderV2 (
            ton_api::ton::ton_node::rempcatchainrecordv2::RempCatchainMessageHeaderV2 {
                message_id: Default::default(),
                message_uid: Default::default(),
                source_key_id: Default::default(),
                source_idx: idx as i32,
                masterchain_seqno: idx as i32
            }
        )
    ));
    println!("{} headers give total payload of {} bytes", hp_ids.len(), header_payload.data().len());
    assert!(header_payload.data().len() <= REMP_MAX_BLOCK_PAYLOAD_LEN);

    let (digest_payload, dp_ids) = RempCatchain::pack_payload(&make_vector_of_remp_records(
        |idx| {
            let digest = ton_api::ton::ton_node::rempcatchainrecordv2::RempCatchainMessageDigestV2 {
                masterchain_seqno: idx as i32,
                messages: vec!(ton_api::ton::ton_node::rempcatchainmessageids::RempCatchainMessageIds {
                    id: Default::default(),
                    uid: Default::default()
                })
            };
            ton_api::ton::ton_node::RempCatchainRecordV2::TonNode_RempCatchainMessageDigestV2(digest)
        }
    ));
    println!("{} digests give total payload of {} bytes", dp_ids.len(), digest_payload.data().len());
    assert!(digest_payload.data().len() <= REMP_MAX_BLOCK_PAYLOAD_LEN);

    Ok(())
}

struct RmqTestEngine {
    #[cfg(feature = "telemetry")]
    remp_core_telemetry: RempCoreTelemetry,
}

impl EngineOperations for RmqTestEngine {
    #[cfg(feature = "telemetry")]
    fn remp_core_telemetry(&self) -> &RempCoreTelemetry {
        &self.remp_core_telemetry
    }
}

impl RmqTestEngine {
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "telemetry")]
            remp_core_telemetry : RempCoreTelemetry::new(10),
        }
    }
}

struct RmqTestbench {
    engine: Arc<RmqTestEngine>,
    remp_manager: Arc<RempManager>,
    remp_interface_queues: RempInterfaceQueues,
    params: Arc<GeneralSessionInfo>,
    local_key: PublicKey,
    curr_validators: Vec<ValidatorDescr>,
    next_validators: Vec<ValidatorDescr>,
    node_list_id: UInt256,
    rp_guarantee: Duration,
    message_queue: MessageQueue
}

impl RmqTestbench {
    fn create_runtime() -> Result<tokio::runtime::Runtime> {
        let runtime = match tokio::runtime::Runtime::new() {
            Ok(r) => r,
            Err(e) => fail!(e)
        };
        Ok(runtime)
    }

    async fn new(runtime_handle: &tokio::runtime::Handle, masterchain_seqno: u32, rp_guarantee: Duration) -> Result<Self> {
        let engine = Arc::new(RmqTestEngine::new());

        let remp_config = RempConfig::create_empty();
        let (remp_manager_value, remp_interface_queues) = RempManager::create_with_options(
            engine.clone(), remp_config.clone(), Arc::new(runtime_handle.clone())
        );
        let remp_manager = Arc::new(remp_manager_value);
        let local_validator = ValidatorDescr::with_params (
            SigPubKey::from_bytes(UInt256::rand().as_slice())?,
            1, None, None
        );
        let local_key = sigpubkey_to_publickey(&local_validator.public_key);
        let curr_validators = vec!(local_validator.clone());
        let next_validators = vec!(local_validator.clone());
        let params = Arc::new(GeneralSessionInfo {
            shard: ShardIdent::with_tagged_prefix(0,0xc000_0000_0000_0000)?,
            opts_hash: Default::default(),
            catchain_seqno: 2,
            key_seqno: 1,
            max_vertical_seqno: 0
        });
        let node_list_id = ValidatorListHash::rand();

        for cc in 1..=masterchain_seqno {
            remp_manager.create_master_cc_session(cc, 0.into(), vec!())?;
        }
        let masterchain_range = remp_manager.advance_master_cc(masterchain_seqno, rp_guarantee)?;

        let queue_info = Arc::new(RempCatchainInfo::create(
            params.clone(), &masterchain_range,
            &curr_validators, &next_validators,
            &local_key, node_list_id.clone(),
        )?);
        let message_queue = MessageQueue::create(
            engine.clone(), remp_manager.clone(), queue_info
        )?;

        Ok(RmqTestbench {
            engine,
            remp_manager,
            remp_interface_queues,
            params,
            local_key,
            curr_validators,
            next_validators,
            node_list_id,
            rp_guarantee,
            message_queue
        })
    }

    async fn send_pending_message(&self, msg: &RempMessageWithOrigin, masterchain_seqno: u32) -> Result<bool> {
        self.message_queue.process_pending_remp_catchain_record(
            &msg.as_remp_catchain_record(masterchain_seqno),
            0
        ).await?;
        self.remp_manager.message_cache.update_message_body(Arc::new(msg.message.clone()))
    }

    fn replace_message_queue(&mut self, masterchain_range: &RangeInclusive<u32>) -> Result<()> {
        let info = Arc::new(RempCatchainInfo::create(
            self.params.clone(), masterchain_range,
            &self.curr_validators, &self.next_validators,
            &self.local_key, self.node_list_id.clone())?);

        self.message_queue = MessageQueue::create(
            self.engine.clone(), self.remp_manager.clone(), info
        )?;
        Ok(())
    }

    async fn advance_master_cc(&mut self, masterchain_seqno: u32, mc_time: UnixTime32) -> Result<RempSessionStats> {
        self.remp_manager.create_master_cc_session(masterchain_seqno, mc_time, vec!())?;
        let new_range = self.remp_manager.advance_master_cc(masterchain_seqno, self.rp_guarantee)?;
        self.replace_message_queue(&new_range)?;
        Ok(self.remp_manager.gc_old_messages(*new_range.start()).await)
    }
}

fn random_message() -> Result<RmqMessage> {
    RmqMessage::make_test_message(&SliceData::from(UInt256::rand()))
}

fn make_test_message_with_origin(body: &SliceData) -> Result<RempMessageWithOrigin> {
    let m = RmqMessage::make_test_message(body)?;
    let o = RempMessageOrigin::create_empty()?;
    Ok(RempMessageWithOrigin { message: m, origin: o })
}

fn make_test_random_message_with_origin() -> Result<RempMessageWithOrigin> {
    make_test_message_with_origin(&SliceData::from(UInt256::rand()))
}

#[test]
fn remp_simple_forwarding_test() -> Result<()> {
    // -- seq=0/seq=1,forwarding
    // 5k -- new/(k=0/1rejected_l/2rejected_r/3rejected_lr)
    // 5k+1 -- ignored/()
    // 5k+2 -- duplicate/()
    // 5k+3 -- accepted/()
    // 5k+4 -- rejected/()

    //let mut msgs = Vec::new();
    //init_test_log();
    let runtime = RmqTestbench::create_runtime()?;
    let runtime_handle = runtime.handle().clone();

    runtime.block_on(async move {
        let testbench = RmqTestbench::new(&runtime_handle, 2, Duration::from_secs(10)).await?;

        let blk1 = BlockIdExt::with_params(
            testbench.params.shard.clone(),
            5129,
            UInt256::rand(),
            UInt256::rand()
        );

        let m1 = make_test_random_message_with_origin()?;
        let m2 = make_test_random_message_with_origin()?;

        testbench.remp_manager.message_cache.add_external_message_status(
            m1.get_message_id(),
            &m1.message.message_uid,
            Some(Arc::new(m1.message.clone())),
            Some(Arc::new(m1.origin.clone())),
            RempMessageStatus::TonNode_RempAccepted(RempAccepted {
                level: RempMessageLevel::TonNode_RempShardchain,
                block_id: blk1.clone(),
                master_id: Default::default()
            }),
            |_old,new| new.clone(),
            1
        )?;

        //testbench.remp_manager.options
        println!("{:?}", testbench.remp_manager.message_cache.get_message_with_origin_status_cc(m1.get_message_id()));

        assert!(!(testbench.send_pending_message(&m1, 1).await?)); // false: we've added m1, but not m2
        assert!(testbench.send_pending_message(&m2, 1).await?);

        let ign = RempIgnored { level: RempMessageLevel::TonNode_RempMasterchain, block_id: blk1 };
        assert_eq!(testbench.remp_manager.message_cache.get_message_status(m1.get_message_id())?.unwrap(), RempMessageStatus::TonNode_RempIgnored(ign));
        assert_eq!(testbench.remp_manager.message_cache.get_message_status(m2.get_message_id())?.unwrap(), MessageQueue::forwarded_ignored_status());

        println!("Remp interface queues: {} repsonses", testbench.remp_interface_queues.response_receiver.len());
        Ok(())
    })
}

#[test]
fn remp_simple_collation_equal_uids_test() -> Result<()> {
    //init_test_log();
    let runtime = RmqTestbench::create_runtime()?;
    let runtime_handle = runtime.handle().clone();

    runtime.block_on(async move {
        let testbench = RmqTestbench::new(&runtime_handle, 2, Duration::from_secs(10)).await?;

        let body1 = SliceData::from(UInt256::rand());
        let body2 = SliceData::from(UInt256::rand());

        let mut msgs = Vec::new();
        for _cnt in 0..5 {
            msgs.push(make_test_message_with_origin(&body1)?);
            msgs.push(make_test_message_with_origin(&body2)?);
        }

        let (_min_id,uid_for_min_id) = msgs.iter().map(|a| (a.get_message_id(), &a.message.message_uid)).min().unwrap();
        let msg_to_collate = msgs.iter().filter(|x| &x.message.message_uid != uid_for_min_id).map(|x| x.get_message_id()).min().unwrap();
        let (acc_id,acc_uid) = msgs.iter().filter(|x| &x.message.message_uid == uid_for_min_id).map(|x| (x.get_message_id(),&x.message.message_uid)).max().unwrap();

        let mut must_be_collated = HashSet::<UInt256>::new();
        must_be_collated.insert(msg_to_collate.clone());
        let must_be_rejected : Vec<RempMessageWithOrigin> = msgs.iter()
            .filter(|a| !must_be_collated.contains(a.get_message_id()) && a.get_message_id() != acc_id).cloned()
            .collect();

        for m in msgs.iter() {
            let pc = if must_be_collated.contains(m.get_message_id()) { "C" } else { "" }.to_string();
            let pa = if m.get_message_id() == acc_id { "A" } else { "" }.to_string();
            let pr = if must_be_rejected.contains(m) { "R" } else { "" }.to_string();
            println!("Pending msg: {:x} {}{}{}", m.get_message_id(), pc, pa, pr);

            // All msg ids are different, therefore body must be added
            assert!(testbench.send_pending_message(m, testbench.message_queue.catchain_info.get_master_cc_seqno()).await?);
        }

        let accepted = RempAccepted {
            level: RempMessageLevel::TonNode_RempMasterchain,
            block_id: Default::default(),
            master_id: Default::default()
        };
        testbench.remp_manager.message_cache.add_external_message_status(
            acc_id, acc_uid, None, None,
            RempMessageStatus::TonNode_RempAccepted(accepted),
            |_o,n| n.clone(),
            2
        )?;

        println!("Collecting messages for collation");
        sleep(Duration::from_millis(10)); // To overcome SystemTime inconsistency and make tests reproducible.
        let messages = testbench.message_queue.prepare_messages_for_collation().await?;
//        for (msg_id, msg, _order) in messages.into_iter() {
//            testbench.engine.new_remp_message(msg_id.clone(), msg)?;
//        }

        for (id, _msg, _origin) in messages.iter() {
            println!("collated: {:x}", id);
            assert!(must_be_collated.remove(id));
        }
        assert!(must_be_collated.is_empty());

        for id in must_be_rejected.iter() {
            let status = testbench.remp_manager.message_cache.get_message_status(id.get_message_id())?;
            if let Some(RempMessageStatus::TonNode_RempRejected(rejected)) = &status {
                assert_eq!(rejected.level, RempMessageLevel::TonNode_RempQueue);
                println!("rejected: {}, '{:?}'", id, status)
            }
            else {
                panic!("Expected rejected status, found {:?}", status);
            }
        }

        Ok(())
    })
}

#[test]
fn remp_simple_expiration_test() -> Result<()> {
    //init_test_log();
    let runtime = RmqTestbench::create_runtime()?;
    let runtime_handle = runtime.handle().clone();

    runtime.block_on(async move {
        let mut testbench = RmqTestbench::new(&runtime_handle, 2, Duration::from_secs(10)).await?;

        let mut bodies = Vec::new();
        for _ in 0..5 {
            bodies.push(SliceData::from(UInt256::rand()));
        }

        for b in bodies.iter() {
            let m = make_test_message_with_origin(b)?;
            println!("Pending msg 2: {:x}", m.get_message_id());
            testbench.send_pending_message(&m, testbench.message_queue.catchain_info.get_master_cc_seqno()).await?;
        }
        println!("Collected old messages 3: {}", testbench.advance_master_cc(3, 30.into()).await?);
        println!("Collected old messages 4: {}", testbench.advance_master_cc(4, 40.into()).await?);
        println!("Collected old messages 5: {}", testbench.advance_master_cc(5, 50.into()).await?);

        let mut msgs = Vec::new();
        for b in bodies.iter() {
            let m = make_test_message_with_origin(b)?;
            println!("Pending msg 5: {:x}", m.get_message_id());
            testbench.send_pending_message(&m, testbench.message_queue.catchain_info.get_master_cc_seqno()).await?;
            msgs.push(m);
        }

        for m in msgs.iter() {
            assert_eq!(
                testbench.remp_interface_queues.check_remp_duplicate(m.get_message_id())?,
                RempDuplicateStatus::Fresh(m.message.message_uid.clone())
            );
            testbench.remp_manager.message_cache.add_external_message_status(
                m.get_message_id(),
                &m.message.message_uid,
                Some(Arc::new(m.message.clone())),
                Some(Arc::new(m.origin.clone())),
                RempMessageStatus::TonNode_RempAccepted(RempAccepted {
                    level: RempMessageLevel::TonNode_RempMasterchain,
                    block_id: Default::default(),
                    master_id: Default::default()
                }),
                |_o,n| n.clone(),
                5
            )?;
        }
        msgs.clear();

        println!("Collected old messages 6: {}", testbench.advance_master_cc(6, 60.into()).await?);

        for b in bodies.iter() {
            let m = make_test_message_with_origin(b)?;
            println!("Pending msg 6: {:x}", m.get_message_id());
            testbench.send_pending_message(&m, testbench.message_queue.catchain_info.get_master_cc_seqno()).await?;
            msgs.push(m);
        }

        for m in msgs.iter() {
            match testbench.remp_interface_queues.check_remp_duplicate(m.get_message_id())? {
                RempDuplicateStatus::Absent => panic!("Message {} must present", m),
                RempDuplicateStatus::Fresh(uid) => panic!("Message {} must not be fresh with uid {:x}", m, uid),
                RempDuplicateStatus::Duplicate(_, uid, _) => assert_eq!(m.message.message_uid, uid)
            }
        }
        Ok(())
    })
}

#[test]
fn remp_simple_status_updating_test() -> Result<()> {
    //init_test_log();
    let runtime = RmqTestbench::create_runtime()?;
    let runtime_handle = runtime.handle().clone();

    runtime.block_on(async move {
        let mut testbench = RmqTestbench::new(&runtime_handle, 1, Duration::from_secs(10)).await?;
        let blk1 = BlockIdExt::with_params(
            testbench.params.shard.clone(),
            5129,
            UInt256::rand(),
            UInt256::rand()
        );

        let common_body = SliceData::from(UInt256::rand());

        let m1 = make_test_message_with_origin(&common_body)?;
        let m2 = make_test_message_with_origin(&common_body)?;

        let proc = RempMasterBlockIndexingProcessor::new(
            blk1.clone(), blk1.clone(),
            testbench.remp_manager.message_cache.clone(),
            1
        );
        proc.process_message(m1.get_message_id(), &m1.message.message_uid).await;

        testbench.advance_master_cc(2, 10.into()).await?;

        testbench.remp_manager.message_cache.add_external_message_status(
            m2.get_message_id(),
            &m2.message.message_uid,
            Some(Arc::new(m2.message.clone())),
            Some(Arc::new(m2.origin.clone())),
            RempMessageStatus::TonNode_RempNew,
            |_old,new| new.clone(),
            2
        )?;

        assert_eq!(
            testbench.remp_interface_queues.check_remp_duplicate(m2.get_message_id())?,
            RempDuplicateStatus::Duplicate(
                blk1.clone(), m1.message.message_uid.clone(), m1.get_message_id().clone()
            )
        );

        testbench.advance_master_cc(3, 20.into()).await?;

        assert_eq!(
            testbench.remp_interface_queues.check_remp_duplicate(m2.get_message_id())?,
            RempDuplicateStatus::Fresh(m2.message.message_uid.clone())
        );

        Ok(())
    })
}

async fn push_random_msgs(testbench: &RmqTestbench, count: usize) -> Result<Vec<Arc<RmqMessage>>> {
    let blk1 = BlockIdExt::with_params(
        testbench.params.shard.clone(),
        5129,
        UInt256::rand(),
        UInt256::rand()
    );

    let mut msgs = Vec::new();
    for _ in 0..count {
        msgs.push(Arc::new(random_message()?));
    }

    let proc = RempMasterBlockIndexingProcessor::new(
        blk1.clone(), blk1.clone(),
        testbench.remp_manager.message_cache.clone(),
        testbench.message_queue.catchain_info.get_master_cc_seqno()
    );

    for m in msgs.iter() {
        proc.process_message(&m.message_id, &m.message_uid).await;
    }

    Ok(msgs)
}

#[test]
fn remp_simple_gc_range_test() -> Result<()> {
    //init_test_log();
    let runtime = RmqTestbench::create_runtime()?;
    let runtime_handle = runtime.handle().clone();

    runtime.block_on(async move {
        let mut testbench = RmqTestbench::new(&runtime_handle, 1, Duration::from_secs(10)).await?;

        let msgs = push_random_msgs(&testbench, 20).await?;

        let stats = testbench.advance_master_cc(2, 5.into()).await?;
        assert_eq!(stats.total, 0);

        let msgs2 = push_random_msgs(&testbench, 24).await?;

        let stats = testbench.advance_master_cc(3, 7.into()).await?;
        assert_eq!(stats.total, 0);
        let stats = testbench.advance_master_cc(4, 9.into()).await?;
        assert_eq!(stats.total, 0);
        let stats = testbench.advance_master_cc(5, 14.into()).await?;
        assert_eq!(stats.total, 0);
        let stats = testbench.advance_master_cc(6, 15.into()).await?;
        assert_eq!(stats.has_only_header, msgs.len());
        let stats = testbench.advance_master_cc(7, 17.into()).await?;
        assert_eq!(stats.has_only_header, msgs2.len());
        let stats = testbench.advance_master_cc(8, 18.into()).await?;
        assert_eq!(stats.total, 0);

        let msgs3 = push_random_msgs(&testbench, 17).await?;
        let stats = testbench.advance_master_cc(9, 100.into()).await?;
        assert_eq!(stats.total, 0);
        let _msgs4 = push_random_msgs(&testbench, 417).await?;
        let stats = testbench.advance_master_cc(10, 218.into()).await?;
        assert_eq!(stats.has_only_header, msgs3.len());
        let stats = testbench.advance_master_cc(10, 218.into()).await?;
        assert_eq!(stats.total, 0);

        Ok(())
    })
}

#[test]
fn remp_simple_advance_special_cases() -> Result<()> {
    //init_test_log();
    let runtime = RmqTestbench::create_runtime()?;
    let runtime_handle = runtime.handle().clone();

    runtime.block_on(async move {
        let mut testbench = RmqTestbench::new(&runtime_handle, 1, Duration::from_secs(10)).await?;
        let msgs = push_random_msgs(&testbench, 20).await?;
        let stat = testbench.advance_master_cc(2, 20.into()).await?;
        assert_eq!(stat.total, 0);
        let msgs2 = push_random_msgs(&testbench, 20).await?;
        let stat = testbench.advance_master_cc(3, 30.into()).await?;
        assert_eq!(stat.total, msgs.len());
        assert!(testbench.advance_master_cc(3, 40.into()).await.is_err());
        assert!(testbench.advance_master_cc(3, 29.into()).await.is_err());
        let stat = testbench.advance_master_cc(3, 30.into()).await?;
        assert_eq!(stat.total, 0);
        let stat = testbench.advance_master_cc(4, 40.into()).await?;
        assert_eq!(stat.total, msgs2.len());
        assert!(testbench.advance_master_cc(5, 39.into()).await.is_err());
        Ok(())
    })
}
