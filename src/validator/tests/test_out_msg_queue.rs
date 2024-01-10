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

use std::{collections::BinaryHeap, str::FromStr, time::Instant};

use super::*;
use crate::types::messages::MsgEnqueueStuff;
use ton_block::*;
// TODO: need tests to illustrate adding, routing, fetching message
// create internal message in one shard
// put it to its queue
// then preform hyper routing to another shard
// move it by hops and get fees
// then remove from queue and update processing

#[test]
fn test_already_processed_enqueued_message() {
    let env = MsgEnvelope::construct_from_file("src/tests/static/message.boc").unwrap();
    let last_msg_lt = 93381000002;
    let last_msg_hash = env.message_cell().repr_hash();
    let enq =
        MsgEnqueueStuff::from_enqueue(EnqueuedMsg::with_param(last_msg_lt, &env).unwrap()).unwrap();
    let shards = ShardHashes::construct_from_file("src/tests/static/shards_73107.boc").unwrap();
    shards.dump("73107");

    let mut last_entry = ProcessedUptoStuff::with_params(
        0x9800000000000000,
        73107,
        last_msg_lt,
        last_msg_hash,
        #[cfg(feature = "fast_finality")]
        None,
        #[cfg(feature = "fast_finality")]
        73107,
    );

    last_entry.ref_shards = Some(shards);

    let fake_msg_lt = 93382000003;
    let fake_msg_hash = UInt256::max();
    let shards = ShardHashes::construct_from_file("src/tests/static/shards_73102.boc").unwrap();
    shards.dump("73102");

    let mut fake_entry = ProcessedUptoStuff::with_params(
        0x9800000000000000,
        73102,
        fake_msg_lt,
        fake_msg_hash,
        #[cfg(feature = "fast_finality")]
        None,
        #[cfg(feature = "fast_finality")]
        73102,
    );

    fake_entry.ref_shards = Some(shards);

    assert!(last_entry.already_processed(&enq).unwrap());
    assert!(!fake_entry.already_processed(&enq).unwrap());
    // panic!("msg: {:?}\nenv:{:?}", env.read_message().unwrap(), env);
}

#[test]
fn test_find_shards_by_routing_real() {
    let env = MsgEnvelope::construct_from_file("src/tests/static/message.boc").unwrap();
    let (cur_prefix, next_prefix) = env.calc_cur_next_prefix().unwrap();

    let shards = ShardHashes::construct_from_file("src/tests/static/shards_73107.boc").unwrap();

    let shard_src = ShardIdent::with_tagged_prefix(0, 0xd800000000000000).unwrap();
    let shard_dst = ShardIdent::with_tagged_prefix(0, 0x9800000000000000).unwrap();

    let found_shard = shards.get_shard(&shard_src).unwrap();
    assert!(found_shard.is_some());
    assert_eq!(found_shard.unwrap().shard(), &shard_src);

    let found_shard = shards.get_shard(&shard_dst).unwrap();
    assert!(found_shard.is_some());
    assert_eq!(found_shard.unwrap().shard(), &shard_dst);

    let found_shard = shards.find_shard_by_prefix(&cur_prefix).unwrap();
    assert!(found_shard.is_some());
    assert_eq!(found_shard.unwrap().shard(), &shard_src);

    let found_shard = shards.find_shard_by_prefix(&next_prefix).unwrap();
    assert!(found_shard.is_some());
    assert_eq!(found_shard.unwrap().shard(), &shard_dst);
}

#[test]
fn test_contains_processed_up_to() {
    // crate::test_helper::init_test_log();

    // shards are not ancestors
    let put1 = ProcessedUptoStuff::with_params(
        0x40,
        4,
        8_000_007,
        [0xff; 32].into(),
        #[cfg(feature = "fast_finality")]
        None,
        #[cfg(feature = "fast_finality")]
        4,
    );
    let put2 = ProcessedUptoStuff::with_params(
        0xC0,
        5,
        9_000_007,
        [0xff; 32].into(),
        #[cfg(feature = "fast_finality")]
        None,
        #[cfg(feature = "fast_finality")]
        5,
    );
    // and shard is merge result
    let put3 = ProcessedUptoStuff::with_params(
    0x80,
        6,
        9_000_007,
        [0xff; 32].into(),
        #[cfg(feature = "fast_finality")]
        None,
        #[cfg(feature = "fast_finality")]
        6,
    );
    assert!(!put2.contains(&put1));
    assert!(!put1.contains(&put2));
    assert!(!put1.contains(&put3));
    assert!(!put2.contains(&put3));
    assert!(put3.contains(&put1));
    assert!(put3.contains(&put2));
    let mut entries = vec![put1.clone(), put2.clone()];
    assert!(!OutMsgQueueInfoStuff::compactify_entries(&mut entries).unwrap());

    let put1 = ProcessedUptoStuff::with_params(
        SHARD_FULL,
        5,
        9_000_007,
        [0x11; 32].into(),
        #[cfg(feature = "fast_finality")]
        None,
        #[cfg(feature = "fast_finality")]
        5,
    );
    let put2 = ProcessedUptoStuff::with_params(
        SHARD_FULL,
        5,
        9_000_007,
        [0xff; 32].into(),
        #[cfg(feature = "fast_finality")]
        None,
        #[cfg(feature = "fast_finality")]
        5,
    );
    let put3 = ProcessedUptoStuff::with_params(
        SHARD_FULL,
        6,
        11_000_007,
        [0xff; 32].into(),
        #[cfg(feature = "fast_finality")]
        None,
        #[cfg(feature = "fast_finality")]
        6,
    );
    assert!(put2.contains(&put1));
    assert!(!put1.contains(&put2));
    assert!(put3.contains(&put1));
    assert!(!put1.contains(&put3));

    let mut entries = vec![put1.clone(), put2.clone()];
    assert!(OutMsgQueueInfoStuff::compactify_entries(&mut entries).unwrap());
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0], put2);

    let mut entries = vec![put1.clone(), put3.clone()];
    assert!(OutMsgQueueInfoStuff::compactify_entries(&mut entries).unwrap());
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0], put3);

    //  message from workchain to masterchain
    let src = MsgAddressInt::with_standart(None, 0, [0x11; 32].into()).unwrap();
    let dst = MsgAddressInt::with_standart(None, -1, [0x77; 32].into()).unwrap();
    let value = CurrencyCollection::with_grams(30_000_000_000_000);
    let hdr = InternalMessageHeader::with_addresses_and_bounce(src, dst, value, false);
    let mut msg = Message::with_int_header(hdr);
    msg.set_at_and_lt(1600000000, 10_000_002);
    let enq = MsgEnqueueStuff::new(
        msg,
        &ShardIdent::with_workchain_id(0).unwrap(),
        Grams::zero(),
        true,
    )
    .unwrap();

    let mut put_old = ProcessedUptoStuff::with_params(
        SHARD_FULL,
        5,
        9_000_007,
        [0xff; 32].into(),
        #[cfg(feature = "fast_finality")] 
        None,
        #[cfg(feature = "fast_finality")]
        5,
    );
    let mut put_new = ProcessedUptoStuff::with_params(
        SHARD_FULL,
        6,
        11_000_007,
        [0xff; 32].into(),
        #[cfg(feature = "fast_finality")]
        None,
        #[cfg(feature = "fast_finality")]
        6,
    );
    put_old.mc_end_lt = 9000013;
    put_new.mc_end_lt = 11000013;
    let shards = ShardHashes::construct_from_base64("te6ccgEBBAEAjgABAcABAQPQQAIB61AAAAAoAAAAMAAAAAAExLQAAAAAAATEtCSdhDz+vodYDU9L35RV/vK4ixzHQkhMYcFDir7/EyuAJME/cKoGxAZgjq3+Gh9KmGOJTRGqQMd4Y5SOB04P58NwAAAAAAwAAAAAAAAAAAAAACsAOJ9kwA4ojgAAAMkDABND1ZNGAh3NZQAg").unwrap();
    shards
        .iterate_shards(|shard, descr| {
            assert_eq!(shard, ShardIdent::with_workchain_id(0).unwrap());
            assert_eq!(descr.seq_no, 5);
            assert_eq!(descr.start_lt, 10_000_000);
            assert_eq!(descr.end_lt, 10_000_004);
            Ok(true)
        })
        .unwrap();
    put_new.ref_shards = Some(shards);
    assert!(put_new.contains(&put_old));
    assert!(!put_old.contains(&put_new));

    assert!(!put_old.already_processed(&enq).unwrap());
    assert!(put_new.already_processed(&enq).unwrap());

    //  message from masterchain to workchain
    let src = MsgAddressInt::with_standart(None, -1, [0x77; 32].into()).unwrap();
    let dst = MsgAddressInt::with_standart(None, 0, [0x11; 32].into()).unwrap();
    let value = CurrencyCollection::with_grams(30_000_000_000_000);
    let hdr = InternalMessageHeader::with_addresses_and_bounce(src, dst, value, false);
    let mut msg = Message::with_int_header(hdr);
    msg.set_at_and_lt(1600000000, 5_000_007);
    let enq =
        MsgEnqueueStuff::new(msg, &ShardIdent::masterchain(), Grams::default(), true).unwrap();

    let mut put_old = ProcessedUptoStuff::with_params(
        SHARD_FULL,
        2,
        3_000_012,
        [0xff; 32].into(),
        #[cfg(feature = "fast_finality")]
        None,
        #[cfg(feature = "fast_finality")]
        2
    );
    let mut put_new = ProcessedUptoStuff::with_params(
        SHARD_FULL,
        3,
        5_000_007,
        enq.message_hash(),
        #[cfg(feature = "fast_finality")]
        None,
        #[cfg(feature = "fast_finality")]
        2,
    );
    put_old.mc_end_lt = 3000013;
    put_new.mc_end_lt = 5000015;
    assert!(put_new.contains(&put_old));
    assert!(!put_old.contains(&put_new));

    assert!(!put_old.already_processed(&enq).unwrap());
    assert!(put_new.already_processed(&enq).unwrap());
}

#[derive(Clone, Eq, PartialEq)]
pub struct NewMessageTest {
    lt_hash: (u64, UInt256),
    msg: Message,
    tr_cell: Cell,
    prefix: AccountIdPrefixFull,
}

impl NewMessageTest {
    pub fn new(
        lt_hash: (u64, UInt256),
        msg: Message,
        tr_cell: Cell,
        prefix: AccountIdPrefixFull,
    ) -> Self {
        Self {
            lt_hash,
            msg,
            tr_cell,
            prefix,
        }
    }
}

impl Ord for NewMessageTest {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.lt_hash.cmp(&self.lt_hash)
    }
}

impl PartialOrd for NewMessageTest {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub fn generate_test_queue(
    size: u64,
    enqueue_lt: u64,
    mut test_addresses: Vec<&str>,
) -> (OutMsgQueue, BinaryHeap<NewMessageTest>) {
    use ton_block::*;
    use ton_types::*;

    let use_test_addresses = test_addresses.len() as u64 == size;
    test_addresses.reverse();

    let max_lt = size;

    println!("Queue lenth: {}, enqueue count: {}", max_lt, enqueue_lt);

    let mut queue = OutMsgQueue::default();
    let address = AccountId::from([0x11; 32]);
    let src = MsgAddressInt::with_standart(None, 0, address).unwrap();
    println!("src addr: {src}");

    let mut new_messages = BinaryHeap::default();

    let now = Instant::now();
    for lt in 1..=max_lt {
        let address = AccountId::from(UInt256::rand());
        let prefix = OutMsgQueueKey::first_u64(&address);
        let account_id_prefix = AccountIdPrefixFull {
            workchain_id: 0,
            prefix,
        };
        let dst = if use_test_addresses {
            let addr_str = test_addresses.pop().unwrap();
            MsgAddressInt::from_str(addr_str).unwrap()
        } else {
            MsgAddressInt::with_standart(None, 0, address).unwrap()
        };
        if max_lt < 33 {
            println!("dst addr: {dst}");
        }

        let mut h = InternalMessageHeader::with_addresses(src.clone(), dst, Default::default());
        h.created_lt = lt;
        let builder = lt.write_to_new_cell().unwrap();
        let body = SliceData::load_builder(builder).unwrap();
        let msg = Message::with_int_header_and_body(h.clone(), body);
        let env = MsgEnvelope::with_message_and_fee(&msg, 1_000_000_000.into()).unwrap();
        queue.insert(0, prefix, &env, lt).unwrap();

        if lt < enqueue_lt {
            let new_msg = NewMessageTest::new(
                (lt, env.message_hash()),
                msg,
                Default::default(),
                account_id_prefix.clone(),
            );
            new_messages.push(new_msg);
        }
    }
    println!("...queue filled in {} micros", now.elapsed().as_micros());

    (queue, new_messages)
}

#[test]
fn test_clean_queue() {
    use ton_block::*;
    use ton_types::*;

    // init log
    let level = log::LevelFilter::Debug;
    let stdout = log4rs::append::console::ConsoleAppender::builder()
        .encoder(Box::new(log4rs::encode::pattern::PatternEncoder::new(
            "{m}{n}",
        )))
        .target(log4rs::append::console::Target::Stdout)
        .build();
    let config = log4rs::config::Config::builder()
        .appender(
            log4rs::config::Appender::builder()
                .filter(Box::new(log4rs::filter::threshold::ThresholdFilter::new(
                    level,
                )))
                .build("stdout", Box::new(stdout)),
        )
        .build(
            log4rs::config::Root::builder()
                .appender("stdout")
                .build(level),
        )
        .unwrap();
    let logger_handle = log4rs::init_config(config);
    if let Err(e) = logger_handle {
        println!("Error init log: {}", e);
    }

    fn clean_out_msg_queue(
        mut queue: OutMsgQueue,
        clean_lt: u64,
        log_msgs_order: bool,
    ) -> Result<()> {
        let mut max_process_count = clean_lt;
        let mut log_num = 0;
        queue.hashmap_filter(|_key, mut slice| {
            if max_process_count == 0 {
                return Ok(HashmapFilterResult::Stop);
            }
            max_process_count -= 1;

            let lt = u64::construct_from(&mut slice)?;
            let enq = MsgEnqueueStuff::construct_from(&mut slice, lt)?;

            if log_msgs_order {
                log_num += 1;
                println!("{log_num}: lt {lt}, {}", enq.message().dst().unwrap());
            }

            if enq.created_lt() <= clean_lt {
                return Ok(HashmapFilterResult::Remove);
            } else {
                return Ok(HashmapFilterResult::Accept);
            }
        })
    }
    fn clean_out_msg_queue_ordered(
        mut queue: OutMsgQueue,
        clean_lt: u64,
        log_msgs_order: bool,
    ) -> Result<()> {
        let mut log_num = 0;
        let mut iter = MsgQueueMergerIterator::from_queue(&queue)?;
        while let Some(k_v) = iter.next() {
            let (key, enq, lt, _) = k_v?;

            if log_msgs_order {
                log_num += 1;
                println!("{log_num}: lt {lt}, {}", enq.message().dst().unwrap());
            }

            if lt > clean_lt {
                continue;
            }

            queue.remove(SliceData::load_builder(key.write_to_new_cell()?)?)?;
        }
        queue.after_remove()?;

        Ok(())
    }

    fn clean_out_msg_queue_ordered_new(
        mut queue: OutMsgQueue,
        clean_lt: u64,
        time_limit_micros: i128,
        log_msgs_order: bool,
    ) -> Result<()> {
        use super::super::out_msg_queue_cleaner::*;

        let mut log_num = 0;

        hashmap_filter_ordered_by_lt_hash(&mut queue, clean_lt, time_limit_micros, |node_obj| {
            let lt = node_obj.lt();
            let mut data_and_refs = node_obj.data_and_refs()?;
            let enq = MsgEnqueueStuff::construct_from(&mut data_and_refs, lt)?;

            if log_msgs_order {
                log_num += 1;
                println!("{log_num}: lt {lt}, {}", enq.message().dst().unwrap());
            }

            if enq.created_lt() <= clean_lt {
                return Ok(HashmapFilterResult::Remove);
            } else {
                return Ok(HashmapFilterResult::Accept);
            }
        }, None)?;

        Ok(())
    }

    fn clean_out_msg_queue_combined(
        mut queue: OutMsgQueue,
        clean_lt: u64,
        stop_timeout_ms: u32,
        clean_timeout_percentage_points: u32,
        optimistic_clean_percentage_points: u32,
        log_msgs_order: bool,
    ) -> Result<()> {
        use super::super::out_msg_queue_cleaner::*;

        let timer = std::time::Instant::now();

        let mut partial = false;
        let mut skipped = 0;
        let mut deleted = 0;

        let mut log_num = 0;

        println!(
            "clean_out_msg_queue_combined: stop_timeout_ms = {}, clean_timeout_percentage_points = {}, optimistic_clean_percentage_points = {}",
            stop_timeout_ms,
            clean_timeout_percentage_points,
            optimistic_clean_percentage_points,
        );

        let clean_timeout_nanos = 
            (stop_timeout_ms as i128) * 1_000_000 *
            (clean_timeout_percentage_points as i128) / 1000;

        let fast_filtering_timeout_nanos = clean_timeout_nanos * (optimistic_clean_percentage_points as i128) / 1000;

        queue.hashmap_filter(|_key, mut slice| {
            // stop fast filtering when reached the time limit
            let fast_filtering_elapsed_nanos = timer.elapsed().as_nanos() as i128;
            if fast_filtering_elapsed_nanos >= fast_filtering_timeout_nanos {
                println!(
                    "clean_out_msg_queue_combined: stopped fast cleaning messages queue because of time elapsed {} >= {} limit",
                    fast_filtering_elapsed_nanos, fast_filtering_timeout_nanos,
                );
                partial = true;
                return Ok(HashmapFilterResult::Stop)
            }

            let lt = u64::construct_from(&mut slice)?;
            let enq = MsgEnqueueStuff::construct_from(&mut slice, lt)?;

            if log_msgs_order {
                log_num += 1;
                println!("{log_num}: lt {lt}, {}", enq.message().dst().unwrap());
            }

            if enq.created_lt() <= clean_lt {
                deleted += 1;
                return Ok(HashmapFilterResult::Remove);
            } else {
                skipped += 1;
                return Ok(HashmapFilterResult::Accept);
            }
        })?;

        // if fast cleaning processed only a part of queue
        // then run slower ordered cleaning for the rest of time limit
        if partial {
            let max_processed_lt = clean_lt;

            let fast_filtering_elapsed_nanos = timer.elapsed().as_nanos() as i128;
            let ordered_cleaning_timeout_nanos = 
                clean_timeout_nanos - fast_filtering_elapsed_nanos;
            println!(
                "clean_out_msg_queue_combined: clean_timeout = {}, fast_filtering_elapsed = {}, deleted = {}, skipped = {}, ordered_cleaning_timeout = {}",
                clean_timeout_nanos,
                fast_filtering_elapsed_nanos,
                deleted,
                skipped,
                ordered_cleaning_timeout_nanos,
            );

            hashmap_filter_ordered_by_lt_hash(
                &mut queue,
                max_processed_lt,
                ordered_cleaning_timeout_nanos,
                |node_obj| {
                    let lt = node_obj.lt();
                    let mut data_and_refs = node_obj.data_and_refs()?;
                    let enq = MsgEnqueueStuff::construct_from(&mut data_and_refs, lt)?;

                    if log_msgs_order {
                        log_num += 1;
                        println!("{log_num}: lt {lt}, {}", enq.message().dst().unwrap());
                    }

                    if enq.created_lt() <= clean_lt {
                        deleted += 1;
                        return Ok(HashmapFilterResult::Remove);
                    } else {
                        skipped += 1;
                        return Ok(HashmapFilterResult::Accept);
                    }
                },
                None,
            )?;
        }

        println!(
            "clean_out_msg_queue_combined: deleted = {}, skipped = {}, clean_timeout = {}, elapsed = {}",
            deleted,
            skipped,
            clean_timeout_nanos,
            timer.elapsed().as_nanos(),
        );

        Ok(())
    }

    fn process_new_messages(
        mut new_messages: BinaryHeap<NewMessageTest>,
        shard: &ShardIdent,
    ) -> Result<()> {
        for NewMessageTest {
            lt_hash: (_created_lt, _hash),
            msg,
            tr_cell,
            prefix: _,
        } in new_messages.drain()
        {
            let info = msg
                .int_header()
                .ok_or_else(|| error!("message is not internal"))
                .unwrap();
            let fwd_fee = *info.fwd_fee();
            let enq = MsgEnqueueStuff::new(msg, shard, fwd_fee, false).unwrap();
            // collator_data.add_out_msg_to_state(&enq, true)?;
            let _out_msg = OutMsg::new(enq.envelope_cell(), tr_cell);
            // collator_data.add_out_msg_to_block(hash, &out_msg)?;
        }
        Ok(())
    }
    fn process_new_messages_without_drain(
        mut new_messages: BinaryHeap<NewMessageTest>,
        shard: &ShardIdent,
    ) -> Result<()> {
        while let Some(NewMessageTest {
            lt_hash: (_created_lt, _hash),
            msg,
            tr_cell,
            prefix: _,
        }) = new_messages.pop()
        {
            let info = msg
                .int_header()
                .ok_or_else(|| error!("message is not internal"))
                .unwrap();
            let fwd_fee = *info.fwd_fee();
            let enq = MsgEnqueueStuff::new(msg, shard, fwd_fee, false).unwrap();
            // collator_data.add_out_msg_to_state(&enq, true)?;
            let _out_msg = OutMsg::new(enq.envelope_cell(), tr_cell);
            // collator_data.add_out_msg_to_block(hash, &out_msg)?;
        }

        Ok(())
    }
    fn split_creating_shard(mut queue: OutMsgQueue, shard: &ShardIdent) -> Result<()> {
        let mut sibling_queue = OutMsgQueue::default();
        queue.hashmap_filter(|key, mut slice| {
            let lt = u64::construct_from(&mut slice)?;
            let value = slice.clone();
            let enq = MsgEnqueueStuff::construct_from(&mut slice, lt)?;
            if !shard.contains_full_prefix(enq.cur_prefix()) {
                let key = SliceData::load_builder(key.clone())?;
                sibling_queue.set_builder_serialized(key, &value.into_builder(), &lt)?;
                Ok(HashmapFilterResult::Remove)
            } else {
                Ok(HashmapFilterResult::Accept)
            }
        })
    }

    fn split_filtering_to_shard(mut queue: OutMsgQueue, shard: &ShardIdent) -> Result<()> {
        queue.hashmap_filter(|_key, mut slice| {
            let lt = u64::construct_from(&mut slice)?;
            let enq = MsgEnqueueStuff::construct_from(&mut slice, lt)?;
            if shard.contains_full_prefix(enq.cur_prefix()) {
                Ok(HashmapFilterResult::Accept)
            } else {
                Ok(HashmapFilterResult::Remove)
            }
        })
    }

    fn split_filtering_to_shard_and_creating_remaining(
        mut queue: OutMsgQueue,
        shard: &ShardIdent,
    ) -> Result<()> {
        queue.hashmap_filter_split(|_key, mut slice| {
            let lt = u64::construct_from(&mut slice)?;
            let enq = MsgEnqueueStuff::construct_from(&mut slice, lt)?;
            if shard.contains_full_prefix(enq.cur_prefix()) {
                Ok(HashmapFilterSplitResult::Stay)
            } else {
                Ok(HashmapFilterSplitResult::Move)
            }
        })?;
        Ok(())
    }

    fn split_compare(mut queue: OutMsgQueue, shard: &ShardIdent) -> Result<()> {
        let mut left = queue.clone();
        left.hashmap_filter(|_key, mut slice| {
            let lt = u64::construct_from(&mut slice)?;
            let enq = MsgEnqueueStuff::construct_from(&mut slice, lt)?;
            if shard.contains_full_prefix(enq.cur_prefix()) {
                Ok(HashmapFilterResult::Accept)
            } else {
                Ok(HashmapFilterResult::Remove)
            }
        })?;
        let mut right = queue.clone();
        right.hashmap_filter(|_key, mut slice| {
            let lt = u64::construct_from(&mut slice)?;
            let enq = MsgEnqueueStuff::construct_from(&mut slice, lt)?;
            if shard.contains_full_prefix(enq.cur_prefix()) {
                Ok(HashmapFilterResult::Remove)
            } else {
                Ok(HashmapFilterResult::Accept)
            }
        })?;
        let sibling = queue.hashmap_filter_split(|_key, mut slice| {
            let lt = u64::construct_from(&mut slice)?;
            let enq = MsgEnqueueStuff::construct_from(&mut slice, lt)?;
            if shard.contains_full_prefix(enq.cur_prefix()) {
                Ok(HashmapFilterSplitResult::Stay)
            } else {
                Ok(HashmapFilterSplitResult::Move)
            }
        })?;
        assert_eq!(left, queue);
        assert_eq!(right, sibling);
        println!("Shard left lenth: {}", left.len()?);
        Ok(())
    }

    fn check_multi(attempts: u128, msg: &str, mut f: impl FnMut() -> Result<()>) {
        let mut result = 0;
        for _ in 0..attempts {
            let now = Instant::now();
            f().unwrap();
            result += now.elapsed().as_micros();
        }
        println!("{: >10} micros - {} average", result / attempts, msg);
    }

    let shard = ShardIdent::full(0);
    let max_lt = 100000;
    let clean_lt = 30000;
    let enqueue_lt = 25;

    let test_msgs = vec![
        "0:f4c1fea009ce6b035eecf68a94d8682fa775575016f588a55689ef31ce03fa83",
        "0:3dbb2f54c0e72249169e713f2adb14d232e709ce7702f47d2d0bbb6fd88e6adf",
        "0:fbcefcea78312c359cd3ee94ca5623ee2c92d40d3fa2bea0b501f2d41477934e",
        "0:82a5b555bd48f635e4c77cc993398a44da45fe5adf15e0c677450da397ed436b",
        "0:dcb35d20f89c4136f049068f89a44be56fb37b9069fc03736495a554c2b673ab",
        "0:582744f58b0552d0611408a77c2e8973e8ca50d624f2c106cb032bc5a895d34f",
        "0:53d6b5863a58942c4103588a588e89e6c545078b5b2bff5ee556a46fa1207c0e",
        "0:0148bff9debe380e775159f543e5f9d2f4e277d1ff4482a2439183f3222c1a1f",
    ];

    let (queue, new_messages) = generate_test_queue(max_lt, enqueue_lt, test_msgs);
    println!("to clean count: {clean_lt}",);

    // println!("queue {:#.10}", queue.data().unwrap());

    let mut log_msgs_order = max_lt < 33;
    check_multi(1, "Cleaning queue ordered", || {
        clean_out_msg_queue_ordered(queue.clone(), clean_lt, log_msgs_order)?;
        log_msgs_order = false;
        Ok(())
    });
    let mut log_msgs_order = max_lt < 33;
    check_multi(5, "Cleaning queue ordered new", || {
        clean_out_msg_queue_ordered_new(queue.clone(), clean_lt, 300_000_000, log_msgs_order)?;
        log_msgs_order = false;
        Ok(())
    });
    let mut log_msgs_order = max_lt < 33;
    check_multi(5, "Cleaning queue unordered", || {
        clean_out_msg_queue(queue.clone(), clean_lt, log_msgs_order)?;
        log_msgs_order = false;
        Ok(())
    });

    let mut log_msgs_order = max_lt < 33;
    check_multi(5, "Cleaning queue combined: unordered + new ordered", || {
        clean_out_msg_queue_combined(queue.clone(), clean_lt, 3_000, 300, 400, log_msgs_order)?;
        log_msgs_order = false;
        Ok(())
    });

    let (s0, s1) = shard.split().unwrap();
    split_compare(queue.clone(), &s0).unwrap();

    check_multi(10, "Processing new messages", || {
        process_new_messages(new_messages.clone(), &shard)
    });
    check_multi(10, "Processing new messages without drain", || {
        process_new_messages_without_drain(new_messages.clone(), &shard)
    });

    check_multi(10, "Splitting queue creating left", || {
        split_creating_shard(queue.clone(), &s0)
    });
    check_multi(10, "Splitting queue creating right", || {
        split_creating_shard(queue.clone(), &s1)
    });
    check_multi(10, "Splitting queue filtering to left", || {
        split_filtering_to_shard(queue.clone(), &s0)
    });
    check_multi(10, "Splitting queue filtering to right", || {
        split_filtering_to_shard(queue.clone(), &s1)
    });
    check_multi(
        10,
        "Splitting queue filtering to left and creating remaining",
        || split_filtering_to_shard_and_creating_remaining(queue.clone(), &s0),
    );
}
