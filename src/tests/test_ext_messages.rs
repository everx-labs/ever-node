/*
* Copyright (C) 2019-2023 EverX. All Rights Reserved.
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

use super::*;
use ton_block::{
    InternalMessageHeader, ExternalInboundMessageHeader, Serializable,
    GetRepresentationHash, MsgAddressInt, MsgAddressExt,
};
use ton_types::{
    BuilderData, IBitstring, CellType, SliceData, write_boc, BocWriter
};

#[test]
fn test_create_ext_message_big_data() {
    let big_data = [0; MAX_EXTERNAL_MESSAGE_SIZE + 6];
    create_ext_message(&big_data).expect_err("it must return error");
}

#[test]
fn test_create_ext_message_bad_data() {
    let big_data = [0; 100];
    create_ext_message(&big_data).expect_err("it must not accept wrong format BOC");
}

#[test]
fn test_create_ext_message_bad_boc_roots() {
    let cell1 = 0xfff1u32.serialize().unwrap();
    let cell2 = 0xfff3u32.serialize().unwrap();

    let boc = BocWriter::with_roots([cell1, cell2]).unwrap();
    let mut data = vec![];
    boc.write(&mut data).unwrap();

    create_ext_message(&data).expect_err("it must accept BOC only with one root");
}

#[test]
fn test_create_ext_message_bad_boc_level() {

    let mut root1 = BuilderData::new();
    root1.append_u8(1).unwrap();
    root1.append_u8(1).unwrap();
    root1.append_u8(0).unwrap();
    root1.append_u8(0).unwrap();
    root1.append_u128(0).unwrap();
    root1.append_u128(0).unwrap();
    root1.set_type(CellType::PrunedBranch);
    let cell1 = root1.into_cell().unwrap();

    let data = write_boc(&cell1).unwrap();

    create_ext_message(&data).expect_err("it must accept BOC only with one root");
}

#[test]
fn test_create_ext_message_bad_boc_depth() {
    let mut root = BuilderData::new();
    for _ in 0..600 {
        root.append_u32(0xfff0).unwrap();
        let mut new_root = BuilderData::new();
        new_root.checked_append_reference(root.into_cell().unwrap()).unwrap();
        root = new_root;
    }
    let cell = root.into_cell().unwrap();
    let data = write_boc(&cell).unwrap();

    create_ext_message(&data).expect_err("it must accept BOC only with correct depth size");
}

#[test]
fn test_create_ext_message_bad_boc_cells() {
    let mut root1 = BuilderData::new();
    root1.append_u32(0xfff1).unwrap();
    let cell1 = root1.into_cell().unwrap();
    let data = write_boc(&cell1).unwrap();

    create_ext_message(&data).expect_err("it must accept BOC only with external inbound message");
}

#[test]
fn test_create_ext_message_bad_message_format() {
    let msg = Message::with_int_header(InternalMessageHeader::default());
    let b = msg.serialize().unwrap();
    let data = write_boc(&b.into()).unwrap();

    create_ext_message(&data).expect_err("it must accept BOC only with external inbound message");
}

#[test]
fn test_create_ext_message() {
    let msg = Message::with_ext_in_header(ExternalInboundMessageHeader::default());
    let b = msg.serialize().unwrap();
    let data = write_boc(&b.into()).unwrap();

    create_ext_message(&data).unwrap();
}

#[test]
#[cfg(not(feature = "fast_finality"))]
fn test_message_keeper() {
    let m = Message::with_ext_in_header(ExternalInboundMessageHeader::default());
    let mk = MessageKeeper::new(Arc::new(m));

    assert!(mk.check_active(10000));

    assert!(mk.can_postpone());
    mk.postpone(200);
    mk.postpone(300);
    mk.postpone(400);

    assert!(mk.check_active(201));
    assert!(mk.check_active(205));

    assert!(mk.can_postpone());
    mk.postpone(306);

    assert!(!mk.check_active(307));
    assert!(!mk.check_active(310));
    assert!(mk.check_active(311));
    assert!(mk.check_active(312));

    assert!(mk.can_postpone());
    mk.postpone(320);
    assert!(mk.check_active(335));

    assert!(!mk.can_postpone());
}

#[test]
#[cfg(not(feature = "fast_finality"))]
fn test_message_keeper_multithread() {
    let m = Message::with_ext_in_header(ExternalInboundMessageHeader::default());
    let mk = Arc::new(MessageKeeper::new(Arc::new(m)));

    let mut hs = vec!();
    for _ in 0..50 {
        let mk = mk.clone();
        let h = std::thread::spawn(move || {
            assert!(mk.check_active(10000));
            std::thread::sleep(std::time::Duration::from_millis(200));

            assert!(mk.can_postpone());
            mk.postpone(10001);
            std::thread::sleep(std::time::Duration::from_millis(200));
        
            assert!(mk.check_active(10006));
            std::thread::sleep(std::time::Duration::from_millis(200));

            assert!(mk.can_postpone());
            mk.postpone(10007);
            assert!(!mk.check_active(10009));
            std::thread::sleep(std::time::Duration::from_millis(200));

            assert!(mk.check_active(10012));
            std::thread::sleep(std::time::Duration::from_millis(200));

            assert!(mk.can_postpone());
            mk.postpone(10013);
            std::thread::sleep(std::time::Duration::from_millis(200));

            assert!(mk.check_active(10023));
            assert!(!mk.can_postpone());
       });
       hs.push(h);
    }

    for h in hs {
        h.join().unwrap();
    }
}

fn create_external_message(dst_shard: u8, salt: Vec<u8>) -> Arc<Message>  {
    let mut hdr = ExternalInboundMessageHeader::default();
    let length_in_bits = salt.len() * 8;
    let address = SliceData::from_raw(salt, length_in_bits);
    hdr.src = MsgAddressExt::with_extern(address).unwrap();
    hdr.dst = MsgAddressInt::with_standart(None, 0, [dst_shard; 32].into()).unwrap();
    hdr.import_fee = 10u64.into();
    Arc::new(Message::with_ext_in_header(hdr))
}

include!("../../common/src/log.rs");

#[test]
#[cfg(not(feature = "fast_finality"))]
fn test_messages_pool() {
    init_log_without_config(log::LevelFilter::Trace, None);
    let mp = Arc::new(MessagesPool::new(0));

    // create 3 messages, 2 of them are with the prefix 0x01 and one with 0x22
    let m = create_external_message(1, vec!(1));
    let id1 = m.hash().unwrap();
    mp.new_message(id1.clone(), m, 0).unwrap();

    let m = create_external_message(1, vec!(2));
    let id2 = m.hash().unwrap();
    // mp.new_message(id2.clone(), m, 0).unwrap();

    let m = create_external_message(0x22, vec!(2));
    let id3 = m.hash().unwrap();
    mp.new_message(id3.clone(), m.clone(), 0).unwrap();

    // get messages for shard 0x8000_0000_0000_0000 - total 3 messages
    let m1 = mp.get_messages(&ShardIdent::with_tagged_prefix(0, 0x8000_0000_0000_0000).unwrap(), 1).unwrap(); 
    assert_eq!(m1.len(), 2);

    // get messages for shard 0x3000_0000_0000_0000 - total 1 message with prefix 0x22
    let m1 = mp.get_messages(&ShardIdent::with_tagged_prefix(0, 0x3000_0000_0000_0000).unwrap(), 1).unwrap(); 
    assert_eq!(m1.len(), 1);
    assert_eq!(m1[0].0, m);

    // postpone message with prefix 0x22 for first time and delete second message with prefix 0x01
    mp.complete_messages(vec!(id3.clone()), vec!(id2), 1).unwrap();

    // get messages for shard 0x1000_0000_0000_0000 - total 1 message with prefix 0x01
    let m1 = mp.get_messages(&ShardIdent::with_tagged_prefix(0, 0x1000_0000_0000_0000).unwrap(), 3).unwrap(); 
    assert_eq!(m1.len(), 1);
    assert_eq!(m1[0].1, id1);

    // get messages for shard 0x3000_0000_0000_0000 - total 1 message with prefix 0x22
    let m1 = mp.get_messages(&ShardIdent::with_tagged_prefix(0, 0x3000_0000_0000_0000).unwrap(), 4).unwrap(); 
    assert_eq!(m1.len(), 1);
    assert_eq!(m1[0].0, m);

    // postpone message with prefix 0x22 for second time
    mp.complete_messages(vec!(id3.clone()), vec!(), 5).unwrap();

    // get messages for shard 0x3000_0000_0000_0000 - no messages
    let m1 = mp.get_messages(&ShardIdent::with_tagged_prefix(0, 0x3000_0000_0000_0000).unwrap(), 6).unwrap(); 
    assert_eq!(m1.len(), 0);

    // get messages for shard 0x3000_0000_0000_0000 - total 1 message with prefix 0x22
    let m1 = mp.get_messages(&ShardIdent::with_tagged_prefix(0, 0x3000_0000_0000_0000).unwrap(), 11).unwrap(); 
    assert_eq!(m1.len(), 1);

    // postpone message with prefix 0x22 for third time
    mp.complete_messages(vec!(id3.clone()), vec!(), 12).unwrap();

    // get messages for shard 0x3000_0000_0000_0000 - total 1 message with prefix 0x22
    let m1 = mp.get_messages(&ShardIdent::with_tagged_prefix(0, 0x3000_0000_0000_0000).unwrap(), 30).unwrap(); 
    assert_eq!(m1.len(), 1);

    // try to postpone message with prefix 0x22 for fourth time - it will be deleted
    mp.complete_messages(vec!(id3.clone()), vec!(), 42).unwrap();

    // get messages for shard 0x3000_0000_0000_0000 - no messages
    let m1 = mp.get_messages(&ShardIdent::with_tagged_prefix(0, 0x3000_0000_0000_0000).unwrap(), 100).unwrap(); 
    assert_eq!(m1.len(), 0);

    // get messages for shard 0x1000_0000_0000_0000 - no messages because it was expired
    let m1 = mp.get_messages(&ShardIdent::with_tagged_prefix(0, 0x1000_0000_0000_0000).unwrap(), 601).unwrap(); 
    assert_eq!(m1.len(), 0);
}


async fn check_messages(mp: Arc<MessagesPool>, shard: ShardIdent, now: u32, expected_count: usize) -> Result<()> {
    for _ in 0..20 {
        // let count = mp.clone().get_messages(&shard, now).unwrap().len();
        let count = mp.clone().iter(shard.clone(), now).count();
        if count != expected_count {
            fail!("Wrong messages count for shard {} expected {}, got {}", shard, expected_count, count)
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ext_messages_multi_threads() {
    const M: usize = 50;
    let mp = Arc::new(MessagesPool::new(0));

    // for dst_shard in [0, 0x40, 0x80, 0xC0] {
    for dst_shard in [0, 0x20, 0x40, 0x60, 0x80, 0xA0, 0xC0, 0xE0] {
        for i in 0..M {
            let m = create_external_message(dst_shard, vec!(i as u8));
            let id = m.hash().unwrap();
            mp.new_message(id.clone(), m, 0).unwrap();
        }
    }

    let tests = [
        [(0x20, M*2), (0x60, M*2), (0xA0, M*2), (0xE0, M*2)],
        [(0x40, M*4), (0xA0, M*2), (0xD0, M), (0xF0, M)],
    ];

    for test_case in tests {
        let mut handles = Vec::new();
        for (shard, expected_count) in test_case {
            let shard = ShardIdent::with_tagged_prefix(0, shard << 56).unwrap();
            let mp = Arc::clone(&mp);
            let handle = tokio::spawn(check_messages(mp, shard, 0, expected_count));
            handles.push(handle);
        }
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    }
}

