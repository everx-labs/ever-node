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

use super::*;
use std::str::FromStr;
use ton_block::{
    CurrencyCollection, InternalMessageHeader, MsgAddressInt
};

#[test]
fn test_msg_envelope() {
    let src = MsgAddressInt::from_str("0:a4cd3dfa89c5aa75c5542b3414d1c0c7974aaa5009fbe9c4ab6a5566c50cc607").unwrap();
    let dst = MsgAddressInt::from_str("0:7c270a15afd9b685d6cabe4334172fa0d2e8ceb61ac033e7ce8876f2cf7122be").unwrap();
    let src_shard = ShardIdent::with_tagged_prefix(0, 0xa400000000000000).unwrap();
    let dst_shard = ShardIdent::with_tagged_prefix(0, 0x7c00000000000000).unwrap();
    let hdr = InternalMessageHeader::with_addresses(src, dst, CurrencyCollection::default());
    let msg = Message::with_int_header(hdr);
    let env = MsgEnvelopeStuff::new(msg.clone(), &src_shard, Grams::default(), false).unwrap();
    assert!(src_shard.contains_full_prefix(env.cur_prefix()));
    assert!(dst_shard.contains_full_prefix(env.next_prefix()));

    let env = MsgEnvelopeStuff::new(msg, &src_shard, Grams::default(), true).unwrap();
    assert!(src_shard.contains_full_prefix(env.cur_prefix()));
    assert!(!dst_shard.contains_full_prefix(env.next_prefix()));
    let shard = ShardIdent::with_tagged_prefix(0, 0x7400000000000000).unwrap();
    assert!(shard.contains_full_prefix(env.next_prefix()));
}
