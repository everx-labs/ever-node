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

use super::*;
use crate::{
    collator_test_bundle::{try_collate, CollatorTestBundle},
    engine_traits::EngineOperations,
    test_helper::compare_blocks,
    test_helper::test_async, types::messages::{count_matching_bits, MsgEnvelopeStuff},
};
use ever_block::{Result, AccountIdPrefixFull};
use pretty_assertions::assert_eq;
use std::{fs::{create_dir_all, remove_dir_all}, sync::Arc};

const RES_PATH: &'static str = "target/cmp";

async fn try_collate_by_bundle(bundle: Arc<CollatorTestBundle>) -> Result<CollateResult> {
    let collate_result = try_collate_by_engine(
        bundle.clone(),
        bundle.block_id().shard().clone(),
        bundle.prev_blocks_ids().clone(),
        Some(bundle.created_by().clone()),
        match bundle.ethalon_block()? {
            Some(block) => Some(block.block()?.read_extra().unwrap().rand_seed().clone()),
            None => bundle.rand_seed().cloned()
        },
        true,
    ).await?;
    if let Some(ethalon_block) = bundle.ethalon_block()? {
        let mut block = match &collate_result.candidate {
            Some(candidate) => {
                Block::construct_from_bytes(&candidate.data)?
            }
            None => return Err(collate_result.error.unwrap())
        };
        if let Err(result) = compare_blocks(ethalon_block.block()?, &mut block) {
            panic!("Blocks are not equal: {}", result);
        }
    }
    Ok(collate_result)
}

async fn try_collate_by_engine(
    engine: Arc<dyn EngineOperations>,
    shard: ShardIdent,
    prev_blocks_ids: Vec<BlockIdExt>,
    created_by_opt: Option<UInt256>,
    rand_seed_opt: Option<UInt256>,
    skip_state_update: bool,
) -> Result<CollateResult> {
    std::fs::create_dir_all(RES_PATH).ok();
    try_collate(&engine, shard, prev_blocks_ids, created_by_opt, rand_seed_opt, skip_state_update, false).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_collate_first_block() {
    async fn test() -> Result<()> {
        create_dir_all(RES_PATH).ok();
        //init_test_log();
        match CollatorTestBundle::build_with_zero_state(
        "src/tests/static/zerostate.boc",
        &["src/tests/static/basestate0.boc", "src/tests/static/basestate0.boc"]
        ).await {
            Ok(bundle) => try_collate_by_bundle(Arc::new(bundle)).await.map(|_| ()),
            Err(e) => Err(e)
        }
    }
    test_async(
        || Box::pin(test()),
        || { remove_dir_all(RES_PATH).ok(); }
    ).await;
}

// prepare for testing purposes
fn prepare_test_env_message(
    src_prefix: u64, 
    dst_prefix: u64, 
    bits: u8, 
    at: u32, 
    lt: u64, 
    use_hypercube: bool
) -> Result<MsgEnvelopeStuff> {
    let shard = ShardIdent::with_prefix_len(bits, 0, src_prefix)?;
    let src = UInt256::from_le_bytes(&src_prefix.to_be_bytes());
    let dst = UInt256::from_le_bytes(&dst_prefix.to_be_bytes());
    let src = MsgAddressInt::with_standart(None, 0, src.into())?;
    let dst = MsgAddressInt::with_standart(None, 0, dst.into())?;

    // let src_prefix = AccountIdPrefixFull::prefix(&src).unwrap();
    // let dst_prefix = AccountIdPrefixFull::prefix(&dst).unwrap();
    // let ia = IntermediateAddress::full_src();
    // let route_info = src_prefix.perform_hypercube_routing(&dst_prefix, &shard, ia)?.unwrap();
    // let cur_prefix  = src_prefix.interpolate_addr_intermediate(&dst_prefix, &route_info.0)?;
    // let next_prefix = src_prefix.interpolate_addr_intermediate(&dst_prefix, &route_info.1)?;

    let hdr = InternalMessageHeader::with_addresses(src, dst, CurrencyCollection::with_grams(1_000_000_000));
    let mut msg = Message::with_int_header(hdr);
    msg.set_at_and_lt(at, lt);
    MsgEnvelopeStuff::new(CommonMessage::Std(msg), &shard, Grams::from(1_000_000), use_hypercube, SERDE_OPTS_EMPTY)
}

#[test]
fn test_hypercube_routing_off() {
    let pfx_len = 12;
    let src = 0xd78b3fd904191a09;
    let dst = 0xd4d300cee029b9c7;
    let hop = 0xd48b3fd904191a09;
    let env = prepare_test_env_message(src, dst, pfx_len, 0, 0, false).unwrap();
    let src_shard_id = ShardIdent::with_prefix_len(pfx_len, 0, src).unwrap();
    let dst_shard_id = ShardIdent::with_prefix_len(pfx_len, 0, dst).unwrap();
    let hop_shard_id = ShardIdent::with_prefix_len(pfx_len, 0, hop).unwrap();
    let src_prefix = env.src_prefix();
    let dst_prefix = env.dst_prefix();
    assert!(src_shard_id.contains_full_prefix(&src_prefix));
    assert!(dst_shard_id.contains_full_prefix(&dst_prefix));

    assert_eq!(src_prefix, &AccountIdPrefixFull::workchain(0, src));
    assert_eq!(dst_prefix, &AccountIdPrefixFull::workchain(0, dst));

    let cur_prefix = env.cur_prefix();
    let next_prefix = env.next_prefix();

    assert_eq!(src_prefix, cur_prefix);
    assert_eq!(dst_prefix, next_prefix);
    assert!(src_shard_id.contains_full_prefix(&cur_prefix));
    println!("shard: {}, prefix: {:x}", hop_shard_id, next_prefix.prefix);
    assert!(!hop_shard_id.contains_full_prefix(&next_prefix));
    assert!(dst_shard_id.contains_full_prefix(&next_prefix));

    assert_eq!(cur_prefix,  &AccountIdPrefixFull::workchain(0, src));
    assert_eq!(next_prefix, &AccountIdPrefixFull::workchain(0, dst));

    assert!(count_matching_bits(dst_prefix, next_prefix) >= count_matching_bits(dst_prefix, cur_prefix));
}
