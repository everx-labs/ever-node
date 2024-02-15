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
use crate::{
    collator_test_bundle::CollatorTestBundle, engine_traits::EngineOperations, 
    types::messages::{count_matching_bits, MsgEnvelopeStuff},
    validator::{
        CollatorSettings, collator,
        validate_query::ValidateQuery,
        validator_utils::compute_validator_set_cc,
    },
};
use ton_types::{error, Result};
use pretty_assertions::assert_eq;
use std::sync::Arc;

const RES_PATH: &'static str = "target/cmp";

async fn try_collate_by_bundle(bundle: Arc<CollatorTestBundle>) -> Result<(Block, ShardStateUnsplit)> {
    try_collate_by_engine(
        bundle.clone(),
        bundle.block_id().shard().clone(),
        bundle.prev_blocks_ids().clone(),
        Some(bundle.created_by().clone()),
        match bundle.ethalon_block()? {
            Some(block) => Some(block.block()?.read_extra().unwrap().rand_seed().clone()),
            None => bundle.rand_seed().cloned()
        }
    ).await
}

async fn try_collate_by_engine(
    engine: Arc<dyn EngineOperations>,
    shard: ShardIdent,
    prev_blocks_ids: Vec<BlockIdExt>,
    created_by_opt: Option<UInt256>,
    rand_seed_opt: Option<UInt256>,
) -> Result<(Block, ShardStateUnsplit)> {
    std::fs::create_dir_all(RES_PATH).ok();
    let mc_state = engine.load_last_applied_mc_state().await?;
    let mc_state_extra = mc_state.shard_state_extra()?;
    let mut cc_seqno_with_delta = 0;
    let cc_seqno_from_state = if shard.is_masterchain() {
        mc_state_extra.validator_info.catchain_seqno
    } else {
        mc_state_extra.shards.calc_shard_cc_seqno(&shard)?
    };
    let nodes = compute_validator_set_cc(
        &mc_state,
        &shard,
        prev_blocks_ids.iter().max_by_key(|id1| id1.seq_no).unwrap().seq_no() + 1,
        cc_seqno_from_state,
        &mut cc_seqno_with_delta
    )?;
    let validator_set = ValidatorSet::with_cc_seqno(0, 0, 0, cc_seqno_with_delta, nodes)?;

    // log::debug!("{}", block_stuff.id());

    log::info!("TRY COLLATE block {}", shard);

    let min_mc_seq_no = if prev_blocks_ids[0].seq_no() == 0 {
        0
    } else {
        let handle = engine.load_block_handle(&prev_blocks_ids[0])?.ok_or_else(
            || error!("Cannot load handle for prev block {}", prev_blocks_ids[0])
        )?;
        engine.load_block(&handle).await?.block()?.read_info()?.min_ref_mc_seqno()
    };

    let collator = collator::Collator::new(
        shard.clone(),
        min_mc_seq_no,
        prev_blocks_ids.clone(),
        validator_set.clone(),
        created_by_opt.unwrap_or_default(),
        engine.clone(),
        rand_seed_opt,
        CollatorSettings::default(),
    )?;
    let (block_candidate, new_state) = collator.collate().await?;

    let new_block = Block::construct_from_bytes(&block_candidate.data)?;

    // std::fs::write(&format!("{}/state_candidate.json", RES_PATH), ton_block_json::debug_state(new_state.clone())?)?;
    // std::fs::write(&format!("{}/block_candidate.json", RES_PATH), ton_block_json::debug_block_full(&new_block)?)?;

    let validator_query = ValidateQuery::new(
        shard.clone(),
        min_mc_seq_no,
        prev_blocks_ids.clone(),
        block_candidate.clone(),
        validator_set.clone(),
        engine.clone(),
        true,
        true,
    );
    validator_query.try_validate().await?;
    Ok((new_block, new_state))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_collate_first_block() {
    std::fs::create_dir_all(RES_PATH).ok();
    // init_test_log();
    let bundle = Arc::new(CollatorTestBundle::build_with_zero_state(
        "src/tests/static/zerostate.boc",
        &["src/tests/static/basestate0.boc", "src/tests/static/basestate0.boc"]
    ).await.unwrap());
    try_collate_by_bundle(bundle).await.unwrap();
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
    MsgEnvelopeStuff::new(msg, &shard, Grams::from(1_000_000), use_hypercube)
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
