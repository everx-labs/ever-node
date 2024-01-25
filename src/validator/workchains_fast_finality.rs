use std::sync::Arc;
use ton_types::{Result, fail, error};
use ton_block::{ShardHashes, ShardIdent, ValidatorSet};
use crate::shard_state::ShardStateStuff;
use crate::validator::sessions_computing::SessionValidatorsInfo;
use crate::validator::validator_utils::ValidatorSubsetInfo;

#[cfg(test)]
#[path = "tests/test_workchains_fast_finality.rs"]
mod tests;

/* #[cfg(feature = "fast_finality")]
fn get_possible_next_collator_range_for_shard(
    col: &ShardCollators, current_shard: &ShardIdent, shard_id: &ShardIdent
) -> Result<Option<CollatorRange>>
{
    let mut shards = Vec::new();
    // Shard may stay in place
    if col.next2.is_none() {
        shards.push((current_shard.clone(), col.next.clone()))
    }
    // Shard may merge with something else
    if !current_shard.is_full() && col.next2.is_none() {
        shards.push((current_shard.merge()?, col.next.clone()));
    }
    // Shard may split
    if current_shard.can_split() {
        if let Some(next_right) = &col.next2 {
            let (l, r) = current_shard.split()?;
            shards.push((l, col.next.clone()));
            shards.push((r, next_right.clone()));
        }
    }

    shards.retain(|(candidate,_collator)| candidate == shard_id);
    if shards.len() > 1 {
        fail!("Too many next collator ranges for shard {}: current shard {}, {:?}", shard_id, current_shard, shards);
    }
    Ok(shards.get(0).map(|(_,collator)| collator.clone()))
} */

/*
*/

/* #[cfg(feature = "fast_finality")]
pub fn try_calc_next_subset_for_workchain_fast_finality(
    vset: &ValidatorSet,
    cc_seqno: u32,
    mc_state: &ShardStateStuff,
    future_shard: &ShardIdent
) -> Result<Option<ValidatorSubsetInfo>> {

    log::debug!(
        target: "validator_manager",
        "try_calc_next_subset_for_workchain_fast_finality: mc_state: {}, future_shard {}",
        mc_state.block_id(), future_shard
    );

    let shards = mc_state.shards()?;
    let mut possible_validators = Vec::new();

    shards.iterate_shards_for_workchain(future_shard.workchain_id(), |ident, descr| {
        let collators = descr.collators.ok_or_else(|| error!("{} has no collator info", ident))?;
        let possible_next = get_possible_next_collator_range_for_shard(&collators, &ident, &future_shard)?;
        if let Some(rng) = possible_next {
            if !possible_validators.contains(&rng) {
                possible_validators.push(rng);
            }
        };
        Ok(true)
    })?;

    get_validator_descrs_by_collator_range(&vset, cc_seqno, &possible_validators, true)
} */

