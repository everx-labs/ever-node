use std::sync::Arc;
use ton_types::{Result, fail, error};
#[cfg(feature = "fast_finality")]
use ton_block::{CollatorRange, ShardCollators};
use ton_block::{ShardHashes, ShardIdent, ValidatorSet};
use crate::shard_state::ShardStateStuff;
use crate::validator::sessions_computing::SessionValidatorsInfo;
use crate::validator::validator_utils::ValidatorSubsetInfo;

#[cfg(test)]
#[path = "tests/test_workchains_fast_finality.rs"]
mod tests;

#[cfg(feature = "fast_finality")]
/// Function checks whether the given block with number `block_seqno` from shard `block_shard`
/// is validated by the collators, specified as prev, curr or next `collators` for shard,
/// currently known as `collator_shard`.
fn get_ranges_that_may_validate_given_block(
    collators: &ShardCollators, current_shard: &ShardIdent, block_shard: &ShardIdent, block_seqno: u32
) -> Result<Option<CollatorRange>> {
    // Current shard
    let mut shards_to_check = Vec::from([(current_shard.clone(), collators.current.clone())]);
    // The shard was same or will be same
    if collators.prev2.is_none() { shards_to_check.push((current_shard.clone(), collators.prev.clone())); }
    if collators.next2.is_none() { shards_to_check.push((current_shard.clone(), collators.next.clone())); }
    // The shard was split or will be merged
    if !current_shard.is_full() {
        if collators.prev2.is_none() { shards_to_check.push((current_shard.merge()?, collators.prev.clone())); }
        if collators.next2.is_none() { shards_to_check.push((current_shard.merge()?, collators.next.clone())); }
    }
    // The shard was merged or will be split
    if current_shard.can_split() {
        let (l, r) = current_shard.split()?;
        if let Some(prev2) = &collators.prev2 {
            shards_to_check.push((l.clone(), collators.prev.clone()));
            shards_to_check.push((r.clone(), prev2.clone()));
        }
        if let Some(next2) = &collators.next2 {
            shards_to_check.push((l, collators.next.clone()));
            shards_to_check.push( (r, next2.clone()));
        }
    }

    shards_to_check.retain( |(id,range)| {
        id == block_shard && range.start <= block_seqno && block_seqno <= range.finish
    });

    if let Some((id,rng)) = shards_to_check.get(0) {
        if shards_to_check.len() > 1 {
            fail!("Impossilbe state: shard {}, block {} corresponds to two different ranges: ({},{:?}) and {:?}",
                    current_shard, block_seqno, id, rng, shards_to_check.get(1)
                )
        }

        Ok(Some((*rng).clone()))
    }
    else {
        Ok(None)
    }
}

#[cfg(feature = "fast_finality")]
pub fn try_calc_range_for_workchain_fast_finality(
    shards: &ShardHashes,
    shard_id: &ShardIdent,
    block_seqno: u32
) -> Result<Vec<CollatorRange>> {
    let mut possible_validators: Vec<CollatorRange> = Vec::new();

    shards.iterate_shards_for_workchain(shard_id.workchain_id(), |ident, descr| {
        let collators = descr.collators.ok_or_else(|| error!("{} has no collator info", ident))?;
        if let Some(rng) = get_ranges_that_may_validate_given_block(&collators, &ident, shard_id, block_seqno)? {
            if !possible_validators.contains(&rng) {
                possible_validators.push(rng);
            }
        }
        Ok(true)
    })?;
    Ok(possible_validators)
}

#[cfg(feature = "fast_finality")]
fn get_possible_prev_collator_range_for_shard(
    col: &ShardCollators, current_shard: &ShardIdent, shard_id: &ShardIdent
) -> Result<Option<CollatorRange>>
{
    let mut shards = Vec::new();
    // Shard could remain the same
    if col.prev2.is_none() {
        shards.push((current_shard.clone(), col.prev.clone()))
    }
    // Maybe shard was split
    if !current_shard.is_full() && col.prev2.is_none() {
        shards.push((current_shard.merge()?, col.prev.clone()));
    }
    // Maybe shard was merged
    if current_shard.can_split() {
        if let Some(prev_right) = &col.prev2 {
            let (l, r) = current_shard.split()?;
            shards.push((l, col.prev.clone()));
            shards.push((r, prev_right.clone()));
        }
    }

    shards.retain(|(candidate,_collator)| candidate == shard_id);
    if shards.len() > 1 {
        fail!("Too many next collator ranges for shard {}: current shard {}, {:?}", shard_id, current_shard, shards);
    }
    Ok(shards.get(0).map(|(_,collator)| collator.clone()))
}

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

#[cfg(feature = "fast_finality")]
fn get_validator_descrs_by_collator_range(
    vset: &ValidatorSet,
    cc_seqno: u32,
    possible_validators: &Vec<CollatorRange>,
    validator_must_be_single: bool,
) -> Result<Option<ValidatorSubsetInfo>> {
    if possible_validators.len() == 0 {
        return Ok(None);
    }

    if validator_must_be_single && possible_validators.len() != 1 {
        fail!("Too many validators for collating block");
    }

    let vset_len = vset.list().len();
    let subset = possible_validators.iter().map(|rng|
        vset.list().get(rng.collator as usize % vset_len)
            .ok_or_else(|| error!("No validator no {} in validator set {:?}", rng.collator, vset))?
            .clone()
    ).collect();

    let short_hash = ValidatorSet::calc_subset_hash_short(subset.as_slice(), cc_seqno)?;

    Ok(Some(ValidatorSubsetInfo { validators: subset, short_hash, collator_range: possible_validators.clone() }))
}

#[cfg(feature = "fast_finality")]
pub fn try_calc_prev_subset_for_workchain_fast_finality(
    vset: &ValidatorSet,
    cc_seqno: u32,
    mc_state: &ShardStateStuff,
    prev_shard: &ShardIdent
) -> Result<Option<ValidatorSubsetInfo>> {

    log::debug!(
        target: "validator_manager",
        "try_calc_prev_subset_for_workchain_fast_finality: mc_state: {}, prev_shard {}",
        mc_state.block_id(), prev_shard
    );

    let shards = mc_state.shards()?;
    let mut possible_validators = Vec::new();

    shards.iterate_shards_for_workchain(prev_shard.workchain_id(), |ident, descr| {
        let collators = descr.collators.ok_or_else(|| error!("{} has no collator info", ident))?;
        let possible_next = get_possible_prev_collator_range_for_shard(&collators, &ident, &prev_shard)?;
        if let Some(rng) = possible_next {
            if !possible_validators.contains(&rng) {
                possible_validators.push(rng);
            }
        };
        Ok(true)
    })?;

    get_validator_descrs_by_collator_range(&vset, cc_seqno, &possible_validators, false)
}

/*
#[cfg(feature = "fast_finality")]
pub fn calc_session_validators_info_from_collator_range(v: &CollatorRange) -> Vec<Arc<SessionValidatorsInfo>> {
    SessionValidatorsInfo::new()
}
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

#[cfg(feature = "fast_finality")]
pub fn try_calc_subset_for_workchain_fast_finality(
    vset: &ValidatorSet,
    cc_seqno: u32,
    mc_state: &ShardStateStuff,
    shard_id: &ShardIdent,
    block_seqno: u32
) -> Result<Option<ValidatorSubsetInfo>> {

    log::debug!(
        target: "validator_manager",
        "try_calc_subset_for_workchain_fast_finality: mc_state: {}, shard_id {}, block_seqno {}",
        mc_state.block_id(), shard_id, block_seqno
    );

    let possible_validators = try_calc_range_for_workchain_fast_finality(
        mc_state.shards()?, shard_id, block_seqno
    )?;
    get_validator_descrs_by_collator_range(vset, cc_seqno, &possible_validators, true)
}

#[cfg(feature = "fast_finality")]
pub fn calc_subset_for_workchain_fast_finality(
    vset: &ValidatorSet,
    cc_seqno: u32,
    mc_state: &ShardStateStuff,
    shard_id: &ShardIdent,
    block_seqno: u32,
) -> Result<ValidatorSubsetInfo> {
    if shard_id.is_masterchain() {
        fail!("calc_subset_for_workchain_fast_finality must be called for shardchain only, but called for {}", shard_id);
    }

    match try_calc_subset_for_workchain_fast_finality(vset, cc_seqno, mc_state, shard_id, block_seqno)? {
        Some(x) => Ok(x),
        None => {
            fail!(
                "Not enough fast_finality validators  from total {} for workchain {}, block_seqno {}",
                vset.list().len(), shard_id, block_seqno
            )
        }
    }
}
