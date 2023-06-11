use ton_types::{Result, fail, error};
use ton_block::{ShardHashes, ShardIdent, ValidatorSet};
use crate::shard_state::ShardStateStuff;
use crate::validator::validator_utils::ValidatorSubsetInfo;

    possible_validators: &Vec<CollatorRange>,
    validator_must_be_single: bool,
) -> Result<Option<ValidatorSubsetInfo>> {
    if let Some(rng) = possible_validators.iter().next() {
        if validator_must_be_single && possible_validators.len() != 1 {
            fail!("Too many validators for collating block");
        }
        let mut subset = Vec::new();
        subset.push(vset.list().get(rng.collator as usize)
            .ok_or_else(|| error!("No validator no {} in validator set {:?}", rng.collator, vset))?.clone());
        let short_hash = ValidatorSet::calc_subset_hash_short(subset.as_slice(), cc_seqno)?;
        Ok(Some(ValidatorSubsetInfo { validators: subset, short_hash, collator_range: Some(rng.clone()) }))
    }
    else {
        fail!("No validators present")
    }
}

