use ton_block::{CollatorRange, ShardCollators, ShardIdent};
use ton_types::Result;

#[cfg(feature = "fast_finality")]
use crate::validator::workchains_fast_finality::{
    get_ranges_that_may_validate_given_block,
// TODO uncomment
//    get_possible_next_collator_range_for_shard
};

#[cfg(feature = "fast_finality")]
#[test]
pub fn test_ranges_computation_for_fast_finality() -> Result<()> {
    let collators = ShardCollators {
        prev: CollatorRange {
            collator: 0,
            start: 1,
            finish: 10,
        },
        prev2: None,
        current: CollatorRange {
            collator: 1,
            start: 11,
            finish: 14,
        },
        next: CollatorRange {
            collator: 2,
            start: 15,
            finish: 30,
        },
        next2: Some (CollatorRange {
            collator: 3,
            start: 15,
            finish: 30,
        }),
        updated_at: 0,
    };

    let current_shard = ShardIdent::with_tagged_prefix(0,0xc000_0000_0000_0000)?;
    let (bs_left, bs_right) = current_shard.split()?;                                                   
    let cs_merge = current_shard.merge()?;
    assert_eq!(get_ranges_that_may_validate_given_block(&collators, &current_shard, &bs_left, 16)?.unwrap().collator, 2);
    assert_eq!(get_ranges_that_may_validate_given_block(&collators, &current_shard, &bs_right, 16)?.unwrap().collator, 3);
    assert_eq!(get_ranges_that_may_validate_given_block(&collators, &current_shard, &cs_merge, 16)?, None);
    assert_eq!(get_ranges_that_may_validate_given_block(&collators, &current_shard, &current_shard, 16)?, None);
    assert_eq!(get_ranges_that_may_validate_given_block(&collators, &current_shard, &current_shard, 3)?.unwrap().collator, 0);
    assert_eq!(get_ranges_that_may_validate_given_block(&collators, &current_shard, &cs_merge, 3)?.unwrap().collator, 0);

    // TODO uncomment
    //assert_eq!(get_possible_next_collator_range_for_shard(&collators, &current_shard, &current_shard)?, None);
    //assert_eq!(get_possible_next_collator_range_for_shard(&collators, &current_shard, &cs_merge)?, None);
    //assert_eq!(get_possible_next_collator_range_for_shard(&collators, &current_shard, &bs_left)?.unwrap().collator, 2);
    //assert_eq!(get_possible_next_collator_range_for_shard(&collators, &current_shard, &bs_right)?.unwrap().collator, 3);
    Ok(())
}