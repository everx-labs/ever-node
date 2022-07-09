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

use crate::validator::validator_utils::compute_validator_set_cc;
use ton_block::{
    ShardIdent, BlockIdExt, ConfigParams, McStateExtra, ShardHashes, ValidatorSet, McShardRecord,
    FutureSplitMerge, INVALID_WORKCHAIN_ID, MASTERCHAIN_ID, GlobalCapabilities,
};
use ton_types::{fail, error, Result};
use std::{collections::HashSet, cmp::max};

pub fn supported_capabilities() -> u64 {
    GlobalCapabilities::CapCreateStatsEnabled as u64 |
    GlobalCapabilities::CapBounceMsgBody as u64 |
    GlobalCapabilities::CapReportVersion as u64 |
    GlobalCapabilities::CapShortDequeue as u64 |
    GlobalCapabilities::CapInitCodeHash as u64 |
    GlobalCapabilities::CapOffHypercube as u64 |
    GlobalCapabilities::CapFixTupleIndexBug as u64 |
    GlobalCapabilities::CapFastStorageStat as u64 |
    GlobalCapabilities::CapMycode as u64 |
    GlobalCapabilities::CapCopyleft as u64 |
    GlobalCapabilities::CapFullBodyInBounced as u64 |
    GlobalCapabilities::CapStorageFeeToTvm as u64
}

pub fn supported_version() -> u32 {
    31
}

pub fn check_this_shard_mc_info(
    shard: &ShardIdent,
    block_id: &BlockIdExt,
    after_merge: bool,
    after_split: bool,
    mut before_split: bool,
    prev_blocks: &Vec<BlockIdExt>,
    config_params: &ConfigParams,
    mc_state_extra: &McStateExtra,
    is_validate: bool,
    now: u32,
) -> Result<(u32, bool, bool)> {
    let mut now_upper_limit = u32::MAX;
    // let mut before_split = false;
    // let mut accept_msgs = false;

    let wc_info = config_params.workchains()?.get(&shard.workchain_id())?
        .ok_or_else(|| error!("cannot create new block for workchain {} absent \
            from workchain configuration", shard.workchain_id()))?;
    if !wc_info.active() {
        fail!("cannot create new block for disabled workchain {}", shard.workchain_id());
    }
    if !wc_info.basic() {
        fail!("cannot create new block for non-basic workchain {}", shard.workchain_id());
    }
    if wc_info.enabled_since != 0 && wc_info.enabled_since >  now {
        fail!("cannot create new block for workchain {} which is not enabled yet", shard.workchain_id())
    }
    // if !is_validate {
    //     if wc_info.min_addr_len != 0x100 || wc_info.max_addr_len != 0x100 {
    //         fail!("wc_info.min_addr_len == 0x100 || wc_info.max_addr_len == 0x100");
    //     }
    // }
    let accept_msgs = wc_info.accept_msgs;
    let mut split_allowed = false;
    if !mc_state_extra.shards().has_workchain(shard.workchain_id())? {
        // creating first block for a new workchain
        log::debug!(target: "validator", "creating first block for workchain {}", shard.workchain_id());
        fail!("cannot create first block for workchain {} after previous block {} \
            because no shard for this workchain is declared yet",
                shard.workchain_id(), prev_blocks[0])
    }
    let left = mc_state_extra.shards().find_shard(&shard.left_ancestor_mask()?)?
        .ok_or_else(|| error!("cannot create new block for shard {} because there is no \
            similar shard in existing masterchain configuration", shard))?;
    // log::info!(target: "validator", "left for {} is {:?}", block_id(), left.descr());
    if left.shard() == shard {

        log::trace!("check_this_shard_mc_info, block: {} left: {:?}", block_id, left);

        // no split/merge
        if after_merge || after_split {
            fail!("cannot generate new shardchain block for {} after a supposed split or merge event \
                because this event is not reflected in the masterchain", shard)
        }
        check_prev_block(&left.block_id, &prev_blocks[0], true)?;
        if left.descr().before_split {
            fail!("cannot generate new unsplit shardchain block for {} \
                after previous block {} with before_split set", shard, left.block_id())
        }
        if left.descr().before_merge {
            let sib = mc_state_extra.shards().get_shard(&shard.sibling())?
                .ok_or_else(|| error!("No sibling for {}", shard))?;
            if sib.descr().before_merge {
                fail!("cannot generate new unmerged shardchain block for {} after both {} \
                    and {} set before_merge flags", shard, left.block_id(), sib.block_id())
            }
        }
        if left.descr().is_fsm_split() {
            if is_validate {
                if now >= left.descr().fsm_utime() && now < left.descr().fsm_utime_end() {
                    split_allowed = true;
                }
            } else {
                // t-node's collator contains next code:
                // auto tmp_now = std::max<td::uint32>(config_->utime, (unsigned)std::time(nullptr));
                // but `now` parameter passed to this function is already initialized same way.
                // `13` and `11` is a magic from t-node
                if now >= left.descr().fsm_utime() && now + 13 < left.descr().fsm_utime_end() {
                    now_upper_limit = left.descr().fsm_utime_end() - 11;  // ultimate value of now_ must be at most now_upper_limit_
                    before_split = true;
                    log::info!("BEFORE_SPLIT set for the new block of shard {}", shard);
                }
            }
        }
    } else if shard.is_parent_for(left.shard()) {
        // after merge
        if !left.descr().before_merge {
            fail!("cannot create new merged block for shard {} \
                because its left ancestor {} has no before_merge flag", shard, left.block_id())
        }
        let right = match mc_state_extra.shards().find_shard(&shard.right_ancestor_mask()?)? {
            Some(right) => right,
            None => fail!("cannot create new block for shard {} after a preceding merge \
                because there is no right ancestor shard in existing masterchain configuration", shard)
        };
        if !shard.is_parent_for(right.shard()) {
            fail!("cannot create new block for shard {} after a preceding merge \
                because its right ancestor appears to be {}", shard, right.block_id());
        }
        if !right.descr().before_merge {
            fail!("cannot create new merged block for shard {} \
                because its right ancestor {} has no before_merge flag", shard, right.block_id())
        }
        if after_split {
            fail!("cannot create new block for shard {} after a purported split \
                because existing shard configuration suggests a merge", shard)
        } else if after_merge {
            check_prev_block_exact(shard, left.block_id(), &prev_blocks[0])?;
            check_prev_block_exact(shard, right.block_id(), &prev_blocks[1])?;
        } else {
            let cseqno = std::cmp::max(left.descr().seq_no, right.descr.seq_no);
            if prev_blocks[0].seq_no <= cseqno {
                fail!("cannot create new block for shard {} after previous block {} \
                    because masterchain contains newer possible ancestors {} and {}",
                        shard, prev_blocks[0], left.block_id(), right.block_id())
            }
            if prev_blocks[0].seq_no >= cseqno + 8 {
                fail!("cannot create new block for shard {} after previous block {} \
                    because this would lead to an unregistered chain of length > 8 \
                    (masterchain contains only {} and {})",
                        shard, prev_blocks[0], left.block_id(), right.block_id())
            }
        }
    } else if left.shard().is_parent_for(shard) {
        // after split
        if !left.descr().before_split {
            fail!("cannot generate new split shardchain block for {} \
                after previous block {} without before_split", shard, left.block_id())
        }
        if after_merge {
            fail!("cannot create new block for shard {} \
                after a purported merge because existing shard configuration suggests a split", shard)
        } else if after_split {
            check_prev_block_exact(shard, left.block_id(), &prev_blocks[0])?;
        } else {
            check_prev_block(left.block_id(), &prev_blocks[0], true)?;
        }
    } else {
        fail!("masterchain configuration contains only block {} \
            which belongs to a different shard from ours {}", left.block_id(), shard)
    }
    if is_validate && before_split && !split_allowed {
        fail!("new block {} has before_split set, \
            but this is forbidden by masterchain configuration", block_id)
    }
    Ok((now_upper_limit, before_split, accept_msgs))
}

pub fn check_prev_block_exact(shard: &ShardIdent, listed: &BlockIdExt, prev: &BlockIdExt) -> Result<()> {
    if listed != prev {
        fail!("cannot generate shardchain block for shard {} \
            after previous block {} because masterchain configuration expects \
            another previous block {} and we are immediately after a split/merge event",
            shard, prev, listed)
    }
    Ok(())
}

pub fn check_prev_block(listed: &BlockIdExt, prev: &BlockIdExt, chk_chain_len: bool) -> Result<()> {
    if listed.seq_no > prev.seq_no {
        fail!("cannot generate a shardchain block after previous block {} \
            because masterchain configuration already contains a newer block {}", prev, listed)
    }
    if listed.seq_no == prev.seq_no && listed != prev {
        fail!("cannot generate a shardchain block after previous block {} \
            because masterchain configuration lists another block {} of the same height", prev, listed)
    }
    if chk_chain_len && prev.seq_no >= listed.seq_no + 8 {
        fail!("cannot generate next block after {} because this would lead to \
            an unregistered chain of length > 8 (only {} is registered in the masterchain)", prev, listed)
    }
    Ok(())
}

pub fn check_cur_validator_set(
    validator_set: &ValidatorSet,
    block_id: &BlockIdExt,
    shard: &ShardIdent,
    mc_state_extra: &McStateExtra,
    old_mc_shards: &ShardHashes,
    config_params: &ConfigParams,
    now: u32,
    is_fake: bool,
) -> Result<bool> {
    if is_fake { return Ok(true) }

    let mut cc_seqno_with_delta = 0; // cc_seqno delta = 0
    let cc_seqno_from_state = if shard.is_masterchain() {
        mc_state_extra.validator_info.catchain_seqno
    } else {
        old_mc_shards.calc_shard_cc_seqno(&shard)?
    };
    let nodes = compute_validator_set_cc(config_params, &shard, now, cc_seqno_from_state, &mut cc_seqno_with_delta)?;
    if nodes.is_empty() {
        fail!("Cannot compute masterchain validator set from old masterchain state")
    }
    if validator_set.catchain_seqno() != cc_seqno_with_delta {
        fail!("Current validator set catchain seqno mismatch: this validator set has cc_seqno={}, \
            only validator set with cc_seqno={} is entitled to create block {}",
                validator_set.catchain_seqno(), cc_seqno_with_delta, block_id);
    }

    // TODO: check compute_validator_set
    let nodes_set = ValidatorSet::new(0, 0, 0, nodes)?;
    let export_nodes = validator_set.list();
    // log::debug!(target: "validator", "block candidate validator set {:?}", export_nodes);
    // log::debug!(target: "validator", "current validator set {:?}", nodes);
    // CHECK!(export_nodes, &nodes);
    if export_nodes != nodes_set.list() /* && !is_fake */ {
        fail!("current validator set mismatch: this validator set is not \
            entitled to create block {}", block_id)
    }
    Ok(true)
}

pub fn may_update_shard_block_info(
    shards: &ShardHashes,
    new_info: &McShardRecord,
    old_blkids: &Vec<BlockIdExt>,
    lt_limit: u64,
    shards_updated: Option<&HashSet<ShardIdent>>,
) -> Result<(bool, Option<McShardRecord>)> {

    log::trace!("may_update_shard_block_info new_info.block_id(): {}", new_info.block_id());

    let wc = new_info.shard().workchain_id();
    if wc == INVALID_WORKCHAIN_ID || wc == MASTERCHAIN_ID {
        fail!("new top shard block description belongs to an invalid workchain {}", wc)
    }

    if !shards.has_workchain(wc)? {
        fail!("new top shard block belongs to an unknown or disabled workchain {}", wc)
    }

    if old_blkids.len() != 1 && old_blkids.len() != 2 {
        fail!("`old_blkids` must have either one or two start blocks in a top shard block update")
    }


    let before_split = old_blkids[0].shard().is_parent_for(new_info.shard());
    let before_merge = old_blkids.len() == 2;
    if before_merge {
        if old_blkids[0].shard().sibling() != *old_blkids[1].shard() {
            fail!("the two start blocks of a top shard block update must be siblings")
        }
        if !new_info.shard().is_parent_for(old_blkids[0].shard()) {
            fail!(
                "the two start blocks of a top shard block update do not merge into expected \
                final shard {}",
                old_blkids[0].shard()
            )
        }
    } else if (new_info.shard() != old_blkids[0].shard()) && !before_split {
        fail!(
            "the start block of a top shard block update must either coincide with the final\
            shard or be its parent"
        )
    }

    let mut ancestor = None;
    let mut old_cc_seqno = 0;
    for ob in old_blkids {

        let odef = shards
            .get_shard(ob.shard())
            .unwrap_or_default()
            .ok_or_else(|| {
                error!(
                    "the start block's shard {} of a top shard block update is not contained \
                    in the previous shard configuration",
                    ob,
                )
            })?;

        if odef.block_id().seq_no() != ob.seq_no() ||
            &odef.block_id().root_hash != ob.root_hash() || &odef.block_id().file_hash != ob.file_hash() {
            fail!(
                "the start block {} of a top shard block update is not contained \
                in the previous shard configuration",
                ob,
            )
        }

        old_cc_seqno = max(old_cc_seqno, odef.descr.next_catchain_seqno);

        if let Some(shards_updated) = shards_updated {
            if shards_updated.contains(ob.shard()) {
                fail!(
                    "the shard of the start block {} of a top shard block update has been \
                    already updated in the current shard configuration",
                    ob
                )
            }
        }

        if odef.descr.before_split != before_split {
            fail!(
                "the shard of the start block {} has before_split={} \
                but the top shard block update is valid only if before_split={}",
                ob,
                odef.descr.before_split,
                before_split,
            )
        }

        if odef.descr.before_merge != before_merge {
            fail!(
                "the shard of the start block {} has before_merge={} \
                but the top shard block update is valid only if before_merge={}",
                ob,
                odef.descr.before_merge,
                before_merge,
            )
        }

        if new_info.descr.before_split {
            if before_merge || before_split {
                fail!(
                    "cannot register a before-split block {} at the end of a chain that itself \
                    starts with a split/merge event",
                    new_info.block_id()
                )
            }
            if odef.descr.is_fsm_merge() || odef.descr.is_fsm_none() {
                fail!(
                    "cannot register a before-split block {} because fsm_split state was not \
                    set for this shard beforehand",
                    new_info.block_id()
                )
            }
            if new_info.descr.gen_utime < odef.descr.fsm_utime() ||
                new_info.descr.gen_utime >= odef.descr.fsm_utime() + odef.descr.fsm_interval() {
                fail!(
                    "cannot register a before-split block {} because fsm_split state was \
                    enabled for unixtime {} .. {} but the block is generated at {}",
                    new_info.block_id(),
                    odef.descr.fsm_utime(),
                    odef.descr.fsm_utime() + odef.descr.fsm_interval(),
                    new_info.descr.gen_utime
                )
            }
        }
        if before_merge {
            if odef.descr.is_fsm_split() || odef.descr.is_fsm_none() {
                fail!(
                    "cannot register a merged block {} because fsm_merge state was not \
                    set for shard {} beforehand",
                    new_info.block_id(),
                    odef.block_id().shard()
                )
            }
            if new_info.descr.gen_utime < odef.descr.fsm_utime() ||
                new_info.descr.gen_utime >= odef.descr.fsm_utime() + odef.descr.fsm_interval() {
                fail!(
                    "cannot register merged block {} because fsm_merge state was \
                    enabled for shard {} for unixtime {} .. {} but the block is generated at {}",
                    new_info.block_id(),
                    odef.block_id().shard(),
                    odef.descr.fsm_utime(),
                    odef.descr.fsm_utime() + odef.descr.fsm_interval(),
                    new_info.descr.gen_utime
                )
            }
        }

        if !before_merge && !before_split {
            ancestor = Some(odef);
        }
    }

    let expected_next_catchain_seqno = old_cc_seqno + if before_merge {1} else {0};
    
    if expected_next_catchain_seqno != new_info.descr.next_catchain_seqno {
        fail!(
            "the top shard block update is generated with catchain_seqno={} but previous shard \
            configuration expects {}",
            new_info.descr.next_catchain_seqno,
            expected_next_catchain_seqno
        )
    }
    if new_info.descr.end_lt >= lt_limit {
        fail!(
            "the top shard block update has end_lt {} which is larger than the current limit {}",
            new_info.descr.end_lt,
            lt_limit
        )
    }

    log::trace!("after may_update_shard_block_info new_info.block_id(): {}", new_info.block_id());
    return Ok((!before_split, ancestor))
}

pub fn update_shard_block_info(
    shardes: &mut ShardHashes,
    mut new_info: McShardRecord,
    old_blkids: &Vec<BlockIdExt>,
    shards_updated: Option<&mut HashSet<ShardIdent>>,
) -> Result<()> {

    let (res, ancestor) = may_update_shard_block_info(shardes, &new_info, old_blkids, !0,
        shards_updated.as_ref().map(|s| &**s))?;
    
    if !res {
        fail!(
            "cannot apply the after-split update for {} without a corresponding sibling update",
            new_info.blk_id()
        );
    }
    if let Some(ancestor) = ancestor {
        if ancestor.descr.split_merge_at != FutureSplitMerge::None {
            new_info.descr.split_merge_at = ancestor.descr.split_merge_at;
        }
    }
    
    let shard = new_info.shard().clone();

    if old_blkids.len() == 2 {
        shardes.merge_shards(&shard, |_, _| Ok(new_info.descr))?;
    } else {
        shardes.update_shard(&shard, |_| Ok(new_info.descr))?;
    }

    if let Some(shards_updated) = shards_updated {
        shards_updated.insert(shard);
    }
    Ok(())
}

pub fn update_shard_block_info2(
    shardes: &mut ShardHashes,
    mut new_info1: McShardRecord,
    mut new_info2: McShardRecord,
    old_blkids: &Vec<BlockIdExt>,
    shards_updated: Option<&mut HashSet<ShardIdent>>,
) -> Result<()> {

    let (res1, _) = may_update_shard_block_info(shardes, &new_info1, old_blkids, !0,
                        shards_updated.as_ref().map(|s| &**s))?;
    let (res2, _) = may_update_shard_block_info(shardes, &new_info2, old_blkids, !0,
                        shards_updated.as_ref().map(|s| &**s))?;

    if res1 || res2 {
        fail!("the two updates in update_shard_block_info2 must follow a shard split event");
    }
    if new_info1.shard().shard_prefix_with_tag() > new_info2.shard().shard_prefix_with_tag() {
        std::mem::swap(&mut new_info1, &mut new_info2);
    }

    let shard1 = new_info1.shard().clone();

    shardes.split_shard(&new_info1.shard().merge()?, |_| Ok((new_info1.descr, new_info2.descr)))?;

    if let Some(shards_updated) = shards_updated {
        shards_updated.insert(shard1);
    }
    
    Ok(())
}
