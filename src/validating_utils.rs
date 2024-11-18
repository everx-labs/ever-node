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

use crate::{validator::validator_utils::compute_validator_set_cc, shard_state::ShardStateStuff};
use ever_block::{
    ShardIdent, BlockIdExt, ConfigParams, McStateExtra, ShardHashes, ValidatorSet, McShardRecord,
    INVALID_WORKCHAIN_ID, MASTERCHAIN_ID, GlobalCapabilities,
};
use ever_block::{fail, error, Result, Sha256, UInt256};
use std::{collections::HashSet, cmp::max, iter::Iterator};

#[cfg(not(feature = "fast_finality_extra"))]
pub const UNREGISTERED_CHAIN_MAX_LEN: u32 = 8;
#[cfg(feature = "fast_finality_extra")]
pub const UNREGISTERED_CHAIN_MAX_LEN: u32 = 32;

pub fn supported_capabilities() -> u64 {
    let caps =
        GlobalCapabilities::CapCreateStatsEnabled as u64 |
        GlobalCapabilities::CapBounceMsgBody as u64 |
        GlobalCapabilities::CapReportVersion as u64 |
        GlobalCapabilities::CapShortDequeue as u64 |
        GlobalCapabilities::CapRemp as u64 |
        GlobalCapabilities::CapSmft as u64 |
        GlobalCapabilities::CapInitCodeHash as u64 |
        GlobalCapabilities::CapOffHypercube as u64 |
        GlobalCapabilities::CapFixTupleIndexBug as u64 |
        GlobalCapabilities::CapFastStorageStat as u64 |
        GlobalCapabilities::CapMycode as u64 |
        GlobalCapabilities::CapCopyleft as u64 |
        GlobalCapabilities::CapFullBodyInBounced as u64 |
        GlobalCapabilities::CapStorageFeeToTvm as u64 |
        GlobalCapabilities::CapWorkchains as u64 |
        GlobalCapabilities::CapStcontNewFormat as u64 |
        GlobalCapabilities::CapFastStorageStatBugfix as u64 |
        GlobalCapabilities::CapResolveMerkleCell as u64 |
        GlobalCapabilities::CapFeeInGasUnits as u64 |
        GlobalCapabilities::CapBounceAfterFailedAction as u64 |
        GlobalCapabilities::CapSuspendedList as u64 |
        GlobalCapabilities::CapsTvmBugfixes2022 as u64 |
        GlobalCapabilities::CapNoSplitOutQueue as u64 |
        GlobalCapabilities::CapTvmV19 as u64 |
        GlobalCapabilities::CapTvmV20 as u64 |
        GlobalCapabilities::CapDuePaymentFix as u64 |
        GlobalCapabilities::CapCommonMessage as u64 |
        GlobalCapabilities::CapUndeletableAccounts as u64;
    #[cfg(feature = "gosh")] 
    let caps = caps | GlobalCapabilities::CapDiff as u64;
    #[cfg(feature = "signature_with_id")] 
    let caps = caps | GlobalCapabilities::CapSignatureWithId as u64;
    caps
}

pub fn supported_version() -> u32 {
    60
}

#[allow(clippy::too_many_arguments)]
pub fn check_this_shard_mc_info(
    shard: &ShardIdent,
    block_id: &BlockIdExt,
    after_merge: bool,
    after_split: bool,
    mut before_split: bool,
    prev_blocks: &[BlockIdExt],
    config_params: &ConfigParams,
    mc_state_extra: &McStateExtra,
    is_validate: bool,
    now: u32,
) -> Result<(u32, bool, bool)> {
    let next_block_descr = fmt_next_block_descr(block_id);

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
        log::debug!(target: "validator", "({}): creating first block for workchain {}", next_block_descr, shard.workchain_id());
        fail!("cannot create first block for workchain {} after previous block {} \
            because no shard for this workchain is declared yet",
                shard.workchain_id(), prev_blocks[0])
    }
    let left = mc_state_extra.shards().find_shard(&shard.left_ancestor_mask()?)?
        .ok_or_else(|| error!("cannot create new block for shard {} because there is no \
            similar shard in existing masterchain configuration", shard))?;
    // log::info!(target: "validator", "left for {} is {:?}", block_id(), left.descr());
    if left.shard() == shard {

        log::trace!("({}): check_this_shard_mc_info, block: {} left: {:?}", next_block_descr, block_id, left);

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
                    log::info!("({}): BEFORE_SPLIT set for the new block of shard {}", next_block_descr, shard);
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
            if prev_blocks[0].seq_no >= cseqno + UNREGISTERED_CHAIN_MAX_LEN {
                fail!("cannot create new block for shard {} after previous block {} \
                    because this would lead to an unregistered chain of length > {} \
                    (masterchain contains only {} and {})",
                    shard, prev_blocks[0], UNREGISTERED_CHAIN_MAX_LEN, left.block_id(), right.block_id()
                )
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
    if chk_chain_len && prev.seq_no >= listed.seq_no + UNREGISTERED_CHAIN_MAX_LEN {
        fail!(
            "cannot generate next block after {} because this would lead to \
            an unregistered chain of length > {} (only {} is registered in the masterchain)", 
            prev, UNREGISTERED_CHAIN_MAX_LEN, listed
        )
    }
    Ok(())
}

pub fn check_cur_validator_set(
    validator_set: &ValidatorSet,
    block_id: &BlockIdExt,
    shard: &ShardIdent,
    mc_state_extra: &McStateExtra,
    old_mc_shards: &ShardHashes,
    mc_state: &ShardStateStuff,
    is_fake: bool,
) -> Result<bool> {
    if is_fake { return Ok(true) }

    let mut cc_seqno_with_delta = 0; // cc_seqno delta = 0
    let cc_seqno_from_state = if shard.is_masterchain() {
        mc_state_extra.validator_info.catchain_seqno
    } else {
        old_mc_shards.calc_shard_cc_seqno(shard)?
    };
    let nodes = compute_validator_set_cc(
        mc_state,
        block_id.shard(),
        block_id.seq_no(),
        cc_seqno_from_state,
        &mut cc_seqno_with_delta
    )?;
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
    old_blkids: &[BlockIdExt],
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

        let old_info = shards
            .get_shard(ob.shard())
            .unwrap_or_default()
            .ok_or_else(|| {
                error!(
                    "the start block's shard {} of a top shard block update is not contained \
                    in the previous shard configuration",
                    ob,
                )
            })?;

        if old_info.block_id().seq_no() != ob.seq_no() ||
            &old_info.block_id().root_hash != ob.root_hash() || &old_info.block_id().file_hash != ob.file_hash() {
            fail!(
                "the start block {} of a top shard block update is not contained \
                in the previous shard configuration",
                ob,
            )
        }

        old_cc_seqno = max(old_cc_seqno, old_info.descr.next_catchain_seqno);

        if let Some(shards_updated) = shards_updated {
            if shards_updated.contains(ob.shard()) {
                fail!(
                    "the shard of the start block {} of a top shard block update has been \
                    already updated in the current shard configuration",
                    ob
                )
            }
        }

        if old_info.descr.before_split != before_split {
            fail!(
                "the shard of the start block {} has before_split={} \
                but the top shard block update is valid only if before_split={}",
                ob,
                old_info.descr.before_split,
                before_split,
            )
        }

        if old_info.descr.before_merge != before_merge {
            fail!(
                "the shard of the start block {} has before_merge={} \
                but the top shard block update is valid only if before_merge={}",
                ob,
                old_info.descr.before_merge,
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
            if old_info.descr.is_fsm_merge() || old_info.descr.is_fsm_none() {
                fail!(
                    "cannot register a before-split block {} because fsm_split state was not \
                    set for this shard beforehand",
                    new_info.block_id()
                )
            }

            if new_info.descr.gen_utime < old_info.descr.fsm_utime() ||
                new_info.descr.gen_utime >= old_info.descr.fsm_utime() + old_info.descr.fsm_interval() {
                fail!(
                    "cannot register a before-split block {} because fsm_split state was \
                    enabled for unixtime {} .. {} but the block is generated at {}",
                    new_info.block_id(),
                    old_info.descr.fsm_utime(),
                    old_info.descr.fsm_utime() + old_info.descr.fsm_interval(),
                    new_info.descr.gen_utime
                )
            }

        }
        if before_merge {
            if old_info.descr.is_fsm_split() || old_info.descr.is_fsm_none() {
                fail!(
                    "cannot register a merged block {} because fsm_merge state was not \
                    set for shard {} beforehand",
                    new_info.block_id(),
                    old_info.block_id().shard()
                )
            }
            if new_info.descr.gen_utime < old_info.descr.fsm_utime() ||
                new_info.descr.gen_utime >= old_info.descr.fsm_utime() + old_info.descr.fsm_interval() {
                fail!(
                    "cannot register merged block {} because fsm_merge state was \
                    enabled for shard {} for unixtime {} .. {} but the block is generated at {}",
                    new_info.block_id(),
                    old_info.block_id().shard(),
                    old_info.descr.fsm_utime(),
                    old_info.descr.fsm_utime() + old_info.descr.fsm_interval(),
                    new_info.descr.gen_utime
                )
            }
        }

        if !before_merge && !before_split {
            ancestor = Some(old_info);
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
    Ok((!before_split, ancestor))
}

pub fn calc_remp_msg_ordering_hash<'a>(msg_id: &UInt256, prev_blocks_ids: impl Iterator<Item = &'a BlockIdExt>) -> UInt256 {
    let mut hasher = Sha256::new();
    for prev_id in prev_blocks_ids {
        hasher.update(prev_id.root_hash().as_slice());
    }
    hasher.update(msg_id.as_slice());
    UInt256::from(hasher.finalize().as_slice())
}

pub fn fmt_block_id_short(block_id: &BlockIdExt) -> String {
    let rh_part = &block_id.root_hash().as_slice()[0..2];
    let rh_part = hex::encode(rh_part);
    format!(
        "{}:{}, {}, rh {}",
        block_id.shard().workchain_id(),
        block_id.shard().shard_prefix_as_str_with_tag(),
        block_id.seq_no(),
        rh_part,
    )
}

pub fn fmt_next_block_descr(next_block_id: &BlockIdExt) -> String {
    let rh_part = &next_block_id.root_hash().as_slice()[0..2];
    let rh_part = hex::encode(rh_part);
    format!(
        "{}:{}, {}, rh {}",
        next_block_id.shard().workchain_id(),
        next_block_id.shard().shard_prefix_as_str_with_tag(),
        next_block_id.seq_no(),
        rh_part,
    )
}
