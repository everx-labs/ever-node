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

use super::{validator_utils::calc_subset_for_workchain, BlockCandidate, McData};
use crate::{
    block::BlockStuff,
    engine_traits::EngineOperations,
    error::NodeError,
    shard_state::ShardStateStuff,
    types::{
        messages::{count_matching_bits, perform_hypercube_routing, MsgEnqueueStuff},
        top_block_descr::{Mode as TopBlockDescrMode, TopBlockDescrStuff},
    },
    validating_utils::{
        check_cur_validator_set, check_this_shard_mc_info, may_update_shard_block_info,
        supported_capabilities, supported_version,
    },
    validator::out_msg_queue::MsgQueueManager,
    CHECK,
};
use adnl::common::add_unbound_object_to_map_with_update;
use std::{
    collections::HashMap,
    io::Cursor,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};
use ton_block::{
    config_params, Account, AccountBlock, AccountIdPrefixFull, AccountStatus, AddSub,
    BlockCreateStats, BlockError, BlockExtra, BlockIdExt, BlockInfo, BlockLimits, ConfigParamEnum,
    ConfigParams, CopyleftRewards, Counters, CreatorStats, CurrencyCollection, DepthBalanceInfo,
    Deserializable, EnqueuedMsg, GlobalCapabilities, Grams, HashmapAugType, InMsg, InMsgDescr,
    KeyExtBlkRef, KeyMaxLt, LibDescr, Libraries, McBlockExtra, McShardRecord, McStateExtra,
    MerkleProof, MerkleUpdate, Message, MsgAddressInt, MsgEnvelope, OutMsg, OutMsgDescr,
    OutMsgQueueKey, Serializable, ShardAccount, ShardAccountBlocks, ShardAccounts, ShardFeeCreated,
    ShardHashes, ShardIdent, StateInitLib, TopBlockDescrSet, TrComputePhase, Transaction,
    TransactionDescr, ValidatorSet, ValueFlow, WorkchainDescr, INVALID_WORKCHAIN_ID,
    MASTERCHAIN_ID, MAX_SPLIT_DEPTH, U15,
};
use ton_executor::{
    BlockchainConfig, CalcMsgFwdFees, ExecuteParams, OrdinaryTransactionExecutor,
    TickTockTransactionExecutor, TransactionExecutor,
};
use ton_types::{
    deserialize_cells_tree, fail, AccountId, Cell, CellType, HashmapType, Result, SliceData,
    UInt256,
};

#[cfg(feature = "metrics")]
use crate::engine::STATSD;


// pub const SPLIT_MERGE_DELAY: u32 = 100;        // prepare (delay) split/merge for 100 seconds
// pub const SPLIT_MERGE_INTERVAL: u32 = 100;     // split/merge is enabled during 60 second interval
pub const MIN_SPLIT_MERGE_INTERVAL: u32 = 30;  // split/merge interval must be at least 30 seconds
pub const MAX_SPLIT_MERGE_DELAY: u32 = 1000;   // end of split/merge interval must be at most 1000 seconds in the future

macro_rules! error {
    ($($arg:tt)*) => {
        failure::Error::from(NodeError::ValidatorReject(format!("=====> {}:{} {}", file!(), line!(), format_args!($($arg)*))))
    };
}

macro_rules! reject_query {
    ($($arg:tt)*) => {
        return Err(error!($($arg)*))
    }
}

macro_rules! soft_reject_query {
    ($($arg:tt)*) => {
        return Err(failure::Error::from(NodeError::ValidatorSoftReject(format!("=====> {}:{} {}", file!(), line!(), format_args!($($arg)*)))))
    }
}

type LibPublisher = (UInt256, UInt256, bool);

struct ValidateResult {
    lt_hash: Arc<lockfree::map::Map<u8, (u64, UInt256)>>,

    msg_proc_lt: Arc<lockfree::queue::Queue<(UInt256, u64, u64)>>,
    lib_publishers: Arc<lockfree::queue::Queue<LibPublisher>>,

    min_shard_ref_mc_seqno: Arc<AtomicU32>,
    max_shard_utime: Arc<AtomicU32>, // TODO: is never used
    max_shard_lt: Arc<AtomicU64>,
}

impl Default for ValidateResult {
    fn default() -> Self {
        let result = Self {
            lt_hash: Default::default(),
            msg_proc_lt: Default::default(),
            lib_publishers: Default::default(),
            min_shard_ref_mc_seqno: Arc::new(AtomicU32::new(std::u32::MAX)),
            max_shard_utime: Arc::new(AtomicU32::new(std::u32::MIN)),
            max_shard_lt: Arc::new(AtomicU64::new(std::u64::MIN)),
        };
        result.lt_hash.insert(0, (std::u64::MIN, UInt256::MAX));
        result.lt_hash.insert(1, (std::u64::MAX, UInt256::MAX));
        result.lt_hash.insert(2, (std::u64::MAX, UInt256::MAX)); // claimed_proc_lt_hash
        result
    }

}
#[derive(Default)]
struct ValidateBase {
    global_id: i32,
    is_fake: bool,
    created_by: UInt256,
    after_merge: bool,
    after_split: bool,

    prev_blocks_ids: Vec<BlockIdExt>,

    // TODO: maybe make some fileds Option
    // data from block_candidate
    capabilities: u64,
    block: BlockStuff,
    info: BlockInfo,
    value_flow: ValueFlow,
    state_update: MerkleUpdate,
    extra: BlockExtra,
    mc_extra: McBlockExtra,
    in_msg_descr: InMsgDescr,
    out_msg_descr: OutMsgDescr,
    account_blocks: ShardAccountBlocks,
    recover_create_msg: Option<InMsg>,
    copyleft_msgs: Vec<InMsg>,
    mint_msg: Option<InMsg>,
    // from collated_data
    top_shard_descr_dict: TopBlockDescrSet,
    virt_roots: HashMap<UInt256, Cell>, // never used proofs of processed messages
    gas_used: Arc<AtomicU64>,
    transactions_executed: Arc<AtomicU32>,

    config_params: ConfigParams,

    prev_states: Vec<Arc<ShardStateStuff>>,
    prev_state: Option<Arc<ShardStateStuff>>, // TODO: remove
    prev_state_accounts: ShardAccounts,
    prev_state_extra: McStateExtra,
    prev_validator_fees: CurrencyCollection,

    // from next_state of masterchain or from global masterchain for workcain
    next_state: Option<Arc<ShardStateStuff>>,
    next_state_accounts: ShardAccounts,
    next_state_extra: McStateExtra,

    copyleft_rewards: Arc<lockfree::queue::Queue<(AccountId, Grams)>>,

    result: ValidateResult,
}

impl ValidateBase {
    fn shard(&self) -> &ShardIdent {
        &self.block_id().shard()
    }
    fn block_id(&self) -> &BlockIdExt {
        self.block.id()
    }
    fn now(&self) -> u32 {
        self.info.gen_utime().as_u32()
    }
    fn is_special_in_msg(&self, in_msg: &InMsg) -> bool {
        let is_fee_msg = self.recover_create_msg.as_ref() == Some(in_msg) || self.mint_msg.as_ref() == Some(in_msg);
        if is_fee_msg {
            true
        } else {
            self.copyleft_msgs.iter().find(|&msg| msg == in_msg).is_some()
        }
    }
    fn min_shard_ref_mc_seqno(&self) -> u32 {
        self.result.min_shard_ref_mc_seqno.load(Ordering::Relaxed)
    }
    fn max_shard_lt(&self) -> u64 {
        self.result.max_shard_lt.load(Ordering::Relaxed)
    }
}

pub struct ValidateQuery {
    // current state of blockchain
    shard: ShardIdent,
    min_mc_seq_no: u32,
    // block_id: BlockIdExt,
    block_candidate: BlockCandidate,
    // other
    validator_set: ValidatorSet,
    is_fake: bool,
    multithread: bool,
    // previous state can be as two states for merge
    prev_blocks_ids: Vec<BlockIdExt>,
    old_mc_shards: ShardHashes, // old_shard_conf_
    // master chain state members
    block_limits: BlockLimits,
    // new state after applying block_candidate
    new_mc_shards: ShardHashes, // new_shard_conf_
    // temp
    update_shard_cc: bool,

    create_stats_enabled: bool,
    block_create_total: u64,
    block_create_count: HashMap<UInt256, u64>,

    engine: Arc<dyn EngineOperations>,
}

impl ValidateQuery {
    fn shard(&self) -> &ShardIdent {
        &self.shard
    }
    pub fn new(
        shard: ShardIdent,
        min_mc_seq_no: u32,
        prev_blocks_ids: Vec<BlockIdExt>,
        block_candidate: BlockCandidate,
        validator_set: ValidatorSet,
        engine: Arc<dyn EngineOperations>,
        is_fake: bool,
        multithread: bool,
    ) -> Self {
        Self {
            engine,
            shard,
            min_mc_seq_no,
            block_candidate,
            validator_set,
            is_fake,
            multithread,
            prev_blocks_ids,
            old_mc_shards: Default::default(),
            block_limits: Default::default(),
            // new state after applying block_candidate
            new_mc_shards: Default::default(),
            update_shard_cc: Default::default(),
            create_stats_enabled: Default::default(),
            block_create_total: Default::default(),
            block_create_count: Default::default(),
        }
    }

/*
 * 
 *   INITIAL PARSE & LOAD REQUIRED DATA
 * 
 */


    fn init_base(&mut self) -> Result<ValidateBase> {
        let mut base = ValidateBase::default();
        base.is_fake = self.is_fake;
        base.created_by = self.block_candidate.created_by.clone();
        base.prev_blocks_ids = std::mem::take(&mut self.prev_blocks_ids);
        let block_id = &self.block_candidate.block_id;
        log::info!(target: "validate_query", "validate query for {:#} started", block_id);
        if block_id.shard() != self.shard() {
            soft_reject_query!("block candidate belongs to shard {} different from current shard {}",
                block_id.shard(), self.shard())
        }
        if !block_id.shard().is_masterchain() && !block_id.shard().is_standard_workchain() {
            soft_reject_query!("can validate block candidates only for masterchain (-1) and base workchain (0) and standard workchain (1-255)")
        }
        if block_id.shard().is_masterchain() && base.prev_blocks_ids.is_empty() {
            self.min_mc_seq_no = 0
        }
        match base.prev_blocks_ids.len() {
            2 => {
                if block_id.shard().is_masterchain() {
                    soft_reject_query!("cannot merge shards in masterchain")
                }
                if !(block_id.shard().is_parent_for(&base.prev_blocks_ids[0].shard()) && block_id.shard().is_parent_for(&base.prev_blocks_ids[1].shard())
                    && (base.prev_blocks_ids[0].shard().shard_prefix_with_tag() < base.prev_blocks_ids[1].shard().shard_prefix_with_tag())) {
                    soft_reject_query!("the two previous blocks for a merge operation are not siblings or are not children of current shard")
                }
                for blk in &base.prev_blocks_ids {
                    if blk.seq_no == 0 {
                        soft_reject_query!("previous blocks for a block merge operation must have non-zero seqno")
                    }
                }
                base.after_merge = true;
            }
            1 => {
                // creating next block
                if base.prev_blocks_ids[0].shard() != block_id.shard() {
                    base.after_split = true;
                    if !base.prev_blocks_ids[0].shard().is_parent_for(block_id.shard()) {
                        soft_reject_query!("previous block does not belong to the shard we are generating a new block for")
                    }
                    if block_id.shard().is_masterchain() {
                        soft_reject_query!("cannot split shards in masterchain")
                    }
                }
                if block_id.shard().is_masterchain() && self.min_mc_seq_no > base.prev_blocks_ids[0].seq_no {
                    soft_reject_query!("cannot refer to specified masterchain block {} \
                        because it is later than {} the immediately preceding masterchain block",
                            self.min_mc_seq_no, base.prev_blocks_ids[0].seq_no)
                }
            }
            0 => soft_reject_query!("must have one or two previous blocks to generate a next block"),
            _ => soft_reject_query!("cannot have more than two previous blocks")
        }
        
        // 4. unpack block candidate (while necessary data is being loaded)
        Self::unpack_block_candidate(base, &mut self.block_candidate)
    }

    async fn init_mc_data(&mut self, base: &mut ValidateBase) -> Result<McData> {
        // 2. learn latest masterchain state and block id
        let mc_data = self.get_ref_mc_state(base).await?;

        // 3. load state(s) corresponding to previous block(s)
        for i in 0..base.prev_blocks_ids.len() {
            log::debug!(target: "validate_query", "load state for prev block {} of {} {}", i + 1, base.prev_blocks_ids.len(), base.prev_blocks_ids[i]);
            let prev_state = (self.engine.clone()).wait_state(&base.prev_blocks_ids[i], Some(1_000), true).await?;
            if &self.shard == prev_state.shard() && prev_state.state().before_split() {
                reject_query!("cannot accept new unsplit shardchain block for {} \
                    after previous block {} with before_split set", self.shard, prev_state.block_id())
            }
            base.prev_states.push(prev_state);
        }
        if !base.shard().is_masterchain() {
            // It is impossible to get the master state (it have got in 'get_ref_mc_state' above)
            // for block without proof. But proof can appear bit later due to parralelism specials.
            // So this check is not needed.

            // 5.1. request corresponding block handle
            // let _handle = self.engine.load_block_handle(mc_data.state.block_id())?.ok_or_else(
            //     || error!("Cannot load handle for masterblock {}", mc_data.state.block_id())
            // )?;
            // if !self.is_fake && !handle.has_proof() && handle.id().seq_no() != 0 {
            //     reject_query!("reference masterchain block {} for block {} does not have a valid proof",
            //         handle.id(), base.block_id())
            // }
        } else if &base.prev_blocks_ids[0] != mc_data.state.block_id() {
            soft_reject_query!("cannot validate masterchain block {} because it refers to masterchain \
                block {} but its (expected) previous block is {}",
                    base.block_id(), mc_data.state.block_id(), base.prev_blocks_ids[0])
        }
        Ok(mc_data)
    }

    // unpack block candidate, and check root hash and file hash
    fn unpack_block_candidate(mut base: ValidateBase, block_candidate: &mut BlockCandidate) -> Result<ValidateBase> {
        CHECK!(!block_candidate.data.is_empty());
        // 1. deserialize block itself
        let data = std::mem::take(&mut block_candidate.data);
        base.block = BlockStuff::new(block_candidate.block_id.clone(), data)?;
        // 3. initial block parse
        Self::init_parse(&mut base)?;
        // ...
        Self::extract_collated_data(&mut base, block_candidate)?;
        Ok(base)
    }

    // init_parse
    fn init_parse(base: &mut ValidateBase) -> Result<()> {
        base.global_id = base.block.block().global_id();
        base.info = base.block.block().read_info()?;
        let block_id = BlockIdExt::from_ext_blk(base.info.read_master_id()?);
        CHECK!(block_id.shard_id.is_masterchain());
        let prev_blocks_ids = base.info.read_prev_ids()?;

        if prev_blocks_ids.len() != base.prev_blocks_ids.len() {
            soft_reject_query!("block header declares {} previous blocks, but we are given {}",
                prev_blocks_ids.len(), base.prev_blocks_ids.len())
        }
        for (i, blk) in base.prev_blocks_ids.iter().enumerate() {
            if &prev_blocks_ids[i] != blk {
                soft_reject_query!("previous block #{} mismatch: expected {}, found in header {}",
                    i + 1, blk, prev_blocks_ids[i]);
            }
        }
        if base.info.after_split() != base.after_split {
            // ??? impossible
            reject_query!("after_split mismatch in block header")
        }
        if base.info.shard() != base.shard() {
            reject_query!("shard mismatch in the block header")
        }
        base.state_update = base.block.block().read_state_update()?;
        base.value_flow = base.block.block().read_value_flow()?;

        if base.info.key_block() {
            log::info!(target: "validate_query", "validating key block {}", base.block_id());
        }
        if base.info.start_lt() >= base.info.end_lt() {
            reject_query!("block has start_lt greater than or equal to end_lt")
        }
        if base.info.shard().is_masterchain() && (base.info.after_merge() || base.info.before_split() || base.info.after_split()) {
            reject_query!("block header declares split/merge for a masterchain block")
        }
        if base.info.after_merge() && base.info.after_split() {
            reject_query!("a block cannot be both after merge and after split at the same time")
        }
        if base.info.after_split() && base.shard().is_full() {
            reject_query!("a block with empty shard prefix cannot be after split")
        }
        if base.info.after_merge() && !base.shard().can_split() {
            reject_query!("a block split 60 times cannot be after merge")
        }
        if base.info.key_block() && !base.shard().is_masterchain() {
            reject_query!("a non-masterchain block cannot be a key block")
        }
        if base.info.vert_seqno_incr() != 0 {
            // what about non-masterchain blocks?
            reject_query!("new blocks cannot have vert_seqno_incr set")
        }
        if base.info.after_merge() != base.after_merge {
            reject_query!("after_merge value mismatch in block header")
        }
        base.extra = base.block.block().read_extra()?;
        if &base.created_by != base.extra.created_by() {
            reject_query!("block candidate {} has creator {:x} but the block header contains different value {:x}",
                base.block_id(), base.created_by, base.extra.created_by())
        }
        if base.shard().is_masterchain() {
            base.mc_extra = base.extra.read_custom()?
                .ok_or_else(|| error!("masterchain block candidate without McBlockExtra"))?;
            if base.mc_extra.is_key_block() != base.info.key_block() {
                reject_query!("key_block flag mismatch in BlockInfo and McBlockExtra")
            }
            if base.info.key_block() && base.mc_extra.config().is_none() {
                reject_query!("key_block must contain ConfigParams in McBlockExtra")
            }
            base.recover_create_msg = base.mc_extra.read_recover_create_msg()?;
            base.copyleft_msgs = base.mc_extra.read_copyleft_msgs()?;
            base.mint_msg = base.mc_extra.read_mint_msg()?;
        } else if base.extra.is_key_block() {
            reject_query!("non-masterchain block cannot have McBlockExtra")
        }
        // ...
        Ok(())
    }

    // extract_collated_data_from
    fn extract_collated_data_from(base: &mut ValidateBase, croot: Cell, idx: usize) -> Result<()> {
        match croot.cell_type() {
            CellType::Ordinary => match TopBlockDescrSet::construct_from_cell(croot) {
                Ok(descr) => if base.top_shard_descr_dict.is_empty() {
                    log::debug!(target: "validate_query", "collated datum #{} is a TopBlockDescrSet", idx);
                    base.top_shard_descr_dict = descr;
                    base.top_shard_descr_dict.count(10000)
                        .map_err(|err| error!("invalid TopBlockDescrSet : {}", err))?;
                } else {
                    reject_query!("duplicate TopBlockDescrSet in collated data")
                }
                Err(err) => if let Some(BlockError::InvalidConstructorTag{ t, s: _ }) = err.downcast_ref() {
                    log::warn!(target: "validate_query", "collated datum # {} has unknown type (magic {:X}), ignoring", idx, t);
                } else {
                    return Err(err)
                }
            }
            CellType::MerkleProof => {
                let merkle_proof = match MerkleProof::construct_from_cell(croot) {
                    Err(err) => reject_query!("invalid Merkle proof: {:?}", err),
                    Ok(mp) => mp
                };
                let virt_root = merkle_proof.proof.virtualize(1);
                let virt_root_hash = merkle_proof.hash;
                log::debug!(target: "validate_query", "collated datum # {} is a Merkle proof with root hash {:x}",
                    idx, virt_root_hash);
                if base.virt_roots.insert(virt_root_hash.clone(), virt_root).is_some() {
                    reject_query!("Merkle proof with duplicate virtual root hash {:x}", virt_root_hash);
                }
            }
            _ => reject_query!("it is a special cell, but not a Merkle proof root")
        }
        Ok(())
    }

    // processes further and sorts data in collated_roots
    fn extract_collated_data(base: &mut ValidateBase, block_candidate: &BlockCandidate) -> Result<()> {
        if !block_candidate.collated_data.is_empty() {
            // 8. deserialize collated data
            let collated_roots = match deserialize_cells_tree(&mut Cursor::new(&block_candidate.collated_data)) {
                Ok(cells) => cells,
                Err(err) => reject_query!("cannot deserialize collated data: {}", err)
            };
            // 9. extract/classify collated data
            for i in 0..collated_roots.len() {
                let croot = collated_roots[i].clone();
                Self::extract_collated_data_from(base, croot, i)?;
            }
        }
        Ok(())
    }

    async fn get_ref_mc_state(&mut self, base: &ValidateBase) -> Result<McData> {
        let mc_state = match base.info.read_master_ref()? {
            Some(master_ref) => (self.engine.clone()).wait_state(&master_ref.master.master_block_id().1, Some(1_000), true).await?,
            None => (self.engine.clone()).wait_state(&base.prev_blocks_ids[0], Some(1_000), true).await?
        };
        log::debug!(target: "validate_query", "in ValidateQuery::get_ref_mc_state() {}", mc_state.block_id());
        if mc_state.state().seq_no() < self.min_mc_seq_no {
            reject_query!("requested to validate a block referring to an unknown future masterchain block {} < {}",
                mc_state.state().seq_no(), self.min_mc_seq_no)
        }
        self.try_unpack_mc_state(&base, mc_state)
    }

    // fn process_mc_state(&mut self, mc_data: &McData, mc_state: &ShardStateStuff) -> Result<()> {
    //     if mc_data.state.block_id() != mc_state.block_id() {
    //         if !mc_data.state.has_prev_block(mc_state.block_id())? {
    //             reject_query!("attempting to register masterchain state for block {} \
    //                 which is not an ancestor of most recent masterchain block {}",
    //                     mc_state.block_id(), mc_data.state.block_id())
    //         }
    //     }
    //     self.engine.set_aux_mc_state(mc_state)?;
    //     Ok(())
    // }

    fn try_unpack_mc_state(
        &mut self, 
        base: &ValidateBase, 
        mc_state: Arc<ShardStateStuff>
    ) -> Result<McData> {
        log::debug!(target: "validate_query", "unpacking reference masterchain state {}", mc_state.block_id());
        let mc_state_extra = mc_state.shard_state_extra()?.clone();
        let config_params = mc_state_extra.config();
        CHECK!(config_params, inited);
        if let Some(gs) = base.info.gen_software() {
            if gs.version < config_params.global_version() {
                reject_query!("This block version {} is too old, node_version: {} net version: {}",
                    gs.version, supported_version(), config_params.global_version())
            }
        }
        // ihr_enabled_ = config_params->ihr_enabled();
        self.create_stats_enabled = config_params.has_capability(GlobalCapabilities::CapCreateStatsEnabled);
        if config_params.has_capabilities() && (config_params.capabilities() & !supported_capabilities()) != 0 {
            log::error!(target: "validate_query", "block generation capabilities {} have been enabled in global configuration, \
                but we support only {} (upgrade validator software?)",
                    config_params.capabilities(), supported_capabilities());
        }
        if config_params.global_version() > supported_version() {
            log::error!(target: "validate_query", "block version {} have been enabled in global configuration, \
                but we support only {} (upgrade validator software?)",
                    config_params.global_version(), supported_version());
        }
        self.old_mc_shards = mc_state_extra.shards().clone();
        self.new_mc_shards = if base.shard().is_masterchain() {
            base.mc_extra.shards().clone()
        } else {
            self.old_mc_shards.clone()
        };
        if base.global_id != mc_state.state().global_id() {
            reject_query!("blockchain global id mismatch: new block has {} while the masterchain configuration expects {}",
                base.global_id, mc_state.state().global_id())
        }
        CHECK!(&base.info, inited);
        if base.info.vert_seq_no() != mc_state.state().vert_seq_no() {
            reject_query!("vertical seqno mismatch: new block has {} while the masterchain configuration expects {}",
                base.info.vert_seq_no(), mc_state.state().vert_seq_no())
        }
        let (prev_key_block_seqno, prev_key_block);
        if mc_state_extra.after_key_block {
            prev_key_block_seqno = mc_state.block_id().seq_no();
            prev_key_block = Some(mc_state.block_id().clone());
        } else if let Some(block_ref) = mc_state_extra.last_key_block.clone() {
            prev_key_block_seqno = block_ref.seq_no;
            prev_key_block = Some(block_ref.master_block_id().1);
        } else {
            prev_key_block_seqno = 0;
            prev_key_block = None;
        };
        if base.info.prev_key_block_seqno() != prev_key_block_seqno {
            reject_query!("previous key block seqno value in candidate block header is {} \
                while the correct value corresponding to reference masterchain state {} is {}",
                    base.info.prev_key_block_seqno(), mc_state.block_id(), prev_key_block_seqno)
        }
        self.block_limits = config_params.block_limits(base.shard().is_masterchain())?;
        if !base.shard().is_masterchain() {
            check_this_shard_mc_info(
               base.shard(),
               base.block_id(),
               base.after_merge,
               base.after_split,
               base.info.before_split(),
               &base.prev_blocks_ids,
               &config_params,
               &mc_state_extra,
               true,
               base.now()
            ).map_err(|e| error!("masterchain configuration does not admit creating block {}: {}", base.block_id(), e))?;
        }
        Ok(McData {
            mc_state_extra,
            state: mc_state,
            prev_key_block_seqno,
            prev_key_block,
        })
    }

/*
 * 
 *  METHODS CALLED FROM try_validate() stage 0
 * 
 */

    fn compute_next_state(&mut self, base: &mut ValidateBase, mc_data: &McData) -> Result<()> {
        let prev_state = base.prev_states[0].clone();
        base.prev_state = Some(prev_state.clone());
        let prev_state_root = if base.after_merge && base.prev_states.len() == 2 {
            self.check_one_prev_state(base, &base.prev_states[0])?;
            self.check_one_prev_state(base, &base.prev_states[1])?;
            let left  = base.prev_states[0].root_cell().clone();
            let right = base.prev_states[1].root_cell().clone();
            ShardStateStuff::construct_split_root(left, right)?
        } else {
            CHECK!(base.prev_states.len(), 1);
            self.check_one_prev_state(base, &base.prev_states[0])?;
            base.prev_states[0].root_cell().clone()
        };
        log::debug!(target: "validate_query", "computing next state");
        let next_state_root = base.state_update.apply_for(&prev_state_root)
            .map_err(|err| error!("cannot apply Merkle update from block to compute new state : {}", err))?;
        log::debug!(target: "validate_query", "next state computed");
        let next_state = ShardStateStuff::from_root_cell(
            base.block_id().clone(), 
            next_state_root.clone(),
            #[cfg(feature = "telemetry")]
            self.engine.engine_telemetry(),
            self.engine.engine_allocated()
        )?;
        base.next_state = Some(next_state.clone());
        if base.info.end_lt() != next_state.state().gen_lt() {
            reject_query!("new state contains generation lt {} distinct from end_lt {} in block header",
                next_state.state().gen_lt(), base.info.end_lt())
        }
        if base.now() != next_state.state().gen_time() {
            reject_query!("new state contains generation time {} distinct from the value {} in block header",
                next_state.state().gen_time(), base.now())
        }
        if base.info.before_split() != next_state.state().before_split() {
            reject_query!("before_split value mismatch in new state and in block header")
        }
        if (base.block_id().seq_no != next_state.state().seq_no()) || (base.shard() != next_state.state().shard()) {
            reject_query!("header of new state claims it belongs to block {} instead of {}",
                next_state.state().shard(), base.block_id().shard())
        }
        if next_state.state().custom_cell().is_some() != base.shard().is_masterchain() {
            reject_query!("McStateExtra in the new state of a non-masterchain block, or conversely")
        }
        if base.shard().is_masterchain() {
            base.prev_state_extra = prev_state.shard_state_extra()?.clone();
            base.next_state_extra = next_state.shard_state_extra()?.clone();
            let next_state_extra = next_state.shard_state_extra()?;
            if next_state_extra.shards() != base.mc_extra.shards() {
                reject_query!("ShardHashes in the new state and in the block differ")
            }
            if base.info.key_block() {
                CHECK!(base.mc_extra.config().is_some());
                if let Some(config) = base.mc_extra.config() {
                    if config != &next_state_extra.config {
                        reject_query!("ConfigParams in the header of the new key block and in the new state differ")
                    }
                }
            }
        } else {
            base.prev_state_extra = mc_data.state.shard_state_extra()?.clone();
            base.next_state_extra = mc_data.state.shard_state_extra()?.clone();
        }
        Ok(())
    }

    // similar to Collator::unpack_one_last_state()
    fn check_one_prev_state(&self, base: &ValidateBase, ss: &ShardStateStuff) -> Result<()> {
        if ss.state().vert_seq_no() > base.info.vert_seq_no() {
            reject_query!("one of previous states {} has vertical seqno {} larger than that of the new block {}",
                ss.state().id(), ss.state().vert_seq_no(), base.info.vert_seq_no())
        }
        Ok(())
    }

    fn unpack_prev_state(&mut self, base: &mut ValidateBase) -> Result<()> {
        base.prev_state_accounts = base.prev_states[0].state().read_accounts()?;
        base.prev_validator_fees = base.prev_states[0].state().total_validator_fees().clone();
        if let Some(state) = base.prev_states.get(1) {
            CHECK!(base.after_merge);
            let key = state.shard().merge()?.shard_key(false);
            base.prev_state_accounts.hashmap_merge(&state.state().read_accounts()?, &key)?;
            base.prev_state_accounts.update_root_extra()?;
            base.prev_validator_fees.add(state.state().total_validator_fees())?;
        } else if base.after_split {
            base.prev_state_accounts.split_for(&base.shard().shard_key(false))?;
            base.prev_state_accounts.update_root_extra()?;
            if base.shard().is_right_child() {
                base.prev_validator_fees.grams += 1;
            }
            base.prev_validator_fees.grams /= 2;
        }
        Ok(())
    }

    fn unpack_next_state(&self, base: &mut ValidateBase, mc_data: &McData) -> Result<()> {
        log::debug!(target: "validate_query", "unpacking new state");
        let next_state = base.next_state.as_ref().ok_or_else(
            || error!("Next state is not initialized in validator query")
        )?.state();
        if next_state.gen_time() != base.now() {
            reject_query!(
                "new state of {} claims to have been generated at unixtime {}, \
                but the block header contains {}",
                next_state.id(), next_state.gen_time(), base.info.gen_utime()
            )
        }
        if next_state.gen_lt() != base.info.end_lt() {
            reject_query!(
                "new state of {} claims to have been generated at logical time {}, \
                but the block header contains end lt {}",
                next_state.id(), next_state.gen_lt(), base.info.end_lt()
            )
        }
        if !base.shard().is_masterchain() {
            let mc_blkid = next_state.master_ref().ok_or_else(
                || error!("new state of {} doesn't have have master ref", next_state.id())
            )?.master.clone().master_block_id().1;
            if &mc_blkid != mc_data.state.block_id() {
                reject_query!(
                    "new state refers to masterchain block {} different from {} \
                    indicated in block header",
                    mc_blkid, mc_data.state.block_id()
                )
            }
        }
        if next_state.vert_seq_no() != base.info.vert_seq_no() {
            reject_query!(
                "new state has vertical seqno {} different from {} \
                declared in the new block header",
                next_state.vert_seq_no(), base.info.vert_seq_no()
            )
        }
        base.next_state_accounts = next_state.read_accounts()?;
        // ...
        Ok(())
    }

    async fn init_output_queue_manager(&self, mc_data: &McData, base: &ValidateBase) -> Result<MsgQueueManager> {
        MsgQueueManager::init(
            &self.engine,
            &mc_data.state,
            self.shard().clone(),
            &self.new_mc_shards,
            &base.prev_states,
            base.next_state.as_ref(),
            base.after_merge,
            base.after_split,
            None,
        ).await
    }

    // similar to Collator::update_one_shard()
    fn check_one_shard(
        &mut self,
        base: &ValidateBase,
        mc_data: &McData,
        info: &McShardRecord,
        sibling: Option<&McShardRecord>,
        wc_info: Option<&WorkchainDescr>
    ) -> Result<()> {
        let shard = info.shard();
        log::debug!(target: "validate_query", "checking shard {} in new shard configuration", shard);
        if info.descr.next_validator_shard != shard.shard_prefix_with_tag() {
            reject_query!("new shard configuration for shard {} contains different next_validator_shard {}",
                shard, info.descr.next_validator_shard)
        }
        let old = self.old_mc_shards.find_shard(&shard.left_ancestor_mask()?)?;
        let mut prev: Option<McShardRecord> = None;
        let mut cc_seqno = !0;
        let mut old_before_merge = false;
        let workchain_created = false;
        if old.is_none() {
            if !shard.is_full() {
                reject_query!("new shard configuration contains split shard {} unknown before", shard)
            }
            let wc_info = wc_info.ok_or_else(|| error!("new shard configuration contains newly-created \
                shard {} for an unknown workchain", shard))?;
            if !wc_info.active {
                reject_query!("new shard configuration contains newly-created shard {} for an inactive workchain", shard)
            }
            if info.descr.seq_no != 0 {
                reject_query!("newly-created shard {} starts with non-zero seqno {}",
                    shard, info.descr.seq_no)
            }
            if info.descr.root_hash != wc_info.zerostate_root_hash || info.descr.file_hash != wc_info.zerostate_file_hash {
                reject_query!("new shard configuration contains newly-created shard {} with incorrect zerostate hashes",
                    shard)
            }
            if info.descr.end_lt >= base.info.start_lt() {
                reject_query!("newly-created shard {} has incorrect logical time {} for a new block with start_lt={}",
                    shard, info.descr.end_lt, base.info.start_lt());
            }
            if info.descr.gen_utime > base.now() {
                reject_query!("newly-created shard {} has incorrect creation time {} for a new block created only at {}",
                    shard, info.descr.gen_utime, base.now())
            }
            if info.descr.before_split || info.descr.before_merge || info.descr.want_split || info.descr.want_merge {
                reject_query!("newly-created shard {} has merge/split flags (incorrectly) set", shard)
            }
            if info.descr.min_ref_mc_seqno != 0 {
                reject_query!("newly-created shard {} has finite min_ref_mc_seqno", shard)
            }
            if info.descr.reg_mc_seqno != base.block_id().seq_no {
                reject_query!("newly-created shard {} has registration mc seqno {} different from seqno of current block {}",
                    shard, info.descr.reg_mc_seqno, base.block_id().seq_no)
            }
            if !info.descr.fees_collected.is_zero()? {
                reject_query!("newly-created shard {} has non-zero fees_collected", shard)
            }
            cc_seqno = 0;
        } else if let Some(old) = old {
            if old.block_id == info.block_id {
                // shard unchanged ?
                log::debug!(target: "validate_query", "shard {} unchanged", shard);
                if !old.basic_info_equal(&info, true, true) {
                    reject_query!("shard information for block {}  listed in new shard configuration \
                        differs from that present in the old shard configuration for the same block",
                        info.block_id);
                }
                cc_seqno = old.descr.next_catchain_seqno;
                prev = Some(old);
                // ...
            } else {
                // shard changed, extract and check TopShardBlockDescr from collated data
                log::debug!(target: "validate_query", "shard {} changed from {} to {}", shard, old.block_id.seq_no, info.block_id.seq_no);
                if info.descr.reg_mc_seqno != base.block_id().seq_no {
                    reject_query!("shard information for block {} has been updated in the new shard configuration, but it has reg_mc_seqno={} different from that of the current block {}",
                        info.block_id, info.descr.reg_mc_seqno, base.block_id().seq_no)
                }
                let sh_bd = match base.top_shard_descr_dict.get_top_block_descr(info.block_id.shard())? {
                    Some(tbd) => TopBlockDescrStuff::new(tbd, &info.block_id, base.is_fake)?,
                    None => reject_query!("no ShardTopBlockDescr for newly-registered shard {} is present in collated data",
                        info.block_id)
                };
                if sh_bd.proof_for() != &info.block_id {
                    reject_query!("ShardTopBlockDescr for shard {} is for new block {} \
                        instead of {} declared in new shardchain configuration",
                            shard, sh_bd.proof_for(), info.block_id)
                }
                // following checks are similar to those of Collator::import_new_shard_top_blocks()
                let do_flags = TopBlockDescrMode::FAIL_NEW | TopBlockDescrMode::FAIL_TOO_NEW;
                let mut res_flags = 0;
                let chain_len = sh_bd.prevalidate(mc_data.state.block_id(), &mc_data.mc_state_extra,
                    mc_data.state.state().vert_seq_no(), do_flags, &mut res_flags)
                    .map_err(|err| error!("ShardTopBlockDescr for {} is invalid: res_flags={}, err: {}",
                        sh_bd.proof_for(), res_flags, err))?;
                if chain_len <= 0 || chain_len > 8 {
                    reject_query!("ShardTopBlockDescr for {} is invalid: its chain length is {} (not in range 1..8)",
                        sh_bd.proof_for(), chain_len)
                }
                let chain_len = chain_len as usize;
                if sh_bd.gen_utime() > base.now() {
                    reject_query!("ShardTopBlockDescr for {} is invalid: it claims to be generated at {} while it is still {}",
                        sh_bd.proof_for(), sh_bd.gen_utime(), base.now())
                }
                let descr = sh_bd.get_top_descr(chain_len)
                    .map_err(|err| error!("No top descr for {:?}: {}", sh_bd, err))?;
                CHECK!(&descr, inited);
                CHECK!(descr.block_id(), sh_bd.proof_for());
                let start_blks = sh_bd.get_prev_at(chain_len);
                may_update_shard_block_info(mc_data.state.shards()?, &descr, &start_blks, base.info.start_lt(), None)
                    .map_err(|err| error!("new top shard block {:?} cannot be added to shard configuration: {}", sh_bd, err))?;
                if !descr.basic_info_equal(info, true, false) {
                    reject_query!("shard information for block {:?} listed in new shard configuration \
                        differs from that present in ShardTopBlockDescr (and block header)", info.block_id)
                }
                // all fields in info and descr are equal
                // except fsm*, before_merge_, next_catchain_seqno_
                // of these, only next_catchain_seqno_ makes sense in descr
                cc_seqno = descr.descr.next_catchain_seqno;
                // check that there is a corresponding record in ShardFees
                if let Some(import) =  base.mc_extra.fees().get_serialized(shard.full_key_with_tag()?)? {
                    if import.fees != descr.descr.fees_collected {
                        reject_query!("ShardFees record for new shard top block {:?} declares fees_collected={:?}, \
                            but the shard configuration contains a different value {}",
                            sh_bd, import.fees, descr.descr.fees_collected)
                    }
                    if import.create != descr.descr.funds_created {
                        reject_query!("ShardFees record for new shard top block {:?} declares funds_created={:?}, \
                            but the shard configuration contains a different value {}",
                            sh_bd, import.create, descr.descr.funds_created);
                    }
                } else if !descr.descr.fees_collected.is_zero()? {
                    reject_query!("new shard top block {:?} has been registered and has non-zero collected fees {}, \
                        but there is no corresponding entry in ShardFees", sh_bd, descr.descr.fees_collected)
                }
                // register shard block creators
                self.register_shard_block_creators(base, &sh_bd.get_creator_list(chain_len)?)?;
                // ...
                if old.shard().is_parent_for(shard) {
                    // shard has been split
                    log::debug!(target: "validate_query", "detected shard split {} -> {}", old.shard(), shard);
                    // ...
                } else if shard.is_parent_for(old.shard()) {
                    // shard has been merged
                    if let Some(old2) = self.old_mc_shards.find_shard(&shard.right_ancestor_mask()?)? {
                        if &old.shard().sibling() != old2.shard() {
                            reject_query!("shard {} has been impossibly merged from more than two shards \
                                {}, {} and others", shard, old.shard(), old2.shard())
                        }
                        log::debug!(target: "validate_query", "detected shard merge {} + {} -> {}",
                            old.shard(), old2.shard(), shard);
                    } else {
                        // CHECK!(old2.is_some());
                        reject_query!("No plus_one shard") // TODO: check here
                    }
                    // ...
                } else if shard == old.shard() {
                    // shard updated without split/merge
                    prev = Some(old);
                    // ...
                } else {
                    reject_query!("new configuration contains shard {} that could not be \
                        obtained from previously existing shard {}", shard, old.shard());
                // ...
                }
            }
        }
        let mut fsm_inherited = false;
        if let Some(prev) = prev {
            // shard was not created, split or merged; it is a successor of `prev`
            old_before_merge = prev.descr.before_merge;
            if !prev.descr().is_fsm_none() && !prev.descr().fsm_equal(info.descr())
                && base.now() < prev.descr().fsm_utime_end() && !info.descr.before_split {
                reject_query!("future split/merge information for shard {} has been arbitrarily \
                    changed without a good reason", shard)
            }
            fsm_inherited = !prev.descr().is_fsm_none() && prev.descr().fsm_equal(info.descr());
            if fsm_inherited && (base.now() > prev.descr().fsm_utime_end() || info.descr.before_split) {
                reject_query!("future split/merge information for shard {}\
                    has been carried on to the new shard configuration, but it is either expired (expire time {}, now {}), 
                    or before_split bit has been set ({})",
                        shard, prev.descr().fsm_utime_end(), base.now(), info.descr.before_split);
            }
        } else {
            // shard was created, split or merged
            if info.descr.before_split {
                reject_query!("a newly-created, split or merged shard {} cannot have before_split set immediately after", shard)
            }
        }
        let wc_info = wc_info.expect("in ton node it is a bug");
        let depth = shard.prefix_len();
        let split_cond = (info.descr.want_split || depth < wc_info.min_split()) && depth < wc_info.max_split() && depth < MAX_SPLIT_DEPTH;
        let merge_cond = depth > wc_info.min_split() && (info.descr.want_merge || depth > wc_info.max_split())
            && (sibling.map(|s| s.descr.want_merge).unwrap_or_default() || depth > wc_info.max_split());
        if !fsm_inherited && !info.descr().is_fsm_none() {
            if info.descr().fsm_utime() < base.now() || info.descr().fsm_utime_end() <= info.descr().fsm_utime()
                || info.descr().fsm_utime_end() < info.descr().fsm_utime() + MIN_SPLIT_MERGE_INTERVAL
                || info.descr().fsm_utime_end() > base.now() + MAX_SPLIT_MERGE_DELAY {
                reject_query!("incorrect future split/merge interval {} .. {} \
                    set for shard {} in new shard configuration (it is {} now)",
                        info.descr().fsm_utime(), info.descr().fsm_utime_end(), shard, base.now());
            }
            if info.descr().is_fsm_split() && !split_cond {
                reject_query!("announcing future split for shard {} in new shard configuration, \
                    but split conditions are not met", shard)
            }
            if info.descr().is_fsm_merge() && !merge_cond {
                reject_query!("announcing future merge for shard {} in new shard configuration, \
                    but merge conditions are not met", shard) 
            }
        }
        if info.descr.before_merge {
            if !sibling.map(|s| s.descr.before_merge).unwrap_or_default() {
                reject_query!("before_merge set for shard {} in shard configuration, \
                    but not for its sibling", shard)
            }
            if !info.descr().is_fsm_merge() {
                reject_query!("before_merge set for shard {} in shard configuration, \
                    but it has not been announced in future split/merge for this shard", shard)
            }
            if !merge_cond {
                reject_query!("before_merge set for shard {} in shard configuration, \
                    but merge conditions are not met", shard)
            }
        }
        CHECK!(cc_seqno != !0);
        let cc_updated = info.descr.next_catchain_seqno != cc_seqno;
        if info.descr.next_catchain_seqno != cc_seqno + cc_updated as u32 {
            reject_query!("new shard configuration for shard {} changed catchain seqno \
                from {} to {} (only updates by at most one are allowed)", shard, cc_seqno, info.descr.next_catchain_seqno)
        }
        if !cc_updated && self.update_shard_cc {
            reject_query!("new shard configuration for shard {} has unchanged catchain seqno {}, \
                but it must have been updated for all shards", shard, cc_seqno)
        }
        let bm_cleared = !info.descr.before_merge && old_before_merge;
        if !cc_updated && bm_cleared && !workchain_created {
            reject_query!("new shard configuration for shard {} has unchanged catchain seqno {} \
                while the before_merge bit has been cleared", shard, cc_seqno)
        }
        if cc_updated && !(self.update_shard_cc || bm_cleared) {
            reject_query!("new shard configuration for shard {} has increased catchain seqno {} \
                without a good reason", shard, cc_seqno);
        }
        base.result.min_shard_ref_mc_seqno.fetch_min(info.descr.min_ref_mc_seqno, Ordering::Relaxed);
        base.result.max_shard_utime.fetch_max(info.descr.gen_utime, Ordering::Relaxed);
        base.result.max_shard_lt.fetch_max(info.descr.end_lt, Ordering::Relaxed);
        // dbg!(base.min_shard_ref_mc_seqno, base.max_shard_utime, base.max_shard_lt);
        Ok(())
    }

    // checks old_shard_conf_ -> base.mc_extra.shards() transition using top_shard_descr_dict_ from collated data
    // similar to Collator::update_shard_config()
    fn check_shard_layout(&mut self, base: &ValidateBase, mc_data: &McData) -> Result<()> {
        if !base.shard().is_masterchain() {
            return Ok(())
        }
        let prev_now = base.prev_state.as_ref().ok_or_else(
            || error!("Prev state is not initialized in validator query")
        )?.state().gen_time();
        if prev_now > base.now() {
            reject_query!("creation time is not monotonic: {} after {}", base.now(), prev_now)
        }
        let ccvc = base.next_state_extra.config.catchain_config()?;
        let wc_set = base.next_state_extra.config.workchains()?;
        self.update_shard_cc = base.info.key_block()
            || (base.now() / ccvc.shard_catchain_lifetime > prev_now / ccvc.shard_catchain_lifetime);
        if self.update_shard_cc {
            log::debug!(target: "validate_query", "catchain_seqno of all shards must be updated");
        }

        let mut wc_id = INVALID_WORKCHAIN_ID;
        let mut wc_info = None;
        self.new_mc_shards.clone().iterate_shards_with_siblings(|shard, descr, sibling| {
            if wc_id != shard.workchain_id() {
                wc_id = shard.workchain_id();
                if wc_id == INVALID_WORKCHAIN_ID || wc_id == MASTERCHAIN_ID {
                    reject_query!("new shard configuration contains shards of invalid workchain {}", wc_id)
                }
                wc_info = wc_set.get(&wc_id)?;
            }
            let descr = McShardRecord::from_shard_descr(shard, descr);
            if let Some(sibling) = sibling {
                let sibling = McShardRecord::from_shard_descr(descr.shard().sibling(), sibling);
                self.check_one_shard(base, mc_data, &sibling, Some(&descr), wc_info.as_ref())?;
                self.check_one_shard(base, mc_data, &descr, Some(&sibling), wc_info.as_ref())?;
            } else {
                self.check_one_shard(base, mc_data, &descr, None, wc_info.as_ref())?;
            }
            Ok(true)
        }).map_err(|err| error!("new shard configuration is invalid : {}", err))?;
        base.prev_state_extra.config.workchains()?.iterate_keys(|wc_id: i32| {
            if base.mc_extra.shards().get(&wc_id)?.is_none() {
                reject_query!("shards of workchain {} existed in previous \
                    shardchain configuration, but are absent from new", wc_id)
            }
            Ok(true)
        })?;
        wc_set.iterate_with_keys(|wc_id: i32, wc_info| {
            if wc_info.active && !base.mc_extra.shards().get(&wc_id)?.is_some() {
                reject_query!("workchain {} is active, but is absent from new shard configuration", wc_id)
            }
            Ok(true)
        })?;
        self.check_mc_validator_info(base, base.info.key_block()
            || (base.now() / ccvc.mc_catchain_lifetime > prev_now / ccvc.mc_catchain_lifetime))
    }

    // similar to Collator::register_shard_block_creators
    fn register_shard_block_creators(&mut self, _base: &ValidateBase, creator_list: &Vec<UInt256>) -> Result<()> {
        for x in creator_list {
            log::debug!(target: "validate_query", "registering block creator {}", x.to_hex_string());
            if !x.is_zero() {
                *self.block_create_count.entry(x.clone()).or_default() += 1;
            }
            self.block_create_total += 1;
        }
        Ok(())
    }

    // parallel to 4. of Collator::create_mc_state_extra()
    // checks validator_info in mc_state_extra
    fn check_mc_validator_info(&self, base: &ValidateBase, update_mc_cc: bool) -> Result<()> {
        CHECK!(&base.prev_state_extra, inited);
        CHECK!(&base.next_state_extra, inited);
        let old_info = &base.prev_state_extra.validator_info;
        let new_info = &base.next_state_extra.validator_info;

        let cc_updated = new_info.catchain_seqno != old_info.catchain_seqno;
        if new_info.catchain_seqno != old_info.catchain_seqno + cc_updated as u32 {
            reject_query!("new masterchain state increased masterchain catchain seqno from {} to {} \
                (only updates by at most one are allowed)", old_info.catchain_seqno, new_info.catchain_seqno)
        }
        if cc_updated != update_mc_cc {
            match cc_updated {
                true => reject_query!("masterchain catchain seqno increased without any reason"),
                false => reject_query!("masterchain catchain seqno unchanged while it had to")
            }
        }
        let now = base.next_state.as_ref().ok_or_else(
            || error!("Next state is not initialized in validator query")
        )?.state().gen_time();
        let prev_now = base.prev_state.as_ref().ok_or_else(
            || error!("Prev state is not initialized in validator query")
        )?.state().gen_time();
        let ccvc = base.next_state_extra.config.catchain_config()?;
        let cur_validators = base.next_state_extra.config.validator_set()?;
        let lifetime = ccvc.mc_catchain_lifetime;
        let is_key_block = base.info.key_block();
        let mut cc_updated = false;
        let catchain_seqno = new_info.catchain_seqno;
        if is_key_block || (now / lifetime > prev_now / lifetime) {
            cc_updated = true;
            log::debug!(target: "validate_query", "increased masterchain catchain seqno to {}", catchain_seqno);
        }
        let (nodes, _hash_short) = calc_subset_for_workchain(
            &cur_validators,
            &base.next_state_extra.config,
            &ccvc, 
            self.shard.shard_prefix_with_tag(), 
            self.shard.workchain_id(), 
            catchain_seqno,
            now.into()
        )?;

        if nodes.is_empty() {
            reject_query!("cannot compute next masterchain validator set from new masterchain state")
        }

        let vlist_hash = ValidatorSet::calc_subset_hash_short(&nodes, /* new_info.catchain_seqno */ 0)?;
        if new_info.validator_list_hash_short != vlist_hash {
            reject_query!("new masterchain validator list hash incorrect hash: expected {}, found {}",
                new_info.validator_list_hash_short, vlist_hash);
        }
        log::debug!(target: "validate_query", "masterchain validator set hash changed from {} to {}",
            old_info.validator_list_hash_short, vlist_hash);
        if new_info.nx_cc_updated != cc_updated & self.update_shard_cc {
            reject_query!("new_info.nx_cc_updated has incorrect value {}", new_info.nx_cc_updated)
        }
        Ok(())
    }

    fn check_utime_lt(&self, base: &ValidateBase, mc_data: &McData) -> Result<()> {
        CHECK!(&base.config_params, inited);
        let mut gen_lt = std::u64::MIN;
        for state in &base.prev_states {
            if base.info.start_lt() <= state.state().gen_lt() {
                reject_query!("block has start_lt {} less than or equal to lt {} of the previous state",
                    base.info.start_lt(), state.state().gen_lt())
            }
            if base.now() <= state.state().gen_time() {
                reject_query!("block has creation time {} less than or equal to that of the previous state ({})",
                    base.now(), state.state().gen_time())
            }
            gen_lt = std::cmp::max(gen_lt, state.state().gen_lt());
        }
        if base.now() <= mc_data.state.state().gen_time() {
            reject_query!("block has creation time {} less than or equal to that of the reference masterchain state ({})",
                base.now(), mc_data.state.state().gen_time())
        }
        /*
        if base.now() > (unsigned)std::time(nullptr) + 15 {
            reject_query!("block has creation time " << base.now() too much in the future (it is only " << (unsigned)std::time(nullptr) now)");
        }
        */
        if base.info.start_lt() <= mc_data.state.state().gen_lt() {
            reject_query!("block has start_lt {} less than or equal to lt {} of the reference masterchain state",
                base.info.start_lt(), mc_data.state.state().gen_lt())
        }
        let lt_bound = std::cmp::max(gen_lt, std::cmp::max(mc_data.state.state().gen_lt(), base.max_shard_lt()));
        if base.info.start_lt() > lt_bound + base.config_params.get_lt_align() * 4 {
            reject_query!("block has start_lt {} which is too large without a good reason (lower bound is {})",
                base.info.start_lt(), lt_bound + 1)
        }
        if base.shard().is_masterchain() && base.info.start_lt() - gen_lt > base.config_params.get_max_lt_growth() {
            reject_query!("block increases logical time from previous state by {} which exceeds the limit ({})",
                base.info.start_lt() - gen_lt, base.config_params.get_max_lt_growth())
        }
        let delta_hard = base.config_params.block_limits(base.shard().is_masterchain())?.lt_delta().hard_limit() as u64;
        if base.info.end_lt() - base.info.start_lt() > delta_hard {
            reject_query!("block increased logical time by {} which is larger than the hard limit {}",
                base.info.end_lt() - base.info.start_lt(), delta_hard)
        }
    Ok(())
    }

/*
 * 
 *  METHODS CALLED FROM try_validate() stage 1
 * 
 */

    fn load_block_data(base: &mut ValidateBase) -> Result<()> {
        log::debug!(target: "validate_query", "unpacking block structures");
        base.in_msg_descr = base.extra.read_in_msg_descr()?;
        base.out_msg_descr = base.extra.read_out_msg_descr()?;
        base.account_blocks = base.extra.read_account_blocks()?;
        // run some hand-written checks from block::tlb::
        // (letmatic tests from block::gen:: have been already run for the entire block)
        // count and validate
        log::debug!(target: "validate_query", "validating InMsgDescr");
        // base.in_msg_descr.count(1000000)?;
        log::debug!(target: "validate_query", "validating OutMsgDescr");
        // base.out_msg_descr.count(1000000)?;
        log::debug!(target: "validate_query", "validating ShardAccountBlocks");
        // base.account_blocks.count(1000000)?;
        Ok(())
    }

    fn precheck_value_flow(base: Arc<ValidateBase>) -> Result<()> {
        log::debug!(target: "validate_query", "value flow: {}", base.value_flow);
        // if !base.value_flow.validate() {
        //     reject_query!("ValueFlow of block {} is invalid (in-balance is not equal to out-balance)", base.block_id())
        // }
        if !base.shard().is_masterchain() && !base.value_flow.minted.is_zero()? {
            reject_query!("ValueFlow of block {} \
                is invalid (non-zero minted value in a non-masterchain block)", base.block_id())
        }
        if !base.shard().is_masterchain() && !base.value_flow.recovered.is_zero()? {
            reject_query!("ValueFlow of block {} \
                is invalid (non-zero recovered value in a non-masterchain block)", base.block_id())
        }
        if !base.value_flow.recovered.is_zero()? && base.recover_create_msg.is_none() {
            reject_query!("ValueFlow of block {} \
                has a non-zero recovered fees value, but there is no recovery InMsg", base.block_id())
        }
        if base.value_flow.recovered.is_zero()? && base.recover_create_msg.is_some() {
            reject_query!("ValueFlow of block {} \
                has a zero recovered fees value, but there is a recovery InMsg", base.block_id())
        }
        if !base.value_flow.minted.is_zero()? && base.mint_msg.is_none() {
            reject_query!("ValueFlow of block {} \
                has a non-zero minted value, but there is no mint InMsg", base.block_id())
        }
        if base.value_flow.minted.is_zero()? && base.mint_msg.is_some() {
            reject_query!("ValueFlow of block {} \
                has a zero minted value, but there is a mint InMsg", base.block_id())
        }
        if !base.value_flow.minted.is_zero()? {
            let to_mint = Self::compute_minted_amount(&base)
                .map_err(|err| error!("cannot compute the correct amount of extra currencies to be minted : {}", err))?;
            if base.value_flow.minted != to_mint {
                reject_query!("invalid extra currencies amount to be minted: declared {}, expected {}",
                    base.value_flow.minted, to_mint)
            }
        }
        let create_fee = base.config_params.block_create_fees(base.shard().is_masterchain()).unwrap_or_default();
        let create_fee = CurrencyCollection::from_grams(create_fee >> base.shard().prefix_len());
        if base.value_flow.created != create_fee {
            reject_query!("ValueFlow of block {} declares block creation fee {}, \
                but the current configuration expects it to be {}",
                    base.block_id(), base.value_flow.created, create_fee)
        }
        if !base.value_flow.fees_imported.is_zero()? && !base.shard().is_masterchain() {
            reject_query!("ValueFlow of block {} \
                is invalid (non-zero fees_imported in a non-masterchain block)", base.block_id())
        }
        let cc = base.prev_state_accounts.full_balance();
        if cc != &base.value_flow.from_prev_blk {
            reject_query!("ValueFlow for {} declares from_prev_blk={} \
                but the sum over all accounts present in the previous state is {}",
                    base.block_id(), base.value_flow.from_prev_blk, cc)
        }
        let cc = base.next_state_accounts.full_balance();
        if cc != &base.value_flow.to_next_blk {
            reject_query!("ValueFlow for {} declares to_next_blk={} but the sum over all accounts \
                present in the new state is {}", base.block_id(), base.value_flow.to_next_blk, cc)
        }
        let cc = base.in_msg_descr.full_import_fees();
        if cc.value_imported != base.value_flow.imported {
            reject_query!("ValueFlow for {} declares imported={} but the sum over all inbound messages \
                listed in InMsgDescr is {}", base.block_id(), base.value_flow.imported, cc.value_imported);
        }
        let fees_import = CurrencyCollection::from_grams(cc.fees_collected);
        let cc = base.out_msg_descr.full_exported();
        if cc != &base.value_flow.exported {
            reject_query!("ValueFlow for {} declares exported={} but the sum over all outbound messages \
                listed in OutMsgDescr is {}", base.block_id(), base.value_flow.exported, cc)
        }
        let transaction_fees = base.account_blocks.full_transaction_fees();
        let mut expected_fees = transaction_fees.clone();
        expected_fees.add(&base.value_flow.fees_imported)?;
        expected_fees.add(&base.value_flow.created)?;
        expected_fees.add(&fees_import)?;
        if base.value_flow.fees_collected != expected_fees {
            reject_query!("ValueFlow for {} declares fees_collected={} but the total message import fees are {}, \
                the total transaction fees are {}, creation fee for this block is {} \
                and the total imported fees from shards are {} with a total of {}", base.block_id(),
                    base.value_flow.fees_collected.grams, fees_import, transaction_fees.grams,
                    base.value_flow.created.grams, base.value_flow.fees_imported.grams, expected_fees.grams)
        }
        Ok(())
    }

    // similar to Collator::compute_minted_amount()
    fn compute_minted_amount(base: &ValidateBase) -> Result<CurrencyCollection> {
        let mut to_mint = CurrencyCollection::default();
        if !base.shard().is_masterchain() {
            return Ok(to_mint)
        }
        let to_mint_config = match base.config_params.config(7)? {
            Some(ConfigParamEnum::ConfigParam7(param)) => param.to_mint,
            _ => return Ok(to_mint)
        };
        to_mint_config.iterate_with_keys(|curr_id: u32, amount| {
            let amount2 = base.prev_state_extra.global_balance.get_other(curr_id)?.unwrap_or_default();
            if amount >= amount2 {
                let mut delta = amount.clone();
                delta.sub(&amount2)?;
                log::debug!(target: "validate_query", "currency #{}: existing {}, required {}, to be minted {}",
                    curr_id, amount2, amount, delta);
                to_mint.set_other_ex(curr_id, &delta)?;
            }
            Ok(true)
        }).map_err(|err| error!("error scanning extra currencies to be minted : {}", err))?;
        if !to_mint.is_zero()? {
            log::debug!(target: "validate_query", "new currencies to be minted: {}", to_mint);
        }
        Ok(to_mint)
    }

    fn precheck_one_account_update(base: &ValidateBase, acc_id: UInt256,
        old_val_extra: Option<(ShardAccount, DepthBalanceInfo)>,
        new_val_extra: Option<(ShardAccount, DepthBalanceInfo)>
    ) -> Result<bool> {
        log::debug!(target: "validate_query", "checking update of account {}", acc_id.to_hex_string());
        let acc_blk = base.account_blocks.get(&acc_id)?.ok_or_else(|| error!("the state of account {} \
            changed in the new state with respect to the old state, but the block contains no \
            AccountBlock for this account", acc_id.to_hex_string()))?;
        // if acc_blk_root.is_none() {
        //     if (verbosity >= 3 * 0) {
        //         std::cerr << "state of account " << workchain() << ":" << acc_id.to_hex_string()
        //                     << " in the old shardchain state:" << std::endl;
        //         if (old_value.is_some()) {
        //             block::gen::t_ShardAccount.print(std::cerr, *old_value);
        //         } else {
        //             std::cerr << "<absent>" << std::endl;
        //         }
        //         std::cerr << "state of account " << workchain() << ":" << acc_id.to_hex_string()
        //                     << " in the new shardchain state:" << std::endl;
        //         if (new_value.is_some()) {
        //             block::gen::t_ShardAccount.print(std::cerr, *new_value);
        //         } else {
        //             std::cerr << "<absent>" << std::endl;
        //         }
        //     }
        // }
        let hash_upd = acc_blk.read_state_update().map_err(|err| error!("cannot extract \
            (HASH_UPDATE Account) from the AccountBlock of {} : {}", acc_id.to_hex_string(), err))?;
        if acc_id != *acc_blk.account_id() {
            reject_query!("AccountBlock of account {} appears to belong to another account {}",
                acc_id.to_hex_string(), acc_blk.account_id().to_hex_string())
        }
        if let Some((old_state, _old_extra)) = old_val_extra {
            if hash_upd.old_hash != old_state.account_cell().repr_hash() {
                reject_query!("(HASH_UPDATE Account) from the AccountBlock of {} \
                    has incorrect old hash", acc_id.to_hex_string())
            }
            // if (!block::gen::t_ShardAccount.validate_csr(10000, new_value)) {
            // reject_query!("new state of account "s + acc_id.to_hex_string() +
            //     failed to pass letmated validity checks for ShardAccount");
            // }
            // if (!block::tlb::t_ShardAccount.validate_csr(10000, new_value)) {
            // reject_query!("new state of account "s + acc_id.to_hex_string() +
            //     failed to pass hand-written validity checks for ShardAccount");
            // }
        }
        if let Some((new_state, _new_extra)) = new_val_extra {
            if hash_upd.new_hash != new_state.account_cell().repr_hash() {
                reject_query!("(HASH_UPDATE Account) from the AccountBlock of {} \
                    has incorrect new hash", acc_id.to_hex_string())
            }
            // if (!block::gen::t_ShardAccount.validate_csr(10000, new_value)) {
            // reject_query!("new state of account "s + acc_id.to_hex_string() +
            //     failed to pass letmated validity checks for ShardAccount");
            // }
            // if (!block::tlb::t_ShardAccount.validate_csr(10000, new_value)) {
            // reject_query!("new state of account "s + acc_id.to_hex_string() +
            //     failed to pass hand-written validity checks for ShardAccount");
            // }
        }
        Ok(true)
    }

    fn precheck_account_updates(base: Arc<ValidateBase>) -> Result<()> {
        log::debug!(target: "validate_query", "pre-checking all Account updates between the old and the new state");
        // let prev_accounts = base.prev_state_accounts.clone();
        // let next_accounts = base.next_state_accounts.clone();
        base.prev_state_accounts.scan_diff_with_aug(
            &base.next_state_accounts, |key, old_val_extra, new_val_extra|
            Self::precheck_one_account_update(&base, key, old_val_extra, new_val_extra)
        )?;
        Ok(())
    }

    fn precheck_one_transaction(
        base: &ValidateBase, acc_id: &AccountId, trans_lt: u64, trans_root: Cell,
        prev_trans_lt: &mut u64, prev_trans_hash: &mut UInt256, prev_trans_lt_len: &mut u64,
        acc_state_hash: &mut UInt256
    ) -> Result<bool> {
        log::debug!(target: "validate_query", "pre-checking Transaction {}", trans_lt);
        let trans = Transaction::construct_from_cell(trans_root.clone())?;
        if &trans.account_id() != &acc_id || trans.logical_time() != trans_lt {
            reject_query!("transaction {} of {} claims to be transaction {} of {}",
                trans_lt, acc_id.to_hex_string(), trans.logical_time(), trans.account_id().to_hex_string())
        }
        if trans.now() != base.now() {
            reject_query!("transaction {} of {} claims that current time is {}
                while the block header indicates {}", trans_lt, acc_id.to_hex_string(), trans.now(), base.now())
        }
        if &trans.prev_trans_hash() != &prev_trans_hash
            || &trans.prev_trans_lt() != prev_trans_lt {
            reject_query!("transaction {} of {} claims that the previous transaction was {}:{} \
                while the correct value is {}:{}", trans_lt, acc_id.to_hex_string(),
                    trans.prev_trans_lt(), trans.prev_trans_hash().to_hex_string(),
                    prev_trans_lt, prev_trans_hash.to_hex_string())
        }
        if trans_lt < *prev_trans_lt + *prev_trans_lt_len {
            reject_query!("transaction {} of {} starts at logical time {}, \
                earlier than the previous transaction {} .. {} ends",
                    trans_lt, acc_id.to_hex_string(), trans_lt, prev_trans_lt,
                    *prev_trans_lt + *prev_trans_lt_len)
        }
        let lt_len = trans.msg_count() as u64 + 1;
        if trans_lt <= base.info.start_lt() || trans_lt + lt_len > base.info.end_lt() {
            reject_query!("transaction {} .. {} of {:x} is not inside the logical time interval {} .. {} \
                of the encompassing new block", trans_lt, trans_lt + lt_len, acc_id,
                    base.info.start_lt(), base.info.end_lt())
        }
        let hash_upd = trans.read_state_update()?;
        if &hash_upd.old_hash != acc_state_hash {
            reject_query!("transaction {} of {} claims to start from account state with hash {} \
                while the actual value is {}", trans_lt, acc_id.to_hex_string(),
                hash_upd.old_hash.to_hex_string(), acc_state_hash.to_hex_string())
        }
        *prev_trans_lt_len = lt_len;
        *prev_trans_lt = trans_lt;
        *prev_trans_hash = trans_root.repr_hash();
        *acc_state_hash = hash_upd.new_hash;
        let mut c = 0;
        // trans.out_msgs.iterate_slices_with_keys(|key, value| {
        trans.out_msgs.iterate_keys(|key: U15| {
            if c != key.0 {
                reject_query!("transaction {} of {} has invalid indices in the out_msg dictionary (keys 0 .. {} expected)", 
                trans_lt, acc_id.to_hex_string(), trans.msg_count() - 1)
            } else {
                c += 1;
                Ok(true)
            }
        })?;
        return Ok(true)
    }

    // NB: could be run in parallel for different accounts
    fn precheck_one_account_block(base: &ValidateBase, acc_id: &UInt256, acc_blk: AccountBlock) -> Result<()> {
        let acc_id = AccountId::from(acc_id.clone());
        log::debug!(target: "validate_query", "pre-checking AccountBlock for {}", acc_id.to_hex_string());
        
        if !base.shard().contains_account(acc_id.clone())? {
            reject_query!("new block {} contains AccountBlock for account {} not belonging to the block's shard {}",
                base.block_id(), acc_id.to_hex_string(), base.shard())
        }
        // CHECK!(acc_blk_root.is_some());
        // acc_blk_root->print_rec(std::cerr);
        // block::gen::t_AccountBlock.print(std::cerr, acc_blk_root);
        let hash_upd = acc_blk.read_state_update()?;
        if acc_blk.account_id() != &acc_id {
            reject_query!("AccountBlock of account {} appears to belong to another account {}",
                acc_id.to_hex_string(), acc_blk.account_id().to_hex_string())
        }
        let old_state = base.prev_state_accounts.get_serialized(acc_id.clone())?.unwrap_or_default();
        let new_state = base.next_state_accounts.get_serialized(acc_id.clone())?;
        if hash_upd.old_hash != old_state.account_cell().repr_hash() {
            reject_query!("(HASH_UPDATE Account) from the AccountBlock of {} has incorrect old hash",
                acc_id.to_hex_string())
        }
        if hash_upd.new_hash != new_state.clone().unwrap_or_default().account_cell().repr_hash() {
            reject_query!("(HASH_UPDATE Account) from the AccountBlock of {} has incorrect new hash",
                acc_id.to_hex_string())
        }
        // acc_blk.transactions.count(1000000)?;
        // reject_query!("AccountBlock of "s + acc_id.to_hex_string() + " failed to pass letmated validity checks");
        // reject_query!("AccountBlock of "s + acc_id.to_hex_string() + " failed to pass hand-written validity checks");
        let min_trans = acc_blk.transactions().get_min(false)?;
        let max_trans = acc_blk.transactions().get_max(false)?;
        let (min_trans, max_trans) = match (min_trans, max_trans) {
            (Some(min), Some(max)) => (min.0, max.0),
            _ => reject_query!("cannot extract minimal and maximal keys from the transaction dictionary of account {}",
                acc_id.to_hex_string())
        };
        if min_trans <= base.info.start_lt() || max_trans >= base.info.end_lt() {
            reject_query!("new block contains transactions {} .. {} outside of the block's lt range {} .. {}",
                min_trans, max_trans, base.info.start_lt(), base.info.end_lt())
        }
        let mut last_trans_lt_len = 1;
        let mut last_trans_lt = old_state.last_trans_lt();
        let mut last_trans_hash = old_state.last_trans_hash().clone();
        let mut acc_state_hash = hash_upd.old_hash;
        acc_blk.transactions().iterate_slices(|key, trans_slice|
            key.clone().get_next_u64().and_then(|key| Self::precheck_one_transaction(base,
                &acc_id, key, trans_slice.reference(0)?, &mut last_trans_lt, &mut last_trans_hash,
                &mut last_trans_lt_len, &mut acc_state_hash
            )).map_err(|err| error!("transaction {:x} of account {:x} is invalid : {}", key, acc_id, err))
        )?;
        if let Some(new_state) = new_state {
            if last_trans_lt != new_state.last_trans_lt() || &last_trans_hash != new_state.last_trans_hash() {
                reject_query!("last transaction mismatch for account {} : block lists {}:{} but \
                    the new state claims that it is {}:{}", acc_id.to_hex_string(),
                        last_trans_lt, last_trans_hash.to_hex_string(),
                        new_state.last_trans_lt(), new_state.last_trans_hash().to_hex_string())
            }
        }
        if acc_state_hash != hash_upd.new_hash {
            reject_query!("final state hash mismatch in (HASH_UPDATE Account) for account {}", acc_id.to_hex_string())
        }
        Ok(())
    }

    fn precheck_account_transactions(base: Arc<ValidateBase>, tasks: &mut Vec<Box<dyn FnOnce() -> Result<()> + Send + 'static>>) -> Result<()> {
        log::debug!(target: "validate_query", "pre-checking all AccountBlocks, and all transactions of all accounts");
        base.account_blocks.iterate_with_keys(|key, acc_blk| {
            let base = base.clone();
            Self::add_task(tasks, move ||
                Self::precheck_one_account_block(&base, &key, acc_blk).map_err(|err|
                    error!("invalid AccountBlock for account {} in the new block {} : {}",
                        key.to_hex_string(), base.block_id(), err))
            );
            Ok(true)
        })?;
        Ok(())
    }

    fn lookup_transaction(base: &ValidateBase, addr: &AccountId, lt: u64) -> Result<Option<Cell>> {
        match base.account_blocks.get_serialized(addr.clone())? {
            Some(block) => block.transactions().get_as_cell(&lt),
            None => Ok(None)
        }
    }

    // checks that a ^Transaction refers to a transaction present in the ShardAccountBlocks
    fn is_valid_transaction_ref(base: &ValidateBase, transaction: &Transaction, hash: UInt256) -> Result<()> {
        match Self::lookup_transaction(base, &transaction.account_id(), transaction.logical_time())? {
            Some(trans_cell) => if trans_cell != hash {
                reject_query!("transaction {} of {:x} has a different hash", transaction.logical_time(), transaction.account_id())
            } else {
                Ok(())
            }
            None => reject_query!("transaction {} of {:x} not found", transaction.logical_time(), transaction.account_id())
        }
    }

    // checks that any change in OutMsgQueue in the state is accompanied by an OutMsgDescr record in the block
    // also checks that the keys are correct
    fn precheck_one_message_queue_update(
        base: &ValidateBase,
        out_msg_id: &OutMsgQueueKey,
        old_value: Option<(EnqueuedMsg, u64)>,
        new_value: Option<(EnqueuedMsg, u64)>
    ) -> Result<()> {
        log::debug!(target: "validate_query", "checking update of enqueued outbound message {:x}", out_msg_id);
        CHECK!(old_value.is_some() || new_value.is_some());
        let m_str = if new_value.is_some() && old_value.is_some() {
            reject_query!("EnqueuedMsg with key {:x} has been changed in the OutMsgQueue, \
                but the key did not change", out_msg_id)
        } else if new_value.is_some() {
            "en"
        } else if old_value.is_some() {
            "de"
        } else {
            ""
        };
        let out_msg = base.out_msg_descr.get(&out_msg_id.hash)?
            .ok_or_else(|| error!( "no OutMsgDescr corresponding to {}queued message with key {:x}",
                m_str, out_msg_id))?;
        let correct = match out_msg {
            OutMsg::New(_) | OutMsg::Transit(_) => new_value.is_some(),
            OutMsg::Dequeue(_) | OutMsg::DequeueImmediate(_) | OutMsg::DequeueShort(_) => old_value.is_some(),
            OutMsg::TransitRequeued(_) => true,
            _ => false
        };
        if !correct {
            reject_query!("OutMsgDescr corresponding to {} queued message with key {:x} has invalid tag ${:3x}",
                m_str, out_msg_id, out_msg.tag())
        }
        let enq;
        if let Some((_enq, _lt)) = old_value {
            enq = MsgEnqueueStuff::from_enqueue_and_lt(_enq, _lt)?;
            if enq.enqueued_lt() >= base.info.start_lt() {
                reject_query!("new EnqueuedMsg with key {:x} has enqueued_lt={} greater \
                    than or equal to this block's start_lt={}", out_msg_id,
                        enq.enqueued_lt(), base.info.start_lt())
            }
            // dequeued message
            if let OutMsg::TransitRequeued(info) = out_msg {
                // this is a msg_export_tr_req$111, a re-queued transit message (after merge)
                // check that q_msg_env still contains msg
                let q_msg = info.out_message_cell();
                if info.out_message_cell().repr_hash() != out_msg_id.hash {
                    reject_query!("MsgEnvelope in the old outbound queue with key {:x} \
                        contains a Message with incorrect hash {}", out_msg_id, q_msg.repr_hash().to_hex_string())
                }
                // must be msg_import_tr$100
                let in_msg = info.read_imported()?;
                match in_msg {
                    InMsg::Immediate(info) => {
                        if info.envelope_message_hash() != enq.envelope_hash() {
                            reject_query!("OutMsgDescr corresponding to dequeued message with key {:x} \
                                is a msg_export_tr_req referring to a reimport InMsgDescr that contains a MsgEnvelope \
                                distinct from that originally kept in the old queue", out_msg_id)
                        }
                    }
                    _ => reject_query!("OutMsgDescr for {:x} refers to a reimport InMsgDescr with invalid tag ${:3x} \
                        instead of msg_import_tr$100", out_msg_id, in_msg.tag())
                }
            } else if out_msg.envelope_message_hash() != Some(enq.envelope_hash()) {
                reject_query!("OutMsgDescr corresponding to dequeued message with key {:x} contains a \
                    MsgEnvelope distinct from that originally kept in the old queue", out_msg_id)
            }
        } else if let Some((_enq, _lt)) = new_value {
            enq = MsgEnqueueStuff::from_enqueue_and_lt(_enq, _lt)?;
            if enq.enqueued_lt() < base.info.start_lt() || enq.enqueued_lt() >= base.info.end_lt() {
                reject_query!("new EnqueuedMsg with key {:x} has enqueued_lt={} outside of \
                    this block's range {} .. {}", out_msg_id,
                    enq.enqueued_lt(), base.info.start_lt(), base.info.end_lt())
            }
            if out_msg.envelope_message_hash() != Some(enq.envelope_hash()) {
                reject_query!("OutMsgDescr corresponding to enqueued message with key {:x} \
                    contains a MsgEnvelope distinct from that stored in the new queue", out_msg_id)
            }
        } else {
            log::error!(target: "validate_query", "EnqueuedMsg with key {:x} has been not changed in the OutMsgQueue", out_msg_id);
            return Ok(())
        };
        // let msg_env = enq.env;
        if enq.message_hash() != out_msg_id.hash {
            reject_query!("OutMsgDescr for {:x} contains a message with different hash {:x}",
                out_msg_id.hash, enq.message_hash())
        }
        // in all cases above, we have to check that all 352-bit key is correct (including first 96 bits)
        // otherwise we might not be able to correctly recover OutMsgQueue entries starting from OutMsgDescr later
        // or we might have several OutMsgQueue entries with different 352-bit keys all having the same last 256 bits (with the message hash)
        let new_key = enq.out_msg_key();
        if &new_key != out_msg_id {
            reject_query!("OutMsgDescr for {:x} contains a MsgEnvelope that should be stored under different key {:x}",
            out_msg_id, new_key)
        }
        Ok(())
    }

    fn precheck_message_queue_update(base: Arc<ValidateBase>, manager: &MsgQueueManager, tasks: &mut Vec<Box<dyn FnOnce() -> Result<()> + Send + 'static>>) -> Result<()> {
        log::debug!(target: "validate_query", "pre-checking the difference between the \
            old and the new outbound message queues");
        manager.prev().out_queue().scan_diff_with_aug(
            manager.next().out_queue(),
            |key, val1, val2| {
                let base = base.clone();
                Self::add_task(tasks, move || Self::precheck_one_message_queue_update(&base, &key, val1, val2));
                Ok(true)
            }
        )?;
        Ok(())
    }

    fn update_max_processed_lt_hash(base: &ValidateBase, lt: u64, hash: &UInt256) -> Result<bool> {
        add_unbound_object_to_map_with_update(
            &base.result.lt_hash,
            0,
            |lt_hash| {
                if let Some((proc_lt, proc_hash)) = lt_hash {
                    if !(proc_lt < &lt || (&lt == proc_lt && proc_hash < hash)) {
                        return Ok(None)
                    }
                }
                Ok(Some((lt, hash.clone())))
            }
        )
    }

    fn update_min_enqueued_lt_hash(base: &ValidateBase, lt: u64, hash: &UInt256) -> Result<bool> {
        add_unbound_object_to_map_with_update(
            &base.result.lt_hash,
            1,
            |lt_hash| {
                if let Some((min_enq_lt, min_enq_hash)) = lt_hash {
                    if !(&lt < min_enq_lt || (&lt == min_enq_lt && hash < min_enq_hash)) {
                        return Ok(None)
                    }
                }
                Ok(Some((lt, hash.clone())))
            }
        )
    }

    // check that the enveloped message (MsgEnvelope) was present in the output queue of a neighbor, and that it has not been processed before
    fn check_imported_message(base: &ValidateBase, manager: &MsgQueueManager, env: &MsgEnvelope, env_hash: &UInt256, created_lt: u64) -> Result<()> {
        let (cur_prefix, next_prefix) = env.calc_cur_next_prefix()?;
        if !base.shard().contains_full_prefix(&next_prefix) {
            reject_query!("imported message with hash {} has next hop address {}... not in this shard",
                env_hash.to_hex_string(), next_prefix)
        }
        let key = OutMsgQueueKey::with_account_prefix(&next_prefix, env.message_cell().repr_hash());
        if let (Some(block_id), enq) = manager.find_message(&key, &cur_prefix)? {
            let enq_msg_descr = enq.ok_or_else(|| error!("imported internal message with hash {:x}\
                and previous address {}..., next hop address {} could not be found in \
                the outbound message queue of neighbor {} under key {:x}",
                    env_hash, cur_prefix, next_prefix, block_id, key))?;
            if &enq_msg_descr.envelope_hash() != env_hash {
                reject_query!("imported internal message from the outbound message queue of \
                    neighbor {} under key {:x} has a different MsgEnvelope in that outbound message queue",
                        block_id, key)
            }
            if manager.prev().already_processed(&enq_msg_descr)? {
                reject_query!("imported internal message with hash {} and lt={} has been already imported \
                    by a previous block of this shardchain", env_hash.to_hex_string(), created_lt)
            }
            Self::update_max_processed_lt_hash(base, created_lt, &key.hash)?;
            return Ok(())
        } else {
            reject_query!("imported internal message with hash {} and \
                previous address {}..., next hop address {} has previous address not belonging to any neighbor",
                    env_hash.to_hex_string(), cur_prefix, next_prefix)
        }
    }

    fn check_in_msg(base: &ValidateBase, manager: &MsgQueueManager, key: &UInt256, in_msg: &InMsg) -> Result<()> {
        log::debug!(target: "validate_query", "checking InMsg with key {}", key.to_hex_string());
        CHECK!(in_msg, inited);
        // initial checks and unpack
        let msg_hash = in_msg.message_cell()?.repr_hash();
        if &msg_hash != key {
            reject_query!("InMsg with key {} refers to a message with different hash {}",
                key.to_hex_string(), msg_hash.to_hex_string())
        }
        let trans_cell = in_msg.transaction_cell();
        let msg_env = in_msg.in_msg_envelope_cell();
        let msg_env_hash = msg_env.clone().map(|cell| cell.repr_hash()).unwrap_or_default();
        let env = in_msg.read_in_msg_envelope()?.unwrap_or_default();
        let msg = in_msg.read_message()?;
        let created_lt = msg.lt().unwrap_or_default();
        // dbg!(&in_msg, &env, &msg);
        if let Some(trans_cell) = trans_cell.clone() {
            let transaction = Transaction::construct_from_cell(trans_cell.clone())?;
            // check that the transaction reference is valid, and that it points to a Transaction which indeed processes this input message
            Self::is_valid_transaction_ref(base, &transaction, trans_cell.repr_hash())
                .map_err(|err| error!("InMsg corresponding to inbound message with key {} contains an invalid \
                    Transaction reference (transaction not in the block's transaction list) : {}",
                        key.to_hex_string(), err))?;
            if !transaction.in_msg_cell().map(|cell| cell.repr_hash() == msg_hash).unwrap_or_default() {
                reject_query!("InMsg corresponding to inbound message with key {} \
                    refers to transaction that does not process this inbound message", key.to_hex_string())
            }
            let (_workchain_id, addr) = msg.dst_ref().ok_or_else(|| error!("No dest address"))
                .and_then(|addr| addr.extract_std_address(true))?;
            if &addr != transaction.account_id() {
                reject_query!("InMsg corresponding to inbound message with hash {} and destination address {} \
                   claims that the message is processed by transaction {} of another account {}",
                        key.to_hex_string(), addr.to_hex_string(), transaction.logical_time(), transaction.account_id().to_hex_string())
            }
        }
        let fwd_fee = match in_msg {
            // msg_import_ext$000 msg:^(Message Any) transaction:^Transaction
            // importing an inbound external message
            InMsg::External(_) => {
                let dst = msg.dst_ref().ok_or_else(|| error!("destination of inbound external message with hash {:x} \
                    is an invalid blockchain address", key))?;
                let dest_prefix = AccountIdPrefixFull::prefix(dst)?;
                if !dest_prefix.is_valid() {
                    reject_query!("destination of inbound external message with hash {:x} \
                        is an invalid blockchain address", key)
                }
                if !base.shard().contains_full_prefix(&dest_prefix) {
                    reject_query!("inbound external message with hash {} has destination address \
                        {}... not in this shard", key.to_hex_string(), dest_prefix)
                }
                if dst.extract_std_address(true).is_err()  {
                    reject_query!("cannot unpack destination address of inbound external message with hash {}", key.to_hex_string())
                }
                return Ok(()) // nothing to check more for external messages
            }
            // msg_import_imm$011 in_msg:^MsgEnvelope transaction:^Transaction fwd_fee:Grams
            // importing and processing an internal message generated in this very block
            InMsg::Immediate(ref info) => {
                if !base.is_special_in_msg(&in_msg) {
                    Self::update_max_processed_lt_hash(base, created_lt, key)?;
                }
                info.fwd_fee.clone()
            }
            // msg_import_fin$100 in_msg:^MsgEnvelope transaction:^Transaction fwd_fee:Grams
            // importing and processing an internal message with destination in this shard
            InMsg::Final(ref info) => info.fwd_fee.clone(),
            // msg_import_tr$101 in_msg:^MsgEnvelope out_msg:^MsgEnvelope transit_fee:Grams
            // importing and relaying a (transit) internal message with destination outside this shard
            InMsg::Transit(ref info) => info.transit_fee.clone(),
            // msg_import_ihr$010 msg:^(Message Any) transaction:^Transaction ihr_fee:Grams proof_created:^Cell
            InMsg::IHR(_) => reject_query!("InMsg with key {} \
                is a msg_import_ihr, but IHR messages are not enabled in this version", key.to_hex_string()),
            // msg_discard_tr$111 in_msg:^MsgEnvelope transaction_id:uint64 fwd_fee:Grams proof_delivered:^Cell
            InMsg::DiscardedTransit(_) => reject_query!("InMsg with key {} \
                is a msg_discard_tr, but IHR messages are not enabled in this version", key.to_hex_string()),
            // msg_discard_fin$110 in_msg:^MsgEnvelope transaction_id:uint64 fwd_fee:Grams
            InMsg::DiscardedFinal(_) => reject_query!("InMsg with key {} \
                is a msg_discard_fin, but IHR messages are not enabled in this version", key.to_hex_string()),
            _ => reject_query!("InMsg with key {} has impossible tag", key.to_hex_string())
        };
        // common checks for all (non-external) inbound messages
        // CHECK!(msg.is_some());
        // unpack int_msg_info$0 ... = CommonMsgInfo, especially message addresses
        let header = msg.int_header().ok_or_else(|| error!("InMsg with key {} is not a msg_import_ext$000, \
            but it does not refer to an inbound internal message", key.to_hex_string()))?;
        // extract source, current, next hop and destination address prefixes
        let dest_prefix = msg.dst_ref()
            .ok_or_else(|| error!(""))
            .and_then(|address| AccountIdPrefixFull::checked_prefix(address))
            .map_err(|_| error!("destination of inbound \
                internal message with hash {:x} is an invalid blockchain address", key))?;
        let src_prefix = msg.src_ref()
            .ok_or_else(|| error!(""))
            .and_then(|address| AccountIdPrefixFull::checked_prefix(address))
            .map_err(|_| error!("source of inbound \
                internal message with hash {:x} is an invalid blockchain address", key))?;
        let cur_prefix  = src_prefix.interpolate_addr_intermediate(&dest_prefix, &env.cur_addr())?;
        let next_prefix = src_prefix.interpolate_addr_intermediate(&dest_prefix, &env.next_addr())?;
        if !(cur_prefix.is_valid() && next_prefix.is_valid()) {
            reject_query!("cannot compute current and next hop addresses of inbound internal message with hash {}",
                key.to_hex_string())
        }
        // check that next hop is nearer to the destination than the current address
        if count_matching_bits(&dest_prefix, &next_prefix) < count_matching_bits(&dest_prefix, &cur_prefix) {
            reject_query!("next hop address {}... of inbound internal message with hash {} \
                is further from its destination {}... than its current address {}...",
                    next_prefix, key.to_hex_string(), dest_prefix, cur_prefix)
        }
        // next hop address must belong to this shard (otherwise we should never had imported this message)
        if !base.shard().contains_full_prefix(&next_prefix) {
            reject_query!("next hop address {}... of inbound internal message with hash {} does not belong \
                to the current block's shard {}", next_prefix, key.to_hex_string(), base.shard())
        }
        // next hop may coincide with current address only if destination is already reached
        if next_prefix == cur_prefix && cur_prefix != dest_prefix {
            reject_query!("next hop address {}... of inbound internal message with hash {} \
                coincides with its current address, but this message has not reached its final destination {}... yet",
                    next_prefix, key.to_hex_string(), dest_prefix)
        }
        // if a message is processed by a transaction, it must have destination inside the current shard
        if trans_cell.is_some() && !base.shard().contains_full_prefix(&dest_prefix) {
            reject_query!("inbound internal message with hash {} has destination address {}... not in this shard, \
                but it is processed nonetheless", key.to_hex_string(), dest_prefix)
        }
        // if a message is not processed by a transaction, its final destination must be outside this shard
        if trans_cell.is_none() && base.shard().contains_full_prefix(&dest_prefix) {
            reject_query!("inbound internal message with hash {} has destination address {}... in this shard, \
                but it is not processed by a transaction", key.to_hex_string(), dest_prefix)
        }
        // it already checked in AccountIdPrefixFull::from_address
        // unpack complete destination address if it is inside this shard
        // it already checked in AccountIdPrefixFull::from_address
        // if trans_cell.is_some() && msg.dst_ref().extract_std_address(true).is_err() {
        //     reject_query!("cannot unpack destination address of inbound internal message with hash {}", key.to_hex_string())
        // }
        // unpack original forwarding fee
        let orig_fwd_fee = &header.fwd_fee;
        // CHECK!(orig_fwd_fee.is_some());
        if env.fwd_fee_remaining() > orig_fwd_fee {
            reject_query!("inbound internal message with hash {} has remaining forwarding fee {} \
                larger than the original (total) forwarding fee {}", key.to_hex_string(), env.fwd_fee_remaining(), orig_fwd_fee)
        }
        let out_msg_opt = base.out_msg_descr.get(&key)?;
        let (out_msg_env, reimport) = match out_msg_opt.as_ref() {
            Some(out_msg) => (
                out_msg.out_message_cell(),
                out_msg.read_reimport_message()?
            ),
            None => (None, None)
        };

        // continue checking inbound message
        match in_msg {
            InMsg::Immediate(_) => {
                // msg_import_imm$011 in_msg:^MsgEnvelope transaction:^Transaction fwd_fee:Grams
                // importing and processing an internal message generated in this very block
                if cur_prefix != dest_prefix {
                    reject_query!("inbound internal message with hash {:x} is a msg_import_imm$011, \
                        but its current address {} is somehow distinct from its final destination {}",
                            key, cur_prefix, dest_prefix)
                }
                CHECK!(trans_cell.is_some());
                // check that the message has been created in this very block
                if !base.shard().contains_full_prefix(&src_prefix) {
                    reject_query!("inbound internal message with hash {} is a msg_import_imm$011, \
                        but its source address {} does not belong to this shard", key.to_hex_string(), src_prefix)
                }
                if let Some(OutMsg::Immediate(_)) = out_msg_opt.as_ref() {
                    CHECK!(out_msg_env.is_some());
                    CHECK!(reimport.is_some());
                } else if !base.is_special_in_msg(&in_msg) {
                    reject_query!("inbound internal message with hash {} is a msg_import_imm$011, \
                        but the corresponding OutMsg does not exist, or is not a valid msg_export_imm$010",
                            key.to_hex_string())
                }
                // fwd_fee must be equal to the fwd_fee_remaining of this MsgEnvelope
                if &fwd_fee != env.fwd_fee_remaining() {
                    reject_query!("msg_import_imm$011 InMsg with hash {} is invalid because its collected fwd_fee={} \
                        is not equal to fwd_fee_remaining={} of this message (envelope)",
                            key.to_hex_string(), fwd_fee, env.fwd_fee_remaining())
                }
                // ...
            }
            InMsg::Final(info) => {
                // msg_import_fin$100 in_msg:^MsgEnvelope transaction:^Transaction fwd_fee:Grams
                // importing and processing an internal message with destination in this shard
                CHECK!(trans_cell.is_some());
                CHECK!(base.shard().contains_full_prefix(&next_prefix));
                if base.shard().contains_full_prefix(&cur_prefix) {
                    // we imported this message from our shard!
                    if let Some(OutMsg::DequeueImmediate(_)) = out_msg_opt.as_ref() {
                        CHECK!(out_msg_env.is_some());
                        CHECK!(reimport.is_some());
                    } else {
                        reject_query!("inbound internal message with hash {} is a msg_import_fin$100 \
                            with current address {}... already in our shard, but the corresponding OutMsg \
                            does not exist, or is not a valid msg_export_deq_imm$100",
                                key.to_hex_string(), cur_prefix)
                    }
                } else {
                    CHECK!(cur_prefix != next_prefix);
                    // check that the message was present in the output queue of a neighbor, and that it has not been processed before
                    Self::check_imported_message(base, manager, &env, &info.envelope_message_hash(), created_lt)?;
                }
                // ...
                // fwd_fee must be equal to the fwd_fee_remaining of this MsgEnvelope
                if &fwd_fee != env.fwd_fee_remaining() {
                    reject_query!("msg_import_fin$100 InMsg with hash {} is invalid because \
                        its collected fwd_fee={} is not equal to fwd_fee_remaining={} of \
                        this message (envelope)", key.to_hex_string(), fwd_fee, env.fwd_fee_remaining())
                }
                // ...
            }
            InMsg::Transit(info) => {
                // msg_import_tr$101 in_msg:^MsgEnvelope out_msg:^MsgEnvelope transit_fee:Grams
                // importing and relaying a (transit) internal message with destination outside this shard
                if cur_prefix == dest_prefix {
                    reject_query!("inbound internal message with hash {} is a msg_import_tr$101 (a transit message), \
                        but its current address {}... is already equal to its final destination",
                            key.to_hex_string(), cur_prefix)
                }
                CHECK!(trans_cell.is_none());
                CHECK!(cur_prefix != next_prefix);
                let out_msg = out_msg_opt.as_ref().ok_or_else(|| error!("inbound internal message with hash {} \
                    is a msg_import_tr$101 (transit message), but the corresponding OutMsg does not exist",
                        key.to_hex_string()))?;
                let tr_req = if base.shard().contains_full_prefix(&cur_prefix) {
                    // we imported this message from our shard!
                    // (very rare situation possible only after merge)
                    match out_msg {
                        OutMsg::TransitRequeued(_) => {
                            CHECK!(out_msg_env.is_some());
                            CHECK!(reimport.is_some());
                        }
                        _ => reject_query!("inbound internal message with hash {} is a msg_import_tr$101 \
                            (transit message) with current address {}... already in our shard, but the \
                            corresponding OutMsg is not a valid msg_export_tr_req$111",
                                key.to_hex_string(), cur_prefix)
                    }
                    "requeued"
                } else {
                    match out_msg {
                        OutMsg::Transit(_) => {
                            CHECK!(out_msg_env.is_some());
                            CHECK!(reimport.is_some());
                            // check that the message was present in the output queue of a neighbor, and that it has not been processed before
                            Self::check_imported_message(base, manager, &env, &msg_env_hash, created_lt)?;
                        }
                        _ => reject_query!("inbound internal message with hash {} is a msg_import_tr$101 \
                            (transit message) with current address {}... outside of our shard, but the \
                            corresponding OutMsg is not a valid msg_export_tr$011",
                                key.to_hex_string(), cur_prefix)
                    }
                    "usual"
                };
                // perform hypercube routing for this transit message
                let use_hypercube = !base.config_params.has_capability(GlobalCapabilities::CapOffHypercube);
                let route_info = perform_hypercube_routing(&next_prefix, &dest_prefix, &base.shard(), use_hypercube)
                    .map_err(|err| error!("cannot perform (check) hypercube routing for \
                        transit inbound message with hash {}: src={} cur={} next={} dest={}; \
                        our shard is {} : {}", key.to_hex_string(), src_prefix, cur_prefix,
                            next_prefix, dest_prefix, base.shard(), err))?;
                let new_cur_prefix  = next_prefix.interpolate_addr_intermediate(&dest_prefix, &route_info.0)?;
                let new_next_prefix = next_prefix.interpolate_addr_intermediate(&dest_prefix, &route_info.1)?;
                // unpack out_msg:^MsgEnvelope from msg_import_tr
                let tr_env = info.read_out_message().map_err(|err| error!("InMsg for transit message with hash {} \
                    refers to an invalid rewritten message envelope : {}", key.to_hex_string(), err))?;
                // the rewritten transit message envelope must contain the same message
                if &tr_env.message_cell().repr_hash() != key {
                    reject_query!("InMsg for transit message with hash {} refers to a rewritten message \
                        envelope containing another message", key.to_hex_string())
                }
                // check that the message has been routed according to hypercube routing
                let tr_cur_prefix  = src_prefix.interpolate_addr_intermediate(&dest_prefix, &tr_env.cur_addr())?;
                let tr_next_prefix = src_prefix.interpolate_addr_intermediate(&dest_prefix, &tr_env.next_addr())?;
                if tr_cur_prefix != new_cur_prefix || tr_next_prefix != new_next_prefix {
                    reject_query!("InMsg for transit message with hash {} tells us that it has been adjusted to \
                        current address {}... and hext hop address {} while the correct values dictated by hypercube \
                        routing are {}... and {}...", key.to_hex_string(), tr_cur_prefix,
                            tr_next_prefix, new_cur_prefix, new_next_prefix)
                }
                // check that the collected transit fee with new fwd_fee_remaining equal the original fwd_fee_remaining
                // (correctness of fwd_fee itself will be checked later)
                let mut fee = tr_env.fwd_fee_remaining().clone();
                fee.add(&fwd_fee)?;
                if tr_env.fwd_fee_remaining() > orig_fwd_fee || &fee != env.fwd_fee_remaining() {
                    reject_query!("InMsg for transit message with hash {} declares transit fees of {}, \
                        but fwd_fees_remaining has decreased from {} to {} in transit",
                            key.to_hex_string(), fwd_fee, env.fwd_fee_remaining(), tr_env.fwd_fee_remaining())
                }
                if Some(info.out_envelope_message_cell()) != out_msg_env {
                    reject_query!("InMsg for transit message with hash {} contains rewritten MsgEnvelope \
                        different from that stored in corresponding OutMsgDescr ({} transit)", key.to_hex_string(), tr_req)
                }
                // check the amount of the transit fee
                let transit_fee = base.config_params.fwd_prices(false)?.next_fee_checked(env.fwd_fee_remaining())?;
                if transit_fee != fwd_fee {
                    reject_query!("InMsg for transit message with hash {} declared collected transit fees \
                        to be {} (deducted from the remaining forwarding fees of {}), but \
                        we have computed another value of transit fees {}",
                            key.to_hex_string(), fwd_fee, env.fwd_fee_remaining(), transit_fee)
                }
            }
            in_msg => reject_query!("Forbiden InMsg type : {:?}", in_msg)
        }

        if let Some(reimport) = reimport {
            // transit message: msg_export_tr + msg_import_tr
            // or message re-imported from this very shard
            // either msg_export_imm + msg_import_imm
            // or msg_export_deq_imm + msg_import_fin
            // or msg_export_tr_req + msg_import_tr (rarely, only after merge)
            // must have a corresponding OutMsg record
            if in_msg != &reimport {
                reject_query!("OutMsg corresponding to reimport InMsg with hash {} \
                    refers to a different reimport InMsg", key.to_hex_string())
            }
            // for transit messages, OutMsg refers to the newly-created outbound messages (not to the re-imported old outbound message)
            match in_msg {
                InMsg::Transit(_) => (),
                _ => if out_msg_env != msg_env {
                    reject_query!("InMsg with hash {} is a reimport record, but the \
                        corresponding OutMsg exports a MsgEnvelope with a different hash", key.to_hex_string())
                }
            }
        }
        Ok(())
    }

    fn check_in_msg_descr(base: Arc<ValidateBase>, manager: Arc<MsgQueueManager>, tasks: &mut Vec<Box<dyn FnOnce() -> Result<()> + Send + 'static>>) -> Result<()> {
        log::debug!(target: "validate_query", "checking inbound messages listed in InMsgDescr");
        base.in_msg_descr.iterate_with_keys(|key, in_msg| {
            let base = base.clone();
            let manager = manager.clone();
            Self::add_task(tasks, move || {
                Self::check_in_msg(&base, &manager, &key, &in_msg)
                    .map_err(|err| error!("invalid InMsg with key (message hash) {} in the new block {} : {}",
                        key.to_hex_string(), base.block_id(), err))
            });
            Ok(true)
        }).map_err(|err| error!("invalid InMsgDescr dictionary in the new block {} : {}", base.block_id(), err))?;
        Ok(())
    }

    fn check_reimport(base: &ValidateBase, out_msg: &OutMsg, in_msg_key: &UInt256) -> Result<()> {
        if let Some(reimport_cell) = out_msg.reimport_cell() {
            // transit message: msg_export_tr + msg_import_tr
            // or message re-imported from this very shard
            // either msg_export_imm + msg_import_imm
            // or msg_export_deq_imm + msg_import_fin (rarely)
            // or msg_export_tr_req + msg_import_tr (rarely)
            // (the last two cases possible only after merge)
            //
            // check that reimport is a valid InMsg registered in InMsgDescr
            let in_msg_slice = base.in_msg_descr.get_as_slice(in_msg_key)?
                .ok_or_else(|| error!("OutMsg with key {} refers to a (re)import InMsg, \
                    but there is no InMsg with such a key", in_msg_key.to_hex_string()))?;
            if in_msg_slice != SliceData::from(reimport_cell) {
                reject_query!("OutMsg with key {} refers to a (re)import InMsg, \
                    but the actual InMsg with this key is different from the one referred to",
                        in_msg_key.to_hex_string())
            }
        }
        Ok(())
    }

    // key of Message
    fn check_out_msg(base: &ValidateBase, manager: &MsgQueueManager, key: &UInt256, out_msg: &OutMsg) -> Result<()> {
        log::debug!(target: "validate_query", "checking OutMsg with key {}", key.to_hex_string());
        CHECK!(out_msg, inited);
        // initial checks and unpack
        let msg_cell_opt = out_msg.message_cell()?;
        let msg = match msg_cell_opt.as_ref() {
            Some(msg_cell) => {
                if &msg_cell.repr_hash() != key {
                    reject_query!("OutMsg with key {} refers to a message with different hash {}",
                        key.to_hex_string(), msg_cell.repr_hash().to_hex_string())
                }
                Message::construct_from_cell(msg_cell.clone())?
            }
            None => Default::default()
        };
        let src = msg.src_ref().cloned().unwrap_or_default();
        let msg_env_hash = out_msg.envelope_message_hash().unwrap_or_default();

        let trans_cell = out_msg.transaction_cell();
        if let Some(trans_cell) = trans_cell.clone() {
            let transaction = Transaction::construct_from_cell(trans_cell.clone())?;
            // check that the transaction reference is valid, and that it points to a Transaction which indeed creates this outbound internal message
            Self::is_valid_transaction_ref(base, &transaction, trans_cell.repr_hash())
                .map_err(|err| error!("OutMsg corresponding to outbound message with key {} \
                        contains an invalid Transaction reference (transaction not in the \
                        block's transaction list : {})", key.to_hex_string(), err))?;
            if !transaction.contains_out_msg(&msg, key) {
                reject_query!("OutMsg corresponding to outbound message with key {} \
                    refers to transaction that does not create this outbound message", key.to_hex_string())
            }
            let addr = src.extract_std_address(true)?.1;
            if &addr != transaction.account_id() {
                reject_query!("OutMsg corresponding to outbound message with hash {} and source address {} \
                    claims that the message was created by transaction {}  of another account {}",
                        key.to_hex_string(), addr.to_hex_string(),
                        transaction.logical_time(), transaction.account_id().to_hex_string())
            }
            // log::debug!(target: "validate_query", "OutMsg " << key.to_hex_string() + " is indeed a valid outbound message of transaction " << trans_lt
            //           of " << trans_addr.to_hex_string();
        }

        let (add, remove) = match out_msg {
            // msg_export_ext$000 msg:^(Message Any) transaction:^Transaction = OutMsg;
            // exporting an outbound external message
            OutMsg::External(_) => {
                let src_prefix = match msg.src_ref() {
                    Some(src) => AccountIdPrefixFull::prefix(&src)?,
                    None => reject_query!("source of outbound external message with hash {} \
                        is an invalid blockchain address", key.to_hex_string())
                };
                if !base.shard().contains_full_prefix(&src_prefix) {
                    reject_query!("outbound external message with hash {} has source address {}... not in this shard",
                        key.to_hex_string(), src_prefix)
                }
                return Ok(()) // nothing to check more for external messages
            }
            OutMsg::DequeueShort(_) => {
                return Ok(()) // nothing to check here for msg_export_deq_short ?
            }
            // msg_export_imm$010 out_msg:^MsgEnvelope transaction:^Transaction reimport:^InMsg = OutMsg;
            OutMsg::Immediate(_) => (false, false),
            // msg_export_new$001 out_msg:^MsgEnvelope transaction:^Transaction = OutMsg;
            // msg_export_tr$011 out_msg:^MsgEnvelope imported:^InMsg = OutMsg;
            OutMsg::New(_) | OutMsg::Transit(_) => (true, false),  // added to OutMsgQueue
            // msg_export_deq$1100 out_msg:^MsgEnvelope import_block_lt:uint63 = OutMsg;
            // msg_export_deq_short$1101 msg_env_hash:bits256 next_workchain:int32 next_addr_pfx:uint64 import_block_lt:uint64 = OutMsg;
            // msg_export_deq$110 out_msg:^MsgEnvelope import_block_lt:uint64 = OutMsg;
            OutMsg::Dequeue(_) | OutMsg::DequeueImmediate(_) => (false, true),  // removed from OutMsgQueue
            // msg_export_tr_req$111 out_msg:^MsgEnvelope imported:^InMsg = OutMsg;
            OutMsg::TransitRequeued(_) => (true, true), // removed from OutMsgQueue, and then added
            _ => reject_query!("OutMsg with key (message hash) {} has an unknown tag", key.to_hex_string())
        };
        // common checks for all (non-external) inbound messages
        // CHECK!(msg.is_some());
        // unpack int_msg_info$0 ... = CommonMsgInfo, especially message addresses
        let header = msg.int_header().ok_or_else(|| error!("OutMsg with key {} is not a msg_export_ext$000, \
            but it does not refer to an internal message", key.to_hex_string()))?;
        // extract source, current, next hop and destination address prefixes
        let src_prefix = AccountIdPrefixFull::checked_prefix(&src).map_err(|_| error!("source of outbound \
            internal message with hash {} is an invalid blockchain address", key.to_hex_string()))?;
        let dest_prefix = AccountIdPrefixFull::checked_prefix(&header.dst).map_err(|_| error!("destination of outbound \
            internal message with hash {} is an invalid blockchain address", key.to_hex_string()))?;
        let env = out_msg.read_out_message()?.unwrap_or_default();
        let cur_prefix  = src_prefix.interpolate_addr_intermediate(&dest_prefix, &env.cur_addr())?;
        let next_prefix = src_prefix.interpolate_addr_intermediate(&dest_prefix, &env.next_addr())?;
        if !(cur_prefix.is_valid() && next_prefix.is_valid()) {
            reject_query!("cannot compute current and next hop addresses of outbound internal message with hash {}",
                key.to_hex_string());
        }
        // check that next hop is nearer to the destination than the current address
        if count_matching_bits(&dest_prefix, &next_prefix) < count_matching_bits(&dest_prefix, &cur_prefix) {
            reject_query!("next hop address {}... of outbound internal message with hash {} \
                is further from its destination {}... than its current address {}...",
                    next_prefix, key.to_hex_string(), dest_prefix, cur_prefix)
        }
        // current address must belong to this shard (otherwise we should never had exported this message)
        if !base.shard().contains_full_prefix(&cur_prefix) {
            reject_query!("current address {}... of outbound internal message with hash {} \
                does not belong to the current block's shard {}",
                    cur_prefix, key.to_hex_string(), base.shard())
        }
        // next hop may coincide with current address only if destination is already reached
        if next_prefix == cur_prefix && cur_prefix != dest_prefix {
            reject_query!("next hop address {}... of outbound internal message with hash {} \
                coincides with its current address, but this message has not reached its final destination {} ... yet", 
                  next_prefix, key.to_hex_string(), dest_prefix)
        }
        // if a message is created by a transaction, it must have source inside the current shard
        if trans_cell.is_some() && !base.shard().contains_full_prefix(&src_prefix) {
            reject_query!("outbound internal message with hash {} has source address {}... not in this shard, \
                but it has been created here by a Transaction nonetheless",
                    key.to_hex_string(), src_prefix)
        }
        // unpack complete source address if it is inside this shard
        // it's already checked below
        // if transaction.is_some() && src.extract_std_address(true).is_err() {
        //     reject_query!("cannot unpack source address of outbound internal message with hash {} \
        //         created in this shard", key.to_hex_string())
        // }
        // unpack original forwarding fee
        if env.fwd_fee_remaining() > &header.fwd_fee {
            reject_query!("outbound internal message with hash {} has remaining forwarding fee {} \
                larger than the original (total) forwarding fee {}",
                    key.to_hex_string(), env.fwd_fee_remaining(), header.fwd_fee)
        }
        // check the OutMsgQueue update effected by this OutMsg
        let q_key = OutMsgQueueKey::with_account_prefix(&next_prefix, key.clone());
        let q_entry = manager.next().message(&q_key)?;
        let old_q_entry = manager.prev().message(&q_key)?;
        if old_q_entry.is_some() && q_entry.is_some() {
            reject_query!("OutMsg with key (message hash) {:x} should have removed or \
                added OutMsgQueue entry with key {:x}, but it is present both in the old \
                and in the new output queues", key, q_key)
        }
        if (add || remove) && old_q_entry.is_none() && q_entry.is_none() {
            reject_query!("OutMsg with key (message hash) {:x} should have removed or added \
                OutMsgQueue entry with key {:x}, but it is absent both from the old and from \
                the new output queues", key, q_key)
        }
        if (!add && !remove) && (old_q_entry.is_some() || q_entry.is_some()) {
            reject_query!("OutMsg with key (message hash) {:x} is a msg_export_imm$010, \
                so the OutMsgQueue entry with key {:x} should never be created, but it is present \
                in either the old or the new output queue", key, q_key)
        }
        // NB: if mode!=0, the OutMsgQueue entry has been changed, so we have already checked some conditions in precheck_one_message_queue_update()
        if add {
            match q_entry {
                Some(q_entry) => if q_entry.envelope_hash() != msg_env_hash {
                    reject_query!("OutMsg with key {:x} has created OutMsgQueue entry with key {:x} \
                        containing a different MsgEnvelope", key, q_key)
                }
                None => reject_query!("OutMsg with key {:x} was expected to create OutMsgQueue entry \
                    with key {:x} but it did not", key, q_key)
            }
            // ...
        } else if remove {
            match old_q_entry {
                Some(ref old_q_entry) => if old_q_entry.envelope_hash() != msg_env_hash {
                    reject_query!("OutMsg with key {:x} has dequeued OutMsgQueue entry \
                        with key {:x} containing a different MsgEnvelope", key, q_key)
                }
                None => reject_query!("OutMsg with key {:x} was expected to remove OutMsgQueue \
                    entry with key {:x} but it did not exist in the old queue", key, q_key)
            }
            // ...
        }

        Self::check_reimport(base, &out_msg, key)?;
        let reimport = out_msg.read_reimport_message()?;

        // ...
        match out_msg {
            // msg_export_imm
            OutMsg::Immediate(_) => match reimport {
                // msg_import_imm
                Some(InMsg::Immediate(ref info)) => {
                    if info.envelope_message_hash() != msg_env_hash {
                        reject_query!("msg_import_imm InMsg record corresponding to msg_export_imm OutMsg record with key {} \
                            re-imported a different MsgEnvelope", key.to_hex_string())
                    }
                    if !base.shard().contains_full_prefix(&dest_prefix) {
                        reject_query!("msg_export_imm OutMsg record with key {} refers to a message with destination {} \
                            outside this shard", key.to_hex_string(), dest_prefix)
                    }
                    if cur_prefix != dest_prefix || next_prefix != dest_prefix {
                        reject_query!("msg_export_imm OutMsg record with key {} refers to a message \
                            that has not been routed to its final destination", key.to_hex_string())
                    }
                }
                _ => reject_query!("cannot unpack msg_import_imm InMsg record corresponding to \
                    msg_export_imm OutMsg record with key {}", key.to_hex_string())
            // ...
            }
            // msg_export_new
            OutMsg::New(_) => {
                log::debug!(target: "validate_query", "src: {}, dst: {}, shard: {}", src_prefix, dest_prefix, base.shard());
                // perform hypercube routing for this new message
                let use_hypercube = !base.config_params.has_capability(GlobalCapabilities::CapOffHypercube);
                let route_info = perform_hypercube_routing(&src_prefix, &dest_prefix, &base.shard(), use_hypercube)
                    .map_err(|err| error!("cannot perform (check) hypercube routing for \
                        new outbound message with hash {} : {}", key.to_hex_string(), err))?;
                let new_cur_prefix  = src_prefix.interpolate_addr_intermediate(&dest_prefix, &route_info.0)?;
                let new_next_prefix = src_prefix.interpolate_addr_intermediate(&dest_prefix, &route_info.1)?;
                if cur_prefix != new_cur_prefix || next_prefix != new_next_prefix {
                    reject_query!("OutMsg for new message with hash {} \
                        tells us that it has been routed to current address {}... and hext hop address {} \
                        while the correct values dictated by hypercube routing are {}... and {}...",
                            key.to_hex_string(), cur_prefix, next_prefix, new_cur_prefix, new_next_prefix)
                }
                CHECK!(base.shard().contains_full_prefix(&src_prefix));
                if base.shard().contains_full_prefix(&dest_prefix) {
                    // log::debug!(target: "validate_query", "(THIS) src=" << src_prefix cur=" << cur_prefix next=" << next_prefix dest=" << dest_prefix route_info=(" << route_info.first << "," << route_info.second << ")";
                    CHECK!(cur_prefix == dest_prefix);
                    CHECK!(next_prefix == dest_prefix);
                    Self::update_min_enqueued_lt_hash(base, header.created_lt, key)?;
                } else {
                    // sanity check of the implementation of hypercube routing
                    // log::debug!(target: "validate_query", "(THAT) src=" << src_prefix cur=" << cur_prefix next=" << next_prefix dest=" << dest_prefix;
                    CHECK!(base.shard().contains_full_prefix(&cur_prefix));
                    CHECK!(!base.shard().contains_full_prefix(&next_prefix));
                }
            // ...
            }
            // msg_export_tr
            OutMsg::Transit(_) => match reimport {
                // msg_import_tr
                Some(InMsg::Transit(info)) => {
                    let in_env = info.read_in_message()?;
                    CHECK!(Some(in_env.message_cell().clone()), msg_cell_opt);
                    let in_env = info.read_in_message()?;
                    let in_cur_prefix  = src_prefix.interpolate_addr_intermediate(&dest_prefix, &in_env.cur_addr())?;
                    let in_next_prefix = src_prefix.interpolate_addr_intermediate(&dest_prefix, &in_env.next_addr())?;
                    if base.shard().contains_full_prefix(&in_cur_prefix) {
                        reject_query!("msg_export_tr OutMsg record with key {} corresponds to msg_import_tr InMsg record \
                            with current imported message address {} inside the current shard \
                            (msg_export_tr_req should have been used instead)", key.to_hex_string(), in_cur_prefix);
                    }
                    // we have already checked correctness of hypercube routing in InMsg::msg_import_tr case of check_in_msg()
                    CHECK!(base.shard().contains_full_prefix(&in_next_prefix));
                    CHECK!(base.shard().contains_full_prefix(&cur_prefix));
                    CHECK!(!base.shard().contains_full_prefix(&next_prefix));
                    // ...
                }
                _ => reject_query!("cannot unpack msg_import_tr InMsg record corresponding to \
                    msg_export_tr OutMsg record with key {}", key.to_hex_string())
            }
            // msg_export_deq
            OutMsg::Dequeue(ref info) => {
                // check that the message has been indeed processed by a neighbor
                let enq_msg_descr = old_q_entry.ok_or_else(|| error!("cannot unpack old OutMsgQueue entry corresponding to \
                    msg_export_deq OutMsg entry with key {}", key.to_hex_string()))?;
                let mut delivered = false;
                let mut deliver_lt = 0;
                for neighbor in manager.neighbors() {
                    // could look up neighbor with shard containing enq_msg_descr.next_prefix more efficiently
                    // (instead of checking all neighbors)
                    if !neighbor.is_disabled() && neighbor.already_processed(&enq_msg_descr)? {
                        delivered = true;
                        deliver_lt = neighbor.end_lt();
                        break;
                    }
                }
                if !delivered {
                    reject_query!("msg_export_deq OutMsg entry with key {} attempts to dequeue a message with next hop {} \
                        that has not been yet processed by the corresponding neighbor", key.to_hex_string(), next_prefix)
                }
                if deliver_lt != info.import_block_lt() {
                    log::warn!(target: "validate_query", "msg_export_deq OutMsg entry with key {} claims the dequeued message with next hop {} \
                        has been delivered in block with end_lt={} while the correct value is {}",
                            key.to_hex_string(), next_prefix, info.import_block_lt(), deliver_lt);
                }
            }
            // msg_export_tr_req
            OutMsg::TransitRequeued(_) => match reimport {
                // msg_import_tr
                Some(InMsg::Transit(info)) => {
                    let in_env = info.read_in_message()?;
                    if &in_env.message_cell().repr_hash() != key {
                        reject_query!("hash {:x} != Message hash {:x}", info.in_envelope_message_hash(), key)
                    }
                    let in_env = info.read_in_message()?;
                    let in_cur_prefix  = src_prefix.interpolate_addr_intermediate(&dest_prefix, &in_env.cur_addr())?;
                    let in_next_prefix = src_prefix.interpolate_addr_intermediate(&dest_prefix, &in_env.next_addr())?;
                    if !base.shard().contains_full_prefix(&in_cur_prefix) {
                            reject_query!("msg_export_tr_req OutMsg record with key {:x} corresponds to \
                                msg_import_tr InMsg record with current imported message address {} \
                                outside the current shard (msg_export_tr should have been used instead, because there \
                                was no re-queueing)", key, in_cur_prefix)
                    }
                    // we have already checked correctness of hypercube routing in InMsg::msg_import_tr case of check_in_msg()
                    CHECK!(base.shard().contains_full_prefix(&in_next_prefix));
                    CHECK!(base.shard().contains_full_prefix(&cur_prefix));
                    CHECK!(!base.shard().contains_full_prefix(&next_prefix));
                    // so we have just to check that the rewritten message (envelope) has been enqueued
                    // (already checked above for q_entry since mode = 3)
                    // and that the original message (envelope) has been dequeued
                    let q_key = OutMsgQueueKey::with_account_prefix(&in_next_prefix, key.clone());
                    let q_entry = manager.next().message(&q_key)?;
                    let enq_msg_descr = manager.prev().message(&q_key)?.ok_or_else(|| error!(
                            "msg_export_tr_req OutMsg record with key {:x} was expected to dequeue message from \
                            OutMsgQueue with key {:x} but such a message is absent from the old OutMsgQueue", 
                                key, q_key))?;
                    if q_entry.is_some() {
                        reject_query!("msg_export_tr_req OutMsg record with key {:x} \
                            was expected to dequeue message from OutMsgQueue with key {:x} \
                            but such a message is still present in the new OutMsgQueue",
                                key, q_key)
                    }
                    if enq_msg_descr.envelope_hash() != info.in_envelope_message_hash() {
                        reject_query!("msg_import_tr InMsg entry corresponding to msg_export_tr_req OutMsg entry with key {} \
                            has re-imported a different MsgEnvelope from that present in the old OutMsgQueue", key.to_hex_string())
                    }
                }
                _ => reject_query!("cannot unpack msg_import_tr InMsg record corresponding to \
                    msg_export_tr_req OutMsg record with key {}", key.to_hex_string())
            }
            // msg_export_deq_imm
            OutMsg::DequeueImmediate(_) => match reimport {
                // msg_import_fin
                Some(InMsg::Final(info)) => {
                    if info.envelope_message_hash() != msg_env_hash {
                        reject_query!("msg_import_fin InMsg record corresponding to msg_export_deq_imm OutMsg record with key {} \
                            somehow imported a different MsgEnvelope from that dequeued by msg_export_deq_imm", key.to_hex_string())
                    }
                    if !base.shard().contains_full_prefix(&cur_prefix) {
                        reject_query!("msg_export_deq_imm OutMsg record with key {} 
                            dequeued a MsgEnvelope with current address {}... outside current shard",
                                key.to_hex_string(), cur_prefix)
                    }
                    // we have already checked more conditions in check_in_msg() case msg_import_fin
                    CHECK!(base.shard().contains_full_prefix(&next_prefix));  // sanity check
                    CHECK!(base.shard().contains_full_prefix(&dest_prefix));  // sanity check
                    // ...
                }
                _ => reject_query!("cannot unpack msg_import_fin InMsg record corresponding to msg_export_deq_imm \
                    OutMsg record with key {}", key.to_hex_string())
            }
            msg => reject_query!("unknown OutMsg tag {:?}", msg)
        }
        Ok(())
    }

    fn check_out_msg_descr(base: Arc<ValidateBase>, manager: Arc<MsgQueueManager>, tasks: &mut Vec<Box<dyn FnOnce() -> Result<()> + Send + 'static>>) -> Result<()> {
        log::debug!(target: "validate_query", "checking outbound messages listed in OutMsgDescr");
        base.out_msg_descr.iterate_with_keys(|key, out_msg| {
            let base = base.clone();
            let manager = manager.clone();
            Self::add_task(tasks, move ||
                Self::check_out_msg(&base, &manager, &key, &out_msg)
                    .map_err(|err| error!("invalid OutMsg with key {} in the new block {} : {}",
                        key.to_hex_string(), base.block_id(), err))
            );
            Ok(true)
        }).map_err(|err| error!("invalid OutMsgDescr dictionary in the new block {} : {}", base.block_id(), err))?;
        Ok(())
    }

    // compare to Collator::update_processed_upto()
    fn check_processed_upto(base: &ValidateBase, manager: &MsgQueueManager, mc_data: &McData) -> Result<()> {
        log::debug!(target: "validate_query", "checking ProcessedInfo");
        if !manager.next().is_reduced() {
            reject_query!("new ProcessedInfo is not reduced (some entries completely cover other entries)");
        }
        let (claimed_proc_lt, claimed_proc_hash);
        let ok_upd = manager.next().is_simple_update_of(manager.prev());
        if !ok_upd.0 {
            reject_query!("new ProcessedInfo is not obtained from old ProcessedInfo by adding at most one new entry")
        } else if let Some(upd) = ok_upd.1 {
            if upd.shard != base.shard().shard_prefix_with_tag() {
                reject_query!("newly-added ProcessedInfo entry refers to shard {} distinct from the current shard {}",
                    ShardIdent::with_tagged_prefix(base.shard().workchain_id(), upd.shard)?, base.shard())
            }
            let ref_mc_seqno = match base.shard().is_masterchain() {
                true => base.block_id().seq_no,
                false => mc_data.state.state().seq_no()
            };
            if upd.mc_seqno != ref_mc_seqno {
                reject_query!("newly-added ProcessedInfo entry refers to masterchain block {} but \
                    the processed inbound message queue belongs to masterchain block {}",
                        upd.mc_seqno, ref_mc_seqno)
            }
            if upd.last_msg_lt >= base.info.end_lt() {
                reject_query!("newly-added ProcessedInfo entry claims that the last processed message has lt {} \
                    larger than this block's end lt {}", upd.last_msg_lt, base.info.end_lt())
            }
            if upd.last_msg_lt == 0 {
                reject_query!("newly-added ProcessedInfo entry claims that the last processed message has zero lt")
            }
            claimed_proc_lt = upd.last_msg_lt;
            claimed_proc_hash = upd.last_msg_hash;
        } else {
            claimed_proc_lt = 0;
            claimed_proc_hash = UInt256::default();
        }
        log::debug!(target: "validate_query", "ProcessedInfo claims to have processed all inbound messages up to ({},{:x})",
            claimed_proc_lt, claimed_proc_hash);
        if let Some(key_val) = base.result.lt_hash.get(&0) {
            let (proc_lt, proc_hash) = key_val.val();
            if &claimed_proc_lt < proc_lt
                || (&claimed_proc_lt == proc_lt && proc_lt != &0 && &claimed_proc_hash < proc_hash) {
                reject_query!("the ProcessedInfo claims to have processed messages only upto ({},{:x}), \
                    but there is a InMsg processing record for later message ({},{:x})",
                        claimed_proc_lt, claimed_proc_hash, proc_lt, proc_hash)
            }
        }
        if let Some(key_val) = base.result.lt_hash.get(&1) {
            let (min_enq_lt, min_enq_hash) = key_val.val();
            if min_enq_lt < &claimed_proc_lt
                || (min_enq_lt == &claimed_proc_lt && !(&claimed_proc_hash < min_enq_hash)) {
                reject_query!("the ProcessedInfo claims to have processed all messages only upto ({},{:x}), \
                    but there is a OutMsg enqueuing record for earlier message ({},{:x})",
                        claimed_proc_lt, claimed_proc_hash, min_enq_lt, min_enq_hash)
            }
        }
        base.result.lt_hash.insert(2, (claimed_proc_lt, claimed_proc_hash));
        // ...
        Ok(())
    }

    // similar to Collator::process_inbound_message
    fn check_neighbor_outbound_message_processed(
        base: &ValidateBase,
        manager: &MsgQueueManager,
        enq: MsgEnqueueStuff,
        created_lt: u64,
        key: &OutMsgQueueKey,
        nb_block_id: &BlockIdExt,
    ) -> Result<bool> {
        if created_lt != enq.created_lt() {
            reject_query!("EnqueuedMsg with key {:x} in outbound queue of our neighbor {} \
                pretends to have been created at lt {} but its actual creation lt is {}",
                    key, nb_block_id, created_lt, enq.created_lt())
        }
        CHECK!(base.shard().contains_full_prefix(&enq.next_prefix()));

        let in_msg = base.in_msg_descr.get(&key.hash)?;
        let out_msg = base.out_msg_descr.get(&key.hash)?;
        let f0 = manager.prev().already_processed(&enq)?;
        let f1 = manager.next().already_processed(&enq)?;
        if f0 && !f1 {
            reject_query!("a previously processed message has been un-processed \
                (impossible situation after the validation of ProcessedInfo)")
        } else if f0 { // f0 && f1
            // this message has been processed in a previous block of this shard
            // just check that we have not imported it once again
            if in_msg.is_some() {
                reject_query!("have an InMsg entry for processing again already processed EnqueuedMsg with key {:x} \
                    of neighbor {}", key, nb_block_id)
            }
            if base.shard().contains_full_prefix(&enq.cur_prefix()) {
                // if this message comes from our own outbound queue, we must have dequeued it
                let deq_hash = match out_msg {
                    None => reject_query!("our old outbound queue contains EnqueuedMsg with key {:x} \
                        already processed by this shard, but there is no ext_message_deq OutMsg record for this \
                        message in this block", key),
                    Some(OutMsg::DequeueShort(deq)) => deq.msg_env_hash,
                    Some(OutMsg::DequeueImmediate(deq)) => deq.out_message_cell().repr_hash(),
                    Some(deq) => reject_query!("{:?} msg_export_deq OutMsg record for already \
                        processed EnqueuedMsg with key {:x} of old outbound queue", deq, key)
                };
                if deq_hash != enq.envelope_hash() {
                    reject_query!("unpack ext_message_deq OutMsg record for already processed EnqueuedMsg with key {:x} \
                        of old outbound queue contains a different MsgEnvelope", key)
                }
            }
            // next check is incorrect after a merge, when ns_.processed_upto has > 1 entries
            // we effectively comment it out
            Ok(true)
            // NB. we might have a non-trivial dequeueing out_entry with this message hash, but another envelope (for transit messages)
            // (so we cannot assert that out_entry is null)
            // if self.claimed_proc_lt != 0
            //     && (self.claimed_proc_lt < created_lt || (self.claimed_proc_lt == created_lt && self.claimed_proc_hash < key.hash)) {
            //     log::error!(target: "validate_query", "internal inconsistency: new ProcessedInfo claims \
            //         to have processed all messages up to ({},{}) but we had somehow already processed a message ({},{}) \
            //         from OutMsgQueue of neighbor {} key {}", self.claimed_proc_lt, self.claimed_proc_hash.to_hex_string(),
            //             created_lt, key.hash.to_hex_string(), nb_block_id, key.to_hex_string());
            //     return Ok(false)
            // }
            // Ok(true)
        } else if f1 { // !f0 && f1
            // this message must have been imported and processed in this very block
            // (because it is marked processed after this block, but not before)
            if let Some(key_val) = base.result.lt_hash.get(&2) {
                let (claimed_proc_lt, claimed_proc_hash) = key_val.val();
                if claimed_proc_lt == &0 || claimed_proc_lt < &created_lt
                    || (claimed_proc_lt == &created_lt && claimed_proc_hash < &key.hash) {
                        reject_query!("internal inconsistency: new ProcessedInfo claims \
                        to have processed all messages up to ({},{:x}), but we had somehow processed in this block \
                        a message ({},{:x}) from OutMsgQueue of neighbor {} key {:x}",
                            claimed_proc_lt, claimed_proc_hash,
                            created_lt, key, nb_block_id, key)
                }
            }
            // must have a msg_import_fin or msg_import_tr InMsg record
            let hash = match in_msg {
                Some(InMsg::Final(info)) => info.envelope_message_hash(),
                Some(InMsg::Transit(info)) => info.in_envelope_message_hash(),
                None => reject_query!("there is no InMsg entry for processing EnqueuedMsg with key {:x} \
                    of neighbor {} which is claimed to be processed by new ProcessedInfo of this block", 
                        key, nb_block_id),
                _ => reject_query!("expected either a msg_import_fin or a msg_import_tr InMsg record \
                    for processing EnqueuedMsg with key {:x} of neighbor {} which is claimed to be processed \
                    by new ProcessedInfo of this block", key, nb_block_id)
            };
            if hash != enq.envelope_hash() {
                reject_query!("InMsg record for processing EnqueuedMsg with key {:x} of neighbor {} \
                    which is claimed to be processed by new ProcessedInfo of this block contains a reference \
                    to a different MsgEnvelope", key, nb_block_id);
            }
            // all other checks have been done while checking InMsgDescr
            Ok(true)
        } else { // !f0 && !f1
            // the message is left unprocessed in our virtual "inbound queue"
            // just a simple sanity check
            if let Some(key_val) = base.result.lt_hash.get(&2) {
                let (claimed_proc_lt, claimed_proc_hash) = key_val.val();
                if claimed_proc_lt != &0
                    && !(claimed_proc_lt < &created_lt || (claimed_proc_lt == &created_lt && claimed_proc_hash < &key.hash)) {
                    log::error!(target: "validate_query", "internal inconsistency: new ProcessedInfo claims \
                        to have processed all messages up to ({},{:x}), but we somehow have not processed a message ({},{:x}) \
                        from OutMsgQueue of neighbor {} key {:x}",
                            claimed_proc_lt, claimed_proc_hash,
                            created_lt, key.hash,
                            nb_block_id, key);
                }
            }
            Ok(false)
        }
    }

    // return true if all queues are processed
    fn check_in_queue(base: &ValidateBase, manager: &MsgQueueManager) -> Result<bool> {
        log::debug!(target: "validate_query", "check_in_queue len: {}", manager.neighbors().len());
        let mut iter = manager.merge_out_queue_iter(base.shard())?;
        while let Some(k_v) = iter.next() {
            let (msg_key, enq, lt, nb_block_id) = k_v?;
            log::debug!(target: "validate_query", "processing inbound message with \
                (lt,hash)=({},{:x}) from neighbor - {}", lt, msg_key.hash, nb_block_id);
            // if (verbosity > 3) {
            //     std::cerr << "inbound message: lt=" << kv->lt from=" << kv->source key=" << kv->key.to_hex_string() msg=";
            //     block::gen::t_EnqueuedMsg.print(std::cerr, *(kv->msg));
            // }
            match Self::check_neighbor_outbound_message_processed(base, manager, enq, lt, &msg_key, &nb_block_id) {
                Err(err) => {
                    // if (verbosity > 1) {
                    //     std::cerr << "invalid neighbor outbound message: lt=" << kv->lt from=" << kv->source
                    //             key=" << kv->key.to_hex_string() msg=";
                    //     block::gen::t_EnqueuedMsg.print(std::cerr, *(kv->msg));
                    // }
                    reject_query!("error processing outbound internal message {:x} of neighbor {} : {}",
                        msg_key.hash, nb_block_id, err)
                }
                Ok(false) => return Ok(false),
                _ => ()
            }
        }
        return Ok(true)
    }

    // checks that all messages imported from our outbound queue into neighbor shards have been dequeued
    // similar to Collator::out_msg_queue_cleanup()
    // (but scans new outbound queue instead of the old)
    fn check_delivered_dequeued(base: &ValidateBase, manager: &MsgQueueManager) -> Result<bool> {
        log::debug!(target: "validate_query", "scanning new outbound queue and checking delivery status of all messages");
        for nb in manager.neighbors() {
            if !nb.is_disabled() && !nb.can_check_processed() {
                reject_query!("internal error: no info for checking processed messages from neighbor {}", nb.block_id())
            }
        }
        // TODO: warning may be too much messages
        manager.next().out_queue().iterate_with_keys_and_aug(|msg_key, enq, created_lt| {
            // log::debug!(target: "validate_query", "key is " << key.to_hex_string(n));
            let enq = MsgEnqueueStuff::from_enqueue_and_lt(enq, created_lt)?;
            if msg_key.hash != enq.message_hash() {
                reject_query!("cannot unpack EnqueuedMsg with key {:x} in the new OutMsgQueue", msg_key)
            }
            log::debug!(target: "validate_query", "scanning outbound message with (lt,hash)=({},{}) enqueued_lt={}",
                created_lt, msg_key.hash.to_hex_string(), enq.enqueued_lt());
            for nb in manager.neighbors() {
                // could look up neighbor with shard containing enq_msg_descr.next_prefix more efficiently
                // (instead of checking all neighbors)
                if !nb.is_disabled() && nb.already_processed(&enq)? {
                    // the message has been delivered but not removed from queue!
                    log::warn!(target: "validate_query", "outbound queue not cleaned up completely (overfull block?): \
                        outbound message with (lt,hash)=({},{}) enqueued_lt={} has been already delivered and \
                        processed by neighbor {} but it has not been dequeued in this block and it is still \
                        present in the new outbound queue", created_lt, msg_key.hash.to_hex_string(),
                            enq.enqueued_lt(), nb.block_id());
                    return Ok(false)
                }
            }
            if created_lt >= base.info.start_lt() {
                log::debug!(target: "validate_query", "stop scanning new outbound queue");
                return Ok(false)
            }
            Ok(true)
        })?;
        Ok(true)
    }


    // similar to Collator::make_account_from()
    // std::unique_ptr<block::Account> ValidateQuery::make_account_from(td::ConstBitPtr addr, Ref<vm::CellSlice> account,
    //                                                                     Ref<vm::CellSlice> extra) {
    //     let ptr = std::make_unique<block::Account>(workchain(), addr);
    //     if (account.is_none()) {
    //         ptr->created = true;
    //         if (!ptr->init_new(base.now())) {
    //         return nullptr;
    //         }
    //     } else if (!ptr->unpack(std::move(account), std::move(extra), base.now(),
    //                             base.shard().is_masterchain() && base.config_params->is_special_smartcontract(addr))) {
    //         return nullptr;
    //     }
    //     ptr->block_lt = base.info.start_lt();
    //     return ptr;
    // }

    // similar to Collator::make_account()
    fn unpack_account(base: &ValidateBase, addr: &UInt256) -> Result<(Cell, Account)> {
        match base.prev_state_accounts.get(addr)? {
            Some(shard_acc) => {
                let new_acc = shard_acc.read_account()?;
                if !new_acc.belongs_to_shard(base.shard())? {
                    reject_query!("old state of account {} does not really belong to current shard", addr.to_hex_string())
                }
                Ok((shard_acc.account_cell(), new_acc))
            }
            None => {
                let new_acc = Account::default();
                Ok((new_acc.serialize()?, new_acc))
            }
        }
    }

    fn check_one_transaction(
        base: &ValidateBase,
        config: BlockchainConfig,
        libraries: Libraries,
        account_addr: &UInt256,
        account_root: &mut Cell,
        account: &mut Account,
        lt: u64,
        trans_root: Cell,
        is_first: bool,
        is_last: bool
    ) -> Result<bool> {
        log::debug!(target: "validate_query", "checking {} transaction {} of account {}",
            lt, trans_root.repr_hash().to_hex_string(), account_addr.to_hex_string());
        let trans = Transaction::construct_from_cell(trans_root.clone())?;
        let account_create = account.is_none();

        // check input message
        let mut money_imported = CurrencyCollection::default();
        let mut money_exported = CurrencyCollection::default();
        let in_msg = trans.read_in_msg()?;
        if let Some(in_msg_root) = trans.in_msg_cell() {
            let in_msg = base.in_msg_descr.get(&in_msg_root.repr_hash())?.ok_or_else(|| error!("inbound message with hash {} of \
                transaction {} of account {} does not have a corresponding InMsg record",
                    in_msg_root.repr_hash().to_hex_string(), lt, account_addr.to_hex_string()))?;
            let msg = Message::construct_from_cell(in_msg_root.clone())?;
            // once we know there is a InMsg with correct hash, we already know that it contains a message with this hash (by the verification of InMsg), so it is our message
            // have still to check its destination address and imported value
            // and that it refers to this transaction
            match in_msg {
                InMsg::External(_) => (),
                InMsg::IHR(_) | InMsg::Immediate(_) | InMsg::Final(_) => {
                    let header = msg.int_header().ok_or_else(|| error!("inbound message transaction {} of {} must have \
                        internal message header", lt, account_addr.to_hex_string()))?;
                    if header.created_lt >= lt {
                        reject_query!("transaction {} of {} processed inbound message created later at logical time {}",
                            lt, account_addr.to_hex_string(), header.created_lt)
                    }
                    if header.created_lt != base.info.start_lt() || !base.is_special_in_msg(&in_msg) {
                        base.result.msg_proc_lt.push((account_addr.clone(), lt, header.created_lt));
                    }
                    money_imported = header.value.clone();
                    if let InMsg::IHR(_) = in_msg {
                        money_imported.grams.add(&header.ihr_fee)?;
                    }
                }
                _ => reject_query!("inbound message with hash {} of transaction {} of account {} \
                    has an invalid InMsg record (not one of msg_import_ext, msg_import_fin, msg_import_imm or msg_import_ihr)",
                        in_msg_root.repr_hash().to_hex_string(), lt, account_addr.to_hex_string())
            }
            let dst = match msg.dst_ref() {
                Some(MsgAddressInt::AddrStd(dst)) => dst,
                _ => reject_query!("inbound message with hash {} transaction {} of {} must have std internal destination address",
                    in_msg_root.repr_hash().to_hex_string(), lt, account_addr.to_hex_string())
            };
            if dst.workchain_id as i32 != base.shard().workchain_id() || account_addr != dst.address {
                reject_query!("inbound message of transaction {} of account {} has a different destination address {}:{}",
                    account_addr.to_hex_string(), lt, dst.workchain_id, dst.address.to_hex_string())
            }
            CHECK!(in_msg.transaction_cell().is_some());
            if let Some(cell) = in_msg.transaction_cell() {
                if cell != trans_root {
                    reject_query!("InMsg record for inbound message with hash {:x} of transaction {} of account {:x} \
                        refers to a different processing transaction",
                            in_msg_root.repr_hash(), lt, account_addr)
                }
            }
        }
        // check output messages
        trans.out_msgs.iterate_slices_with_keys(|ref mut key, ref out_msg| {
            let out_msg_root = out_msg.reference(0)?;
            let i = key.get_next_int(15)? + 1;
            let out_msg = base.out_msg_descr.get(&out_msg_root.repr_hash())?.ok_or_else(|| error!("outbound message #{} \
                with hash {} of transaction {} of account {} does not have a corresponding  record",
                    i, out_msg_root.repr_hash().to_hex_string(), lt, account_addr.to_hex_string()))?;
            // once we know there is an OutMsg with correct hash, we already know that it contains a message with this hash (by the verification of OutMsg), so it is our message
            // have still to check its source address, lt and imported value
            // and that it refers to this transaction as its origin
            let msg = Message::construct_from_cell(out_msg_root.clone())?;
            match out_msg {
                OutMsg::External(_) => (),
                OutMsg::Immediate(_) | OutMsg::New(_) => {
                    let header = msg.int_header().ok_or_else(|| error!("transaction {} of {} \
                        must have internal message header", lt, account_addr.to_hex_string()))?;
                    money_exported.add(&header.value)?;
                    money_exported.grams.add(&header.ihr_fee)?;
                    if let Some(msg_env) = out_msg.read_out_message()? {
                        money_exported.grams.add(&msg_env.fwd_fee_remaining())?;
                    }
                    // unpack exported message value (from this transaction)
                }
                _ => reject_query!("outbound message #{} with hash {} of transaction {} of account {} \
                    has an invalid OutMsg record (not one of msg_export_ext, msg_export_new or msg_export_imm)",
                        i, out_msg_root.repr_hash().to_hex_string(), lt, account_addr.to_hex_string())
            }
            let src = match msg.src_ref() {
                Some(MsgAddressInt::AddrStd(src)) => src,
                _ => reject_query!("outbound message #{} with hash {} of transaction {} of account {} \
                    does not have a correct header",
                        i, out_msg_root.repr_hash().to_hex_string(), lt, account_addr.to_hex_string())
            };
            if src.workchain_id as i32 != base.shard().workchain_id() || account_addr != src.address {
                reject_query!("outbound message #{} of transaction {} of account {} has a different source address {}:{}",
                    i, lt, account_addr.to_hex_string(), src.workchain_id, src.address.to_hex_string())
            }
            match out_msg.transaction_cell() {
                Some(cell) => if cell.clone() != trans_root {
                    reject_query!("OutMsg record for outbound message #{} with hash {} of transaction {} \
                        of account {} refers to a different processing transaction",
                            i, out_msg_root.repr_hash().to_hex_string(), lt, account_addr.to_hex_string())
                }
                None => CHECK!(out_msg.transaction_cell().is_some())
            }
            Ok(true)
        })?;
        // check general transaction data
        let old_balance = account.balance().cloned().unwrap_or_default();
        let descr = trans.read_description()?;
        let split = descr.is_split();
        if split || descr.is_merge() {
            if base.shard().is_masterchain() {
                reject_query!("transaction {} of account {} is a split/merge prepare/install transaction, which is impossible in a masterchain block",
                    lt, account_addr.to_hex_string())
            }
            if split && !base.info.before_split() {
                reject_query!("transaction {} of account {} is a split prepare/install transaction, but this block is not before a split",
                    lt, account_addr.to_hex_string())
            }
            if split && !is_last {
                reject_query!("transaction {} of account {} is a split prepare/install transaction, \
                    but it is not the last transaction for this account in this block",
                        lt, account_addr.to_hex_string())
            }
            if !split && !base.info.after_merge() {
                reject_query!("transaction {} of account {} is a merge prepare/install transaction, \
                    is a merge prepare/install transaction, but this block is not immediately after a merge",
                        lt, account_addr.to_hex_string())
            }
            if !split && !is_first {
                reject_query!("transaction {} of account {} is a merge prepare/install transaction, \
                    is a merge prepare/install transaction, but it is not the first transaction for this account in this block",
                        lt, account_addr.to_hex_string())
            }
            // check later a global configuration flag in base.config_params.global_flags_
            // (for now, split/merge transactions are always globally disabled)
            reject_query!("transaction {} of account {} is a split/merge prepare/install transaction, which are globally disabled",
                lt, account_addr.to_hex_string())
        }
        if let TransactionDescr::TickTock(ref info) = descr {
            if !base.shard().is_masterchain() {
                reject_query!("transaction {} of account {} is a tick-tock transaction, which is impossible outside a masterchain block",
                    lt, account_addr.to_hex_string())
            }
            if let Some(acc_tick_tock) = account.get_tick_tock() {
                if !info.tt.is_tock() {
                    if !is_first {
                        reject_query!("transaction {} of account {} is a tick transaction, but this is not the first transaction of this account",
                            lt, account_addr.to_hex_string())
                    }
                    if lt != base.info.start_lt() + 1 {
                        reject_query!("transaction {} of account {} is a tick transaction, but its logical start time differs from block's start time {} by more than one",
                            lt, account_addr.to_hex_string(), base.info.start_lt())
                    }
                    if !acc_tick_tock.tick {
                        reject_query!("transaction {} of account {} is a tick transaction, but this account has not enabled tick transactions",
                            lt, account_addr.to_hex_string())
                    }
                } else {
                    if !is_last {
                        reject_query!("transaction {} of account {} is a tock transaction, but this is not the last transaction of this account",
                            lt, account_addr.to_hex_string())
                    }
                    if !acc_tick_tock.tock {
                        reject_query!("transaction {} of account {} is a tock transaction, but this account has not enabled tock transactions",
                            lt, account_addr.to_hex_string())
                    }
                }
            } else {
                reject_query!("transaction {} of account {} is a tick-tock transaction, but this account is not listed as special",
                    lt, account_addr.to_hex_string())
            }
        }
        // check if account has tick_tock attribute
        if let Some(ref acc_tick_tock) = account.get_tick_tock() {
            let tick_tock = match descr {
                TransactionDescr::TickTock(ref info) => Some(info.tt.clone()),
                _ => None
            };
            if is_first && base.shard().is_masterchain() && acc_tick_tock.tick
                && !tick_tock.as_ref().map(|ref tt| tt.is_tick()).unwrap_or_default()
                && !account_create {
                reject_query!("transaction {} of account {} is the first transaction \
                    for this special tick account in this block, \
                    but the transaction is not a tick transaction",
                        lt, account_addr.to_hex_string())
            }
            if is_last && base.shard().is_masterchain() && acc_tick_tock.tock
                && !tick_tock.as_ref().map(|ref tt| tt.is_tock()).unwrap_or_default()
                && trans.end_status == AccountStatus::AccStateActive {
                reject_query!("transaction {} of account {} is the last transaction \
                    for this special tock account in this block, \
                    but the transaction is not a tock transaction",
                        lt, account_addr.to_hex_string())
            }
        }
        if let TransactionDescr::Storage(_) = descr {
            if !is_first {
                reject_query!(
                    "transaction {} of account {} is a storage transaction, \
                        but it is not the first transaction for this account in this block",
                            lt, account_addr.to_hex_string())
            }
        }
        // check that the original account state has correct hash
        let state_update = trans.read_state_update()?;
        let old_hash = account_root.repr_hash();
        if state_update.old_hash != old_hash {
            reject_query!("transaction {} of account {:x} claims that the original \
                account state hash must be {:x} but the actual value is {:x}",
                    lt, account_addr, state_update.old_hash, old_hash)
        }
        // some type-specific checks
        let tr_lt = Arc::new(AtomicU64::new(lt));
        let executor: Box<dyn TransactionExecutor> = match descr {
            TransactionDescr::Ordinary(_) => {
                if trans.in_msg_cell().is_none() {
                    reject_query!("ordinary transaction {} of account {} has no inbound message",
                        lt, account_addr.to_hex_string())
                }
                Box::new(OrdinaryTransactionExecutor::new(config))
            }
            TransactionDescr::Storage(_) => {
                if trans.in_msg_cell().is_some() {
                    reject_query!("storage transaction {} of account {} has an inbound message",
                        lt, account_addr.to_hex_string())
                }
                if trans.msg_count() != 0 {
                    reject_query!("storage transaction {} of account {} has at least one outbound message",
                        lt, account_addr.to_hex_string())
                }
                // FIXME
                reject_query!("unable to verify storage transaction {} of account {}", lt, account_addr.to_hex_string())
            }
            TransactionDescr::TickTock(ref info) => {
                if trans.in_msg_cell().is_some() {
                    reject_query!("{} transaction {} of account {} has an inbound message",
                        if info.tt.is_tock() {"tock"} else {"tick"}, lt, account_addr.to_hex_string())
                }
                Box::new(TickTockTransactionExecutor::new(config, info.tt.clone()))
            }
            TransactionDescr::MergePrepare(_) => {
                if trans.in_msg_cell().is_some() {
                    reject_query!("merge prepare transaction {} of account {} has an inbound message",
                        lt, account_addr.to_hex_string())
                }
                if trans.msg_count() != 1 {
                    reject_query!("merge prepare transaction {} of account {} must have exactly one outbound message",
                            lt, account_addr.to_hex_string())
                }
                // FIXME
                reject_query!("unable to verify merge prepare transaction {} of account {}", lt, account_addr.to_hex_string())
            }
            TransactionDescr::MergeInstall(_) => {
                if trans.in_msg_cell().is_none() {
                    reject_query!("merge install transaction {} of account {} has no inbound message",
                        lt, account_addr.to_hex_string())
                }
                // FIXME
                reject_query!("unable to verify merge install transaction {} of account {}", lt, account_addr.to_hex_string())
            }
            TransactionDescr::SplitPrepare(_) => {
                if trans.in_msg_cell().is_some() {
                    reject_query!("split prepare transaction {} of account {} has an inbound message",
                        lt, account_addr.to_hex_string())
                }
                if trans.msg_count() != 1 {
                    reject_query!("merge prepare transaction {} of account {} must have exactly one outbound message",
                            lt, account_addr.to_hex_string())
                }
                // FIXME
                reject_query!("unable to verify split prepare transaction {} of account {}", lt, account_addr.to_hex_string())
            }
            TransactionDescr::SplitInstall(_) => {
                if trans.in_msg_cell().is_none() {
                    reject_query!("split install transaction {} of account {} has no inbound message",
                        lt, account_addr.to_hex_string())
                }
                // FIXME
                reject_query!("unable to verify split install transaction {} of account {}", lt, account_addr.to_hex_string())
            }
        };
        let params = ExecuteParams {
            state_libs: libraries.inner(),
            block_unixtime: base.now(),
            block_lt: base.info.start_lt(),
            last_tr_lt: tr_lt,
            seed_block: base.extra.rand_seed().clone(),
            debug: false,
            block_version: base.info.gen_software().unwrap_or(&config_params::GlobalVersion::new()).version,
            ..ExecuteParams::default()
        };
        let _old_account_root = account_root.clone();
        let mut trans_execute = executor.execute_with_libs_and_params(in_msg.as_ref(), account_root, params)?;
        let copyleft_reward = trans_execute.copyleft_reward().clone();
        *account = Account::construct_from_cell(account_root.clone())?;
        let new_hash = account_root.repr_hash();
        if state_update.new_hash != new_hash {
            Self::prepare_transaction_for_log(&_old_account_root, account_root, executor.config().raw_config(), &trans, &trans_execute).ok();
            reject_query!("transaction {} of {:x} is invalid: it claims that the new \
                account state hash is {:x} but the re-computed value is {:x}",
                    lt, account_addr, state_update.new_hash, new_hash)
        }
        if trans.out_msgs != trans_execute.out_msgs {
            Self::prepare_transaction_for_log(&_old_account_root, account_root, executor.config().raw_config(), &trans, &trans_execute).ok();
            reject_query!("transaction {} of {:x} is invalid: it has produced a set of \
                outbound messages different from that listed in the transaction",
                    lt, account_addr)
        }
        if let Some(TrComputePhase::Vm(compute_ph)) = descr.compute_phase_ref() {
            base.gas_used.fetch_add(compute_ph.gas_used.as_u64(), Ordering::Relaxed);
        }
        base.transactions_executed.fetch_add(1, Ordering::Relaxed);

        if let Some(copyleft_reward) = &copyleft_reward {
            base.copyleft_rewards.push((copyleft_reward.address.clone(), copyleft_reward.reward.clone()));
        }

        // we cannot know prev transaction in executor
        trans_execute.set_prev_trans_hash(trans.prev_trans_hash().clone());
        trans_execute.set_prev_trans_lt(trans.prev_trans_lt());
        let trans_execute_root = trans_execute.serialize()?;
        if trans_root != trans_execute_root {
            Self::prepare_transaction_for_log(&_old_account_root, account_root, executor.config().raw_config(), &trans, &trans_execute).ok();
            reject_query!("re created transaction {} doesn't correspond", lt)
        }
        // check new balance and value flow
        let mut left_balance = old_balance.clone();
        left_balance.add(&money_imported)?;
        if let Some(in_msg) = in_msg {
            if let Some(header) = in_msg.int_header() {
                left_balance.grams.add(&header.ihr_fee)?;
            }
        }
        let new_balance = account.balance().cloned().unwrap_or_default();
        let mut right_balance = new_balance.clone();
        let copyleft_reward_after = copyleft_reward.map_or_else(Grams::zero, |copyleft| copyleft.reward);
        right_balance.add(&CurrencyCollection::from_grams(copyleft_reward_after))?;
        right_balance.add(&money_exported)?;
        right_balance.add(&trans.total_fees())?;
        if left_balance != right_balance {
            Self::prepare_transaction_for_log(&_old_account_root, account_root, executor.config().raw_config(), &trans, &trans_execute).ok();
            reject_query!("transaction {} of {} violates the currency flow condition: \
                old balance={} + imported={} does not equal new balance={} + exported=\
                {} + total_fees={}", lt, account_addr.to_hex_string(),
                    old_balance.grams, money_imported.grams,
                    new_balance.grams, money_exported.grams, trans.total_fees().grams)
        }
        Ok(true)
    }

    // NB: may be run in parallel for different accounts
    fn check_account_transactions(
        base: &ValidateBase,
        config: BlockchainConfig,
        libraries: Libraries,
        account_addr: &UInt256,
        account_root: &mut Cell,
        account: Account,
        acc_block: AccountBlock,
        _fee: CurrencyCollection
    ) -> Result<()> {
        // CHECK!(account.get_id(), Some(account_addr.as_slice().into()));
        let (min_trans_lt, _) = acc_block.transactions().get_min(false)?.ok_or_else(|| error!("no minimal transaction"))?;
        let (max_trans_lt, _) = acc_block.transactions().get_max(false)?.ok_or_else(|| error!("no maximal transaction"))?;
        let mut new_account = account.clone();
        acc_block.transactions().iterate_slices_with_keys(|lt, trans| {
            Self::check_one_transaction(
                base,
                config.clone(),
                libraries.clone(),
                account_addr,
                account_root,
                &mut new_account,
                lt,
                trans.reference(0)?,
                lt == min_trans_lt,
                lt == max_trans_lt
            )
        }).map_err(|err| error!("at least one Transaction of account {} is invalid : {}", account_addr.to_hex_string(), err))?;
        if base.shard().is_masterchain() {
            Self::scan_account_libraries(base, account.libraries(), new_account.libraries(), account_addr)
        } else {
            Ok(())
        }
    }

    fn check_transactions(base: Arc<ValidateBase>, libraries: Libraries, tasks: &mut Vec<Box<dyn FnOnce() -> Result<()> + Send + 'static>>) -> Result<()> {
        log::debug!(target: "validate_query", "checking all transactions");
        let config = BlockchainConfig::with_config(base.config_params.clone())?;
        base.account_blocks.iterate_with_keys_and_aug(|account_addr, acc_block, fee| {
            let base = base.clone();
            let libraries = libraries.clone();
            let (mut account_root, account) = Self::unpack_account(&base, &account_addr)?;
            let config = config.clone();
            Self::add_task(tasks, move ||
                Self::check_account_transactions(
                    &base,
                    config,
                    libraries,
                    &account_addr,
                    &mut account_root,
                    account,
                    acc_block,
                    fee
                )
            );
            Ok(true)
        })?;
        Ok(())
    }

    // similar to Collator::update_account_public_libraries()
    fn scan_account_libraries(base: &ValidateBase, orig_libs: StateInitLib, final_libs: StateInitLib, addr: &UInt256) -> Result<()> {
        orig_libs.scan_diff(&final_libs, |key: UInt256, old, new| {
            let f = old.map(|descr| descr.is_public_library()).unwrap_or_default();
            let g = new.map(|descr| descr.is_public_library()).unwrap_or_default();
            if f != g {
                base.result.lib_publishers.push((key, addr.clone(), g));
            }
            Ok(true)
        }).map_err(|err| error!("error scanning old and new libraries of account {} : {}", addr.to_hex_string(), err))?;
        Ok(())
    }

    fn check_all_ticktock_processed(base: &ValidateBase) -> Result<()> {
        if !base.shard().is_masterchain() {
            return Ok(())
        }
        log::debug!(target: "validate_query", "getting the list of special tick-tock smart contracts");
        let ticktock_smcs = base.config_params.special_ticktock_smartcontracts(3, &base.prev_state_accounts)?;
        log::debug!(target: "validate_query", "have {} tick-tock smart contracts", ticktock_smcs.len());
        for addr in ticktock_smcs {
            log::debug!(target: "validate_query", "special smart contract {} with ticktock={}", addr.0.to_hex_string(), addr.1);
            if base.account_blocks.get(&addr.0)?.is_none() {
                reject_query!("there are no transactions (and in particular, no tick-tock transactions) \
                    for special smart contract {} with ticktock={}",
                        addr.0.to_hex_string(), addr.1)
            }
        }
        Ok(())
    }

    fn check_message_processing_order(base: &mut ValidateBase) -> Result<()> {
        if let Some(msg_proc_lt) = Arc::get_mut(&mut base.result.msg_proc_lt) {
            let mut msg_proc_lt = msg_proc_lt.collect::<Vec<_>>();
            msg_proc_lt.sort();
            for i in 1..msg_proc_lt.len() {
                let a = &msg_proc_lt[i - 1];
                let b = &msg_proc_lt[i];
                if a.0 == b.0 && a.2 > b.2 {
                    reject_query!("incorrect message processing order: transaction ({},{:x}) processes message created at logical time {}, \
                        but a later transaction ({},{:x}) processes an earlier message created at logical time {}",
                        a.1, a.0, a.2, b.1, b.0, b.2)
                }
            }
            Ok(())
        } else{
            reject_query!("Internal error with msg_proc_lt")
        }
    }

    fn check_special_message(base: &ValidateBase, in_msg: Option<&InMsg>, amount: &CurrencyCollection, addr: UInt256) -> Result<()> {
        let in_msg = match in_msg {
            Some(in_msg) => in_msg,
            None if amount.is_zero()? => return Ok(()),
            None => reject_query!("no special message, but amount is {}", amount)
        };
        CHECK!(!amount.is_zero()?);
        if !base.shard().is_masterchain() {
            reject_query!("special messages can be present in masterchain only")
        }
        let env = match in_msg {
            InMsg::Immediate(in_msg) => in_msg.read_envelope_message()?,
            _ => reject_query!("wrong type of message")
        };
        let msg_hash = env.message_cell().repr_hash();
        log::debug!(target: "validate_query", "checking special message with hash {} and expected amount {}",
            msg_hash.to_hex_string(), amount);
        match base.in_msg_descr.get(&msg_hash)? {
            Some(msg) => if &msg != in_msg {
                reject_query!("InMsg of special message with hash {} differs from the InMsgDescr entry with this key", msg_hash.to_hex_string())
            }
            None => reject_query!("InMsg of special message with hash {} is not registered in InMsgDescr", msg_hash.to_hex_string())
        };

        let msg = env.read_message()?;
        // CHECK!(tlb::unpack(cs, info));  // this has been already checked for all InMsgDescr
        let header = msg.int_header().ok_or_else(|| error!("InMsg of special message with hash {} has wrong header", msg_hash.to_hex_string()))?;
        let src = msg.src_ref().ok_or_else(|| error!("source address of message {:x} is invalid", env.message_hash()))?;
        if src.rewrite_pfx().is_some() {
            fail!("source address of message {:x} contains anycast info - it is not supported", env.message_hash())
        }
        let src_prefix = AccountIdPrefixFull::prefix(src)?;
        let dst = msg.dst_ref().ok_or_else(|| error!("destination address of message {:x} is invalid", env.message_hash()))?;
        if dst.rewrite_pfx().is_some() {
            fail!("destination address of message {:x} contains anycast info - it is not supported", env.message_hash())
        }
        let dst_prefix = AccountIdPrefixFull::prefix(dst)?;
        // CHECK!(src_prefix.is_valid() && dst_prefix.is_valid());  // we have checked this for all InMsgDescr
        let cur_prefix  = src_prefix.interpolate_addr_intermediate(&dst_prefix, &env.cur_addr())?;
        let next_prefix = src_prefix.interpolate_addr_intermediate(&dst_prefix, &env.next_addr())?;
        if cur_prefix != dst_prefix || next_prefix != dst_prefix {
            reject_query!("special message with hash {} has not been routed to its final destination", msg_hash.to_hex_string())
        }
        if !base.shard().contains_full_prefix(&src_prefix) {
            reject_query!("special message with hash {} has source address {} outside this shard",
                msg_hash.to_hex_string(), src_prefix)
        }
        if !base.shard().contains_full_prefix(&dst_prefix) {
            reject_query!("special message with hash {} has destination address {} outside this shard",
                msg_hash.to_hex_string(), dst_prefix)
        }
        if !env.fwd_fee_remaining().is_zero() {
            reject_query!("special message with hash {} has a non-zero fwd_fee_remaining", msg_hash.to_hex_string());
        }
        if !header.fwd_fee.is_zero() {
            reject_query!("special message with hash {} has a non-zero fwd_fee", msg_hash.to_hex_string());
        }
        if !header.ihr_fee.is_zero() {
            reject_query!("special message with hash {} has a non-zero ihr_fee", msg_hash.to_hex_string());
        }
        if &header.value != amount {
            reject_query!("special message with hash {} carries an incorrect amount {} instead of {} postulated by ValueFlow",
                msg_hash.to_hex_string(), header.value, amount)
        }
        let (src, dst) = match (&header.src_ref(), &header.dst) {
            (Some(MsgAddressInt::AddrStd(ref src)), MsgAddressInt::AddrStd(ref dst)) => (src.clone(), dst.clone()),
            _ => reject_query!("cannot unpack source and destination addresses of special message with hash {}", msg_hash.to_hex_string())
        };
        let src_addr = UInt256::construct_from(&mut src.address.clone())?;
        if src.workchain_id as i32 != MASTERCHAIN_ID || !src_addr.is_zero() {
            reject_query!("special message with hash {} has a non-zero source address {}:{}",
                msg_hash.to_hex_string(), src.workchain_id, src_addr.to_hex_string())
        }
        // CHECK!(dest_wc == masterchainId);
        // CHECK!(vm::load_cell_slice(addr_cell).prefetch_bits_to(correct_addr));
        let dst_addr = UInt256::construct_from(&mut dst.address.clone())?;
        if dst_addr != addr {
            reject_query!("special message with hash {} has destination address -1:{} \
                but the correct address defined by the configuration is {}",
                    msg_hash.to_hex_string(), dst_addr.to_hex_string(), addr.to_hex_string())
        }
        
        if msg.body().map(|cs| !cs.is_empty()).unwrap_or_default() {
            reject_query!("special message with hash {} has a non-empty body", msg_hash.to_hex_string())
        }
        Ok(())
    }

    fn check_special_messages(base: &ValidateBase, sent_rewards: &HashMap<AccountId, Grams>) -> Result<()> {
        Self::check_special_message(base, base.recover_create_msg.as_ref(), &base.value_flow.recovered, base.config_params.fee_collector_address()?)?;
        Self::check_special_message(base, base.mint_msg.as_ref(), &base.value_flow.minted, base.config_params.minter_address()?)?;
        if sent_rewards.len() != base.copyleft_msgs.len() {
            reject_query!("sent rewards len incorrect: {} {}", sent_rewards.len(), base.copyleft_msgs.len())
        }
        for index in 0..base.copyleft_msgs.len() {
            let in_msg = &base.copyleft_msgs[index];
            let mut dst = in_msg.read_message()?.dst().ok_or_else(
                || error!("cannot find dst address in copyleft reward message")
            )?.address();
            let value = sent_rewards.get(&dst).ok_or_else(
                || error!("cannot find sended copyleft reward on address {:x}", dst)
            )?.clone();
            Self::check_special_message(base, Some(in_msg), &CurrencyCollection::from_grams(value), UInt256::construct_from(&mut dst)?)?;
        }
        Ok(())
    }

    fn check_one_library_update(key: UInt256, old: Option<LibDescr>, new: Option<LibDescr>, lib_publishers2: &mut Vec<LibPublisher>) -> Result<bool> {
        let new = match new {
            Some(new) => {
                if new.lib().repr_hash() != key {
                    reject_query!("LibDescr with key {} in the libraries dictionary of the new state \
                        contains a library with different root hash {}", key.to_hex_string(), new.lib().repr_hash())
                }
                new
            }
            None => LibDescr::default()
        };
        let old = match old {
            Some(old) => old,
            None => LibDescr::default()
        };
        old.publishers().scan_diff(new.publishers(), |publisher: UInt256, old, new| {
            if old != new {
                reject_query!("publisher {} in old={} and new={} state", publisher.to_hex_string(), old.is_some(), new.is_some())
            }
            lib_publishers2.push((key.clone(), publisher, new.is_some()));
            Ok(true)
        }).map_err(|err| error!("invalid publishers set for shard library with hash {} : {}", key.to_hex_string(), err))
    }

    fn check_shard_libraries(base: &mut ValidateBase) -> Result<()> {
        let mut lib_publishers2 = vec![];
        let old = base.prev_state.as_ref().ok_or_else(
            || error!("Prev state is not initialized in validator query")
        )?.state().libraries().clone();
        let new = base.next_state.as_ref().ok_or_else(
            || error!("Next state is not initialized in validator query")
        )?.state().libraries().clone();
        old.scan_diff(
            &new, 
            |key: UInt256, old, new| Self::check_one_library_update(key, old, new, &mut lib_publishers2)
        ).map_err(|err| error!("invalid shard libraries dictionary in the new state : {}", err))?;

        if let Some(lib_publishers)  = Arc::get_mut(&mut base.result.lib_publishers) {
            let mut lib_publishers = lib_publishers.collect::<Vec<_>>();
            lib_publishers.sort();
            lib_publishers2.sort();
            if lib_publishers != lib_publishers2 {
                // TODO: better error message with by-element comparison?
                reject_query!("the set of public libraries and their publishing accounts has not been updated correctly")
            }
            Ok(())
        } else{
            reject_query!("Internal error with lib_publishers")
        }
    }

    fn check_new_state(base: &mut ValidateBase, mc_data: &McData, manager: &MsgQueueManager) -> Result<()> {
        log::debug!(target: "validate_query", "checking header of the new shardchain state");
        let prev_state = base.prev_state.as_ref().ok_or_else(
            || error!("Prev state is not initialized in validator query")
        )?.state();
        if let Some(next_state) = &base.next_state {
            let next_state = next_state.state();
            let my_mc_seqno = if base.shard().is_masterchain() {
                base.block_id().seq_no()
            } else {
                mc_data.state.block_id().seq_no()
            };
            let min_seq_no = match manager.next().min_seqno() {
                0 => std::u32::MAX,
                min_seq_no => min_seq_no
            };
            let ref_mc_seqno = std::cmp::min(
                min_seq_no, 
                std::cmp::min(base.min_shard_ref_mc_seqno(), my_mc_seqno)
            );
            if next_state.min_ref_mc_seqno() != ref_mc_seqno {
                reject_query!(
                    "new state of {} has minimal referenced masterchain block seqno {} \
                    but the value computed from all shard references and previous masterchain \
                    block reference is {} = min({},{},{})",
                    base.block_id(), next_state.min_ref_mc_seqno(), ref_mc_seqno,
                    my_mc_seqno, base.min_shard_ref_mc_seqno(), min_seq_no
                )
            }            
            if !next_state.read_out_msg_queue_info()?.ihr_pending().is_empty() {
                reject_query!(
                    "IhrPendingInfo in the new state of {} is non-empty, 
                    but IHR delivery is now disabled", base.block_id()
                )
            }
            // before_split:(## 1) -> checked in unpack_next_state()
            // accounts:^ShardAccounts -> checked in precheck_account_updates() + other
            // ^[ overload_history:uint64 underload_history:uint64
            if next_state.overload_history() & next_state.underload_history() & 1 != 0 {
                reject_query!(
                    "lower-order bits both set in the new state's overload_history \
                    and underload history (block cannot be both overloaded and underloaded)"
                )
            }
            if base.after_split || base.after_merge {
                if (next_state.overload_history() | next_state.underload_history()) & !1 != 0 {
                    reject_query!(
                        "new block is immediately after split or after merge, \
                        but the old underload or overload history has not been cleared"
                    )
                }
            } else {
                if (next_state.overload_history() ^ (prev_state.overload_history() << 1)) & !1 != 0 {
                    reject_query!(
                        "new overload history {} is not compatible with the old overload history {}",
                        next_state.overload_history(), prev_state.overload_history()
                    )
                }
                if (next_state.underload_history() ^ (prev_state.underload_history() << 1)) & !1 != 0 {
                    reject_query!(
                        "new underload history {}  is not compatible with the old underload history {}",
                        next_state.underload_history(), prev_state.underload_history()
                    )
                }
            }
            if next_state.total_balance() != &base.value_flow.to_next_blk {
                reject_query!(
                    "new state declares total balance {} different from to_next_blk in value flow \
                    (obtained by summing balances of all accounts in the new state): {}",
                    next_state.total_balance(), base.value_flow.to_next_blk
                )
            }
            log::debug!(
                target: "validate_query", 
                "checking total validator fees: new={}+recovered={} == old={}+collected={}",
                next_state.total_validator_fees(), base.value_flow.recovered,
                base.prev_validator_fees, base.value_flow.fees_collected
            );
            let mut new = base.value_flow.recovered.clone();
            new.add(next_state.total_validator_fees())?;
            let mut old = base.value_flow.fees_collected.clone();
            old.add(&base.prev_validator_fees)?;
            if new != old {
                reject_query!(
                    "new state declares total validator fees {} not equal to the sum of \
                    old total validator fees {} and the fees collected in this block {} \
                    minus the recovered fees {}",
                    next_state.total_validator_fees(), &base.prev_validator_fees,
                    base.value_flow.fees_collected, base.value_flow.recovered
                )
            }

            let mut copyleft_rewards = CopyleftRewards::new();
            if let Some(base_copyleft_rewards) = Arc::get_mut(&mut base.copyleft_rewards) {
                for (acc, reward) in base_copyleft_rewards {
                    copyleft_rewards.add_copyleft_reward(&acc, &reward)?;
                }
            }

            if copyleft_rewards != base.value_flow.copyleft_rewards {
                reject_query!(
                    "new state declares copyleft_rewards not equal to the calculated copyleft_rewards"
                )
            }
        } else {
            fail!("Prev state is not initialized in validator query")
        }
        // libraries:(HashmapE 256 LibDescr)
        if base.shard().is_masterchain() {
            Self::check_shard_libraries(base).map_err(
                |err| error!("the set of public libraries in the new state is invalid : {}", err)
            )?;
        } 
        let next_state = base.next_state.as_ref().ok_or_else(
            || error!("Next state is not initialized in validator query")
        )?.state();
        if !base.shard().is_masterchain() && !next_state.libraries().is_empty() {
            reject_query!(
                "new state contains a non-empty public library collection, \
                which is not allowed for non-masterchain blocks"
            )
        }
        // TODO: it seems was tested in unpack_next_state
        if base.shard().is_masterchain() && next_state.master_ref().is_some() {
            reject_query!("new state contains a masterchain block reference (master_ref)")
        } else if !base.shard().is_masterchain() && next_state.master_ref().is_none() {
            reject_query!("new state does not contain a masterchain block reference (master_ref)")
        }

        // custom:(Maybe ^McStateExtra) -> checked in check_mc_state_extra()
        // = ShardStateUnsplit;
        Ok(())
    }

    fn check_config_update(base: &ValidateBase) -> Result<()> {
        if base.next_state_extra.config.config_params.count(10000).is_err() {
            reject_query!("new configuration failed to pass letmated validity checks")
        }
        if base.prev_state_extra.config.config_params.count(10000).is_err() {
            reject_query!("old configuration failed to pass letmated validity checks")
        }
        if !base.next_state_extra.config.valid_config_data(false, None)? {
            reject_query!("new configuration parameters failed to pass per-parameter letmated validity checks, 
                or one of mandatory configuration parameters is missing")
        }
        let new_accounts = base.next_state.as_ref().ok_or_else(
            || error!("Next state is not initialized in validator query")
        )?.state().read_accounts()?;
        let old_config_root = match new_accounts.get(&base.prev_state_extra.config.config_addr)? {
            Some(account) => account.read_account()?.get_data(),
            None => reject_query!("cannot extract configuration from the new state of the (old) configuration smart contract {}",
                base.prev_state_extra.config.config_addr.to_hex_string())
        };
        let new_config_root = match new_accounts.get(&base.next_state_extra.config.config_addr)? {
            Some(account) => account.read_account()?.get_data(),
            None => reject_query!("cannot extract configuration from the new state of the (new) configuration smart contract {}",
                base.next_state_extra.config.config_addr.to_hex_string())
        };
        let cfg_acc_changed = base.prev_state_extra.config.config_addr != base.next_state_extra.config.config_addr;
        if old_config_root != new_config_root {
            reject_query!("the new configuration is different from that stored in the persistent data of the (new) configuration smart contract {}",
                base.prev_state_extra.config.config_addr.to_hex_string())
        }
        let old_config = base.prev_state_extra.config.clone();
        let new_config = base.next_state_extra.config.clone();
        if !old_config.valid_config_data(true, None)? {
            reject_query!("configuration extracted from (old) configuration smart contract {} \
                failed to pass per-parameter validity checks, or one of mandatory parameters is missing",
                old_config.config_addr.to_hex_string())
        }
        if new_config.important_config_parameters_changed(&old_config, false)? {
            // same as the check in Collator::create_mc_state_extra()
            log::warn!(target: "validate_query", "the global configuration changes in block {}", base.block_id());
            if !base.info.key_block() {
                reject_query!("important parameters in the global configuration have changed, but the block is not marked as a key block")
            }
        } else if base.info.key_block()
            && !(cfg_acc_changed || new_config.important_config_parameters_changed(&old_config, true)?) {
            reject_query!("no important parameters have been changed, but the block is marked as a key block")
        }
        let want_cfg_addr = match old_config.config(0)? {
            Some(ConfigParamEnum::ConfigParam0(param)) => param.config_addr,
            _ => if cfg_acc_changed {
                reject_query!("new state of old configuration smart contract {} \
                contains no value for parameter 0 (new configuration smart contract address), but the \
                configuration smart contract has been somehow changed to {}",
                    old_config.config_addr.to_hex_string(), new_config.config_addr.to_hex_string())
            } else {
                return Ok(())
            }
        };
        if want_cfg_addr == base.prev_state_extra.config.config_addr {
            if cfg_acc_changed {
                reject_query!("new state of old configuration smart contract {} \
                    contains the same value for parameter 0 (configuration smart contract address), but the \
                    configuration smart contract has been somehow changed to {}",
                        old_config.config_addr.to_hex_string(), new_config.config_addr.to_hex_string())
            }
            return Ok(())
        }
        if want_cfg_addr != new_config.config_addr && cfg_acc_changed {
            reject_query!("new state of old configuration smart contract {} contains {} \
                as the value for parameter 0 (new configuration smart contract address), but the \
                configuration smart contract has been somehow changed to a different value {}",
                    old_config.config_addr.to_hex_string(),
                    want_cfg_addr.to_hex_string(),
                    new_config.config_addr.to_hex_string())
        }
        // now old_cfg_addr = new_cfg_addr != want_cfg_addr
        // the configuration smart contract has not been switched to want_cfg_addr, have to check why
        let want_config_root = match new_accounts.get(&want_cfg_addr) {
            Ok(Some(account)) => account.read_account()?.get_data(),
            _ => {
                log::warn!(target: "validate_query", "switching of configuration smart contract did not happen \
                    because the suggested new configuration smart contract {} does not contain a valid configuration",
                        want_cfg_addr.to_hex_string());
                return Ok(())
            }
        };
        // if !base.config_params.valid_config_data(&base.prev_state_extra.config, true, false)? {
        let want_config = ConfigParams::with_address_and_params(want_cfg_addr.clone(), want_config_root);
        if !want_config.valid_config_data(false, None)? {
            log::warn!(target: "validate_query", "switching of configuration smart contract did not happen \
                because the configuration extracted from suggested new configuration smart contract {} \
                failed to pass per-parameter validity checks, or one of mandatory configuration parameters is missing",
                    want_cfg_addr.to_hex_string());
            return Ok(())
        }
        reject_query!("old configuration smart contract {} suggested {} \
            as the new configuration smart contract, but the switchover did not happen without a good \
            reason (the suggested configuration appears to be valid)",
            old_config.config_addr.to_hex_string(), want_cfg_addr.to_hex_string())
    }

    fn check_one_prev_dict_update(
        base: &ValidateBase,
        mc_data: &McData, seq_no: u32,
        old: Option<(KeyExtBlkRef, KeyMaxLt)>,
        new: Option<(KeyExtBlkRef, KeyMaxLt)>
    ) -> Result<bool> {
        let new_val = if old.is_some() {
            if new.is_some() {
                reject_query!("entry with seqno {} changed in the new previous blocks dictionary as compared to \
                    its old version (entries should never change once they have been added)", seq_no)
            } else {
                // if this becomes allowed in some situations, then check necessary conditions and return true
                reject_query!("entry with seqno {} disappeared in the new previous blocks dictionary as compared to \
                    the old previous blocks dictionary", seq_no)
            }
        } else if seq_no != mc_data.state.state().seq_no() {
            reject_query!("new previous blocks dictionary contains a new entry with seqno {} \
                while the only new entry must be for the previous block with seqno {}", seq_no, mc_data.state.state().seq_no())
        } else {
            new.expect("check scan_diff for Hashmap").0
        };
        log::debug!(target: "validate_query", "prev block id for {} is present", seq_no);
        let (end_lt, block_id, key) = new_val.master_block_id();
        if block_id.seq_no != seq_no {
            reject_query!("new previous blocks dictionary entry with seqno {} in fact describes a block {} with different seqno",
                seq_no, block_id)
        }
        if block_id != base.prev_blocks_ids[0] {
            reject_query!("new previous blocks dictionary has a new entry for previous block {} while the correct previous block is {}",
                block_id, base.prev_blocks_ids[0])
        }
        if end_lt != mc_data.state.state().gen_lt() {
            reject_query!("previous blocks dictionary has new entry for previous block {} \
                indicating end_lt={} but the correct value is {}",
                    block_id, end_lt, mc_data.state.state().gen_lt())
        }
        if key != mc_data.mc_state_extra.after_key_block {
            reject_query!("previous blocks dictionary has new entry for previous block {} indicating is_key_block={} but the correct value is {}",
                block_id, key, mc_data.mc_state_extra.after_key_block)
        }
        Ok(true)
    }

    // somewhat similar to Collator::create_mc_state_extra()
    fn check_mc_state_extra(&self, base: &ValidateBase, mc_data: &McData) -> Result<HashMap<AccountId, Grams>> {
        let prev_state = base.prev_state.as_ref().ok_or_else(
            || error!("Prev state is not initialized in validator query")
        )?.state();
        let next_state = base.next_state.as_ref().ok_or_else(
            || error!("Next state is not initialized in validator query")
        )?.state();
        if !base.shard().is_masterchain() {
            if next_state.custom_cell().is_some() {
                reject_query!(
                    "new state defined by non-masterchain block {} contains a McStateExtra", 
                    base.block_id()
                )
            }
            return Ok(Default::default())
        }
        let import_created = base.mc_extra.fees().root_extra().create.clone();
        log::debug!(
            target: "validate_query", 
            "checking header of McStateExtra in the new masterchain state"
        );
        if prev_state.custom_cell().is_none() {
            reject_query!("previous masterchain state did not contain a McStateExtra")
        }
        if next_state.custom_cell().is_none() {
            reject_query!("new masterchain state does not contain a McStateExtra")
        }
        // masterchain_state_extra#cc26
        // shard_hashes:ShardHashes has been checked separately
        // config:ConfigParams
        Self::check_config_update(base)?;
        // ...
        if base.next_state_extra.block_create_stats.is_some() != self.create_stats_enabled {
            reject_query!(
                "new McStateExtra has block_create_stats, \
                but active configuration defines create_stats_enabled={}",
                self.create_stats_enabled
            )
        }
        // validator_info:ValidatorInfo
        // (already checked in check_mc_validator_info())
        // prev_blocks_ids:OldMcBlocksInfo
        // comment this temporary if long test
        base.prev_state_extra.prev_blocks.scan_diff_with_aug(
            &base.next_state_extra.prev_blocks,
            |seq_no: u32, old, new| Self::check_one_prev_dict_update(base, mc_data, seq_no, old, new)
        ).map_err(|err| error!("invalid previous block dictionary in the new state : {}", err))?;
        if let Some((seq_no, _)) = base.prev_state_extra.prev_blocks.get_max(false)? {
            if seq_no >= mc_data.state.state().seq_no() {
                reject_query!(
                    "previous block dictionary for the previous state with seqno {} \
                    contains information about 'previous' masterchain block with seqno {}",
                    mc_data.state.state().seq_no(), seq_no
                )
            }
        }
        let (seq_no, _) = base.next_state_extra.prev_blocks.get_max(false)?.ok_or_else(
            || error!(
                "new previous blocks dictionary is empty \
                (at least the immediately previous block should be there)"
            )
        )?;
        CHECK!(base.block_id().seq_no == mc_data.state.block_id().seq_no() + 1);
        if seq_no > mc_data.state.state().seq_no() {
            reject_query!(
                "previous block dictionary for the new state with seqno {} \
                contains information about a future masterchain block with seqno {}",
                base.block_id().seq_no, seq_no
            )
        }
        if seq_no != mc_data.state.state().seq_no() {
            reject_query!(
                "previous block dictionary for the new state of masterchain block {} \
                does not contain information about immediately previous block with seqno {}",
                base.block_id(), mc_data.state.state().seq_no()
            )
        }
        // after_key_block:Bool
        if base.next_state_extra.after_key_block != base.info.key_block() {
            reject_query!(
                "new McStateExtra has after_key_block={} \
                while the block header claims is_master_state={}",
                base.next_state_extra.after_key_block, base.info.key_block()
            )
        }
        if base.prev_state_extra.last_key_block.is_some() && base.next_state_extra.last_key_block.is_none() {
            reject_query!("old McStateExtra had a non-trivial last_key_block, but the new one does not")
        }
        if base.next_state_extra.last_key_block == base.prev_state_extra.last_key_block {
            // TODO: check here
            if mc_data.mc_state_extra.after_key_block {
                reject_query!(
                    "last_key_block remains unchanged in the new masterchain state, but \
                    the previous block is a key block (it should become the new last_key_block)"
                )
            }
        } else if base.next_state_extra.last_key_block.is_none() {
            reject_query!(
                "last_key_block:(Maybe ExtBlkRef) changed in the new state, \
                but it became a nothing$0"
            )
        } else if let Some(ref last_key_block) = base.next_state_extra.last_key_block {
            let block_id = BlockIdExt::from_ext_blk(last_key_block.clone());
            if block_id != base.prev_blocks_ids[0] || last_key_block.end_lt != mc_data.state.state().gen_lt() {
                reject_query!(
                    "last_key_block has been set in the new masterchain state to {} with lt {}, \
                    but the only possible value for this update is the previous block {} \
                    with lt {}", block_id, last_key_block.end_lt, base.prev_blocks_ids[0], 
                    mc_data.state.state().gen_lt()
                )
            }
            if !mc_data.mc_state_extra.after_key_block {
                reject_query!(
                    "last_key_block has been updated to the previous block {}, \
                    but it is not a key block", block_id
                )
            }
        }
        if let Some(block_ref) = base.next_state_extra.last_key_block.clone() {
            let key_block_id = block_ref.master_block_id().1;
            if Some(&key_block_id) != mc_data.prev_key_block() {
                reject_query!(
                    "new masterchain state declares previous key block to be {:?} \
                    but the value computed from previous masterchain state is {:?}",
                    key_block_id, mc_data.prev_key_block()
                )
            }
        } else if let Some(last_key_block) = mc_data.prev_key_block() {
            reject_query!(
                "new masterchain state declares no previous key block, but the block header \
                announces previous key block seqno {}", last_key_block.seq_no
            )
        }
        if let Some(new_block_create_stats) = base.next_state_extra.block_create_stats.clone() {
            let old_block_create_stats = base.prev_state_extra.block_create_stats
                .clone().unwrap_or_default();
            if !base.is_fake && !self.check_block_create_stats(base, old_block_create_stats, new_block_create_stats)? {
                reject_query!("invalid BlockCreateStats update in the new masterchain state")
            }
        }
        let mut expected_global_balance = base.prev_state_extra.global_balance.clone();
        expected_global_balance.add(&base.value_flow.minted)?;
        expected_global_balance.add(&base.value_flow.created)?;
        expected_global_balance.add(&import_created)?;
        if base.next_state_extra.global_balance != expected_global_balance {
            reject_query!("global balance changed in unexpected way: expected old+minted+created+import_created = \
                {} + {} + {} + {} + {}, {}",
                base.prev_state_extra.global_balance, base.value_flow.minted, base.value_flow.created,
                import_created, expected_global_balance, base.next_state_extra.global_balance)
        }

        let mut workchains_coyleft_rewards = CopyleftRewards::default();
        base.mc_extra.fees().iterate_slices_with_keys_and_aug(|key, _created, _aug| {
            let shard = ShardIdent::with_tagged_prefix(key.workchain_id, key.prefix)?;
            let descr = base.mc_extra.shards().get_shard(&shard)?.ok_or_else(|| error!("ShardFees contains a record for shard {} \
                but there is no corresponding record in the new shard configuration", shard))?;
            workchains_coyleft_rewards.merge_rewards(&descr.descr.copyleft_rewards)?;
            Ok(true)
        })?;
        let mut expected_copyleft_rewards = base.prev_state_extra.state_copyleft_rewards.clone();
        let send_rewards = if let Ok(copyleft_config) = mc_data.config().copyleft_config() {
            let result = expected_copyleft_rewards.merge_rewards_with_threshold(&workchains_coyleft_rewards, &copyleft_config.copyleft_reward_threshold)?;
            result.into_iter().collect()
        } else {
            Default::default()
        };
        if !base.value_flow.copyleft_rewards.is_empty() {
            log::warn!("copyleft rewards in masterchain must be empty, real rewards count: {}",
                base.value_flow.copyleft_rewards.len()?
            )
        }
        if base.next_state_extra.state_copyleft_rewards != expected_copyleft_rewards {
            reject_query!("copyleft rewards unsplit changed in unexpected way")
        }

        // ...
        Ok(send_rewards)
    }

    fn check_counter_update(base: &ValidateBase, oc: &Counters, nc: &Counters, expected_incr: u64) -> Result<()> {
        let mut cc = oc.clone();
        if nc.is_zero() {
            if expected_incr != 0 {
                reject_query!("new counter total is zero, but the total should have been increased by {}", expected_incr)
            }
            if oc.is_zero() {
                return Ok(())
            }
            cc.increase_by(0, base.now());
            if !cc.almost_zero() {
                reject_query!("counter has been reset to zero, but it still has non-zero components after relaxation: {:?}; \
                    original value before relaxation was {:?}", cc, oc)
            }
            return Ok(())
        }
        if expected_incr == 0 {
            if oc == nc {
                return Ok(())
            } else {
                reject_query!("unnecessary relaxation of counter from {:?} to {:?} without an increment", oc, nc)
            }
        }
        if nc.total() < oc.total() {
            reject_query!("total counter goes back from {} to {} (increment by {} expected instead)",
                oc.total(), nc.total(), expected_incr)
        }
        if nc.total() != oc.total() + expected_incr {
            reject_query!("total counter has been incremented by {}, from {} to {} (increment by {} expected instead)",
                nc.total() - oc.total(), oc.total(), nc.total(), expected_incr)
        }
        if !cc.increase_by(expected_incr, base.now()) {
            reject_query!("old counter value {:?} cannot be increased by {}", oc, expected_incr)
        }
        if !cc.almost_equals(nc) {
            reject_query!("counter {:?} has been increased by {} with an incorrect resulting value {:?}; \
                correct result should be {:?} (up to +/-1 in the last two components)", oc, expected_incr, nc, cc);
        }
        Ok(())
    }

    fn check_one_block_creator_update(&self, base: &ValidateBase, key: &UInt256, old: Option<CreatorStats>, new: Option<CreatorStats>) -> Result<bool> {
        log::debug!(target: "validate_query", "checking update of CreatorStats for {}", key.to_hex_string());
        let (new, new_exists) = match new {
            Some(new) => (new, true),
            None => (CreatorStats::default(), false)
        };
        let old = old.unwrap_or_default();
        let (mc_incr, shard_incr) = if key.is_zero() {
            (!base.created_by.is_zero(), self.block_create_total)
        } else {
            (&base.created_by == key, self.block_create_count.get(key).cloned().unwrap_or(0))
        };
        Self::check_counter_update(base, old.mc_blocks(), new.mc_blocks(), mc_incr as u64)
        .map_err(|err| error!("invalid update of created masterchain blocks counter in CreatorStats for \
            {} : {}", key.to_hex_string(), err))?;
        Self::check_counter_update(base, old.shard_blocks(), new.shard_blocks(), shard_incr)
        .map_err(|err| error!("invalid update of created shardchain blocks counter in CreatorStats for \
            {} : {}", key.to_hex_string(), err))?;
        if new.mc_blocks().is_zero() && new.shard_blocks().is_zero() && new_exists {
            reject_query!("new CreatorStats for {} contains two zero counters \
                (it should have been completely deleted instead)", key.to_hex_string())
        }
        Ok(true)
    }

    // similar to Collator::update_block_creator_stats()
    fn check_block_create_stats(&self, base: &ValidateBase, old: BlockCreateStats, new: BlockCreateStats) -> Result<bool> {
        log::debug!(target: "validate_query", "checking all CreatorStats updates between the old and the new state");
        old.counters.scan_diff(
            &new.counters,
            |key: UInt256, old, new| self.check_one_block_creator_update(base, &key, old, new)
        ).map_err(|err| error!("invalid BlockCreateStats dictionary in the new state : {}", err))?;
        for (key, _count) in &self.block_create_count {
            let old_val = old.counters.get(key)?;
            let new_val = new.counters.get(key)?;
            if old_val.is_none() != new_val.is_none() || old_val == new_val {
                continue
            }
            if !self.check_one_block_creator_update(base, key, old_val, new_val)? {
                reject_query!("invalid update of BlockCreator entry for {}", key.to_hex_string())
            }
        }
        let key = UInt256::default();
        let old_val = old.counters.get(&key)?;
        let new_val = new.counters.get(&key)?;
        if new_val.is_none() && (!base.created_by.is_zero() || self.block_create_total != 0) {
            reject_query!("new masterchain state does not contain a BlockCreator entry with zero key with total statistics")
        }
        if !self.check_one_block_creator_update(base, &key, old_val, new_val)? {
            reject_query!("invalid update of BlockCreator entry for {}", key.to_hex_string())
        }
        Ok(true)
    }

    fn check_one_shard_fee(base: &ValidateBase, shard: &ShardIdent, fees: &CurrencyCollection, created: &CurrencyCollection) -> Result<bool> {
        let descr = base.mc_extra.shards().get_shard(shard)?.ok_or_else(|| error!("ShardFees contains a record for shard {} \
            but there is no corresponding record in the new shard configuration", shard))?;
        if descr.descr.reg_mc_seqno != base.block_id().seq_no {
            reject_query!("ShardFees contains a record for shard {} but the corresponding record in the shard configuration has not been updated by this block", shard)
        }
        if fees != &descr.descr.fees_collected {
            reject_query!("ShardFees record for shard {} contains fees_collected value {} different from that present in shard configuration {}",
                shard, fees, descr.descr.fees_collected)
        }
        if created != &descr.descr.funds_created {
            reject_query!("ShardFees record for shard {} contains funds_created value {} different from that present in shard configuration {}",
            shard, created, descr.descr.funds_created)
        }
        Ok(true)
    }

    fn check_mc_block_extra(base: &ValidateBase, _mc_data: &McData) -> Result<()> {
        log::debug!(target: "validate_query", "checking all CreatorStats updates between the old and the new state");
        if !base.shard().is_masterchain() {
            return Ok(())
        }
        // masterchain_block_extra#cca5
        // key_block:(## 1) -> checked in init_parse()
        // shard_hashes:ShardHashes -> checked in compute_next_state() and check_shard_layout()
        // shard_fees:ShardFees
        base.mc_extra.fees().iterate_slices_with_keys_and_aug(|key, mut created, aug| {
            let created = ShardFeeCreated::construct_from(&mut created)?;
            let shard = ShardIdent::with_tagged_prefix(key.workchain_id, key.prefix)?;
            if created != aug || !Self::check_one_shard_fee(base, &shard, &created.fees, &created.create)? {
                reject_query!("ShardFees entry with key {:x} corresponding to shard {} is invalid",
                    key, shard)
            }
            Ok(true)
        })?;
        let fees_imported = base.mc_extra.fees().root_extra().fees.clone();
        if fees_imported != base.value_flow.fees_imported {
            reject_query!("invalid fees_imported in value flow: declared {}, correct value is {}",
                base.value_flow.fees_imported, fees_imported)
        }
        // ^[ prev_blk_signatures:(HashmapE 16 CryptoSignaturePair)
        if !base.mc_extra.prev_blk_signatures().is_empty() && base.block_id().seq_no == 1 {
            reject_query!("block contains non-empty signature set for the zero state of the masterchain")
        }
        if base.block_id().seq_no > 1 {
            if !base.mc_extra.prev_blk_signatures().is_empty() {
            // TODO: check signatures here
            } else if !base.is_fake && false {  // FIXME: remove "&& false" when collator serializes signatures
                reject_query!("block contains an empty signature set for the previous block")
            }
        }
        //   recover_create_msg:(Maybe ^InMsg)
        //   mint_msg:(Maybe ^InMsg) ]
        // config:key_block?ConfigParams -> checked in compute_next_state() and ???
        Ok(())
    }

/*
 * 
 *   MAIN VALIDATOR FUNCTION
 *     (invokes other methods in a suitable order)
 * 
 */

    async fn common_preparation(&mut self) -> Result<(ValidateBase, McData)> {
        let mut base = self.init_base()?;
        let mc_data = self.init_mc_data(&mut base).await?;
        // stage 0
        self.compute_next_state(&mut base, &mc_data)?;
        self.unpack_prev_state(&mut base)?;
        self.unpack_next_state(&mut base, &mc_data)?;
        base.config_params = mc_data.state().config_params()?.clone();
        base.capabilities = base.config_params.capabilities();
        Self::load_block_data(&mut base)?;
        Ok((base, mc_data))
    }

    fn add_task(tasks: &mut Vec<Box<dyn FnOnce() -> Result<()> + Send + 'static>>, task: impl FnOnce() -> Result<()> + Send + 'static) {
        tasks.push(Box::new(task))
    }

    async fn run_tasks(&self, tasks: Vec<Box<dyn FnOnce() -> Result<()> + Send + 'static>>) -> Result<()> {
        if self.multithread {
            let tasks = tasks.into_iter().map(|t| tokio::task::spawn_blocking(t));
            futures::future::join_all(tasks).await
                .into_iter().find(|r| match r {
                    Err(_) => true,
                    Ok(Err(_)) => true,
                    Ok(Ok(_)) => false,
                })
                .unwrap_or(Ok(Ok(())))??;
        } else {
            for task in tasks {
                task()?;
            }
        }
        Ok(())
    }

    async fn validate(&mut self) -> Result<ValidateBase> {
        let (base, mc_data) = self.common_preparation().await?;

        let manager = self.init_output_queue_manager(&mc_data, &base).await?;
        self.check_shard_layout(&base, &mc_data)?;
        check_cur_validator_set(
            &self.validator_set,
            base.block_id(),
            base.shard(),
            mc_data.mc_state_extra(),
            &self.old_mc_shards,
            &base.config_params,
            base.now(),
            base.is_fake
        )?;
        self.check_utime_lt(&base, &mc_data)?;
        // stage 1
        // log::debug!(target: "validate_query", "running letmated validity checks for block candidate {}", base.block_id());
        // if (!block::gen::t_Block.validate_ref(1000000, block_root_)) {
        //     reject_query!("block ",  id_ + " failed to pass letmated validity checks");
        // }

        let base = Arc::new(base);
        let manager = Arc::new(manager);
        let mut tasks = vec![];
        let b = base.clone();
        Self::add_task(&mut tasks, move || Self::precheck_value_flow(b));
        let b = base.clone();
        Self::add_task(&mut tasks, move || Self::precheck_account_updates(b));
        Self::precheck_account_transactions(base.clone(), &mut tasks)?;
        Self::precheck_message_queue_update(base.clone(), &manager, &mut tasks)?;

        Self::check_in_msg_descr(base.clone(), manager.clone(), &mut tasks)?;
        Self::check_out_msg_descr(base.clone(), manager.clone(), &mut tasks)?;
        Self::check_transactions(base.clone(), mc_data.libraries().clone(), &mut tasks)?;

        self.run_tasks(tasks).await?;

        let mut base = Arc::try_unwrap(base)
            .map_err(|base| error!("Somebody haven't released Arc: strong: {} weak: {}",
                Arc::strong_count(&base), Arc::weak_count(&base)))?;

        Self::check_processed_upto(&base, &manager, &mc_data)?;
        Self::check_in_queue(&base, &manager)?;
        Self::check_delivered_dequeued(&base, &manager)?;
        Self::check_all_ticktock_processed(&base)?;
        Self::check_message_processing_order(&mut base)?;
        Self::check_new_state(&mut base, &mc_data, &manager)?;
        Self::check_mc_block_extra(&base, &mc_data)?;
        let sent_rewards = self.check_mc_state_extra(&base, &mc_data)?;
        Self::check_special_messages(&base, &sent_rewards)?;

        Ok(base)
    }

    pub async fn try_validate(mut self) -> Result<()> {
        let block_id = self.block_candidate.block_id.clone();
        log::trace!("VALIDATE {}", block_id);
        let now = std::time::Instant::now();

        let result = self.validate().await;
        let duration = now.elapsed().as_millis() as u64;
        let base = result.map_err(|e| {
            log::warn!("VALIDATION FAILED {} TIME {}ms ERR {}", block_id, duration, e);
            e
        })?;

        let gas_used = base.gas_used.load(Ordering::Relaxed);
        let ratio = gas_used.checked_div(duration).unwrap_or(gas_used);
        log::info!("ASYNC VALIDATED {} TIME {}ms GAS_RATE: {}", base.block_id(), duration, ratio);

        #[cfg(feature = "metrics")]
        STATSD.gauge(&format!("gas_rate_validator_{}", base.block_id().shard()), ratio as f64);

        #[cfg(feature = "telemetry")]
        self.engine.validator_telemetry().succeeded_attempt(
            &self.shard,
            now.elapsed(),
            base.transactions_executed.load(Ordering::Relaxed),
            gas_used as u32
        );

        Ok(())
    }
}


impl ValidateQuery {
    fn prepare_transaction_for_log(
        account_before: &Cell,
        account_after: &Cell,
        config: &ConfigParams,
        trans: &Transaction,
        trans_execute: &Transaction
    ) -> Result<()> {
        log::trace!(target: "validate_reject",
            "acc_before: {}\nacc_after: {}\nconfig: {}\ntrans_origin: {}\ntrans_execute: {}",
            base64::encode(ton_types::serialize_toc(&account_before)?),
            base64::encode(ton_types::serialize_toc(&account_after)?),
            base64::encode(config.write_to_bytes()?),
            base64::encode(trans.write_to_bytes()?),
            base64::encode(trans_execute.write_to_bytes()?)
        );
        Ok(())
    }
}
