use crate::{
    CHECK,
    block::BlockStuff,
    engine_traits::EngineOperations,
    error::NodeError,
    out_msg_queue::MsgQueueManager,
    shard_state::ShardStateStuff,
    types::{
        messages::MsgEnqueueStuff,
        top_block_descr::{TopBlockDescrStuff, Mode as TopBlockDescrMode},
    },
    validating_utils::{
        check_this_shard_mc_info, check_cur_validator_set, may_update_shard_block_info,
        supported_version, supported_capabilities,
    },
};
use std::{collections::HashMap, io::Cursor, ops::Deref, sync::{atomic::AtomicU64, Arc}};
use ton_block::{
    AddSub, BlockError, UnixTime32, HashmapAugType, Deserializable, Serializable,
    CurrencyCollection,
    ConfigParams, ConfigParamEnum,
    BlockExtra, BlockInfo, BlockIdExt,
    KeyExtBlkRef,
    McBlockExtra, McShardRecord, McStateExtra,
    MerkleUpdate,
    BlockCreateStats, U15, DepthBalanceInfo,
    ShardIdent,
    ValidatorSet,
    TopBlockDescrSet,
    ValueFlow, WorkchainDescr, Counters, CreatorStats, AccountIdPrefixFull,
    OutMsg,
    Message, MsgEnvelope, EnqueuedMsg, ShardHashes, MerkleProof, StateInitLib,
    KeyMaxLt, LibDescr, Transaction,
    InMsg, MsgAddressInt, MsgAddressIntOrNone, IntermediateAddress,
    InMsgDescr, OutMsgDescr, ShardAccount, ShardAccountBlocks, ShardAccounts,
    Account, AccountBlock, AccountStatus,
    ShardFeeCreated, TransactionDescr, OutMsgQueueKey, BlockLimits, Libraries,
    GlobalCapabilities,
};
use ton_executor::{
    BlockchainConfig, CalcMsgFwdFees, OrdinaryTransactionExecutor, TickTockTransactionExecutor, 
    TransactionExecutor
};
use ton_types::{
    Result,
    deserialize_cells_tree,
    AccountId, Cell, CellType, HashmapType, SliceData, UInt256,
};

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

#[derive(Default, Debug)]
pub struct WorkchainInfo {
    active: bool,
    zerostate_root_hash: UInt256,
    zerostate_file_hash: UInt256,
}

#[derive(Clone, Default, Debug)]
pub struct BlockCandidate {
    pub block_id: BlockIdExt,
    pub data: Vec<u8>,
    pub collated_data: Vec<u8>,
    pub collated_file_hash: UInt256,
    pub created_by: UInt256,
}

pub struct ValidatorQuery {
    global_id: i32,
    // current state of blockchain
    shard: ShardIdent,
    min_mc_seq_no: u32,
    // block_id: BlockIdExt,
    block_candidate: BlockCandidate,
    // other
    validator_set: ValidatorSet,
    is_fake: bool,
    after_merge: bool,
    after_split: bool,
    mc_blkid: BlockIdExt,
    // data from block_candidate
    block: BlockStuff,
    info: BlockInfo,
    extra: BlockExtra,
    in_msg_descr: InMsgDescr,
    out_msg_descr: OutMsgDescr,
    account_blocks: ShardAccountBlocks,
    value_flow: ValueFlow,
    mc_extra: McBlockExtra,
    state_update: MerkleUpdate,
    // previous state can be as two states for merge
    prev_blocks_ids: Vec<BlockIdExt>,
    prev_state: ShardStateStuff, // TODO: remove
    prev_libraries: Libraries,
    prev_states: Vec<ShardStateStuff>,
    prev_state_extra: McStateExtra,
    prev_state_accounts: ShardAccounts,
    prev_validator_fees: CurrencyCollection,
    prev_key_block_seqno: u32,
    prev_key_block: Option<BlockIdExt>,
    old_mc_shards: ShardHashes, // old_shard_conf_
    // master chain state members
    mc_state: ShardStateStuff,
    mc_state_extra: McStateExtra,
    config: BlockchainConfig,
    config_params: ConfigParams,
    block_limits: BlockLimits,
    // new state after applying block_candidate
    next_state: ShardStateStuff,
    next_libraries: Libraries,
    next_state_accounts: ShardAccounts,
    new_mc_shards: ShardHashes, // new_shard_conf_
    // from next_state of masterchain or from global masterchain for workcain
    next_state_extra: McStateExtra,
    // from collated_data
    top_shard_descr_dict: TopBlockDescrSet,
    virt_roots: HashMap<UInt256, Cell>, // never used proofs of processed messages
    output_queue_manager: MsgQueueManager,
    // temp
    update_shard_cc: bool,

    import_created: CurrencyCollection,
    create_stats_enabled: bool,
    block_create_total: u64,
    block_create_count: HashMap<UInt256, u64>,

    engine: Arc<dyn EngineOperations>,

    lib_publishers: Vec<(UInt256, UInt256, bool)>,
    lib_publishers2: Vec<(UInt256, UInt256, bool)>, // from check_shard_libraries

    recover_create_msg: Option<InMsg>,
    mint_msg: Option<InMsg>,
    msg_proc_lt: Vec<(UInt256, u64, u64)>,

    min_shard_ref_mc_seqno: u32,
    max_shard_utime: u32,
    max_shard_lt: u64,
    claimed_proc_lt: u64,
    claimed_proc_hash: UInt256,
    proc_lt: u64,
    proc_hash: UInt256,
    min_enq_lt: u64,
    min_enq_hash: UInt256,
}

impl ValidatorQuery {
    fn shard(&self) -> &ShardIdent {
        &self.shard
    }
    fn now(&self) -> u32 {
        self.info.gen_utime().0
    }
    fn block_id(&self) -> &BlockIdExt {
        &self.block_candidate.block_id
    }
    fn created_by(&self) -> &UInt256 {
        &self.block_candidate.created_by
    }
    pub fn new(
        shard: ShardIdent,
        _min_ts: UnixTime32,
        min_mc_seq_no: u32,
        prev_blocks_ids: Vec<BlockIdExt>,
        block_candidate: BlockCandidate,
        validator_set: ValidatorSet,
        engine: Arc<dyn EngineOperations>,
        _timeout: u128,
        is_fake: bool
    ) -> Self {
        Self {
            engine,
            global_id: 0,
            shard,
            min_mc_seq_no,
            block_candidate,
            validator_set,
            is_fake,
            after_merge: Default::default(),
            after_split: Default::default(),
            mc_blkid: Default::default(),
            block: Default::default(),
            info: Default::default(),
            extra: Default::default(),
            in_msg_descr: Default::default(),
            out_msg_descr: Default::default(),
            account_blocks: Default::default(),
            value_flow: Default::default(),
            mc_extra: Default::default(),
            state_update: Default::default(),
            prev_blocks_ids,
            prev_state: Default::default(),
            prev_libraries: Default::default(),
            prev_states: vec![],
            prev_state_extra: Default::default(),
            prev_state_accounts: Default::default(),
            prev_validator_fees: Default::default(),
            prev_key_block_seqno: Default::default(),
            prev_key_block: Default::default(),
            old_mc_shards: Default::default(),
            mc_state: Default::default(),
            mc_state_extra: Default::default(),
            config: Default::default(),
            config_params: Default::default(),
            block_limits: Default::default(),
            // new state after applying block_candidate
            next_state: Default::default(),
            next_libraries: Default::default(),
            next_state_accounts: Default::default(),
            new_mc_shards: Default::default(),
            next_state_extra: Default::default(),
            top_shard_descr_dict: Default::default(),
            virt_roots: Default::default(),
            output_queue_manager: Default::default(),
            update_shard_cc: Default::default(),
            import_created: Default::default(),
            create_stats_enabled: Default::default(),
            block_create_total: Default::default(),
            block_create_count: Default::default(),
            lib_publishers: Default::default(),
            lib_publishers2: Default::default(),
            recover_create_msg: Default::default(),
            mint_msg: Default::default(),
            msg_proc_lt: Default::default(),
            min_shard_ref_mc_seqno: std::u32::MAX,
            max_shard_utime: std::u32::MIN,
            max_shard_lt: std::u64::MIN,
            claimed_proc_lt: std::u64::MAX,
            claimed_proc_hash: UInt256::from([0xFF; 32]),
            proc_lt: std::u64::MIN,
            proc_hash: UInt256::from([0xFF; 32]),
            min_enq_lt: std::u64::MAX,
            min_enq_hash: UInt256::from([0xFF; 32]),
        }
    }

/*
 * 
 *   INITIAL PARSE & LOAD REQUIRED DATA
 * 
 */

    async fn start_up(&mut self) -> Result<()> {
        log::info!(target: "validator", "validate query for {:#} started", self.block_id());
        if self.block_id().shard() != self.shard() {
            soft_reject_query!("block candidate belongs to shard {} different from current shard {}",
                self.block_id().shard(), self.shard())
        }
        if !self.shard().is_masterchain() && !self.shard().is_base_workchain() {
            soft_reject_query!("can validate block candidates only for masterchain (-1) and base workchain (0)")
        }
        if self.shard().is_masterchain() && self.prev_blocks_ids.is_empty() {
            self.min_mc_seq_no = 0
        }
        match self.prev_blocks_ids.len() {
            2 => {
                if self.shard().is_masterchain() {
                    soft_reject_query!("cannot merge shards in masterchain")
                }
                if !(self.shard().is_parent_for(&self.prev_blocks_ids[0].shard()) && self.shard().is_parent_for(&self.prev_blocks_ids[1].shard())
                    && (self.prev_blocks_ids[0].shard().shard_prefix_with_tag() < self.prev_blocks_ids[1].shard().shard_prefix_with_tag())) {
                    soft_reject_query!("the two previous blocks for a merge operation are not siblings or are not children of current shard")
                }
                for blk in &self.prev_blocks_ids {
                    if blk.seq_no == 0 {
                        soft_reject_query!("previous blocks for a block merge operation must have non-zero seqno")
                    }
                }
                self.after_merge = true;
            }
            1 => {
                // creating next block
                if self.prev_blocks_ids[0].shard() != self.shard() {
                    self.after_split = true;
                    if !self.prev_blocks_ids[0].shard().is_parent_for(self.shard()) {
                        soft_reject_query!("previous block does not belong to the shard we are generating a new block for")
                    }
                    if self.shard().is_masterchain() {
                        soft_reject_query!("cannot split shards in masterchain")
                    }
                }
                if self.shard().is_masterchain() && self.min_mc_seq_no > self.prev_blocks_ids[0].seq_no {
                    soft_reject_query!("cannot refer to specified masterchain block {} \
                        because it is later than {} the immediately preceding masterchain block",
                            self.min_mc_seq_no, self.prev_blocks_ids[0].seq_no)
                }
            }
            0 => soft_reject_query!("must have one or two previous blocks to generate a next block"),
            _ => soft_reject_query!("cannot have more than two previous blocks")
        }
        // 2. learn latest masterchain state and block id
        log::debug!(target: "validator", "sending get_top_masterchain_state_block() to Manager");
        self.get_latest_mc_state().await?;

        // 4. unpack block candidate (while necessary data is being loaded)
        self.unpack_block_candidate()?;
        // 3. load state(s) corresponding to previous block(s)
        for i in 0..self.prev_blocks_ids.len() {
            log::debug!(target: "validator", "load state for prev block {} of {} {}", i + 1, self.prev_blocks_ids.len(), self.prev_blocks_ids[i]);
            let prev_state = self.load_prev_shard_state(i).await?;
            self.prev_states.push(prev_state);
        }
        // 5. request masterchain state referred to in the block
        if !self.shard().is_masterchain() {
            // TODO: check here reload latest mc state
            self.process_mc_state(self.engine.load_state(&self.mc_blkid).await?)?;

            // 5.1. request corresponding block handle
            let handle = self.engine.load_block_handle(&self.mc_blkid)?;
            if !self.is_fake && !handle.proof_inited() && handle.id().seq_no() != 0 {
                reject_query!("reference masterchain block {} for block {} does not have a valid proof",
                    handle.id(), self.block_id())
            }
        } else if self.prev_blocks_ids[0] != self.mc_blkid {
            soft_reject_query!("cannot validate masterchain block {} because it refers to masterchain \
                block {} but its (expected) previous block is {}",
                    self.block_id(), self.mc_blkid, self.prev_blocks_ids[0])
        }
        Ok(())
    }

    // unpack block candidate, and check root hash and file hash
    fn unpack_block_candidate(&mut self) -> Result<()> {
        CHECK!(!self.block_candidate.data.is_empty());
        // 1. deserialize block itself
        let data = std::mem::replace(&mut self.block_candidate.data, Vec::new());
        self.block = BlockStuff::new(self.block_id().clone(), data)?;
        // 3. initial block parse
        self.init_parse()?;
        // ...
        self.extract_collated_data()
    }

    // init_parse
    fn init_parse(&mut self) -> Result<()> {
        self.global_id = self.block.block().global_id();
        self.info = self.block.block().read_info()?;
        let block_id = BlockIdExt::from_ext_blk(self.info.read_master_id()?);
        CHECK!(block_id.shard_id.is_masterchain());
        let prev_blocks_ids = self.info.read_prev_ids()?;

        if prev_blocks_ids.len() != self.prev_blocks_ids.len() {
            soft_reject_query!("block header declares {} previous blocks, but we are given {}",
                prev_blocks_ids.len(), self.prev_blocks_ids.len())
        }
        for (i, blk) in self.prev_blocks_ids.iter().enumerate() {
            if &prev_blocks_ids[i] != blk {
                soft_reject_query!("previous block #{} mismatch: expected {}, found in header {}",
                    i + 1, blk, prev_blocks_ids[i]);
            }
        }
        if self.info.after_split() != self.after_split {
            // ??? impossible
            reject_query!("after_split mismatch in block header")
        }
        if self.info.shard() != self.shard() {
            reject_query!("shard mismatch in the block header")
        }
        self.state_update = self.block.block().read_state_update()?;
        self.value_flow = self.block.block().read_value_flow()?;

        if self.info.key_block() {
            log::info!(target: "validator", "validating key block {}", self.block_id());
        }
        if self.info.start_lt() >= self.info.end_lt() {
            reject_query!("block has start_lt greater than or equal to end_lt")
        }
        if self.info.shard().is_masterchain() && (self.info.after_merge() || self.info.before_split() || self.info.after_split()) {
            reject_query!("block header declares split/merge for a masterchain block")
        }
        if self.info.after_merge() && self.info.after_split() {
            reject_query!("a block cannot be both after merge and after split at the same time")
        }
        if self.info.after_split() && self.shard().is_full() {
            reject_query!("a block with empty shard prefix cannot be after split")
        }
        if self.info.after_merge() && !self.shard().can_split() {
            reject_query!("a block split 60 times cannot be after merge")
        }
        if self.info.key_block() && !self.shard().is_masterchain() {
            reject_query!("a non-masterchain block cannot be a key block")
        }
        if self.info.vert_seqno_incr() != 0 {
            // what about non-masterchain blocks?
            reject_query!("new blocks cannot have vert_seqno_incr set")
        }
        if self.info.after_merge() != self.after_merge {
            reject_query!("after_merge value mismatch in block header")
        }
        self.extra = self.block.block().read_extra()?;
        if self.created_by() != self.extra.created_by() {
            reject_query!("block candidate {} has creator {:x} but the block header contains different value {:x}",
                self.block_id(), self.created_by(), self.extra.created_by())
        }
        if self.shard().is_masterchain() {
            self.mc_extra = self.extra.read_custom()?
                .ok_or_else(|| error!("masterchain block candidate without McBlockExtra"))?;
            if self.mc_extra.is_key_block() != self.info.key_block() {
                reject_query!("key_block flag mismatch in BlockInfo and McBlockExtra")
            }
            if self.info.key_block() && self.mc_extra.config().is_none() {
                reject_query!("key_block must contain ConfigParams in McBlockExtra")
            }
            self.recover_create_msg = self.mc_extra.read_recover_create_msg()?;
            self.mint_msg = self.mc_extra.read_mint_msg()?;
        } else if self.extra.is_key_block() {
            reject_query!("non-masterchain block cannot have McBlockExtra")
        }
        // ...
        Ok(())
    }

    // extract_collated_data_from
    fn extract_collated_data_from(&mut self, croot: Cell, idx: usize) -> Result<()> {
        match croot.cell_type() {
            CellType::Ordinary => match TopBlockDescrSet::construct_from_cell(croot) {
                Ok(descr) => if self.top_shard_descr_dict.is_empty() {
                    log::debug!(target: "validator", "collated datum #{} is a TopBlockDescrSet", idx);
                    self.top_shard_descr_dict = descr;
                    self.top_shard_descr_dict.count(10000)
                        .map_err(|err| error!("invalid TopBlockDescrSet : {}", err))?;
                } else {
                    reject_query!("duplicate TopBlockDescrSet in collated data")
                }
                Err(err) => if let Some(BlockError::InvalidConstructorTag{ t, s: _ }) = err.downcast_ref() {
                    log::warn!(target: "validator", "collated datum # {} has unknown type (magic {:X}), ignoring", idx, t);
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
                log::debug!(target: "validator", "collated datum # {} is a Merkle proof with root hash {}",
                    idx, virt_root_hash.to_hex_string());
                if self.virt_roots.insert(virt_root_hash.clone(), virt_root).is_some() {
                    reject_query!("Merkle proof with duplicate virtual root hash {}", virt_root_hash.to_hex_string());
                }
            }
            _ => reject_query!("it is a special cell, but not a Merkle proof root")
        }
        Ok(())
    }

    // processes further and sorts data in collated_roots
    fn extract_collated_data(&mut self) -> Result<()> {
        if !self.block_candidate.collated_data.is_empty() {
            // 8. deserialize collated data
            let collated_roots = match deserialize_cells_tree(&mut Cursor::new(&self.block_candidate.collated_data)) {
                Ok(cells) => cells,
                Err(err) => reject_query!("cannot deserialize collated data: {}", err)
            };
            // 9. extract/classify collated data
            for i in 0..collated_roots.len() {
                let croot = collated_roots[i].clone();
                self.extract_collated_data_from(croot, i)?;
            }
        }
        Ok(())
    }

    async fn get_latest_mc_state(&mut self) -> Result<()> {
        self.mc_state = self.engine.load_last_applied_mc_state().await?;
        log::debug!(target: "validator", "in ValidateQuery::get_latest_mc_state() {}", self.mc_state.block_id());
        CHECK!(&self.mc_blkid, default);
        self.mc_blkid = self.mc_state.block_id().clone();
        if self.mc_state.state().seq_no() < self.min_mc_seq_no {
            reject_query!("requested to validate a block referring to an unknown future masterchain block {} < {}",
                self.mc_state.state().seq_no(), self.min_mc_seq_no)
        }
        Ok(())
    }

    async fn load_prev_shard_state(&mut self, idx: usize) -> Result<ShardStateStuff> {
        log::debug!(target: "validator", "in ValidateQuery::load_prev_shard_state({})", idx);
        // got state of previous block #i
        let state = self.engine.load_state(&self.prev_blocks_ids[idx]).await?;
        CHECK!(&state, inited);
        if self.shard().is_masterchain() {
            CHECK!(idx == 0);
            if self.prev_blocks_ids[0] != self.mc_blkid {
                reject_query!("impossible situation: previous block {} is not the block {} \
                    referred to by the current block", self.prev_blocks_ids[0], self.mc_blkid)
            }
            self.process_mc_state(state.clone())?;
        }
        Ok(state)
    }

    fn process_mc_state(&mut self, mc_state: ShardStateStuff) -> Result<()> {
        if &self.mc_blkid != mc_state.block_id() {
            reject_query!("reference masterchain state for {} is in fact for different block {} {}",
                self.mc_blkid, mc_state.state().shard(), mc_state.state().seq_no())
        }
        CHECK!(!self.prev_blocks_ids.is_empty());
        if &self.mc_blkid != mc_state.block_id() {
            self.mc_state_extra.prev_blocks.check_block(mc_state.block_id())
            .map_err(|err| error!("attempting to register masterchain state for block {} \
                which is not an ancestor of most recent masterchain block {} : {}",
                    mc_state.block_id(), self.mc_state.block_id(), err))?;
        }
        self.engine.set_aux_mc_state(&mc_state)?;
        self.mc_state = mc_state;
        self.try_unpack_mc_state()?;
        Ok(())
    }

    // TODO: investigate why overwrite
    fn try_unpack_mc_state(&mut self) -> Result<()> {
        log::debug!(target: "validator", "unpacking reference masterchain state {}", self.mc_blkid);
        // TODO: try to simplify here
        if self.mc_state == ShardStateStuff::default() {
            reject_query!("no previous masterchain state present")
        }
        self.mc_state_extra = self.mc_state.shard_state_extra()?.clone();
        self.config_params = self.mc_state_extra.config.clone();
        CHECK!(&self.config_params, inited);
        // ihr_enabled_ = self.config_params->ihr_enabled();
        self.create_stats_enabled = self.mc_state_extra.config().has_capability(GlobalCapabilities::CapCreateStatsEnabled);
        if self.config_params.has_capabilities() && (self.config_params.capabilities() & !supported_capabilities()) != 0 {
            log::error!(target: "validator", "block generation capabilities {} have been enabled in global configuration, \
                but we support only {} (upgrade validator software?)",
                    self.config_params.capabilities(), supported_capabilities());
        }
        if self.config_params.global_version() > supported_version() {
            log::error!(target: "validator", "block version {} have been enabled in global configuration, \
                but we support only {} (upgrade validator software?)",
                    self.config_params.global_version(), supported_version());
        }
        self.old_mc_shards = self.mc_state_extra.shards().clone();
        self.new_mc_shards = if self.shard().is_masterchain() {
            self.mc_extra.shards().clone()
        } else {
            self.old_mc_shards.clone()
        };
        if self.global_id != self.mc_state.shard_state().global_id() {
            reject_query!("blockchain global id mismatch: new block has {} while the masterchain configuration expects {}",
                self.global_id, self.mc_state.shard_state().global_id())
        }
        CHECK!(&self.info, inited);
        if self.info.vert_seq_no() != self.mc_state.shard_state().vert_seq_no() {
            reject_query!("vertical seqno mismatch: new block has {} while the masterchain configuration expects {}",
                self.info.vert_seq_no(), self.mc_state.shard_state().vert_seq_no())
        }
        if self.mc_state_extra.after_key_block {
            self.prev_key_block_seqno = self.mc_state.block_id().seq_no();
            self.prev_key_block = Some(self.mc_state.block_id().clone());
        } else if let Some(block_ref) = self.mc_state_extra.last_key_block.clone() {
            self.prev_key_block_seqno = block_ref.seq_no;
            self.prev_key_block = Some(block_ref.master_block_id().1);
        } else {
            self.prev_key_block_seqno = 0;
            self.prev_key_block = None;
        }
        if self.info.prev_key_block_seqno() != self.prev_key_block_seqno {
            reject_query!("previous key block seqno value in candidate block header is {} \
                while the correct value corresponding to reference masterchain state {} is {}",
                    self.info.prev_key_block_seqno(), self.mc_blkid, self.prev_key_block_seqno)
        }
        self.block_limits = self.config_params.block_limits(self.shard().is_masterchain())?;
        if !self.shard().is_masterchain() {
            check_this_shard_mc_info(
               self.shard(),
               self.block_id(),
               self.after_merge,
               self.after_split,
               self.info.before_split(),
               &self.prev_blocks_ids,
               &self.config_params,
               &self.mc_state_extra,
               true,
               self.now()
            ).map_err(|e| error!("masterchain configuration does not admit creating block {}: {}", self.block_id(), e))?;
        }
        Ok(())
    }

/*
 * 
 *  METHODS CALLED FROM try_validate() stage 0
 * 
 */

    fn compute_next_state(&mut self) -> Result<()> {
        self.prev_state = self.prev_states[0].clone();
        let prev_state_root = if self.after_merge && self.prev_states.len() == 2 {
            self.check_one_prev_state(&self.prev_states[0])?;
            self.check_one_prev_state(&self.prev_states[1])?;
            let left  = self.prev_states[0].root_cell().clone();
            let right = self.prev_states[1].root_cell().clone();
            ShardStateStuff::construct_split_root(left, right)?
        } else {
            CHECK!(self.prev_states.len(), 1);
            self.check_one_prev_state(&self.prev_states[0])?;
            self.prev_states[0].root_cell().clone()
        };
        log::debug!(target: "validator", "computing next state");
        let next_state_root = self.state_update.apply_for(&prev_state_root)
            .map_err(|err| error!("cannot apply Merkle update from block to compute new state : {}", err))?;
        log::debug!(target: "validator", "next state computed");
        self.next_state = ShardStateStuff::new(self.block_id().clone(), next_state_root.clone())?;
        if self.info.end_lt() != self.next_state.state().gen_lt() {
            reject_query!("new state contains generation lt {} distinct from end_lt {} in block header",
                self.next_state.state().gen_lt(), self.info.end_lt())
        }
        if self.now() != self.next_state.state().gen_time() {
            reject_query!("new state contains generation time {} distinct from the value {} in block header",
                self.next_state.state().gen_time(), self.now())
        }
        if self.info.before_split() != self.next_state.state().before_split() {
            reject_query!("before_split value mismatch in new state and in block header")
        }
        if (self.block_id().seq_no != self.next_state.state().seq_no()) || (self.shard() != self.next_state.state().shard()) {
            reject_query!("header of new state claims it belongs to block {} instead of {}",
                self.next_state.state().shard(), self.block_id().shard())
        }
        if self.next_state.state().custom_cell().is_some() != self.shard().is_masterchain() {
            reject_query!("McStateExtra in the new state of a non-masterchain block, or conversely")
        }
        if self.shard().is_masterchain() {
            self.prev_state_extra = self.prev_state.shard_state_extra()?.clone();
            self.next_state_extra = self.next_state.shard_state_extra()?.clone();
            let next_state_extra = self.next_state.shard_state_extra()?;
            if next_state_extra.shards() != self.mc_extra.shards() {
                reject_query!("ShardHashes in the new state and in the block differ")
            }
            if self.info.key_block() {
                CHECK!(self.mc_extra.config().is_some());
                if let Some(config) = self.mc_extra.config() {
                    if config != &next_state_extra.config {
                        reject_query!("ConfigParams in the header of the new key block and in the new state differ")
                    }
                }
            }
        } else {
            self.prev_state_extra = self.mc_state.shard_state_extra()?.clone();
            self.next_state_extra = self.mc_state.shard_state_extra()?.clone();
        }
        Ok(())
    }

    // similar to Collator::unpack_one_last_state()
    fn check_one_prev_state(&self, ss: &ShardStateStuff) -> Result<()> {
        if ss.shard_state().vert_seq_no() > self.info.vert_seq_no() {
            reject_query!("one of previous states {} has vertical seqno {} larger than that of the new block {}",
                ss.shard_state().id(), ss.shard_state().vert_seq_no(), self.info.vert_seq_no())
        }
        Ok(())
    }

    fn unpack_prev_state(&mut self) -> Result<()> {
        self.prev_state_accounts = self.prev_states[0].state().read_accounts()?;
        self.prev_validator_fees = self.prev_states[0].state().total_validator_fees().clone();
        if let Some(state) = self.prev_states.get(1) {
            CHECK!(self.after_merge);
            let key = state.shard().merge()?.shard_key(false);
            self.prev_state_accounts.hashmap_merge(&state.state().read_accounts()?, &key)?;
            self.prev_state_accounts.update_root_extra()?;
            self.prev_validator_fees.add(state.state().total_validator_fees())?;
        } else if self.after_split {
            self.prev_state_accounts.split_for(&self.shard().shard_key(false))?;
            self.prev_state_accounts.update_root_extra()?;
            if self.shard().is_right_child() {
                self.prev_validator_fees.grams.0 += 1;
            }
            self.prev_validator_fees.grams.0 /= 2;
        }
        Ok(())
    }

    fn unpack_next_state(&mut self) -> Result<()> {
        log::debug!(target: "validator", "unpacking new state");
        CHECK!(&self.next_state, inited);
        if self.next_state.state().gen_time() != self.now() {
            reject_query!("new state of {} claims to have been generated at unixtime {}, but the block header contains {}",
                self.next_state.state().id(), self.next_state.state().gen_time(), self.info.gen_utime())
        }
        if self.next_state.state().gen_lt() != self.info.end_lt() {
            reject_query!("new state of {} claims to have been generated at logical time {}, but the block header contains end lt {}",
                self.next_state.state().id(), self.next_state.state().gen_lt(), self.info.end_lt())
        }
        if !self.shard().is_masterchain() {
            let mc_blkid = self.next_state.state().master_ref()
                .ok_or_else(|| error!("new state of {} doesn't have have master ref", self.next_state.state().id()))?
                .master.clone().master_block_id().1;
            if mc_blkid != self.mc_blkid {
                reject_query!("new state refers to masterchain block {} different from {} indicated in block header",
                    mc_blkid, self.mc_blkid)
            }
        }
        if self.next_state.state().vert_seq_no() != self.info.vert_seq_no() {
            reject_query!("new state has vertical seqno {} different from {} declared in the new block header",
                self.next_state.state().vert_seq_no(), self.info.vert_seq_no())
        }
        // ...
        self.next_state_accounts = self.next_state.state().read_accounts()?;
        Ok(())
    }

    async fn init_output_queue_manager(&mut self) -> Result<()> {
        self.output_queue_manager.init(
            self.engine.deref(),
            self.shard().clone(),
            &self.new_mc_shards,
            &self.prev_states,
            Some(&self.next_state),
            self.after_merge,
            self.after_split,
        ).await
    }

    // similar to Collator::update_one_shard()
    fn check_one_shard(
        &mut self,
        info: &McShardRecord,
        sibling: Option<&McShardRecord>,
        wc_info: Option<&WorkchainDescr>
    ) -> Result<()> {
        let shard = &info.shard;
        log::debug!(target: "validator", "checking shard {} in new shard configuration", shard);
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
            if info.descr.end_lt >= self.info.start_lt() {
                reject_query!("newly-created shard {} has incorrect logical time {} for a new block with start_lt={}",
                    shard, info.descr.end_lt, self.info.start_lt());
            }
            if info.descr.gen_utime > self.now() {
                reject_query!("newly-created shard {} has incorrect creation time {} for a new block created only at {}",
                    shard, info.descr.gen_utime, self.now())
            }
            if info.descr.before_split || info.descr.before_merge || info.descr.want_split || info.descr.want_merge {
                reject_query!("newly-created shard {} has merge/split flags (incorrectly) set", shard)
            }
            if info.descr.min_ref_mc_seqno != 0 {
                reject_query!("newly-created shard {} has finite min_ref_mc_seqno", shard)
            }
            if info.descr.reg_mc_seqno != self.block_id().seq_no {
                reject_query!("newly-created shard {} has registration mc seqno {} different from seqno of current block {}",
                    shard, info.descr.reg_mc_seqno, self.block_id().seq_no)
            }
            if !info.descr.fees_collected.is_zero()? {
                reject_query!("newly-created shard {} has non-zero fees_collected", shard)
            }
            cc_seqno = 0;
        } else if let Some(old) = old {
            if old.block_id == info.block_id {
                // shard unchanged ?
                log::debug!(target: "validator", "shard {} unchanged", shard);
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
                log::debug!(target: "validator", "shard {} changed from {} to {}", shard, old.block_id.seq_no, info.block_id.seq_no);
                if info.descr.reg_mc_seqno != self.block_id().seq_no {
                    reject_query!("shard information for block {} has been updated in the new shard configuration, but it has reg_mc_seqno={} different from that of the current block {}",
                        info.block_id, info.descr.reg_mc_seqno, self.block_id().seq_no)
                }
                let sh_bd = match self.top_shard_descr_dict.get_top_block_descr(info.block_id.shard())? {
                    Some(tbd) => TopBlockDescrStuff::new(tbd, &info.block_id, self.is_fake)?,
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
                let chain_len = sh_bd.prevalidate(&self.mc_blkid, &self.mc_state_extra,
                    self.mc_state.shard_state().vert_seq_no(), do_flags, &mut res_flags)
                    .map_err(|err| error!("ShardTopBlockDescr for {} is invalid: res_flags={}, err: {}",
                        sh_bd.proof_for(), res_flags, err))?;
                if chain_len <= 0 || chain_len > 8 {
                    reject_query!("ShardTopBlockDescr for {} is invalid: its chain length is {} (not in range 1..8)",
                        sh_bd.proof_for(), chain_len)
                }
                let chain_len = chain_len as usize;
                if sh_bd.gen_utime() > self.now() {
                    reject_query!("ShardTopBlockDescr for {} is invalid: it claims to be generated at {} while it is still {}",
                        sh_bd.proof_for(), sh_bd.gen_utime(), self.now())
                }
                let descr = sh_bd.get_top_descr(chain_len)
                    .map_err(|err| error!("No top descr for {:?}: {}", sh_bd, err))?;
                CHECK!(&descr, inited);
                CHECK!(descr.block_id(), sh_bd.proof_for());
                let start_blks = sh_bd.get_prev_at(chain_len);
                may_update_shard_block_info(self.mc_state.shards()?, &descr, &start_blks, self.info.start_lt(), None)
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
                if let Some(import) =  self.mc_extra.fees().get_serialized(shard.full_key_with_tag()?)? {
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
                self.register_shard_block_creators(&sh_bd.get_creator_list(chain_len)?)?;
                // ...
                if old.shard.is_parent_for(shard) {
                    // shard has been split
                    log::debug!(target: "validator", "detected shard split {} -> {}", old.shard, shard);
                    // ...
                } else if shard.is_parent_for(&old.shard) {
                    // shard has been merged
                    if let Some(old2) = self.old_mc_shards.find_shard(&shard.right_ancestor_mask()?)? {
                        if !old.shard().is_ancestor_for(&old2.shard()) {
                            reject_query!("shard {} has been impossibly merged from more than two shards \
                                {}, {} and others", shard, old.shard(), old2.shard())
                        }
                        log::debug!(target: "validator", "detected shard merge {} + {} -> {}",
                            old.shard(), old2.shard(), shard);
                    } else {
                        // CHECK!(old2.is_some());
                        reject_query!("No plus_one shard") // TODO: check here
                    }
                    // ...
                } else if shard == &old.shard {
                    // shard updated without split/merge
                    prev = Some(old);
                    // ...
                } else {
                    reject_query!("new configuration contains shard {} that could not be \
                        obtained from previously existing shard {}", shard, old.shard);
                // ...
                }
            }
        }
        let mut fsm_inherited = false;
        if let Some(prev) = prev {
            // shard was not created, split or merged; it is a successor of `prev`
            old_before_merge = prev.descr.before_merge;
            if !prev.descr().is_fsm_none() && !prev.descr().fsm_equal(info.descr())
                && self.now() < prev.descr().fsm_utime_end() && !info.descr.before_split {
                reject_query!("future split/merge information for shard {} has been arbitrarily \
                    changed without a good reason", shard)
            }
            fsm_inherited = !prev.descr().is_fsm_none() && prev.descr().fsm_equal(info.descr());
            if fsm_inherited && (self.now() > prev.descr().fsm_utime_end() || info.descr.before_split) {
                reject_query!("future split/merge information for shard {}\
                    has been carried on to the new shard configuration, but it is either expired (expire time {}, now {}), 
                    or before_split bit has been set ({})",
                        shard, prev.descr().fsm_utime_end(), self.now(), info.descr.before_split);
            }
        } else {
            // shard was created, split or merged
            if info.descr.before_split {
                reject_query!("a newly-created, split or merged shard {} cannot have before_split set immediately after", shard)
            }
        }
        let wc_info = wc_info.expect("in ton node it is a bug");
        let depth = shard.prefix_len();
        let split_cond = (info.descr.want_split || depth < wc_info.min_split()) && depth < wc_info.max_split() && depth < ton_block::shard::MAX_SPLIT_DEPTH;
        let merge_cond = depth > wc_info.min_split() && (info.descr.want_merge || depth > wc_info.max_split())
            && (sibling.map(|s| s.descr.want_merge).unwrap_or_default() || depth > wc_info.max_split());
        if !fsm_inherited && !info.descr().is_fsm_none() {
            if info.descr().fsm_utime() < self.now() || info.descr().fsm_utime_end() <= info.descr().fsm_utime()
                || info.descr().fsm_utime_end() < info.descr().fsm_utime() + MIN_SPLIT_MERGE_INTERVAL
                || info.descr().fsm_utime_end() > self.now() + MAX_SPLIT_MERGE_DELAY {
                reject_query!("incorrect future split/merge interval {} .. {} \
                    set for shard {} in new shard configuration (it is {} now)",
                        info.descr().fsm_utime(), info.descr().fsm_utime_end(), shard, self.now());
            }
            if info.descr().is_fsm_split() && !split_cond {
                reject_query!("announcing future split for shard {} in new shard configuration, \
                    but split conditions are not met", shard)
            }
            if info.descr().is_fsm_merge() && !merge_cond {
                reject_query!("announcing future merge for shard {} in new shard configuration, 
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
        self.min_shard_ref_mc_seqno = std::cmp::min(self.min_shard_ref_mc_seqno, info.descr.min_ref_mc_seqno);
        self.max_shard_utime        = std::cmp::max(self.max_shard_utime, info.descr.gen_utime);
        self.max_shard_lt           = std::cmp::max(self.max_shard_lt, info.descr.end_lt);
        // dbg!(self.min_shard_ref_mc_seqno, self.max_shard_utime, self.max_shard_lt);
        Ok(())
    }

    // checks old_shard_conf_ -> self.mc_extra.shards() transition using top_shard_descr_dict_ from collated data
    // similar to Collator::update_shard_config()
    fn check_shard_layout(&mut self) -> Result<()> {
        if !self.shard().is_masterchain() {
            return Ok(())
        }
        let prev_now = self.prev_state.state().gen_time();
        if prev_now > self.now() {
            reject_query!("creation time is not monotonic: {} after {}", self.now(), prev_now)
        }
        let ccvc = self.config_params.catchain_config()?;
        let wc_set = self.config_params.workchains()?;
        self.update_shard_cc = self.info.key_block()
            || (self.now() / ccvc.shard_catchain_lifetime > prev_now / ccvc.shard_catchain_lifetime);
        if self.update_shard_cc {
            log::debug!(target: "validator", "catchain_seqno of all shards must be updated");
        }

        let mut wc_id = ton_block::INVALID_WORKCHAIN_ID;
        let mut wc_info = None;
        self.new_mc_shards.clone().iterate_shards_with_siblings(|shard, descr, sibling| {
            if wc_id != shard.workchain_id() {
                wc_id = shard.workchain_id();
                if wc_id == ton_block::INVALID_WORKCHAIN_ID || wc_id == ton_block::MASTERCHAIN_ID {
                    reject_query!("new shard configuration contains shards of invalid workchain {}", wc_id)
                }
                wc_info = wc_set.get(&wc_id)?;
            }
            let descr = McShardRecord::from_shard_descr(shard, descr);
            if let Some(sibling) = sibling {
                let sibling = McShardRecord::from_shard_descr(descr.shard().sibling(), sibling);
                self.check_one_shard(&sibling, Some(&descr), wc_info.as_ref())?;
                self.check_one_shard(&descr, Some(&sibling), wc_info.as_ref())?;
            } else {
                self.check_one_shard(&descr, None, wc_info.as_ref())?;
            }
            Ok(true)
        }).map_err(|err| error!("new shard configuration is invalid : {}", err))?;
        self.prev_state_extra.config.workchains()?.iterate_keys(|wc_id: i32| {
            if self.mc_extra.shards().get(&wc_id)?.is_none() {
                reject_query!("shards of workchain {} existed in previous \
                    shardchain configuration, but are absent from new", wc_id)
            }
            Ok(true)
        })?;
        wc_set.iterate_with_keys(|wc_id: i32, wc_info| {
            if wc_info.active && !self.mc_extra.shards().get(&wc_id)?.is_some() {
                reject_query!("workchain {} is active, but is absent from new shard configuration", wc_id)
            }
            Ok(true)
        })?;
        self.check_mc_validator_info(self.info.key_block()
            || (self.now() / ccvc.mc_catchain_lifetime > prev_now / ccvc.mc_catchain_lifetime))
    }

    // similar to Collator::register_shard_block_creators
    fn register_shard_block_creators(&mut self, creator_list: &Vec<UInt256>) -> Result<()> {
        for x in creator_list {
            log::debug!(target: "validator", "registering block creator {}", x.to_hex_string());
            if !x.is_zero() {
                *self.block_create_count.entry(x.clone()).or_default() += 1;
            }
            self.block_create_total += 1;
        }
        Ok(())
    }

    // parallel to 4. of Collator::create_mc_state_extra()
    // checks validator_info in mc_state_extra
    fn check_mc_validator_info(&self, update_mc_cc: bool) -> Result<()> {
        CHECK!(&self.prev_state_extra, inited);
        CHECK!(&self.next_state_extra, inited);
        let old_info = &self.prev_state_extra.validator_info;
        let new_info = &self.next_state_extra.validator_info;

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
        let now = self.next_state.shard_state().gen_time();
        let prev_now = self.prev_state.shard_state().gen_time();
        let ccvc = self.next_state_extra.config.catchain_config()?;
        let cur_validators = self.next_state_extra.config.validator_set()?;
        let lifetime = ccvc.mc_catchain_lifetime;
        let is_key_block = self.info.key_block();
        let mut cc_updated = false;
        let catchain_seqno = new_info.catchain_seqno;
        if is_key_block || (now / lifetime > prev_now / lifetime) {
            cc_updated = true;
            log::debug!(target: "validator", "increased masterchain catchain seqno to {}", catchain_seqno);
        }
        let (nodes, _hash_short) = cur_validators.calc_subset(
            &ccvc, 
            self.shard.shard_prefix_with_tag(), 
            self.shard.workchain_id(), 
            catchain_seqno,
            UnixTime32(now)
        )?;

        if nodes.is_empty() {
            reject_query!("cannot compute next masterchain validator set from new masterchain state")
        }

        let vlist_hash = ValidatorSet::calc_subset_hash_short(&nodes, /* new_info.catchain_seqno */ 0)?;
        if new_info.validator_list_hash_short != vlist_hash {
            reject_query!("new masterchain validator list hash incorrect hash: expected {}, found {}",
                new_info.validator_list_hash_short, vlist_hash);
        }
        log::debug!(target: "validator", "masterchain validator set hash changed from {} to {}",
            old_info.validator_list_hash_short, vlist_hash);
        if new_info.nx_cc_updated != cc_updated & self.update_shard_cc {
            reject_query!("new_info.nx_cc_updated has incorrect value {}", new_info.nx_cc_updated)
        }
        Ok(())
    }

    fn check_utime_lt(&self) -> Result<()> {
        CHECK!(&self.config_params, inited);
        let mut gen_lt = std::u64::MIN;
        for state in &self.prev_states {
            if self.info.start_lt() <= state.state().gen_lt() {
                reject_query!("block has start_lt {} less than or equal to lt {} of the previous state",
                    self.info.start_lt(), state.state().gen_lt())
            }
            if self.now() <= state.state().gen_time() {
                reject_query!("block has creation time {} less than or equal to that of the previous state ({})",
                    self.now(), state.state().gen_time())
            }
            gen_lt = std::cmp::max(gen_lt, state.state().gen_lt());
        }
        if self.now() <= self.mc_state.state().gen_time() {
            reject_query!("block has creation time {} less than or equal to that of the reference masterchain state ({})",
                self.now(), self.mc_state.state().gen_time())
        }
        /*
        if self.now() > (unsigned)std::time(nullptr) + 15 {
            reject_query!("block has creation time " << self.now() too much in the future (it is only " << (unsigned)std::time(nullptr) now)");
        }
        */
        if self.info.start_lt() <= self.mc_state.state().gen_lt() {
            reject_query!("block has start_lt {} less than or equal to lt {} of the reference masterchain state",
                self.info.start_lt(), self.mc_state.state().gen_lt())
        }
        let lt_bound = std::cmp::max(gen_lt, std::cmp::max(self.mc_state.state().gen_lt(), self.max_shard_lt));
        if self.info.start_lt() > lt_bound + self.config_params.get_lt_align() * 4 {
            reject_query!("block has start_lt {} which is too large without a good reason (lower bound is {})",
                self.info.start_lt(), lt_bound + 1)
        }
        if self.shard().is_masterchain() && self.info.start_lt() - gen_lt > self.config_params.get_max_lt_growth() {
            reject_query!("block increases logical time from previous state by {} which exceeds the limit ({})",
                self.info.start_lt() - gen_lt, self.config_params.get_max_lt_growth())
        }
        let delta_hard = self.config_params.block_limits(self.shard().is_masterchain())?.lt_delta().hard_limit() as u64;
        if self.info.end_lt() - self.info.start_lt() > delta_hard {
            reject_query!("block increased logical time by {} which is larger than the hard limit {}",
                self.info.end_lt() - self.info.start_lt(), delta_hard)
        }
    Ok(())
    }

/*
 * 
 *  METHODS CALLED FROM try_validate() stage 1
 * 
 */

    fn load_block_data(&mut self) -> Result<()> {
        log::debug!(target: "validator", "unpacking block structures");
        self.in_msg_descr = self.extra.read_in_msg_descr()?;
        self.out_msg_descr = self.extra.read_out_msg_descr()?;
        self.account_blocks = self.extra.read_account_blocks()?;
        // run some hand-written checks from block::tlb::
        // (letmatic tests from block::gen:: have been already run for the entire block)
        // count and validate
        log::debug!(target: "validator", "validating InMsgDescr");
        // self.in_msg_descr.count(1000000)?;
        log::debug!(target: "validator", "validating OutMsgDescr");
        // self.out_msg_descr.count(1000000)?;
        log::debug!(target: "validator", "validating ShardAccountBlocks");
        // self.account_blocks.count(1000000)?;
        Ok(())
    }

    fn precheck_value_flow(&self) -> Result<()> {
        log::debug!(target: "validator", "value flow: {}", self.value_flow);
        // if !self.value_flow.validate() {
        //     reject_query!("ValueFlow of block {} is invalid (in-balance is not equal to out-balance)", self.block_id())
        // }
        if !self.shard().is_masterchain() && !self.value_flow.minted.is_zero()? {
            reject_query!("ValueFlow of block {} \
                is invalid (non-zero minted value in a non-masterchain block)", self.block_id())
        }
        if !self.shard().is_masterchain() && !self.value_flow.recovered.is_zero()? {
            reject_query!("ValueFlow of block {} \
                is invalid (non-zero recovered value in a non-masterchain block)", self.block_id())
        }
        if !self.value_flow.recovered.is_zero()? && self.recover_create_msg.is_none() {
            reject_query!("ValueFlow of block {} \
                has a non-zero recovered fees value, but there is no recovery InMsg", self.block_id())
        }
        if self.value_flow.recovered.is_zero()? && self.recover_create_msg.is_some() {
            reject_query!("ValueFlow of block {} \
                has a zero recovered fees value, but there is a recovery InMsg", self.block_id())
        }
        if !self.value_flow.minted.is_zero()? && self.mint_msg.is_none() {
            reject_query!("ValueFlow of block {} \
                has a non-zero minted value, but there is no mint InMsg", self.block_id())
        }
        if self.value_flow.minted.is_zero()? && self.mint_msg.is_some() {
            reject_query!("ValueFlow of block {} \
                has a zero minted value, but there is a mint InMsg", self.block_id())
        }
        if !self.value_flow.minted.is_zero()? {
            let to_mint = self.compute_minted_amount()
                .map_err(|err| error!("cannot compute the correct amount of extra currencies to be minted : {}", err))?;
            if self.value_flow.minted != to_mint {
                reject_query!("invalid extra currencies amount to be minted: declared {}, expected {}",
                    self.value_flow.minted, to_mint)
            }
        }
        let create_fee = self.config_params.block_create_fees(self.shard().is_masterchain()).unwrap_or_default();
        let create_fee = CurrencyCollection::from_grams(create_fee.shr(self.shard().prefix_len()));
        if self.value_flow.created != create_fee {
            reject_query!("ValueFlow of block {} declares block creation fee {}, \
                but the current configuration expects it to be {}",
                    self.block_id(), self.value_flow.created, create_fee)
        }
        if !self.value_flow.fees_imported.is_zero()? && !self.shard().is_masterchain() {
            reject_query!("ValueFlow of block {} \
                is invalid (non-zero fees_imported in a non-masterchain block)", self.block_id())
        }
        let cc = self.prev_state_accounts.full_balance();
        if cc != &self.value_flow.from_prev_blk {
            reject_query!("ValueFlow for {} declares from_prev_blk={} \
                but the sum over all accounts present in the previous state is {}",
                    self.block_id(), self.value_flow.from_prev_blk, cc)
        }
        let cc = self.next_state_accounts.full_balance();
        if cc != &self.value_flow.to_next_blk {
            reject_query!("ValueFlow for {} declares to_next_blk={} but the sum over all accounts \
                present in the new state is {}", self.block_id(), self.value_flow.to_next_blk, cc)
        }
        let cc = self.in_msg_descr.full_import_fees();
        if cc.value_imported != self.value_flow.imported {
            reject_query!("ValueFlow for {} declares imported={} but the sum over all inbound messages \
                listed in InMsgDescr is {}", self.block_id(), self.value_flow.imported, cc.value_imported);
        }
        let fees_import = CurrencyCollection::from_grams(cc.fees_collected.clone());
        let cc = self.out_msg_descr.full_exported();
        if cc != &self.value_flow.exported {
            reject_query!("ValueFlow for {} declares exported={} but the sum over all outbound messages \
                listed in OutMsgDescr is {}", self.block_id(), self.value_flow.exported, cc)
        }
        let transaction_fees = self.account_blocks.full_transaction_fees();
        let mut expected_fees = transaction_fees.clone();
        expected_fees.add(&self.value_flow.fees_imported)?;
        expected_fees.add(&self.value_flow.created)?;
        expected_fees.add(&fees_import)?;
        if self.value_flow.fees_collected != expected_fees {
            reject_query!("ValueFlow for {} declares fees_collected={} but the total message import fees are {}, \
                the total transaction fees are {}, creation fee for this block is {} \
                and the total imported fees from shards are {} with a total of {}", self.block_id(),
                    self.value_flow.fees_collected.grams, fees_import, transaction_fees.grams,
                    self.value_flow.created.grams, self.value_flow.fees_imported.grams, expected_fees.grams)
        }
        Ok(())
    }

    // similar to Collator::compute_minted_amount()
    fn compute_minted_amount(&self) -> Result<CurrencyCollection> {
        let mut to_mint = CurrencyCollection::default();
        if !self.shard().is_masterchain() {
            return Ok(to_mint)
        }
        let to_mint_config = match self.config_params.config(7)? {
            Some(ConfigParamEnum::ConfigParam7(param)) => param.to_mint,
            _ => return Ok(to_mint)
        };
        to_mint_config.iterate_with_keys(|curr_id: u32, amount| {
            let amount2 = self.prev_state_extra.global_balance.get_other(curr_id)?.unwrap_or_default();
            if amount >= amount2 {
                let mut delta = amount.clone();
                delta.sub(&amount2)?;
                log::debug!(target: "validator", "currency #{}: existing {}, required {}, to be minted {}",
                    curr_id, amount2, amount, delta);
                to_mint.set_other_ex(curr_id, &delta)?;
            }
            Ok(true)
        }).map_err(|err| error!("error scanning extra currencies to be minted : {}", err))?;
        if !to_mint.is_zero()? {
            log::debug!(target: "validator", "new currencies to be minted: {}", to_mint);
        }
        Ok(to_mint)
    }

    fn precheck_one_account_update(&self, acc_id: UInt256,
        old_val_extra: Option<(ShardAccount, DepthBalanceInfo)>,
        new_val_extra: Option<(ShardAccount, DepthBalanceInfo)>
    ) -> Result<bool> {
        log::debug!(target: "validator", "checking update of account {}", acc_id.to_hex_string());
        let acc_blk = self.account_blocks.get(&acc_id)?.ok_or_else(|| error!("the state of account {} \
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

    fn precheck_account_updates(&self) -> Result<()> {
        log::debug!(target: "validator", "pre-checking all Account updates between the old and the new state");
        let prev_accounts = self.prev_state_accounts.clone();
        let next_accounts = self.next_state_accounts.clone();
        prev_accounts.scan_diff_with_aug(&next_accounts, |key, old_val_extra, new_val_extra|
            self.precheck_one_account_update(key, old_val_extra, new_val_extra)
        )?;
        Ok(())
    }

    fn precheck_one_transaction(&self, acc_id: &AccountId, trans_lt: u64, trans_root: Cell,
        prev_trans_lt: &mut u64, prev_trans_hash: &mut UInt256, prev_trans_lt_len: &mut u64,
        acc_state_hash: &mut UInt256
    ) -> Result<bool> {
        log::debug!(target: "validator", "pre-checking Transaction {}", trans_lt);
        let trans = Transaction::construct_from_cell(trans_root.clone())?;
        if &trans.account_addr != acc_id || trans.lt != trans_lt {
            reject_query!("transaction {} of {} claims to be transaction {} of {}",
                trans_lt, acc_id.to_hex_string(), trans.lt, trans.account_addr.to_hex_string())
        }
        if trans.now != self.now() {
            reject_query!("transaction {} of {} claims that current time is {}
                while the block header indicates {}", trans_lt, acc_id.to_hex_string(), trans.now, self.now())
        }
        if &trans.prev_trans_hash != prev_trans_hash
            || &trans.prev_trans_lt != prev_trans_lt {
            reject_query!("transaction {} of {} claims that the previous transaction was {}:{} \
                while the correct value is {}:{}", trans_lt, acc_id.to_hex_string(),
                    trans.prev_trans_lt, trans.prev_trans_hash.to_hex_string(),
                    prev_trans_lt, prev_trans_hash.to_hex_string())
        }
        if trans_lt < *prev_trans_lt + *prev_trans_lt_len {
            reject_query!("transaction {} of {} starts at logical time {}, \
                earlier than the previous transaction {} .. {} ends",
                    trans_lt, acc_id.to_hex_string(), trans_lt, prev_trans_lt,
                    *prev_trans_lt + *prev_trans_lt_len)
        }
        let lt_len = trans.outmsg_cnt as u64 + 1;
        if trans_lt <= self.info.start_lt() || trans_lt + lt_len > self.info.end_lt() {
            reject_query!("transaction {} .. {} of {:x} is not inside the logical time interval {} .. {} \
                of the encompassing new block", trans_lt, trans_lt + lt_len, acc_id,
                    self.info.start_lt(), self.info.end_lt())
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
                trans_lt, acc_id.to_hex_string(), trans.outmsg_cnt - 1)
            } else {
                c += 1;
                Ok(true)
            }
        })?;
        return Ok(true)
    }

    // NB: could be run in parallel for different accounts
    fn precheck_one_account_block(&self, acc_id: &UInt256, acc_blk: AccountBlock) -> Result<bool> {
        let acc_id = AccountId::from(acc_id.clone());
        log::debug!(target: "validator", "pre-checking AccountBlock for {}", acc_id.to_hex_string());
        
        if !self.shard().contains_account(acc_id.clone())? {
            reject_query!("new block {} contains AccountBlock for account {} not belonging to the block's shard {}",
                self.block_id(), acc_id.to_hex_string(), self.shard())
        }
        // CHECK!(acc_blk_root.is_some());
        // acc_blk_root->print_rec(std::cerr);
        // block::gen::t_AccountBlock.print(std::cerr, acc_blk_root);
        let hash_upd = acc_blk.read_state_update()?;
        if acc_blk.account_id() != &acc_id {
            reject_query!("AccountBlock of account {} appears to belong to another account {}",
                acc_id.to_hex_string(), acc_blk.account_id().to_hex_string())
        }
        let old_state = self.prev_state_accounts.get_serialized(acc_id.clone())?.unwrap_or_default();
        let new_state = self.next_state_accounts.get_serialized(acc_id.clone())?;
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
        let min_trans = acc_blk.transactions().get_max(false)?;
        let max_trans = acc_blk.transactions().get_max(false)?;
        let (min_trans, max_trans) = match (min_trans, max_trans) {
            (Some(min), Some(max)) => (min.0, max.0),
            _ => reject_query!("cannot extract minimal and maximal keys from the transaction dictionary of account {}",
                acc_id.to_hex_string())
        };
        if min_trans <= self.info.start_lt() || max_trans >= self.info.end_lt() {
            reject_query!("new block contains transactions {} .. {} outside of the block's lt range {} .. {}",
                min_trans, max_trans, self.info.start_lt(), self.info.end_lt())
        }
        let mut last_trans_lt_len = 1;
        let mut last_trans_lt = old_state.last_trans_lt();
        let mut last_trans_hash = old_state.last_trans_hash().clone();
        let mut acc_state_hash = hash_upd.old_hash;
        acc_blk.transactions().iterate_slices(|key, trans_slice|
            key.clone().get_next_u64().and_then(|key| self.precheck_one_transaction(
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
        Ok(true)
    }

    fn precheck_account_transactions(&self) -> Result<()> {
        log::debug!(target: "validator", "pre-checking all AccountBlocks, and all transactions of all accounts");
        self.account_blocks.iterate_with_keys(|key, acc_blk|
            self.precheck_one_account_block(&key, acc_blk).map_err(|err|
                error!("invalid AccountBlock for account {} in the new block {} : {}",
                    key.to_hex_string(), self.block_id(), err)
            )
        )?;
        Ok(())
    }

    fn lookup_transaction(&self, addr: &AccountId, lt: u64) -> Result<Option<Cell>> {
        match self.account_blocks.get_serialized(addr.clone())? {
            Some(block) => block.transactions().get_as_cell(&lt),
            None => Ok(None)
        }
    }

    // checks that a ^Transaction refers to a transaction present in the ShardAccountBlocks
    fn is_valid_transaction_ref(&self, transaction: &Transaction, hash: UInt256) -> Result<()> {
        match self.lookup_transaction(&transaction.account_addr, transaction.lt)? {
            Some(trans_cell) => if trans_cell != hash {
                reject_query!("transaction {} of {:x} has a different hash", transaction.lt, transaction.account_addr)
            } else {
                Ok(())
            }
            None => reject_query!("transaction {} of {:x} not found", transaction.lt, transaction.account_addr)
        }
    }

    // checks that any change in OutMsgQueue in the state is accompanied by an OutMsgDescr record in the block
    // also checks that the keys are correct
    fn precheck_one_message_queue_update(
        &self,
        out_msg_id: &OutMsgQueueKey,
        old_value: Option<(EnqueuedMsg, u64)>,
        new_value: Option<(EnqueuedMsg, u64)>
    ) -> Result<bool> {
        log::debug!(target: "validator", "checking update of enqueued outbound message {:x}", out_msg_id);
        CHECK!(old_value.is_some() || new_value.is_some());
        let m_str = if new_value.is_some() && old_value.is_some() {
            reject_query!("EnqueuedMsg with key {} has been changed in the OutMsgQueue, \
                but the key did not change", out_msg_id.to_hex_string())
        } else if new_value.is_some() {
            "en"
        } else if old_value.is_some() {
            "de"
        } else {
            ""
        };
        let out_msg = self.out_msg_descr.get(&out_msg_id.hash)?
            .ok_or_else(|| error!( "no OutMsgDescr corresponding to {}queued message with key {}",
                m_str, out_msg_id.to_hex_string()))?;
        let correct = match out_msg {
            OutMsg::New(_) | OutMsg::Transit(_) => new_value.is_some(),
            OutMsg::Dequeue(_) | OutMsg::DequeueImmediately(_) | OutMsg::DequeueShort(_) => old_value.is_some(),
            OutMsg::TransitRequeued(_) => true,
            _ => false
        };
        if !correct {
            reject_query!("OutMsgDescr corresponding to {} queued message with key {} has invalid tag ${:3x}",
                m_str, out_msg_id.to_hex_string(), out_msg.tag())
        }
        let enq;
        if let Some((_enq, _lt)) = old_value {
            enq = MsgEnqueueStuff::from_enqueue_and_lt(_enq, _lt)?;
            if enq.enqueued_lt() >= self.info.start_lt() {
                reject_query!("new EnqueuedMsg with key {} has enqueued_lt={} greater \
                    than or equal to this block's start_lt={}", out_msg_id.to_hex_string(),
                        enq.enqueued_lt(), self.info.start_lt())
            }
            // dequeued message
            if let OutMsg::TransitRequeued(info) = out_msg {
                // this is a msg_export_tr_req$111, a re-queued transit message (after merge)
                // check that q_msg_env still contains msg
                let q_msg = info.out_message_cell();
                if info.out_message_cell().repr_hash() != out_msg_id.hash {
                    reject_query!("MsgEnvelope in the old outbound queue with key {} \
                        contains a Message with incorrect hash {}", out_msg_id.to_hex_string(), q_msg.repr_hash().to_hex_string())
                }
                // must be msg_import_tr$100
                let in_msg = info.read_imported()?;
                match in_msg {
                    InMsg::Immediatelly(info) => {
                        if info.message_cell().repr_hash() != enq.envelope_hash() {
                            reject_query!("OutMsgDescr corresponding to dequeued message with key {} \
                                is a msg_export_tr_req referring to a reimport InMsgDescr that contains a MsgEnvelope \
                                distinct from that originally kept in the old queue", out_msg_id.to_hex_string())
                        }
                    }
                    _ => reject_query!("OutMsgDescr for {} refers to a reimport InMsgDescr with invalid tag ${:3x} \
                        instead of msg_import_tr$100", out_msg_id.to_hex_string(), in_msg.tag())
                }
            } else if out_msg.envelope_message_hash() != Some(enq.envelope_hash()) {
                reject_query!("OutMsgDescr corresponding to dequeued message with key {} contains a \
                    MsgEnvelope distinct from that originally kept in the old queue", out_msg_id.to_hex_string())
            }
        } else if let Some((_enq, _lt)) = new_value {
            enq = MsgEnqueueStuff::from_enqueue_and_lt(_enq, _lt)?;
            if enq.enqueued_lt() < self.info.start_lt() || enq.enqueued_lt() >= self.info.end_lt() {
                reject_query!("new EnqueuedMsg with key {} has enqueued_lt={} outside of \
                    this block's range {} .. {}", out_msg_id.to_hex_string(),
                    enq.enqueued_lt(), self.info.start_lt(), self.info.end_lt())
            }
            if out_msg.envelope_message_hash() != Some(enq.envelope_hash()) {
                reject_query!("OutMsgDescr corresponding to enqueued message with key {} \
                    contains a MsgEnvelope distinct from that stored in the new queue", out_msg_id.to_hex_string())
            }
        } else {
            log::error!(target: "validator", "EnqueuedMsg with key {} has been not changed in the OutMsgQueue", out_msg_id.to_hex_string());
            return Ok(true)
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
            reject_query!("OutMsgDescr for {} contains a MsgEnvelope that should be stored under different key {}",
                out_msg_id.to_hex_string(), new_key.to_hex_string())
        }
        Ok(true)
    }

    fn precheck_message_queue_update(&self) -> Result<bool> {
        log::debug!(target: "validator", "pre-checking the difference between the \
            old and the new outbound message queues");
        self.output_queue_manager.prev().out_queue().scan_diff_with_aug(
            self.output_queue_manager.next().out_queue(),
            |key, val1, val2| self.precheck_one_message_queue_update(&key, val1, val2)
        )
    }

    fn update_max_processed_lt_hash(&mut self, lt: u64, hash: &UInt256) {
        if self.proc_lt < lt || (self.proc_lt == lt && &self.proc_hash < hash) {
            self.proc_lt = lt;
            self.proc_hash = hash.clone();
        }
    }

    fn update_min_enqueued_lt_hash(&mut self, lt: u64, hash: &UInt256) {
        if lt < self.min_enq_lt || (lt == self.min_enq_lt && hash < &self.min_enq_hash) {
            self.min_enq_lt = lt;
            self.min_enq_hash = hash.clone();
        }
    }

    // check that the enveloped message (MsgEnvelope) was present in the output queue of a neighbor, and that it has not been processed before
    fn check_imported_message(&mut self, env: &MsgEnvelope, env_hash: &UInt256, created_lt: u64) -> Result<()> {
        let (cur_prefix, next_prefix) = env.calc_cur_next_prefix()?;
        if !self.shard().contains_full_prefix(&next_prefix) {
            reject_query!("imported message with hash {} has next hop address {}... not in this shard",
                env_hash.to_hex_string(), next_prefix)
        }
        let key = OutMsgQueueKey::with_account_prefix(&next_prefix, env.message_cell().repr_hash());
        if let (Some(block_id), enq) = self.output_queue_manager.find_message(&key, &cur_prefix)? {
            let enq_msg_descr = enq.ok_or_else(|| error!("imported internal message with hash {} and \
                previous address {}..., next hop address {} could not be found in \
                the outbound message queue of neighbor {} under key {}",
                    env_hash.to_hex_string(), cur_prefix, next_prefix, block_id, key.to_hex_string()))?;
            if &enq_msg_descr.envelope_hash() != env_hash {
                reject_query!("imported internal message from the outbound message queue of \
                    neighbor {} under key {} has a different MsgEnvelope in that outbound message queue",
                        block_id, key.to_hex_string())
            }
            if self.output_queue_manager.prev().already_processed(&enq_msg_descr)? {
                reject_query!("imported internal message with hash {} and lt={} has been already imported \
                    by a previous block of this shardchain", env_hash.to_hex_string(), created_lt)
            }
            self.update_max_processed_lt_hash(created_lt, &key.hash);
            return Ok(())
        } else {
            reject_query!("imported internal message with hash {} and \
                previous address {}..., next hop address {} has previous address not belonging to any neighbor",
                    env_hash.to_hex_string(), cur_prefix, next_prefix)
        }
    }

    fn is_special_in_msg(&self, in_msg: &InMsg) -> bool {
        self.recover_create_msg.as_ref() == Some(in_msg) || self.mint_msg.as_ref() == Some(in_msg)
    }

    fn check_in_msg(&mut self, key: &UInt256, in_msg: &InMsg) -> Result<()> {
        log::debug!(target: "validator", "checking InMsg with key {}", key.to_hex_string());
        CHECK!(in_msg, inited);
        // initial checks and unpack
        let msg_hash = in_msg.message_cell()?.repr_hash();
        if &msg_hash != key {
            reject_query!("InMsg with key {} refers to a message with different hash {}",
                key.to_hex_string(), msg_hash.to_hex_string())
        }
        let trans_cell = in_msg.transaction_cell();
        let msg_env = in_msg.in_msg_envelope_cell();
        let msg_env_hash = msg_env.map(|cell| cell.repr_hash()).unwrap_or_default();
        let env = in_msg.read_in_msg_envelope()?.unwrap_or_default();
        let msg = in_msg.read_message()?;
        let created_lt = msg.lt().unwrap_or_default();
        // dbg!(&in_msg, &env, &msg);
        if let Some(trans_cell) = trans_cell {
            let transaction = Transaction::construct_from_cell(trans_cell.clone())?;
            // check that the transaction reference is valid, and that it points to a Transaction which indeed processes this input message
            self.is_valid_transaction_ref(&transaction, trans_cell.repr_hash())
                .map_err(|err| error!("InMsg corresponding to inbound message with key {} contains an invalid \
                    Transaction reference (transaction not in the block's transaction list) : {}",
                        key.to_hex_string(), err))?;
            if !transaction.in_msg_cell().map(|cell| cell.repr_hash() == msg_hash).unwrap_or_default() {
                reject_query!("InMsg corresponding to inbound message with key {} \
                    refers to transaction that does not process this inbound message", key.to_hex_string())
            }
            let (_workchain_id, addr) = msg.dst().ok_or_else(|| error!("No dest address"))
                .and_then(|addr| addr.extract_std_address(true))?;
            if addr != transaction.account_addr {
                reject_query!("InMsg corresponding to inbound message with hash {} and destination address {} \
                   claims that the message is processed by transaction {} of another account {}",
                        key.to_hex_string(), addr.to_hex_string(), transaction.lt, transaction.account_addr.to_hex_string())
            }
        }
        let fwd_fee = match in_msg {
            // msg_import_ext$000 msg:^(Message Any) transaction:^Transaction
            // importing an inbound external message
            InMsg::External(_) => {
                let dst = msg.dst().unwrap_or_default();
                let dest_prefix = AccountIdPrefixFull::prefix(&dst)?;
                if !dest_prefix.is_valid() {
                    reject_query!("destination of inbound external message with hash {} \
                        is an invalid blockchain address", key.to_hex_string())
                }
                if !self.shard().contains_full_prefix(&dest_prefix) {
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
            InMsg::Immediatelly(ref info) => {
                if !self.is_special_in_msg(&in_msg) {
                    self.update_max_processed_lt_hash(created_lt, key);
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
        let dst = msg.dst().unwrap_or_default();
        let dest_prefix = AccountIdPrefixFull::checked_prefix(&dst).map_err(|_| error!("destination of inbound \
            internal message with hash {} is an invalid blockchain address", key.to_hex_string()))?;
        let src = msg.src().unwrap_or_default();
        let src_prefix = AccountIdPrefixFull::checked_prefix(&src).map_err(|_| error!("source of inbound \
            internal message with hash {} is an invalid blockchain address", key.to_hex_string()))?;
        let cur_prefix  = src_prefix.interpolate_addr_intermediate(&dest_prefix, &env.cur_addr())?;
        let next_prefix = src_prefix.interpolate_addr_intermediate(&dest_prefix, &env.next_addr())?;
        if !(cur_prefix.is_valid() && next_prefix.is_valid()) {
            reject_query!("cannot compute current and next hop addresses of inbound internal message with hash {}",
                key.to_hex_string())
        }
        // check that next hop is nearer to the destination than the current address
        if dest_prefix.count_matching_bits(&next_prefix) < dest_prefix.count_matching_bits(&cur_prefix) {
            reject_query!("next hop address {}... of inbound internal message with hash {} \
                is further from its destination {}... than its current address {}...",
                    next_prefix, key.to_hex_string(), dest_prefix, cur_prefix)
        }
        // next hop address must belong to this shard (otherwise we should never had imported this message)
        if !self.shard().contains_full_prefix(&next_prefix) {
            reject_query!("next hop address {}... of inbound internal message with hash {} does not belong \
                to the current block's shard {}", next_prefix, key.to_hex_string(), self.shard())
        }
        // next hop may coincide with current address only if destination is already reached
        if next_prefix == cur_prefix && cur_prefix != dest_prefix {
            reject_query!("next hop address {}... of inbound internal message with hash {} \
                coincides with its current address, but this message has not reached its final destination {}... yet",
                    next_prefix, key.to_hex_string(), dest_prefix)
        }
        // if a message is processed by a transaction, it must have destination inside the current shard
        if trans_cell.is_some() && !self.shard().contains_full_prefix(&dest_prefix) {
            reject_query!("inbound internal message with hash {} has destination address {}... not in this shard, \
                but it is processed nonetheless", key.to_hex_string(), dest_prefix)
        }
        // if a message is not processed by a transaction, its final destination must be outside this shard
        if trans_cell.is_none() && self.shard().contains_full_prefix(&dest_prefix) {
            reject_query!("inbound internal message with hash {} has destination address {}... in this shard, \
                but it is not processed by a transaction", key.to_hex_string(), dest_prefix)
        }
        // unpack complete destination address if it is inside this shard
        if trans_cell.is_some() && dst.extract_std_address(true).is_err() {
            reject_query!("cannot unpack destination address of inbound internal message with hash {}", key.to_hex_string())
        }
        // unpack original forwarding fee
        let orig_fwd_fee = &header.fwd_fee;
        // CHECK!(orig_fwd_fee.is_some());
        if env.fwd_fee_remaining() > orig_fwd_fee {
            reject_query!("inbound internal message with hash {} has remaining forwarding fee {} \
                larger than the original (total) forwarding fee {}", key.to_hex_string(), env.fwd_fee_remaining(), orig_fwd_fee)
        }
        let out_msg_opt = self.out_msg_descr.get(&key)?;
        let (out_msg_env, reimport) = match out_msg_opt.as_ref() {
            Some(out_msg) => (
                out_msg.out_message_cell(),
                out_msg.read_reimport_message()?
            ),
            None => (None, None)
        };

        // continue checking inbound message
        match in_msg {
            InMsg::Immediatelly(_) => {
                // msg_import_imm$011 in_msg:^MsgEnvelope transaction:^Transaction fwd_fee:Grams
                // importing and processing an internal message generated in this very block
                if cur_prefix != dest_prefix {
                    reject_query!("inbound internal message with hash {} is a msg_import_imm$011, \
                        but its current address {} is somehow distinct from its final destination {}",
                            key.to_hex_string(), cur_prefix, dest_prefix)
                }
                CHECK!(trans_cell.is_some());
                // check that the message has been created in this very block
                if !self.shard().contains_full_prefix(&src_prefix) {
                    reject_query!("inbound internal message with hash {} is a msg_import_imm$011, \
                        but its source address {} does not belong to this shard", key.to_hex_string(), src_prefix)
                }
                if let Some(OutMsg::Immediately(_)) = out_msg_opt.as_ref() {
                    CHECK!(out_msg_env.is_some());
                    CHECK!(reimport.is_some());
                } else if !self.is_special_in_msg(&in_msg) {
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
                CHECK!(self.shard().contains_full_prefix(&next_prefix));
                if self.shard().contains_full_prefix(&cur_prefix) {
                    // we imported this message from our shard!
                    if let Some(OutMsg::DequeueImmediately(_)) = out_msg_opt.as_ref() {
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
                    self.check_imported_message(&env, &info.message_cell().repr_hash(), created_lt)?;
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
                let tr_req = if self.shard().contains_full_prefix(&cur_prefix) {
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
                            self.check_imported_message(&env, &msg_env_hash, created_lt)?;
                        }
                        _ => reject_query!("inbound internal message with hash {} is a msg_import_tr$101 \
                            (transit message) with current address {}... outside of our shard, but the \
                            corresponding OutMsg is not a valid msg_export_tr$011",
                                key.to_hex_string(), cur_prefix)
                    }
                    "usual"
                };
                // perform hypercube routing for this transit message
                let ia = IntermediateAddress::default();
                let route_info = next_prefix.perform_hypercube_routing(&dest_prefix, &self.shard(), &ia)
                    .map_err(|err| error!("cannot perform (check) hypercube routing for \
                        transit inbound message with hash {}: src={} cur={} next={} dest={}; \
                        our shard is {} : {}", key.to_hex_string(), src_prefix, cur_prefix,
                            next_prefix, dest_prefix, self.shard(), err))?;
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
                if Some(info.out_message_cell()) != out_msg_env {
                    reject_query!("InMsg for transit message with hash {} contains rewritten MsgEnvelope \
                        different from that stored in corresponding OutMsgDescr ({} transit)", key.to_hex_string(), tr_req)
                }
                // check the amount of the transit fee
                let transit_fee = self.config.get_fwd_prices(false).next_fee(env.fwd_fee_remaining());
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

    fn check_in_msg_descr(&mut self) -> Result<bool> {
        log::debug!(target: "validator", "checking inbound messages listed in InMsgDescr");
        let in_msg_descr = self.in_msg_descr.clone();
        in_msg_descr.iterate_with_keys(|key, in_msg| match self.check_in_msg(&key, &in_msg) {
            Err(err) => {
                reject_query!("invalid InMsg with key (message hash) {} in the new block {} : {}",
                    key.to_hex_string(), self.block_id(), err)
            }
            Ok(_) => Ok(true)
        }).map_err(|err| error!("invalid InMsgDescr dictionary in the new block {} : {}", self.block_id(), err))
    }

    fn check_reimport(&self, out_msg: &OutMsg, in_msg_key: &UInt256) -> Result<()> {
        if let Some(reimport_cell) = out_msg.reimport_cell() {
            // transit message: msg_export_tr + msg_import_tr
            // or message re-imported from this very shard
            // either msg_export_imm + msg_import_imm
            // or msg_export_deq_imm + msg_import_fin (rarely)
            // or msg_export_tr_req + msg_import_tr (rarely)
            // (the last two cases possible only after merge)
            //
            // check that reimport is a valid InMsg registered in InMsgDescr
            let in_msg_slice = self.in_msg_descr.get_as_slice(in_msg_key)?
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
    fn check_out_msg(&mut self, key: &UInt256, out_msg: &OutMsg) -> Result<bool> {
        log::debug!(target: "validator", "checking OutMsg with key {}", key.to_hex_string());
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
        let src = msg.src().unwrap_or_default();
        let msg_env_hash = out_msg.envelope_message_hash().unwrap_or_default();

        let trans_cell = out_msg.transaction_cell();
        if let Some(trans_cell) = trans_cell {
            let transaction = Transaction::construct_from_cell(trans_cell.clone())?;
            // check that the transaction reference is valid, and that it points to a Transaction which indeed creates this outbound internal message
            self.is_valid_transaction_ref(&transaction, trans_cell.repr_hash())
                .map_err(|err| error!("OutMsg corresponding to outbound message with key {} \
                        contains an invalid Transaction reference (transaction not in the \
                        block's transaction list : {})", key.to_hex_string(), err))?;
            if !transaction.contains_out_msg(&msg, key) {
                reject_query!("OutMsg corresponding to outbound message with key {} \
                    refers to transaction that does not create this outbound message", key.to_hex_string())
            }
            let addr = src.extract_std_address(true)?.1;
            if addr != transaction.account_addr {
                reject_query!("OutMsg corresponding to outbound message with hash {} and source address {} \
                    claims that the message was created by transaction {}  of another account {}",
                        key.to_hex_string(), addr.to_hex_string(),
                        transaction.logical_time(), transaction.account_addr.to_hex_string())
            }
            // log::debug!(target: "validator", "OutMsg " << key.to_hex_string() + " is indeed a valid outbound message of transaction " << trans_lt
            //           of " << trans_addr.to_hex_string();
        }

        let (add, remove) = match out_msg {
            // msg_export_ext$000 msg:^(Message Any) transaction:^Transaction = OutMsg;
            // exporting an outbound external message
            OutMsg::External(_) => {
                let src_prefix = match msg.src() {
                    Some(src) => AccountIdPrefixFull::prefix(&src)?,
                    None => reject_query!("source of outbound external message with hash {} \
                        is an invalid blockchain address", key.to_hex_string())
                };
                if !self.shard().contains_full_prefix(&src_prefix) {
                    reject_query!("outbound external message with hash {} has source address {}... not in this shard",
                        key.to_hex_string(), src_prefix)
                }
                return Ok(true) // nothing to check more for external messages
            }
            OutMsg::DequeueShort(_) => {
                return Ok(true) // nothing to check here for msg_export_deq_short ?
            }
            // msg_export_imm$010 out_msg:^MsgEnvelope transaction:^Transaction reimport:^InMsg = OutMsg;
            OutMsg::Immediately(_) => (false, false),
            // msg_export_new$001 out_msg:^MsgEnvelope transaction:^Transaction = OutMsg;
            // msg_export_tr$011 out_msg:^MsgEnvelope imported:^InMsg = OutMsg;
            OutMsg::New(_) | OutMsg::Transit(_) => (true, false),  // added to OutMsgQueue
            // msg_export_deq$1100 out_msg:^MsgEnvelope import_block_lt:uint63 = OutMsg;
            // msg_export_deq_short$1101 msg_env_hash:bits256 next_workchain:int32 next_addr_pfx:uint64 import_block_lt:uint64 = OutMsg;
            // msg_export_deq$110 out_msg:^MsgEnvelope import_block_lt:uint64 = OutMsg;
            OutMsg::Dequeue(_) | OutMsg::DequeueImmediately(_) => (false, true),  // removed from OutMsgQueue
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
        if dest_prefix.count_matching_bits(&next_prefix) < dest_prefix.count_matching_bits(&cur_prefix) {
            reject_query!("next hop address {}... of outbound internal message with hash {} \
                is further from its destination {}... than its current address {}...",
                    next_prefix, key.to_hex_string(), dest_prefix, cur_prefix)
        }
        // current address must belong to this shard (otherwise we should never had exported this message)
        if !self.shard().contains_full_prefix(&cur_prefix) {
            reject_query!("current address {}... of outbound internal message with hash {} \
                does not belong to the current block's shard {}",
                    cur_prefix, key.to_hex_string(), self.shard())
        }
        // next hop may coincide with current address only if destination is already reached
        if next_prefix == cur_prefix && cur_prefix != dest_prefix {
            reject_query!("next hop address {}... of outbound internal message with hash {} \
                coincides with its current address, but this message has not reached its final destination {} ... yet", 
                  next_prefix, key.to_hex_string(), dest_prefix)
        }
        // if a message is created by a transaction, it must have source inside the current shard
        if trans_cell.is_some() && !self.shard().contains_full_prefix(&src_prefix) {
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
        let q_entry = self.output_queue_manager.next().message(&q_key)?;
        let old_q_entry = self.output_queue_manager.prev().message(&q_key)?;
        if old_q_entry.is_some() && q_entry.is_some() {
            reject_query!("OutMsg with key (message hash) {} should have removed or added OutMsgQueue entry with key {}, \
                but it is present both in the old and in the new output queues",
                    key.to_hex_string(), q_key.to_hex_string())
        }
        if (add || remove) && old_q_entry.is_none() && q_entry.is_none() {
            reject_query!("OutMsg with key (message hash) {} should have removed or added OutMsgQueue entry with key {}, \
                but it is absent both from the old and from the new output queues",
                    key.to_hex_string(), q_key.to_hex_string())
        }
        if (!add && !remove) && (old_q_entry.is_some() || q_entry.is_some()) {
            reject_query!("OutMsg with key (message hash) {} is a msg_export_imm$010, so the OutMsgQueue entry with key {} \
                should never be created, but it is present in either the old or the new output queue",
                    key.to_hex_string(), q_key.to_hex_string())
        }
        // NB: if mode!=0, the OutMsgQueue entry has been changed, so we have already checked some conditions in precheck_one_message_queue_update()
        if add {
            match q_entry {
                Some(q_entry) => if q_entry.envelope_hash() != msg_env_hash {
                    reject_query!("OutMsg with key {} has created OutMsgQueue entry with key {} containing a different MsgEnvelope",
                        key.to_hex_string(), q_key.to_hex_string())
                }
                None => reject_query!("OutMsg with key {} was expected to create OutMsgQueue entry with key {} but it did not",
                    key.to_hex_string(), q_key.to_hex_string())
            }
            // ...
        } else if remove {
            match old_q_entry {
                Some(ref old_q_entry) => if old_q_entry.envelope_hash() != msg_env_hash {
                    reject_query!("OutMsg with key {} has dequeued OutMsgQueue entry with key {} containing a different MsgEnvelope",
                        key.to_hex_string(), q_key.to_hex_string())
                }
                None => reject_query!("OutMsg with key {} was expected to remove OutMsgQueue entry with key {} \
                    but it did not exist in the old queue", key.to_hex_string(), q_key.to_hex_string())
            }
            // ...
        }

        self.check_reimport(&out_msg, key)?;
        let reimport = out_msg.read_reimport_message()?;

        // ...
        match out_msg {
            // msg_export_imm
            OutMsg::Immediately(_) => match reimport {
                // msg_import_imm
                Some(InMsg::Immediatelly(ref info)) => {
                    if info.message_cell().repr_hash() != msg_env_hash {
                        reject_query!("msg_import_imm InMsg record corresponding to msg_export_imm OutMsg record with key {} \
                            re-imported a different MsgEnvelope", key.to_hex_string())
                    }
                    if !self.shard().contains_full_prefix(&dest_prefix) {
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
                log::debug!(target: "validator", "src: {}, dst: {}, shard: {}", src_prefix, dest_prefix, self.shard());
                // perform hypercube routing for this new message
                let ia = IntermediateAddress::default();
                let route_info = src_prefix.perform_hypercube_routing(&dest_prefix, &self.shard(), &ia)
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
                CHECK!(self.shard().contains_full_prefix(&src_prefix));
                if self.shard().contains_full_prefix(&dest_prefix) {
                    // log::debug!(target: "validator", "(THIS) src=" << src_prefix cur=" << cur_prefix next=" << next_prefix dest=" << dest_prefix route_info=(" << route_info.first << "," << route_info.second << ")";
                    CHECK!(cur_prefix == dest_prefix);
                    CHECK!(next_prefix == dest_prefix);
                    self.update_min_enqueued_lt_hash(header.created_lt, key);
                } else {
                    // sanity check of the implementation of hypercube routing
                    // log::debug!(target: "validator", "(THAT) src=" << src_prefix cur=" << cur_prefix next=" << next_prefix dest=" << dest_prefix;
                    CHECK!(self.shard().contains_full_prefix(&cur_prefix));
                    CHECK!(!self.shard().contains_full_prefix(&next_prefix));
                }
            // ...
            }
            // msg_export_tr
            OutMsg::Transit(_) => match reimport {
                // msg_import_tr
                Some(InMsg::Transit(info)) => {
                    let in_env = info.read_in_message()?;
                    CHECK!(Some(in_env.message_cell()), msg_cell_opt.as_ref());
                    let in_env = info.read_in_message()?;
                    let in_cur_prefix  = src_prefix.interpolate_addr_intermediate(&dest_prefix, &in_env.cur_addr())?;
                    let in_next_prefix = src_prefix.interpolate_addr_intermediate(&dest_prefix, &in_env.next_addr())?;
                    if self.shard().contains_full_prefix(&in_cur_prefix) {
                        reject_query!("msg_export_tr OutMsg record with key {} corresponds to msg_import_tr InMsg record \
                            with current imported message address {} inside the current shard \
                            (msg_export_tr_req should have been used instead)", key.to_hex_string(), in_cur_prefix);
                    }
                    // we have already checked correctness of hypercube routing in InMsg::msg_import_tr case of check_in_msg()
                    CHECK!(self.shard().contains_full_prefix(&in_next_prefix));
                    CHECK!(self.shard().contains_full_prefix(&cur_prefix));
                    CHECK!(!self.shard().contains_full_prefix(&next_prefix));
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
                for neighbor in self.output_queue_manager.neighbors() {
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
                    log::warn!(target: "validator", "msg_export_deq OutMsg entry with key {} claims the dequeued message with next hop {} \
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
                        reject_query!("hash {:x} != Message hash {:x}", info.in_message_cell().repr_hash(), key)
                    }
                    let in_env = info.read_in_message()?;
                    let in_cur_prefix  = src_prefix.interpolate_addr_intermediate(&dest_prefix, &in_env.cur_addr())?;
                    let in_next_prefix = src_prefix.interpolate_addr_intermediate(&dest_prefix, &in_env.next_addr())?;
                    if !self.shard().contains_full_prefix(&in_cur_prefix) {
                            reject_query!("msg_export_tr_req OutMsg record with key {} corresponds to \
                                msg_import_tr InMsg record with current imported message address {} \
                                outside the current shard (msg_export_tr should have been used instead, because there \
                                was no re-queueing)", key.to_hex_string(), in_cur_prefix)
                    }
                    // we have already checked correctness of hypercube routing in InMsg::msg_import_tr case of check_in_msg()
                    CHECK!(self.shard().contains_full_prefix(&in_next_prefix));
                    CHECK!(self.shard().contains_full_prefix(&cur_prefix));
                    CHECK!(!self.shard().contains_full_prefix(&next_prefix));
                    // so we have just to check that the rewritten message (envelope) has been enqueued
                    // (already checked above for q_entry since mode = 3)
                    // and that the original message (envelope) has been dequeued
                    let q_key = OutMsgQueueKey::with_account_prefix(&in_next_prefix, key.clone());
                    let q_entry = self.output_queue_manager.next().message(&q_key)?;
                    let enq_msg_descr = self.output_queue_manager.prev().message(&q_key)?.ok_or_else(|| error!(
                            "msg_export_tr_req OutMsg record with key {} was expected to dequeue message from \
                            OutMsgQueue with key {} but such a message is absent from the old OutMsgQueue", 
                                key.to_hex_string(), q_key.to_hex_string()))?;
                    if q_entry.is_some() {
                        reject_query!("msg_export_tr_req OutMsg record with key {} \
                            was expected to dequeue message from OutMsgQueue with key {} \
                            but such a message is still present in the new OutMsgQueue",
                                key.to_hex_string(), q_key.to_hex_string())
                    }
                    if enq_msg_descr.envelope_hash() != info.in_message_cell().repr_hash() {
                        reject_query!("msg_import_tr InMsg entry corresponding to msg_export_tr_req OutMsg entry with key {} \
                            has re-imported a different MsgEnvelope from that present in the old OutMsgQueue", key.to_hex_string())
                    }
                }
                _ => reject_query!("cannot unpack msg_import_tr InMsg record corresponding to \
                    msg_export_tr_req OutMsg record with key {}", key.to_hex_string())
            }
            // msg_export_deq_imm
            OutMsg::DequeueImmediately(_) => match reimport {
                // msg_import_fin
                Some(InMsg::Final(info)) => {
                    if info.message_cell().repr_hash() != msg_env_hash {
                        reject_query!("msg_import_fin InMsg record corresponding to msg_export_deq_imm OutMsg record with key {} \
                            somehow imported a different MsgEnvelope from that dequeued by msg_export_deq_imm", key.to_hex_string())
                    }
                    if !self.shard().contains_full_prefix(&cur_prefix) {
                        reject_query!("msg_export_deq_imm OutMsg record with key {} 
                            dequeued a MsgEnvelope with current address {}... outside current shard",
                                key.to_hex_string(), cur_prefix)
                    }
                    // we have already checked more conditions in check_in_msg() case msg_import_fin
                    CHECK!(self.shard().contains_full_prefix(&next_prefix));  // sanity check
                    CHECK!(self.shard().contains_full_prefix(&dest_prefix));  // sanity check
                    // ...
                }
                _ => reject_query!("cannot unpack msg_import_fin InMsg record corresponding to msg_export_deq_imm \
                    OutMsg record with key {}", key.to_hex_string())
            }
            _ => {
                log::error!(target: "validator", "unknown OutMsg tag");
                return Ok(false)
            }
        }
        Ok(true)
    }

    fn check_out_msg_descr(&mut self) -> Result<bool> {
        log::debug!(target: "validator", "checking outbound messages listed in OutMsgDescr");
        let out_msg_descr = self.out_msg_descr.clone();
        out_msg_descr.iterate_with_keys(|key, out_msg|
            self.check_out_msg(&key, &out_msg)
                .map_err(|err| error!("invalid OutMsg with key {} in the new block {} : {}",
                    key.to_hex_string(), self.block_id(), err))
        ).map_err(|err| error!("invalid OutMsgDescr dictionary in the new block {} : {}", self.block_id(), err))
    }

    // compare to Collator::update_processed_upto()
    fn check_processed_upto(&mut self) -> Result<bool> {
        log::debug!(target: "validator", "checking ProcessedInfo");
        if !self.output_queue_manager.next().is_reduced() {
            reject_query!("new ProcessedInfo is not reduced (some entries completely cover other entries)");
        }
        let ok_upd = self.output_queue_manager.next().is_simple_update_of(self.output_queue_manager.prev());
        if !ok_upd.0 {
            reject_query!("new ProcessedInfo is not obtained from old ProcessedInfo by adding at most one new entry")
        } else if let Some(upd) = ok_upd.1 {
            if upd.shard != self.shard().shard_prefix_with_tag() {
                reject_query!("newly-added ProcessedInfo entry refers to shard {} distinct from the current shard {}",
                    ShardIdent::with_tagged_prefix(self.shard().workchain_id(), upd.shard)?, self.shard())
            }
            let ref_mc_seqno = match self.shard().is_masterchain() {
                true => self.block_id().seq_no,
                false => self.mc_state.state().seq_no()
            };
            if upd.mc_seqno != ref_mc_seqno {
                reject_query!("newly-added ProcessedInfo entry refers to masterchain block {} but \
                    the processed inbound message queue belongs to masterchain block {}",
                        upd.mc_seqno, ref_mc_seqno)
            }
            if upd.last_msg_lt >= self.info.end_lt() {
                reject_query!("newly-added ProcessedInfo entry claims that the last processed message has lt {} \
                    larger than this block's end lt {}", upd.last_msg_lt, self.info.end_lt())
            }
            if upd.last_msg_lt == 0 {
                reject_query!("newly-added ProcessedInfo entry claims that the last processed message has zero lt")
            }
            self.claimed_proc_lt = upd.last_msg_lt;
            self.claimed_proc_hash = upd.last_msg_hash;
        } else {
            self.claimed_proc_lt = 0;
            self.claimed_proc_hash = UInt256::from([0; 32]);
        }
        log::debug!(target: "validator", "ProcessedInfo claims to have processed all inbound messages up to ({},{})",
            self.claimed_proc_lt, self.claimed_proc_hash.to_hex_string());
        if self.claimed_proc_lt < self.proc_lt
            || (self.claimed_proc_lt == self.proc_lt && self.proc_lt != 0 && self.claimed_proc_hash < self.proc_hash) {
            reject_query!("the ProcessedInfo claims to have processed messages only upto ({},{}), \
                but there is a InMsg processing record for later message ({},{})", self.claimed_proc_lt, 
                    self.claimed_proc_hash.to_hex_string(), self.proc_lt, self.proc_hash.to_hex_string())
        }
        if self.min_enq_lt < self.claimed_proc_lt
            || (self.min_enq_lt == self.claimed_proc_lt && !(self.claimed_proc_hash < self.min_enq_hash)) {
            reject_query!("the ProcessedInfo claims to have processed all messages only upto ({},{}), \
                but there is a OutMsg enqueuing record for earlier message ({},{})", self.claimed_proc_lt,
                    self.claimed_proc_hash.to_hex_string(), self.min_enq_lt, self.min_enq_hash.to_hex_string())
        }
        // ...
        Ok(true)
    }

    // similar to Collator::process_inbound_message
    fn check_neighbor_outbound_message_processed(
        &self,
        enq: MsgEnqueueStuff,
        created_lt: u64,
        key: &OutMsgQueueKey,
        nb_block_id: &BlockIdExt,
    ) -> Result<bool> {
        if created_lt != enq.created_lt() {
            reject_query!("EnqueuedMsg with key {} in outbound queue of our neighbor {} \
                pretends to have been created at lt {} but its actual creation lt is {}",
                    key.to_hex_string(), nb_block_id, created_lt, enq.created_lt())
        }
        CHECK!(self.shard().contains_full_prefix(&enq.next_prefix()));

        let in_msg = self.in_msg_descr.get(&key.hash)?;
        let out_msg = self.out_msg_descr.get(&key.hash)?;
        let f0 = self.output_queue_manager.prev().already_processed(&enq)?;
        let f1 = self.output_queue_manager.next().already_processed(&enq)?;
        if f0 && !f1 {
            reject_query!("a previously processed message has been un-processed \
                (impossible situation after the validation of ProcessedInfo)")
        } else if f0 { // f0 && f1
            // this message has been processed in a previous block of this shard
            // just check that we have not imported it once again
            if in_msg.is_some() {
                reject_query!("have an InMsg entry for processing again already processed EnqueuedMsg with key {} \
                    of neighbor {}",  key.to_hex_string(), nb_block_id)
            }
            if self.shard().contains_full_prefix(&enq.cur_prefix()) {
                // if this message comes from our own outbound queue, we must have dequeued it
                let deq_hash = match out_msg {
                    None => reject_query!("our old outbound queue contains EnqueuedMsg with key {} \
                        already processed by this shard, but there is no ext_message_deq OutMsg record for this \
                        message in this block", key.to_hex_string()),
                    Some(OutMsg::DequeueShort(deq)) => deq.msg_env_hash,
                    Some(OutMsg::DequeueImmediately(deq)) => deq.out_message_cell().repr_hash(),
                    Some(deq) => reject_query!("{:?} msg_export_deq OutMsg record for already \
                        processed EnqueuedMsg with key {} of old outbound queue", deq, key.to_hex_string())
                };
                if deq_hash != enq.envelope_hash() {
                    reject_query!("unpack ext_message_deq OutMsg record for already processed EnqueuedMsg with key {} \
                        of old outbound queue contains a different MsgEnvelope", key.to_hex_string())
                }
            }
            // next check is incorrect after a merge, when ns_.processed_upto has > 1 entries
            // we effectively comment it out
            Ok(true)
            // NB. we might have a non-trivial dequeueing out_entry with this message hash, but another envelope (for transit messages)
            // (so we cannot assert that out_entry is null)
            // if self.claimed_proc_lt != 0
            //     && (self.claimed_proc_lt < created_lt || (self.claimed_proc_lt == created_lt && self.claimed_proc_hash < key.hash)) {
            //     log::error!(target: "validator", "internal inconsistency: new ProcessedInfo claims \
            //         to have processed all messages up to ({},{}) but we had somehow already processed a message ({},{}) \
            //         from OutMsgQueue of neighbor {} key {}", self.claimed_proc_lt, self.claimed_proc_hash.to_hex_string(),
            //             created_lt, key.hash.to_hex_string(), nb_block_id, key.to_hex_string());
            //     return Ok(false)
            // }
            // Ok(true)
        } else if f1 { // !f0 && f1
            // this message must have been imported and processed in this very block
            // (because it is marked processed after this block, but not before)
            if self.claimed_proc_lt == 0 || self.claimed_proc_lt < created_lt
                || (self.claimed_proc_lt == created_lt && self.claimed_proc_hash < key.hash) {
                    reject_query!("internal inconsistency: new ProcessedInfo claims \
                    to have processed all messages up to ({},{}), but we had somehow processed in this block \
                    a message ({},{}) from OutMsgQueue of neighbor {} key {}",
                        self.claimed_proc_lt, self.claimed_proc_hash.to_hex_string(),
                        created_lt, key.hash.to_hex_string(), nb_block_id, key.to_hex_string())
            }
            // must have a msg_import_fin or msg_import_tr InMsg record
            let hash = match in_msg {
                Some(InMsg::Final(info)) => info.message_cell().repr_hash(),
                Some(InMsg::Transit(info)) => info.in_message_cell().repr_hash(),
                None => reject_query!("there is no InMsg entry for processing EnqueuedMsg with key {} \
                    of neighbor {} which is claimed to be processed by new ProcessedInfo of this block", 
                        key.to_hex_string(), nb_block_id),
                _ => reject_query!("expected either a msg_import_fin or a msg_import_tr InMsg record \
                    for processing EnqueuedMsg with key {} of neighbor {} which is claimed to be processed \
                    by new ProcessedInfo of this block", key.to_hex_string(), nb_block_id)
            };
            if hash != enq.envelope_hash() {
                reject_query!("InMsg record for processing EnqueuedMsg with key {} of neighbor {} \
                    which is claimed to be processed by new ProcessedInfo of this block contains a reference \
                    to a different MsgEnvelope", key.to_hex_string(), nb_block_id);
            }
            // all other checks have been done while checking InMsgDescr
            Ok(true)
        } else { // !f0 && !f1
            // the message is left unprocessed in our virtual "inbound queue"
            // just a simple sanity check
            if self.claimed_proc_lt != 0
                && !(self.claimed_proc_lt < created_lt || (self.claimed_proc_lt == created_lt && self.claimed_proc_hash < key.hash)) {
                log::error!(target: "validator", "internal inconsistency: new ProcessedInfo claims \
                    to have processed all messages up to ({},{}), but we somehow have not processed a message ({},{}) \
                    from OutMsgQueue of neighbor {} key {}",
                        self.claimed_proc_lt, self.claimed_proc_hash.to_hex_string(), created_lt, key.hash.to_hex_string(),
                        nb_block_id, key.to_hex_string());
            }
            Ok(false)
        }
    }

    // return true if all queues are processed
    fn check_in_queue(&mut self) -> Result<bool> {
        log::debug!(target: "validator", "check_in_queue len: {}", self.output_queue_manager.neighbors().len());
        let mut iter = self.output_queue_manager.merge_out_queue_iter(self.shard())?;
        while let Some(k_v) = iter.next() {
            let (msg_key, enq, lt, nb_block_id) = k_v?;
            log::debug!(target: "validator", "processing inbound message with \
                (lt,hash)=({},{:x}) from neighbor - {}", lt, msg_key.hash, nb_block_id);
            // if (verbosity > 3) {
            //     std::cerr << "inbound message: lt=" << kv->lt from=" << kv->source key=" << kv->key.to_hex_string() msg=";
            //     block::gen::t_EnqueuedMsg.print(std::cerr, *(kv->msg));
            // }
            match self.check_neighbor_outbound_message_processed(enq, lt, &msg_key, &nb_block_id) {
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
    fn check_delivered_dequeued(&self) -> Result<bool> {
        log::debug!(target: "validator", "scanning new outbound queue and checking delivery status of all messages");
        for nb in self.output_queue_manager.neighbors() {
            if !nb.is_disabled() && !nb.can_check_processed() {
                reject_query!("internal error: no info for checking processed messages from neighbor {}", nb.block_id())
            }
        }
        // TODO: warning may be too much messages
        self.output_queue_manager.next().out_queue().iterate_with_keys_and_aug(|msg_key, enq, created_lt| {
            // log::debug!(target: "validator", "key is " << key.to_hex_string(n));
            let enq = MsgEnqueueStuff::from_enqueue_and_lt(enq, created_lt)?;
            if msg_key.hash != enq.message_hash() {
                reject_query!("cannot unpack EnqueuedMsg with key {} in the new OutMsgQueue", msg_key.to_hex_string())
            }
            log::debug!(target: "validator", "scanning outbound message with (lt,hash)=({},{}) enqueued_lt={}",
                created_lt, msg_key.hash.to_hex_string(), enq.enqueued_lt());
            for nb in self.output_queue_manager.neighbors() {
                // could look up neighbor with shard containing enq_msg_descr.next_prefix more efficiently
                // (instead of checking all neighbors)
                if !nb.is_disabled() && nb.already_processed(&enq)? {
                    // the message has been delivered but not removed from queue!
                    log::warn!(target: "validator", "outbound queue not cleaned up completely (overfull block?): \
                        outbound message with (lt,hash)=({},{}) enqueued_lt={} has been already delivered and \
                        processed by neighbor {} but it has not been dequeued in this block and it is still \
                        present in the new outbound queue", created_lt, msg_key.hash.to_hex_string(),
                            enq.enqueued_lt(), nb.block_id());
                    return Ok(false)
                }
            }
            if created_lt >= self.info.start_lt() {
                log::debug!(target: "validator", "stop scanning new outbound queue");
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
    //         if (!ptr->init_new(self.now())) {
    //         return nullptr;
    //         }
    //     } else if (!ptr->unpack(std::move(account), std::move(extra), self.now(),
    //                             self.shard().is_masterchain() && self.config_params->is_special_smartcontract(addr))) {
    //         return nullptr;
    //     }
    //     ptr->block_lt = self.info.start_lt();
    //     return ptr;
    // }

    // similar to Collator::make_account()
    fn unpack_account(&self, addr: &UInt256) -> Result<(Cell, Account)> {
        match self.prev_state_accounts.get(addr)? {
            Some(shard_acc) => {
                let new_acc = shard_acc.read_account()?;
                if !new_acc.belongs_to_shard(&self.shard)? {
                    reject_query!("old state of account {} does not really belong to current shard", addr.to_hex_string())
                }
                Ok((shard_acc.account_cell().clone(), new_acc))
            }
            None => {
                let new_acc = Account::AccountNone;
                Ok((new_acc.serialize()?, new_acc))
            }
        }
    }

    fn check_one_transaction(
        &mut self,
        account_addr: &UInt256,
        account_root: &mut Cell,
        account: &mut Account,
        lt: u64,
        trans_root: Cell,
        is_first: bool,
        is_last: bool
    ) -> Result<bool> {
        log::debug!(target: "validator", "checking {} transaction {} of account {}",
            lt, trans_root.repr_hash().to_hex_string(), account_addr.to_hex_string());
        let trans = Transaction::construct_from_cell(trans_root.clone())?;
        let account_create = account == &Account::AccountNone;

        // check input message
        let mut money_imported = CurrencyCollection::default();
        let mut money_exported = CurrencyCollection::default();
        let in_msg = trans.read_in_msg()?;
        if let Some(in_msg_root) = trans.in_msg_cell() {
            let in_msg = self.in_msg_descr.get(&in_msg_root.repr_hash())?.ok_or_else(|| error!("inbound message with hash {} of \
                transaction {} of account {} does not have a corresponding InMsg record",
                    in_msg_root.repr_hash().to_hex_string(), lt, account_addr.to_hex_string()))?;
            let msg = Message::construct_from_cell(in_msg_root.clone())?;
            // once we know there is a InMsg with correct hash, we already know that it contains a message with this hash (by the verification of InMsg), so it is our message
            // have still to check its destination address and imported value
            // and that it refers to this transaction
            match in_msg {
                InMsg::External(_) => (),
                InMsg::IHR(_) | InMsg::Immediatelly(_) | InMsg::Final(_) => {
                    let header = msg.int_header().ok_or_else(|| error!("inbound message transaction {} of {} must have \
                        internal message header", lt, account_addr.to_hex_string()))?;
                    if header.created_lt >= lt {
                        reject_query!("transaction {} of {} processed inbound message created later at logical time {}",
                            lt, account_addr.to_hex_string(), header.created_lt)
                    }
                    if header.created_lt != self.info.start_lt() || !self.is_special_in_msg(&in_msg) {
                        self.msg_proc_lt.push((account_addr.clone(), lt, header.created_lt));
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
            let dst = match msg.dst() {
                Some(MsgAddressInt::AddrStd(dst)) => dst,
                _ => reject_query!("inbound message with hash {} transaction {} of {} must have std internal destination address",
                    in_msg_root.repr_hash().to_hex_string(), lt, account_addr.to_hex_string())
            };
            if dst.workchain_id as i32 != self.shard().workchain_id() || &UInt256::from(dst.address.get_bytestring(0)) != account_addr {
                reject_query!("inbound message of transaction {} of account {} has a different destination address {}:{}",
                    account_addr.to_hex_string(), lt, dst.workchain_id, dst.address.to_hex_string())
            }
            CHECK!(in_msg.transaction_cell().is_some());
            if let Some(cell) = in_msg.transaction_cell() {
                if cell != &trans_root {
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
            let out_msg = self.out_msg_descr.get(&out_msg_root.repr_hash())?.ok_or_else(|| error!("outbound message #{} \
                with hash {} of transaction {} of account {} does not have a corresponding  record",
                    i, out_msg_root.repr_hash().to_hex_string(), lt, account_addr.to_hex_string()))?;
            // once we know there is an OutMsg with correct hash, we already know that it contains a message with this hash (by the verification of OutMsg), so it is our message
            // have still to check its source address, lt and imported value
            // and that it refers to this transaction as its origin
            let msg = Message::construct_from_cell(out_msg_root.clone())?;
            match out_msg {
                OutMsg::External(_) => (),
                OutMsg::Immediately(_) | OutMsg::New(_) => {
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
            let src = match msg.src() {
                Some(MsgAddressInt::AddrStd(src)) => src,
                _ => reject_query!("outbound message #{} with hash {} of transaction {} of account {} \
                    does not have a correct header",
                        i, out_msg_root.repr_hash().to_hex_string(), lt, account_addr.to_hex_string())
            };
            if src.workchain_id as i32 != self.shard().workchain_id() || &UInt256::from(src.address.get_bytestring(0)) != account_addr {
                reject_query!("outbound message #{} of transaction {} of account {} has a different source address {}:{}",
                    i, lt, account_addr.to_hex_string(), src.workchain_id, src.address.to_hex_string())
            }
            match out_msg.transaction_cell() {
                Some(cell) => if cell != &trans_root {
                    reject_query!("OutMsg record for outbound message #{} with hash {} of transaction {} \
                        of account {} refers to a different processing transaction",
                            i, out_msg_root.repr_hash().to_hex_string(), lt, account_addr.to_hex_string())
                }
                None => CHECK!(out_msg.transaction_cell().is_some())
            }
            Ok(true)
        })?;
        // check general transaction data
        let old_balance = account.get_balance().cloned().unwrap_or_default();
        let descr = trans.read_description()?;
        let split = descr.is_split();
        if split || descr.is_merge() {
            if self.shard().is_masterchain() {
                reject_query!("transaction {} of account {} is a split/merge prepare/install transaction, which is impossible in a masterchain block",
                    lt, account_addr.to_hex_string())
            }
            if split && !self.info.before_split() {
                reject_query!("transaction {} of account {} is a split prepare/install transaction, but this block is not before a split",
                    lt, account_addr.to_hex_string())
            }
            if split && !is_last {
                reject_query!("transaction {} of account {} is a split prepare/install transaction, \
                    but it is not the last transaction for this account in this block",
                        lt, account_addr.to_hex_string())
            }
            if !split && !self.info.after_merge() {
                reject_query!("transaction {} of account {} is a merge prepare/install transaction, \
                    is a merge prepare/install transaction, but this block is not immediately after a merge",
                        lt, account_addr.to_hex_string())
            }
            if !split && !is_first {
                reject_query!("transaction {} of account {} is a merge prepare/install transaction, \
                    is a merge prepare/install transaction, but it is not the first transaction for this account in this block",
                        lt, account_addr.to_hex_string())
            }
            // check later a global configuration flag in self.config_params.global_flags_
            // (for now, split/merge transactions are always globally disabled)
            reject_query!("transaction {} of account {} is a split/merge prepare/install transaction, which are globally disabled",
                lt, account_addr.to_hex_string())
        }
        if let TransactionDescr::TickTock(ref info) = descr {
            if !self.shard().is_masterchain() {
                reject_query!("transaction {} of account {} is a tick-tock transaction, which is impossible outside a masterchain block",
                    lt, account_addr.to_hex_string())
            }
            if let Some(acc_tick_tock) = account.get_tick_tock() {
                if !info.tt.is_tock() {
                    if !is_first {
                        reject_query!("transaction {} of account {} is a tick transaction, but this is not the first transaction of this account",
                            lt, account_addr.to_hex_string())
                    }
                    if lt != self.info.start_lt() + 1 {
                        reject_query!("transaction {} of account {} is a tick transaction, but its logical start time differs from block's start time {} by more than one",
                            lt, account_addr.to_hex_string(), self.info.start_lt())
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
            if is_first && self.shard().is_masterchain() && acc_tick_tock.tick
                && !tick_tock.as_ref().map(|ref tt| tt.is_tick()).unwrap_or_default()
                && !account_create {
                reject_query!("transaction {} of account {} is the first transaction \
                    for this special tick account in this block, \
                    but the transaction is not a tick transaction",
                        lt, account_addr.to_hex_string())
            }
            if is_last && self.shard().is_masterchain() && acc_tick_tock.tock
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
        if state_update.old_hash != account_root.repr_hash() {
            reject_query!("transaction {} of account {:x} claims that the original \
                account state hash must be {:x} but the actual value is {:x}",
                    lt, account_addr, state_update.old_hash, account_root.repr_hash())
        }
        // some type-specific checks
        let tr_lt = Arc::new(AtomicU64::new(lt));
        let executor: Arc<dyn TransactionExecutor> = match descr {
            TransactionDescr::Ordinary(_) => {
                if trans.in_msg_cell().is_none() {
                    reject_query!("ordinary transaction {} of account {} has no inbound message",
                        lt, account_addr.to_hex_string())
                }
                Arc::new(OrdinaryTransactionExecutor::new(self.config.clone()))
            }
            TransactionDescr::Storage(_) => {
                if trans.in_msg_cell().is_some() {
                    reject_query!("storage transaction {} of account {} has an inbound message",
                        lt, account_addr.to_hex_string())
                }
                if trans.outmsg_cnt != 0 {
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
                Arc::new(TickTockTransactionExecutor::new(self.config.clone(), info.tt.clone()))
            }
            TransactionDescr::MergePrepare(_) => {
                if trans.in_msg_cell().is_some() {
                    reject_query!("merge prepare transaction {} of account {} has an inbound message",
                        lt, account_addr.to_hex_string())
                }
                if trans.outmsg_cnt != 1 {
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
                if trans.outmsg_cnt != 1 {
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
        let mut trans2 = executor.execute(in_msg.as_ref(), account_root, self.now(), self.info.start_lt(), tr_lt, false)?;
        // we cannot know prev transaction in executor
        trans2.set_prev_trans_hash(trans.prev_trans_hash());
        trans2.set_prev_trans_lt(trans.prev_trans_lt());
        let trans2_root = trans2.serialize()?;
        // ....
        // check transaction computation by re-doing it
        // similar to Collator::create_ordinary_transaction() and Collator::create_ticktock_transaction()
        // ....
        if trans_root != trans2_root {
            reject_query!("re created transaction {} doesn't correspond", lt)
        }
        if state_update.new_hash != account_root.repr_hash() {
            reject_query!("transaction {} of {} is invalid: it claims that the new \
                account state hash is {} but the re-computed value is {}",
                    lt, account_addr.to_hex_string(), state_update.new_hash.to_hex_string(), 
                    account_root.repr_hash().to_hex_string())
        }
        if trans.out_msgs != trans2.out_msgs {
            reject_query!("transaction {} of {} is invalid: it has produced a set of outbound messages different from that listed in the transaction",
                lt, account_addr.to_hex_string())
        }
        // check new balance and value flow
        let mut left_balance = old_balance.clone();
        left_balance.add(&money_imported)?;
        let new_account = Account::construct_from_cell(account_root.clone())?;
        let new_balance = new_account.get_balance().cloned().unwrap_or_default();
        let mut right_balance = new_balance.clone();
        right_balance.add(&money_exported)?;
        right_balance.add(&trans.total_fees)?;
        if left_balance != right_balance {
            reject_query!("transaction {} of {} violates the currency flow condition: \
                old balance={} + imported={} does not equal new balance={} + exported=\
                {} + total_fees={}", lt, account_addr.to_hex_string(),
                    old_balance.grams, money_imported.grams,
                    new_balance.grams, money_exported.grams, trans.total_fees.grams)
        }
        *account = new_account;
        Ok(true)
    }

    // NB: may be run in parallel for different accounts
    fn check_account_transactions(
        &mut self,
        account_addr: &UInt256,
        account_root: &mut Cell,
        account: Account,
        acc_block: AccountBlock,
        _fee: CurrencyCollection
    ) -> Result<bool> {
        // CHECK!(account.get_id(), Some(account_addr.as_slice().into()));
        let min_trans_lt: u64 = acc_block.transactions().get_min(false)?.ok_or_else(|| error!("no minimal transaction"))?.0;
        let max_trans_lt: u64 = acc_block.transactions().get_max(false)?.ok_or_else(|| error!("no maximal transaction"))?.0;
        let mut new_account = account.clone();
        acc_block.transactions().iterate_slices_with_keys(|lt, trans| {
            self.check_one_transaction(account_addr, account_root, &mut new_account, lt, trans.reference(0)?,
                lt == min_trans_lt, lt == max_trans_lt)
        }).map_err(|err| error!("at least one Transaction of account {} is invalid : {}", account_addr.to_hex_string(), err))?;
        if self.shard().is_masterchain() {
            self.scan_account_libraries(account.libraries(), new_account.libraries(), account_addr)
        } else {
            Ok(true)
        }
    }

    fn check_transactions(&mut self) -> Result<bool> {
        log::debug!(target: "validator", "checking all transactions");
        let account_blocks = self.account_blocks.clone();
        account_blocks.iterate_with_keys_and_aug(|account_addr, acc_block, fee| {
            let (mut account_root, account) = self.unpack_account(&account_addr)?;
            self.check_account_transactions(&account_addr, &mut account_root, account, acc_block, fee)
        })
    }

    // similar to Collator::update_account_public_libraries()
    fn scan_account_libraries(&mut self, orig_libs: StateInitLib, final_libs: StateInitLib, addr: &UInt256) -> Result<bool> {
        orig_libs.scan_diff(&final_libs, |key: UInt256, old, new| {
            let f = old.map(|descr| descr.is_public_library()).unwrap_or_default();
            let g = new.map(|descr| descr.is_public_library()).unwrap_or_default();
            if f != g {
                self.lib_publishers.push((key, addr.clone(), g));
            }
            Ok(true)
        }).map_err(|err| error!("error scanning old and new libraries of account {} : {}", addr.to_hex_string(), err))
    }

    fn check_all_ticktock_processed(&self) -> Result<()> {
        if !self.shard().is_masterchain() {
            return Ok(())
        }
        log::debug!(target: "validator", "getting the list of special tick-tock smart contracts");
        let ticktock_smcs = self.config_params.special_ticktock_smartcontracts(3, &self.prev_state_accounts)?;
        log::debug!(target: "validator", "have {} tick-tock smart contracts", ticktock_smcs.len());
        for addr in ticktock_smcs {
            log::debug!(target: "validator", "special smart contract {} with ticktock={}", addr.0.to_hex_string(), addr.1);
            if self.account_blocks.get(&addr.0)?.is_none() {
                reject_query!("there are no transactions (and in particular, no tick-tock transactions) \
                    for special smart contract {} with ticktock={}",
                        addr.0.to_hex_string(), addr.1)
            }
        }
        Ok(())
    }

    fn check_message_processing_order(&mut self) -> Result<()> {
        self.msg_proc_lt.sort();
        for i in 1..self.msg_proc_lt.len() {
            let a = &self.msg_proc_lt[i - 1];
            let b = &self.msg_proc_lt[i];
            if a.0 == b.0 && a.2 > b.2 {
                reject_query!("incorrect message processing order: transaction ({},{}) processes message created at logical time {}, \
                    but a later transaction ({},{}) processes an earlier message created at logical time {}",
                    a.1, a.0.to_hex_string(), a.2, b.1, b.0.to_hex_string(), b.2)
            }
        }
        Ok(())
    }

    fn check_special_message(&self, in_msg: Option<&InMsg>, amount: &CurrencyCollection, addr: UInt256) -> Result<()> {
        let in_msg = match in_msg {
            Some(in_msg) => in_msg,
            None if amount.is_zero()? => return Ok(()),
            None => reject_query!("no special message, but amount is {}", amount)
        };
        CHECK!(!amount.is_zero()?);
        if !self.shard().is_masterchain() {
            reject_query!("special messages can be present in masterchain only")
        }
        let env = match in_msg {
            InMsg::Immediatelly(in_msg) => in_msg.read_message()?,
            _ => reject_query!("wrong type of message")
        };
        let msg_hash = env.message_cell().repr_hash();
        log::debug!(target: "validator", "checking special message with hash {} and expected amount {}",
            msg_hash.to_hex_string(), amount);
        match self.extra.read_in_msg_descr()?.get(&msg_hash)? {
            Some(msg) => if &msg != in_msg {
                reject_query!("InMsg of special message with hash {} differs from the InMsgDescr entry with this key", msg_hash.to_hex_string())
            }
            None => reject_query!("InMsg of special message with hash {} is not registered in InMsgDescr", msg_hash.to_hex_string())
        };

        let msg = env.read_message()?;
        // CHECK!(tlb::unpack(cs, info));  // this has been already checked for all InMsgDescr
        let header = msg.int_header().ok_or_else(|| error!("InMsg of special message with hash {} has wrong header", msg_hash.to_hex_string()))?;
        let src_prefix = AccountIdPrefixFull::prefix(&msg.src().unwrap_or_default())?;
        let dst_prefix = AccountIdPrefixFull::prefix(&header.dst)?;
        // CHECK!(src_prefix.is_valid() && dst_prefix.is_valid());  // we have checked this for all InMsgDescr
        let cur_prefix  = src_prefix.interpolate_addr_intermediate(&dst_prefix, &env.cur_addr())?;
        let next_prefix = src_prefix.interpolate_addr_intermediate(&dst_prefix, &env.next_addr())?;
        if cur_prefix != dst_prefix || next_prefix != dst_prefix {
            reject_query!("special message with hash {} has not been routed to its final destination", msg_hash.to_hex_string())
        }
        if !self.shard().contains_full_prefix(&src_prefix) {
            reject_query!("special message with hash {} has source address {} outside this shard",
                msg_hash.to_hex_string(), src_prefix)
        }
        if !self.shard().contains_full_prefix(&dst_prefix) {
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
        let (src, dst) = match (&header.src, &header.dst) {
            (MsgAddressIntOrNone::Some(MsgAddressInt::AddrStd(ref src)), MsgAddressInt::AddrStd(ref dst)) => (src.clone(), dst.clone()),
            _ => reject_query!("cannot unpack source and destination addresses of special message with hash {}", msg_hash.to_hex_string())
        };
        let src_addr = UInt256::construct_from(&mut src.address.clone())?;
        if src.workchain_id as i32 != ton_block::MASTERCHAIN_ID || !src_addr.is_zero() {
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

    fn check_special_messages(&self) -> Result<()> {
        self.check_special_message(self.recover_create_msg.as_ref(), &self.value_flow.recovered, self.config_params.fee_collector_address()?)?;
        self.check_special_message(self.mint_msg.as_ref(), &self.value_flow.minted, self.config_params.minter_address()?)?;
        Ok(())
    }

    fn check_one_library_update(&mut self, key: UInt256, old: Option<LibDescr>, new: Option<LibDescr>) -> Result<bool> {
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
            self.lib_publishers2.push((key.clone(), publisher, new.is_some()));
            Ok(true)
        }).map_err(|err| error!("invalid publishers set for shard library with hash {} : {}", key.to_hex_string(), err))
    }

    fn check_shard_libraries(&mut self) -> Result<()> {
        let old = self.prev_libraries.clone();
        let new = self.next_libraries.clone();
        old.scan_diff(&new, |key: UInt256, old, new| self.check_one_library_update(key, old, new))
            .map_err(|err| error!("invalid shard libraries dictionary in the new state : {}", err))?;
        self.lib_publishers.sort();
        self.lib_publishers2.sort();
        if self.lib_publishers != self.lib_publishers2 {
            // TODO: better error message with by-element comparison?
            reject_query!("the set of public libraries and their publishing accounts has not been updated correctly")
        }
        Ok(())
    }

    fn check_new_state(&mut self) -> Result<()> {
        log::debug!(target: "validator", "checking header of the new shardchain state");
        let my_mc_seqno = if self.shard().is_masterchain() {
            self.block_id().seq_no()
        } else {
            self.mc_blkid.seq_no()
        };
        let min_seq_no = match self.output_queue_manager.next().min_seqno() {
            0 => std::u32::MAX,
            min_seq_no => min_seq_no
        };
        let ref_mc_seqno = std::cmp::min(min_seq_no, std::cmp::min(self.min_shard_ref_mc_seqno, my_mc_seqno));
        if self.next_state.state().min_ref_mc_seqno() != ref_mc_seqno {
            reject_query!("new state of {} has minimal referenced masterchain block seqno {} but the value \
                computed from all shard references and previous masterchain block reference is {} = min({},{},{})",
                    self.block_id(), self.next_state.state().min_ref_mc_seqno(), ref_mc_seqno,
                    my_mc_seqno, self.min_shard_ref_mc_seqno, min_seq_no)
        }

        if !self.next_state.state().read_out_msg_queue_info()?.ihr_pending().is_empty() {
            reject_query!("IhrPendingInfo in the new state of {} is non-empty, \
                but IHR delivery is now disabled", self.block_id())
        }
        // before_split:(## 1) -> checked in unpack_next_state()
        // accounts:^ShardAccounts -> checked in precheck_account_updates() + other
        // ^[ overload_history:uint64 underload_history:uint64
        if self.next_state.state().overload_history() & self.next_state.state().underload_history() & 1 != 0 {
            reject_query!("lower-order bits both set in the new state's overload_history and underload history \
                (block cannot be both overloaded and underloaded)")
        }
        if self.after_split || self.after_merge {
            if (self.next_state.state().overload_history() | self.next_state.state().underload_history()) & !1 != 0 {
                reject_query!("new block is immediately after split or after merge, \
                    but the old underload or overload history has not been cleared")
            }
        } else {
            if (self.next_state.state().overload_history() ^ (self.prev_state.state().overload_history() << 1)) & !1 != 0 {
                reject_query!("new overload history {} is not compatible with the old overload history {}",
                    self.next_state.state().overload_history(), self.prev_state.state().overload_history())
            }
            if (self.next_state.state().underload_history() ^ (self.prev_state.state().underload_history() << 1)) & !1 != 0 {
                reject_query!("new underload history {}  is not compatible with the old underload history {}",
                    self.next_state.state().underload_history(), self.prev_state.state().underload_history())
            }
        }
        if self.next_state.state().total_balance() != &self.value_flow.to_next_blk {
            reject_query!("new state declares total balance {} different from to_next_blk in value flow \
                (obtained by summing balances of all accounts in the new state): {}",
                self.next_state.state().total_balance(), self.value_flow.to_next_blk)
        }
        log::debug!(target: "validator", "checking total validator fees: new={}+recovered={} == old={}+collected={}",
            self.next_state.state().total_validator_fees(), self.value_flow.recovered,
            self.prev_validator_fees, self.value_flow.fees_collected);
        let mut new = self.value_flow.recovered.clone();
        new.add(self.next_state.state().total_validator_fees())?;
        let mut old = self.value_flow.fees_collected.clone();
        old.add(&self.prev_validator_fees)?;
        if new != old {
            reject_query!("new state declares total validator fees {} \
                not equal to the sum of old total validator fees {} \
                and the fees collected in this block {} \
                minus the recovered fees {}",
                    self.next_state.state().total_validator_fees(), &self.prev_validator_fees,
                    self.value_flow.fees_collected, self.value_flow.recovered)
        }
        // libraries:(HashmapE 256 LibDescr)
        if self.shard().is_masterchain() {
            self.check_shard_libraries()
                .map_err(|err| error!("the set of public libraries in the new state is invalid : {}", err))?;
        } else if !self.next_state.state().libraries().is_empty() {
            reject_query!("new state contains a non-empty public library collection, which is not allowed for non-masterchain blocks")
        }
        // TODO: it seems was tested in unpack_next_state
        if self.shard().is_masterchain() && self.next_state.state().master_ref().is_some() {
            reject_query!("new state contains a masterchain block reference (master_ref)")
        } else if !self.shard().is_masterchain() && self.next_state.state().master_ref().is_none() {
            reject_query!("new state does not contain a masterchain block reference (master_ref)")
        }
        // custom:(Maybe ^McStateExtra) -> checked in check_mc_state_extra()
        // = ShardStateUnsplit;
        Ok(())
    }

    fn check_config_update(&self) -> Result<()> {
        if self.next_state_extra.config.config_params.count(10000).is_err() {
            reject_query!("new configuration failed to pass letmated validity checks")
        }
        if self.prev_state_extra.config.config_params.count(10000).is_err() {
            reject_query!("old configuration failed to pass letmated validity checks")
        }
        if !self.next_state_extra.config.valid_config_data(false, None)? {
            reject_query!("new configuration parameters failed to pass per-parameter letmated validity checks, 
                or one of mandatory configuration parameters is missing")
        }
        let new_accounts = self.next_state.state().read_accounts()?;
        let old_config_root = match new_accounts.get(&self.prev_state_extra.config.config_addr)? {
            Some(account) => account.read_account()?.get_data(),
            None => reject_query!("cannot extract configuration from the new state of the (old) configuration smart contract {}",
                self.prev_state_extra.config.config_addr.to_hex_string())
        };
        let new_config_root = match new_accounts.get(&self.next_state_extra.config.config_addr)? {
            Some(account) => account.read_account()?.get_data(),
            None => reject_query!("cannot extract configuration from the new state of the (new) configuration smart contract {}",
                self.next_state_extra.config.config_addr.to_hex_string())
        };
        let cfg_acc_changed = self.prev_state_extra.config.config_addr != self.next_state_extra.config.config_addr;
        if old_config_root != new_config_root {
            reject_query!("the new configuration is different from that stored in the persistent data of the (new) configuration smart contract {}",
                self.prev_state_extra.config.config_addr.to_hex_string())
        }
        let old_config = self.prev_state_extra.config.clone();
        let new_config = self.next_state_extra.config.clone();
        if !old_config.valid_config_data(true, None)? {
            reject_query!("configuration extracted from (old) configuration smart contract {} \
                failed to pass per-parameter validity checks, or one of mandatory parameters is missing",
                old_config.config_addr.to_hex_string())
        }
        if new_config.important_config_parameters_changed(&old_config, false)? {
            // same as the check in Collator::create_mc_state_extra()
            log::warn!(target: "validator", "the global configuration changes in block {}", self.block_id());
            if !self.info.key_block() {
                reject_query!("important parameters in the global configuration have changed, but the block is not marked as a key block")
            }
        } else if self.info.key_block()
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
        if want_cfg_addr == self.prev_state_extra.config.config_addr {
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
                log::warn!(target: "validator", "switching of configuration smart contract did not happen \
                    because the suggested new configuration smart contract {} does not contain a valid configuration",
                        want_cfg_addr.to_hex_string());
                return Ok(())
            }
        };
        // if !self.config_params.valid_config_data(&self.prev_state_extra.config, true, false)? {
        let want_config = ConfigParams::with_address_and_params(want_cfg_addr.clone(), want_config_root);
        if !want_config.valid_config_data(false, None)? {
            log::warn!(target: "validator", "switching of configuration smart contract did not happen \
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
        &self, seq_no: u32,
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
        } else if seq_no != self.mc_state.state().seq_no() {
            reject_query!("new previous blocks dictionary contains a new entry with seqno {} \
                while the only new entry must be for the previous block with seqno {}", seq_no, self.mc_state.state().seq_no())
        } else {
            new.expect("check scan_diff for Hashmap").0
        };
        log::debug!(target: "validator", "prev block id for {} is present", seq_no);
        let (end_lt, block_id, key) = new_val.master_block_id();
        if block_id.seq_no != seq_no {
            reject_query!("new previous blocks dictionary entry with seqno {} in fact describes a block {} with different seqno",
                seq_no, block_id)
        }
        if block_id != self.prev_blocks_ids[0] {
            reject_query!("new previous blocks dictionary has a new entry for previous block {} while the correct previous block is {}",
                block_id, self.prev_blocks_ids[0])
        }
        if end_lt != self.mc_state.state().gen_lt() {
            reject_query!("previous blocks dictionary has new entry for previous block {} \
                indicating end_lt={} but the correct value is {}",
                    block_id, end_lt, self.mc_state.state().gen_lt())
        }
        if key != self.mc_state_extra.after_key_block {
            reject_query!("previous blocks dictionary has new entry for previous block {} indicating is_key_block={} but the correct value is {}",
                block_id, key, self.mc_state_extra.after_key_block)
        }
        Ok(true)
    }

    // somewhat similar to Collator::create_mc_state_extra()
    fn check_mc_state_extra(&mut self) -> Result<()> {
        if !self.shard().is_masterchain() {
            if self.next_state.state().custom_cell().is_some() {
                reject_query!("new state defined by non-masterchain block {} contains a McStateExtra", self.block_id())
            }
            return Ok(())
        }
        log::debug!(target: "validator", "checking header of McStateExtra in the new masterchain state");
        if self.prev_state.state().custom_cell().is_none() {
            reject_query!("previous masterchain state did not contain a McStateExtra")
        }
        if self.next_state.state().custom_cell().is_none() {
            reject_query!("new masterchain state does not contain a McStateExtra")
        }
        self.prev_state_extra = self.prev_state.shard_state_extra()?.clone();
        self.next_state_extra = self.next_state.shard_state_extra()?.clone();
        // masterchain_state_extra#cc26
        // shard_hashes:ShardHashes has been checked separately
        // config:ConfigParams
        self.check_config_update()?;
        // ...
        if self.next_state_extra.block_create_stats.is_some() != self.create_stats_enabled {
            reject_query!("new McStateExtra has block_create_stats, but active configuration defines create_stats_enabled={}",
                self.create_stats_enabled)
        }
        // validator_info:ValidatorInfo
        // (already checked in check_mc_validator_info())
        // prev_blocks_ids:OldMcBlocksInfo
        // comment this temporary if long test
        self.prev_state_extra.prev_blocks.scan_diff_with_aug(
            &self.next_state_extra.prev_blocks,
            |seq_no: u32, old, new| self.check_one_prev_dict_update(seq_no, old, new)
        ).map_err(|err| error!("invalid previous block dictionary in the new state : {}", err))?;
        if let Some((seq_no, _)) = self.prev_state_extra.prev_blocks.get_max(false)? {
            if seq_no >= self.mc_state.state().seq_no() {
                reject_query!("previous block dictionary for the previous state with seqno {} \
                    contains information about 'previous' masterchain block with seqno {}",
                    self.mc_state.state().seq_no(), seq_no)
            }
        }
        let (seq_no, _) = self.next_state_extra.prev_blocks.get_max(false)?.ok_or_else(|| error!("new previous blocks \
            dictionary is empty (at least the immediately previous block should be there)"))?;
        CHECK!(self.block_id().seq_no == self.mc_blkid.seq_no + 1);
        if seq_no > self.mc_state.state().seq_no() {
            reject_query!("previous block dictionary for the new state with seqno {} \
                contains information about a future masterchain block with seqno {}",
                self.block_id().seq_no, seq_no)
        }
        if seq_no != self.mc_state.state().seq_no() {
            reject_query!("previous block dictionary for the new state of masterchain block {} \
                does not contain information about immediately previous block with seqno {}",
                self.block_id(), self.mc_state.state().seq_no())
        }
        // after_key_block:Bool
        if self.next_state_extra.after_key_block != self.info.key_block() {
            reject_query!("new McStateExtra has after_key_block={} while the block header claims is_master_state={}",
                self.next_state_extra.after_key_block, self.info.key_block())
        }
        if self.prev_state_extra.last_key_block.is_some() && self.next_state_extra.last_key_block.is_none() {
            reject_query!("old McStateExtra had a non-trivial last_key_block, but the new one does not")
        }
        if self.next_state_extra.last_key_block == self.prev_state_extra.last_key_block {
            // TODO: check here
            if self.mc_state_extra.after_key_block {
                reject_query!("last_key_block remains unchanged in the new masterchain state, but the previous block \
                    is a key block (it should become the new last_key_block)")
            }
        } else if self.next_state_extra.last_key_block.is_none() {
            reject_query!("last_key_block:(Maybe ExtBlkRef) changed in the new state, but it became a nothing$0")
        } else if let Some(ref last_key_block) = self.next_state_extra.last_key_block {
            let block_id = BlockIdExt::from_ext_blk(last_key_block.clone());
            if block_id != self.prev_blocks_ids[0] || last_key_block.end_lt != self.mc_state.state().gen_lt() {
                reject_query!("last_key_block has been set in the new masterchain state to {} with lt {}, \
                    but the only possible value for this update is the previous block {} with lt {}",
                    block_id, last_key_block.end_lt, self.prev_blocks_ids[0], self.mc_state.state().gen_lt())
            }
            if !self.mc_state_extra.after_key_block {
                reject_query!("last_key_block has been updated to the previous block {}, but it is not a key block", block_id)
            }
        }
        if let Some(block_ref) = self.next_state_extra.last_key_block.clone() {
            let key_block_id = Some(block_ref.master_block_id().1);
            if key_block_id != self.prev_key_block {
                reject_query!("new masterchain state declares previous key block to be {:?} \
                    but the value computed from previous masterchain state is {:?}",
                    key_block_id, self.prev_key_block)
            }
        } else if let Some(last_key_block) = &self.prev_key_block {
            reject_query!("new masterchain state declares no previous key block, but the block header \
                announces previous key block seqno {}", last_key_block.seq_no)
        }
        if let Some(new_block_create_stats) = self.next_state_extra.block_create_stats.clone() {
            let old_block_create_stats = self.prev_state_extra.block_create_stats.clone().unwrap_or_default();
            if !self.is_fake && !self.check_block_create_stats(old_block_create_stats, new_block_create_stats)? {
                reject_query!("invalid BlockCreateStats update in the new masterchain state")
            }
        }
        let mut expected_global_balance = self.prev_state_extra.global_balance.clone();
        expected_global_balance.add(&self.value_flow.minted)?;
        expected_global_balance.add(&self.value_flow.created)?;
        expected_global_balance.add(&self.import_created)?;
        if self.next_state_extra.global_balance != expected_global_balance {
            reject_query!("global balance changed in unexpected way: expected old+minted+created+import_created = \
                {} + {} + {} + {} + {}, {}",
                self.prev_state_extra.global_balance, self.value_flow.minted, self.value_flow.created,
                self.import_created, expected_global_balance, self.next_state_extra.global_balance)
        }
        // ...
        Ok(())
    }

    fn check_counter_update(&self, oc: &Counters, nc: &Counters, expected_incr: u64) -> Result<()> {
        let mut cc = oc.clone();
        if nc.is_zero() {
            if expected_incr != 0 {
                reject_query!("new counter total is zero, but the total should have been increased by {}", expected_incr)
            }
            if oc.is_zero() {
                return Ok(())
            }
            cc.increase_by(0, self.now());
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
        if !cc.increase_by(expected_incr, self.now()) {
            reject_query!("old counter value {:?} cannot be increased by {}", oc, expected_incr)
        }
        if !cc.almost_equals(nc) {
            reject_query!("counter {:?} has been increased by {} with an incorrect resulting value {:?}; \
                correct result should be {:?} (up to +/-1 in the last two components)", oc, expected_incr, nc, cc);
        }
        Ok(())
    }

    fn check_one_block_creator_update(&self, key: &UInt256, old: Option<CreatorStats>, new: Option<CreatorStats>) -> Result<bool> {
        log::debug!(target: "validator", "checking update of CreatorStats for {}", key.to_hex_string());
        let (new, new_exists) = match new {
            Some(new) => (new, true),
            None => (CreatorStats::default(), false)
        };
        let old = old.unwrap_or_default();
        let (mc_incr, shard_incr) = if key.is_zero() {
            (!self.created_by().is_zero(), self.block_create_total)
        } else {
            (self.created_by() == key, self.block_create_count.get(key).cloned().unwrap_or(0))
        };
        self.check_counter_update(old.mc_blocks(), new.mc_blocks(), mc_incr as u64)
        .map_err(|err| error!("invalid update of created masterchain blocks counter in CreatorStats for \
            {} : {}", key.to_hex_string(), err))?;
        self.check_counter_update(old.shard_blocks(), new.shard_blocks(), shard_incr)
        .map_err(|err| error!("invalid update of created shardchain blocks counter in CreatorStats for \
            {} : {}", key.to_hex_string(), err))?;
        if new.mc_blocks().is_zero() && new.shard_blocks().is_zero() && new_exists {
            reject_query!("new CreatorStats for {} contains two zero counters \
                (it should have been completely deleted instead)", key.to_hex_string())
        }
        Ok(true)
    }

    // similar to Collator::update_block_creator_stats()
    fn check_block_create_stats(&self, old: BlockCreateStats, new: BlockCreateStats) -> Result<bool> {
        log::debug!(target: "validator", "checking all CreatorStats updates between the old and the new state");
        old.counters.scan_diff(
            &new.counters,
            |key: UInt256, old, new| self.check_one_block_creator_update(&key, old, new)
        ).map_err(|err| error!("invalid BlockCreateStats dictionary in the new state : {}", err))?;
        for (key, _count) in &self.block_create_count {
            let old_val = old.counters.get(key)?;
            let new_val = new.counters.get(key)?;
            if old_val.is_none() != new_val.is_none() || old_val == new_val {
                continue
            }
            if !self.check_one_block_creator_update(key, old_val, new_val)? {
                reject_query!("invalid update of BlockCreator entry for {}", key.to_hex_string())
            }
        }
        let key = UInt256::from([0; 32]);
        let old_val = old.counters.get(&key)?;
        let new_val = new.counters.get(&key)?;
        if new_val.is_none() && (!self.created_by().is_zero() || self.block_create_total != 0) {
            reject_query!("new masterchain state does not contain a BlockCreator entry with zero key with total statistics")
        }
        if !self.check_one_block_creator_update(&key, old_val, new_val)? {
            reject_query!("invalid update of BlockCreator entry for {}", key.to_hex_string())
        }
        Ok(true)
    }

    fn check_one_shard_fee(&self, shard: &ShardIdent, fees: &CurrencyCollection, created: &CurrencyCollection) -> Result<bool> {
        let descr = self.mc_extra.shards().get_shard(shard)?.ok_or_else(|| error!("ShardFees contains a record for shard {} \
            but there is no corresponding record in the new shard configuration", shard))?;
        if descr.descr.reg_mc_seqno != self.block_id().seq_no {
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

    fn check_mc_block_extra(&mut self) -> Result<()> {
        log::debug!(target: "validator", "checking all CreatorStats updates between the old and the new state");
        if !self.shard().is_masterchain() {
            return Ok(())
        }
        // masterchain_block_extra#cca5
        // key_block:(## 1) -> checked in init_parse()
        // shard_hashes:ShardHashes -> checked in compute_next_state() and check_shard_layout()
        // shard_fees:ShardFees
        self.mc_extra.fees().iterate_slices_with_keys_and_aug(|key, mut created, aug| {
            let created = ShardFeeCreated::construct_from(&mut created)?;
            let shard = ShardIdent::with_tagged_prefix(key.workchain_id, key.prefix)?;
            if created != aug || !self.check_one_shard_fee(&shard, &created.fees, &created.create)? {
                reject_query!("ShardFees entry with key {} corresponding to shard {} is invalid",
                    key.to_hex_string(), shard)
            }
            Ok(true)
        })?;
        let fees_imported = self.mc_extra.fees().root_extra().fees.clone();
        if fees_imported != self.value_flow.fees_imported {
            reject_query!("invalid fees_imported in value flow: declared {}, correct value is {}",
                self.value_flow.fees_imported, fees_imported)
        }
        self.import_created = self.mc_extra.fees().root_extra().create.clone();
        // ^[ prev_blk_signatures:(HashmapE 16 CryptoSignaturePair)
        if !self.mc_extra.prev_blk_signatures().is_empty() && self.block_id().seq_no == 1 {
            reject_query!("block contains non-empty signature set for the zero state of the masterchain")
        }
        if self.block_id().seq_no > 1 {
            if !self.mc_extra.prev_blk_signatures().is_empty() {
            // TODO: check signatures here
            } else if !self.is_fake && false {  // FIXME: remove "&& false" when collator serializes signatures
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

    async fn common_preparation(&mut self) -> Result<()> {
        self.start_up().await?;
        // stage 0
        self.compute_next_state()?;
        self.unpack_prev_state()?;
        self.unpack_next_state()?;
        self.config = BlockchainConfig::with_config(self.config_params.clone())?;
        self.load_block_data()?;
        Ok(())
    }

    pub async fn try_validate(mut self) -> Result<()> {
        log::trace!("VALIDATE {}", self.block_candidate.block_id);
        let now = std::time::Instant::now();

        self.common_preparation().await?;

        self.init_output_queue_manager().await?;
        self.check_shard_layout()?;
        check_cur_validator_set(
            &self.validator_set,
            self.block_id(),
            self.shard(),
            &self.mc_state_extra,
            &self.old_mc_shards,
            &self.config_params,
            self.now(),
            self.is_fake
        )?;
        self.check_utime_lt()?;
        // stage 1
        // log::debug!(target: "validator", "running letmated validity checks for block candidate {}", self.block_id());
        // if (!block::gen::t_Block.validate_ref(1000000, block_root_)) {
        //     reject_query!("block ",  id_ + " failed to pass letmated validity checks");
        // }

        self.precheck_value_flow()?;
        self.precheck_account_updates()?;
        self.precheck_account_transactions()?;
        self.precheck_message_queue_update()?;
        self.check_in_msg_descr()?;
        self.check_out_msg_descr()?;
        self.check_processed_upto()?;
        self.check_in_queue()?;
        self.check_delivered_dequeued()?;
        self.check_transactions()?;
        self.check_all_ticktock_processed()?;
        self.check_message_processing_order()?;
        self.check_special_messages()?;
        self.check_new_state()?;
        self.check_mc_block_extra()?;
        self.check_mc_state_extra()?;

        log::info!("VALIDATED {} TIME {}ms", self.block_candidate.block_id, now.elapsed().as_millis());

        Ok(())
    }
}

