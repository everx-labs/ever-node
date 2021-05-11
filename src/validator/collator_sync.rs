#![allow(dead_code, unused_variables)]

use std::{
    cmp::{min, max},
    collections::{HashMap, HashSet, BinaryHeap},
    sync::{atomic::{AtomicU64, Ordering}, Arc},
};
use crate::{
    CHECK,
    engine_traits::EngineOperations,
    out_msg_queue::{MsgQueueManager, OutMsgQueueInfoStuff},
    shard_state::ShardStateStuff,
    types::{
        accounts::ShardAccountStuff,
        limits::BlockLimitStatus,
        messages::MsgEnqueueStuff,
        top_block_descr::{Mode as TbdMode, TopBlockDescrStuff, cmp_shard_block_descr},
    },
    validating_utils::{
        check_this_shard_mc_info, check_cur_validator_set, may_update_shard_block_info,
        update_shard_block_info, update_shard_block_info2, supported_version,
        supported_capabilities,
    },
    rng::random::secure_bytes,
};
use super::{BlockCandidate, CollatorSettings};
use ton_block::{
    AddSub, BlockExtra, BlockIdExt, BlkMasterInfo, BlkPrevInfo, ExtBlkRef, GetRepresentationHash,
    Block, BlockInfo, CurrencyCollection, Grams, HashmapAugType, Libraries,
    MerkleUpdate, UnixTime32, ShardStateUnsplit, ShardFees, ShardAccountBlocks,
    Message, MsgEnvelope, Serializable, ShardAccount, ShardAccounts,
    ShardIdent, Transaction, ValueFlow, InternalMessageHeader, MsgAddressInt,
    McStateExtra, BlockCreateStats, ParamLimitIndex, 
    InMsg, InMsgDescr, ConfigParams, TransactionTickTock, McBlockExtra,
    OutMsg, OutMsgDescr, OutMsgQueueKey, TopBlockDescrSet,
    ValidatorSet, ShardHashes, CommonMsgInfo, Deserializable,
    GlobalCapabilities, McShardRecord, ShardDescr, FutureSplitMerge, Workchains, WorkchainDescr,
    CreatorStats, KeyExtBlkRef, KeyMaxLt, IntermediateAddress, ShardStateSplit, GlobalVersion,
};
use ton_executor::{
    BlockchainConfig, OrdinaryTransactionExecutor, TickTockTransactionExecutor, TransactionExecutor
};
use ton_types::{
    error, fail, Cell, Result, AccountId, HashmapType, UInt256, HashmapE, UsageTree,
};
use futures::try_join;
use rand::Rng;

// TODO move all constants (see validator query too) into one place
pub const SPLIT_MERGE_DELAY: u32 = 100;        // prepare (delay) split/merge for 100 seconds
pub const SPLIT_MERGE_INTERVAL: u32 = 100;     // split/merge is enabled during 60 second interval

struct ImportedData {
    mc_state: ShardStateStuff,
    prev_states: Vec<ShardStateStuff>,
    prev_ext_blocks_refs: Vec<ExtBlkRef>, 
    top_shard_blocks_descr: Vec<Arc<TopBlockDescrStuff>>,
}

struct McData {
    mc_state_extra: McStateExtra,
    prev_key_block_seqno: u32,
    prev_key_block: Option<BlockIdExt>,
    state: ShardStateStuff,

    // TODO put here what you need from masterchain state and block and init in `unpack_last_mc_state`
}

impl McData {
    fn new(mc_state: ShardStateStuff) -> Result<Self> {

        let mc_state_extra = mc_state.state().read_custom()?
            .ok_or_else(|| error!("Can't read custom field from mc state"))?;

        // prev key block
        let (prev_key_block_seqno, prev_key_block) = if mc_state_extra.after_key_block {
            (mc_state.block_id().seq_no(), Some(mc_state.block_id().clone()))
        } else if let Some(block_ref) = mc_state_extra.last_key_block.clone() {
            (block_ref.seq_no, Some(block_ref.master_block_id().1))
        } else {
            (0, None)
        };
        Ok(Self{
            mc_state_extra,
            prev_key_block,
            prev_key_block_seqno,
            state: mc_state
        })
    }

    fn config(&self) -> &ConfigParams { self.mc_state_extra.config() }
    fn mc_state_extra(&self) -> &McStateExtra { &self.mc_state_extra }
    fn prev_key_block_seqno(&self) -> u32 { self.prev_key_block_seqno }
    fn prev_key_block(&self) -> Option<&BlockIdExt> { self.prev_key_block.as_ref() }
    fn state(&self) -> &ShardStateStuff { &self.state }
    fn vert_seq_no(&self) -> u32 { self.state().state().vert_seq_no() }
    fn get_lt_align(&self) -> u64 { 1000000 }
    fn global_balance(&self) -> &CurrencyCollection { &self.mc_state_extra.global_balance }
    fn block_create_stats(&self) -> Option<&BlockCreateStats> { self.mc_state_extra.block_create_stats.as_ref() }
    fn libraries(&self) -> &Libraries { self.state.state().libraries() }
    fn master_ref(&self) -> BlkMasterInfo {
        let end_lt = self.state.state().gen_lt();
        let master = ExtBlkRef {
            end_lt,
            seq_no: self.state.state().seq_no(),
            root_hash: self.state.block_id().root_hash().clone(),
            file_hash: self.state.block_id().file_hash().clone(),
        };
        BlkMasterInfo { master }
    }
}

pub struct PrevData {
    states: Vec<ShardStateStuff>,
    pure_states: Vec<ShardStateStuff>,
    state_root: Cell, // pure cell without used tree my be no need
    accounts: ShardAccounts,
    gen_utime: u32,
    gen_lt: u64,
    total_validator_fees: CurrencyCollection,
    overload_history: u64,
    underload_history: u64,
}

impl PrevData {
    pub fn from_prev_states(
        states: Vec<ShardStateStuff>,
        pure_states: Vec<ShardStateStuff>,
        state_root: Cell,
        subshard: Option<&ShardIdent>,
    ) -> Result<Self> {
        let mut gen_utime = states[0].state().gen_time();
        let mut gen_lt = states[0].state().gen_lt();
        let mut accounts = states[0].state().read_accounts()?;
        let mut total_validator_fees = states[0].state().total_validator_fees().clone();
        let mut overload_history = 0;
        let mut underload_history = 0;
        if let Some(state) = states.get(1) {
            gen_utime = std::cmp::max(gen_utime, state.state().gen_time());
            gen_lt = std::cmp::max(gen_lt, state.state().gen_lt());
            let key = state.shard().merge()?.shard_key(false);
            accounts.merge(&state.state().read_accounts()?, &key)?;
            total_validator_fees.add(state.state().total_validator_fees())?;
        } else if let Some(subshard) = subshard {
            accounts.split_for(&subshard.shard_key(false))?;
            if subshard.is_right_child() {
                total_validator_fees.grams.0 += 1;
            }
            total_validator_fees.grams.0 /= 2;
        } else {
            overload_history = states[0].state().overload_history();
            underload_history = states[0].state().underload_history();
        }
        Ok(Self {
            states,
            pure_states,
            state_root,
            accounts,
            gen_utime,
            gen_lt,
            total_validator_fees,
            overload_history,
            underload_history,
        })
    }

    fn accounts(&self) -> &ShardAccounts { &self.accounts }
    fn overload_history(&self) -> u64 { self.overload_history }
    fn underload_history(&self) -> u64 { self.underload_history }
    fn prev_state_utime(&self) -> u32 { self.gen_utime }
    fn prev_state_lt(&self) -> u64 { self.gen_lt }
    fn shard_libraries(&self) -> &Libraries { self.states[0].state().libraries() }
    fn prev_vert_seqno(&self) -> u32 { self.states[0].state().vert_seq_no() }
    fn total_balance(&self) -> &CurrencyCollection { self.accounts.root_extra().balance() }
    fn total_validator_fees(&self) -> &CurrencyCollection { &self.total_validator_fees }
    fn state(&self) -> &ShardStateStuff { &self.states[0] }
    fn account(&self, account_id: &AccountId) -> Result<Option<ShardAccount>> {
        self.accounts.get_serialized(account_id.clone())
    }
}


#[derive(Eq, PartialEq)]
struct NewMessage {
    lt_hash: (u64, UInt256),
    msg: Message,
    tr_cell: Cell,
}

impl NewMessage {
    fn new(lt_hash: (u64, UInt256), msg: Message, tr_cell: Cell) -> Self {
        Self {
            lt_hash,
            msg,
            tr_cell,
        }
    }
}

impl Ord for NewMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.lt_hash.cmp(&self.lt_hash)
    }
}

impl PartialOrd for NewMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct CollatorData {
    // lists, empty by default
    changed_accounts: HashMap<AccountId, ShardAccountStuff>,
    in_msgs: InMsgDescr,
    out_msgs: OutMsgDescr,
    accounts: ShardAccountBlocks,
    out_msg_queue_info: OutMsgQueueInfoStuff,
    shard_fees: ShardFees,
    shard_top_block_descriptors: Vec<Arc<TopBlockDescrStuff>>,
    block_create_count: HashMap<UInt256, u64>,
    new_messages: BinaryHeap<NewMessage>, // using for priority queue
    usage_tree: UsageTree,

    // determined fields
    gen_utime: u32,
    config: BlockchainConfig,
    collated_block_descr: String,

    // fields, uninitialized by default
    start_lt: Option<u64>,
    max_lt: Option<u64>,
    value_flow: ValueFlow,
    min_ref_mc_seqno: Option<u32>,
    prev_stuff: Option<BlkPrevInfo>,
    shards: Option<ShardHashes>,
    mint_msg: Option<InMsg>,
    recover_create_msg: Option<InMsg>,

    // fields with default values
    skip_topmsgdescr: bool,
    skip_extmsg: bool,
    shard_conf_adjusted: bool,
    block_limit_status: BlockLimitStatus,
    block_create_total: u64,
    inbound_queues_empty: bool,
    last_proc_int_msg: (u64, UInt256),
    shards_max_end_lt: u64,
    before_split: bool,
    now_upper_limit: u32,

    // Split/merge
    want_merge: bool,
    underload_history: u64,
    want_split: bool,
    overload_history: u64,
    block_full: bool,

    // Block metrics (to report statsd)
    dequeue_count: usize,
    enqueue_count: usize,
    transit_count: usize,
    execute_count: usize,
    out_msg_count: usize,
    in_msg_count: usize,
}

impl CollatorData {

    pub fn new(
        gen_utime: u32, 
        config: BlockchainConfig, 
        usage_tree: UsageTree,
        prev_data: &PrevData,
        is_masterchain: bool,
        collated_block_descr: String,
) -> Result<Self> {
        let limits = Arc::new(config.raw_config().block_limits(is_masterchain)?);
        let ret = Self {
            changed_accounts: HashMap::new(),
            in_msgs: InMsgDescr::default(),
            out_msgs: OutMsgDescr::default(),
            accounts: ShardAccountBlocks::default(),
            out_msg_queue_info: OutMsgQueueInfoStuff::default(),
            shard_fees: ShardFees::default(),
            shard_top_block_descriptors: Vec::new(),
            block_create_count: HashMap::new(),
            new_messages: Default::default(),
            usage_tree,
            gen_utime,
            config,
            collated_block_descr,
            start_lt: None,
            max_lt: None,
            value_flow: ValueFlow::default(),
            now_upper_limit: u32::MAX,
            shards_max_end_lt: 0,
            min_ref_mc_seqno: None,
            prev_stuff: None,
            shards: None,
            mint_msg: None,
            recover_create_msg: None,
            skip_topmsgdescr: false,
            skip_extmsg: false,
            shard_conf_adjusted: false,
            block_limit_status: BlockLimitStatus::with_limits(limits),
            block_create_total: 0,
            inbound_queues_empty: false,
            last_proc_int_msg: (0, UInt256::default()),
            want_merge: false,
            underload_history: prev_data.underload_history() << 1,
            want_split: false,
            overload_history: prev_data.overload_history() << 1,
            block_full: false,
            dequeue_count: 0,
            enqueue_count: 0,
            transit_count: 0,
            execute_count: 0,
            out_msg_count: 0,
            in_msg_count: 0,
            before_split: false,
        };
        Ok(ret)
    }

    fn gen_utime(&self) -> u32 { self.gen_utime }

    //
    // Lists
    //

    /// remove account from temp collection
    fn account(&mut self, account_id: &AccountId) -> Option<ShardAccountStuff> {
        self.changed_accounts.remove(account_id)
    }

    /// put account ot temp collection
    fn update_account(&mut self, acc: ShardAccountStuff) {
        self.changed_accounts.insert(acc.account_addr().clone(), acc);
    }

    fn in_msgs_root(&self) -> Result<Cell> {
        self.in_msgs.data().cloned().ok_or_else(|| error!("in msg descr is empty"))
    }

    fn out_msgs_root(&self) -> Result<Cell> {
        self.out_msgs.data().cloned().ok_or_else(|| error!("out msg descr is empty"))
    }

    fn update_last_proc_int_msg(&mut self, account_id: &AccountId, new_lt_hash: (u64, UInt256)) -> Result<()> {
        if self.last_proc_int_msg < new_lt_hash {
            CHECK!(new_lt_hash.0 > 0);
            log::debug!("{}: last_proc_int_msg updated to ({},{:x})",
                self.collated_block_descr, new_lt_hash.0, new_lt_hash.1);
            self.last_proc_int_msg = new_lt_hash;
            Ok(())
        } else {
            log::error!("account: {:x} processed message ({},{:x}) AFTER message ({},{:x})",
                account_id, new_lt_hash.0, new_lt_hash.1,
                self.last_proc_int_msg.0, self.last_proc_int_msg.1);
            self.last_proc_int_msg.0 = std::u64::MAX;
            fail!("internal message processing order violated!")
        }
    }

    fn update_max_lt(&mut self, lt: u64) {
        log::debug!("update_max_lt {}", lt);
        self.max_lt = Some(std::cmp::max(lt, self.max_lt.unwrap_or(0)));
        self.block_limit_status.update_lt(lt);
    }

    /// add in and out messages from to block, and to new message queue
    fn new_transaction(&mut self, transaction: &Transaction, in_msg_opt: Option<&InMsg>) -> Result<()> {
        self.execute_count += 1;
        let gas_used = transaction.gas_used().unwrap_or(0);
        self.block_limit_status.add_gas_used(gas_used as u32);
        self.block_limit_status.add_transaction(transaction.logical_time() == self.start_lt()? + 1);
        if let Some(in_msg) = in_msg_opt {
            self.add_in_msg_to_block(in_msg)?;
        }
        let tr_cell = transaction.serialize()?;
        transaction.out_msgs.iterate_slices(|slice| {
            let msg_cell = slice.reference(0)?;
            let msg_hash = msg_cell.repr_hash();
            let msg = Message::construct_from_cell(msg_cell)?;
            match msg.header() {
                CommonMsgInfo::IntMsgInfo(info) => {
                    self.new_messages.push(NewMessage::new((info.created_lt, msg_hash), msg, tr_cell.clone()));
                }
                CommonMsgInfo::ExtOutMsgInfo(_) => {
                    let out_msg = OutMsg::external(&msg, tr_cell.clone())?;
                    self.add_out_msg_to_block(out_msg.read_message_hash()?, &out_msg)?;
                }
                CommonMsgInfo::ExtInMsgInfo(_) => fail!("External inbound message cannot be output")
            };
            Ok(true)
        })?;
        Ok(())
    }

    /// put InMsg to block
    fn add_in_msg_to_block(&mut self, in_msg: &InMsg) -> Result<()> {
        self.in_msg_count += 1;
        let msg_cell = in_msg.serialize()?;
        self.in_msgs.insert(in_msg)?;
        self.block_limit_status.register_in_msg_op(&msg_cell, &self.in_msgs_root()?)
    }

    /// put OutMsg to block
    fn add_out_msg_to_block(&mut self, key: UInt256, out_msg: &OutMsg) -> Result<()> {
        self.out_msg_count += 1;
        let msg_cell = out_msg.serialize()?;
        self.out_msgs.insert_with_key(key, out_msg)?;
        self.block_limit_status.register_out_msg_op(&msg_cell, &self.out_msgs_root()?)
    }

    /// delete message from state queue
    fn del_out_msg_from_state(&mut self, key: &OutMsgQueueKey) -> Result<()> {
        self.dequeue_count += 1;
        self.out_msg_queue_info.del_message(key)?;
        self.block_limit_status.register_out_msg_queue_op(
            self.out_msg_queue_info.out_queue().data(),
            &self.usage_tree,
            false
        )?;
        self.block_full |= !self.block_limit_status.fits(ParamLimitIndex::Normal);
        Ok(())
    }

    /// add message to state queue
    fn add_out_msg_to_state(&mut self, enq: &MsgEnqueueStuff, force: bool) -> Result<()> {
        self.enqueue_count += 1;
        self.out_msg_queue_info.add_message(enq)?;
        self.block_limit_status.register_out_msg_queue_op(
            self.out_msg_queue_info.out_queue().data(),
            &self.usage_tree,
            force
        )?;
        self.block_full |= !self.block_limit_status.fits(ParamLimitIndex::Normal);
        Ok(())
    }

    fn enqueue_transit_message(
        &mut self,
        shard: &ShardIdent,
        _key: &OutMsgQueueKey,
        enq: &MsgEnqueueStuff,
        requeue: bool,
    ) -> Result<()> {
        self.transit_count += 1;
        log::debug!("{}: enqueue_transit_message {}", self.collated_block_descr, enq);
        let enqueued_lt = self.start_lt()?;
        let (new_enq, transit_fee) = enq.next_hop(shard, enqueued_lt, self.config.raw_config())?;
        let in_msg = InMsg::transit(enq.envelope(), new_enq.envelope(), transit_fee)?;
        let out_msg = OutMsg::transit(new_enq.envelope(), &in_msg, requeue)?;

        self.add_in_msg_to_block(&in_msg)?;
        self.add_out_msg_to_block(enq.message_hash(), &out_msg)?;
        self.add_out_msg_to_state(&new_enq, false)
    }

    pub fn shard_top_block_descriptors(&self) -> &Vec<Arc<TopBlockDescrStuff>> {
        &self.shard_top_block_descriptors 
    }
    pub fn add_top_block_descriptor(&mut self, tbd: Arc<TopBlockDescrStuff>) {
        self.shard_top_block_descriptors.push(tbd)
    }

    pub fn shard_fees(&self) -> &ShardFees { &self.shard_fees }

    pub fn store_shard_fees_zero(&mut self, shard: &ShardIdent) -> Result<()> {
        self.shard_fees.store_shard_fees(shard, CurrencyCollection::with_grams(0),
            CurrencyCollection::with_grams(0))
    }

    pub fn store_shard_fees(&mut self, shard: &McShardRecord) -> Result<()> {
        self.shard_fees.store_shard_fees(
            shard.shard(),
            shard.descr.fees_collected.clone(),
            shard.descr.funds_created.clone()
        )
    }

    pub fn register_shard_block_creators(&mut self, creators: Vec<UInt256>) -> Result<()> {
        for creator in creators {
            let prev_value = *self.block_create_count.get(&creator).unwrap_or(&0);
            self.block_create_count.insert(creator, prev_value + 1);
            self.block_create_total += 1;
        }
        Ok(())
    }
    pub fn block_create_count(&self) -> &HashMap<UInt256, u64> { &self.block_create_count }
    pub fn block_create_total(&self) -> u64 { self.block_create_total }

    fn count_bits_u64(mut x: u64) -> usize {
        x = (x & 0x5555555555555555) + ((x >>  1) & 0x5555555555555555);
        x = (x & 0x3333333333333333) + ((x >>  2) & 0x3333333333333333);
        x = (x & 0x0F0F0F0F0F0F0F0F) + ((x >>  4) & 0x0F0F0F0F0F0F0F0F);
        x = (x & 0x00FF00FF00FF00FF) + ((x >>  8) & 0x00FF00FF00FF00FF);
        x = (x & 0x0000FFFF0000FFFF) + ((x >> 16) & 0x0000FFFF0000FFFF);
        x = (x & 0x00000000FFFFFFFF) + ((x >> 32) & 0x00000000FFFFFFFF);
        x as usize
    }

    fn history_weight(history: u64) -> isize {
        (Self::count_bits_u64(history & 0xffff) * 3
            + Self::count_bits_u64(history & 0xffff0000) * 2
            + Self::count_bits_u64(history & 0xffff00000000)) as isize
            - (3 + 2 + 1) * 16 * 2 / 3
    }

    //
    // fields, uninitialized by default
    //

    fn start_lt(&self) -> Result<u64> { 
        self.start_lt.ok_or_else(|| error!("`start_lt` is not initialized yet"))
    }

    fn set_start_lt(&mut self, lt: u64) -> Result<()> {
        if self.start_lt.is_some() {
            fail!("`start_lt` is already initialized")
        }
        self.block_limit_status.update_lt(lt);
        self.start_lt = Some(lt);
        self.max_lt   = Some(lt);
        Ok(())
    }

    fn max_lt(&self) -> Result<u64> {
        self.max_lt.ok_or_else(|| error!("`max_lt` is not initialized yet"))
    }

    fn prev_stuff(&self) -> Result<&BlkPrevInfo> {
        self.prev_stuff.as_ref().ok_or_else(|| error!("`prev_stuff` is not initialized yet"))
    }

    fn now_upper_limit(&self) -> u32 {
        self.now_upper_limit
    }

    fn set_now_upper_limit(&mut self, val: u32) {
        self.now_upper_limit = val;
    }

    fn shards_max_end_lt(&self) -> u64 {
        self.shards_max_end_lt
    }

    fn update_shards_max_end_lt(&mut self, val: u64) {
        if val > self.shards_max_end_lt {
            self.shards_max_end_lt = val;
        }
    }

    fn update_min_mc_seqno(&mut self, mc_seqno: u32) -> u32 {
        let min_ref_mc_seqno = min(self.min_ref_mc_seqno.unwrap_or(std::u32::MAX), mc_seqno);
        self.min_ref_mc_seqno = Some(min_ref_mc_seqno);
        min_ref_mc_seqno
    }

    fn min_mc_seqno(&self) -> Result<u32> {
        self.min_ref_mc_seqno.ok_or_else(|| error!("`min_ref_mc_seqno` is not initialized yet"))
    }

    fn shards(&self) -> Result<&ShardHashes> {
        self.shards.as_ref().ok_or_else(|| error!("`shards` is not initialized yet"))
    }

    fn shards_mut(&mut self) -> Result<&mut ShardHashes> {
        self.shards.as_mut().ok_or_else(|| error!("`shards` is not initialized yet"))
    }

    fn set_shards(&mut self, shards: ShardHashes) -> Result<()> {
        if self.shards.is_some() {
            fail!("`shards` is already initialized")
        }
        self.shards = Some(shards);
        Ok(())
    }

    //
    // fields with default values
    //

    fn skip_topmsgdescr(&self) -> bool { self.skip_topmsgdescr }
    fn set_skip_topmsgdescr(&mut self) { self.skip_topmsgdescr = true; }

    fn skip_extmsg(&self) -> bool { self.skip_extmsg }
    fn set_skip_extmsg(&mut self) { self.skip_extmsg = true; }

    fn shard_conf_adjusted(&self) -> bool { self.shard_conf_adjusted }
    fn set_shard_conf_adjusted(&mut self) { self.shard_conf_adjusted = true; }

    fn dequeue_message(&mut self, enq: MsgEnqueueStuff, deliver_lt: u64, short: bool) -> Result<()> {
        self.dequeue_count += 1;
        log::debug!("{}: dequeue message: {:x}", self.collated_block_descr, enq.message_hash());
        let out_msg = OutMsg::dequeue(enq.envelope(), deliver_lt, short)?;
        self.add_out_msg_to_block(enq.message_hash(), &out_msg)
    }

    fn want_merge(&self) -> (bool, u64) {
        (self.want_merge, self.underload_history)
    }

    fn want_split(&self) -> (bool, u64) {
        (self.want_split, self.overload_history)
    }

    fn before_split(&self) -> bool { self.before_split }
    fn set_before_split(&mut self, value: bool) { self.before_split = value }
}

pub struct Collator {
    engine: Arc<dyn EngineOperations>,
    shard: ShardIdent,
    min_mc_block_id: BlockIdExt,
    prev_blocks_ids: Vec<BlockIdExt>,
    new_block_id_part: BlockIdExt,
    created_by: UInt256,
    after_merge: bool,
    after_split: bool,
    validator_set: ValidatorSet,

    // string with format like `-1:8000000000000000, 100500`, is used for logging.
    collated_block_descr: String,

    debug: bool,
    rand_seed: Option<UInt256>,
    collator_settings: CollatorSettings,
}

impl Collator {
    pub fn new(
        shard: ShardIdent,
        min_mc_block_id: BlockIdExt,
        prev_blocks_ids: Vec<BlockIdExt>,
        validator_set: ValidatorSet,
        created_by: UInt256,
        engine: Arc<dyn EngineOperations>,
        rand_seed: Option<UInt256>,
        collator_settings: CollatorSettings
    ) -> Result<Self> {

        log::debug!(
            "prev_blocks_ids: {} {}",
            prev_blocks_ids[0],
            if prev_blocks_ids.len() > 1 { format!("{}", prev_blocks_ids[1]) } else { "".to_owned() }
        );

        let new_block_seqno = match prev_blocks_ids.len() {
            1 => prev_blocks_ids[0].seq_no() + 1,
            2 => max(prev_blocks_ids[0].seq_no(), prev_blocks_ids[1].seq_no()) + 1,
            _ => fail!("`prev_blocks_ids` has invlid length"),
        };

        let collated_block_descr = format!("{}:{}, {}", 
            shard.workchain_id(), 
            shard.shard_prefix_as_str_with_tag(), 
            new_block_seqno
        );

        log::trace!("{}: new", collated_block_descr);

        // check inputs

        if !shard.is_base_workchain() && !shard.is_masterchain() {
            fail!("Collator can create block candidates only for masterchain (-1) and base workchain (0)")
        }
        if shard.is_masterchain() && !shard.is_masterchain_ext() {
            fail!("Sub-shards cannot exist in the masterchain")
        }
        if !min_mc_block_id.shard().is_masterchain_ext() {
            fail!("requested minimal masterchain block id does not belong to masterchain")
        }
        let mut after_merge = false;
        let mut after_split = false;
        if prev_blocks_ids.len() == 2 {
            if shard.is_masterchain() {
                fail!("cannot merge shards in masterchain")
            }
            if !(
                prev_blocks_ids.iter().all(|id| shard.is_parent_for(id.shard())) &&
                prev_blocks_ids[0].shard().shard_prefix_with_tag() <
                prev_blocks_ids[1].shard().shard_prefix_with_tag()
            ) {
                fail!("The two previous blocks for a merge operation are not siblings or are not \
                    children of current shard");
            }
            if prev_blocks_ids.iter().any(|id| id.seq_no() == 0) {
                fail!("previous blocks for a block merge operation must have non-zero seqno");
            }
            after_merge = true;
        } else {
            CHECK!(prev_blocks_ids.len(), 1);
            if *prev_blocks_ids[0].shard() != shard {
                if !prev_blocks_ids[0].shard().is_ancestor_for(&shard) {
                    fail!("Previous block does not belong to the shard we are generating a new block for");
                }
                if shard.is_masterchain() {
                    fail!("cannot split shards in masterchain");
                }
                after_split = true;
            }
            if shard.is_masterchain() && min_mc_block_id.seq_no() > prev_blocks_ids[0].seq_no() {
                fail!("cannot refer to specified masterchain block because it is later than \
                    the immediately preceding masterchain block");
            }
        }

        Ok(Self {
            new_block_id_part: BlockIdExt {
                shard_id: shard.clone(),
                seq_no: new_block_seqno,
                root_hash: UInt256::default(),
                file_hash: UInt256::default(),
            },
            engine,
            shard,
            min_mc_block_id,
            prev_blocks_ids,
            created_by,
            after_merge,
            after_split,
            validator_set,
            collated_block_descr,
            debug: true,
            rand_seed,
            collator_settings,
        })
    }

    pub async fn collate(self) -> Result<(BlockCandidate, ShardStateUnsplit)> {
        log::info!(
            "{}: COLLATE min_mc_block_id.seqno = {}, prev_blocks_ids: {} {}",
            self.collated_block_descr,
            self.min_mc_block_id.seq_no(),
            self.prev_blocks_ids[0],
            if self.prev_blocks_ids.len() > 1 { format!("{}", self.prev_blocks_ids[1]) } else { "".to_owned() }
        );
        let now = std::time::Instant::now();

        let imported_data = self.import_data().await?;
        let (mc_data, prev_data, mut collator_data) = self.prepare_data(imported_data).await?;
        let (candidate, state) = self.do_collate(&mc_data, &prev_data, &mut collator_data).await?;

        let duration = now.elapsed().as_millis() as u32;
        let ratio = match duration {
            0 => collator_data.block_limit_status.gas_used(),
            duration => collator_data.block_limit_status.gas_used() / duration
        };
        log::info!(
            "{}: COLLATED SIZE: {} GAS: {} TIME: {}ms RATIO: {}",
            self.collated_block_descr,
            candidate.data.len(),
            collator_data.block_limit_status.gas_used(),
            duration,
            ratio,
        );

        #[cfg(feature = "metrics")]
        crate::validator::collator::report_collation_metrics(
            &self.shard,
            collator_data.dequeue_count,
            collator_data.enqueue_count,
            collator_data.in_msg_count,
            collator_data.out_msg_count,
            collator_data.transit_count,
            collator_data.execute_count,
            collator_data.block_limit_status.gas_used(),
            ratio,
            candidate.data.len(),
            duration,
        );

        #[cfg(not(test))]
        #[cfg(feature = "telemetry")]
        self.engine.collator_telemetry().succeeded_attempt(
            &self.shard,
            now.elapsed(),
            collator_data.execute_count as u32,
            collator_data.block_limit_status.gas_used()
        );

        Ok((candidate, state))
    }

    async fn import_data(&self) -> Result<ImportedData> {
        log::trace!("{}: import_data", self.collated_block_descr);

        if self.shard.is_masterchain() {
            let (prev_states, prev_ext_blocks_refs) =
                self.import_prev_stuff().await?;
            let top_shard_blocks_descr = self.engine.get_shard_blocks(self.prev_blocks_ids[0].seq_no())?;

            Ok(ImportedData{
                mc_state: prev_states[0].clone(),
                prev_states,
                prev_ext_blocks_refs,
                top_shard_blocks_descr 
            })
        } else {
            let (mc_state, (prev_states, prev_ext_blocks_refs)) =
                try_join!(
                    self.import_mc_stuff(),
                    self.import_prev_stuff(),
                )?;

            Ok(ImportedData{
                mc_state,
                prev_states,
                prev_ext_blocks_refs,
                top_shard_blocks_descr: vec!()
            })
        }
    }

    async fn prepare_data(&self, mut imported_data: ImportedData) 
        -> Result<(McData, PrevData, CollatorData)> {
        log::trace!("{}: prepare_data", self.collated_block_descr);

        CHECK!(imported_data.prev_states.len() == 1 + self.after_merge as usize);
        CHECK!(imported_data.prev_states.len() == self.prev_blocks_ids.len());

        CHECK!(imported_data.mc_state.block_id(), inited);

        let mc_data = self.unpack_last_mc_state(imported_data.mc_state)?;
        let state_root = self.unpack_last_state(&mc_data, &imported_data.prev_states)?;
        let pure_states = imported_data.prev_states.clone();
        let usage_tree = self.create_usage_tree(state_root.clone(), &mut imported_data.prev_states)?;

        let subshard = match self.after_split {
            true => Some(&self.shard),
            false => None
        };
        let prev_data = PrevData::from_prev_states(imported_data.prev_states, pure_states, state_root, subshard)?;
        let is_masterchain = self.shard.is_masterchain();

        let now = self.init_utime(&mc_data, &prev_data)?;
        let config = BlockchainConfig::with_config(mc_data.config().clone())?;
        let mut collator_data = CollatorData::new(now, config, usage_tree, &prev_data,
            is_masterchain, self.collated_block_descr.clone())?;

        if !self.shard.is_masterchain() {
            let (now_upper_limit, before_split, _accept_msgs) = check_this_shard_mc_info(
                &self.shard,
                &self.new_block_id_part,
                self.after_merge,
                self.after_split,
                false,
                &self.prev_blocks_ids,
                mc_data.config(),
                mc_data.mc_state_extra(),
                false,
                now,
            )?;
            collator_data.set_now_upper_limit(now_upper_limit);
            collator_data.set_before_split(before_split);
        }

        check_cur_validator_set(
            &self.validator_set,
            &self.new_block_id_part,
            &self.shard,
            mc_data.mc_state_extra(),
            mc_data.mc_state_extra().shards(),
            mc_data.config(),
            now,
            false,
        )?;

        self.check_utime(&mc_data, &prev_data, &mut collator_data)?;

        if is_masterchain {
            self.adjust_shard_config(&mc_data, &mut collator_data)?;
            self.import_new_shard_top_blocks(
                imported_data.top_shard_blocks_descr,
                &prev_data,
                &mc_data,
                &mut collator_data
            )?;
        }

        self.init_lt(&mc_data, &prev_data, &mut collator_data)?;

        // TODO
        self.init_block_limits(&mc_data, &mut collator_data)?;

        collator_data.prev_stuff = Some(BlkPrevInfo::new(imported_data.prev_ext_blocks_refs)?);

        Ok((mc_data, prev_data, collator_data))
    }

    async fn do_collate(
        &self,
        mc_data: &McData,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
    ) -> Result<(BlockCandidate, ShardStateUnsplit)> {
        log::trace!("{}: do_collate", self.collated_block_descr);

        let ext_messages = self.engine.get_external_messages(&self.shard)?;

        let mut output_queue_manager = self.request_neighbor_msg_queues(mc_data, prev_data, collator_data).await?;

        // delete delivered messages from output queue
        // preload output_queue_manager
        // output_queue_manager.prev().out_queue().iterate_with_keys_and_aug(|_key, enq, created_lt| {
        //     MsgEnqueueStuff::from_enqueue_and_lt(enq, created_lt)?;
        //     Ok(true)
        // })?;
        let now = std::time::Instant::now();
        self.clean_out_msg_queue(mc_data, collator_data, &mut output_queue_manager)?;
        log::trace!("{}: TIME: clean_out_msg_queue {}ms;", self.collated_block_descr, now.elapsed().as_millis());

        // copy out msg queue from next state
        collator_data.out_msg_queue_info = output_queue_manager.take_next();

        // compute created / minted / recovered / from_prev_blk
        self.update_value_flow(mc_data, &prev_data, collator_data)?;

        // tick & special transactions
        if self.shard.is_masterchain() {
            self.create_ticktock_transactions(false, mc_data, prev_data, collator_data)?;
            self.create_special_transactions(mc_data, prev_data, collator_data)?;
        }

        // merge prepare / merge install
        // ** will be implemented later **

        // import inbound internal messages, process or transit
        let now = std::time::Instant::now();
        self.process_inbound_internal_messages(prev_data, collator_data, &output_queue_manager)?;
        log::trace!("{}: TIME: process_inbound_internal_messages {}ms;", self.collated_block_descr, now.elapsed().as_millis());

        // import inbound external messages (if space&gas left)
        let now = std::time::Instant::now();
        self.process_inbound_external_messages(prev_data, collator_data, ext_messages, collator_data.max_lt()? + 1)?;
        log::trace!("{}: TIME: process_inbound_external_messages {}ms;", self.collated_block_descr, now.elapsed().as_millis());

        // process newly-generated messages (if space&gas left)
        // (if we were unable to process all inbound messages, all new messages must be queued)
        let now = std::time::Instant::now();
        self.process_new_messages(!collator_data.inbound_queues_empty, prev_data, collator_data, collator_data.max_lt()?)?;
        log::trace!("{}: TIME: process_new_messages {}ms;", self.collated_block_descr, now.elapsed().as_millis());

        // split prepare / split install
        // ** will be implemented later **

        // tock transactions
        if self.shard.is_masterchain() {
            self.create_ticktock_transactions(true, mc_data, prev_data, collator_data)?;
        }

        // process newly-generated messages (only by including them into output queue)
        self.process_new_messages(true, prev_data, collator_data, collator_data.max_lt()? + 1)?;

        // update block history
        self.check_block_overload(collator_data);

        // update processed upto
        self.update_processed_upto(mc_data, collator_data)?;

        //collator_data.block_limit_status.dump_block_size();

        // serialize everything
        self.finalize_block(mc_data, prev_data, collator_data)
    }

    fn clean_out_msg_queue(
        &self,
        mc_data: &McData,
        collator_data: &mut CollatorData,
        output_queue_manager: &mut MsgQueueManager
    ) -> Result<bool> {
        log::trace!("{}: clean_out_msg_queue", self.collated_block_descr);
        let short = mc_data.config().has_capability(GlobalCapabilities::CapShortDequeue);
        output_queue_manager.clean_out_msg_queue(
            |message, root| if let Some((enq, deliver_lt)) = message {
                collator_data.dequeue_message(enq, deliver_lt, short)?;
                collator_data.block_limit_status.register_out_msg_queue_op(root, &collator_data.usage_tree, false)?;
                collator_data.block_full |= !collator_data.block_limit_status.fits(ParamLimitIndex::Normal);
                Ok(collator_data.block_full)
            } else {
                collator_data.block_limit_status.register_out_msg_queue_op(root, &collator_data.usage_tree, true)?;
                collator_data.block_full |= !collator_data.block_limit_status.fits(ParamLimitIndex::Normal);
                Ok(true)
            }
        )
    }

    //
    // import
    //

    async fn import_mc_stuff(&self) -> Result::<ShardStateStuff> {
        log::trace!("{}: import_mc_stuff", self.collated_block_descr);
        let mc_state = self.engine.load_last_applied_mc_state().await?;
        
        if mc_state.block_id().seq_no() < self.min_mc_block_id.seq_no() {
            fail!("requested to create a block referring to a non-existent future masterchain block");
        }
        Ok(mc_state)
    }

    async fn import_prev_stuff(&self) -> Result::<(Vec<ShardStateStuff>, Vec<ExtBlkRef>)> {
        log::trace!("{}: import_prev_stuff", self.collated_block_descr);
        let mut prev_states = vec!();
        let mut prev_ext_blocks_refs = vec![];
        for (i, prev_id) in self.prev_blocks_ids.iter().enumerate() {
            let prev_state = self.engine.load_state(prev_id).await?;
            if &self.shard == prev_state.shard() && prev_state.state().before_split() {
                fail!("cannot generate new unsplit shardchain block for {} \
                    after previous block {} with before_split set", self.shard, prev_id)
            }

            let end_lt = prev_state.state().gen_lt();
            let ext_block_ref = ExtBlkRef {
                end_lt,
                seq_no: prev_id.seq_no,
                root_hash: prev_id.root_hash.clone(),
                file_hash: prev_id.file_hash.clone(),
            };
            prev_ext_blocks_refs.push(ext_block_ref);
            prev_states.push(prev_state);
            if self.shard.is_masterchain() {
                if prev_states[i].block_id().seq_no() < self.min_mc_block_id.seq_no() {
                    fail!("requested to create a block referring to a non-existent future masterchain block");
                }
            }
        }
        Ok((prev_states, prev_ext_blocks_refs))
    }

    //
    // prepare
    //

    fn unpack_last_mc_state(&self, mc_state: ShardStateStuff) -> Result<McData> {
        log::trace!("{}: unpack_last_mc_state", self.collated_block_descr);

        let mc_data = McData::new(mc_state)?;

        // capabilities & global version
        if mc_data.config().has_capabilities() && 
           (0 != (mc_data.config().capabilities() & !supported_capabilities())) {
            fail!(
                "block generation capabilities {:016x} have been enabled in global configuration, \
                 but we support only {:016x} (upgrade validator software?)",
                 mc_data.config().capabilities(),
               supported_capabilities()
            );
        }
        if mc_data.config().global_version() > supported_version() {
            fail!(
                "block version {} have been enabled in global configuration, but we support only {} \
                 (upgrade validator software?)",
                 mc_data.config().global_version(),
                supported_version()
            );
        }

        Ok(mc_data)
    }

    fn unpack_last_state(&self, mc_data: &McData, prev_states: &Vec<ShardStateStuff>) -> Result<Cell> {
        log::trace!("{}: unpack_last_state", self.collated_block_descr);
        for state in prev_states.iter() {
            self.check_one_state(mc_data, state)?;
        }
        let mut state_root = prev_states[0].root_cell().clone();
        if self.after_merge {
            state_root = ShardStateStuff::construct_split_root(state_root, prev_states[1].root_cell().clone())?;
        }
        Ok(state_root)
    }

    fn check_one_state(&self, mc_data: &McData, state: &ShardStateStuff) -> Result<()> {
        log::trace!("{}: check_one_state {}", self.collated_block_descr, state.block_id());

        if state.state().vert_seq_no() > mc_data.vert_seq_no() {
            fail!(
                "cannot create new block with vertical seqno {} prescribed by the current \
                masterchain configuration because the previous state of shard {} \
                has larger vertical seqno {}",
                mc_data.vert_seq_no(),
                state.block_id().shard(),
                state.state().vert_seq_no()
            );
        }
        Ok(())
    }

    // create usage tree and recreate prev states with usage tree
    fn create_usage_tree(&self, state_root: Cell, prev_states: &mut Vec<ShardStateStuff>) -> Result<UsageTree> {
        log::trace!("{}: create_usage_tree", self.collated_block_descr);
        let usage_tree = UsageTree::with_params(state_root, true);
        let root_cell = usage_tree.root_cell();
        *prev_states = if prev_states.len() == 2 {
            let ss_split = ShardStateSplit::construct_from_cell(root_cell.clone())?;
            vec![
                ShardStateStuff::new(prev_states[0].block_id().clone(), ss_split.left)?,
                ShardStateStuff::new(prev_states[1].block_id().clone(), ss_split.right)?,
            ]
        } else {
            let ss = ShardStateUnsplit::construct_from_cell(root_cell.clone())?;
            vec![ShardStateStuff::with_state(prev_states[0].block_id().clone(), ss)?]
        };
        Ok(usage_tree)
    }

    fn init_utime(&self, mc_data: &McData, prev_data: &PrevData) -> Result<u32> {
        log::trace!("{}: init_utime", self.collated_block_descr);

        // consider unixtime and lt from previous block(s) of the same shardchain
        let prev_now = prev_data.prev_state_utime();
        let prev = max(mc_data.state().state().gen_time(), prev_now);
        Ok(max(prev + 1, self.engine.now()))
    }

    fn check_utime(&self, mc_data: &McData, prev_data: &PrevData, collator_data: &mut CollatorData) -> Result<()> {

        let now = collator_data.gen_utime;
        if now > collator_data.now_upper_limit() {
            fail!("error initializing unix time for the new block: \
                failed to observe end of fsm_split time interval for this shard");
        }
        
        // check whether masterchain catchain rotation is overdue
        let prev_now = prev_data.prev_state_utime();
        let ccvc = mc_data.config().catchain_config()?;
        let lifetime = ccvc.mc_catchain_lifetime;
        if self.shard.is_masterchain() &&
           now / lifetime > prev_now / lifetime &&
           now > (prev_now / lifetime + 1) * lifetime + 20 {

            let overdue = now - (prev_now / lifetime + 1) * lifetime;
            let mut rng = rand::thread_rng();
            let skip_topmsgdescr = rng.gen_range(0, 1024) < 256; // probability 1/4
            let skip_extmsg = rng.gen_range(0, 1024) < 256; // skip ext msg probability 1/4
            if skip_topmsgdescr {
                collator_data.set_skip_topmsgdescr();
                log::warn!(
                    "{}: randomly skipping import of new shard data because of overdue masterchain \
                    catchain rotation (overdue by {} seconds)",
                    self.collated_block_descr,
                    overdue
                );
            }
            if skip_extmsg {
                collator_data.set_skip_extmsg();
                log::warn!(
                    "{}: randomly skipping external message import because of overdue masterchain \
                    catchain rotation (overdue by {} seconds)",
                    self.collated_block_descr,
                    overdue
                );
            }
        } else if self.shard.is_masterchain() && now > prev_now + 60 {
            let interval = now - prev_now;
            let mut rng = rand::thread_rng();
            let skip_topmsgdescr = rng.gen_range(0, 1024) < 128; // probability 1/8
            let skip_extmsg = rng.gen_range(0, 1024) < 128; // skip ext msg probability 1/8
            if skip_topmsgdescr {
                collator_data.set_skip_topmsgdescr();
                log::warn!(
                    "{}: randomly skipping import of new shard data because of overdue masterchain \
                    block (last block was {} seconds ago)",
                    self.collated_block_descr,
                    interval
                );
            }
            if skip_extmsg {
                collator_data.set_skip_extmsg();
                log::warn!(
                    "{}: randomly skipping external message import because of overdue masterchain \
                    block (last block was {} seconds ago)",
                    self.collated_block_descr,
                    interval
                );
            }
        }
        Ok(())
    }

    fn init_lt(&self, mc_data: &McData, prev_data: &PrevData, collator_data: &mut CollatorData) 
    -> Result<()> {
        log::trace!("{}: init_lt", self.collated_block_descr);

        let mut start_lt = if !self.shard.is_masterchain() {
            max(mc_data.state().state().gen_lt(), prev_data.prev_state_lt())
        } else {
            max(mc_data.state().state().gen_lt(), collator_data.shards_max_end_lt())
        };

        let align = mc_data.get_lt_align();
        let incr = align - start_lt % align;
        if incr < align || 0 == start_lt {
            if start_lt >= (!incr + 1) {
                fail!("cannot compute start logical time (uint64 overflow)");
            }
            start_lt += incr;
        }

        collator_data.set_start_lt(start_lt)?;
        log::debug!("{}: start_lt set to {}", self.collated_block_descr, start_lt);

        Ok(())
    }

    fn init_block_limits(&self, _mc_data: &McData, collator_data: &mut CollatorData) -> Result<()> {
        log::trace!("{}: init_block_limits", collator_data.collated_block_descr);
        Ok(())
    }

    async fn request_neighbor_msg_queues(&self, mc_data: &McData, prev_data: &PrevData, collator_data: &mut CollatorData) -> Result<MsgQueueManager> {
        log::trace!("{}: request_neighbor_msg_queues", self.collated_block_descr);
        MsgQueueManager::init(
            &self.engine,
            mc_data.state(),
            self.shard.clone(),
            collator_data.shards.as_ref().unwrap_or_else(|| mc_data.mc_state_extra.shards()),
            &prev_data.states,
            None,
            self.after_merge,
            self.after_split,
            None,
        ).await
    }

    fn adjust_shard_config(&self, mc_data: &McData, collator_data: &mut CollatorData) -> Result<()> {
        log::trace!("{}: adjust_shard_config", self.collated_block_descr);
        CHECK!(self.shard.is_masterchain());
        let mut shards = mc_data.state().shards()?.clone();
        let wc_set = mc_data.config().workchains()?;
        wc_set.iterate_with_keys(|wc_id: i32, wc_info| {
            if wc_info.active() && wc_info.enabled_since <= collator_data.gen_utime {
                if !shards.has_workchain(wc_id)? {
                    collator_data.set_shard_conf_adjusted();
                    shards.add_workchain(
                        wc_id,
                        self.new_block_id_part.seq_no(),
                        wc_info.zerostate_root_hash,
                        wc_info.zerostate_file_hash,
                    )?;
                    collator_data.store_shard_fees_zero(&ShardIdent::with_workchain_id(wc_id)?)?;
                }
            }
            Ok(true)
        })?;
        collator_data.set_shards(shards)?;
        Ok(())
    }

    fn import_new_shard_top_blocks(
        &self,
        mut shard_top_blocks: Vec<Arc<TopBlockDescrStuff>>,
        prev_data: &PrevData,
        mc_data: &McData,
        collator_data: &mut CollatorData
    ) -> Result<()> {
        log::trace!("{}: import_new_shard_top_blocks", self.collated_block_descr);

        if collator_data.skip_topmsgdescr() {
            log::warn!("{}: import_new_shard_top_blocks: SKIPPED", self.collated_block_descr);
            return Ok(());
        }

        let lt_limit = prev_data.prev_state_lt() + mc_data.config().get_max_lt_growth();
        shard_top_blocks.sort_by(|a, b| cmp_shard_block_descr(a, b));
        let mut shards_updated = HashSet::new();
        let mut tb_act = 0;
        let mut prev_bd = Option::<Arc<TopBlockDescrStuff>>::None;
        let mut prev_descr = Option::<McShardRecord>::None;
        let mut prev_shard = ShardIdent::default();
        let mut prev_chain_len = 0;
        for sh_bd in shard_top_blocks {
            let mut res_flags = 0;
            let chain_len = match sh_bd.prevalidate(
                mc_data.state().block_id(),
                mc_data.mc_state_extra(),
                mc_data.state().state().vert_seq_no(),
                TbdMode::FAIL_NEW | TbdMode::FAIL_TOO_NEW,
                &mut res_flags
            ) {
                Ok(len) => len as usize,
                Err(e) => {
                    log::debug!("{}: ShardTopBlockDescr for {} skipped: res_flags = {}, error: {}",
                        self.collated_block_descr, sh_bd.proof_for(), res_flags, e);
                    continue;
                }
            };
            if chain_len <= 0 || chain_len > 8 {
                log::debug!("{}: ShardTopBlockDescr for {} skipped: its chain length is {}",
                    self.collated_block_descr, sh_bd.proof_for(), chain_len);
                continue;
            }
            if sh_bd.gen_utime() >= collator_data.gen_utime {
                log::debug!(
                    "{}: ShardTopBlockDescr for {} skipped: it claims to be generated at {} \
                    while it is still {}",
                    self.collated_block_descr,
                    sh_bd.proof_for(),
                    sh_bd.gen_utime(),
                    collator_data.gen_utime()
                );
                continue;
            }
            let mut descr = sh_bd.get_top_descr(chain_len)?;
            CHECK!(descr.block_id() == sh_bd.proof_for());
            let shard = &descr.shard;
            let start_blks = sh_bd.get_prev_at(chain_len);
            match may_update_shard_block_info(collator_data.shards()?, &descr, &start_blks, lt_limit, Some(&mut shards_updated)) {
                Err(e) => {
                    log::debug!("{}: cannot add new top shard block {} to shard configuration: {}",
                        self.collated_block_descr, sh_bd.proof_for(), e);
                    continue;
                }
                Ok((false, _)) => {
                    CHECK!(start_blks.len() == 1);

                    if prev_shard.sibling() == *shard {

                        CHECK!(start_blks.len() == 1);
                        let prev_bd = prev_bd.clone().ok_or_else(|| error!("Can't unwrap `prev_bd`"))?;
                        let start_blks2 = prev_bd.get_prev_at(prev_chain_len);
                        CHECK!(start_blks2.len() == 1);
                        CHECK!(start_blks == start_blks2);
                        let mut prev_descr = prev_descr.clone().ok_or_else(|| error!("Can't unwrap `prev_descr`"))?;

                        prev_descr.descr.reg_mc_seqno = self.new_block_id_part.seq_no;
                        descr.descr.reg_mc_seqno = self.new_block_id_part.seq_no;
                        let end_lt = max(prev_descr.descr.end_lt, descr.descr.end_lt);
                        if let Err(e) = update_shard_block_info2(collator_data.shards_mut()?,
                                            prev_descr.clone(), descr.clone(), &start_blks2, Some(&mut shards_updated)) {
                            log::debug!(
                                "{}: cannot add new split top shard blocks {} and {} to shard configuration: {}",
                                self.collated_block_descr,
                                sh_bd.proof_for(),
                                prev_bd.proof_for(),
                                e
                            );
                            //prev_descr.clear();
                            //descr.clear();

                            // t-node doesn't contain next line, but I think it needs here.
                            prev_shard = ShardIdent::default();
                        } else {
                            log::debug!("{}: updated top shard block information with {} and {}",
                                self.collated_block_descr, sh_bd.proof_for(), prev_bd.proof_for());
                            collator_data.store_shard_fees(&prev_descr)?;
                            collator_data.store_shard_fees(&descr)?;
                            collator_data.register_shard_block_creators(prev_bd.get_creator_list(prev_chain_len)?)?;
                            collator_data.register_shard_block_creators(sh_bd.get_creator_list(chain_len)?)?;
                            collator_data.add_top_block_descriptor(prev_bd.clone());
                            collator_data.add_top_block_descriptor(sh_bd.clone());
                            tb_act += 2;
                            //prev_bd.clear();
                            //prev_descr.clear();
                            prev_shard = ShardIdent::default();
                            collator_data.update_shards_max_end_lt(end_lt);
                        }
                    } else if *shard == prev_shard {
                        log::debug!("{}: skip postponing new top shard block {}",
                        self.collated_block_descr, sh_bd.proof_for());
                    } else {
                        log::debug!("{}: postpone adding new top shard block {}",
                            self.collated_block_descr, sh_bd.proof_for());
                        prev_bd = Some(sh_bd);
                        prev_descr = Some(descr.clone());
                        prev_shard = shard.clone();
                        prev_chain_len = chain_len;
                    }
                }
                Ok((true, _)) => {
                    if prev_bd.is_some() {
                        prev_bd = None;
                        prev_descr = None;
                        prev_shard = ShardIdent::default();
                    }
        
                    descr.descr.reg_mc_seqno = self.new_block_id_part.seq_no;
                    let end_lt = descr.descr.end_lt;
                    if let Err(e) = update_shard_block_info(collator_data.shards_mut()?,
                                        descr.clone(), &start_blks, Some(&mut shards_updated)) {
                        log::debug!("{}: cannot add new top shard block {} to shard configuration: {}",
                            self.collated_block_descr, sh_bd.proof_for(), e);
                        //descr.clear();
                    } else {
                        collator_data.store_shard_fees(&descr)?;
                        collator_data.register_shard_block_creators(sh_bd.get_creator_list(chain_len)?)?;
                        collator_data.update_shards_max_end_lt(end_lt);
                        log::debug!("{}: updated top shard block information with {}",
                            self.collated_block_descr, sh_bd.proof_for());
                        tb_act += 1;
                        collator_data.add_top_block_descriptor(sh_bd.clone());
                    }
                }
            }
        }

        if tb_act > 0 {
            collator_data.set_shard_conf_adjusted();
        
            // LOG(INFO) << "updated shard block configuration to ";
            // auto csr = shard_conf_->get_root_csr();
            // block::gen::t_ShardHashes.print(std::cerr, csr.write());
        }

        // block::gen::ShardFeeCreated::Record fc;
        // if (!(tlb::csr_unpack(fees_import_dict_->get_root_extra(),
        //                         fc)  // _ fees:CurrencyCollection create:CurrencyCollection = ShardFeeCreated;
        //         && value_flow_.fees_imported.validate_unpack(fc.fees) && import_created_.validate_unpack(fc.create))) {
        //     return fatal_error("cannot read the total imported fees from the augmentation of the root of ShardFees");
        // }

        // log::debug!(
        //     "total fees_imported = {}; out of them, total fees_created = {}", 
        //     value_flow_.fees_imported,
        //     import_created
        // );

        let shard_fees = collator_data.shard_fees().root_extra().clone();

        collator_data.value_flow.fees_collected.add(&shard_fees.fees)?;
        collator_data.value_flow.fees_imported = shard_fees.fees;

        Ok(())
    }

    //
    // collate
    //
    fn update_value_flow(
        &self,
        mc_data: &McData,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
    ) -> Result<()> {
        log::trace!("{}: update_value_flow", self.collated_block_descr);

        if self.shard.is_masterchain() {
            collator_data.value_flow.created.grams = mc_data.config().block_create_fees(true)?;
            
            collator_data.value_flow.recovered = collator_data.value_flow.created.clone();
            collator_data.value_flow.recovered.add(&collator_data.value_flow.fees_collected)?;
            collator_data.value_flow.recovered.add(mc_data.state().state().total_validator_fees())?;

            match mc_data.config().fee_collector_address() {
                Err(_) => {
                    log::debug!("{}: fee recovery disabled (no collector smart contract defined in configuration)",
                        self.collated_block_descr);
                    collator_data.value_flow.recovered = CurrencyCollection::default();
                }
                Ok(_addr) => {
                    if collator_data.value_flow.recovered.grams < Grams::from(1_000_000_000) {
                        log::debug!("{}: fee recovery skipped ({})", 
                            self.collated_block_descr, collator_data.value_flow.recovered);
                        collator_data.value_flow.recovered = CurrencyCollection::default();
                    }
                }
            };

            collator_data.value_flow.minted = self.compute_minted_amount(mc_data)?;

            if !collator_data.value_flow.minted.is_zero()? && mc_data.config().minter_address().is_err() {
                log::warn!("{}: minting of {} disabled: no minting smart contract defined",
                    self.collated_block_descr, collator_data.value_flow.minted);
                collator_data.value_flow.minted = CurrencyCollection::default();
            }
        } else {
            collator_data.value_flow.created.grams = mc_data.config().block_create_fees(false)?;
            collator_data.value_flow.created.grams.0 >>= self.shard.prefix_len();
        }
        collator_data.value_flow.from_prev_blk = prev_data.total_balance().clone();
        Ok(())
    }

    fn compute_minted_amount(&self, mc_data: &McData) -> Result<CurrencyCollection> {
        log::trace!("{}: compute_minted_amount", self.collated_block_descr);
        
        CHECK!(self.shard.is_masterchain());
        let mut to_mint = CurrencyCollection::default();

        let to_mint_cp = match mc_data.config().to_mint() {
            Err(e) => {
                log::warn!("{}: Can't get config param 7 (to_mint): {}", self.collated_block_descr, e);
                return Ok(to_mint)
            },
            Ok(v) => v
        };

        let old_global_balance = &mc_data.mc_state_extra().global_balance;
        to_mint_cp.iterate_with_keys(|key: u32, amount| {
            let amount2 = old_global_balance.get_other(key)?.unwrap_or_default();
            if amount > amount2 {
                let mut delta = amount.clone();
                delta.sub(&amount2)?;
                log::debug!("{}: currency #{}: existing {}, required {}, to be minted {}",
                    self.collated_block_descr, key, amount2, amount, delta);
                if key != 0 {
                    to_mint.set_other_ex(key, &delta)?;
                }
            }
            Ok(true)
        })?;

        Ok(to_mint)
    }

    fn create_ticktock_transactions(
        &self,
        tock: bool,
        mc_data: &McData,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
    ) -> Result<()> {
        log::trace!("{}: create_ticktock_transactions", self.collated_block_descr);
        let config_account_id = AccountId::from(mc_data.config().config_addr.clone());
        let fundamental_dict = mc_data.config().fundamental_smc_addr()?;
        let req_lt = collator_data.max_lt()? + 1;
        for res in &fundamental_dict {
            let account_id = res?.0.into();
            self.create_ticktock_transaction(account_id, tock, prev_data, collator_data, req_lt)?;
        }
        self.create_ticktock_transaction(config_account_id, tock, prev_data, collator_data, req_lt)?;
        Ok(())
    }

    fn create_ticktock_transaction(
        &self,
        account_id: AccountId,
        tock: bool,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
        req_lt: u64,
    ) -> Result<()> {
        log::trace!(
            "{}: create_ticktock_transaction({}) acc: {:x}",
            self.collated_block_descr,
            if tock { "tock" } else { "tick" },
            account_id
        );
        CHECK!(self.shard.is_masterchain());

        // TODO: get account from collator data
        let account = prev_data
            .account(&account_id)?
            .ok_or_else(|| error!("Can't find account {}", account_id))?
            .read_account()?;
        let tick_tock = account.get_tick_tock().cloned().unwrap_or_default();

        if (tick_tock.tock && tock) || (tick_tock.tick && !tock) {
            let config = collator_data.config.clone();
            let tt = if tock {TransactionTickTock::Tock} else {TransactionTickTock::Tick};
            let executor = TickTockTransactionExecutor::new(config, tt);
            let transaction = self.execute(&executor, account_id, None, prev_data, collator_data, req_lt)?;

            collator_data.new_transaction(&transaction, None)?;
        }

        Ok(())
    }

    fn create_special_transactions(
        &self,
        mc_data: &McData,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
    ) -> Result<()> {
        if !self.shard.is_masterchain() {
            return Ok(())
        }
        log::trace!("{}: create_special_transactions", self.collated_block_descr);

        let config = collator_data.config.clone();
        let executor = OrdinaryTransactionExecutor::new(config);
        let account_id = AccountId::from(mc_data.config().fee_collector_address()?.write_to_new_cell()?);
        collator_data.recover_create_msg = self.create_special_transaction(
            account_id,
            collator_data.value_flow.recovered.clone(),
            &executor,
            prev_data,
            collator_data,
        )?;

        let account_id = AccountId::from(mc_data.config().minter_address()?.write_to_new_cell()?);
        collator_data.mint_msg = self.create_special_transaction(
            account_id,
            collator_data.value_flow.minted.clone(),
            &executor,
            prev_data,
            collator_data,
        )?;

        Ok(())
    }

    fn create_special_transaction(
        &self,
        account_id: AccountId,
        amount: CurrencyCollection,
        executor: &dyn TransactionExecutor,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
    ) -> Result<Option<InMsg>> {
        log::trace!(
            "{}: create_special_transaction: recover {} to account {:x}",
            self.collated_block_descr,
            amount.grams,
            account_id
        );
        if amount.is_zero()? || !self.shard.is_masterchain() {
            return Ok(None)
        }
        let mut hdr = InternalMessageHeader::with_addresses(
            MsgAddressInt::with_standart(None, -1, [0; 32].into())?,
            MsgAddressInt::with_standart(None, -1, account_id.clone())?,
            amount
        );
        hdr.ihr_disabled = true;
        hdr.bounce = true;
        hdr.created_lt = collator_data.start_lt()?;
        hdr.created_at = UnixTime32(collator_data.gen_utime);
        let msg = Message::with_int_header(hdr);
        let transaction = self.execute(executor, account_id, Some(&msg), prev_data, collator_data, 0)?;

        let mut env = MsgEnvelope::with_message_and_fee(&msg, Grams::default())?;
        env.set_cur_addr(IntermediateAddress::full_dest());
        env.set_next_addr(IntermediateAddress::full_dest());
        let in_msg = InMsg::immediatelly(&env, &transaction, Grams::default())?;
        collator_data.new_transaction(&transaction, Some(&in_msg))?;
        Ok(Some(in_msg))
    }

    fn process_inbound_internal_messages(
        &self,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
        output_queue_manager: &MsgQueueManager,
    ) -> Result<()> {
        log::trace!("{}: process_inbound_internal_messages", self.collated_block_descr);

        let config = collator_data.config.clone();
        let executor = OrdinaryTransactionExecutor::new(config);

        let mut iter = output_queue_manager.merge_out_queue_iter(&self.shard)?;
        while let Some(k_v) = iter.next() {
            if collator_data.block_full {
                log::trace!("{}: BLOCK FULL, stop processing internal messages", self.collated_block_descr);
                break
            }
            let (key, enq, created_lt, block_id) = k_v?;
            let account_id = enq.dst_account_id()?;
            collator_data.update_last_proc_int_msg(&account_id, (created_lt, enq.message_hash()))?;
            if collator_data.out_msg_queue_info.already_processed(&enq)? {
                log::debug!("{}: inbound internal message with lt={} hash={:x} enqueued_lt={} \
                    has been already processed by us before, skipping",
                    self.collated_block_descr, created_lt, key.hash, enq.enqueued_lt());
            } else {
                self.check_inbound_internal_message(&key, &enq, created_lt, block_id.shard())
                    .map_err(|err| error!("problem processing internal inbound message \
                        with hash {:x} : {}", key.hash, err))?;
                let our = self.shard.contains_full_prefix(&enq.cur_prefix());
                let to_us = self.shard.contains_full_prefix(&enq.dst_prefix());
                if to_us {
                    let msg_opt = Some(enq.message());
                    let fwd_fee = enq.fwd_fee_remaining();
                    let req_lt = collator_data.max_lt()?;
                    let transaction = self.execute(&executor, account_id, msg_opt, prev_data, collator_data, req_lt)?;

                    let in_msg = InMsg::finally(enq.envelope(), &transaction, fwd_fee.clone())?;
                    collator_data.new_transaction(&transaction, Some(&in_msg))?;
                    if our {
                        let out_msg = OutMsg::dequeue_immediately(enq.envelope(), &in_msg)?;
                        collator_data.add_out_msg_to_block(enq.message_hash(), &out_msg)?;
                        collator_data.del_out_msg_from_state(&key)?;
                    }
                    collator_data.block_full |= !collator_data.block_limit_status.fits(ParamLimitIndex::Normal);
                } else {
                    // println!("{:x} {:#}", key, enq);
                    // println!("cur: {}, dst: {}", enq.cur_prefix(), enq.dst_prefix());
                    collator_data.enqueue_transit_message(&self.shard, &key, &enq, our)?;
                    if our {
                        collator_data.del_out_msg_from_state(&key)?;
                    }
                }
            }
        }
        // all internal messages are processed
        collator_data.inbound_queues_empty = iter.next().is_none();
        Ok(())
    }

    fn check_inbound_internal_message(
        &self,
        key: &OutMsgQueueKey,
        enq: &MsgEnqueueStuff,
        created_lt: u64,
        nb_shard: &ShardIdent,
    ) -> Result<()> {
        let header = enq.message().int_header()
            .ok_or_else(|| error!("message is not internal"))?;
        if created_lt != header.created_lt {
            fail!("inbound internal message has an augmentation value in source OutMsgQueue \
                distinct from the one in its contents")
        }
        if enq.fwd_fee_remaining() > header.fwd_fee() {
            fail!("inbound internal message has fwd_fee_remaining={} larger than original fwd_fee={}",
                enq.fwd_fee_remaining(), header.fwd_fee())
        }
        if !nb_shard.contains_full_prefix(&enq.cur_prefix()) {
            fail!("inbound internal message does not have current address in the originating neighbor shard")
        }
        if !self.shard.contains_full_prefix(&enq.next_prefix()) {
            fail!("inbound internal message does not have next hop address in our shard")
        }
        if key.workchain_id != enq.next_prefix().workchain_id {
            fail!("inbound internal message has invalid key in OutMsgQueue \
                : its first 96 bits differ from next_hop_addr")
        }
        Ok(())
    }

    fn process_inbound_external_messages(
        &self,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
        mut ext_messages: Vec<(Arc<Message>, UInt256)>,
        req_lt: u64,
    ) -> Result<()> {
        if collator_data.skip_extmsg {
            log::trace!("{}: skipping processing of inbound external messages", self.collated_block_descr);
            return Ok(())
        }
        log::trace!("{}: process_inbound_external_messages", self.collated_block_descr);

        let config = collator_data.config.clone();
        let executor = OrdinaryTransactionExecutor::new(config);

        let mut complete = vec![];
        let mut to_delay = vec![];
        for (msg, id) in ext_messages.drain(..) {
            let header = msg.ext_in_header().ok_or_else(|| error!("message {:x} \
                is not external inbound message", id))?;
            if self.shard.contains_address(&header.dst)? {
                if !collator_data.block_limit_status.fits(ParamLimitIndex::Soft) {
                    log::trace!("{}: BLOCK FULL, stop processing external messages", self.collated_block_descr);
                    break
                }
                let (_, account_id) = header.dst.extract_std_address(true)?;
                let msg_opt = Some(msg.as_ref());
                let transaction = match self.execute(&executor, account_id.clone(), msg_opt, prev_data, collator_data, req_lt) {
                    Err(err) => {

                        // TODO compare this case with t-node

                        log::warn!("{}: account {:x} rejected inbound external message {:x} by reason: {}", 
                            self.collated_block_descr, account_id, id, err);
                        continue
                    }
                    Ok(transaction) => transaction
                };

                let in_msg = InMsg::external(&msg, &transaction)?;
                collator_data.new_transaction(&transaction, Some(&in_msg))?;
                complete.push(id);
            } else {
                to_delay.push(id);
            }
        }
        self.engine.complete_external_messages(to_delay, complete)
        }

    fn process_new_messages(
        &self,
        mut enqueue_only: bool,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
        req_lt: u64,
    ) -> Result<()> {
        log::trace!("{}: process_new_messages", self.collated_block_descr);
        let config = collator_data.config.clone();
        let executor = OrdinaryTransactionExecutor::new(config);
        while let Some(NewMessage{ lt_hash: _, msg, tr_cell }) = collator_data.new_messages.pop() {
            let info = msg.int_header().ok_or_else(|| error!("message is not internal"))?;
            let fwd_fee = info.fwd_fee().clone();
            enqueue_only |= collator_data.block_full;
            let out_msg = if !self.shard.contains_address(&info.dst)? {
                let env = MsgEnvelope::hypercube_routing(&msg, &self.shard, fwd_fee.clone())?;
                let enq = MsgEnqueueStuff::new(&msg, &self.shard)?;
                collator_data.add_out_msg_to_state(&enq, true)?;
                OutMsg::new(&env, tr_cell)?
            } else if enqueue_only {
                let env = MsgEnvelope::with_message_and_fee(&msg, fwd_fee.clone())?;
                let enq = MsgEnqueueStuff::new(&msg, &self.shard)?;
                collator_data.add_out_msg_to_state(&enq, true)?;
                OutMsg::new(&env, tr_cell)?
            } else {
                let env = MsgEnvelope::with_message_and_fee(&msg, fwd_fee.clone())?;
                let hash = env.message_cell().repr_hash();
                CHECK!(info.created_at.0, collator_data.gen_utime);
                let account_id = msg.int_dst_account_id().unwrap_or_default();
                collator_data.update_last_proc_int_msg(&account_id, (info.created_lt, hash))?;
                let req_lt = std::cmp::max(info.created_lt + 1, req_lt);
                let new_transaction = self.execute(&executor, account_id, Some(&msg), prev_data, collator_data, req_lt)?;
                let env = MsgEnvelope::with_message_and_fee(&msg, fwd_fee.clone())?;
                let in_msg = InMsg::immediatelly(&env, &new_transaction, fwd_fee.clone())?;
                collator_data.new_transaction(&new_transaction, Some(&in_msg))?;
                OutMsg::immediately(&env, tr_cell, &in_msg)?
            };
            collator_data.add_out_msg_to_block(out_msg.read_message_hash()?, &out_msg)?;
            collator_data.block_full |= !collator_data.block_limit_status.fits(ParamLimitIndex::Normal);
        }
        Ok(())
    }

    fn execute(
        &self,
        executor: &dyn TransactionExecutor,
        account_id: AccountId,
        msg_opt: Option<&Message>,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
        req_lt: u64,
    ) -> Result<Transaction> {
        log::trace!(
            "{}: execute acc: {:x}, msg: {}",
            self.collated_block_descr,
            account_id,
            if let Some(ref msg) = msg_opt { msg.hash()?.to_hex_string() } else { "none".to_string() }
        );

        let mut shard_acc = match collator_data.account(&account_id) {
            Some(shard_acc) => shard_acc,
            None => ShardAccountStuff::from_shard_state(
                account_id,
                &prev_data.accounts,
                Arc::new(AtomicU64::new(collator_data.start_lt()? + 1)),
            )?
        };
        let mut account_root = shard_acc.account_cell().clone();
        
        shard_acc.lt().fetch_max(req_lt, Ordering::Relaxed);
        let lt = shard_acc.lt().clone();

        let now = std::time::Instant::now();
        let (mut result, account_root) = {
            let lt = lt.clone();
            let gen_utime = collator_data.gen_utime;
            let block_lt = collator_data.start_lt()?;
            let debug = self.debug;
            (
                executor.execute(
                    msg_opt,
                    &mut account_root,
                    gen_utime,
                    block_lt,
                    lt,
                    debug
                ),
                account_root
            )
        };
        if let Ok(mut transaction) = result.as_mut() {
            let gas = transaction.gas_used().unwrap_or(0);
            log::trace!("{}: GAS: {} TIME: {}ms execute for {}", 
                self.collated_block_descr, gas, now.elapsed().as_millis(), transaction.logical_time());

            shard_acc.add_transaction(&mut transaction, account_root)?;
            // LT of last out message or transaction itself
            collator_data.update_max_lt(lt.load(Ordering::Relaxed) - 1);
        }
        collator_data.update_account(shard_acc);
        result
    }

    fn update_processed_upto(&self, mc_data: &McData, collator_data: &mut CollatorData) -> Result<()> {
        log::trace!("{}: update_processed_upto", self.collated_block_descr);
        let ref_mc_seqno = match self.shard.is_masterchain() {
            true => self.new_block_id_part.seq_no,
            false => mc_data.state().block_id().seq_no
        };
        collator_data.update_min_mc_seqno(ref_mc_seqno);
        let lt = collator_data.last_proc_int_msg.0;
        if lt != 0 {
            let hash = collator_data.last_proc_int_msg.1.clone();
            collator_data.out_msg_queue_info.add_processed_upto(ref_mc_seqno, lt, hash)?;
            collator_data.out_msg_queue_info.compactify()?;
        // TODO: need to think about this later, maybe config->lt is 0 always...
        } else if collator_data.inbound_queues_empty {
            if let Some(lt) = mc_data.state().state().gen_lt().checked_sub(1) {
                collator_data.out_msg_queue_info.add_processed_upto(ref_mc_seqno, lt, UInt256::MAX)?;
                collator_data.out_msg_queue_info.compactify()?;
            }
        }
        Ok(())
    }

    fn check_block_overload(&self, collator_data: &mut CollatorData) {
        let class = collator_data.block_limit_status.classify();
        if class <= ParamLimitIndex::Underload {
            collator_data.underload_history |= 1;
            log::info!("{}: Block is underloaded", self.collated_block_descr);
        } else if class >= ParamLimitIndex::Soft {
            collator_data.overload_history |= 1;
            log::info!("{}: Block is overloaded (category {:?})", self.collated_block_descr, class);
        } else {
            log::info!("{}: Block is loaded normally", self.collated_block_descr);
        }

        if let Some(true) = self.collator_settings.want_split {
            log::info!("{}: want_split manually set", self.collated_block_descr);
            collator_data.want_split = true;
            return
        } else if let Some(true) = self.collator_settings.want_merge {
            log::info!("{}: want_merge manually set", self.collated_block_descr);
            collator_data.want_merge = true;
            return
        }

        if CollatorData::history_weight(collator_data.overload_history) >= 0 {
            log::info!("{}: want_split set because of overload history 0x{:X}",
                self.collated_block_descr, collator_data.overload_history);
            collator_data.want_split = true;
        } else if CollatorData::history_weight(collator_data.underload_history) >= 0 {
            log::info!("{}: want_merge set because of underload history 0x{:X}",
                self.collated_block_descr, collator_data.underload_history);
            collator_data.want_merge = true;
        }
    }

    //
    // finalize
    //
    fn finalize_block(
        &self, 
        mc_data: &McData, 
        prev_data: &PrevData, 
        collator_data: &mut CollatorData,
    ) -> Result<(BlockCandidate, ShardStateUnsplit)> {
        log::trace!("{}: finalize_block", self.collated_block_descr);

        let (want_split, overload_history)  = collator_data.want_split();
        let (want_merge, underload_history) = collator_data.want_merge();

        // update shard accounts tree and prepare accounts blocks
        let mut new_accounts = prev_data.accounts.clone();
        let mut accounts = ShardAccountBlocks::default();
        for (_account_id, shard_acc) in collator_data.changed_accounts.iter() {
            let acc_block = shard_acc.update_shard_state(&mut new_accounts)?;
            if !acc_block.transactions().is_empty() {
                accounts.insert(&acc_block)?;
            }
        }

        log::trace!("{}: finalize_block: calc value flow", self.collated_block_descr);
        // calc value flow
        let mut value_flow = collator_data.value_flow.clone();
        value_flow.imported = collator_data.in_msgs.root_extra().value_imported.clone();
        value_flow.exported = collator_data.out_msgs.root_extra().clone();
        value_flow.fees_collected = accounts.root_extra().clone();
        value_flow.fees_collected.grams.add(&collator_data.in_msgs.root_extra().fees_collected)?;

        // value_flow.fees_collected.grams.add(&out_msg_dscr.root_extra().grams)?; // TODO: Why only grams?

        value_flow.fees_collected.add(&value_flow.fees_imported)?;
        value_flow.fees_collected.add(&value_flow.created)?;
        value_flow.to_next_blk = new_accounts.full_balance().clone();
        //value_flow.to_next_blk.add(&value_flow.recovered)?;

        // println!("{}", &value_flow);

        let (out_msg_queue_info, min_ref_mc_seqno) = collator_data.out_msg_queue_info.serialize()?;
        collator_data.update_min_mc_seqno(min_ref_mc_seqno);
        let (mc_state_extra, master_ref) = if self.shard.is_masterchain() {
            let (extra, min_seqno) = self.create_mc_state_extra(prev_data, collator_data)?;
            collator_data.update_min_mc_seqno(min_seqno);
            (Some(extra), None)
        } else {
            (None, Some(mc_data.master_ref()))
        };
        let gen_validator_list_hash_short = ValidatorSet::calc_subset_hash_short(
                                self.validator_set.list(), self.validator_set.catchain_seqno())?;

        log::trace!("{}: finalize_block: fill block info", self.collated_block_descr);
        // calc block info
        let mut info = BlockInfo::default();
        info.set_version(0);
        info.set_before_split(collator_data.before_split());
        info.set_want_merge(want_merge);
        info.set_want_split(want_split);
        info.set_after_split(self.after_split);
        info.set_prev_stuff(self.after_merge, collator_data.prev_stuff()?)?;
        info.set_shard(self.shard.clone());
        info.set_seq_no(self.new_block_id_part.seq_no)?;
        info.set_start_lt(collator_data.start_lt()?);
        info.set_end_lt(collator_data.max_lt()? + 1);
        info.set_gen_utime(UnixTime32::from(collator_data.gen_utime));
        info.set_gen_validator_list_hash_short(gen_validator_list_hash_short);
        info.set_gen_catchain_seqno(self.validator_set.catchain_seqno());
        info.set_min_ref_mc_seqno(collator_data.min_mc_seqno()?);
        info.set_prev_key_block_seqno(mc_data.prev_key_block_seqno);
        info.write_master_ref(master_ref.as_ref())?;

        if mc_data.config().has_capability(GlobalCapabilities::CapReportVersion) {
            info.set_gen_software(Some(GlobalVersion {
                version: supported_version(),
                capabilities: supported_capabilities(),
            }));
        }

        log::trace!("{}: finalize_block: calc new state", self.collated_block_descr);
        // Calc new state, then state update

        let mut new_state = ShardStateUnsplit::with_ident(self.shard.clone());
        new_state.set_global_id(prev_data.state().state().global_id());
        new_state.set_seq_no(self.new_block_id_part.seq_no);
        new_state.set_gen_time(collator_data.gen_utime);
        new_state.set_gen_lt(info.end_lt());
        new_state.set_before_split(info.before_split());
        new_state.set_overload_history(overload_history);
        new_state.set_underload_history(underload_history);
        new_state.set_min_ref_mc_seqno(collator_data.min_mc_seqno()?);
        new_state.write_accounts(&new_accounts)?;
        new_state.write_out_msg_queue_info(&out_msg_queue_info)?;
        new_state.set_master_ref(master_ref);
        new_state.set_total_balance(new_accounts.root_extra().balance().clone());
        let mut total_validator_fees = prev_data.total_validator_fees().clone();
        // total_validator_fees.add(&value_flow.created)?;
        // total_validator_fees.add(&accounts.root_extra())?;
        total_validator_fees.add(&value_flow.fees_collected)?;
        total_validator_fees.sub(&value_flow.recovered)?;
        new_state.set_total_validator_fees(total_validator_fees);
        *new_state.libraries_mut() = self.update_public_libraries(
            prev_data.states[0].state().libraries().clone(),
            &collator_data.changed_accounts
        )?;
        new_state.write_custom(mc_state_extra.as_ref())?;

        log::trace!("{}: finalize_block: calc merkle update", self.collated_block_descr);
        let visited = std::mem::take(&mut collator_data.usage_tree).visited();
        let new_ss_root = new_state.serialize()?;

        // let mut visited_from_root = HashSet::new();
        // Self::_check_visited_integrity(&prev_data.state_root, &visited, &mut visited_from_root);
        // assert_eq!(visited.len(), visited_from_root.len());

        let now = std::time::Instant::now();
        let state_update = MerkleUpdate::create_fast(
            &prev_data.state_root,
            &new_ss_root,
            |h| visited.contains(h)
        )?;
        log::trace!("{}: TIME: merkle update creating {}ms;",
            self.collated_block_descr, now.elapsed().as_millis());

        // let new_root2 = state_update.apply_for(&prev_data.state_root)?;
        // assert_eq!(new_root2.repr_hash(), new_ss_root.repr_hash());

        // calc block extra
        let mut extra = BlockExtra::default();
        extra.write_in_msg_descr(&collator_data.in_msgs)?;
        extra.write_out_msg_descr(&collator_data.out_msgs)?;
        extra.write_account_blocks(&accounts)?;

        // mc block extra
        if let Some(mc_state_extra) = mc_state_extra {
            let mut mc_block_extra = McBlockExtra::default();
            *mc_block_extra.hashes_mut() = collator_data.shards()?.clone();
            *mc_block_extra.fees_mut() = collator_data.shard_fees.clone();
            mc_block_extra.write_recover_create_msg(collator_data.recover_create_msg.as_ref())?;
            mc_block_extra.write_mint_msg(collator_data.mint_msg.as_ref())?;
            if mc_state_extra.after_key_block {
                info.set_key_block(true);
                *mc_block_extra.config_mut() = Some(mc_state_extra.config().clone());
            }
            extra.write_custom(Some(&mc_block_extra))?;
        }
        extra.rand_seed = match self.rand_seed.as_ref() {
            None => {
                let mut key: Vec<u8> = Vec::new(); 
                secure_bytes(&mut key, 32);
                key.into()
            }
            Some(rand_seed) => rand_seed.clone()
        };
        extra.created_by = self.created_by.clone();

        // construct block
        let new_block = Block::with_params(
            mc_data.state().state().global_id(),
            info,
            value_flow,
            state_update,
            extra,
        )?;
        let mut block_id = self.new_block_id_part.clone();

        log::trace!("{}: finalize_block: fill block candidate", self.collated_block_descr);
        let cell = new_block.serialize()?;
        block_id.root_hash = cell.repr_hash();
        let data = ton_types::serialize_toc(&cell)?;
        block_id.file_hash = UInt256::calc_file_hash(&data);

        let collated_data = if !collator_data.shard_top_block_descriptors.is_empty() {
            let mut tbds = TopBlockDescrSet::default();
            for stbd in &collator_data.shard_top_block_descriptors {
                tbds.insert(stbd.proof_for().shard(), stbd.top_block_descr())?;
            }
            tbds.write_to_bytes()?
        } else {
            vec!()
        };

        let candidate = BlockCandidate {
            block_id,
            data,
            collated_data,
            collated_file_hash: UInt256::default(),
            created_by: self.created_by.clone(),
        };
        log::trace!(
            "{} dequeue_count: {}, enqueue_count: {}, in_msg_count: {}, out_msg_count: {},\
            execute_count: {}, transit_count: {}",
            self.collated_block_descr, collator_data.dequeue_count, collator_data.enqueue_count,
            collator_data.in_msg_count, collator_data.out_msg_count, collator_data.execute_count,
            collator_data.transit_count
        );
        Ok((candidate, new_state))
    }

    fn _check_visited_integrity(cell: &Cell, visited: &HashSet<UInt256>, visited_from_root: &mut HashSet<UInt256>) {
        if visited.contains(&cell.repr_hash()) {
            visited_from_root.insert(cell.repr_hash());
            for r in cell.clone_references() {
                Self::_check_visited_integrity(&r, visited, visited_from_root);
            }
        }
    }

    fn update_public_libraries(
        &self,
        mut libraries: Libraries,
        accounts: &HashMap<AccountId, ShardAccountStuff>
    ) -> Result<Libraries> {
        log::trace!("{}: update_public_libraries", self.collated_block_descr);
        for (_, acc) in accounts.iter() {
            acc.update_public_libraries(&mut libraries)?;
        }
        Ok(libraries)
    }

    fn create_mc_state_extra(
        &self,
        prev_data: &PrevData,
        collator_data: &mut CollatorData
    ) -> Result<(McStateExtra, u32)> {
        log::trace!("{}: build_mc_state_extra", self.collated_block_descr);
        CHECK!(!self.after_merge);
        CHECK!(self.new_block_id_part.shard_id.is_masterchain());

        // 1. update config:ConfigParams
        let state_extra = prev_data.state().shard_state_extra()?;
        let config_addr = state_extra.config().config_address()?;
        let old_config = state_extra.config();
        let (config, is_key_block) = if let Some(config_smc) = collator_data.account(&config_addr.into()) {
            let new_config_root = config_smc
                .shard_account()
                .read_account()?
                .get_data()
                .ok_or_else(|| error!("Can't extract config's contract data"))?
                .reference(0)?;
            let mut new_config = ConfigParams::new();
            new_config.config_params = HashmapE::with_hashmap(32, Some(new_config_root));
            new_config.config_addr = new_config.config_address()?;
            if !new_config.valid_config_data(true, None)? {
                fail!("configuration smart contract {} contains an invalid configuration in its data",
                    new_config.config_addr);
            }
            let is_key_block =
                new_config.important_config_parameters_changed(state_extra.config(), false)?;
            (new_config, is_key_block)
        } else {
            (old_config.clone(), false)
        };

        let now = collator_data.gen_utime();
        let prev_now = prev_data.prev_state_utime();

        // 2. update shard_hashes and shard_fees
        let ccvc = config.catchain_config()?;
        let workchains = config.workchains()?;
        let lifetimes = now / ccvc.shard_catchain_lifetime;
        let prev_lifetimes = prev_now / ccvc.shard_catchain_lifetime;
        let update_shard_cc = is_key_block || (lifetimes > prev_lifetimes);
        let min_ref_mc_seqno = self.update_shard_config(collator_data, &workchains, update_shard_cc)?;

        // 3. save new shard_hashes
        // just take collator_data.shards()

        // 4. check extension flags
        // tate_extra.flags is checked in the McStateExtra::read_from 

        // 5. update validator_info
        let mut validator_info = state_extra.validator_info.clone();
        let cur_validators = config.validator_set()?;
        let lifetime = ccvc.mc_catchain_lifetime;
        let mut cc_updated = false;
        if is_key_block || (now / lifetime > prev_now / lifetime) {
            validator_info.catchain_seqno += 1;
            cc_updated = true;
            log::debug!("{}: increased masterchain catchain seqno to {}",
                self.collated_block_descr, validator_info.catchain_seqno);
        }
        let (validators, _hash_short) = cur_validators.calc_subset(
            &ccvc, 
            self.shard.shard_prefix_with_tag(), 
            self.shard.workchain_id(), 
            validator_info.catchain_seqno,
            UnixTime32(now)
        )?;
        // t-node calculates subset with valid catchain_seqno and then subset_hash_short with zero one...
        let hash_short = ValidatorSet::calc_subset_hash_short(&validators, 0)?; 

        validator_info.nx_cc_updated = cc_updated & update_shard_cc;
        validator_info.validator_list_hash_short = hash_short;

        // 6. update prev_blocks (add prev block's id to the dictionary)
        let key = self.new_block_id_part.seq_no == 1 || // prev block is a zerostate, not sure it is correct TODO
                  state_extra.after_key_block;
        let mut prev_blocks = state_extra.prev_blocks.clone();
        let prev_blk_ref = ExtBlkRef {
            end_lt: prev_data.prev_state_lt(),
            seq_no: prev_data.state().block_id().seq_no,
            root_hash: prev_data.state().block_id().root_hash.clone(),
            file_hash: prev_data.state().block_id().file_hash.clone(),
        };

        prev_blocks.set(
            &self.prev_blocks_ids[0].seq_no,
            &KeyExtBlkRef {
                key,
                blk_ref: prev_blk_ref.clone()
            },
            &KeyMaxLt {
                key,
                max_end_lt: prev_data.prev_state_lt()
            }
        )?;

        // 7. update after_key_block:Bool and last_key_block:(Maybe ExtBlkRef)
        let last_key_block = if state_extra.after_key_block {
            Some(prev_blk_ref)
        } else {
            state_extra.last_key_block.clone()
        };

        // 8. update global balance
        let mut global_balance = state_extra.global_balance.clone();
        global_balance.add(&collator_data.value_flow.created)?;
        global_balance.add(&collator_data.value_flow.minted)?;
        global_balance.add(&collator_data.shard_fees().root_extra().create)?;

        // 9. update block creator stats
        let block_create_stats =
            if state_extra.config().has_capability(GlobalCapabilities::CapCreateStatsEnabled) {
                let mut stat = state_extra.block_create_stats.clone().unwrap_or_default();
                self.update_block_creator_stats(collator_data, &mut stat)?;
                Some(stat)
            } else {
                None
            };

        // 10. pack new McStateExtra
        Ok((
            McStateExtra {
                shards: collator_data.shards()?.clone(),
                config,
                validator_info,
                prev_blocks,
                after_key_block: is_key_block,
                last_key_block,
                block_create_stats, 
                global_balance
            }, 
            min_ref_mc_seqno
        ))
    }

    fn update_shard_config(
        &self,
        collator_data: &mut CollatorData,
        wc_set: &Workchains,
        update_cc: bool
    ) -> Result<u32> {
        log::trace!("{}: update_shard_config, (update_cc: {})", self.collated_block_descr, update_cc);

        let now = collator_data.gen_utime();
        let mut min_ref_mc_seqno = u32::max_value();
        
        
        // TODO iterate_shards_with_siblings_mut when it will be done
       
        // temp code, delete after iterate_shards_with_siblings_mut
        let mut changed_shards = HashMap::new();
        collator_data.shards()?.iterate_shards_with_siblings(|shard, descr, sibling| {
            min_ref_mc_seqno = min(min_ref_mc_seqno, descr.min_ref_mc_seqno);

            if let Some(sibling) = sibling.as_ref() {
                min_ref_mc_seqno = min(min_ref_mc_seqno, sibling.min_ref_mc_seqno);

                let shard = shard.sibling();
                if let Some(new_info) = self.update_one_shard(
                    &shard,
                    sibling.clone(),
                    Some(&descr),
                    wc_set.get(&shard.workchain_id())?,
                    now,
                    update_cc
                )? {
                    changed_shards.insert(shard, new_info);
                }
            }

            if let Some(new_info) = self.update_one_shard(
                &shard,
                descr,
                sibling.as_ref(),
                wc_set.get(&shard.workchain_id())?,
                now,
                update_cc
            )? {
                changed_shards.insert(shard, new_info);
            }
            Ok(true)
        })?;
        for (shard, info) in changed_shards {
            collator_data.shards_mut()?.update_shard(&shard, |_| Ok(info))?;
        }
        // end of the temp code

        Ok(min_ref_mc_seqno)
    }

    fn update_one_shard(
        &self,
        shard: &ShardIdent,
        mut info: ShardDescr,
        sibling: Option<&ShardDescr>,
        wc_info: Option<WorkchainDescr>,
        now: u32,
        mut update_cc: bool
    ) -> Result<Option<ShardDescr>> {
        log::trace!("{}: update_one_shard {}", self.collated_block_descr, shard);

        let mut changed = false;
        let old_before_merge = info.before_merge;
        info.before_merge = false;

        if !info.is_fsm_none() && (now >= info.fsm_utime_end() || info.before_split) {
            info.split_merge_at = FutureSplitMerge::None;
            changed = true;
        } else if info.is_fsm_merge() && (sibling.is_none() || sibling.as_ref().unwrap().before_split) {
            info.split_merge_at = FutureSplitMerge::None;
            changed = true;
        }

        if !info.before_split {
            if let Some(wc_info) = wc_info {
                // workchain present in configuration?
                let depth = shard.prefix_len();
                if info.is_fsm_none() && (info.want_split || depth < wc_info.min_split()) &&
                   depth < wc_info.max_split() && depth < 60 {
                    // prepare split
                    info.split_merge_at = FutureSplitMerge::Split {
                        split_utime: now + SPLIT_MERGE_DELAY,
                        interval: SPLIT_MERGE_INTERVAL,
                    };
                    changed = true;
                    log::debug!("{}: preparing to split shard {} during {}..{}",
                        self.collated_block_descr, shard, info.fsm_utime(), info.fsm_utime_end());

                } else {
                    if let Some(sibling) = sibling {
                        if info.is_fsm_none() &&
                           depth > wc_info.min_split() &&
                          (info.want_merge || depth > wc_info.max_split()) &&
                          !sibling.before_split && sibling.is_fsm_none() &&
                          (sibling.want_merge || depth > wc_info.max_split()) {
                            // prepare merge
                            info.split_merge_at = FutureSplitMerge::Merge {
                                merge_utime: now + SPLIT_MERGE_DELAY,
                                interval: SPLIT_MERGE_INTERVAL,
                            };
                            changed = true;
                            log::debug!("{}: preparing to merge shard {} with {} during {}..{}", 
                                self.collated_block_descr, shard,
                                shard.sibling(), info.fsm_utime(), info.fsm_utime_end());

                        } else if info.is_fsm_merge() &&
                                  depth > wc_info.min_split() &&
                                 !sibling.before_split &&
                                  sibling.is_fsm_merge() &&
                                  now >= info.fsm_utime() && now >= sibling.fsm_utime() &&
                                 (depth > wc_info.max_split() || (info.want_merge && sibling.want_merge)) {
                            // force merge
                            info.before_merge = true;
                            changed = true;
                            log::debug!("{}: force immediate merging of shard {} with {}",
                                self.collated_block_descr, shard, shard.sibling());
                        }
                    }
                }
            }
        }

        if info.before_merge != old_before_merge {
            update_cc |= old_before_merge;
            changed = true;
        }
        if update_cc {
            info.next_catchain_seqno += 1;
            changed = true;
        }

        if changed {
            log::trace!("{}: update_one_shard {} changed {:?}", self.collated_block_descr, shard, info);
        }

        Ok(if changed {Some(info)} else {None})
    }

    fn update_block_creator_stats(
        &self,
        collator_data: &CollatorData,
        block_create_stats: &mut BlockCreateStats,
    ) -> Result<()> {
        log::trace!("{}: update_block_creator_stats", self.collated_block_descr);

        for (creator, count) in collator_data.block_create_count().iter() {
            self.update_block_creator_count(
                block_create_stats,
                collator_data.gen_utime(),
                creator,
                *count,
                0
            )?;
        }

        let has_creator = self.created_by != UInt256::default();
        if has_creator {
            self.update_block_creator_count(
                block_create_stats,
                collator_data.gen_utime(),
                &self.created_by,
                0,
                1
            )?;
        }
        if has_creator || collator_data.block_create_total() > 0 {
            self.update_block_creator_count(
                block_create_stats,
                collator_data.gen_utime(),
                &UInt256::default(),
                collator_data.block_create_total(), 
                if has_creator {1} else {0}
            )?;
        }

        let mut rng = rand::thread_rng();
        let key: [u8; 32] = rng.gen();
        let mut key: UInt256 = key.into();
        let mut scanned = 0;
        let mut removed = 0;
        while scanned < 100 {
            let stat = block_create_stats.counters.find_leaf(&key, false, false, false)?;
            if let Some((key1, mut stat)) = stat {
                let res = self.creator_count_outdated(
                    &key1,
                    collator_data.gen_utime(),
                    &mut stat
                )?;
                if !res {
                    log::trace!("{}: prunning CreatorStats for {:x}", self.collated_block_descr, key);
                    block_create_stats.counters.remove(&key)?;
                    removed += 1;
                } 
                scanned += 1;
                key = key1;
            } else {
                break;
            }
        }
        log::trace!("{}: removed {} stale CreatorStats entries out of {} scanned",
            self.collated_block_descr, removed, scanned);
        Ok(())
    }

    fn update_block_creator_count(
        &self,
        stats: &mut BlockCreateStats,
        now: u32,
        key: &UInt256,
        shard_incr: u64,
        mc_incr: u64
    ) -> Result<()> {
        log::trace!("{}: update_block_creator_count, key {:x}, shard_incr {}, mc_incr {}",
            self.collated_block_descr, key, shard_incr, mc_incr);

        let mut stat = stats.counters.get(key)?.unwrap_or_default();
        if mc_incr > 0 {
            if !stat.mc_blocks.increase_by(mc_incr, now) {
                fail!(
                    "cannot increase masterchain block counter in CreatorStats for {:x} by {} \
                    (old value is {:?})",
                    key,
                    mc_incr,
                    stat.mc_blocks
                );
            }
        }
        if shard_incr > 0 {
            if !stat.shard_blocks.increase_by(shard_incr, now) {
                fail!(
                    "cannot increase shardchain block counter in CreatorStats for {:x} by {} \
                    (old value is {:?})",
                    key,
                    shard_incr,
                    stat.shard_blocks
                );
            }
        };
        stats.counters.set(key, &stat)?;
        Ok(())
    }

    fn creator_count_outdated(
        &self,
        key: &UInt256,
        now: u32,
        stat: &mut CreatorStats
    ) -> Result<bool> {
        log::trace!("{}: creator_count_outdated, key {:x}", self.collated_block_descr, key);

        if !(stat.mc_blocks.increase_by(0, now) && stat.shard_blocks.increase_by(0, now)) {
          fail!("cannot amortize counters in CreatorStats for {:x}", key);
        }
        if 0 == (stat.mc_blocks.cnt65536() | stat.shard_blocks.cnt65536()) {
          log::trace!("{}: removing stale CreatorStats for {:x}", self.collated_block_descr, key);
          Ok(false)
        } else {
          Ok(true)
        }
    }
}
