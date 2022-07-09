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

#![allow(dead_code, unused_variables)]

use super::{validator_utils::calc_subset_for_workchain, BlockCandidate, CollatorSettings, McData};
use crate::{
    engine_traits::EngineOperations,
    ext_messages::EXT_MESSAGES_TRACE_TARGET,
    rng::random::secure_256_bits,
    shard_state::ShardStateStuff,
    types::{
        accounts::ShardAccountStuff,
        limits::BlockLimitStatus,
        messages::{MsgEnqueueStuff, MsgEnvelopeStuff},
        top_block_descr::{cmp_shard_block_descr, Mode as TbdMode, TopBlockDescrStuff},
    },
    validating_utils::{
        check_cur_validator_set, check_this_shard_mc_info, may_update_shard_block_info,
        supported_capabilities, supported_version, update_shard_block_info,
        update_shard_block_info2,
    },
    validator::out_msg_queue::{MsgQueueManager, OutMsgQueueInfoStuff},
    CHECK,
};
use adnl::common::Wait;
use futures::try_join;
use rand::Rng;
use std::{
    cmp::{max, min},
    collections::{BinaryHeap, HashMap, HashSet},
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use ton_block::{
    AddSub, BlkPrevInfo, Block, BlockCreateStats, BlockExtra, BlockIdExt, BlockInfo, CommonMsgInfo,
    ConfigParams, CopyleftRewards, CreatorStats, CurrencyCollection, Deserializable, ExtBlkRef,
    FutureSplitMerge, GlobalCapabilities, GlobalVersion, Grams, HashmapAugType, InMsg, InMsgDescr,
    InternalMessageHeader, KeyExtBlkRef, KeyMaxLt, Libraries, McBlockExtra, McShardRecord,
    McStateExtra, MerkleUpdate, Message, MsgAddressInt, OutMsg, OutMsgDescr, OutMsgQueueKey,
    ParamLimitIndex, Serializable, ShardAccount, ShardAccountBlocks, ShardAccounts, ShardDescr,
    ShardFees, ShardHashes, ShardIdent, ShardStateSplit, ShardStateUnsplit, TopBlockDescrSet,
    Transaction, TransactionTickTock, UnixTime32, ValidatorSet, ValueFlow, WorkchainDescr,
    Workchains,
};
use ton_executor::{
    BlockchainConfig, ExecuteParams, OrdinaryTransactionExecutor, TickTockTransactionExecutor,
    TransactionExecutor,
};
use ton_types::{error, fail, AccountId, Cell, HashmapType, Result, UInt256, UsageTree};

#[cfg(feature = "metrics")]
use crate::engine::STATSD;

// TODO move all constants (see validator query too) into one place
pub const SPLIT_MERGE_DELAY: u32 = 100;        // prepare (delay) split/merge for 100 seconds
pub const SPLIT_MERGE_INTERVAL: u32 = 100;     // split/merge is enabled during 60 second interval

pub const DEFAULT_COLLATE_TIMEOUT: u32 = 2000;

const MAX_COLLATE_THREADS: usize = 10;

struct ImportedData {
    mc_state: Arc<ShardStateStuff>,
    prev_states: Vec<Arc<ShardStateStuff>>,
    prev_ext_blocks_refs: Vec<ExtBlkRef>, 
    top_shard_blocks_descr: Vec<Arc<TopBlockDescrStuff>>,
}

pub struct PrevData {
    states: Vec<Arc<ShardStateStuff>>,
    pure_states: Vec<Arc<ShardStateStuff>>,
    state_root: Cell, // pure cell without used tree my be no need
    accounts: ShardAccounts,
    gen_utime: u32,
    gen_lt: u64,
    total_validator_fees: CurrencyCollection,
    overload_history: u64,
    underload_history: u64,
    state_copyleft_rewards: CopyleftRewards,
}

impl PrevData {
    pub fn from_prev_states(
        states: Vec<Arc<ShardStateStuff>>,
        pure_states: Vec<Arc<ShardStateStuff>>,
        state_root: Cell,
        subshard: Option<&ShardIdent>,
    ) -> Result<Self> {
        let mut gen_utime = states[0].state().gen_time();
        let mut gen_lt = states[0].state().gen_lt();
        let mut accounts = states[0].state().read_accounts()?;
        let mut total_validator_fees = states[0].state().total_validator_fees().clone();
        let state_copyleft_rewards = if states[0].shard().is_masterchain() {
            let state_copyleft_rewards = states[0].state().copyleft_rewards()?;
            log::trace!("Masterchain copyleft reward count: {}", state_copyleft_rewards.len()?);
            state_copyleft_rewards.clone()
        } else {
            CopyleftRewards::default()
        };
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
                total_validator_fees.grams += 1;
            }
            total_validator_fees.grams /= 2;
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
            state_copyleft_rewards,
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

#[derive(Debug)]
enum AsyncMessage {
    Recover(Message),
    Mint(Message),
    Copyleft(Message),
    Ext(Message),
    Int(MsgEnqueueStuff, bool),
    New(MsgEnvelopeStuff, Cell), // prev_trans_cell
    TickTock(TransactionTickTock),
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
    in_msgs: InMsgDescr,
    out_msgs: OutMsgDescr,
    accounts: ShardAccountBlocks,
    out_msg_queue_info: OutMsgQueueInfoStuff,
    shard_fees: ShardFees,
    shard_top_block_descriptors: Vec<Arc<TopBlockDescrStuff>>,
    block_create_count: HashMap<UInt256, u64>,
    new_messages: BinaryHeap<NewMessage>, // using for priority queue
    to_delay: Vec<UInt256>, // list of not compleeted external messages
    usage_tree: UsageTree,

    // determined fields
    gen_utime: u32,
    config: BlockchainConfig,

    // fields, uninitialized by default
    start_lt: Option<u64>,
    value_flow: ValueFlow,
    min_ref_mc_seqno: Option<u32>,
    prev_stuff: Option<BlkPrevInfo>,
    shards: Option<ShardHashes>,
    mint_msg: Option<InMsg>,
    recover_create_msg: Option<InMsg>,
    copyleft_msgs: Vec<InMsg>,

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
    ) -> Result<Self> {
        let limits = Arc::new(config.raw_config().block_limits(is_masterchain)?);
        let ret = Self {
            in_msgs: InMsgDescr::default(),
            out_msgs: OutMsgDescr::default(),
            accounts: ShardAccountBlocks::default(),
            out_msg_queue_info: OutMsgQueueInfoStuff::default(),
            shard_fees: ShardFees::default(),
            shard_top_block_descriptors: Vec::new(),
            block_create_count: HashMap::new(),
            new_messages: Default::default(),
            to_delay: Default::default(),
            usage_tree,
            gen_utime,
            config,
            start_lt: None,
            value_flow: ValueFlow::default(),
            now_upper_limit: u32::MAX,
            shards_max_end_lt: 0,
            min_ref_mc_seqno: None,
            prev_stuff: None,
            shards: None,
            mint_msg: None,
            recover_create_msg: None,
            copyleft_msgs: Default::default(),
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

    fn in_msgs_root(&self) -> Result<Cell> {
        self.in_msgs.data().cloned().ok_or_else(|| error!("in msg descr is empty"))
    }

    fn out_msgs_root(&self) -> Result<Cell> {
        self.out_msgs.data().cloned().ok_or_else(|| error!("out msg descr is empty"))
    }

    fn update_last_proc_int_msg(&mut self, new_lt_hash: (u64, UInt256)) -> Result<()> {
        if self.last_proc_int_msg < new_lt_hash {
            CHECK!(new_lt_hash.0 > 0);
            log::debug!("last_proc_int_msg updated to ({},{:x})", new_lt_hash.0, new_lt_hash.1);
            self.last_proc_int_msg = new_lt_hash;
        } else {
            log::error!("processed message ({},{:x}) AFTER message ({},{:x})", new_lt_hash.0, new_lt_hash.1,
                self.last_proc_int_msg.0, self.last_proc_int_msg.1);
            self.last_proc_int_msg.0 = std::u64::MAX;
            fail!("internal message processing order violated!")
        }
        Ok(())
    }

    fn update_lt(&mut self, lt: u64) {
        self.block_limit_status.update_lt(lt);
    }


    /// add in and out messages from to block, and to new message queue
    fn new_transaction(&mut self, transaction: &Transaction, tr_cell: Cell, in_msg_opt: Option<&InMsg>) -> Result<()> {
        self.execute_count += 1;
        let gas_used = transaction.gas_used().unwrap_or(0);
        self.block_limit_status.add_gas_used(gas_used as u32);
        self.block_limit_status.add_transaction(transaction.logical_time() == self.start_lt()? + 1);
        if let Some(in_msg) = in_msg_opt {
            self.add_in_msg_to_block(in_msg)?;
        }
        transaction.out_msgs.iterate_slices(|slice| {
            let msg_cell = slice.reference(0)?;
            let msg_hash = msg_cell.repr_hash();
            let msg = Message::construct_from_cell(msg_cell.clone())?;
            match msg.header() {
                CommonMsgInfo::IntMsgInfo(info) => {
                    self.new_messages.push(NewMessage::new((info.created_lt, msg_hash), msg, tr_cell.clone()));
                }
                CommonMsgInfo::ExtOutMsgInfo(_) => {
                    let out_msg = OutMsg::external(msg_cell, tr_cell.clone());
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
        let enqueued_lt = self.start_lt()?;
        let (new_enq, transit_fee) = enq.next_hop(shard, enqueued_lt, &self.config)?;
        let in_msg = InMsg::transit(enq.envelope_cell(), new_enq.envelope_cell(), transit_fee);
        let out_msg = OutMsg::transit(new_enq.envelope_cell(), in_msg.serialize()?, requeue);

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

    pub fn store_workchain_copyleft_rewards(&mut self, shard: &McShardRecord) -> Result<()> {
        self.value_flow.copyleft_rewards.merge_rewards(&shard.descr.copyleft_rewards)
    }

    pub fn get_workchains_copyleft_rewards(&self) -> &CopyleftRewards {
        &self.value_flow.copyleft_rewards
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
        Ok(())
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
        let out_msg = match short {
            true => OutMsg::dequeue_short(enq.envelope_hash(), enq.next_prefix(), deliver_lt),
            false => OutMsg::dequeue_long(enq.envelope_cell(), deliver_lt)
        };
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

    fn withdraw_ext_msg_to_delay(&mut self) -> Vec<UInt256> {
        std::mem::take(&mut self.to_delay)
    }
}

struct ExecutionManager {
    changed_accounts: HashMap<
        AccountId, 
        (
            tokio::sync::mpsc::UnboundedSender<Arc<AsyncMessage>>,
            tokio::task::JoinHandle<Result<ShardAccountStuff>>
        )
    >,
    
    receive_tr: tokio::sync::mpsc::UnboundedReceiver<Option<(Arc<AsyncMessage>, Result<Transaction>)>>,
    wait_tr: Arc<Wait<(Arc<AsyncMessage>, Result<Transaction>)>>,
    max_collate_threads: usize,
    libraries: Libraries,
    gen_utime: u32,

    // bloc's start logical time
    start_lt: u64,
    // actual maximum logical time
    max_lt: Arc<AtomicU64>,
    // this time is used if account's lt is smaller
    min_lt: Arc<AtomicU64>,
    // block random seed
    seed_block: UInt256,

    total_trans_duration: Arc<AtomicU64>,
    collated_block_descr: Arc<String>,
    debug: bool,
    config: BlockchainConfig,
}

impl ExecutionManager {
    pub fn new(
        gen_utime: u32,
        start_lt: u64,
        seed_block: UInt256,
        libraries: Libraries,
        config: BlockchainConfig,
        max_collate_threads: usize,
        collated_block_descr: Arc<String>,
        debug: bool,
    ) -> Result<Self> {
        log::trace!("{}: ExecutionManager::new", collated_block_descr);
        let (wait_tr, receive_tr) = Wait::new();
        Ok(Self {
            changed_accounts: HashMap::new(),
            receive_tr,
            wait_tr,
            max_collate_threads,
            libraries,
            config,
            start_lt,
            gen_utime,
            seed_block,
            max_lt: Arc::new(AtomicU64::new(start_lt + 1)),
            min_lt: Arc::new(AtomicU64::new(start_lt + 1)),
            total_trans_duration: Arc::new(AtomicU64::new(0)),
            collated_block_descr,
            debug,
        })
    }

    // waits and finalizes all parallel tasks
    pub async fn wait_transactions(&mut self, collator_data: &mut CollatorData) -> Result<()> {
        log::trace!("{}: wait_transactions", self.collated_block_descr);
        while self.wait_tr.count() > 0 {
            self.wait_transaction(collator_data).await?;
        }
        self.min_lt.fetch_max(self.max_lt.load(Ordering::Relaxed), Ordering::Relaxed);
        Ok(())
    }

    // checks if a number of parallel transactilns is not too big, waits and finalizes some if needed.
    pub async fn check_parallel_transactions(&mut self, collator_data: &mut CollatorData) -> Result<()> {
        log::trace!("{}: check_parallel_transactions", self.collated_block_descr);
        if self.wait_tr.count() >= self.max_collate_threads {
            self.wait_transaction(collator_data).await?;
        }
        Ok(())
    }

    pub async fn execute(
        &mut self,
        account_id: AccountId,
        msg: AsyncMessage,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
    ) -> Result<()> {
        log::trace!("{}: execute (adding into queue): {:x}", self.collated_block_descr, account_id);
        self.wait_tr.request();
        let msg = Arc::new(msg);
        match self.changed_accounts.get(&account_id) {
            None => {
                let (sender, handle) = self.start_account_job(
                    account_id.clone(),
                    prev_data.accounts(),
                )?;
                sender.send(msg)?;
                self.changed_accounts.insert(account_id, (sender, handle));
            }
            Some((sender, _handle)) => sender.send(msg)?
        }

        self.check_parallel_transactions(collator_data).await?;

        Ok(())
    }

    fn start_account_job(
        &self,
        account_addr: AccountId,
        accounts: &ShardAccounts,
    ) -> Result<(tokio::sync::mpsc::UnboundedSender<Arc<AsyncMessage>>, tokio::task::JoinHandle<Result<ShardAccountStuff>>)> {
        log::trace!("{}: start_account_job: {:x}", self.collated_block_descr, account_addr);

        let mut shard_acc = ShardAccountStuff::from_shard_state(
            account_addr,
            accounts,
            Arc::new(AtomicU64::new(self.min_lt.load(Ordering::Relaxed))),
        )?;

        let debug = self.debug;
        let block_unixtime = self.gen_utime;
        let block_lt = self.start_lt;
        let seed_block = self.seed_block.clone();
        let collated_block_descr = self.collated_block_descr.clone();
        let total_trans_duration = self.total_trans_duration.clone();
        let wait_tr = self.wait_tr.clone();
        let config = self.config.clone();
        let min_lt = self.min_lt.clone();
        let max_lt = self.max_lt.clone();
        let libraries = self.libraries.clone().inner();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<Arc<AsyncMessage>>();
        let handle = tokio::spawn(async move {
            while let Some(new_msg) = receiver.recv().await {
                log::trace!("{}: new message for {:x}", collated_block_descr, shard_acc.account_addr());
                let config = config.clone(); // TODO: use Arc

                shard_acc.lt().fetch_max(min_lt.load(Ordering::Relaxed), Ordering::Relaxed);
                shard_acc.lt().fetch_max(
                    shard_acc.last_trans_lt() + 1, 
                    Ordering::Relaxed
                );
                shard_acc.lt().fetch_max(
                    shard_acc.last_trans_lt() + 1, 
                    Ordering::Relaxed
                );

                let mut account_root = shard_acc.account_root();
                let params = ExecuteParams {
                    state_libs: libraries.clone(),
                    block_unixtime,
                    block_lt,
                    last_tr_lt: shard_acc.lt(),
                    seed_block: seed_block.clone(),
                    debug,
                    block_version: supported_version(),
                    ..ExecuteParams::default()
                };
                let new_msg1 = new_msg.clone();
                let (mut transaction_res, account_root, duration) = tokio::task::spawn_blocking(move || {
                    let now = std::time::Instant::now();
                    (
                        Self::execute_new_message(&new_msg1, &mut account_root, config, params),
                        account_root,
                        now.elapsed().as_micros() as u64
                    )
                }).await?;

                if let Ok(transaction) = transaction_res.as_mut() {
                    shard_acc.add_transaction(transaction, account_root)?;
                }
                total_trans_duration.fetch_add(duration, Ordering::Relaxed);
                log::trace!("{}: account {:x} TIME execute {}μ;", 
                    collated_block_descr, shard_acc.account_addr(), duration);

                max_lt.fetch_max(shard_acc.lt().load(Ordering::Relaxed), Ordering::Relaxed);
                wait_tr.respond(Some((new_msg, transaction_res)));
            }
            Ok(shard_acc)
        });
        Ok((sender, handle))
    }

    fn execute_new_message(
        new_msg: &AsyncMessage,
        account_root: &mut Cell,
        config: BlockchainConfig,
        params: ExecuteParams,
    ) -> Result<Transaction> {
        let (executor, msg_opt): (Box<dyn TransactionExecutor>, _) = match new_msg {
            AsyncMessage::Int(enq, _our) => {
                (Box::new(OrdinaryTransactionExecutor::new(config)), Some(enq.message()))
            }
            AsyncMessage::New(env, _prev_tr_cell) => {
                (Box::new(OrdinaryTransactionExecutor::new(config)), Some(env.message()))
            }
            AsyncMessage::Recover(msg) | AsyncMessage::Mint(msg) | AsyncMessage::Ext(msg) => {
                (Box::new(OrdinaryTransactionExecutor::new(config)), Some(msg))
            }
            AsyncMessage::Copyleft(msg) => {
                (Box::new(OrdinaryTransactionExecutor::new(config)), Some(msg))
            }
            AsyncMessage::TickTock(tt) => {
                (Box::new(TickTockTransactionExecutor::new(config, tt.clone())), None)
            }
        };
        executor.execute_with_libs_and_params(msg_opt, account_root, params)
    }

    async fn wait_transaction(&mut self, collator_data: &mut CollatorData) -> Result<()> {
        log::trace!("{}: wait_transaction", self.collated_block_descr);
        let wait_op = self.wait_tr.wait(&mut self.receive_tr, false).await;
        if let Some(Some((new_msg, transaction_res))) = wait_op {
            self.finalize_transaction(new_msg, transaction_res, collator_data)?;
        }
        Ok(())
    }

    fn finalize_transaction(
        &mut self,
        new_msg: Arc<AsyncMessage>,
        transaction_res: Result<Transaction>,
        collator_data: &mut CollatorData
    ) -> Result<()> {
        if let AsyncMessage::Ext(ref msg) = new_msg.deref() {
            let msg_id = msg.serialize()?.repr_hash();
            let account_id = msg.int_dst_account_id().unwrap_or_default();
            if let Err(err) = transaction_res {
                log::warn!(
                    target: EXT_MESSAGES_TRACE_TARGET,
                    "{}: account {:x} rejected inbound external message {:x} by reason: {}", 
                    self.collated_block_descr, account_id, msg_id, err
                );
                collator_data.to_delay.push(msg_id);
                return Ok(())
            } else {
                log::debug!(
                    target: EXT_MESSAGES_TRACE_TARGET,
                    "{}: account {:x} accepted inbound external message {:x}",
                    self.collated_block_descr, account_id, msg_id,
                );
            }
        }
        let tr = transaction_res?;
        let tr_cell = tr.serialize()?;
        log::trace!("{}: finalize_transaction {} with hash {:x}, {:x}",
            self.collated_block_descr, tr.logical_time(), tr_cell.repr_hash(), tr.account_id());
        let in_msg_opt = match new_msg.deref() {
            AsyncMessage::Int(enq, our) => {
                let in_msg = InMsg::final_msg(enq.envelope_cell(), tr_cell.clone(), enq.fwd_fee_remaining().clone());
                if *our {
                    let out_msg = OutMsg::dequeue_immediate(enq.envelope_cell(), in_msg.serialize()?);
                    collator_data.add_out_msg_to_block(enq.message_hash(), &out_msg)?;
                    collator_data.del_out_msg_from_state(&enq.out_msg_key())?;
                }
                Some(in_msg)
            }
            AsyncMessage::New(env, prev_tr_cell) => {
                let env_cell = env.inner().serialize()?;
                let in_msg = InMsg::immediate(env_cell.clone(), tr_cell.clone(), env.fwd_fee_remaining().clone());
                let out_msg = OutMsg::immediate(env_cell, prev_tr_cell.clone(), in_msg.serialize()?);
                collator_data.add_out_msg_to_block(env.message_hash(), &out_msg)?;
                Some(in_msg)
            }
            AsyncMessage::Mint(msg) |
            AsyncMessage::Recover(msg) => {
                let env = MsgEnvelopeStuff::new(msg.clone(), &ShardIdent::masterchain(), Grams::default(), false)?;
                Some(InMsg::immediate(env.inner().serialize()?, tr_cell.clone(), Grams::default()))
            }
            AsyncMessage::Copyleft(msg) => {
                let env = MsgEnvelopeStuff::new(msg.clone(), &ShardIdent::masterchain(), Grams::default(), false)?;
                Some(InMsg::immediate(env.inner().serialize()?, tr_cell.clone(), Grams::default()))
            }
            AsyncMessage::Ext(msg) => {
                let in_msg = InMsg::external(msg.serialize()?, tr_cell.clone());
                Some(in_msg)
            }
            AsyncMessage::TickTock(_) => None
        };
        if tr.orig_status != tr.end_status {
            log::info!(
                "{}: Status of account {:x} was changed from {:?} to {:?} by message {:X}",
                self.collated_block_descr, tr.account_id(), tr.orig_status, tr.end_status,
                tr.in_msg_cell().unwrap_or_default().repr_hash()
            );
        }
        collator_data.new_transaction(&tr, tr_cell, in_msg_opt.as_ref())?;

        collator_data.update_lt(self.max_lt.load(Ordering::Relaxed));

        match new_msg.deref() {
            AsyncMessage::Mint(_) => collator_data.mint_msg = in_msg_opt,
            AsyncMessage::Recover(_) => collator_data.recover_create_msg = in_msg_opt,
            AsyncMessage::Copyleft(_) => collator_data.copyleft_msgs.push(in_msg_opt.ok_or_else(|| error!("Can't unwrap `in_msg_opt`"))?),
            _ => ()
        }
        collator_data.block_full |= !collator_data.block_limit_status.fits(ParamLimitIndex::Normal);

        Ok(())
    }
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
    collated_block_descr: Arc<String>,

    debug: bool,
    rand_seed: UInt256,
    collator_settings: CollatorSettings,

    started: Instant,
    cutoff_timeout: Duration,
    stop_flag: Arc<AtomicBool>,
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

        let collated_block_descr = Arc::new(format!("{}:{}, {}", 
            shard.workchain_id(), 
            shard.shard_prefix_as_str_with_tag(), 
            new_block_seqno
        ));

        log::trace!("{}: new", collated_block_descr);

        // check inputs

        if !shard.is_masterchain() && !shard.is_standard_workchain() {
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

        let rand_seed = rand_seed.unwrap_or_else(|| secure_256_bits().into());

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
            started: Instant::now(),
            cutoff_timeout: Default::default(),
            stop_flag: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn collate(mut self, timeout_ms: u32) -> Result<(BlockCandidate, ShardStateUnsplit)> {
        log::info!(
            "{}: COLLATE min_mc_block_id.seqno = {}, prev_blocks_ids: {} {}",
            self.collated_block_descr,
            self.min_mc_block_id.seq_no(),
            self.prev_blocks_ids[0],
            if self.prev_blocks_ids.len() > 1 { format!("{}", self.prev_blocks_ids[1]) } else { "".to_owned() }
        );
        self.init_timeout(timeout_ms);

        let imported_data = self.import_data()
            .await.map_err(|e| {
                log::warn!("{}: COLLATION FAILED: TIME: {}ms import_data: {}",
                    self.collated_block_descr, self.started.elapsed().as_millis(), e);
                e
            })?;

        let (mc_data, prev_data, mut collator_data) = self.prepare_data(imported_data)
            .await.map_err(|e| {
                log::warn!("{}: COLLATION FAILED: TIME: {}ms prepare_data: {}",
                    self.collated_block_descr, self.started.elapsed().as_millis(), e);
                e
            })?;
        
        let (candidate, state, exec_manager) = 
            self.do_collate(&mc_data, &prev_data, &mut collator_data).await
            .map_err(|e| {
                log::warn!("{}: COLLATION FAILED: TIME: {}ms do_collate: {}",
                    self.collated_block_descr, self.started.elapsed().as_millis(), e);
                e
            })?;

        let duration = self.started.elapsed().as_millis() as u32;
        let ratio = match duration {
            0 => collator_data.block_limit_status.gas_used(),
            duration => collator_data.block_limit_status.gas_used() / duration
        };
        log::info!(
            "{}: ASYNC COLLATED SIZE: {} GAS: {} TIME: {}ms GAS_RATE: {} TRANS: {}ms ID: {}",
            self.collated_block_descr,
            candidate.data.len(),
            collator_data.block_limit_status.gas_used(),
            duration,
            ratio,
            exec_manager.total_trans_duration.load(Ordering::Relaxed) / 1000,
            candidate.block_id,
        );

        #[cfg(feature = "metrics")]
        report_collation_metrics(
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

        #[cfg(feature = "telemetry")]
        self.engine.collator_telemetry().succeeded_attempt(
            &self.shard,
            self.started.elapsed(),
            collator_data.execute_count as u32,
            collator_data.block_limit_status.gas_used()
        );

        Ok((candidate, state))
    }

    async fn import_data(&self) -> Result<ImportedData> {
        log::trace!("{}: import_data", self.collated_block_descr);

        if self.shard.is_masterchain() {
            let (prev_states, prev_ext_blocks_refs) = self.import_prev_stuff().await?;
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

        self.check_stop_flag()?;

        CHECK!(imported_data.prev_states.len() == 1 + self.after_merge as usize);
        CHECK!(imported_data.prev_states.len() == self.prev_blocks_ids.len());

        CHECK!(imported_data.mc_state.block_id(), inited);

        let mc_data = self.unpack_last_mc_state(imported_data.mc_state)?;
        let state_root = self.unpack_last_state(&mc_data, &imported_data.prev_states)?;
        let pure_states = imported_data.prev_states.clone();
        let usage_tree = self.create_usage_tree(state_root.clone(), &mut imported_data.prev_states)?;
        self.check_stop_flag()?;

        let subshard = match self.after_split {
            true => Some(&self.shard),
            false => None
        };
        let prev_data = PrevData::from_prev_states(imported_data.prev_states, pure_states, state_root, subshard)?;
        let is_masterchain = self.shard.is_masterchain();
        self.check_stop_flag()?;

        let now = self.init_utime(&mc_data, &prev_data)?;
        let config = BlockchainConfig::with_config(mc_data.config().clone())?;
        let mut collator_data = CollatorData::new(
            now,
            config,
            usage_tree,
            &prev_data,
            is_masterchain,
        )?;
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
        self.check_stop_flag()?;

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

        collator_data.prev_stuff = Some(BlkPrevInfo::new(imported_data.prev_ext_blocks_refs)?);

        Ok((mc_data, prev_data, collator_data))
    }

    async fn do_collate(
        &self,
        mc_data: &McData,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
    ) -> Result<(BlockCandidate, ShardStateUnsplit, ExecutionManager)> {
        log::trace!("{}: do_collate", self.collated_block_descr);

        let ext_messages = self.engine.get_external_messages(&self.shard)?;
        self.check_stop_flag()?;

        let mut output_queue_manager = self.request_neighbor_msg_queues(mc_data, prev_data, collator_data).await?;

        // delete delivered messages from output queue
        let now = std::time::Instant::now();
        self.clean_out_msg_queue(mc_data, collator_data, &mut output_queue_manager)?;
        log::trace!("{}: TIME: clean_out_msg_queue {}ms;",
            self.collated_block_descr, now.elapsed().as_millis());

        // copy out msg queue from next state
        collator_data.out_msg_queue_info = output_queue_manager.take_next();
        collator_data.out_msg_queue_info.forced_fix_out_queue()?;

        // compute created / minted / recovered / from_prev_blk
        self.update_value_flow(mc_data, &prev_data, collator_data)?;

        let mut exec_manager = ExecutionManager::new(
            collator_data.gen_utime(),
            collator_data.start_lt()?,
            self.rand_seed.clone(),
            mc_data.libraries().clone(),
            collator_data.config.clone(),
            self.collator_settings.max_collate_threads.unwrap_or(MAX_COLLATE_THREADS),
            self.collated_block_descr.clone(),
            self.debug,
        )?;

        // tick & special transactions
        if self.shard.is_masterchain() {
            self.create_ticktock_transactions(
                false, mc_data, prev_data, collator_data, &mut exec_manager).await?;
            self.create_special_transactions(
                mc_data, prev_data, collator_data, &mut exec_manager).await?;
        }

        let new_state_copyleft_rewards = self.send_copyleft_rewards(mc_data, prev_data, collator_data, &mut exec_manager).await?;

        // merge prepare / merge install
        // ** will be implemented later **

        // import inbound internal messages, process or transit
        let now = std::time::Instant::now();
        self.process_inbound_internal_messages(prev_data, collator_data, &output_queue_manager,
            &mut exec_manager).await?;
        log::trace!("{}: TIME: process_inbound_internal_messages {}ms;", 
            self.collated_block_descr, now.elapsed().as_millis());

        // import inbound external messages (if space&gas left)
        let now = std::time::Instant::now();
        self.process_inbound_external_messages(prev_data, collator_data, &mut exec_manager, 
            ext_messages).await?;
        log::trace!("{}: TIME: process_inbound_external_messages {}ms;", 
            self.collated_block_descr, now.elapsed().as_millis());

        // process newly-generated messages (if space&gas left)
        // (if we were unable to process all inbound messages, all new messages must be queued)
        let now = std::time::Instant::now();
        self.process_new_messages(!collator_data.inbound_queues_empty, prev_data, 
            collator_data, &mut exec_manager).await?;
        log::trace!("{}: TIME: process_new_messages {}ms;", 
            self.collated_block_descr, now.elapsed().as_millis());

        // split prepare / split install
        // ** will be implemented later **

        // tock transactions
        if self.shard.is_masterchain() {
            self.create_ticktock_transactions(
                true, mc_data, prev_data, collator_data, &mut exec_manager).await?;
        }

        // process newly-generated messages (only by including them into output queue)
        self.process_new_messages(
            true, prev_data, collator_data, &mut exec_manager).await?;

        // update block history
        self.check_block_overload(collator_data);

        // update processed upto
        self.update_processed_upto(mc_data, collator_data)?;

        //collator_data.block_limit_status.dump_block_size();

        // serialize everything
        self.finalize_block(mc_data, prev_data, collator_data, exec_manager, new_state_copyleft_rewards).await
    }

    fn clean_out_msg_queue(
        &self,
        mc_data: &McData,
        collator_data: &mut CollatorData,
        output_queue_manager: &mut MsgQueueManager
    ) -> Result<bool> {
        log::trace!("{}: clean_out_msg_queue", self.collated_block_descr);
        let short = mc_data.config().has_capability(GlobalCapabilities::CapShortDequeue);
        output_queue_manager.clean_out_msg_queue(|message, root| {
            self.check_stop_flag()?;
            if let Some((enq, deliver_lt)) = message {
                log::debug!("{}: dequeue message: {:x}", self.collated_block_descr, enq.message_hash());
                collator_data.dequeue_message(enq, deliver_lt, short)?;
                collator_data.block_limit_status.register_out_msg_queue_op(root, &collator_data.usage_tree, false)?;
                collator_data.block_full |= !collator_data.block_limit_status.fits(ParamLimitIndex::Normal);
                Ok(collator_data.block_full)
            } else {
                collator_data.block_limit_status.register_out_msg_queue_op(root, &collator_data.usage_tree, true)?;
                collator_data.block_full |= !collator_data.block_limit_status.fits(ParamLimitIndex::Normal);
                Ok(true)
            }
        })
    }

    //
    // import
    //

    async fn import_mc_stuff(&self) -> Result<Arc<ShardStateStuff>> {
        log::trace!("{}: import_mc_stuff", self.collated_block_descr);
        let mc_state = self.engine.load_last_applied_mc_state().await?;
        
        if mc_state.block_id().seq_no() < self.min_mc_block_id.seq_no() {
            fail!("requested to create a block referring to a non-existent future masterchain block");
        }
        Ok(mc_state)
    }

    async fn import_prev_stuff(&self) -> Result<(Vec<Arc<ShardStateStuff>>, Vec<ExtBlkRef>)> {
        log::trace!("{}: import_prev_stuff", self.collated_block_descr);
        let mut prev_states = vec!();
        let mut prev_ext_blocks_refs = vec![];
        for (i, prev_id) in self.prev_blocks_ids.iter().enumerate() {
            let prev_state = self.engine.clone().wait_state(prev_id, Some(1_000), true).await?;

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

    fn unpack_last_mc_state(&self, mc_state: Arc<ShardStateStuff>) -> Result<McData> {
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

    fn unpack_last_state(&self, mc_data: &McData, prev_states: &Vec<Arc<ShardStateStuff>>) -> Result<Cell> {
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

    fn check_one_state(&self, mc_data: &McData, state: &Arc<ShardStateStuff>) -> Result<()> {
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
    fn create_usage_tree(
        &self, 
        state_root: Cell, 
        prev_states: &mut Vec<Arc<ShardStateStuff>>
    ) -> Result<UsageTree> {
        log::trace!("{}: create_usage_tree", self.collated_block_descr);
        let usage_tree = UsageTree::with_params(state_root, true);
        let root_cell = usage_tree.root_cell();
        *prev_states = if prev_states.len() == 2 {
            let ss_split = ShardStateSplit::construct_from_cell(root_cell.clone())?;
            vec![
                ShardStateStuff::from_root_cell(
                    prev_states[0].block_id().clone(), 
                    ss_split.left,
                    #[cfg(feature = "telemetry")]
                    self.engine.engine_telemetry(),
                    self.engine.engine_allocated()
                )?,
                ShardStateStuff::from_root_cell(
                    prev_states[1].block_id().clone(), 
                    ss_split.right,
                    #[cfg(feature = "telemetry")]
                    self.engine.engine_telemetry(),
                    self.engine.engine_allocated()
                )?
            ]
        } else {
            vec![
                ShardStateStuff::from_state(
                    prev_states[0].block_id().clone(), 
                    ShardStateUnsplit::construct_from_cell(root_cell.clone())?,
                    #[cfg(feature = "telemetry")]
                    self.engine.engine_telemetry(),
                    self.engine.engine_allocated()
                )?
            ]
        };
        Ok(usage_tree)
    }

    fn init_utime(&self, mc_data: &McData, prev_data: &PrevData) -> Result<u32> {

        // consider unixtime and lt from previous block(s) of the same shardchain
        let prev_now = prev_data.prev_state_utime();
        let prev = max(mc_data.state().state().gen_time(), prev_now);
        log::trace!("{}: init_utime prev_time: {}", self.collated_block_descr, prev);
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
                    self.collated_block_descr, overdue
                );
            }
            if skip_extmsg {
                collator_data.set_skip_extmsg();
                log::warn!(
                    "{}: randomly skipping external message import because of overdue masterchain \
                    catchain rotation (overdue by {} seconds)",
                    self.collated_block_descr, overdue
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
                    self.collated_block_descr, interval
                );
            }
            if skip_extmsg {
                collator_data.set_skip_extmsg();
                log::warn!(
                    "{}: randomly skipping external message import because of overdue masterchain \
                    block (last block was {} seconds ago)",
                    self.collated_block_descr, interval
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

    async fn request_neighbor_msg_queues(
        &self, 
        mc_data: &McData, 
        prev_data: &PrevData, 
        collator_data: &mut CollatorData
    ) -> Result<MsgQueueManager> {
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
            Some(&self.stop_flag),
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
                    self.check_stop_flag()?;
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
            self.check_stop_flag()?;
            let mut res_flags = 0;
            let now = std::time::Instant::now();
            let result = sh_bd.prevalidate(
                mc_data.state().block_id(),
                mc_data.mc_state_extra(),
                mc_data.state().state().vert_seq_no(),
                TbdMode::FAIL_NEW | TbdMode::FAIL_TOO_NEW,
                &mut res_flags
            );
            log::debug!("{}: prevalidate TIME: {}μ for {}", self.collated_block_descr, now.elapsed().as_micros(), sh_bd.proof_for().shard());
            let chain_len = match result {
                Ok(len) => {
                    if len <= 0 || len > 8 {
                        log::debug!("{}: ShardTopBlockDescr for {} skipped: its chain length is {}",
                            self.collated_block_descr, sh_bd.proof_for(), len);
                        continue;
                    }
                    len as usize
                }
                Err(e) => {
                    log::debug!("{}: ShardTopBlockDescr for {} skipped: res_flags = {}, error: {}",
                    self.collated_block_descr, sh_bd.proof_for(), res_flags, e);
                    continue
                }
            };
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
            let shard = descr.shard();
            let start_blks = sh_bd.get_prev_at(chain_len);
            let now = std::time::Instant::now();
            let result = may_update_shard_block_info(collator_data.shards()?, &descr, &start_blks, lt_limit, Some(&mut shards_updated));
            log::debug!("{}: may_update_shard_block_info TIME: {}μ for {}", self.collated_block_descr, now.elapsed().as_micros(), descr.shard());
            match result {
                Err(e) => {
                    log::debug!("{}: cannot add new top shard block {} to shard configuration: {}",
                        self.collated_block_descr, sh_bd.proof_for(), e);
                    continue
                }
                Ok((false, _)) => {
                    CHECK!(start_blks.len() == 1);

                    if &prev_shard.sibling() == shard {

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
                            collator_data.store_workchain_copyleft_rewards(&prev_descr)?;
                            collator_data.store_workchain_copyleft_rewards(&descr)?;
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
                    let result = update_shard_block_info(
                        collator_data.shards_mut()?,
                        descr.clone(),
                        &start_blks,
                        Some(&mut shards_updated)
                    );
                    if let Err(e) = result {
                        log::debug!("{}: cannot add new top shard block {} to shard configuration: {}",
                            self.collated_block_descr, sh_bd.proof_for(), e);
                        //descr.clear();
                    } else {
                        collator_data.store_shard_fees(&descr)?;
                        collator_data.store_workchain_copyleft_rewards(&descr)?;
                        collator_data.register_shard_block_creators(sh_bd.get_creator_list(chain_len)?)?;
                        collator_data.update_shards_max_end_lt(end_lt);
                        log::debug!("{}: updated top shard block information with {}",
                            self.collated_block_descr, sh_bd.proof_for());
                        tb_act += 1;
                        collator_data.add_top_block_descriptor(sh_bd.clone());
                    }
                }
            }
            if self.check_cutoff_timeout() {
                log::warn!("{}: TIMEOUT ({}ms) is elapsed, stop processing import_new_shard_top_blocks",
                        self.collated_block_descr, self.cutoff_timeout.as_millis());
                break
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
                    if collator_data.value_flow.recovered.grams.as_u128() < 1_000_000_000 {
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
            collator_data.value_flow.created.grams >>= self.shard.prefix_len();
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

        let old_global_balance = mc_data.global_balance();
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

    async fn create_ticktock_transactions(
        &self,
        tock: bool,
        mc_data: &McData,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
        exec_manager: &mut ExecutionManager,
    ) -> Result<()> {
        log::trace!("{}: create_ticktock_transactions", self.collated_block_descr);
        let config_account_id = AccountId::from(mc_data.config().config_addr.clone());
        let fundamental_dict = mc_data.config().fundamental_smc_addr()?;
        for res in &fundamental_dict {
            let account_id = res?.0.into_cell()?.into();
            self.create_ticktock_transaction(account_id, tock, prev_data, collator_data, 
                exec_manager).await?;
            self.check_stop_flag()?;
        }
        self.create_ticktock_transaction(config_account_id, tock, prev_data, collator_data, 
            exec_manager).await?;
        exec_manager.wait_transactions(collator_data).await?;
        Ok(())
    }

    async fn create_ticktock_transaction(
        &self,
        account_id: AccountId,
        tock: bool,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
        exec_manager: &mut ExecutionManager,
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
            let tt = if tock {TransactionTickTock::Tock} else {TransactionTickTock::Tick};
            // different accounts can produce messages with same LT which cause order violation
            exec_manager.execute(account_id, AsyncMessage::TickTock(tt), prev_data, collator_data).await?;
        }

        Ok(())
    }

    async fn create_special_transactions(
        &self,
        mc_data: &McData,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
        exec_manager: &mut ExecutionManager,
    ) -> Result<()> {
        if !self.shard.is_masterchain() {
            return Ok(())
        }
        log::trace!("{}: create_special_transactions", self.collated_block_descr);

        let account_id = AccountId::from(mc_data.config().fee_collector_address()?.serialize()?);
        self.create_special_transaction(
            account_id,
            collator_data.value_flow.recovered.clone(),
            |msg| AsyncMessage::Recover(msg),
            prev_data,
            collator_data,
            exec_manager
        ).await?;
        self.check_stop_flag()?;

        let account_id = AccountId::from(mc_data.config().minter_address()?);
        self.create_special_transaction(
            account_id,
            collator_data.value_flow.minted.clone(),
            |msg| AsyncMessage::Mint(msg),
            prev_data,
            collator_data,
            exec_manager
        ).await?;

        exec_manager.wait_transactions(collator_data).await?;

        Ok(())
    }

    async fn create_special_transaction(
        &self,
        account_id: AccountId,
        amount: CurrencyCollection,
        f: impl FnOnce(Message) -> AsyncMessage,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
        exec_manager: &mut ExecutionManager,
    ) -> Result<()> {
        log::trace!(
            "{}: create_special_transaction: recover {} to account {:x}",
            self.collated_block_descr,
            amount.grams,
            account_id
        );
        if amount.is_zero()? || !self.shard.is_masterchain() {
            return Ok(())
        }
        let mut hdr = InternalMessageHeader::with_addresses_and_bounce(
            MsgAddressInt::with_standart(None, -1, [0; 32].into())?,
            MsgAddressInt::with_standart(None, -1, account_id.clone())?,
            amount,
            true
        );
        hdr.created_lt = collator_data.start_lt()?;
        hdr.created_at = collator_data.gen_utime.into();
        let msg = Message::with_int_header(hdr);
        exec_manager.execute(account_id, f(msg), prev_data, collator_data).await?;
        Ok(())
    }

    async fn process_inbound_internal_messages(
        &self,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
        output_queue_manager: &MsgQueueManager,
        exec_manager: &mut ExecutionManager,
    ) -> Result<()> {
        log::trace!("{}: process_inbound_internal_messages", self.collated_block_descr);
        let mut iter = output_queue_manager.merge_out_queue_iter(&self.shard)?;
        while let Some(k_v) = iter.next() {
            let (key, enq, created_lt, block_id) = k_v?;
            collator_data.update_last_proc_int_msg((created_lt, enq.message_hash()))?;
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
                    let account_id = enq.dst_account_id()?;
                    let msg = AsyncMessage::Int(enq, our);
                    exec_manager.execute(account_id, msg, prev_data, collator_data).await?;
                } else {
                    // println!("{:x} {:#}", key, enq);
                    // println!("cur: {}, dst: {}", enq.cur_prefix(), enq.dst_prefix());
                    log::debug!("{}: enqueue_transit_message {:x}", self.collated_block_descr, enq.message_hash());
                    collator_data.enqueue_transit_message(&self.shard, &key, &enq, our)?;
                    if our {
                        collator_data.del_out_msg_from_state(&key)?;
                    }
                }
            }
            if collator_data.block_full {
                log::trace!("{}: BLOCK FULL, stop processing internal messages", self.collated_block_descr);
                break
            }
            if self.check_cutoff_timeout() {
                log::warn!("{}: TIMEOUT ({}ms) is elapsed, stop processing internal messages",
                        self.collated_block_descr, self.cutoff_timeout.as_millis());
                break
            }
            self.check_stop_flag()?;
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

    async fn process_inbound_external_messages(
        &self,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
        exec_manager: &mut ExecutionManager,
        mut ext_messages: Vec<(Arc<Message>, UInt256)>,
    ) -> Result<()> {
        if collator_data.skip_extmsg() {
            log::trace!("{}: skipping processing of inbound external messages", self.collated_block_descr);
            return Ok(())
        }

        //LK temporary: check external messaging delivery
        {
            use rand::thread_rng;
            use rand::seq::SliceRandom;
            let mut rng = thread_rng();
            ext_messages.shuffle(&mut rng);
            log::debug!("{}: try to process {} external messages", self.collated_block_descr, ext_messages.len());
        }
        //LK temporary end

        log::trace!("{}: process_inbound_external_messages", self.collated_block_descr);
        for (msg, id) in ext_messages.drain(..) {
            let header = msg.ext_in_header().ok_or_else(|| error!("message {:x} \
                is not external inbound message", id))?;
            if self.shard.contains_address(&header.dst)? {
                if !collator_data.block_limit_status.fits(ParamLimitIndex::Soft) {
                    log::trace!("{}: BLOCK FULL, stop processing external messages", self.collated_block_descr);
                    break
                }
                if self.check_cutoff_timeout() {
                    log::warn!("{}: TIMEOUT ({}ms) is elapsed, stop processing external messages",
                        self.collated_block_descr, self.cutoff_timeout.as_millis());
                    break
                }
                let (_, account_id) = header.dst.extract_std_address(true)?;
                let msg = AsyncMessage::Ext(msg.deref().clone());
                exec_manager.execute(account_id, msg, prev_data, collator_data).await?;
            } else {
                // usually node collates more than one shard, the message can belong another one,
                // so we can't postpone it
                // (difference with t-node)
                // collator_data.to_delay.push(id);
            }
            self.check_stop_flag()?;
        }
        exec_manager.wait_transactions(collator_data).await?;
        self.engine.complete_external_messages(collator_data.withdraw_ext_msg_to_delay(), vec![])?;
        Ok(())
    }

    async fn process_new_messages(
        &self,
        mut enqueue_only: bool,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
        exec_manager: &mut ExecutionManager,
    ) -> Result<()> {
        log::trace!("{}: process_new_messages", self.collated_block_descr);
        let use_hypercube = !collator_data.config.has_capability(GlobalCapabilities::CapOffHypercube);
        while !collator_data.new_messages.is_empty() {

            // In the iteration we execute only existing messages.
            // Newly generating messages will be executed next itaration (only after waiting).

            let mut new_messages = std::mem::take(&mut collator_data.new_messages);
            while let Some(NewMessage{ lt_hash: _, msg, tr_cell }) = new_messages.pop() {
                let info = msg.int_header().ok_or_else(|| error!("message is not internal"))?;
                let fwd_fee = info.fwd_fee().clone();
                enqueue_only |= collator_data.block_full | self.check_cutoff_timeout();
                if !self.shard.contains_address(&info.dst)? || enqueue_only {
                    let enq = MsgEnqueueStuff::new(msg, &self.shard, fwd_fee, use_hypercube)?;
                    collator_data.add_out_msg_to_state(&enq, true)?;
                    let out_msg = OutMsg::new(enq.envelope_cell(), tr_cell);
                    collator_data.add_out_msg_to_block(out_msg.read_message_hash()?, &out_msg)?;
                } else {
                    CHECK!(info.created_at.as_u32(), collator_data.gen_utime);
                    let created_lt = info.created_lt;
                    let account_id = msg.int_dst_account_id().unwrap_or_default();
                    let env = MsgEnvelopeStuff::new(msg, &self.shard, fwd_fee, use_hypercube)?;
                    let hash = env.message_hash();
                    collator_data.update_last_proc_int_msg((created_lt, hash))?;
                    let msg = AsyncMessage::New(env, tr_cell);
                    exec_manager.execute(account_id, msg, prev_data, collator_data).await?;
                };
                self.check_stop_flag()?;
            }
            exec_manager.wait_transactions(collator_data).await?;
            self.check_stop_flag()?;
        }

        Ok(())
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
        log::trace!("{}: check_block_overload", self.collated_block_descr);
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

    async fn send_copyleft_rewards(
        &self,
        mc_data: &McData,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
        exec_manager: &mut ExecutionManager
    ) -> Result<CopyleftRewards> {
        if self.shard.is_masterchain() {
            if let Ok(copyleft_config) = mc_data.config().copyleft_config() {
                let mut new_state_copyleft_rewards = prev_data.state_copyleft_rewards.clone();
                let send_rewards = new_state_copyleft_rewards.merge_rewards_with_threshold(
                    &collator_data.get_workchains_copyleft_rewards(), &copyleft_config.copyleft_reward_threshold
                )?;
                log::debug!("send copyleft rewards count: {}", send_rewards.len());

                for (account_id, value) in send_rewards {
                    log::trace!(
                        "{}: create copyleft reward transaction: reward {} to account {:x}",
                        self.collated_block_descr,
                        value,
                        account_id
                    );
                    let mut hdr = InternalMessageHeader::with_addresses(
                        MsgAddressInt::with_standart(None, -1, [0; 32].into())?,
                        MsgAddressInt::with_standart(None, -1, account_id.clone())?,
                        CurrencyCollection::from_grams(value)
                    );
                    hdr.ihr_disabled = true;
                    hdr.bounce = false;
                    hdr.created_lt = collator_data.start_lt()?;
                    hdr.created_at = UnixTime32::new(collator_data.gen_utime);
                    let msg = Message::with_int_header(hdr);
                    exec_manager.execute(account_id, AsyncMessage::Copyleft(msg), prev_data, collator_data).await?;

                    self.check_stop_flag()?;
                }
                exec_manager.wait_transactions(collator_data).await?;

                return Ok(new_state_copyleft_rewards)
            }
        }
        Ok(CopyleftRewards::default())
    }

    //
    // finalize
    //
    async fn finalize_block(
        &self,
        mc_data: &McData,
        prev_data: &PrevData,
        collator_data: &mut CollatorData,
        mut exec_manager: ExecutionManager,
        new_state_copyleft_rewards: CopyleftRewards,
    ) -> Result<(BlockCandidate, ShardStateUnsplit, ExecutionManager)> {
        log::trace!("{}: finalize_block", self.collated_block_descr);

        let (want_split, overload_history)  = collator_data.want_split();
        let (want_merge, underload_history) = collator_data.want_merge();

        // update shard accounts tree and prepare accounts blocks
        let mut new_accounts = prev_data.accounts.clone();
        let mut accounts = ShardAccountBlocks::default();
        let config_addr = match self.shard.is_masterchain() {
            true => prev_data.state().config_params()?.config_address().ok(),
            false => None
        };
        let mut changed_accounts = HashMap::new();
        let mut new_config_opt = None;
        let mut current_workchain_copyleft_rewards = CopyleftRewards::default();
        for (account_id, (sender, handle)) in exec_manager.changed_accounts.drain() {
            std::mem::drop(sender);
            let mut shard_acc = handle.await
                .map_err(|err| error!("account {:x} thread didn't finish: {}", account_id, err))??;
            let account = shard_acc.read_account()?;
            if let Some(addr) = &config_addr {
                if addr == &account_id {
                    let new_config_root = account
                        .get_data()
                        .ok_or_else(|| error!("Can't extract config's contract data"))?
                        .reference(0)?;
                    new_config_opt = Some(ConfigParams::with_address_and_params(config_addr.clone().unwrap(), Some(new_config_root)));
                }
            }
            let acc_block = shard_acc.update_shard_state(&mut new_accounts)?;
            if !acc_block.transactions().is_empty() {
                accounts.insert(&acc_block)?;
            }
            current_workchain_copyleft_rewards.merge_rewards(shard_acc.copyleft_rewards())?;
            changed_accounts.insert(account_id, shard_acc);
        }

        log::trace!("{}: finalize_block: calc value flow", self.collated_block_descr);
        // calc value flow
        let mut value_flow = collator_data.value_flow.clone();
        value_flow.imported = collator_data.in_msgs.root_extra().value_imported.clone();
        value_flow.exported = collator_data.out_msgs.root_extra().clone();
        value_flow.fees_collected = accounts.root_extra().clone();
        value_flow.fees_collected.grams.add(&collator_data.in_msgs.root_extra().fees_collected)?;
        log::trace!("Current workchain copyleft rewards count in finalize block: {}", current_workchain_copyleft_rewards.len()?);
        value_flow.copyleft_rewards = current_workchain_copyleft_rewards;

        // value_flow.fees_collected.grams.add(&out_msg_dscr.root_extra().grams)?; // TODO: Why only grams?

        value_flow.fees_collected.add(&value_flow.fees_imported)?;
        value_flow.fees_collected.add(&value_flow.created)?;
        value_flow.to_next_blk = new_accounts.full_balance().clone();
        //value_flow.to_next_blk.add(&value_flow.recovered)?;

        // println!("{}", &value_flow);

        let (out_msg_queue_info, min_ref_mc_seqno) = collator_data.out_msg_queue_info.serialize()?;
        collator_data.update_min_mc_seqno(min_ref_mc_seqno);
        let (mut mc_state_extra, master_ref) = if self.shard.is_masterchain() {
            let (extra, min_seqno) = self.create_mc_state_extra(prev_data, collator_data, new_config_opt)?;
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
        info.set_end_lt(collator_data.block_limit_status.lt() + 1);
        info.set_gen_utime(UnixTime32::new(collator_data.gen_utime));
        info.set_gen_validator_list_hash_short(gen_validator_list_hash_short);
        info.set_gen_catchain_seqno(self.validator_set.catchain_seqno());
        info.set_min_ref_mc_seqno(collator_data.min_mc_seqno()?);
        info.set_prev_key_block_seqno(mc_data.prev_key_block_seqno());
        info.write_master_ref(master_ref.as_ref())?;

        if mc_data.config().has_capability(GlobalCapabilities::CapReportVersion) {
            info.set_gen_software(Some(GlobalVersion {
                version: supported_version(),
                capabilities: supported_capabilities(),
            }));
        }

        log::trace!("{}: finalize_block: calc new state", self.collated_block_descr);
        // Calc new state, then state update

        log::trace!("copyleft rewards count from workchains: {}", collator_data.get_workchains_copyleft_rewards().len()?);
        if self.shard.is_masterchain() && !value_flow.copyleft_rewards.is_empty() {
            log::warn!("copyleft rewards in masterchain must be empty")
        }

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
        if let Some(mc_state_extra) = &mut mc_state_extra {
            log::trace!("New unsplit copyleft rewards count: {}", new_state_copyleft_rewards.len()?);
            mc_state_extra.state_copyleft_rewards = new_state_copyleft_rewards;
        }
        let mut total_validator_fees = prev_data.total_validator_fees().clone();
        // total_validator_fees.add(&value_flow.created)?;
        // total_validator_fees.add(&accounts.root_extra())?;
        total_validator_fees.add(&value_flow.fees_collected)?;
        total_validator_fees.sub(&value_flow.recovered)?;
        new_state.set_total_validator_fees(total_validator_fees);
        if self.shard.is_masterchain() {
            *new_state.libraries_mut() = self.update_public_libraries(
                exec_manager.libraries.clone(),
                &changed_accounts
            )?;
        }
        new_state.write_custom(mc_state_extra.as_ref())?;

        log::trace!("{}: finalize_block: calc merkle update", self.collated_block_descr);
        let visited = std::mem::take(&mut collator_data.usage_tree).visited();
        let new_ss_root = new_state.serialize()?;

        self.check_stop_flag()?;

        // let mut visited_from_root = HashSet::new();
        // Self::_check_visited_integrity(&prev_data.state_root, &visited, &mut visited_from_root);
        // assert_eq!(visited.len(), visited_from_root.len());

        let now = std::time::Instant::now();
        let state_update = MerkleUpdate::create_fast(
            &prev_data.state_root,
            &new_ss_root,
            |h| visited.contains(h)
        )?;
        log::trace!("{}: TIME: merkle update creating {}ms;", self.collated_block_descr, now.elapsed().as_millis());

        self.check_stop_flag()?;

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
            *mc_block_extra.hashes_mut() = collator_data.shards.clone().unwrap();
            *mc_block_extra.fees_mut() = collator_data.shard_fees.clone();
            mc_block_extra.write_recover_create_msg(collator_data.recover_create_msg.as_ref())?;
            mc_block_extra.write_mint_msg(collator_data.mint_msg.as_ref())?;
            mc_block_extra.write_copyleft_msgs(&collator_data.copyleft_msgs)?;
            if mc_state_extra.after_key_block {
                info.set_key_block(true);
                *mc_block_extra.config_mut() = Some(mc_state_extra.config().clone());
            }
            extra.write_custom(Some(&mc_block_extra))?;
        }
        extra.rand_seed = self.rand_seed.clone();
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

        self.check_stop_flag()?;

        let collated_data = if !collator_data.shard_top_block_descriptors.is_empty() {
            let mut tbds = TopBlockDescrSet::default();
            for stbd in collator_data.shard_top_block_descriptors.drain(..) {
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
            "{}: dequeue_count: {}, enqueue_count: {}, in_msg_count: {}, out_msg_count: {},\
            execute_count: {}, transit_count: {}",
            self.collated_block_descr, collator_data.dequeue_count, collator_data.enqueue_count,
            collator_data.in_msg_count, collator_data.out_msg_count, collator_data.execute_count,
            collator_data.transit_count
        );
        Ok((candidate, new_state, exec_manager))
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
        collator_data: &mut CollatorData,
        new_config_opt: Option<ConfigParams>,
    ) -> Result<(McStateExtra, u32)> {
        log::trace!("{}: build_mc_state_extra", self.collated_block_descr);
        CHECK!(!self.after_merge);
        CHECK!(self.new_block_id_part.shard_id.is_masterchain());

        // 1. update config:ConfigParams
        let state_extra = prev_data.state().shard_state_extra()?;
        let old_config = state_extra.config();
        let (config, is_key_block) = if let Some(new_config) = new_config_opt {
            if !new_config.valid_config_data(true, None)? {
                fail!("configuration smart contract {} contains an invalid configuration in its data",
                    new_config.config_addr);
            }
            let is_key_block = new_config.important_config_parameters_changed(state_extra.config(), false)?;
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
        let (validators, _hash_short) = calc_subset_for_workchain(
            &cur_validators,
            &config,
            &ccvc, 
            self.shard.shard_prefix_with_tag(), 
            self.shard.workchain_id(), 
            validator_info.catchain_seqno,
            now.into()
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
                global_balance,
                state_copyleft_rewards: CopyleftRewards::default(),
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
        wc_info: Option<WorkchainDescr>, // new wc config (with changes made in the current block)
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
                if info.is_fsm_none() &&                                // split/merge is not in progress
                   (info.want_split || depth < wc_info.min_split()) &&  // shard want splits (because of limits) or min_split was increased ↑ (in current or prev blocks)
                   depth < wc_info.max_split() &&                       // max_split allows split
                   depth < 60                                           // hardcoded max max split allows split
                {
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
                        if info.is_fsm_none() &&                                // split/merge is not in progress
                           depth > wc_info.min_split() &&                       // current min_split allows merge
                          (info.want_merge || depth > wc_info.max_split()) &&   // shard wants merge (because of limits) or max_split was decreased ↓ (in current or prev blocks)
                          !sibling.before_split && sibling.is_fsm_none() &&     // sibling shard is not going to split/merge now
                          (sibling.want_merge || depth > wc_info.max_split())   // sibling shard want merge or need merge (because of max_split)
                        {
                            // prepare merge
                            info.split_merge_at = FutureSplitMerge::Merge {
                                merge_utime: now + SPLIT_MERGE_DELAY,
                                interval: SPLIT_MERGE_INTERVAL,
                            };
                            changed = true;
                            log::debug!("{}: preparing to merge shard {} with {} during {}..{}",
                                self.collated_block_descr, shard, shard.sibling(), info.fsm_utime(),
                                info.fsm_utime_end());

                        } else if info.is_fsm_merge() &&                                               // merge is in progress
                             depth > wc_info.min_split() &&                                            // min_split allows merge
                            !sibling.before_split &&                                                   // sibling is not going to split
                             sibling.is_fsm_merge() &&                                                 // sibling is in merge progress too
                             now >= info.fsm_utime() && now >= sibling.fsm_utime() &&                  // merge time has come
                            (depth > wc_info.max_split() || (info.want_merge && sibling.want_merge))   // max_split was decreased or both shardes want merge
                        {
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
            if let Some((found_key, mut stat)) = stat {
                let res = self.creator_count_outdated(
                    &found_key,
                    collator_data.gen_utime(),
                    &mut stat
                )?;
                if !res {
                    log::trace!("{}: prunning CreatorStats for {:x}", self.collated_block_descr, found_key);
                    block_create_stats.counters.remove(&found_key)?;
                    removed += 1;
                } 
                scanned += 1;
                key = found_key;
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

    fn init_timeout(&mut self, timeout_ms: u32) {
        self.started = Instant::now();
        self.cutoff_timeout = Duration::from_millis(timeout_ms as u64);

        let stop_timeout = timeout_ms * 15 / 10;
        let stop_flag = self.stop_flag.clone();
        tokio::spawn(async move {
            futures_timer::Delay::new(Duration::from_millis(stop_timeout as u64)).await;
            stop_flag.store(true, Ordering::Relaxed);
        });
    }

    fn check_cutoff_timeout(&self) -> bool {
        log::debug!("check_cutoff_timeout {} {}", self.started.elapsed().as_millis(), self.cutoff_timeout.as_millis());
        self.started.elapsed() > self.cutoff_timeout
    }

    fn check_stop_flag(&self) -> Result<()> {
        if self.stop_flag.load(Ordering::Relaxed) {
            fail!("Stop flag was set")
        }
        Ok(())
    }
}

#[test]
fn test_count_bits_u64() {
    fn count_bits(mut value: u64) -> usize {
        let mut result = 0;
        while value > 0 {
            result += (value & 1) as usize;
            value >>= 1;
        }
        result
    }
    let test_cases = vec![
        0,
        1,
        2,
        3,
        0b101011_10110111, 0b01111010_11101101_11101011_10110111u64,
        0b01111010_11101101_11101011_10110111_01111010_11101101_11101011_10110111u64,
        0xFFFFFFFFFFFFFFFF
    ];

    for test_case in test_cases {
        assert_eq!(CollatorData::count_bits_u64(test_case), count_bits(test_case), "test case: {}", test_case);
    }
}

#[cfg(feature = "metrics")]
pub fn report_collation_metrics(
    shard: &ShardIdent,
    dequeue_msg_count: usize,
    enqueue_msg_count: usize,
    in_msg_count: usize,
    out_msg_count: usize,
    transit_msg_count: usize,
    executed_trs_count: usize,
    gas_used: u32,
    gas_rate: u32,
    block_size: usize,
    time: u32,
) {
    let mut pipeline = STATSD.pipeline();

    pipeline.timer(&format!("collation_time_{}", shard), time as f64);
    pipeline.gauge(&format!("dequeue_msg_count_{}", shard), dequeue_msg_count as f64);
    pipeline.gauge(&format!("enqueue_msg_count_{}", shard), enqueue_msg_count as f64);
    pipeline.gauge(&format!("in_msg_count_{}", shard), in_msg_count as f64);
    pipeline.gauge(&format!("out_msg_count_{}", shard), out_msg_count as f64);
    pipeline.gauge(&format!("transit_msg_count_{}", shard), transit_msg_count as f64);
    pipeline.gauge(&format!("executed_trs_count_{}", shard), executed_trs_count as f64);
    pipeline.gauge(&format!("gas_used_{}", shard), gas_used as f64);
    pipeline.gauge(&format!("gas_rate_collator_{}", shard), gas_rate as f64);
    pipeline.gauge(&format!("block_size_{}", shard), block_size as f64);

    pipeline.send(&STATSD);
}

