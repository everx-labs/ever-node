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

use crate::{
    CHECK,
    engine_traits::EngineOperations,
    shard_state::{ShardHashesStuff, ShardStateStuff},
    types::messages::MsgEnqueueStuff,
};
use std::{
    cmp::max, iter::Iterator, sync::{Arc, atomic::{AtomicBool, Ordering}},
    collections::HashMap,
};
use ton_block::{
    BlockIdExt, ShardIdent, Serializable, Deserializable,
    OutMsgQueueInfo, OutMsgQueue, OutMsgQueueKey, IhrPendingInfo,
    ProcessedInfo, ProcessedUpto, ProcessedInfoKey,
    ShardHashes, AccountIdPrefixFull,
    HashmapAugType,
};
use ton_types::{
    error, fail,
    BuilderData, Cell, SliceData, IBitstring, Result, UInt256,
    HashmapSubtree, HashmapType, HashmapFilterResult, HashmapRemover,
};


#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct ProcessedUptoStuff {
    pub shard: u64,
    pub mc_seqno: u32,
    pub last_msg_lt: u64,
    pub last_msg_hash: UInt256,
    mc_end_lt: u64,
    ref_shards: Option<ShardHashes>,
}

impl ProcessedUptoStuff {
    pub fn with_params(shard: u64, mc_seqno: u32, last_msg_lt: u64, last_msg_hash: UInt256) -> Self {
        Self {
            shard,
            mc_seqno,
            last_msg_lt,
            last_msg_hash,
            mc_end_lt: 0,
            ref_shards: None,
        }
    }
    pub fn new(key: ProcessedInfoKey, value: ProcessedUpto) -> Self {
        Self::with_params(key.shard, key.mc_seqno, value.last_msg_lt, value.last_msg_hash)
    }
    pub fn contains(&self, other: &Self) -> bool {
        ShardIdent::is_ancestor(self.shard, other.shard)
            && self.mc_seqno >= other.mc_seqno
            && ((self.last_msg_lt > other.last_msg_lt)
            || ((self.last_msg_lt == other.last_msg_lt) && (self.last_msg_hash >= other.last_msg_hash))
        )
    }
    pub fn can_check_processed(&self) -> bool {
        self.ref_shards.is_some()
    }
    fn already_processed(&self, enq: &MsgEnqueueStuff) -> Result<bool> {
        if enq.created_lt() > self.last_msg_lt {
            return Ok(false)
        }
        if !ShardIdent::contains(self.shard, enq.next_prefix().prefix) {
            return Ok(false)
        }
        if enq.created_lt() == self.last_msg_lt && self.last_msg_hash < enq.message_hash() {
            return Ok(false)
        }
        if enq.same_workchain() && ShardIdent::contains(self.shard, enq.cur_prefix().prefix) {
            // this branch is needed only for messages generated in the same shard
            // (such messages could have been processed without a reference from the masterchain)
            // enable this branch only if an extra boolean parameter is set
            return Ok(true)
        }
        let shard_end_lt = self.compute_shard_end_lt(&enq.cur_prefix())?;
        Ok(enq.enqueued_lt() < shard_end_lt)
    }
    pub fn compute_shard_end_lt(&self, prefix: &AccountIdPrefixFull) -> Result<u64> {
        let shard_end_lt = if prefix.is_masterchain() {
            self.mc_end_lt
        } else  {
            self.ref_shards.as_ref()
                .ok_or_else(|| error!("PrcessedUpTo record for {} ({}:{:x}) have not got info about shards",
                    self.mc_seqno, self.last_msg_lt, self.last_msg_hash))?
                .find_shard_by_prefix(&prefix)?
                .ok_or_else(|| error!("PrcessedUpTo record for {} ({}:{:x}) have not got info about shard prefix {}",
                    self.mc_seqno, self.last_msg_lt, self.last_msg_hash, prefix))?
                .descr().end_lt
        };
        Ok(shard_end_lt)
    }
}

impl std::fmt::Display for ProcessedUptoStuff {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "shard: {:016X}, mc_seqno: {}, mc_end_lt: {}, last_msg_lt: {}, last_msg_hash: {:x}",
            self.shard, self.mc_seqno, self.mc_end_lt, self.last_msg_lt, self.last_msg_hash)
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct OutMsgQueueInfoStuff {
    block_id: BlockIdExt,
    out_queue: OutMsgQueue,
    ihr_pending: IhrPendingInfo,
    entries: Vec<ProcessedUptoStuff>,
    min_seqno: u32,
    end_lt: u64,
    disabled: bool,
}

impl OutMsgQueueInfoStuff {
    pub fn from_shard_state(state: &ShardStateStuff) -> Result<Self> {
        let out_queue_info = state.state().read_out_msg_queue_info()?;
        Self::from_out_queue_info(state.block_id().clone(), out_queue_info, state.state().gen_lt())
    }
    fn from_out_queue_info(block_id: BlockIdExt, out_queue_info: OutMsgQueueInfo, end_lt: u64) -> Result<Self> {
        // TODO: comment the next line in the future when the output queues become huge
        // (do this carefully)
        // out_queue_info.out_queue().count_cells(1000000)?;
        let mut out_queue = out_queue_info.out_queue().clone();


        // Due to the lack of necessary checks shardstate already has an internal message with anycast info.
        // Due to anycast info the message was added into wrong shardstate's subtree.
        // Need to delete the message.
        // Needed checks were added, so this code is only a single patch which might be deleted later.
        if block_id.seq_no == 20094516 && block_id.shard().shard_prefix_with_tag() == 0x5800000000000000u64 {
            let key = OutMsgQueueKey::with_workchain_id_and_prefix(
                0, 
                0x5777784F96FB1CFFu64,
                UInt256::from_str("05aa297e3a2e003e1449e1297742d64f188985dc029c620edc84264f9786c0c3").unwrap()
            );
            out_queue.remove(key.serialize()?.into()).unwrap();
        }


        let ihr_pending = out_queue_info.ihr_pending().clone();
        // unpack ProcessedUptoStuff
        let mut entries = vec![];
        let mut min_seqno = std::u32::MAX;
        out_queue_info.proc_info().iterate_slices_with_keys(|ref mut key, ref mut value| {
            let key = ProcessedInfoKey::construct_from(key)?;
            let value = ProcessedUpto::construct_from(value)?;
            let entry = ProcessedUptoStuff::new(key, value);
            if entry.mc_seqno < min_seqno {
                min_seqno = entry.mc_seqno;
            }
            entries.push(entry);
            Ok(true)
        })?;
        Ok(Self {
            block_id,
            out_queue,
            ihr_pending,
            entries,
            min_seqno,
            end_lt,
            disabled: false,
        })
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        let shard = self.shard().merge()?;

        self.out_queue.combine_with(&other.out_queue)?;
        self.out_queue.update_root_extra()?;
        self.ihr_pending.merge(&other.ihr_pending, &shard.shard_key(false))?;
        for entry in &other.entries {
            if self.min_seqno > entry.mc_seqno {
                self.min_seqno = entry.mc_seqno;
            }
            self.entries.push(entry.clone());
        }
        self.block_id = BlockIdExt::with_params(
            shard, 
            max(self.block_id.seq_no, other.block_id.seq_no),
            UInt256::default(),
            UInt256::default()
        );
        self.compactify()?;
        Ok(())
    }

    fn split(&mut self, subshard: ShardIdent) -> Result<Self> {
        let sibling = subshard.sibling();
        let mut out_queue = OutMsgQueue::default();
        self.out_queue.hashmap_filter(|key, value| {
            let mut slice = value.clone();
            let lt = u64::construct_from(&mut slice)?;
            let enq = MsgEnqueueStuff::construct_from(&mut slice, lt)?;
            if !subshard.contains_full_prefix(enq.cur_prefix()) {
                out_queue.set_builder_serialized(key.clone().into_cell()?.into(), &BuilderData::from_slice(&value), &lt)?;
                Ok(HashmapFilterResult::Remove)
            } else {
                Ok(HashmapFilterResult::Accept)
            }
        })?;
        self.out_queue.update_root_extra()?;
        let mut ihr_pending = self.ihr_pending.clone();
        self.ihr_pending.split_inplace(&subshard.shard_key(false))?;
        ihr_pending.split_inplace(&sibling.shard_key(false))?;

        let mut entries = vec![];
        let mut min_seqno = std::u32::MAX;
        self.min_seqno = min_seqno;
        for mut entry in std::mem::take(&mut self.entries).drain(..) {
            if ShardIdent::shard_intersects(entry.shard, sibling.shard_prefix_with_tag()) {
                let mut entry = entry.clone();
                entry.shard = ShardIdent::shard_intersection(entry.shard, sibling.shard_prefix_with_tag());
                log::debug!("to sibling {}", entry);
                if min_seqno > entry.mc_seqno {
                    min_seqno = entry.mc_seqno;
                }
                entries.push(entry);
            }
            if ShardIdent::shard_intersects(entry.shard, subshard.shard_prefix_with_tag()) {
                entry.shard = ShardIdent::shard_intersection(entry.shard, subshard.shard_prefix_with_tag());
                log::debug!("to us {}", entry);
                if self.min_seqno > entry.mc_seqno {
                    self.min_seqno = entry.mc_seqno;
                }
                self.entries.push(entry);
            }
        }
        self.compactify()?;
        self.block_id.shard_id = subshard;

        let block_id = BlockIdExt::with_params(
            sibling, 
            self.block_id().seq_no,
            UInt256::default(),
            UInt256::default()
        );
        let mut sibling = OutMsgQueueInfoStuff {
            block_id,
            out_queue,
            ihr_pending,
            entries,
            min_seqno,
            end_lt: self.end_lt,
            disabled: false,
        };
        sibling.compactify()?;
        Ok(sibling)
    }

    pub fn serialize(&self) -> Result<(OutMsgQueueInfo, u32)> {
        let mut min_seqno = std::u32::MAX;
        let mut proc_info = ProcessedInfo::default();
        for entry in &self.entries {
            min_seqno = std::cmp::min(min_seqno, entry.mc_seqno);
            let key = ProcessedInfoKey::with_params(entry.shard, entry.mc_seqno);
            let value = ProcessedUpto::with_params(entry.last_msg_lt, entry.last_msg_hash.clone());
            proc_info.set(&key, &value)?
        }
        Ok((OutMsgQueueInfo::with_params(self.out_queue.clone(), proc_info, self.ihr_pending.clone()), min_seqno))
    }

    pub fn fix_processed_upto(
        &mut self,
        cur_mc_seqno: u32,
        next_mc_end_lt: u64,
        next_shards: Option<&ShardHashes>,
        mc_shard_states: &HashMap<u32, Arc<ShardStateStuff>>,
        stop_flag: &Option<&AtomicBool>,
    ) -> Result<()> {
        let masterchain = self.shard().is_masterchain();
        for mut entry in &mut self.entries {
            if entry.ref_shards.is_none() {
                check_stop_flag(stop_flag)?;
                let mc_seqno = std::cmp::min(entry.mc_seqno, cur_mc_seqno);
                if next_shards.is_some() && masterchain && entry.mc_seqno == cur_mc_seqno + 1 {
                    entry.mc_end_lt = next_mc_end_lt;
                    entry.ref_shards = next_shards.cloned();
                } else {
                    let state = mc_shard_states.get(&mc_seqno)
                        .ok_or_else(|| error!("mastechain state for block {} was not previously cached", mc_seqno))?;
                    entry.mc_end_lt = state.state().gen_lt();
                    entry.ref_shards = Some(state.shards()?.clone());
                };
            }
        }
        Ok(())
    }
    pub fn block_id(&self) -> &BlockIdExt { &self.block_id }

    pub fn shard(&self) -> &ShardIdent { self.block_id.shard() }

    fn set_shard(&mut self, shard_ident: ShardIdent) { self.block_id.shard_id = shard_ident }

    fn disable(&mut self) { self.disabled = true }

    pub fn is_disabled(&self) -> bool { self.disabled }

    pub fn out_queue(&self) -> &OutMsgQueue { &self.out_queue }

    pub fn forced_fix_out_queue(&mut self) -> Result<()> {
        if self.out_queue.is_empty() && self.out_queue.root_extra() != &0 {
            self.out_queue.after_remove()?;
        }
        Ok(())
    }

    pub fn message(&self, key: &OutMsgQueueKey) -> Result<Option<MsgEnqueueStuff>> {
        self.out_queue.get_with_aug(&key)?.map(|(enq, lt)| MsgEnqueueStuff::from_enqueue_and_lt(enq, lt)).transpose()
    }

    pub fn add_message(&mut self, enq: &MsgEnqueueStuff) -> Result<()> {
        let key = enq.out_msg_key();
        self.out_queue.set(&key, enq.enqueued(), &enq.created_lt())
    }

    pub fn del_message(&mut self, key: &OutMsgQueueKey) -> Result<()> {
        if self.out_queue.remove(key.serialize()?.into())?.is_none() {
            fail!("error deleting from out_msg_queue dictionary: {:x}", key.hash)
        }
        Ok(())
    }

    // remove all messages which are not from new_shard
    fn filter_messages(&mut self, new_shard: &ShardIdent, shard_prefix: &SliceData) -> Result<()> {
        let old_shard = self.shard().clone();
        self.out_queue.into_subtree_with_prefix(&shard_prefix, &mut 0)?;
        self.out_queue.hashmap_filter(|_key, mut slice| {
            // log::debug!("scanning OutMsgQueue entry with key {:x}", key);
            let lt = u64::construct_from(&mut slice)?;
            let enq = MsgEnqueueStuff::construct_from(&mut slice, lt)?;
            if !old_shard.contains_full_prefix(&enq.cur_prefix()) {
                fail!("OutMsgQueue message with key {:x} does not contain current \
                    address belonging to shard {}", enq.out_msg_key(), old_shard)
            }
            match new_shard.contains_full_prefix(&enq.cur_prefix()) {
                true => Ok(HashmapFilterResult::Accept),
                false => Ok(HashmapFilterResult::Remove)
            }
        })
    }

    pub fn end_lt(&self) -> u64 { self.end_lt }
    pub fn can_check_processed(&self) -> bool {
        for entry in &self.entries {
            if !entry.can_check_processed() {
                return false
            }
        }
        true
    }
    pub fn add_processed_upto(&mut self, mc_seqno: u32, last_msg_lt: u64, last_msg_hash: UInt256) -> Result<()> {
        let entry = ProcessedUptoStuff {
            shard: self.shard().shard_prefix_with_tag(),
            mc_seqno,
            last_msg_lt,
            last_msg_hash,
            mc_end_lt: 0,
            ref_shards: None
        };
        self.entries.push(entry);
        self.compactify()?;
        Ok(())
    }
    pub fn entries(&self) -> &Vec<ProcessedUptoStuff> { &self.entries }
    pub fn min_seqno(&self) -> u32 { self.min_seqno }
    pub fn already_processed(&self, enq: &MsgEnqueueStuff) -> Result<bool> {
        if self.shard().contains_full_prefix(&enq.next_prefix()) {
            for entry in &self.entries {
                if entry.already_processed(enq)? {
                    return Ok(true)
                }
            }
        }
        Ok(false)
    }
    pub fn compactify(&mut self) -> Result<bool> {
        Self::compactify_entries(&mut self.entries)
    }
    fn compactify_entries(entries: &mut Vec<ProcessedUptoStuff>) -> Result<bool> {
        let n = entries.len();
        let mut mark = Vec::new();
        mark.resize(n, false);
        let mut found = false;
        for i in 0..n {
            for j in 0..n {
                if i != j && !mark[j] && entries[j].contains(&entries[i]) {
                    mark[i] = true;
                    found = true;
                    break;
                }
            }
        }
        if found {
            for i in (0..n).rev() {
                if mark[i] {
                    entries.remove(i);
                }
            }
        }
        Ok(found)
    }
    pub fn is_reduced(&self) -> bool {
        Self::is_reduced_entries(&self.entries)
    }
    fn is_reduced_entries(entries: &Vec<ProcessedUptoStuff>) -> bool {
        let n = entries.len();
        for i in 1..n {
            for j in 0..i {
                if entries[i].contains(&entries[j]) || entries[j].contains(&entries[i]) {
                    return false
                }
            }
        }
        true
    }
    pub fn contains(&self, other: &Self) -> bool {
        for entry in &other.entries {
            if !self.contains_value(entry) {
                return false
            }
        }
        true
    }
    pub fn contains_value(&self, value: &ProcessedUptoStuff) -> bool {
        for entry in &self.entries {
            if entry.contains(value) {
                return true
            }
        }
        false
    }
    pub fn is_simple_update_of(&self, other: &Self) -> (bool, Option<ProcessedUptoStuff>) {
        if !self.contains(other) {
            log::debug!("Does not cointain the previous value");
            return (false, None)
        }

        if other.contains(self) {
            log::debug!("Coincides with the previous value");
            return (true, None)
        }

        let mut found = None;
        for entry in &self.entries {
            if !other.contains_value(entry) {
                if found.is_some() {
                    log::debug!("Has more than two new entries");
                    return (false, found)  // ok = false: update is not simple
                }
                found = Some(entry.clone());
            }
        }
        (true, found)
    }
}

pub struct MsgQueueManager {
// Unused
//    shard: ShardIdent,
    prev_out_queue_info: OutMsgQueueInfoStuff,
    next_out_queue_info: OutMsgQueueInfoStuff,
    neighbors: Vec<OutMsgQueueInfoStuff>,
}

impl MsgQueueManager {

    pub async fn init(
        engine: &Arc<dyn EngineOperations>,
        last_mc_state: &Arc<ShardStateStuff>,
        shard: ShardIdent,
        shards: &ShardHashes,
        prev_states: &Vec<Arc<ShardStateStuff>>,
        next_state_opt: Option<&Arc<ShardStateStuff>>,
        after_merge: bool,
        after_split: bool,
        stop_flag: Option<&AtomicBool>,
    ) -> Result<Self> {
        let mut mc_shard_states = HashMap::new();
        mc_shard_states.insert(last_mc_state.block_id().seq_no, last_mc_state.clone());
        let next_mc_end_lt = match next_state_opt {
            Some(state) => {
                if state.shard().is_masterchain() {
                    mc_shard_states.insert(state.block_id().seq_no(), state.clone());
                }
                state.state().gen_lt()
            }
            None => 0
        };
        log::debug!("request a preliminary list of neighbors for {}", shard);
        let shards = ShardHashesStuff::from(shards.clone());
        let (_master, workchain_id) = engine.processed_workchain().await?;
        let neighbor_list = shards.neighbours_for(&shard, workchain_id)?;
        let mut neighbors = vec![];
        log::debug!("got a preliminary list of {} neighbors for {}", neighbor_list.len(), shard);
        for (i, shard) in neighbor_list.iter().enumerate() {
            log::debug!("neighbors #{} ---> {:#}", i + 1, shard.shard());
            
            // TODO add loop and stop_flag checking
            let shard_state = engine.clone().wait_state(shard.block_id(), Some(1_000), true).await?;
            
            let nb = Self::load_out_queue_info(engine, &shard_state, &last_mc_state, &mut mc_shard_states).await?;
            neighbors.push(nb);
            check_stop_flag(&stop_flag)?;
        }
        let mc_seqno = last_mc_state.block_id().seq_no();
        if shards.is_empty() || mc_seqno != 0 {
            let nb = Self::load_out_queue_info(engine, &last_mc_state, &last_mc_state, &mut mc_shard_states).await?;
            neighbors.push(nb);
        }
        let mut next_out_queue_info;
        let mut prev_out_queue_info = Self::load_out_queue_info(engine, &prev_states[0], &last_mc_state, &mut mc_shard_states).await?;
        if prev_out_queue_info.block_id().seq_no != 0 {
            if let Some(state) = prev_states.get(1) {
                CHECK!(after_merge);
                let merge_out_queue_info = Self::load_out_queue_info(engine, state, &last_mc_state, &mut mc_shard_states).await?;
                log::debug!("prepare merge for states {} and {}", prev_out_queue_info.block_id(), merge_out_queue_info.block_id());
                prev_out_queue_info.merge(&merge_out_queue_info)?;
                Self::add_trivial_neighbor_after_merge(&mut neighbors, &shard, &prev_out_queue_info, prev_states, &stop_flag)?;
                next_out_queue_info = match next_state_opt {
                    Some(next_state) => Self::load_out_queue_info(engine, next_state, &last_mc_state, &mut mc_shard_states).await?,
                    None => prev_out_queue_info.clone()
                };
            } else if after_split {
                log::debug!("prepare split for state {}", prev_out_queue_info.block_id());
                let sibling_out_queue_info = prev_out_queue_info.split(shard.clone())?;
                Self::add_trivial_neighbor(&mut neighbors, &shard, &prev_out_queue_info, 
                    Some(sibling_out_queue_info), prev_states[0].shard(), &stop_flag)?;
                next_out_queue_info = match next_state_opt {
                    Some(next_state) => Self::load_out_queue_info(engine, next_state, &last_mc_state, &mut mc_shard_states).await?,
                    None => prev_out_queue_info.clone()
                };
            } else {
                Self::add_trivial_neighbor(&mut neighbors, &shard, &prev_out_queue_info, None, 
                    prev_out_queue_info.shard(), &stop_flag)?;
                next_out_queue_info = match next_state_opt {
                    Some(next_state) => Self::load_out_queue_info(engine, next_state, &last_mc_state, &mut mc_shard_states).await?,
                    None => prev_out_queue_info.clone()
                };
            }
        } else {
            next_out_queue_info = match next_state_opt {
                Some(next_state) => Self::load_out_queue_info(engine, next_state, &last_mc_state, &mut mc_shard_states).await?,
                None => prev_out_queue_info.clone()
            };
        }

        prev_out_queue_info.fix_processed_upto(mc_seqno, 0, None, &mc_shard_states, &stop_flag)?;
        next_out_queue_info.fix_processed_upto(mc_seqno, next_mc_end_lt, Some(shards.as_ref()), &mc_shard_states, &stop_flag)?;

        for neighbor in &mut neighbors {
            neighbor.fix_processed_upto(mc_seqno, 0, None, &mc_shard_states, &stop_flag)?;
        }
        Ok(MsgQueueManager {
        //Unused
          // shard,
            prev_out_queue_info,
            next_out_queue_info,
            neighbors,
        })
    }

    pub async fn load_out_queue_info(
        engine: &Arc<dyn EngineOperations>,
        state: &Arc<ShardStateStuff>,
        last_mc_state: &Arc<ShardStateStuff>,
        mc_shard_states: &mut HashMap<u32, Arc<ShardStateStuff>>
    ) -> Result<OutMsgQueueInfoStuff> {
        log::debug!("unpacking OutMsgQueueInfo of neighbor {:#}", state.block_id());
        let nb = OutMsgQueueInfoStuff::from_shard_state(&state)?;
        // if (verbosity >= 2) {
        //     block::gen::t_ProcessedInfo.print(std::cerr, qinfo.proc_info);
        //     qinfo.proc_info->print_rec(std::cerr);
        // }
        // require masterchain blocks referred to in ProcessedUpto
        // TODO: perform this only if there are messages for this shard in our output queue
        // .. (have to check the above condition and perform a `break` here) ..
        // ..
        for entry in nb.entries() {

            // TODO add loop and stop_flag checking
            Self::request_mc_state(engine, last_mc_state, entry.mc_seqno,
                Some(10_000), mc_shard_states).await?;
        }
        Ok(nb)
    }

    fn already_processed(&self, enq: &MsgEnqueueStuff) -> Result<(bool, u64)> {
        for neighbor in &self.neighbors {
            if !neighbor.is_disabled() && neighbor.already_processed(&enq)? {
                return Ok((true, neighbor.end_lt()))
            }
        }
        Ok((false, 0))
    }

    fn add_trivial_neighbor_after_merge(
        neighbors: &mut Vec<OutMsgQueueInfoStuff>,
        shard: &ShardIdent,
        real_out_queue_info: &OutMsgQueueInfoStuff,
        prev_states: &Vec<Arc<ShardStateStuff>>,
        stop_flag: &Option<&AtomicBool>,
    ) -> Result<()> {
        log::debug!("in add_trivial_neighbor_after_merge()");
        CHECK!(prev_states.len(), 2);
        let mut found = 0;
        let n = neighbors.len();
        for i in 0..n {
            if shard.intersect_with(neighbors[i].shard()) {
                let nb = &neighbors[i];
                found += 1;
                log::debug!("neighbor #{} : {} intersects our shard {}", i, nb.block_id(), shard);
                if !shard.is_parent_for(nb.shard()) || found > 2 {
                    fail!("impossible shard configuration in add_trivial_neighbor_after_merge()")
                }
                let prev_shard = prev_states[found - 1].shard();
                if nb.shard() != prev_shard {
                    fail!("neighbor shard {} does not match that of our ancestor {}",
                        nb.shard(), prev_shard)
                }
                if found == 1 {
                    neighbors[i] = real_out_queue_info.clone();
                    log::debug!("adjusted neighbor #{} : {} with shard expansion \
                        (immediate after-merge adjustment)", i, neighbors[i].block_id());
                } else {
                    neighbors[i].disable();
                    log::debug!("disabling neighbor #{} : {} \
                        (immediate after-merge adjustment)", i, neighbors[i].block_id());
                }

                check_stop_flag(stop_flag)?;
            }
        }
        CHECK!(found == 2);
        Ok(())
    }

    fn add_trivial_neighbor(
        neighbors: &mut Vec<OutMsgQueueInfoStuff>,
        shard: &ShardIdent,
        real_out_queue_info: &OutMsgQueueInfoStuff,
        sibling_out_queue_info: Option<OutMsgQueueInfoStuff>,
        prev_shard: &ShardIdent,
        stop_flag: &Option<&AtomicBool>,
    ) -> Result<()> {
        log::debug!("in add_trivial_neighbor()");
        // Possible cases are:
        // 1. prev_shard = shard = one of neighbors
        //    => replace neighbor by (more recent) prev_shard info
        // 2. shard is child of prev_shard = one of neighbors
        //    => after_split must be set;
        //       replace neighbor by new split data (and shrink its shard);
        //       insert new virtual neighbor (our future sibling).
        // 3. prev_shard = shard = child of one of neighbors
        //    => after_split must be clear (we are continuing an after-split chain);
        //       make our virtual sibling from the neighbor (split its queue);
        //       insert ourselves from prev_shard data
        // In all of the above cases, our shard intersects exactly one neighbor, which has the same shard or its parent.
        // 4. there are two neighbors intersecting shard = prev_shard, which are its children.
        // 5. there are two prev_shards, the two children of shard, and two neighbors coinciding with prev_shards
        let mut found = 0;
        let mut cs = 0;
        let n = neighbors.len();
        for i in 0..n {
            if shard.intersect_with(neighbors[i].shard()) {
                let nb = &neighbors[i];
                found += 1;
                log::debug!("neighbor #{} : {} intersects our shard {}", i, nb.block_id(), shard);
                if nb.shard() == prev_shard {
                    if prev_shard == shard {
                        // case 1. Normal.
                        CHECK!(found == 1);
                        neighbors[i] = real_out_queue_info.clone();
                        log::debug!("adjusted neighbor #{} : {} (simple replacement)", i, neighbors[i].block_id());
                        cs = 1;
                    } else if nb.shard().is_parent_for(&shard) {
                        // case 2. Immediate after-split.
                        CHECK!(found == 1);
                        CHECK!(sibling_out_queue_info.is_some());
                        if let Some(ref sibling) = sibling_out_queue_info {
                            neighbors[i] = sibling.clone();
                            neighbors[i].set_shard(shard.clone());
                        }
                        log::debug!("adjusted neighbor #{} : {} with shard \
                            shrinking to our sibling (immediate after-split adjustment)", i, neighbors[i].block_id());

                        let nb = real_out_queue_info.clone();
                        log::debug!("created neighbor #{} : {} with shard \
                            shrinking to our (immediate after-split adjustment)", n, nb.block_id());
                        neighbors.push(nb);
                        cs = 2;
                    } else {
                        fail!("impossible shard configuration in add_trivial_neighbor()")
                    }
                } else if nb.shard().is_parent_for(shard) && shard == prev_shard {
                    // case 3. Continued after-split
                    CHECK!(found == 1);
                    CHECK!(sibling_out_queue_info.is_none());

                    // compute the part of virtual sibling's OutMsgQueue with destinations in our shard
                    let sib_shard = shard.sibling();
                    let shard_prefix = shard.shard_key(true);
                    neighbors[i].filter_messages(&sib_shard, &shard_prefix)
                        .map_err(|err| error!("cannot filter virtual sibling's OutMsgQueue from that of \
                            the last common ancestor: {}", err))?;
                    neighbors[i].set_shard(sib_shard);
                    log::debug!("adjusted neighbor #{} : {} with shard shrinking \
                        to our sibling (continued after-split adjustment)", i, neighbors[i].block_id());

                    let nb = real_out_queue_info.clone();
                    log::debug!("created neighbor #{} : {} from our preceding state \
                        (continued after-split adjustment)", n, nb.block_id());
                    neighbors.push(nb);
                    cs = 3;
                } else if shard.is_parent_for(nb.shard()) && shard == prev_shard {
                    // case 4. Continued after-merge.
                    if found == 1 {
                        cs = 4;
                    }
                    CHECK!(cs == 4);
                    CHECK!(found <= 2);
                    if found == 1 {
                        neighbors[i] = real_out_queue_info.clone();
                        log::debug!("adjusted neighbor #{} : {} with shard expansion \
                            (continued after-merge adjustment)", i, neighbors[i].block_id());
                    } else {
                        neighbors[i].disable();
                        log::debug!("disabling neighbor #{} : {} (continued after-merge adjustment)",
                            i, neighbors[i].block_id());
                    }
                } else {
                    fail!("impossible shard configuration in add_trivial_neighbor()")
                }

                check_stop_flag(stop_flag)?;
            }
        }
        // dbg!(found, cs);
        CHECK!(found != 0 && cs != 0);
        CHECK!(found == (1 + (cs == 4) as usize));
        Ok(())
    }

    pub fn clean_out_msg_queue(
        &mut self, 
        mut on_message: impl FnMut(Option<(MsgEnqueueStuff, u64)>, Option<&Cell>) -> Result<bool>
    ) -> Result<bool> {
        log::debug!("in clean_out_msg_queue: cleaning output messages imported by neighbors");
        if self.next_out_queue_info.out_queue.is_empty() {
            return Ok(false)
        }
        // if (verbosity >= 2) {
        //     auto rt = out_msg_queue_->get_root();
        //     std::cerr << "old out_msg_queue is ";
        //     block::gen::t_OutMsgQueue.print(std::cerr, *rt);
        //     rt->print_rec(std::cerr);
        // }
        for neighbor in self.neighbors.iter() {
            if !neighbor.is_disabled() && !neighbor.can_check_processed() {
                fail!(
                    "Internal error: no info for checking processed messages from neighbor {}",
                    neighbor.block_id()
                )
            }
        }
        let mut block_full = false;
        let mut partial = false;
        let mut queue = self.next_out_queue_info.out_queue.clone();
        let mut skipped = 0;
        let mut deleted = 0;
        queue.hashmap_filter(|_key, mut slice| {
            if block_full {
                log::warn!("BLOCK FULL when cleaning output queue, cleanup is partial");
                partial = true;
                return Ok(HashmapFilterResult::Stop)
            }
            let lt = u64::construct_from(&mut slice)?;
            let enq = MsgEnqueueStuff::construct_from(&mut slice, lt)?;
            log::debug!("Scanning outbound {}", enq);
            let (processed, end_lt) = self.already_processed(&enq)?;
            if processed {
                log::debug!("Outbound {} has beed already delivered, dequeueing", enq);
                block_full = on_message(Some((enq, end_lt)), self.next_out_queue_info.out_queue.data())?;
                deleted += 1;
                return Ok(HashmapFilterResult::Remove)
            }
            skipped += 1;
            Ok(HashmapFilterResult::Accept)
        })?;
        log::debug!("Deleted {} messages from out_msg_queue, skipped {}", deleted, skipped);
        self.next_out_queue_info.out_queue = queue;
        // if (verbosity >= 2) {
        //     std::cerr << "new out_msg_queue is ";
        //     block::gen::t_OutMsgQueue.print(std::cerr, *rt);
        //     rt->print_rec(std::cerr);
        // }
        // CHECK(block::gen::t_OutMsgQueue.validate_upto(100000, *rt));  // DEBUG, comment later if SLOW
        on_message(None, self.next_out_queue_info.out_queue.data())?;
        Ok(partial)
    }

    async fn request_mc_state(
        engine: &Arc<dyn EngineOperations>,
        last_mc_state: &Arc<ShardStateStuff>,
        seq_no: u32,
        timeout_ms: Option<u64>,
        mc_shard_states: &mut HashMap<u32, Arc<ShardStateStuff>>,
    ) -> Result<()> {
        if !mc_shard_states.contains_key(&seq_no) {
            let last_mc_seqno = last_mc_state.state().seq_no();
            if seq_no >= last_mc_seqno {
                fail!("Requested too new master chain state {}, last is {}", seq_no, last_mc_seqno);
            }
            let block_id = match last_mc_state.shard_state_extra()?.prev_blocks.get(&seq_no) {
                Ok(Some(result)) => result.master_block_id().1,
                _ => fail!("cannot find previous masterchain block with seqno {} \
                    to load corresponding state as required", seq_no)
            };
            mc_shard_states.insert(
                seq_no,
                engine.clone().wait_state(&block_id, timeout_ms, true).await?
            );
        }
        Ok(())
    }
}

impl MsgQueueManager {
    /// create iterator for merging all output messages from all neighbors to our shard
    pub fn merge_out_queue_iter(&self, shard: &ShardIdent) -> Result<MsgQueueMergerIterator> {
        MsgQueueMergerIterator::from_manager(self, shard)
    }
    /// find enquque message and return it with neighbor id 
    pub fn find_message(&self, key: &OutMsgQueueKey, prefix: &AccountIdPrefixFull) -> Result<(Option<BlockIdExt>, Option<MsgEnqueueStuff>)> {
        for nb in &self.neighbors {
            if !nb.is_disabled() && nb.shard().contains_full_prefix(prefix) {
                return Ok((Some(nb.block_id().clone()), nb.message(key)?))
            }
        }
        Ok((None, None))
    }
    pub fn prev(&self) -> &OutMsgQueueInfoStuff { &self.prev_out_queue_info }
    pub fn next(&self) -> &OutMsgQueueInfoStuff { &self.next_out_queue_info }
    pub fn take_next(&mut self) -> OutMsgQueueInfoStuff { std::mem::take(&mut self.next_out_queue_info) }
// Unused
//    pub fn shard(&self) -> &ShardIdent { &self.shard }
    pub fn neighbors(&self) -> &Vec<OutMsgQueueInfoStuff> { &self.neighbors }
// Unused
//    pub fn neighbor(&self, shard: &ShardIdent) -> Option<&OutMsgQueueInfoStuff> {
//        for nb in &self.neighbors {
//            if nb.shard() == shard {
//                return Some(nb)
//            }
//        }
//        None
//    }
}

#[derive(Eq, PartialEq)]
struct RootRecord {
    lt: u64,
    cursor: SliceData,
    bit_len: usize,
    key: BuilderData,
    block_id: BlockIdExt
}

impl RootRecord {
    fn new(
        lt: u64,
        cursor: SliceData,
        bit_len: usize,
        key: BuilderData,
        block_id: BlockIdExt
    ) -> Self {
        Self {
            lt,
            cursor,
            bit_len,
            key,
            block_id
        }
    }
}

impl Ord for RootRecord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // first compare lt descending, because Vec is a stack
        let mut cmp = self.lt.cmp(&other.lt);
        if cmp == std::cmp::Ordering::Equal {
            // check if we have full key and leaf
            cmp = self.key.length_in_bits().cmp(&other.key.length_in_bits());
            // compare hashes descending, because Vec is a stack
            if cmp == std::cmp::Ordering::Equal && self.key.length_in_bits() == 352 {
                cmp = self.key.data()[12..44].cmp(&other.key.data()[12..44]);
            }
        }
        cmp.reverse()
    }
}
impl PartialOrd for RootRecord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

/// it iterates messages ascending create_lt and hash
pub struct MsgQueueMergerIterator {
    // store branches descending by lt and hash because Vec works like Stack
    roots: Vec<RootRecord>,
}

impl MsgQueueMergerIterator {
    pub fn from_manager(manager: &MsgQueueManager, shard: &ShardIdent) -> Result<Self> {
        let shard_prefix = shard.shard_key(true);
        let mut roots = vec![];
        for nb in manager.neighbors.iter().filter(|nb| !nb.is_disabled()) {
            let mut out_queue_short = nb.out_queue.clone();
            out_queue_short.into_subtree_with_prefix(&shard_prefix, &mut 0)?;
            if let Some(root) = out_queue_short.data() {
                let mut cursor = SliceData::from(root);
                let mut bit_len = out_queue_short.bit_len();
                let key = cursor.get_label_raw(&mut bit_len, BuilderData::default())?;
                let lt = cursor.get_next_u64()?;
                roots.push(RootRecord::new(lt, cursor, bit_len, key, nb.block_id().clone()));
            }
        }
        if !roots.is_empty() {
            roots.sort();
            debug_assert!(roots.first().unwrap().lt >= roots.last().unwrap().lt);
        }
        Ok(Self { roots })
    }
    fn insert(&mut self, root: RootRecord) {
        let idx = self.roots.binary_search(&root).unwrap_or_else(|x| x);
        self.roots.insert(idx, root);
        debug_assert!(self.roots.first().unwrap().lt >= self.roots.last().unwrap().lt);
    }
    fn next_item(&mut self) -> Result<Option<(OutMsgQueueKey, MsgEnqueueStuff, u64, BlockIdExt)>> {
        while let Some(mut root) = self.roots.pop() {
            if root.bit_len == 0 {
                let key = OutMsgQueueKey::construct_from_cell(root.key.into_cell()?)?;
                let enq = MsgEnqueueStuff::construct_from(&mut root.cursor, root.lt)?;
                return Ok(Some((key, enq, root.lt, root.block_id)))
            }
            for idx in 0..2 {
                let mut bit_len = root.bit_len - 1;
                let mut cursor = SliceData::from(root.cursor.reference(idx)?);
                let mut key = root.key.clone();
                key.append_bit_bool(idx == 1)?;
                key = cursor.get_label_raw(&mut bit_len, key)?;
                let lt = cursor.get_next_u64()?;
                self.insert(RootRecord::new(lt, cursor, bit_len, key, root.block_id.clone()));
            }
        }
        Ok(None)
    }
}

impl Iterator for MsgQueueMergerIterator {
    type Item = Result<(OutMsgQueueKey, MsgEnqueueStuff, u64, BlockIdExt)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_item().transpose()
    }
}

fn check_stop_flag(stop_flag: &Option<&AtomicBool>) -> Result<()> {
    if let Some(stop_flag) = stop_flag {
        if stop_flag.load(Ordering::Relaxed) {
            fail!("Stop flag was set")
        }
    }
    Ok(())
}
