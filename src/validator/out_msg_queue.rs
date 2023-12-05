/*
* Copyright (C) 2019-2023 EverX. All Rights Reserved.
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
use super::out_msg_queue_cleaner;
use std::{
    cmp::max, iter::Iterator, sync::{Arc, atomic::{AtomicBool, Ordering}},
    collections::{btree_map::{self, BTreeMap}, HashMap, HashSet},
};
use ton_block::{
    BlockIdExt, ShardIdent, Serializable, Deserializable,
    OutMsgQueueInfo, OutMsgQueue, OutMsgQueueKey, IhrPendingInfo,
    ProcessedInfo, ProcessedUpto, ProcessedInfoKey,
    ShardHashes, AccountIdPrefixFull,
    HashmapAugType, ShardStateUnsplit,
};
use ton_types::{
    error, fail, BuilderData, Cell, LabelReader, SliceData, IBitstring, Result, UInt256, 
    HashmapType, HashmapFilterResult, HashmapRemover, UsageTree, HashmapSubtree,
};

#[cfg(test)]
#[path = "tests/test_out_msg_queue.rs"]
mod tests;

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct ProcessedUptoStuff {
    /// An abstract at-least-an-ancestor shard which can refer to
    /// a newly created shard during split.
    pub shard: u64,

    /// Block seqno with a different meaning depending on a context:
    /// - Masterchain block seqno if used without a direct intershard communication.
    /// - A block seqno, corresponding to [`exact_shard`] otherwise.
    ///
    /// [`exact_shard`]: ProcessedUptoStuff::exact_shard
    pub seqno: u32,

    pub last_msg_lt: u64,
    pub last_msg_hash: UInt256,

    /// An original shard in case of altering [`ProcessedUptoStuff::shard`].
    #[cfg(feature = "fast_finality")]
    original_shard: Option<u64>,
    /// A computed masterchain block seqno.
    #[cfg(feature = "fast_finality")]
    real_mc_seqno: u32,

    mc_end_lt: u64,
    ref_shards: Option<ShardHashes>,
}

impl ProcessedUptoStuff {
    pub fn with_params(
        shard: u64,
        seqno: u32,
        last_msg_lt: u64,
        last_msg_hash: UInt256,
        #[cfg(feature = "fast_finality")]
        original_shard: Option<u64>,
        #[cfg(feature = "fast_finality")]
        real_mc_seqno: u32,
    ) -> Self {
        Self {
            shard,
            seqno,
            last_msg_lt,
            last_msg_hash,
            #[cfg(feature = "fast_finality")]
            original_shard,
            #[cfg(feature = "fast_finality")]
            real_mc_seqno,
            mc_end_lt: 0,
            ref_shards: None,
        }
    }

    #[cfg(feature = "fast_finality")]
    pub fn exact_shard(&self) -> u64 {
        self.original_shard.unwrap_or(self.shard)
    }

    #[cfg(feature = "fast_finality")]
    pub fn mc_seqno(&self) -> u32 {
        self.real_mc_seqno
    }
    #[cfg(not(feature = "fast_finality"))]
    pub fn mc_seqno(&self) -> u32 {
        self.seqno
    }

    pub fn contains(&self, other: &Self) -> bool {
        // NOTE: an abstract shards are checked here.
        // In case of direct intershard communication `mc_seqno` does not change
        // its order properties:
        //   - `shard` field behaves the same (we use an additional field `original_shard`
        //     if we need to know an exact shard)
        //   - `mc_seqno` as shard seqno grows the same way as masterchain seqno

        ShardIdent::is_ancestor(self.shard, other.shard)
            && self.seqno >= other.seqno
            && ((self.last_msg_lt > other.last_msg_lt)
            || ((self.last_msg_lt == other.last_msg_lt) && (self.last_msg_hash >= other.last_msg_hash))
        )
    }
    pub fn can_check_processed(&self) -> bool {
        self.ref_shards.is_some()
    }

    fn already_processed(&self, enq: &MsgEnqueueStuff) -> Result<bool> {
        log::trace!(
            "already_processed: shard={:016x}, last_msg_lt={}, last_msg_hash={}, \
            cur_prefix={:016x}, dst_prefix={:016x}, enq_hash={}", 
            self.shard,
            self.last_msg_lt,
            self.last_msg_hash.to_hex_string(),
            enq.cur_prefix().prefix,
            enq.dst_prefix().prefix,
            enq.message_hash().to_hex_string(),
        );
        if enq.created_lt() > self.last_msg_lt {
            log::trace!(
                "already_processed: enq_hash={} `enq.created_lt() > self.last_msg_lt`",
                enq.message_hash().to_hex_string()
            );
            return Ok(false)
        }
        if !ShardIdent::contains(self.shard, enq.next_prefix().prefix) {
            log::trace!(
                "already_processed: enq_hash={} `!ShardIdent::contains(next_prefix)`", 
                enq.message_hash().to_hex_string()
            );
            return Ok(false)
        }
        if enq.created_lt() == self.last_msg_lt && self.last_msg_hash < enq.message_hash() {
            log::trace!(
                "already_processed: enq_hash={} `enq.created_lt() == self.last_msg_lt`", 
                enq.message_hash().to_hex_string()
            );
            return Ok(false)
        }
        if enq.same_workchain() && ShardIdent::contains(self.shard, enq.cur_prefix().prefix) {
            log::trace!(
                "already_processed: enq_hash={} `ShardIdent::contains(cur_prefix)`", 
                enq.message_hash().to_hex_string()
            );
            // this branch is needed only for messages generated in the same shard
            // (such messages could have been processed without a reference from the masterchain)
            // enable this branch only if an extra boolean parameter is set
            return Ok(true)
        }
        let shard_end_lt = self.compute_shard_end_lt(&enq.cur_prefix())?;
        log::trace!(
            "already_processed: enq_hash={} shard_end_lt={shard_end_lt}, processed={}", 
            enq.message_hash().to_hex_string(), enq.enqueued_lt() < shard_end_lt
        );
        Ok(enq.enqueued_lt() < shard_end_lt)
    }
    
    pub fn compute_shard_end_lt(&self, prefix: &AccountIdPrefixFull) -> Result<u64> {
        let shard_end_lt = if prefix.is_masterchain() {
            self.mc_end_lt
        } else  {
            let shard = self.ref_shards.as_ref()
                .ok_or_else(
                    || error!(
                        "ProcessedUpTo record for {} ({}:{:x}) has no info about shards",
                        self.seqno, self.last_msg_lt, self.last_msg_hash
                    )
                )?
                .find_shard_by_prefix(&prefix)?
                .ok_or_else(
                    || error!(
                        "ProcessedUpTo record for {} ({}:{:x}) has no info about shard prefix {}",
                        self.seqno, self.last_msg_lt, self.last_msg_hash, prefix
                    )
                )?;
                
            log::trace!(
                "compute_shard_end_lt: prefix={:016x}, seqno={:016x}, end_lt={}, full_id={}",
                prefix.prefix, self.seqno, shard.descr().end_lt, shard.block_id()
            );

            shard.descr().end_lt
        };
        Ok(shard_end_lt)
    }
}

impl std::fmt::Display for ProcessedUptoStuff {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f, 
            "shard: {:016X}, mc_seqno: {}, mc_end_lt: {}, last_msg_lt: {}, last_msg_hash: {:x}",
            self.shard, self.seqno, self.mc_end_lt, self.last_msg_lt, self.last_msg_hash
        )
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct OutMsgQueueInfoStuff {
    block_id: BlockIdExt,
    out_queue: Option<OutMsgQueue>,
    out_queue_part: Option<OutMsgQueue>,
    ihr_pending: Option<IhrPendingInfo>,
    entries: Vec<ProcessedUptoStuff>,
    min_seqno: u32,
    end_lt: u64,
    disabled: bool,
}

impl OutMsgQueueInfoStuff {
    pub async fn from_shard_state(
        state: &ShardStateStuff,
        cached_states: &mut CachedStates,
    ) -> Result<Self> {
        let out_queue_info = state.state()?.read_out_msg_queue_info()?;
        Self::from_out_queue_info(
            state.block_id().clone(),
            out_queue_info,
            state.state()?.gen_lt(),
            cached_states,
        ).await
    }

    async fn from_out_queue_info(
        block_id: BlockIdExt, 
        out_queue_info: OutMsgQueueInfo, 
        end_lt: u64,
        cached_states: &mut CachedStates,
    ) -> Result<Self> {
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
                "05aa297e3a2e003e1449e1297742d64f188985dc029c620edc84264f9786c0c3".parse().unwrap()
            );
            let key = SliceData::load_bitstring(key.write_to_new_cell()?)?;
            out_queue.remove(key)?;
        }

        let ihr_pending = out_queue_info.ihr_pending().clone();

        Self::with_params(
            block_id, 
            Some(out_queue), 
            None, 
            out_queue_info.proc_info(), 
            Some(ihr_pending), 
            end_lt,
            cached_states,
        ).await
    }

    pub async fn from_queue_part(
        block_id: BlockIdExt, 
        out_queue_part: OutMsgQueue,
        proc_info: ProcessedInfo,
        end_lt: u64,
        cached_states: &mut CachedStates,
    ) -> Result<Self> {
        Self::with_params(
            block_id,
            None,
            Some(out_queue_part),
            &proc_info,
            None,
            end_lt,
            cached_states,
        ).await
    }

    async fn with_params(
        block_id: BlockIdExt, 
        out_queue: Option<OutMsgQueue>,
        out_queue_part: Option<OutMsgQueue>,
        proc_info: &ProcessedInfo,
        ihr_pending: Option<IhrPendingInfo>,
        end_lt: u64,
        cached_states: &mut CachedStates,
    ) -> Result<Self> {
        // NOTE: no new states are loaded for an old implementation
        #[cfg(not(feature = "fast_finality"))]
        let _ = cached_states;

        // unpack ProcessedUptoStuff
        let mut entries = vec![];
        let mut min_seqno = std::u32::MAX;

        for item in proc_info.clone().inner().iter() {
            let (key, mut value) = item?;
            let key = ProcessedInfoKey::construct_from(&mut SliceData::load_builder(key)?)?;
            let value = ProcessedUpto::construct_from(&mut value)?;
            #[cfg(not(feature = "fast_finality"))]
            if value.original_shard.is_some() {
                fail!("ProcessedUpto could not contain original_shard");
            }

            #[cfg(not(feature = "fast_finality"))]
            let entry = {
                if key.mc_seqno < min_seqno {
                    min_seqno = key.mc_seqno;
                }
                ProcessedUptoStuff::with_params(
                    key.shard,
                    key.mc_seqno,
                    value.last_msg_lt,
                    value.last_msg_hash,
                )
            };

            #[cfg(feature = "fast_finality")]
            let entry = {
                let real_mc_seqno = if block_id.shard().is_masterchain() {
                    key.mc_seqno
                } else {
                    let shard = ShardIdent::with_tagged_prefix(
                        block_id.shard().workchain_id(),
                        value.original_shard.unwrap_or(key.shard),
                    )?;
    
                    let state = cached_states
                        .request_shard_state(shard, key.mc_seqno, Some(10_000))
                        .await?;

                    state.state_or_queue()?.master_ref()
                        .ok_or_else(|| error!("shard state {block_id} doesn't have master_ref"))?
                        .master.seq_no
                };
    
                if real_mc_seqno < min_seqno {
                    min_seqno = real_mc_seqno;
                };
    
                ProcessedUptoStuff::with_params(
                    key.shard,
                    key.mc_seqno,
                    value.last_msg_lt,
                    value.last_msg_hash,
                    value.original_shard,
                    real_mc_seqno,
                )
            };

            entries.push(entry);
        }

        Ok(Self {
            block_id,
            out_queue,
            out_queue_part,
            ihr_pending,
            entries,
            min_seqno,
            end_lt,
            disabled: false,
        })
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        let shard = self.shard().merge()?;

        self.out_queue_mut()?.combine_with(other.out_queue()?)?;
        self.out_queue_mut()?.update_root_extra()?;
        self.ihr_pending_mut()?.merge(other.ihr_pending()?, &shard.shard_key(false))?;
        for entry in &other.entries {
            if self.min_seqno > entry.mc_seqno() {
                self.min_seqno = entry.mc_seqno();
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

    fn calc_split_queues(
        self_queue: &mut OutMsgQueue,
        self_shard: &ShardIdent
    ) -> Result<OutMsgQueue> {
        let mut sibling_queue = self_queue.clone();
        self_queue.hashmap_filter(|_key, mut slice| {
            let created_lt = u64::construct_from(&mut slice)?;
            let enq = MsgEnqueueStuff::construct_from(&mut slice, created_lt)?;
            if self_shard.contains_full_prefix(enq.cur_prefix()) {
                Ok(HashmapFilterResult::Accept)
            } else {
                Ok(HashmapFilterResult::Remove)
            }
        })?;
        self_queue.update_root_extra()?;

        sibling_queue.hashmap_filter(|_key, mut slice| {
            let created_lt = u64::construct_from(&mut slice)?;
            let enq = MsgEnqueueStuff::construct_from(&mut slice, created_lt)?;
            if self_shard.contains_full_prefix(enq.cur_prefix()) {
                Ok(HashmapFilterResult::Remove)
            } else {
                Ok(HashmapFilterResult::Accept)
            }
        })?;
        sibling_queue.update_root_extra()?;
        Ok(sibling_queue)
    }

    pub async fn precalc_split_queues(
        engine: &Arc<dyn EngineOperations>,
        block_id: &BlockIdExt
    ) -> Result<()> {
        if engine.set_split_queues_calculating(block_id) {
            let ss = engine.clone().wait_state(block_id, Some(10_000), false).await?;
            let usage_tree = UsageTree::with_params(ss.root_cell().clone(), true);
            let root_cell = usage_tree.root_cell();
            let ss = ShardStateStuff::from_state(
                block_id.clone(), 
                ShardStateUnsplit::construct_from_cell(root_cell)?,
                #[cfg(feature = "telemetry")]
                engine.engine_telemetry(),
                engine.engine_allocated()
            )?;
            let mut queue0 = ss.state()?.read_out_msg_queue_info()?;
            let queue0 = queue0.out_queue_mut();
            let (s0, _s1) = block_id.shard().split()?;
            let now = std::time::Instant::now();
            let queue1 = Self::calc_split_queues(queue0, &s0)?;
            log::info!("precalc_split_queues after block {}, TIME {}ms", 
                block_id, now.elapsed().as_millis());
            engine.set_split_queues(block_id, queue0.clone(), queue1, usage_tree.build_visited_set());
        } else {
            log::trace!("precalc_split_queues {} already calculating or calculated", block_id);
        }
        Ok(())
    }

    fn split(
        &mut self,
        subshard: ShardIdent,
        engine: &Arc<dyn EngineOperations>,
        usage_tree: &UsageTree,
        imported_visited: Option<&mut HashSet<UInt256>>,
    ) -> Result<Self> {

        let (s0, _s1) = self.block_id().shard().split()?;
        let sibling_queue = if let Some((q0, q1, visited)) = engine.get_split_queues(self.block_id()) {
            if let Some(imported_visited) = imported_visited {
                for cell_id in visited {
                    imported_visited.insert(cell_id);
                }
            }
            log::info!("Use split queues from cache (prev block {})", self.block_id());
            if s0 == subshard {
                self.out_queue = Some(q0);
                q1
            } else {
                self.out_queue = Some(q1);
                q0
            }
        } else {
            let now = std::time::Instant::now();
            let sibling_queue = Self::calc_split_queues(self.out_queue_mut()?, &subshard)?;
            let (q0, q1) = if s0 == subshard {
                (self.out_queue()?.clone(), sibling_queue.clone())
            } else {
                (sibling_queue.clone(), self.out_queue()?.clone())
            };
            let visited = usage_tree.build_visited_set();
            engine.set_split_queues(self.block_id(), q0, q1, visited);
            log::warn!(
                "There is no precalculated split queues (prev block {}), calculated TIME {}ms", 
                self.block_id(), now.elapsed().as_millis());
            sibling_queue
        };

        let sibling = subshard.sibling();
        let mut ihr_pending = self.ihr_pending()?.clone();
        self.ihr_pending_mut()?.split_inplace(&subshard.shard_key(false))?;
        ihr_pending.split_inplace(&sibling.shard_key(false))?;

        let mut entries = vec![];
        let mut min_seqno = std::u32::MAX;
        self.min_seqno = min_seqno;
        for mut entry in std::mem::take(&mut self.entries).drain(..) {
            if ShardIdent::shard_intersects(entry.shard, sibling.shard_prefix_with_tag()) {
                let mut entry = entry.clone();
                #[cfg(feature = "fast_finality")] {
                    entry.original_shard = Some(entry.exact_shard());
                }
                entry.shard = ShardIdent::shard_intersection(entry.shard, sibling.shard_prefix_with_tag());
                log::debug!("to sibling {}", entry);
                if min_seqno > entry.mc_seqno() {
                    min_seqno = entry.mc_seqno();
                }
                entries.push(entry);
            }
            if ShardIdent::shard_intersects(entry.shard, subshard.shard_prefix_with_tag()) {
                #[cfg(feature = "fast_finality")] {
                    entry.original_shard = Some(entry.exact_shard());
                }
                entry.shard = ShardIdent::shard_intersection(entry.shard, subshard.shard_prefix_with_tag());
                log::debug!("to us {}", entry);
                if self.min_seqno > entry.mc_seqno() {
                    self.min_seqno = entry.mc_seqno();
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
            out_queue: Some(sibling_queue),
            out_queue_part: None,
            ihr_pending: Some(ihr_pending),
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
            min_seqno = std::cmp::min(min_seqno, entry.mc_seqno());
            let key = ProcessedInfoKey::with_params(entry.shard, entry.seqno);
            let value = ProcessedUpto::with_params(
                entry.last_msg_lt, 
                entry.last_msg_hash.clone(), 
                #[cfg(feature = "fast_finality")] entry.original_shard,
                #[cfg(not(feature = "fast_finality"))] None,
            );
            proc_info.set(&key, &value)?
        }
        Ok((OutMsgQueueInfo::with_params(self.out_queue()?.clone(), proc_info, self.ihr_pending()?.clone()), min_seqno))
    }

    fn fix_processed_upto(
        &mut self,
        seqno: u32,
        next_mc_end_lt: u64,
        next_shards: Option<&ShardHashes>,
        cached_states: &CachedStates,
        stop_flag: &Option<&AtomicBool>,
    ) -> Result<()> {
        let workchain = self.shard().workchain_id();
        let masterchain = workchain == ton_block::MASTERCHAIN_ID;
        for entry in &mut self.entries {
            if entry.ref_shards.is_none() {
                check_stop_flag(stop_flag)?;
                if next_shards.is_some() && masterchain && entry.seqno == seqno + 1 {
                    entry.mc_end_lt = next_mc_end_lt;
                    entry.ref_shards = next_shards.cloned();
                } else {
                    #[cfg(not(feature = "fast_finality"))]
                    let (shard, seqno) = (ShardIdent::masterchain(), std::cmp::min(entry.seqno, seqno));

                    #[cfg(feature = "fast_finality")]
                    let (shard, seqno) = (
                        ShardIdent::with_tagged_prefix(workchain, entry.exact_shard())?,
                        entry.seqno
                    );

                    let (mc_end_lt, ref_shards) = cached_states.get_entry_data(&shard, seqno)?;

                    entry.mc_end_lt = mc_end_lt;
                    entry.ref_shards = Some(ref_shards);
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

    pub fn out_queue(&self) -> Result<&OutMsgQueue> {
        self.out_queue.as_ref().ok_or_else(|| error!("out_queue is None"))
    }

    pub fn out_queue_mut(&mut self) -> Result<&mut OutMsgQueue> {
        self.out_queue.as_mut().ok_or_else(|| error!("out_queue is None"))
    }

    pub fn out_queue_part(&self) -> Result<&OutMsgQueue> {
        self.out_queue_part.as_ref().ok_or_else(|| error!("out_queue_part is None"))
    }

    pub fn out_queue_or_part(&self) -> Result<&OutMsgQueue> {
        self.out_queue.as_ref()
            .or_else(|| self.out_queue_part.as_ref())
            .ok_or_else(|| error!("INTERNAL ERROR: both `out_queue` and `out_queue_part` are None"))
    }

    pub fn ihr_pending(&self) -> Result<&IhrPendingInfo> {
        self.ihr_pending.as_ref().ok_or_else(|| error!("ihr_pending is None"))
    }

    pub fn ihr_pending_mut(&mut self) -> Result<&mut IhrPendingInfo> {
        self.ihr_pending.as_mut().ok_or_else(|| error!("ihr_pending is None"))
    }

    pub fn forced_fix_out_queue(&mut self) -> Result<()> {
        let queue = self.out_queue_mut()?;
        if queue.is_empty() && queue.root_extra() != &0 {
            queue.after_remove()?;
        }
        Ok(())
    }

    pub fn message(&self, key: &OutMsgQueueKey) -> Result<Option<MsgEnqueueStuff>> {
        self.out_queue_or_part()?
            .get_with_aug(&key)?
            .map(|(enq, lt)| MsgEnqueueStuff::from_enqueue_and_lt(enq, lt))
            .transpose()
    }

    pub fn add_message(&mut self, enq: &MsgEnqueueStuff) -> Result<()> {
        let labels = [("shard", self.shard().to_string())];
        metrics::counter!("out_msg_queue_add", 1, &labels);
        let key = enq.out_msg_key();
        self.out_queue_mut()?.set(&key, enq.enqueued(), &enq.created_lt())
    }

    pub fn del_message(&mut self, key: &OutMsgQueueKey) -> Result<()> {
        let labels = [("shard", self.shard().to_string())];
        metrics::counter!("out_msg_queue_del", 1, &labels);
        if self.out_queue_mut()?.remove(SliceData::load_bitstring(key.write_to_new_cell()?)?)?.is_none() {
            fail!("error deleting from out_msg_queue dictionary: {:x}", key)
        }
        Ok(())
    }

    // remove all messages which are not from new_shard
    fn filter_messages(&mut self, new_shard: &ShardIdent) -> Result<()> {
        let old_shard = self.shard().clone();
        self.out_queue_mut()?.hashmap_filter(|_key, mut slice| {
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
        })?;
        Ok(())
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
    pub fn add_processed_upto(
        &mut self,
        seqno: u32,
        #[cfg(feature = "fast_finality")]
        real_mc_seqno: u32,
        last_msg_lt: u64,
        last_msg_hash: UInt256,
    ) -> Result<()> {
        let entry = ProcessedUptoStuff {
            shard: self.shard().shard_prefix_with_tag(),
            seqno,
            last_msg_lt,
            last_msg_hash,
            #[cfg(feature = "fast_finality")]
            real_mc_seqno,
            #[cfg(feature = "fast_finality")]
            original_shard: None,
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
    block_descr: Arc<String>,
}

impl MsgQueueManager {

    pub async fn init(
        engine: &Arc<dyn EngineOperations>,
        last_mc_state: &Arc<ShardStateStuff>,
        shard: ShardIdent,
        new_seq_no: u32,
        shards: &ShardHashes,
        prev_states: &Vec<Arc<ShardStateStuff>>,
        next_state_opt: Option<&Arc<ShardStateStuff>>,
        after_merge: bool,
        after_split: bool,
        stop_flag: Option<&AtomicBool>,
        usage_tree: Option<&UsageTree>,
        imported_visited: Option<&mut HashSet<UInt256>>,
        block_descr: Option<Arc<String>>,
    ) -> Result<Self> {
        let block_descr = block_descr.unwrap_or_else(|| Arc::new(String::default()));

        let mut cached_states = CachedStates::new(engine);
        cached_states.insert(last_mc_state.clone());

        // NOTE: cached previous states (for shard) are not needed for the
        // original queue manager implementation.
        #[cfg(feature = "fast_finality")]
        for prev_state in prev_states {
            cached_states.insert(prev_state.clone());
        }

        // Cache the next state if exists and precompute the next `mc_end_lt`
        // to reduce the compute of state queries in `fix_processed_upto`
        let next_mc_end_lt = match next_state_opt {
            Some(state) => {
                cached_states.insert(state.clone());
                state.shard().is_masterchain().then(|| state.gen_lt()).transpose()?.unwrap_or_default()
            }
            None => 0,
        };

        log::debug!("{}: request a preliminary list of neighbors for {}", block_descr, shard);
        let shards = ShardHashesStuff::from(shards.clone());
        let neighbor_list = shards.neighbours_for(&shard)?;
        let mut neighbors = vec![];
        log::debug!("{}: got a preliminary list of {} neighbors for {}", block_descr, neighbor_list.len(), shard);
        for (i, nb_shard_record) in neighbor_list.iter().enumerate() {
            let nb_block_id = nb_shard_record.block_id();

            log::debug!("{}: neighbors #{} ---> {:#}", block_descr, i + 1, nb_block_id.shard());

            let nb = if nb_block_id.shard().is_masterchain() ||
                nb_block_id.shard().workchain_id() == shard.workchain_id()
            {
                let shard_state = engine.clone().wait_state(nb_block_id, Some(1_000), true).await?;
                cached_states.insert(shard_state.clone());

                Self::load_out_queue_info(&shard_state, &last_mc_state, &mut cached_states, block_descr.clone()).await?
            } else {
                log::debug!("{}: loading OutMsgQueueInfo of neighbor {:#}", block_descr, nb_shard_record.block_id());
                let queue_part = 
                    engine.clone().wait_state(nb_shard_record.block_id(), Some(1_000), true).await?;

                // Non-masterchain shard states are only required for the new implementation
                #[cfg(feature = "fast_finality")]
                cached_states.insert(queue_part.clone());

                let nb = OutMsgQueueInfoStuff::from_queue_part(
                    nb_block_id.clone(),
                    queue_part.queue_for_wc(shard.workchain_id())?,
                    queue_part.proc_info()?,
                    queue_part.gen_lt()?,
                    &mut cached_states,
                ).await?;

                // Request masterchain states of neighbours for the old implementation
                #[cfg(not(feature = "fast_finality"))]
                for entry in nb.entries() {
                    cached_states.request_mc_state(last_mc_state, entry.mc_seqno(), Some(10_000)).await?;
                }

                // Request exact shard states of neighbours for the new implementation
                #[cfg(feature = "fast_finality")]
                for entry in nb.entries() {
                    let shard = 
                        ShardIdent::with_tagged_prefix(nb_block_id.shard().workchain_id(), entry.exact_shard())?;
                    cached_states.request_shard_state(shard, entry.seqno, Some(10_000)).await?;
                }

                nb
            };
            neighbors.push(nb);
            check_stop_flag(&stop_flag)?;
        }

        // TODO: `shards.is_empty()` might not be needed
        if shards.is_empty() || last_mc_state.block_id().seq_no() != 0 {
            let nb = Self::load_out_queue_info(&last_mc_state, &last_mc_state, &mut cached_states, block_descr.clone()).await?;
            neighbors.push(nb);
        }

        let mut next_out_queue_info;
        let mut prev_out_queue_info = Self::load_out_queue_info(&prev_states[0], &last_mc_state, &mut cached_states, block_descr.clone()).await?;
        if prev_out_queue_info.block_id().seq_no != 0 {
            if let Some(state) = prev_states.get(1) {
                CHECK!(after_merge);
                let merge_out_queue_info = Self::load_out_queue_info(state, &last_mc_state, &mut cached_states, block_descr.clone()).await?;
                log::debug!("{}: prepare merge for states {} and {}", block_descr, prev_out_queue_info.block_id(), merge_out_queue_info.block_id());
                prev_out_queue_info.merge(&merge_out_queue_info)?;
                Self::add_trivial_neighbor_after_merge(&mut neighbors, &shard, &prev_out_queue_info, prev_states, &stop_flag, block_descr.clone())?;
                next_out_queue_info = match next_state_opt {
                    Some(next_state) => Self::load_out_queue_info(next_state, &last_mc_state, &mut cached_states, block_descr.clone()).await?,
                    None => prev_out_queue_info.clone()
                };
            } else if after_split {
                log::debug!("{}: prepare split for state {}", block_descr, prev_out_queue_info.block_id());
                let own_usage_tree;
                let usage_tree = if let Some(ut) = usage_tree {
                    ut
                } else {
                    let ss = &prev_states[0];
                    own_usage_tree = UsageTree::with_params(ss.root_cell().clone(), true);
                    let root_cell = own_usage_tree.root_cell();
                    let usage_state = ShardStateStuff::from_state(
                        ss.block_id().clone(), 
                        ShardStateUnsplit::construct_from_cell(root_cell)?,
                        #[cfg(feature = "telemetry")]
                        engine.engine_telemetry(),
                        engine.engine_allocated()
                    )?;
                    prev_out_queue_info = OutMsgQueueInfoStuff::from_shard_state(&usage_state, &mut cached_states).await?;
                    &own_usage_tree
                };
                let sibling_out_queue_info = prev_out_queue_info.split(shard.clone(), engine, &usage_tree, imported_visited)?;
                Self::add_trivial_neighbor(&mut neighbors, &shard, &prev_out_queue_info, 
                    Some(sibling_out_queue_info), prev_states[0].shard(), &stop_flag, block_descr.clone())?;
                next_out_queue_info = match next_state_opt {
                    Some(next_state) => Self::load_out_queue_info(next_state, &last_mc_state, &mut cached_states, block_descr.clone()).await?,
                    None => prev_out_queue_info.clone()
                };
            } else {
                Self::add_trivial_neighbor(&mut neighbors, &shard, &prev_out_queue_info, None, 
                    prev_out_queue_info.shard(), &stop_flag, block_descr.clone())?;
                next_out_queue_info = match next_state_opt {
                    Some(next_state) => Self::load_out_queue_info(next_state, &last_mc_state, &mut cached_states, block_descr.clone()).await?,
                    None => prev_out_queue_info.clone()
                };
            }
        } else {
            next_out_queue_info = match next_state_opt {
                Some(next_state) => Self::load_out_queue_info(next_state, &last_mc_state, &mut cached_states, block_descr.clone()).await?,
                None => prev_out_queue_info.clone()
            };
        }

        // `ProcessedUptoStuff` seqno is a masterchain seqno for the old implementation
        #[cfg(not(feature = "fast_finality"))]
        let seqno = {
            _ = new_seq_no; // unused
            last_mc_state.block_id().seq_no()
        };

        // `ProcessedUptoStuff` seqno is an exact shard seqno for the new implementation
        #[cfg(feature = "fast_finality")]
        let seqno = new_seq_no;

        prev_out_queue_info.fix_processed_upto(seqno, 0, None, &cached_states, &stop_flag)?;
        next_out_queue_info.fix_processed_upto(seqno, next_mc_end_lt, Some(shards.as_ref()), &cached_states, &stop_flag)?;

        for neighbor in &mut neighbors {
            neighbor.fix_processed_upto(seqno, 0, None, &cached_states, &stop_flag)?;
        }

        Ok(MsgQueueManager {
        //Unused
          // shard,
            prev_out_queue_info,
            next_out_queue_info,
            neighbors,
            block_descr,
        })
    }

    pub async fn load_out_queue_info(
        state: &Arc<ShardStateStuff>,
        last_mc_state: &Arc<ShardStateStuff>,
        cached_states: &mut CachedStates,
        block_descr: Arc<String>,
    ) -> Result<OutMsgQueueInfoStuff> {
        log::debug!("{}: unpacking OutMsgQueueInfo of neighbor {:#}", block_descr, state.block_id());
        let nb = OutMsgQueueInfoStuff::from_shard_state(&state, cached_states).await?;
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

            #[cfg(not(feature = "fast_finality"))] {
                cached_states.request_mc_state(last_mc_state, entry.seqno, Some(10_000)).await?;
            }

            #[cfg(feature = "fast_finality")] {
                if state.shard().is_masterchain() {
                    cached_states.request_mc_state(last_mc_state, entry.seqno, Some(10_000)).await?;
                } else {
                    let shard = ShardIdent::with_tagged_prefix(state.shard().workchain_id(), entry.exact_shard())?;
                    cached_states.request_shard_state(shard, entry.seqno, Some(10_000)).await?;
                }
            }
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

    fn get_max_processed_lt_from_queue_info(&self) -> u64 {
        let current_shard = self.prev_out_queue_info.shard();
        let mut max_lt = 0;
        for neighbor in &self.neighbors {
            if neighbor.shard().workchain_id() != -1 || neighbor.shard() == current_shard {
                for entry in &neighbor.entries {
                    if entry.last_msg_lt > max_lt {
                        max_lt = entry.last_msg_lt;
                    }
                }
            }
            log::trace!(
                "{}: get_max_processed_lt: (current shard {} != neighbor shard {}) = {}, max_lt {}",
                self.block_descr,
                current_shard,
                neighbor.shard(),
                neighbor.shard() != current_shard,
                max_lt,
            );
        }
        max_lt
    }

    fn add_trivial_neighbor_after_merge(
        neighbors: &mut Vec<OutMsgQueueInfoStuff>,
        shard: &ShardIdent,
        real_out_queue_info: &OutMsgQueueInfoStuff,
        prev_states: &Vec<Arc<ShardStateStuff>>,
        stop_flag: &Option<&AtomicBool>,
        block_descr: Arc<String>,
    ) -> Result<()> {
        log::debug!("{}: in add_trivial_neighbor_after_merge()", block_descr);
        CHECK!(prev_states.len(), 2);
        let mut found = 0;
        let n = neighbors.len();
        for i in 0..n {
            if shard.intersect_with(neighbors[i].shard()) {
                let nb = &neighbors[i];
                found += 1;
                log::debug!("{}: neighbor #{} : {} intersects our shard {}", block_descr, i, nb.block_id(), shard);
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
                    log::debug!("{}: adjusted neighbor #{} : {} with shard expansion \
                        (immediate after-merge adjustment)", block_descr, i, neighbors[i].block_id());
                } else {
                    neighbors[i].disable();
                    log::debug!("{}: disabling neighbor #{} : {} \
                        (immediate after-merge adjustment)", block_descr, i, neighbors[i].block_id());
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
        block_descr: Arc<String>,
    ) -> Result<()> {
        log::debug!("{}: in add_trivial_neighbor()", block_descr);
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
                log::debug!("{}: neighbor #{} : {} intersects our shard {}", block_descr, i, nb.block_id(), shard);
                if nb.shard() == prev_shard {
                    if prev_shard == shard {
                        // case 1. Normal.
                        CHECK!(found == 1);
                        neighbors[i] = real_out_queue_info.clone();
                        log::debug!("{}: adjusted neighbor #{} : {} (simple replacement)", block_descr, i, neighbors[i].block_id());
                        cs = 1;
                    } else if nb.shard().is_parent_for(&shard) {
                        // case 2. Immediate after-split.
                        CHECK!(found == 1);
                        CHECK!(sibling_out_queue_info.is_some());
                        if let Some(ref sibling) = sibling_out_queue_info {
                            neighbors[i] = sibling.clone();
                            neighbors[i].set_shard(shard.clone());
                        }
                        log::debug!("{}: adjusted neighbor #{} : {} with shard \
                            shrinking to our sibling (immediate after-split adjustment)", block_descr, i, neighbors[i].block_id());

                        let nb = real_out_queue_info.clone();
                        log::debug!("{}: created neighbor #{} : {} with shard \
                            shrinking to our (immediate after-split adjustment)", block_descr, n, nb.block_id());
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
                    neighbors[i].out_queue_mut()?.into_subtree_with_prefix(&shard_prefix, &mut 0)?;
                    neighbors[i].filter_messages(&sib_shard)
                        .map_err(|err| error!("cannot filter virtual sibling's OutMsgQueue from that of \
                            the last common ancestor: {}", err))?;
                    neighbors[i].set_shard(sib_shard);
                    log::debug!("{}: adjusted neighbor #{} : {} with shard shrinking \
                        to our sibling (continued after-split adjustment)", block_descr, i, neighbors[i].block_id());

                    let nb = real_out_queue_info.clone();
                    log::debug!("{}: created neighbor #{} : {} from our preceding state \
                        (continued after-split adjustment)", block_descr, n, nb.block_id());
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
                        log::debug!("{}: adjusted neighbor #{} : {} with shard expansion \
                            (continued after-merge adjustment)", block_descr, i, neighbors[i].block_id());
                    } else {
                        neighbors[i].disable();
                        log::debug!("{}: disabling neighbor #{} : {} (continued after-merge adjustment)",
                        block_descr, i, neighbors[i].block_id());
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

    pub async fn clean_out_msg_queue(
        &mut self,
        clean_timeout_nanos: i128,
        optimistic_clean_percentage_points: u32,
        mut on_message: impl FnMut(Option<(MsgEnqueueStuff, u64)>, Option<&Cell>) -> Result<bool>
    ) -> Result<(bool, i32, i32)> {
        let timer = std::time::Instant::now();

        log::debug!("{}: in clean_out_msg_queue: cleaning output messages imported by neighbors", self.block_descr);
        if self.next_out_queue_info.out_queue()?.is_empty() {
            return Ok((false, 0, 0))
        }

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
        let mut queue = self.next_out_queue_info.out_queue()?.clone();
        let mut deleted = 0;
        let mut skipped = 0;

        let ordered_cleaning_timeout_nanos = clean_timeout_nanos * (optimistic_clean_percentage_points as i128) / 1000;
        let random_cleaning_timeout_nanos = clean_timeout_nanos - ordered_cleaning_timeout_nanos;

        log::debug!(
            "{}: clean_out_msg_queue: clean_timeout = {} nanos, ordered_cleaning_timeout = {} nanos, random_cleaning_timeout = {} nanos",
            self.block_descr,
            clean_timeout_nanos,
            ordered_cleaning_timeout_nanos,
            random_cleaning_timeout_nanos,
        );

        if ordered_cleaning_timeout_nanos > 0 {
            let max_processed_lt = self.get_max_processed_lt_from_queue_info();

            let mut clean_timeout_check = 50_000_000;
            let max_clean_timeout_check = 550_000_000;

            partial = out_msg_queue_cleaner::hashmap_filter_ordered_by_lt_hash(
                &mut queue,
                max_processed_lt,
                ordered_cleaning_timeout_nanos,
                |node_obj| {
                    if block_full {
                        log::debug!("{}: BLOCK FULL when ordered cleaning output queue, cleanup is partial", self.block_descr);
                        partial = true;
                        return Ok(HashmapFilterResult::Stop);
                    }

                    let elapsed_nanos = timer.elapsed().as_nanos() as i128;
                    if clean_timeout_check <= max_clean_timeout_check && elapsed_nanos >= clean_timeout_check {
                        log::debug!(
                            "{}: clean_out_msg_queue: ordered cleaning time elapsed {} nanos: processed = {}, deleted = {}, skipped = {}",
                            self.block_descr, elapsed_nanos, deleted + skipped, deleted, skipped,
                        );
                        clean_timeout_check += 50_000_000;
                    }

                    let lt = node_obj.lt();
                    let mut data_and_refs = node_obj.data_and_refs()?;
                    let enq = MsgEnqueueStuff::construct_from(&mut data_and_refs, lt)?;

                    let (processed, end_lt) = self.already_processed(&enq)?;
                    if processed {
                        block_full = on_message(
                            Some((enq, end_lt)),
                            self.next_out_queue_info.out_queue()?.data(),
                        )?;
                        deleted += 1;
                        return Ok(HashmapFilterResult::Remove);
                    }
                    skipped += 1;
                    Ok(HashmapFilterResult::Accept)
                },
                Some(format!("{}: ", self.block_descr)),
            ).map_err(|e| {
                log::error!(
                    "{}: clean_out_msg_queue: error while ordered cleaning output queue, last state was:{} processed = {}, deleted = {}, skipped = {}: e = {}",
                    self.block_descr,
                    if partial { " partial," } else { "" },
                    deleted + skipped, deleted, skipped,
                    e,
                );
                e
            })?;

            let ordered_clean_elapsed = timer.elapsed().as_nanos() as i128;

            log::debug!(
                "{}: clean_out_msg_queue: cleaning finished (ordered) in {} nanos:{} processed = {}, deleted = {}, skipped = {}",
                self.block_descr,
                ordered_clean_elapsed,
                if partial { " partial," } else { "" },
                deleted + skipped, deleted, skipped,
            );
        } else {
            // not time limit on the ordered clean = queue not cleaned = partial
            partial = true;
        }

        if random_cleaning_timeout_nanos > 0 && partial {
            partial = false;

            let mut random_deleted = 0;
            let mut random_skipped = 0;

            let random_clean_timer = std::time::Instant::now();

            let mut clean_timeout_check = 50_000_000;
            let max_clean_timeout_check = 550_000_000;

            queue.hashmap_filter(|_key, mut slice| {
                if block_full {
                    log::debug!("{}: BLOCK FULL when random cleaning output queue, cleanup is partial", self.block_descr);
                    partial = true;
                    return Ok(HashmapFilterResult::Stop)
                }

                let elapsed_nanos = random_clean_timer.elapsed().as_nanos() as i128;

                if clean_timeout_check <= max_clean_timeout_check && elapsed_nanos >= clean_timeout_check {
                    log::debug!(
                        "{}: clean_out_msg_queue: random cleaning time elapsed {} nanos: processed = {}, deleted = {}, skipped = {}",
                        self.block_descr, elapsed_nanos,
                        random_deleted + random_skipped, random_deleted, random_skipped,
                    );
                    clean_timeout_check += 50_000_000;
                }

                // stop when reached the time limit
                if elapsed_nanos >= random_cleaning_timeout_nanos {
                    log::debug!(
                        "{}: clean_out_msg_queue: stopped random cleaning output queue because of time elapsed {} nanos >= {} nanos limit",
                        self.block_descr, elapsed_nanos, random_cleaning_timeout_nanos,
                    );
                    partial = true;
                    return Ok(HashmapFilterResult::Stop)
                }

                let lt = u64::construct_from(&mut slice)?;
                let enq = MsgEnqueueStuff::construct_from(&mut slice, lt)?;
                let (processed, end_lt) = self.already_processed(&enq)?;
                if processed {
                    block_full = on_message(Some((enq, end_lt)), self.next_out_queue_info.out_queue()?.data())?;
                    random_deleted += 1;
                    return Ok(HashmapFilterResult::Remove)
                }
                random_skipped += 1;
                Ok(HashmapFilterResult::Accept)
            }).map_err(|e| {
                log::error!(
                    "{}: clean_out_msg_queue: error while random cleaning output queue, last state was:{} processed = {}, deleted = {}, skipped = {}: e = {}",
                    self.block_descr,
                    if partial { " partial," } else { "" },
                    random_deleted + random_skipped, random_deleted, random_skipped,
                    e,
                );
                e
            })?;

            let random_clean_elapsed = random_clean_timer.elapsed().as_nanos() as i128;

            log::debug!(
                "{}: clean_out_msg_queue: cleaning finished (random) in {} nanos:{} processed = {}, deleted = {}, skipped = {}",
                self.block_descr,
                random_clean_elapsed,
                if partial { " partial," } else { "" },
                random_deleted + random_skipped, random_deleted, random_skipped,
            );

            deleted += random_deleted;
            skipped += random_skipped;
        }

        let total_clean_elapsed_nanos = timer.elapsed().as_nanos();

        log::debug!(
            "{}: clean_out_msg_queue: cleaning finished (total) in {} nanos:{} processed = {}, deleted = {}, skipped = {}",
            self.block_descr,
            total_clean_elapsed_nanos,
            if partial { " partial," } else { "" },
            deleted + skipped, deleted, skipped,
        );

        self.next_out_queue_info.out_queue = Some(queue);

        on_message(None, self.next_out_queue_info.out_queue()?.data())?;

        Ok((partial, deleted + skipped, deleted))
    }
}

pub struct CachedStates {
    engine: Arc<dyn EngineOperations>,
    states: HashMap<ShardIdent, BTreeMap<u32, Arc<ShardStateStuff>>>,
}

impl CachedStates {
    pub fn new(engine: &Arc<dyn EngineOperations>) -> Self {
        Self {
            engine: engine.clone(),
            states: Default::default(),
        }
    }

    pub fn get_entry_data(&self, shard: &ShardIdent, seq_no: u32) -> Result<(u64, ShardHashes)> {
        if let Some(states) = self.states.get(shard) {
            if let Some(state_stuff) = states.get(&seq_no) {
                let state = state_stuff.state()?;

                let mc_end_lt = match state.master_ref() {
                    None => state.gen_lt(),
                    Some(master_ref) => master_ref.master.end_lt,
                };

                #[cfg(not(feature = "fast_finality"))]
                let shard_hashes = state_stuff.shards()?.clone();

                #[cfg(feature = "fast_finality")]
                let shard_hashes = match state_stuff.shard_hashes_raw_opt() {
                    Some(shard_hashes) => shard_hashes.clone(),
                    // TODO: replace shard hashes with something more optimal
                    None => state
                        .ref_shard_blocks()
                        .map(crate::validating_utils::extend_ref_shard_blocks)
                        .transpose()?
                        .unwrap_or_default(),
                };

                return Ok((mc_end_lt, shard_hashes));
            }
        }
        fail!("state for block {}:{} was not previously cached", shard, seq_no)
    }

    pub fn insert(&mut self, state: Arc<ShardStateStuff>) {
        self.states
            .entry(state.shard().clone())
            .or_default()
            .insert(state.seq_no(), state);
    }

    pub async fn request_mc_state(
        &mut self,
        last_mc_state: &Arc<ShardStateStuff>,
        seq_no: u32,
        timeout_ms: Option<u64>,
    ) -> Result<()> {
        let states = self.states.entry(ShardIdent::masterchain()).or_default();
        if let btree_map::Entry::Vacant(entry) = states.entry(seq_no) {
            let last_mc_seqno = last_mc_state.state()?.seq_no();
            if seq_no >= last_mc_seqno {
                fail!("Requested too new master chain state {}, last is {}", seq_no, last_mc_seqno);
            }

            let block_id = match last_mc_state.shard_state_extra()?.prev_blocks.get(&seq_no) {
                Ok(Some(result)) => result.master_block_id().1,
                _ => fail!("cannot find previous masterchain block with seqno {} \
                    to load corresponding state as required", seq_no)
            };

            let state = self.engine.clone().wait_state(&block_id, timeout_ms, true).await?;
            entry.insert(state);
        }
        Ok(())
    }

    #[cfg(feature = "fast_finality")]
    pub async fn request_shard_state(
        &mut self,
        shard: ShardIdent,
        seq_no: u32,
        timeout_ms: Option<u64>,
    ) -> Result<Arc<ShardStateStuff>> {
        let has_requested_shard = match self.states.get(&shard) {
            Some(shard) => {
                if let Some(shard_state) = shard.get(&seq_no) {
                    return Ok(shard_state.clone())
                }
                true
            }
            None => false,
        };

        let mut closest_state = None;

        // Fast path if the specified path already exists
        if has_requested_shard {
            if let Some(states) = &self.states.get(&shard) {
                if let Some((_, state)) = states.range(seq_no..).next() {
                    closest_state = Some(state.clone());
                }
            }
        }

        // Slow path is there were splits or merges for the specified shard
        if closest_state.is_none() {
            for (known_shard, states) in &self.states {
                // Ignore shards where we will not find some references
                if !shard.intersect_with(known_shard) {
                    continue;
                }

                // Find the closest state in some shard
                let Some((_, state)) = states.range(seq_no..).next() else {
                    continue;
                };

                // Update the closest state if the found one is closer
                if !matches!(&closest_state, Some(closest_state) if closest_state.seq_no() <= state.seq_no()) {
                    closest_state = Some(state.clone());
                }
            }
        }

        if log::log_enabled!(log::Level::Trace) {
            let mut cached_list = string_builder::Builder::default();
            for (shard, states) in &self.states {
                for seqno in states.keys() {
                    cached_list.append(format!("\n{shard}:{seqno}"));
                }
            }
            log::trace!("Cached states for {shard}:{seq_no}: {}", cached_list.string().unwrap_or_default());
        }

        // Find full block id
        let is_target_block = |block_id: &BlockIdExt| {
            block_id.shard() == &shard && block_id.seq_no == seq_no
        };
        let is_possible_block = |block_id: &BlockIdExt| {
            block_id.shard().intersect_with(&shard) && block_id.seq_no > seq_no
        };

        let mut closest_block_id = match closest_state {
            Some(state) => state.block_id().clone(),
            None => fail!("Failed to find the closest state for {}:{}", shard, seq_no),
        };
        log::trace!("Closest block id for {shard}:{seq_no} is {closest_block_id}");

        // TODO: simplify
        let block_id = loop {
            let prev1 = self.engine.load_block_prev1(&closest_block_id)?;
            log::trace!("Found prev1 for {shard}:{seq_no} = {prev1}");

            // Fast check if the target block was found
            if is_target_block(&prev1) {
                break Some(prev1);
            }

            // Check if left shard is ok
            let suits_prev1 = is_possible_block(&prev1);

            if closest_block_id.shard().is_parent_for(prev1.shard()) {
                if let Some(prev2) = self.engine.load_block_prev2(&closest_block_id)? {
                    log::trace!("Found prev2 for {shard}:{seq_no} = {prev2}");

                    // Fast check if the target block was found
                    if is_target_block(&prev2) {
                        break Some(prev2);
                    }

                    // Check if right shard is ok
                    let suits_prev2 = is_possible_block(&prev2);
                    if suits_prev1 && suits_prev2 {
                        // Choose the closest if both shards are ok
                        closest_block_id = if prev1.seq_no < prev2.seq_no {
                            prev1
                        } else {
                            prev2
                        };
                        continue;
                    } else if suits_prev2 {
                        // Choose right shard if it is the only suitable one
                        closest_block_id = prev2;
                        continue;
                    }
                }
            }

            if suits_prev1 {
                // Choose left shard if it is the only suitable one
                closest_block_id = prev1;
            } else {
                // No suitable shards found
                break None;
            }
        }.ok_or_else(|| error!("Failed to find the full shard block id for {}:{}", shard, seq_no))?;

        let state = self.engine.clone().wait_state(&block_id, timeout_ms, true).await?;
        self.states.entry(shard).or_default().insert(block_id.seq_no, state.clone());

        Ok(state)
    }
}

impl MsgQueueManager {
    /// create iterator for merging all output messages from all neighbors to our shard
    pub fn merge_out_queue_iter(&self, shard: &ShardIdent) -> Result<MsgQueueMergerIterator<BlockIdExt>> {
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
    pub fn next_mut(&mut self) -> &mut OutMsgQueueInfoStuff { &mut self.next_out_queue_info }
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
struct RootRecord<T> {
    lt: u64,
    cursor: SliceData,
    bit_len: usize,
    key: BuilderData,
    id: T
}

impl<T: Eq> RootRecord<T> {
    fn new(
        lt: u64,
        cursor: SliceData,
        bit_len: usize,
        key: BuilderData,
        id: T
    ) -> Self {
        Self {
            lt,
            cursor,
            bit_len,
            key,
            id
        }
    }
    fn from_cell(cell: &Cell, mut bit_len: usize, id: T) -> Result<Self> {
        let mut cursor = SliceData::load_cell_ref(cell)?;
        let key = cursor.get_label_raw(&mut bit_len, BuilderData::default())?;
        let lt = cursor.get_next_u64()?;
        Ok(Self {
            lt,
            cursor,
            bit_len,
            key,
            id
        })
    }
}

impl<T: Eq> Ord for RootRecord<T> {
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
impl<T: Eq> PartialOrd for RootRecord<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

/// it iterates messages ascending create_lt and hash
pub struct MsgQueueMergerIterator<T> {
    // store branches descending by lt and hash because Vec works like Stack
    roots: Vec<RootRecord<T>>,
}

impl MsgQueueMergerIterator<BlockIdExt> {
    pub fn from_manager(manager: &MsgQueueManager, shard: &ShardIdent) -> Result<Self> {
        let shard_prefix = shard.shard_key(true);
        let mut roots = vec![];
        for nb in manager.neighbors.iter().filter(|nb| !nb.is_disabled()) {
            let out_queue_short = if let Ok(full_queue) = nb.out_queue() {
                let mut q = full_queue.clone();
                q.into_subtree_with_prefix(&shard_prefix, &mut 0)?;
                q
            } else {
                let mut q = nb.out_queue_part()?.clone();
                q.into_subtree_with_prefix(&shard_prefix, &mut 0)?;
                q
            };
            if let Some(cell) = out_queue_short.data() {
                roots.push(RootRecord::from_cell(cell, out_queue_short.bit_len(), nb.block_id().clone())?);
                // roots.push(RootRecord::new(lt, cursor, bit_len, key, nb.block_id().clone()));
            }
        }
        if !roots.is_empty() {
            roots.sort();
            debug_assert!(roots.first().unwrap().lt >= roots.last().unwrap().lt);
        }
        Ok(Self { roots })
    }
}

impl MsgQueueMergerIterator<u8> {
    #[cfg(test)]
    pub fn from_queue(out_queue: &OutMsgQueue) -> Result<Self> {
        let mut roots = Vec::new();
        if let Some(cell) = out_queue.data() {
            roots.push(RootRecord::from_cell(cell, out_queue.bit_len(), 0)?);
        }
        Ok(Self { roots })
    }
}

impl<T: Clone + Eq> MsgQueueMergerIterator<T> {
    fn insert(&mut self, root: RootRecord<T>) {
        let idx = self.roots.binary_search(&root).unwrap_or_else(|x| x);
        self.roots.insert(idx, root);
        debug_assert!(self.roots.first().unwrap().lt >= self.roots.last().unwrap().lt);
    }
    fn next_item(&mut self) -> Result<Option<(OutMsgQueueKey, MsgEnqueueStuff, u64, T)>> {
        while let Some(mut root) = self.roots.pop() {
            if root.bit_len == 0 {
                let key = OutMsgQueueKey::construct_from_cell(root.key.into_cell()?)?;
                let enq = MsgEnqueueStuff::construct_from(&mut root.cursor, root.lt)?;
                return Ok(Some((key, enq, root.lt, root.id)))
            }
            for idx in 0..2 {
                let mut bit_len = root.bit_len - 1;
                let mut cursor = SliceData::load_cell(root.cursor.reference(idx)?)?;
                let mut key = root.key.clone();
                key.append_bit_bool(idx == 1)?;
                key = LabelReader::read_label_raw(&mut cursor, &mut bit_len, key)?;
                let lt = cursor.get_next_u64()?;
                self.insert(RootRecord::new(lt, cursor, bit_len, key, root.id.clone()));
            }
        }
        Ok(None)
    }
}

impl<T: Clone + Eq> Iterator for MsgQueueMergerIterator<T> {
    type Item = Result<(OutMsgQueueKey, MsgEnqueueStuff, u64, T)>;
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
