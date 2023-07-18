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
    engine::Engine, engine_traits::{EngineAlloc, EngineOperations}, shard_state::ShardStateStuff,
    types::top_block_descr::{
        Mode as TbdMode, TopBlockDescrStuff, TopBlockDescrGroup, TopBlockDescrId
    } 
};
#[cfg(feature = "telemetry")]
use crate::engine_traits::EngineTelemetry;

use adnl::{declare_counted, common::{CountedObject, Counter}};
use rand::Rng;
use std::{
    collections::{hash_map::{self, HashMap}, BTreeMap}, ops::Deref,
    sync::{Arc, atomic::{AtomicU32, Ordering}}, time::{Duration, Instant}
};
use ton_block::{
    BlockIdExt, TopBlockDescr, Deserializable, BlockSignatures, ShardIdent, CollatorRange
};
use ton_types::{fail, Result};

pub enum StoreAction {
    Save(TopBlockDescrId, Arc<TopBlockDescrStuff>),
    Remove(TopBlockDescrId)
}

pub enum ShardBlockProcessingResult {
    Duplication,
    MightBeAdded(Arc<TopBlockDescrStuff>)
}

declare_counted!(
    struct ShardBlocksPoolItem {
        top_block: Arc<TopBlockDescrStuff>,
        /// Flag used for async retain. Should not be used outside of `update_shard_blocks`
        is_valid: bool
    }
);

impl std::fmt::Debug for ShardBlocksPoolItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardBlocksPoolItem")
            .field("top_block_id", self.top_block.proof_for())
            .field("is_own", &self.top_block.is_own())
            .field("ref_shard_blocks", self.top_block.ref_shard_blocks())
            .finish()
    }
}

pub struct ShardBlocksPool {
    last_mc_seq_no: AtomicU32,
    shard_blocks: tokio::sync::RwLock<HashMap<TopBlockDescrGroup, ShardBlocksPoolItems>>,
    storage_sender: Option<tokio::sync::mpsc::UnboundedSender<StoreAction>>,
    is_fake: bool,
}

struct ShardBlocksPoolItems {
    items: BTreeMap<u32, ShardBlocksPoolItem>,
    /// Flag used for async retain. Should not be used outside of `update_shard_blocks`
    is_valid: bool,
}

impl Default for ShardBlocksPoolItems {
    fn default() -> Self {
        Self {
            items: Default::default(),
            is_valid: true,
        }
    }
}

impl ShardBlocksPool {

    pub fn new(
        shard_blocks: HashMap<TopBlockDescrId, TopBlockDescrStuff>,
        last_mc_seqno: u32,
        is_fake: bool,
        #[cfg(feature = "telemetry")]
        telemetry: &Arc<EngineTelemetry>,
        allocated: &Arc<EngineAlloc>
    ) -> Result<(Self, tokio::sync::mpsc::UnboundedReceiver<StoreAction>)> {
        let mut tsbs = HashMap::<TopBlockDescrGroup, ShardBlocksPoolItems>::new();
        for (key, val) in shard_blocks {
            let seqno = val.proof_for().seq_no;
            let item = ShardBlocksPoolItem {
                top_block: Arc::new(val),
                is_valid: true,
                counter: allocated.top_blocks.clone().into(),
            };

            match tsbs.entry(key.group) {
                hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().items.insert(seqno, item);
                }
                hash_map::Entry::Vacant(entry) => {
                    entry.insert(ShardBlocksPoolItems {
                        items: [(seqno, item)].into(),
                        is_valid: true,
                    });
                }
            };

            #[cfg(feature = "telemetry")]
            telemetry.top_blocks.update(allocated.top_blocks.load(Ordering::Relaxed));
        }

        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let ret = ShardBlocksPool {
            last_mc_seq_no: AtomicU32::new(last_mc_seqno),
            shard_blocks: tokio::sync::RwLock::new(tsbs),
            storage_sender: Some(sender.clone()),
            is_fake,
        };
        Ok((ret, receiver))
    }

    pub async fn process_shard_block_raw(
        &self,
        id: &BlockIdExt,
        cc_seqno: u32,
        data: Vec<u8>,
        own: bool,
        check_only: bool,
        engine: &dyn EngineOperations,
    ) -> Result<ShardBlockProcessingResult> {
        let factory = || {
            let tbd = if self.is_fake {
                TopBlockDescr::with_id_and_signatures(id.clone(), BlockSignatures::default())
            } else {
                TopBlockDescr::construct_from_bytes(&data)?
            };
            Ok(Arc::new(TopBlockDescrStuff::new(tbd, &id, self.is_fake, own)?))
        };
        self.process_shard_block(id, cc_seqno, factory, check_only, engine).await
    }

    pub async fn process_shard_block(
        &self,
        id: &BlockIdExt,
        cc_seqno: u32,
        mut factory: impl FnMut() -> Result<Arc<TopBlockDescrStuff>>,
        check_only: bool,
        engine: &dyn EngineOperations,
    ) -> Result<ShardBlockProcessingResult> {
        let tbds_id = TopBlockDescrGroup::new(id.shard().clone(), cc_seqno).with_seqno(id.seq_no);
    
        log::trace!("process_shard_block cc_seqno: {} id: {}", cc_seqno, id);

        // TODO: do more work for broadcasts VS aquire mutex twise
        let tbds = factory()?;
        if !self.is_fake {
            tbds.validate(&engine.load_last_applied_mc_state().await?)?;
        }

        let mut shard_blocks = self.shard_blocks.write().await;
        
        // check for duplication
        if let Some(prev) = shard_blocks.get(&tbds_id.group) {
            if let Some(existing) = prev.items.get(&id.seq_no) {
                log::trace!("process_shard_block duplication cc_seqno: {} id: {} prev: {}",
                cc_seqno, id, existing.top_block.proof_for());
                return Ok(ShardBlockProcessingResult::Duplication);
            }
        }

        if check_only {
            log::trace!("process_shard_block check only  id: {}", id);
        } else {
            let counter = &engine.engine_allocated().top_blocks;

            shard_blocks
                .entry(tbds_id.group.clone())
                .or_default()
                .items
                .insert(id.seq_no, ShardBlocksPoolItem {
                    top_block: tbds.clone(),
                    is_valid: true,
                    counter: counter.clone().into(),
                });

            #[cfg(feature = "telemetry")]
            engine.engine_telemetry().top_blocks.update(
                counter.load(Ordering::Relaxed)
            );

            log::trace!("process_shard_block added cc_seqno: {} id: {}", cc_seqno, id);
        }

        drop(shard_blocks);

        self.send_to_storage(StoreAction::Save(tbds_id.clone(), tbds.clone()));
        Ok(ShardBlockProcessingResult::MightBeAdded(tbds))
    }

    pub async fn get_shard_blocks(
        &self,
        last_mc_state: &Arc<ShardStateStuff>,
        engine: &dyn EngineOperations,
        only_own: bool,
        mut actual_last_mc_seqno: Option<&mut u32>,
    ) -> Result<Vec<Arc<TopBlockDescrStuff>>> {
        let last_mc_seq_no = last_mc_state.block_id().seq_no;

        let mut check_mc_seqno_updated = || {
            let mc_seqno = self.last_mc_seq_no.load(Ordering::Acquire);
            if let Some(actual_last_mc_seqno) = actual_last_mc_seqno.as_mut() {
                **actual_last_mc_seqno = mc_seqno;
            }

            if last_mc_seq_no != mc_seqno {
                log::error!(
                    "get_shard_blocks: Given last_mc_seq_no {} is not actual {}", 
                    last_mc_seq_no, mc_seqno
                );
                fail!("Given last_mc_seq_no {} is not actual {}", last_mc_seq_no, mc_seqno);
            }

            Result::Ok(())
        };

        check_mc_seqno_updated()?;

        // Read shards from state
        let (shards_info, ctx) = prepare_resolver_ctx(last_mc_state)?;

        let shard_blocks = self.shard_blocks.read().await;
        let now = Instant::now();

        let mut returned_list = string_builder::Builder::default();

        let mut blocks = Vec::new();
        if only_own {
            for shard in shards_info.shards.keys() {
                // Compute the required shard group
                let Some((group, info)) = shards_info.get_group_and_info(shard) else {
                    continue;
                };

                // Add all own broadcasts from this group
                if let Some(group) = shard_blocks.get(&group) {
                    for ShardBlocksPoolItem { top_block, .. } in group.items.values() {
                        let seqno = top_block.proof_for().seq_no();
                        if top_block.is_own() && seqno >= info.min_seqno {
                            blocks.push(top_block.clone());
                            returned_list.append(format!("\n{shard} {seqno}"));
                        }
                    }
                }
            }
        } else {
            // NOTE: include `StreamExt` locally to prevent some possible collisions with `next`
            use futures::StreamExt;

            let mut futures = futures::stream::FuturesUnordered::new();

            // Start resolving shards in parallel
            for shard in shards_info.shards.keys() {
                // Return only the latest valid top block description
                futures.push(ctx.resolve_shard_blocks(shard, engine, &shards_info, &shard_blocks));
            }

            // Collect resolver results into a blocks list (in any order)
            while let Some(res) = futures.next().await {
                if let Some(tbds) = res {
                    let shard = tbds.proof_for().shard();
                    let seqno = tbds.proof_for().seq_no();
                    returned_list.append(format!("\n{shard} {seqno}"));
                    blocks.push(tbds);
                }
            }
        }

        drop(shard_blocks);

        const TIME_THRESHOLD: Duration = Duration::from_millis(1);
        if now.elapsed() > TIME_THRESHOLD {
            log::warn!(
                "get_shard_blocks lock took longer than expected: {}ms / 1ms",
                now.elapsed().as_millis(),
            );
        }

        if !only_own {
            check_mc_seqno_updated()?
        }

        log::trace!("get_shard_blocks last_mc_seq_no {} returned: {}", 
            last_mc_seq_no, returned_list.string().unwrap_or_default());
        Ok(blocks)
    }

    pub async fn update_shard_blocks(&self, last_mc_state: &Arc<ShardStateStuff>) -> Result<()> {
        // Read shards from state
        let (shards_info, _) = prepare_resolver_ctx(last_mc_state)?;

        let mut removed_list = string_builder::Builder::default();

        let mut shard_hashes = self.shard_blocks.write().await;
        let now = Instant::now();

        // Mark all invalid items
        for (group, value) in shard_hashes.iter_mut() {
            // Find an existing info
            let existing = shards_info
                .get_group_and_info(&group.shard_ident)
                .filter(|(expected_group, _)| expected_group == group);

            let retain = match existing {
                Some((_, info)) => {
                    // Mark all invalid top block descriptions in a valid group
                    for (&seqno, item) in value.items.iter_mut() {
                        let is_new = seqno >= info.min_seqno;
                        let is_valid = is_new && item.top_block.validate(last_mc_state).is_ok();

                        if is_new {
                            // Yield after each `validate`, because it takes ~100mcs
                            tokio::task::yield_now().await;
                        }

                        if !is_valid {
                            let id = group.clone().with_seqno(seqno);
                            self.send_to_storage(StoreAction::Remove(id));
                            removed_list.append(format!("\n{} {}", seqno, group));
                        }

                        item.is_valid = is_valid;
                    }

                    !value.items.is_empty()
                }
                None => {
                    // Remove all items
                    for &seqno in value.items.keys() {
                        let id = group.clone().with_seqno(seqno);
                        self.send_to_storage(StoreAction::Remove(id));
                        removed_list.append(format!("\n{} {}", seqno, group));
                    }
                    false
                }
            };
            value.is_valid = retain;
        }

        // Remove all marked values
        shard_hashes.retain(|_, value| {
            if value.is_valid {
                value.items.retain(|_, item| item.is_valid);
                !value.items.is_empty()
            } else {
                false
            }
        });

        drop(shard_hashes);

        const TIME_THRESHOLD: Duration = Duration::from_millis(1);
        if now.elapsed() > TIME_THRESHOLD {
            log::warn!(
                "update_shard_blocks lock took longer than expected: {}ms / 1ms",
                now.elapsed().as_millis(),
            );
        }

        self.last_mc_seq_no.store(last_mc_state.block_id().seq_no(), Ordering::Release);

        log::trace!("update_shard_blocks last_mc_state {} removed: {}", 
            last_mc_state.block_id(), removed_list.string().unwrap_or_default());
        Ok(())
    }

    fn send_to_storage(&self, action: StoreAction) {
        if let Some(storage_sender) = self.storage_sender.as_ref() {
            match storage_sender.send(action) {
                Ok(_) => log::trace!("ShardBlocksPool::send_to_storage: sent"),
                Err(err) => log::error!("ShardBlocksPool::send_to_storage: can't send {}", err),
            }
        }
    }
}

pub fn resend_top_shard_blocks_worker(engine: Arc<dyn EngineOperations>) {
    tokio::spawn(async move {
        engine.acquire_stop(Engine::MASK_SERVICE_TOP_SHARDBLOCKS_SENDER);
        loop {
            if engine.check_stop() {
                break
            }
            // 2..3 seconds
            let delay = rand::thread_rng().gen_range(2000, 3000);
            futures_timer::Delay::new(Duration::from_millis(delay)).await;
            match resend_top_shard_blocks(engine.deref()).await {
                Ok(_) => log::trace!("resend_top_shard_blocks: ok"),
                Err(e) => log::error!("resend_top_shard_blocks: {:?}", e)
            }
        }
        engine.release_stop(Engine::MASK_SERVICE_TOP_SHARDBLOCKS_SENDER);
    });
}

async fn resend_top_shard_blocks(engine: &dyn EngineOperations) -> Result<()> {
    let mc_state = engine.load_last_applied_mc_state().await?;

    let tsbs = engine.get_own_shard_blocks(&mc_state).await?;
    for tsb in tsbs {
        engine.send_top_shard_block_description(tsb, 0, true).await?;
    }
    Ok(())
}

pub fn save_top_shard_blocks_worker(
    engine: Arc<dyn EngineOperations>,
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<StoreAction>
) {
    tokio::spawn(async move {
        while let Some(action) = receiver.recv().await {
            match action {
                StoreAction::Save(id, tsb) => {
                    match engine.save_top_shard_block(&id, &tsb) {
                        Ok(_) => log::trace!("save_top_shard_block {}: OK", id),
                        Err(e) => log::error!("save_top_shard_block {}: {:?}", id, e),
                    }
                }
                StoreAction::Remove(id) => {
                    match engine.remove_top_shard_block(&id) {
                        Ok(_) => log::trace!("remove_top_shard_block {}: OK", id),
                        Err(e) => log::error!("remove_top_shard_block {}: {:?}", id, e),
                    }
                }
            }
        }
    });
}

fn prepare_resolver_ctx(
    mc_state: &Arc<ShardStateStuff>,
) -> Result<(TopBlockResolverShardsInfo, TopBlockResolverContext)> {
    let shard_hashes = mc_state.shard_state_extra()?.shards();

    let mut shards = HashMap::new();
    let resolved_block_ids = dashmap::DashMap::default();
    shard_hashes.iterate_shards(|key, value| {
        let min_seqno = value.seq_no + 1;

        if value.before_split {
            // Add shard info for each shard after split
            let (left, right) = key.split()?;

            let (left_collators, right_collators) = value
                .collators
                .map(|c| (Some(c.next), c.next2))
                .unwrap_or_default();

            // Same info for both shards
            let mut info = TopBlockResolverShardInfo {
                cc_seqno: value.next_catchain_seqno,
                min_seqno,
                collators: left_collators,
            };
            shards.insert(left, info.clone());

            info.collators = right_collators;
            shards.insert(right, info);
        } else if value.before_merge {
            // Add only one info for the merged shards

            let key = key.merge()?;
            match shards.entry(key) {
                hash_map::Entry::Occupied(mut entry) => {
                    let merged = entry.get_mut();
                    merged.min_seqno = std::cmp::max(merged.min_seqno, min_seqno);
                    merged.cc_seqno = std::cmp::max(merged.cc_seqno, value.next_catchain_seqno) + 1;
                }
                hash_map::Entry::Vacant(entry) => {
                    entry.insert(TopBlockResolverShardInfo {
                        cc_seqno: value.next_catchain_seqno,
                        min_seqno,
                        collators: value.collators.map(|c| c.next),
                    });
                }
            }
        } else {
            shards.insert(key.clone(), TopBlockResolverShardInfo {
                cc_seqno: value.next_catchain_seqno,
                min_seqno,
                collators: value.collators.map(|c| if min_seqno <= c.current.finish {
                    c.current
                } else {
                    c.next
                }),
            });
        }

        // Applied blocks are resolved by definition
        resolved_block_ids.insert((key, value.seq_no), BlockIdPart {
            root_hash: value.root_hash,
            file_hash: value.file_hash,
        });

        Ok(true)
    })?;

    let mc_state = mc_state.clone();
    let shards = TopBlockResolverShardsInfo { mc_state, shards };
    let ctx = TopBlockResolverContext {
        resolved_block_ids,
        block_flags: Default::default(),
    };

    Ok((shards, ctx))
}

struct TopBlockResolverContext {
    resolved_block_ids: dashmap::DashMap<(ShardIdent, u32), BlockIdPart>,
    block_flags: dashmap::DashMap<BlockIdExt, Option<BlockFlags>>,
}

impl TopBlockResolverContext {
    async fn resolve_shard_blocks(
        &self,
        shard_ident: &ShardIdent,
        engine: &dyn EngineOperations,
        shards: &TopBlockResolverShardsInfo,
        shard_blocks: &HashMap<TopBlockDescrGroup, ShardBlocksPoolItems>,
    ) -> Option<Arc<TopBlockDescrStuff>> {
        enum NextCandidate {
            Leaf,
            Found(Arc<TopBlockDescrStuff>),
        }

        // Prepare an iterator over possible candidates
        let (group, info) = shards.get_group_and_info(shard_ident)?;
        let top_blocks = shard_blocks.get(&group)?;
        let mut tbds_candidates = top_blocks
            .items
            .range(info.allowed_range()?)
            .map(|(_, value)| value);

        let mut result = None;
        let mut stack = Vec::<RefShardBlocksIter>::new();

        // Iterate for each possible candidate, starting from the earliest
        'candidates: while let Some(candidate) = tbds_candidates.next() {
            let candidate = &candidate.top_block;

            // Fast path for already applied candidates
            match self.is_resolved(candidate.proof_for()) {
                // Found the exact resolved block
                Some(true) => {
                    result = Some(candidate);
                    continue;
                }
                // Fork happened
                None => break,
                // Block hasn't beed resolved yet
                _ => {},
            }

            // Check candidate with the latest masterchain state
            if !shards.prevalidate(&candidate) {
                break;
            }

            // Yield after each `prevalidate`, because it takes ~100mcs
            tokio::task::yield_now().await;

            // Reset and reuse the stack of graph nodes
            stack.clear();
            // Start DFS traversing from the candidate itself
            stack.push(candidate.clone().into());

            while let Some(iter) = stack.last_mut() {
                // Get next reference from the deepest (current) graph node
                let Some(ref_block_id) = iter.next() else {
                    // Mark as applied if there are no more references to check.
                    // (The oldest candidates will be marked first)
                    match self.mark_as_resolved(iter.tbds.proof_for().clone()) {
                        Ok(()) => {
                            stack.pop();
                            continue;
                        },
                        Err(e) => {
                            log::error!("Failed to mark block as resolved: {e:?}");
                            break 'candidates;
                        }
                    }
                };

                // Don't visit already applied tbds
                match self.is_resolved(ref_block_id) {
                    // Found the exact resolved block
                    Some(true) => continue,
                    // Fork happened
                    None => break 'candidates,
                    // Block hasn't beed resolved yet
                    _ => {}
                }

                // Search for the top block description
                let tbds = 'tbds: {
                    let group_and_info = shards.get_group_and_info(ref_block_id.shard());

                    // Handle old blocks case
                    if group_and_info.is_none() && shards.is_old_block(ref_block_id) {
                        break 'tbds if matches!(
                            self.get_block_flags(engine, ref_block_id),
                            Some(flags) if flags.is_applied
                        ) {
                            Some(NextCandidate::Leaf)
                        } else {
                            None
                        }
                    }

                    group_and_info.and_then(|(group, info)| {
                        let is_old_block = ref_block_id.seq_no < info.min_seqno;

                        if !matches!(
                            self.get_block_flags(engine, ref_block_id),
                            Some(flags) if
                                is_old_block && flags.is_applied ||
                                !is_old_block && flags.has_state
                        ) {
                            return None;
                        }

                        if is_old_block {
                            // Referenced block id is not later than last applied block for this shard
                            return Some(NextCandidate::Leaf);
                        }

                        // Search for the specified top block description
                        shard_blocks
                            .get(&group)?
                            .items
                            .get(&ref_block_id.seq_no)
                            .map(|group| NextCandidate::Found(group.top_block.clone()))
                    })
                };

                match tbds {
                    // We found a new node to resolve
                    Some(NextCandidate::Found(tbds)) => {
                        if shards.prevalidate(&tbds) {
                            // Yield after each `prevalidate`, because it takes ~100mcs
                            tokio::task::yield_now().await;
                
                            stack.push(tbds.into())
                        } else {
                            // We found an invalid candidate
                            break 'candidates;
                        }
                    }
                    // We found a reference which was already applied
                    Some(NextCandidate::Leaf) => {
                        match self.mark_as_resolved(ref_block_id.clone()) {
                            Ok(()) => continue,
                            Err(e) => {
                                log::error!("Failed to mark block as resolved: {e:?}");
                                break 'candidates;
                            }
                        }
                    },
                    // We found a candidate which has a hole in its references tree
                    None => {
                        break 'candidates;
                    }
                }
            }

            // We can get to this point only if there were no holes in the references tree
            result = Some(candidate);
        }

        // Return the latest known candidate
        result.cloned()
    }

    fn is_resolved(&self, block_id: &BlockIdExt) -> Option<bool> {
        match self.resolved_block_ids.get(&(block_id.shard().clone(), block_id.seq_no())) {
            Some(part) => {
                if part.root_hash == block_id.root_hash && part.file_hash == block_id.file_hash {
                    // There is a resolved block with the exact full id
                    Some(true)
                } else {
                    // There is some block but with a different id. It indicates that there were
                    // some forks in shards and we must not include such block.
                    None
                }
            },
            None => Some(false),
        }
    }

    fn mark_as_resolved(&self, block_id: BlockIdExt) -> Result<()> {
        use dashmap::mapref::entry::Entry;
        match self.resolved_block_ids.entry((block_id.shard_id, block_id.seq_no)) {
            Entry::Vacant(entry) => {
                entry.insert(BlockIdPart {
                    root_hash: block_id.root_hash,
                    file_hash: block_id.file_hash,
                });
                Ok(())
            }
            Entry::Occupied(entry)  => {
                let existing = entry.get();
                if existing.root_hash == block_id.root_hash && existing.file_hash == block_id.file_hash {
                    Ok(())
                } else {
                    fail!(
                        "Trying to replace an already resolved block {} with a different block {} for {}: {}",
                        existing.root_hash,
                        block_id.root_hash,
                        entry.key().0,
                        entry.key().1,
                    )
                }
            }
        }
    }

    fn get_block_flags(
        &self,
        engine: &dyn EngineOperations,
        block_id: &BlockIdExt,
    ) -> Option<BlockFlags> {
        use dashmap::mapref::entry::Entry;

        if let Some(flags) = self.block_flags.get(block_id) {
            return *flags;
        }

        // NOTE: "error" state must also be cached
        let flags = engine.load_block_handle(block_id).ok().flatten().map(|handle| {
            BlockFlags {
                is_applied: handle.is_applied(),
                has_state: handle.has_state(),
            }
        });

        match self.block_flags.entry(block_id.clone()) {
            // Ignore new value if previous already exists
            Entry::Occupied(entry) => *entry.get(),
            // Cache flags if it is the first time we fetched them
            Entry::Vacant(entry) => *entry.insert(flags),
        }
    }
}

// TODO: wrap flags in BlockMeta to ensure atomicity across multiple flags.
#[derive(Copy, Clone)]
struct BlockFlags {
    is_applied: bool,
    has_state: bool,
}

struct BlockIdPart {
    root_hash: ton_types::UInt256,
    file_hash: ton_types::UInt256,
}

struct TopBlockResolverShardsInfo {
    mc_state: Arc<ShardStateStuff>,
    shards: HashMap<ShardIdent, TopBlockResolverShardInfo>,
}

impl TopBlockResolverShardsInfo {
    fn get_group_and_info(
        &self,
        shard_ident: &ton_block::ShardIdent,
    ) -> Option<(TopBlockDescrGroup, &TopBlockResolverShardInfo)> {
        let info = self.shards.get(shard_ident)?;

        let group = TopBlockDescrGroup {
            shard_ident: shard_ident.clone(),
            cc_seqno: info.cc_seqno,
        };

        Some((group, info))
    }

    fn is_old_block(&self, block_id: &BlockIdExt) -> bool {
        for (shard, info) in &self.shards {
            if block_id.shard().intersect_with(shard) && block_id.seq_no < info.min_seqno {
                return true;
            }
        }
        false
    }

    fn prevalidate(&self, tbds: &TopBlockDescrStuff) -> bool {
        // NOTE: `prevalidate` is more strict than `validate`
        let mut res_flags = 0;
        match tbds.prevalidate(
            self.mc_state.block_id(),
            &self.mc_state,
            TbdMode::FAIL_NEW | TbdMode::FAIL_TOO_NEW,
            &mut res_flags,
        ) {
            Ok(_) => true,
            // We found an invalid candidate
            Err(e) => {
                log::debug!(
                    "ShardTopBlockDescr for {} skipped: res_flags = {}, error: {}",
                    tbds.proof_for(), res_flags, e
                );
                false
            }
        }
    }
}

#[derive(Debug, Clone)]
struct TopBlockResolverShardInfo {
    cc_seqno: u32,
    min_seqno: u32,
    collators: Option<CollatorRange>,
}

impl TopBlockResolverShardInfo {
    fn allowed_range(&self) -> Option<AllowedRange> {
        let max_seqno = self
            .collators
            .as_ref()
            .map(|collators| collators.finish);

        if matches!(max_seqno, Some(max_seqno) if max_seqno < self.min_seqno) {
            None
        } else {
            Some(AllowedRange {
                min_seqno: self.min_seqno,
                max_seqno,
            })
        }
    }
}

#[derive(Copy, Clone)]
struct AllowedRange {
    min_seqno: u32,
    max_seqno: Option<u32>,
}

impl std::ops::RangeBounds<u32> for AllowedRange {
    fn start_bound(&self) -> std::ops::Bound<&u32> {
        std::ops::Bound::Included(&self.min_seqno)
    }

    fn end_bound(&self) -> std::ops::Bound<&u32> {
        match &self.max_seqno {
            Some(max_seqno) => std::ops::Bound::Included(max_seqno),
            None => std::ops::Bound::Unbounded,
        }
    }
}

/// A lending iterator around shared ids list.
struct RefShardBlocksIter {
    tbds: Arc<TopBlockDescrStuff>,
    index: usize,
}

impl RefShardBlocksIter {
    fn next(&mut self) -> Option<&BlockIdExt> {
        let result = self.tbds.ref_shard_blocks().get(self.index);
        self.index += result.is_some() as usize;
        result.map(|(id, _)| id)
    }
}

impl From<Arc<TopBlockDescrStuff>> for RefShardBlocksIter {
    fn from(tbds: Arc<TopBlockDescrStuff>) -> Self {
        Self {
            tbds,
            index: 0,
        }
    }
}
