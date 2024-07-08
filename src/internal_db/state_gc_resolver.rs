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

use storage::shardstate_db_async::AllowStateGcResolver;
use ever_block::{BlockIdExt, ShardIdent};
use ever_block::{Result, fail};
use adnl::common::add_unbound_object_to_map_with_update;
use crate::engine_traits::EngineOperations;
use std::{
    sync::atomic::{AtomicU32, Ordering},
    collections::{HashSet, HashMap},
};

pub struct AllowStateGcSmartResolver {
    last_processed_block: AtomicU32,
    min_ref_mc_block: AtomicU32,
    min_actual_ss: lockfree::map::Map<ShardIdent, AtomicU32>,
    min_mesh_mc_block: lockfree::map::Map<i32, AtomicU32>,
    life_time_sec: u64,
    pinned_roots: parking_lot::RwLock<HashMap<BlockIdExt, u32>>,
}

impl AllowStateGcSmartResolver {
    pub fn new(life_time_sec: u64) -> Self {
        Self {
            last_processed_block: AtomicU32::new(0),
            min_ref_mc_block: AtomicU32::new(0),
            min_actual_ss: lockfree::map::Map::new(),
            min_mesh_mc_block: lockfree::map::Map::new(),
            life_time_sec,
            pinned_roots: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    pub async fn advance(&self, mc_block_id: &BlockIdExt, engine: &dyn EngineOperations) -> Result<bool> {
        let seqno = mc_block_id.seq_no();
        if seqno <= self.last_processed_block.fetch_max(seqno, Ordering::Relaxed) {
            return Ok(false)
        }

        let mc_state = engine.load_state(mc_block_id).await?;
        let new_min_ref_mc_seqno = mc_state.state()?.min_ref_mc_seqno();
        let old_min_ref_mc_seqno = self.min_ref_mc_block.fetch_max(new_min_ref_mc_seqno, Ordering::Relaxed);

        log::trace!(
            "AllowStateGcSmartResolver::advance:  new_min_ref_mc_seqno {}  old_min_ref_mc_seqno {}",
            new_min_ref_mc_seqno, old_min_ref_mc_seqno,
        );

        if new_min_ref_mc_seqno > old_min_ref_mc_seqno {
            log::debug!(
                "AllowStateGcSmartResolver::advance: updated min_ref_mc_block {} -> {}, mc_block_id: {}",
                old_min_ref_mc_seqno, new_min_ref_mc_seqno, mc_block_id,
            );

            if let Ok(handle) = engine.find_mc_block_by_seq_no(new_min_ref_mc_seqno).await {
                let min_mc_state = engine.load_state(handle.id()).await?;

                let top_blocks = min_mc_state.shard_hashes()?.top_blocks_all()?;
                let mut actual_shardes = HashSet::new();
                for id in top_blocks {
                    add_unbound_object_to_map_with_update(
                        &self.min_actual_ss,
                        id.shard().clone(),
                        |found| if let Some(a) = found {
                            let old_val = a.fetch_max(id.seq_no(), Ordering::Relaxed);
                            if old_val != id.seq_no() {
                                log::debug!(
                                    "AllowStateGcSmartResolver::advance: updated min actual state for shard {}: {} -> {}",
                                    id.shard(), old_val, id.seq_no()
                                );
                            }
                            Ok(None)
                        } else {
                            log::debug!(
                                "AllowStateGcSmartResolver::advance: added min actual state for shard {}: {}",
                                id.shard(), id.seq_no()
                            );
                            Ok(Some(AtomicU32::new(id.seq_no())))
                        }
                    )?;
                    actual_shardes.insert(id.shard().clone());
                }

                for kv in self.min_actual_ss.iter() {
                    if !actual_shardes.contains(kv.key()) {
                        self.min_actual_ss.remove(kv.key());
                        log::debug!(
                            "AllowStateGcSmartResolver::advance: removed shard {}", kv.key()
                        );
                    }
                }

                let top_mesh_blocks = min_mc_state.mesh_top_blocks()?;
                for (nw_id, id) in top_mesh_blocks {
                    add_unbound_object_to_map_with_update(
                        &self.min_mesh_mc_block,
                        nw_id,
                        |found| if let Some(a) = found {
                            let old_val = a.fetch_max(id.seq_no(), Ordering::Relaxed);
                            if old_val != id.seq_no() {
                                log::info!(
                                    "AllowStateGcSmartResolver::advance: updated min mesh block for network {}: {} -> {}",
                                    nw_id, old_val, id.seq_no()
                                );
                            }
                            Ok(None)
                        } else {
                            log::info!(
                                "AllowStateGcSmartResolver::advance: added min mesh block for network {}: {}",
                                nw_id, id.seq_no()
                            );
                            Ok(Some(AtomicU32::new(id.seq_no())))
                        }
                    )?;
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn pin_state(&self, block_id: &BlockIdExt, saved_at: u64, gc_utime: u64) -> Result<bool> {
        let mut pinned_roots = self.pinned_roots.write();
        let allow = AllowStateGcSmartResolver::allow_state_gc(self, 0, block_id, saved_at, gc_utime)?;
        if allow {
            return Ok(false);
        }
        pinned_roots.entry(block_id.clone())
            .and_modify(|e| {
                log::trace!("AllowStateGcSmartResolver::pin_state: increment pin counter for {}", block_id);
                *e += 1;
            })
            .or_insert_with(|| {
                log::trace!("AllowStateGcSmartResolver::pin_state: pinned {}", block_id);
                1
            });
        Ok(true)
    }

    pub fn add_pin_for_state(&self, block_id: &BlockIdExt) -> Result<()> {
        if let Some(counter) = self.pinned_roots.write().get_mut(block_id) {
            *counter += 1;
            log::trace!("AllowStateGcSmartResolver::pin_state: added pin for {}", block_id);
        } else {
            fail!("Can't add pin for state {block_id} - it is not pinned");
        }
        Ok(())
    }

    pub fn unpin_state(&self, block_id: &BlockIdExt) -> Result<()> {
        let mut pinned_roots = self.pinned_roots.write();
        let counter = if let Some(counter) = pinned_roots.get_mut(block_id) {
            *counter -= 1;
            *counter
        } else {
            fail!("Can't unpin state - it is not pinned");
        };
        if counter == 0 {
            pinned_roots.remove(block_id);
            log::trace!("AllowStateGcSmartResolver::unpin_state: unpinned {}", block_id);
        } else {
            log::trace!("AllowStateGcSmartResolver::unpin_state: decrement pin counter for {}", block_id);
        }
        Ok(())
    }

    fn allow_state_gc(
        &self,
        nw_id: i32, // connected network id; zero for own.
        block_id: &BlockIdExt,
        saved_at: u64,
        gc_utime: u64
    ) -> Result<bool> {
        if gc_utime > saved_at && gc_utime - saved_at < self.life_time_sec {
            return Ok(false)
        }
        if nw_id == 0 {
            if block_id.shard().is_masterchain() {
                if block_id.seq_no() != 0 { // we need zerostate
                    let min_ref_mc_block = self.min_ref_mc_block.load(Ordering::Relaxed);
                    return Ok(block_id.seq_no() < min_ref_mc_block);
                }
            } else {
                if let Some(kv) = self.min_actual_ss.get(block_id.shard()) {
                    let min_actual = kv.val().load(Ordering::Relaxed);
                    return Ok(block_id.seq_no() < min_actual);
                } else {
                    for kv in self.min_actual_ss.iter() {
                        if block_id.shard().intersect_with(kv.key()) {
                            let min_actual = kv.val().load(Ordering::Relaxed); 
                            return Ok(block_id.seq_no() < min_actual);
                        }
                    }
                }
            }
        } else {
            if !block_id.shard().is_masterchain() {
                fail!("Non-masterchain block id is not supported for a connected network (mesh)")
            } else {
                if let Some(kv) = self.min_mesh_mc_block.get(&nw_id) {
                    let min_mesh = kv.val().load(Ordering::Relaxed);
                    return Ok(block_id.seq_no() < min_mesh);
                }
            }
        }
        Ok(false)
    }
}

impl AllowStateGcResolver for AllowStateGcSmartResolver {
    fn allow_state_gc(
        &self,
        nw_id: i32, // connected network id; zero for own.
        block_id: &BlockIdExt,
        saved_at: u64,
        gc_utime: u64
    ) -> Result<bool> {
        let old = AllowStateGcSmartResolver::allow_state_gc(self, nw_id, block_id, saved_at, gc_utime)?;
        let pinned = nw_id == 0 &&
                     self.pinned_roots.read().get(block_id).map(|c| *c > 0).unwrap_or(false);
        Ok(old && !pinned)
    }
}