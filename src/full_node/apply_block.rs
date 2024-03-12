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
    block::BlockStuff, engine_traits::EngineOperations, shard_state::ShardStateStuff,
    validating_utils::{UNREGISTERED_CHAIN_MAX_LEN, fmt_block_id_short}
};
use std::{ops::Deref, sync::Arc, time::Instant};
use storage::block_handle_db::BlockHandle;
use ton_types::{error, fail, Result};
use ton_block::{BlockIdExt, MerkleProof, Deserializable, Serializable, ShardIdent, ConnectedNwOutDescr, OutMsgQueueInfo};

pub const MAX_RECURSION_DEPTH: u32 = UNREGISTERED_CHAIN_MAX_LEN * 2;

pub async fn apply_block(
    handle: &Arc<BlockHandle>,
    block: &BlockStuff,
    mc_seq_no: u32,
    engine: &Arc<dyn EngineOperations>,
    pre_apply: bool,
    recursion_depth: u32
) -> Result<()> {
    if handle.id() != block.id() {
        fail!("Block id mismatch in apply block: {} vs {}", handle.id(), block.id())
    }
    log::trace!("apply_block: block: {}", handle.id());

    let prev_ids = block.construct_prev_id()?;
    check_prev_blocks(&prev_ids, engine, mc_seq_no, pre_apply, recursion_depth).await?;

    if handle.is_queue_update() {
        calc_out_msg_queue(handle, block, &prev_ids, engine).await?;
        set_prev_ids(&handle, &prev_ids, engine.deref())?;
        set_next_ids(&handle, &prev_ids, engine.deref())?;
    } else if handle.is_mesh() {
        calc_mesh_queues(handle, block, &prev_ids, engine).await?;
        set_prev_ids(&handle, &prev_ids, engine.deref())?;
        set_next_ids(&handle, &prev_ids, engine.deref())?;
    } else {
        if !handle.has_state() {
            calc_shard_state(handle, block, &prev_ids, engine).await?;
        }
        set_prev_ids(&handle, &prev_ids, engine.deref())?;
        if !pre_apply {
            set_next_ids(&handle, &prev_ids, engine.deref())?;
        }
    }
    Ok(())
}

// Checks is prev block(s) applied and apply if need
async fn check_prev_blocks(
    prev_ids: &(BlockIdExt, Option<BlockIdExt>),
    engine: &Arc<dyn EngineOperations>,
    mc_seq_no: u32,
    pre_apply: bool,
    recursion_depth: u32
) -> Result<()> {
    match prev_ids {
        (prev1_id, Some(prev2_id)) => {
            let mut apply_prev_futures = Vec::with_capacity(2);
            apply_prev_futures.push(
                engine.clone().download_and_apply_block_internal(&prev1_id, mc_seq_no, pre_apply, recursion_depth + 1)
            );
            apply_prev_futures.push(
                engine.clone().download_and_apply_block_internal(&prev2_id, mc_seq_no, pre_apply, recursion_depth + 1)
            );
            futures::future::join_all(apply_prev_futures)
                .await
                .into_iter()
                .find(|r| r.is_err())
                .unwrap_or(Ok(()))?;
        },
        (prev_id, None) => {
            engine.clone().download_and_apply_block_internal(&prev_id, mc_seq_no, pre_apply, recursion_depth + 1).await?;
        }
    }
    Ok(())
}

// Gets prev block(s) state and applies merkle update from block to calculate new state
pub async fn calc_shard_state(
    handle: &Arc<BlockHandle>,
    block: &BlockStuff,
    prev_ids: &(BlockIdExt, Option<BlockIdExt>),
    engine: &Arc<dyn EngineOperations>
) -> Result<(Arc<ShardStateStuff>, (Arc<ShardStateStuff>, Option<Arc<ShardStateStuff>>))> {
    let block_descr = fmt_block_id_short(block.id());

    log::trace!("({}): calc_shard_state: block: {}", block_descr, block.id());

    let (prev_ss_root, prev_ss) = match prev_ids {
        (prev1, Some(prev2)) => {
            let ss1 = engine.clone().wait_state(prev1, None, true).await?;
            let ss2 = engine.clone().wait_state(prev2, None, true).await?;
            let root = ShardStateStuff::construct_split_root(
                ss1.root_cell().clone(), 
                ss2.root_cell().clone()
            )?;
            (root, (ss1, Some(ss2)))
        },
        (prev, None) => {
            let ss = engine.clone().wait_state(prev, None, true).await?;
            (ss.root_cell().clone(), (ss, None))
        }
    };

    let merkle_update = block.block()?.read_state_update()?;
    let block_id = block.id().clone();
    let engine_cloned = engine.clone();

    let block_descr_clone = block_descr.clone();
    let ss = tokio::task::spawn_blocking(
        move || -> Result<Arc<ShardStateStuff>> {
            let now = std::time::Instant::now();
            let cf = engine_cloned.db_cells_factory()?;
            let (ss_root, _metrics) = merkle_update.apply_for_with_cells_factory(&prev_ss_root, &cf)
                .map_err(|e| error!(
                    "Error applying Merkle update for block {}: {}\
                    prev_ss_root: {:#.2}\
                    merkle_update: {}",
                    block_id, e, prev_ss_root, merkle_update
                ))?;
            let elapsed = now.elapsed();
            log::trace!("({}): TIME: calc_shard_state: applied Merkle update {}ms   {}",
                block_descr_clone,
                elapsed.as_millis(), block_id);
            #[cfg(feature = "telemetry")]
            if _metrics.loaded_old_cells != 0 {
                let one_cell_time = 
                    _metrics.loaded_old_cells_time.as_nanos() / _metrics.loaded_old_cells as u128;
                engine_cloned.engine_telemetry().old_state_cell_load_time
                    .update(one_cell_time as u64);
            }
            metrics::histogram!("calc_shard_state_merkle_update_time", elapsed);
            ShardStateStuff::from_state_root_cell(
                block_id.clone(), 
                ss_root,
                #[cfg(feature = "telemetry")]
                engine_cloned.engine_telemetry(),
                engine_cloned.engine_allocated()
            )
        }
    ).await??;

    let ss = engine.store_state(handle, ss).await?;
    Ok((ss, prev_ss))
}

// Gets prev block(s) state and applies merkle update from block to calculate new state
pub async fn calc_out_msg_queue(
    handle: &Arc<BlockHandle>,
    block: &BlockStuff,
    prev_ids: &(BlockIdExt, Option<BlockIdExt>),
    engine: &Arc<dyn EngineOperations>
) -> Result<()> {

    log::trace!("calc_out_msg_queue: block: {}", block.id());

    let prev_ss_root = match prev_ids {
        (prev1, Some(prev2)) => {
            let ss1 = engine.clone().wait_state(prev1, None, true).await?;
            let ss2 = engine.clone().wait_state(prev2, None, true).await?;
            let root = ShardStateStuff::construct_split_root(
                MerkleProof::construct_from_cell(ss1.root_cell().clone())?.proof,
                MerkleProof::construct_from_cell(ss2.root_cell().clone())?.proof 
            )?;
            MerkleProof {
                hash: root.hash(0),
                depth: root.depth(0),
                proof: root,
            }.serialize()?
        },
        (prev, None) => engine.clone().wait_state(prev, None, true).await?.root_cell().clone(),
    };
    let target_wc = block.is_queue_update_for()
        .ok_or_else(|| error!("Block {} is not a queue update", block.id()))?;
    let merkle_update = block.get_queue_update_for(target_wc)?.update;
    let block_id = block.id().clone();
    let engine_cloned = engine.clone();

    let ss = tokio::task::spawn_blocking(
        move || -> Result<Arc<ShardStateStuff>> {
            let now = std::time::Instant::now();
            let cf = engine_cloned.db_cells_factory()?;
            let (ss_root, _metrics) = merkle_update.apply_for_with_cells_factory(&prev_ss_root, &cf)?;
            let elapsed = now.elapsed();
            log::trace!("TIME: calc_out_msg_queue: applied Merkle update {}ms   {}",
                elapsed.as_millis(), block_id);
            #[cfg(feature = "telemetry")]
            if _metrics.loaded_old_cells != 0 {
                let one_cell_time = 
                    _metrics.loaded_old_cells_time.as_nanos() / _metrics.loaded_old_cells as u128;
                engine_cloned.engine_telemetry().old_state_cell_load_time
                    .update(one_cell_time as u64);
            }
            metrics::histogram!("calc_out_msg_queue_merkle_update_time", elapsed);
            ShardStateStuff::from_out_msg_queue_root_cell(
                block_id.clone(),
                ss_root,
                target_wc,
                #[cfg(feature = "telemetry")]
                engine_cloned.engine_telemetry(),
                engine_cloned.engine_allocated()
            )
        }
    ).await??;

    let now = std::time::Instant::now();
    engine.store_state(handle, ss).await?;
    log::trace!("TIME: calc_out_msg_queue: store_state {}ms   {}",
            now.elapsed().as_millis(), handle.id());
    metrics::histogram!("calc_out_msg_queue_store_state_time", now.elapsed());
    Ok(())
}

pub async fn calc_mesh_queues(
    _handle: &Arc<BlockHandle>,
    mesh_update: &BlockStuff,
    prev_ids: &(BlockIdExt, Option<BlockIdExt>),
    engine: &Arc<dyn EngineOperations>
) -> Result<()> {

    fn process_one_shard(
        mesh_update: &BlockStuff,
        cn_descr: &ConnectedNwOutDescr,
        src_shard: &ShardIdent,
        prev_ids: &(BlockIdExt, Option<BlockIdExt>),
        engine: &Arc<dyn EngineOperations>
    ) -> Result<()> {
        let old_queue = engine.load_mesh_queue(
            mesh_update.network_global_id(),
            &prev_ids.0,
            &src_shard
        )?;
        let old_queue_root = old_queue.write_to_new_cell()?.into_cell()?;

        let old_hash = old_queue_root.repr_hash();
        if old_hash != cn_descr.out_queue_update.old_hash {
            fail!(
                "INTERNAL ERROR: mesh update {}: old queue hash mismatch for {} ({} != {})", 
                mesh_update.id(), src_shard, old_hash, cn_descr.out_queue_update.old_hash
            );
        }

        let new_queue = if cn_descr.out_queue_update.old_hash != cn_descr.out_queue_update.new_hash {
            let new_queue_root = mesh_update.mesh_update(&src_shard)?.apply_for(&old_queue_root)?;
            let new_hash = new_queue_root.repr_hash();
            if new_queue_root.repr_hash() != cn_descr.out_queue_update.new_hash {
                fail!(
                    "INTERNAL ERROR: mesh update {}: new queue hash mismatch for {} ({} != {})", 
                    mesh_update.id(), src_shard, new_hash, cn_descr.out_queue_update.new_hash
                );
            }
            Arc::new(OutMsgQueueInfo::construct_from_cell(new_queue_root)?)
        } else {
            old_queue
        };

        engine.store_mesh_queue(
            mesh_update.network_global_id(),
            &mesh_update.id(),
            &src_shard,
            new_queue
        )?;

        Ok(())
    }

    log::trace!("calc_mesh_queues: network: {}, mesh update: {}",
        mesh_update.network_global_id(), mesh_update.id());
    let now = Instant::now();

    let host_network_id = engine.network_global_id();

    let cn_descr = mesh_update
        .virt_block()?
        .read_extra()?
        .read_custom()?
        .ok_or_else(|| error!("Mesh update {} doesn't contain extra->custom field", mesh_update.id()))?
        .mesh_descr()
        .get(&host_network_id)?
        .ok_or_else(|| error!("Mesh update {} doesn't contain queue from masterchain to us", mesh_update.id()))?;
    process_one_shard(mesh_update, &cn_descr.queue_descr, &ShardIdent::masterchain(), prev_ids, engine)?;

    mesh_update.shards()?.iterate_shards(|ident, descr| {
        let cn_descr = descr.mesh_msg_queues.get(&host_network_id)?
            .ok_or_else(|| error!("Mesh update {} doesn't contain queue for us", mesh_update.id()))?;
        process_one_shard(mesh_update, &cn_descr, &ident, prev_ids, engine)?;
        Ok(true)
    })?;

    log::trace!("calc_mesh_queues: network: {}, mesh update: {}, DONE TIME: {}ms",
        mesh_update.network_global_id(), mesh_update.id(), now.elapsed().as_millis());
    metrics::histogram!("calc_mesh_queues_time", now.elapsed());

    Ok(())
}

// set next block ids for prev blocks
pub fn set_next_ids(
    handle: &Arc<BlockHandle>,
    prev_ids: &(BlockIdExt, Option<BlockIdExt>),
    engine: &dyn EngineOperations
) -> Result<()> {
    log::trace!("set_next_ids: block: {}", handle.id());
    match prev_ids {
        (prev_id1, Some(prev_id2)) => {
            // After merge
            let prev_handle1 = engine.load_block_handle(&prev_id1)?.ok_or_else(
                || error!("Cannot load handle for prev1 block {}", prev_id1)
            )?;
            engine.store_block_next1(&prev_handle1, handle.id())?;
            let prev_handle2 = engine.load_block_handle(&prev_id2)?.ok_or_else(
                || error!("Cannot load handle for prev2 block {}", prev_id2)
            )?;
            engine.store_block_next1(&prev_handle2, handle.id())?;
        },
        (prev_id, None) => {
            // if after split and it is second ("1" branch) shard - set next2 for prev block
            let prev_shard = prev_id.shard().clone();
            let shard = handle.id().shard().clone();
            let prev_handle = engine.load_block_handle(&prev_id)?.ok_or_else(
                || error!("Cannot load handle for prev block {}", prev_id)
            )?;
            if (prev_shard != shard) && (prev_shard.split()?.1 == shard) {
                engine.store_block_next2(&prev_handle, handle.id())?;
            } else {
                engine.store_block_next1(&prev_handle, handle.id())?;
            }
        }
    }
    Ok(())
}

// Set prev block ids for (pre-)applied block
pub fn set_prev_ids(
    handle: &Arc<BlockHandle>,
    prev_ids: &(BlockIdExt, Option<BlockIdExt>),
    engine: &dyn EngineOperations
) -> Result<()> {
    log::trace!("set_prev_ids: block: {}", handle.id());
    match prev_ids {
        (prev_id1, Some(prev_id2)) => {
            // After merge
            engine.store_block_prev1(handle, &prev_id1)?;
            engine.store_block_prev2(handle, &prev_id2)?;
        },
        (prev_id, None) => {
            engine.store_block_prev1(handle, &prev_id)?;
        }
    }
    Ok(())
}
