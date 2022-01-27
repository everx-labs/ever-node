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
    block::BlockStuff, engine_traits::EngineOperations, shard_state::ShardStateStuff
};
use std::{ops::Deref, sync::Arc};
use storage::block_handle_db::BlockHandle;
use ton_types::{error, fail, Result};
use ton_block::BlockIdExt;

pub const MAX_RECURSION_DEPTH: u32 = 16;

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
    let prev_ids = block.construct_prev_id()?;    
    check_prev_blocks(&prev_ids, engine, mc_seq_no, pre_apply, recursion_depth).await?;
    let (shard_state, prev_states) = if handle.has_state() {
        let shard_state = engine.load_state(handle.id()).await?;
        let prev1 = engine.load_state(&prev_ids.0).await?;
        let prev2 = if let Some(id) = &prev_ids.1 {
            Some(engine.load_state(id).await?)
        } else {
            None
        };
        (shard_state, (prev1, prev2))
    } else {
        calc_shard_state(handle, block, &prev_ids, engine).await?
    };
    if !pre_apply {
        set_next_prev_ids(&handle, &prev_ids, engine.deref())?;
        engine.process_block_in_ext_db(
            handle, &block, None, &shard_state, (&prev_states.0, prev_states.1.as_ref()), mc_seq_no
        ).await?;
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

    log::trace!("calc_shard_state: block: {}", block.id());

    let (prev_ss_root, prev_ss) = match prev_ids {
        (prev1, Some(prev2)) => {
            let ss1 = engine.clone().wait_state(prev1, None, true).await?.root_cell().clone();
            let ss2 = engine.clone().wait_state(prev2, None, true).await?.root_cell().clone();

            let root = ShardStateStuff::construct_split_root(ss1.clone(), ss2.clone())?;
            let ss1 = ShardStateStuff::from_root_cell(
                prev1.clone(), 
                ss1,
                #[cfg(feature = "telemetry")]
                engine.engine_telemetry(),
                engine.engine_allocated()
            )?;
            let ss2 = ShardStateStuff::from_root_cell(
                prev2.clone(), 
                ss2,
                #[cfg(feature = "telemetry")]
                engine.engine_telemetry(),
                engine.engine_allocated()
            )?;
            (root, (ss1, Some(ss2)))
        },
        (prev, None) => {
            let root = engine.clone().wait_state(prev, None, true).await?.root_cell().clone();
            let ss = ShardStateStuff::from_root_cell(
                prev.clone(), 
                root.clone(),
                #[cfg(feature = "telemetry")]
                engine.engine_telemetry(),
                engine.engine_allocated()
            )?;
            (root, (ss, None))
        }
    };

    let merkle_update = block.block().read_state_update()?;
    let block_id = block.id().clone();
    let engine_cloned = engine.clone();

    let ss = tokio::task::spawn_blocking(
        move || -> Result<Arc<ShardStateStuff>> {
            let now = std::time::Instant::now();
            let ss_root = merkle_update.apply_for(&prev_ss_root)?;
            log::trace!("TIME: calc_shard_state: applied Merkle update {}ms   {}",
                now.elapsed().as_millis(), block_id);
            ShardStateStuff::from_root_cell(
                block_id.clone(), 
                ss_root,
                #[cfg(feature = "telemetry")]
                engine_cloned.engine_telemetry(),
                engine_cloned.engine_allocated()
            )
        }
    ).await??;

    let now = std::time::Instant::now();
    let ss = engine.store_state(handle, ss, None).await?;
    log::trace!("TIME: calc_shard_state: store_state {}ms   {}",
            now.elapsed().as_millis(), handle.id());
    Ok((ss, prev_ss))

}

// Sets next block link for prev. block and prev. for current one
pub fn set_next_prev_ids(
    handle: &Arc<BlockHandle>,
    prev_ids: &(BlockIdExt, Option<BlockIdExt>),
    engine: &dyn EngineOperations
) -> Result<()> {
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
            engine.store_block_prev1(handle, &prev_id1)?;
            engine.store_block_prev2(handle, &prev_id2)?;
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
            engine.store_block_prev1(handle, &prev_id)?;
        }
    }
    Ok(())
}
