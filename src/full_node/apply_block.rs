use crate::{
    block::{BlockStuff},
    engine_traits::EngineOperations,
    shard_state::ShardStateStuff,
    db::BlockHandle,
};

use std::{ops::Deref, sync::Arc};
use ton_types::Result;
use ton_block::{BlockIdExt, AccountIdPrefixFull};

pub async fn apply_block(handle: &BlockHandle, block: &BlockStuff, engine: &Arc<dyn EngineOperations>) -> Result<()> {

    let prev_ids = block.construct_prev_id()?;

    check_prev_blocks(&prev_ids, engine).await?;

    let shard_state = if handle.state_inited() {
        engine.load_state(handle.id()).await?
    } else {
        calc_shard_state(handle, block, &prev_ids, engine).await?
    };

    set_next_prev_ids(&handle, block.id(), &prev_ids, engine.deref()).await?;

    engine.process_block_in_ext_db(handle, &block, None, &shard_state).await?;

    Ok(())
}

// Checks is prev block(s) applied and apply if need
async fn check_prev_blocks(
    prev_ids: &(BlockIdExt, Option<BlockIdExt>),
    engine: &Arc<dyn EngineOperations>) 
-> Result<()> {
    match prev_ids {
        (prev1_id, Some(prev2_id)) => {
            let prev1_handle = engine.load_block_handle(&prev1_id)?;
            let prev2_handle = engine.load_block_handle(&prev2_id)?;
            let mut apply_prev_futures = Vec::with_capacity(2);
            if !prev1_handle.applied() {
                apply_prev_futures.push(
                    engine.clone().apply_block(prev1_handle.deref(), None)
                );
            }
            if !prev2_handle.applied() {
                apply_prev_futures.push(
                    engine.clone().apply_block(prev2_handle.deref(), None)
                );
            }
            futures::future::join_all(apply_prev_futures)
                .await
                .into_iter()
                .find(|r| r.is_err())
                .unwrap_or(Ok(()))?;
        },
        (prev_id, None) => {
            let prev_handle = engine.load_block_handle(&prev_id)?;
            if !prev_handle.applied() {
                engine.clone().apply_block(prev_handle.deref(), None).await?;
            }
        }
    }
    Ok(())
}

// Gets prev block(s) state and applies merkle update from block to calculate new state
pub async fn calc_shard_state(
    handle: &BlockHandle,
    block: &BlockStuff,
    prev_ids: &(BlockIdExt, Option<BlockIdExt>),
    engine: &Arc<dyn EngineOperations>
) -> Result<ShardStateStuff> {
    log::trace!("wait_shard_state: block: {}", block.id());

    let prev_ss_root = match prev_ids {
        (prev1, Some(prev2)) => {
            let ss1 = engine.load_state(&prev1).await?;
            let ss2 = engine.load_state(&prev2).await?;
            ShardStateStuff::construct_split_root(ss1, ss2)?
        },
        (prev, None) => {
            engine.load_state(&prev).await?
                .root_cell()
                .clone()
        }
    };

    let merkle_update = block
        .block()
        .read_state_update()?;
    let block_id = block.id().clone();

    let ss = tokio::task::spawn_blocking(move || -> Result<ShardStateStuff> {
        let now = std::time::Instant::now();
        let ss_root = merkle_update.apply_for(&prev_ss_root)?;
        log::trace!("TIME: wait_shard_state: applied Merkle update {}ms   {}",
            now.elapsed().as_millis(), block_id);
        ShardStateStuff::new(block_id.clone(), ss_root)
    }).await??;

    engine.store_state(handle, &ss).await?;

    if block.is_key_block()? {
        let prev_key_block_seqno = block.block().read_info()?.prev_key_block_seqno();
        let mc_pfx = AccountIdPrefixFull::any_masterchain();
        let prev_key_block_handle = engine.find_block_by_seq_no(&mc_pfx, prev_key_block_seqno).await?;
        if engine.is_persistent_state(handle.gen_utime()?, prev_key_block_handle.gen_utime()?) {
            engine.clone().store_persistent_state(ss.clone()).await?;
        }
    }

    Ok(ss)
}

// Sets next block link for prev. block and prev. for current one
pub async fn set_next_prev_ids(
    handle: &BlockHandle,
    id: &BlockIdExt,
    prev_ids: &(BlockIdExt, Option<BlockIdExt>),
    engine: &dyn EngineOperations
) -> Result<()> {
    match prev_ids {
        (prev_id1, Some(prev_id2)) => {
            // After merge
            let prev_handle1 = engine.load_block_handle(&prev_id1)?;
            engine.store_block_next1(&prev_handle1, id).await?;

            let prev_handle2 = engine.load_block_handle(&prev_id1)?;
            engine.store_block_next1(&prev_handle2, id).await?;

            engine.store_block_prev(handle, &prev_id1).await?;
            engine.store_block_prev2(handle, &prev_id2).await?;
        },
        (prev_id, None) => {
            // if after split and it is second ("1" branch) shard - set next2 for prev block
            let prev_shard = prev_id.shard().clone();
            let shard = id.shard().clone();
            let prev_handle = engine.load_block_handle(&prev_id)?;
            if prev_shard != shard && prev_shard.split()?.1 == shard {
                engine.store_block_next2(&prev_handle, id).await?;
            } else {
                engine.store_block_next1(&prev_handle, id).await?;
            }
            engine.store_block_prev(handle, &prev_id).await?;
        }
    }
    Ok(())
}