use crate::{
    engine::Engine,
    full_node::state_helper::download_persistent_state,
    engine_traits::EngineOperations
};

use std::{sync::Arc, ops::Deref};
use ton_block::{
    self, SHARD_FULL, BlockIdExt, ConfigParamEnum, ShardIdent, BASE_WORKCHAIN_ID,
};
use ton_types::{Result, error, fail};

pub async fn cold_sync_stub(engine: Arc<Engine>) -> Result<BlockIdExt> {

    log::info!("cold_sync_stub");

    // For now we can't read key blocks chain, so start not from zerostate but from last key block


    let mc_overlay = engine.get_masterchain_overlay().await?;
    let shards_overlay = engine.get_full_node_overlay(0, SHARD_FULL).await?;

    let init_mc_block_handle = engine.db().load_block_handle(engine.init_mc_block_id())?;

    let ss = if !init_mc_block_handle.state_inited() {
        let ss = download_persistent_state(engine.init_mc_block_id(), engine.init_mc_block_id(), mc_overlay.deref()).await?;
        engine.db().store_shard_state_dynamic(&init_mc_block_handle, &ss)?;
        engine.process_full_state_in_ext_db(&ss).await?;
        ss
    } else {
        engine.load_state(&init_mc_block_handle).await?
    };

    if init_mc_block_handle.id().seq_no() == 0 {
        engine.db().store_block_applied(init_mc_block_handle.id())?;

        // load workchain zerostate

        let custom = ss
            .shard_state()
            .read_custom()?
            .ok_or_else(|| error!("No custom field in zerostate"))?;
        let cp12 = custom.config.config(12)?.ok_or_else(|| error!("No config param 12 in zerostate"))?;

        if let ConfigParamEnum::ConfigParam12(cp12) = cp12 {
            let wc = cp12.get(0)?.ok_or_else(|| error!("No description for base workchain"))?;

            let zerostate_id = BlockIdExt {
                shard_id: ShardIdent::with_tagged_prefix(BASE_WORKCHAIN_ID, SHARD_FULL)?,
                seq_no: 0,
                root_hash: wc.zerostate_root_hash,
                file_hash: wc.zerostate_file_hash,
            };

            let handle = engine.db().load_block_handle(&zerostate_id)?;

            if !handle.applied() {
                let ss = download_persistent_state(&zerostate_id, engine.init_mc_block_id(), shards_overlay.deref()).await?;
                engine.db().store_shard_state_dynamic(&handle, &ss)?;
                engine.process_full_state_in_ext_db(&ss).await?;
                engine.db().store_block_applied(&zerostate_id)?;
            }

        } else {
            fail!("Can't read config param 12")
        }
    } else {

        // Load master block and state

        let init_mc_block = if !init_mc_block_handle.data_inited() {

            let (block, proof) = loop {
                if let Ok(Some((block, proof))) = mc_overlay.download_block_full(engine.init_mc_block_id(), 300).await {
                    break (block, proof);
                }
            };
            engine.db().store_block_data(&init_mc_block_handle, &block)?;
            engine.db().store_block_proof(&init_mc_block_handle, &proof)?;
            engine.process_block_in_ext_db(&init_mc_block_handle, &block, Some(&proof), &ss).await?;
            engine.db().store_block_applied(engine.init_mc_block_id())?;
            block
        } else {
            engine.db().load_block_data(engine.init_mc_block_id())?
        };

        // Load shards blocks and states

        for (_shard_ident, block_id) in init_mc_block.shards_blocks()? {

            let block_handle = engine.db().load_block_handle(&block_id)?;

            let ss = if !block_handle.state_inited() {
                let ss = download_persistent_state(&block_id, engine.init_mc_block_id(), shards_overlay.deref()).await?;
                engine.db().store_shard_state_dynamic(&block_handle, &ss)?;
                engine.process_full_state_in_ext_db(&ss).await?;
                ss
            } else {
                engine.load_state(&block_handle).await?
            };

            if !block_handle.data_inited() {
                let (block, proof) = loop {
                    if let Ok(Some((block, proof))) = shards_overlay.download_block_full(&block_id, 30).await {
                        break (block, proof);
                    }
                };
                engine.db().store_block_data(&block_handle, &block)?;
                engine.db().store_block_proof(&block_handle, &proof)?;
                engine.process_block_in_ext_db(&block_handle, &block, None, &ss).await?;
                engine.db().store_block_applied(&block_id)?;
            }
        }
    }

    Ok(engine.init_mc_block_id().clone())
}