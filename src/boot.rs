use std::{
    ops::Deref,
    sync::Arc,
    time::Duration,
};
use ton_block::{BlockIdExt, ShardIdent, BASE_WORKCHAIN_ID, SHARD_FULL};
use ton_types::{
    error, fail, Result,
};

use crate::{
    CHECK,
    block::BlockStuff,
    block_proof::BlockProofStuff,
    db::BlockHandle,
    engine_traits::EngineOperations,
    shard_state::ShardStateStuff,
};


/// cold boot entry point
/// download zero state or block proof link and check it
async fn run_cold(engine: &dyn EngineOperations) -> Result<(Arc<BlockHandle>, Option<ShardStateStuff>, Option<BlockProofStuff>)> {
    let block_id = engine.init_mc_block_id();
    log::info!(target: "boot", "cold boot start: init_block_id={}", block_id);
    CHECK!(block_id.shard().is_masterchain());
    CHECK!(block_id.seq_no >= engine.get_last_fork_masterchain_seqno());
    let handle = engine.load_block_handle(&block_id)?;
    if block_id.seq_no() == 0 {
        log::info!(target: "boot", "download zero state {}", block_id);
        let zero_state = download_zero_state(engine, &handle).await?;
        Ok((handle, Some(zero_state), None))
    } else {
        // id should be key block if not it will never sync
        log::info!(target: "boot", "download init block proof link {}", block_id);
        let proof = if handle.proof_link_inited() || handle.proof_inited() {
            match engine.load_block_proof(&handle, true).await {
                Ok(proof) => proof,
                Err(err) => {
                    log::warn!(target: "boot", "load_block_proof for init_block {} error: {}", handle.id(), err);
                    engine.load_block_proof(&handle, false).await?
                }
            }
        } else {
            loop {
                match engine.download_block_proof(&block_id, true, true).await {
                    Ok(proof) => match proof.check_proof_link() {
                        Ok(_) => {
                            engine.store_block_proof(&handle, &proof).await?;
                            break proof
                        }
                        Err(err) => log::warn!(target: "boot", "check_proof_link for init_block {} error: {}", handle.id(), err)
                    }
                    Err(err) => log::warn!(target: "boot", "download block proof link for init_block {} error: {}", handle.id(), err)
                }
                futures_timer::Delay::new(Duration::from_secs(1)).await;
            }
        };
        CHECK!(handle.is_key_block()?);
        Ok((handle, None, Some(proof)))
    }
}

/// download key blocks
/// 1. define time period
/// 2. get next key blocks ids infinitely
/// 3. download key block proofs and check with each other or with zero state for the first
/// 4. check if last key block can be selected for current state
async fn get_key_blocks(
    engine: &dyn EngineOperations,
    mut handle: Arc<BlockHandle>,
    zero_state: Option<&ShardStateStuff>,
    mut prev_block_proof: Option<BlockProofStuff>,
) -> Result<Vec<Arc<BlockHandle>>> {
    let download_new_key_blocks_until = match engine.allow_blockchain_init() {
        true => engine.now() + 60,
        false => engine.now() + 600
    };
    let mut key_blocks = vec!(handle.clone());
    loop {
        log::info!(target: "boot", "download_next_key_blocks_ids {}", handle.id());
        let ids = match engine.download_next_key_blocks_ids(handle.id(), 10).await {
            Err(err) => {
                log::warn!(target: "boot", "download_next_key_blocks_ids {}: {}", handle.id(), err);
                futures_timer::Delay::new(Duration::from_secs(1)).await;
                continue
            }
            Ok(ids) => ids,
        };
        if let Some(block_id) = ids.last() {
            log::info!(target: "boot", "last key block is {}", block_id);
            for block_id in &ids {
                let prev_time = handle.gen_utime()?;
                handle = engine.load_block_handle(block_id)?;
                let proof = download_key_block_proof(engine, &handle, zero_state, prev_block_proof.as_ref()).await?;
                CHECK!(handle.is_key_block()?);
                CHECK!(handle.gen_utime()? != 0);
                if engine.is_persistent_state(handle.gen_utime()?, prev_time) {
                    engine.set_init_mc_block_id(block_id);
                }
                key_blocks.push(handle.clone());
                prev_block_proof = Some(proof);
            }
        }
        if let Some(handle) = key_blocks.last() {
            let utime = handle.gen_utime()?;
            log::info!(target: "boot", "id: {}, utime: {}, now: {}", handle.id(), utime, engine.now());
            CHECK!(utime != 0);
            CHECK!(utime < engine.now());
            if (engine.sync_blocks_before() > engine.now() - utime)
                || (2 * engine.key_block_utime_step() > engine.now() - utime)
                || (engine.allow_blockchain_init() && download_new_key_blocks_until < engine.now()) {
                return Ok(key_blocks)
            }
        }
    }
}

/// choose correct masterchain state
async fn choose_masterchain_state(engine: &dyn EngineOperations, mut key_blocks: Vec<Arc<BlockHandle>>) -> Result<Arc<BlockHandle>> {
    while let Some(handle) = key_blocks.pop() {
        let utime = handle.gen_utime()?;
        let ptime = if let Some(handle) = key_blocks.last() {
            handle.gen_utime()?
        } else {
            0
        };
        log::info!(target: "boot", "key block candidate: seqno={} \
            is_persistent={} ttl={} syncbefore={}", handle.id().seq_no(),
                ptime == 0 || engine.is_persistent_state(utime, ptime),
                engine.persistent_state_ttl(utime),
                engine.sync_blocks_before());
        if engine.sync_blocks_before() > engine.now() - utime {
            log::info!(target: "boot", "ignoring: too new block");
            continue;
        }
        if ptime == 0 || engine.is_persistent_state(utime, ptime) {
            let ttl = engine.persistent_state_ttl(utime);
            let time_to_download = 3600;
            if ttl > engine.now() + time_to_download {
                log::info!(target: "boot", "best handle is {}", handle.id());
                return Ok(handle)
            } else {
                log::info!(target: "boot", "ignoring: state is expiring shortly: expire_at={}", ttl);
            }
        } else {
            log::info!(target: "boot", "ignoring: state is not persistent");
        }
    }
    CHECK!(key_blocks.is_empty());
    fail!("Cannot boot node")
}

/// Download zerostate for base workchain
async fn download_base_wc_zerosate(engine: &dyn EngineOperations, zerostate: &ShardStateStuff) -> Result<()> {
    let cp12 = zerostate.config_params()?.workchains()?;
    let wc = cp12.get(&0)?.ok_or_else(|| error!("No description for base workchain"))?;
    let zerostate_id = BlockIdExt {
        shard_id: ShardIdent::with_tagged_prefix(BASE_WORKCHAIN_ID, SHARD_FULL)?,
        seq_no: 0,
        root_hash: wc.zerostate_root_hash,
        file_hash: wc.zerostate_file_hash,
    };
    let handle = engine.load_block_handle(&zerostate_id)?;
    download_zero_state(engine, &handle).await?;
    Ok(())
}

/// Download persistent master block & state, enumerate shards and download block & state for each
async fn download_start_blocks_and_states(engine: &dyn EngineOperations, handle: &BlockHandle) -> Result<()> {
    let init_mc_block = download_block_and_state(engine, handle.id(), handle.id()).await?;
    CHECK!(handle.state_inited());
    CHECK!(handle.applied());

    for (_shard_ident, block_id) in init_mc_block.shards_blocks()? {
        let shard_handle = engine.load_block_handle(&block_id)?;
        download_block_and_state(engine, &block_id, handle.id()).await?;
        CHECK!(shard_handle.state_inited());
        CHECK!(shard_handle.applied());
    }

    Ok(())
}

/// download zero state and store it
async fn download_zero_state(engine: &dyn EngineOperations, handle: &BlockHandle) -> Result<ShardStateStuff> {
    if handle.state_inited() {
        return engine.load_state(handle.id()).await
    }
    loop {
        match engine.download_state(handle.id(), handle.id()).await {
            Ok(state) => {
                engine.store_state(&handle, &state).await?;
                engine.set_applied(handle.id())?;
                engine.process_full_state_in_ext_db(&state).await?;
                return Ok(state)
            }
            Err(err) => log::warn!(target: "boot", "download_zero_state error: {}", err)
        }
        futures_timer::Delay::new(Duration::from_secs(1)).await;
    }
}

/// download key block proof, check it and store
async fn download_key_block_proof(
    engine: &dyn EngineOperations,
    handle: &BlockHandle,
    zero_state: Option<&ShardStateStuff>,
    prev_block_proof: Option<&BlockProofStuff>,
) -> Result<BlockProofStuff> {
    if let Ok(proof) = engine.load_block_proof(handle, false).await {
        return Ok(proof)
    }
    loop {
        let proof = engine.download_block_proof(handle.id(), false, true).await?;
        let result = if let Some(prev_block_proof) = prev_block_proof {
            proof.check_with_prev_key_block_proof(prev_block_proof)
        } else if let Some(zero_state) = zero_state {
            proof.check_with_master_state(zero_state)
        } else {
            unreachable!("Impossible variant")
        };
        match result {
            Ok(_) => {
                engine.store_block_proof(&handle, &proof).await?;
                return Ok(proof)
            }
            Err(err) => {
                log::warn!(target: "boot", "check_proof error: {}", err);
                futures_timer::Delay::new(Duration::from_secs(1)).await;
            }
        }
    }
}

/// download any state, check its hash and store it, if need to download block and proof we trust them already
async fn download_block_and_state(engine: &dyn EngineOperations, block_id: &BlockIdExt, master_id: &BlockIdExt) -> Result<BlockStuff> {
    let handle = engine.load_block_handle(block_id)?;
    let block = if !handle.data_inited() {
        let (block, proof) = engine.download_block(handle.id()).await?;
        engine.store_block(&handle, &block).await?;
        if !handle.proof_inited() {
            engine.store_block_proof(&handle, &proof).await?;
        }
        block
    } else {
        engine.load_applied_block(&handle).await?
    };
    if !handle.state_inited() {
        engine.store_block(&handle, &block).await?;
        let state_update = block.block().read_state_update()?;
        log::info!(target: "boot", "download state {}", handle.id());
        let state = engine.download_state(handle.id(), master_id).await?;
        let state_hash = state.root_cell().repr_hash();
        if state_update.new_hash != state_hash {
            fail!("root_hash {} of downloaded state {} is wrong", state_hash.to_hex_string(), handle.id())
        }
        engine.store_state(&handle, &state).await?;
        engine.process_full_state_in_ext_db(&state).await?;
    }
    engine.set_applied(handle.id())?;
    Ok(block)
}

/// Cold load best key block and its state
/// Must be used only zero_state or key_block id
pub async fn cold_boot(engine: Arc<dyn EngineOperations>) -> Result<BlockIdExt> {
    // TODO: rewrite hard forks from opts to DB
    // engine.get_hardforks();
    // engine.update_hardforks();
    let (mut handle, zero_state, init_block_proof_link) = run_cold(engine.deref()).await?;
    if !engine.initial_sync_disabled() {
        let key_blocks = get_key_blocks(engine.deref(), handle, zero_state.as_ref(), init_block_proof_link).await?;
        handle = choose_masterchain_state(engine.deref(), key_blocks).await?;
    }
    if handle.id().seq_no() == 0 {
        CHECK!(zero_state.is_some());
        download_base_wc_zerosate(engine.deref(), &zero_state.unwrap()).await?;
    } else {
        download_start_blocks_and_states(engine.deref(), &handle).await?;
    }
    Ok(handle.id().clone())
}

pub async fn warm_boot(engine: Arc<dyn EngineOperations>, mut block_id: BlockIdExt) -> Result<BlockIdExt> {
    let mut handle = engine.load_block_handle(&block_id)?;
    while !handle.applied() { // go back to find last applied block
        CHECK!(handle.state_inited());
        CHECK!(handle.prev1_inited());
        block_id = engine.load_block_prev(handle.id()).await?;
        handle = engine.load_block_handle(&block_id)?;
    }
    log::info!(target: "boot", "last applied block id = {}", &block_id);
    let state = engine.load_state(&block_id).await?;
    let init_block_id = engine.init_mc_block_id();
    CHECK!(&block_id == init_block_id || state.has_prev_block(init_block_id)?);
    if block_id.seq_no() != 0 && !handle.is_key_block()? { // find last key block
        block_id = state.shard_state_extra()?.last_key_block.clone()
            .ok_or_else(|| error!("Masterchain state for {} doesn't contain info about prevous key block", state.block_id()))?
            .master_block_id().1;
    }
    log::info!(target: "boot", "last key block id = {}", block_id);
    Ok(block_id)
}

/*
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
*/