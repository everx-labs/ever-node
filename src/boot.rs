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
    CHECK, block_proof::BlockProofStuff, engine_traits::EngineOperations, 
    shard_state::ShardStateStuff, engine::Engine
};

use std::{ops::Deref, sync::Arc, time::Duration};
use std::path::Path;
use storage::block_handle_db::BlockHandle;
use ton_block::{BlockIdExt, ShardIdent, SHARD_FULL};
use ton_types::{error, fail, KeyId, Result};
use crate::block::BlockStuff;
use crate::validator::accept_block::create_new_proof_link;

pub const PSS_PERIOD_BITS: u32 = 17;
const RETRY_MASTER_STATE_DOWNLOAD: usize = 10;

/// cold boot entry point
/// download zero state or block proof link and check it
async fn run_cold(
    engine: &dyn EngineOperations
) -> Result<(Arc<BlockHandle>, Option<Arc<ShardStateStuff>>, Option<BlockProofStuff>)> {

    let block_id = engine.init_mc_block_id();
    log::info!(target: "boot", "cold boot start: init_block_id={}", block_id);
    CHECK!(block_id.shard().is_masterchain());
    CHECK!(block_id.seq_no >= engine.get_last_fork_masterchain_seqno());
    if block_id.seq_no() == 0 {
        let handle = download_zerostate(engine, &block_id).await?;
        let zero_state = engine.load_state(handle.id()).await?;
        return Ok((handle, Some(zero_state), None));
    }

    // id should be key block if not it will never sync
    log::info!(target: "boot", "check if block proof is in database {}", block_id);
    let handle = if let Some(handle) = engine.load_block_handle(&block_id)? {
        if handle.has_proof_link() || handle.has_proof() {
            let proof = match engine.load_block_proof(&handle, false).await {
                Ok(proof) => proof,
                Err(err) => {
                    log::warn!(
                        target: "boot", 
                        "load_block_proof for init_block {} error: {}", 
                        handle.id(), err
                    );
                    engine.load_block_proof(&handle, true).await?
                }
            };
            CHECK!(handle.is_key_block()?);
            return Ok((handle, None, Some(proof)))
        }
        Some(handle)
    } else {
        None
    };

    let (handle, proof) = loop {

        if engine.check_stop() {
            fail!("Boot was stopped");
        }

        log::info!(target: "boot", "download init block proof {}", block_id);
        match engine.download_block_proof(0, &block_id, false, true).await {
            Ok(proof) => match proof.check_proof_as_link() {
                Ok(_) => {
                    log::info!(target: "boot", "block proof downloaded {}", block_id);
                    let handle = engine.store_block_proof(0, &block_id, handle, &proof).await? 
                        .to_non_created()
                        .ok_or_else( 
                            || error!(
                                "INTERNAL ERROR: Bad result in store block proof {}",
                                block_id
                            )
                        )?;
                    engine.save_last_applied_mc_block_id(handle.id())?;
                    break (handle, proof)
                },
                Err(err) => log::warn!(
                    target: "boot", 
                    "check_proof for init_block {} error: {}", 
                    block_id, err
                )
            },
            Err(err) => log::warn!(
                target: "boot", 
                "download block proof for init_block {} error: {}", 
                block_id, err
            )
        }
        futures_timer::Delay::new(Duration::from_secs(1)).await;

        log::info!(target: "boot", "download init block proof link {}", block_id);
        match engine.download_block_proof(0, &block_id, true, true).await {
            Ok(proof) => match proof.check_proof_link() {
                Ok(_) => {
                    let handle = engine.store_block_proof(0, &block_id, handle, &proof).await? 
                        .to_non_created()
                        .ok_or_else( 
                            || error!(
                                "INTERNAL ERROR: Bad result in store block proof link {}",
                                block_id
                            )
                        )?;
                    engine.save_last_applied_mc_block_id(handle.id())?;
                    break (handle, proof)
                },
                Err(err) => log::warn!(
                    target: "boot", 
                    "check_proof_link for init_block {} error: {}", 
                    block_id, err
                )
            },
            Err(err) => log::warn!(
                target: "boot", 
                "download block proof link for init_block {} error: {}", 
                block_id, err
            )
        }
        futures_timer::Delay::new(Duration::from_secs(1)).await;

    };

    CHECK!(handle.is_key_block()?);
    Ok((handle, None, Some(proof)))

}

/// download key blocks
/// 1. define time period
/// 2. get next key blocks ids infinitely
/// 3. download key block proofs and check with each other or with zero state for the first
/// 4. check if last key block can be selected for current state
async fn get_key_blocks(
    engine: &dyn EngineOperations,
    mut handle: Arc<BlockHandle>,
    zero_state: Option<&Arc<ShardStateStuff>>,
    mut prev_block_proof: Option<BlockProofStuff>,
) -> Result<Vec<Arc<BlockHandle>>> {
    let mut hardfork_iter = engine.hardforks().iter();
    let mut hardfork = hardfork_iter.next();
    let mut key_blocks = vec!(handle.clone());
    'main_loop: loop {
        if engine.check_stop() {
            fail!("Boot was stopped");
        }
        log::info!(target: "boot", "download_next_key_blocks_ids {}", handle.id());
        // this information is not trusted
        let ids = match engine.download_next_key_blocks_ids(0, handle.id()).await {
            Err(err) => {
                log::warn!(target: "boot", "download_next_key_blocks_ids {}: {}", handle.id(), err);
                futures_timer::Delay::new(Duration::from_secs(1)).await;
                continue;
            }
            Ok(ids) => {
                if let Some(block_id) = ids.last() {
                    log::info!(target: "boot", "last key block is {}", block_id);
                    ids
                } else {
                    return Ok(key_blocks);
                }
            }
        };
        for block_id in &ids {
            if block_id.seq_no() == 0 {
                log::warn!("somebody sent next key block with zero state {}", block_id);
                continue;
            }
            
            if let Some(last_handle) = key_blocks.last() {
                if block_id.seq_no() <= last_handle.id().seq_no() {
                    log::warn!("somebody sent next key block id with seq_no less or equal to already got {}", block_id);
                    continue;
                }

                // we need to check presence and correctness of every hardfork
                if let Some(hardfork_id) = hardfork {
                    if hardfork_id.seq_no == block_id.seq_no {
                        if hardfork_id == block_id {
                            log::debug!(target: "boot", "hardfork {} found", block_id);
                            hardfork = hardfork_iter.next();
                        } else {
                            log::warn!(target: "boot", "keyblock is {}, but must equal to hardfork {}", block_id, hardfork_id);
                            break
                        }
                    } else if hardfork_id.seq_no < block_id.seq_no {
                        log::warn!(target: "boot", "keyblock is {}, but missed hardfork {}", block_id, hardfork_id);
                        break
                    }
                }
                //let prev_time = handle.gen_utime()?;
                match download_and_check_key_block_proof(engine, block_id, zero_state, prev_block_proof.as_ref()).await {
                    Ok((next_handle, proof)) => {
                        handle = next_handle;
                        CHECK!(handle.is_key_block()?);
                        CHECK!(handle.gen_utime()? != 0);
                        // if engine.is_persistent_state(handle.gen_utime()?, prev_time) {
                        //     engine.set_init_mc_block_id(block_id);
                        // }
                        key_blocks.push(handle.clone());
                        prev_block_proof = Some(proof);
                    }
                    Err(err) => {
                        log::warn!(target: "boot", "cannot get block proof link for {}: {}", block_id, err);
                        futures_timer::Delay::new(Duration::from_secs(1)).await;
                        continue 'main_loop;
                    }
                }
            }
        }
        if let Some(handle) = key_blocks.last() {
            let utime = handle.gen_utime()?;
            log::info!(target: "boot", "id: {}, utime: {}, now: {}", handle.id(), utime, engine.now());
            CHECK!(utime != 0);
            CHECK!(utime < engine.now());
            if
                (engine.sync_blocks_before() > engine.now() - utime)
                || (2 * engine.key_block_utime_step() > engine.now() - utime)
            {
                return Ok(key_blocks)
            }
        }
    }
}

/// choose correct masterchain state
async fn choose_masterchain_state(
    engine: &dyn EngineOperations,
    mut key_blocks: Vec<Arc<BlockHandle>>,
    pss_period_bits: u32,
) -> Result<Arc<BlockHandle>> {
    while let Some(handle) = key_blocks.pop() {
        let utime = handle.gen_utime()?;
        let ptime = if let Some(handle) = key_blocks.last() {
            handle.gen_utime()?
        } else {
            0
        };
        log::info!(target: "boot", "key block candidate: seqno={} \
            is_persistent={} ttl={} syncbefore={}", handle.id().seq_no(),
                ptime == 0 || engine.is_persistent_state(utime, ptime, pss_period_bits),
                engine.persistent_state_ttl(utime, pss_period_bits),
                engine.sync_blocks_before());
        if engine.sync_blocks_before() > engine.now() - utime {
            log::info!(target: "boot", "ignoring: too new block");
            continue;
        }
        if ptime == 0 || engine.is_persistent_state(utime, ptime, pss_period_bits) {
            let ttl = engine.persistent_state_ttl(utime, pss_period_bits);
            let time_to_download = 3600; 
            if ttl > engine.now() + time_to_download {
                log::info!(target: "boot", "best handle is {}", handle.id());
                return Ok(handle)
            } else {
               log::info!(target: "boot", "state is expiring shortly: expire_at={}", ttl);
               return Ok(handle)
            }
        } else {
            log::info!(target: "boot", "ignoring: state is not persistent");
        }
    }
    CHECK!(key_blocks.is_empty());
    fail!("Cannot boot node")
}

/// Download zerostate for all workchains
async fn download_wc_zerostates(
    engine: &dyn EngineOperations,
    mc_zerostate: &ShardStateStuff
) -> Result<()> {
    let workchains = mc_zerostate.config_params()?.workchains()?;
    let mut ids = vec!();
    workchains.iterate_with_keys(|wc_id, wc| {
        ids.push(BlockIdExt {
            shard_id: ShardIdent::with_tagged_prefix(wc_id, SHARD_FULL)?,
            seq_no: 0,
            root_hash: wc.zerostate_root_hash,
            file_hash: wc.zerostate_file_hash,
        });
        Ok(true)
    })?;
    for zerostate_id in ids {
        download_zerostate(engine, &zerostate_id).await?;
    }
    Ok(())
}

/// Download persistent master block & state, enumerate shards and download block & state for each
async fn download_start_blocks_and_states(
    engine: &dyn EngineOperations, 
    master_handle: &Arc<BlockHandle>
) -> Result<()> {

    engine.set_sync_status(Engine::SYNC_STATUS_LOAD_MASTER_STATE);
    let active_peers = Arc::new(lockfree::set::Set::new());
    let init_mc_state = download_state(
        engine, &master_handle, master_handle.id(), &active_peers, Some(RETRY_MASTER_STATE_DOWNLOAD)
    ).await?;
    CHECK!(master_handle.has_state());
    CHECK!(master_handle.is_applied());
    let top_blocks = init_mc_state.top_blocks_all()?;

    CHECK!(!top_blocks.is_empty());

    engine.set_sync_status(Engine::SYNC_STATUS_LOAD_SHARD_STATES);
    for block_id in &top_blocks {
        log::info!(target: "boot", "download shardchain state {}", block_id);
        let shard_handle = if block_id.seq_no() == 0 {
            download_zerostate(engine, block_id).await?
        } else {
            let handle = if let Some(handle) = engine.load_block_handle(block_id)? {
                handle
            } else if engine.flags().starting_block_disabled {
                let proof = engine.download_block_proof(0, block_id, true, false).await?;
                let handle = engine.store_block_proof(0, block_id, None, &proof).await?
                    .to_non_created()
                    .ok_or_else(
                        || error!("INTERNAL ERROR: Bad result in store block {} proof", block_id)
                    )?;
                handle
            } else {
                let (block, proof) = engine.download_block(block_id, None).await?;
                let handle = engine.store_block(&block).await?
                    .to_non_created()
                    .ok_or_else(
                        || error!("INTERNAL ERROR: Bad result in store block {} proof", block_id)
                    )?;
                if let Some(proof) = proof {
                    engine.store_block_proof(0, block_id, Some(handle.clone()), &proof).await?
                        .to_updated()
                        .ok_or_else(
                            || error!("INTERNAL ERROR: Bad result in store block {} proof", block_id)
                        )?;
                }
                handle
            };
            download_state(engine, &handle, master_handle.id(), &active_peers, None).await?;
            handle
        };
        CHECK!(shard_handle.has_state());
        CHECK!(shard_handle.is_applied());
    }
    Ok(())

}

/// download zero state and store it
pub(crate) async fn download_zerostate(
    engine: &dyn EngineOperations, 
    block_id: &BlockIdExt
) -> Result<Arc<BlockHandle>> {
    if let Some(handle) = engine.load_block_handle(block_id)? {
        if handle.has_state() {
            return Ok(handle)
        }
    }
    log::info!(target: "boot", "download zero state {}", block_id);
    loop {
        if engine.check_stop() {
            fail!("Boot was stopped");
        }
        match engine.download_zerostate(0, block_id).await {
            Ok((state, state_bytes)) => {
                log::info!(target: "boot", "zero state {} received", block_id);
                let (state, handle) = engine.store_zerostate(state, &state_bytes).await?;
                engine.set_applied(&handle, 0).await?;
                engine.save_last_applied_mc_block_id(handle.id())?;
                engine.process_full_state_in_ext_db(&state).await?;
                return Ok(handle)
            }
            Err(err) => log::warn!(target: "boot", "download_zerostate error: {}", err)
        }
        futures_timer::Delay::new(Duration::from_secs(1)).await;
    }
}

/// download key block proof, check it and store
async fn download_and_check_key_block_proof(
    engine: &dyn EngineOperations,
    block_id: &BlockIdExt,
    zero_state: Option<&Arc<ShardStateStuff>>,
    prev_block_proof: Option<&BlockProofStuff>,
) -> Result<(Arc<BlockHandle>, BlockProofStuff)> {
    if let Some(handle) = engine.load_block_handle(block_id)? {
        if let Ok(proof) = engine.load_block_proof(&handle, false).await {
            return Ok((handle, proof))
        }
    }
    loop {
        if engine.check_stop() {
            fail!("Boot was stopped");
        }
        let proof = engine.download_block_proof(0, block_id, false, true).await?;
        let result = if let Some(prev_block_proof) = prev_block_proof {
            proof.check_with_prev_key_block_proof(prev_block_proof)
        } else if let Some(zero_state) = zero_state {
            proof.check_with_master_state(zero_state)
        } else {
            unreachable!("Impossible variant")
        };
        match result {
            Ok(_) => {
                let handle = engine.store_block_proof(0, block_id, None, &proof).await?
                    .to_non_created()
                    .ok_or_else(
                        || error!("INTERNAL ERROR: Bad result in store block {} proof", block_id)
                    )?;
                engine.save_last_applied_mc_block_id(handle.id())?;
                return Ok((handle, proof))
            }
            Err(err) => {
                log::warn!(target: "boot", "check_proof error: {}", err);
                futures_timer::Delay::new(Duration::from_secs(1)).await;
            }
        }
    }
}

/// download any state, check its hash and store it
async fn download_state(
    engine: &dyn EngineOperations, 
    handle: &Arc<BlockHandle>, 
    master_id: &BlockIdExt,
    active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>,
    attempts: Option<usize>
) -> Result<Arc<ShardStateStuff>> {
    let state = if !handle.has_state() {
        CHECK!(handle.has_proof() || handle.has_proof_link());
        // we can download last key block to get shards description to download shard states in parallel
        let proof = if !engine.flags().starting_block_disabled && !handle.has_data() {
            log::info!(target: "boot", "downloading block {}", handle.id());
            let (block, proof) = engine.download_block(handle.id(), None).await?;
            engine.store_block(&block).await?.to_updated().ok_or_else(
                || error!("INTERNAL ERROR: mismatch in block {} store result during boot", handle.id())
            )?;
            if !handle.has_proof() {
                if let Some(proof) = proof {
                    engine.store_block_proof(0, handle.id(), Some(handle.clone()), &proof).await?
                        .to_updated()
                        .ok_or_else(
                            || error!(
                                "INTERNAL ERROR: mismatch in block {} proof store result during boot",
                                handle.id()
                            )
                        )?;
                    proof
                } else {
                    fail!("we don't have proof for {}", handle.id())
                }
            } else {
                engine.load_block_proof(handle, false).await?
            }
        } else if handle.has_proof() {
            engine.load_block_proof(handle, false).await?
        } else if handle.has_proof_link() {
            engine.load_block_proof(handle, true).await?
        } else {
            fail!("handle for has neither proof nor proof link {}", handle.id())
        };
        // let state_update = block.virt_block()?.read_state_update()?;
        let (block, _) = proof.virtualize_block()?;
        let state_update = block.read_state_update()?;
        let state = engine.download_and_store_state(
            &handle, &state_update.new_hash, master_id, active_peers, attempts
        ).await?;
        engine.process_full_state_in_ext_db(&state).await?;
        state
    } else {
        engine.load_state(handle.id()).await?
    };
    engine.set_applied(&handle, master_id.seq_no()).await?;
    Ok(state)
}

/// Cold load best key block and its state
/// Must be used only zero_state or key_block id
pub async fn cold_boot(engine: Arc<dyn EngineOperations>) -> Result<Arc<BlockHandle>> {
    let (mut handle, zero_state, init_block_proof_link) = run_cold(engine.deref()).await?;
    let key_blocks = get_key_blocks(
        engine.deref(), handle, zero_state.as_ref(), init_block_proof_link
    ).await?;
    
    handle = choose_masterchain_state(engine.deref(), key_blocks.clone(), PSS_PERIOD_BITS).await?;

    if handle.id().seq_no() == 0 {
        let Some(zero_state) = zero_state.as_ref() else {
            fail!("Zero state is not set")
        };
        download_wc_zerostates(engine.deref(), zero_state).await?;
    } else {
        download_start_blocks_and_states(engine.deref(), &handle).await?;
    }
    Ok(handle)
}

pub async fn warm_boot(
    engine: Arc<Engine>,
    block_id: Arc<BlockIdExt>,
    hardfork_path: impl AsRef<Path>,
) -> Result<BlockIdExt> {
    log::info!("Warm boot");
    if let Some(block_id) = check_hardforks(&engine, &block_id, hardfork_path).await? {
        return Ok(block_id)
    }
    let mut block_id = block_id.deref().clone();
    loop {
        let handle = engine.load_block_handle(&block_id)?.ok_or_else(
            || error!("Cannot load handle for block {}", block_id)
        )?;
        // go back to find last applied block
        if handle.is_applied() {
            break;
        }
        CHECK!(handle.has_state());
        CHECK!(handle.has_prev1());
        block_id = engine.load_block_prev1(&block_id)?;
    }
    log::info!(target: "boot", "last applied block id = {}", block_id);
    let state = engine.load_state(&block_id).await?;
    let init_block_id = engine.init_mc_block_id();
    CHECK!(&block_id == init_block_id || state.has_prev_block(init_block_id)?);
    Ok(block_id)
}

async fn check_hardforks(
    engine: &Arc<Engine>, 
    last_applied_mc_block: &Arc<BlockIdExt>,
    hardfork_path: impl AsRef<Path>,
) -> Result<Option<BlockIdExt>> {
    let Some(hardfork_id) = engine.hardforks().last() else { return Ok(None); };
    log::info!(
        target: "boot",
        "last hardfork block id = {} last applied block id = {}",
        hardfork_id, last_applied_mc_block);
    if hardfork_id.seq_no == 0 {
        fail!("hardfork block id wrong seq_no 0")
    }
    let mc_state = engine.load_state(&last_applied_mc_block).await?;
    if mc_state.seq_no() + 1 == hardfork_id.seq_no {
        log::info!(target: "boot", "previous block of hardfork is the last, just apply hardfork");
    } else if mc_state.seq_no() < hardfork_id.seq_no {
        fail!(
            "we cannot continue, because database does not have enough blocks to make hardfork by {}",
            hardfork_id
        )
    } else if &mc_state.find_block_id(hardfork_id.seq_no)? != hardfork_id {
        log::info!(
            target: "boot",
            "last hardfork block id = {} is not yet applied, truncating database",
            hardfork_id
        );
        engine.truncate_database(hardfork_id.seq_no).await?;
    } else {
        log::info!(target: "boot", "last hardfork block id = {} already applied", hardfork_id);
        return Ok(None)
        // hardfork already applied
    }
    let (block, proof);
    let handle = if let Some(handle) = engine.load_block_handle(hardfork_id)? {
        log::info!(target: "boot", "crafted block is already in the database");
        block = engine.load_block(&handle).await?;
        // we don't check error loading proof, if so - create new proof
        proof = match engine.load_block_proof(&handle, true).await {
            Ok(proof) => {
                log::info!(
                    target: "boot",
                    "the proof for crafted block is already in the database"
                );
                Some(proof)
            }
            Err(err) => {
                log::info!(
                    target: "boot",
                    "the proof for crafted block is not in the database: {}",
                    err
                );
                None
            }
        };
        handle
    } else {
        // then find new master block file in folders by root hash
        let file_name = hardfork_path.as_ref()
            .join(hardfork_id.root_hash().as_hex_string());
        // if we have such block in folder - apply it
        match std::fs::read(&file_name) {
            Ok(data) => {
                block = BlockStuff::deserialize_block(hardfork_id.clone(), data)?;
                // we don't want to check presence of this block
                let handle = engine.store_block(&block).await?
                    .to_non_created()
                    .ok_or_else(|| error!("crafted block is already in the database"))?;
                log::info!(
                    target: "boot",
                    "crafted block was loaded from the file and stored to the database"
                );
                proof = None;
                handle
            }
            // if we don't have crafted block
            Err(err) => {
                log::warn!(
                    target: "boot",
                    "cannot read crafted block {:?} : {}",
                    file_name, err
                );
                return Ok(Some(mc_state.find_block_id(hardfork_id.seq_no - 1)?))
            }
        }
    };
    if proof.is_none() {
        let proof = create_new_proof_link(&block)
            .map_err(|err| error!("cannot create proof link for crafted block : {}", err))?;
        engine.store_block_proof(0, hardfork_id, Some(handle.clone()), &proof).await?;
        log::info!(
            target: "boot",
            "the proof for crafted block is created and stored to the database"
        );
    }
    if !handle.is_applied() {
        let prev_id = mc_state.find_block_id(hardfork_id.seq_no - 1)?;
        let hardfork_prev_id = block.construct_prev_id()?.0;
        if prev_id != hardfork_prev_id {
            fail!(
                "prev block id of crafted block doesn't equal to previous block in the database {} != {}",
                prev_id, hardfork_prev_id
            )
        }
        engine.clone().apply_hardfork_block(&handle, &block).await?;
    }
    Ok(Some(hardfork_id.clone()))
}
