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
    block::BlockStuff, block_proof::BlockProofStuff, engine::Engine, 
    engine_traits::{ChainRange, EngineOperations}, error::NodeError,
    validator::validator_utils::{calc_subset_for_workchain, check_crypto_signatures},
};

use std::{sync::Arc, mem::drop, time::Duration};
use tokio::task::JoinHandle;
use ton_block::{BlockIdExt, BlockSignaturesPure, CryptoSignaturePair, CryptoSignature, ConfigParams};
use ton_types::{Result, fail, error};
use ton_api::ton::ton_node::broadcast::BlockBroadcast;

pub fn start_masterchain_client(
    engine: Arc<dyn EngineOperations>, 
    last_got_block_id: BlockIdExt
) -> Result<JoinHandle<()>> {
    let join_handle = tokio::spawn(async move {
        engine.acquire_stop(Engine::MASK_SERVICE_MASTERCHAIN_CLIENT);
        loop {
            if let Err(e) = load_master_blocks_cycle(engine.clone(), last_got_block_id.clone()).await {
                log::error!("Unexpected error in master blocks loading cycle: {:?}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else {
                break;
            }
        }
        engine.release_stop(Engine::MASK_SERVICE_MASTERCHAIN_CLIENT);
    });
    Ok(join_handle)
}

pub fn start_shards_client(
    engine: Arc<dyn EngineOperations>, 
    shards_mc_block_id: BlockIdExt
) -> Result<JoinHandle<()>> {
    let join_handle = tokio::spawn(async move {
        engine.acquire_stop(Engine::MASK_SERVICE_SHARDCHAIN_CLIENT);
        loop {
            if let Err(e) = load_shard_blocks_cycle(engine.clone(), &shards_mc_block_id).await {
                log::error!("Unexpected error in shards client: {:?}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else {
                break;
            }
        }
        engine.release_stop(Engine::MASK_SERVICE_SHARDCHAIN_CLIENT);
    });
    Ok(join_handle)
}

async fn load_master_blocks_cycle(
    engine: Arc<dyn EngineOperations>, 
    mut last_got_block_id: BlockIdExt
) -> Result<()> {
    let mut attempt = 0;
    loop {
        if engine.check_stop() {
            break Ok(())
        }
        last_got_block_id = match load_next_master_block(&engine, &last_got_block_id).await {
            Ok(id) => {
                attempt = 0;
                id
            },
            Err(e) => {
                log::error!(
                    "Error while load and apply next master block, prev: {}: attempt: {}, err: {:?}",
                    last_got_block_id,
                    attempt,
                    e
                );
                attempt += 1;
                // TODO make method to ban bad peer who gave bad block
                continue;
            }
        };
    }
}

async fn load_next_master_block(
    engine: &Arc<dyn EngineOperations>, 
    prev_id: &BlockIdExt
) -> Result<BlockIdExt> {

    log::trace!("load_blocks_cycle: prev block: {}", prev_id);
    if let Some(prev_handle) = engine.load_block_handle(prev_id)? {
        if prev_handle.has_next1() {
            let next_id = engine.load_block_next1(prev_id).await?;
            engine.clone().download_and_apply_block(&next_id, next_id.seq_no(), false).await?; 
            return Ok(next_id)
        }
    } else {
        fail!("Cannot load handle for prev block {}", prev_id)
    };

    log::trace!("load_blocks_cycle: downloading next block... prev: {}", prev_id);
    let (block, proof) = engine.download_next_block(prev_id).await?;
    log::trace!("load_blocks_cycle: got next block: {}", prev_id);
    if block.id().seq_no != prev_id.seq_no + 1 {
        fail!("Invalid next master block got: {}, prev: {}", block.id(), prev_id);
    }

    let prev_state = engine.clone().wait_state(&prev_id, None, true).await?;
    proof.check_with_master_state(&prev_state)?;
    let mut next_handle = loop {
        if let Some(next_handle) = engine.load_block_handle(block.id())? {
            if !next_handle.has_data() {
                log::warn!("Unitialized handle detected for block {}", block.id())
            } else {
                break next_handle
            }
        }
        if let Some(next_handle) = engine.store_block(&block).await?.as_non_created() {
            break next_handle
        } else {
            continue
        }
    };
    if !next_handle.has_proof() {
        next_handle = engine.store_block_proof(block.id(), Some(next_handle), &proof).await?
            .as_non_created()
            .ok_or_else(
                || error!("INTERNAL ERROR: bad result for store block {} proof", block.id())
            )?;
    }
    engine.clone().apply_block(&next_handle, &block, next_handle.id().seq_no(), false).await?;
    Ok(block.id().clone())

}

// TODO: We limited this window to 1 thread instead of 2 because of the issue with archives.
//       If we still need to process 2 parallel MC blocks or more, we should develop an algorithm
//       to mark correctly shard blocks with appropriate mc_seq_no despite of application order.
const SHARD_CLIENT_WINDOW: usize = 1;

async fn load_shard_blocks_cycle(
    engine: Arc<dyn EngineOperations>, 
    shards_mc_block_id: &BlockIdExt
) -> Result<()> {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(SHARD_CLIENT_WINDOW));
    let mut mc_handle = engine.load_block_handle(shards_mc_block_id)?.ok_or_else(
        || error!("Cannot load handle for shard master block {}", shards_mc_block_id)
    )?;
    let (_master, workchain_id) = engine.processed_workchain().await?;
    loop {
        if engine.check_stop() {
            break Ok(())
        }
        log::trace!("load_shard_blocks_cycle: mc block: {}", mc_handle.id());
        let r = match engine.wait_next_applied_mc_block(&mc_handle, Some(5)).await {
            Err(e) => {
                log::debug!("load_shard_blocks_cycle: no next mc block: {}", e);
                continue;
            }
            Ok(r) => r,
        };
        mc_handle = r.0;
        let mc_block = r.1;
        let shard_ids = mc_block.shard_hashes()?.top_blocks(&[workchain_id])?;

        log::trace!("load_shard_blocks_cycle: waiting semaphore: {}", mc_block.id());
        let semaphore_permit = Arc::clone(&semaphore).acquire_owned().await?;

        log::trace!("load_shard_blocks_cycle: process next mc block: {}", mc_block.id());

        let engine = Arc::clone(&engine);
        tokio::spawn(async move {
            if let Err(e) = load_shard_blocks(engine.clone(), semaphore_permit, &mc_block, shard_ids).await {
                log::error!("FATAL!!! Unexpected error in shard blocks processing for mc block {}: {:?}", mc_block.id(), e);
            }
            if engine.produce_chain_ranges_enabled() {
                if let Err(err) = produce_chain_range(engine, &mc_block).await {
                    log::error!("Unexpected error in chain range processing for mc block {}: {:?}", mc_block.id(), err);
                }
            }
        });
    }
}

pub async fn produce_chain_range(
    engine: Arc<dyn EngineOperations>,
    mc_block: &BlockStuff
) -> Result<()> {
    let mut range = ChainRange {
        master_block: mc_block.id().clone(),
        shard_blocks: Vec::new(),
    };
    let (_master, workchain_id) = engine.processed_workchain().await?;

    let prev_master = engine.load_block_prev1(mc_block.id())?;
    let prev_master = engine.load_block_handle(&prev_master)?
        .ok_or_else(|| NodeError::InvalidData(format!("Can not load block handle for {}", prev_master)))?;
    let prev_master = engine.load_block(&prev_master).await?;
    let prev_master_shards = prev_master.shards_blocks(workchain_id)?;

    let mut blocks: Vec<BlockIdExt> = mc_block.shards_blocks(workchain_id)?.values().cloned().collect();
    // for new rust let mut blocks: Vec<BlockIdExt> = mc_block.shards_blocks(workchain_id)?.into_values().collect();

    while let Some(block_id) = blocks.pop() {
        let handle = engine.load_block_handle(&block_id)?
            .ok_or_else(|| NodeError::InvalidData(format!("Can not load block handle for {}", block_id)))?;

        if prev_master_shards.get(handle.id().shard()) != Some(handle.id()) {
            if handle.has_prev1() {
                blocks.push(engine.load_block_prev1(&block_id)?);
            }
            if handle.has_prev2() {
                blocks.push(engine.load_block_prev2(&block_id)?);
            }
            range.shard_blocks.push(block_id);
        }
    }

    engine.process_chain_range_in_ext_db(&range).await?;

    Ok(())
}

pub async fn load_shard_blocks(
    engine: Arc<dyn EngineOperations>,
    semaphore_permit: tokio::sync::OwnedSemaphorePermit,
    mc_block: &BlockStuff,
    shard_ids: Vec<BlockIdExt>,
) -> Result<()> {

    let mut apply_tasks = Vec::new();
    let mc_seq_no = mc_block.id().seq_no();
    for shard_block_id in shard_ids {
        let msg = format!(
            "process mc block {}, shard block {} {}", 
            mc_block.id(), shard_block_id.shard(), shard_block_id
        );
        if let Some(shard_block_handle) = engine.load_block_handle(&shard_block_id)? {
            if shard_block_handle.is_applied() {
                continue;
            }
        }
        let engine = Arc::clone(&engine);
        let shard_block_id = shard_block_id.clone();
        let apply_task = tokio::spawn(
            async move {
                let mut attempt = 0;
                log::trace!("load_shard_blocks_cycle: {}, applying...", msg);
                while let Err(e) = Arc::clone(&engine).download_and_apply_block(
                    &shard_block_id, 
                    mc_seq_no, 
                    false
                ).await {
                    log::error!(
                        "Error while applying shard block (attempt {}) {}: {}",
                        attempt, shard_block_id, e
                    );
                    attempt += 1;
                    // TODO make method to ban bad peer who gave bad block
                    if engine.check_stop() {
                        break;
                    }
                }
                log::trace!("load_shard_blocks_cycle: {}, applied", msg);
            }
        );
        apply_tasks.push(apply_task);
    }

    futures::future::join_all(apply_tasks)
        .await
        .into_iter()
        .find(|r| r.is_err())
        .unwrap_or(Ok(()))?;

    log::trace!("load_shard_blocks_cycle: processed mc block: {}", mc_block.id());
    engine.save_shard_client_mc_block_id(mc_block.id())?;
    drop(semaphore_permit);                                    	
    Ok(())

}

pub const SHARD_BROADCAST_WINDOW: u32 = 8;

pub async fn process_block_broadcast(
    engine: &Arc<dyn EngineOperations>, 
    broadcast: &BlockBroadcast
) -> Result<()> {

    log::trace!("process_block_broadcast: {}", broadcast.id);
    if let Some(handle) = engine.load_block_handle(&broadcast.id)? {
        if handle.has_data() {
            #[cfg(feature = "telemetry")] {
                let duplicate = handle.got_by_broadcast();
                let unneeded = !duplicate;
                engine.full_node_telemetry().new_block_broadcast(
                    &broadcast.id, 
                    duplicate, 
                    unneeded
                );
            }
            return Ok(());
        }
    }
    #[cfg(feature = "telemetry")]
    engine.full_node_telemetry().new_block_broadcast(&broadcast.id, false, false);

    let is_master = broadcast.id.shard().is_masterchain();
    let proof = BlockProofStuff::deserialize(
        &broadcast.id, 
        broadcast.proof.0.clone(), 
        !is_master
    )?;
    let (virt_block, _) = proof.virtualize_block()?;
    let block_info = virt_block.read_info()?;
    let prev_key_block_seqno = block_info.prev_key_block_seqno();
    let last_applied_mc_state = engine.load_last_applied_mc_state_or_zerostate().await.map_err(
        |e| error!("INTERNAL ERROR: can't load last mc state: {}", e)
    )?;
    if prev_key_block_seqno > last_applied_mc_state.block_id().seq_no() {
        log::debug!(
            "Skipped block broadcast {} because it refers too new key block: {}, \
            but last processed mc block is {})",
            broadcast.id, prev_key_block_seqno, last_applied_mc_state.block_id().seq_no()
        );
        return Ok(());
    }

    let config_params = last_applied_mc_state.config_params()?;
    validate_brodcast(broadcast, config_params, &broadcast.id)?;

    // Build and save block and proof
    if is_master {
        proof.check_with_master_state(last_applied_mc_state.as_ref())?;
    } else {
        proof.check_proof_link()?;
    }
    let block = BlockStuff::deserialize_checked(broadcast.id.clone(), broadcast.data.0.clone())?;
    let mut handle = if let Some(handle) = engine.store_block(&block).await?.as_updated() {
        handle
    } else {
        log::debug!(
            "Skipped apply for block {} broadcast because block is already in processing",
            block.id()
        );
        return Ok(())
    };
    #[cfg(feature = "telemetry")]
    handle.set_got_by_broadcast(true);

    if !handle.has_proof() {
        let result = engine.store_block_proof(block.id(), Some(handle), &proof).await?;
        handle = if let Some(handle) = result.as_updated() {
            handle
        } else {
            log::debug!(
                "Skipped apply for block {} broadcast because block is already in processing",
                block.id()
            );
            return Ok(())
        }
    }

    // Apply (only blocks that is not too new for us)
    if block.id().shard().is_masterchain() {
        if block.id().seq_no() == last_applied_mc_state.block_id().seq_no() + 1 {
            engine.clone().apply_block(&handle, &block, block.id().seq_no(), false).await?;
        } else {
            log::debug!(
                "Skipped apply for block broadcast {} because it is too new (last master block: {})",
                block.id(), last_applied_mc_state.block_id().seq_no()
            )
        }
    } else {
        let master_ref = block
            .block()
            .read_info()?
            .read_master_ref()?
            .ok_or_else(|| NodeError::InvalidData(format!(
                "Block {} doesn't contain masterchain block extra", block.id(),
            )))?;
        let shard_client_mc_block_id = engine.load_shard_client_mc_block_id()?.ok_or_else(
            || error!("INTERNAL ERROR: No shard client MC block after sync")
        )?;
        if shard_client_mc_block_id.seq_no() + SHARD_BROADCAST_WINDOW >= master_ref.master.seq_no {
            engine.clone().apply_block(&handle, &block, shard_client_mc_block_id.seq_no(), true).await?;
        } else {
            log::debug!(
                "Skipped pre-apply for block broadcast {} because it refers to master block {}, but shard client is on {}",
                block.id(), master_ref.master.seq_no, shard_client_mc_block_id.seq_no()
            )
        }
    }
    Ok(())

}

fn validate_brodcast(
    broadcast: &BlockBroadcast,
    config_params: &ConfigParams,
    block_id: &BlockIdExt,
) -> Result<()> {

    let validator_set = config_params.validator_set()?;
    let cc_config = config_params.catchain_config()?;

    // build validator set
    let (validators, validators_hash_short) = calc_subset_for_workchain(
        &validator_set,
        config_params,
        &cc_config, 
        block_id.shard().shard_prefix_with_tag(), 
        block_id.shard().workchain_id(), 
        broadcast.catchain_seqno as u32,
        0.into() // TODO: unix time is not realy used in algorithm, but exists in t-node,
                 // maybe delete it from `calc_subset` parameters?
    )?;

    if validators_hash_short != broadcast.validator_set_hash as u32 {
        fail!(NodeError::InvalidData(format!(
            "Bad validator set hash in broadcast with block {}, calculated: {}, found: {}",
            block_id,
            validators_hash_short,
            broadcast.validator_set_hash
        )));
    }

    // extract signatures - build ton_block::BlockSignaturesPure
    let mut blk_pure_signatures = BlockSignaturesPure::default();
    for api_sig in broadcast.signatures.iter() {
        blk_pure_signatures.add_sigpair(
            CryptoSignaturePair {
                node_id_short: api_sig.who.clone(),
                sign: CryptoSignature::from_bytes(&api_sig.signature)?,
            }
        );
    }

    // Check signatures
    let checked_data = ton_block::Block::build_data_for_sign(
        &block_id.root_hash,
        &block_id.file_hash
    );
    let total_weight: u64 = validators.iter().map(|v| v.weight).sum();
    let weight = check_crypto_signatures(&blk_pure_signatures, &validators, &checked_data)
        .map_err(|err| NodeError::InvalidData(
            format!("Bad signatures in broadcast with block {}: {}", block_id, err)
        ))?;

    if weight * 3 <= total_weight * 2 {
        fail!(NodeError::InvalidData(format!(
            "Too small signatures weight in broadcast with block {}",
            block_id,
        )));
    }

    Ok(())
}
