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
    validator::validator_utils::{
        check_crypto_signatures, calc_subset_for_masterchain,
    },
    shard_state::ShardStateStuff,
};
use crate::validator::validator_utils::calc_subset_for_workchain_standard;

use std::{sync::Arc, mem::drop, time::Duration, collections::HashMap};
use ton_block::{
    BlockIdExt, BlockSignaturesPure, CryptoSignaturePair, CryptoSignature, 
    GlobalCapabilities, ProofChain, MerkleProof, Deserializable, Block, Serializable,
    BASE_WORKCHAIN_ID,
};
use ton_types::{Result, fail, error};
use ton_api::ton::ton_node::broadcast::{BlockBroadcast, QueueUpdateBroadcast};

pub fn start_masterchain_client(
    engine: Arc<dyn EngineOperations>, 
    last_got_block_id: BlockIdExt
) -> Result<tokio::task::JoinHandle<()>> {
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
) -> Result<tokio::task::JoinHandle<()>> {
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

// Remember about ShardStatesKeeper::MAX_CATCH_UP_DEPTH and ShardStateDb::MAX_QUEUE_LEN
pub const MC_MAX_SUPERIORITY: u32 = 500;

async fn load_master_blocks_cycle(
    engine: Arc<dyn EngineOperations>, 
    mut last_got_block_id: BlockIdExt
) -> Result<()> {
    let mut attempt = 0;
    loop {
        if engine.check_stop() {
            break Ok(())
        }
        if let Some(shard_client) = engine.load_shard_client_mc_block_id()? {
            if shard_client.seq_no() < last_got_block_id.seq_no() && 
            last_got_block_id.seq_no() - shard_client.seq_no() > MC_MAX_SUPERIORITY 
            {
                log::info!(
                    "load_next_master_block (block {last_got_block_id}): waiting for shard client ({shard_client})");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
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

    log::trace!("load_next_master_block: prev block: {}", prev_id);
    if let Some(prev_handle) = engine.load_block_handle(prev_id)? {
        if prev_handle.has_next1() {
            let next_id = engine.load_block_next1(prev_id).await?;
            engine.clone().download_and_apply_block(&next_id, next_id.seq_no(), false).await?; 
            return Ok(next_id)
        }
    } else {
        fail!("Cannot load handle for prev block {}", prev_id)
    };

    log::trace!("load_next_master_block: downloading next block... prev: {}", prev_id);
    let (block, proof) = engine.download_next_block(prev_id).await?;
    log::trace!("load_next_master_block: got next block: {}", prev_id);
    if block.id().seq_no != prev_id.seq_no + 1 {
        fail!("Invalid next master block got: {}, prev: {}", block.id(), prev_id);
    }

    let prev_state = engine.clone().wait_state(&prev_id, None, true).await?;
    proof.check_with_master_state(&prev_state)?;
    let mut next_handle = loop {
        if let Some(next_handle) = engine.load_block_handle(block.id())? {
            if !next_handle.has_data() {
                log::warn!(
                    "load_next_master_block: unitialized handle detected for block {}", block.id())
            } else {
                break next_handle
            }
        }
        if let Some(next_handle) = engine.store_block(&block).await?.to_non_created() {
            break next_handle
        } else {
            continue
        }
    };
    if !next_handle.has_proof() {
        next_handle = engine.store_block_proof(block.id(), Some(next_handle), &proof).await?
            .to_non_created()
            .ok_or_else(
                || error!(
                    "INTERNAL ERROR: load_next_master_block: bad result for store block {} proof",
                    block.id()
                )
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
    loop {
        if engine.check_stop() {
            break Ok(())
        }
        log::trace!("load_shard_blocks_cycle: mc block: {}", mc_handle.id());
        let r = match engine.wait_next_applied_mc_block(&mc_handle, Some(5_000)).await {
            Err(e) => {
                log::debug!("load_shard_blocks_cycle: no next mc block: {}", e);
                continue;
            }
            Ok(r) => r,
        };
        mc_handle = r.0;
        let mc_block = r.1;

        log::trace!("load_shard_blocks_cycle: waiting semaphore: {}", mc_block.id());
        let semaphore_permit = Arc::clone(&semaphore).acquire_owned().await?;

        log::trace!("load_shard_blocks_cycle: process next mc block: {}", mc_block.id());

        let engine = Arc::clone(&engine);
        tokio::spawn(async move {
            if let Err(e) = load_shard_blocks(engine.clone(), semaphore_permit, &mc_block).await {
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
    mc_block: &BlockStuff,
) -> Result<()> {
    let mut range = ChainRange {
        master_block: mc_block.id().clone(),
        shard_blocks: Vec::new(),
    };

    let processed_wc = engine.processed_workchain().unwrap_or(BASE_WORKCHAIN_ID);
    let prev_master = engine.load_block_prev1(mc_block.id())?;
    let prev_master = engine.load_block_handle(&prev_master)?
        .ok_or_else(|| NodeError::InvalidData(format!("Can not load block handle for {}", prev_master)))?;
    let prev_master = engine.load_block(&prev_master).await?;
    let prev_blocks = prev_master.top_blocks(processed_wc)?;
    let mut prev_master_shards = HashMap::new();
    for id in prev_blocks {
        prev_master_shards.insert(id.shard().clone(), id);
    }

    let mut blocks = mc_block.top_blocks(processed_wc)?;
    // for new rust let mut blocks: Vec<BlockIdExt> = mc_block.top_blocks(processed_wc)?.into_values().collect();

    while let Some(block_id) = blocks.pop() {
        let handle = engine.load_block_handle(&block_id)?
            .ok_or_else(|| NodeError::InvalidData(format!("Can not load block handle for {}", block_id)))?;

        if prev_master_shards.get(handle.id().shard()) != Some(handle.id()) {
            if handle.has_prev1() {
                blocks.push(engine.load_block_prev1(&block_id)?)
            }
            if handle.has_prev2() {
                blocks.push(
                    engine.load_block_prev2(&block_id)?.ok_or_else(
                        || error!("No assigned prev2 for block {} in DB", block_id)
                    )?
                )
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
) -> Result<()> {

    fn start_apply_block_task(
        engine: Arc<dyn EngineOperations>,
        shard_block_id: BlockIdExt,
        mc_seq_no: u32,
        msg: String,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut attempt = 0;
            log::trace!("load_shard_blocks_cycle: {}, applying block...", msg);
            loop { 
                if let Err(e) = Arc::clone(&engine).download_and_apply_block(
                    &shard_block_id, 
                    mc_seq_no, 
                    false
                ).await {
                    log::error!(
                        "Error while applying shard block (attempt {}) {}: {:?}",
                        attempt, shard_block_id, e
                    );
                    attempt += 1;
                    // TODO make method to ban bad peer who gave bad block
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if engine.check_stop() {
                        break;
                    }
                } else {
                    log::trace!("load_shard_blocks_cycle: {}, applied block", msg);
                    break;
                }
            }
        })
    }

    fn start_apply_proof_chain_task(
        proof_chain: ProofChain,
        own_wc: i32,
        engine: Arc<dyn EngineOperations>,
        shard_block_id: BlockIdExt,
        mc_seq_no: u32,
        msg: String,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut attempt = 0;
            log::trace!("load_shard_blocks_cycle: {}, applying proof chain...", msg);
            loop { 
                if let Err(e) = apply_proof_chain(
                    &proof_chain,
                    own_wc,
                    &engine,
                    &shard_block_id, 
                    mc_seq_no, 
                    false,
                    false,
                ).await {
                    log::error!(
                        "Error while applying proof chain (attempt {}) {}: {:?}",
                        attempt, shard_block_id, e
                    );
                    attempt += 1;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if engine.check_stop() {
                        break;
                    }
                } else {
                    log::trace!("load_shard_blocks_cycle: {}, applied proof chain", msg);
                    break;
                }
            }
        })
    }

    let mut apply_tasks = Vec::new();
    let mc_seq_no = mc_block.id().seq_no();

    if engine.load_actual_config_params().await?.has_capability(GlobalCapabilities::CapWorkchains) {
        // Apply own blocks and foreign queue updates
        for (shard_block_id, shard_header) in mc_block.top_blocks_all_headers()? {
            let wc = shard_block_id.shard().workchain_id();
            let (is_foreign_wc, own_wc) = engine.is_foreign_wc(wc).await?;
            let msg = format!(
                "load_shard_blocks: mc block {}, {} {}", 
                mc_block.id(),
                if is_foreign_wc {
                    format!("queue update (wc: {})", wc)
                } else {
                    "shard block".to_owned()
                },
                shard_block_id
            );
            if let Some(shard_block_handle) = engine.load_block_handle(&shard_block_id)? {
                if shard_block_handle.is_applied() {
                    continue;
                }
            }
            if is_foreign_wc {
                apply_tasks.push(start_apply_proof_chain_task(
                    shard_header.proof_chain
                        .ok_or_else(|| error!("INTERNAL ERROR: no proof chain for {}", shard_block_id))?,
                    own_wc,
                    engine.clone(),
                    shard_block_id.clone(),
                    mc_seq_no,
                    msg,
                ));
            } else {
                apply_tasks.push(start_apply_block_task(
                    engine.clone(),
                    shard_block_id.clone(),
                    mc_seq_no,
                    msg,
                ));
            }
        }
    } else {
        // Apply full shard blocks (classic single wc config)
        for shard_block_id in mc_block.top_blocks(BASE_WORKCHAIN_ID)? {
            let msg = format!(
                "load_shard_blocks: mc block {}, shard block {}", 
                mc_block.id(),
                shard_block_id
            );
            if let Some(shard_block_handle) = engine.load_block_handle(&shard_block_id)? {
                if shard_block_handle.is_applied() {
                    continue;
                }
            }
            apply_tasks.push(start_apply_block_task(
                engine.clone(),
                shard_block_id.clone(),
                mc_seq_no,
                msg,
            ));
        }
    };

    futures::future::join_all(apply_tasks)
        .await
        .into_iter()
        .find(|r| r.is_err())
        .unwrap_or(Ok(()))?;

    if engine.check_stop() {
        return Ok(());
    }

    log::trace!("load_shard_blocks_cycle: processed mc block: {}", mc_block.id());
    engine.save_shard_client_mc_block_id(mc_block.id())?;
    drop(semaphore_permit);
    Ok(())

}

pub async fn apply_proof_chain(
    proof_chain: &ProofChain,
    own_wc: i32,
    engine: &Arc<dyn EngineOperations>,
    shard_block_id: &BlockIdExt,
    mc_seq_no: u32,
    pre_apply: bool,
    apply_only_empty: bool,
) -> Result<bool> {

    // - chain contains proofs of blocks n, n-1, n-2 etc.
    // - each block proof contains dictionary with queue updates
    // - we need to get update for own WC
    //      - if update is empty - it was fully wrote into proof.
    //      - if not - run usual download_and_apply_block func

    log::trace!(
        "apply_proof_chain: {shard_block_id}, len: {}, mc_seq_no: {mc_seq_no}, pre_apply: {pre_apply}",
        proof_chain.len()
    );

    if proof_chain.len() == 0 {
        return Ok(true);
    }

    let mut merkle_proof = MerkleProof::construct_from_cell(proof_chain[proof_chain.len() - 1].clone())?;
    let mut next_merkle_proof;

    for i in (0..proof_chain.len()).rev() {

        let id = if let Some(next_proof_cell) = proof_chain.get(i - 1) {
            let merkle_proof = MerkleProof::construct_from_cell(next_proof_cell.clone())?;
            let block_virt_root = merkle_proof.proof.clone().virtualize(1);
            let block = Block::construct_from_cell(block_virt_root.clone())?;
            let id = block.read_info()?.read_prev_ref()?.prev1()?
                .workchain_block_id(shard_block_id.shard().clone()).1;
            next_merkle_proof = Some(merkle_proof);
            id
        } else {
            next_merkle_proof = None;
            shard_block_id.clone()
        };

        if let Some(handle) = engine.load_block_handle(&id)? {
            if !pre_apply && handle.is_applied() || pre_apply && handle.has_state() {
                merkle_proof = next_merkle_proof.unwrap_or_default();
                log::trace!("apply_proof_chain: {} already done", id);
                continue;
            }
        }

        log::trace!("apply_proof_chain: next proof {}", id);

        let data = merkle_proof.write_to_bytes()?;
        let block_stuff = BlockStuff::deserialize_queue_update(id.clone(), own_wc, data)?;
        let queue_update = block_stuff.get_queue_update_for(own_wc)?;

        log::trace!("apply_proof_chain: apply {}, is_empty {}", id, queue_update.is_empty);
        if queue_update.is_empty {
            let handle = engine.create_handle_for_empty_queue_update(&block_stuff)?
                .to_non_updated().ok_or_else(
                    || error!("INTERNAL ERROR: mismatch in empty queue update {} storing", id)
                )?;
            Arc::clone(&engine).apply_block(
                &handle,
                &block_stuff,
                mc_seq_no,
                pre_apply,
            ).await?;
        } else if !apply_only_empty {
            Arc::clone(&engine).download_and_apply_block(
                &id,
                mc_seq_no, 
                pre_apply
            ).await?;
        } else {
            log::trace!("apply_proof_chain {} finished because of non empty update", id);
            return Ok(false);
        }

        merkle_proof = next_merkle_proof.unwrap_or_default();
    }

    Ok(true)
}

pub trait BlockOrQueueUpdateBroadcast: Sync + Send {
    fn id(&self) -> &BlockIdExt;
    fn catchain_seqno(&self) -> u32;
    fn validator_set_hash(&self) -> u32;
    fn proof(&self) -> Option<Vec<u8>>;
    fn data(&self)-> Vec<u8>;
    fn is_queue_update_for(&self) -> Option<i32>;
    fn signatures(&self) -> &[ton_api::ton::ton_node::blocksignature::BlockSignature];

    fn extract_signatures(&self) -> Result<BlockSignaturesPure> {
        let mut blk_pure_signatures = BlockSignaturesPure::default();
        for api_sig in self.signatures().iter() {
            blk_pure_signatures.add_sigpair(
                CryptoSignaturePair {
                    node_id_short: api_sig.who.clone(),
                    sign: CryptoSignature::from_bytes(&api_sig.signature)?,
                }
            );
        }
        Ok(blk_pure_signatures)
    }
}

impl BlockOrQueueUpdateBroadcast for BlockBroadcast {
    fn id(&self) -> &BlockIdExt { &self.id }
    fn catchain_seqno(&self) -> u32 { self.catchain_seqno as u32 }
    fn validator_set_hash(&self) -> u32 { self.validator_set_hash as u32 }
    fn proof(&self) -> Option<Vec<u8>> { Some(self.proof.0.clone()) }
    fn data(&self)-> Vec<u8> { self.data.0.clone() }
    fn is_queue_update_for(&self) -> Option<i32> { None }
    fn signatures(&self) -> &[ton_api::ton::ton_node::blocksignature::BlockSignature] {
        &self.signatures
    }
}

impl BlockOrQueueUpdateBroadcast for QueueUpdateBroadcast {
    fn id(&self) -> &BlockIdExt { &self.id }
    fn catchain_seqno(&self) -> u32 { self.catchain_seqno as u32 }
    fn validator_set_hash(&self) -> u32 { self.validator_set_hash as u32 }
    fn proof(&self) -> Option<Vec<u8>> { None }
    fn data(&self)-> Vec<u8> { self.data.0.clone() }
    fn is_queue_update_for(&self) -> Option<i32> { Some(self.target_wc) }
    fn signatures(&self) -> &[ton_api::ton::ton_node::blocksignature::BlockSignature] {
        &self.signatures
    }
}

pub const SHARD_BROADCAST_WINDOW: u32 = 8;

pub async fn process_block_broadcast(
    engine: &Arc<dyn EngineOperations>, 
    broadcast: &dyn BlockOrQueueUpdateBroadcast
) -> Result<Option<BlockStuff>> {

    log::trace!("process_block_broadcast: {}", broadcast.id());
    if let Some(handle) = engine.load_block_handle(broadcast.id())? {
        if handle.has_data() {
            #[cfg(feature = "telemetry")] {
                let duplicate = handle.got_by_broadcast();
                let unneeded = !duplicate;
                engine.full_node_telemetry().new_block_broadcast(
                    broadcast.id(),
                    duplicate, 
                    unneeded
                );
            }
            return Ok(None);
        }
    }
    #[cfg(feature = "telemetry")]
    engine.full_node_telemetry().new_block_broadcast(broadcast.id(), false, false);

    let is_master = broadcast.id().shard().is_masterchain();
    let mut proof_opt = None;

    let last_applied_mc_state = engine.load_last_applied_mc_state().await.map_err(
        |e| error!("INTERNAL ERROR: can't load last mc state: {}", e)
    )?;


    let (is_foreign_block, own_wc) = engine.is_foreign_wc(broadcast.id().shard().workchain_id()).await?;
    let block;
    if is_foreign_block { 
        let target_wc = broadcast.is_queue_update_for()
            .ok_or_else(|| error!("Got full block broadcast {} from foreign wc", broadcast.id()))?;
        if target_wc != own_wc {
            fail!("Got queue update brodcast {} for foreign wc {}", broadcast.id(), target_wc);
        }
        block = BlockStuff::deserialize_queue_update(
            broadcast.id().clone(),
            target_wc,
            broadcast.data()
        )?;
        BlockProofStuff::check_queue_update(&block)?;

        validate_brodcast(broadcast, &last_applied_mc_state, broadcast.id())?;

    } else if let Some(proof_data) = broadcast.proof() {

        let proof = BlockProofStuff::deserialize(
            broadcast.id(),
            proof_data,
            !is_master
        )?;
        let (virt_block, _) = proof.virtualize_block()?;
        let block_info = virt_block.read_info()?;
        let prev_key_block_seqno = block_info.prev_key_block_seqno();
        if prev_key_block_seqno > last_applied_mc_state.block_id().seq_no() {
            log::debug!(
                "Skipped block broadcast {} because it refers too new key block: {}, \
                but last processed mc block is {})",
                broadcast.id(), prev_key_block_seqno, last_applied_mc_state.block_id().seq_no()
            );
            return Ok(None);
        }

        validate_brodcast(broadcast, &last_applied_mc_state, broadcast.id())?;

        // Build and save block and proof
        if is_master {
            proof.check_with_master_state(last_applied_mc_state.as_ref())?;
        } else {
            proof.check_proof_link()?;
        }

        proof_opt = Some(proof);

        block = BlockStuff::deserialize_block_checked(broadcast.id().clone(), broadcast.data())?;
    } else {
        fail!("Invalid block or queue broadcast {} - it doesn't have both proof and target_wc",
            broadcast.id());
    }
    
    let mut handle = if let Some(handle) = engine.store_block(&block).await?.to_updated() {
        handle
    } else {
        log::debug!(
            "Skipped apply for block {} broadcast because block is already in processing",
            block.id()
        );
        return Ok(None);
    };

    #[cfg(feature = "telemetry")]
    handle.set_got_by_broadcast(true);

    if let Some(proof) = proof_opt.as_ref() {
        if !handle.has_proof() {
            let result = engine.store_block_proof(block.id(), Some(handle), proof).await?;
            handle = if let Some(handle) = result.to_updated() {
                handle
            } else {
                log::debug!(
                    "Skipped apply for block {} broadcast because block is already in processing",
                    block.id()
                );
                return Ok(None);
            }
        }
    }

    if broadcast.is_queue_update_for().is_some() {
        // skip pre-apply for queue updates because it may cause unneeded 
        // network requests of empty previous updates
        return Ok(None)
    }

    // Apply (only blocks that is not too new for us)
    if is_master {
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
            .block_or_queue_update()?
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
    Ok(Some(block))
}

fn validate_brodcast(
    broadcast: &dyn BlockOrQueueUpdateBroadcast,
    mc_state: &ShardStateStuff,
    block_id: &BlockIdExt,
) -> Result<()> {

    let config = mc_state.config_params()?;
    let val_set = config.validator_set()?;
    let cc_seqno = broadcast.catchain_seqno();

    // build validator set
    let subset = if block_id.shard().is_masterchain() {
        calc_subset_for_masterchain(&val_set, config, cc_seqno)?
    } else {
         {
            calc_subset_for_workchain_standard(&val_set, config, block_id.shard(), cc_seqno)?
        }
    };

    if subset.short_hash != broadcast.validator_set_hash() {
        fail!(NodeError::InvalidData(format!(
            "Bad validator set hash in broadcast with block {}, calculated: {}, found: {}",
            block_id,
            subset.short_hash,
            broadcast.validator_set_hash()
        )));
    }

    // extract signatures - build ton_block::BlockSignaturesPure
    let blk_pure_signatures = broadcast.extract_signatures()?;

    // Check signatures
    let checked_data = ton_block::Block::build_data_for_sign(
        &block_id.root_hash,
        &block_id.file_hash
    );
    let total_weight: u64 = subset.validators.iter().map(|v| v.weight).sum();
    let weight = check_crypto_signatures(&blk_pure_signatures, &subset.validators, &checked_data)
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
