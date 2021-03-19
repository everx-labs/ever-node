use crate::{
    block::{BlockStuff, convert_block_id_ext_api2blk}, block_proof::BlockProofStuff, 
    engine_traits::EngineOperations, error::NodeError
};

use std::{sync::Arc, mem::drop};
use tokio::task::JoinHandle;
use ton_block::{
    BlockIdExt, BlockSignaturesPure, CryptoSignaturePair, CryptoSignature,
    AccountIdPrefixFull, UnixTime32, ValidatorSet, CatchainConfig,
};
use ton_types::{Result, fail, error, UInt256};
use ton_api::ton::ton_node::broadcast::BlockBroadcast;

pub fn start_masterchain_client(engine: Arc<dyn EngineOperations>, last_got_block_id: BlockIdExt) -> Result<JoinHandle<()>> {
    let join_handle = tokio::spawn(async move {
        if let Err(e) = load_master_blocks_cycle(engine, last_got_block_id).await {
            log::error!("FATAL!!! Unexpected error in master blocks loading cycle: {:?}", e);
        }
    });
    Ok(join_handle)
}

pub fn start_shards_client(engine: Arc<dyn EngineOperations>, shards_mc_block_id: BlockIdExt) -> Result<JoinHandle<()>> {
    let join_handle = tokio::spawn(async move {
        if let Err(e) = load_shard_blocks_cycle(engine, shards_mc_block_id).await {
            log::error!("FATAL!!! Unexpected error in shards client: {:?}", e);
        }
    });
    Ok(join_handle)
}

async fn load_master_blocks_cycle(
    engine: Arc<dyn EngineOperations>, 
    mut last_got_block_id: BlockIdExt
) -> Result<()> {
    let mut attempt = 0;
    loop {
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

    let prev_state = engine.clone().wait_state(&prev_id, None).await?;
    proof.check_with_master_state(&prev_state)?;
    let mut next_handle = if let Some(next_handle) = engine.load_block_handle(block.id())? {
        if !next_handle.has_data() {
            fail!("Unitialized handle detected for block {}", block.id())
        }
        next_handle
    } else {
        engine.store_block(&block).await?
    };
    if !next_handle.has_proof() {
        next_handle = engine.store_block_proof(block.id(), Some(next_handle), &proof).await?;
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
    shards_mc_block_id: BlockIdExt
) -> Result<()> {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(SHARD_CLIENT_WINDOW));
    let mut mc_handle = engine.load_block_handle(&shards_mc_block_id)?.ok_or_else(
        || error!("Cannot load handle for shard master block {}", shards_mc_block_id)
    )?;
    loop {
        log::trace!("load_shard_blocks_cycle: mc block: {}", mc_handle.id());
        let r = engine.wait_next_applied_mc_block(&mc_handle, None).await?;
        mc_handle = r.0;
        let mc_block = r.1;

        log::trace!("load_shard_blocks_cycle: waiting semaphore: {}", mc_block.id());
        let semaphore_permit = Arc::clone(&semaphore).acquire_owned().await?;

        log::trace!("load_shard_blocks_cycle: process next mc block: {}", mc_block.id());

        let engine = Arc::clone(&engine);
        tokio::spawn(async move {
            if let Err(e) = load_shard_blocks(engine, semaphore_permit, &mc_block).await {
                log::error!("FATAL!!! Unexpected error in shard blocks processing for mc block {}: {:?}", mc_block.id(), e);
            }
        });
    }
}

pub async fn load_shard_blocks(
    engine: Arc<dyn EngineOperations>,
    semaphore_permit: tokio::sync::OwnedSemaphorePermit,
    mc_block: &BlockStuff
) -> Result<()> {

    let mut apply_tasks = Vec::new();
    let mc_seq_no = mc_block.id().seq_no();
    for (shard_ident, shard_block_id) in mc_block.shards_blocks()?.iter() {
        let msg = format!(
            "process mc block {}, shard block {} {}", 
            mc_block.id(), shard_ident, shard_block_id
        );
        if let Some(shard_block_handle) = engine.load_block_handle(shard_block_id)? {
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
    engine.store_shards_client_mc_block_id(mc_block.id()).await?;
    drop(semaphore_permit);                                    	
    Ok(())

}

const SHARD_BROADCAST_WINDOW: u32 = 8;

pub async fn process_block_broadcast(
    engine: &Arc<dyn EngineOperations>, 
    broadcast: &BlockBroadcast
) -> Result<()> {

    log::trace!("process_block_broadcast: {}", broadcast.id);
    let block_id = convert_block_id_ext_api2blk(&broadcast.id)?;
    if let Some(handle) = engine.load_block_handle(&block_id)? {
        if handle.has_data() {
            return Ok(());
        }
    }

    let is_master = block_id.shard().is_masterchain();
    let proof = BlockProofStuff::deserialize(&block_id, broadcast.proof.0.clone(), !is_master)?;
    let block_info = proof.virtualize_block()?.0.read_info()?;
    let prev_key_block_seqno = block_info.prev_key_block_seqno();
    let last_applied_mc_block_id = engine.load_last_applied_mc_block_id().await?;
    if prev_key_block_seqno > last_applied_mc_block_id.seq_no() {
        log::debug!(
            "Skipped block broadcast {} because it refers too new key block: {}, but last processed mc block is {})",
            block_id, prev_key_block_seqno, last_applied_mc_block_id.seq_no()
        );
        return Ok(());
    }

    // get validator set from...
    let mut key_block_proof = None;
    let mut zerostate = None;
    let (validator_set, cc_config) = if prev_key_block_seqno == 0 {
        // ...zerostate
        let zs = engine.load_mc_zero_state().await?;
        let vs = zs.state().read_cur_validator_set_and_cc_conf()?;
        zerostate = Some(zs);
        vs
    } else {
        // ...prev key block
        let mc_pfx = AccountIdPrefixFull::any_masterchain();
        let handle = engine.find_block_by_seq_no(&mc_pfx, prev_key_block_seqno).await?;
        let proof = engine.load_block_proof(&handle, false).await?;
        let vs = proof.get_cur_validators_set()?;
        key_block_proof = Some(proof);
        vs
    };

    validate_brodcast(broadcast, &block_id, &validator_set, &cc_config)?;

    // Build and save block and proof
    if is_master {
        if prev_key_block_seqno == 0 {
            proof.check_with_master_state(zerostate.as_ref().unwrap())?;
        } else {
            proof.check_with_prev_key_block_proof(key_block_proof.as_ref().unwrap())?;
        }
    } else {
        proof.check_proof_link()?;
    }
    let block = BlockStuff::deserialize_checked(block_id, broadcast.data.0.clone())?;
    let mut handle = engine.store_block(&block).await?; 
    if !handle.has_proof() {
        handle = engine.store_block_proof(block.id(), Some(handle), &proof).await?;
    }

    // Apply (only blocks that is not too new for us)
    if block.id().shard().is_masterchain() {
        if block.id().seq_no() == last_applied_mc_block_id.seq_no() + 1 {
            engine.clone().apply_block(&handle, &block, block.id().seq_no(), false).await?;
        } else {
            log::debug!(
                "Skipped apply for block broadcast {} because it is too new (last master block: {})",
                block.id(), last_applied_mc_block_id.seq_no()
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
        let shards_client_mc_block_id = engine.load_shards_client_mc_block_id().await?;
        if shards_client_mc_block_id.seq_no() + SHARD_BROADCAST_WINDOW >= master_ref.master.seq_no {
            engine.clone().apply_block(&handle, &block, shards_client_mc_block_id.seq_no(), true).await?;
        } else {
            log::debug!(
                "Skipped pre-apply for block broadcast {} because it refers to master block {}, but shard client is on {}",
                block.id(), master_ref.master.seq_no, shards_client_mc_block_id.seq_no()
            )
        }
    }
    Ok(())
}

fn validate_brodcast(
    broadcast: &BlockBroadcast,
    block_id: &BlockIdExt,
    validator_set: &ValidatorSet,
    cc_config: &CatchainConfig,
) -> Result<()> {

    // build validator set
    let (validators, validators_hash_short) = validator_set.calc_subset(
        &cc_config, 
        block_id.shard().shard_prefix_with_tag(), 
        block_id.shard().workchain_id(), 
        broadcast.catchain_seqno as u32,
        UnixTime32(0) // TODO: unix time is not realy used in algorithm, but exists in t-node,
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
                node_id_short: UInt256::from(&api_sig.who.0),
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
    let weight = blk_pure_signatures.check_signatures(validators, &checked_data)
        .map_err(|err| { 
            NodeError::InvalidData(
                format!("Bad signatures in broadcast with block {}: {}", block_id, err)
            )
        })?;

    if weight * 3 <= total_weight * 2 {
        fail!(NodeError::InvalidData(format!(
            "Too small signatures weight in broadcast with block {}",
            block_id,
        )));
    }

    Ok(())
}
