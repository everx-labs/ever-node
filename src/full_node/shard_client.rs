use crate::{
    block::{BlockStuff, convert_block_id_ext_api2blk},
    block_proof::BlockProofStuff,
    engine_traits::EngineOperations,
    error::NodeError,
};

use std::{sync::Arc, ops::Deref};
use tokio::task::JoinHandle;
use ton_block::{
    BlockIdExt, BlockSignaturesPure, CryptoSignaturePair, CryptoSignature,
    AccountIdPrefixFull, UnixTime32, ValidatorSet, CatchainConfig,
};
use ton_types::{Result, fail, error, UInt256};
use ton_api::ton::ton_node::broadcast::BlockBroadcast;

pub struct ShardClient {
    engine: Arc<dyn EngineOperations>
}

impl ShardClient {
    pub fn new(engine: Arc<dyn EngineOperations>) -> Result<ShardClient> {
        Ok(
            ShardClient {
                engine
            }
        )
    }

    pub async fn start_master_chain(self: Arc<Self>, last_got_block_id: BlockIdExt) -> Result<JoinHandle<()>> {
        let join_handle = tokio::spawn(async move {
            if let Err(e) = self.load_blocks_cycle(last_got_block_id).await {
                log::error!("FATAL!!! Unexpected error in master blocks loading cycle: {:?}", e);
            }
        });
        Ok(join_handle)
    }

    async fn load_blocks_cycle(self: Arc<Self>, last_got_block_id: BlockIdExt) -> Result<()> {
        let mut handle = self.engine.load_block_handle(&last_got_block_id)?;
        loop {
            handle = if handle.next1_inited() {
                let next_handle = self.engine.load_block_handle(
                    &self.engine.load_block_next1(handle.id()).await?
                )?;
                if !next_handle.applied() {
                    self.engine.clone().apply_block(next_handle.deref(), None).await?;
                }
                next_handle
            } else {
                let (block, proof) = self.engine.download_next_block(handle.id()).await?;
                log::trace!("load_blocks_cycle: got next block: {}", block.id());

                if block.id().seq_no != handle.id().seq_no + 1 {
                    fail!("Invalid next master block got: {}, prev: {}", block.id(), handle.id());
                }

                let next_handle = self.engine.load_block_handle(block.id())?;

                if !next_handle.proof_inited() {
                    let prev_state = self.engine.wait_state(&handle).await?;
                    proof.check_with_master_state(&prev_state)?;
                    self.engine.store_block_proof(&next_handle, &proof).await?;
                }
                if !next_handle.data_inited() {
                    self.engine.store_block(&next_handle, &block).await?;
                }
                self.engine.clone().apply_block(next_handle.deref(), Some(&block)).await?;
                next_handle
            };
        }
    }
}

pub async fn process_block_broadcast(engine: &Arc<dyn EngineOperations>, broadcast: &BlockBroadcast) -> Result<()> {
    log::trace!("process_block_broadcast: {}", broadcast.id);

    let block_id = convert_block_id_ext_api2blk(&broadcast.id)?;
    let handle = engine.load_block_handle(&block_id)?;

    if handle.applied() {
        return Ok(());
    }

    let is_master = handle.id().shard().is_masterchain();
    let proof = BlockProofStuff::deserialize(&block_id, &broadcast.proof.0, !is_master)?;
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
        let vs = zs.shard_state().read_cur_validator_set_and_cc_conf()?;
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
    let block = BlockStuff::deserialize(&broadcast.data.0)?;
    if *block.id() != block_id {
        fail!("Block's id and broadcast's id mismatch");
    }
    if !handle.proof_inited() {
        if is_master {
            if prev_key_block_seqno == 0 {
                proof.check_with_master_state(zerostate.as_ref().unwrap())?;
            } else {
                proof.check_with_prev_key_block_proof(key_block_proof.as_ref().unwrap())?;
            }
        } else {
            proof.check_proof_link()?;
        }
        engine.store_block_proof(&handle, &proof).await?;
    }
    if !handle.data_inited() {
        engine.store_block(&handle, &block).await?;
    }

    // Apply (only blocks that is not too new for us)
    if block.id().shard().is_masterchain() {
        if block.id().seq_no() == last_applied_mc_block_id.seq_no() + 1 {
            engine.clone().apply_block(handle.deref(), Some(&block)).await?;
        } else {
            log::debug!(
                "Skipped apply for block broadcast {} because it is too new (last master block: {})",
                block.id(), last_applied_mc_block_id.seq_no())
        }
    } else {
        let master_ref = block
            .block()
            .read_info()?
            .read_master_ref()?
            .ok_or_else(|| NodeError::InvalidData(format!(
                "Block {} doesn't contain masterchain block extra", block.id(),
            )))?;
        if last_applied_mc_block_id.seq_no() >= master_ref.master.seq_no {
            engine.clone().apply_block(handle.deref(), Some(&block)).await?;
        } else {
            log::debug!(
                "Skipped apply for block broadcast {} because it refers to master block {}, but exists {}",
                block.id(), master_ref.master.seq_no, last_applied_mc_block_id.seq_no())
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