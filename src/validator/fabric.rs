use std::{
    sync::Arc,
    time::SystemTime,
    ops::Deref,
};
use super::validator_utils::{validator_query_candidate_to_validator_block_candidate, pairvec_to_cryptopair_vec};
use crate::{
    collator_test_bundle::CollatorTestBundle, engine_traits::EngineOperations, 
    validator::{collator_sync::Collator, CollatorSettings, validate_query::ValidateQuery}
};
use ton_block::{BlockIdExt, ShardIdent, ValidatorSet};
use ton_types::{Result, UInt256};
use validator_session::{ValidatorBlockCandidate, BlockPayloadPtr, PublicKeyHash, PublicKey};

pub async fn run_validate_query(
    shard: ShardIdent,
    _min_ts: SystemTime,
    min_masterchain_block_id: BlockIdExt,
    prev: Vec<BlockIdExt>,
    block: super::BlockCandidate,
    set: ValidatorSet,
    engine: Arc<dyn EngineOperations>,
    _timeout: SystemTime,
) -> Result<SystemTime> {

    let seqno = prev.iter().fold(0, |a, b| u32::max(a, b.seq_no));
    log::info!(
        target: "validator", 
        "before validator query shard: {}, min: {}, seqno: {}",
        shard,
        min_masterchain_block_id,
        seqno + 1
    );

    let test_bundles_config = &engine.test_bundles_config().validator;
    if !test_bundles_config.is_enable() {
        ValidateQuery::new(
            shard,
            min_masterchain_block_id.seq_no(),
            prev,
            block,
            set,
            engine,
            false,
            true,
        ).try_validate().await?;
        return Ok(SystemTime::now())
    }

    let query = ValidateQuery::new(
        shard.clone(),
        min_masterchain_block_id.seq_no(),
        prev.clone(),
        block.clone(),
        set,
        engine.clone(),
        false,
        true,
    );

    if let Err(err) = query.try_validate().await {
        let err_str = err.to_string();
        if test_bundles_config.need_to_build_for(&err_str) {
            let id = block.block_id.clone();
            if !CollatorTestBundle::exists(test_bundles_config.path(), &id) {
                let path = test_bundles_config.path().to_string();
                let engine = engine.clone();
                tokio::spawn(
                    async move {
                        match CollatorTestBundle::build_for_validating_block(
                            shard, min_masterchain_block_id, prev, block, engine
                        ).await {
                            Err(e) => log::error!(
                                "Error while test bundle for {} building: {}", id, e
                            ),
                            Ok(mut b) => {
                                b.set_notes(err_str);
                                if let Err(e) = b.save(&path) {
                                    log::error!("Error while test bundle for {} saving: {}", id, e)
                                } else {
                                    log::info!("Built test bundle for {}", id)
                                }
                            }
                        }
                    }
                );
            }
        }
        Err(err)
    } else {
        Ok(SystemTime::now())
    }

}

pub async fn run_accept_block_query(
    id: BlockIdExt,
    data: Option<Vec<u8>>,
    prev: Vec<BlockIdExt>,
    set: ValidatorSet,
    signatures: Vec<(PublicKeyHash, BlockPayloadPtr)>,
    approve_signatures: Vec<(PublicKeyHash, BlockPayloadPtr)>,
    send_broadcast: bool,
    engine: Arc<dyn EngineOperations>,
) -> ton_types::Result<()> {
    let sigs = pairvec_to_cryptopair_vec(signatures)?;
    let approve_sigs = pairvec_to_cryptopair_vec(approve_signatures)?;
    super::accept_block::accept_block(
        id,
        data,
        prev,
        set,
        sigs,
        approve_sigs,
        send_broadcast,
        engine,
    )
    .await
}

pub async fn run_collate_query (
    shard: ShardIdent,
    _min_ts: SystemTime,
    min_masterchain_block_id: BlockIdExt,
    prev: Vec<BlockIdExt>,
    collator_id: PublicKey,
    set: ValidatorSet,
    engine: Arc<dyn EngineOperations>,
    _timeout: SystemTime,
) -> Result<ValidatorBlockCandidate>
{
    let collator = Collator::new(
        shard,
        min_masterchain_block_id,
        prev.clone(),
        set,
        UInt256::from(collator_id.pub_key()?),
        engine.clone(),
        None,
        CollatorSettings::default()
    )?;
    match collator.collate().await {
        Ok((candidate, _)) => {
            return Ok(validator_query_candidate_to_validator_block_candidate(collator_id, candidate))
        }
        Err(err) => {
            let test_bundles_config = &engine.test_bundles_config().collator;
            if test_bundles_config.is_enable() {
                let err_str = err.to_string();
                if test_bundles_config.need_to_build_for(&err_str) {
                    let id = BlockIdExt {
                        shard_id: shard,
                        seq_no: prev.iter().max_by_key(|id| id.seq_no()).unwrap().seq_no() + 1,
                        root_hash: UInt256::default(),
                        file_hash: UInt256::default(),
                    };
                    if !CollatorTestBundle::exists(test_bundles_config.path(), &id) {
                        let path = test_bundles_config.path().to_string();
                        let engine = engine.clone();
                        tokio::spawn(async move {
                            match CollatorTestBundle::build_for_collating_block(prev, engine.deref()).await {
                                Err(e) => log::error!("Error while test bundle for {} building: {}", id, e),
                                Ok(mut b) => {
                                    b.set_notes(err_str);
                                    if let Err(e) = b.save(&path) {
                                        log::error!("Error while test bundle for {} saving: {}", id, e);
                                    } else {
                                        log::info!("Built test bundle for {}", id);
                                    }
                                }
                            }
                        });
                    }
                }
            }
            return Err(err);
        }
    }
}

