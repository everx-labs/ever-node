use std::sync::Arc;
use std::time::SystemTime;

use super::validator_utils::{validator_query_candidate_to_validator_block_candidate, pairvec_to_cryptopair_vec};
use crate::{
    engine_traits::EngineOperations,
    validator::{collator::Collator, CollatorSettings, validate_query::ValidateQuery},
    collator_test_bundle::CollatorTestBundle,
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

    if cfg!(feature = "build_test_bundles") {
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
            if format!("{}", err).contains("new masterchain validator list hash") {
                let id = block.block_id.clone();
                match CollatorTestBundle::build_for_validating_block(
                    shard, min_masterchain_block_id, prev, block, engine).await {
                    Err(e) => log::error!("Error while test bundle for {} building: {}", id, e),
                    Ok(b) => {
                        if let Err(e) = b.save("/shared/") {
                            log::error!("Error while test bundle for {} saving: {}", id, e);
                        } else {
                            log::info!("Built test bundle for {}", id);
                        }
                    }
                }
            }
            return Err(err);
        }
    } else {
        let query = ValidateQuery::new(
            shard,
            min_masterchain_block_id.seq_no(),
            prev,
            block,
            set,
            engine,
            false,
            true,
        );
        query.try_validate().await?;
    }

    Ok(SystemTime::now())
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
        prev,
        set,
        UInt256::from(collator_id.pub_key()?),
        engine,
        None,
        CollatorSettings::default()
    )?;

    let (candidate, _) = collator.collate().await?;

    Ok(validator_query_candidate_to_validator_block_candidate (collator_id, candidate))
}

