#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_macros)]

use std::sync::*;
use std::time::*;

use super::validator_query::*;
use super::validator_utils::*;
use crate::{
    engine_traits::EngineOperations,
    collator::{CollatorNew, CollatorSettings},
    collator_test_bundle::CollatorTestBundle,
};
use ton_block::{types::*, BlockIdExt, CryptoSignature, ShardIdent, ValidatorSet};
use ton_types::{Result, UInt256};
use validator_session::*;

pub async fn run_validate_query(
    shard: ShardIdent,
    min_ts: SystemTime,
    min_masterchain_block_id: BlockIdExt,
    prev: Vec<BlockIdExt>,
    block: super::validator_query::BlockCandidate,
    set: ValidatorSet,
    engine: Arc<dyn EngineOperations>,
    timeout: SystemTime,
) -> Result<SystemTime> {
    let seqno = prev.iter().fold(0, |a, b| u32::max(a, b.seq_no));
    log::info!(
        target: "validator", 
        "before validator query shard: {}, min: {}, seqno: {}",
        shard,
        min_masterchain_block_id,
        seqno + 1
    );

    let timeout = ton_block::types::UnixTime32(min_ts.duration_since(UNIX_EPOCH).unwrap().as_secs() as u32);

    if cfg!(feature = "build_test_bundles") {
        let query = super::validator_query::ValidatorQuery::new(
            shard.clone(),
            timeout,
            min_masterchain_block_id.seq_no(),
            prev.clone(),
            block.clone(),
            set,
            engine.clone(),
            0,
            false,
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
        let query = super::validator_query::ValidatorQuery::new(
            shard,
            timeout,
            min_masterchain_block_id.seq_no(),
            prev,
            block,
            set,
            engine,
            0,
            false,
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
    min_ts: SystemTime,
    min_masterchain_block_id: BlockIdExt,
    prev: Vec<BlockIdExt>,
    collator_id: PublicKey,
    set: ValidatorSet,
    engine: Arc<dyn EngineOperations>,
    timeout: SystemTime,
) -> Result<ValidatorBlockCandidate>
{
    let collator = CollatorNew::new(
        shard,
        min_masterchain_block_id,
        prev,
        set,
        UInt256::from(collator_id.pub_key()?),
        engine,
        None, // TODO rand: Option<UInt256>,
        CollatorSettings {
            want_split: None,
            want_merge: None,
            is_fake: false,
        }
    )?;

    let (candidate, _) = collator.collate().await?;

    Ok(validator_query_candidate_to_validator_block_candidate (collator_id, candidate))
}

