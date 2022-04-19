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

use std::{
    ops::Deref,
    sync::Arc,
    time::SystemTime,
};
use super::validator_utils::{validator_query_candidate_to_validator_block_candidate, pairvec_to_cryptopair_vec};
use crate::{
    collator_test_bundle::CollatorTestBundle, engine_traits::EngineOperations, 
    validator::{CollatorSettings, validate_query::ValidateQuery, collator}
};
use ton_block::{BlockIdExt, ShardIdent, ValidatorSet, Deserializable};
use ton_types::{Result, UInt256};
use validator_session::{ValidatorBlockCandidate, BlockPayloadPtr, PublicKeyHash, PublicKey};

#[cfg(feature = "metrics")]
use crate::engine::STATSD;

#[allow(dead_code)]
pub async fn run_validate_query_any_candidate(
    block: super::BlockCandidate,
    engine: Arc<dyn EngineOperations>,
) -> Result<SystemTime> {
    let real_block = ton_block::Block::construct_from_bytes(&block.data)?;
    let shard = block.block_id.shard().clone();
    let info = real_block.read_info()?;
    let prev = info.read_prev_ids()?;
    let mc_state = engine.load_last_applied_mc_state().await?;
    let min_masterchain_block_id = mc_state.find_block_id(info.min_ref_mc_seqno())?;
    let (set, _) = mc_state.read_cur_validator_set_and_cc_conf()?;
    run_validate_query(
        shard,
        SystemTime::now(),
        min_masterchain_block_id,
        prev,
        block,
        set,
        engine,
        SystemTime::now()
    ).await
}

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

    #[cfg(feature = "metrics")]
    STATSD.incr(&format!("run_validators_{}", shard));

    let test_bundles_config = &engine.test_bundles_config().validator;
    let validator_result = if !test_bundles_config.is_enable() {
        ValidateQuery::new(
            shard.clone(),
            min_masterchain_block_id.seq_no(),
            prev,
            block,
            set,
            engine.clone(),
            false,
            true,
        ).try_validate().await
    } else {
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
        let validator_result = query.try_validate().await;
        if let Err(err) = &validator_result {
            let err_str = err.to_string();
            if test_bundles_config.need_to_build_for(&err_str) {
                let id = block.block_id.clone();
                if !CollatorTestBundle::exists(test_bundles_config.path(), &id) {
                    let path = test_bundles_config.path().to_string();
                    let engine = engine.clone();
                    let shard = shard.clone();
                    tokio::spawn(
                        async move {
                            match CollatorTestBundle::build_for_validating_block(
                                shard, min_masterchain_block_id, prev, block, engine.deref()
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
        };
        validator_result
    };

    #[cfg(feature = "metrics")]
    STATSD.decr(&format!("run_validators_{}", shard));

    match validator_result {
        Ok(_) => {
            #[cfg(feature = "metrics")]
            STATSD.incr(&format!("succeessful_validations_{}", shard));
            Ok(SystemTime::now())
        }
        Err(e) =>  {
            #[cfg(feature = "metrics")]
            STATSD.incr(&format!("failed_validations_{}", shard));

            #[cfg(feature = "telemetry")]
            engine.validator_telemetry().failed_attempt(&shard, &e.to_string());

            Err(e)
        }
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
    timeout: u32,
) -> Result<ValidatorBlockCandidate>
{
    #[cfg(feature = "metrics")]
    STATSD.incr(&format!("run_collators_{}", shard));

    let collator = collator::Collator::new(
        shard.clone(),
        min_masterchain_block_id,
        prev.clone(),
        set,
        UInt256::from(collator_id.pub_key()?),
        engine.clone(),
        None,
        CollatorSettings::default()
    )?;
    let collator_result = collator.collate(timeout).await;

    #[cfg(feature = "metrics")]
    STATSD.decr(&format!("run_collators_{}", shard));

    match collator_result {
        Ok((candidate, _)) => {
            #[cfg(feature = "metrics")]
            STATSD.incr(&format!("succeessful_collations_{}", shard));

            return Ok(validator_query_candidate_to_validator_block_candidate(collator_id, candidate))
        }
        Err(err) => {
            #[cfg(feature = "metrics")]
            STATSD.incr(&format!("failed_collations_{}", shard));
            let test_bundles_config = &engine.test_bundles_config().collator;

            let err_str = if test_bundles_config.is_enable() {
                err.to_string()
            } else {
                String::default()
            };

            #[cfg(feature = "telemetry")]
            engine.collator_telemetry().failed_attempt(&shard, &err_str);

            if test_bundles_config.is_enable() {
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
                                    b.set_notes(err_str.to_string());
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

