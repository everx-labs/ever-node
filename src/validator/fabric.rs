/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific EVERX DEV software governing permissions and
* limitations under the License.
*/

use std::{
    sync::Arc,
    time::SystemTime,
};
use super::validator_utils::{
    get_first_block_seqno_after_prevs,
    pairvec_to_cryptopair_vec,
    validator_query_candidate_to_validator_block_candidate,
};
use crate::{
    collator_test_bundle::CollatorTestBundle,
    engine_traits::{EngineOperations, RempQueueCollatorInterface},
    validating_utils::{fmt_next_block_descr_from_next_seqno, fmt_next_block_descr},
    validator::{CollatorSettings, validate_query::ValidateQuery, collator, verification::VerificationManagerPtr}
};
use ever_block::{Block, BlockIdExt, Deserializable, Result, ShardIdent, UInt256, ValidatorSet};
use validator_session::{ValidatorBlockCandidate, BlockPayloadPtr, PublicKeyHash, PublicKey};

#[allow(dead_code)]
pub async fn run_validate_query_any_candidate(
    block: super::BlockCandidate,
    engine: Arc<dyn EngineOperations>,
) -> Result<SystemTime> {
    let real_block = Block::construct_from_bytes(&block.data)?;
    let shard = block.block_id.shard().clone();
    let info = real_block.read_info()?;
    let prev = info.read_prev_ids()?;
    let mc_state = engine.load_last_applied_mc_state().await?;
    let min_masterchain_block_id = mc_state.find_block_id(info.min_ref_mc_seqno())?;
    let mut cc_seqno_with_delta = 0;
    if let Some(mc_state_extra) = mc_state.state()?.read_custom()? {
        let cc_seqno_from_state = if shard.is_masterchain() {
            mc_state_extra.validator_info.catchain_seqno
        } else {
            mc_state_extra.shards.calc_shard_cc_seqno(&shard)?
        };
        let nodes = crate::validator::validator_utils::compute_validator_set_cc(
            &*engine.load_last_applied_mc_state().await?,
            &shard,
            engine.now(),
            cc_seqno_from_state,
            &mut cc_seqno_with_delta
        )?;
        let validator_set = ValidatorSet::with_cc_seqno(0, 0, 0, cc_seqno_with_delta, nodes)?;

        log::debug!(
            target: "verificator", 
            "ValidatorSetForVerification cc_seqno: {:?}", validator_set.cc_seqno()
        );
        run_validate_query(
            shard,
            SystemTime::now(),
            min_masterchain_block_id,
            prev,
            block,
            validator_set,
            engine,
            SystemTime::now(),
            None, //no verification manager for validations within verification
        ).await
    } else {
        Err(failure::format_err!("MC state is None"))
    }
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
    verification_manager: Option<VerificationManagerPtr>,
) -> Result<SystemTime> {

    let next_block_descr = fmt_next_block_descr(&block.block_id);

    let seqno = prev.iter().fold(0, |a, b| u32::max(a, b.seq_no));
    log::info!(
        target: "validator", 
        "({}): before validator query shard: {}, min: {}, seqno: {}",
        next_block_descr,
        shard,
        min_masterchain_block_id,
        seqno + 1
    );

    let labels = [("shard", shard.to_string())];
    #[cfg(not(feature = "statsd"))]
    metrics::increment_gauge!("run_validators", 1.0 ,&labels);

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
            verification_manager,
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
            verification_manager,
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
                                shard, min_masterchain_block_id, prev, block, &engine
                            ).await {
                                Err(e) => log::error!(
                                    "({}): Error while test bundle for {} building: {}", next_block_descr, id, e
                                ),
                                Ok(mut b) => {
                                    b.set_notes(err_str);
                                    if let Err(e) = b.save(&path) {
                                        log::error!("({}): Error while test bundle for {} saving: {}", next_block_descr, id, e)
                                    } else {
                                        log::info!("({}): Built test bundle for {}", next_block_descr, id)
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

    #[cfg(not(feature = "statsd"))]
    metrics::decrement_gauge!("run_validators", 1.0, &labels);

    match validator_result {
        Ok(_) => {
            metrics::increment_counter!("successful_validations", &labels);
            Ok(SystemTime::now())
        }
        Err(e) =>  {
            metrics::increment_counter!("failed_validations", &labels);

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
) -> Result<()> {
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
    min_mc_seqno: u32,
    prev: Vec<BlockIdExt>,
    remp_collator_interface: Option<Arc<dyn RempQueueCollatorInterface>>,
    collator_id: PublicKey,
    set: ValidatorSet,
    engine: Arc<dyn EngineOperations>,
) -> Result<ValidatorBlockCandidate>
{
    #[cfg(not(feature = "statsd"))]
    let labels = [("shard", shard.to_string())];
    #[cfg(not(feature = "statsd"))]
    metrics::increment_gauge!("run_collators", 1.0, &labels);

    let next_block_descr = fmt_next_block_descr_from_next_seqno(&shard, get_first_block_seqno_after_prevs(&prev));

    let collator = collator::Collator::new(
        shard.clone(),
        min_mc_seqno,
        prev.clone(),
        set,
        UInt256::from(collator_id.pub_key()?),
        engine.clone(),
        None,
        remp_collator_interface,
        CollatorSettings::default()
    )?;
    let collator_result = collator.collate().await;


    let labels = [("shard", shard.to_string())];
    #[cfg(not(feature = "statsd"))]
    metrics::decrement_gauge!("run_collators", 1.0, &labels);

    match collator_result {
        Ok((candidate, _)) => {
            metrics::increment_counter!("successful_collations", &labels);

            return Ok(validator_query_candidate_to_validator_block_candidate(collator_id, candidate))
        }
        Err(err) => {
            let labels = [("shard", shard.to_string())];
            metrics::increment_counter!("failed_collations", &labels);
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
                            match CollatorTestBundle::build_for_collating_block(prev, &engine).await {
                                Err(e) => log::error!("({}): Error while test bundle for {} building: {}", next_block_descr, id, e),
                                Ok(mut b) => {
                                    b.set_notes(err_str.to_string());
                                    if let Err(e) = b.save(&path) {
                                        log::error!("({}): Error while test bundle for {} saving: {}", next_block_descr, id, e);
                                    } else {
                                        log::info!("({}): Built test bundle for {}", next_block_descr, id);
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
