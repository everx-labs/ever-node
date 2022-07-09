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

#[cfg(feature = "metrics")]
use crate::engine::STATSD;
use crate::engine_traits::EngineOperations;
use crate::engine_traits::ValidatedBlockStat;
use crate::engine_traits::ValidatedBlockStatNode;
use crate::shard_state::ShardStateStuff;
use crate::validator::UInt256;
use ever_crypto::Ed25519KeyOption;
use num_bigint::BigUint;
use rand::Rng;
use spin::mutex::SpinMutex;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use storage::block_handle_db::BlockHandle;
use ton_abi::contract::Contract;
use ton_abi::function::Function;
use ton_abi::Token;
use ton_abi::TokenValue;
use ton_abi::Uint;
use ton_block::ConfigParamEnum;
use ton_block::ExternalInboundMessageHeader;
use ton_block::HashmapAugType;
use ton_block::Message;
use ton_block::MsgAddressExt;
use ton_block::MsgAddressInt;
use ton_block::Serializable;
use ton_block::SlashingConfig;
use ton_types::Result;
use ton_types::SliceData;
use validator_session::slashing::AggregatedMetric;
use validator_session::PrivateKey;
use validator_session::PublicKey;
#[cfg(feature = "metrics")]
use validator_session::PublicKeyHash;
#[cfg(feature = "metrics")]
use validator_session::SlashingAggregatedValidatorStat;
use validator_session::SlashingValidatorStat;

const ELECTOR_ABI: &[u8] = include_bytes!("Elector.abi.json"); //elector's ABI
const ELECTOR_REPORT_FUNC_NAME: &str = "report"; //elector slashing report function name

/// Slashing manager pointer
pub type SlashingManagerPtr = Arc<SlashingManager>;

/// Slashing manager
pub struct SlashingManager {
    manager: SpinMutex<SlashingManagerImpl>,
    send_messages_block_offset: u32,
}

/// Internal slashing manager details
struct SlashingManagerImpl {
    stat: SlashingValidatorStat,
    first_mc_block: u32,
    slashing_messages: HashMap<UInt256, Arc<Message>>,
    report_fn: Function,
}

impl SlashingManager {
    /// Create new slashing manager
    pub(crate) fn create() -> SlashingManagerPtr {
        let contract = Contract::load(ELECTOR_ABI).expect("Elector's ABI must be valid");
        let report_fn = contract
            .function(ELECTOR_REPORT_FUNC_NAME)
            .expect("Elector contract must have 'report' function for slashing")
            .clone();

        log::info!(target: "validator", "Use slashing report function '{}' with id={:08X}",
            ELECTOR_REPORT_FUNC_NAME, report_fn.get_function_id());

        let mut rng = rand::thread_rng();

        Arc::new(SlashingManager {
            send_messages_block_offset: rng.gen(),
            manager: SpinMutex::new(SlashingManagerImpl {
                stat: SlashingValidatorStat::default(),
                first_mc_block: 0,
                slashing_messages: HashMap::new(),
                report_fn,
            }),
        })
    }

    /// Update slashing statistics
    pub fn update_statistics(&self, stat: &SlashingValidatorStat) {
        self.manager.lock().stat.merge(&stat);
        log::debug!(target: "validator", "{}({}): merge slashing statistics {:?}", file!(), line!(), stat);
    }

    /// Is slashing available
    fn is_slashing_available(mc_state: &ShardStateStuff) -> bool {
        if let Ok(state) = mc_state.state().read_custom() {
            if let Some(state) = state {
                if let Ok(vset) = state.config.prev_validator_set() {
                    return vset.list().len() > 0 && vset.total_weight() > 0;
                }
            }
        }

        false
    }

    /// Get slashing params
    fn get_slashing_config(mc_state: &ShardStateStuff) -> SlashingConfig {
        if let Ok(Some(mc_state_extra)) = mc_state.state().read_custom() {
            if let Ok(config) = mc_state_extra.config.config(40) {
                if let Some(ConfigParamEnum::ConfigParam40(cc)) = config {
                    return SlashingConfig {
                        slashing_period_mc_blocks_count : cc.slashing_config.slashing_period_mc_blocks_count,
                        resend_mc_blocks_count : cc.slashing_config.resend_mc_blocks_count,
                        min_samples_count : cc.slashing_config.min_samples_count,
                        collations_score_weight : cc.slashing_config.collations_score_weight,
                        signing_score_weight : cc.slashing_config.signing_score_weight,
                        min_slashing_protection_score : cc.slashing_config.min_slashing_protection_score,
                        z_param_numerator : cc.slashing_config.z_param_numerator,
                        z_param_denominator : cc.slashing_config.z_param_denominator,
                    }
                }
            }
        }

        SlashingConfig {
            slashing_period_mc_blocks_count: 100,
            resend_mc_blocks_count: 4,
            min_samples_count: 30,
            collations_score_weight: 0,
            signing_score_weight: 1,
            min_slashing_protection_score: 70,
            z_param_numerator: 2326, //98% confidence
            z_param_denominator: 1000,
        }
    }

    /// New masterchain block notification
    pub async fn handle_masterchain_block(
        &self,
        block: &Arc<BlockHandle>,
        mc_state: &ShardStateStuff,
        local_key: &PrivateKey,
        engine: &Arc<dyn EngineOperations>,
    ) {
        // read slashing params

        let slashing_config = Self::get_slashing_config(mc_state);

        // process queue of received validated block stat events

        while let Ok(validated_block_stat) = engine.pop_validated_block_stat() {
            log::debug!(target: "validator", "Slashing statistics has been received {:?}", validated_block_stat);

            let mut slashing_stat = SlashingValidatorStat::default();

            for src_node in &validated_block_stat.nodes {
                use validator_session::slashing;

                let pub_key = src_node.public_key.key_bytes();
                let public_key = Ed25519KeyOption::from_public_key(pub_key);
                let mut dst_node = slashing::Node::new(&public_key);

                dst_node.metrics[slashing::Metric::ApplyLevelTotalBlocksCount as usize] += 1;

                if src_node.collated {
                    dst_node.metrics[slashing::Metric::ApplyLevelCollationsCount as usize] += 1;
                }

                if src_node.signed {
                    dst_node.metrics[slashing::Metric::ApplyLevelCommitsCount as usize] += 1;
                }

                slashing_stat
                    .validators_stat
                    .insert(public_key.id().clone(), dst_node);
            }

            self.update_statistics(&slashing_stat);
        }

        if !Self::is_slashing_available(mc_state) {
            log::info!(target: "validator", "{}({}): slashing is disabled until the first elections", file!(), line!());
            return;
        }

        // check slashing event

        let mc_block_seqno = block.masterchain_ref_seq_no();
        let remove_delivered_messages_only = (mc_block_seqno + self.send_messages_block_offset)
            % slashing_config.resend_mc_blocks_count
            != 0;

        self.resend_messages(engine, block, remove_delivered_messages_only)
            .await;

        let (aggregated_stat, report_fn) = {
            let mut manager = self.manager.lock(); //TODO: check long lock

            if manager.first_mc_block == 0 {
                //initialize slashing statistics for first full interval

                if (mc_block_seqno + self.send_messages_block_offset)
                    % slashing_config.slashing_period_mc_blocks_count
                    != 0
                {
                    return;
                }

                manager.first_mc_block = mc_block_seqno;
                manager.stat.clear();
                manager.slashing_messages.clear();
                return;
            }

            let mc_blocks_count = mc_block_seqno - manager.first_mc_block;

            if mc_blocks_count < slashing_config.slashing_period_mc_blocks_count {
                return;
            }

            //compute aggregated slashing statistics

            let aggregated_stat = manager.stat.aggregate(&slashing_config);

            log::info!(target: "validator", "{}({}): stat={:?}", file!(), line!(), aggregated_stat);

            //reset slashing statistics

            manager.first_mc_block = mc_block_seqno;
            manager.stat.clear();
            manager.slashing_messages.clear();

            (aggregated_stat, manager.report_fn.clone())
        };

        //slash validators

        let slashed_validators = aggregated_stat.get_slashed_validators();

        log::info!(target: "validator", "{}({}): slashed={:?}", file!(), line!(), slashed_validators);

        if slashed_validators.len() > 0 {
            for validator in slashed_validators {
                self.slash_validator(
                    &local_key,
                    &validator.public_key,
                    validator.metric_id.clone(),
                    &engine,
                    &report_fn,
                )
                .await;
            }

            #[cfg(feature = "metrics")]
            Self::report_slashing_metrics(&aggregated_stat, local_key.id());
        }
    }

    fn convert_to_u256(value: &[u8]) -> TokenValue {
        assert!(value.len() == 32);
        TokenValue::Uint(Uint {
            number: BigUint::from_bytes_be(value),
            size: 256,
        })
    }

    async fn slash_validator(
        &self,
        reporter_privkey: &PrivateKey,
        validator_pubkey: &PublicKey,
        metric_id: AggregatedMetric,
        engine: &Arc<dyn EngineOperations>,
        report_fn: &Function,
    ) {
        //prepare params for serialization

        let reporter_pubkey = reporter_privkey
            .pub_key()
            .expect("PublicKey is assigned");
        let victim_pubkey = validator_pubkey
            .pub_key()
            .expect("PublicKey is assigned");
        let metric_id: u8 = metric_id as u8;

        log::warn!(
            target: "validator",
            "{}({}): Slash validator {} on metric {:?} by reporter validator {}{}",
            file!(),
            line!(),
            hex::encode(&victim_pubkey),
            metric_id,
            hex::encode(&reporter_pubkey),
            if metric_id != AggregatedMetric::ValidationScore as u8 { " (metric ignored)" } else { "" },
        );

        if metric_id != AggregatedMetric::ValidationScore as u8 {
            return;
        }

        //compute signature for params

        let mut params_data = Vec::new();

        params_data.extend_from_slice(&reporter_pubkey);
        params_data.extend_from_slice(&victim_pubkey);
        params_data.extend_from_slice(&metric_id.to_be_bytes());

        let signature = match reporter_privkey.sign(&params_data) {
            Err(err) => {
                log::error!(target: "validator", "SlashingManager::slash_validator: failed to sign block {:?}: {:?}", params_data, err);
                return;
            }
            Ok(signature) => signature,
        };

        //prepare header for external message

        let time_now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let header: HashMap<_, _> = vec![("time".to_owned(), TokenValue::Time(time_now_ms))]
            .into_iter()
            .collect();

        //serialize parameters for external message body

        let parameters: [Token; 5] = [
            Token::new("signature_hi", Self::convert_to_u256(&signature[..32])),
            Token::new("signature_lo", Self::convert_to_u256(&signature[32..])),
            Token::new("reporter_pubkey", Self::convert_to_u256(&reporter_pubkey)),
            Token::new("victim_pubkey", Self::convert_to_u256(&victim_pubkey)),
            Token::new(
                "metric_id",
                TokenValue::Uint(Uint::new(metric_id.into(), 8)),
            ),
        ];

        const INTERNAL_CALL: bool = false; //external message

        let result = report_fn
            .encode_input(&header, &parameters, INTERNAL_CALL, None)
            .and_then(|builder| builder.into_cell());
        let body = match result {
            Err(err) => {
                log::error!(target: "validator", "SlashingManager::slash_validator: failed to encode input: {:?}", err);
                return;
            }
            Ok(result) => result,
        };

        log::info!(target: "validator", "{}({}): SlashingManager::slash_validator: message body {:?}", file!(), line!(), body);

        let body = SliceData::from(body);

        //prepare header of the message

        let hdr = ExternalInboundMessageHeader::new(
            MsgAddressExt::with_extern(SliceData::new(vec![0xffu8])).unwrap(),
            MsgAddressInt::with_standart(None, -1, [0x33; 32].into()).unwrap(),
        );

        //create external message

        let mut message = Message::with_ext_in_header(hdr);
        message.set_body(body);

        let message = Arc::new(message);

        //send message

        self.send_message(engine, message, true).await;
    }

    async fn send_message(
        &self,
        engine: &Arc<dyn EngineOperations>,
        message: Arc<Message>,
        store_for_resending: bool,
    ) {
        match Self::serialize_message(&message) {
            Ok((message_id, serialized_message)) => {
                if store_for_resending {
                    self.manager
                        .lock()
                        .slashing_messages
                        .insert(message_id.clone(), message.clone());
                }

                if let Err(err) = engine.redirect_external_message(&serialized_message).await {
                    log::warn!(target: "validator", "{}({}): SlashingManager::slash_validator: can't send message: {:?}, error: {:?}", file!(), line!(), message, err);
                } else {
                    log::info!(target: "validator", "{}({}): SlashingManager::slash_validator: message: {:?} -> {} has been successfully {}", file!(), line!(), message_id, base64::encode(&serialized_message),
                        if store_for_resending { "sent" } else { "resent" });
                }
            }
            Err(err) => {
                log::warn!(target: "validator", "{}({}): SlashingManager::slash_validator: can't serialize message: {:?}, error: {:?}", file!(), line!(), message, err)
            }
        }
    }

    fn serialize_message(message: &Arc<Message>) -> Result<(UInt256, Vec<u8>)> {
        let cell = message.serialize()?;
        let id = cell.repr_hash();
        let bytes = ton_types::serialize_toc(&cell)?;

        Ok((id, bytes))
    }

    async fn resend_messages(
        &self,
        engine: &Arc<dyn EngineOperations>,
        block: &Arc<BlockHandle>,
        remove_only: bool,
    ) {
        let msg_desc = match engine.load_block(block).await {
            Ok(block) => match block.block().read_extra() {
                Ok(extra) => match extra.read_in_msg_descr() {
                    Ok(msg_desc) => Some(msg_desc),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        };

        let messages = self.manager.lock().slashing_messages.clone();
        let mut messages_to_remove = Vec::new();

        for (message_id, message) in &messages {
            if let Some(msg_desc) = &msg_desc {
                if let Ok(message) = msg_desc.get(&message_id) {
                    if message.is_some() {
                        messages_to_remove.push(message_id.clone());
                        continue;
                    }
                }
            }

            if !remove_only {
                self.send_message(engine, message.clone(), false).await;
            }
        }

        if !messages_to_remove.is_empty() {
            let mut manager_lock = self.manager.lock(); //acquire lock

            for message_id in messages_to_remove {
                log::info!(target: "validator", "{}({}): SlashingManager::slash_validator: message: {:?} has been successfully delivered", file!(), line!(), message_id);
                manager_lock.slashing_messages.remove(&message_id);
            }

            //release lock
        }
    }

    #[cfg(feature = "metrics")]
    pub fn report_slashing_metrics(
        aggregated_stat: &SlashingAggregatedValidatorStat,
        reporter_key: &PublicKeyHash,
    ) {
        let node = aggregated_stat.get_aggregated_node(reporter_key);
        let (validation_score, slashing_score) = match node {
            Some(node) => {
                let validation_score = node.metrics[AggregatedMetric::ValidationScore as usize];
                let slashing_score = node.metrics[AggregatedMetric::SlashingScore as usize];

                (validation_score, slashing_score)
            }
            _ => (-1.0, -1.0),
        };

        let mut pipeline = STATSD.pipeline();

        pipeline.gauge("validation_score", validation_score);
        pipeline.gauge("slashing_score", slashing_score);

        pipeline.send(&STATSD);
    }
}

impl fmt::Debug for ValidatedBlockStat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ValidatedBlockStat(nodes=[{:?}])", self.nodes)
    }
}

impl fmt::Debug for ValidatedBlockStatNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ValidatedBlockStatNode(public_key={:?}, collated={:?}, signed={:?})",
            self.public_key, self.collated, self.signed
        )
    }
}
