/*
* Copyright (C) 2019-2023 TON Labs. All Rights Reserved.
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
use crate::{engine_traits::EngineOperations, shard_state::ShardStateStuff, validator::UInt256};
use ever_crypto::Ed25519KeyOption;
use num_bigint::BigUint;
use rand::Rng;
use spin::mutex::SpinMutex;
use std::{fmt, sync::Arc};
use storage::block_handle_db::BlockHandle;
use ton_abi::{Contract, Function, Token, TokenValue, Uint};
use ton_block::{
    ConfigParamEnum, ExternalInboundMessageHeader, HashmapAugType, Message, MsgAddressExt,
    MsgAddressInt, Serializable, SigPubKey, SlashingConfig,
};
use ton_types::{error, Result, write_boc, SliceData};
use validator_session::{
    PrivateKey, PublicKey, SlashingAggregatedMetric, SlashingMetric, SlashingNode,
    SlashingValidatorStat,
};
#[cfg(feature = "metrics")]
use validator_session::{PublicKeyHash, SlashingAggregatedValidatorStat};

const ELECTOR_ABI: &[u8] = include_bytes!("Elector.abi.json"); //elector's ABI
const ELECTOR_REPORT_FUNC_NAME: &str = "report"; //elector slashing report function name
lazy_static::lazy_static! {
    static ref ELECTOR_CONTRACT_ABI: Contract = Contract::load(ELECTOR_ABI)
        .expect("Elector's ABI must be valid");
    static ref REPORT_FN: Function = ELECTOR_CONTRACT_ABI
        .function(ELECTOR_REPORT_FUNC_NAME)
        .expect("Elector contract must have 'report' function for slashing")
        .clone();
    // it should be got from config params
    static ref ELECTOR_ADDRESS: MsgAddressInt = "-1:3333333333333333333333333333333333333333333333333333333333333333".parse().unwrap();
}

/// Slashing manager pointer
pub type SlashingManagerPtr = Arc<SlashingManager>;

/// Slashing manager
pub struct SlashingManager {
    manager: SpinMutex<SlashingManagerImpl>,
    send_messages_block_offset: u32,
}

/// Internal slashing manager details
#[derive(Default)]
struct SlashingManagerImpl {
    stat: SlashingValidatorStat,
    first_mc_block: u32,
    slashing_messages: Vec<(UInt256, Arc<Message>)>,
}

impl SlashingManager {
    /// Create new slashing manager
    pub(crate) fn create() -> SlashingManagerPtr {
        log::info!(target: "slashing", "Use slashing report function '{}' with id={:08X}",
            ELECTOR_REPORT_FUNC_NAME, REPORT_FN.get_function_id());

        let mut rng = rand::thread_rng();

        Arc::new(SlashingManager {
            send_messages_block_offset: rng.gen(),
            manager: SpinMutex::new(SlashingManagerImpl::default()),
        })
    }

    /// Update slashing statistics
    pub fn update_statistics(&self, stat: &SlashingValidatorStat) {
        self.manager.lock().stat.merge(&stat);
        log::debug!(target: "slashing", "merge slashing statistics {:?}", stat);
    }

    /// Is slashing available
    fn is_slashing_available(mc_state: &ShardStateStuff) -> bool {
        if let Ok(config) = mc_state.config_params() {
            if let Ok(vset) = config.prev_validator_set() {
                return vset.list().len() > 0 && vset.total_weight() > 0;
            }
        }
        false
    }

    /// Get slashing params
    fn get_slashing_config(mc_state: &ShardStateStuff) -> SlashingConfig {
        if let Ok(config) = mc_state.config_params() {
            if let Ok(Some(ConfigParamEnum::ConfigParam40(cc))) = config.config(40) {
                return cc.slashing_config;
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
        block_handle: &Arc<BlockHandle>,
        mc_state: &ShardStateStuff,
        local_key: &PrivateKey,
        engine: &Arc<dyn EngineOperations>,
    ) {
        // read slashing params

        let slashing_config = Self::get_slashing_config(mc_state);

        // process queue of received validated block stat events

        while let Ok(validated_block_stat) = engine.pop_validated_block_stat() {
            log::debug!(target: "slashing", "Statistics has been received {:?}", validated_block_stat);

            let mut slashing_stat = SlashingValidatorStat::default();

            for src_node in &validated_block_stat.nodes {
                let pub_key = src_node.public_key.key_bytes();
                let public_key = Ed25519KeyOption::from_public_key(pub_key);
                let mut dst_node = SlashingNode::new(public_key.clone());

                dst_node.metrics[SlashingMetric::ApplyLevelTotalBlocksCount as usize] += 1;

                if src_node.collated {
                    dst_node.metrics[SlashingMetric::ApplyLevelCollationsCount as usize] += 1;
                }

                if src_node.signed {
                    dst_node.metrics[SlashingMetric::ApplyLevelCommitsCount as usize] += 1;
                }

                slashing_stat
                    .validators_stat
                    .insert(public_key.id().clone(), dst_node);
            }

            self.update_statistics(&slashing_stat);
        }

        if !Self::is_slashing_available(mc_state) {
            log::info!(target: "slashing", "slashing is disabled until the first elections");
            return;
        }

        // check slashing event

        let mc_block_seqno = block_handle.masterchain_ref_seq_no();
        let remove_delivered_messages_only = (mc_block_seqno + self.send_messages_block_offset)
            % slashing_config.resend_mc_blocks_count
            != 0;

        self.resend_messages(engine, block_handle, remove_delivered_messages_only)
            .await;

        let aggregated_stat;
        {
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

            aggregated_stat = manager.stat.aggregate(&slashing_config);

            log::info!(target: "slashing", "stat={:?}", aggregated_stat);

            //reset slashing statistics

            manager.first_mc_block = mc_block_seqno;
            manager.stat.clear();
            manager.slashing_messages.clear();
        }

        //slash validators

        let slashed_validators = aggregated_stat.get_slashed_validators();

        log::debug!(target: "slashing", "slashed={:?}", slashed_validators);
        let time_now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        for validator in slashed_validators {
            match Self::prepare_slash_validator_message(
                ELECTOR_ADDRESS.clone(),
                &local_key,
                &validator.public_key,
                validator.metric_id,
                time_now_ms
            ) {
                Ok(Some(message)) => self.send_message(engine, Arc::new(message)).await,
                Err(err) => {
                    log::warn!(target: "slashing", "cannot compose blaiming message due {}", err)
                }
                Ok(None) => (),
            }
        }

        if slashed_validators.len() > 0 {
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

    fn prepare_slash_validator_message(
        elector_address: MsgAddressInt,
        reporter_privkey: &PrivateKey,
        validator_pubkey: &PublicKey,
        metric_id: SlashingAggregatedMetric,
        time_now_ms: u64,
    ) -> Result<Option<Message>> {
        //prepare params for serialization

        let reporter_pubkey = reporter_privkey.pub_key()?;
        let victim_pubkey = validator_pubkey.pub_key()?;
        let metric_id: u8 = metric_id as u8;

        log::warn!(
            target: "slashing",
            "Slash validator {} on metric {:?} by reporter validator {}{}",
            hex::encode(&victim_pubkey),
            metric_id,
            hex::encode(&reporter_pubkey),
            if metric_id != SlashingAggregatedMetric::ValidationScore as u8 { " (metric ignored)" } else { "" },
        );

        if metric_id != SlashingAggregatedMetric::ValidationScore as u8 {
            return Ok(None);
        }

        //compute signature for params

        let mut params_data = reporter_pubkey.to_vec();
        params_data.extend_from_slice(&victim_pubkey);
        params_data.push(metric_id);
        let signature = match reporter_privkey.sign(&params_data) {
            Ok(signature) => signature,
            Err(_) => vec![0; 64]
        };

        //prepare header for external message

        let header = [("time".to_owned(), TokenValue::Time(time_now_ms))]
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

        let src = MsgAddressExt::with_extern(SliceData::new(vec![0xffu8])).unwrap();
        let dst = elector_address;
        let body = REPORT_FN
            .encode_input(&header, &parameters, INTERNAL_CALL, None, Some(dst.clone()))
            .and_then(|builder| SliceData::load_builder(builder))
            .map_err(|err| error!("SlashingManager::prepare_slash_validator_message: failed to encode input: {:?}", err))?;
        log::trace!(target: "slashing", "message body {}", base64::encode(&write_boc(&body.cell())?));

        //prepare header of the message

        let hdr = ExternalInboundMessageHeader::new(src, dst);

        //create external message

        Ok(Some(Message::with_ext_in_header_and_body(hdr, body)))
    }

    async fn send_message(&self, engine: &Arc<dyn EngineOperations>, message: Arc<Message>) {
        match Self::serialize_message(&message) {
            Ok((message_id, serialized_message)) => {
                self.manager
                    .lock()
                    .slashing_messages
                    .push((message_id.clone(), message.clone()));

                if let Err(err) = engine.redirect_external_message(
                                    &serialized_message, message_id.clone()).await 
                {
                    log::warn!(target: "slashing", "can't send message: {:?}, error: {:?}", message, err);
                } else {
                    log::info!(target: "slashing", "message: {:?} -> {} has been successfully sent",
                            message_id, base64::encode(&serialized_message));
                }
            }
            Err(err) => {
                log::warn!(target: "slashing", "can't serialize message: {:?}, error: {:?}", message, err)
            }
        }
    }

    fn serialize_message(message: &Arc<Message>) -> Result<(UInt256, Vec<u8>)> {
        let cell = message.serialize()?;
        let id = cell.repr_hash();
        let bytes = ton_types::write_boc(&cell)?;

        Ok((id, bytes))
    }

    async fn resend_messages(
        &self,
        engine: &Arc<dyn EngineOperations>,
        block_handle: &Arc<BlockHandle>,
        remove_only: bool,
    ) {
        let msg_desc = engine
            .load_block(block_handle)
            .await
            .and_then(|block| block.block()?.read_extra()?.read_in_msg_descr());

        let mut slashing_messages = std::mem::take(&mut self.manager.lock().slashing_messages);
        for (message_id, message) in slashing_messages.drain(..) {
            if let Ok(msg_desc) = &msg_desc {
                if let Ok(Some(_)) = msg_desc.get(&message_id) {
                    log::debug!(target: "slashing", "message: {:?} has been successfully delivered", message_id);
                    continue;
                }
            }

            if !remove_only {
                self.send_message(engine, message).await;
            }
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
                let validation_score =
                    node.metrics[SlashingAggregatedMetric::ValidationScore as usize];
                let slashing_score = node.metrics[SlashingAggregatedMetric::SlashingScore as usize];

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

pub struct ValidatedBlockStatNode {
    pub public_key: SigPubKey,
    pub signed: bool,
    pub collated: bool,
}

impl fmt::Debug for ValidatedBlockStatNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ValidatedBlockStatNode(public_key={}, collated={}, signed={})",
            hex::encode(self.public_key.key_bytes()), self.collated, self.signed
        )
    }
}

pub struct ValidatedBlockStat {
    pub nodes: Vec<ValidatedBlockStatNode>,
}

impl fmt::Debug for ValidatedBlockStat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // f.write_str("ValidatedBlockStat")?;
        // let v = self
        //     .nodes
        //     .iter()
        //     .map(|node| format!("Id({}): {:?}", hex::encode(key.data())));
        // f.debug_list().entries(v).finish()
        write!(f, "ValidatedBlockStat(nodes=[{:?}])", self.nodes)
    }
}

