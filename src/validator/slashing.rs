#[cfg(feature = "metrics")]
use crate::engine::STATSD;
use crate::engine_traits::EngineOperations;
use crate::shard_state::ShardStateStuff;
use crate::validator::UInt256;
use num_bigint::BigUint;
use spin::mutex::SpinMutex;
use std::collections::HashMap;
use std::sync::Arc;
use storage::types::BlockHandle;
use ton_abi::contract::Contract;
use ton_abi::function::Function;
use ton_abi::Token;
use ton_abi::TokenValue;
use ton_abi::Uint;
use ton_block::ExternalInboundMessageHeader;
use ton_block::HashmapAugType;
use ton_block::Message;
use ton_block::MsgAddressExt;
use ton_block::MsgAddressInt;
use ton_block::Serializable;
use ton_types::Result;
use ton_types::SliceData;
#[cfg(feature = "metrics")]
use validator_session::slashing::AggregatedMetric;
use validator_session::PrivateKey;
use validator_session::PublicKey;
#[cfg(feature = "metrics")]
use validator_session::PublicKeyHash;
#[cfg(feature = "metrics")]
use validator_session::SlashingAggregatedValidatorStat;
use validator_session::SlashingValidatorStat;

const SLASHING_MC_BLOCKS_COUNT: u32 = 1500; //number of blocks for slashing computations
const SLASHING_Z_CONFIDENCE: f64 = 2.576; //corresponds to 99% confidence; see https://en.wikipedia.org/wiki/Confidence_interval#Basic_steps for details
const ELECTOR_ABI: &str = include_str!("Elector.abi.json"); //elector's ABI
const ELECTOR_REPORT_FUNC_NAME: &str = "report"; //elector slashing report function name
const SLASHING_RESEND_MC_BLOCKS_PERIOD: u32 = 5; //number for blocks for resending slashing messages

/// Slashing manager pointer
pub type SlashingManagerPtr = Arc<SlashingManager>;

/// Slashing manager
pub struct SlashingManager {
    manager: SpinMutex<SlashingManagerImpl>,
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
        let contract = Contract::load(ELECTOR_ABI.as_bytes()).expect("Elector's ABI must be valid");
        let report_fn = contract
            .function(ELECTOR_REPORT_FUNC_NAME)
            .expect("Elector contract must have 'report' function for slashing")
            .clone();

        log::info!(target: "validator", "Use slashing report function '{}' with id={:08X}", ELECTOR_REPORT_FUNC_NAME, report_fn.get_function_id());

        Arc::new(SlashingManager {
            manager: SpinMutex::new(SlashingManagerImpl {
                stat: SlashingValidatorStat::default(),
                first_mc_block: 0,
                slashing_messages: HashMap::new(),
                report_fn: report_fn,
            }),
        })
    }

    /// Update slashing statistics
    pub fn update_statistics(&self, stat: &SlashingValidatorStat) {
        self.manager.lock().stat.merge(&stat)
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

    /// New masterchain block notification
    pub async fn handle_masterchain_block(
        &self,
        block: &Arc<BlockHandle>,
        mc_state: &ShardStateStuff,
        local_key: &PrivateKey,
        engine: &Arc<dyn EngineOperations>,
    ) {
        if !Self::is_slashing_available(mc_state) {
            log::info!(target: "validator", "{}({}): slashing is disabled until the first elections", file!(), line!());
            return;
        }

        let mc_block_seqno = block.masterchain_ref_seq_no();
        let remove_delivered_messages_only = mc_block_seqno % SLASHING_RESEND_MC_BLOCKS_PERIOD != 0;

        self.resend_messages(engine, block, remove_delivered_messages_only)
            .await;

        let (aggregated_stat, report_fn) = {
            let mut manager = self.manager.lock(); //TODO: check long lock

            if manager.first_mc_block == 0 {
                //initialize slashing statistics for first full interval

                if mc_block_seqno % SLASHING_MC_BLOCKS_COUNT != 0 {
                    return;
                }

                manager.first_mc_block = mc_block_seqno;
                manager.stat.clear();
                manager.slashing_messages.clear();
                return;
            }

            let mc_blocks_count = mc_block_seqno - manager.first_mc_block;

            //TODO: split slashing intervals by validator set change

            if mc_blocks_count < SLASHING_MC_BLOCKS_COUNT {
                return;
            }

            //TODO: match MC block seqno with validators set

            //compute aggregated slashing statistics

            let aggregated_stat = manager.stat.aggregate(SLASHING_Z_CONFIDENCE);

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
        metric_id: validator_session::slashing::AggregatedMetric,
        engine: &Arc<dyn EngineOperations>,
        report_fn: &Function,
    ) {
        //prepare params for serialization

        let reporter_pubkey: [u8; 32] = reporter_privkey
            .pub_key()
            .expect("PublicKey is assigned")
            .clone();
        let victim_pubkey: [u8; 32] = validator_pubkey
            .pub_key()
            .expect("PublicKey is assigned")
            .clone();
        let metric_id: u8 = metric_id as u8;

        log::warn!(
            target: "validator",
            "{}({}): Slash validator {} on metric {:?} by reporter validator {}",
            file!(),
            line!(),
            hex::encode(&victim_pubkey),
            metric_id,
            hex::encode(&reporter_pubkey),
        );

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

        let body = match report_fn.encode_input(&header, &parameters, INTERNAL_CALL, None) {
            Err(err) => {
                log::error!(target: "validator", "SlashingManager::slash_validator: failed to encode input: {:?}", err);
                return;
            }
            Ok(result) => result,
        };

        log::info!(target: "validator", "{}({}): SlashingManager::slash_validator: message body {:?}", file!(), line!(), body);

        let body: SliceData = body.into();

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
                    log::info!(target: "validator", "{}({}): SlashingManager::slash_validator: message: {:?} -> {} has been successfully sent", file!(), line!(), message_id, base64::encode(&serialized_message));
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
        let mut messages = self.manager.lock().slashing_messages.clone();
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
            for message_id in messages_to_remove {
                log::info!(target: "validator", "{}({}): SlashingManager::slash_validator: message: {:?} has been successfully delivered", file!(), line!(), message_id);
                messages.remove(&message_id);
            }

            self.manager.lock().slashing_messages = messages;
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
                let collation_participation =
                    node.metrics[AggregatedMetric::CollationsParticipation as usize];
                let approvals_participation =
                    node.metrics[AggregatedMetric::ApprovalsParticipation as usize];
                let commits_participation =
                    node.metrics[AggregatedMetric::CommitsParticipation as usize];
                let approvals_weight = 0.2;
                let commits_weight = 0.3;
                let collations_weight = 0.5;
                let validation_score = approvals_weight * approvals_participation
                    + commits_weight * commits_participation
                    + collations_weight * collation_participation;
                let slashing_score = 1.0 - validation_score;

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
