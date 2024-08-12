/*
* Copyright (C) 2019-2022 TON Labs. All Rights Reserved.
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

use super::block::Block;
use super::block::BlockCandidateBody;
use super::block::BlockPtr;
use super::block::MultiSignature;
use super::workchain_overlay::WorkchainOverlay;
use super::workchain_overlay::WorkchainOverlayListener;
use super::*;
use super::utils::HangCheck;
use super::utils::get_adnl_id;
use crate::validator::validator_utils::sigpubkey_to_publickey;
use storage::block_handle_db::BlockHandle;
use ever_block::SmftParams;
use catchain::BlockPayloadPtr;
use catchain::PublicKeyHash;
use catchain::profiling::ResultStatusCounter;
use catchain::profiling::InstanceCounter;
use catchain::check_execution_time;
use ever_block::fail;
use log::*;
use rand::Rng;
use tokio::time::sleep;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::Duration;
use anyhow::format_err;
use spin::mutex::SpinMutex;
use ever_block::{crc32_digest, Result, bls::BLS_PUBLIC_KEY_LEN};
use ton_api::ton::ton_node::blockcandidatestatus::BlockCandidateStatus;
use ton_api::ton::ton_node::broadcast::BlockCandidateBroadcast;
use validator_session::ValidatorWeight;
use catchain::utils::MetricsDumper;
use catchain::utils::MetricsHandle;
use metrics::Recorder;
use catchain::utils::add_compute_relative_metric;
use catchain::utils::add_compute_result_metric;

//TODO: force request block from MC

/*
===============================================================================
    Constants
===============================================================================
*/

//TODO: cutoff weight configuration
const MANUAL_CANDIDATE_LOADING_DELAY_MS: u64 = 4000; //delay for manual candidate loading
const ALLOWED_FORCE_DELIVERY_DELAY_MS: u128 = 4000;   //delay between allowed force delivery requests
const FORCE_DELIVERY_DUPLICATION_FACTOR: f64 = 2.0;  //duplication factor for force delivery requests (each MC validator sents request to FACTOR x WC_VALIDATORS_COUNT/MC_VALIDATORS_COUNT workchain nodes)

/*
===============================================================================
    Workchain
===============================================================================
*/

pub type WorkchainPtr = Arc<Workchain>;

#[derive(Clone, Debug)]
struct WorkchainDeliveryParams {
    block_status_forwarding_neighbours_count: usize, //neighbours count for block status synchronization
    block_status_far_neighbours_count: usize,        //far neighbours count for block status synchronization
    config_params: SmftParams,                       //configuration parameters
}

pub struct Workchain {
    engine: EnginePtr,                    //pointer to engine
    runtime: tokio::runtime::Handle,      //runtime handle for spawns
    metrics_receiver: MetricsHandle,      //metrics receiver
    wc_validator_set_hash: UInt256,       //hash of validators set for WC
    mc_validator_set_hash: UInt256,       //hash of validators set for MC
    wc_validators: Vec<ValidatorDescr>,   //WC validators
    mc_validators: Vec<ValidatorDescr>,   //MC validators
    wc_validators_adnl_ids: Vec<PublicKeyHash>, //WC validators ADNL IDs
    wc_pub_keys: Vec<[u8; BLS_PUBLIC_KEY_LEN]>, //WC validators pubkeys
    local_adnl_id: PublicKeyHash,         //ADNL ID for this node
    wc_local_idx: i16,                    //local index in WC validator set
    mc_local_idx: i16,                    //local index in MC validator set
    workchain_id: i32,                    //workchain identifier
    self_weak_ref: SpinMutex<Option<Weak<Workchain>>>, //self weak reference
    wc_total_weight: ValidatorWeight,     //total weight for consensus in WC
    wc_cutoff_weight: ValidatorWeight,    //cutoff weight for consensus in WC
    mc_total_weight: ValidatorWeight,     //total weight for consensus in MC
    mc_cutoff_weight: ValidatorWeight,    //cutoff weight for consensus in MC
    local_bls_key: PrivateKey,            //private BLS key
    local_id: PublicKeyHash,              //local ID for this node
    workchain_delivery_params: SpinMutex<WorkchainDeliveryParams>, //delivery params
    workchain_overlay: SpinMutex<Option<Arc<WorkchainOverlay>>>, //workchain overlay
    mc_overlay: SpinMutex<Option<Arc<WorkchainOverlay>>>, //MC overlay
    blocks: SpinMutex<HashMap<UInt256, BlockPtr>>, //blocks
    listener: VerificationListenerPtr,    //verification listener
    node_debug_id: Arc<String>,           //node debug ID for workchain
    _workchains_instance_counter: InstanceCounter,                   //workchain instances counter
    blocks_instance_counter: Arc<InstanceCounter>,                   //instance counter for blocks
    merge_block_status_counter: metrics::Counter,      //counter for block updates (via merge with other nodes statuses)
    set_block_status_counter: metrics::Counter,        //counter for set block status (by local node)
    process_block_candidate_counter: metrics::Counter, //counter for block candidates processings
    process_block_status_counter: metrics::Counter,    //counter for block statuses processings
    new_block_candidate_counter: metrics::Counter,     //counter for new block candidates
    send_block_status_to_mc_counter: metrics::Counter, //counter of sendings block status to MC
    send_block_status_counter: metrics::Counter,       //counter of sendings block status within workchain
    external_request_counter: metrics::Counter,        //counter of external requests for a block
    external_request_delivered_blocks_counter: metrics::Counter, //counter of external requests for delivered blocks
    external_request_approved_blocks_counter: metrics::Counter,  //counter of external requests for approved block
    external_request_rejected_blocks_counter: metrics::Counter,  //counter of external requests for rejected blocks
    verify_block_counter: ResultStatusCounter,                       //counter for block verifications
    block_status_received_in_mc_counter: ResultStatusCounter,        //counter for block receivings in MC
    block_status_send_to_mc_latency_histogram: metrics::Histogram, //histogram for block candidate sending to MC
    block_status_received_in_mc_latency_histogram: metrics::Histogram, //histogram for block candidate receiving in MC
    candidate_delivered_to_wc_latency_histogram: metrics::Histogram, //histogram for block candidate receiving in WC
    block_status_merges_count_histogram: metrics::Histogram, //histogram for block candidate merges count (hops count)
    block_external_request_delays_histogram: metrics::Histogram, //histogram for block external request delays
}

impl Workchain {
    /*
        Initialization
    */

    fn bls_key_to_string(key: &Option<[u8; BLS_PUBLIC_KEY_LEN]>) -> String {
        match key {
            None => "N/A".to_string(),
            Some(key) => hex::encode(&key),
        }
    }

    fn get_overlay_id(workchain_id: i32, wc_validator_set_hash: &UInt256, mc_validator_set_hash: &UInt256, tag: u32) -> UInt256 {
        let magic_suffix = [0xff, 0xbe, 0x45, 0x23]; //magic suffix to create unique hash different from public overlay hashes
        let mut overlay_id = Vec::new();

        overlay_id.extend_from_slice(&magic_suffix);
        overlay_id.extend_from_slice(&tag.to_le_bytes());
        overlay_id.extend_from_slice(&workchain_id.to_le_bytes());
        overlay_id.extend_from_slice(wc_validator_set_hash.as_slice());
        overlay_id.extend_from_slice(mc_validator_set_hash.as_slice());

        UInt256::calc_file_hash(&overlay_id)
    }

    pub async fn create(
        engine: EnginePtr,
        runtime: tokio::runtime::Handle,
        workchain_id: i32,
        mut wc_validators: Vec<ValidatorDescr>,
        mut mc_validators: Vec<ValidatorDescr>,
        wc_validator_set_hash: UInt256,
        mc_validator_set_hash: UInt256,
        local_id: &PublicKeyHash,
        local_bls_key: &PrivateKey,
        listener: VerificationListenerPtr,
        workchains_instance_counter: Arc<InstanceCounter>,
        blocks_instance_counter: Arc<InstanceCounter>,
        wc_overlays_instance_counter: Arc<InstanceCounter>,
        mc_overlays_instance_counter: Arc<InstanceCounter>,
    ) -> Result<Arc<Self>> {
        if !USE_VALIDATORS_WEIGHTS {
            for desc in wc_validators.iter_mut() {
                desc.weight = 1;
            }
            for desc in mc_validators.iter_mut() {
                desc.weight = 1;
            }
        }

        let mut wc_local_idx = -1;
        let mut mc_local_idx = -1;
        let mut local_adnl_id = None;

        for (idx, desc) in wc_validators.iter().enumerate() {
            let public_key = sigpubkey_to_publickey(&desc.public_key);

            if public_key.id() == local_id {
                wc_local_idx = idx as i16;
                local_adnl_id = Some(get_adnl_id(&desc));
                break;
            }
        }

        for (idx, desc) in mc_validators.iter().enumerate() {
            let public_key = sigpubkey_to_publickey(&desc.public_key);

            if public_key.id() == local_id {
                mc_local_idx = idx as i16;
                local_adnl_id = Some(get_adnl_id(&desc));
                break;
            }
        }

        if local_adnl_id.is_none() {
            fail!("local_adnl_id must exist for workchain {}", workchain_id);
        }

        let local_adnl_id = local_adnl_id.as_ref().expect("local_adnl_id must exist").clone();
        let node_debug_id = Arc::new(format!("#{}.{}", workchain_id, local_adnl_id));

        let wc_total_weight: ValidatorWeight = wc_validators.iter().map(|desc| desc.weight).sum();
        let wc_cutoff_weight = wc_total_weight / 2 + 1;

        let mut wc_pub_keys = Vec::new();

        log::info!(target: "verificator", "Creating verification workchain {} (wc_validator_set_hash={}, mc_validator_set_hash={}) with {} workchain nodes (total_weight={}, cutoff_weight={}, wc_local_idx={}, mc_local_idx={})",
            node_debug_id,
            wc_validator_set_hash.to_hex_string(),
            mc_validator_set_hash.to_hex_string(),
            wc_validators.len(),
            wc_total_weight,
            wc_cutoff_weight,
            wc_local_idx,
            mc_local_idx);

        let wc_validators_count = wc_validators.len();

        for (i, desc) in wc_validators.iter_mut().enumerate() {
            let adnl_id = get_adnl_id(&desc);
            //let adnl_id = desc.adnl_addr.clone().map_or("** no-addr **".to_string(), |x| x.to_hex_string());
            let public_key = sigpubkey_to_publickey(&desc.public_key);
            let mut bls_public_key = desc.bls_public_key.clone();

            if bls_public_key.is_none() && GENERATE_MISSING_BLS_KEY {
                match utils::generate_test_bls_key(&public_key) {
                    Ok(bls_key) => {
                        match bls_key.pub_key() {
                            Ok(bls_key) => {
                                let bls_key_data: [u8; BLS_PUBLIC_KEY_LEN] = bls_key.to_vec().try_into().unwrap_or_else(|v: Vec<u8>| panic!("Expected a Vec of length {} but it was {}", BLS_PUBLIC_KEY_LEN, v.len()));
                                bls_public_key = Some(bls_key_data);
                                desc.bls_public_key = bls_public_key;
                            },
                            Err(err) => log::error!(target: "verificator", "Can't generate test BLS key (can't extract pub key data): {:?}", err),
                        }
                    },
                    Err(err) => log::error!(target: "verificator", "Can't generate test BLS key: {:?}", err),
                }
            }

            if log::log_enabled!(target: "verificator", log::Level::Debug) {
                let serialized_pub_key_tl : ton_api::ton::bytes = catchain::serialize_tl_boxed_object!(&utils::into_public_key_tl(&public_key).unwrap());

                log::debug!(target: "verificator", "...node {}#{}/{} for workchain {}: public_key={}, public_key_bls={}, adnl_id={}, weight={} ({:.2}%)",
                    if local_id == public_key.id() { ">" } else { " " },
                    i, wc_validators_count, node_debug_id,
                    &hex::encode(&serialized_pub_key_tl),
                    Self::bls_key_to_string(&bls_public_key),
                    adnl_id,
                    desc.weight,
                    desc.weight as f64 / wc_total_weight as f64 * 100.0);
            }

            wc_pub_keys.push(match bls_public_key {
                Some(bls_public_key) => bls_public_key.clone().into(),
                None => [0; BLS_PUBLIC_KEY_LEN],
            });
        }

        let mc_total_weight: ValidatorWeight = mc_validators.iter().map(|desc| desc.weight).sum();
        let mc_cutoff_weight = mc_total_weight * 2 / 3 + 1; //different from wc cutoff weight (50% + 1)

        log::debug!(target: "verificator", "Workchain {} (wc_validator_set_hash={}, mc_validator_set_hash={}) has {} linked MC nodes (total_weight={}, cutoff_weight={})",
            node_debug_id,
            wc_validator_set_hash.to_hex_string(),
            mc_validator_set_hash.to_hex_string(),
            mc_validators.len(),
            mc_total_weight,
            mc_cutoff_weight);

        for (i, desc) in mc_validators.iter().enumerate() {
            let adnl_id = get_adnl_id(&desc);
            //let adnl_id = desc.adnl_addr.clone().map_or("** no-addr **".to_string(), |x| x.to_hex_string());
            let public_key = sigpubkey_to_publickey(&desc.public_key);

            if log::log_enabled!(target: "verificator", log::Level::Debug) {
                let serialized_pub_key_tl : ton_api::ton::bytes = catchain::serialize_tl_boxed_object!(&utils::into_public_key_tl(&public_key).unwrap());

                log::debug!(target: "verificator", "...MC node {}#{}/{} for workchain {}: public_key={}, adnl_id={}, weight={} ({:.2}%)",
                    if local_id == public_key.id() { ">" } else { " " },
                    i, mc_validators.len(), node_debug_id,
                    &hex::encode(&serialized_pub_key_tl),
                    adnl_id,
                    desc.weight,
                    desc.weight as f64 / mc_total_weight as f64 * 100.0);
            }
        }

        let metrics_receiver = MetricsHandle::new(Some(Duration::from_secs(30)));
        let workchain = Self {
            engine: engine.clone(),
            workchain_id,
            node_debug_id,
            runtime: runtime.clone(),
            wc_validators_adnl_ids: wc_validators.iter().map(|desc| get_adnl_id(&desc)).collect(),
            wc_validators,
            mc_validators: mc_validators.clone(),
            wc_validator_set_hash,
            mc_validator_set_hash,
            wc_total_weight,
            wc_cutoff_weight,
            mc_total_weight,
            mc_cutoff_weight,
            local_bls_key: local_bls_key.clone(),
            local_adnl_id: local_adnl_id.clone().into(),
            local_id: local_id.clone(),
            wc_local_idx,
            mc_local_idx,
            wc_pub_keys,
            blocks: SpinMutex::new(HashMap::new()),
            workchain_delivery_params: SpinMutex::new(WorkchainDeliveryParams {
                block_status_forwarding_neighbours_count: 0,
                block_status_far_neighbours_count: 0,
                config_params: SmftParams::default(),
            }),
            mc_overlay: SpinMutex::new(None),
            workchain_overlay: SpinMutex::new(None),
            listener,
            self_weak_ref: SpinMutex::new(None),
            _workchains_instance_counter: (*workchains_instance_counter).clone(),
            blocks_instance_counter,
            merge_block_status_counter: metrics_receiver.sink().register_counter(&format!("smft_wc{}_block_status_merges", workchain_id).into()),
            set_block_status_counter: metrics_receiver.sink().register_counter(&format!("smft_wc{}_block_status_sets", workchain_id).into()),
            process_block_candidate_counter: metrics_receiver.sink().register_counter(&format!("smft_wc{}_block_candidate_processings", workchain_id).into()),
            process_block_status_counter: metrics_receiver.sink().register_counter(&format!("smft_wc{}_block_status_processings", workchain_id).into()),
            new_block_candidate_counter: metrics_receiver.sink().register_counter(&format!("smft_wc{}_new_block_candidates", workchain_id).into()),
            send_block_status_to_mc_counter: metrics_receiver.sink().register_counter(&format!("smft_wc{}_block_status_to_mc_sends", workchain_id).into()),
            send_block_status_counter: metrics_receiver.sink().register_counter(&format!("smft_wc{}_block_status_within_wc_sends", workchain_id).into()),
            external_request_counter: metrics_receiver.sink().register_counter(&format!("smft_wc{}_mc_requests", workchain_id).into()),
            external_request_delivered_blocks_counter: metrics_receiver.sink().register_counter(&format!("smft_wc{}_mc_delivered", workchain_id).into()),
            external_request_approved_blocks_counter: metrics_receiver.sink().register_counter(&format!("smft_wc{}_mc_approved", workchain_id).into()),
            external_request_rejected_blocks_counter: metrics_receiver.sink().register_counter(&format!("smft_wc{}_mc_rejected", workchain_id).into()),
            block_status_received_in_mc_counter: ResultStatusCounter::new(&metrics_receiver, &format!("smft_wc{}_block_status_received_in_mc", workchain_id)),
            verify_block_counter: ResultStatusCounter::new(&metrics_receiver, &format!("smft_wc{}_block_candidate_verifications", workchain_id)),
            candidate_delivered_to_wc_latency_histogram: metrics_receiver.sink().register_histogram(&format!("time:smft_wc{}_stage1_block_candidate_delivered_in_wc", workchain_id).into()),
            block_status_send_to_mc_latency_histogram: metrics_receiver.sink().register_histogram(&format!("time:smft_wc{}_stage2_block_status_send_to_mc_latency", workchain_id).into()),
            block_status_received_in_mc_latency_histogram: metrics_receiver.sink().register_histogram(&format!("time:smft_wc{}_stage3_block_status_received_in_mc_latency", workchain_id).into()),
            block_status_merges_count_histogram: metrics_receiver.sink().register_histogram(&format!("smft_wc{}_block_status_merges_count", workchain_id).into()),
            block_external_request_delays_histogram: metrics_receiver.sink().register_histogram(&format!("time:smft_wc{}_block_mc_delay", workchain_id).into()),
            metrics_receiver: metrics_receiver.clone(),
        };
        let workchain = Arc::new(workchain);

        //set self weak reference

        *workchain.self_weak_ref.lock() = Some(Arc::downgrade(&workchain));

        //set initial configuration

        workchain.start_configuration_update().await;

        //start overlay for interactions with MC

        const WC_OVERLAY_TAG: u32 = 1;
        const MC_OVERLAY_TAG: u32 = 2;

        let wc_overlay_id = Self::get_overlay_id(workchain.workchain_id, &workchain.wc_validator_set_hash, &workchain.mc_validator_set_hash, WC_OVERLAY_TAG);
        let mc_overlay_id = Self::get_overlay_id(workchain.workchain_id, &workchain.wc_validator_set_hash, &workchain.mc_validator_set_hash, MC_OVERLAY_TAG);

        let mut full_validators = mc_validators.clone();
        full_validators.append(&mut workchain.wc_validators.clone());

        //TODO: exclude duplicates from full_validators list!!!

        let mc_overlay_listener: Arc<dyn WorkchainOverlayListener> = workchain.clone();
        let mc_overlay = WorkchainOverlay::create(
            workchain.workchain_id,
            format!("MC[{}]{}", workchain.mc_local_idx, *workchain.node_debug_id),
            mc_overlay_id,
            &full_validators,
            workchain.local_adnl_id.clone(),
            Arc::downgrade(&mc_overlay_listener),
            &engine,
            runtime.clone(),
            metrics_receiver.clone(),
            mc_overlays_instance_counter,
            format!("smft_wc{}_mc_overlay", workchain_id),
            true,
        ).await?;
        *workchain.mc_overlay.lock() = Some(mc_overlay);

        if wc_local_idx != -1 {
            //start overlay for private interactions

            let workchain_overlay_listener: Arc<dyn WorkchainOverlayListener> = workchain.clone();
            let workchain_overlay = WorkchainOverlay::create(
                workchain.workchain_id,
                format!("WC[{}]{}", workchain.wc_local_idx, *workchain.node_debug_id),
                wc_overlay_id,
                &workchain.wc_validators,
                workchain.local_adnl_id.clone(),
                Arc::downgrade(&workchain_overlay_listener),
                &engine,
                runtime.clone(),
                metrics_receiver,
                wc_overlays_instance_counter,
                format!("smft_wc{}_wc_overlay", workchain_id),
                false,
            ).await?;
            *workchain.workchain_overlay.lock() = Some(workchain_overlay);
        }

        Ok(workchain)
    }

    /*
        Dumper
    */

    pub fn get_metrics_receiver(&self) -> &MetricsHandle {
        &self.metrics_receiver
    }

    pub fn configure_dumper(&self, metrics_dumper: &mut MetricsDumper) {
        log::debug!(target: "verificator", "Creating verification workchain {} metrics dumper", self.node_debug_id);

        let workchain_id = self.workchain_id;

        metrics_dumper.add_derivative_metric(format!("smft_wc{}_block_candidate_verifications.total", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_block_candidate_verifications.success", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_block_candidate_verifications.failure", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_new_block_candidates", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_block_status_processings", workchain_id));

        metrics_dumper.add_derivative_metric(format!("smft_wc{}_block_status_received_in_mc.total", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_block_status_received_in_mc.success", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_block_status_received_in_mc.failure", workchain_id));

        metrics_dumper.add_derivative_metric(format!("smft_wc{}_in_block_candidates", workchain_id));

        add_compute_result_metric(metrics_dumper, &format!("smft_wc{}_block_candidate_verifications", workchain_id));

        add_compute_relative_metric(
            metrics_dumper,
            &format!("smft_wc{}_merges_per_block", workchain_id),
            &format!("smft_wc{}_block_status_merges", workchain_id),
            &format!("smft_wc{}_new_block_candidates", workchain_id),
            0.0,
        );

        add_compute_relative_metric(
            metrics_dumper,
            &format!("smft_wc{}_updates_per_mc_send", workchain_id),
            &format!("smft_wc{}_block_status_processings", workchain_id),
            &format!("smft_wc{}_block_status_to_mc_sends", workchain_id),
            0.0,
        );

        add_compute_relative_metric(
            metrics_dumper,
            &format!("smft_wc{}_mc_sends_per_block_candidate", workchain_id),
            &format!("smft_wc{}_block_status_to_mc_sends", workchain_id),
            &format!("smft_wc{}_new_block_candidates", workchain_id),
            0.0,
        );

        //metrics for requests from validator to SMFT

        use catchain::utils::add_compute_percentage_metric;

        add_compute_percentage_metric(
            metrics_dumper,
            &format!("smft_wc{}_mc_delivered.frequency", workchain_id),
            &format!("smft_wc{}_mc_delivered", workchain_id),
            &format!("smft_wc{}_mc_requests", workchain_id),
            0.0,
        );

        add_compute_percentage_metric(
            metrics_dumper,
            &format!("smft_wc{}_mc_rejected.frequency", workchain_id),
            &format!("smft_wc{}_mc_rejected", workchain_id),
            &format!("smft_wc{}_mc_requests", workchain_id),
            0.0,
        );

        add_compute_percentage_metric(
            metrics_dumper,
            &format!("smft_wc{}_mc_approved.frequency", workchain_id),
            &format!("smft_wc{}_mc_approved", workchain_id),
            &format!("smft_wc{}_mc_requests", workchain_id),
            0.0,
        );

        //metrics for WC overlay

        add_compute_result_metric(metrics_dumper, &format!("smft_wc{}_wc_overlay_out_queries", workchain_id));

        metrics_dumper.add_derivative_metric(format!("smft_wc{}_wc_overlay_send_message_to_neighbours_calls", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_wc_overlay_send_message_to_far_neighbours_calls", workchain_id));

        metrics_dumper.add_derivative_metric(format!("smft_wc{}_wc_overlay_in_broadcasts", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_wc_overlay_in_broadcasts_size", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_wc_overlay_out_broadcasts", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_wc_overlay_out_broadcasts_size", workchain_id));

        metrics_dumper.add_derivative_metric(format!("smft_wc{}_wc_overlay_in_queries", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_wc_overlay_in_queries_size", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_wc_overlay_out_queries.total", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_wc_overlay_out_queries_size", workchain_id));

        metrics_dumper.add_derivative_metric(format!("smft_wc{}_wc_overlay_in_messages", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_wc_overlay_in_messages_size", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_wc_overlay_out_messages", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_wc_overlay_out_messages_size", workchain_id));

        add_compute_relative_metric(
            metrics_dumper,
            &format!("smft_wc{}_wc_overlay_in_broadcast_avg_size", workchain_id),
            &format!("smft_wc{}_wc_overlay_in_broadcasts_size", workchain_id),
            &format!("smft_wc{}_wc_overlay_in_broadcasts", workchain_id),
            0.0,
        );

        add_compute_relative_metric(
            metrics_dumper,
            &format!("smft_wc{}_wc_overlay_out_broadcast_avg_size", workchain_id),
            &format!("smft_wc{}_wc_overlay_out_broadcasts_size", workchain_id),
            &format!("smft_wc{}_wc_overlay_out_broadcasts", workchain_id),
            0.0,
        );

        add_compute_relative_metric(
            metrics_dumper,
            &format!("smft_wc{}_wc_overlay_in_query_avg_size", workchain_id),
            &format!("smft_wc{}_wc_overlay_in_queries_size", workchain_id),
            &format!("smft_wc{}_wc_overlay_in_queries", workchain_id),
            0.0,
        );

        add_compute_relative_metric(
            metrics_dumper,
            &format!("smft_wc{}_wc_overlay_out_query_avg_size", workchain_id),
            &format!("smft_wc{}_wc_overlay_out_queries_size", workchain_id),
            &format!("smft_wc{}_wc_overlay_out_queries.total", workchain_id),
            0.0,
        );

        add_compute_relative_metric(
            metrics_dumper,
            &format!("smft_wc{}_wc_overlay_in_message_avg_size", workchain_id),
            &format!("smft_wc{}_wc_overlay_in_messages_size", workchain_id),
            &format!("smft_wc{}_wc_overlay_in_messages", workchain_id),
            0.0,
        );

        add_compute_relative_metric(
            metrics_dumper,
            &format!("smft_wc{}_wc_overlay_out_message_avg_size", workchain_id),
            &format!("smft_wc{}_wc_overlay_out_messages_size", workchain_id),
            &format!("smft_wc{}_wc_overlay_out_messages", workchain_id),
            0.0,
        );

        //metrics for MC overlay

        add_compute_result_metric(metrics_dumper, &format!("smft_wc{}_mc_overlay_out_queries", workchain_id));

        metrics_dumper.add_derivative_metric(format!("smft_wc{}_mc_overlay_send_message_to_neighbours_calls", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_mc_overlay_send_message_to_far_neighbours_calls", workchain_id));

        metrics_dumper.add_derivative_metric(format!("smft_wc{}_mc_overlay_in_broadcasts", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_mc_overlay_in_broadcasts_size", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_mc_overlay_out_broadcasts", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_mc_overlay_out_broadcasts_size", workchain_id));

        metrics_dumper.add_derivative_metric(format!("smft_wc{}_mc_overlay_in_queries", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_mc_overlay_in_queries_size", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_mc_overlay_out_queries.total", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_mc_overlay_out_queries_size", workchain_id));

        metrics_dumper.add_derivative_metric(format!("smft_wc{}_mc_overlay_in_messages", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_mc_overlay_in_messages_size", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_mc_overlay_out_messages", workchain_id));
        metrics_dumper.add_derivative_metric(format!("smft_wc{}_mc_overlay_out_messages_size", workchain_id));

        add_compute_relative_metric(
            metrics_dumper,
            &format!("smft_wc{}_mc_overlay_in_broadcast_avg_size", workchain_id),
            &format!("smft_wc{}_mc_overlay_in_broadcasts_size", workchain_id),
            &format!("smft_wc{}_mc_overlay_in_broadcasts", workchain_id),
            0.0,
        );

        add_compute_relative_metric(
            metrics_dumper,
            &format!("smft_wc{}_mc_overlay_out_broadcast_avg_size", workchain_id),
            &format!("smft_wc{}_mc_overlay_out_broadcasts_size", workchain_id),
            &format!("smft_wc{}_mc_overlay_out_broadcasts", workchain_id),
            0.0,
        );

        add_compute_relative_metric(
            metrics_dumper,
            &format!("smft_wc{}_mc_overlay_in_query_avg_size", workchain_id),
            &format!("smft_wc{}_mc_overlay_in_queries_size", workchain_id),
            &format!("smft_wc{}_mc_overlay_in_queries", workchain_id),
            0.0,
        );

        add_compute_relative_metric(
            metrics_dumper,
            &format!("smft_wc{}_mc_overlay_out_query_avg_size", workchain_id),
            &format!("smft_wc{}_mc_overlay_out_queries_size", workchain_id),
            &format!("smft_wc{}_mc_overlay_out_queries.total", workchain_id),
            0.0,
        );

        add_compute_relative_metric(
            metrics_dumper,
            &format!("smft_wc{}_mc_overlay_in_message_avg_size", workchain_id),
            &format!("smft_wc{}_mc_overlay_in_messages_size", workchain_id),
            &format!("smft_wc{}_mc_overlay_in_messages", workchain_id),
            0.0,
        );

        add_compute_relative_metric(
            metrics_dumper,
            &format!("smft_wc{}_mc_overlay_out_message_avg_size", workchain_id),
            &format!("smft_mc{}_wc_overlay_out_messages_size", workchain_id),
            &format!("smft_mc{}_wc_overlay_out_messages", workchain_id),
            0.0,
        );
    }

    /// Debug dump state
    pub fn dump_state(&self, result: &mut string_builder::Builder) {      
        let blocks = self.blocks.lock().clone();
        let mut blocks: Vec<(std::time::SystemTime, BlockPtr)> = blocks.into_iter().map(|(_candidate_id, block)| {
            (block.lock().get_first_appearance_time().clone(), block.clone())
        }).collect::<Vec<_>>();

        blocks.sort_by(|a, b| b.0.cmp(&a.0));

        let config_params = self.workchain_delivery_params.lock().config_params.clone();

        result.append(format!("Workchain {} dump:\n", self.node_debug_id));
        result.append(format!("  - blocks: {}\n", blocks.len()));
        result.append(format!("  - local_adnl_id: {}\n", self.local_adnl_id));
        result.append(format!("  - local_bls_key_id: {}\n", self.local_bls_key.id()));
        result.append(format!("  - mc_validators_count: {}\n", self.mc_validators.len()));
        result.append(format!("  - mc_local_idx: {}\n", self.mc_local_idx));
        result.append(format!("  - mc_total_weight: {}\n", self.mc_total_weight));
        result.append(format!("  - mc_cutoff_weight: {}\n", self.mc_cutoff_weight));
        result.append(format!("  - wc_validators_count: {}\n", self.wc_validators.len()));
        result.append(format!("  - wc_local_idx: {}\n", self.wc_local_idx));

        result.append(format!("  - wc_total_weight: {}\n", self.wc_total_weight));
        result.append(format!("  - wc_cutoff_weight: {}\n", self.wc_cutoff_weight));

        result.append(format!("  - fwd_neighbours_count: [{};{}]\n", config_params.min_forwarding_neighbours_count, config_params.max_forwarding_neighbours_count));
        result.append(format!("  - far_neighbours_count: [{};{}]\n", config_params.min_far_neighbours_count, config_params.max_far_neighbours_count));
        result.append(format!("  - far_sync_period_ms: [{};{}]\n", config_params.min_far_neighbours_sync_period_ms, config_params.max_far_neighbours_sync_period_ms));
        result.append(format!("  - far_neighbours_resync_period_ms: {}\n", config_params.far_neighbours_resync_period_ms));
        result.append(format!("  - block_sync_period_ms: [{};{}]\n", config_params.min_block_sync_period_ms, config_params.max_block_sync_period_ms));
        result.append(format!("  - verification_obligation_cutoff: {}%\n", config_params.verification_obligation_cutoff));

        result.append(format!("  - block_lifetime_period: {:.3}s\n", Duration::from_millis(config_params.block_lifetime_period_ms as u64).as_secs_f64()));
        result.append(format!("  - block_sync_lifetime_period: {:.3}s\n", Duration::from_millis(config_params.block_sync_lifetime_period_ms as u64).as_secs_f64()));
        result.append(format!("  - blocks:\n"));

        for (i, (first_appearance_time, block)) in blocks.iter().enumerate() {
            let life_time = match first_appearance_time.elapsed() {
                Ok(elapsed) => elapsed.as_secs_f64(),
                Err(_) => 0.0,
            };
            let block = block.lock();
            let delivered_weight = block.get_deliveries_signature().get_total_weight(&self.wc_validators);

            result.append(format!("      - b{:03} ({:6.2}%): {}{}{}{}{} | delivery_latency={:.3}s, lifetime={:.3}s, hops={:2}, acks={:?}, nacks={:?}, stats={:?}, id={}\n",
                i,
                delivered_weight as f64 / self.wc_total_weight as f64 * 100.0,
                if block.is_delivered(&self.wc_validators, self.wc_cutoff_weight) { " " } else { "↔" },
                if block.is_sent_to_mc() { "↑" } else { " " },
                if block.is_received_from_mc() { "↓" } else { " " },
                if block.is_rejected() { "N" } else { " "},
                if block.has_approves() { "A" } else { " " },
                if let Some(state_change_time) = block.get_delivery_state_change_time() { 
                    if let Some(creation_time) = block.get_creation_time() {
                        if let Ok(latency) = state_change_time.duration_since(creation_time) {
                            latency.as_secs_f64()
                        } else {
                            0.0 
                        }
                    } else {
                        0.0
                    }
                } else { 
                    0.0 
                },
                life_time,
                block.get_merges_count(),
                block.get_approvals_signature(),
                block.get_rejections_signature(),
                block.get_delivery_stats(),
                block.get_id(),
            ));
        }
    }

    /*
        Common methods
    */

    /// Workchain validator set hash for WC
    pub fn get_wc_validator_set_hash(&self) -> &UInt256 {
        &self.wc_validator_set_hash
    }

    /// Workchain validator set hash for MC
    pub fn get_mc_validator_set_hash(&self) -> &UInt256 {
        &self.mc_validator_set_hash
    }    

    /// Get self weak reference
    fn get_self(&self) -> WorkchainPtr {
        self.self_weak_ref
            .lock()
            .clone()
            .expect("Self ref must be set")
            .upgrade()
            .expect("Self ref must exist")
    }

    /*
        Block management
    */

    /// Block status (delivered, rejected)
    pub fn get_block_status(&self, block: &BlockPtr) -> (bool, bool) {
        let block = block.lock();
        let is_delivered = block.is_delivered(&self.wc_validators, self.wc_cutoff_weight);
        let is_rejected = block.is_rejected();

        (is_delivered, is_rejected)
    }

    /// Should block be sent to MC
    fn should_send_to_mc(&self, block: &BlockPtr) -> bool {
        let (is_delivered, is_rejected) = self.get_block_status(block);

        is_rejected || is_delivered
    }

    /// Get block by its ID
    pub fn get_block_by_id(&self, block_id: &BlockIdExt) -> Option<BlockPtr> {
        Self::get_block_by_id_impl(&self.blocks.lock(), block_id)
    }

    /// Get block by its ID without lock
    fn get_block_by_id_impl(blocks: &HashMap<UInt256, BlockPtr>, block_id: &BlockIdExt) -> Option<BlockPtr> {
        match blocks.get(&Self::get_candidate_id(block_id)) {
            Some(block) => Some(block.clone()),
            None => None,
        }
    }

    /// Put new block to map
    fn add_block_impl(
        &self,
        block_id: &BlockIdExt,
        block_candidate: Option<Arc<BlockCandidateBody>>,
    ) -> BlockPtr {
        check_execution_time!(1_000);

        let block = {
            let mut blocks = self.blocks.lock();

            match Self::get_block_by_id_impl(&blocks, block_id) {
                Some(existing_block) => existing_block,
                None => {
                    trace!(target: "verificator", "Creating new block {:?} for workchain {}", block_id, self.node_debug_id);

                    let new_block = Block::create(block_id, &*self.blocks_instance_counter, self.wc_validators.len());

                    blocks.insert(Self::get_candidate_id(block_id), new_block.clone());

                    drop(blocks); //to release lock

                    self.start_synchronizing_block(&new_block);

                    new_block
                }
            }
        };

        if let Some(block_candidate) = block_candidate {
            let status = block.lock().update_block_candidate(block_candidate.clone());

            trace!(target: "verificator", "Block {:?} status is {} (node={})", block_id, status, self.node_debug_id);

            if status
            {
                trace!(target: "verificator", "Block candidate {} is delivered (node={})", block_id, self.node_debug_id);

                //measure latency for initial delivery

                let latency = block.lock().get_delivery_latency();
                if let Some(latency) = latency {
                    self.candidate_delivered_to_wc_latency_histogram.record(latency.as_millis() as f64);
                }

                //set block status to delivered

                self.set_block_status(&block_id, None);

                //initiate verification of the block

                self.verify_block(block_id, block_candidate);
            }
        }

        block
    }

    /// Remove block
    fn remove_block(&self, block_id: &BlockIdExt) {
        trace!(target: "verificator", "Remove block {:?} for workchain {}", block_id, self.node_debug_id);

        self.blocks.lock().remove(&Self::get_candidate_id(block_id));
    }

    /// Block update function
    fn synchronize_block(workchain_weak: Weak<Workchain>, block_weak: Weak<SpinMutex<Block>>, far_neighbours_sync_time: Option<SystemTime>, mut manual_candidate_loading_time: Option<SystemTime>) {
        let workchain = {
            if let Some(workchain) = workchain_weak.upgrade() {
                workchain
            } else {
                return;
            }
        };
        let block = {
            if let Some(block) = block_weak.upgrade() {
                block
            } else {
                return;
            }
        };
        let config_params = workchain.workchain_delivery_params.lock().config_params.clone();

        let (block_id, block_end_of_life_time, block_end_of_sync_time, delivery_weight) = {
            let block = block.lock();
            let delivered_weight = block.get_deliveries_signature().get_total_weight(&workchain.wc_validators);

            block.get_delivery_stats().syncs_count.fetch_add(1, Ordering::SeqCst);

            //if block candidate was received after block status - shift sync time to allow neighbours to sync

            let block_start_sync_time = if let Some(creation_time) = block.get_creation_time() {
                std::cmp::max(*block.get_first_appearance_time(), creation_time)
            } else {
                *block.get_first_appearance_time()
            };

            (block.get_id().clone(),
             *block.get_first_appearance_time() + Duration::from_millis(config_params.block_lifetime_period_ms as u64),
             block_start_sync_time + Duration::from_millis(config_params.block_sync_lifetime_period_ms as u64),
             delivered_weight
            )
        };

        //remove old blocks

        if let Ok(_) = block_end_of_life_time.elapsed() {
            workchain.remove_block(&block_id);
            return; //prevent all further sync logic because the block is expired
        }

        let mut rng = rand::thread_rng();
        let delay = Duration::from_millis(rng.gen_range(
            config_params.min_block_sync_period_ms,
            config_params.max_block_sync_period_ms + 1,
        ) as u64);
        let next_sync_time = SystemTime::now() + delay;
        let far_neighbours_force_sync = far_neighbours_sync_time.is_some() && far_neighbours_sync_time.unwrap().elapsed().is_ok();
        let far_neighbours_sync_time = {
            if far_neighbours_sync_time.is_none() || far_neighbours_force_sync {
                Some(SystemTime::now() + Duration::from_millis(rng.gen_range(config_params.min_far_neighbours_sync_period_ms,
                                        config_params.max_far_neighbours_sync_period_ms + 1) as u64))
            } else {
                far_neighbours_sync_time
            }
        };

        //manually load block candidate

        let has_candidate = block.lock().has_candidate();
        let is_delivered = block.lock().is_delivered(&workchain.wc_validators, workchain.wc_cutoff_weight);

        if !has_candidate && !is_delivered {
            if let Some(time) = manual_candidate_loading_time {
                if time.elapsed().is_ok() {
                    manual_candidate_loading_time = Some(SystemTime::now() + Duration::from_millis(MANUAL_CANDIDATE_LOADING_DELAY_MS as u64));
                    
                    log::debug!(target: "verificator", "Manually load block candidate for block {:?} in workchain {} (next attempt in {:.3}s from now)", block_id, workchain.node_debug_id,
                        MANUAL_CANDIDATE_LOADING_DELAY_MS as f64 / 1000.0);

                    workchain.load_block_candidate(&block_id);                 
                }
            } else {
                manual_candidate_loading_time = Some(*block.lock().get_first_appearance_time() + Duration::from_millis(MANUAL_CANDIDATE_LOADING_DELAY_MS as u64));
            }
        }

        //sync with far neighbours

        if block_end_of_sync_time.elapsed().is_err() {
            //trace!(target: "verificator", "Synchronize block {:?} for workchain {}", candidate_id, workchain.node_debug_id);
            trace!(target: "verificator", "Synchronize block {:?} for workchain {}: {:?}", block_id, workchain.node_debug_id, block.lock());

            //get delivery params

            let delivery_params = workchain.workchain_delivery_params.lock().clone();

            //periodically force push block status to neighbours

            let workchain_id = workchain.workchain_id;
            let node_debug_id = workchain.node_debug_id.clone();

            if far_neighbours_force_sync {
                if delivery_weight > 0 && delivery_weight < workchain.wc_total_weight {
                    trace!(
                        target: "verificator",
                        "Force block {:?} synchronization for workchain's #{} private overlay between far neighbours (overlay={})",
                        block_id,
                        workchain_id,
                        node_debug_id);

                    workchain.send_block_status_to_far_neighbours(&block, delivery_params.block_status_far_neighbours_count,
                        Duration::from_millis(config_params.far_neighbours_resync_period_ms as u64));
                }
            }
        }

        //sync with forward neighbours
        //check if block updates has to be sent to network (updates buffering)
        //sync even after end of sync time if neighbour sent us an update

        let ready_to_send = block.lock().toggle_send_ready(false);

        if ready_to_send {
            workchain.send_block_status_to_forwarding_neighbours(&block, workchain.workchain_delivery_params.lock().block_status_forwarding_neighbours_count);
        }        

        //schedule next synchronization

        workchain.runtime.spawn(async move {
            if let Ok(timeout) = next_sync_time.duration_since(SystemTime::now()) {
                /*trace!(
                    target: "verificator",
                    "Next block {:?} synchronization for workchain's #{} private overlay is scheduled at {} (in {:.3}s from now; overlay={})",
                    block_id,
                    workchain_id,
                    catchain::utils::time_to_string(&next_sync_time),
                    timeout.as_secs_f64(),
                    node_debug_id);*/

                sleep(timeout).await;
            }

            //synchronize block

            Self::synchronize_block(workchain_weak, block_weak, far_neighbours_sync_time, manual_candidate_loading_time);
        });
    }

    /// Start block synchronization
    fn start_synchronizing_block(&self, block: &BlockPtr) {
        Self::synchronize_block(Arc::downgrade(&self.get_self()), Arc::downgrade(block), None, None);
    }

    /// Put new block to map after delivery
    fn add_delivered_block(&self, block_candidate: Arc<BlockCandidateBody>) -> BlockPtr {
        let block_id = &block_candidate.candidate().id;

        //register block

        let block = self.add_block_impl(&block_id, Some(block_candidate.clone()));

        block.lock().get_delivery_stats().in_candidates_count.fetch_add(1, Ordering::SeqCst);

        block
    }

    /// Merge block status
    fn merge_block_status(
        &self,
        block_id: &BlockIdExt,
        source_node_idx: Option<usize>,
        deliveries_signature: &MultiSignature,
        approvals_signature: &MultiSignature,
        rejections_signature: &MultiSignature,
        merges_count: u32,
        created_timestamp: i64,
        received_from_workchain: bool,
    ) -> BlockPtr {
        check_execution_time!(5_000);

        self.merge_block_status_counter.increment(1);

        //get existing block or create it

        let block = self.add_block_impl(block_id, None);

        {
            let block = block.lock();

            if received_from_workchain {
                block.get_delivery_stats().in_wc_merges_count.fetch_add(1, Ordering::SeqCst);
            } else {
                block.get_delivery_stats().in_mc_merges_count.fetch_add(1, Ordering::SeqCst);
            }
        }

        //update node's knowledge of the block

        if let Some(source_node_idx) = source_node_idx {
            let source_node_delivery_weight = deliveries_signature.get_total_weight(&self.wc_validators);
            let mut block = block.lock();

            block.set_node_delivery_weight(source_node_idx, source_node_delivery_weight);
            block.set_node_status_received_time(source_node_idx, SystemTime::now());
        }

        //check if block is MC originated

        if !received_from_workchain {
            block.lock().mark_as_received_from_mc();
        }

        //update status

        let status = block.lock().merge_status(deliveries_signature, approvals_signature, rejections_signature, merges_count, created_timestamp
            );
        match status {
            Ok(status) => {
                if !status {
                    //block status is the same
                    return block.clone();
                }
            }
            Err(err) => {
                error!(target: "verificator", "Can't merge block status for block {:?} in workchain {}: {:?}", block_id, self.node_debug_id, err);
            }
        }

        {
            let block = block.lock();

            if received_from_workchain {
                block.get_delivery_stats().in_wc_real_merges_count.fetch_add(1, Ordering::SeqCst);
            } else {
                block.get_delivery_stats().in_mc_real_merges_count.fetch_add(1, Ordering::SeqCst);
            }
        }

        //compute latency for MC deliveries

        if !received_from_workchain {
            let was_mc_processed = block.lock().was_mc_processed();
            let (is_delivered, _) = self.get_block_status(&block);

            self.block_status_received_in_mc_counter.total_increment();

            if is_delivered {
                self.block_status_received_in_mc_counter.success();
            } else {
                self.block_status_received_in_mc_counter.failure();
            }

            if !was_mc_processed && is_delivered {
                let latency = block.lock().get_delivery_latency();
                if let Some(latency) = latency {
                    self.block_status_received_in_mc_latency_histogram.record(latency.as_millis() as f64);
                }

                self.update_block_external_delivery_metrics_impl(&block, None, Some(std::time::SystemTime::now()));                

                self.block_status_merges_count_histogram.record(merges_count as f64);

                block.lock().mark_as_mc_processed();
            }
        }

        //TODO: do not send block to neighbours if it was received from MC overlay

        //block status was updated

        let send_immediately_to_mc = false;

        self.send_block_status(&block, received_from_workchain, send_immediately_to_mc);

        block
    }

    /// Set blocks status (delivered - None, ack - Some(true), nack - Some(false))
    fn set_block_status(&self, block_id: &BlockIdExt, status: Option<bool>) {
        check_execution_time!(5_000);

        self.set_block_status_counter.increment(1);

        //get existing block or create it

        let block = self.add_block_impl(block_id, None);

        //update block status

        if self.wc_local_idx != -1 {
            let update_status = block.lock().set_status(&self.local_bls_key, self.wc_local_idx as u16, self.wc_validators.len() as u16, status);

            match update_status {
                Ok(update_status) => {
                    if update_status {
                        //block status was updated
                        let received_from_workchain = true;
                        let send_immediately_to_mc = status.is_some(); //send to MC immediately ACK/NACK without buffering
                        self.send_block_status(&block, received_from_workchain, send_immediately_to_mc);
                    }
                }
                Err(err) => {
                    warn!(target: "verificator", "Can't sign block {} in workchain's node {} private overlay: {:?}", block_id, self.node_debug_id, err);
                }
            }
        }
    }

    /// Update block delivery metrics
    pub fn update_block_external_delivery_metrics(
        &self,
        block_id: &BlockIdExt,
        external_request_time: &std::time::SystemTime) {
        let block = self.add_block_impl(block_id, None);

        self.update_block_external_delivery_metrics_impl(&block, Some(external_request_time.clone()), None);
    }

    fn update_block_external_delivery_metrics_impl(
        &self,
        block: &BlockPtr,
        external_request_time: Option<std::time::SystemTime>,
        delivery_state_change_time: Option<std::time::SystemTime>,
    ) {
        let (should_update_delivery_metrics, is_first_time_external_request, has_approves, is_rejected, is_delivered, latency) = {
            let mut block = block.lock();
            let is_delivered = block.is_delivered(&self.wc_validators, self.wc_cutoff_weight);

            let is_first_time_external_request = block.get_first_external_request_time().is_none() && external_request_time.is_some();
            if let Some(external_request_time) = external_request_time {
                block.set_first_external_request_time(&external_request_time);
            }

            let is_delivery_state_changed = block.get_delivery_state_change_time().is_none() && delivery_state_change_time.is_some();
            if let Some(delivery_state_change_time) = delivery_state_change_time {
                block.set_delivery_state_change_time(&delivery_state_change_time);
            }

            let latency = if let Some(external_request_time) = external_request_time {
                if let Some(delivery_state_change_time) = delivery_state_change_time {
                    if let Ok(latency) = delivery_state_change_time.duration_since(external_request_time) {
                        Some(latency.as_millis())
                    } else {
                        Some(0) //default case - no latency
                    }
                } else {
                    None
                }
            } else {
                None
            };

            let should_update_delivery_metrics =  block.get_first_external_request_time().is_some() && block.get_delivery_state_change_time().is_some() &&
                (is_first_time_external_request || is_delivery_state_changed);

            (should_update_delivery_metrics, is_first_time_external_request, block.has_approves(), block.is_rejected(), is_delivered, latency)
        };

        if is_first_time_external_request {
            self.external_request_counter.increment(1);
        }

        if !should_update_delivery_metrics {
            return;
        }

        if is_delivered {
            self.external_request_delivered_blocks_counter.increment(1);
        }

        if is_rejected {
            self.external_request_rejected_blocks_counter.increment(1);
        }

        if has_approves {
            self.external_request_approved_blocks_counter.increment(1);
        }

        if let Some(latency) = latency {
            self.block_external_request_delays_histogram.record(latency as f64);
        }
    }

    /*
        Broadcast delivery protection methods
    */

    /// Get candidate ID
    pub fn get_candidate_id(id: &BlockIdExt) -> UInt256 {
        id.root_hash.clone()
    }

    /// Process new block candidate broadcast
    fn process_block_candidate(&self, block_candidate: Arc<BlockCandidateBody>) {
        check_execution_time!(5_000);

        trace!(target: "verificator", "BlockCandidateBroadcast received by verification workchain's node {} private overlay: {:?}", self.node_debug_id, block_candidate.candidate());

        self.process_block_candidate_counter.increment(1);

        self.add_delivered_block(block_candidate);
    }

    /// New block broadcast has been generated
    pub fn send_new_block_candidate(&self, candidate: BlockCandidateBroadcast) {
        check_execution_time!(5_000);

        let _hang_checker = HangCheck::new(self.runtime.clone(), format!("Workchain::send_new_block_candidate: {:?} for workchain {}", candidate.id, self.node_debug_id), Duration::from_millis(1000));

        self.new_block_candidate_counter.increment(1);

        //process block candidate

        let block_id = candidate.id.clone();
        let block_candidate = Arc::new(BlockCandidateBody::new(candidate));
        let serialized_candidate = block_candidate.serialized_candidate().clone();

        self.process_block_candidate(block_candidate);

        //send candidate to other workchain validators

        if let Some(workchain_overlay) = self.get_workchain_overlay() {
            trace!(target: "verificator", "Send new block broadcast in workchain {} with block_id {:?}", self.node_debug_id, block_id);

            workchain_overlay.send_broadcast(
                &self.local_adnl_id,
                &self.local_id,
                serialized_candidate,
            );
        }
    }

    /// Load block candidate manually
    fn load_block_candidate(&self, block_id: &BlockIdExt) {
        //check if block candidate is already loaded

        let block_status = self.get_block_by_id(block_id);

        if let Some(block_status) = block_status {
            if block_status.lock().has_candidate() {
                return;
            }
        }

        //load block candidate

        let block_handle = match self.engine.load_block_handle(&block_id) {
            Ok(Some(block_handle)) => block_handle,
            Err(err) => {
                log::warn!(target: "verificator", "Can't load block handle for block ID {:?} in workchain {}: {:?}", block_id, self.node_debug_id, err);
                return;
            },
            _ => {
                log::warn!(target: "verificator", "Block handle for block ID {:?} not found in workchain {}", block_id, self.node_debug_id);
                return;
            }
        };

        //start async block loading

        log::trace!(target: "verificator", "Start async block loading for block ID {:?} in workchain {}", block_id, self.node_debug_id);

        let node_debug_id = self.node_debug_id.clone();
        let engine = self.engine.clone();
        let block_id = block_id.clone();
        let workchain = Arc::downgrade(&self.get_self());

        self.runtime.spawn(async move {
            log::trace!(target: "verificator", "Block loading for block ID {:?} in workchain {}", block_id, node_debug_id);

            //load block

            let block = match engine.load_block(&block_handle).await {
                Ok(block) => block,
                Err(err) => {
                    log::warn!(target: "verificator", "Can't load block for block ID {:?} in workchain {}: {:?}", block_id, node_debug_id, err);
                    return;
                }
            };

            //read created_by field

            let created_by = match block.block() {
                Ok(block) => match block.read_extra() {
                    Ok(extra) => extra.created_by().clone(),
                    Err(err) => {
                        log::warn!(target: "verificator", "Can't read created_by field from extra for block ID {:?} in workchain {}: {:?}", block_id, node_debug_id, err);
                        return;
                    }
                },
                Err(err) => {
                    log::warn!(target: "verificator", "Can't read created_by field for block ID {:?} in workchain {}: {:?}", block_id, node_debug_id, err);
                    return;
                }
            };

            //get creation timestamp

            let created_timestamp = match SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                Ok(n) => n.as_millis(),
                Err(err) => {
                    log::warn!(target: "verificator", "Can't get current time for block ID {:?} in workchain {}: {:?}", block_id, node_debug_id, err);
                    return;
                }
            };

            //convert block to block candidate

            let block_candidate = BlockCandidateBroadcast {
                id: block_id.clone(),
                data: block.data().into(),
                collated_data: Vec::new(),
                collated_data_file_hash: UInt256::default(),
                created_by,
                created_timestamp: created_timestamp as i64,
            };

            log::debug!(target: "verificator", "Block candidate loaded for block ID {:?} in workchain {}", block_id, node_debug_id);

            //process block candidate

            let workchain = match workchain.upgrade() {
                Some(workchain) => workchain,
                None => {
                    log::warn!(target: "verificator", "Workchain is already dropped for block ID {:?} in workchain {}", block_id, node_debug_id);
                    return;
                }
            };

            let block_status = workchain.get_block_by_id(&block_id);

            if let Some(block_status) = block_status {
                if block_status.lock().has_candidate() {
                    log::trace!(target: "verificator", "Block candidate already loaded for block ID {:?} in workchain {}", block_id, node_debug_id);
                    return;
                }
            }

            let block_candidate = Arc::new(BlockCandidateBody::new(block_candidate));

            workchain.process_block_candidate(block_candidate);
        });
    }

    /// Start force block delivery flow
    pub fn start_force_block_delivery(&self, block_id: &BlockIdExt) {
        let block_status = self.add_block_impl(block_id, None);
        let mut block = block_status.lock();
        let last_block_force_delivery_time = block.get_last_block_force_delivery_request_time();

        if let Some(last_block_force_delivery_time) = last_block_force_delivery_time {
            if let Ok(elapsed) = last_block_force_delivery_time.elapsed() {
                if elapsed.as_millis() < ALLOWED_FORCE_DELIVERY_DELAY_MS {
                    log::trace!(target: "verificator", "Force block delivery for block ID {:?} in workchain {} is already started (next attempt in {:.3}s)", block_id, self.node_debug_id,
                        (ALLOWED_FORCE_DELIVERY_DELAY_MS - elapsed.as_millis()) as f64 / 1000.0);
                    return;
                }
            }
        }

        log::debug!(target: "verificator", "Start force block delivery for block ID {:?} in workchain {}", block_id, self.node_debug_id);

        block.set_last_block_force_delivery_request_time(&SystemTime::now());

        drop(block);

        let workchain = Arc::downgrade(&self.get_self());
        let node_debug_id = self.node_debug_id.clone();
        let block_id = block_id.clone();

        self.runtime.spawn(async move {
            let workchain = match workchain.upgrade() {
                Some(workchain) => workchain,
                None => {
                    log::warn!(target: "verificator", "Workchain is already dropped for block ID {:?} in workchain {}", block_id, node_debug_id);
                    return;
                }
            };

            //calculate number of workchain nodes for force delivery request

            let request_nodes_count = std::cmp::max(1, (workchain.wc_validators.len() as f64 / workchain.mc_validators.len() as f64 * FORCE_DELIVERY_DUPLICATION_FACTOR) as usize);

            //request block delivery from several workchain nodes

            log::debug!(target: "verificator", "Force block delivery for block ID {:?} in workchain {} is started (requesting from {} nodes)", block_id, node_debug_id, request_nodes_count);

            workchain.send_block_status_to_workchain(&block_status, request_nodes_count);
        });
    }

    /// Get node index by adnl id
    fn get_wc_validator_idx_by_adnl_id(&self, adnl_id: &PublicKeyHash) -> Option<usize> {
        for (idx, src_adnl_id) in self.wc_validators_adnl_ids.iter().enumerate() {
            if src_adnl_id == adnl_id {
                return Some(idx);
            }
        }

        None
    }

    /// Block status update has been received
    pub fn process_block_status(&self, adnl_id: &PublicKeyHash, block_status: BlockCandidateStatus, received_from_workchain: bool) -> Result<BlockPtr> {
        check_execution_time!(50_000);

        trace!(target: "verificator", "BlockCandidateStatus received by verification workchain's node {} private overlay: {:?}", self.node_debug_id, block_status);

        self.process_block_status_counter.increment(1);

        let wc_pub_key_refs: Vec<&[u8; BLS_PUBLIC_KEY_LEN]> = self.wc_pub_keys.iter().map(|x| x).collect();

        //parse incoming status

        //TODO: add incoming block status signature check to prevent malicious status updates

        let block_id: BlockIdExt = block_status.id.clone().into();
        let candidate_id = Self::get_candidate_id(&block_id);
        let deliveries_signature = MultiSignature::deserialize(1, &candidate_id, &wc_pub_key_refs, &block_status.deliveries_signature);
        let approvals_signature = MultiSignature::deserialize(2, &candidate_id, &wc_pub_key_refs, &block_status.approvals_signature);
        let rejections_signature = MultiSignature::deserialize(3, &candidate_id, &wc_pub_key_refs, &block_status.rejections_signature);

        if let Err(err) = deliveries_signature {
            fail!(
                "Can't parse block candidate status (deliveries signature) {:?}: {:?}",
                block_status,
                err
            );
        }

        if let Err(err) = approvals_signature {
            fail!(
                "Can't parse block candidate status (approvals signature) {:?}: {:?}",
                block_status,
                err
            );
        }

        if let Err(err) = rejections_signature {
            fail!(
                "Can't parse block candidate status (rejections signature) {:?}: {:?}",
                block_status,
                err
            );
        }

        let deliveries_signature = deliveries_signature.unwrap();
        let approvals_signature = approvals_signature.unwrap();
        let rejections_signature = rejections_signature.unwrap();

        let source_node_idx = if received_from_workchain {
            self.get_wc_validator_idx_by_adnl_id(adnl_id)
        } else { 
            None
        };

        //merge block status

        Ok(self.merge_block_status(
            &block_id,
            source_node_idx,
            &deliveries_signature,
            &approvals_signature,
            &rejections_signature,
            block_status.merges_cnt as u32,
            block_status.created_timestamp,
            received_from_workchain,
        ))
    }

    /// Send block for delivery
    fn send_block_status(&self, block: &BlockPtr, received_from_workchain: bool, send_immediately_to_mc: bool) {
        //serialize block status

        let (serialized_block_status, block_id, is_sent_to_mc) = {
            //this block is needeed to minimize lock of block
            let mut block = block.lock();
            let block_id = block.get_id().clone();

            (block.serialize(), block_id, block.is_sent_to_mc())
        };

        //check if block need to be send to mc

        let should_send_to_mc = self.should_send_to_mc(block) && received_from_workchain && !is_sent_to_mc;

        if send_immediately_to_mc || should_send_to_mc {
            if let Some(_mc_overlay) = self.get_mc_overlay() {
                trace!(target: "verificator", "Send block {:?} to MC after update (node={})", block_id, self.node_debug_id);

                let serialized_block_status = serialized_block_status.clone();

                let latency = block.lock().get_delivery_latency();
                if let Some(latency) = latency {
                    self.block_status_send_to_mc_latency_histogram.record(latency.as_millis() as f64);
                }

                if should_send_to_mc {
                    //prevent double sending of block because of new delivery signatures
                    //do not mark block as delivered for ACK/NACK signals (because they can appear earlier than cutoff weight for delivery BLS)
                    let mut block = block.lock();

                    block.mark_as_sent_to_mc();
                    block.get_delivery_stats().out_mc_syncs_count.fetch_add(1, Ordering::SeqCst);
                    block.get_delivery_stats().out_mc_sends_count.fetch_add(self.mc_validators.len(), Ordering::SeqCst);
                }

                self.send_block_status_to_mc(serialized_block_status);
            }
        }

        //mark as ready for send within workchain

        block.lock().toggle_send_ready(true);
    }

    /// Generate random list of workchain validators
    fn generate_random_wc_validators(&self, count: usize) -> Vec<usize> {
        //TODO: remove duplicate sends? (maybe)
        let mut rng = rand::thread_rng();
        let mut validators = Vec::with_capacity(count);

        for _i in 0..count {
            let idx = rng.gen_range(0, self.wc_validators.len());

            if idx == self.wc_local_idx as usize {
                continue;
            }            

            validators.push(idx);
        }

        validators
    }

    /// Send block from masterchain to workchain
    fn send_block_status_to_workchain(&self, block: &BlockPtr, nodes_count: usize) {        
        let nodes = self.generate_random_wc_validators(nodes_count);

        if nodes.len() < 1 {
            return;
        }

        self.send_block_status_counter.increment(1);

        //serialize block status & update its sent metrics

        let (serialized_block_status, block_id) = {
            //this block is needeed to minimize lock of block
            let now = SystemTime::now();
            let mut block = block.lock();

            block.get_delivery_stats().mc_to_wc_sends.fetch_add(1, Ordering::SeqCst);
            block.get_delivery_stats().out_wc_sends_count.fetch_add(nodes.len(), Ordering::SeqCst);
        
            for node_idx in nodes.iter() {
                block.set_node_status_sent_time(*node_idx, now);
            }

            (block.serialize(), block.get_id().clone())
        };

        //send block status to WC validators       

        trace!(target: "verificator", "Send block {:?} to WC validators {:?} (node={})", block_id, nodes, self.node_debug_id);

        self.send_message_to_workchain_validators(serialized_block_status, &nodes);
    }

    /// Send block to forwarding neighbours
    fn send_block_status_to_forwarding_neighbours(&self, block: &BlockPtr, neighbours_count: usize) {
        let forwarding_neighbours = self.generate_random_wc_validators(neighbours_count);

        if forwarding_neighbours.len() < 1 {
            return;
        }

        self.send_block_status_counter.increment(1);

        //serialize block status & update its sent metrics

        let (serialized_block_status, block_id) = {
            //this block is needeed to minimize lock of block
            let now = SystemTime::now();
            let mut block = block.lock();

            block.get_delivery_stats().forwarding_neighbours_sends.fetch_add(1, Ordering::SeqCst);
            block.get_delivery_stats().out_wc_sends_count.fetch_add(forwarding_neighbours.len(), Ordering::SeqCst);
        
            for node_idx in forwarding_neighbours.iter() {
                block.set_node_status_sent_time(*node_idx, now);
            }

            (block.serialize(), block.get_id().clone())
        };

        //send block status to neighbours

        trace!(target: "verificator", "Send block {:?} to forwarding neighbours {:?} (node={})", block_id, forwarding_neighbours, self.node_debug_id);

        self.send_message_to_forwarding_neighbours(serialized_block_status, &forwarding_neighbours);
    }

    /// Send block to far neighbours
    fn send_block_status_to_far_neighbours(&self, block: &BlockPtr, max_neighbours_count: usize, far_neighbours_resync_period: Duration) {
        let max_far_neighbours_sync_time = std::time::SystemTime::now() - far_neighbours_resync_period;
        let far_neighbours = block.lock().calc_low_delivery_nodes_indexes(max_neighbours_count, self.wc_cutoff_weight, self.wc_local_idx as usize, max_far_neighbours_sync_time);

        if far_neighbours.len() < 1 {
            return;
        }

        self.send_block_status_counter.increment(1);        

        //serialize block status & update its sent metrics

        let (serialized_block_status, block_id) = {
            //this block is needeed to minimize lock of block
            let now = SystemTime::now();
            let mut block = block.lock();

            block.get_delivery_stats().far_neighbours_sends.fetch_add(1, Ordering::SeqCst);
            block.get_delivery_stats().out_wc_sends_count.fetch_add(far_neighbours.len(), Ordering::SeqCst);

            for node_idx in far_neighbours.iter() {
                block.set_node_status_sent_time(*node_idx, now);
            }

            (block.serialize(), block.get_id().clone())
        };

        //send block status to neighbours

        trace!(target: "verificator", "Send block {:?} to far neighbours {:?} (node={})", block_id, far_neighbours, self.node_debug_id);

        self.send_message_to_far_neighbours(serialized_block_status, &far_neighbours);
    }

    /*
        Verification management
    */

    fn should_verify(&self, block_id: &BlockIdExt) -> bool {
        if let Ok(local_bls_key) = self.local_bls_key.export_key() {
            let mut local_key_payload : Vec<u8> = local_bls_key.into();
            let candidate_id = Self::get_candidate_id(block_id);
            let mut payload : Vec<u8> = candidate_id.as_array().into();
            
            payload.append(&mut local_key_payload);

            let payload_ptr = unsafe { std::slice::from_raw_parts(payload.as_ptr() as *const u8, payload.len()) };
            let hash = crc32_digest(payload_ptr);

            if self.wc_validators.len () < 1 {
                return false; //no verification in empty workchain
            }
            let random_value = (((hash as usize) % self.wc_validators.len()) as f64) / (self.wc_validators.len() as f64);
            let verification_obligation_cutoff = self.workchain_delivery_params.lock().config_params.verification_obligation_cutoff as f64 / 100.0;
            let result = random_value < verification_obligation_cutoff;

            trace!(target: "verificator", "Verification obligation for block candidate {} is {:.3} < {:.3} -> {} (node={})", block_id, random_value, verification_obligation_cutoff, result, self.node_debug_id);
    
            return result;
        }

        log::warn!(target: "verificator", "Can't export BLS secret key (node={})", self.node_debug_id);
        
        false //can't verify without secret BLS key
    }

    fn verify_block(&self, block_id: &BlockIdExt, block_candidate: Arc<BlockCandidateBody>) {
        trace!(target: "verificator", "Verifying block candidate {} (node={})", block_id, self.node_debug_id);

        if !self.should_verify(block_id) {
            trace!(target: "verificator", "Skipping verification of block candidate {} (node={})", block_id, self.node_debug_id);
            return;
        }

        self.verify_block_counter.total_increment();

        if let Some(verification_listener) = self.listener.upgrade() {
            let block_id = block_id.clone();
            let workchain = Arc::downgrade(&self.get_self());
            let node_debug_id = self.node_debug_id.clone();
            let runtime = self.runtime.clone();
            let _verification_future = self.runtime.spawn(async move {
                if let Some(workchain) = workchain.upgrade() {
                    check_execution_time!(1_000);
                    let _hang_checker = HangCheck::new(runtime, format!("Workchain::verify_block: {} for workchain {}", block_id, node_debug_id), Duration::from_millis(2000));

                    let candidate = super::BlockCandidate {
                        block_id: block_candidate.candidate().id.clone().into(),
                        data: block_candidate.candidate().data.to_vec().into(),
                        collated_file_hash: block_candidate
                            .candidate()
                            .collated_data_file_hash
                            .clone()
                            .into(),
                        collated_data: block_candidate.candidate().collated_data.to_vec().into(),
                        created_by: block_candidate.candidate().created_by.clone().into(),
                    };

                    let verification_status = verification_listener.verify(&candidate).await;

                    workchain.set_block_verification_status(&block_id, verification_status);
                }
            });
        }
    }

    fn set_block_verification_status(&self, block_id: &BlockIdExt, verification_status: bool) {
        trace!(target: "verificator", "Verified block candidate {:?} status is {} (node={})", block_id, verification_status, self.node_debug_id);

        if verification_status {
            self.verify_block_counter.success();
        } else {
            self.verify_block_counter.failure();
        }

        self.set_block_status(block_id, Some(verification_status));

        if !verification_status {
            error!(target: "verificator", "Malicios block candidate {:?} detected (node={})", block_id, self.node_debug_id);
        }
    }

    /*
        Arbitrage
    */

    pub fn start_arbitrage(&self, block_id: &BlockIdExt) {
        log::warn!(target: "verificator", "Arbitrage for block candidate {:?} (node={})", block_id, self.node_debug_id);

        //TODO: arbitrage implementation
    }

    /*
        Private network (for workchains)
    */

    /// Workchain's private overlay
    fn get_workchain_overlay(&self) -> Option<Arc<WorkchainOverlay>> {
        self.workchain_overlay.lock().clone()
    }

    /// Send message to forwarding neighbours in a private workchain overlay
    fn send_message_to_forwarding_neighbours(&self, data: BlockPayloadPtr, neighbours: &Vec<usize>) {
        if let Some(workchain_overlay) = self.get_workchain_overlay() {
            workchain_overlay.send_message_to_forwarding_neighbours(data, neighbours);
        }
    }

    /// Send message to far neighbours in a private workchain overlay
    fn send_message_to_far_neighbours(&self, data: BlockPayloadPtr, neighbours: &Vec<usize>) {
        if let Some(workchain_overlay) = self.get_workchain_overlay() {
            workchain_overlay.send_message_to_far_neighbours(data, neighbours);
        }
    }

    /// Send message to workchain validators in a private workchain overlay
    fn send_message_to_workchain_validators(&self, data: BlockPayloadPtr, validators: &Vec<usize>) {
        if let Some(mc_overlay) = self.get_mc_overlay() {
            //convert nodes indexes to full list of validators indexes (shifted by number of MC validators)

            let validators = validators.clone().iter().map(|idx| *idx + self.mc_validators.len()).collect();

            mc_overlay.send_message_to_validators(data, &validators);
        }
    }

    /*
        Public network (for interaction with MC)
    */

    /// MC public overlay
    fn get_mc_overlay(&self) -> Option<Arc<WorkchainOverlay>> {
        self.mc_overlay.lock().clone()
    }

    fn send_block_status_to_mc(&self, data: BlockPayloadPtr) {
        log::trace!(target: "verificator", "Workchain::send_block_status_to_mc");

        if let Some(mc_overlay) = self.get_mc_overlay() {
            self.send_block_status_to_mc_counter.increment(1);

            mc_overlay.send_all(data, 0, self.mc_validators.len());
        }
    }

    /*
        Configuration management
    */

    /// Update dynamic configuration
    async fn compute_delivery_params(&self) -> Result<WorkchainDeliveryParams> {
        check_execution_time!(1_000);

        let engine = self.engine.clone();

        let config_params = engine.load_actual_config_params().await?;
        let config_params = config_params.smft_parameters()?;

        let status_fwd_neighbours_count = ((self.wc_validators.len() as f64).sqrt() as usize).clamp(config_params.min_forwarding_neighbours_count as usize, config_params.max_forwarding_neighbours_count as usize);

        let params = WorkchainDeliveryParams {
            block_status_forwarding_neighbours_count: status_fwd_neighbours_count,
            block_status_far_neighbours_count: ((status_fwd_neighbours_count as f64).sqrt() as usize).clamp(config_params.min_far_neighbours_count as usize, config_params.max_far_neighbours_count as usize),
            config_params,
        };

        Ok(params)
    }

    /// Start updating of dynamic configuration
    async fn start_configuration_update(&self) {
        let workchain_weak = Arc::downgrade(&self.get_self());
        match self.compute_delivery_params().await {
            Ok(params) => {
                trace!(target: "verificator", "Workchain's #{} dynamic configuration initialized (overlay={}): {:?}", self.workchain_id, self.node_debug_id, params);
                *self.workchain_delivery_params.lock() = params;
            }
            Err(err) => {
                log::error!(target: "verificator", "Can't initialize workchain's #{} configuration; using default (overlay={}): {:?}", self.workchain_id, self.node_debug_id, err);
            }
        }

        self.runtime.spawn(async move {
            Self::update_configuration(workchain_weak, None).await
        });
    }

    /// Get first MC handle
    fn get_mc_block_handle(&self) -> Result<Arc<BlockHandle>> {
        match self.engine.load_last_applied_mc_block_id() {
            Ok(Some(mc_block_id)) => {
                match self.engine.load_block_handle(&mc_block_id) {
                    Ok(Some(mc_block_handle)) => Ok(mc_block_handle),
                    Err(err) => Err(err),
                    _ => Err(format_err!("Can't get MC block handle for block ID {:?}", mc_block_id)),
                }
            },
            Err(err) => Err(err),
            _ => Err(format_err!("Can't get last applied MC block ID")),
        }
    }

    /// Update dynamic configuration routine
    async fn update_configuration(workchain_weak: Weak<Workchain>, mut prev_mc_block_handle: Option<Arc<BlockHandle>>) {
        loop {
            let workchain = {
                if let Some(workchain) = workchain_weak.upgrade() {
                    workchain
                } else {
                    break;
                }
            };

            trace!(target: "verificator", "Workchain's #{} configuration update for {}", workchain.workchain_id, workchain.node_debug_id);

            //update configuration

            match workchain.compute_delivery_params().await {
                Ok(params) => {
                    trace!(target: "verificator", "Workchain's #{} dynamic configuration updated (overlay={}): {:?}", workchain.workchain_id, workchain.node_debug_id, params);
                    *workchain.workchain_delivery_params.lock() = params;
                }
                Err(err) => {
                    log::error!(target: "verificator", "Can't update workchain's #{} configuration (overlay={}): {:?}", workchain.workchain_id, workchain.node_debug_id, err);
                }
            }

            //schedule next update

            if prev_mc_block_handle.is_none() {
                match workchain.get_mc_block_handle() {
                    Ok(mc_block_handle) => prev_mc_block_handle = Some(mc_block_handle),
                    Err(err) => {
                        log::error!(target: "verificator", "Can't get MC block handle for workchain's #{} configuration update (overlay={}): {:?}", workchain.workchain_id, workchain.node_debug_id, err);
                    }
                }
            }

            let engine = workchain.engine.clone();
            let runtime = workchain.runtime.clone();
            let node_debug_id = workchain.node_debug_id.clone();

            drop(workchain); //release variable to prevent retaining of workchain

            if let Some(mc_block_handle) = prev_mc_block_handle.clone() {
                loop {
                    let _hang_checker = HangCheck::new(runtime.clone(), format!("Workchain::update_configuration: for workchain {}", node_debug_id), Duration::from_millis(20000));

                    if let Ok(r) = engine.wait_next_applied_mc_block(&mc_block_handle, Some(1000)).await {
                        let block = r.0;

                        prev_mc_block_handle = Some(block.clone());

                        if let Ok(status) = block.is_key_block() {
                            if status {
                                break;
                            }
                        }
                    } else {
                        if let Ok(block_gen_time) = mc_block_handle.gen_utime() {
                            let diff = engine.now() - block_gen_time;
                            if diff > 15 {
                                log::warn!(target:"verificator", "No next mc block more then {diff} sec");
                            }
                        }
                    }
    
                    if workchain_weak.upgrade().is_none() {
                        break;
                    }
                }
            } else {
                sleep(Duration::from_millis(1000)).await;
            };
        }
    }
}

impl WorkchainOverlayListener for Workchain {
    /// Block status has been updated
    fn on_workchain_block_status_updated(
        &self,
        adnl_id: &PublicKeyHash,
        block_status: BlockCandidateStatus,
        received_from_workchain: bool,
    ) -> Result<BlockPtr> {
        let block_id: BlockIdExt = block_status.id.clone().into();
        let _hang_checker = HangCheck::new(self.runtime.clone(), format!("Workchain::on_workchain_block_status_updated: {} for workchain {}", block_id, self.node_debug_id), Duration::from_millis(1000));

        self.process_block_status(adnl_id, block_status, received_from_workchain)
    }

    /// Process new block candidate broadcast
    fn on_workchain_block_candidate(&self, block_candidate: Arc<BlockCandidateBody>) {
        let _hang_checker = HangCheck::new(self.runtime.clone(), format!("Workchain::on_workchain_block_candidate: {:?} for workchain {}", block_candidate.candidate().id, self.node_debug_id), Duration::from_millis(1000));

        self.process_block_candidate(block_candidate);
    }
}

impl Drop for Workchain {
    fn drop(&mut self) {
        log::info!(target: "verificator", "Dropping verification workchain {}", self.node_debug_id);
    }
}
