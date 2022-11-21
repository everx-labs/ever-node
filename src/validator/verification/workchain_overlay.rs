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

use super::block::BlockCandidateBody;
use super::block::BlockPtr;
use super::*;
use super::utils::HangCheck;
use crate::engine_traits::PrivateOverlayOperations;
use crate::validator::validator_utils::get_adnl_id;
use crate::validator::validator_utils::sigpubkey_to_publickey;
use catchain::BlockPayloadPtr;
use catchain::CatchainNode;
use catchain::CatchainOverlayListener;
use catchain::CatchainOverlayLogReplayListener;
use catchain::ExternalQueryResponseCallback;
use catchain::PrivateOverlayShortId;
use catchain::PublicKeyHash;
use catchain::profiling::ResultStatusCounter;
use catchain::profiling::InstanceCounter;
use catchain::profiling::check_execution_time;
use log::*;
use overlay::OverlayShortId;
use rand::Rng;
use spin::mutex::SpinMutex;
use std::time::Duration;
use std::time::SystemTime;
use tokio::time::sleep;
use ton_api::ton::ton_node::blockcandidatestatus::BlockCandidateStatus;
use ton_api::ton::ton_node::Broadcast;
use ton_types::Result;

//TODO: traffic metrics & derivatives
//TODO: remove dependency from CatchainClient? (use private overlay directly)

/*
===============================================================================
    Constants
===============================================================================
*/

const MAX_NEIGHBOURS_COUNT: usize = 16; //max number of neighbours to synchronize
const NEIGHBOURS_ROTATE_MIN_PERIOD_MS: u64 = 60000; //min time for neighbours rotation
const NEIGHBOURS_ROTATE_MAX_PERIOD_MS: u64 = 120000; //max time for neighbours rotation
const USE_QUERIES_FOR_BLOCK_STATUS: bool = false; //use queries for block status delivery, otherwise use messages

/*
===============================================================================
    Workchain's private overlay listener
===============================================================================
*/

pub trait WorkchainOverlayListener: Send + Sync {
    /// Block status has been updated
    fn on_workchain_block_status_updated(
        &self,
        block_status: BlockCandidateStatus,
        is_master_chain_overlay: bool,
    ) -> Result<BlockPtr>;

    /// Process new block candidate broadcast
    fn on_workchain_block_candidate(&self, block_candidate: Arc<BlockCandidateBody>);
}

/*
===============================================================================
    Workchain's private overlay
===============================================================================
*/

type PrivateOverlayNetworkPtr = Weak<dyn PrivateOverlayOperations>;
type OverlayPtr = catchain::CatchainOverlayPtr;

pub struct WorkchainOverlay {
    engine: EnginePtr,                      //pointer to engine
    workchain_id: i32,                      //workchain identifier
    runtime_handle: tokio::runtime::Handle, //runtime handle for further interaction between threads
    local_adnl_id: PublicKeyHash,           //ADNL ID for this node
    network: PrivateOverlayNetworkPtr,      //private overlay network
    overlay: OverlayPtr,                    //private overlay
    overlay_short_id: Arc<OverlayShortId>,  //private overlay short ID
    overlay_id: UInt256,             //ID of validator list
    neighbours_adnl_ids: SpinMutex<Vec<PublicKeyHash>>, //ADNL IDs of neighbours
    active_validators_adnl_ids: Vec<PublicKeyHash>, //ADNL IDs of active validators
    listener: Arc<WorkchainListener>,       //linked listener
    node_debug_id: Arc<String>,             //debug ID of node
    _instance_counter: InstanceCounter,     //instance counter
    send_message_to_neighbours_counter: metrics_runtime::data::Counter, //number of send message to neighbours calls
    out_message_counter: metrics_runtime::data::Counter,                //number of outgoing messages
    in_message_counter: metrics_runtime::data::Counter,                 //number of incoming messages
    send_all_counter: metrics_runtime::data::Counter,                   //number of send to all active nodes calls
    out_broadcast_counter: metrics_runtime::data::Counter,              //number of outgoing broadcasts
    out_query_counter: ResultStatusCounter,                             //number of outgoing queries
    block_status_update_counter: metrics_runtime::data::Counter,        //number of block status updates
    is_master_chain_overlay: bool,                                      //is this overlay for communication with MC
}

impl WorkchainOverlay {
    /*
        Constructor
    */

    /// Create new overlay
    pub async fn create(
        workchain_id: i32,
        debug_id_prefix: String,
        overlay_id: UInt256,
        validators: &Vec<ValidatorDescr>,
        active_validators_count: usize,
        local_adnl_id: PublicKeyHash,
        listener: Weak<dyn WorkchainOverlayListener>,
        engine: &EnginePtr,
        runtime: tokio::runtime::Handle,
        metrics_receiver: Arc<metrics_runtime::Receiver>,
        overlays_instance_counter: Arc<InstanceCounter>,
        metrics_prefix: String,
        is_master_chain_overlay: bool,
    ) -> Result<Arc<Self>> {
        let overlay_short_id =
            PrivateOverlayShortId::from_data(overlay_id.as_slice().clone());
        let node_debug_id = Arc::new(format!("{}.{}", debug_id_prefix, overlay_short_id));

        log::debug!(target: "verificator", "Creating verification workchain's #{} private overlay - overlay_id={}, overlay={}", workchain_id, overlay_id.to_hex_string(), node_debug_id);

        let network = engine.validator_network();
        let nodes: Vec<CatchainNode> = validators
            .iter()
            .map(|desc| CatchainNode {
                adnl_id: get_adnl_id(&desc),
                public_key: sigpubkey_to_publickey(&desc.public_key),
            })
            .collect();

        let result = network.set_validator_list(overlay_id.clone(), &nodes).await?;

        assert!(result.is_some());

        let in_broadcast_counter = metrics_receiver.sink().counter(format!("{}_in_broadcasts", metrics_prefix));
        let in_query_counter = metrics_receiver.sink().counter(format!("{}_in_queries", metrics_prefix));
        let in_block_candidate_counter = metrics_receiver.sink().counter(format!("{}_in_block_candidates", metrics_prefix));
        let block_status_update_counter = metrics_receiver.sink().counter(format!("{}_block_status_updates", metrics_prefix));
        let out_message_counter = metrics_receiver.sink().counter(format!("{}_out_messages", metrics_prefix));
        let in_message_counter = metrics_receiver.sink().counter(format!("{}_in_messages", metrics_prefix));
        let out_query_counter = ResultStatusCounter::new(&metrics_receiver, &format!("{}_out_queries", metrics_prefix));

        let listener = Arc::new(WorkchainListener {
            workchain_id,
            listener,
            runtime_handle: runtime.clone(),
            node_debug_id: node_debug_id.clone(),
            in_broadcast_counter: in_broadcast_counter.clone(),
            in_query_counter: in_query_counter.clone(),
            in_message_counter: in_message_counter.clone(),
            in_block_candidate_counter: in_block_candidate_counter.clone(),
            block_status_update_counter: block_status_update_counter.clone(),
            is_master_chain_overlay,
        });
        let overlay_listener: Arc<dyn CatchainOverlayListener + Send + Sync> = listener.clone();
        let replay_listener: Arc<dyn CatchainOverlayLogReplayListener + Send + Sync> =
            listener.clone();

        let overlay = network.create_catchain_client(
            overlay_id.clone(),
            &overlay_short_id,
            &nodes,
            Arc::downgrade(&overlay_listener),
            Arc::downgrade(&replay_listener),
        )?;

        let active_validators_adnl_ids: Vec<PublicKeyHash> = nodes[0..active_validators_count].iter().map(|node| node.adnl_id.clone()).collect();

        let overlay = Self {
            node_debug_id,
            workchain_id,
            overlay,
            overlay_short_id,
            network: Arc::downgrade(&network.clone()),
            neighbours_adnl_ids: SpinMutex::new(Vec::new()),
            active_validators_adnl_ids,
            overlay_id,
            local_adnl_id,
            runtime_handle: runtime.clone(),
            engine: engine.clone(),
            listener,
            _instance_counter: (*overlays_instance_counter).clone(),
            send_message_to_neighbours_counter: metrics_receiver.sink().counter(format!("{}_send_message_to_neighbours_calls", metrics_prefix)),
            send_all_counter: metrics_receiver.sink().counter(format!("{}_send_all_calls", metrics_prefix)),
            out_broadcast_counter: metrics_receiver.sink().counter(format!("{}_out_broadcasts", metrics_prefix)),
            out_query_counter,
            out_message_counter,
            in_message_counter,
            block_status_update_counter,
            is_master_chain_overlay,
        };
        let overlay = Arc::new(overlay);

        log::debug!(target: "verificator", "Verification workchain's #{} overlay {} has been started", workchain_id, overlay.node_debug_id);

        Self::rotate_neighbours(Arc::downgrade(&overlay));

        Ok(overlay)
    }

    /*
        Sending API
    */

    /// Send message to neighbours
    pub fn send_message_to_private_neighbours(&self, data: BlockPayloadPtr) {
        let neighbours_adnl_ids = self.neighbours_adnl_ids.lock().clone();
        
        log::trace!(target: "verificator", "Sending multicast to {} neighbours (overlay={})", neighbours_adnl_ids.len(), self.node_debug_id);

        self.send_message_to_neighbours_counter.increment();

        self.send_message_multicast(&neighbours_adnl_ids, data);
    }

    /// Send message to all nodes
    pub fn send_all(&self, data: BlockPayloadPtr) {
        log::trace!(target: "verificator", "Sending multicast to all {} active nodes (overlay={})", self.active_validators_adnl_ids.len(), self.node_debug_id);

        self.send_all_counter.increment();

        self.send_message_multicast(&self.active_validators_adnl_ids, data);
    }

    /// Send message to other nodes
    fn send_message_multicast(&self, nodes: &Vec<PublicKeyHash>, data: BlockPayloadPtr) {
        log::trace!(target: "verificator", "Sending multicast {} bytes to {} nodes (overlay={})", data.data().len(), nodes.len(), self.node_debug_id);

        let received_from_workchain = !self.is_master_chain_overlay;

        for adnl_id_ref in nodes {
            if USE_QUERIES_FOR_BLOCK_STATUS {
                static BLOCK_STATUS_QUERY_TIMEOUT: Duration = Duration::from_millis(5000);

                let workchain_id = self.workchain_id;
                let adnl_id = adnl_id_ref.clone();
                let node_debug_id = self.node_debug_id.clone();
                let listener = self.listener.listener.clone();
                let out_query_counter = self.out_query_counter.clone();
                let block_status_update_counter = self.block_status_update_counter.clone();

                let response_callback = Box::new(move |result: Result<BlockPayloadPtr>| {
                    check_execution_time!(50_000);

                    log::trace!(target: "verificator", "Block status query response received from {} (overlay={})", &adnl_id, node_debug_id);

                    match result {
                        Ok(responsed_data) => {
                            out_query_counter.success();

                            if let Some(listener) = listener.upgrade() {
                                match WorkchainListener::process_serialized_block_status(workchain_id, &adnl_id, &responsed_data, &*listener, block_status_update_counter, received_from_workchain) {
                                    Ok(_block) => {
                                        /* do nothing */
                                    }
                                    Err(err) => {
                                        warn!(target: "verificator", "Block status response processing error (overlay={}): {:?}", node_debug_id, err);
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            out_query_counter.failure();
                            warn!(target: "verificator", "Invalid block status response (overlay={}): {:?}", node_debug_id, err);
                        }
                    }
                });

                self.out_query_counter.total_increment();

                self.overlay.send_query(
                    adnl_id_ref,
                    &self.local_adnl_id,
                    "block status",
                    BLOCK_STATUS_QUERY_TIMEOUT,
                    &data,
                    response_callback,
                );
            } else {
                self.out_message_counter.increment();

                self.overlay.send_message(adnl_id_ref, &self.local_adnl_id, &data);
            }
        }
    }

    /// Send broadcast
    pub fn send_broadcast(
        &self,
        sender_id: &PublicKeyHash,
        send_as: &PublicKeyHash,
        payload: BlockPayloadPtr,
    ) {
        self.overlay
            .send_broadcast_fec_ex(sender_id, send_as, payload);

        self.out_broadcast_counter.increment();
    }

    /*
        Neighbours management
    */

    /// Rotate neighbours
    fn rotate_neighbours(overlay_weak: Weak<WorkchainOverlay>) {
        let overlay = {
            if let Some(overlay) = overlay_weak.upgrade() {
                overlay
            } else {
                return;
            }
        };

        trace!(target: "verificator", "Rotate neighbours (overlay={})", overlay.node_debug_id);

        let mut rng = rand::thread_rng();

        //initiate next neighbours rotation

        let delay = Duration::from_millis(rng.gen_range(
            NEIGHBOURS_ROTATE_MIN_PERIOD_MS,
            NEIGHBOURS_ROTATE_MAX_PERIOD_MS + 1,
        ));
        let next_neighbours_rotate_time = SystemTime::now() + delay;
        let workchain_id = overlay.workchain_id;
        let node_debug_id = overlay.node_debug_id.clone();

        overlay.runtime_handle.spawn(async move {
            if let Ok(timeout) = next_neighbours_rotate_time.duration_since(SystemTime::now()) {
                trace!(
                    target: "verificator",
                    "Next neighbours rotation for workchain's #{} private overlay is scheduled at {} (in {:.3}s from now; overlay={})",
                    workchain_id,
                    catchain::utils::time_to_string(&next_neighbours_rotate_time),
                    timeout.as_secs_f64(),
                    node_debug_id);

                sleep(timeout).await;
            }

            //rotate neighbours

            Self::rotate_neighbours(overlay_weak);
        });

        //randomly choose max neighbours from sources

        let sources_count = overlay.active_validators_adnl_ids.len();
        let mut items_count = MAX_NEIGHBOURS_COUNT;
        let mut new_neighbours = Vec::with_capacity(items_count);

        trace!(
            target: "verificator",
            "...choose {} neighbours from {} sources (overlay={})",
            items_count,
            sources_count,
            overlay.node_debug_id,
        );

        if items_count > sources_count {
            items_count = sources_count;
        }

        let mut neighbours_string = "".to_string();
        let local_adnl_id = overlay.local_adnl_id.clone();

        for i in 0..sources_count {
            let random_value = rng.gen_range(0, sources_count - i);
            if random_value >= items_count {
                continue;
            }

            let adnl_id = overlay.active_validators_adnl_ids[i].clone();

            if adnl_id == local_adnl_id {
                continue;
            }

            new_neighbours.push(adnl_id.clone());
            items_count -= 1;

            if neighbours_string.len() > 0 {
                neighbours_string = format!("{}, ", neighbours_string);
            }

            neighbours_string = format!("{}{}", neighbours_string, adnl_id);
        }

        trace!(target: "verificator", "...new workchain's #{} private overlay {} neighbours are: [{:?}]", overlay.workchain_id, overlay.node_debug_id, neighbours_string);

        *overlay.neighbours_adnl_ids.lock() = new_neighbours;
    }
}

impl Drop for WorkchainOverlay {
    fn drop(&mut self) {
        info!(target: "verificator", "Dropping verification workchain's #{} private overlay (overlay={})", self.workchain_id, self.node_debug_id);

        if let Some(network) = self.network.upgrade() {
            debug!(target: "verificator", "Stopping verification workchain's #{} overlay {}", self.workchain_id, self.node_debug_id);
            network.stop_catchain_client(&self.overlay_short_id);
        }

        let _result = self.engine.remove_validator_list(self.overlay_id.clone());
    }
}

struct WorkchainListener {
    workchain_id: i32,                                           //workchain identifier
    node_debug_id: Arc<String>,                                  //node debug ID
    listener: Weak<dyn WorkchainOverlayListener>,                //listener for workchain overlay events
    runtime_handle: tokio::runtime::Handle,                      //runtime handle for further interaction between threads
    in_broadcast_counter: metrics_runtime::data::Counter,        //number of incoming broadcasts
    in_query_counter: metrics_runtime::data::Counter,            //number of incoming queries
    in_message_counter: metrics_runtime::data::Counter,          //number of incoming messages
    in_block_candidate_counter: metrics_runtime::data::Counter,  //number of incoming block candidates
    block_status_update_counter: metrics_runtime::data::Counter, //number of block status updates
    is_master_chain_overlay: bool,                               //is this overlay for communication with MC
}

impl Drop for WorkchainListener {
    fn drop(&mut self) {
        trace!(target: "verificator", "Dropping verification workchain's #{} private overlay listener (overlay={})", self.workchain_id, self.node_debug_id);
    }
}

impl CatchainOverlayLogReplayListener for WorkchainListener {
    fn on_time_changed(&self, _timestamp: std::time::SystemTime) { /* do nothing */
    }
}

impl CatchainOverlayListener for WorkchainListener {
    fn on_message(&self, adnl_id: PublicKeyHash, data: &BlockPayloadPtr) {
        check_execution_time!(20_000);

        trace!(target: "verificator", "WorkchainListener::on_message (overlay={})", self.node_debug_id);

        let _hang_checker = HangCheck::new(self.runtime_handle.clone(), format!("WorkchainListener::on_message: for workchain overlay {}", self.node_debug_id), Duration::from_millis(1000));

        self.in_message_counter.increment();

        let data = data.clone();
        let workchain_id = self.workchain_id;
        let node_debug_id = self.node_debug_id.clone();
        let listener = self.listener.clone();
        let block_status_update_counter = self.block_status_update_counter.clone();
        let received_from_workchain = !self.is_master_chain_overlay;

        self.runtime_handle.spawn(async move {
            if let Some(listener) = listener.upgrade() {
                log::trace!(target: "verificator", "WorkchainListener::on_message from {} (overlay={})", adnl_id, node_debug_id);

                match Self::process_serialized_block_status(workchain_id, &adnl_id, &data, &*listener, block_status_update_counter, received_from_workchain) {
                    Ok(_block) => {
                        //ignore reverse block candidate status message
                    }
                    Err(err) => {
                        let message = format!("WorkchainListener::on_message error (overlay={}): {:?}", node_debug_id, err);
                        warn!(target: "verificator", "{}", message);
                    }
                }
            } else {
                warn!(target: "verificator", "Query listener is not bound");
            }
        });
    }

    fn on_broadcast(&self, source_key_hash: PublicKeyHash, data: &BlockPayloadPtr) {
        trace!(target: "verificator", "WorkchainListener::on_broadcast from node with pubkeyhash={} (overlay={})", source_key_hash, self.node_debug_id);

        let _hang_checker = HangCheck::new(self.runtime_handle.clone(), format!("WorkchainListener::on_broadcast: for workchain overlay {}", self.node_debug_id), Duration::from_millis(1000));                

        self.in_broadcast_counter.increment();

        let data = data.clone();
        let workchain_id = self.workchain_id;
        let node_debug_id = self.node_debug_id.clone();
        let in_block_candidate_counter = self.in_block_candidate_counter.clone();

        if let Some(listener) = self.listener.upgrade() {
            self.runtime_handle.spawn(async move {
                match ton_api::Deserializer::new(&mut &data.data().0[..]).read_boxed::<ton_api::ton::TLObject>() {
                    Ok(broadcast) => {
                        let broadcast = match broadcast.downcast::<Broadcast>() {
                            Ok(broadcast) => {
                                match broadcast {
                                    Broadcast::TonNode_BlockCandidateBroadcast(broadcast) => {
                                        let block_candidate = Arc::new(BlockCandidateBody::new(broadcast));

                                        in_block_candidate_counter.increment();

                                        listener.on_workchain_block_candidate(block_candidate);
                                        return;
                                    }
                                    broadcast => {
                                        warn!(target: "verificator", "Unexpected broadcast subtype received in verification workchain's #{} private overlay {}: {:?}", workchain_id, node_debug_id, broadcast);
                                    }
                                }

                                return;
                            }
                            Err(broadcast) => broadcast
                        };

                        warn!(target: "verificator", "Unexpected broadcast received in verification workchain's #{} private overlay {}: {:?}", workchain_id, node_debug_id, broadcast);
                    }
                    Err(err) => {
                        warn!(target: "verificator", "Can't parse broadcast received from {} in verification workchain's #{} private overlay {}: {:?}: {:?}", source_key_hash, workchain_id, node_debug_id, data.data(), err);
                    }
                }
            });
        }
    }

    fn on_query(
        &self,
        adnl_id: PublicKeyHash,
        data: &BlockPayloadPtr,
        response_callback: ExternalQueryResponseCallback,
    ) {
        check_execution_time!(20_000);

        trace!(target: "verificator", "WorkchainListener::on_query (overlay={})", self.node_debug_id);

        let _hang_checker = HangCheck::new(self.runtime_handle.clone(), format!("WorkchainListener::on_query: for workchain overlay {}", self.node_debug_id), Duration::from_millis(1000));        

        self.in_query_counter.increment();

        let data = data.clone();
        let workchain_id = self.workchain_id;
        let node_debug_id = self.node_debug_id.clone();
        let listener = self.listener.clone();
        let block_status_update_counter = self.block_status_update_counter.clone();
        let received_from_workchain = !self.is_master_chain_overlay;

        self.runtime_handle.spawn(async move {
            if let Some(listener) = listener.upgrade() {
                log::trace!(target: "verificator", "WorkchainListener::on_query from {} (overlay={})", adnl_id, node_debug_id);

                match Self::process_serialized_block_status(workchain_id, &adnl_id, &data, &*listener, block_status_update_counter, received_from_workchain) {
                    Ok(block) => {
                        let serialized_response = block.lock().serialize();
                        response_callback(Ok(serialized_response));
                    }
                    Err(err) => {
                        let message = format!("WorkchainListener::on_query error (overlay={}): {:?}", node_debug_id, err);
                        warn!(target: "verificator", "{}", message);
                        response_callback(Err(failure::format_err!("{}", message)));
                    }
                }
            } else {
                warn!(target: "verificator", "Query listener is not bound");
                response_callback(Err(failure::format_err!("Query listener is not bound")));
            }
        });
    }
}

impl WorkchainListener {
    fn process_serialized_block_status(
        workchain_id: i32,
        adnl_id: &PublicKeyHash,
        data: &BlockPayloadPtr,
        listener: &dyn WorkchainOverlayListener,
        block_status_update_counter: metrics_runtime::data::Counter,
        received_from_workchain: bool,
    ) -> Result<BlockPtr> {
        check_execution_time!(50_000);

        match ton_api::Deserializer::new(&mut &data.data().0[..]).read_boxed::<ton_api::ton::TLObject>() {
            Ok(message) => {
                use ton_api::ton::ton_node::BlockCandidateStatus;

                let message = match message.downcast::<BlockCandidateStatus>() {
                    Ok(block_status) => {
                        let block_status = block_status.only();

                        block_status_update_counter.increment();

                        match listener.on_workchain_block_status_updated(block_status, received_from_workchain) {
                            Ok(block) => {
                                return Ok(block);
                            }
                            Err(err) => {
                                failure::bail!("Can't process message received from {} in verification workchain's #{} private overlay: {:?}", adnl_id, workchain_id, err);
                            }
                        }
                    }
                    Err(message) => message
                };

                failure::bail!("Unexpected message received in verification workchain's #{} private overlay: {:?}", workchain_id, message);
            }
            Err(err) => {
                failure::bail!("Can't parse message received from {} in verification workchain's #{} private overlay: {:?}: {:?}", adnl_id, workchain_id, data.data(), err);
            }
        }
    }
}
