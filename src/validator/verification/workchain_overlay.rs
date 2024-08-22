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
use super::utils::get_adnl_id;
use crate::engine_traits::PrivateOverlayOperations;
use crate::validator::validator_utils::sigpubkey_to_publickey;
use catchain::utils::MetricsHandle;
use metrics::Recorder;
use catchain::BlockPayloadPtr;
use catchain::CatchainNode;
use catchain::CatchainOverlayListener;
use catchain::CatchainOverlayLogReplayListener;
use catchain::ExternalQueryResponseCallback;
use catchain::PublicKeyHash;
use catchain::profiling::ResultStatusCounter;
use catchain::profiling::InstanceCounter;
use catchain::check_execution_time;
use log::*;
use adnl::{OverlayShortId, PrivateOverlayShortId};
use std::time::Duration;
use ton_api::ton::ton_node::blockcandidatestatus::BlockCandidateStatus;
use ton_api::ton::ton_node::Broadcast;
use ever_block::{error, fail, Result};

//TODO: remove dependency from CatchainClient? (use private overlay directly)

/*
===============================================================================
    Constants
===============================================================================
*/

const USE_QUERIES_FOR_BLOCK_STATUS: bool = false; //use queries for block status delivery, otherwise use messages
const MAX_BROADCAST_HOPS: usize = 3; //max number of hops for broadcast

/*
===============================================================================
    Workchain's private overlay listener
===============================================================================
*/

pub trait WorkchainOverlayListener: Send + Sync {
    /// Block status has been updated
    fn on_workchain_block_status_updated(
        &self,
        adnl_id: &PublicKeyHash,
        block_status: BlockCandidateStatus,
        received_from_workchain: bool,
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
    _runtime_handle: tokio::runtime::Handle, //runtime handle for further interaction between threads
    local_adnl_id: PublicKeyHash,           //ADNL ID for this node
    network: PrivateOverlayNetworkPtr,      //private overlay network
    overlay: OverlayPtr,                    //private overlay
    overlay_short_id: Arc<OverlayShortId>,  //private overlay short ID
    overlay_id: UInt256,             //ID of validator list
    validators_adnl_ids: Vec<PublicKeyHash>, //ADNL IDs of active validators
    listener: Arc<WorkchainListener>,       //linked listener
    node_debug_id: Arc<String>,             //debug ID of node
    _instance_counter: InstanceCounter,     //instance counter
    send_message_to_neighbours_counter: metrics::Counter, //number of send message to private neighbours calls
    send_message_to_far_neighbours_counter: metrics::Counter, //number of send message to far neighbours calls
    send_all_counter: metrics::Counter,                   //number of send to all active nodes calls
    out_broadcast_counter: metrics::Counter,              //number of outgoing broadcasts
    out_broadcast_size_counter: metrics::Counter,         //number of outgoing broadcasts size
    out_query_counter: ResultStatusCounter,               //number of outgoing queries
    out_query_size_counter: metrics::Counter,             //number of outgoing queries size
    out_message_counter: metrics::Counter,                //number of outgoing messages
    out_message_size_counter: metrics::Counter,           //number of outgoing messages
    block_status_update_counter: metrics::Counter,        //number of block status updates
    is_master_chain_overlay: bool,                        //is this overlay for communication with MC
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
        local_adnl_id: PublicKeyHash,
        listener: Weak<dyn WorkchainOverlayListener>,
        engine: &EnginePtr,
        runtime: tokio::runtime::Handle,
        metrics_receiver: MetricsHandle,
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

        if result.is_none() {
            fail!("Can't resolve ADNL IDs for overlay_id={}", overlay_id.to_hex_string());
        }

        let in_broadcast_counter = metrics_receiver.sink().register_counter(&format!("{}_in_broadcasts", metrics_prefix).into());
        let in_broadcast_size_counter = metrics_receiver.sink().register_counter(&format!("{}_in_broadcasts_size", metrics_prefix).into());
        let out_broadcast_counter = metrics_receiver.sink().register_counter(&format!("{}_out_broadcasts", metrics_prefix).into());
        let out_broadcast_size_counter = metrics_receiver.sink().register_counter(&format!("{}_out_broadcasts_size", metrics_prefix).into());
        let in_query_counter = metrics_receiver.sink().register_counter(&format!("{}_in_queries", metrics_prefix).into());
        let in_query_size_counter = metrics_receiver.sink().register_counter(&format!("{}_in_queries_size", metrics_prefix).into());
        let out_query_counter = ResultStatusCounter::new(&metrics_receiver, &format!("{}_out_queries", metrics_prefix));
        let out_query_size_counter = metrics_receiver.sink().register_counter(&format!("{}_out_queries_size", metrics_prefix).into());
        let in_message_counter = metrics_receiver.sink().register_counter(&format!("{}_in_messages", metrics_prefix).into());
        let in_message_size_counter = metrics_receiver.sink().register_counter(&format!("{}_in_messages_size", metrics_prefix).into());
        let out_message_counter = metrics_receiver.sink().register_counter(&format!("{}_out_messages", metrics_prefix).into());
        let out_message_size_counter = metrics_receiver.sink().register_counter(&format!("{}_out_message_size", metrics_prefix).into());
        let in_block_candidate_counter = metrics_receiver.sink().register_counter(&format!("{}_in_block_candidates", metrics_prefix).into());
        let block_status_update_counter = metrics_receiver.sink().register_counter(&format!("{}_block_status_updates", metrics_prefix).into());

        let listener = Arc::new(WorkchainListener {
            workchain_id,
            listener,
            runtime_handle: runtime.clone(),
            node_debug_id: node_debug_id.clone(),
            in_broadcast_counter,
            in_query_counter,
            in_message_counter,
            in_broadcast_size_counter,
            in_query_size_counter,
            in_message_size_counter,
            in_block_candidate_counter,
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
            Some(MAX_BROADCAST_HOPS),
        )?;

        let validators_adnl_ids: Vec<PublicKeyHash> = nodes.iter().map(|node| node.adnl_id.clone()).collect();

        let overlay = Self {
            node_debug_id,
            workchain_id,
            overlay,
            overlay_short_id,
            network: Arc::downgrade(&network.clone()),
            validators_adnl_ids,
            overlay_id,
            local_adnl_id,
            _runtime_handle: runtime.clone(),
            engine: engine.clone(),
            listener,
            _instance_counter: (*overlays_instance_counter).clone(),
            send_message_to_neighbours_counter: metrics_receiver.sink().register_counter(&format!("{}_send_message_to_neighbours_calls", metrics_prefix).into()),
            send_message_to_far_neighbours_counter: metrics_receiver.sink().register_counter(&format!("{}_send_message_to_far_neighbours_calls", metrics_prefix).into()),
            send_all_counter: metrics_receiver.sink().register_counter(&format!("{}_send_all_calls", metrics_prefix).into()),
            out_broadcast_counter,
            out_broadcast_size_counter,
            out_query_counter,
            out_query_size_counter,
            out_message_counter,
            out_message_size_counter,
            block_status_update_counter,
            is_master_chain_overlay,
        };
        let overlay = Arc::new(overlay);

        log::debug!(target: "verificator", "Verification workchain's #{} overlay {} has been started", workchain_id, overlay.node_debug_id);

        Ok(overlay)
    }

    /*
        Common API
    */

    /// Nodes count
    pub fn get_nodes_count(&self) -> usize {
        self.validators_adnl_ids.len()
    }

    /*
        Sending API
    */

    /// Send message to neighbours
    pub fn send_message_to_forwarding_neighbours(&self, data: BlockPayloadPtr, neighbours: &Vec<usize>) {
        log::trace!(target: "verificator", "Sending multicast to {} forwarding neighbours (overlay={})", neighbours.len(), self.node_debug_id);

        self.send_message_to_neighbours_counter.increment(1);

        self.send_message_to_validators(data, neighbours);
    }

    /// Send message to custom neighbours
    pub fn send_message_to_far_neighbours(&self, data: BlockPayloadPtr, neighbours: &Vec<usize>) {
        log::trace!(target: "verificator", "Sending multicast to {} far neighbours (overlay={})", neighbours.len(), self.node_debug_id);

        self.send_message_to_far_neighbours_counter.increment(1);

        self.send_message_to_validators(data, neighbours);
    }

    /// Send message to validators
    pub fn send_message_to_validators(&self, data: BlockPayloadPtr, validators: &Vec<usize>) {
        let mut adnl_ids = Vec::with_capacity(validators.len());

        for idx in validators {
            if *idx >= self.validators_adnl_ids.len() {
                continue;
            }

            let adnl_id = self.validators_adnl_ids[*idx].clone();

            if adnl_id == self.local_adnl_id {
                continue;
            }

            adnl_ids.push(adnl_id.clone());
        }

        self.send_message_multicast(&adnl_ids, data);
    }    

    /// Send message to all nodes
    pub fn send_all(&self, data: BlockPayloadPtr, first_idx: usize, count: usize) {
        log::trace!(target: "verificator", "Sending multicast to nodes [{};{}) (overlay={})", first_idx, first_idx + count, self.node_debug_id);

        self.send_all_counter.increment(1);

        let nodes_slice = &self.validators_adnl_ids[first_idx..first_idx + count];

        self.send_message_multicast(nodes_slice, data);
    }

    /// Send message to other nodes
    fn send_message_multicast(&self, nodes: &[PublicKeyHash], data: BlockPayloadPtr) {
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
                self.out_query_size_counter.increment(data.data().len() as u64);

                self.overlay.send_query(
                    adnl_id_ref,
                    &self.local_adnl_id,
                    "block status",
                    BLOCK_STATUS_QUERY_TIMEOUT,
                    &data,
                    response_callback,
                );
            } else {
                self.out_message_counter.increment(1);
                self.out_message_size_counter.increment(data.data().len() as u64);

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
        self.out_broadcast_counter.increment(1);
        self.out_broadcast_size_counter.increment(payload.data().len() as u64);

        self.overlay
            .send_broadcast_fec_ex(sender_id, send_as, payload);
    }
}

impl Drop for WorkchainOverlay {
    fn drop(&mut self) {
        info!(target: "verificator", "Dropping verification workchain's #{} private overlay (overlay={})", self.workchain_id, self.node_debug_id);

        if let Some(network) = self.network.upgrade() {
            debug!(target: "verificator", "Stopping verification workchain's #{} overlay {}", self.workchain_id, self.node_debug_id);
            network.stop_catchain_client(&self.overlay_short_id);
        }

        if let Err(e) = self.engine.remove_validator_list(self.overlay_id.clone()) {
            log::warn!(target: "verificator", "Error when removing validator list: {}", e)
        }
    }
}

struct WorkchainListener {
    workchain_id: i32,                             //workchain identifier
    node_debug_id: Arc<String>,                    //node debug ID
    listener: Weak<dyn WorkchainOverlayListener>,  //listener for workchain overlay events
    runtime_handle: tokio::runtime::Handle,        //runtime handle for further interaction between threads
    in_broadcast_counter: metrics::Counter,        //number of incoming broadcasts
    in_query_counter: metrics::Counter,            //number of incoming queries
    in_message_counter: metrics::Counter,          //number of incoming messages
    in_broadcast_size_counter: metrics::Counter,   //number of incoming broadcasts bytes
    in_query_size_counter: metrics::Counter,       //number of incoming queries bytes
    in_message_size_counter: metrics::Counter,     //number of incoming messages bytes
    in_block_candidate_counter: metrics::Counter,  //number of incoming block candidates
    block_status_update_counter: metrics::Counter, //number of block status updates
    is_master_chain_overlay: bool,                 //is this overlay for communication with MC
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

        self.in_message_counter.increment(1);
        self.in_message_size_counter.increment(data.data().len() as u64);

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

        self.in_broadcast_counter.increment(1);
        self.in_broadcast_size_counter.increment(data.data().len() as u64);

        let data = data.clone();
        let workchain_id = self.workchain_id;
        let node_debug_id = self.node_debug_id.clone();
        let in_block_candidate_counter = self.in_block_candidate_counter.clone();

        if let Some(listener) = self.listener.upgrade() {
            self.runtime_handle.spawn(async move {
                match ton_api::Deserializer::new(&mut &data.data()[..]).read_boxed::<ton_api::ton::TLObject>() {
                    Ok(broadcast) => {
                        let broadcast = match broadcast.downcast::<Broadcast>() {
                            Ok(broadcast) => {
                                match broadcast {
                                    Broadcast::TonNode_BlockCandidateBroadcast(broadcast) => {
                                        let block_candidate = Arc::new(BlockCandidateBody::new(broadcast));

                                        in_block_candidate_counter.increment(1);

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

        self.in_query_counter.increment(1);
        self.in_query_size_counter.increment(data.data().len() as u64);

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
                        response_callback(Err(error!("{}", message)));
                    }
                }
            } else {
                warn!(target: "verificator", "Query listener is not bound");
                response_callback(Err(error!("Query listener is not bound")));
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
        block_status_update_counter: metrics::Counter,
        received_from_workchain: bool,
    ) -> Result<BlockPtr> {
        check_execution_time!(50_000);

        match ton_api::Deserializer::new(&mut &data.data()[..]).read_boxed::<ton_api::ton::TLObject>() {
            Ok(message) => {
                use ton_api::ton::ton_node::BlockCandidateStatus;

                let message = match message.downcast::<BlockCandidateStatus>() {
                    Ok(block_status) => {
                        let block_status = block_status.only();

                        block_status_update_counter.increment(1);

                        match listener.on_workchain_block_status_updated(adnl_id, block_status, received_from_workchain) {
                            Ok(block) => {
                                return Ok(block);
                            }
                            Err(err) => {
                                fail!("Can't process message received from {} in verification workchain's #{} private overlay: {:?}", adnl_id, workchain_id, err);
                            }
                        }
                    }
                    Err(message) => message
                };

                fail!("Unexpected message received in verification workchain's #{} private overlay: {:?}", workchain_id, message);
            }
            Err(err) => {
                fail!("Can't parse message received from {} in verification workchain's #{} private overlay: {:?}: {:?}", adnl_id, workchain_id, data.data(), err);
            }
        }
    }
}
