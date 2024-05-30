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

use std::sync::atomic::AtomicUsize;
use std::time::SystemTime;
//pub use multi_signature_unsafe::MultiSignature;
pub use multi_signature_bls::MultiSignature;
use super::*;
use catchain::BlockPayloadPtr;
use log::*;
use catchain::profiling::InstanceCounter;
use catchain::check_execution_time;
use spin::mutex::SpinMutex;
use ton_api::ton::ton_node::blockcandidatestatus::BlockCandidateStatus;
use ton_api::ton::ton_node::broadcast::BlockCandidateBroadcast;
use ton_api::IntoBoxed;
use ever_block::Result;
use validator_session::ValidatorWeight;
use catchain::serialize_tl_boxed_object;
use rand::Rng;

/*
===============================================================================
    Block
===============================================================================
*/

#[derive(Debug)]
pub struct BlockCandidateBody {
    candidate: BlockCandidateBroadcast,    //candidate
    serialized_candidate: BlockPayloadPtr, //serialized candidate
    hash: UInt256,                         //hash of this candiate
}

impl BlockCandidateBody {
    /// Access to broadcast
    pub fn candidate(&self) -> &BlockCandidateBroadcast {
        &self.candidate
    }

    /// Access to serialized broadcast
    pub fn serialized_candidate(&self) -> &BlockPayloadPtr {
        &self.serialized_candidate
    }

    /// Constructor
    pub fn new(candidate: BlockCandidateBroadcast) -> Self {
        //todo: optimize clone
        let serialized_candidate = catchain::CatchainFactory::create_block_payload(
            serialize_tl_boxed_object!(&candidate.clone().into_boxed()),
        );

        Self {
            serialized_candidate: serialized_candidate.clone(),
            candidate,
            hash: catchain::utils::get_hash_from_block_payload(&serialized_candidate),
        }
    }
}

#[derive(Clone)]
struct NodeDeliveryDesc {
    delivery_weight: ValidatorWeight,                         //delivery weight
    last_status_sent_time: Option<std::time::SystemTime>,     //last status time
    last_status_received_time: Option<std::time::SystemTime>, //last received time
}
#[derive(Default)]
pub struct BlockDeliveryStats {
    pub in_candidates_count: AtomicUsize,         //incoming candidates count
    pub in_wc_merges_count: AtomicUsize,          //incoming WC updates
    pub in_mc_merges_count: AtomicUsize,          //incoming MC updates
    pub out_wc_sends_count: AtomicUsize,          //outgoing WC updates
    pub out_mc_syncs_count: AtomicUsize,          //outgoing MC syncs
    pub out_mc_sends_count: AtomicUsize,          //outgoing MC sends
    pub forwarding_neighbours_sends: AtomicUsize, //forwarding neighbours sends
    pub far_neighbours_sends: AtomicUsize,        //far neighbours sends
    pub syncs_count: AtomicUsize,                 //synchronizations count
}

pub struct Block {
    block_candidate: Option<Arc<BlockCandidateBody>>, //block candidate
    serialized_block_status: Option<BlockPayloadPtr>, //serialized status
    candidate_id: UInt256,                            //block ID
    deliveries_signature: MultiSignature,             //signature for deliveries
    approvals_signature: MultiSignature,              //signature for approvals
    rejections_signature: MultiSignature,             //signature for rejections
    signatures_hash: u32,                             //hash for signatures
    created_timestamp: Option<i64>,                   //time of block creation
    first_appearance_time: std::time::SystemTime,     //time of first block appearance in a network
    delivery_state_change_time: Option<std::time::SystemTime>, //time when block is delivered
    merges_count: u32,                                //merges count
    initially_mc_processed: bool,                     //was this block process in MC
    received_from_mc: bool,                           //was this block received from MC overlay
    sent_to_mc: bool,                                 //was this block sent to MC because of cutoff weight of delivery signatures
    ready_for_send: bool,                             //is this block ready for sending
    _instance_counter: InstanceCounter,               //instance counter
    first_external_request_time: Option<std::time::SystemTime>, //time of first external request
    node_delivery_descs: Vec<NodeDeliveryDesc>,       //delivery info for node
    delivery_stats: BlockDeliveryStats,               //delivery stats
}

pub type BlockPtr = Arc<SpinMutex<Block>>;

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Block[candidate_id={:?}, deliveries={:?}, approvals={:?}, rejections={:?}, signatures_hash={}]", self.candidate_id, self.deliveries_signature, self.approvals_signature, self.rejections_signature, self.signatures_hash)
    }
}

impl std::fmt::Debug for BlockDeliveryStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[in_candidates={}, in_wc_merges={}, out_wc_sends={}, out_mc_syncs={}, out_mc_sends={}, in_mc_merges={}, neighbours_sends={}/{}, syncs={}]",
            self.in_candidates_count.load(std::sync::atomic::Ordering::Relaxed),
            self.in_wc_merges_count.load(std::sync::atomic::Ordering::Relaxed),
            self.out_wc_sends_count.load(std::sync::atomic::Ordering::Relaxed),
            self.out_mc_syncs_count.load(std::sync::atomic::Ordering::Relaxed),
            self.out_mc_sends_count.load(std::sync::atomic::Ordering::Relaxed),
            self.in_mc_merges_count.load(std::sync::atomic::Ordering::Relaxed),
            self.forwarding_neighbours_sends.load(std::sync::atomic::Ordering::Relaxed),
            self.far_neighbours_sends.load(std::sync::atomic::Ordering::Relaxed),
            self.syncs_count.load(std::sync::atomic::Ordering::Relaxed))
    }
}

impl Block {
    /// Candidate ID
    pub fn get_id(&self) -> &UInt256 {
        &self.candidate_id
    }

    /// Is block delivered
    pub fn is_delivered(
        &self,
        validators: &Vec<ValidatorDescr>,
        cutoff_weight: ValidatorWeight,
    ) -> bool {
        let delivered_weight = self.deliveries_signature.get_total_weight(validators);

        delivered_weight >= cutoff_weight
    }

    /// Is block rejected
    pub fn is_rejected(&self) -> bool {
        !self.rejections_signature.empty()
    }

    /// Does block have approves
    pub fn has_approves(&self) -> bool {
        !self.approvals_signature.empty()
    }

    /// Block first appearance time
    pub fn get_first_appearance_time(&self) -> &std::time::SystemTime {
        &self.first_appearance_time
    }

    /// Block creation time
    pub fn get_creation_time(&self) -> Option<std::time::SystemTime> {
        match self.created_timestamp {
            Some(value) => Some(std::time::UNIX_EPOCH + std::time::Duration::from_millis(value as u64)),
            None => None
        }
    }

    /// Get block delivery latency
    pub fn get_delivery_latency(&self) -> Option<std::time::Duration> {
        match self.get_creation_time() {
            Some(value) => match value.elapsed() {
                Ok(elapsed) => Some(elapsed),
                Err(_) => None,
            },
            None => None,
        }
    }

    /// Set block creation time
    pub fn set_creation_timestamp(&mut self, created_timestamp: i64) {
        if created_timestamp == 0 {
            return;
        }

        match self.created_timestamp {
            None => self.created_timestamp = Some(created_timestamp),
            Some(value) => {
                if value > created_timestamp {
                    self.created_timestamp = Some(created_timestamp);
                }
            }
        }
    }

    /// Get first external request time
    pub fn get_first_external_request_time(&self) -> &Option<std::time::SystemTime> {
        &self.first_external_request_time
    }

    /// Set first external request time
    pub fn set_first_external_request_time(&mut self, request_time: &std::time::SystemTime) {
        if let Some(first_external_request_time) = self.first_external_request_time {
            if first_external_request_time < *request_time {
                return;
            }
        }

        self.first_external_request_time = Some(*request_time);
    }

    /// Get time when block becomes delivered
    pub fn get_delivery_state_change_time(&self) -> &Option<std::time::SystemTime> {
        &self.delivery_state_change_time
    }

    /// Set delivery state change time
    pub fn set_delivery_state_change_time(&mut self, time: &std::time::SystemTime) {
        if let Some(delivery_state_change_time) = self.delivery_state_change_time {
            if delivery_state_change_time < *time {
                return;
            }
        }

        self.delivery_state_change_time = Some(*time);
    }

    /// Delivery stats
    pub fn get_delivery_stats(&self) -> &BlockDeliveryStats {
        &self.delivery_stats
    }

    /// Get nodes where block is undelivered for further delivery
    pub fn calc_low_delivery_nodes_indexes(&self, max_nodes_count: usize, cutoff_weight: ValidatorWeight, local_idx: usize, max_last_sent_time: SystemTime) -> Vec<usize> {
        check_execution_time!(1_000);

        let mut result = Vec::with_capacity(self.node_delivery_descs.len());

        for (idx, desc) in self.node_delivery_descs.iter().enumerate() {
            if desc.delivery_weight >= cutoff_weight {
                continue;
            }
            
            if desc.last_status_sent_time.unwrap_or(SystemTime::UNIX_EPOCH) >= max_last_sent_time {
                continue;
            }

            if idx == local_idx {
                continue;
            }

            result.push(idx);
        }

        if result.len() <= max_nodes_count {
            return result;
        }

        //shuffle result

        let mut shuffled_result = Vec::with_capacity(max_nodes_count);
        let mut rng = rand::thread_rng();

        for _i in 0..max_nodes_count {
            let idx = rng.gen_range(0, result.len());
            shuffled_result.push(result[idx]);
            result.remove(idx);
        }

        shuffled_result
    }

    /// Update node status sent time
    pub fn set_node_status_sent_time(&mut self, source_node_idx: usize, time: SystemTime) {
        if source_node_idx >= self.node_delivery_descs.len() {
            return;
        }

        self.node_delivery_descs[source_node_idx].last_status_sent_time = Some(time);
    }

    /// Update node status received time
    pub fn set_node_status_received_time(&mut self, source_node_idx: usize, time: SystemTime) {
        if source_node_idx >= self.node_delivery_descs.len() {
            return;
        }

        self.node_delivery_descs[source_node_idx].last_status_received_time = Some(time);
    }

    /// Set node's delivery weight
    pub fn set_node_delivery_weight(&mut self, source_node_idx: usize, delivery_weight: ValidatorWeight) {
        if source_node_idx >= self.node_delivery_descs.len() {
            return;
        }

        let node_delivery_weight = &mut self.node_delivery_descs[source_node_idx];

        if node_delivery_weight.delivery_weight < delivery_weight {
            node_delivery_weight.delivery_weight = delivery_weight;
        }
    }

    /// Set MC processed status
    pub fn mark_as_mc_processed(&mut self) {
        self.initially_mc_processed = true;
    }

    /// Get MC processed status
    pub fn was_mc_processed(&self) -> bool {
        self.initially_mc_processed
    }

    /// Set sent to MC status
    pub fn mark_as_sent_to_mc(&mut self) {
        self.sent_to_mc = true;
    }

    /// Get sent to MC status
    pub fn is_sent_to_mc(&self) -> bool {
        self.sent_to_mc
    }    

    /// Set received from MC flag
    pub fn mark_as_received_from_mc(&mut self) {
        self.received_from_mc = true;
    }

    /// Is this block received from MC
    pub fn is_received_from_mc(&self) -> bool {
        self.received_from_mc
    }

    /// Set ready for sending flag
    pub fn toggle_send_ready(&mut self, new_state: bool) -> bool {
        let prev_state = self.ready_for_send;

        self.ready_for_send = new_state;

        prev_state
    }

    /// Block ID
    pub fn get_id_ext(&self) -> Option<BlockIdExt> {
        match &self.block_candidate {
            Some(candidate) => Some(candidate.candidate().id.clone()),
            None => None
        }
    }

    /// Delivery signature
    pub fn get_deliveries_signature(&self) -> &MultiSignature {
        &self.deliveries_signature
    }

    /// Approvals signature
    pub fn get_approvals_signature(&self) -> &MultiSignature {
        &self.approvals_signature
    }

    pub fn get_rejections_signature(&self) -> &MultiSignature {
        &self.rejections_signature
    }

    /// Get status
    pub fn status(&self) -> BlockCandidateStatus {
        BlockCandidateStatus {
            candidate_id: self.candidate_id.clone(),
            deliveries_signature: self.deliveries_signature.serialize().into(),
            approvals_signature: self.approvals_signature.serialize().into(),
            rejections_signature: self.rejections_signature.serialize().into(),
            merges_cnt: (self.merges_count + 1) as i32, //increase number of merges before send
            created_timestamp: match self.created_timestamp { Some (value) => value, None => 0 },
        }
    }

    /// Serialized TON block
    pub fn serialize(&mut self) -> BlockPayloadPtr {
        if let Some(serialized_block_status) = &self.serialized_block_status {
            return serialized_block_status.clone();
        }

        let ever_block = self.status().into_boxed();
        let serialized_block_status = serialize_tl_boxed_object!(&ever_block);
        let serialized_block_status =
            catchain::CatchainFactory::create_block_payload(serialized_block_status);

        self.serialized_block_status = Some(serialized_block_status.clone());

        serialized_block_status
    }

    /// Update status of the block (returns true, if the block should be updated in the network)
    pub fn set_status(&mut self, local_key: &PrivateKey, local_idx: u16, nodes_count: u16, status: Option<bool>) -> Result<bool> {
        let prev_hash = self.get_signatures_hash();

        let mut new_deliveries_signature = self.deliveries_signature.clone();
        let mut new_approvals_signature = self.approvals_signature.clone();
        let mut new_rejections_signature = self.rejections_signature.clone();

        if let Some(status) = status {
            let signature = if status {
                &mut new_approvals_signature
            } else {
                &mut new_rejections_signature
            };

            signature.sign(local_key, local_idx, nodes_count)?;
        }

        new_deliveries_signature.sign(local_key, local_idx, nodes_count)?;

        let new_hash = Self::compute_hash(
            &new_deliveries_signature,
            &new_approvals_signature,
            &new_rejections_signature);

        if new_hash != prev_hash { //prevent duplicate merges
            self.deliveries_signature = new_deliveries_signature;
            self.approvals_signature = new_approvals_signature;
            self.rejections_signature = new_rejections_signature;

            self.signatures_hash = new_hash;
        }

        Ok(new_hash != prev_hash)
    }

    /// Get number of merges
    pub fn get_merges_count(&self) -> u32 {
        self.merges_count
    }

    /// Merge status from another block
    pub fn merge_status(
        &mut self,
        deliveries_signature: &MultiSignature,
        approvals_signature: &MultiSignature,
        rejections_signature: &MultiSignature,
        merges_count: u32,
        created_timestamp: i64,
    ) -> Result<bool> {
        let prev_hash = self.get_signatures_hash();
        let mut new_deliveries_signature = self.deliveries_signature.clone();
        let mut new_approvals_signature = self.approvals_signature.clone();
        let mut new_rejections_signature = self.rejections_signature.clone();

        new_deliveries_signature.merge(deliveries_signature)?;
        new_approvals_signature.merge(approvals_signature)?;
        new_rejections_signature.merge(rejections_signature)?;

        let new_hash = Self::compute_hash(
            &new_deliveries_signature,
            &new_approvals_signature,
            &new_rejections_signature);

        if new_hash != prev_hash { //prevent duplicate merges
            self.deliveries_signature = new_deliveries_signature;
            self.approvals_signature = new_approvals_signature;
            self.rejections_signature = new_rejections_signature;

            if self.merges_count < merges_count {
                self.merges_count = merges_count; //does not change hash
            }

            self.signatures_hash = new_hash;
        }

        self.set_creation_timestamp(created_timestamp);        

        Ok(new_hash != prev_hash)
    }

    /// Get signatures hash
    pub(crate) fn get_signatures_hash(&self) -> u32 { //TODO: remove pub(crate)
        self.signatures_hash
    }

    /// Compute hash
    fn compute_hash(
        deliveries_signature: &MultiSignature,
        approvals_signature: &MultiSignature,
        rejections_signature: &MultiSignature,
    ) -> u32 {
        deliveries_signature.get_hash() ^ 
        approvals_signature.get_hash() ^
        rejections_signature.get_hash()
    }

    /// Update hash
    fn update_hash(&mut self) {
        let prev_hash = self.signatures_hash;

        self.signatures_hash = Self::compute_hash(
            &self.deliveries_signature,
            &self.approvals_signature,
            &self.rejections_signature);

        if self.signatures_hash != prev_hash {
            self.serialized_block_status = None;
        }
    }

    /// Update candidate (for later candidate body receivements)
    pub fn update_block_candidate(&mut self, new_block_candidate: Arc<BlockCandidateBody>) -> bool {
        if self.block_candidate.is_none() {
            self.created_timestamp = Some(new_block_candidate.candidate.created_timestamp);
            self.block_candidate = Some(new_block_candidate);
            return true;
        }

        let cur_block_candidate = self.block_candidate.as_ref().unwrap();
        if &new_block_candidate.hash != &cur_block_candidate.hash {
            warn!(target: "verificator", "Attempt to update block candidate {:?} body with a new data: prev={:?}, new={:?}", self.candidate_id, cur_block_candidate.hash, new_block_candidate.hash);
            return false;
        }

        self.set_creation_timestamp(new_block_candidate.candidate.created_timestamp);

        false
    }

    /// Create new block
    pub fn create(
        candidate_id: UInt256,
        instance_counter: &InstanceCounter,
        nodes_count: usize,
    ) -> Arc<SpinMutex<Self>> {
        let mut body = Self {
            candidate_id: candidate_id.clone(),
            block_candidate: None,
            deliveries_signature: MultiSignature::new(1, candidate_id.clone()),
            approvals_signature: MultiSignature::new(2, candidate_id.clone()),
            rejections_signature: MultiSignature::new(3, candidate_id.clone()),
            signatures_hash: 0,
            serialized_block_status: None,
            created_timestamp: None,
            merges_count: 0,
            initially_mc_processed: false,
            received_from_mc: false,
            sent_to_mc: false,
            ready_for_send: false,
            first_appearance_time: std::time::SystemTime::now(),
            _instance_counter: instance_counter.clone(),
            first_external_request_time: None,
            delivery_state_change_time: None,
            node_delivery_descs: vec![NodeDeliveryDesc {
                delivery_weight: 0,
                last_status_sent_time: None,
                last_status_received_time: None,
            }; nodes_count],
            delivery_stats: BlockDeliveryStats::default(),
        };

        body.update_hash();

        Arc::new(SpinMutex::new(body))
    }
}
