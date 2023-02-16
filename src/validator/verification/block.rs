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

//pub use multi_signature_unsafe::MultiSignature;
pub use multi_signature_bls::MultiSignature;
use super::*;
use catchain::BlockPayloadPtr;
use log::*;
use catchain::profiling::InstanceCounter;
use spin::mutex::SpinMutex;
use ton_api::ton::ton_node::blockcandidatestatus::BlockCandidateStatus;
use ton_api::ton::ton_node::broadcast::BlockCandidateBroadcast;
use ton_api::IntoBoxed;
use ton_types::Result;
use validator_session::ValidatorWeight;

//TODO: merges count

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
            catchain::utils::serialize_tl_boxed_object!(&candidate.clone().into_boxed()),
        );

        Self {
            serialized_candidate: serialized_candidate.clone(),
            candidate,
            hash: catchain::utils::get_hash_from_block_payload(&serialized_candidate),
        }
    }
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
    merges_count: u32,                                //merges count
    initially_mc_processed: bool,                     //was this block process in MC
    mc_originated: bool,                              //was this block appeared from MC
    mc_delivered: bool,                               //was this block delivered to MC because of cutoff weight of delivery signatures
    ready_for_send: bool,                             //is this block ready for sending
    _instance_counter: InstanceCounter,               //instance counter
}

pub type BlockPtr = Arc<SpinMutex<Block>>;

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Block[candidate_id={}, deliveries={:?}, approvals={:?}, rejections={:?}, signatures_hash={}]", self.candidate_id, self.deliveries_signature, self.approvals_signature, self.rejections_signature, self.signatures_hash)
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

    /// Set MC processed status
    pub fn mark_as_mc_processed(&mut self) {
        self.initially_mc_processed = true;
    }

    /// Get MC processed status
    pub fn was_mc_processed(&self) -> bool {
        self.initially_mc_processed
    }

    /// Set MC delivery status
    pub fn mark_as_mc_delivered(&mut self) {
        self.mc_delivered = true;
    }

    /// Get MC delivery status
    pub fn was_mc_delivered(&self) -> bool {
        self.mc_delivered
    }    

    /// Set origin
    pub fn mark_as_mc_originated(&mut self) {
        self.mc_originated = true;
    }

    /// Get origin
    pub fn is_mc_originated(&self) -> bool {
        self.mc_originated
    }

    /// Set ready for sending flag
    pub fn toggle_send_ready(&mut self, new_state: bool) -> bool {
        let prev_state = self.ready_for_send;

        self.ready_for_send = new_state;

        prev_state
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

        let ton_block = self.status().into_boxed();
        let serialized_block_status = catchain::utils::serialize_tl_boxed_object!(&ton_block);
        let serialized_block_status =
            catchain::CatchainFactory::create_block_payload(serialized_block_status);

        self.serialized_block_status = Some(serialized_block_status.clone());

        serialized_block_status
    }

    /// Update status of the block (returns true, if the block should be updated in the network)
    pub fn set_status(&mut self, local_key: &PrivateKey, local_idx: u16, nodes_count: u16, status: Option<bool>) -> Result<bool> {
        let prev_hash = self.get_signatures_hash();

        if let Some(status) = status {
            let signature = if status {
                &mut self.approvals_signature
            } else {
                &mut self.rejections_signature
            };

            signature.sign(local_key, local_idx, nodes_count)?;
        }

        self.deliveries_signature.sign(local_key, local_idx, nodes_count)?;

        self.update_hash();

        Ok(self.get_signatures_hash() != prev_hash)
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

        self.deliveries_signature = new_deliveries_signature;
        self.approvals_signature = new_approvals_signature;
        self.rejections_signature = new_rejections_signature;

        if self.merges_count < merges_count {
            self.merges_count = merges_count; //do not change hash
        }

        self.set_creation_timestamp(created_timestamp);        

        self.update_hash();

        Ok(self.get_signatures_hash() != prev_hash)
    }

    /// Get signatures hash
    pub(crate) fn get_signatures_hash(&self) -> u32 { //TODO: remove pub(crate)
        self.signatures_hash
    }

    /// Update hash
    fn update_hash(&mut self) {
        let prev_hash = self.signatures_hash;

        self.signatures_hash = self.deliveries_signature.get_hash()
            ^ self.approvals_signature.get_hash()
            ^ self.rejections_signature.get_hash();

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
            mc_originated: false,
            mc_delivered: false,
            ready_for_send: false,
            first_appearance_time: std::time::SystemTime::now(),
            _instance_counter: instance_counter.clone(),
        };

        body.update_hash();

        Arc::new(SpinMutex::new(body))
    }
}
