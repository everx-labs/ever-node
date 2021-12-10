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

pub use super::*;
use crate::ton_api::IntoBoxed;
use std::collections::BTreeMap;
use utils::*;

/*
    Implementation details for ReceiverSource
*/

pub(crate) struct ReceiverSourceImpl {
    id: usize,                                       //source identifier
    adnl_id: PublicKeyHash,                          //ADNL identifier of the source
    public_key: PublicKey,                           //public key of the source
    public_key_hash: PublicKeyHash,                  //public key hash of the source
    blamed: bool,                                    //is this source blamed
    blocks: BTreeMap<BlockHeight, ReceivedBlockPtr>, //map from height to block for this source - our knowledge about the source chain
    delivered_height: BlockHeight, //how many blocks have been delivered for this source
    received_height: BlockHeight,  //how many blocks have been received for this source
    fork_proof: Option<ton::BlockDataFork>, //fork proof
    fork_proof_serialized: Option<BlockPayloadPtr>, //fork proof serialized
    fork_ids: Vec<usize>,          //fork identifiers for this source
    blamed_heights: Vec<BlockHeight>, //blamed heights for each fork id
    statistics: ReceiverSourceStatistics, //source statistics
}

/// Functions which converts public ReceiverSource to its implementation
#[allow(dead_code)]
fn get_impl(receiver_source: &dyn ReceiverSource) -> &ReceiverSourceImpl {
    receiver_source
        .get_impl()
        .downcast_ref::<ReceiverSourceImpl>()
        .unwrap()
}

#[allow(dead_code)]
fn get_mut_impl(receiver_source: &mut dyn ReceiverSource) -> &mut ReceiverSourceImpl {
    receiver_source
        .get_mut_impl()
        .downcast_mut::<ReceiverSourceImpl>()
        .unwrap()
}

/*
    Implementation for public ReceiverSource trait
*/

impl ReceiverSource for ReceiverSourceImpl {
    /*
        General purpose methods
    */

    fn get_impl(&self) -> &dyn Any {
        self
    }
    fn get_mut_impl(&mut self) -> &mut dyn Any {
        self
    }

    /*
        Accessors
    */

    fn get_id(&self) -> usize {
        self.id
    }

    fn get_public_key_hash(&self) -> &PublicKeyHash {
        &self.public_key_hash
    }

    fn get_public_key(&self) -> &PublicKey {
        &self.public_key
    }

    fn get_adnl_id(&self) -> &PublicKeyHash {
        &self.adnl_id
    }

    fn is_blamed(&self) -> bool {
        self.blamed
    }

    fn get_statistics(&self) -> &ReceiverSourceStatistics {
        &self.statistics
    }

    fn get_mut_statistics(&mut self) -> &mut ReceiverSourceStatistics {
        &mut self.statistics
    }

    /*
        Blocks management
    */

    fn get_block(&self, height: BlockHeight) -> Option<ReceivedBlockPtr> {
        match self.blocks.get(&height) {
            None => None,
            Some(t) => Some(t.clone()),
        }
    }

    fn process_new_block(&mut self, block_cell: ReceivedBlockPtr, receiver: &mut dyn Receiver) {
        if self.is_fork_found() {
            return;
        }

        let block = block_cell.borrow();

        assert!(block.get_source_id() == self.id);

        if let Some(existing_block) = self.get_block(block.get_height()) {
            assert!(block.get_hash() != existing_block.borrow().get_hash());

            warn!(
                "fork found on height {} for source #{}: blocks {} and {}",
                block.get_height(),
                self.id,
                block.get_hash().to_hex_string(),
                existing_block.borrow().get_hash().to_hex_string()
            );

            if !self.is_fork_found() {
                let fork = ton::BlockDataFork {
                    left: block.export_tl_dep().into_boxed(),
                    right: existing_block.borrow().export_tl_dep().into_boxed(),
                };

                self.set_fork_proof(fork);

                receiver.add_fork_proof(self.fork_proof_serialized.as_ref().unwrap());
            }

            self.mark_as_blamed(receiver);
            return;
        }

        self.blocks.insert(block.get_height(), block_cell.clone());
    }

    /*
        Forks management
    */

    fn get_forks_count(&self) -> usize {
        self.fork_ids.len()
    }

    fn add_fork(&mut self, receiver: &mut dyn Receiver) -> usize {
        if self.fork_ids.len() > 0 {
            self.mark_as_blamed(receiver);
        }

        let fork_id = receiver.add_fork();

        assert!(fork_id > 0);

        self.fork_ids.push(fork_id);

        trace!("...adding new fork {} of source {}", fork_id, self.id);

        if self.fork_ids.len() > 1 {
            assert!(self.is_blamed());
        }

        fork_id
    }

    fn blame(&mut self, fork: usize, height: BlockHeight, receiver: &mut dyn Receiver) {
        self.mark_as_blamed(receiver);

        //associate blamed height with a fork id
        //we don't check blamed_heights.len() > 0 because it's a dead code in original TON implementation

        if self.blamed_heights.len() <= fork {
            self.blamed_heights.resize(fork + 1, 0);
        }

        if self.blamed_heights[fork] == 0 || self.blamed_heights[fork] > height {
            info!(
                "Source {} has been blamed at fork {} and height {}",
                self.id, fork, height
            );
            self.blamed_heights[fork] = height;
        }
    }

    fn mark_as_blamed(&mut self, receiver: &mut dyn Receiver) {
        if !self.blamed {
            debug!("Blaming source {}", self.id);

            self.blocks.clear();
            self.delivered_height = 0;

            receiver.blame(self.id);
        }

        self.blamed = true;
    }

    fn get_forks(&self) -> &Vec<usize> {
        &self.fork_ids
    }

    fn get_blamed_heights(&self) -> &Vec<BlockHeight> {
        &self.blamed_heights
    }

    fn is_fork_found(&self) -> bool {
        !self.fork_proof.is_none()
    }

    fn set_fork_proof(&mut self, fork_proof: ton::BlockDataFork) {
        if self.is_fork_found() {
            return;
        }

        self.fork_proof = Some(fork_proof.clone());
        self.fork_proof_serialized = Some(CatchainFactory::create_block_payload(
            serialize_tl_boxed_object!(&fork_proof.into_boxed()),
        ));

        error!(
            "Fork has been found for source {} hash={:?}",
            self.get_id(),
            get_hash(self.fork_proof_serialized.as_ref().unwrap().data())
        );
    }

    fn get_fork_proof(&self) -> &Option<ton::BlockDataFork> {
        &self.fork_proof
    }

    /*
        Receivement & Delivery management
    */

    fn has_unreceived(&self) -> bool {
        if self.is_blamed() {
            return true;
        }

        if self.blocks.len() == 0 {
            return false;
        }

        let (_, last_received_block) = self.blocks.iter().next_back().unwrap();
        let last_received_block_height = last_received_block.borrow().get_height();

        assert!(last_received_block_height >= self.received_height);

        last_received_block_height > self.received_height
    }

    fn has_undelivered(&self) -> bool {
        self.delivered_height < self.received_height
    }

    fn get_received_height(&self) -> BlockHeight {
        self.received_height
    }

    fn get_delivered_height(&self) -> BlockHeight {
        self.delivered_height
    }

    fn block_received(&mut self, height: BlockHeight) {
        if self.is_blamed() {
            return;
        }

        if self.received_height + 1 == height {
            self.received_height = height;
        }

        loop {
            let block_result = self.get_block(self.received_height + 1);

            if block_result.is_none() {
                return;
            }

            let block = block_result.unwrap();

            if !block.borrow().is_initialized() {
                return;
            }

            self.received_height += 1;
        }
    }

    fn block_delivered(&mut self, height: BlockHeight) {
        if self.is_blamed() {
            return;
        }

        if self.delivered_height + 1 == height {
            self.delivered_height = height;
        }

        loop {
            let block_result = self.get_block(self.delivered_height + 1);

            if block_result.is_none() {
                return;
            }

            let block = block_result.unwrap();

            if !block.borrow().is_delivered() {
                return;
            }

            self.delivered_height += 1;
        }
    }
}

/*
    Private ReceiverSourceImpl details
*/

impl ReceiverSourceImpl {
    /*
        Creation
    */

    fn new(id: usize, public_key: PublicKey, adnl_id: &PublicKeyHash) -> Self {
        let public_key_hash = get_public_key_hash(&public_key);

        trace!(
            "...creating source #{} with public_key_hash={}, adnl_id={}",
            id,
            public_key_hash,
            adnl_id
        );

        Self {
            id: id,
            adnl_id: adnl_id.clone(),
            public_key: public_key,
            public_key_hash: public_key_hash,
            blamed: false,
            delivered_height: 0,
            received_height: 0,
            blocks: BTreeMap::new(),
            fork_proof: None,
            fork_proof_serialized: None,
            fork_ids: Vec::new(),
            blamed_heights: Vec::new(),
            statistics: ReceiverSourceStatistics::default(),
        }
    }

    pub(crate) fn create(
        id: usize,
        public_key: PublicKey,
        adnl_id: &PublicKeyHash,
    ) -> ReceiverSourcePtr {
        Rc::new(RefCell::new(ReceiverSourceImpl::new(
            id, public_key, adnl_id,
        )))
    }
}
