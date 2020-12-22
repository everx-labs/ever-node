pub use super::*;

/*
    Implementation details for OldRoundState
*/

#[derive(Clone)]
pub(crate) struct OldRoundStateImpl {
    pool: SessionPool,                                    //pool of the object
    hash: HashType,                                       //hash of the round
    sequence_number: u32,                                 //round sequence number
    block: SentBlockPtr,                                  //signed block candidate
    signatures: BlockCandidateSignatureVectorPtr,         //commit signatures
    approve_signatures: BlockCandidateSignatureVectorPtr, //approve signatures
    instance_counter: CachedInstanceCounter,              //instance counter
}

/*
    Implementation for public OldRoundState trait
*/

impl OldRoundState for OldRoundStateImpl {
    /*
        General purpose methods & accessors
    */

    fn get_impl(&self) -> &dyn Any {
        self
    }

    fn get_sequence_number(&self) -> u32 {
        self.sequence_number
    }

    /*
        Approval management
    */

    fn get_approve_signatures(&self) -> &BlockCandidateSignatureVectorPtr {
        &self.approve_signatures
    }

    fn check_block_is_approved_by(&self, src_idx: u32) -> bool {
        self.approve_signatures.at(src_idx as usize).is_some()
    }

    /*
       Commit management
    */

    fn get_block(&self) -> &SentBlockPtr {
        &self.block
    }

    fn get_signatures(&self) -> &BlockCandidateSignatureVectorPtr {
        &self.signatures
    }

    fn get_block_id(&self) -> &BlockId {
        self.block.get_id()
    }

    fn check_block_is_signed_by(&self, src_idx: u32) -> bool {
        self.signatures.at(src_idx as usize).is_some()
    }

    /*
        Merging
    */

    fn merge_round(
        &self,
        right: &dyn RoundState,
        desc: &mut dyn SessionDescription,
    ) -> PoolPtr<dyn OldRoundState> {
        let left = self;

        assert!(left.get_sequence_number() == right.get_sequence_number());

        let signed_block = right.get_block(left.get_block_id());
        let signatures = left.signatures.merge(right.get_signatures(), desc);
        let approve_signatures = match signed_block {
            Some(signed_block) => left
                .approve_signatures
                .merge(signed_block.get_approvers_list(), desc),
            None => left.approve_signatures.clone(),
        };

        OldRoundStateImpl::create(
            desc,
            left.get_sequence_number(),
            left.block.clone(),
            signatures,
            approve_signatures,
        )
    }

    /*
        Clone object to persistent pool
    */

    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn OldRoundState> {
        let self_cloned = Self::new(
            self.sequence_number,
            self.block.move_to_persistent(cache),
            self.signatures.move_to_persistent(cache),
            self.approve_signatures.move_to_persistent(cache),
            self.hash,
            &self.instance_counter,
        );

        Self::create_persistent_object(self_cloned, cache)
    }
}

/*
    Implementation of OldRoundStateWrapper
*/

impl OldRoundStateWrapper for OldRoundStatePtr {
    fn apply_action(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
        message: &ton::Message,
    ) -> OldRoundStatePtr {
        get_impl(&**self).apply_action_dispatch(desc, src_idx, attempt_id, message, self.clone())
    }
}

/*
    Implementation for Merge trait
*/

impl Merge<PoolPtr<dyn OldRoundState>> for PoolPtr<dyn OldRoundState> {
    fn merge(&self, right: &Self, desc: &mut dyn SessionDescription) -> Self {
        let left = &get_impl(&**self);
        let right = &get_impl(&**right);

        assert!(left.get_sequence_number() == right.get_sequence_number());

        let signatures = left.signatures.merge(&right.signatures, desc);
        let approve_signatures = left
            .approve_signatures
            .merge(&right.approve_signatures, desc);

        OldRoundStateImpl::create(
            desc,
            left.get_sequence_number(),
            left.block.clone(),
            signatures,
            approve_signatures,
        )
    }
}

/*
    Implementation for public HashableObject & PoolObject & MovablePoolObject public traits
*/

impl HashableObject for OldRoundStateImpl {
    fn get_hash(&self) -> HashType {
        self.hash
    }
}

impl PoolObject for OldRoundStateImpl {
    fn set_pool(&mut self, pool: SessionPool) {
        self.pool = pool;
        self.instance_counter.set_pool(pool);
    }

    fn get_pool(&self) -> SessionPool {
        self.pool
    }
}

impl MovablePoolObject<PoolPtr<dyn OldRoundState>> for PoolPtr<dyn OldRoundState> {
    fn move_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn OldRoundState> {
        if SessionPool::Persistent == self.get_pool() {
            return self.clone();
        }

        self.clone_to_persistent(cache)
    }
}

/*
    Comparison (address only because of the cache)
*/

impl std::cmp::PartialEq for dyn OldRoundState {
    fn eq(&self, other: &Self) -> bool {
        //compare addresses only because each vector is unique in cache system

        (self as *const Self) == (other as *const Self)
    }
}

/*
    Implementation for public CacheObject trait
*/

impl CacheObject<OldRoundStateImpl> for OldRoundStateImpl {
    fn compare(&self, value: &Self) -> bool {
        self.sequence_number == value.sequence_number
            && self.block == value.block
            && &self.signatures == &value.signatures
            && &self.approve_signatures == &value.approve_signatures
            && self.hash == value.hash
    }
}

/*
    Implementation for public Display & Debug
*/

impl fmt::Display for OldRoundStateImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Debug for OldRoundStateImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.pool == SessionPool::Temp {
            write!(f, "~").unwrap();
        }

        f.debug_struct("OldRoundState")
            .field("hash", &format_args!("{:08x}", &self.hash))
            .field("sequence_number", &self.sequence_number)
            .field("block", &self.block)
            .field("signatures", &self.signatures)
            .field("approve_signatures", &self.approve_signatures)
            .finish()
    }
}

/*
    Implementation internals of OldRoundStateImpl
*/

/// Function which converts public trait to its implementation
fn get_impl(value: &dyn OldRoundState) -> &OldRoundStateImpl {
    value
        .get_impl()
        .downcast_ref::<OldRoundStateImpl>()
        .unwrap()
}

impl OldRoundStateImpl {
    /*
        Incremental updates management
    */

    fn apply_action_dispatch(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
        message: &ton::Message,
        default_state: OldRoundStatePtr,
    ) -> OldRoundStatePtr {
        trace!("...applying action on old round #{}", self.sequence_number);

        let new_round_state = match message {
            ton::Message::ValidatorSession_Message_ApprovedBlock(message) => {
                self.apply_approved_block_action(desc, src_idx, attempt_id, message, default_state)
            }
            ton::Message::ValidatorSession_Message_Commit(message) => {
                self.apply_committed_block_action(desc, src_idx, attempt_id, message, default_state)
            }
            _ => {
                warn!(
                    "Node {} sent an invalid message in old round: expected APPROVE/COMMIT: {:?}",
                    desc.get_source_public_key_hash(src_idx),
                    message
                );

                return default_state;
            }
        };

        new_round_state
    }

    fn apply_approved_block_action(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        _attempt_id: u32,
        message: &ton::message::ApprovedBlock,
        default_state: OldRoundStatePtr,
    ) -> OldRoundStatePtr {
        trace!("...applying approved block action");

        let candidate_id: BlockId = message.candidate.clone().into();

        //check if approve is received for a committed block

        if &candidate_id != self.get_block_id() {
            warn!(
                "Node {} sent an invalid message: approved not committed block in old round {:?}",
                desc.get_source_public_key_hash(src_idx),
                message
            );

            return default_state;
        }

        //check if block has been already approved

        if self.approve_signatures.at(src_idx as usize).is_some() {
            warn!(
                "Node {} sent an invalid message: double approve in old round {:?}",
                desc.get_source_public_key_hash(src_idx),
                message
            );

            return default_state;
        }

        //check signature

        if candidate_id != *SKIP_ROUND_CANDIDATE_BLOCKID {
            let committed_block = self.get_block().as_ref().unwrap();

            if let Err(err) = desc.check_approve_signature(
                committed_block.get_root_hash(),
                committed_block.get_file_hash(),
                src_idx,
                &message.signature,
            ) {
                warn!(
                    "Node {} invalid APPROVE signature has been received: message={:?}, err={:?}",
                    desc.get_source_public_key_hash(src_idx),
                    message,
                    err
                );

                return default_state;
            }
        } else {
            if message.signature.len() != 0 {
                warn!("Node {} invalid APPROVE signature has been received (expected unsigned block): {:?}",
          desc.get_source_public_key_hash(src_idx), message);

                return default_state;
            }
        }

        //update state

        let approve_signature =
            SessionFactory::create_block_candidate_signature(desc, message.signature.clone());
        let approve_signatures =
            self.approve_signatures
                .change(desc, src_idx as usize, approve_signature);

        Self::create(
            desc,
            self.sequence_number,
            self.block.clone(),
            self.signatures.clone(),
            approve_signatures,
        )
    }

    fn apply_committed_block_action(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        _attempt_id: u32,
        message: &ton::message::Commit,
        default_state: OldRoundStatePtr,
    ) -> OldRoundStatePtr {
        trace!("...applying commit action");

        //check if the node is trying to sign a block which has not been committed

        let block_id = self.get_block_id();
        let candidate_id: BlockId = message.candidate.clone().into();

        if &candidate_id != block_id {
            warn!("Node {} sent an invalid message: signed wrong block in old round (should be {:?}): {:?}",
        desc.get_source_public_key_hash(src_idx), block_id, message);

            return default_state;
        }

        //check signature

        if candidate_id != *SKIP_ROUND_CANDIDATE_BLOCKID {
            let committed_block = self.get_block().as_ref().unwrap();

            if let Err(err) = desc.check_signature(
                committed_block.get_root_hash(),
                committed_block.get_file_hash(),
                src_idx,
                &message.signature,
            ) {
                warn!(
                    "Node {} invalid COMMIT signature has been received: message={:?}, err={:?}",
                    desc.get_source_public_key_hash(src_idx),
                    message,
                    err
                );

                return default_state;
            }
        } else {
            if message.signature.len() != 0 {
                warn!("Node {} invalid COMMIT signature has been received (expected unsigned block): {:?}",
          desc.get_source_public_key_hash(src_idx), message);

                return default_state;
            }
        }

        //check if block has been already approved

        if self.check_block_is_signed_by(src_idx) {
            warn!(
                "Node {} sent an invalid message: double commit in old round {:?}",
                desc.get_source_public_key_hash(src_idx),
                message
            );

            return default_state;
        }

        //update state

        let commit_signature =
            SessionFactory::create_block_candidate_signature(desc, message.signature.clone());
        let commit_signatures = self
            .signatures
            .change(desc, src_idx as usize, commit_signature);

        Self::create(
            desc,
            self.sequence_number,
            self.block.clone(),
            commit_signatures,
            self.approve_signatures.clone(),
        )
    }

    /*
        Creation
    */

    fn compute_hash(
        sequence_number: u32,
        block: &SentBlockPtr,
        signatures: &BlockCandidateSignatureVectorPtr,
        approve_signatures: &BlockCandidateSignatureVectorPtr,
    ) -> HashType {
        crate::utils::compute_hash(ton::hashable::ValidatorSessionOldRound {
            seqno: sequence_number as ton::int,
            block: block.get_ton_hash(),
            signatures: signatures.get_ton_hash(),
            approve_signatures: approve_signatures.get_ton_hash(),
        })
    }

    fn new(
        sequence_number: u32,
        block: SentBlockPtr,
        signatures: BlockCandidateSignatureVectorPtr,
        approve_signatures: BlockCandidateSignatureVectorPtr,
        hash: HashType,
        instance_counter: &CachedInstanceCounter,
    ) -> Self {
        Self {
            pool: SessionPool::Temp,
            sequence_number: sequence_number,
            hash: hash,
            block: block,
            signatures: signatures,
            approve_signatures: approve_signatures,
            instance_counter: instance_counter.clone(),
        }
    }

    fn create(
        desc: &mut dyn SessionDescription,
        sequence_number: u32,
        block: SentBlockPtr,
        signatures: BlockCandidateSignatureVectorPtr,
        approve_signatures: BlockCandidateSignatureVectorPtr,
    ) -> OldRoundStatePtr {
        let hash = Self::compute_hash(sequence_number, &block, &signatures, &approve_signatures);
        let body = Self::new(
            sequence_number,
            block,
            signatures,
            approve_signatures,
            hash,
            desc.get_old_rounds_instance_counter(),
        );

        Self::create_temp_object(body, desc.get_cache())
    }

    pub(crate) fn create_from_round(
        desc: &mut dyn SessionDescription,
        round: RoundStatePtr,
    ) -> OldRoundStatePtr {
        let block = round.get_precommitted_block().unwrap();

        assert!(round.check_block_is_signed(desc));

        let block_candidate = round.get_block(block.get_id()).unwrap();

        Self::create(
            desc,
            round.get_sequence_number(),
            block,
            round.get_signatures().clone(),
            block_candidate.get_approvers_list().clone(),
        )
    }
}
