pub use super::*;

/*
    Constants
*/

const DEBUG_DUMP_MERGE: bool = false; //dump merge results for debugging
const MIN_ATTEMPT_ID: u32 = 1024; //minimal attempt ID

/*
    Implementation details for SessionState
*/

type AttemptIdVector = PoolPtr<dyn Vector<u32>>;
type OldRoundVector = Option<PoolPtr<dyn Vector<PoolPtr<dyn OldRoundState>>>>;

#[derive(Clone)]
pub(crate) struct SessionStateImpl {
    pool: SessionPool,                       //pool of the object
    hash: HashType,                          //hash of the state
    attempt_ids: AttemptIdVector,            //attempt sequence numbers for each validator
    current_round: RoundStatePtr,            //current round
    old_rounds: OldRoundVector,              //old rounds
    instance_counter: CachedInstanceCounter, //instance counter
}

/*
    Utils
*/

fn get_round_id(message: &ton::Message) -> u32 {
    let round = match message {
        ton::Message::ValidatorSession_Message_ApprovedBlock(message) => message.round,
        ton::Message::ValidatorSession_Message_Commit(message) => message.round,
        ton::Message::ValidatorSession_Message_Empty(message) => message.round,
        ton::Message::ValidatorSession_Message_Precommit(message) => message.round,
        ton::Message::ValidatorSession_Message_RejectedBlock(message) => message.round,
        ton::Message::ValidatorSession_Message_SubmittedBlock(message) => message.round,
        ton::Message::ValidatorSession_Message_Vote(message) => message.round,
        ton::Message::ValidatorSession_Message_VoteFor(message) => message.round,
    };

    round as u32
}

/*
    Implementation for public SessionState trait
*/

impl SessionState for SessionStateImpl {
    /*
        General purpose methods
    */

    fn get_impl(&self) -> &dyn Any {
        self
    }

    /*
        Validators management
    */

    fn get_ts(&self, src_idx: u32) -> u32 {
        *self.attempt_ids.at(src_idx as usize)
    }

    /*
        Round management
    */

    fn get_current_round_sequence_number(&self) -> u32 {
        self.current_round.get_sequence_number()
    }

    /*
        Blocks management
    */

    fn get_block(
        &self,
        _desc: &dyn SessionDescription,
        block_id: &BlockId,
    ) -> Option<SentBlockPtr> {
        match self.current_round.get_block(block_id) {
            None => None,
            Some(block_candidate) => Some(block_candidate.get_block().clone()),
        }
    }

    fn check_block_is_sent_by(&self, src_idx: u32) -> bool {
        self.current_round.check_block_is_sent_by(src_idx)
    }

    /*
        Approval management
    */

    fn get_committed_block_approve_signatures(
        &self,
        sequence_number: u32,
    ) -> Option<BlockCandidateSignatureVectorPtr> {
        if (sequence_number as usize) < self.old_rounds.len() {
            Some(
                self.old_rounds
                    .at(sequence_number as usize)
                    .get_approve_signatures()
                    .clone(),
            )
        } else {
            None
        }
    }

    fn get_blocks_approved_by(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
    ) -> Vec<SentBlockPtr> {
        self.current_round.get_blocks_approved_by(desc, src_idx)
    }

    fn choose_blocks_to_approve(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
    ) -> Vec<SentBlockPtr> {
        self.current_round.choose_blocks_to_approve(desc, src_idx)
    }

    fn check_block_is_approved_by(&self, src_idx: u32, block_id: &BlockId) -> bool {
        match self.current_round.get_block(block_id) {
            None => false,
            Some(block_candidate) => block_candidate.check_block_is_approved_by(src_idx),
        }
    }

    fn get_block_approvers(&self, desc: &dyn SessionDescription, block_id: &BlockId) -> Vec<u32> {
        self.current_round.get_block_approvers(desc, block_id)
    }

    /*
        Voting management
    */

    fn check_need_generate_vote_for(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
        attempt: u32,
    ) -> bool {
        self.current_round
            .check_need_generate_vote_for(desc, src_idx, attempt)
    }

    fn generate_vote_for(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
    ) -> ton::Message {
        self.current_round
            .generate_vote_for(desc, src_idx, attempt_id)
    }

    /*
        Commit management
    */

    fn get_committed_block_signatures(
        &self,
        sequence_number: u32,
    ) -> Option<BlockCandidateSignatureVectorPtr> {
        if (sequence_number as usize) < self.old_rounds.len() {
            Some(
                self.old_rounds
                    .at(sequence_number as usize)
                    .get_signatures()
                    .clone(),
            )
        } else {
            None
        }
    }

    fn choose_block_to_sign(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
    ) -> Option<SentBlockPtr> {
        if self.current_round.check_block_is_signed_by(src_idx) {
            return None;
        }

        self.current_round.choose_block_to_sign(desc, src_idx)
    }

    fn get_committed_block(
        &self,
        _desc: &dyn SessionDescription,
        round_seqno: u32,
    ) -> Option<SentBlockPtr> {
        if (round_seqno as usize) < self.old_rounds.len() {
            Some(self.old_rounds.at(round_seqno as usize).get_block().clone())
        } else {
            None
        }
    }

    fn check_block_is_signed_by(&self, src_idx: u32) -> bool {
        self.current_round.check_block_is_signed_by(src_idx)
    }

    /*
        Incremental updates management
    */

    fn apply_action(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        mut attempt_id: u32,
        message: &ton::Message,
    ) -> SessionStatePtr {
        trace!(
            "...received message from node #{} with public key hash {}",
            src_idx,
            desc.get_source_public_key_hash(src_idx)
        );

        //clamp attempt ID according to the node attempts history

        let current_attempt_id_for_node = *self.attempt_ids.at(src_idx as usize);

        if attempt_id < current_attempt_id_for_node {
            warn!(
                "...received message with invalid timestamp which goes back: {} -> {}",
                current_attempt_id_for_node, attempt_id
            );

            attempt_id = current_attempt_id_for_node;
        }

        if attempt_id < MIN_ATTEMPT_ID {
            warn!(
                "...received message with invalid timestamp which is too small {}",
                attempt_id
            );

            attempt_id = MIN_ATTEMPT_ID;
        }

        trace!("...clamped attempt ID is {}", attempt_id);

        //update attempts state

        let new_attempt_ids = self.attempt_ids.change(desc, src_idx as usize, attempt_id);

        //update round state

        let round_id = get_round_id(message);
        let current_round_id = self.current_round.get_sequence_number();

        if round_id > current_round_id {
            //attempt to update non existing future round

            warn!(
                "...too big round ID {} (current round is {})",
                round_id, current_round_id
            );

            return Self::create(
                desc,
                new_attempt_ids,
                self.current_round.clone(),
                self.old_rounds.clone(),
            );
        }

        if round_id == current_round_id {
            trace!("...forward action applying to a round #{}", round_id);

            let mut new_current_round = self
                .current_round
                .apply_action(desc, src_idx, attempt_id, message);
            let mut new_old_rounds = self.old_rounds.clone();

            if new_current_round.check_block_is_signed(desc) {
                //if new signed block appeared in the current round - create the new round and save previous round in old rounds

                trace!(
                    "...mark round #{} as old round and create new current round",
                    round_id
                );

                let old_round = SessionFactory::create_old_round(desc, new_current_round.clone());

                assert!(new_old_rounds.len() as u32 == old_round.get_sequence_number());

                new_old_rounds = new_old_rounds.push(desc, old_round);
                new_current_round =
                    SessionFactory::create_round(desc, new_current_round.get_sequence_number() + 1);
            }

            return Self::create(desc, new_attempt_ids, new_current_round, new_old_rounds);
        } else {
            //change state of one of the old rounds

            trace!("...update state of old round #{}", round_id);

            let new_old_round = self
                .old_rounds
                .at(round_id as usize)
                .apply_action(desc, src_idx, attempt_id, message);
            let new_old_rounds = self
                .old_rounds
                .change(desc, round_id as usize, new_old_round);

            return Self::create(
                desc,
                new_attempt_ids,
                self.current_round.clone(),
                new_old_rounds,
            );
        }
    }

    fn create_action(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
    ) -> Option<ton::Message> {
        self.current_round.create_action(desc, src_idx, attempt_id)
    }

    /*
        Clone object to persistent pool
    */

    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn SessionState> {
        let self_cloned = Self::new(
            self.attempt_ids.move_to_persistent(cache),
            self.current_round.move_to_persistent(cache),
            self.old_rounds.move_to_persistent(cache),
            self.hash,
            &self.instance_counter,
        );

        Self::create_persistent_object(self_cloned, cache)
    }

    /*
        Dump state
    */

    fn dump(&self, desc: &dyn SessionDescription) -> String {
        self.current_round.dump(desc)
    }
}

/*
    Implementation of SessionStateWrapper
*/

impl SessionStateWrapper for SessionStatePtr {
    fn make_all(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
    ) -> SessionStatePtr {
        trace!("...actualizing state");

        let mut state = self.clone();

        loop {
            let (new_state, made) =
                get_impl(&*state).make_one(desc, src_idx, attempt_id, state.clone());

            state = new_state;

            if !made {
                return state;
            }
        }
    }
}

/*
    Implementation for Merge trait
*/

impl Merge<PoolPtr<dyn SessionState>> for PoolPtr<dyn SessionState> {
    fn merge(&self, right: &Self, desc: &mut dyn SessionDescription) -> Self {
        let left = &get_impl(&**self);
        let right = &get_impl(&**right);

        assert!(left.attempt_ids.len() == desc.get_total_nodes() as usize);
        assert!(right.attempt_ids.len() == desc.get_total_nodes() as usize);

        //merge attempts & old rounds

        let attempt_ids =
            left.attempt_ids
                .merge_custom(&right.attempt_ids, desc, &|left, right, _desc| {
                    std::cmp::max(*left, *right)
                });

        let mut old_rounds = left.old_rounds.merge(&right.old_rounds, desc);

        //merge current round

        let mut round = {
            let left_sequence_number = left.current_round.get_sequence_number();
            let right_sequence_number = right.current_round.get_sequence_number();

            //choose round with highest sequence number and merge opposite round to old rounds

            if left_sequence_number < right_sequence_number {
                let old_round = old_rounds.at(left_sequence_number as usize);
                let old_round = old_round.merge_round(&*left.current_round, desc);

                old_rounds = old_rounds.change(desc, left_sequence_number as usize, old_round);

                right.current_round.clone()
            } else if left_sequence_number > right_sequence_number {
                let old_round = old_rounds.at(right_sequence_number as usize);
                let old_round = old_round.merge_round(&*right.current_round, desc);

                old_rounds = old_rounds.change(desc, right_sequence_number as usize, old_round);

                left.current_round.clone()
            } else {
                left.current_round.merge(&right.current_round, desc)
            }
        };

        //switch current round to a new one in case of signed block appearance after merging

        if round.check_block_is_signed(desc) {
            assert!((round.get_sequence_number() as usize) == old_rounds.len());

            let old_round = SessionFactory::create_old_round(desc, round.clone());

            assert!(old_rounds.len() as u32 == old_round.get_sequence_number());

            old_rounds = old_rounds.push(desc, old_round); //push the current round as an old round
            round = SessionFactory::create_round(desc, round.get_sequence_number() + 1);
            //create new round
        }

        //create merged state

        let result = SessionStateImpl::create(desc, attempt_ids, round, old_rounds);

        if DEBUG_DUMP_MERGE {
            debug!(
                "leftstate={:?} ============= rightstate={:?} =========== mergedstate={:?}",
                left, right, result
            );
        }

        result
    }
}

/*
    Implementation for public HashableObject & PoolObject & MovablePoolObject public traits
*/

impl HashableObject for SessionStateImpl {
    fn get_hash(&self) -> HashType {
        self.hash
    }
}

impl PoolObject for SessionStateImpl {
    fn set_pool(&mut self, pool: SessionPool) {
        self.pool = pool;
        self.instance_counter.set_pool(pool);
    }

    fn get_pool(&self) -> SessionPool {
        self.pool
    }
}

impl MovablePoolObject<PoolPtr<dyn SessionState>> for PoolPtr<dyn SessionState> {
    fn move_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn SessionState> {
        if SessionPool::Persistent == self.get_pool() {
            return self.clone();
        }

        self.clone_to_persistent(cache)
    }
}

/*
    Comparison (address only because of the cache)
*/

impl std::cmp::PartialEq for dyn SessionState {
    fn eq(&self, other: &Self) -> bool {
        //compare addresses only because each vector is unique in cache system

        (self as *const Self) == (other as *const Self)
    }
}

/*
    Implementation for public CacheObject trait
*/

impl CacheObject<SessionStateImpl> for SessionStateImpl {
    fn compare(&self, value: &Self) -> bool {
        self.hash == value.hash
            && &self.attempt_ids == &value.attempt_ids
            && &self.current_round == &value.current_round
            && &self.old_rounds == &value.old_rounds
    }
}

/*
    Implementation for public Display
*/

impl fmt::Display for SessionStateImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Debug for SessionStateImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.pool == SessionPool::Temp {
            write!(f, "~").unwrap();
        }

        f.debug_struct("ValidatorSessionState")
            .field("hash", &format_args!("{:08x}", &self.hash))
            .field("attempt_ids", &self.attempt_ids)
            .field("current_round", &self.current_round)
            .field("old_rounds", &self.old_rounds)
            .finish()
    }
}

/*
    Implementation internals of SessionStateImpl
*/

/// Function which converts public trait to its implementation
fn get_impl(value: &dyn SessionState) -> &SessionStateImpl {
    value.get_impl().downcast_ref::<SessionStateImpl>().unwrap()
}

impl SessionStateImpl {
    /*
        Incremental updates management
    */

    fn make_one(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        mut attempt_id: u32,
        default_state: SessionStatePtr,
    ) -> (SessionStatePtr, bool) {
        trace!(
            "......actualizing state (source={}, attempt={})",
            src_idx,
            attempt_id
        );

        //clamp attempt ID according to the node attempts history

        let attempt_ids = self.attempt_ids.clone();
        let current_attempt_id_for_node = *attempt_ids.at(src_idx as usize);

        if attempt_id < current_attempt_id_for_node {
            warn!(
                "Node {} generated has invalid attempt ID which goes back ({} -> {})",
                desc.get_source_public_key_hash(src_idx),
                current_attempt_id_for_node,
                attempt_id
            );

            attempt_id = current_attempt_id_for_node;
        }

        if attempt_id < MIN_ATTEMPT_ID {
            warn!(
                "Node {} has invalid attempt ID {} which is too small",
                desc.get_source_public_key_hash(src_idx),
                attempt_id
            );

            attempt_id = MIN_ATTEMPT_ID;
        }

        trace!("......clamped attempt ID is {}", attempt_id);

        let (attempt_ids, time_updated) = if current_attempt_id_for_node >= attempt_id {
            (attempt_ids, false)
        } else {
            let attempt_ids = attempt_ids.change(desc, src_idx as usize, attempt_id);

            warn!(
                "Node {} updating time in make_all()",
                desc.get_source_public_key_hash(src_idx)
            );

            (attempt_ids, true)
        };

        //forward actualization to a round

        let (round, made_changes) = self.current_round.make_one(desc, src_idx, attempt_id);

        if !made_changes && !time_updated {
            return (default_state, false);
        }

        assert!(!round.check_block_is_signed(desc));

        //update state

        trace!("......create updated state");

        let state = Self::create(desc, attempt_ids, round, self.old_rounds.clone());

        (state, true)
    }

    /*
        Hash calculation
    */

    fn compute_hash(
        attempt_ids: &AttemptIdVector,
        current_round: &RoundStatePtr,
        old_rounds: &OldRoundVector,
    ) -> HashType {
        crate::utils::compute_hash(ton::hashable::ValidatorSession {
            ts: attempt_ids.get_ton_hash(),
            old_rounds: old_rounds.get_ton_hash(),
            cur_round: current_round.get_ton_hash(),
        })
    }

    /*
        Creation
    */

    fn new(
        attempt_ids: AttemptIdVector,
        current_round: RoundStatePtr,
        old_rounds: OldRoundVector,
        hash: HashType,
        instance_counter: &CachedInstanceCounter,
    ) -> Self {
        Self {
            pool: SessionPool::Temp,
            attempt_ids: attempt_ids,
            old_rounds: old_rounds,
            current_round: current_round,
            hash: hash,
            instance_counter: instance_counter.clone(),
        }
    }

    fn create(
        desc: &mut dyn SessionDescription,
        attempt_ids: AttemptIdVector,
        current_round: RoundStatePtr,
        old_rounds: OldRoundVector,
    ) -> SessionStatePtr {
        let hash = Self::compute_hash(&attempt_ids, &current_round, &old_rounds);
        let body = Self::new(
            attempt_ids,
            current_round,
            old_rounds,
            hash,
            desc.get_session_states_instance_counter(),
        );

        Self::create_temp_object(body, desc.get_cache())
    }

    pub(crate) fn create_empty(desc: &mut dyn SessionDescription) -> SessionStatePtr {
        let nodes_count = desc.get_total_nodes() as usize;
        let attempt_ids = SessionFactory::create_vector(desc, vec![0; nodes_count]);
        let current_round = SessionFactory::create_round(desc, 0);

        Self::create(desc, attempt_ids, current_round, None)
    }
}
