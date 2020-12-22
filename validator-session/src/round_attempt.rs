pub use super::*;
use crate::ton_api::IntoBoxed;

/*
    Implementation details for RoundAttemptState
*/

#[derive(Clone)]
pub(crate) struct RoundAttemptStateImpl {
    pool: SessionPool,                       //pool of the object
    hash: HashType,                          //hash of the attempt
    sequence_number: u32,                    //sequence number of the attempt
    votes: VoteCandidateVectorPtr,           //votes for blocks
    precommitted: BoolVectorPtr,             //precommits from sources
    vote_for: Option<SentBlockPtr>,          //sent block for voting
    instance_counter: CachedInstanceCounter, //instance counter
}

/*
    Implementation for public RoundAttemptState trait
*/

impl RoundAttemptState for RoundAttemptStateImpl {
    /*
        General purpose methods & accessors
    */

    fn get_impl(&self) -> &dyn Any {
        self
    }

    fn get_sequence_number(&self) -> u32 {
        self.sequence_number
    }

    fn get_votes(&self) -> &VoteCandidateVectorPtr {
        &self.votes
    }

    fn get_precommits(&self) -> &BoolVectorPtr {
        &self.precommitted
    }

    /*
        Voting management
    */

    fn get_voted_block(&self, desc: &dyn SessionDescription) -> Option<SentBlockPtr> {
        if self.votes.is_none() {
            return None;
        }

        for vote in self.votes.get_iter() {
            if vote.check_block_is_voted(desc) {
                return Some(vote.get_block().clone());
            }
        }

        None
    }

    fn get_vote_for_block(&self) -> &Option<SentBlockPtr> {
        &self.vote_for
    }

    fn check_vote_received_from(&self, src_idx: u32) -> bool {
        if self.votes.is_none() {
            return false;
        }

        for vote in self.votes.get_iter() {
            if vote.check_block_is_voted_by(src_idx) {
                return true;
            }
        }

        false
    }

    /*
        Precommit management
    */

    fn check_attempt_is_precommitted(&self, desc: &dyn SessionDescription) -> bool {
        let mut weight = 0;

        for i in 0..desc.get_total_nodes() {
            if !self.precommitted.at(i as usize) {
                continue;
            }

            weight += desc.get_node_weight(i);

            if weight >= desc.get_cutoff_weight() {
                return true;
            }
        }

        false
    }

    fn check_precommit_received_from(&self, src_idx: u32) -> bool {
        *self.precommitted.at(src_idx as usize)
    }

    /*
        Incremental updates management
    */

    fn create_action(
        &self,
        desc: &dyn SessionDescription,
        round: &dyn RoundState,
        src_idx: u32,
        attempt_id: u32,
    ) -> Option<ton::Message> {
        trace!(
            "...create attempt action for source={} and attempt={}",
            src_idx,
            attempt_id
        );

        //vote if the node did not vote in the attempt

        if !self.check_vote_received_from(src_idx) {
            trace!("...choose block to vote");

            if let Some(block_to_vote) =
                round.choose_block_to_vote(desc, src_idx, attempt_id, self.vote_for.clone())
            {
                let block_to_vote_id = block_to_vote.get_id();

                trace!("...block {:?} has been choosen to vote", block_to_vote_id);

                return Some(
                    ton::message::Vote {
                        round: round.get_sequence_number() as ton::int,
                        attempt: self.sequence_number as ton::int,
                        candidate: block_to_vote_id.clone().into(),
                    }
                    .into_boxed(),
                );
            }
        }

        //precommit if the node did not have precommitted block in the attempt

        if !self.check_precommit_received_from(src_idx) {
            trace!("...get voted block");

            if let Some(voted_block) = self.get_voted_block(desc) {
                let voted_block_id = voted_block.get_id();

                trace!(
                    "...block {:?} has been choosen to precommit",
                    voted_block_id
                );

                return Some(
                    ton::message::Precommit {
                        round: round.get_sequence_number() as ton::int,
                        attempt: self.sequence_number as ton::int,
                        candidate: voted_block_id.clone().into(),
                    }
                    .into_boxed(),
                );
            }
        }

        //return empty marker if we can't neither vote nor precommit

        Some(
            ton::message::Empty {
                round: round.get_sequence_number() as ton::int,
                attempt: self.sequence_number as ton::int,
            }
            .into_boxed(),
        )
    }

    /*
        Clone object to persistent pool
    */

    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn RoundAttemptState> {
        let self_cloned = Self::new(
            self.sequence_number,
            self.votes.move_to_persistent(cache),
            self.precommitted.move_to_persistent(cache),
            self.vote_for.move_to_persistent(cache),
            self.hash,
            &self.instance_counter,
        );

        Self::create_persistent_object(self_cloned, cache)
    }

    /*
        Dump state
    */

    fn dump(&self, desc: &dyn SessionDescription) -> String {
        let mut result = "".to_string();

        result = format!("{}      - attempt #{}\n", result, self.sequence_number);

        //dump "vote for"

        if let Some(ref vote_for) = self.vote_for {
            if let Some(vote_for) = vote_for {
                result = format!(
                    "{}        - vote_for: v{:03} (block_hash={})\n",
                    result,
                    vote_for.get_source_index(),
                    vote_for.get_hash()
                );
            } else {
                result = format!("{}        - vote_for: EMPTY\n", result);
            }
        } else {
            result = format!("{}        - vote_for: N/A\n", result);
        }

        //dump votes

        let total_nodes_count = desc.get_total_nodes();

        if let Some(ref votes) = self.votes {
            let mut votes_map: Vec<(ValidatorWeight, String)> = Vec::new();

            votes_map.reserve(votes.len());

            for vote_candidate in votes.get_iter() {
                let mut votes_weight: ValidatorWeight = 0;
                let voters = vote_candidate.get_voters_list();
                let mut vote_dump = "".to_string();

                for i in 0..total_nodes_count {
                    if *voters.at(i as usize) {
                        votes_weight += desc.get_node_weight(i);

                        if vote_dump != "" {
                            vote_dump = format!("{}, ", vote_dump);
                        }

                        vote_dump = format!("{}v{:03}", vote_dump, i);
                    }
                }

                let normalized_votes_weight = votes_weight as f64 / desc.get_total_weight() as f64;
                let normalized_cutoff_weight =
                    desc.get_cutoff_weight() as f64 / desc.get_total_weight() as f64;

                if let Some(block) = vote_candidate.get_block() {
                    vote_dump = format!(
                        "block source={}, hash={}, voted={} ({:.2}%/{:.2}%), voters: [{}]",
                        block.get_source_index(),
                        block.get_hash(),
                        votes_weight,
                        normalized_votes_weight * 100.0,
                        normalized_cutoff_weight * 100.0,
                        vote_dump
                    );
                } else {
                    vote_dump = format!(
                        "SKIP block voted={} ({:.2}%/{:.2}%), voters: [{}]",
                        votes_weight,
                        normalized_votes_weight * 100.0,
                        normalized_cutoff_weight * 100.0,
                        vote_dump
                    );
                }

                votes_map.push((votes_weight, vote_dump));
            }

            //sort votes in a reverse order

            votes_map.sort_by(|a, b| {
                if a.0 > b.0 {
                    std::cmp::Ordering::Less
                } else if a.0 < b.0 {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            });

            result = format!(
                "{}        - votes ({} candidates):\n",
                result,
                votes_map.len()
            );

            for (_weight, vote_dump) in votes_map {
                result = format!("{}          - {}\n", result, vote_dump);
            }
        } else {
            result = format!("{}        - votes: EMPTY\n", result);
        }

        //dump precommits

        let mut precommitted_weight: ValidatorWeight = 0;
        let mut precomitters = "".to_string();

        for i in 0..total_nodes_count {
            if !*self.precommitted.at(i as usize) {
                continue;
            }

            precommitted_weight += desc.get_node_weight(i);

            if precomitters != "" {
                precomitters = format!("{}, ", precomitters);
            }

            precomitters = format!("{}v{:03}", precomitters, i);
        }

        let normalized_precommitted_weight =
            precommitted_weight as f64 / desc.get_total_weight() as f64;
        let normalized_cutoff_weight =
            desc.get_cutoff_weight() as f64 / desc.get_total_weight() as f64;

        if precommitted_weight > 0 {
            result = format!(
                "{}        - precommited: weight={} ({:.2}%/{:.2}%), validators=[{}]\n",
                result,
                precommitted_weight,
                normalized_precommitted_weight * 100.0,
                normalized_cutoff_weight * 100.0,
                precomitters
            );
        } else {
            result = format!("{}        - precommited: N/A\n", result);
        }

        result
    }
}

/*
    Implementation of RoundAttemptStateWrapper
*/

impl RoundAttemptStateWrapper for RoundAttemptStatePtr {
    fn apply_action(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
        message: &ton::Message,
        round_state: &dyn RoundState,
    ) -> RoundAttemptStatePtr {
        get_impl(&**self).apply_action_dispatch(
            desc,
            src_idx,
            attempt_id,
            message,
            round_state,
            self.clone(),
        )
    }

    fn make_one(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
        round_state: &dyn RoundState,
    ) -> (RoundAttemptStatePtr, bool) {
        trace!(
            "......actualizing attempt {} of round {} for source {}",
            attempt_id,
            round_state.get_sequence_number(),
            src_idx
        );

        get_impl(&**self).apply_default_action(
            desc,
            src_idx,
            attempt_id,
            None,
            round_state,
            self.clone(),
        )
    }
}

/*
    Implementation for Merge trait
*/

impl Merge<PoolPtr<dyn RoundAttemptState>> for PoolPtr<dyn RoundAttemptState> {
    fn merge(&self, right: &Self, desc: &mut dyn SessionDescription) -> Self {
        let left = &get_impl(&**self);
        let right = &get_impl(&**right);

        assert!(left.get_sequence_number() == right.get_sequence_number());

        let vote_for = {
            if left.vote_for.is_none() {
                right.vote_for.clone()
            } else if right.vote_for.is_none() {
                left.vote_for.clone()
            } else if left.vote_for == right.vote_for {
                left.vote_for.clone()
            } else {
                let left_block_id = left.vote_for.as_ref().unwrap().get_id();
                let right_block_id = right.vote_for.as_ref().unwrap().get_id();

                if left_block_id < right_block_id {
                    left.vote_for.clone()
                } else {
                    right.vote_for.clone()
                }
            }
        };

        let precommitted = left.precommitted.merge(&right.precommitted, desc);
        let votes = left.votes.merge(&right.votes, desc);

        RoundAttemptStateImpl::create(desc, left.sequence_number, votes, precommitted, vote_for)
    }
}

/*
    Implementation for public HashableObject & PoolObject & MovablePoolObject public trait
*/

impl HashableObject for RoundAttemptStateImpl {
    fn get_hash(&self) -> HashType {
        self.hash
    }
}

impl PoolObject for RoundAttemptStateImpl {
    fn set_pool(&mut self, pool: SessionPool) {
        self.pool = pool;
        self.instance_counter.set_pool(pool);
    }

    fn get_pool(&self) -> SessionPool {
        self.pool
    }
}

impl MovablePoolObject<PoolPtr<dyn RoundAttemptState>> for PoolPtr<dyn RoundAttemptState> {
    fn move_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn RoundAttemptState> {
        if SessionPool::Persistent == self.get_pool() {
            return self.clone();
        }

        self.clone_to_persistent(cache)
    }
}

/*
    Comparison (address only because of the cache)
*/

impl std::cmp::PartialEq for dyn RoundAttemptState {
    fn eq(&self, other: &Self) -> bool {
        //compare addresses only because each vector is unique in cache system

        (self as *const Self) == (other as *const Self)
    }
}

/*
    Implementation for public CacheObject trait
*/

impl CacheObject<RoundAttemptStateImpl> for RoundAttemptStateImpl {
    fn compare(&self, value: &Self) -> bool {
        self.sequence_number == value.sequence_number
            && &self.votes == &value.votes
            && &self.precommitted == &value.precommitted
            && self.vote_for == value.vote_for
            && self.hash == value.hash
    }
}

/*
    Implementation for public Display & Debug
*/

impl fmt::Display for RoundAttemptStateImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Debug for RoundAttemptStateImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.pool == SessionPool::Temp {
            write!(f, "~").unwrap();
        }

        f.debug_struct("RoundAttemptState")
            .field("hash", &format_args!("{:08x}", &self.hash))
            .field("sequence_number", &self.sequence_number)
            .field("votes", &self.votes)
            .field("precommitted", &self.precommitted)
            .field("vote_for", &self.vote_for)
            .finish()
    }
}

/*
    Implementation internals of RoundAttemptStateImpl
*/

/// Function which converts public trait to its implementation
fn get_impl(value: &dyn RoundAttemptState) -> &RoundAttemptStateImpl {
    value
        .get_impl()
        .downcast_ref::<RoundAttemptStateImpl>()
        .unwrap()
}

impl RoundAttemptStateImpl {
    /*
        Utilities
    */

    fn get_candidate(
        votes: &VoteCandidateVectorPtr,
        block_id: &BlockId,
    ) -> Option<VoteCandidatePtr> {
        for candidate in votes.get_iter() {
            if candidate.get_id() == block_id {
                return Some(candidate.clone());
            }
        }

        None
    }

    /*
        Incremental updates management
    */

    fn apply_action_dispatch(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
        message: &ton::Message,
        round_state: &dyn RoundState,
        default_state: RoundAttemptStatePtr,
    ) -> RoundAttemptStatePtr {
        //dispatching incremental message based on its ID

        trace!("...dispatching incoming attempt message");

        let new_attempt_state = match message {
            ton::Message::ValidatorSession_Message_VoteFor(message) => self.apply_vote_for_action(
                desc,
                src_idx,
                attempt_id,
                message,
                round_state,
                default_state,
            ),
            ton::Message::ValidatorSession_Message_Vote(_) => {
                trace!("...apply vote action");
                self.apply_default_action(
                    desc,
                    src_idx,
                    attempt_id,
                    Some(message),
                    round_state,
                    default_state,
                )
                .0
            }
            ton::Message::ValidatorSession_Message_Precommit(_) => {
                trace!("...apply precommit action");
                self.apply_default_action(
                    desc,
                    src_idx,
                    attempt_id,
                    Some(message),
                    round_state,
                    default_state,
                )
                .0
            }
            ton::Message::ValidatorSession_Message_Empty(_) => {
                trace!("...apply empty action");
                self.apply_default_action(
                    desc,
                    src_idx,
                    attempt_id,
                    Some(message),
                    round_state,
                    default_state,
                )
                .0
            }
            _ => {
                unreachable!(
                    "Unknown message appears for attempt processing: {:?}",
                    message
                );
            }
        };

        new_attempt_state
    }

    fn apply_vote_for_action(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
        message: &ton::message::VoteFor,
        round_state: &dyn RoundState,
        default_state: RoundAttemptStatePtr,
    ) -> RoundAttemptStatePtr {
        trace!("...apply vote-for action");

        //check the node has already sent vote-for message

        if self.vote_for.is_some() {
            warn!(
                "Node {} sent an invalid message: duplicate VOTEFOR: {:?}",
                desc.get_source_public_key_hash(src_idx),
                message
            );
            return default_state;
        }

        //check that expected for this attempt author of vote-for message is the node

        if src_idx != desc.get_vote_for_author(attempt_id) {
            warn!("Node {} sent an invalid message: invalid VOTEFOR author (expected {} but received {}): attempt_id={}, message={:?}",
        desc.get_source_public_key_hash(src_idx), desc.get_vote_for_author(attempt_id), src_idx, attempt_id, message);
            return default_state;
        }

        //check if vote-for is sent in appropriate time
        //vote-for attempts should be sent if nodes can't vote based of node priorities

        let first_attempt = round_state.get_first_attempt(src_idx);
        let max_attempts = desc.opts().max_round_attempts;

        if first_attempt == 0 && max_attempts > 0 {
            warn!("Node {} sent an invalid message: too early for VOTEFOR: src_idx={}, attempt_id={}, first_attempt={}, max_round_attempts={}, message={:?}",
        desc.get_source_public_key_hash(src_idx), src_idx, attempt_id, first_attempt, max_attempts, message);
            return default_state;
        }

        if first_attempt + max_attempts > attempt_id && max_attempts == 0 {
            warn!("Node {} sent an invalid message: too early for VOTEFOR: src_idx={}, attempt_id={}, first_attempt={}, max_round_attempts={}, message={:?}",
        desc.get_source_public_key_hash(src_idx), src_idx, attempt_id, first_attempt, max_attempts, message);
            return default_state;
        }

        //check the vote-for block-candidate

        let candidate_id: BlockId = message.candidate.clone().into();
        let vote_for_block = round_state.get_block(&candidate_id);

        if vote_for_block.is_none()
            || !vote_for_block
                .as_ref()
                .unwrap()
                .check_block_is_approved(desc)
        {
            warn!("Node {} sent an invalid message: VOTEFOR for non approved block: attempt_id={}, message={:?}",
        desc.get_source_public_key_hash(src_idx), attempt_id, message);
            return default_state;
        }

        //update state

        Self::create(
            desc,
            self.sequence_number,
            self.votes.clone(),
            self.precommitted.clone(),
            Some(vote_for_block.as_ref().unwrap().get_block().clone()),
        )
    }

    fn apply_default_action(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
        message: Option<&ton::Message>,
        round_state: &dyn RoundState,
        default_state: RoundAttemptStatePtr,
    ) -> (RoundAttemptStatePtr, bool) {
        trace!("......apply default action");

        //try to vote

        let vote_result = self.try_vote(
            desc,
            src_idx,
            attempt_id,
            message,
            round_state,
            default_state.clone(),
        );

        if vote_result.1 {
            trace!("......has new vote");
            return vote_result;
        }

        //try to precomit

        let precommit_result = self.try_precommit(
            desc,
            src_idx,
            attempt_id,
            message,
            round_state,
            default_state.clone(),
        );

        if precommit_result.1 {
            trace!("......has a precommit");
            return precommit_result;
        }

        //expect empty message (voting and precommitting have been processed above)

        if let Some(message) = message {
            match message {
                ton::Message::ValidatorSession_Message_Empty(_) => {
                    //do nothing
                }
                _ => {
                    warn!(
                        "Node {} sent an invalid message (expected EMPTY): {:?}",
                        desc.get_source_public_key_hash(src_idx),
                        message
                    );
                }
            }
        }

        (default_state, false)
    }

    fn try_vote(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
        message: Option<&ton::Message>,
        round_state: &dyn RoundState,
        default_state: RoundAttemptStatePtr,
    ) -> (RoundAttemptStatePtr, bool) {
        trace!("......try to vote");

        //check if we have already received a vote from the node

        if self.check_vote_received_from(src_idx) {
            trace!(
                "......vote has been already received from the node #{}; skip {:?}",
                src_idx,
                &message
            );
            return (default_state, false);
        }

        //choose block to vote

        let block_to_vote =
            round_state.choose_block_to_vote(desc, src_idx, attempt_id, self.vote_for.clone());

        if block_to_vote.is_none() {
            trace!(
                "......can't choose block to vote for node #{}; skip {:?}",
                src_idx,
                &message
            );
            return (default_state, false);
        }

        let block_to_vote = block_to_vote.unwrap();
        let block_to_vote_id = block_to_vote.get_id();

        trace!("......block to vote is {:?}", &block_to_vote);

        //check if block to vote is an expected one

        if let Some(message) = message {
            match message {
                ton::Message::ValidatorSession_Message_Vote(message) => {
                    let candidate_id: BlockId = message.candidate.clone().into();
                    if &candidate_id != block_to_vote_id {
                        warn!(
                            "Node {} sent an invalid message (expected VOTE({:?})): {:?}",
                            desc.get_source_public_key_hash(src_idx),
                            block_to_vote_id,
                            message
                        );
                    }
                }
                _ => {
                    warn!(
                        "Node {} sent an invalid message (expected VOTE({:?})): {:?}",
                        desc.get_source_public_key_hash(src_idx),
                        block_to_vote_id,
                        message
                    );
                }
            }
        } else {
            warn!(
                "Node {}: making implicit VOTE({:?})",
                desc.get_source_public_key_hash(src_idx),
                block_to_vote_id
            );
        }

        //update state

        let vote_candidate =
            if let Some(vote_candidate) = Self::get_candidate(&self.votes, block_to_vote_id) {
                vote_candidate
            } else {
                SessionFactory::create_vote_candidate(desc, block_to_vote)
            };
        let vote_candidate = vote_candidate.push(desc, src_idx);
        let votes = self.votes.push(desc, vote_candidate);

        trace!("......new votes for attempt {}: {:?}", attempt_id, &votes);

        let attempt_state = Self::create(
            desc,
            self.sequence_number,
            votes,
            self.precommitted.clone(),
            self.vote_for.clone(),
        );

        (attempt_state, true)
    }

    fn try_precommit(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        _attempt_id: u32,
        message: Option<&ton::Message>,
        _round_state: &dyn RoundState,
        default_state: RoundAttemptStatePtr,
    ) -> (RoundAttemptStatePtr, bool) {
        trace!("......try to precommit");

        //check if we have already received a precommit from the node

        if self.check_precommit_received_from(src_idx) {
            trace!(
                "......precommit has been already received from the node #{}; skip {:?}",
                src_idx,
                &message
            );
            return (default_state, false);
        }

        //choose block to vote

        let voted_block = self.get_voted_block(desc);

        if voted_block.is_none() {
            trace!(
                "......can't choose block to precommit for node #{}; skip {:?}",
                src_idx,
                &message
            );
            return (default_state, false);
        }

        let voted_block = voted_block.unwrap();
        let voted_block_id = voted_block.get_id();

        trace!("......block to precommit is {:?}", &voted_block);

        //check if voted block is an expected one

        if let Some(message) = message {
            match message {
                ton::Message::ValidatorSession_Message_Precommit(message) => {
                    let candidate_id: BlockId = message.candidate.clone().into();
                    if &candidate_id != voted_block_id {
                        warn!(
                            "Node {} sent an invalid message (expected PRECOMMIT({:?})): {:?}",
                            desc.get_source_public_key_hash(src_idx),
                            voted_block_id,
                            message
                        );
                    }
                }
                _ => {
                    warn!(
                        "Node {} sent an invalid message (expected PRECOMMIT({:?})): {:?}",
                        desc.get_source_public_key_hash(src_idx),
                        voted_block_id,
                        message
                    );
                }
            }
        } else {
            warn!(
                "Node {}: making implicit PRECOMMIT({:?})",
                desc.get_source_public_key_hash(src_idx),
                voted_block_id
            );
        }

        //update state

        let precommitted = self.precommitted.change(desc, src_idx as usize, true);
        let attempt_state = Self::create(
            desc,
            self.sequence_number,
            self.votes.clone(),
            precommitted,
            self.vote_for.clone(),
        );

        (attempt_state, true)
    }

    /*
        Creation
    */

    fn compute_hash(
        sequence_number: u32,
        votes: &VoteCandidateVectorPtr,
        precommitted: &BoolVectorPtr,
        vote_for: &Option<SentBlockPtr>,
    ) -> HashType {
        crate::utils::compute_hash(ton::hashable::ValidatorSessionRoundAttempt {
            seqno: sequence_number as ton::int,
            votes: votes.get_ton_hash(),
            precommitted: precommitted.get_ton_hash(),
            vote_for_inited: vote_for.is_some() as ton::int,
            vote_for: vote_for.get_ton_hash(),
        })
    }

    fn new(
        sequence_number: u32,
        votes: VoteCandidateVectorPtr,
        precommitted: BoolVectorPtr,
        vote_for: Option<SentBlockPtr>,
        hash: HashType,
        instance_counter: &CachedInstanceCounter,
    ) -> Self {
        Self {
            pool: SessionPool::Temp,
            sequence_number: sequence_number,
            votes: votes,
            precommitted: precommitted,
            vote_for: vote_for,
            hash: hash,
            instance_counter: instance_counter.clone(),
        }
    }

    fn create(
        desc: &mut dyn SessionDescription,
        sequence_number: u32,
        votes: VoteCandidateVectorPtr,
        precommitted: BoolVectorPtr,
        vote_for: Option<SentBlockPtr>,
    ) -> RoundAttemptStatePtr {
        let hash = Self::compute_hash(sequence_number, &votes, &precommitted, &vote_for);
        let body = Self::new(
            sequence_number,
            votes,
            precommitted,
            vote_for,
            hash,
            desc.get_round_attempts_instance_counter(),
        );

        Self::create_temp_object(body, desc.get_cache())
    }

    pub(crate) fn create_empty(
        desc: &mut dyn SessionDescription,
        sequence_number: u32,
    ) -> RoundAttemptStatePtr {
        let precommitted = vec![false; desc.get_total_nodes() as usize];
        let precommitted = SessionFactory::create_vector(desc, precommitted);

        Self::create(desc, sequence_number, None, precommitted, None)
    }
}
