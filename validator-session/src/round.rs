//TODO: remove duplicates for make_one & apply_action

pub use super::*;
use std::collections::HashSet;
use ton_api::IntoBoxed;

/*
    Implementation details for RoundState
*/

/// Sorting predicate for block candidate
struct BlockCandidateSortingPredicate;

impl SortingPredicate<PoolPtr<dyn BlockCandidate>> for BlockCandidateSortingPredicate {
    fn less(first: &PoolPtr<dyn BlockCandidate>, second: &PoolPtr<dyn BlockCandidate>) -> bool {
        first.get_id() < second.get_id()
    }
}

/// Sorting predicate for attempt
struct AttemptSortingPredicate;

impl SortingPredicate<PoolPtr<dyn RoundAttemptState>> for AttemptSortingPredicate {
    fn less(
        first: &PoolPtr<dyn RoundAttemptState>,
        second: &PoolPtr<dyn RoundAttemptState>,
    ) -> bool {
        first.get_sequence_number() < second.get_sequence_number()
    }
}

type AttemptIdVector = Option<PoolPtr<dyn Vector<u32>>>;
type ApproveVector =
    Option<PoolPtr<dyn SortedVector<PoolPtr<dyn BlockCandidate>, BlockCandidateSortingPredicate>>>;
type AttemptVector =
    Option<PoolPtr<dyn SortedVector<PoolPtr<dyn RoundAttemptState>, AttemptSortingPredicate>>>;

#[derive(Clone)]
pub(crate) struct RoundStateImpl {
    pool: SessionPool,                            //pool of the object
    hash: HashType,                               //hash of the round
    precommitted_block: Option<SentBlockPtr>,     //precommitted block
    sequence_number: u32,                         //sequence number of the round
    first_attempt: AttemptIdVector, //vector of first attempt sequence number for each validator
    last_precommit: AttemptIdVector, //vector of last precommit sequence number for each validator
    sent_blocks: ApproveVector,     //list of blocks to approve
    signatures: BlockCandidateSignatureVectorPtr, //commit signatures for the block candidate
    attempts: AttemptVector,        //list of attempts for this round
    instance_counter: CachedInstanceCounter, //instance counter
}

/*
    Implementation for public RoundState trait
*/

impl RoundState for RoundStateImpl {
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
        Attempts management
    */

    fn get_first_attempt(&self, src_idx: u32) -> u32 {
        *self.first_attempt.at(src_idx as usize)
    }

    /*
        Blocks management
    */

    fn get_block(&self, block_id: &BlockId) -> Option<BlockCandidatePtr> {
        match self.sent_blocks {
            Some(_) => self.get_candidate(block_id),
            _ => None,
        }
    }

    fn check_block_is_sent_by(&self, src_idx: u32) -> bool {
        if self.sent_blocks.is_none() {
            return false;
        }

        let candidates = self.sent_blocks.as_ref().unwrap();

        for block_candidate in candidates.iter() {
            if let Some(sent_block) = block_candidate.get_block() {
                if sent_block.get_source_index() == src_idx {
                    return true;
                }
            }
        }

        false
    }

    /*
        Approval management
    */

    fn get_block_approvers(&self, desc: &dyn SessionDescription, block_id: &BlockId) -> Vec<u32> {
        let block_ptr = self.get_candidate(block_id);

        if block_ptr.is_none() {
            return Vec::new();
        }

        let block = block_ptr.unwrap();

        let mut result = Vec::new();

        result.reserve(desc.get_total_nodes() as usize);

        for i in 0..desc.get_total_nodes() {
            if block.check_block_is_approved_by(i as u32) {
                result.push(i);
            }
        }

        result
    }

    fn get_blocks_approved_by(
        &self,
        _desc: &dyn SessionDescription,
        src_idx: u32,
    ) -> Vec<SentBlockPtr> {
        if self.sent_blocks.is_none() {
            return Vec::new();
        }

        let candidates = self.sent_blocks.as_ref().unwrap();
        let mut result = Vec::new();

        result.reserve(candidates.len());

        for block_candidate in candidates.iter() {
            if block_candidate.check_block_is_approved_by(src_idx) {
                result.push(block_candidate.get_block().clone());
            }
        }

        result
    }

    fn check_block_is_approved_by(&self, src_idx: u32, block_id: &BlockId) -> bool {
        if self.sent_blocks.is_none() {
            return false;
        }

        let candidates = self.sent_blocks.as_ref().unwrap();

        for block_candidate in candidates.iter() {
            if block_candidate.get_id() == block_id {
                return block_candidate.check_block_is_approved_by(src_idx);
            }
        }

        false
    }

    fn choose_blocks_to_approve(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
    ) -> Vec<SentBlockPtr> {
        //if there are no sent blocks, then nothing to approve

        if self.sent_blocks.is_none() {
            return [None].to_vec();
        }

        //sort sent blocks according to validator priority in current round

        let mut block_candidates: Vec<Option<&BlockCandidatePtr>> =
            vec![None; (desc.get_max_priority() + 2) as usize];
        let mut sources_proposal_set = HashSet::<u32>::new();
        let mut has_empty_unapproved_candidate = false;

        for block_candidate in self.sent_blocks.get_iter() {
            let block = block_candidate.get_block();

            if block.is_none() {
                //if null-candidate appears set the flag for returning it in a vector instead of returning empty approval list

                if !block_candidate.check_block_is_approved_by(src_idx) {
                    has_empty_unapproved_candidate = true;
                }

                continue;
            }

            //get it node priority in this round for the block candidate

            let block_source_index = block_candidate.get_source_index();
            let priority = desc.get_node_priority(block_source_index, self.sequence_number);

            assert!(priority >= 0);

            let priority = priority as usize;

            //rank block according to a priority

            if sources_proposal_set.contains(&block_source_index) {
                //if a validator has already proposed block for approval in this round, skip second approval propose

                block_candidates[priority] = None;
            } else {
                //choose block for approval if it has not been yet approved by the specified node

                sources_proposal_set.insert(block_source_index);

                if !block_candidate.check_block_is_approved_by(src_idx) {
                    block_candidates[priority] = Some(block_candidate);
                }
            }
        }

        //transform result: BlockCandidate -> SentBlock

        let mut blocks = Vec::with_capacity(block_candidates.len());

        for block_candidate in &block_candidates {
            if let Some(block_candidate) = block_candidate {
                blocks.push(block_candidate.get_block().clone());
            }
        }

        if has_empty_unapproved_candidate {
            blocks.push(None);
        }

        blocks
    }

    /*
        Voting management
    */

    fn check_need_generate_vote_for(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
    ) -> bool {
        if src_idx != desc.get_vote_for_author(attempt_id) {
            //do not need to generate vote for if validator is not an author for this attempt

            return false;
        }

        if self.precommitted_block.is_some() {
            //do not generate vote for if validator has precommitted block already

            return false;
        }

        //TODO: comment code below

        if self.get_first_attempt(src_idx) == 0 && desc.opts().max_round_attempts > 0 {
            return false;
        }

        if self.get_first_attempt(src_idx) + desc.opts().max_round_attempts > attempt_id
            && desc.opts().max_round_attempts > 0
        {
            return false;
        }

        if let Some(attempt) = Self::get_attempt(&self.attempts, attempt_id) {
            //do not generate vote for block if it has been already received for this attempt

            if attempt.get_vote_for_block().is_some() {
                return false;
            }
        }

        //do not generate vote for block if there are not blocks at all

        if self.sent_blocks.is_none() {
            return false;
        }

        for block in self.sent_blocks.get_iter() {
            if block.check_block_is_approved(desc) {
                //generate vote for block is there is approved block

                return true;
            }
        }

        return false;
    }

    fn choose_block_to_vote(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
        vote_for: Option<SentBlockPtr>,
    ) -> Option<SentBlockPtr> {
        let src_idx = src_idx as usize;

        //if there are no sent blocks, then nothing to vote

        if self.sent_blocks.is_none() {
            return None;
        }

        //if there is a precommitted block from specified validator, then use this block as choosen for voting in this round too

        let last_precommit_attempt_id = *self.last_precommit.at(src_idx);

        if last_precommit_attempt_id > 0 {
            let attempt = Self::get_attempt(&self.attempts, last_precommit_attempt_id);

            assert!(attempt.is_some());

            let block = attempt.unwrap().get_voted_block(desc);

            assert!(block.is_some());

            return block.clone();
        }

        //Check if the first attempt for this round is less than round attempts count,
        //use default approach with "vote for" candidate from the leader node for this attempt.
        //In other case (none "vote for" block fits), returns specified in the call "vote for" block

        let first_attempt_id = *self.first_attempt.at(src_idx);
        let slow_mode = first_attempt_id > 0
            && first_attempt_id + desc.opts().max_round_attempts <= attempt_id
            || desc.opts().max_round_attempts == 0;

        if slow_mode {
            return vote_for.clone();
        }

        //enumerate attempts from the newest one to the latest one; if attempt has voted block, use it

        for attempt in self.attempts.get_iter().rev() {
            if let Some(block) = attempt.get_voted_block(desc) {
                return Some(block);
            }
        }

        //search for block from node with minimal priority in this round

        let max_priority: i32 = (desc.get_max_priority() + 2) as i32;
        let mut min_priority = max_priority;
        let mut block = None;

        for block_candidate in self.sent_blocks.get_iter() {
            //we can't vote for unapproved blocks

            if !block_candidate.check_block_is_approved(desc) {
                continue;
            }

            //compute validator priority for this round

            let priority = match block_candidate.get_block() {
                Some(_block) => {
                    desc.get_node_priority(block_candidate.get_source_index(), self.sequence_number)
                }
                _ => (desc.get_max_priority() + 1) as i32,
            };

            assert!(priority >= 0);

            if priority < min_priority {
                min_priority = priority;
                block = Some(block_candidate.get_block().clone());
            }
        }

        if min_priority >= max_priority {
            return None;
        }

        block.clone()
    }

    fn generate_vote_for(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
    ) -> ton::Message {
        assert!(src_idx == desc.get_vote_for_author(attempt_id));

        let mut candidate_block_ids = Vec::with_capacity(self.sent_blocks.len());

        for block in self.sent_blocks.get_iter() {
            if block.check_block_is_approved(desc) {
                candidate_block_ids.push(block.get_id().clone());
            }
        }

        assert!(candidate_block_ids.len() > 0);

        let random_candidate_index = desc.generate_random_usize() % candidate_block_ids.len();

        ton::message::VoteFor {
            round: self.get_sequence_number() as i32,
            attempt: attempt_id as i32,
            candidate: candidate_block_ids[random_candidate_index].clone().into(),
        }
        .into_boxed()
    }

    /*
        Precommit management
    */

    fn get_precommitted_block(&self) -> Option<SentBlockPtr> {
        self.precommitted_block.clone()
    }

    fn get_last_precommit(&self, src_idx: u32) -> u32 {
        *self.last_precommit.at(src_idx as usize)
    }

    /*
        Commit management
    */

    fn get_signatures(&self) -> &BlockCandidateSignatureVectorPtr {
        &self.signatures
    }

    fn check_block_is_signed(&self, desc: &dyn SessionDescription) -> bool {
        let mut weight = 0;

        for i in 0..desc.get_total_nodes() {
            if self.signatures.at(i as usize).is_none() {
                continue;
            }

            weight += desc.get_node_weight(i);

            if weight >= desc.get_cutoff_weight() {
                return true;
            }
        }

        false
    }

    fn check_block_is_signed_by(&self, src_idx: u32) -> bool {
        self.signatures.at(src_idx as usize).is_some()
    }

    fn choose_block_to_sign(
        &self,
        _desc: &dyn SessionDescription,
        _src_idx: u32,
    ) -> Option<SentBlockPtr> {
        self.precommitted_block.clone()
    }

    /*
        Incremental updates management
    */

    fn create_action(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
    ) -> Option<ton::Message> {
        use ton_api::ton::validator_session::round::validator_session::message::message::*;

        //stop actions generation if precommitted blocks exists

        if self.precommitted_block.is_some() {
            return Some(
                Empty {
                    round: self.get_sequence_number() as i32,
                    attempt: attempt_id as i32,
                }
                .into_boxed(),
            );
        }

        //delegate attempt generation to attempt

        let attempt = Self::get_attempt(&self.attempts, attempt_id);

        if let Some(attempt) = attempt {
            return attempt.create_action(desc, self, src_idx, attempt_id);
        }

        //choose block to vote

        let vote_candidate = self.choose_block_to_vote(desc, src_idx, attempt_id, None);

        if let Some(vote_candidate) = vote_candidate {
            let block_id = vote_candidate.get_id();

            trace!(
                "...choose block {:?} to vote in round {} and attempt {}",
                block_id,
                self.get_sequence_number(),
                attempt_id
            );

            return Some(
                ton::message::Vote {
                    round: self.sequence_number as ton::int,
                    attempt: attempt_id as ton::int,
                    candidate: block_id.clone().into(),
                }
                .into_boxed(),
            );
        } else {
            return Some(
                ton::message::Empty {
                    round: self.sequence_number as ton::int,
                    attempt: attempt_id as ton::int,
                }
                .into_boxed(),
            );
        }
    }

    /*
        Clone object to persistent pool
    */

    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn RoundState> {
        let self_cloned = Self::new(
            self.precommitted_block.move_to_persistent(cache),
            self.sequence_number,
            self.first_attempt.move_to_persistent(cache),
            self.last_precommit.move_to_persistent(cache),
            self.sent_blocks.move_to_persistent(cache),
            self.signatures.move_to_persistent(cache),
            self.attempts.move_to_persistent(cache),
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

        result = format!("{}    - round: {}\n", result, self.sequence_number);

        //dump singing status

        let mut sign_weight = 0;
        let mut sign_dump = "".to_string();

        for i in 0..desc.get_total_nodes() {
            if self.signatures.at(i as usize).is_none() {
                continue;
            }

            sign_weight += desc.get_node_weight(i);

            if sign_dump != "" {
                sign_dump = format!("{}, ", sign_dump);
            }

            sign_dump = format!("{}v{:03}", sign_dump, i);
        }

        let normalized_sign_weight = sign_weight as f64 / desc.get_total_weight() as f64;
        let normalized_cutoff_weight =
            desc.get_cutoff_weight() as f64 / desc.get_total_weight() as f64;

        result = format!(
            "{}    - {}: {} ({:.2}%/{:.2}%), signers: [{}]\n",
            result,
            if sign_weight < desc.get_cutoff_weight() {
                "unsigned"
            } else {
                "signed"
            },
            sign_weight,
            normalized_sign_weight * 100.0,
            normalized_cutoff_weight * 100.0,
            sign_dump
        );

        //dump precommit status

        if let Some(ref block) = self.precommitted_block {
            if let Some(block) = block {
                result = format!("{}    - precommitted: {:?}\n", result, block.get_id());
            } else {
                result = format!("{}    - precommitted: SKIP\n", result);
            }
        } else {
            result = format!("{}    - precommitted: N/A\n", result);
        }

        //dump sent blocks status

        let total_nodes_count = desc.get_total_nodes();

        if let Some(sent_blocks) = &self.sent_blocks {
            result = format!("{}    - sent_blocks:\n", result);

            for block_candidate in sent_blocks.get_iter() {
                let block = block_candidate.get_block();

                let priority: i32 = if let Some(block) = block {
                    desc.get_node_priority(block.get_source_index(), self.sequence_number)
                } else {
                    (desc.get_max_priority() + 1) as i32
                };

                let mut approved_weight: ValidatorWeight = 0;
                let approvers = block_candidate.get_approvers_list();

                for i in 0..total_nodes_count {
                    if approvers.at(i as usize).is_none() {
                        continue;
                    }

                    approved_weight += desc.get_node_weight(i);
                }

                let normalized_approved_weight =
                    approved_weight as f64 / desc.get_total_weight() as f64;
                let normalized_cutoff_weight =
                    desc.get_cutoff_weight() as f64 / desc.get_total_weight() as f64;

                if let Some(block) = block {
                    result = format!("{}      - block {:?}: source={}, hash={:?}, root_hash={:?}, file_hash={:?}, approved={} ({:.2}%/{:.2}%) priority={}\n",
            result, block.get_id(), block.get_source_index(), block.get_id(), block.get_root_hash(), block.get_file_hash(), approved_weight, normalized_approved_weight * 100.0,
            normalized_cutoff_weight * 100.0, priority);
                } else {
                    result = format!(
                        "{}      - SKIP block: approved={} ({:.2}%/{:.2}%) priority={}\n",
                        result,
                        approved_weight,
                        normalized_approved_weight * 100.0,
                        normalized_cutoff_weight * 100.0,
                        priority
                    );
                }
            }
        } else {
            result = format!("{}    - sent_blocks: EMPTY\n", result);
        }

        //dump first attempt

        let mut first_attempts_dump = "".to_string();

        for i in 0..total_nodes_count {
            let attempt_id = *self.first_attempt.at(i as usize);

            if attempt_id == 0 {
                continue;
            }

            first_attempts_dump =
                format!("{}      - v{:03}: {}\n", first_attempts_dump, i, attempt_id);
        }

        if first_attempts_dump != "" {
            result = format!("{}    - first_attempt:\n{}", result, first_attempts_dump);
        } else {
            result = format!("{}    - first_attempt: N/A\n", result);
        }

        //dump last precommit

        let mut last_precommit_dump = "".to_string();

        for i in 0..total_nodes_count {
            let attempt_id = *self.last_precommit.at(i as usize);

            if attempt_id == 0 {
                continue;
            }

            last_precommit_dump =
                format!("{}      - v{:03}: {}\n", last_precommit_dump, i, attempt_id);
        }

        if last_precommit_dump != "" {
            result = format!("{}    - last_precommit:\n{}", result, last_precommit_dump);
        } else {
            result = format!("{}    - last_precommit: N/A\n", result);
        }

        //dump attempts

        let mut attempts_dump_count = 0;
        let mut attempts_dump = "".to_string();

        for attempt in self.attempts.get_iter().rev() {
            const MAX_ATTEMPTS_DUMP_COUNT: u32 = 10;

            if attempts_dump_count >= MAX_ATTEMPTS_DUMP_COUNT {
                break;
            }

            attempts_dump = format!("{}{}\n", attempts_dump, attempt.dump(desc));
            attempts_dump_count += 1;
        }

        if attempts_dump != "" {
            result = format!("{}    - attempts:\n{}", result, attempts_dump);
        } else {
            result = format!("{}    - attempts: N/A\n", result);
        }

        result
    }
}

/*
    Implementation for Merge trait
*/

impl Merge<PoolPtr<dyn RoundState>> for PoolPtr<dyn RoundState> {
    fn merge(&self, right: &Self, desc: &mut dyn SessionDescription) -> Self {
        let left = &get_impl(&**self);
        let right = &get_impl(&**right);

        assert!(left.get_sequence_number() == right.get_sequence_number());

        //merge attempts

        let first_attempt =
            left.first_attempt
                .merge_custom(&right.first_attempt, desc, &|left, right, _desc| {
                    if *left == 0 {
                        *right
                    } else if *right == 0 {
                        *left
                    } else {
                        *std::cmp::min(left, right)
                    }
                });

        let attempts = left.attempts.merge(&right.attempts, desc);

        //merge precommitted block

        if left.precommitted_block.is_some() && right.precommitted_block.is_some() {
            assert!(
                left.precommitted_block.as_ref().unwrap().get_id()
                    == right.precommitted_block.as_ref().unwrap().get_id()
            );
        }

        let mut precommitted_block = left
            .precommitted_block
            .merge(&right.precommitted_block, desc);

        //switching between precommitted blocks in left and right states - choose latest precommit

        let mut diversed_voted_blocks: [SentBlockPtr; 2] = [None, None]; //pointers to diversed voted blocks in same attempt slot in left and right states
        let mut diversed_voted_attempts: [u32; 2] = [0; 2]; //sequence numbers of attempts with diversed voted blocks in same attempt slot
        let mut diversed_voted_attempts_count = 0;

        for attempt in attempts.get_iter().rev() {
            //we may have up to two diversed voted blocks because of left and right different states
            //find them and save for further processing

            if diversed_voted_attempts_count <= 1 {
                if let Some(voted_block) = attempt.get_voted_block(desc) {
                    let mut diversed_voted_block_found = true;

                    //ignore "diversed" blocks with the same block ID

                    if diversed_voted_attempts_count == 1 {
                        let left = diversed_voted_blocks[0].get_id();
                        let right = voted_block.get_id();

                        if left == right {
                            diversed_voted_block_found = false;
                        }
                    }

                    if diversed_voted_block_found {
                        diversed_voted_blocks[diversed_voted_attempts_count] = voted_block.clone();
                        diversed_voted_attempts[diversed_voted_attempts_count] =
                            attempt.get_sequence_number();

                        diversed_voted_attempts_count += 1;
                    }
                }
            }

            //set precommit block if left or right state has precommitted block

            if precommitted_block.is_none() && attempt.check_attempt_is_precommitted(desc) {
                precommitted_block = attempt.get_voted_block(desc);

                assert!(precommitted_block.is_some()); //check we have voted block
            }

            //stop searching if we have found precommitted block and 2 voted blocks from left and right states

            if precommitted_block.is_some() && diversed_voted_attempts_count == 2 {
                break;
            }
        }

        if diversed_voted_attempts_count >= 1 {
            //check that we have right order of attempt sequence numbers and first found attempt has been started
            //later than second one

            assert!(diversed_voted_attempts[0] > diversed_voted_attempts[1]);
        }

        //update last precommit sequence numbers for validators

        let last_precommit = left.last_precommit.merge_impl(
            &right.last_precommit,
            desc,
            true,
            &|left, right, _desc| {
                let last_precommit_attempt_sequence_number = *std::cmp::max(left, right);

                if diversed_voted_attempts_count == 0 {
                    //we have not found any attempt with voted block, so there should be no precommits too

                    assert!(last_precommit_attempt_sequence_number == 0);

                    return last_precommit_attempt_sequence_number;
                }

                if last_precommit_attempt_sequence_number > diversed_voted_attempts[1] {
                    return last_precommit_attempt_sequence_number;
                } else {
                    return 0;
                }
            },
        );

        //merge signatures & sent blocks

        let signatures = left.signatures.merge(&right.signatures, desc);
        let sent_blocks = left.sent_blocks.merge(&right.sent_blocks, desc);

        //create merged state

        RoundStateImpl::create(
            desc,
            precommitted_block,
            left.get_sequence_number(),
            first_attempt,
            last_precommit,
            sent_blocks,
            signatures,
            attempts,
        )
    }
}

/*
    Implementation of RoundStateWrapper
*/

impl RoundStateWrapper for RoundStatePtr {
    fn apply_action(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
        message: &ton::Message,
    ) -> RoundStatePtr {
        get_impl(&**self).apply_action_dispatch(desc, src_idx, attempt_id, message, self.clone())
    }

    fn make_one(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
    ) -> (RoundStatePtr, bool) {
        RoundStateImpl::make_one_impl(self.clone(), desc, src_idx, attempt_id)
    }
}

/*
    Implementation for public HashableObject & PoolObject & MovablePoolObject public traits
*/

impl HashableObject for RoundStateImpl {
    fn get_hash(&self) -> HashType {
        self.hash
    }
}

impl PoolObject for RoundStateImpl {
    fn set_pool(&mut self, pool: SessionPool) {
        self.pool = pool;
        self.instance_counter.set_pool(pool);
    }

    fn get_pool(&self) -> SessionPool {
        self.pool
    }
}

impl MovablePoolObject<PoolPtr<dyn RoundState>> for PoolPtr<dyn RoundState> {
    fn move_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn RoundState> {
        if SessionPool::Persistent == self.get_pool() {
            return self.clone();
        }

        self.clone_to_persistent(cache)
    }
}

/*
    Comparison (address only because of the cache)
*/

impl std::cmp::PartialEq for dyn RoundState {
    fn eq(&self, other: &Self) -> bool {
        //compare addresses only because each vector is unique in cache system

        (self as *const Self) == (other as *const Self)
    }
}

/*
    Implementation for public CacheObject trait
*/

impl CacheObject<RoundStateImpl> for RoundStateImpl {
    fn compare(&self, value: &Self) -> bool {
        self.sequence_number == value.sequence_number
            && self.hash == value.hash
            && self.precommitted_block == value.precommitted_block
            && &self.first_attempt == &value.first_attempt
            && &self.last_precommit == &value.last_precommit
            && &self.sent_blocks == &value.sent_blocks
            && &self.signatures == &value.signatures
            && &self.attempts == &value.attempts
    }
}

/*
    Implementation for public Display & Debug
*/

impl fmt::Display for RoundStateImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Debug for RoundStateImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.pool == SessionPool::Temp {
            write!(f, "~").unwrap();
        }

        f.debug_struct("RoundState")
            .field("hash", &format_args!("{:08x}", &self.hash))
            .field("sequence_number", &self.sequence_number)
            .field("precommitted_block", &self.precommitted_block)
            .field("first_attempt", &self.first_attempt)
            .field("last_precommit", &self.last_precommit)
            .field("signatures", &self.signatures)
            .field("attempts", &self.attempts)
            .field("sent_blocks", &self.sent_blocks)
            .finish()
    }
}

/*
    Implementation internals of RoundStateImpl
*/

/// Function which converts public trait to its implementation
fn get_impl(value: &dyn RoundState) -> &RoundStateImpl {
    value.get_impl().downcast_ref::<RoundStateImpl>().unwrap()
}

impl RoundStateImpl {
    /*
        Utilities
    */

    fn get_attempt(attempts_opt: &AttemptVector, attempt_id: u32) -> Option<RoundAttemptStatePtr> {
        if attempts_opt.is_none() {
            return None;
        }

        let attempts = attempts_opt.as_ref().unwrap();

        //binary search for attempt with specified ID

        let mut left: i32 = -1;
        let mut right: i32 = attempts.len() as i32;

        while right - left > 1 {
            let middle = (right + left) / 2;
            let current_attempt = attempts.at(middle as usize);
            let current_attempt_attempt_id = current_attempt.get_sequence_number();

            if current_attempt_attempt_id < attempt_id {
                left = middle;
            } else if current_attempt_attempt_id > attempt_id {
                right = middle;
            } else {
                return Some(current_attempt.clone());
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
        mut default_state: RoundStatePtr,
    ) -> RoundStatePtr {
        //update attempts state

        trace!("...applying action on round #{}", self.sequence_number);

        let first_attempt = self.first_attempt.clone();

        if *first_attempt.at(src_idx as usize) == 0
            || first_attempt.at(src_idx as usize) > &attempt_id
        {
            //we have received new first attempt for the (round, node)

            trace!("...new first attempt {} has been detected for round #{} (previous first attempt was {})",
        attempt_id, self.sequence_number, first_attempt.at(src_idx as usize));

            let first_attempt = first_attempt.change(desc, src_idx as usize, attempt_id);
            let new_round_state = Self::create(
                desc,
                self.precommitted_block.clone(),
                self.sequence_number,
                first_attempt,
                self.last_precommit.clone(),
                self.sent_blocks.clone(),
                self.signatures.clone(),
                self.attempts.clone(),
            );

            default_state = new_round_state;
        }

        //dispatching incremental message based on its ID

        trace!("...dispatching incoming message");

        let state = get_impl(&*default_state);
        let default_state = default_state.clone();

        let new_round_state = match message {
            ton::Message::ValidatorSession_Message_SubmittedBlock(message) => state
                .apply_submitted_block_action(desc, src_idx, attempt_id, message, default_state),
            ton::Message::ValidatorSession_Message_ApprovedBlock(message) => {
                state.apply_approved_block_action(desc, src_idx, attempt_id, message, default_state)
            }
            ton::Message::ValidatorSession_Message_RejectedBlock(message) => {
                state.apply_rejected_block_action(desc, src_idx, attempt_id, message, default_state)
            }
            ton::Message::ValidatorSession_Message_Commit(message) => state
                .apply_committed_block_action(desc, src_idx, attempt_id, message, default_state),
            _ => state.apply_action_on_attempt(desc, src_idx, attempt_id, message, default_state),
        };

        new_round_state
    }

    fn apply_submitted_block_action(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        _attempt_id: u32,
        message: &ton::message::SubmittedBlock,
        default_state: RoundStatePtr,
    ) -> RoundStatePtr {
        trace!("...applying submitted block action");

        //check if node can submit block in this round

        if desc.get_node_priority(src_idx, self.sequence_number) < 0 {
            warn!(
                "Node {} sent an invalid message: node cannot propose blocks in this round: {:?}",
                desc.get_source_public_key_hash(src_idx),
                message
            );

            return default_state;
        }

        //check if node has already proposed a block

        if self.check_block_is_sent_by(src_idx) {
            warn!(
                "Node {} sent an invalid message: duplicate block propose: {:?}",
                desc.get_source_public_key_hash(src_idx),
                message
            );

            return default_state;
        }

        //update state with new proposed block

        let sent_block = SessionFactory::create_sent_block(
            desc,
            src_idx,
            message.root_hash.clone().into(),
            message.file_hash.clone().into(),
            message.collated_data_file_hash.clone().into(),
        );
        let block_candidate = SessionFactory::create_unapproved_block_candidate(desc, sent_block);
        let block_id = block_candidate.get_id().clone();
        let new_sent_blocks = self.sent_blocks.push(desc, block_candidate);

        trace!(
            "...create updated state (add block {:?} to round {}); sent_blocks={:?}, old_sent_blocks={:?}",
            block_id,
            self.sequence_number,
            &new_sent_blocks,
            &self.sent_blocks
        );

        Self::create(
            desc,
            self.precommitted_block.clone(),
            self.sequence_number,
            self.first_attempt.clone(),
            self.last_precommit.clone(),
            new_sent_blocks,
            self.signatures.clone(),
            self.attempts.clone(),
        )
    }

    fn apply_approved_block_action(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        _attempt_id: u32,
        message: &ton::message::ApprovedBlock,
        default_state: RoundStatePtr,
    ) -> RoundStatePtr {
        trace!("...applying approved block action");

        let candidate_id: BlockId = message.candidate.clone().into();

        //check if we know the incoming block

        let mut sent_block = self.get_block(&candidate_id);

        if candidate_id != *SKIP_ROUND_CANDIDATE_BLOCKID && sent_block.is_none() {
            warn!(
                "Node {} sent an invalid message: the node has approved unknown block: {:?}",
                desc.get_source_public_key_hash(src_idx),
                message
            );

            return default_state;
        }

        //check if we have already approved this block

        if sent_block.is_some()
            && sent_block
                .as_ref()
                .unwrap()
                .check_block_is_approved_by(src_idx)
        {
            warn!(
                "Node {} sent an invalid message: duplicate block has been sent for approval: {:?}",
                desc.get_source_public_key_hash(src_idx),
                message
            );

            return default_state;
        }

        //check if two different blocks have been sent for approval in the same round from the node

        if candidate_id != *SKIP_ROUND_CANDIDATE_BLOCKID {
            assert!(sent_block.is_some());

            let sent_block = sent_block.as_ref().unwrap();

            for block_candidate in self.sent_blocks.get_iter() {
                if block_candidate.get_source_index() == sent_block.get_source_index()
                    && block_candidate.check_block_is_approved_by(src_idx)
                {
                    warn!("Node {} sent an invalid message: another block has been sent for approval from the same node: {:?}",
            desc.get_source_public_key_hash(src_idx), message);

                    return default_state;
                }
            }
        }

        //check block signature

        trace!("...check block signature");

        if candidate_id != *SKIP_ROUND_CANDIDATE_BLOCKID {
            let sent_block = sent_block.as_ref().unwrap().get_block();
            let sent_block = sent_block.as_ref().unwrap();

            if let Err(err) = desc.check_approve_signature(
                sent_block.get_root_hash(),
                sent_block.get_file_hash(),
                src_idx,
                &message.signature,
            ) {
                warn!(
                    "Node {} invalid signature has been received: message={:?}, err={:?}",
                    desc.get_source_public_key_hash(src_idx),
                    message,
                    err
                );

                return default_state;
            }
        } else {
            if message.signature.len() != 0 {
                warn!(
                    "Node {} invalid signature has been received (expected unsigned block): {:?}",
                    desc.get_source_public_key_hash(src_idx),
                    message
                );

                return default_state;
            }
        }

        //create empty block if empty candidate ID has been received

        if sent_block.is_none() {
            assert!(candidate_id == *SKIP_ROUND_CANDIDATE_BLOCKID);

            sent_block = Some(SessionFactory::create_unapproved_block_candidate(
                desc, None,
            ));
        }

        //add signature to the block & create updated state

        trace!("...create updated state");

        let signature = SessionFactory::create_block_candidate_signature(
            desc,
            message.signature.to_vec().into(),
        );
        let sent_block = sent_block.unwrap().push(desc, src_idx, signature);
        let new_sent_blocks = self.sent_blocks.push(desc, sent_block);

        Self::create(
            desc,
            self.precommitted_block.clone(),
            self.sequence_number,
            self.first_attempt.clone(),
            self.last_precommit.clone(),
            new_sent_blocks,
            self.signatures.clone(),
            self.attempts.clone(),
        )
    }

    fn apply_rejected_block_action(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        _attempt_id: u32,
        message: &ton::message::RejectedBlock,
        default_state: RoundStatePtr,
    ) -> RoundStatePtr {
        trace!("...applying rejected block action");
        error!(
            "Node {} rejected candidate {:?} with reason {:?}",
            desc.get_source_public_key_hash(src_idx),
            message.candidate,
            message.reason
        );

        default_state
    }

    fn apply_committed_block_action(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        _attempt_id: u32,
        message: &ton::message::Commit,
        default_state: RoundStatePtr,
    ) -> RoundStatePtr {
        trace!("...applying commit action");

        //check if node is trying to commit block before the precommit phase

        if self.precommitted_block.is_none() {
            warn!(
                "Node {} sent an invalid message: {:?}; attempt to commit non precommitted block",
                desc.get_source_public_key_hash(src_idx),
                message
            );

            return default_state;
        }

        let candidate_id: BlockId = message.candidate.clone().into();
        let precommitted_block = self.precommitted_block.as_ref().unwrap();
        let block_id = precommitted_block.get_id();

        if *block_id != candidate_id {
            warn!(
                "Node {} sent an invalid message: {:?}; attempt to commit wrong block {:?}. Expected {:?}",
                desc.get_source_public_key_hash(src_idx),
                message,
                message.candidate,
                block_id
            );

            return default_state;
        }

        //check if node is trying to sign block second time in the same round

        if self.signatures.at(src_idx as usize).is_some() {
            warn!(
                "Node {} sent an invalid message: {:?}; duplicate signature",
                desc.get_source_public_key_hash(src_idx),
                message
            );

            return default_state;
        }

        //check if node's signature

        if &candidate_id == &*SKIP_ROUND_CANDIDATE_BLOCKID {
            if message.signature.len() != 0 {
                warn!("Node {} sent an invalid signature: {:?}; attempt to sign a skip round candidate",
          desc.get_source_public_key_hash(src_idx), message);

                return default_state;
            }
        } else {
            let precommitted_block = precommitted_block.as_ref().unwrap();
            let signature = desc.check_signature(
                precommitted_block.get_root_hash(),
                precommitted_block.get_file_hash(),
                src_idx,
                &message.signature,
            );

            if let Err(err) = signature {
                warn!(
                    "Node {} sent an invalid signature: {:?}; {:?}",
                    desc.get_source_public_key_hash(src_idx),
                    message,
                    err
                );

                return default_state;
            }
        }

        //update state

        let signature =
            SessionFactory::create_block_candidate_signature(desc, message.signature.clone());
        let new_signatures = self.signatures.change(desc, src_idx as usize, signature);

        Self::create(
            desc,
            self.precommitted_block.clone(),
            self.sequence_number,
            self.first_attempt.clone(),
            self.last_precommit.clone(),
            self.sent_blocks.clone(),
            new_signatures,
            self.attempts.clone(),
        )
    }

    fn apply_action_on_attempt(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
        message: &ton::Message,
        default_state: RoundStatePtr,
    ) -> RoundStatePtr {
        trace!("...applying action on attempt {}", attempt_id);

        //check node is trying to change attempt after a precommit message

        if self.precommitted_block.is_some() {
            match message {
                ton::Message::ValidatorSession_Message_Empty(_) => {
                    //do nothing
                }
                _ => {
                    warn!("Node {} sent an invalid message in precommitted round (expected EMPTY): {:?}", desc.get_source_public_key_hash(src_idx), message);

                    return default_state;
                }
            }
        }

        //get or create attempt handle

        let attempt = if let Some(attempt) = Self::get_attempt(&self.attempts, attempt_id) {
            attempt.clone()
        } else {
            SessionFactory::create_attempt(desc, attempt_id)
        };

        //check if we had voted block in the round BEFORE message dispatching

        let _had_voted_block = attempt.get_voted_block(desc).is_some();

        //dispatch message for processing in an attempt

        let attempt = attempt.apply_action(desc, src_idx, attempt_id, message, self);

        //check if we have voted block in the round AFTER message dispatching

        let voted_block = attempt.get_voted_block(desc);
        let has_voted_block = voted_block.is_some();

        //check if we have precommitted block

        let mut new_precommitted_block = self.precommitted_block.clone();

        if new_precommitted_block.is_none() && attempt.check_attempt_is_precommitted(desc) {
            //after actions applying new precommitted block appears in the round

            assert!(has_voted_block);

            new_precommitted_block = Some(voted_block.clone().unwrap());
        }

        //get current last precommit attempt ID for the node

        let new_last_precommit = self.last_precommit.clone();
        let max_precommit_attempt_id = {
            let mut max_attempt_id = 0;

            for attempt_id_iter in new_last_precommit.get_iter() {
                max_attempt_id = std::cmp::max(max_attempt_id, *attempt_id_iter);
            }

            max_attempt_id
        };

        //update last precommit attempt ID for the node if voted block has changed after action applying to the current attempt

        let (new_voted_block_appears, mut new_last_precommit) = (|| -> (bool, AttemptIdVector) {
            //if there was no attempt with precommitted block or block has not appeared after action applying -> no changes are needed

            if max_precommit_attempt_id == 0 || !has_voted_block {
                return (false, new_last_precommit);
            }

            //get attempt with the max precommit attempt ID and check if the voted block is same as current precommitted block

            let last_precommit_attempt =
                Self::get_attempt(&self.attempts, max_precommit_attempt_id);

            assert!(last_precommit_attempt.is_some());

            let last_precommit_attempt = last_precommit_attempt.as_ref().unwrap();
            let last_precommit_voted_block = last_precommit_attempt.get_voted_block(desc);

            assert!(last_precommit_voted_block.is_some());

            let last_precommit_voted_block = last_precommit_voted_block.as_ref().unwrap();
            let voted_block = voted_block.as_ref().unwrap();

            //if block has not changed -> no changes are needed

            if last_precommit_voted_block.get_id() == voted_block.get_id() {
                return (false, new_last_precommit);
            }

            //block has been changed (new voted block appears) -> update last precommit state

            let current_attempt_id = attempt_id;
            let modifier_fn: Box<dyn Fn(&u32) -> u32 + 'static> =
                Box::new(move |attempt_id_iter: &u32| -> u32 {
                    //keep only attempt IDs which are greater than current attempt and ignore all previous attempts,
                    //because we definitely know that current attempt contains precommit message (so previous attempts can't have last precommit)

                    if *attempt_id_iter > current_attempt_id {
                        *attempt_id_iter
                    } else {
                        0
                    }
                });
            let new_last_precommit = new_last_precommit.modify(desc, &modifier_fn);

            //return updated state

            (true, new_last_precommit)
        })();

        //update last precommit attempt ID for the node if new precommit appears

        if attempt.check_precommit_received_from(src_idx) && //if we received a precommit from this node
       new_last_precommit.at(src_idx as usize) < &attempt_id
        //and last precommit attempt ID for this node is less than current attempt ID
        {
            //precommit appears for this node; check if we have precommit from the later attempt
            //also update last precommit attempt if new precommit was sent again from the node (newest precommit is later)

            if attempt_id > max_precommit_attempt_id || !new_voted_block_appears {
                assert!(has_voted_block);
                new_last_precommit = new_last_precommit.change(desc, src_idx as usize, attempt_id);
            }
        }

        //update state

        let new_attempts = self.attempts.push(desc, attempt); //attempts are sorted vector; no problem to push duplicate

        trace!(
            "...create updated attempts for round {}: {:?}",
            self.sequence_number,
            &new_attempts
        );

        Self::create(
            desc,
            new_precommitted_block,
            self.sequence_number,
            self.first_attempt.clone(),
            new_last_precommit,
            self.sent_blocks.clone(),
            self.signatures.clone(),
            new_attempts,
        )
    }

    fn make_one_impl(
        state: RoundStatePtr,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
    ) -> (RoundStatePtr, bool) {
        trace!("......actualizing round {}", state.get_sequence_number());

        let mut made_changes = false;
        let default_state = state;
        let state = get_impl(&*default_state);

        //update attempts

        trace!("......actualizing attempts");

        let first_attempt = state.first_attempt.clone();

        let default_state = if *first_attempt.at(src_idx as usize) == 0
            || first_attempt.at(src_idx as usize) > &attempt_id
        {
            //we have received new first attempt for the (round, node)

            trace!(
                "......new first attempt {} has been found for source {}",
                attempt_id,
                src_idx
            );

            made_changes = true;

            let first_attempt = first_attempt.change(desc, src_idx as usize, attempt_id);
            let new_round_state = Self::create(
                desc,
                state.precommitted_block.clone(),
                state.sequence_number,
                first_attempt,
                state.last_precommit.clone(),
                state.sent_blocks.clone(),
                state.signatures.clone(),
                state.attempts.clone(),
            );

            new_round_state
        } else {
            default_state
        };

        let state = get_impl(&*default_state);

        //if we have precommitted block, stop actualization

        if state.precommitted_block.is_some() {
            return (default_state, made_changes);
        }

        //create attempt if it does not exist

        let attempt = if let Some(attempt) = Self::get_attempt(&state.attempts, attempt_id) {
            attempt
        } else {
            trace!("......creating new attempt");

            SessionFactory::create_attempt(desc, attempt_id)
        };

        //forward actualization to attempt

        let (attempt, attempt_made_changes) = attempt.make_one(desc, src_idx, attempt_id, state);

        if !attempt_made_changes {
            return (default_state, made_changes);
        }

        //update state

        trace!("......create updated round {} state", state.sequence_number);

        let attempts = state.attempts.push(desc, attempt);
        let state = Self::create(
            desc,
            state.precommitted_block.clone(),
            state.sequence_number,
            state.first_attempt.clone(),
            state.last_precommit.clone(),
            state.sent_blocks.clone(),
            state.signatures.clone(),
            attempts,
        );

        (state, true)
    }

    /*
        Hash calculation
    */

    fn compute_hash(
        precommitted_block: &Option<SentBlockPtr>,
        sequence_number: u32,
        first_attempt: &AttemptIdVector,
        last_precommit: &AttemptIdVector,
        sent_blocks: &ApproveVector,
        signatures: &BlockCandidateSignatureVectorPtr,
        attempts: &AttemptVector,
    ) -> HashType {
        //The mistake below in a naming of fields repeats the mistake in Telegram's node
        //reference implementation of ValidatorSessionRoundState::create_hash function.
        //Such mistake is needed to generate the same hash as in Telegram's node

        crate::utils::compute_hash(ton::hashable::ValidatorSessionRound {
            locked_round: precommitted_block.get_ton_hash(),
            locked_block: sequence_number as ton::int,
            seqno: if precommitted_block.is_some() { 1 } else { 0 },
            precommitted: match first_attempt.get_ton_hash() {
                //hack for conversion: u32 -> bool
                0 => ::ton_api::ton::Bool::BoolFalse,
                _ => ::ton_api::ton::Bool::BoolTrue,
            },
            first_attempt: last_precommit.get_ton_hash(),
            approved_blocks: sent_blocks.get_ton_hash(),
            signatures: signatures.get_ton_hash(),
            attempts: attempts.get_ton_hash(),
        })
    }

    /*
        Blocks management
    */

    fn get_candidate(&self, block_id: &BlockId) -> Option<BlockCandidatePtr> {
        if self.sent_blocks.is_none() {
            return None;
        }

        let candidates = self.sent_blocks.as_ref().unwrap();

        for block_candidate in candidates.iter() {
            if block_candidate.get_id() == block_id {
                return Some(block_candidate.clone());
            }
        }

        None
    }

    /*
        Creation
    */

    fn new(
        precommitted_block: Option<SentBlockPtr>,
        sequence_number: u32,
        first_attempt: AttemptIdVector,
        last_precommit: AttemptIdVector,
        sent_blocks: ApproveVector,
        signatures: BlockCandidateSignatureVectorPtr,
        attempts: AttemptVector,
        hash: HashType,
        instance_counter: &CachedInstanceCounter,
    ) -> Self {
        Self {
            pool: SessionPool::Temp,
            precommitted_block: precommitted_block,
            sequence_number: sequence_number,
            first_attempt: first_attempt,
            last_precommit: last_precommit,
            sent_blocks: sent_blocks,
            signatures: signatures,
            attempts: attempts,
            hash: hash,
            instance_counter: instance_counter.clone(),
        }
    }

    fn create(
        desc: &mut dyn SessionDescription,
        precommitted_block: Option<SentBlockPtr>,
        sequence_number: u32,
        first_attempt: AttemptIdVector,
        last_precommit: AttemptIdVector,
        sent_blocks: ApproveVector,
        signatures: BlockCandidateSignatureVectorPtr,
        attempts: AttemptVector,
    ) -> RoundStatePtr {
        let hash = Self::compute_hash(
            &precommitted_block,
            sequence_number,
            &first_attempt,
            &last_precommit,
            &sent_blocks,
            &signatures,
            &attempts,
        );
        let body = Self::new(
            precommitted_block,
            sequence_number,
            first_attempt,
            last_precommit,
            sent_blocks,
            signatures,
            attempts,
            hash,
            desc.get_rounds_instance_counter(),
        );

        Self::create_temp_object(body, desc.get_cache())
    }

    pub(crate) fn create_current_round(
        desc: &mut dyn SessionDescription,
        sequence_number: u32,
    ) -> RoundStatePtr {
        let null_sign: BlockCandidateSignaturePtr = None;
        let signs = vec![null_sign; desc.get_total_nodes() as usize];
        let signatures = SessionFactory::create_vector(desc, signs);

        let attempt_null_indexes = vec![0; desc.get_total_nodes() as usize];
        let first_attempt =
            SessionFactory::create_vector_wrapper(desc, attempt_null_indexes.clone());
        let last_precommit = SessionFactory::create_vector_wrapper(desc, attempt_null_indexes);

        Self::create(
            desc,
            None,
            sequence_number,
            first_attempt,
            last_precommit,
            None,
            signatures,
            None,
        )
    }
}
