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

use catchain::serialize_tl_boxed_object;
use colored::Colorize;
use rand::Rng;
use std::{fmt, io::Write, time::{Duration, SystemTime}};
use ton_api::IntoBoxed;
use ton_types::{fail, KeyId};
use validator_session::*;

/*
    Constants
*/

const PERSISTENT_CACHE_SIZE: usize = 10000;
const TEMP_CACHE_SIZE: usize = 10000;

/*
    Implementation details for SessionDescription
*/

type CacheEntryPtr = Option<CacheEntry>;

struct SessionDescriptionImpl {
    options: SessionOptions,                      //validator session options
    hashes: Vec<PublicKeyHash>,                   //hashes
    persistent_objects_cache: Vec<CacheEntryPtr>, //cache of persistent objects
    temp_objects_cache: Vec<CacheEntryPtr>,       //cache of temporary objects objects
    rng: rand::rngs::ThreadRng,                   //random generator
    metrics_receiver: catchain::utils::MetricsHandle,  //receiver for profiling metrics
    sent_blocks_instance_counter: CachedInstanceCounter, //instance counter for sent blocks
    block_candidate_signatures_instance_counter: CachedInstanceCounter, //instance counter for block candidate signatures
    block_candidates_instance_counter: CachedInstanceCounter, //instance counter for block candidates
    vote_candidates_instance_counter: CachedInstanceCounter,  //instance counter for vote candidates
    round_attempts_instance_counter: CachedInstanceCounter,   //instance counter for round attempts
    rounds_instance_counter: CachedInstanceCounter,           //instance counter for rounds
    old_rounds_instance_counter: CachedInstanceCounter,       //instance counter for old rounds
    session_states_instance_counter: CachedInstanceCounter,   //instance counter for session states
    integer_vectors_instance_counter: CachedInstanceCounter,  //instance counter for integer vectors
    bool_vectors_instance_counter: CachedInstanceCounter,     //instance counter for bool vectors
    block_candidate_vectors_instance_counter: CachedInstanceCounter, //instance counter for block candidate vectors
    block_candidate_signature_vectors_instance_counter: CachedInstanceCounter, //instance counter for block candidate signatures vectors
    vote_candidate_vectors_instance_counter: CachedInstanceCounter, //instance counter for vote candidate vectors
    round_attempt_vectors_instance_counter: CachedInstanceCounter, //instance counter for round attempt vectors
    old_round_vectors_instance_counter: CachedInstanceCounter, //instance counter for old round vectors
}

/*
    Implementation for public SessionDescription trait
*/

impl SessionDescription for SessionDescriptionImpl {
    /*
        General purpose methods & accessors
    */

    fn opts(&self) -> &SessionOptions {
        &self.options
    }

    fn get_cache(&mut self) -> &mut dyn SessionCache {
        self
    }

    /*
        Validators management
    */

    fn get_source_public_key_hash(&self, src_idx: u32) -> &PublicKeyHash {
        &self.hashes[src_idx as usize]
    }

    fn get_source_public_key(&self, _src_idx: u32) -> &PublicKey {
        unreachable!();
    }

    fn get_source_adnl_id(&self, _src_idx: u32) -> &PublicKeyHash {
        unreachable!();
    }

    fn get_source_index(&self, public_key_hash: &PublicKeyHash) -> u32 {
        public_key_hash.data()[0] as u32
    }

    fn get_node_weight(&self, _src_idx: u32) -> ValidatorWeight {
        1
    }

    fn get_total_nodes(&self) -> u32 {
        self.hashes.len() as u32
    }

    fn get_self_idx(&self) -> u32 {
        unreachable!();
    }

    fn export_nodes(&self) -> Vec<PublicKeyHash> {
        unreachable!();
    }

    fn export_full_nodes(&self) -> Vec<PublicKey> {
        unreachable!();
    }

    fn export_catchain_nodes(&self) -> Vec<CatchainNode> {
        unreachable!();
    }

    /*
        Weights & priorities
    */

    fn get_cutoff_weight(&self) -> ValidatorWeight {
        (2 * self.hashes.len() / 3 + 1) as u64
    }

    fn get_total_weight(&self) -> ValidatorWeight {
        self.hashes.len() as ValidatorWeight
    }

    fn get_node_priority(&self, src_idx: u32, round: u32) -> i32 {
        let round = round % self.get_total_nodes();
        let src_idx = if src_idx < round {
            src_idx + self.get_total_nodes()
        } else {
            src_idx
        };

        if src_idx - round < self.options.round_candidates {
            return (src_idx - round) as i32;
        }

        -1
    }

    fn get_max_priority(&self) -> u32 {
        self.options.round_candidates - 1
    }

    fn get_vote_for_author(&self, attempt: u32) -> u32 {
        attempt % self.get_total_nodes()
    }

    /*
        Time management
    */

    fn set_time(&mut self, _time: std::time::SystemTime) {
        unreachable!();
    }

    fn get_time(&self) -> SystemTime {
        SystemTime::now()
    }

    fn is_in_future(&self, time: SystemTime) -> bool {
        time > self.get_time()
    }

    fn is_in_past(&self, time: SystemTime) -> bool {
        time < self.get_time()
    }

    fn get_unixtime(&self, ts: u64) -> u32 {
        (ts >> 32) as u32
    }

    fn get_attempt_sequence_number(&self, ts: u64) -> u32 {
        let round_attempt_duration_in_secs: u32 =
            self.options.round_attempt_duration.as_secs() as u32;
        (self.get_unixtime(ts) / round_attempt_duration_in_secs) as u32
    }

    fn get_ts(&self) -> u64 {
        let now = self.get_time();
        let time_elapsed = match now.duration_since(std::time::UNIX_EPOCH) {
            Ok(elapsed) => elapsed.as_secs_f64(),
            Err(_err) => {
                log::error!("SessionDescription::get_ts: can't get system time");
                panic!("SessionDescription::get_ts");
            }
        };

        const TS_INTEGER_PART_MULTIPLIER: u64 = 1u64 << 32;

        let int_part = time_elapsed as u32;
        let frac_part =
            ((TS_INTEGER_PART_MULTIPLIER as f64) * (time_elapsed - (int_part as f64))) as u64;

        assert!(frac_part < TS_INTEGER_PART_MULTIPLIER);

        ((int_part as u64) << 32) + frac_part
    }

    fn get_delay(&self, mut _priority: u32) -> std::time::Duration {
        std::time::Duration::from_millis(0)
    }

    fn get_empty_block_delay(&self) -> std::time::Duration {
        std::time::Duration::from_millis(0)
    }

    fn get_attempt_start_at(&self, attempt: u32) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + attempt * self.options.round_attempt_duration
    }

    /*
        Signatures
    */

    fn candidate_id(
        &self,
        src_idx: u32,
        root_hash: &BlockHash,
        file_hash: &BlockHash,
        collated_data_file_hash: &BlockHash,
    ) -> BlockId {
        let candidate_id = ::ton_api::ton::validator_session::candidateid::CandidateId {
            src: self.get_source_public_key_hash(src_idx).data().into(),
            root_hash: root_hash.clone().into(),
            file_hash: file_hash.clone().into(),
            collated_data_file_hash: collated_data_file_hash.clone().into(),
        }
        .into_boxed();
        let serialized_candidate_id = serialize_tl_boxed_object!(&candidate_id);

        catchain::utils::get_hash(&serialized_candidate_id)
    }

    fn check_signature(
        &self,
        _root_hash: &BlockHash,
        _file_hash: &BlockHash,
        _src_idx: u32,
        signature: &BlockSignature,
    ) -> Result<()> {
        if signature.len() == 0 {
            fail!("wrong size");
        }

        if signature[0] == 126 {
            Ok(())
        } else {
            fail!("invalid")
        }
    }

    fn check_approve_signature(
        &self,
        _root_hash: &BlockHash,
        _file_hash: &BlockHash,
        _src_idx: u32,
        signature: &BlockSignature,
    ) -> Result<()> {
        if signature.len() == 0 {
            fail!("wrong size");
        }

        if signature[0] == 127 {
            Ok(())
        } else {
            fail!("invalid")
        }
    }

    /*
        Random
    */

    fn generate_random_usize(&mut self) -> usize {
        self.rng.gen()
    }

    /*
        Metrics
    */

    fn get_metrics_receiver(&self) -> &catchain::utils::MetricsHandle {
        &self.metrics_receiver
    }

    fn get_sent_blocks_instance_counter(&self) -> &CachedInstanceCounter {
        &self.sent_blocks_instance_counter
    }

    fn get_block_candidate_signatures_instance_counter(&self) -> &CachedInstanceCounter {
        &self.block_candidate_signatures_instance_counter
    }

    fn get_block_candidates_instance_counter(&self) -> &CachedInstanceCounter {
        &self.block_candidates_instance_counter
    }

    fn get_vote_candidates_instance_counter(&self) -> &CachedInstanceCounter {
        &self.vote_candidates_instance_counter
    }

    fn get_round_attempts_instance_counter(&self) -> &CachedInstanceCounter {
        &self.round_attempts_instance_counter
    }

    fn get_rounds_instance_counter(&self) -> &CachedInstanceCounter {
        &self.rounds_instance_counter
    }

    fn get_old_rounds_instance_counter(&self) -> &CachedInstanceCounter {
        &self.old_rounds_instance_counter
    }

    fn get_session_states_instance_counter(&self) -> &CachedInstanceCounter {
        &self.session_states_instance_counter
    }

    fn get_integer_vectors_instance_counter(&self) -> &CachedInstanceCounter {
        &self.integer_vectors_instance_counter
    }

    fn get_bool_vectors_instance_counter(&self) -> &CachedInstanceCounter {
        &self.bool_vectors_instance_counter
    }

    fn get_block_candidate_vectors_instance_counter(&self) -> &CachedInstanceCounter {
        &self.block_candidate_vectors_instance_counter
    }

    fn get_block_candidate_signature_vectors_instance_counter(&self) -> &CachedInstanceCounter {
        &self.block_candidate_signature_vectors_instance_counter
    }

    fn get_vote_candidate_vectors_instance_counter(&self) -> &CachedInstanceCounter {
        &self.vote_candidate_vectors_instance_counter
    }

    fn get_round_attempt_vectors_instance_counter(&self) -> &CachedInstanceCounter {
        &self.round_attempt_vectors_instance_counter
    }

    fn get_old_round_vectors_instance_counter(&self) -> &CachedInstanceCounter {
        &self.old_round_vectors_instance_counter
    }
}

/*
    Implementation for public Cache trait
*/

impl SessionCache for SessionDescriptionImpl {
    fn get_cache_entry_by_hash(&self, hash: HashType, allow_temp: bool) -> Option<&CacheEntry> {
        let mut cache_index = hash as usize % PERSISTENT_CACHE_SIZE;

        if let Some(ref entry) = self.persistent_objects_cache[cache_index] {
            return Some(&entry);
        }

        if !allow_temp {
            return None;
        }

        cache_index = hash as usize % TEMP_CACHE_SIZE;

        match &self.temp_objects_cache[cache_index] {
            Some(entry) => Some(&entry),
            _ => None,
        }
    }

    fn add_cache_entry(&mut self, hash: HashType, cache_entry: CacheEntry, pool: SessionPool) {
        let pool = match pool {
            SessionPool::Persistent => &mut self.persistent_objects_cache,
            SessionPool::Temp => &mut self.temp_objects_cache,
        };
        let cache_index = hash as usize % pool.len();

        pool[cache_index] = Some(cache_entry);
    }

    fn clear_temp_memory(&mut self) {
        let temp_pool_size = self.temp_objects_cache.len();

        self.temp_objects_cache.clear();
        self.temp_objects_cache.resize(temp_pool_size, None);
    }

    fn increment_reuse_counter(&mut self, _pool: SessionPool) {}
}

/*
    Implementation for public Display & Debug
*/

impl fmt::Display for SessionDescriptionImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Debug for SessionDescriptionImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionDescription")
            .field("options", &self.options)
            .finish()
    }
}

/*
    Implementation internals of SessionDescriptionImpl
*/

impl SessionDescriptionImpl {
    pub(crate) fn new(options: &SessionOptions, total_nodes: u32) -> Self {
        let metrics_receiver = catchain::utils::MetricsHandle::new(Some(Duration::from_secs(30)));

        let mut hashes = Vec::new();

        for i in 0..total_nodes {
            let mut hash: [u8; 32] = [0; 32];

            hash[0] = i as u8;

            let hash = KeyId::from_data(hash);
            // let hash = PublicKeyHash

            hashes.push(hash);
        }

        let body = Self {
            options: *options,
            persistent_objects_cache: vec![None; PERSISTENT_CACHE_SIZE],
            temp_objects_cache: vec![None; TEMP_CACHE_SIZE],
            rng: rand::thread_rng(),
            hashes,
            sent_blocks_instance_counter: CachedInstanceCounter::new(
                &metrics_receiver,
                &"sent_blocks".to_string(),
            ),
            block_candidate_signatures_instance_counter: CachedInstanceCounter::new(
                &metrics_receiver,
                &"block_candidates_signatures".to_string(),
            ),
            block_candidates_instance_counter: CachedInstanceCounter::new(
                &metrics_receiver,
                &"block_candidates".to_string(),
            ),
            vote_candidates_instance_counter: CachedInstanceCounter::new(
                &metrics_receiver,
                &"vote_candidates".to_string(),
            ),
            round_attempts_instance_counter: CachedInstanceCounter::new(
                &metrics_receiver,
                &"round_attempts".to_string(),
            ),
            rounds_instance_counter: CachedInstanceCounter::new(
                &metrics_receiver,
                &"rounds".to_string(),
            ),
            old_rounds_instance_counter: CachedInstanceCounter::new(
                &metrics_receiver,
                &"old_rounds".to_string(),
            ),
            session_states_instance_counter: CachedInstanceCounter::new(
                &metrics_receiver,
                &"session_states".to_string(),
            ),
            integer_vectors_instance_counter: CachedInstanceCounter::new(
                &metrics_receiver,
                &"integer_vectors".to_string(),
            ),
            bool_vectors_instance_counter: CachedInstanceCounter::new(
                &metrics_receiver,
                &"bool_vectors".to_string(),
            ),
            block_candidate_vectors_instance_counter: CachedInstanceCounter::new(
                &metrics_receiver,
                &"block_candidate_vectors".to_string(),
            ),
            block_candidate_signature_vectors_instance_counter: CachedInstanceCounter::new(
                &metrics_receiver,
                &"block_candidate_signature_vectors".to_string(),
            ),
            vote_candidate_vectors_instance_counter: CachedInstanceCounter::new(
                &metrics_receiver,
                &"vote_candidate_vectors".to_string(),
            ),
            round_attempt_vectors_instance_counter: CachedInstanceCounter::new(
                &metrics_receiver,
                &"round_attempt_vectors".to_string(),
            ),
            old_round_vectors_instance_counter: CachedInstanceCounter::new(
                &metrics_receiver,
                &"old_round_vectors".to_string(),
            ),
            metrics_receiver: metrics_receiver,
        };

        body
    }
}

fn check_empty_actions(
    description: &mut dyn SessionDescription,
    state: &SessionStatePtr,
    attempt_id: u32,
) {
    let total_nodes = description.get_total_nodes();

    for i in 0..total_nodes {
        let action = state.create_action(description, i, attempt_id);

        assert!(action.is_some());

        let action = action.unwrap();

        assert!(
            matches!(action, ton::Message::ValidatorSession_Message_Empty(_)),
            "Action should be Empty instead of {:?}", action
        );
    }
}

fn test_state_hashes_part1(options: &SessionOptions, total_nodes: u32) {
    let mut description = SessionDescriptionImpl::new(&options, total_nodes);
    let now = std::time::SystemTime::now();

    {
        //check primitives

        let x1: u32 = 1;
        let x2: bool = true;
        let x3: Option<u32> = None;

        assert!(x1.get_hash() == 932806723);
        assert!(x2.get_hash() == 831612713);
        assert!(x3.get_hash() == 0);
    }

    {
        //check empty state hash

        let state = SessionFactory::create_state(&mut description);

        assert!(state.get_hash() == 321933076);
    }

    {
        //check bool vector hash

        let v: Vec<bool> = vec![false; 3];
        let v = SessionFactory::create_bool_vector(&mut description, v);
        let v = v.change(&mut description, 0, true);

        assert!(v.get_hash() == 2502091135);
    }

    {
        //check vec of u32

        let v: Vec<u32> = vec![0; 3];
        let v = SessionFactory::create_vector_wrapper(&mut description, v);
        let v = v.change(&mut description, 0, 1);
        let v = v.change(&mut description, 1, 1);

        assert!(v.get_hash() == 2424132713);
    }

    let zero_hash = BlockHash::default().into();
    let c1 = description.candidate_id(0, &zero_hash, &zero_hash, &zero_hash);
    let c2 = description.candidate_id(1, &zero_hash, &zero_hash, &zero_hash);

    assert!(c1 != c2);

    let zero_hash: ton_types::UInt256 = zero_hash.into();
    let mut state = SessionFactory::create_state(&mut description);
    let mut attempt_id = 1000000000;

    check_empty_actions(&mut description, &state, attempt_id);

    {
        //check block submission

        let message = ton::message::SubmittedBlock {
            round: 0,
            root_hash: zero_hash.clone(),
            file_hash: zero_hash.clone(),
            collated_data_file_hash: zero_hash.clone(),
        }
        .into_boxed();

        state = state.apply_action(
            &mut description,
            1,
            attempt_id,
            &message,
            now.clone(),
            now.clone(),
        );
        state = state.move_to_persistent(description.get_cache());

        assert!(state.get_hash() == 3718863710);
    }

    check_empty_actions(&mut description, &state, attempt_id);

    {
        //check approving & signing

        for i in 0..total_nodes {
            let block = state.choose_block_to_sign(&mut description, i as u32);

            assert!(!block.is_some());

            let blocks_to_approve = state.choose_blocks_to_approve(&mut description, i as u32);

            assert!(blocks_to_approve.len() == 2);
            assert!(blocks_to_approve[0].is_some());
            assert!(blocks_to_approve[0].get_id() == &c2);
            assert!(blocks_to_approve[1].is_none());
            assert!(blocks_to_approve[1].get_id() == &*SKIP_ROUND_CANDIDATE_BLOCKID);
        }
    }

    {
        //check approving with signature #1

        for i in 0..2 * total_nodes / 3 {
            let signature = ::ton_api::ton::bytes(vec![127; 1]);
            let action = ton::message::ApprovedBlock {
                round: 0,
                candidate: c2.clone().into(),
                signature: signature,
            }
            .into_boxed();

            state = state.apply_action(
                &mut description,
                i as u32,
                attempt_id,
                &action,
                now.clone(),
                now.clone(),
            );
            state = state.move_to_persistent(description.get_cache());
        }

        assert!(state.get_hash() == 3873054883);
    }

    check_empty_actions(&mut description, &state, attempt_id);

    {
        //check approving with signature #2

        for i in 2 * total_nodes / 3..total_nodes {
            let signature = ::ton_api::ton::bytes(vec![127; 1]);
            let action = ton::message::ApprovedBlock {
                round: 0,
                candidate: c2.clone().into(),
                signature: signature,
            }
            .into_boxed();

            state = state.apply_action(
                &mut description,
                i as u32,
                attempt_id,
                &action,
                now.clone(),
                now.clone(),
            );
            state = state.move_to_persistent(description.get_cache());
        }

        assert!(state.get_hash() == 1013875306);
    }

    {
        //check approving & signing

        for i in 0..total_nodes {
            let block = state.choose_block_to_sign(&mut description, i as u32);

            assert!(!block.is_some());

            let blocks_to_approve = state.choose_blocks_to_approve(&mut description, i as u32);

            assert!(blocks_to_approve.len() == 1);
            assert!(blocks_to_approve[0].is_none());
            assert!(blocks_to_approve[0].get_id() == &*SKIP_ROUND_CANDIDATE_BLOCKID);
        }
    }

    {
        //check voting

        for i in 0..total_nodes {
            let action = state.create_action(&description, i, attempt_id);

            assert!(action.is_some());

            let action = action.unwrap();

            assert!(
                matches!(action, ton::Message::ValidatorSession_Message_Vote(_)),
                "Action should be Vote instead of {:?}", &action
            );

            state = state.apply_action(
                &mut description,
                i,
                attempt_id,
                &action,
                now.clone(),
                now.clone(),
            );
            state = state.move_to_persistent(description.get_cache());

            let action = state.create_action(&description, i, attempt_id);

            assert!(action.is_some());

            let action = action.unwrap();

            if i < 2 * total_nodes / 3 {
                assert!(
                    matches!(action, ton::Message::ValidatorSession_Message_Empty(_)),
                    "Action should be Empty instead of {:?}", &action
                );
            } else {
                assert!(
                    matches!(action, ton::Message::ValidatorSession_Message_Precommit(_)),
                    "Action should be Precommit instead of {:?}", &action
                );
            }
        }

        assert!(state.get_hash() == 2802687479);
    }

    {
        //check attempts

        for j in 1..options.max_round_attempts {
            let action = state.create_action(&description, 0, attempt_id + j);

            assert!(action.is_some());

            let action = action.unwrap();

            assert!(
                matches!(action, ton::Message::ValidatorSession_Message_Vote(_)),
                "Action should be Vote instead of {:?}",
                action
            );
        }

        for j in options.max_round_attempts..options.max_round_attempts + 10 {
            let action = state.create_action(&description, 0, attempt_id + j);

            assert!(action.is_some());

            let action = action.unwrap();

            assert!(
                matches!(action, ton::Message::ValidatorSession_Message_Empty(_)),
                "Action should be Empty instead of {:?}", &action
            );
        }
    }

    {
        //check precommits #1 (fast voting)

        let mut state = state.clone();
        let mut attempt_id = attempt_id;

        for i in 0..total_nodes {
            let action = state.create_action(&description, i, attempt_id);

            assert!(action.is_some());

            let action = action.unwrap();

            if i <= 2 * total_nodes / 3 {
                assert!(
                    matches!(action, ton::Message::ValidatorSession_Message_Precommit(_)),
                    "Action for validator {} should be Precommit instead of {:?}",
                    i, &action
                );
            } else {
                assert!(
                    matches!(action, ton::Message::ValidatorSession_Message_Empty(_)),
                    "Action for validator {} should be Empty instead of {:?}",
                    i, &action
                );
            }

            state = state.apply_action(
                &mut description,
                i,
                attempt_id,
                &action,
                now.clone(),
                now.clone(),
            );
            state = state.move_to_persistent(description.get_cache());

            let action = state.create_action(&description, i, attempt_id);

            assert!(action.is_some());

            let action = action.unwrap();

            assert!(
                matches!(action, ton::Message::ValidatorSession_Message_Empty(_)),
                "Action should be Empty instead of {:?}", action
            );
        }

        assert!(state.get_hash() == 3568563261);

        attempt_id += 10;

        check_empty_actions(&mut description, &state, attempt_id);

        //check signing candidate

        for i in 0..total_nodes {
            let block = state.choose_block_to_sign(&mut description, i as u32);

            assert!(block.is_some());
            assert!(block.unwrap().get_id() == &c2);
        }

        //check commits

        for i in 0..2 * total_nodes / 3 {
            let signature = ::ton_api::ton::bytes(vec![126; 1]);
            let action = ton::message::Commit {
                round: 0,
                candidate: c2.clone().into(),
                signature: signature,
            }
            .into_boxed();

            state = state.apply_action(
                &mut description,
                i as u32,
                attempt_id,
                &action,
                now.clone(),
                now.clone(),
            );
            state = state.move_to_persistent(description.get_cache());
        }

        assert!(state.get_current_round_sequence_number() == 0);
        assert!(state.get_hash() == 1464332123);

        for i in 2 * total_nodes / 3..total_nodes {
            let signature = ::ton_api::ton::bytes(vec![126; 1]);
            let action = ton::message::Commit {
                round: 0,
                candidate: c2.clone().into(),
                signature: signature,
            }
            .into_boxed();

            state = state.apply_action(
                &mut description,
                i as u32,
                attempt_id,
                &action,
                now.clone(),
                now.clone(),
            );
            state = state.move_to_persistent(description.get_cache());
        }

        //check round switching

        assert!(state.get_current_round_sequence_number() == 1);

        assert!(state.get_hash() == 943640875);

        //check signatures

        let signatures = state.get_committed_block_signatures(0);

        for signature in signatures.get_iter() {
            assert!(signature.is_some());
        }
    }

    {
        //check precommits #2 (slow voting)

        for i in 0..total_nodes / 3 {
            let action = state.create_action(&description, i, attempt_id);

            assert!(action.is_some());

            let action = action.unwrap();

            assert!(
                matches!(action, ton::Message::ValidatorSession_Message_Precommit(_)),
                "Action for validator {} should be Precommit instead of {:?}",
                i, action
            );

            state = state.apply_action(
                &mut description,
                i,
                attempt_id,
                &action,
                now.clone(),
                now.clone(),
            );
            state = state.move_to_persistent(description.get_cache());

            let action = state.create_action(&description, i, attempt_id);

            assert!(action.is_some());

            let action = action.unwrap();

            assert!(
                matches!(action, ton::Message::ValidatorSession_Message_Empty(_)),
                "Action should be Empty instead of {:?}", action
            );
        }

        assert!(state.get_hash() == 1629278980);

        attempt_id += options.max_round_attempts - 1;

        loop {
            attempt_id += 1;

            for i in 0..total_nodes {
                let action = state.create_action(&description, i, attempt_id);

                assert!(action.is_some());

                let action = action.unwrap();

                if i < total_nodes / 3 {
                    assert!(
                        matches!(action, ton::Message::ValidatorSession_Message_Vote(_)),
                        "Action for validator {} should be Vote instead of {:?}",
                        i, &action
                    );
                } else {
                    assert!(
                        matches!(action, ton::Message::ValidatorSession_Message_Empty(_)),
                        "Action for validator {} should be Empty instead of {:?}",
                        i, &action
                    );
                }

                state = state.apply_action(
                    &mut description,
                    i,
                    attempt_id,
                    &action,
                    now.clone(),
                    now.clone(),
                );
                state = state.move_to_persistent(description.get_cache());

                let action = state.create_action(&description, i, attempt_id);

                assert!(action.is_some());

                let action = action.unwrap();

                assert!(
                    matches!(action, ton::Message::ValidatorSession_Message_Empty(_)),
                    "Action should be Empty instead of {:?}", &action
                );
            }

            description.get_cache().clear_temp_memory();

            if description.get_vote_for_author(attempt_id) >= total_nodes / 3 {
                break;
            }
        }

        assert!(state.get_hash() == 4130462109);
    }

    {
        //send new candidate for old round from another source

        let message = ton::message::SubmittedBlock {
            round: 0,
            root_hash: zero_hash.clone(),
            file_hash: zero_hash.clone(),
            collated_data_file_hash: zero_hash.clone(),
        }
        .into_boxed();

        state = state.apply_action(
            &mut description,
            0,
            attempt_id,
            &message,
            now.clone(),
            now.clone(),
        );
        state = state.move_to_persistent(description.get_cache());

        assert!(state.get_hash() == 3571838886);
    }

    let mut idx = description.get_vote_for_author(attempt_id);

    {
        //checking votes

        for i in 0..total_nodes {
            assert!(state.check_need_generate_vote_for(&description, i, attempt_id) == (i == idx));
        }
    }

    {
        //check approvals for another candidate

        for i in 0..total_nodes {
            let signature = ::ton_api::ton::bytes(vec![127; 1]);
            let action = ton::message::ApprovedBlock {
                round: 0,
                candidate: c1.clone().into(),
                signature: signature,
            }
            .into_boxed();

            state = state.apply_action(
                &mut description,
                i as u32,
                attempt_id,
                &action,
                now.clone(),
                now.clone(),
            );
            state = state.move_to_persistent(description.get_cache());
        }

        assert!(state.get_hash() == 2876715556);
    }

    {
        //check vote-for generation

        let mut action = state.generate_vote_for(&mut description, idx, attempt_id);

        if let ton::Message::ValidatorSession_Message_VoteFor(ref mut vote_for) = action {
            vote_for.candidate = c1.clone().into();
        } else {
            unreachable!();
        }

        state = state.apply_action(
            &mut description,
            idx,
            attempt_id,
            &action,
            now.clone(),
            now.clone(),
        );
        state = state.move_to_persistent(description.get_cache());

        assert!(state.get_hash() == 645596951);

        println!("state: {}", state.dump(&description));
    }

    {
        //check voting

        for i in 0..total_nodes {
            let action = state.create_action(&description, i, attempt_id);

            assert!(action.is_some());

            let action = action.unwrap();

            if i < total_nodes / 3 {
                assert!(
                    matches!(action, ton::Message::ValidatorSession_Message_Empty(_)),
                    "Action for validator {} should be Empty instead of {:?}",
                    i, &action
                );
            } else {
                assert!(
                    matches!(action, ton::Message::ValidatorSession_Message_Vote(_)),
                    "Action for validator {} should be Vote instead of {:?}",
                    i, &action
                );
            }

            state = state.apply_action(
                &mut description,
                i as u32,
                attempt_id,
                &action,
                now.clone(),
                now.clone(),
            );
            state = state.move_to_persistent(description.get_cache());
        }

        assert!(state.get_hash() == 224182806);
    }

    attempt_id += 1;
    idx = description.get_vote_for_author(attempt_id);

    {
        //check voting results

        for i in 0..total_nodes {
            assert!(
                state.check_need_generate_vote_for(&description, i, attempt_id) == (i == idx),
                "check_need_generate_vote_for failed for source {} and attempt {}",
                i, attempt_id
            );
        }

        for i in 0..total_nodes {
            let action = state.create_action(&description, i, attempt_id);

            assert!(action.is_some());

            let action = action.unwrap();

            assert!(
                matches!(action, ton::Message::ValidatorSession_Message_Empty(_)),
                "Action for validator {} should be Empty instead of {:?}",
                i, &action
            );

            state = state.apply_action(
                &mut description,
                i as u32,
                attempt_id,
                &action,
                now.clone(),
                now.clone(),
            );
            state = state.move_to_persistent(description.get_cache());
        }

        assert!(state.get_hash() == 3203236222);
    }

    {
        //check vote-for generation

        let mut action = state.generate_vote_for(&mut description, idx, attempt_id);

        if let ton::Message::ValidatorSession_Message_VoteFor(ref mut vote_for) = action {
            vote_for.candidate = c1.clone().into();
        } else {
            unreachable!();
        }

        state = state.apply_action(
            &mut description,
            idx,
            attempt_id,
            &action,
            now.clone(),
            now.clone(),
        );
        state = state.move_to_persistent(description.get_cache());

        assert!(state.get_hash() == 870429773);
    }

    {
        //check voting

        for i in 0..total_nodes {
            let action = state.create_action(&description, i, attempt_id);

            assert!(action.is_some());

            let action = action.unwrap();

            assert!(
                matches!(action, ton::Message::ValidatorSession_Message_Vote(_)),
                "Action for validator {} should be Vote instead of {:?}",
                i, &action
            );

            state = state.apply_action(
                &mut description,
                i as u32,
                attempt_id,
                &action,
                now.clone(),
                now.clone(),
            );
            state = state.move_to_persistent(description.get_cache());
        }

        assert!(state.get_hash() == 1237048486);
    }

    {
        //check precommits

        for i in 0..total_nodes / 3 {
            let action = state.create_action(&description, i, attempt_id);

            assert!(action.is_some());

            let action = action.unwrap();

            assert!(
                matches!(action, ton::Message::ValidatorSession_Message_Precommit(_)),
                "Action for validator {} should be Precommit instead of {:?}",
                i, &action
            );

            state = state.apply_action(
                &mut description,
                i as u32,
                attempt_id,
                &action,
                now.clone(),
                now.clone(),
            );
            state = state.move_to_persistent(description.get_cache());

            let action = state.create_action(&description, i, attempt_id);

            assert!(action.is_some());

            let action = action.unwrap();

            assert!(
                matches!(action, ton::Message::ValidatorSession_Message_Empty(_)),
                "Action for validator {} should be Empty instead of {:?}",
                i, &action
            );
        }

        assert!(state.get_hash() == 2558822588);
    }

    attempt_id += 1;
    idx = description.get_vote_for_author(attempt_id);

    {
        //check vote-for generation

        let mut action = state.generate_vote_for(&mut description, idx, attempt_id);

        if let ton::Message::ValidatorSession_Message_VoteFor(ref mut vote_for) = action {
            vote_for.candidate = c1.clone().into();
        } else {
            unreachable!();
        }

        state = state.apply_action(
            &mut description,
            idx,
            attempt_id,
            &action,
            now.clone(),
            now.clone(),
        );
        state = state.move_to_persistent(description.get_cache());

        assert!(state.get_hash() == 3437384989);
    }

    {
        //check voting

        for i in 0..total_nodes {
            let action = state.create_action(&description, i, attempt_id);

            assert!(action.is_some());

            let action = action.unwrap();

            assert!(
                matches!(action, ton::Message::ValidatorSession_Message_Vote(_)),
                "Action for validator {} should be Vote instead of {:?}",
                i, &action
            );

            state = state.apply_action(
                &mut description,
                i as u32,
                attempt_id,
                &action,
                now.clone(),
                now.clone(),
            );
            state = state.move_to_persistent(description.get_cache());
        }

        assert!(state.get_hash() == 193196250);
    }

    {
        //check precommits

        for i in 0..total_nodes {
            let action = state.create_action(&description, i, attempt_id);

            assert!(action.is_some());

            let action = action.unwrap();

            if i <= 2 * total_nodes / 3 {
                assert!(
                    matches!(action, ton::Message::ValidatorSession_Message_Precommit(_)),
                    "Action for validator {} should be Precommit instead of {:?}",
                    i, &action
                );
            } else {
                assert!(
                    matches!(action, ton::Message::ValidatorSession_Message_Empty(_)),
                    "Action for validator {} should be Empty instead of {:?}",
                    i, &action
                );
            }

            state = state.apply_action(
                &mut description,
                i as u32,
                attempt_id,
                &action,
                now.clone(),
                now.clone(),
            );
            state = state.move_to_persistent(description.get_cache());

            let action = state.create_action(&description, i, attempt_id);

            assert!(action.is_some());

            let action = action.unwrap();

            assert!(
                matches!(action, ton::Message::ValidatorSession_Message_Empty(_)),
                "Action for validator {} should be Empty instead of {:?}",
                i, &action
            );
        }

        assert!(state.get_hash() == 2711711321);
    }
}

fn get_sign_string(signature: &BlockCandidateSignaturePtr) -> String {
    if signature.is_none() {
        return "".to_string();
    }

    match std::str::from_utf8(&signature.as_ref().unwrap().get_signature().0) {
        Ok(s) => s.to_string(),
        _ => "Error".to_string(),
    }
}

fn test_state_hashes_part2(options: &SessionOptions, total_nodes: u32) {
    let mut description = SessionDescriptionImpl::new(&options, total_nodes);
    let zero_hash = BlockHash::default();
    let now = std::time::SystemTime::now();

    let sig1 = SessionFactory::create_block_candidate_signature(
        &mut description,
        ::ton_api::ton::bytes(vec!['a' as u8; 1]),
    );
    let sig2 = SessionFactory::create_block_candidate_signature(
        &mut description,
        ::ton_api::ton::bytes(vec!['b' as u8; 1]),
    );
    let sig3 = SessionFactory::create_block_candidate_signature(
        &mut description,
        ::ton_api::ton::bytes(vec!['c' as u8; 1]),
    );
    let sig4 = SessionFactory::create_block_candidate_signature(
        &mut description,
        ::ton_api::ton::bytes(vec!['d' as u8; 1]),
    );

    {
        let m1 = sig1.merge(&sig2, &mut description);
        assert!(m1.as_ref().unwrap().get_signature().0[0] == 'a' as u8);
        let m2 = sig2.merge(&sig1, &mut description);
        assert!(m2.as_ref().unwrap().get_signature().0[0] == 'a' as u8);
    }

    let sig_vec_null: Vec<BlockCandidateSignaturePtr> =
        vec![None; description.get_total_nodes() as usize];
    let mut sig_vec1 = SessionFactory::create_vector(&mut description, sig_vec_null.clone());
    let mut sig_vec2 = SessionFactory::create_vector(&mut description, sig_vec_null.clone());

    sig_vec1 = sig_vec1.change(&mut description, 0, sig1.clone());
    sig_vec1 = sig_vec1.change(&mut description, 1, sig3.clone());
    sig_vec2 = sig_vec2.change(&mut description, 0, sig4.clone());
    sig_vec2 = sig_vec2.change(&mut description, 1, sig2.clone());
    sig_vec2 = sig_vec2.change(&mut description, 2, sig4.clone());

    assert!(sig_vec1.get_hash() == 1072513633);
    assert!(sig_vec2.get_hash() == 2228639731);

    {
        //check vectors merge

        let m1 = sig_vec1.merge(&sig_vec2, &mut description);

        assert!(get_sign_string(m1.at(0)) == "a");
        assert!(get_sign_string(m1.at(1)) == "b");
        assert!(get_sign_string(m1.at(2)) == "d");
        assert!(get_sign_string(m1.at(3)) == "");

        let m2 = sig_vec2.merge(&sig_vec1, &mut description);

        assert!(get_sign_string(m2.at(0)) == "a");
        assert!(get_sign_string(m2.at(1)) == "b");
        assert!(get_sign_string(m2.at(2)) == "d");
        assert!(get_sign_string(m2.at(3)) == "");

        assert!(m1.get_hash() == 3697200380 && m2.get_hash() == 3697200380);
    }

    let sentb = SessionFactory::create_sent_block(
        &mut description,
        0,
        zero_hash.clone(),
        zero_hash.clone(),
        zero_hash.clone(),
        now.clone(),
        now.clone(),
    );
    let cand1 =
        SessionFactory::create_block_candidate(&mut description, sentb.clone(), sig_vec1.clone());
    let cand2 =
        SessionFactory::create_block_candidate(&mut description, sentb.clone(), sig_vec2.clone());

    {
        //check candidates merge

        let m1 = cand1.merge(&cand2, &mut description);

        assert!(m1.get_block() == &sentb);
        assert!(get_sign_string(m1.get_approvers_list().at(0)) == "a");
        assert!(get_sign_string(m1.get_approvers_list().at(1)) == "b");
        assert!(get_sign_string(m1.get_approvers_list().at(2)) == "d");
        assert!(get_sign_string(m1.get_approvers_list().at(3)) == "");

        let m2 = cand2.merge(&cand1, &mut description);

        assert!(m1.get_block() == &sentb);
        assert!(get_sign_string(m2.get_approvers_list().at(0)) == "a");
        assert!(get_sign_string(m2.get_approvers_list().at(1)) == "b");
        assert!(get_sign_string(m2.get_approvers_list().at(2)) == "d");
        assert!(get_sign_string(m2.get_approvers_list().at(3)) == "");

        assert!(m1.get_hash() == 3910441588 && m2.get_hash() == 3910441588);
    }
}

#[test]
fn test_state_hashes() {
    env_logger::Builder::new()
        .format(move |buf, record| {
            let message = format!("{}", record.args());
            let level = format!("{}", record.level());
            let line = match record.line() {
                Some(line) => format!("({})", line),
                None => "".to_string(),
            };
            let source = format!("{}{}", record.target(), line);
            let thread_name = {
                let current_thread = std::thread::current();

                if let Some(name) = current_thread.name() {
                    name.to_string()
                } else {
                    let id = current_thread.id();
                    format!("#{:?}", id)
                        .replace("ThreadId(", "")
                        .replace(")", "")
                }
            };

            let (message, level) = match record.level() {
                log::Level::Error => (message.red(), level.red()),
                log::Level::Warn => (message.yellow(), level.yellow()),
                log::Level::Trace => (message.dimmed(), level.dimmed()),
                log::Level::Info => {
                    if record.target() == module_path!() {
                        (message.bright_green().bold(), level.bright_green().bold())
                    } else {
                        (message.bright_white().bold(), level.bright_white().bold())
                    }
                }
                _ => (message.normal(), level.normal()),
            };

            let (message, level) = if thread_name == "VS2" {
                (message.bright_green().bold(), level.bright_green().bold())
            } else {
                (message, level)
            };

            match record.level() {
              log::Level::Trace /*| log::Level::Debug*/ => Ok(()),
              _ => {
                  writeln!(
                      buf,
                      "{} [{: <5}] - {: <5} - {: <45}| {}",
                      chrono::Local::now().format("%Y-%m-%dT%H:%M:%S.%f"),
                      level,
                      thread_name,
                      source,
                      message
                  )?;

                  std::io::stdout().flush()
              }
          }
        })
        .filter(None, log::LevelFilter::Trace)
        .init();

    let options = SessionOptions::default();
    let total_nodes: u32 = 100;

    test_state_hashes_part1(&options, total_nodes);
    test_state_hashes_part2(&options, total_nodes);
}
