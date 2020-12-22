pub use super::*;
use crate::ton_api::IntoBoxed;
use rand::rngs::ThreadRng;
use rand::Rng;
use std::collections::HashMap;
use std::time::Duration;
use std::time::SystemTime;

/*
    Constants
*/

const PERSISTENT_CACHE_SIZE: usize = 1000000;
const TEMP_CACHE_SIZE: usize = 10000;

/*
    SessionOptions default
*/

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            catchain_idle_timeout: Duration::from_millis(16000),
            catchain_max_deps: 4,
            catchain_skip_processed_blocks: false,
            round_candidates: 3,
            next_candidate_delay: Duration::from_millis(2000),
            round_attempt_duration: Duration::from_millis(16000),
            max_round_attempts: 4,
            max_block_size: 4 << 20,
            max_collated_data_size: 4 << 20,
            new_catchain_ids: false,
        }
    }
}

/*
    Implementation details for SessionDescription
*/

type CacheEntryPtr = Option<CacheEntry>;

/// Validator source node description
#[derive(Debug)]
struct Source {
    id: PublicKeyHash,       //public key hash for the node
    public_key: PublicKey,   //public key of the node
    adnl_id: PublicKeyHash,  //ADNL identifier
    weight: ValidatorWeight, //node's weight according to a stake
}

pub(crate) struct SessionDescriptionImpl {
    options: SessionOptions,                      //validator session options
    persistent_objects_cache: Vec<CacheEntryPtr>, //cache of persistent objects
    temp_objects_cache: Vec<CacheEntryPtr>,       //cache of temporary objects objects
    sources: Vec<Source>,                         //list of sources
    rev_sources: HashMap<PublicKeyHash, u32>,     //mapping between public key hash and source index
    cutoff_weight: ValidatorWeight,               //cutoff weight for validators decision making
    total_weight: ValidatorWeight,                //total weight of all validators
    self_idx: u32,                                //index of this validator in a list of sources
    rng: ThreadRng,                               //random generator
    current_time: Option<std::time::SystemTime>,  //current time for log replaying
    metrics_receiver: metrics_runtime::Receiver,  //receiver for profiling metrics
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
        &self.get_source(src_idx).id
    }

    fn get_source_public_key(&self, src_idx: u32) -> &PublicKey {
        &self.get_source(src_idx).public_key
    }

    fn get_source_adnl_id(&self, src_idx: u32) -> &PublicKeyHash {
        &self.get_source(src_idx).adnl_id
    }

    fn get_source_index(&self, public_key_hash: &PublicKeyHash) -> u32 {
        if let Some(source) = self.rev_sources.get(public_key_hash) {
            return *source;
        }

        unreachable!("Unknown public key");
    }

    fn get_node_weight(&self, src_idx: u32) -> ValidatorWeight {
        self.get_source(src_idx).weight
    }

    fn get_total_nodes(&self) -> u32 {
        self.sources.len() as u32
    }

    fn get_self_idx(&self) -> u32 {
        self.self_idx
    }

    fn export_nodes(&self) -> Vec<PublicKeyHash> {
        self.sources
            .iter()
            .map(|source| source.id.clone())
            .collect()
    }

    fn export_full_nodes(&self) -> Vec<PublicKey> {
        self.sources
            .iter()
            .map(|source| source.public_key.clone())
            .collect()
    }

    fn export_catchain_nodes(&self) -> Vec<CatchainNode> {
        self.sources
            .iter()
            .map(|source| CatchainNode {
                public_key: source.public_key.clone(),
                adnl_id: source.adnl_id.clone(),
            })
            .collect()
    }

    /*
        Weights & priorities
    */

    fn get_cutoff_weight(&self) -> ValidatorWeight {
        self.cutoff_weight
    }

    fn get_total_weight(&self) -> ValidatorWeight {
        self.total_weight
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

    fn set_time(&mut self, time: std::time::SystemTime) {
        self.current_time = Some(time);
    }

    fn get_time(&self) -> SystemTime {
        if let Some(time) = self.current_time {
            time
        } else {
            SystemTime::now()
        }
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
                error!("SessionDescription::get_ts: can't get system time");
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

    fn get_delay(&self, mut priority: u32) -> std::time::Duration {
        if self.sources.len() < 5 {
            priority += 1;
        }

        priority * self.options.next_candidate_delay
    }

    fn get_empty_block_delay(&self) -> std::time::Duration {
        let mut delay = self.get_delay(self.get_max_priority() + 1);

        const MIN_DELAY: std::time::Duration = std::time::Duration::from_millis(1000);

        if delay < MIN_DELAY {
            delay = MIN_DELAY;
        }

        delay
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
            src: ::ton_api::ton::int256(*self.get_source_public_key_hash(src_idx).data()),
            root_hash: root_hash.clone().into(),
            file_hash: file_hash.clone().into(),
            collated_data_file_hash: collated_data_file_hash.clone().into(),
        }
        .into_boxed();
        let serialized_candidate_id = catchain::utils::serialize_tl_boxed_object!(&candidate_id);

        catchain::utils::get_hash(&serialized_candidate_id)
    }

    fn check_signature(
        &self,
        root_hash: &BlockHash,
        file_hash: &BlockHash,
        src_idx: u32,
        signature: &BlockSignature,
    ) -> Result<()> {
        let block_id = ton_api::ton::ton::blockid::BlockId {
            root_cell_hash: root_hash.clone().into(),
            file_hash: file_hash.clone().into(),
        }
        .into_boxed();
        let serialized_block_id = catchain::utils::serialize_tl_boxed_object!(&block_id);

        self.sources[src_idx as usize]
            .public_key
            .verify(&serialized_block_id, signature)
    }

    fn check_approve_signature(
        &self,
        root_hash: &BlockHash,
        file_hash: &BlockHash,
        src_idx: u32,
        signature: &BlockSignature,
    ) -> Result<()> {
        let block_id = ton_api::ton::ton::blockid::BlockIdApprove {
            root_cell_hash: root_hash.clone().into(),
            file_hash: file_hash.clone().into(),
        }
        .into_boxed();
        let serialized_block_id = catchain::utils::serialize_tl_boxed_object!(&block_id);

        self.sources[src_idx as usize]
            .public_key
            .verify(&serialized_block_id, signature)
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

    fn get_metrics_receiver(&self) -> &metrics_runtime::Receiver {
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
            .field("sources", &self.sources)
            .field("self_idx", &self.self_idx)
            .field("total_weight", &self.total_weight)
            .field("cutoff_weight", &self.cutoff_weight)
            .finish()
    }
}

/*
    Implementation internals of SessionDescriptionImpl
*/

impl SessionDescriptionImpl {
    /*
        Accessors
    */

    fn get_source(&self, src_idx: u32) -> &Source {
        assert!((src_idx as usize) < self.sources.len());
        &self.sources[src_idx as usize]
    }

    /*
        SessionDescription creation
    */

    pub(crate) fn new(
        options: &SessionOptions,
        nodes: &Vec<SessionNode>,
        local_id: &PublicKeyHash,
    ) -> Self {
        let mut total_weight = 0;
        let mut sources = Vec::new();
        let mut rev_sources = HashMap::new();
        let mut node_index = 0;

        sources.reserve(nodes.len());

        for node in nodes {
            let source = Source {
                public_key: node.public_key.clone(),
                adnl_id: node.adnl_id.clone(),
                id: node.public_key.id().clone(),
                weight: node.weight,
            };

            rev_sources.insert(source.id.clone(), node_index as u32);
            sources.push(source);

            total_weight += node.weight;
            node_index += 1;
        }

        let cutoff_weight = total_weight * 2 / 3 + 1;

        let self_idx = match rev_sources.get(local_id) {
            Some(source) => *source,
            None => {
                error!("SessionDescription::new: can't find validator with local ID {} in a list of sources {:?}", local_id, nodes);
                panic!("SessionDescription::new");
            }
        };

        let metrics_receiver = metrics_runtime::Receiver::builder()
            .build()
            .expect("failed to create validator session metrics receiver");

        let body = Self {
            options: *options,
            persistent_objects_cache: vec![None; PERSISTENT_CACHE_SIZE],
            temp_objects_cache: vec![None; TEMP_CACHE_SIZE],
            rng: rand::thread_rng(),
            total_weight: total_weight,
            cutoff_weight: cutoff_weight,
            sources: sources,
            rev_sources: rev_sources,
            self_idx: self_idx,
            current_time: None,
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
