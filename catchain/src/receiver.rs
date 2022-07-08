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

use self::ton::CatchainSentResponse;
pub use super::*;
use crate::profiling::check_execution_time;
use crate::profiling::instrument;
use crate::profiling::ResultStatusCounter;
use crate::ton_api::IntoBoxed;
use ever_crypto::Ed25519KeyOption;
use rand::Rng;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::time::Duration;
use utils::*;

/*
    Constants
*/

const MAX_NEIGHBOURS_COUNT: usize = 5; //max number of neighbours to synchronize
const CATCHAIN_NEIGHBOURS_SYNC_MIN_PERIOD_MS: u64 = 100; //min time for catchain sync with neighbour nodes
const CATCHAIN_NEIGHBOURS_SYNC_MAX_PERIOD_MS: u64 = 200; //max time for catchain sync with neighbour nodes
const CATCHAIN_NEIGHBOURS_ROTATE_MIN_PERIOD_MS: u64 = 60000; //min time for catchain neighbours rotation
const CATCHAIN_NEIGHBOURS_ROTATE_MAX_PERIOD_MS: u64 = 120000; //max time for catchain neighbours rotation
const MAX_SOURCES_SYNC_ATTEMPS: usize = 3; //max number of attempts to find a source to synchronize
const MAX_UNSAFE_INITIAL_SYNC_COMPLETE_TIME_SECS: u64 = 300; //max time to finish synchronization (unsafe mode)
const MAX_SAFE_INITIAL_SYNC_COMPLETE_TIME_SECS: u64 = 5; //max time to finish synchronization (safe mode)
const INFINITE_INITIAL_SYNC_COMPLETE_TIME_SECS: u64 = 10 * 365 * 24 * 3600; //inifinite time to finish synchronization

lazy_static! {
  static ref ZERO_HASH : BlockHash = BlockHash::default(); //default block hash to save the root in the DB
}

/*
    Implementation details for Receiver
*/

struct PendingBlock {
    payload: BlockPayloadPtr,   //payload of a new block
    dep_hashes: Vec<BlockHash>, //list of dependencies for a new block
}

pub(crate) struct ReceiverImpl {
    incarnation: SessionId, //session ID (incarnation, overlay short ID)
    options: Options,       //Catchain options
    blocks: HashMap<BlockHash, ReceivedBlockPtr>, //all received blocks
    blocks_to_run: VecDeque<ReceivedBlockPtr>, //blocks which has been scheduled for delivering (fully resolved)
    root_block: ReceivedBlockPtr, //root block for catchain sesssion (hash is equal to session incarnation)
    last_sent_block: ReceivedBlockPtr, //last block sent to catchain
    active_send: bool,            //active send flag for adding new blocks
    pending_blocks: VecDeque<PendingBlock>, //pending blocks for sending
    sources: Vec<ReceiverSourcePtr>, //receiver sources (knowledge about other catchain validators)
    source_public_key_hashes: Vec<PublicKeyHash>, //public key hashes of all sources
    public_key_hash_to_source: HashMap<PublicKeyHash, ReceiverSourcePtr>, //map from public key hash to source
    adnl_id_to_source: HashMap<PublicKeyHash, ReceiverSourcePtr>, //map from ADNL ID to source
    local_id: PublicKeyHash,                                      //this node's public key hash
    local_key: PrivateKey,                                        //this node's private key
    local_idx: usize,                                             //this node's source index
    _local_ids: Vec<CatchainNode>, //receiver sources identifiers (pub keys, ADNL ids)
    total_forks: usize,            //total forks number for this receiver
    neighbours: Vec<usize>,        //list of neighbour indices to synchronize
    listener: ReceiverListenerPtr, //listener for callbacks
    metrics_receiver: Arc<metrics_runtime::Receiver>, //receiver for profiling metrics
    received_blocks_instance_counter: InstanceCounter, //received blocks instances
    in_queries_counter: ResultStatusCounter, //result status counter for queries
    out_queries_counter: ResultStatusCounter, //result status counter for queries
    in_messages_counter: metrics_runtime::data::Counter, //incoming messages counter
    out_messages_counter: metrics_runtime::data::Counter, //outgoing messages counter
    in_broadcasts_counter: metrics_runtime::data::Counter, //incoming broadcasts counter
    pending_in_db: i32,            //blocks pending to read from DB
    db: Option<DatabasePtr>,       //database with (BlockHash, Payload)
    read_db: bool,                 //flag to indicate receiver is in reading DB mode
    db_root_block: BlockHash,      //root DB block
    db_suffix: String,             //DB name suffix
    allow_unsafe_self_blocks_resync: bool, //indicates we can receive self blocks from other validators
    unsafe_root_block_writing: bool, //indicates we are in the middle of unsafe root block writing
    started: bool,                   //indicates catchain is started
    next_awake_time: SystemTime,     //next awake timestamp
    next_sync_time: SystemTime,      //time to do next sync with a random neighbour
    next_neighbours_rotate_time: SystemTime, //time to change neighbours
    initial_sync_complete_time: SystemTime, //time to finish initial synchronization
    rng: rand::rngs::ThreadRng,      //random generator
    get_pending_deps_call_id: u64, //unique ID for calling get_pending_deps (to cut off duplications during the blocks graph traverse)
}

/// Functions which converts public Receiver to its implementation
#[allow(dead_code)]
fn get_impl(receiver: &dyn Receiver) -> &ReceiverImpl {
    receiver.get_impl().downcast_ref::<ReceiverImpl>().unwrap()
}

#[allow(dead_code)]
fn get_mut_impl(receiver: &mut dyn Receiver) -> &mut ReceiverImpl {
    receiver
        .get_mut_impl()
        .downcast_mut::<ReceiverImpl>()
        .unwrap()
}

/*
    Implementation for public Receiver trait
*/

impl Receiver for ReceiverImpl {
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

    fn get_incarnation(&self) -> &SessionId {
        &self.incarnation
    }

    fn get_options(&self) -> &Options {
        &self.options
    }

    /*
        Work with sources
    */

    fn get_sources_count(&self) -> usize {
        self.sources.len()
    }

    fn get_source(&self, source_id: usize) -> ReceiverSourcePtr {
        self.sources[source_id].clone()
    }

    fn get_source_public_key_hash(&self, source_id: usize) -> &PublicKeyHash {
        &self.source_public_key_hashes[source_id]
    }

    /*
        Forks management
    */

    fn get_forks_count(&self) -> usize {
        unimplemented!();
    }

    fn add_fork(&mut self) -> usize {
        self.total_forks += 1;

        let fork_id = self.total_forks;

        trace!("...new fork {} has been added for receiver", fork_id);

        fork_id
    }

    fn blame(&mut self, source_id: usize) {
        self.notify_on_blame(source_id);
    }

    fn add_fork_proof(&mut self, fork_proof: &BlockPayloadPtr) {
        trace!("...add block {:?} as a fork proof", fork_proof);

        self.add_block_for_delivery(fork_proof.clone(), Vec::new());
    }

    /*
        General ReceivedBlock methods
    */

    fn get_block_by_hash(&self, b: &BlockHash) -> Option<ReceivedBlockPtr> {
        instrument!();

        match self.blocks.get(b) {
            None => None,
            Some(t) => Some(t.clone()),
        }
    }

    /*
        Block validation management
    */

    fn validate_block_dependency(&self, dep: &ton::BlockDep) -> Result<()> {
        instrument!();

        (received_block::ReceivedBlockImpl::pre_validate_block_dependency(self, dep))?;

        if dep.height <= 0 {
            return Ok(());
        }

        let id = &self.get_block_dependency_id(&dep);
        let serialized_block_id = utils::serialize_tl_boxed_object!(id);

        if let Some(_block) = self.get_block_by_hash(&utils::get_hash(&serialized_block_id)) {
            return Ok(());
        }

        let source = self.get_source_by_hash(&utils::int256_to_public_key_hash(id.src()));

        assert!(source.is_some());

        let public_key = source.unwrap().borrow().get_public_key().clone();

        match public_key.verify(&serialized_block_id.0, &dep.signature.0) {
            Err(err) => Err(err),
            Ok(_) => Ok(()),
        }
    }

    /*
        Queries processing
    */

    fn receive_query_from_overlay(
        &mut self,
        adnl_id: &PublicKeyHash,
        data: &BlockPayloadPtr,
        response_callback: ExternalQueryResponseCallback,
    ) {
        check_execution_time!(50000);
        instrument!();

        let source = self.get_source_by_adnl_id(adnl_id);

        if let Some(ref source) = source {
            source.borrow_mut().get_mut_statistics().in_queries_count += 1;
        }

        let in_query_status = self.in_queries_counter.clone();

        in_query_status.total_increment();

        if !self.read_db {
            response_callback(Err(format_err!(
                "DB is not read for catchain receiver {}",
                self.incarnation.to_hex_string()
            )));
            return;
        }

        let (processed, result) = self.process_query(&adnl_id, &data);

        if processed {
            if result.is_ok() {
                in_query_status.success();
            } else {
                in_query_status.failure();
            }

            response_callback(result);
            return;
        }

        if let Some(source) = source {
            let source_public_key_hash = source.borrow().get_public_key_hash().clone();

            in_query_status.success(); //TODO: add statistics processing for custom queries

            self.notify_on_custom_query(&source_public_key_hash, data, response_callback);
        }
    }

    /*
        ReceivedBlock receiving flow implementation
    */

    fn receive_block(
        &mut self,
        adnl_id: &PublicKeyHash,
        block: &ton::Block,
        payload: BlockPayloadPtr,
    ) -> Result<ReceivedBlockPtr> {
        instrument!();

        let id = self.get_block_id(&block, payload.data());
        let hash = get_block_id_hash(&id);
        let block_opt = self.get_block_by_hash(&hash);

        trace!(
            "New block with hash={:?} and id={:?} has been received",
            hash,
            id
        );

        if let Some(block) = block_opt {
            if block.borrow().is_initialized() {
                trace!(
                    "...skip block {:?} because it has been already initialized",
                    hash
                );

                return Ok(block.clone());
            }
        }

        if block.incarnation != self.incarnation {
            let message = format!(
                "Block from source {} incarnation mismatch: expected {} but received {:?}",
                adnl_id,
                self.incarnation.to_hex_string(),
                block.incarnation
            );
            warn!("{}", message);
            return Err(err_msg(message));
        }

        if let Err(validation_error) = self.validate_block_with_payload(&block, &payload) {
            let message = format!(
                "Receiver {} received broken block from source {}: {}",
                self.incarnation.to_hex_string(),
                adnl_id,
                validation_error
            );

            warn!("{}", message);

            return Err(err_msg(message));
        }

        if block.src as usize == self.local_idx {
            if !self.allow_unsafe_self_blocks_resync || self.started {
                error!(
                    "Receiver {} has received unknown SELF block from {} (unsafe={})",
                    self.incarnation.to_hex_string(),
                    adnl_id,
                    self.allow_unsafe_self_blocks_resync
                );

                if !cfg!(debug_assertions) {
                    panic!("Unknown SELF block is received");
                }
            } else {
                error!("Receiver {} has received unknown SELF block from {}. UPDATING LOCAL DATABASE. UNSAFE", self.incarnation.to_hex_string(),
          adnl_id);

                self.initial_sync_complete_time = SystemTime::now()
                    + Duration::from_secs(MAX_UNSAFE_INITIAL_SYNC_COMPLETE_TIME_SECS);

                self.set_next_awake_time(self.initial_sync_complete_time);
            }
        }

        let received_block = self.create_block_with_payload(&block, payload.clone())?;

        if let Some(ref db) = self.db {
            let hash = hash.clone();
            let block = block.clone();
            let payload = payload.clone();
            let db = db.clone();

            if let Some(listener) = self.listener.upgrade() {
                let listener = listener.borrow();
                let task_queue = listener.get_task_queue();
                let task_queue_clone = task_queue.clone();
                let id = id.clone();
                let hash = hash.clone();

                task_queue.post_utility_closure(Box::new(move || {
                    match utils::serialize_block_with_payload(&block, &payload) {
                        Ok(raw_data) => {
                            db.put_block(&hash, raw_data);

                            task_queue_clone.post_closure(Box::new(move |receiver| {
                                let receiver = receiver
                                    .get_mut_impl()
                                    .downcast_mut::<ReceiverImpl>()
                                    .unwrap();

                                receiver.block_written_to_db(&id);

                                trace!(
                                    "...block {:?} has been successfully processed after receiving",
                                    hash
                                );
                            }));
                        }
                        Err(err) => warn!("Block serialization error: {:?}", err),
                    }
                }));
            }
        }

        Ok(received_block)
    }

    fn receive_message_from_overlay(
        &mut self,
        adnl_id: &PublicKeyHash,
        bytes: &mut &[u8],
    ) -> Result<ReceivedBlockPtr> {
        instrument!();

        self.in_messages_counter.increment();

        if let Some(ref source) = self.get_source_by_adnl_id(adnl_id) {
            source.borrow_mut().get_mut_statistics().in_messages_count += 1;
        }

        if !self.read_db {
            bail!("DB is not read");
        }

        let reader: &mut dyn std::io::Read = bytes;
        let mut deserializer = ton_api::Deserializer::new(reader);

        match deserializer.read_boxed::<ton_api::ton::TLObject>() {
            Ok(message) => {
                if message.is::<::ton_api::ton::catchain::Update>() {
                    let mut payload = Vec::new();

                    reader.read_to_end(&mut payload)?;

                    let payload =
                        CatchainFactory::create_block_payload(ton_api::ton::bytes(payload));

                    return self.receive_block(
                        adnl_id,
                        &message
                            .downcast::<::ton_api::ton::catchain::Update>()
                            .unwrap()
                            .block(),
                        payload,
                    );
                } else {
                    let err = format_err!("unknown message received {:?}", message);

                    //error!("{}", err);

                    return Err(err);
                }
            }
            Err(err) => {
                return Err(err);
            }
        }
    }

    fn receive_broadcast_from_overlay(
        &mut self,
        source_key_hash: &PublicKeyHash,
        data: &BlockPayloadPtr,
    ) {
        instrument!();

        if let Some(ref source) = self.get_source_by_hash(source_key_hash) {
            source.borrow_mut().get_mut_statistics().in_broadcasts_count += 1;
        }

        if !self.read_db {
            return;
        }

        self.in_broadcasts_counter.increment();

        self.notify_on_broadcast(source_key_hash, &data);
    }

    /*
        ReceivedBlock delivery flow implementation
    */

    fn run_block(&mut self, block: ReceivedBlockPtr) {
        self.blocks_to_run.push_back(block.clone());
    }

    fn deliver_block(&mut self, block: &mut dyn ReceivedBlock) {
        instrument!();

        trace!(
            "Catchain delivering block {:?} from source={} fork={} height={} custom={} deps={:?}",
            block.get_hash(),
            block.get_source_id(),
            block.get_fork_id(),
            block.get_height(),
            block.is_custom(),
            block.get_dep_hashes()
        );

        //notify listeners about new block appearance

        lazy_static! {
            static ref DEFAULT_BLOCK: BlockPayloadPtr =
                CatchainFactory::create_empty_block_payload();
        }

        self.notify_on_new_block(
            block.get_source_id(),
            block.get_fork_id(),
            block.get_hash().clone(),
            block.get_height(),
            match block.get_prev() {
                Some(ref prev) => prev.borrow().get_hash().clone(),
                _ => BlockHash::default(),
            },
            block.get_dep_hashes(),
            block.get_forks_dep_heights().clone(),
            if block.is_custom() {
                block.get_payload()
            } else {
                &DEFAULT_BLOCK
            },
        );

        //prepare and send message with a new block to current overlay neighbours

        let mut receiver_addresses = Vec::new();

        for &it in &self.neighbours {
            let neighbour = self.get_source(it);
            let adnl_id = neighbour.borrow().get_adnl_id().clone();

            if !block.mark_block_for_sending(&adnl_id) {
                continue;
            }

            receiver_addresses.push(adnl_id);
        }

        if receiver_addresses.len() < 1 {
            return;
        }

        self.send_block_update_event_multicast(
            receiver_addresses.as_ref(),
            block.get_serialized_block_with_payload(),
        );
    }

    fn process(&mut self) {
        instrument!();

        if self.blocks_to_run.len() > 0 {
            self.run_scheduler();
        }
    }

    /*
        ReceivedBlock creation
    */

    fn create_block(&mut self, block: &ton::BlockDep) -> ReceivedBlockPtr {
        instrument!();

        if block.height == 0 {
            return self.root_block.clone();
        }

        let hash = get_block_dependency_hash(block, self);
        let block_opt = self.get_block_by_hash(&hash);

        if let Some(block) = block_opt {
            return block;
        } else {
            let new_block = received_block::ReceivedBlockImpl::create(&block, self);

            self.add_received_block(new_block.clone());

            return new_block;
        }
    }

    fn create_block_from_string_dump(&self, dump: &String) -> ReceivedBlockPtr {
        received_block::ReceivedBlockImpl::create_from_string_dump(dump, self)
    }

    fn parse_add_received_block(&mut self, s: &String) {
        self.add_received_block(self.create_block_from_string_dump(s));
    }

    /*
        Adding new block (initiated by validator session during new block creation)
    */

    fn add_block(&mut self, payload: BlockPayloadPtr, deps: Vec<BlockHash>) {
        instrument!();

        if self.active_send {
            self.add_block_for_delivery(payload, deps);
            return;
        }

        self.add_block_impl(payload, deps);
    }

    /*
        Debug methods
    */

    fn to_string(&self) -> String {
        let mut res: String = "".to_string();

        res += "ReceivedBlocks:";

        for (k, v) in &self.blocks {
            res += &format!("\n {:?} ->  {}", k, v.borrow().to_string().as_str());
        }

        res
    }

    /*
        Profiling tools
    */

    fn get_metrics_receiver(&self) -> &metrics_runtime::Receiver {
        &self.metrics_receiver
    }

    fn get_received_blocks_instance_counter(&self) -> &InstanceCounter {
        &self.received_blocks_instance_counter
    }

    /*
        Triggers
    */

    fn check_all(&mut self) {
        instrument!();

        let now = SystemTime::now();
        trace!("Catchain_startup: check_all called; now {:?}", now);

        //synchronize with chosen neighbours

        if let Ok(_elapsed) = self.next_sync_time.elapsed() {
            self.synchronize();

            let delay = Duration::from_millis(self.rng.gen_range(
                CATCHAIN_NEIGHBOURS_SYNC_MIN_PERIOD_MS..CATCHAIN_NEIGHBOURS_SYNC_MAX_PERIOD_MS + 1,
            ));

            self.next_sync_time = now + delay;

            trace!(
                "...next sync is scheduled at {} (in {:.3}s from now)",
                utils::time_to_string(&self.next_sync_time),
                delay.as_secs_f64(),
            );
        }

        //rotate neighbours

        if let Ok(_elapsed) = self.next_neighbours_rotate_time.elapsed() {
            self.choose_neighbours();

            let delay = Duration::from_millis(self.rng.gen_range(
                CATCHAIN_NEIGHBOURS_ROTATE_MIN_PERIOD_MS
                    ..CATCHAIN_NEIGHBOURS_ROTATE_MAX_PERIOD_MS + 1,
            ));

            self.next_neighbours_rotate_time = now + delay;

            trace!(
                "...next neighbours rotation is scheduled at {} (in {:.3}s from now)",
                utils::time_to_string(&self.next_neighbours_rotate_time),
                delay.as_secs_f64(),
            );
        }

        //start up checks (for unsafe startup)
        trace!(
            "Catchain_startup: self.started {}, self.read_db {}",
            self.started,
            self.read_db
        );

        if !self.started && self.read_db {
            let elapsed = self.initial_sync_complete_time.elapsed();
            trace!(
                "Catchain_startup: initial sync complete time {:?}, now {:?}, elapsed? {:?}",
                self.initial_sync_complete_time,
                SystemTime::now(),
                elapsed
            );

            if let Ok(_elapsed) = elapsed {
                let allow = if self.allow_unsafe_self_blocks_resync {
                    self.unsafe_start_up_check_completed()
                } else {
                    true
                };
                trace!("Catchain_startup: allow {}", allow);

                if allow {
                    self.initial_sync_complete_time =
                        now + Duration::from_secs(INFINITE_INITIAL_SYNC_COMPLETE_TIME_SECS);
                    self.started = true;

                    self.notify_on_started();
                }
            }
        }

        //update awake time

        self.set_next_awake_time(self.initial_sync_complete_time);
        self.set_next_awake_time(self.next_neighbours_rotate_time);
        self.set_next_awake_time(self.next_sync_time);
    }

    fn set_next_awake_time(&mut self, timestamp: std::time::SystemTime) {
        let now = std::time::SystemTime::now();

        //ignore set new awake point if it is in the past

        if timestamp < now {
            return;
        }

        //do not set next awake point if we will awake earlier in the future

        if self.next_awake_time > now && self.next_awake_time <= timestamp {
            return;
        }

        self.next_awake_time = timestamp;
    }

    fn get_next_awake_time(&self) -> std::time::SystemTime {
        self.next_awake_time.clone()
    }

    /*
        Database management
    */

    fn destroy_db(&mut self) {
        if let Some(db) = &self.db {
            db.destroy();
        }
    }
}

/*
    Dummy listener for receiver
*/

//TODO: move dummy listener & dummy task queue to tests

struct DummyTaskQueue {}

impl ReceiverTaskQueue for DummyTaskQueue {
    fn post_utility_closure(&self, _handler: Box<dyn FnOnce() + Send>) {}

    fn post_closure(&self, _handler: Box<dyn FnOnce(&mut dyn Receiver) + Send>) {}
}

struct DummyListener {
    task_queue: ReceiverTaskQueuePtr,
}

impl ReceiverListener for DummyListener {
    fn get_impl(&self) -> &dyn Any {
        self
    }
    fn get_mut_impl(&mut self) -> &mut dyn Any {
        self
    }

    fn on_started(&mut self) {}

    fn on_new_block(
        &mut self,
        _receiver: &mut dyn Receiver,
        _source_id: usize,
        _fork_id: usize,
        _hash: BlockHash,
        _height: BlockHeight,
        _prev: BlockHash,
        _deps: Vec<BlockHash>,
        _forks_dep_heights: Vec<BlockHeight>,
        _payload: &BlockPayloadPtr,
    ) {
    }

    fn on_broadcast(
        &mut self,
        _receiver: &mut dyn Receiver,
        _source_key_hash: &PublicKeyHash,
        _data: &BlockPayloadPtr,
    ) {
    }

    fn on_blame(&mut self, _receiver: &mut dyn Receiver, _source_id: usize) {}

    fn on_custom_query(
        &mut self,
        _receiver: &mut dyn Receiver,
        _source_public_key_hash: &PublicKeyHash,
        _data: &BlockPayloadPtr,
        _response_callback: ExternalQueryResponseCallback,
    ) {
    }

    fn send_message(
        &mut self,
        _receiver_id: &PublicKeyHash,
        _sender_id: &PublicKeyHash,
        _message: &BlockPayloadPtr,
        _can_be_postponed: bool,
    ) {
    }

    fn send_message_multicast(
        &mut self,
        _receiver_ids: &[PublicKeyHash],
        _sender_id: &PublicKeyHash,
        _message: &BlockPayloadPtr,
    ) {
    }

    fn send_query(
        &mut self,
        _receiver_id: &PublicKeyHash,
        _sender_id: &PublicKeyHash,
        _name: &str,
        _timeout: std::time::Duration,
        _message: &BlockPayloadPtr,
        _response_callback: QueryResponseCallback,
    ) {
    }

    fn get_task_queue(&self) -> &ReceiverTaskQueuePtr {
        &self.task_queue
    }
}

impl DummyListener {
    fn create() -> Rc<RefCell<dyn ReceiverListener>> {
        Rc::new(RefCell::new(Self {
            task_queue: Arc::new(DummyTaskQueue {}),
        }))
    }
}

/*
    Private ReceiverImpl details
*/

impl ReceiverImpl {
    /*
        Unsafe startup
    */

    fn unsafe_start_up_check_completed(&mut self) -> bool {
        instrument!();

        let source = self.get_source(self.local_idx);
        let source = source.borrow();
        let now = SystemTime::now();

        assert!(!source.is_blamed());

        if source.has_unreceived() || source.has_undelivered() {
            info!(
                "Catchain: has_unreceived={} has_undelivered={}",
                source.has_unreceived(),
                source.has_undelivered()
            );
            self.run_scheduler();

            self.initial_sync_complete_time = now + Duration::from_secs(60);

            return false;
        }

        let delivered_height = source.get_delivered_height();

        if delivered_height == 0 {
            assert!(self.last_sent_block.borrow().get_height() == 0);
            assert!(!self.unsafe_root_block_writing);

            return true;
        }

        if self.last_sent_block.borrow().get_height() == delivered_height {
            assert!(!self.unsafe_root_block_writing);

            return true;
        }

        if self.unsafe_root_block_writing {
            self.initial_sync_complete_time = now + Duration::from_secs(5);

            info!("Catchain: writing=true");

            return false;
        }

        self.unsafe_root_block_writing = true;

        let block = source.get_block(delivered_height);

        assert!(block.is_some());

        let received_block = block.unwrap();
        let block = received_block.borrow();

        assert!(block.is_delivered());
        assert!(block.in_db());

        let block_id_hash = block.get_hash();
        let block_raw_data = ::ton_api::ton::bytes(block_id_hash.as_slice().to_vec());

        self.db
            .as_ref()
            .unwrap()
            .put_block(&ZERO_HASH, block_raw_data);

        assert!(self.last_sent_block.borrow().get_height() < block.get_height());

        self.last_sent_block = received_block.clone();
        self.unsafe_root_block_writing = false;
        self.initial_sync_complete_time = now + Duration::from_secs(5);

        info!("Catchain: need update root");

        return false;
    }

    /*
        Work with sources
    */

    fn get_source_by_hash(
        &self,
        source_public_key_hash: &PublicKeyHash,
    ) -> Option<ReceiverSourcePtr> {
        if let Some(source) = self.public_key_hash_to_source.get(source_public_key_hash) {
            return Some(source.clone());
        }

        None
    }

    fn get_source_by_adnl_id(&self, adnl_id: &PublicKeyHash) -> Option<ReceiverSourcePtr> {
        if let Some(source) = self.adnl_id_to_source.get(adnl_id) {
            return Some(source.clone());
        }

        None
    }

    /*
        General ReceivedBlock methods
    */

    fn get_block_id(&self, block: &ton::Block, payload: &RawBuffer) -> ton::BlockId {
        utils::get_block_id(
            &block.incarnation,
            &self.get_source_public_key_hash(block.src as usize),
            block.height,
            payload,
        )
    }

    fn get_block_dependency_id(&self, dep: &ton::BlockDep) -> ton::BlockId {
        let incarnation = &self.incarnation;
        let source_hash = self.get_source_public_key_hash(dep.src as usize);
        let height = dep.height;
        let data_hash = dep.data_hash.clone();
        ::ton_api::ton::catchain::block::id::Id {
            incarnation: incarnation.clone(),
            src: public_key_hash_to_int256(source_hash),
            height,
            data_hash,
        }
        .into_boxed()
    }

    fn add_received_block(&mut self, block: ReceivedBlockPtr) {
        let hash = block.borrow().get_hash().clone();
        self.blocks.insert(hash, block);
    }

    /*
        Block validation management
    */

    fn validate_block_with_payload(
        &self,
        block: &ton::Block,
        payload: &BlockPayloadPtr,
    ) -> Result<()> {
        instrument!();

        (received_block::ReceivedBlockImpl::pre_validate_block(self, block, payload))?;

        if block.height <= 0 {
            return Ok(());
        }

        let id = &self.get_block_id(&block, payload.data());
        let serialized_block_id = utils::serialize_tl_boxed_object!(id);

        if let Some(_block) = self.get_block_by_hash(&utils::get_hash(&serialized_block_id)) {
            return Ok(());
        }

        let source = self.get_source_by_hash(&utils::int256_to_public_key_hash(id.src()));

        assert!(source.is_some());

        let public_key = source.unwrap().borrow().get_public_key().clone();

        match public_key.verify(&serialized_block_id.0, &block.signature.0) {
            Err(err) => Err(err),
            Ok(_) => Ok(()),
        }
    }

    /*
        Block delivery management
    */

    fn run_scheduler(&mut self) {
        instrument!();

        while let Some(block) = self.blocks_to_run.pop_front() {
            block.borrow_mut().process(self);
        }
    }

    fn add_block_for_delivery(&mut self, payload: BlockPayloadPtr, deps: Vec<BlockHash>) {
        self.pending_blocks.push_back(PendingBlock {
            payload: payload,
            dep_hashes: deps,
        });
    }

    fn add_block_impl(&mut self, payload: BlockPayloadPtr, deps: Vec<BlockHash>) {
        instrument!();

        trace!(
            "Adding new block with deps {:?} and payload {:?}",
            deps,
            payload
        );

        self.active_send = true;

        //check source

        let source_opt = self.get_source_by_hash(&self.local_id);

        assert!(source_opt.is_some());

        let source = source_opt.unwrap();

        assert!(source.borrow().get_id() == self.local_idx);

        //prepare prev block and dependencies

        let prev = self.last_sent_block.borrow().export_tl_dep();
        let mut dep_tls = Vec::new();

        dep_tls.reserve(deps.len());

        for dep in deps {
            let block = self.get_block_by_hash(&dep);

            if block.is_none() {
                error!("...can't find block with hash {:?}", dep);
                unreachable!();
            }

            dep_tls.push(block.unwrap().borrow().export_tl_dep());
        }

        //prepare block

        let height = prev.height + 1;
        let mut block = ton::Block {
            incarnation: self.incarnation.clone().into(),
            src: self.local_idx as i32,
            height: height,
            data: ton::BlockData {
                prev: prev,
                deps: dep_tls.into(),
            },
            signature: Vec::new().into(), //block will be signed later
        };

        let block_id = self.get_block_id(&block, payload.data());
        let block_id_serialized = serialize_tl_boxed_object!(&block_id);

        //block ID signing

        match self.local_key.sign(&block_id_serialized) {
            Ok(block_id_signature) => {
                block.signature = block_id_signature.to_vec().into();
            }
            Err(_err) => {
                error!("...block signing error: {:?}", _err);
                return;
            }
        }

        trace!("...block has been signed {:?}", block);

        //save block to DB

        trace!("...save block to DB");

        if let Some(ref db) = self.db {
            let db = db.clone();
            let block_id_hash = get_block_id_hash(&block_id);
            let block = block.clone();
            let payload = payload.clone();

            if let Some(listener) = self.listener.upgrade() {
                let listener = listener.borrow();
                let task_queue = listener.get_task_queue();
                let task_queue_clone = task_queue.clone();

                task_queue.post_utility_closure(Box::new(move || {
                    //save mapping: sha256(block_id) -> serialized block with payload

                    match utils::serialize_block_with_payload(&block, &payload) {
                        Ok(raw_data) => {
                            db.put_block(&block_id_hash, raw_data);
                        }
                        Err(err) => warn!("Block serialization error: {:?}", err),
                    }

                    //save mapping for root block to it's ID

                    db.put_block(
                        &ZERO_HASH,
                        ::ton_api::ton::bytes(block_id_hash.as_slice().to_vec()),
                    );

                    //create new block and send

                    task_queue_clone.post_closure(Box::new(move |receiver| {
                        let receiver = receiver
                            .get_mut_impl()
                            .downcast_mut::<ReceiverImpl>()
                            .unwrap();

                        //initiate delivery flow

                        trace!("...deliver a new created block {:?}", block);

                        match receiver.create_block_with_payload(&block, payload) {
                            Ok(block) => {
                                receiver.last_sent_block = block.clone();

                                block.borrow_mut().written(receiver);
                            }
                            Err(err) => error!("...creation block error: {:?}", err),
                        }

                        receiver.run_scheduler();

                        receiver.active_send = false;

                        if let Some(pending_block) = receiver.pending_blocks.pop_front() {
                            receiver.add_block(pending_block.payload, pending_block.dep_hashes);
                        }
                    }));
                }));
            }
        }
    }

    /*
        Receiver blocks DB management
    */

    fn start_up_db(&mut self, path: String) -> Result<()> {
        instrument!();

        trace!("...starting up DB");

        if self.options.debug_disable_db {
            self.read_db();
            return Ok(());
        }

        // we create special table for catchain receiver
        let db = CatchainFactory::create_database(
            path,
            format!(
                "catchainreceiver{}{}",
                self.db_suffix,
                base64::encode_config(self.incarnation.as_slice(), base64::URL_SAFE),
            ),
            self.get_metrics_receiver(),
        )?;

        self.db = Some(db.clone());

        if let Ok(root_block) = db.get_block(&ZERO_HASH) {
            let hash: [u8; 32] = root_block
                .0
                .try_into()
                .map_err(|_| ton_types::error!("Cannot convert root block hash"))?;
            let root_block_id_hash: BlockHash = hash.into();
            self.read_db_from(root_block_id_hash);
        } else {
            self.read_db();
        }
        Ok(())
    }

    fn read_db(&mut self) {
        instrument!();

        trace!("...reading DB");

        trace!("Catchain_startup: db_root_block {:?}", self.db_root_block);
        if self.db_root_block != ZERO_HASH.clone() {
            self.run_scheduler();

            match self.get_block_by_hash(&self.db_root_block) {
                None => warn!(
                    "Catchain_startup: no block with hash {:?} in db",
                    self.db_root_block
                ),
                Some(blk) => self.last_sent_block = blk,
            }

            assert!(self.last_sent_block.borrow().is_delivered());
        }

        self.read_db = true;

        let now = SystemTime::now();

        self.next_neighbours_rotate_time = now
            + Duration::from_millis(self.rng.gen_range(
                CATCHAIN_NEIGHBOURS_ROTATE_MIN_PERIOD_MS..CATCHAIN_NEIGHBOURS_ROTATE_MAX_PERIOD_MS,
            ));
        self.next_sync_time =
            now + Duration::from_millis(((0.001 * self.rng.gen_range(0.0..60.0)) * 1000.0) as u64);
        self.initial_sync_complete_time = now
            + Duration::from_secs(if self.allow_unsafe_self_blocks_resync {
                MAX_UNSAFE_INITIAL_SYNC_COMPLETE_TIME_SECS
            } else {
                MAX_SAFE_INITIAL_SYNC_COMPLETE_TIME_SECS
            });

        trace!(
            "...waiting until {:?} for DB initial complete",
            utils::time_to_string(&self.initial_sync_complete_time)
        );

        self.set_next_awake_time(self.initial_sync_complete_time);
        self.set_next_awake_time(self.next_neighbours_rotate_time);
        self.set_next_awake_time(self.next_sync_time);
    }

    fn read_db_from(&mut self, id: BlockHash) {
        instrument!();

        trace!("...reading DB from block {:?}", id);

        let listener = self.listener.upgrade().unwrap();
        let listener = listener.borrow();
        let task_queue = listener.get_task_queue();
        let task_queue_clone = task_queue.clone();

        self.pending_in_db = 1;
        self.db_root_block = id.clone();

        let block_raw_data = self.db.as_ref().unwrap().get_block(&id).unwrap();

        task_queue.post_closure(Box::new(move |receiver| {
            get_mut_impl(receiver).read_block_from_db(&id, block_raw_data, task_queue_clone);
        }));
    }

    fn read_block_from_db(
        &mut self,
        id: &BlockHash,
        raw_data: RawBuffer,
        task_queue: ReceiverTaskQueuePtr,
    ) {
        instrument!();

        trace!("...reading block {:?} from DB", id);

        self.pending_in_db -= 1;

        //parse header of a block

        let reader: &mut dyn std::io::Read = &mut &raw_data.0[..];
        let mut deserializer = ton_api::Deserializer::new(reader);
        let message = deserializer.read_boxed::<ton_api::ton::TLObject>();

        if let Err(err) = message {
            error!("DB block {} parsing error: {:?}", id, err);
            return;
        }

        let message = message.unwrap();

        if !message.is::<::ton_api::ton::catchain::Block>() {
            error!(
                "DB block {:?} parsing error: object does not contain Block message: object={:?}",
                id, message
            );
            return;
        }

        let block = message
            .downcast::<::ton_api::ton::catchain::Block>()
            .unwrap()
            .only();

        //parse payload of a block

        let mut payload = Vec::new();

        reader.read_to_end(&mut payload).unwrap();

        let payload = CatchainFactory::create_block_payload(ton_api::ton::bytes(payload));

        //check block ID

        let block_id = self.get_block_id(&block, payload.data());
        let block_id_hash = get_block_id_hash(&block_id);

        assert!(&block_id_hash == id);

        //skip duplicates

        if let Some(block) = self.get_block_by_hash(id) {
            if block.borrow().is_initialized() {
                assert!(block.borrow().in_db());

                if self.pending_in_db == 0 {
                    //if all dependencies are read start blocks delivering

                    self.read_db();
                }

                return;
            }
        }

        //block validation

        let _source = self.get_source(block.src as usize);

        assert!(block.incarnation == self.incarnation);

        if let Err(err) = self.validate_block_with_payload(&block, &payload) {
            let message = format!(
                "Receiver {} parsed broken block {:?} from DB: {}",
                self.incarnation.to_hex_string(),
                block,
                err
            );

            warn!("{}", message);

            return;
        }

        //create received block

        let block = self.create_block_with_payload(&block, payload).unwrap();

        block.borrow_mut().written(self);

        //resolve dependencies

        let mut deps = block.borrow().get_dep_hashes();
        deps.push(block.borrow().get_prev_hash().unwrap());

        for dep in &deps {
            let dep_block = self.get_block_by_hash(dep);

            if let Some(dep_block) = dep_block {
                if dep_block.borrow().is_initialized() {
                    continue;
                }
            }

            //query dependency from DB

            self.pending_in_db += 1;

            let dep_block = self.db.as_ref().unwrap().get_block(dep).unwrap();
            let dep = dep.clone();
            let task_queue_clone = task_queue.clone();

            //do recursion for block parsing

            task_queue.post_closure(Box::new(move |receiver| {
                get_mut_impl(receiver).read_block_from_db(&dep, dep_block, task_queue_clone);
            }));
        }

        //deliver blocks when all dependencies are requested from DB

        if self.pending_in_db == 0 {
            self.read_db();
        }
    }

    fn block_written_to_db(&mut self, block_id: &ton::BlockId) {
        instrument!();

        let block = self
            .get_block_by_hash(&get_block_id_hash(block_id))
            .unwrap();

        block.borrow_mut().written(self);

        self.run_scheduler();
    }

    /*
        Neighbours management
    */

    fn choose_neighbours(&mut self) {
        instrument!();

        trace!("Rotate neighbours");

        //randomly choose max neighbours from sources

        let sources_count = self.get_sources_count();
        let mut new_neighbours: Vec<usize> = Vec::new();
        let mut items_count = MAX_NEIGHBOURS_COUNT;

        trace!(
            "...choose {} neighbours from {} sources",
            items_count,
            sources_count
        );

        if items_count > sources_count {
            items_count = sources_count;
        }

        for i in 0..sources_count {
            if i == self.local_idx {
                continue;
            }

            if self.get_source(i).borrow().is_blamed() {
                continue;
            }

            let random_value = self.rng.gen_range(0..sources_count - i);
            if random_value >= items_count {
                continue;
            }

            new_neighbours.push(i);
            items_count -= 1;
        }

        trace!("...new receiver neighbours are: {:?}", new_neighbours);

        self.neighbours = new_neighbours;
    }

    fn synchronize(&mut self) {
        instrument!();

        trace!("Synchronize with other validators");

        let sources_count = self.get_sources_count();

        for _i in 0..MAX_SOURCES_SYNC_ATTEMPS {
            let source_index = self.rng.gen_range(0..sources_count);
            let source = self.get_source(source_index);

            if source.borrow().is_blamed() {
                continue;
            }

            self.synchronize_with(source);
            break;
        }
    }

    /*
        Sources synchronization
    */

    fn synchronize_with(&mut self, source: ReceiverSourcePtr) {
        instrument!();

        trace!("...synchronize with source {}", source.borrow().get_id());

        assert!(!source.borrow().is_blamed());

        //prepare the list of known delivered heights for each source
        //this list will be sent to syncrhonization source to obtain partial absent difference back

        let sources_delivered_heights: Vec<BlockHeight> = (0..self.get_sources_count())
            .map(|i| {
                let source = self.get_source(i);
                if source.borrow().is_blamed() {
                    -1
                } else {
                    source.borrow().get_delivered_height()
                }
            })
            .collect();

        let get_difference_request = ton::GetDifferenceRequest {
            rt: sources_delivered_heights.into(),
        };

        //send a difference query to a synchronization source

        let get_difference_response_handler =
            |result: Result<ton::GetDifferenceResponse>,
             _payload: BlockPayloadPtr,
             receiver: &mut dyn Receiver| {
                use ton_api::ton::catchain::*;

                match result {
                    Err(err) => {
                        get_mut_impl(receiver).out_queries_counter.failure();

                        warn!("GetDifference query error: {:?}", err)
                    }
                    Ok(response) => {
                        get_mut_impl(receiver).out_queries_counter.success();

                        match response {
                            Difference::Catchain_Difference(difference) => {
                                trace!("GetDifference response: {:?}", difference);
                            }
                            Difference::Catchain_DifferenceFork(difference_fork) => {
                                get_mut_impl(receiver).got_fork_proof(&difference_fork);
                            }
                        }
                    }
                }
            };

        source.borrow_mut().get_mut_statistics().out_queries_count += 1;

        self.out_queries_counter.total_increment();

        self.send_get_difference_request(
            source.borrow().get_adnl_id(),
            get_difference_request,
            get_difference_response_handler,
        );

        //request for absent blocks
        let delivered_height = source.borrow().get_delivered_height();
        let received_height = source.borrow().get_received_height();

        if delivered_height >= received_height {
            return;
        }

        //get first undelivered block for the source and request its dependencies

        let first_block = source.borrow().get_block(delivered_height + 1);

        if let Some(first_block) = first_block {
            let mut dep_hashes = Vec::new();

            {
                instrument!();

                const MAX_PENDING_DEPS_COUNT: usize = 16;

                self.get_pending_deps_call_id += 1;

                first_block.borrow_mut().get_pending_deps(
                    self.get_pending_deps_call_id,
                    MAX_PENDING_DEPS_COUNT,
                    &mut dep_hashes,
                );
            }

            for dep_hash in dep_hashes {
                //send getBlock request for each absent hash

                let get_block_request = ton::GetBlockRequest {
                    block: dep_hash.clone().into(),
                };
                let source_adnl_id = source.borrow().get_adnl_id().clone();
                let source_adnl_id_clone = source_adnl_id.clone();

                let get_block_response_handler =
                    move |result: Result<ton::BlockResultResponse>,
                          payload: BlockPayloadPtr,
                          receiver: &mut dyn Receiver| {
                        use ton_api::ton::catchain::*;

                        match result {
                            Err(err) => {
                                get_mut_impl(receiver).out_queries_counter.failure();

                                warn!(
                                    "GetBlock {:} query error: {:?}",
                                    dep_hash.to_hex_string(),
                                    err
                                );
                            }
                            Ok(response) => {
                                get_mut_impl(receiver).out_queries_counter.success();

                                match response {
                                    BlockResult::Catchain_BlockNotFound => warn!(
                                        "GetBlock {:} query didn't find the block",
                                        dep_hash.to_hex_string()
                                    ),
                                    BlockResult::Catchain_BlockResult(block_result) => {
                                        let _block = receiver.receive_block(
                                            &source_adnl_id,
                                            &block_result.block,
                                            payload,
                                        );
                                    }
                                }
                            }
                        }
                    };

                source.borrow_mut().get_mut_statistics().out_queries_count += 1;

                self.out_queries_counter.total_increment();

                self.send_get_block_request(
                    &source_adnl_id_clone,
                    get_block_request,
                    get_block_response_handler,
                );
            }
        }
    }

    fn process_query(
        &mut self,
        adnl_id: &PublicKeyHash,
        data: &BlockPayloadPtr,
    ) -> (bool, Result<BlockPayloadPtr>) {
        instrument!();

        trace!("Receiver: received query from {}: {:?}", adnl_id, data);

        match ton_api::Deserializer::new(&mut &data.data().0[..])
            .read_boxed::<ton_api::ton::TLObject>()
        {
            Ok(message) => {
                let message = match message.downcast::<ton::GetDifferenceRequest>() {
                    Ok(message) => {
                        return (
                            true,
                            utils::serialize_query_boxed_response(
                                self.process_get_difference_query(
                                    adnl_id,
                                    &message,
                                    data.get_creation_time().elapsed().unwrap(),
                                ),
                            ),
                        )
                    }
                    Err(message) => message,
                };

                let message = match message.downcast::<ton::GetBlockRequest>() {
                    Ok(message) => match self.process_get_block_query(adnl_id, &message) {
                        Ok(response) => {
                            let mut ret: RawBuffer = RawBuffer::default();
                            let mut serializer = ton_api::Serializer::new(&mut ret.0);

                            serializer.write_boxed(&response.0).unwrap();
                            serializer.write_bare(response.1.data()).unwrap();

                            return (true, Ok(CatchainFactory::create_block_payload(ret)));
                        }
                        Err(err) => return (true, Err(err)),
                    },
                    Err(message) => message,
                };

                let message = match message.downcast::<ton::GetBlocksRequest>() {
                    Ok(message) => {
                        return (
                            true,
                            utils::serialize_query_boxed_response(
                                self.process_get_blocks_query(adnl_id, &message),
                            ),
                        )
                    }
                    Err(message) => message,
                };

                let message = match message.downcast::<ton::GetBlockHistoryRequest>() {
                    Ok(message) => {
                        return (
                            true,
                            utils::serialize_query_boxed_response(
                                self.process_get_block_history_query(adnl_id, &message),
                            ),
                        )
                    }
                    Err(message) => message,
                };

                return (
                    false,
                    Err(format_err!("unknown query received {:?}", message)),
                );
            }
            Err(err) => {
                return (true, Err(err));
            }
        }
    }

    fn process_get_difference_query(
        &mut self,
        adnl_id: &PublicKeyHash,
        query: &ton::GetDifferenceRequest,
        query_latency: std::time::Duration,
    ) -> Result<ton::GetDifferenceResponse> {
        instrument!();

        trace!("Got GetDifferenceRequest: {:?}", query);

        let sources_delivered_heights = &*query.rt;

        if sources_delivered_heights.len() != self.get_sources_count() {
            warn!("Incorrect GetDifferenceRequest query from {}", adnl_id);
            bail!("bad vt size");
        }

        //check is fork detected for sources

        let sources_count = self.get_sources_count();

        for i in 0..sources_count {
            if sources_delivered_heights[i] < 0 {
                continue;
            }

            let source_ptr = self.get_source(i).clone();
            let ref source = source_ptr.borrow();

            if let Some(fork) = source.get_fork_proof() {
                //return differenceFork as response
                return Ok(ton::DifferenceFork {
                    left: fork.left.clone().only(),
                    right: fork.right.clone().only(),
                }
                .into_boxed());
            }
        }

        //prepare list of delivered heights for current node

        let mut total_diff = 0;

        let ours_sources_delivered_heights: Vec<BlockHeight> = (0..sources_count)
            .map(|i| {
                if sources_delivered_heights[i] >= 0 {
                    let height = sources_delivered_heights[i];
                    let source_ptr = self.get_source(i).clone();
                    let source = source_ptr.borrow();

                    if source.get_delivered_height() > height {
                        total_diff += source.get_delivered_height() - height;
                    }

                    source.get_delivered_height()
                } else {
                    -1
                }
            })
            .collect();

        //compute optimal number of blocks for sending

        const MAX_BLOCKS_TO_SEND: BlockHeight = 100;
        const OVERLOAD_DELAY: std::time::Duration = std::time::Duration::from_millis(500);
        const OVERLOAD_MAX_DIVIDER: f64 = 20.0;

        let mut max_blocks_to_send = MAX_BLOCKS_TO_SEND;

        if query_latency > OVERLOAD_DELAY {
            let mut divider = query_latency.as_secs_f64() / OVERLOAD_DELAY.as_secs_f64();

            if divider > OVERLOAD_MAX_DIVIDER {
                divider = OVERLOAD_MAX_DIVIDER;
            }

            max_blocks_to_send = (max_blocks_to_send as f64 / divider) as i32;
        }

        let mut left: BlockHeight = 0;
        let mut right: BlockHeight = max_blocks_to_send + 1;

        while right - left > 1 {
            let middle = (right + left) / 2;
            let mut sum: i64 = 0;

            for i in 0..sources_count {
                let diff = ours_sources_delivered_heights[i] - sources_delivered_heights[i];

                if sources_delivered_heights[i] >= 0 && diff > 0 {
                    //increase number of blocks for delivering if there are delivered blocks on current validator
                    //which are not known by counterparty

                    sum += if diff > middle { middle } else { diff } as i64;
                }
            }

            //limit number of blocks for sending

            if sum > max_blocks_to_send as i64 {
                right = middle;
            } else {
                left = middle;
            }
        }

        //send blocks to counterparty

        assert!(right > 0);

        let mut response_sources_delivered_heights: Vec<BlockHeight> =
            sources_delivered_heights.to_vec().clone();

        let mut total_sent_blocks = 0;

        for i in 0..sources_count {
            let diff = ours_sources_delivered_heights[i] - sources_delivered_heights[i];

            if sources_delivered_heights[i] < 0 || diff <= 0 {
                continue;
            }

            let source = self.get_source(i);
            let blocks_to_send = if diff > right { right } else { diff };

            assert!(blocks_to_send > 0);

            for _j in 0..blocks_to_send {
                response_sources_delivered_heights[i] += 1;

                let block_ptr = source
                    .borrow()
                    .get_block(response_sources_delivered_heights[i])
                    .unwrap();
                let mut block = block_ptr.borrow_mut();

                if block.mark_block_for_sending(&adnl_id) {
                    //send block update event to counterparty

                    self.send_block_update_event(
                        adnl_id,
                        block.get_serialized_block_with_payload(),
                        true,
                    );

                    total_sent_blocks += 1;
                }
            }
        }

        const BLOCKS_SENT_WARN_THRESHOLD: usize = MAX_BLOCKS_TO_SEND as usize / 2;

        if total_sent_blocks > BLOCKS_SENT_WARN_THRESHOLD {
            warn!(
                "Sending {} absent blocks to node with ADNL ID {}",
                total_sent_blocks, adnl_id
            );
        }

        //send response to counterparty

        let response = ::ton_api::ton::catchain::difference::Difference {
            sent_upto: response_sources_delivered_heights.into(),
        }
        .into_boxed();

        Ok(response)
    }

    fn process_get_block_query(
        &mut self,
        _adnl_id: &PublicKeyHash,
        query: &ton::GetBlockRequest,
    ) -> Result<(ton::BlockResultResponse, BlockPayloadPtr)> {
        instrument!();

        trace!("Got GetBlockQuery: {:?}", query);

        let block_hash = query.block.clone();
        let block_result = self.get_block_by_hash(&block_hash);

        if let Some(block_ptr) = block_result {
            let block = block_ptr.borrow();

            if block.get_height() != 0 && block.is_initialized() {
                let response = ::ton_api::ton::catchain::blockresult::BlockResult {
                    block: block.export_tl(),
                }
                .into_boxed();

                return Ok((response, block.get_payload().clone()));
            }
        }

        let response = ::ton_api::ton::catchain::BlockResult::Catchain_BlockNotFound;

        return Ok((response, CatchainFactory::create_empty_block_payload()));
    }

    fn process_get_blocks_query(
        &mut self,
        adnl_id: &PublicKeyHash,
        query: &ton::GetBlocksRequest,
    ) -> Result<CatchainSentResponse> {
        instrument!();

        //limit number of blocks to be process by the request

        if query.blocks.len() > 100 {
            bail!("too many blocks");
        }

        //process blocks and send them back to the requester

        let mut response_blocks_count = 0;

        for block_hash in &query.blocks.0 {
            if let Some(block_ptr) = self.get_block_by_hash(&block_hash) {
                let mut block = block_ptr.borrow_mut();

                assert!(block.get_payload().data().len() > 0);

                if block.get_height() <= 0 {
                    continue;
                }

                if !block.mark_block_for_sending(&adnl_id) {
                    continue;
                }

                //send block to the requester

                self.send_block_update_event(
                    adnl_id,
                    block.get_serialized_block_with_payload(),
                    true,
                );

                response_blocks_count += 1;
            }
        }

        //prepare response the the requester

        Ok(ton_api::ton::catchain::sent::Sent {
            cnt: response_blocks_count,
        }
        .into_boxed())
    }

    fn process_get_block_history_query(
        &mut self,
        adnl_id: &PublicKeyHash,
        query: &ton::GetBlockHistoryRequest,
    ) -> Result<CatchainSentResponse> {
        instrument!();

        //limit number of blocks to be processed by the request

        let mut height = match query.height {
            height if height <= 0 => bail!("not-positive height"),
            height if height > 100 => 100,
            height => height,
        };

        //process blocks and send them back to the requester

        let mut response_blocks_count = 0;

        if let Some(block) = self.get_block_by_hash(&query.block) {
            //limit height by the requested block height

            let block_height = block.borrow().get_height() as i64;

            if height < block_height {
                height = block_height;
            }

            //iterate from the block to the root of the chain

            let block_terminators = &query.stop_if;
            let mut block_it = block.clone();

            for _i in 0..height {
                let mut block = block_it.borrow_mut();

                //terminate loop if the termination block has been found

                if block_terminators.contains(block.get_hash()) {
                    break;
                }

                assert!(block.get_payload().data().len() > 0);

                if block.mark_block_for_sending(&adnl_id) {
                    //send block back to the requester

                    self.send_block_update_event(
                        adnl_id,
                        block.get_serialized_block_with_payload(),
                        true,
                    );

                    response_blocks_count += 1;
                }

                //move to the next block

                let prev = match block_it.borrow().get_prev() {
                    Some(prev) => prev.clone(),
                    None => bail!("Block.height is incorrect. There is no previous block"),
                };

                drop(block);

                block_it = prev;
            }
        }

        Ok(ton_api::ton::catchain::sent::Sent {
            cnt: response_blocks_count,
        }
        .into_boxed())
    }

    /*
        Forks management
    */

    fn got_fork_proof(&mut self, fork_proof: &ton::DifferenceFork) {
        if let Err(status) = self.validate_block_dependency(&fork_proof.left) {
            warn!("Incorrect fork blame, left is invalid: {:?}", status);
            return;
        }

        if let Err(status) = self.validate_block_dependency(&fork_proof.right) {
            warn!("Incorrect fork blame, right is invalid: {:?}", status);
            return;
        }

        if fork_proof.left.height != fork_proof.right.height
            || fork_proof.left.src != fork_proof.right.src
            || fork_proof.left.data_hash == fork_proof.right.data_hash
        {
            warn!(
                "Incorrect fork blame, not a fork: {}/{}, {}/{}, {:?}/{:?}",
                fork_proof.left.height,
                fork_proof.right.height,
                fork_proof.left.src,
                fork_proof.right.src,
                fork_proof.left.data_hash,
                fork_proof.right.data_hash
            );
            return;
        }

        let source = self.get_source(fork_proof.left.src as usize);

        source.borrow_mut().set_fork_proof(ton::BlockDataFork {
            left: fork_proof.left.clone().into_boxed(),
            right: fork_proof.right.clone().into_boxed(),
        });
        source.borrow_mut().mark_as_blamed(self);
    }

    /*
        Listener callbacks (Receiver -> Catchain)
    */

    fn notify_on_started(&mut self) {
        check_execution_time!(20000);

        if let Some(listener) = self.listener.upgrade() {
            listener.borrow_mut().on_started();
        }
    }

    fn notify_on_new_block(
        &mut self,
        source_id: usize,
        fork_id: usize,
        hash: BlockHash,
        height: BlockHeight,
        prev: BlockHash,
        deps: Vec<BlockHash>,
        forks_dep_heights: Vec<BlockHeight>,
        payload: &BlockPayloadPtr,
    ) {
        check_execution_time!(20000);
        instrument!();

        if let Some(listener) = self.listener.upgrade() {
            listener.borrow_mut().on_new_block(
                self,
                source_id,
                fork_id,
                hash,
                height,
                prev,
                deps,
                forks_dep_heights,
                payload,
            );
        }
    }

    fn notify_on_broadcast(&mut self, source_key_hash: &PublicKeyHash, data: &BlockPayloadPtr) {
        check_execution_time!(20000);
        instrument!();

        if let Some(listener) = self.listener.upgrade() {
            listener
                .borrow_mut()
                .on_broadcast(self, source_key_hash, data);
        }
    }

    fn notify_on_blame(&mut self, source_id: usize) {
        check_execution_time!(20000);
        if let Some(listener) = self.listener.upgrade() {
            listener.borrow_mut().on_blame(self, source_id);
        }
    }

    fn notify_on_custom_query(
        &mut self,
        source_public_key_hash: &PublicKeyHash,
        data: &BlockPayloadPtr,
        response_promise: ExternalQueryResponseCallback,
    ) {
        check_execution_time!(20000);
        profiling::instrument!();

        if let Some(listener) = self.listener.upgrade() {
            listener.borrow_mut().on_custom_query(
                self,
                source_public_key_hash,
                data,
                response_promise,
            );
        }
    }

    /*
        Listener callbacks (Catchain -> Overlay)
    */

    fn send_block_update_event(
        &mut self,
        receiver_adnl_id: &PublicKeyHash,
        serialized_block_with_payload: &BlockPayloadPtr,
        can_be_postponed: bool,
    ) {
        instrument!();

        if let Some(listener) = self.listener.upgrade() {
            trace!(
                "Send to {}: {:?}",
                receiver_adnl_id,
                serialized_block_with_payload
            );

            if let Some(ref source) = self.get_source_by_adnl_id(receiver_adnl_id) {
                source.borrow_mut().get_mut_statistics().out_messages_count += 1;
            }

            self.out_messages_counter.increment();

            listener.borrow_mut().send_message(
                receiver_adnl_id,
                &self.get_source(self.local_idx).borrow().get_adnl_id(),
                &serialized_block_with_payload,
                can_be_postponed,
            );
        }
    }

    fn send_block_update_event_multicast(
        &mut self,
        receiver_adnl_ids: &[PublicKeyHash],
        serialized_block_with_payload: &BlockPayloadPtr,
    ) {
        instrument!();

        if let Some(listener) = self.listener.upgrade() {
            trace!(
                "Send to {:?}: {:?}",
                public_key_hashes_to_string(receiver_adnl_ids),
                serialized_block_with_payload
            );

            self.out_messages_counter.increment();

            for receiver_adnl_id in receiver_adnl_ids {
                if let Some(ref source) = self.get_source_by_adnl_id(receiver_adnl_id) {
                    source.borrow_mut().get_mut_statistics().out_messages_count += 1;
                }
            }

            listener.borrow_mut().send_message_multicast(
                receiver_adnl_ids,
                &self.get_source(self.local_idx).borrow().get_adnl_id(),
                &serialized_block_with_payload,
            );
        }
    }

    fn create_response_handler_boxed<T, F>(
        response_callback: F,
    ) -> Box<dyn FnOnce(Result<BlockPayloadPtr>, &mut dyn Receiver)>
    where
        T: 'static + ::ton_api::BoxedDeserialize + ::ton_api::AnyBoxedSerialize,
        F: FnOnce(Result<T>, BlockPayloadPtr, &mut dyn Receiver) + 'static,
    {
        let boxed_response_callback = Box::new(response_callback);

        let handler =
            move |result: Result<BlockPayloadPtr>, receiver: &mut dyn Receiver| match result {
                Err(err) => boxed_response_callback(
                    Err(err),
                    CatchainFactory::create_empty_block_payload(),
                    receiver,
                ),
                Ok(payload) => {
                    let data: &mut &[u8] = &mut payload.data().0.as_ref();
                    let reader: &mut dyn std::io::Read = data;
                    let mut deserializer = ton_api::Deserializer::new(reader);

                    match deserializer.read_boxed::<ton_api::ton::TLObject>() {
                        Ok(response) => match response.downcast::<T>() {
                            Ok(response) => {
                                let mut payload = Vec::new();

                                match reader.read_to_end(&mut payload) {
                                    Ok(_) => {
                                        let payload = CatchainFactory::create_block_payload(
                                            ton_api::ton::bytes(payload),
                                        );

                                        boxed_response_callback(Ok(response), payload, receiver)
                                    }
                                    Err(err) => boxed_response_callback(
                                        Err(err.into()),
                                        CatchainFactory::create_empty_block_payload(),
                                        receiver,
                                    ),
                                }
                            }
                            Err(obj) => boxed_response_callback(
                                Err(format_err!("unknown response {:?}", obj)),
                                CatchainFactory::create_empty_block_payload(),
                                receiver,
                            ),
                        },
                        Err(err) => boxed_response_callback(
                            Err(err),
                            CatchainFactory::create_empty_block_payload(),
                            receiver,
                        ),
                    }
                }
            };

        Box::new(handler)
    }

    #[allow(dead_code)]
    fn create_response_handler<T, F>(
        response_callback: F,
    ) -> Box<dyn FnOnce(Result<BlockPayloadPtr>, &mut dyn Receiver)>
    where
        T: 'static + ::ton_api::BoxedDeserialize,
        F: FnOnce(Result<T>, BlockPayloadPtr, &mut dyn Receiver) + 'static,
    {
        let boxed_response_callback = Box::new(response_callback);

        let handler =
            move |result: Result<BlockPayloadPtr>, receiver: &mut dyn Receiver| match result {
                Err(err) => boxed_response_callback(
                    Err(err),
                    CatchainFactory::create_empty_block_payload(),
                    receiver,
                ),
                Ok(payload) => boxed_response_callback(
                    utils::deserialize_tl_boxed_object::<T>(payload.data()),
                    payload,
                    receiver,
                ),
            };

        Box::new(handler)
    }

    fn send_get_block_request<F>(
        &mut self,
        receiver_adnl_id: &PublicKeyHash,
        request: ton::GetBlockRequest,
        response_callback: F,
    ) where
        F: FnOnce(Result<ton::BlockResultResponse>, BlockPayloadPtr, &mut dyn Receiver) + 'static,
    {
        instrument!();

        if let Some(listener) = self.listener.upgrade() {
            trace!("...query GetBlock {}: {:?}", receiver_adnl_id, request);

            let serialized_message = serialize_tl_boxed_object!(&request);

            static GET_BLOCK_QUERY_TIMEOUT: Duration = Duration::from_millis(2000);

            listener.borrow_mut().send_query(
                receiver_adnl_id,
                &self.get_source(self.local_idx).borrow().get_adnl_id(),
                "sync blocks",
                GET_BLOCK_QUERY_TIMEOUT,
                &CatchainFactory::create_block_payload(serialized_message),
                Self::create_response_handler_boxed(response_callback),
            );
        }
    }

    fn send_get_difference_request<F>(
        &mut self,
        receiver_adnl_id: &PublicKeyHash,
        request: ton::GetDifferenceRequest,
        response_callback: F,
    ) where
        F: FnOnce(Result<ton::GetDifferenceResponse>, BlockPayloadPtr, &mut dyn Receiver) + 'static,
    {
        instrument!();

        if let Some(listener) = self.listener.upgrade() {
            trace!("...query GetDifference {}: {:?}", receiver_adnl_id, request);

            let serialized_message = serialize_tl_boxed_object!(&request);

            static GET_DIFFERENCE_QUERY_TIMEOUT: Duration = Duration::from_millis(5000);

            listener.borrow_mut().send_query(
                receiver_adnl_id,
                &self.get_source(self.local_idx).borrow().get_adnl_id(),
                "sync",
                GET_DIFFERENCE_QUERY_TIMEOUT,
                &CatchainFactory::create_block_payload(serialized_message),
                Self::create_response_handler_boxed(response_callback),
            );
        }
    }

    /*
        Creation
    */

    fn new(
        listener: ReceiverListenerPtr,
        incarnation: &SessionId,
        ids: &Vec<CatchainNode>,
        local_key: &PrivateKey,
        path: String,
        db_suffix: String,
        allow_unsafe_self_blocks_resync: bool,
        metrics_receiver: Option<Arc<metrics_runtime::Receiver>>,
    ) -> Result<Self> {
        let metrics_receiver = if let Some(metrics_receiver) = metrics_receiver {
            metrics_receiver.clone()
        } else {
            Arc::new(
                metrics_runtime::Receiver::builder()
                    .build()
                    .expect("failed to create metrics receiver"),
            )
        };
        let received_blocks_instance_counter =
            InstanceCounter::new(&metrics_receiver, &"received_blocks".to_owned());
        let out_queries_counter =
            ResultStatusCounter::new(&metrics_receiver, &"receiver_out_queries".to_owned());
        let in_queries_counter =
            ResultStatusCounter::new(&metrics_receiver, &"receiver_in_queries".to_owned());
        let out_messages_counter = metrics_receiver.sink().counter("receiver_out_messages");
        let in_messages_counter = metrics_receiver.sink().counter("receiver_in_messages");
        let in_broadcasts_counter = metrics_receiver.sink().counter("receiver_in_broadcasts");

        let sources_count = ids.len();

        debug!(
            "Creating catchaing receiver for session incarnation {:?} with {} sources",
            incarnation, sources_count
        );

        let root_block = CatchainFactory::create_root_received_block(
            sources_count,
            incarnation,
            &received_blocks_instance_counter,
        );

        trace!(
            "...creating root received block for receiver session incarnation {:?}",
            incarnation
        );

        let local_key_id_finder = || {
            let local_id = local_key.id();
            for (i, id) in ids.iter().enumerate() {
                if get_public_key_hash(&id.public_key) == *local_id {
                    return (local_id, i);
                }
            }

            unreachable!(
                "LocalID {:?} has not been found in catchain nodes",
                local_id
            );
        };
        let (local_id, local_idx) = local_key_id_finder();

        assert!(sources_count == 0 || local_idx != sources_count);

        debug!(
            "Receiver local_idx={}, sources_count={}",
            local_idx, sources_count
        );

        let mut sources = Vec::new();
        let mut public_key_hash_to_source = HashMap::new();
        let mut adnl_id_to_source = HashMap::new();
        let mut source_public_key_hashes = Vec::new();

        for id in ids {
            let source_id = sources.len();
            let source = CatchainFactory::create_receiver_source(
                source_id,
                id.public_key.clone(),
                &id.adnl_id,
            );

            let public_key_hash = id.public_key.id().clone();

            sources.push(source.clone());
            source_public_key_hashes.push(public_key_hash.clone());
            public_key_hash_to_source.insert(public_key_hash.clone(), source.clone());
            adnl_id_to_source.insert(id.adnl_id.clone(), source.clone());
        }

        let now = SystemTime::now();
        let mut obj = ReceiverImpl {
            sources: sources,
            public_key_hash_to_source: public_key_hash_to_source,
            adnl_id_to_source: adnl_id_to_source,
            source_public_key_hashes: source_public_key_hashes,
            incarnation: incarnation.clone(),
            options: Options {
                idle_timeout: std::time::Duration::new(16, 0),
                max_deps: 4,
                debug_disable_db: false,
                skip_processed_blocks: false,
            },
            blocks: HashMap::new(),
            blocks_to_run: VecDeque::new(),
            root_block: root_block.clone(),
            pending_blocks: VecDeque::new(),
            active_send: false,
            last_sent_block: root_block.clone(),
            _local_ids: ids.to_vec(),
            local_id: local_id.clone(),
            local_key: local_key.clone(),
            local_idx: local_idx,
            total_forks: 0,
            neighbours: Vec::new(),
            listener: listener,
            metrics_receiver: metrics_receiver,
            received_blocks_instance_counter: received_blocks_instance_counter,
            out_queries_counter: out_queries_counter,
            in_queries_counter: in_queries_counter,
            out_messages_counter: out_messages_counter,
            in_messages_counter: in_messages_counter,
            in_broadcasts_counter: in_broadcasts_counter,
            pending_in_db: 0,
            db: None,
            read_db: false,
            db_root_block: ZERO_HASH.clone(),
            db_suffix,
            allow_unsafe_self_blocks_resync,
            unsafe_root_block_writing: false,
            started: false,
            next_awake_time: now,
            next_sync_time: now,
            next_neighbours_rotate_time: now,
            initial_sync_complete_time: now
                + Duration::from_secs(INFINITE_INITIAL_SYNC_COMPLETE_TIME_SECS),
            rng: rand::thread_rng(),
            get_pending_deps_call_id: 0,
        };

        obj.add_received_block(root_block.clone());
        obj.start_up_db(path)?;
        obj.choose_neighbours();
        Ok(obj)
    }

    pub(crate) fn create_dummy_listener() -> Rc<RefCell<dyn ReceiverListener>> {
        DummyListener::create()
    }

    pub(crate) fn create_dummy(path: String) -> Result<ReceiverPtr> {
        let public_key = Ed25519KeyOption::generate()?;
        let ids = vec![CatchainNode {
            public_key: public_key.clone(),
            adnl_id: public_key.id().clone(),
        }];
        let incarnation = public_key.id().data().clone().into();
        let receiver_listener = ReceiverImpl::create_dummy_listener();
        let db_suffix = String::new();
        let allow_unsafe_self_blocks_resync = false;

        ReceiverImpl::create(
            Rc::downgrade(&receiver_listener),
            &incarnation,
            &ids,
            &public_key,
            path,
            db_suffix,
            allow_unsafe_self_blocks_resync,
            None,
        )
    }

    pub(crate) fn create(
        listener: ReceiverListenerPtr,
        incarnation: &SessionId,
        ids: &Vec<CatchainNode>,
        local_key: &PrivateKey,
        path: String,
        db_suffix: String,
        allow_unsafe_self_blocks_resync: bool,
        metrics: Option<Arc<metrics_runtime::Receiver>>,
    ) -> Result<ReceiverPtr> {
        let ret = ReceiverImpl::new(
            listener,
            incarnation,
            ids,
            local_key,
            path,
            db_suffix,
            allow_unsafe_self_blocks_resync,
            metrics,
        )?;
        Ok(Rc::new(RefCell::new(ret)))
    }

    fn create_block_with_payload(
        &mut self,
        block: &ton::Block,
        payload: BlockPayloadPtr,
    ) -> Result<ReceivedBlockPtr> {
        instrument!();

        if block.height == 0 {
            return Ok(self.root_block.clone());
        }

        let block_id = self.get_block_id(block, payload.data());
        let block_hash = get_block_id_hash(&block_id);

        if let Some(existing_block_entry) = self.blocks.get(&block_hash) {
            let existing_block = existing_block_entry.clone();

            if !existing_block.borrow().is_initialized() {
                trace!(
                    "...create block with hash={:?} exists but has not been initialized",
                    block_hash
                );

                existing_block
                    .borrow_mut()
                    .initialize(&block, payload, self)?;
            }

            return Ok(existing_block.clone());
        } else {
            trace!("...create block with hash={:?}", block_hash);

            let new_block =
                received_block::ReceivedBlockImpl::create_with_payload(&block, payload, self)?;

            self.add_received_block(new_block.clone());

            return Ok(new_block);
        }
    }
}
