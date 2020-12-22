use self::ton::CatchainSentResponse;
pub use super::*;
use rand::Rng;
use std::collections::HashMap;
use std::collections::VecDeque;
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
    payload: BlockPayload,      //payload of a new block
    dep_hashes: Vec<BlockHash>, //list of dependencies for a new block
}

pub(crate) struct ReceiverImpl {
    incarnation: SessionId,                            //session ID (incarnation)
    options: Options,                                  //Catchain options
    blocks: HashMap<BlockHash, ReceivedBlockPtr>,      //all received blocks
    blocks_to_run: VecDeque<ReceivedBlockPtr>, //blocks which has been scheduled for delivering (fully resolved)
    root_block: ReceivedBlockPtr, //root block for catchain sesssion (hash is equal to session incarnation)
    last_sent_block: ReceivedBlockPtr, //last block sent to catchain
    active_send: bool,            //active send flag for adding new blocks
    pending_blocks: VecDeque<PendingBlock>, //pending blocks for sending
    sources: Vec<ReceiverSourcePtr>, //receiver sources (knowledge about other catchain validators)
    source_public_key_hashes: Vec<PublicKeyHash>, //public key hashes of all sources
    source_adnl_addrs: Vec<PublicKeyHash>, //ADNL ids of all sources
    local_id: PublicKeyHash,      //this node public key hash
    local_key: PublicKey,         //this node public key
    local_idx: usize,             //this node source index
    _local_ids: Vec<CatchainNode>, //receiver sources identifiers (pub keys, ADNL ids)
    total_forks: usize,           //total forks number for this receiver
    neighbours: Vec<usize>,       //list of neighbour indices to synchronize
    listener: ReceiverListenerPtr, //listener for callbacks
    metrics_receiver: metrics_runtime::Receiver, //receiver for profiling metrics
    received_blocks_instance_counter: InstanceCounter, //received blocks instances
    pending_in_db: i32,           //blocks pending to read from DB
    db: Option<DatabasePtr>,      //database with (BlockHash, Payload)
    read_db: bool,                //flag to indicate receiver is in reading DB mode
    db_root_block: BlockHash,     //root DB block
    db_root: String,              //DB root prefix
    db_suffix: String,            //DB name suffix
    allow_unsafe_self_blocks_resync: bool, //indicates we can receive self blocks from other validators
    unsafe_root_block_writing: bool, //indicates we are in the middle of unsafe root block writing
    started: bool,                   //indicates catchain is started
    next_awake_time: SystemTime,     //next awake timestamp
    next_sync_time: SystemTime,      //time to do next sync with a random neighbour
    next_neighbours_rotate_time: SystemTime, //time to change neighbours
    initial_sync_complete_time: SystemTime, //time to finish initial synchronization
    rng: rand::rngs::ThreadRng,      //random generator
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

    fn add_fork_proof(&mut self, fork_proof: &BlockPayload) {
        trace!("...add block {:?} as a fork proof", fork_proof);

        self.add_block_for_delivery(fork_proof.clone(), Vec::new());
    }

    /*
        General ReceivedBlock methods
    */

    fn get_block_by_hash(&self, b: &BlockHash) -> Option<ReceivedBlockPtr> {
        match self.blocks.get(b) {
            None => None,
            Some(t) => Some(t.clone()),
        }
    }

    /*
        Block validation management
    */

    fn validate_block_dependency(&self, dep: &ton::BlockDep) -> Result<()> {
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
        data: &BlockPayload,
        response_callback: ExternalQueryResponseCallback,
    ) {
        if !self.read_db {
            response_callback(Err(format_err!(
                "DB is not read for catchain receiver {}",
                self.incarnation.to_hex_string()
            )));
            return;
        }

        let result = self.process_query(&adnl_id, &data);

        if let Ok(result) = result {
            response_callback(Ok(result));
            return;
        }

        let source_public_key_hash = self
            .get_source_by_adnl_id(adnl_id)
            .unwrap()
            .borrow()
            .get_public_key_hash()
            .clone();

        self.notify_on_custom_query(&source_public_key_hash, data, response_callback);
    }

    /*
        ReceivedBlock receiving flow implementation
    */

    fn receive_block(
        &mut self,
        adnl_id: &PublicKeyHash,
        block: &ton::Block,
        payload: BlockPayload,
    ) -> Result<ReceivedBlockPtr> {
        let id = self.get_block_id(&block, &payload);
        let hash = get_block_id_hash(&id);
        let block_opt = self.get_block_by_hash(&hash);

        if log_enabled!(log::Level::Debug) {
            debug!(
                "New block with hash={:?} and id={:?} has been received",
                hash, id
            );
        }

        if let Some(block) = block_opt {
            if block.borrow().is_initialized() {
                if log_enabled!(log::Level::Debug) {
                    trace!(
                        "...skip block {:?} because it has been already initialized",
                        hash
                    );
                }

                return Ok(block.clone());
            }
        }

        if UInt256::from(block.incarnation.0) != self.incarnation {
            let message = format!(
                "Block from source {} incarnation mismatch: expected {} but received {:?}",
                adnl_id,
                self.incarnation.to_hex_string(),
                UInt256::from(block.incarnation.0)
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
            let raw_data = utils::serialize_block_with_payload(block, &payload).unwrap();

            db.put_block(&hash, raw_data);
        }

        self.block_written_to_db(&id);

        if log_enabled!(log::Level::Debug) {
            trace!(
                "...block {:?} has been successfully processed after receiving",
                hash
            );
        }

        Ok(received_block)
    }

    fn receive_message_from_overlay(
        &mut self,
        adnl_id: &PublicKeyHash,
        bytes: &mut &[u8],
    ) -> Result<ReceivedBlockPtr> {
        if !self.read_db {
            bail!("DB is not read");
        }

        if log_enabled!(log::Level::Debug) {
            debug!(
                "Receive message from overlay for source {}: {}",
                adnl_id,
                &hex::encode(&bytes)
            );
        }

        let reader: &mut dyn std::io::Read = bytes;
        let mut deserializer = ton_api::Deserializer::new(reader);

        match deserializer.read_boxed::<ton_api::ton::TLObject>() {
            Ok(message) => {
                if message.is::<::ton_api::ton::catchain::Update>() {
                    let mut payload = Vec::new();

                    reader.read_to_end(&mut payload)?;

                    let payload = ton_api::ton::bytes(payload);

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
        data: &BlockPayload,
    ) {
        if !self.read_db {
            return;
        }

        self.notify_on_broadcast(source_key_hash, &data);
    }

    /*
        ReceivedBlock delivery flow implementation
    */

    fn run_block(&mut self, block: ReceivedBlockPtr) {
        self.blocks_to_run.push_back(block.clone());
    }

    fn deliver_block(&mut self, block: &mut dyn ReceivedBlock) {
        debug!(
            "Catchain delivering block {:?} from source={} fork={} height={} custom={}",
            block.get_hash(),
            block.get_source_id(),
            block.get_fork_id(),
            block.get_height(),
            block.is_custom()
        );

        //notify listeners about new block appearance

        lazy_static! {
            static ref DEFAULT_BLOCK: BlockPayload = BlockPayload::default();
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

            receiver_addresses.push(neighbour.borrow().get_adnl_id().clone());
        }

        let block_update_event = ton::BlockUpdateEvent {
            block: block.export_tl(),
        };

        self.send_block_update_event_multicast(
            receiver_addresses.as_ref(),
            block_update_event,
            &block.get_payload(),
        );
    }

    fn process(&mut self) {
        while let Some(pending_block) = self.pending_blocks.pop_front() {
            self.add_block_impl(pending_block.payload, pending_block.dep_hashes);
        }

        if self.blocks_to_run.len() > 0 {
            self.run_scheduler();
        }
    }

    /*
        ReceivedBlock creation
    */

    fn create_block(&mut self, block: &ton::BlockDep) -> ReceivedBlockPtr {
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

    fn add_block(&mut self, payload: BlockPayload, deps: Vec<BlockHash>) {
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

    fn dump_metrics(&self) {
        utils::dump_metrics(&self.metrics_receiver, &utils::dump_metric);
    }

    /*
        Triggers
    */

    fn check_all(&mut self) {
        let now = SystemTime::now();

        //synchronize with choosen neighbours

        if let Ok(_elapsed) = self.next_sync_time.elapsed() {
            self.synchronize();

            self.next_sync_time = now
                + Duration::from_millis(self.rng.gen_range(
                    CATCHAIN_NEIGHBOURS_SYNC_MIN_PERIOD_MS,
                    CATCHAIN_NEIGHBOURS_SYNC_MAX_PERIOD_MS + 1,
                ));

            trace!(
                "...next sync is scheduled at {}",
                utils::time_to_string(&self.next_sync_time)
            );
        }

        //rotate neighbours

        if let Ok(_elapsed) = self.next_neighbours_rotate_time.elapsed() {
            self.choose_neighbours();

            self.next_neighbours_rotate_time = now
                + Duration::from_millis(self.rng.gen_range(
                    CATCHAIN_NEIGHBOURS_ROTATE_MIN_PERIOD_MS,
                    CATCHAIN_NEIGHBOURS_ROTATE_MAX_PERIOD_MS + 1,
                ));

            trace!(
                "...next neighbours rotation is scheduled at {}",
                utils::time_to_string(&self.next_neighbours_rotate_time)
            );
        }

        //start up checks (for unsafe startup)

        if !self.started && self.read_db {
            let elapsed = self.initial_sync_complete_time.elapsed();

            if let Ok(_elapsed) = elapsed {
                let allow = if self.allow_unsafe_self_blocks_resync {
                    self.unsafe_start_up_check_completed()
                } else {
                    true
                };

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
}

/*
    Dummy listener for receiver
*/

struct DummyListener {}

impl DummyListener {
    pub(crate) fn new() -> Self {
        Self {}
    }
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
        _payload: &BlockPayload,
    ) {
    }

    fn on_broadcast(
        &mut self,
        _receiver: &mut dyn Receiver,
        _source_key_hash: &PublicKeyHash,
        _data: &BlockPayload,
    ) {
    }

    fn on_blame(&mut self, _receiver: &mut dyn Receiver, _source_id: usize) {}

    fn on_custom_message(
        &mut self,
        _receiver: &mut dyn Receiver,
        _source_public_key_hash: &PublicKeyHash,
        _data: &BlockPayload,
    ) {
    }

    fn on_custom_query(
        &mut self,
        _receiver: &mut dyn Receiver,
        _source_public_key_hash: &PublicKeyHash,
        _data: &BlockPayload,
        _response_callback: ExternalQueryResponseCallback,
    ) {
    }

    fn send_message(
        &mut self,
        _receiver_id: &PublicKeyHash,
        _sender_id: &PublicKeyHash,
        _message: &BlockPayload,
    ) {
    }

    fn send_message_multicast(
        &mut self,
        _receiver_ids: &[PublicKeyHash],
        _sender_id: &PublicKeyHash,
        _message: &BlockPayload,
    ) {
    }

    fn send_query(
        &mut self,
        _receiver_id: &PublicKeyHash,
        _sender_id: &PublicKeyHash,
        _name: &str,
        _timeout: std::time::Duration,
        _message: &BlockPayload,
        _response_callback: QueryResponseCallback,
    ) {
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
        //todo: optimize with hash map

        for (i, hash) in self.source_public_key_hashes.iter().enumerate() {
            if hash != source_public_key_hash {
                continue;
            }

            return Some(self.sources[i].clone());
        }

        None
    }

    fn get_source_by_adnl_id(&self, adnl_id: &PublicKeyHash) -> Option<ReceiverSourcePtr> {
        //todo: optimize with hash map

        for (i, id) in self.source_adnl_addrs.iter().enumerate() {
            if id != adnl_id {
                continue;
            }

            return Some(self.sources[i].clone());
        }

        None
    }

    /*
        General ReceivedBlock methods
    */

    fn get_block_id(&self, block: &ton::Block, payload: &BlockPayload) -> ton::BlockId {
        utils::get_block_id(
            &UInt256::from(block.incarnation.0),
            &self.get_source_public_key_hash(block.src as usize),
            block.height,
            payload,
        )
    }

    fn get_block_dependency_id(&self, dep: &ton::BlockDep) -> ton::BlockId {
        let incarnation = &self.incarnation;
        let source_hash = self.get_source_public_key_hash(dep.src as usize);
        let height = dep.height;
        let data_hash = dep.data_hash;

        ::ton_api::ton::catchain::block::Id::Catchain_Block_Id(Box::new(
            ::ton_api::ton::catchain::block::id::Id {
                incarnation: incarnation.into(),
                src: public_key_hash_to_int256(source_hash),
                height: height,
                data_hash: data_hash,
            },
        ))
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
        payload: &BlockPayload,
    ) -> Result<()> {
        (received_block::ReceivedBlockImpl::pre_validate_block(self, block, payload))?;

        if block.height <= 0 {
            return Ok(());
        }

        let id = &self.get_block_id(&block, &payload);
        let serialized_block_id = utils::serialize_tl_boxed_object!(id);

        if let Some(_block) = self.get_block_by_hash(&utils::get_hash(&serialized_block_id)) {
            return Ok(());
        }

        let source = self.get_source_by_hash(&utils::int256_to_public_key_hash(id.src()));

        assert!(source.is_some());

        let public_key = source.unwrap().borrow().get_public_key().clone();

        match public_key.verify(&serialized_block_id.0, &block.signature.0) {
            Err(err) => {
                error!("Verification failed for block {:?}: {:?}", id, err);
                Ok(())
            } //Err(err), //TODO: return error instead of OK after debugging
            Ok(_) => Ok(()),
        }
    }

    /*
        Block delivery management
    */

    fn run_scheduler(&mut self) {
        while let Some(block) = self.blocks_to_run.pop_front() {
            block.borrow_mut().process(self);
        }
    }

    fn add_block_for_delivery(&mut self, payload: BlockPayload, deps: Vec<BlockHash>) {
        self.pending_blocks.push_back(PendingBlock {
            payload: payload,
            dep_hashes: deps,
        });
    }

    fn add_block_impl(&mut self, payload: BlockPayload, deps: Vec<BlockHash>) {
        debug!(
            "Adding new block with deps {:?} and payload {:?}",
            deps, payload
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

        let block_id = self.get_block_id(&block, &payload);
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
            //save mapping: sha256(block_id) -> serialized block with payload

            let block_id_hash = get_block_id_hash(&block_id);
            let block_raw_data = utils::serialize_block_with_payload(&block, &payload).unwrap();

            db.put_block(&block_id_hash, block_raw_data);

            //save mapping for root block to it's ID

            db.put_block(
                &ZERO_HASH,
                ::ton_api::ton::bytes(block_id_hash.as_slice().to_vec()),
            );
        }

        //initiate delivery flow

        trace!("...deliver a new created block {:?}", block);

        match self.create_block_with_payload(&block, payload) {
            Ok(block) => {
                self.last_sent_block = block.clone();

                block.borrow_mut().written(self);
            }
            Err(err) => error!("...creation block error: {:?}", err),
        }

        self.run_scheduler();

        self.active_send = false;
    }

    /*
        Receiver blocks DB management
    */

    fn start_up_db(&mut self) {
        trace!("...starting up DB");

        if self.options.debug_disable_db {
            self.read_db();
            return;
        }

        let db = CatchainFactory::create_database(&format!(
            "{}/catchainreceiver{}{}",
            self.db_root,
            self.db_suffix,
            base64::encode(self.incarnation.as_slice())
        ));

        self.db = Some(db.clone());

        if let Ok(root_block) = db.get_block(&ZERO_HASH) {
            let root_block_id_hash: BlockHash = root_block.to_vec().into();

            self.read_db_from(&root_block_id_hash);
        } else {
            self.read_db();
        }
    }

    fn read_db(&mut self) {
        trace!("...reading DB");

        if self.db_root_block != ZERO_HASH.clone() {
            self.run_scheduler();

            self.last_sent_block = self.get_block_by_hash(&self.db_root_block).unwrap();

            assert!(self.last_sent_block.borrow().is_delivered());
        }

        self.read_db = true;

        let now = SystemTime::now();

        self.next_neighbours_rotate_time = now
            + Duration::from_millis(self.rng.gen_range(
                CATCHAIN_NEIGHBOURS_ROTATE_MIN_PERIOD_MS,
                CATCHAIN_NEIGHBOURS_ROTATE_MAX_PERIOD_MS,
            ));
        self.next_sync_time =
            now + Duration::from_millis(((0.001 * self.rng.gen_range(0.0, 60.0)) * 1000.0) as u64);
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

    fn read_db_from(&mut self, id: &BlockHash) {
        trace!("...reading DB from block {}", id);

        self.pending_in_db = 1;
        self.db_root_block = id.clone();

        let block_raw_data = self.db.as_ref().unwrap().get_block(id).unwrap();

        self.read_block_from_db(id, block_raw_data);
    }

    fn read_block_from_db(&mut self, id: &BlockHash, raw_data: BlockPayload) {
        trace!("...reading block {} from DB", id);

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

        if !message.is::<::ton_api::ton::catchain::Update>() {
            error!(
                "DB block {:?} parsing error: object does not contain Update message: object={:?}",
                id, message
            );
            return;
        }

        let block = message
            .downcast::<::ton_api::ton::catchain::Update>()
            .unwrap();
        let block = &block.block();

        //parse payload of a block

        let mut payload = Vec::new();

        reader.read_to_end(&mut payload).unwrap();

        let payload = ton_api::ton::bytes(payload);

        //check block ID

        let block_id = self.get_block_id(&block, &payload);
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

        assert!(block.incarnation == self.incarnation.clone().into());

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

        let block = self.create_block_with_payload(block, payload).unwrap();

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

            //do recursion for block parsing

            self.read_block_from_db(dep, dep_block);
        }

        //deliver blocks when all dependencies are requested from DB

        if self.pending_in_db == 0 {
            self.read_db();
        }
    }

    fn block_written_to_db(&mut self, block_id: &ton::BlockId) {
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
        debug!("Rotate neighbours");

        //randomly choose max neighbours from sources

        let sources_count = self.get_sources_count();
        let mut rng = rand::thread_rng();
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

            let source = self.get_source(i);

            if source.borrow().is_blamed() {
                continue;
            }

            let random_value = rng.gen_range(0, sources_count - i);

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
        debug!("Synchronize with other validators");

        let mut rng = rand::thread_rng();
        let sources_count = self.get_sources_count();

        for _i in 0..MAX_SOURCES_SYNC_ATTEMPS {
            let source_index = rng.gen_range(0, sources_count);
            let source = self.get_source(source_index);

            if source.borrow().is_blamed() {
                continue;
            }

            self.synchronize_with(&*source.borrow());
        }
    }

    /*
        Sources synchronization
    */

    fn synchronize_with(&mut self, source: &dyn ReceiverSource) {
        trace!("...synchronize with source {}", source.get_id());

        assert!(!source.is_blamed());

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

        type TonVector = ::ton_api::ton::vector<::ton_api::ton::Bare, ::ton_api::ton::int>;

        let get_difference_request = ton::GetDifferenceRequest {
            rt: TonVector::from(sources_delivered_heights),
        };

        //send a difference query to a synchronization source

        let get_difference_response_handler =
            |result: Result<ton::GetDifferenceResponse>,
             payload: BlockPayload,
             receiver: &mut dyn Receiver| {
                use ton_api::ton::catchain::*;

                match result {
                    Err(err) => warn!("GetDifference query error: {:?}", err),
                    Ok(response) => match response {
                        Difference::Catchain_Difference(difference) => {
                            debug!("GetDifference response: {:?}", difference);
                        }
                        Difference::Catchain_DifferenceFork(_difference_fork) => {
                            get_mut_impl(receiver).got_fork_proof(&payload);
                        }
                    },
                }
            };

        self.send_get_difference_request(
            source.get_adnl_id(),
            get_difference_request,
            get_difference_response_handler,
        );

        //request for absent blocks

        if source.get_delivered_height() >= source.get_received_height() {
            return;
        }

        //get first undelivered block for the source and request its dependencies
        if let Some(first_block) = source.get_block(source.get_delivered_height() + 1) {
            let mut dep_hashes = Vec::new();

            const MAX_PENDING_DEPS_COUNT: usize = 16;

            first_block
                .borrow()
                .get_pending_deps(MAX_PENDING_DEPS_COUNT, &mut dep_hashes);

            for dep_hash in dep_hashes {
                //send getBlock request for each absent hash

                let get_block_request = ton::GetBlockRequest {
                    block: dep_hash.clone().into(),
                };
                let source_adnl_id = source.get_adnl_id().clone();

                let get_block_response_handler =
                    move |result: Result<ton::BlockResultResponse>,
                          payload: BlockPayload,
                          receiver: &mut dyn Receiver| {
                        use ton_api::ton::catchain::*;

                        match result {
                            Err(err) => warn!(
                                "GetBlock {:} query error: {:?}",
                                dep_hash.to_hex_string(),
                                err
                            ),
                            Ok(response) => match response {
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
                            },
                        }
                    };

                self.send_get_block_request(
                    source.get_adnl_id(),
                    get_block_request,
                    get_block_response_handler,
                );
            }
        }
    }

    fn process_query(
        &mut self,
        adnl_id: &PublicKeyHash,
        data: &BlockPayload,
    ) -> Result<BlockPayload> {
        debug!("Receiver: received query from {}: {:?}", adnl_id, data);

        match ton_api::Deserializer::new(&mut &data.0[..]).read_boxed::<ton_api::ton::TLObject>() {
            Ok(message) => {
                if message.is::<ton::GetDifferenceRequest>() {
                    return utils::serialize_query_boxed_response(
                        self.process_get_difference_query(
                            adnl_id,
                            &message.downcast::<ton::GetDifferenceRequest>().unwrap(),
                        ),
                    );
                } else if message.is::<ton::GetBlockRequest>() {
                    match self.process_get_block_query(
                        adnl_id,
                        &message.downcast::<ton::GetBlockRequest>().unwrap(),
                    ) {
                        Ok(response) => {
                            let mut ret: BlockPayload = BlockPayload::default();
                            let mut serializer = ton_api::Serializer::new(&mut ret.0);

                            serializer.write_boxed(&response.0).unwrap();
                            serializer.write_bare(&response.1).unwrap();

                            return Ok(ret);
                        }
                        Err(err) => return Err(err),
                    }
                } else if message.is::<ton::GetBlocksRequest>() {
                    return utils::serialize_query_bare_response(self.process_get_blocks_query(
                        adnl_id,
                        &message.downcast::<ton::GetBlocksRequest>().unwrap(),
                    ));
                } else if message.is::<ton::GetBlockHistoryRequest>() {
                    return utils::serialize_query_bare_response(
                        self.process_get_block_history_query(
                            adnl_id,
                            &message.downcast::<ton::GetBlockHistoryRequest>().unwrap(),
                        ),
                    );
                } else {
                    let err = format_err!("unknown query received {:?}", message);

                    error!("{}", err);

                    return Err(err);
                }
            }
            Err(err) => {
                return Err(err);
            }
        }
    }

    fn process_get_difference_query(
        &mut self,
        adnl_id: &PublicKeyHash,
        query: &ton::GetDifferenceRequest,
    ) -> Result<ton::GetDifferenceResponse> {
        debug!("Got GetDifferenceRequest: {:?}", query);

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

            if let Some(fork_proof) = source.get_fork_proof() {
                //return differenceFork as response

                let fork_proof_deserialized =
                    deserialize_tl_bare_object::<ton::DifferenceFork>(fork_proof).unwrap();
                let response = ton::GetDifferenceResponse::Catchain_DifferenceFork(Box::new(
                    fork_proof_deserialized,
                ));

                return Ok(response);
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

        let mut left: BlockHeight = 0;
        let mut right: BlockHeight = MAX_BLOCKS_TO_SEND + 1;

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

                //limit number of blocks for sending

                if sum > MAX_BLOCKS_TO_SEND as i64 {
                    right = middle;
                } else {
                    left = middle;
                }
            }
        }

        //send blocks to counterparty

        assert!(right > 0);

        let mut response_sources_delivered_heights: Vec<BlockHeight> =
            sources_delivered_heights.to_vec().clone();

        for i in 0..sources_count {
            let diff = ours_sources_delivered_heights[i] - sources_delivered_heights[i];

            if sources_delivered_heights[i] < 0 || diff <= 0 {
                continue;
            }

            let source_ptr = self.get_source(i);
            let source = source_ptr.borrow();
            let blocks_to_send = if diff > right { right } else { diff };

            assert!(blocks_to_send > 0);

            for _j in 0..blocks_to_send {
                response_sources_delivered_heights[i] += 1;

                let block_ptr = source
                    .get_block(response_sources_delivered_heights[i])
                    .unwrap();
                let block = block_ptr.borrow();
                let serialized_block = block.export_tl();
                let block_update_event = ton::BlockUpdateEvent {
                    block: serialized_block,
                };

                //send block update event to counterparty

                self.send_block_update_event(adnl_id, block_update_event, &block.get_payload());
            }
        }

        //send response to counterparty

        type TonVector = ::ton_api::ton::vector<::ton_api::ton::Bare, ::ton_api::ton::int>;

        let difference = ::ton_api::ton::catchain::difference::Difference {
            sent_upto: TonVector::from(response_sources_delivered_heights),
        };
        let response =
            ::ton_api::ton::catchain::Difference::Catchain_Difference(Box::new(difference));

        Ok(response)
    }

    fn process_get_block_query(
        &mut self,
        _adnl_id: &PublicKeyHash,
        query: &ton::GetBlockRequest,
    ) -> Result<(ton::BlockResultResponse, BlockPayload)> {
        debug!("Got GetBlockQuery: {:?}", query);

        let block_hash = query.block;
        let block_result = self.get_block_by_hash(&UInt256::from(block_hash.0));

        if let Some(block_ptr) = block_result {
            let block = block_ptr.borrow();

            if block.get_height() != 0 && block.is_initialized() {
                let response = ::ton_api::ton::catchain::blockresult::BlockResult {
                    block: block.export_tl(),
                };
                let response =
                    ::ton_api::ton::catchain::BlockResult::Catchain_BlockResult(Box::new(response));

                return Ok((response, block.get_payload().clone()));
            }
        }

        let response = ::ton_api::ton::catchain::BlockResult::Catchain_BlockNotFound {};

        return Ok((response, BlockPayload::default()));
    }

    fn process_get_blocks_query(
        &mut self,
        adnl_id: &PublicKeyHash,
        query: &ton::GetBlocksRequest,
    ) -> Result<CatchainSentResponse> {
        //limit number of blocks to be process by the request

        if query.blocks.len() > 100 {
            bail!("too many blocks");
        }

        //process blocks and send them back to the requester

        let mut response_blocks_count = 0;

        for block_hash in &query.blocks.0 {
            if let Some(block_ptr) = self.get_block_by_hash(&UInt256::from(block_hash.0)) {
                let block = block_ptr.borrow();

                assert!(block.get_payload().len() > 0);

                if block.get_height() <= 0 {
                    continue;
                }

                //send block to the requester

                let serialized_block = block.export_tl();
                let block_update_event = ton::BlockUpdateEvent {
                    block: serialized_block,
                };

                self.send_block_update_event(adnl_id, block_update_event, &block.get_payload());

                response_blocks_count += 1;
            }
        }

        //prepare response the the requester

        Ok(ton_api::ton::catchain::sent::Sent {
            cnt: response_blocks_count,
        })
    }

    fn process_get_block_history_query(
        &mut self,
        adnl_id: &PublicKeyHash,
        query: &ton::GetBlockHistoryRequest,
    ) -> Result<CatchainSentResponse> {
        //limit number of blocks to be processed by the request

        let mut height = match query.height {
            height if height <= 0 => bail!("not-positive height"),
            height if height > 100 => 100,
            height => height,
        };

        //process blocks and send them back to the requester

        let mut response_blocks_count = 0;

        if let Some(block) = self.get_block_by_hash(&UInt256::from(query.block.0)) {
            //limit height by the requested block height

            let block_height = block.borrow().get_height() as i64;

            if height < block_height {
                height = block_height;
            }

            //iterate from the block to the root of the chain

            let block_terminators = &query.stop_if;
            let mut block_it = block.clone();

            for _i in 0..height {
                let block = block_it.borrow();

                //terminate loop if the termination block has been found

                if block_terminators.contains(&block.get_hash().into()) {
                    break;
                }

                assert!(block.get_payload().len() > 0);

                //send block back to the requester

                let serialized_block = block.export_tl();
                let block_update_event = ton::BlockUpdateEvent {
                    block: serialized_block,
                };

                self.send_block_update_event(adnl_id, block_update_event, &block.get_payload());

                //move to the next block

                let prev = match block_it.borrow().get_prev() {
                    Some(prev) => prev.clone(),
                    None => bail!("Block.height is incorrect. There is no previous block"),
                };

                drop(block);

                block_it = prev;

                response_blocks_count += 1;
            }
        }

        Ok(ton_api::ton::catchain::sent::Sent {
            cnt: response_blocks_count,
        })
    }

    /*
        Forks management
    */

    fn got_fork_proof(&mut self, fork_proof: &BlockPayload) {
        let fork_difference_result =
            utils::deserialize_tl_bare_object::<ton::DifferenceFork>(fork_proof);

        if let Err(status) = fork_difference_result {
            warn!("Received bad fork proof {:?}: {:?}", fork_proof, status);
            return;
        }

        let fork_diff = fork_difference_result.unwrap();

        if let Err(status) = self.validate_block_dependency(&fork_diff.left) {
            warn!("Incorrect fork blame, left is invalid: {:?}", status);
            return;
        }

        if let Err(status) = self.validate_block_dependency(&fork_diff.right) {
            warn!("Incorrect fork blame, right is invalid: {:?}", status);
            return;
        }

        if fork_diff.left.height != fork_diff.right.height
            || fork_diff.left.src != fork_diff.right.src
            || fork_diff.left.data_hash != fork_diff.right.data_hash
        {
            warn!("Incorrect fork blame, not a fork");
            return;
        }

        let source = self.get_source(fork_diff.left.src as usize);
        let serialized_fork_proof = serialize_tl_bare_object!(&fork_diff.left, &fork_diff.right);

        source.borrow_mut().set_fork_proof(serialized_fork_proof);
        source.borrow_mut().mark_as_blamed(self);
    }

    /*
        Listener callbacks (Receiver -> Catchain)
    */

    fn notify_on_started(&mut self) {
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
        payload: &BlockPayload,
    ) {
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

    fn notify_on_broadcast(&mut self, source_key_hash: &PublicKeyHash, data: &BlockPayload) {
        if let Some(listener) = self.listener.upgrade() {
            listener
                .borrow_mut()
                .on_broadcast(self, source_key_hash, data);
        }
    }

    fn notify_on_blame(&mut self, source_id: usize) {
        if let Some(listener) = self.listener.upgrade() {
            listener.borrow_mut().on_blame(self, source_id);
        }
    }

    #[allow(dead_code)]
    fn notify_on_custom_message(
        &mut self,
        source_public_key_hash: &PublicKeyHash,
        data: &BlockPayload,
    ) {
        if let Some(listener) = self.listener.upgrade() {
            listener
                .borrow_mut()
                .on_custom_message(self, source_public_key_hash, data);
        }
    }

    fn notify_on_custom_query(
        &mut self,
        source_public_key_hash: &PublicKeyHash,
        data: &BlockPayload,
        response_promise: ExternalQueryResponseCallback,
    ) {
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
        receiver_id: &PublicKeyHash,
        request: ton::BlockUpdateEvent,
        payload: &BlockPayload,
    ) {
        if let Some(listener) = self.listener.upgrade() {
            debug!(
                "Send to {}: {:?} with payload {:?}",
                receiver_id, request, payload
            );

            let mut serialized_message = serialize_tl_bare_object!(&request);

            serialized_message.0.extend(payload.iter());

            listener.borrow_mut().send_message(
                receiver_id,
                &self.get_source(self.local_idx).borrow().get_adnl_id(),
                &serialized_message,
            );
        }
    }

    fn send_block_update_event_multicast(
        &mut self,
        receiver_ids: &[PublicKeyHash],
        request: ton::BlockUpdateEvent,
        payload: &BlockPayload,
    ) {
        if let Some(listener) = self.listener.upgrade() {
            debug!(
                "Send to {:?}: {:?} with payload {:?}",
                public_key_hashes_to_string(receiver_ids),
                request,
                payload
            );

            let mut serialized_message = serialize_tl_bare_object!(&request);

            serialized_message.0.extend(payload.iter());

            listener.borrow_mut().send_message_multicast(
                receiver_ids,
                &self.get_source(self.local_idx).borrow().get_adnl_id(),
                &serialized_message,
            );
        }
    }

    fn create_response_handler_boxed<T, F>(
        response_callback: F,
    ) -> Box<dyn FnOnce(Result<BlockPayload>, &mut dyn Receiver)>
    where
        T: 'static + ::ton_api::BoxedDeserialize,
        F: FnOnce(Result<T>, BlockPayload, &mut dyn Receiver) + 'static,
    {
        let boxed_response_callback = Box::new(response_callback);

        let handler = move |result: Result<BlockPayload>, receiver: &mut dyn Receiver| match result
        {
            Err(err) => boxed_response_callback(Err(err), BlockPayload::default(), receiver),
            Ok(payload) => boxed_response_callback(
                utils::deserialize_tl_boxed_object::<T>(&payload),
                payload,
                receiver,
            ),
        };

        Box::new(handler)
    }

    #[allow(dead_code)]
    fn create_response_handler<T, F>(
        response_callback: F,
    ) -> Box<dyn FnOnce(Result<BlockPayload>, &mut dyn Receiver)>
    where
        T: 'static + ::ton_api::BareDeserialize,
        F: FnOnce(Result<T>, BlockPayload, &mut dyn Receiver) + 'static,
    {
        let boxed_response_callback = Box::new(response_callback);

        let handler = move |result: Result<BlockPayload>, receiver: &mut dyn Receiver| match result
        {
            Err(err) => boxed_response_callback(Err(err), BlockPayload::default(), receiver),
            Ok(payload) => boxed_response_callback(
                utils::deserialize_tl_bare_object::<T>(&payload),
                payload,
                receiver,
            ),
        };

        Box::new(handler)
    }

    fn send_get_block_request<F>(
        &mut self,
        receiver_id: &PublicKeyHash,
        request: ton::GetBlockRequest,
        response_callback: F,
    ) where
        F: FnOnce(Result<ton::BlockResultResponse>, BlockPayload, &mut dyn Receiver) + 'static,
    {
        if let Some(listener) = self.listener.upgrade() {
            trace!("...query GetBlock {}: {:?}", receiver_id, request);

            let serialized_message = serialize_tl_bare_object!(&request);

            static GET_BLOCK_QUERY_TIMEOUT: Duration = Duration::from_millis(2000);

            listener.borrow_mut().send_query(
                receiver_id,
                &self.get_source(self.local_idx).borrow().get_adnl_id(),
                "sync blocks",
                GET_BLOCK_QUERY_TIMEOUT,
                &serialized_message,
                Self::create_response_handler_boxed(response_callback),
            );
        }
    }

    fn send_get_difference_request<F>(
        &mut self,
        receiver_id: &PublicKeyHash,
        request: ton::GetDifferenceRequest,
        response_callback: F,
    ) where
        F: FnOnce(Result<ton::GetDifferenceResponse>, BlockPayload, &mut dyn Receiver) + 'static,
    {
        if let Some(listener) = self.listener.upgrade() {
            trace!("...query GetDifference {}: {:?}", receiver_id, request);

            let serialized_message = serialize_tl_bare_object!(&request);

            static GET_DIFFERENCE_QUERY_TIMEOUT: Duration = Duration::from_millis(5000);

            listener.borrow_mut().send_query(
                receiver_id,
                &self.get_source(self.local_idx).borrow().get_adnl_id(),
                "sync",
                GET_DIFFERENCE_QUERY_TIMEOUT,
                &serialized_message,
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
        local_id: &PublicKeyHash,
        db_root: &String,
        db_suffix: &String,
        allow_unsafe_self_blocks_resync: bool,
    ) -> Self {
        let metrics_receiver = metrics_runtime::Receiver::builder()
            .build()
            .expect("failed to create metrics receiver");
        let received_blocks_instance_counter =
            InstanceCounter::new(&metrics_receiver, &"received_blocks".to_owned());

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

        let local_key_finder = || {
            for (i, id) in ids.iter().enumerate() {
                if get_public_key_hash(&id.public_key) == *local_id {
                    return (id.public_key.clone(), i);
                }
            }

            unreachable!(
                "LocalID {:?} has not been found in catchain nodes",
                local_id
            );
        };
        let (local_key, local_idx) = local_key_finder();

        let now = SystemTime::now();
        let mut obj = ReceiverImpl {
            sources: Vec::new(),
            source_public_key_hashes: Vec::new(),
            source_adnl_addrs: Vec::new(),
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
            local_key: local_key,
            local_idx: local_idx,
            total_forks: 0,
            neighbours: Vec::new(),
            listener: listener,
            metrics_receiver: metrics_receiver,
            received_blocks_instance_counter: received_blocks_instance_counter,
            pending_in_db: 0,
            db: None,
            read_db: false,
            db_root_block: ZERO_HASH.clone(),
            db_root: db_root.clone(),
            db_suffix: db_suffix.clone(),
            allow_unsafe_self_blocks_resync: allow_unsafe_self_blocks_resync,
            unsafe_root_block_writing: false,
            started: false,
            next_awake_time: now,
            next_sync_time: now,
            next_neighbours_rotate_time: now,
            initial_sync_complete_time: now
                + Duration::from_secs(INFINITE_INITIAL_SYNC_COMPLETE_TIME_SECS),
            rng: rand::thread_rng(),
        };

        for id in ids {
            let source_id = obj.sources.len();
            let source = CatchainFactory::create_receiver_source(
                source_id,
                id.public_key.clone(),
                &id.adnl_id,
            );

            obj.sources.push(source);
        }

        assert!(sources_count == 0 || obj.local_idx != sources_count);

        obj.source_public_key_hashes = obj
            .sources
            .iter()
            .map(|source| source.borrow().get_public_key_hash().clone())
            .collect();
        obj.source_adnl_addrs = obj
            .sources
            .iter()
            .map(|source| source.borrow().get_adnl_id().clone())
            .collect();

        obj.add_received_block(root_block.clone());

        obj.start_up_db();

        obj.choose_neighbours();

        obj
    }

    pub(crate) fn create_dummy_listener() -> Rc<RefCell<dyn ReceiverListener>> {
        Rc::new(RefCell::new(DummyListener::new()))
    }

    pub(crate) fn create_dummy() -> ReceiverPtr {
        let local_id = parse_hex_as_public_key_hash(
            "0000000000000000000000000000000000000000000000000000000000000000",
        );
        let ids: Vec<CatchainNode> = Vec::new();
        let incarnation = SessionId::default();
        let receiver_listener = ReceiverImpl::create_dummy_listener();
        let db_root = "".to_string();
        let db_suffix = "".to_string();
        let allow_unsafe_self_blocks_resync = false;

        ReceiverImpl::create(
            Rc::downgrade(&receiver_listener),
            &incarnation,
            &ids,
            &local_id,
            &db_root,
            &db_suffix,
            allow_unsafe_self_blocks_resync,
        )
    }

    pub(crate) fn create(
        listener: ReceiverListenerPtr,
        incarnation: &SessionId,
        ids: &Vec<CatchainNode>,
        local_id: &PublicKeyHash,
        db_root: &String,
        db_suffix: &String,
        allow_unsafe_self_blocks_resync: bool,
    ) -> ReceiverPtr {
        Rc::new(RefCell::new(ReceiverImpl::new(
            listener,
            incarnation,
            ids,
            local_id,
            db_root,
            db_suffix,
            allow_unsafe_self_blocks_resync,
        )))
    }

    fn create_block_with_payload(
        &mut self,
        block: &ton::Block,
        payload: BlockPayload,
    ) -> Result<ReceivedBlockPtr> {
        if block.height == 0 {
            return Ok(self.root_block.clone());
        }

        let block_id = self.get_block_id(block, &payload);
        let block_hash = get_block_id_hash(&block_id);

        if let Some(existing_block_entry) = self.blocks.get(&block_hash) {
            let existing_block = existing_block_entry.clone();

            if !existing_block.borrow().is_initialized() {
                if log_enabled!(log::Level::Debug) {
                    trace!(
                        "...create block with hash={:?} exists but has not been initialized",
                        block_hash
                    );
                }

                existing_block
                    .borrow_mut()
                    .initialize(&block, payload, self)?;
            }

            return Ok(existing_block.clone());
        } else {
            if log_enabled!(log::Level::Debug) {
                trace!("...create block with hash={:?}", block_hash);
            }

            let new_block =
                received_block::ReceivedBlockImpl::create_with_payload(&block, payload, self)?;

            self.add_received_block(new_block.clone());

            return Ok(new_block);
        }
    }
}
