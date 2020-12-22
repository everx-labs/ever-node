pub use super::*;

use crate::ton_api::IntoBoxed;
use crate::utils::*;
use crate::CatchainFactory;
use rand::Rng;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::Duration;

/*
    Constants
*/

const CATCHAIN_PROCESSING_PERIOD_MS: u64 = 1000; //idle time for catchain timed events in milliseconds
const CATCHAIN_METRICS_DUMP_PERIOD_MS: u64 = 5000; //time for catchain metrics dump
const CATCHAIN_INFINITE_SEND_PROCESS_TIMEOUT: Duration = Duration::from_secs(3600 * 24 * 3650); //large timeout as a infinite timeout simulation for send process
const MAIN_LOOP_NAME: &str = "CC"; //catchain main loop short thread name

/*
    Options
*/

impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Options")
            .field("idle_timeout", &self.idle_timeout)
            .field("max_deps", &self.max_deps)
            .field("debug_disable_db", &self.debug_disable_db)
            .finish()
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_millis(16000),
            max_deps: 4,
            debug_disable_db: false,
            skip_processed_blocks: false,
        }
    }
}

/*
    Catchain processor (for use in a separate thread)
*/

struct BlockDesc {
    preprocessed: bool, //has this block been preprocessing in a Catchain iteration
    processed: bool,    //has this block been processing in a Catchain iteration
}

type CatchainImplPtr = std::sync::Weak<Mutex<CatchainImpl>>;
type ReceiverListenerRcPtr = Rc<RefCell<dyn ReceiverListener>>;
type OverlayListenerRcPtr = Arc<Mutex<OverlayListenerImpl>>;
type BlockDescPtr = Rc<RefCell<BlockDesc>>;

struct CatchainProcessor {
    receiver_listener: ReceiverListenerRcPtr, //receiver listener ptr (retain)
    _overlay_listener: OverlayListenerRcPtr,  //overlay listener ptr (retain)
    receiver: ReceiverPtr,                    //catchain receiver
    catchain_listener: CatchainListenerPtr,   //listener for outgoing events
    options: Options,                         //catchain options
    receiver_started: bool,                   //flag which indicates that receiver is started
    next_block_generation_time: SystemTime,   //time to generate next block
    blocks: HashMap<BlockHash, BlockPtr>,     //all catchain blocks
    block_descs: HashMap<BlockHash, BlockDescPtr>, //all catchain blocks descriptions (internal processor structures)
    top_blocks: HashMap<BlockHash, BlockPtr>,      //map of top blocks by hash
    top_source_blocks: Vec<Option<BlockPtr>>,      //list of top blocks for each source
    sources: Vec<PublicKeyHash>,                   //list of validator public key hashes
    blamed_sources: Vec<bool>,                     //mask if a sources is blamed
    process_deps: Vec<BlockHash>, //list of block hashes which were used as dependencies for next consensus iteration
    session_id: SessionId,        //catchain session ID
    _ids: Vec<CatchainNode>,      //list of nodes
    local_id: PublicKeyHash,      //public key hash of current validator
    local_idx: usize,             //index of current validator in the list of sources
    _overlay_id: SessionId,       //overlay ID
    overlay: CatchainOverlayPtr,  //overlay
    force_process: bool, //flag which indicates catchain was requested (by validator session) to generate new block
    active_process: bool, //flag which indicates catchain is in process of generation of a new block
    rng: rand::rngs::ThreadRng, //random generator
    current_extra_id: BlockExtraId, //current block extra identifier
}

/*
    Implementation details for Receiver
*/

type Task = Box<dyn FnOnce(&mut CatchainProcessor) + Send>;

pub(crate) struct CatchainImpl {
    queue_sender: crossbeam::channel::Sender<Task>, //queue from outer world to the Catchain
    stop_flag: Arc<AtomicBool>, //atomic flag to indicate that Catchain thread should be stopped
    processing_thread: Option<JoinHandle<()>>, //catchain processing thread
}

/*
    Implementation of OverlayListener
*/

struct OverlayListenerImpl {
    catchain: CatchainImplPtr, //back weak reference to a CatchainImpl
}

impl CatchainOverlayLogReplayListener for OverlayListenerImpl {
    fn on_time_changed(&mut self, timestamp: std::time::SystemTime) {
        if let Some(catchain) = self.catchain.upgrade() {
            catchain
                .lock()
                .unwrap()
                .post_closure(move |processor: &mut CatchainProcessor| {
                    processor.on_time_changed(timestamp);
                });
        }
    }
}

impl CatchainOverlayListener for OverlayListenerImpl {
    fn on_message(&mut self, adnl_id: PublicKeyHash, data: &BlockPayload) {
        if let Some(catchain) = self.catchain.upgrade() {
            let adnl_id = adnl_id.clone();
            let data = data.clone();

            catchain
                .lock()
                .unwrap()
                .post_closure(move |processor: &mut CatchainProcessor| {
                    processor.on_message(adnl_id, data);
                });
        }
    }

    fn on_broadcast(&mut self, source_key_hash: PublicKeyHash, data: &BlockPayload) {
        if let Some(catchain) = self.catchain.upgrade() {
            let source_key_hash_clone = source_key_hash.clone();
            let data_clone = data.clone();

            catchain
                .lock()
                .unwrap()
                .post_closure(move |processor: &mut CatchainProcessor| {
                    processor.on_broadcast_from_overlay(source_key_hash_clone, data_clone);
                });
        }
    }

    fn on_query(
        &mut self,
        adnl_id: PublicKeyHash,
        data: &BlockPayload,
        response_callback: ExternalQueryResponseCallback,
    ) {
        if let Some(catchain) = self.catchain.upgrade() {
            let adnl_id = adnl_id.clone();
            let data = data.clone();

            catchain
                .lock()
                .unwrap()
                .post_closure(move |processor: &mut CatchainProcessor| {
                    processor.on_query(adnl_id, data, response_callback);
                });
        }
    }
}

impl OverlayListenerImpl {
    fn create(catchain: CatchainImplPtr) -> OverlayListenerRcPtr {
        Arc::new(Mutex::new(Self { catchain: catchain }))
    }
}

/*
    Implementation of ReceiverListener
*/

type CompletionHandlerId = u64;

struct SingleThreadedCompletionHandler<T> {
    handler: Option<Box<dyn FnOnce(Result<T>, &mut dyn Receiver)>>,
}

impl<T> SingleThreadedCompletionHandler<T> {
    fn new(handler: Box<dyn FnOnce(Result<T>, &mut dyn Receiver)>) -> Self {
        Self {
            handler: Some(handler),
        }
    }
}

struct ReceiverListenerImpl {
    catchain: CatchainImplPtr,   //back weak reference to a CatchainImpl
    overlay: CatchainOverlayPtr, //network layer for outgoing catchain events
    next_completion_handler_available_index: CompletionHandlerId, //index of next available complete handler
    completion_handlers: HashMap<CompletionHandlerId, Box<dyn Any>>, //complete handlers
}

impl ReceiverListener for ReceiverListenerImpl {
    /*
        Any casts
    */

    fn get_impl(&self) -> &dyn Any {
        self
    }
    fn get_mut_impl(&mut self) -> &mut dyn Any {
        self
    }

    /*
        Catchain started callback
    */

    fn on_started(&mut self) {
        if let Some(catchain) = self.catchain.upgrade() {
            catchain
                .lock()
                .unwrap()
                .post_closure(move |processor: &mut CatchainProcessor| {
                    processor.on_started();
                });
        }
    }

    /*
        Blocks management
    */

    fn on_new_block(
        &mut self,
        _receiver: &mut dyn Receiver,
        source_id: usize,
        fork_id: usize,
        hash: BlockHash,
        height: BlockHeight,
        prev: BlockHash,
        deps: Vec<BlockHash>,
        forks_dep_heights: Vec<BlockHeight>,
        payload: &BlockPayload,
    ) {
        if let Some(catchain) = self.catchain.upgrade() {
            let payload_clone = payload.clone();

            catchain
                .lock()
                .unwrap()
                .post_closure(move |processor: &mut CatchainProcessor| {
                    processor.on_new_block(
                        source_id,
                        fork_id,
                        hash,
                        height,
                        prev,
                        deps,
                        forks_dep_heights,
                        payload_clone,
                    );
                });
        }
    }

    fn on_broadcast(
        &mut self,
        _receiver: &mut dyn Receiver,
        source_key_hash: &PublicKeyHash,
        data: &BlockPayload,
    ) {
        if let Some(catchain) = self.catchain.upgrade() {
            let source_key_hash = source_key_hash.clone();
            let data = data.clone();

            catchain
                .lock()
                .unwrap()
                .post_closure(move |processor: &mut CatchainProcessor| {
                    processor.on_broadcast_from_receiver(source_key_hash, data);
                });
        }
    }

    /*
        Nodes blaming management
    */

    fn on_blame(&mut self, _receiver: &mut dyn Receiver, source_id: usize) {
        if let Some(catchain) = self.catchain.upgrade() {
            catchain
                .lock()
                .unwrap()
                .post_closure(move |processor: &mut CatchainProcessor| {
                    processor.on_blame(source_id);
                });
        }
    }

    /*
        Network messages transfering from Receiver to Validator Session
    */

    fn on_custom_message(
        &mut self,
        _receiver: &mut dyn Receiver,
        source_public_key_hash: &PublicKeyHash,
        data: &BlockPayload,
    ) {
        if let Some(catchain) = self.catchain.upgrade() {
            let source_public_key_hash_clone = source_public_key_hash.clone();
            let data_clone = data.clone();

            catchain
                .lock()
                .unwrap()
                .post_closure(move |processor: &mut CatchainProcessor| {
                    processor.on_custom_message(source_public_key_hash_clone, data_clone);
                });
        }
    }

    fn on_custom_query(
        &mut self,
        _receiver: &mut dyn Receiver,
        source_public_key_hash: &PublicKeyHash,
        data: &BlockPayload,
        response_callback: ExternalQueryResponseCallback,
    ) {
        if let Some(catchain) = self.catchain.upgrade() {
            let source_public_key_hash_clone = source_public_key_hash.clone();
            let data_clone = data.clone();

            catchain
                .lock()
                .unwrap()
                .post_closure(move |processor: &mut CatchainProcessor| {
                    processor.on_custom_query(
                        source_public_key_hash_clone,
                        data_clone,
                        response_callback,
                    );
                });
        }
    }

    /*
        Network messages transfering from Receiver to Overlay
    */

    fn send_message(
        &mut self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        message: &BlockPayload,
    ) {
        self.overlay
            .lock()
            .unwrap()
            .send_message(receiver_id, sender_id, message);
    }

    fn send_message_multicast(
        &mut self,
        receiver_ids: &[PublicKeyHash],
        sender_id: &PublicKeyHash,
        message: &BlockPayload,
    ) {
        self.overlay
            .lock()
            .unwrap()
            .send_message_multicast(receiver_ids, sender_id, message);
    }

    fn send_query(
        &mut self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        name: &str,
        timeout: std::time::Duration,
        message: &BlockPayload,
        response_callback: QueryResponseCallback,
    ) {
        let completion_handler = self.create_completion_handler(response_callback);
        self.overlay.lock().unwrap().send_query(
            receiver_id,
            sender_id,
            name,
            timeout,
            message,
            completion_handler,
        );
    }
}

impl ReceiverListenerImpl {
    /*
        Completion handlers management
    */

    fn store_completion_handler<T>(
        &mut self,
        response_callback: Box<dyn FnOnce(Result<T>, &mut dyn Receiver)>,
    ) -> CompletionHandlerId
    where
        T: 'static,
    {
        let handler_index = self.next_completion_handler_available_index;

        self.next_completion_handler_available_index += 1;

        const MAX_COMPLETION_HANDLER_INDEX: CompletionHandlerId = std::u64::MAX;

        assert!(self.next_completion_handler_available_index < MAX_COMPLETION_HANDLER_INDEX);

        let handler = Box::new(SingleThreadedCompletionHandler::new(response_callback));

        self.completion_handlers.insert(handler_index, handler);

        handler_index
    }

    fn invoke_completion_handler<T>(
        &mut self,
        handler_index: CompletionHandlerId,
        processor: &mut CatchainProcessor,
        result: Result<T>,
    ) where
        T: 'static,
    {
        if let Some(mut handler) = self.completion_handlers.remove(&handler_index) {
            if let Some(handler) = handler.downcast_mut::<SingleThreadedCompletionHandler<T>>() {
                let handler = handler.handler.take();

                (handler.unwrap())(result, &mut *processor.receiver.borrow_mut());
            }
        }
    }

    fn create_completion_handler<T>(
        &mut self,
        response_callback: Box<dyn FnOnce(Result<T>, &mut dyn Receiver)>,
    ) -> Box<dyn FnOnce(Result<T>) + Send>
    where
        T: 'static + Send,
    {
        let handler_index = self.store_completion_handler(response_callback);
        let catchain = self.catchain.clone();

        let handler = move |result: Result<T>| {
            if let Some(catchain) = catchain.upgrade() {
                catchain
                    .lock()
                    .unwrap()
                    .post_closure(move |processor: &mut CatchainProcessor| {
                        let listener = processor.receiver_listener.clone();

                        listener
                            .borrow_mut()
                            .get_mut_impl()
                            .downcast_mut::<ReceiverListenerImpl>()
                            .unwrap()
                            .invoke_completion_handler(handler_index, processor, result);
                    });
            }
        };

        Box::new(handler)
    }

    /*
        Listener creation
    */

    fn create(catchain: CatchainImplPtr, overlay: CatchainOverlayPtr) -> ReceiverListenerRcPtr {
        let body = ReceiverListenerImpl {
            catchain: catchain,
            overlay: overlay,
            next_completion_handler_available_index: 1,
            completion_handlers: HashMap::new(),
        };

        let receiver_listener: ReceiverListenerRcPtr = Rc::new(RefCell::new(body));

        receiver_listener
    }
}

/*
    Implementation of CatchainProcessor
*/

impl CatchainProcessor {
    /*
        Blocks management
    */

    fn get_block(&self, hash: BlockHash) -> Option<BlockPtr> {
        if self.blocks.contains_key(&hash) {
            Some(self.blocks[&hash].clone())
        } else {
            None
        }
    }

    fn get_block_desc(&self, block: &dyn Block) -> Option<BlockDescPtr> {
        let hash = &block.get_hash();

        if self.block_descs.contains_key(hash) {
            Some(self.block_descs[hash].clone())
        } else {
            None
        }
    }

    fn is_processed(&self, block: &dyn Block) -> bool {
        match self.get_block_desc(block) {
            Some(desc) => desc.borrow().processed,
            _ => false,
        }
    }

    /*
        Listener notifications
    */

    fn notify_preprocess_block(&mut self, block: BlockPtr) {
        if let Some(listener) = self.catchain_listener.upgrade() {
            listener.lock().unwrap().preprocess_block(block.clone());
        }
    }

    fn notify_process_blocks(&mut self, blocks: Vec<BlockPtr>) {
        if let Some(listener) = self.catchain_listener.upgrade() {
            listener.lock().unwrap().process_blocks(blocks);
        }
    }

    fn notify_finished_processing(&mut self) {
        if let Some(listener) = self.catchain_listener.upgrade() {
            listener.lock().unwrap().finished_processing();
        }
    }

    fn notify_started(&mut self) {
        if let Some(listener) = self.catchain_listener.upgrade() {
            listener.lock().unwrap().started();
        }
    }

    fn notify_custom_message(&mut self, source_public_key_hash: PublicKeyHash, data: BlockPayload) {
        if let Some(listener) = self.catchain_listener.upgrade() {
            listener
                .lock()
                .unwrap()
                .process_message(source_public_key_hash, data);
        }
    }

    fn notify_custom_query(
        &mut self,
        source_public_key_hash: PublicKeyHash,
        data: BlockPayload,
        response_callback: ExternalQueryResponseCallback,
    ) {
        if let Some(listener) = self.catchain_listener.upgrade() {
            listener
                .lock()
                .unwrap()
                .process_query(source_public_key_hash, data, response_callback);
        }
    }

    fn notify_broadcast(&mut self, source_public_key_hash: PublicKeyHash, data: BlockPayload) {
        if let Some(listener) = self.catchain_listener.upgrade() {
            listener
                .lock()
                .unwrap()
                .process_broadcast(source_public_key_hash, data);
        }
    }

    fn notify_set_time(&mut self, time: std::time::SystemTime) {
        if let Some(listener) = self.catchain_listener.upgrade() {
            listener.lock().unwrap().set_time(time);
        }
    }

    /*
        Main loop
    */

    pub(self) fn main_loop(
        catchain: CatchainImplPtr,
        queue_receiver: crossbeam::channel::Receiver<Task>,
        stop_flag: Arc<AtomicBool>,
        options: Options,
        session_id: SessionId,
        ids: Vec<CatchainNode>,
        local_id: PublicKeyHash,
        db_root: String,
        db_suffix: String,
        allow_unsafe_self_blocks_resync: bool,
        overlay_creator: OverlayCreator,
        listener: CatchainListenerPtr,
    ) {
        debug!("Catchain main loop is started");

        let processor_opt = CatchainProcessor::create(
            catchain,
            options,
            session_id,
            ids,
            local_id,
            db_root,
            db_suffix,
            allow_unsafe_self_blocks_resync,
            overlay_creator,
            listener,
        );

        let mut processor = match processor_opt {
            Ok(processor) => processor,
            Err(err) => {
                error!("CatchainProcessor::main_loop: error during creation of CatchainProcessor: {:?}", err);
                return;
            }
        };

        let mut last_awake = SystemTime::now();
        let mut next_metrics_dump_time = last_awake;

        loop {
            //check if the main loop should be stopped

            if stop_flag.load(Ordering::Relaxed) {
                break;
            }

            //handle catchain event with timeout

            processor.set_next_awake_time(
                last_awake + Duration::from_millis(CATCHAIN_PROCESSING_PERIOD_MS),
            );
            processor.set_next_awake_time(processor.next_block_generation_time);
            processor.set_next_awake_time(next_metrics_dump_time);

            let timeout = match processor
                .receiver
                .borrow()
                .get_next_awake_time()
                .duration_since(SystemTime::now())
            {
                Ok(timeout) => timeout,
                Err(_err) => Duration::default(),
            };

            let task = queue_receiver.recv_timeout(timeout);

            processor.receiver.borrow_mut().set_next_awake_time(
                SystemTime::now() + Duration::from_millis(CATCHAIN_PROCESSING_PERIOD_MS),
            );

            if let Ok(task) = task {
                let start = SystemTime::now();

                task(&mut processor);

                if let Ok(duration) = start.elapsed() {
                    const WARNING_DURATION: Duration = Duration::from_millis(100);

                    if duration > WARNING_DURATION {
                        warn!("Catchain task time execution warning: {:?}", duration);
                    }
                }
            }

            processor.receiver.borrow_mut().process();

            last_awake = SystemTime::now();

            //generate new block

            if processor.receiver_started {
                if let Ok(_elapsed) = processor.next_block_generation_time.elapsed() {
                    //initiate new block generation if there are no active top blocks

                    processor.set_next_block_generation_time(
                        last_awake + CATCHAIN_INFINITE_SEND_PROCESS_TIMEOUT,
                    );
                    processor.send_process_attempt();
                } else {
                    debug!(
                        "Waiting for {:?} for a new block generation time slot",
                        processor
                            .next_block_generation_time
                            .duration_since(SystemTime::now())
                    );
                }
            }

            //update receiver

            processor.receiver.borrow_mut().check_all();

            //dump metrics

            if let Ok(_elapsed) = next_metrics_dump_time.elapsed() {
                debug!("Catchain {} metrics:", processor.session_id.to_hex_string());

                processor.receiver.borrow().dump_metrics();

                next_metrics_dump_time =
                    last_awake + Duration::from_millis(CATCHAIN_METRICS_DUMP_PERIOD_MS);
            }
        }

        debug!("Catchain main loop is finished");
    }

    /*
        New block generation flow
    */

    fn send_preprocess(&mut self, block: BlockPtr, is_root: bool) {
        let block_desc_ptr = self.get_block_desc(&*block).unwrap();
        let mut block_desc = block_desc_ptr.borrow_mut();

        if block_desc.preprocessed {
            return;
        }

        if is_root {
            debug!("CatchainProcessor::send_preprocess for block {}", block);
        }

        //recursive preprocessing of block dependencies

        if let Some(prev) = block.get_prev() {
            self.send_preprocess(prev.clone(), false);
        }

        for dep in block.get_deps().iter() {
            self.send_preprocess(dep.clone(), false);
        }

        //update block flags

        block_desc.preprocessed = true;

        //notify listeners

        trace!(
            "...start preprocessing block {:?} from source {}",
            block.get_hash(),
            block.get_source_id()
        );

        self.notify_preprocess_block(block.clone());

        trace!(
            "...finish preprocessing block {:?} from source {}",
            block.get_hash(),
            block.get_source_id()
        );
    }

    fn remove_random_top_block(&mut self) -> BlockPtr {
        let random_value = self.rng.gen_range(0, self.top_blocks.len());
        let mut index = 0;
        let mut found_hash: Option<BlockHash> = None;

        for (hash, _) in self.top_blocks.iter() {
            if index >= random_value {
                found_hash = Some(hash.clone());
                break;
            }

            index += 1;
        }

        return self.top_blocks.remove(&found_hash.unwrap()).unwrap();
    }

    fn set_processed(&mut self, block: &dyn Block) {
        let block_desc_ptr = self.get_block_desc(block).unwrap();
        let mut block_desc = block_desc_ptr.borrow_mut();

        if block_desc.processed {
            return;
        }

        //recursive preprocessing of block dependencies

        if let Some(prev) = block.get_prev() {
            self.set_processed(&*prev.clone());
        }

        for dep in block.get_deps().iter() {
            self.set_processed(&*dep.clone());
        }

        //set block processing flag

        block_desc.processed = true;
    }

    fn send_process(&mut self) {
        assert!(self.receiver_started);

        debug!("Send blocks processing...");

        let mut blocks: Vec<BlockPtr> = Vec::new();
        let mut block_hashes: Vec<BlockHash> = Vec::new();

        trace!("...{} top blocks found", self.top_blocks.len());

        while self.top_blocks.len() > 0 && blocks.len() < self.options.max_deps as usize {
            let block = self.remove_random_top_block();
            let source_id = block.get_source_id();

            //todo: add comment about source_id == self.sources.len()

            if source_id == self.sources.len() || !self.blamed_sources[source_id] {
                trace!(
                    "...choose block {:?} from source #{} pubkeyhash={}",
                    block.get_hash(),
                    block.get_source_id(),
                    block.get_source_public_key_hash()
                );

                block_hashes.push(block.get_hash().clone());
                blocks.push(block.clone());

                self.set_processed(&*block);
            }
        }

        self.process_deps = block_hashes;

        debug!("...creating block for deps: {:?}", self.process_deps);

        self.notify_process_blocks(blocks);

        trace!("...finish creating block");
    }

    fn set_next_block_generation_time(&mut self, mut time: SystemTime) {
        let now = std::time::SystemTime::now();

        //ignore set new awake point if it is in the past

        if time < now {
            time = now;
        }

        //do not set next awake point if we will awake earlier in the future

        if self.next_block_generation_time > now && self.next_block_generation_time <= time {
            return;
        }

        self.next_block_generation_time = time;
    }

    fn set_next_awake_time(&mut self, time: SystemTime) {
        self.receiver.borrow_mut().set_next_awake_time(time);
    }

    fn send_process_attempt(&mut self) {
        if self.active_process {
            return;
        }

        self.active_process = true;

        self.send_process();
    }

    fn request_new_block(&mut self, time: SystemTime) {
        if !self.receiver_started {
            return;
        }

        if !self.force_process {
            debug!("Catchain forcing creation of a new block");
        }

        self.force_process = true;

        if !self.active_process {
            self.set_next_block_generation_time(time);
        }
    }

    fn processed_block(&mut self, payload: BlockPayload) {
        assert!(self.receiver_started);

        debug!(
            "Catchain created block: deps={:?}, payload size is {}",
            self.process_deps,
            payload.len()
        );

        if !self.options.skip_processed_blocks {
            self.receiver
                .borrow_mut()
                .add_block(payload, self.process_deps.drain(..).collect());
        }

        assert!(self.active_process);

        if self.top_blocks.len() > 0 || self.force_process {
            self.force_process = false;

            self.send_process();
        } else {
            self.active_process = false;

            trace!("...catchain finish processing");

            self.notify_finished_processing();

            self.set_next_block_generation_time(SystemTime::now() + self.options.idle_timeout);
        }
    }

    /*
        Events processing Overlay -> Catchain
    */

    fn on_message(&mut self, adnl_id: PublicKeyHash, data: BlockPayload) {
        match self
            .receiver
            .borrow_mut()
            .receive_message_from_overlay(&adnl_id, &mut data.as_ref())
        {
            Ok(_) => (),
            Err(err) => warn!("CatchainImpl::on_message: {}", err),
        }
    }

    fn on_broadcast_from_overlay(&mut self, source_key_hash: PublicKeyHash, data: BlockPayload) {
        self.receiver
            .borrow_mut()
            .receive_broadcast_from_overlay(&source_key_hash, &data);
    }

    fn on_broadcast_from_receiver(&mut self, source_key_hash: PublicKeyHash, data: BlockPayload) {
        if log_enabled!(log::Level::Debug) {
            debug!(
                "Receive broadcast from overlay for source {}: {:?}",
                source_key_hash, data
            );
        }

        self.notify_broadcast(source_key_hash, data);
    }

    fn on_query(
        &mut self,
        adnl_id: PublicKeyHash,
        data: BlockPayload,
        response_callback: ExternalQueryResponseCallback,
    ) {
        self.receiver
            .borrow_mut()
            .receive_query_from_overlay(&adnl_id, &data, response_callback);
    }

    fn on_time_changed(&mut self, time: std::time::SystemTime) {
        self.notify_set_time(time);
    }

    /*
        Start up callback from Receiver
    */

    fn on_started(&mut self) {
        debug!("Catchain has been successfully started");

        //notify about start of catchain

        self.notify_started();

        self.receiver_started = true;

        //initiate blocks processing

        assert!(!self.active_process);

        self.send_process_attempt();
    }

    /*
        Blocks processing
    */

    fn generate_extra_id(&mut self) -> BlockExtraId {
        const MAX_EXTRA_ID: BlockExtraId = std::u64::MAX;

        assert!(self.current_extra_id < MAX_EXTRA_ID);

        let result = self.current_extra_id;

        self.current_extra_id += 1;

        result
    }

    pub fn on_new_block(
        &mut self,
        source_id: usize,
        fork_id: usize,
        hash: BlockHash,
        height: BlockHeight,
        prev: BlockHash,
        deps: Vec<BlockHash>,
        forks_dep_heights: Vec<BlockHeight>,
        payload: BlockPayload,
    ) {
        debug!(
            "New catchain block {} (source_id={}, fork={}, height={})",
            hash.to_hex_string(),
            source_id,
            fork_id,
            height
        );

        if self.top_blocks.len() == 0 && !self.active_process && self.receiver_started {
            self.set_next_block_generation_time(SystemTime::now() + self.options.idle_timeout);
        }

        //obtain prev block and remove it block from the top blocks because new block is on top now

        let hash_zero = UInt256::from([0; 32]);
        let mut prev_block = None;

        if prev != hash_zero
        //TODO: check if we really need this comparison
        {
            prev_block = self.get_block(prev.clone());

            if self.top_blocks.contains_key(&prev) {
                self.top_blocks.remove(&prev);
            }
        }

        //initialize new block dependencies and update top blocks (if dependency is on top)

        let mut block_deps: Vec<BlockPtr> = Vec::new();

        block_deps.reserve(deps.len());

        for dep in &deps {
            if !self.blamed_sources[source_id] && self.top_blocks.contains_key(dep) {
                self.top_blocks.remove(dep);
            }

            let block_dep = self.get_block(dep.clone());

            assert!(!block_dep.is_none());

            block_deps.push(block_dep.unwrap());
        }

        //create and register a new block

        assert!(source_id < self.sources.len());

        let source_public_key_hash = &self.sources[source_id];
        let block = CatchainFactory::create_block(
            source_id,
            fork_id,
            source_public_key_hash.clone(),
            height,
            hash.clone(),
            payload,
            prev_block,
            block_deps,
            forks_dep_heights,
            self.generate_extra_id(),
        );

        self.blocks.insert(hash.clone(), block.clone());
        self.block_descs.insert(
            hash.clone(),
            Rc::new(RefCell::new(BlockDesc {
                processed: false,
                preprocessed: false,
            })),
        );

        //update top of the blocks and initiate blocks processing if needed

        if !self.blamed_sources[source_id] {
            self.send_preprocess(block.clone(), true);

            self.top_source_blocks[source_id] = Some(block.clone());

            if source_id != self.local_idx {
                self.top_blocks.insert(hash.clone(), block.clone());

                trace!(
                    "...block {:?} has been added to top blocks (top_blocks_count={})",
                    &hash,
                    self.top_blocks.len()
                );
            }

            if self.top_blocks.len() == 0 && !self.active_process && self.receiver_started {
                self.set_next_block_generation_time(SystemTime::now() + self.options.idle_timeout);
            }
        }
    }

    /*
        Nodes blaming management
    */

    pub fn on_blame(&mut self, source_id: usize) {
        //do not blame same validator again

        if self.blamed_sources[source_id] {
            return;
        }

        self.blamed_sources[source_id] = true;

        //remove top block for blamed validator and recompute top blocks

        self.top_source_blocks[source_id] = None;

        self.top_blocks.clear();

        let sources_count = self.sources.len();

        for i in 0..sources_count {
            if self.blamed_sources[i] || self.top_source_blocks[i].is_none() || i == self.local_idx
            {
                continue;
            }

            if let Some(ref parent_block) = self.top_source_blocks[i] {
                let mut need_to_add_block = true;

                if self.is_processed(&*parent_block.clone()) {
                    continue;
                }

                for j in 0..sources_count {
                    if i == j || self.blamed_sources[j] || self.top_source_blocks[j].is_none() {
                        continue;
                    }

                    if let Some(ref block) = self.top_source_blocks[j] {
                        if block.is_descendant_of(&*parent_block.clone()) {
                            need_to_add_block = false;
                            break;
                        }
                    }
                }

                if need_to_add_block {
                    let parent_hash = parent_block.get_hash().clone();

                    self.top_blocks.insert(parent_hash, parent_block.clone());
                }
            }
        }
    }

    /*
        Network messages transfering from Receiver to Validator Session
    */

    pub fn on_custom_message(&mut self, source_public_key_hash: PublicKeyHash, data: BlockPayload) {
        if log_enabled!(log::Level::Debug) {
            debug!(
                "CatchainProcessor.on_custom_message: public_key_hash={} payload={:?}",
                source_public_key_hash, data
            );
        }

        self.notify_custom_message(source_public_key_hash, data);
    }

    pub fn on_custom_query(
        &mut self,
        source_public_key_hash: PublicKeyHash,
        data: BlockPayload,
        response_callback: ExternalQueryResponseCallback,
    ) {
        if log_enabled!(log::Level::Debug) {
            debug!(
                "CatchainProcessor.on_custom_query: public_key_hash={} payload={:?}",
                source_public_key_hash, data
            );
        }

        self.notify_custom_query(source_public_key_hash, data, response_callback);
    }

    /*
        Network messages transfering from Validator Session to Overlay
    */

    fn send_broadcast(&mut self, payload: BlockPayload) {
        self.overlay.lock().unwrap().send_broadcast_fec_ex(
            self.receiver
                .borrow()
                .get_source(self.local_idx)
                .borrow()
                .get_adnl_id(),
            &self.local_id,
            payload,
        );
    }

    /*
        Creation
    */

    pub fn create(
        catchain: CatchainImplPtr,
        options: Options,
        session_id: SessionId,
        ids: Vec<CatchainNode>,
        local_id: PublicKeyHash,
        db_root: String,
        db_suffix: String,
        allow_unsafe_self_blocks_resync: bool,
        overlay_creator: OverlayCreator,
        listener: CatchainListenerPtr,
    ) -> Result<CatchainProcessor> {
        //sources preparation

        let mut sources: Vec<PublicKeyHash> = Vec::new();
        let mut local_idx = ids.len();

        sources.reserve(ids.len());

        for i in 0..ids.len() {
            sources.push(utils::get_public_key_hash(&ids[i].public_key));

            if sources[i] == local_id {
                local_idx = i;
            }
        }

        assert!(local_idx < ids.len());

        //overlay creation

        let sources_as_int256: Vec<::ton_api::ton::int256> = sources
            .clone()
            .into_iter()
            .map(|key| public_key_hash_to_int256(&key))
            .collect();
        let first_block = ::ton_api::ton::catchain::firstblock::Firstblock {
            unique_hash: session_id.clone().into(),
            nodes: sources_as_int256.into(),
        }
        .into_boxed();
        let overlay_id = utils::get_overlay_id(&first_block)?;
        let overlay_listener = OverlayListenerImpl::create(catchain.clone());
        let overlay_data_listener: Arc<Mutex<dyn CatchainOverlayListener + Send>> =
            overlay_listener.clone();
        let overlay_replay_listener: Arc<Mutex<dyn CatchainOverlayLogReplayListener + Send>> =
            overlay_listener.clone();
        let overlay = overlay_creator(
            &sources[local_idx],
            &overlay_id,
            &ids,
            Arc::downgrade(&overlay_data_listener),
            Arc::downgrade(&overlay_replay_listener),
        );

        debug!(
            "CatchainProcessor: starting up overlay with ID {:?}",
            overlay_id
        );

        //receiver creation

        let receiver_listener = ReceiverListenerImpl::create(catchain.clone(), overlay.clone());
        let receiver = CatchainFactory::create_receiver(
            Rc::downgrade(&receiver_listener),
            &overlay_id,
            &ids,
            &local_id,
            &db_root,
            &db_suffix,
            allow_unsafe_self_blocks_resync,
        );

        //catchain processor creation

        let body = Self {
            session_id: session_id,
            next_block_generation_time: SystemTime::now(),
            options: options,
            _overlay_listener: overlay_listener,
            receiver_listener: receiver_listener,
            receiver: receiver,
            catchain_listener: listener,
            blocks: HashMap::new(),
            block_descs: HashMap::new(),
            top_blocks: HashMap::new(),
            top_source_blocks: vec![None; ids.len()],
            sources: sources,
            blamed_sources: vec![false; ids.len()],
            process_deps: Vec::new(),
            _ids: ids,
            local_id: local_id,
            local_idx: local_idx,
            _overlay_id: overlay_id,
            overlay: overlay,
            force_process: false,
            active_process: false,
            rng: rand::thread_rng(),
            current_extra_id: 0,
            receiver_started: false,
        };

        Ok(body)
    }
}

/*
    Implementation of dummy overlay (for testing)
*/

struct DummyCatchainOverlay {}

impl CatchainOverlay for DummyCatchainOverlay {
    fn send_message(
        &mut self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        message: &BlockPayload,
    ) {
        info!(
            "DummyCatchainOverlay: send message {:?} -> {:?}: {:?}",
            sender_id, receiver_id, message
        );
    }

    fn send_message_multicast(
        &mut self,
        receiver_ids: &[PublicKeyHash],
        sender_id: &PublicKeyHash,
        message: &BlockPayload,
    ) {
        info!(
            "DummyCatchainOverlay: send message multicast {:?} -> {:?}: {:?}",
            sender_id, receiver_ids, message
        );
    }

    fn send_query(
        &mut self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        name: &str,
        _timeout: std::time::Duration,
        message: &BlockPayload,
        _response_callback: ExternalQueryResponseCallback,
    ) {
        info!(
            "DummyCatchainOverlay: send query {} {:?} -> {:?}: {:?}",
            name, sender_id, receiver_id, message
        );
    }

    fn send_broadcast_fec_ex(
        &mut self,
        sender_id: &PublicKeyHash,
        send_as: &PublicKeyHash,
        payload: BlockPayload,
    ) {
        info!(
            "DummyCatchainOverlay: send broadcast_fec_ex {:?}/{:?}: {:?}",
            sender_id, send_as, payload
        );
    }
}

/*
    Implementation of public Catchain trait
*/

impl Catchain for CatchainImpl {
    /*
        Catchain stop
    */

    fn stop(&mut self) {
        if self.stop_flag.load(Ordering::Relaxed) {
            return;
        }

        trace!("...waiting for Catchain threads");

        self.stop_flag.store(true, Ordering::Release);

        if let Some(handle) = self.processing_thread.take() {
            handle
                .join()
                .expect("Failed to join Catchain main loop thread");
        }
    }

    /*
        Catchain blocks processing
    */

    fn request_new_block(&mut self, time: SystemTime) {
        self.post_closure(move |processor: &mut CatchainProcessor| {
            processor.request_new_block(time);
        });
    }

    fn processed_block(&mut self, payload: BlockPayload) {
        self.post_closure(move |processor: &mut CatchainProcessor| {
            processor.processed_block(payload);
        });
    }

    /*
        Network access interface
    */

    fn send_broadcast(&mut self, payload: BlockPayload) {
        self.post_closure(move |processor: &mut CatchainProcessor| {
            processor.send_broadcast(payload);
        });
    }
}

/*
    Private CatchainImpl details
*/

impl Drop for CatchainImpl {
    fn drop(&mut self) {
        debug!("Dropping Catchain...");

        self.stop();
    }
}

impl CatchainImpl {
    /*
        Catchain messages processing
    */

    fn post_closure<F>(&mut self, task_fn: F)
    where
        F: FnOnce(&mut CatchainProcessor),
        F: Send + 'static,
    {
        if let Err(send_error) = self.queue_sender.send(Box::new(task_fn)) {
            error!("Catchain method call error: {}", send_error);
        }
    }

    /*
        Catchain creation
    */

    pub fn create_dummy_overlay(
        _local_id: &PublicKeyHash,
        _overlay_full_id: &OverlayFullId,
        _nodes: &Vec<CatchainNode>,
        _listener: CatchainOverlayListenerPtr,
    ) -> CatchainOverlayPtr {
        Arc::new(Mutex::new(DummyCatchainOverlay {}))
    }

    pub fn create(
        options: &Options,
        session_id: &SessionId,
        ids: &Vec<CatchainNode>,
        local_id: &PublicKeyHash,
        db_root: &String,
        db_suffix: &String,
        allow_unsafe_self_blocks_resync: bool,
        overlay_creator: OverlayCreator,
        listener: CatchainListenerPtr,
    ) -> CatchainPtr {
        debug!("Creating Catchain...");

        let (queue_sender, queue_receiver): (
            crossbeam::channel::Sender<Task>,
            crossbeam::channel::Receiver<Task>,
        ) = crossbeam::crossbeam_channel::unbounded();
        let stop_flag = Arc::new(AtomicBool::new(false));
        let body: CatchainImpl = CatchainImpl {
            queue_sender: queue_sender,
            stop_flag: stop_flag.clone(),
            processing_thread: None,
        };

        let catchain = Arc::new(Mutex::new(body));
        let catchain_weak = Arc::downgrade(&catchain);
        let ids = ids.clone();
        let local_id = local_id.clone();
        let session_id = session_id.clone();
        let options = *options;
        let db_root = db_root.clone();
        let db_suffix = db_suffix.clone();

        catchain.lock().unwrap().processing_thread = Some(
            std::thread::Builder::new()
                .name(MAIN_LOOP_NAME.to_string())
                .spawn(move || {
                    CatchainProcessor::main_loop(
                        catchain_weak,
                        queue_receiver,
                        stop_flag.clone(),
                        options,
                        session_id,
                        ids,
                        local_id,
                        db_root,
                        db_suffix,
                        allow_unsafe_self_blocks_resync,
                        overlay_creator,
                        listener,
                    );
                })
                .unwrap(),
        );

        catchain
    }
}
