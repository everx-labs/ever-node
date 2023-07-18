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

use crate::{
    check_execution_time, instrument, ActivityNodePtr, Block, BlockExtraId, BlockHash, 
    BlockHeight, BlockPayloadPtr, BlockPtr, Catchain, CatchainFactory, CatchainListenerPtr,
    CatchainNode, CatchainOverlay, CatchainOverlayListener, CatchainOverlayListenerPtr, 
    CatchainOverlayManager, CatchainOverlayLogReplayListener, 
    CatchainOverlayLogReplayListenerPtr, CatchainOverlayManagerPtr, CatchainOverlayPtr, 
    CatchainPtr, ExternalQueryResponseCallback, Options, PrivateKey, PublicKeyHash, 
    QueryResponseCallback, Receiver, ReceiverListener, ReceiverPtr, ReceiverTaskQueue, 
    ReceiverTaskQueuePtr, SessionId, ton, 
    profiling::Profiler, utils::{self, MetricsDumper, get_elapsed_time}
};
use overlay::{OverlayUtils, PrivateOverlayShortId};                    
use rand::Rng;                                                              
use std::{
    any::Any, cell::RefCell, collections::HashMap, fmt, rc::Rc, 
    sync::{Arc, atomic::{AtomicBool, Ordering}}, 
    time::{Duration, SystemTime, UNIX_EPOCH}
};
use ton_api::IntoBoxed;
use ton_types::{error, Result, UInt256};

/*
    Constants
*/

const CATCHAIN_POSTPONED_SEND_TO_OVERLAY: bool = true; //send all queries to overlay using utility thread
const CATCHAIN_PROCESSING_PERIOD_MS: u64 = 1000; //idle time for catchain timed events in milliseconds
const CATCHAIN_METRICS_DUMP_PERIOD_MS: u64 = 5000; //time for catchain metrics dump
const CATCHAIN_PROFILING_DUMP_PERIOD_MS: u64 = 30000; //time for catchain profiling dump
const CATCHAIN_INFINITE_SEND_PROCESS_TIMEOUT: Duration = Duration::from_secs(3600 * 24 * 3650); //large timeout as a infinite timeout simulation for send process
const CATCHAIN_WARN_PROCESSING_LATENCY: Duration = Duration::from_millis(1000); //max processing latency
const CATCHAIN_LATENCY_WARN_DUMP_PERIOD: Duration = Duration::from_millis(2000); //latency warning dump period
const COMPLETION_HANDLERS_CHECK_PERIOD: Duration = Duration::from_millis(5000); //period of completion handlers checking
const COMPLETION_HANDLERS_MAX_WAIT_PERIOD: Duration = Duration::from_millis(60000); //max delay for completion handler execution
const BLOCKS_PROCESSING_STACK_CAPACITY: usize = 1000; //number of blocks in stack for CatchainProcessor::set_processed method
const MAIN_LOOP_NAME: &str = "CC"; //catchain main loop short thread name
const UTILITY_LOOP_NAME: &str = "CCU"; //catchain utility loop short thread name
const CATCHAIN_MAIN_LOOP_THREAD_STACK_SIZE: usize = 1024 * 1024 * 32; //stack size for catchain main loop thread

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

#[derive(Debug)]
struct BlockDesc {
    preprocessed: bool, //has this block been preprocessing in a Catchain iteration
    processed: bool,    //has this block been processing in a Catchain iteration
}

type CatchainImplPtr = std::sync::Arc<CatchainImpl>;
type CatchainImplWeakPtr = std::sync::Weak<CatchainImpl>;
type ReceiverListenerRcPtr = Rc<RefCell<dyn ReceiverListener>>;
type OverlayListenerRcPtr = Arc<OverlayListenerImpl>;
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
    processing_blocks_stack: Vec<(BlockPtr, BlockDescPtr)>, //array of block desc which are being processed
    processing_blocks_stack_tmp: Vec<(BlockPtr, BlockDescPtr)>, //temporary array of block desc which are being processed
    session_id: SessionId,                                      //catchain session ID
    _ids: Vec<CatchainNode>,                                    //list of nodes
    local_id: PublicKeyHash, //public key hash of current validator
    local_idx: usize,        //index of current validator in the list of sources
    _overlay_id: SessionId,  //overlay ID
    overlay_short_id: Arc<PrivateOverlayShortId>, //overlay short ID
    overlay_manager: CatchainOverlayManagerPtr, //overlay manager
    overlay: CatchainOverlayPtr, //overlay
    force_process: bool, //flag which indicates catchain was requested (by validator session) to generate new block
    active_process: bool, //flag which indicates catchain is in process of generation of a new block
    rng: rand::rngs::ThreadRng, //random generator
    current_extra_id: BlockExtraId, //current block extra identifier
    current_time: Option<std::time::SystemTime>, //current time for log replaying
    utility_queue_sender: crossbeam::channel::Sender<Box<TaskDesc<dyn FnOnce() + Send>>>, //queue from outer world to the Catchain utility thread
    process_blocks_requests_counter: metrics_runtime::data::Counter, //counter for send_process
    process_blocks_responses_counter: metrics_runtime::data::Counter, //counter for processed_block
    process_blocks_skip_responses_counter: metrics_runtime::data::Counter, //counter for processed_block with may_be_skipped=true
    process_blocks_batching_requests_counter: metrics_runtime::data::Counter, //counter for processed_block with batching request
}

/*
    Implementation details for Receiver
*/

struct TaskDesc<F: ?Sized> {
    task: Box<F>,                         //closure for execution
    creation_time: std::time::SystemTime, //time of task creation
}

pub(crate) struct CatchainImpl {
    main_queue_sender:
        crossbeam::channel::Sender<Box<TaskDesc<dyn FnOnce(&mut CatchainProcessor) + Send>>>, //queue from outer world to the Catchain main thread
    utility_queue_sender: crossbeam::channel::Sender<Box<TaskDesc<dyn FnOnce() + Send>>>, //queue from outer world to the Catchain utility thread
    should_stop_flag: Arc<AtomicBool>, //atomic flag to indicate that Catchain thread should be stopped
    main_thread_is_stopped_flag: Arc<AtomicBool>, //atomic flag to indicate that Catchain thread has been stopped
    utility_thread_is_stopped_flag: Arc<AtomicBool>, //atomic flag to indicate that utility processing thread has been stopped
    main_thread_overloaded_flag: Arc<AtomicBool>, //indicates that catchain main thrad is overloaded
    _utility_thread_overloaded_flag: Arc<AtomicBool>, //indicates that catchain utility thread is overloaded
    destroy_db_flag: Arc<AtomicBool>,                 //indicates catchain has to destroy DB
    session_id: SessionId,                            //session ID
    _activity_node: ActivityNodePtr, //activity node for tracing lifetime of this catchain
    main_thread_post_counter: metrics_runtime::data::Counter, //counter for main queue posts
    main_thread_pull_counter: metrics_runtime::data::Counter, //counter for main queue pull
    utility_thread_post_counter: metrics_runtime::data::Counter, //counter for utility queue posts
    _utility_thread_pull_counter: metrics_runtime::data::Counter, //counter for utility queue pull
}

/*
    Implementation of OverlayListener
*/

struct OverlayListenerImpl {
    catchain: CatchainImplWeakPtr, //back weak reference to a CatchainImpl
}

impl CatchainOverlayLogReplayListener for OverlayListenerImpl {
    fn on_time_changed(&self, timestamp: std::time::SystemTime) {
        if let Some(catchain) = self.catchain.upgrade() {
            catchain.post_closure(move |processor: &mut CatchainProcessor| {
                processor.on_time_changed(timestamp);
            });
        }
    }
}

impl CatchainOverlayListener for OverlayListenerImpl {
    fn on_message(&self, adnl_id: PublicKeyHash, data: &BlockPayloadPtr) {
        if let Some(catchain) = self.catchain.upgrade() {
            let adnl_id = adnl_id.clone();
            let data = data.clone();

            catchain.post_closure(move |processor: &mut CatchainProcessor| {
                processor.on_message(adnl_id, data);
            });
        }
    }

    fn on_broadcast(&self, source_key_hash: PublicKeyHash, data: &BlockPayloadPtr) {
        if let Some(catchain) = self.catchain.upgrade() {
            let source_key_hash_clone = source_key_hash.clone();
            let data_clone = data.clone();

            catchain.post_closure(move |processor: &mut CatchainProcessor| {
                processor.on_broadcast_from_overlay(source_key_hash_clone, data_clone);
            });
        }
    }

    fn on_query(
        &self,
        adnl_id: PublicKeyHash,
        data: &BlockPayloadPtr,
        response_callback: ExternalQueryResponseCallback,
    ) {
        if let Some(catchain) = self.catchain.upgrade() {
            if !Self::catchain_main_thread_overloaded_flag(&catchain) {
                let adnl_id = adnl_id.clone();
                let data = data.clone();

                catchain.post_closure(move |processor: &mut CatchainProcessor| {
                    processor.on_query(adnl_id, data, response_callback);
                });

                return;
            }

            let warning = error!(
                "Catchain {:x} is overloaded. Skip query from ADNL ID {}",
                catchain.session_id, adnl_id
            );

            log::warn!("{}", warning);

            let response = match ton_api::Deserializer::new(&mut &data.data().0[..])
                .read_boxed::<ton_api::ton::TLObject>()
            {
                Ok(message) => {
                    if message.is::<ton::GetDifferenceRequest>() {
                        let message = &message.downcast::<ton::GetDifferenceRequest>().unwrap();

                        utils::serialize_query_boxed_response(Ok(
                            ::ton_api::ton::catchain::difference::Difference {
                                sent_upto: message.rt.clone().into(),
                            }
                            .into_boxed(),
                        ))
                    } else if message.is::<ton::GetBlockRequest>() {
                        utils::serialize_query_boxed_response(Ok(
                            ::ton_api::ton::catchain::BlockResult::Catchain_BlockNotFound {},
                        ))
                    } else if message.is::<ton::GetBlocksRequest>() {
                        utils::serialize_query_boxed_response(Ok(
                            ::ton_api::ton::catchain::sent::Sent { cnt: 0 }.into_boxed(),
                        ))
                    } else {
                        Err(warning)
                    }
                }
                Err(err) => Err(err),
            };

            response_callback(response);
        }
    }
}

impl OverlayListenerImpl {
    fn create(catchain: CatchainImplWeakPtr) -> OverlayListenerRcPtr {
        Arc::new(Self { catchain })
    }

    fn catchain_main_thread_overloaded_flag(catchain: &CatchainImplPtr) -> bool {
        catchain.main_thread_overloaded_flag.load(Ordering::SeqCst)
    }
}

/*
    Implementation of ReceiverTaskQueue
*/

struct ReceiverTaskQueueImpl {
    catchain: CatchainImplWeakPtr, //back weak reference to a CatchainImpl
}

impl ReceiverTaskQueue for ReceiverTaskQueueImpl {
    /*
        Closures posting
    */

    fn post_utility_closure(&self, handler: Box<dyn FnOnce() + Send>) {
        if let Some(catchain) = self.catchain.upgrade() {
            catchain.post_utility_closure(handler);
        }
    }

    fn post_closure(&self, handler: Box<dyn FnOnce(&mut dyn Receiver) + Send>) {
        if let Some(catchain) = self.catchain.upgrade() {
            catchain.post_closure(move |processor: &mut CatchainProcessor| {
                handler(&mut *processor.receiver.borrow_mut());
            });
        }
    }
}

impl ReceiverTaskQueueImpl {
    fn create(catchain: CatchainImplWeakPtr) -> ReceiverTaskQueuePtr {
        Arc::new(Self { catchain: catchain })
    }
}

/*
    Implementation of ReceiverListener
*/

type CompletionHandlerId = u64;

trait CompletionHandler: std::any::Any {
    ///Time of handler creation
    fn get_creation_time(&self) -> std::time::SystemTime;

    ///Execute with error
    fn reset_with_error(&mut self, error: failure::Error, receiver: &mut dyn Receiver);
}

struct SingleThreadedCompletionHandler<T> {
    handler: Option<Box<dyn FnOnce(Result<T>, &mut dyn Receiver)>>,
    creation_time: std::time::SystemTime, //time of handler creation
}

impl<T> SingleThreadedCompletionHandler<T> {
    fn new(handler: Box<dyn FnOnce(Result<T>, &mut dyn Receiver)>) -> Self {
        Self {
            handler: Some(handler),
            creation_time: SystemTime::now(),
        }
    }
}

impl<T> CompletionHandler for SingleThreadedCompletionHandler<T>
where
    T: 'static,
{
    ///Time of handler creation
    fn get_creation_time(&self) -> std::time::SystemTime {
        self.creation_time
    }

    ///Execute handler with error
    fn reset_with_error(&mut self, error: failure::Error, receiver: &mut dyn Receiver) {
        if let Some(handler) = self.handler.take() {
            handler(Err(error), receiver);
        }
    }
}

struct ReceiverListenerImpl {
    catchain: CatchainImplWeakPtr, //back weak reference to a CatchainImpl
    task_queue: ReceiverTaskQueuePtr, //task queue for receiver
    overlay: CatchainOverlayPtr,   //network layer for outgoing catchain events
    next_completion_handler_available_index: CompletionHandlerId, //index of next available complete handler
    completion_handlers: HashMap<CompletionHandlerId, Box<dyn CompletionHandler>>, //complete handlers
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
            catchain.post_closure(move |processor: &mut CatchainProcessor| {
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
        payload: &BlockPayloadPtr,
    ) {
        //TODO: call processor directly instead of posting closure
        if let Some(catchain) = self.catchain.upgrade() {
            let payload_clone = payload.clone();

            catchain.post_closure(move |processor: &mut CatchainProcessor| {
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
        data: &BlockPayloadPtr,
    ) {
        //TODO: call processor directly instead of posting closure
        if let Some(catchain) = self.catchain.upgrade() {
            let source_key_hash = source_key_hash.clone();
            let data = data.clone();

            catchain.post_closure(move |processor: &mut CatchainProcessor| {
                processor.on_broadcast_from_receiver(source_key_hash, data);
            });
        }
    }

    /*
        Nodes blaming management
    */

    fn on_blame(&mut self, _receiver: &mut dyn Receiver, source_id: usize) {
        //TODO: call processor directly instead of posting closure
        if let Some(catchain) = self.catchain.upgrade() {
            catchain.post_closure(move |processor: &mut CatchainProcessor| {
                processor.on_blame(source_id);
            });
        }
    }

    /*
        Network messages transfering from Receiver to Validator Session
    */

    fn on_custom_query(
        &mut self,
        _receiver: &mut dyn Receiver,
        source_public_key_hash: &PublicKeyHash,
        data: &BlockPayloadPtr,
        response_callback: ExternalQueryResponseCallback,
    ) {
        //TODO: call processor directly instead of posting closure
        if let Some(catchain) = self.catchain.upgrade() {
            let source_public_key_hash_clone = source_public_key_hash.clone();
            let data_clone = data.clone();

            catchain.post_closure(move |processor: &mut CatchainProcessor| {
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
        message: &BlockPayloadPtr,
        can_be_postponed: bool,
    ) {
        check_execution_time!(20000);
        instrument!();

        //TODO: call processor directly instead of posting closure
        if can_be_postponed || CATCHAIN_POSTPONED_SEND_TO_OVERLAY {
            let overlay = Arc::downgrade(&self.overlay);
            let receiver_id = receiver_id.clone();
            let sender_id = sender_id.clone();
            let message = message.clone();

            if let Some(catchain) = self.catchain.upgrade() {
                catchain.post_utility_closure(move || {
                    check_execution_time!(20000);

                    if let Some(overlay) = overlay.upgrade() {
                        overlay.send_message(&receiver_id, &sender_id, &message);
                    }
                });
            }
        } else {
            self.overlay.send_message(receiver_id, sender_id, message);
        }
    }

    fn send_message_multicast(
        &mut self,
        receiver_ids: &[PublicKeyHash],
        sender_id: &PublicKeyHash,
        message: &BlockPayloadPtr,
    ) {
        check_execution_time!(20000);
        instrument!();

        //TODO: call processor directly instead of posting closure
        if CATCHAIN_POSTPONED_SEND_TO_OVERLAY {
            let overlay = Arc::downgrade(&self.overlay);
            let receiver_ids: Vec<PublicKeyHash> = receiver_ids.clone().into();
            let sender_id = sender_id.clone();
            let message = message.clone();

            if let Some(catchain) = self.catchain.upgrade() {
                catchain.post_utility_closure(move || {
                    check_execution_time!(20000);

                    if let Some(overlay) = overlay.upgrade() {
                        overlay.send_message_multicast(&receiver_ids, &sender_id, &message);
                    }
                });
            }
        } else {
            self.overlay
                .send_message_multicast(receiver_ids, sender_id, message);
        }
    }

    fn send_query(
        &mut self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        name: &str,
        timeout: std::time::Duration,
        message: &BlockPayloadPtr,
        response_callback: QueryResponseCallback,
    ) {
        check_execution_time!(20000);
        instrument!();

        let completion_handler = self.create_completion_handler(response_callback);

        //TODO: call processor directly instead of posting closure
        if CATCHAIN_POSTPONED_SEND_TO_OVERLAY {
            let overlay = Arc::downgrade(&self.overlay);
            let receiver_id = receiver_id.clone();
            let name: String = name.clone().into();
            let sender_id = sender_id.clone();
            let message = message.clone();

            if let Some(catchain) = self.catchain.upgrade() {
                catchain.post_utility_closure(move || {
                    check_execution_time!(20000);

                    if let Some(overlay) = overlay.upgrade() {
                        overlay.send_query(
                            &receiver_id,
                            &sender_id,
                            &name,
                            timeout,
                            &message,
                            completion_handler,
                        );
                    }
                });
            }
        } else {
            self.overlay.send_query(
                receiver_id,
                sender_id,
                name,
                timeout,
                message,
                completion_handler,
            );
        }
    }

    /*
        Task queue
    */

    fn get_task_queue(&self) -> &ReceiverTaskQueuePtr {
        &self.task_queue
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
                catchain.post_closure(move |processor: &mut CatchainProcessor| {
                    let listener = processor.receiver_listener.clone();
                    let handler = listener
                        .borrow_mut()
                        .get_mut_impl()
                        .downcast_mut::<ReceiverListenerImpl>()
                        .unwrap()
                        .completion_handlers
                        .remove(&handler_index);

                    if let Some(mut handler) = handler {
                        let handler_any: &mut dyn std::any::Any = &mut handler;

                        if let Some(handler) =
                            handler_any.downcast_mut::<SingleThreadedCompletionHandler<T>>()
                        {
                            if let Some(handler) = handler.handler.take() {
                                handler(result, &mut *processor.receiver.borrow_mut());
                            }
                        }
                    }
                });
            }
        };

        Box::new(handler)
    }

    fn check_completion_handlers(&mut self, receiver: &mut dyn Receiver) {
        let mut expired_handlers = Vec::new();
        let completion_handlers = &mut self.completion_handlers;

        for (handler_id, handler) in completion_handlers.iter() {
            if let Ok(latency) = handler.get_creation_time().elapsed() {
                if latency > COMPLETION_HANDLERS_MAX_WAIT_PERIOD {
                    expired_handlers.push((handler_id.clone(), latency));
                }
            }
        }

        for (handler_id, latency) in expired_handlers.iter_mut() {
            let handler = completion_handlers.remove(&handler_id);

            if let Some(mut handler) = handler {
                let warning = error!(
                    "Remove Catchain completion handler #{} with latency {:.3}s \
                    (expected max latency is {:.3}s): created at {}", 
                    handler_id, latency.as_secs_f64(), 
                    COMPLETION_HANDLERS_MAX_WAIT_PERIOD.as_secs_f64(), 
                    utils::time_to_string(&handler.get_creation_time())
                );

                log::warn!("{}", warning);
                handler.reset_with_error(warning, receiver);
            }
        }
    }

    /*
        Listener creation
    */

    fn create(catchain: CatchainImplWeakPtr, overlay: CatchainOverlayPtr) -> ReceiverListenerRcPtr {
        let body = ReceiverListenerImpl {
            task_queue: ReceiverTaskQueueImpl::create(catchain.clone()),
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
        Stopping
    */

    fn stop(&mut self) {
        log::debug!("Stopping CatchainProcessor...");

        self.overlay_manager
            .stop_overlay(&self.overlay_short_id, &self.overlay);

        log::debug!("CatchainProcessor has been stopped");
    }

    fn destroy_db(&mut self) {
        log::debug!("Destroying Catchain DB...");

        self.receiver.borrow_mut().destroy_db();
    }

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
        check_execution_time!(10000);

        if let Some(listener) = self.catchain_listener.upgrade() {
            listener.preprocess_block(block.clone());
        }
    }

    fn notify_process_blocks(&mut self, blocks: Vec<BlockPtr>) {
        check_execution_time!(10000);

        if let Some(listener) = self.catchain_listener.upgrade() {
            self.process_blocks_requests_counter.increment();

            listener.process_blocks(blocks);
        }
    }

    fn notify_finished_processing(&mut self) {
        check_execution_time!(10000);

        if let Some(listener) = self.catchain_listener.upgrade() {
            listener.finished_processing();
        }
    }

    fn notify_started(&mut self) {
        check_execution_time!(10000);

        if let Some(listener) = self.catchain_listener.upgrade() {
            listener.started();
        }
    }

    fn notify_custom_query(
        &mut self,
        source_public_key_hash: PublicKeyHash,
        data: BlockPayloadPtr,
        response_callback: ExternalQueryResponseCallback,
    ) {
        if let Some(listener) = self.catchain_listener.upgrade() {
            listener.process_query(source_public_key_hash, data, response_callback);
        }
    }

    fn notify_broadcast(&mut self, source_public_key_hash: PublicKeyHash, data: BlockPayloadPtr) {
        if let Some(listener) = self.catchain_listener.upgrade() {
            listener.process_broadcast(source_public_key_hash, data);
        }
    }

    fn notify_set_time(&mut self, time: std::time::SystemTime) {
        if let Some(listener) = self.catchain_listener.upgrade() {
            listener.set_time(time);
        }
    }

    /*
        Main loop
    */

    pub(self) fn main_loop(
        catchain: CatchainImplPtr,
        queue_receiver: crossbeam::channel::Receiver<
            Box<TaskDesc<dyn FnOnce(&mut CatchainProcessor) + Send>>,
        >,
        should_stop_flag: Arc<AtomicBool>,
        is_stopped_flag: Arc<AtomicBool>,
        overloaded_flag: Arc<AtomicBool>,
        destroy_db_flag: Arc<AtomicBool>,
        options: Options,
        session_id: SessionId,
        ids: Vec<CatchainNode>,
        local_key: PrivateKey,
        path: String,
        db_suffix: String,
        allow_unsafe_self_blocks_resync: bool,
        utility_queue_sender: crossbeam::channel::Sender<Box<TaskDesc<dyn FnOnce() + Send>>>,
        overlay_manager: CatchainOverlayManagerPtr,
        listener: CatchainListenerPtr,
        catchain_activity_node: ActivityNodePtr,
        metrics_receiver: Arc<metrics_runtime::Receiver>,
    ) {
        log::info!(
            "Catchain main loop is started (session_id is {:x})",
            session_id
        );

        //configure metrics

        let loop_counter = metrics_receiver
            .sink()
            .counter("catchain_main_loop_iterations");
        let loop_overloads_counter = metrics_receiver
            .sink()
            .counter("catchain_main_loop_overloads");
        let main_thread_pull_counter = catchain.main_thread_pull_counter.clone();

        //create catchain processor

        let processor_opt = CatchainProcessor::create(
            catchain,
            options,
            session_id.clone(),
            ids,
            local_key,
            path,
            db_suffix,
            allow_unsafe_self_blocks_resync,
            utility_queue_sender,
            overlay_manager,
            listener,
            metrics_receiver,
        );

        let mut processor = match processor_opt {
            Ok(processor) => processor,
            Err(err) => {
                log::error!(
                    "CatchainProcessor::main_loop: error during creation of CatchainProcessor: {:?}", 
                    err
                );
                overloaded_flag.store(false, Ordering::SeqCst);
                is_stopped_flag.store(true, Ordering::SeqCst);
                return;
            }
        };

        //configure metrics dumper

        let mut metrics_dumper = MetricsDumper::new();

        metrics_dumper.add_compute_handler(
            "received_blocks".to_string(),
            &utils::compute_instance_counter,
        );
        metrics_dumper.add_derivative_metric("received_blocks".to_string());
        metrics_dumper.add_derivative_metric("receiver_out_messages".to_string());
        metrics_dumper.add_derivative_metric("receiver_in_messages".to_string());
        metrics_dumper.add_derivative_metric("receiver_out_queries.total".to_string());
        metrics_dumper.add_derivative_metric("receiver_in_queries.total".to_string());
        metrics_dumper.add_derivative_metric("receiver_in_broadcasts".to_string());
        metrics_dumper.add_derivative_metric("db_get_txs".to_string());
        metrics_dumper.add_derivative_metric("db_put_txs".to_string());
        metrics_dumper.add_derivative_metric("catchain_main_loop_iterations".to_string());
        metrics_dumper.add_derivative_metric("catchain_main_loop_overloads".to_string());
        metrics_dumper.add_derivative_metric("catchain_utility_loop_iterations".to_string());
        metrics_dumper.add_derivative_metric("catchain_utility_loop_overloads".to_string());

        metrics_dumper.add_derivative_metric("process_blocks_requests".to_string());
        metrics_dumper.add_derivative_metric("process_blocks_responses".to_string());
        metrics_dumper.add_derivative_metric("process_blocks_skip_responses".to_string());
        metrics_dumper.add_derivative_metric("process_blocks_batching_requests".to_string());

        utils::add_compute_percentage_metric(
            &mut metrics_dumper,
            &"process_blocks_skipping".to_string(),
            &"process_blocks_skip_responses".to_string(),
            &"process_blocks_responses".to_string(),
            0.0,
        );
        utils::add_compute_percentage_metric(
            &mut metrics_dumper,
            &"process_blocks_batching".to_string(),
            &"process_blocks_batching_requests".to_string(),
            &"process_blocks_responses".to_string(),
            0.0,
        );

        utils::add_compute_percentage_metric(
            &mut metrics_dumper,
            &"catchain_main_loop_load".to_string(),
            &"catchain_main_loop_overloads".to_string(),
            &"catchain_main_loop_iterations".to_string(),
            0.0,
        );
        utils::add_compute_percentage_metric(
            &mut metrics_dumper,
            &"catchain_utility_loop_load".to_string(),
            &"catchain_utility_loop_overloads".to_string(),
            &"catchain_utility_loop_iterations".to_string(),
            0.0,
        );
        utils::add_compute_result_metric(&mut metrics_dumper, &"receiver_out_queries".to_string());
        utils::add_compute_result_metric(&mut metrics_dumper, &"receiver_in_queries".to_string());
        utils::add_compute_percentage_metric(
            &mut metrics_dumper,
            &"received_blocks_in_duplication".to_string(),
            &"receiver_in_messages".to_string(),
            &"received_blocks.create".to_string(),
            -1.0,
        );
        utils::add_compute_percentage_metric(
            &mut metrics_dumper,
            &"received_blocks_out_duplication".to_string(),
            &"receiver_out_messages".to_string(),
            &"received_blocks.create".to_string(),
            -1.0,
        );

        metrics_dumper.add_derivative_metric("main_queue.posts".to_string());
        metrics_dumper.add_derivative_metric("main_queue.pulls".to_string());
        metrics_dumper.add_derivative_metric("utility_queue.posts".to_string());
        metrics_dumper.add_derivative_metric("utility_queue.pulls".to_string());
        metrics_dumper
            .add_compute_handler("main_queue".to_string(), &utils::compute_queue_size_counter);
        metrics_dumper.add_compute_handler(
            "utility_queue".to_string(),
            &utils::compute_queue_size_counter,
        );

        //start main loop

        let mut last_awake = SystemTime::now();
        let mut next_metrics_dump_time = last_awake;
        let mut next_profiling_dump_time = last_awake;
        let mut last_latency_warn_dump_time = last_awake;
        let mut last_completion_handlers_check_time = last_awake;
        let mut is_overloaded = false;

        loop {
            {
                instrument!();

                catchain_activity_node.tick();
                loop_counter.increment();

                //check if the main loop should be stopped

                if should_stop_flag.load(Ordering::SeqCst) {
                    processor.stop();

                    if destroy_db_flag.load(Ordering::SeqCst) {
                        processor.destroy_db();
                    }
                    break;
                }

                //check overload flag

                overloaded_flag.store(is_overloaded, Ordering::SeqCst);

                if is_overloaded {
                    loop_overloads_counter.increment();
                }

                //handle catchain event with timeout

                processor.set_next_awake_time(
                    last_awake + Duration::from_millis(CATCHAIN_PROCESSING_PERIOD_MS),
                );
                processor.set_next_awake_time(processor.next_block_generation_time);
                processor.set_next_awake_time(next_metrics_dump_time);
                processor.set_next_awake_time(next_profiling_dump_time);

                let timeout = match processor
                    .receiver
                    .borrow()
                    .get_next_awake_time()
                    .duration_since(SystemTime::now())
                {
                    Ok(timeout) => timeout,
                    Err(_err) => Duration::default(),
                };

                let task_desc = {
                    instrument!();

                    queue_receiver.recv_timeout(timeout)
                };

                processor.receiver.borrow_mut().set_next_awake_time(
                    SystemTime::now() + Duration::from_millis(CATCHAIN_PROCESSING_PERIOD_MS),
                );

                is_overloaded = false;

                if let Ok(task_desc) = task_desc {
                    main_thread_pull_counter.increment();

                    let processing_latency = get_elapsed_time(&task_desc.creation_time);
                    if processing_latency > CATCHAIN_WARN_PROCESSING_LATENCY {
                        is_overloaded = true;

                        if get_elapsed_time(&last_latency_warn_dump_time)
                            > CATCHAIN_LATENCY_WARN_DUMP_PERIOD
                        {
                            log::warn!(
                                "Catchain processing latency is {:.3}s \
                                (expected max latency is {:.3}s)", 
                                processing_latency.as_secs_f64(), 
                                CATCHAIN_WARN_PROCESSING_LATENCY.as_secs_f64()
                            );
                            last_latency_warn_dump_time = SystemTime::now();
                        }
                    }

                    instrument!();
                    check_execution_time!(100000);

                    let task = task_desc.task;

                    task(&mut processor);
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
                        if let Ok(delay) = processor
                            .next_block_generation_time
                            .duration_since(SystemTime::now())
                        {
                            log::trace!(
                                "Waiting for {:.3}s for a new block generation time slot",
                                delay.as_secs_f64()
                            );
                        }
                    }
                }

                //check completion handlers

                if let Ok(completion_handlers_check_elapsed) =
                    last_completion_handlers_check_time.elapsed()
                {
                    if completion_handlers_check_elapsed > COMPLETION_HANDLERS_CHECK_PERIOD {
                        processor
                            .receiver_listener
                            .borrow_mut()
                            .get_mut_impl()
                            .downcast_mut::<ReceiverListenerImpl>()
                            .unwrap()
                            .check_completion_handlers(&mut *processor.receiver.borrow_mut());
                        last_completion_handlers_check_time = SystemTime::now();
                    }
                }

                //update receiver

                processor.receiver.borrow_mut().check_all();
            }

            //dump metrics

            if let Ok(_elapsed) = next_metrics_dump_time.elapsed() {
                instrument!();
                check_execution_time!(50_000);

                if log::log_enabled!(log::Level::Debug) {
                    let receiver = processor.receiver.borrow();

                    metrics_dumper.update(receiver.get_metrics_receiver());

                    let session_id_str = processor.session_id.to_hex_string();

                    log::debug!("Catchain {:x} metrics:", processor.session_id);

                    metrics_dumper.dump(
                        |string| log::debug!("{}{}", session_id_str, string)
                    );

                    let sources_count = receiver.get_sources_count();

                    log::debug!(
                        "Catchain {} debug dump (local_idx={}, sources_count={}):",
                        session_id_str, processor.local_idx, sources_count
                    );

                    for i in 0..sources_count {
                        let source = receiver.get_source(i);
                        let source = source.borrow();
                        let stat = source.get_statistics();

                        log::debug!(
                            "{} {}v{:03}/{:03}: {} delivered={:4}{}, received={:4}{}, forks={}, \
                            queries={:4}/{:4}, msgs={:4}/{:4}, in_bcasts={:4}, adnl_id={}, \
                            pubkey_hash={}",
                            session_id_str, 
                            if processor.local_idx == i { ">" } else { " " }, 
                            i, sources_count, 
                            if source.is_blamed() { "blamed" } else { "" }, 
                            source.get_delivered_height(), 
                            if source.has_undelivered() { "+" } else { " " },
                            source.get_received_height(), 
                            if source.has_unreceived() { "+" } else { " " }, 
                            source.get_forks_count(), 
                            stat.in_queries_count, stat.out_queries_count,
                            stat.in_messages_count, stat.out_messages_count, 
                            stat.in_broadcasts_count, 
                            source.get_adnl_id(), source.get_public_key_hash()
                        );
                    }
                }

                next_metrics_dump_time =
                    last_awake + Duration::from_millis(CATCHAIN_METRICS_DUMP_PERIOD_MS);
            }

            //dump profiling

            if let Ok(_elapsed) = next_profiling_dump_time.elapsed() {
                instrument!();
                check_execution_time!(50_000);

                if log::log_enabled!(log::Level::Debug) {
                    let profiling_dump = Profiler::local_instance()
                        .with(|profiler| profiler.borrow().dump());

                    log::debug!(
                        "Catchain {:x} profiling: {}",
                        processor.session_id, profiling_dump
                    );
                }

                next_profiling_dump_time =
                    last_awake + Duration::from_millis(CATCHAIN_PROFILING_DUMP_PERIOD_MS);
            }
        }

        log::info!("Catchain main loop is finished (session_id is {:x})", session_id);

        overloaded_flag.store(false, Ordering::SeqCst);
        is_stopped_flag.store(true, Ordering::SeqCst);
    }

    /*
        Utility loop
    */

    fn utility_loop(
        should_stop_flag: Arc<AtomicBool>,
        is_stopped_flag: Arc<AtomicBool>,
        overloaded_flag: Arc<AtomicBool>,
        queue_receiver: crossbeam::channel::Receiver<Box<TaskDesc<dyn FnOnce() + Send>>>,
        session_id: SessionId,
        metrics_receiver: Arc<metrics_runtime::Receiver>,
        utility_thread_pull_counter: metrics_runtime::data::Counter,
    ) {
        log::info!(
            "Catchain utility loop is started (session_id is {:x})",
            session_id
        );

        let name = format!("CatchainUtility_{:x}", session_id);
        let activity_node = CatchainFactory::create_activity_node(name);

        //configure metrics

        let loop_counter = metrics_receiver
            .sink()
            .counter("catchain_utility_loop_iterations");
        let loop_overloads_counter = metrics_receiver
            .sink()
            .counter("catchain_utility_loop_overloads");

        //session callbacks processing loop

        let mut last_latency_warn_dump_time = std::time::SystemTime::now();
        let mut is_overloaded = false;

        loop {
            activity_node.tick();
            loop_counter.increment();

            //check if the loop should be stopped

            if should_stop_flag.load(Ordering::SeqCst) {
                break;
            }

            //check overload flag

            overloaded_flag.store(is_overloaded, Ordering::SeqCst);

            if is_overloaded {
                loop_overloads_counter.increment();
            }

            //handle session outgoing event with timeout

            const MAX_TIMEOUT: Duration = Duration::from_secs(1);

            let task_desc = {
                instrument!();

                queue_receiver.recv_timeout(MAX_TIMEOUT)
            };

            is_overloaded = false;

            if let Ok(task_desc) = task_desc {
                utility_thread_pull_counter.increment();

                let processing_latency = get_elapsed_time(&task_desc.creation_time);
                if processing_latency > CATCHAIN_WARN_PROCESSING_LATENCY {
                    is_overloaded = true;

                    if get_elapsed_time(&last_latency_warn_dump_time)
                        > CATCHAIN_LATENCY_WARN_DUMP_PERIOD
                    {
                        log::warn!(
                            "Catchain utility processing latency is {:.3}s \
                            (expected max latency is {:.3}s)", 
                            processing_latency.as_secs_f64(), 
                            CATCHAIN_WARN_PROCESSING_LATENCY.as_secs_f64()
                        );
                        last_latency_warn_dump_time = SystemTime::now();
                    }
                }

                instrument!();
                check_execution_time!(100000);

                let task = task_desc.task;

                task();
            }
        }

        //finishing routines

        log::info!(
            "Catchain utility loop is finished (session_id is {:x})",
            session_id
        );

        overloaded_flag.store(false, Ordering::SeqCst);
        is_stopped_flag.store(true, Ordering::SeqCst);
    }

    /*
        New block generation flow
    */

    fn recursive_blocks_update<Pred, Update>(
        &mut self,
        mut block: BlockPtr,
        pred: Pred,
        update: Update,
    ) where
        Pred: Fn(&BlockDescPtr) -> bool,
        Update: Fn(&mut CatchainProcessor, &BlockDescPtr, BlockPtr),
    {
        let mut block_desc = self.get_block_desc(&*block).unwrap();

        loop {
            if !pred(&block_desc) {
                //recursive processing of block dependencies

                self.processing_blocks_stack_tmp.clear();

                let mut has_unprocessed_deps = false;

                for block in block.get_deps().iter().rev() {
                    let block_desc = self.get_block_desc(&**block).unwrap();

                    if pred(&block_desc) {
                        continue;
                    }

                    self.processing_blocks_stack_tmp
                        .push((block.clone(), block_desc.clone()));

                    has_unprocessed_deps = true;
                }

                if let Some(block) = block.get_prev() {
                    let block_desc = self.get_block_desc(&*block).unwrap();

                    if !pred(&block_desc) {
                        self.processing_blocks_stack_tmp
                            .push((block.clone(), block_desc.clone()));

                        has_unprocessed_deps = true;
                    }
                }

                if has_unprocessed_deps {
                    self.processing_blocks_stack
                        .push((block.clone(), block_desc.clone()));
                    self.processing_blocks_stack
                        .append(&mut self.processing_blocks_stack_tmp);
                } else {
                    update(self, &block_desc, block);
                }
            }

            //trying to move to the next block

            let next = self.processing_blocks_stack.pop();

            if next.is_none() {
                //if all blocks have been processed, stop the loop

                break;
            }

            let (next_block, next_block_desc) = next.unwrap();

            //if we have next block in stack for processing - move to it

            block = next_block;
            block_desc = next_block_desc;
        }
    }

    fn send_preprocess(&mut self, block: BlockPtr, is_root: bool) {
        instrument!();

        if log::log_enabled!(log::Level::Trace)
            && is_root
            && !self.get_block_desc(&*block).unwrap().borrow().preprocessed
        {
            log::trace!("CatchainProcessor::send_preprocess for block {}", block);
        }

        self.recursive_blocks_update(
            block,
            |block_desc| block_desc.borrow().preprocessed,
            |processor, block_desc, block| {
                block_desc.borrow_mut().preprocessed = true;

                //notify listeners

                log::trace!(
                    "...start preprocessing block {:?} from source {}",
                    block.get_hash(),
                    block.get_source_id()
                );

                processor.notify_preprocess_block(block.clone());

                log::trace!(
                    "...finish preprocessing block {:?} from source {}",
                    block.get_hash(),
                    block.get_source_id()
                );
            },
        );
    }

    fn remove_random_top_block(&mut self) -> BlockPtr {
        instrument!();

        let random_value = self.rng.gen_range(0..self.top_blocks.len());
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

    fn set_processed(&mut self, block: BlockPtr) {
        instrument!();

        self.recursive_blocks_update(
            block,
            |block_desc| block_desc.borrow().processed,
            |_processor, block_desc, _block| block_desc.borrow_mut().processed = true,
        );
    }

    fn send_process(&mut self) {
        instrument!();

        assert!(self.receiver_started);

        log::trace!("Send blocks processing...");

        let mut blocks: Vec<BlockPtr> = Vec::new();
        let mut block_hashes: Vec<BlockHash> = Vec::new();

        log::trace!("...{} top blocks found", self.top_blocks.len());

        while self.top_blocks.len() > 0 && blocks.len() < self.options.max_deps as usize {
            let block = self.remove_random_top_block();
            let source_id = block.get_source_id();

            //todo: add comment about source_id == self.sources.len()

            if source_id as usize == self.sources.len() || !self.blamed_sources[source_id as usize]
            {
                log::trace!(
                    "...choose block {:?} from source #{} pubkeyhash={}",
                    block.get_hash(),
                    block.get_source_id(),
                    block.get_source_public_key_hash()
                );

                block_hashes.push(block.get_hash().clone());
                blocks.push(block.clone());

                self.set_processed(block);
            }
        }

        self.process_deps = block_hashes;

        log::trace!("...creating block for deps: {:?}", self.process_deps);

        self.notify_process_blocks(blocks);

        log::trace!("...finish creating block");
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
        instrument!();

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
            log::trace!("Catchain forcing creation of a new block");
        }

        self.force_process = true;

        if !self.active_process {
            self.set_next_block_generation_time(time);
        }
    }

    fn processed_block(
        &mut self,
        payload: BlockPayloadPtr,
        mut may_be_skipped: bool,
        enable_batching_mode: bool,
    ) {
        instrument!();

        self.process_blocks_responses_counter.increment();

        assert!(self.receiver_started);

        if may_be_skipped && self.top_blocks.len() >= self.sources.len() / 3 {
            //skip ONLY if we have unprocessed (not merged) top blocks from less than 1/3 of validators
            //this is needed to reduce number of unprocessed top blocks from other validators in case of hanged consensus
            may_be_skipped = false;
        }

        if !may_be_skipped {
            log::trace!(
                "Catchain created block: deps={:?}, payload size is {}",
                self.process_deps,
                payload.data().len()
            );

            if !self.options.skip_processed_blocks {
                self.receiver
                    .borrow_mut()
                    .add_block(payload, self.process_deps.drain(..).collect());
            }
        } else {
            log::trace!("Catchain created skip-block: deps={:?}", self.process_deps);
        }

        assert!(self.active_process);

        let batching_mode = if self.force_process {
            false //do not batch in case if validator session requested to generate block immediately (collator's flow); default flow, same with T-Node
        } else {
            if self.top_blocks.len() == 0 {
                true //batch if we don't have unprocessed (not merged) top blocks from other validators (default flow; same with T-Node)
            } else {
                enable_batching_mode //batch if it is requested by a validator session
            }
        };

        log::debug!(
            "Catchain top blocks: {}{}",
            self.top_blocks.len(),
            if enable_batching_mode {
                " (batching)"
            } else {
                ""
            },
        );

        if may_be_skipped {
            self.process_blocks_skip_responses_counter.increment();
        }

        if !batching_mode {
            self.force_process = false;

            self.send_process();
        } else {
            self.active_process = false;

            self.process_blocks_batching_requests_counter.increment();

            log::debug!("...catchain finish processing");

            if self.top_blocks.len() == 0 {
                self.notify_finished_processing();
            }

            //force set next block generation time and ignore all earlier wakeups

            self.next_block_generation_time = SystemTime::now() + self.options.idle_timeout;
        }
    }

    /*
        Events processing Overlay -> Catchain
    */

    fn on_message(&mut self, adnl_id: PublicKeyHash, data: BlockPayloadPtr) {
        instrument!();

        let bytes = &mut data.data().as_ref();

        if log::log_enabled!(log::Level::Debug) {
            let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_else(
                |_| Duration::new(0, 0)
            ).as_millis();
            log::debug!(
                "Receive message from overlay for source: \
                size={}, payload={}, source={}, session_id={:x}, timestamp={:?}",
                bytes.len(),
                &hex::encode(&bytes),
                &hex::encode(adnl_id.data()),
                self.session_id,
                elapsed
            );
        }

        match self
            .receiver
            .borrow_mut()
            .receive_message_from_overlay(&adnl_id, bytes)
        {
            Ok(_) => (),
            Err(err) => log::warn!("CatchainImpl::on_message: {}", err),
        }
    }

    fn on_broadcast_from_overlay(&mut self, source_key_hash: PublicKeyHash, data: BlockPayloadPtr) {
        instrument!();

        if log::log_enabled!(log::Level::Debug) {
            let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_else(
                |_| Duration::new(0, 0)
            ).as_millis();
            log::debug!(
                "Receive broadcast from overlay for source: \
                size={}, payload={}, source={}, session_id={:x}, timestamp={:?}",
                data.data().len(),
                &hex::encode(&data.data().as_ref()),
                &hex::encode(source_key_hash.data()),
                self.session_id,
                elapsed
            );
        }

        self.receiver
            .borrow_mut()
            .receive_broadcast_from_overlay(&source_key_hash, &data);
    }

    fn on_broadcast_from_receiver(
        &mut self,
        source_key_hash: PublicKeyHash,
        data: BlockPayloadPtr,
    ) {
        instrument!();

        if log::log_enabled!(log::Level::Trace) {
            log::trace!(
                "Receive broadcast from overlay for source {}: {:?}",
                source_key_hash,
                data
            );
        }

        self.notify_broadcast(source_key_hash, data);
    }

    fn on_query(
        &mut self,
        adnl_id: PublicKeyHash,
        data: BlockPayloadPtr,
        response_callback: ExternalQueryResponseCallback,
    ) {
        instrument!();

        self.receiver
            .borrow_mut()
            .receive_query_from_overlay(&adnl_id, &data, response_callback);
    }

    fn on_time_changed(&mut self, time: std::time::SystemTime) {
        self.current_time = Some(time);

        self.notify_set_time(time);
    }

    /*
        Start up callback from Receiver
    */

    fn on_started(&mut self) {
        log::debug!("Catchain has been successfully started");

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
        payload: BlockPayloadPtr,
    ) {
        instrument!();

        log::trace!(
            "New catchain block {:x} (source_id={}, fork={}, height={})",
            hash,
            source_id,
            fork_id,
            height
        );

        if self.top_blocks.len() == 0 && !self.active_process && self.receiver_started {
            self.set_next_block_generation_time(SystemTime::now() + self.options.idle_timeout);
        }

        //obtain prev block and remove it block from the top blocks because new block is on top now

        let hash_zero = UInt256::default();
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

                log::trace!(
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

    pub fn on_custom_query(
        &mut self,
        source_public_key_hash: PublicKeyHash,
        data: BlockPayloadPtr,
        response_callback: ExternalQueryResponseCallback,
    ) {
        if log::log_enabled!(log::Level::Trace) {
            log::trace!(
                "CatchainProcessor.on_custom_query: public_key_hash={} payload={:?}",
                source_public_key_hash,
                data
            );
        }

        self.notify_custom_query(source_public_key_hash, data, response_callback);
    }

    /*
        Post closures to utility thread
    */

    fn post_utility_closure<F>(&self, task_fn: F)
    where
        F: FnOnce(),
        F: Send + 'static,
    {
        let task_desc = Box::new(TaskDesc::<dyn FnOnce() + Send> {
            task: Box::new(task_fn),
            creation_time: std::time::SystemTime::now(),
        });
        if let Err(send_error) = self.utility_queue_sender.send(task_desc) {
            log::error!("Catchain utility method call error: {}", send_error);
        }
    }

    /*
        Network messages transfering from Validator Session to Overlay
    */

    fn send_broadcast(&mut self, payload: BlockPayloadPtr) {
        instrument!();

        let source = self.receiver.borrow().get_source(self.local_idx);
        let source = source.borrow();
        let adnl_id = source.get_adnl_id();
        let local_id = &self.local_id;
        let overlay = &self.overlay;

        if CATCHAIN_POSTPONED_SEND_TO_OVERLAY {
            let adnl_id = adnl_id.clone();
            let local_id = local_id.clone();
            let overlay = Arc::downgrade(overlay);

            self.post_utility_closure(Box::new(move || {
                check_execution_time!(20000);

                if let Some(overlay) = overlay.upgrade() {
                    overlay.send_broadcast_fec_ex(&adnl_id, &local_id, payload);
                }
            }));
        } else {
            overlay.send_broadcast_fec_ex(adnl_id, local_id, payload);
        }
    }

    fn send_query_via_rldp(
        &mut self,
        dst_adnl_id: PublicKeyHash,
        name: String,
        response_callback: ExternalQueryResponseCallback,
        timeout: std::time::SystemTime,
        query: BlockPayloadPtr,
        max_answer_size: u64,
    ) {
        instrument!();

        if CATCHAIN_POSTPONED_SEND_TO_OVERLAY {
            let overlay = Arc::downgrade(&self.overlay);

            self.post_utility_closure(Box::new(move || {
                check_execution_time!(20000);

                if let Some(overlay) = overlay.upgrade() {
                    overlay.send_query_via_rldp(
                        dst_adnl_id,
                        name,
                        response_callback,
                        timeout,
                        query,
                        max_answer_size,
                    );
                }
            }));
        } else {
            self.overlay.send_query_via_rldp(
                dst_adnl_id,
                name,
                response_callback,
                timeout,
                query,
                max_answer_size,
            );
        }
    }

    /*
        Creation
    */

    pub fn create(
        catchain: CatchainImplPtr,
        options: Options,
        session_id: SessionId,
        ids: Vec<CatchainNode>,
        local_key: PrivateKey,
        path: String,
        db_suffix: String,
        allow_unsafe_self_blocks_resync: bool,
        utility_queue_sender: crossbeam::channel::Sender<Box<TaskDesc<dyn FnOnce() + Send>>>,
        overlay_manager: CatchainOverlayManagerPtr,
        listener: CatchainListenerPtr,
        metrics_receiver: Arc<metrics_runtime::Receiver>,
    ) -> Result<CatchainProcessor> {
        //sources preparation

        let mut sources: Vec<PublicKeyHash> = Vec::new();
        let mut local_idx = ids.len();
        let local_id = local_key.id().clone();

        sources.reserve(ids.len());

        for i in 0..ids.len() {
            sources.push(utils::get_public_key_hash(&ids[i].public_key));

            if sources[i] == local_id {
                local_idx = i;
            }
        }

        assert!(local_idx < ids.len());

        //overlay creation

        let sources_as_int256: Vec<UInt256> = sources
            .clone()
            .into_iter()
            .map(|key| utils::public_key_hash_to_int256(&key))
            .collect();
        let first_block = ::ton_api::ton::catchain::firstblock::Firstblock {
            unique_hash: session_id.clone().into(),
            nodes: sources_as_int256.into(),
        }
        .into_boxed();
        let overlay_id = utils::get_overlay_id(&first_block)?;
        let overlay_short_id = OverlayUtils::calc_private_overlay_short_id(&first_block)?;
        let overlay_listener = OverlayListenerImpl::create(Arc::downgrade(&catchain));
        let overlay_data_listener: Arc<dyn CatchainOverlayListener + Send + Sync> =
            overlay_listener.clone();
        let overlay_replay_listener: Arc<dyn CatchainOverlayLogReplayListener + Send + Sync> =
            overlay_listener.clone();
        let overlay = overlay_manager.start_overlay(
            &local_id,
            &overlay_short_id,
            &ids,
            Arc::downgrade(&overlay_data_listener),
            Arc::downgrade(&overlay_replay_listener),
        )?;

        //configure metrics

        let process_blocks_requests_counter =
            metrics_receiver.sink().counter("process_blocks_requests");
        let process_blocks_responses_counter =
            metrics_receiver.sink().counter("process_blocks_responses");
        let process_blocks_skip_responses_counter = metrics_receiver
            .sink()
            .counter("process_blocks_skip_responses");
        let process_blocks_batching_requests_counter = metrics_receiver
            .sink()
            .counter("process_blocks_batching_requests");

        log::debug!(
            "CatchainProcessor: starting up overlay \
            for session {:x} with ID {:x}, short_id {}",
            session_id, overlay_id, overlay_short_id
        );

        //receiver creation

        let receiver_listener =
            ReceiverListenerImpl::create(Arc::downgrade(&catchain), overlay.clone());
        let receiver = CatchainFactory::create_receiver(
            Rc::downgrade(&receiver_listener),
            &overlay_id,
            &ids,
            &local_key,
            path,
            db_suffix,
            allow_unsafe_self_blocks_resync,
            Some(metrics_receiver),
        )?;

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
            processing_blocks_stack: Vec::with_capacity(BLOCKS_PROCESSING_STACK_CAPACITY),
            processing_blocks_stack_tmp: Vec::with_capacity(BLOCKS_PROCESSING_STACK_CAPACITY),
            _ids: ids,
            local_id: local_id,
            local_idx: local_idx,
            _overlay_id: overlay_id,
            overlay_short_id: overlay_short_id.clone(),
            overlay: overlay,
            overlay_manager: overlay_manager,
            force_process: false,
            active_process: false,
            rng: rand::thread_rng(),
            current_extra_id: 0,
            receiver_started: false,
            current_time: None,
            utility_queue_sender: utility_queue_sender,
            process_blocks_requests_counter,
            process_blocks_responses_counter,
            process_blocks_skip_responses_counter,
            process_blocks_batching_requests_counter,
        };

        Ok(body)
    }
}

impl Drop for CatchainProcessor {
    fn drop(&mut self) {
        log::debug!("Dropping CatchainProcessor...");
        self.stop();
    }
}

/*
    Implementation of dummy overlay (for testing)
*/

struct DummyCatchainOverlay {}

impl CatchainOverlay for DummyCatchainOverlay {
    fn get_impl(&self) -> &dyn Any {
        self
    }

    fn send_message(
        &self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        message: &BlockPayloadPtr,
    ) {
        log::trace!(
            "DummyCatchainOverlay: send message {:?} -> {:?}: {:?}",
            sender_id,
            receiver_id,
            message
        );
    }

    fn send_message_multicast(
        &self,
        receiver_ids: &[PublicKeyHash],
        sender_id: &PublicKeyHash,
        message: &BlockPayloadPtr,
    ) {
        log::trace!(
            "DummyCatchainOverlay: send message multicast {:?} -> {:?}: {:?}",
            sender_id,
            receiver_ids,
            message
        );
    }

    fn send_query(
        &self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        name: &str,
        _timeout: std::time::Duration,
        message: &BlockPayloadPtr,
        _response_callback: ExternalQueryResponseCallback,
    ) {
        log::trace!(
            "DummyCatchainOverlay: send query {} {:?} -> {:?}: {:?}",
            name,
            sender_id,
            receiver_id,
            message
        );
    }

    fn send_query_via_rldp(
        &self,
        dst_adnl_id: PublicKeyHash,
        name: String,
        _response_callback: ExternalQueryResponseCallback,
        _timeout: std::time::SystemTime,
        query: BlockPayloadPtr,
        _max_answer_size: u64,
    ) {
        log::trace!(
            "DummyCatchainOverlay: send query '{}' via RLDP -> {}: {:?}",
            name,
            dst_adnl_id,
            query
        );
    }

    fn send_broadcast_fec_ex(
        &self,
        sender_id: &PublicKeyHash,
        send_as: &PublicKeyHash,
        payload: BlockPayloadPtr,
    ) {
        log::trace!(
            "DummyCatchainOverlay: send broadcast_fec_ex {:?}/{:?}: {:?}",
            sender_id,
            send_as,
            payload
        );
    }
}

struct DummyCatchainOverlayManager {}

impl CatchainOverlayManager for DummyCatchainOverlayManager {
    /// Create new overlay
    fn start_overlay(
        &self,
        _local_id: &PublicKeyHash,
        _overlay_short_id: &Arc<PrivateOverlayShortId>,
        _nodes: &Vec<CatchainNode>,
        _overlay_listener: CatchainOverlayListenerPtr,
        _log_replay_listener: CatchainOverlayLogReplayListenerPtr,
    ) -> Result<CatchainOverlayPtr> {
        Ok(Arc::new(DummyCatchainOverlay {}))
    }

    /// Stop existing overlay
    fn stop_overlay(
        &self,
        _overlay_short_id: &Arc<PrivateOverlayShortId>,
        _overlay: &CatchainOverlayPtr,
    ) {
    }
}

/*
    Implementation of public Catchain trait
*/

impl Catchain for CatchainImpl {
    /*
        Catchain stop
    */

    fn stop(&self, destroy_db: bool) {
        if destroy_db {
            self.destroy_db_flag.store(true, Ordering::SeqCst);
        }

        self.should_stop_flag.store(true, Ordering::SeqCst);

        loop {
            if self.main_thread_is_stopped_flag.load(Ordering::SeqCst)
                && self.utility_thread_is_stopped_flag.load(Ordering::SeqCst)
            {
                break;
            }

            log::info!(
                "...waiting for Catchain threads (session_id is {:x}), main={}, util={}",
                self.session_id,
                self.main_thread_is_stopped_flag.load(Ordering::SeqCst),
                self.utility_thread_is_stopped_flag.load(Ordering::SeqCst)
            );

            const CHECKING_INTERVAL: std::time::Duration = std::time::Duration::from_millis(300);

            std::thread::sleep(CHECKING_INTERVAL);
        }

        log::info!(
            "Catchain has been stopped (session_id is {:x})",
            self.session_id
        );
    }

    /*
        Catchain blocks processing
    */

    fn request_new_block(&self, time: SystemTime) {
        self.post_closure(move |processor: &mut CatchainProcessor| {
            processor.request_new_block(time);
        });
    }

    fn processed_block(
        &self,
        payload: BlockPayloadPtr,
        may_be_skipped: bool,
        enable_batching_mode: bool,
    ) {
        self.post_closure(move |processor: &mut CatchainProcessor| {
            processor.processed_block(payload, may_be_skipped, enable_batching_mode);
        });
    }

    /*
        Network access interface
    */

    fn send_broadcast(&self, payload: BlockPayloadPtr) {
        self.post_closure(move |processor: &mut CatchainProcessor| {
            processor.send_broadcast(payload);
        });
    }

    fn send_query_via_rldp(
        &self,
        dst_adnl_id: PublicKeyHash,
        name: String,
        response_callback: ExternalQueryResponseCallback,
        timeout: std::time::SystemTime,
        query: BlockPayloadPtr,
        max_answer_size: u64,
    ) {
        self.post_closure(move |processor: &mut CatchainProcessor| {
            processor.send_query_via_rldp(
                dst_adnl_id,
                name,
                response_callback,
                timeout,
                query,
                max_answer_size,
            );
        });
    }
}

/*
    Private CatchainImpl details
*/

impl Drop for CatchainImpl {
    fn drop(&mut self) {
        log::debug!("Dropping Catchain...");
        self.stop(false);
    }
}

impl CatchainImpl {
    /*
        Catchain messages processing
    */

    fn post_closure(&self, task_fn: impl FnOnce(&mut CatchainProcessor) + Send + 'static) {
        let task_desc = Box::new(TaskDesc::<dyn FnOnce(&mut CatchainProcessor) + Send> {
            task: Box::new(task_fn),
            creation_time: std::time::SystemTime::now(),
        });
        if let Err(send_error) = self.main_queue_sender.send(task_desc) {
            log::error!("Catchain method call error: {}", send_error);
        } else {
            self.main_thread_post_counter.increment();
        }
    }

    fn post_utility_closure<F>(&self, task_fn: F)
    where
        F: FnOnce(),
        F: Send + 'static,
    {
        let task_desc = Box::new(TaskDesc::<dyn FnOnce() + Send> {
            task: Box::new(task_fn),
            creation_time: std::time::SystemTime::now(),
        });
        if let Err(send_error) = self.utility_queue_sender.send(task_desc) {
            log::error!("Catchain utility method call error: {}", send_error);
        } else {
            self.utility_thread_post_counter.increment();
        }
    }

    /*
        Catchain creation
    */

    pub fn create_dummy_overlay_manager() -> CatchainOverlayManagerPtr {
        Arc::new(DummyCatchainOverlayManager {})
    }

    pub fn create(
        options: &Options,
        session_id: &SessionId,
        ids: &Vec<CatchainNode>,
        local_key: &PrivateKey,
        path: String,
        db_suffix: String,
        allow_unsafe_self_blocks_resync: bool,
        overlay_manager: CatchainOverlayManagerPtr,
        listener: CatchainListenerPtr,
    ) -> CatchainPtr {
        log::debug!("Creating Catchain...");

        let (main_queue_sender, main_queue_receiver): (
            crossbeam::channel::Sender<Box<TaskDesc<dyn FnOnce(&mut CatchainProcessor) + Send>>>,
            crossbeam::channel::Receiver<Box<TaskDesc<dyn FnOnce(&mut CatchainProcessor) + Send>>>,
        ) = crossbeam::crossbeam_channel::unbounded();
        let (utility_queue_sender, utility_queue_receiver): (
            crossbeam::channel::Sender<Box<TaskDesc<dyn FnOnce() + Send>>>,
            crossbeam::channel::Receiver<Box<TaskDesc<dyn FnOnce() + Send>>>,
        ) = crossbeam::crossbeam_channel::unbounded();
        let utility_queue_sender_clone = utility_queue_sender.clone();
        let should_stop_flag = Arc::new(AtomicBool::new(false));
        let destroy_db_flag = Arc::new(AtomicBool::new(false));
        let main_thread_is_stopped_flag = Arc::new(AtomicBool::new(false));
        let utility_thread_is_stopped_flag = Arc::new(AtomicBool::new(false));
        let main_thread_overloaded_flag = Arc::new(AtomicBool::new(false));
        let utility_thread_overloaded_flag = Arc::new(AtomicBool::new(false));
        let name = format!("Catchain_{:x}", session_id);
        let catchain_activity_node = CatchainFactory::create_activity_node(name);

        let metrics_receiver = Arc::new(
            metrics_runtime::Receiver::builder()
                .build()
                .expect("failed to create metrics receiver"),
        );

        let main_thread_post_counter = metrics_receiver.sink().counter("main_queue.posts");
        let main_thread_pull_counter = metrics_receiver.sink().counter("main_queue.pulls");
        let utility_thread_post_counter = metrics_receiver.sink().counter("utility_queue.posts");
        let utility_thread_pull_counter = metrics_receiver.sink().counter("utility_queue.pulls");

        let body: CatchainImpl = CatchainImpl {
            main_queue_sender,
            utility_queue_sender,
            should_stop_flag: should_stop_flag.clone(),
            main_thread_is_stopped_flag: main_thread_is_stopped_flag.clone(),
            utility_thread_is_stopped_flag: utility_thread_is_stopped_flag.clone(),
            main_thread_overloaded_flag: main_thread_overloaded_flag.clone(),
            _utility_thread_overloaded_flag: utility_thread_overloaded_flag.clone(),
            destroy_db_flag: destroy_db_flag.clone(),
            session_id: session_id.clone(),
            _activity_node: catchain_activity_node.clone(),
            main_thread_post_counter,
            main_thread_pull_counter,
            utility_thread_post_counter,
            _utility_thread_pull_counter: utility_thread_pull_counter.clone(),
        };

        let catchain = Arc::new(body);
        let catchain_ret = catchain.clone();
        let ids = ids.clone();
        let local_key = local_key.clone();
        let session_id = session_id.clone();
        let options = *options;

        let stop_flag_for_main_loop = should_stop_flag.clone();
        let session_id_clone = session_id.clone();
        let metrics_receiver_clone = metrics_receiver.clone();
        let _main_thread = std::thread::Builder::new()
            .name(format!("{}:{:x}", MAIN_LOOP_NAME, session_id))
            .stack_size(CATCHAIN_MAIN_LOOP_THREAD_STACK_SIZE)
            .spawn(move || {
                CatchainProcessor::main_loop(
                    catchain,
                    main_queue_receiver,
                    stop_flag_for_main_loop,
                    main_thread_is_stopped_flag,
                    main_thread_overloaded_flag,
                    destroy_db_flag,
                    options,
                    session_id,
                    ids,
                    local_key,
                    path,
                    db_suffix,
                    allow_unsafe_self_blocks_resync,
                    utility_queue_sender_clone,
                    overlay_manager,
                    listener,
                    catchain_activity_node,
                    metrics_receiver,
                );
            });

        let stop_flag_for_utility_loop = should_stop_flag.clone();
        let _utility_thread = std::thread::Builder::new()
            .name(format!("{}:{}", UTILITY_LOOP_NAME, session_id_clone))
            .spawn(move || {
                CatchainProcessor::utility_loop(
                    stop_flag_for_utility_loop,
                    utility_thread_is_stopped_flag,
                    utility_thread_overloaded_flag,
                    utility_queue_receiver,
                    session_id_clone,
                    metrics_receiver_clone,
                    utility_thread_pull_counter,
                );
            });

        catchain_ret
    }
}
