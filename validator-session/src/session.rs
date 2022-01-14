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

pub use super::*;

use super::task_queue::*;
use catchain::check_execution_time;
use catchain::profiling::instrument;
use catchain::utils::compute_instance_counter;
use catchain::utils::MetricsDumper;
use catchain::BlockPtr;
use catchain::CatchainListener;
use catchain::CatchainPtr;
use catchain::ExternalQueryResponseCallback;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

/*
    Constants
*/

const MAIN_LOOP_NAME: &str = "VS1"; //validator session main loop short name
const CALLBACKS_LOOP_NAME: &str = "VS2"; //validator session callbacks loop short name
const TASK_QUEUE_WARN_PROCESSING_LATENCY: Duration = Duration::from_millis(1000); //max processing latency
const TASK_QUEUE_LATENCY_WARN_DUMP_PERIOD: Duration = Duration::from_millis(2000); //latency warning dump period
const SESSION_METRICS_DUMP_PERIOD_MS: u64 = 5000; //period of metrics dump
const SESSION_PROFILING_DUMP_PERIOD_MS: u64 = 30000; //period of profiling dump

/*
===================================================================================================
    TaskQueue
===================================================================================================
*/

trait DefaultTaskFactory<FuncPtr: Send + 'static> {
    fn create_default_task() -> FuncPtr;
}

impl DefaultTaskFactory<TaskPtr> for TaskPtr {
    fn create_default_task() -> TaskPtr {
        Box::new(|_processor: &mut dyn SessionProcessor| {})
    }
}

impl DefaultTaskFactory<CallbackTaskPtr> for CallbackTaskPtr {
    fn create_default_task() -> CallbackTaskPtr {
        Box::new(|| {})
    }
}

struct TaskDesc<FuncPtr> {
    task: FuncPtr,                        //closure for execution
    creation_time: std::time::SystemTime, //task creation time
}

struct TaskQueueImpl<FuncPtr> {
    name: String,                                                         //queue name
    queue_sender: crossbeam::channel::Sender<Box<TaskDesc<FuncPtr>>>, //queue sender from outer world to the ValidatorSession
    queue_receiver: crossbeam::channel::Receiver<Box<TaskDesc<FuncPtr>>>, //queue receiver from outer world to the ValidatorSession
    post_counter: metrics_runtime::data::Counter,                         //counter for queue posts
    pull_counter: metrics_runtime::data::Counter,                         //counter for queue pull
    is_overloaded: Arc<AtomicBool>, //atomic flag to indicate that queue is overloaded
    linked_queue: Option<Arc<dyn TaskQueue<FuncPtr>>>, //linked task queue to wake up
}

/*
    Implementation for crate TaskQueue trait
*/

impl<FuncPtr> TaskQueue<FuncPtr> for TaskQueueImpl<FuncPtr>
where
    FuncPtr: Send + DefaultTaskFactory<FuncPtr> + 'static,
{
    fn is_overloaded(&self) -> bool {
        self.is_overloaded.load(Ordering::Relaxed)
    }

    fn is_empty(&self) -> bool {
        self.queue_receiver.is_empty()
    }

    fn post_closure(&self, task: FuncPtr) {
        let task_desc = Box::new(TaskDesc::<FuncPtr> {
            task: task,
            creation_time: std::time::SystemTime::now(),
        });
        if let Err(send_error) = self.queue_sender.send(task_desc) {
            error!("ValidatorSession method post closure error: {}", send_error);
        } else {
            self.post_counter.increment();

            if let Some(ref linked_queue) = &self.linked_queue {
                linked_queue.post_closure(FuncPtr::create_default_task());
            }
        }
    }

    fn pull_closure(
        &self,
        timeout: std::time::Duration,
        last_warn_dump_time: &mut std::time::SystemTime,
    ) -> Option<FuncPtr> {
        match self.queue_receiver.recv_timeout(timeout) {
            Ok(task_desc) => {
                let processing_latency = task_desc.creation_time.elapsed().unwrap();
                if processing_latency > TASK_QUEUE_WARN_PROCESSING_LATENCY {
                    self.is_overloaded.store(true, Ordering::Release);

                    if let Ok(warn_elapsed) = last_warn_dump_time.elapsed() {
                        if warn_elapsed > TASK_QUEUE_LATENCY_WARN_DUMP_PERIOD {
                            warn!("ValidatorSession {} task queue latency is {:.3}s (expected max latency is {:.3}s)", self.name, processing_latency.as_secs_f64(), TASK_QUEUE_WARN_PROCESSING_LATENCY.as_secs_f64());
                            *last_warn_dump_time = SystemTime::now();
                        }
                    }
                } else {
                    self.is_overloaded.store(false, Ordering::Release);
                }

                self.pull_counter.increment();

                Some(task_desc.task)
            }
            _ => {
                self.is_overloaded.store(false, Ordering::Release);

                None
            }
        }
    }
}

/*
    Implementation internals of TaskQueueImpl
*/

impl<FuncPtr> TaskQueueImpl<FuncPtr>
where
    FuncPtr: Send + 'static,
{
    pub(crate) fn new(
        name: String,
        queue_sender: crossbeam::channel::Sender<Box<TaskDesc<FuncPtr>>>,
        queue_receiver: crossbeam::channel::Receiver<Box<TaskDesc<FuncPtr>>>,
        linked_queue: Option<Arc<dyn TaskQueue<FuncPtr>>>,
        metrics_receiver: Arc<metrics_runtime::Receiver>,
    ) -> Self {
        let pull_counter = metrics_receiver
            .sink()
            .counter(format!("{}_queue.pulls", name));
        let post_counter = metrics_receiver
            .sink()
            .counter(format!("{}_queue.posts", name));

        Self {
            name: name,
            queue_sender: queue_sender,
            queue_receiver: queue_receiver,
            pull_counter: pull_counter,
            post_counter: post_counter,
            is_overloaded: Arc::new(AtomicBool::new(false)),
            linked_queue,
        }
    }
}

/*
===================================================================================================
    CatchainListener
===================================================================================================
*/

type ArcCatchainListenerPtr = Arc<dyn CatchainListener + Send + Sync>;

struct CatchainListenerImpl {
    task_queue: TaskQueuePtr, //task queue
}

impl CatchainListener for CatchainListenerImpl {
    fn preprocess_block(&self, block: BlockPtr) {
        self.post_closure(move |processor: &mut dyn SessionProcessor| {
            processor.preprocess_block(block);
        });
    }

    fn process_blocks(&self, blocks: Vec<BlockPtr>) {
        self.post_closure(move |processor: &mut dyn SessionProcessor| {
            processor.process_blocks(blocks);
        });
    }

    fn finished_processing(&self) {
        self.post_closure(move |processor: &mut dyn SessionProcessor| {
            processor.finished_catchain_processing();
        });
    }

    fn started(&self) {
        self.post_closure(move |processor: &mut dyn SessionProcessor| {
            processor.catchain_started();
        });
    }

    fn process_broadcast(&self, source_id: PublicKeyHash, data: BlockPayloadPtr) {
        self.post_closure(move |processor: &mut dyn SessionProcessor| {
            processor.process_broadcast(source_id, data);
        });
    }

    fn process_query(
        &self,
        source_id: PublicKeyHash,
        data: BlockPayloadPtr,
        callback: ExternalQueryResponseCallback,
    ) {
        self.post_closure(move |processor: &mut dyn SessionProcessor| {
            processor.process_query(source_id, data, callback);
        });
    }

    fn set_time(&self, time: std::time::SystemTime) {
        self.post_closure(move |processor: &mut dyn SessionProcessor| {
            processor.set_time(time);
        });
    }
}

impl CatchainListenerImpl {
    fn post_closure<F>(&self, task_fn: F)
    where
        F: FnOnce(&mut dyn SessionProcessor),
        F: Send + 'static,
    {
        self.task_queue.post_closure(Box::new(task_fn));
    }

    fn create(task_queue: TaskQueuePtr) -> ArcCatchainListenerPtr {
        Arc::new(Self {
            task_queue: task_queue,
        })
    }
}

/*
===================================================================================================
    Session
===================================================================================================
*/

/*
    Implementation details for Session
*/

pub(crate) struct SessionImpl {
    stop_flag: Arc<AtomicBool>, //atomic flag to indicate that all processing threads should be stopped
    main_processing_thread_stopped: Arc<AtomicBool>, //atomic flag to indicate that processing thread has been stopped
    session_callbacks_processing_thread_stopped: Arc<AtomicBool>, //atomic flag to indicate that processing thread has been stopped
    _main_task_queue: TaskQueuePtr, //task queue for main thread tasks processing
    _session_callbacks_responses_task_queue: TaskQueuePtr, //task queue for session callbacks responses processing
    _session_callbacks_task_queue: CallbackTaskQueuePtr, //task queue for session callbacks processing
    catchain: CatchainPtr,                               //catchain session
    session_id: SessionId,
    _catchain_listener: ArcCatchainListenerPtr, //catchain session listener
    _activity_node: ActivityNodePtr,            //activity node for session lifetime tracking
}

/*
    Implementation for public Session trait
*/

impl Session for SessionImpl {
    fn stop(&self) {
        self.stop_impl(true); //manual stop of session requires removing of Catchain's DB
    }
}

/*
    Implementation for public Display
*/

impl fmt::Display for SessionImpl {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unimplemented!();
    }
}

/*
    Drop trait implementation for SessionImpl
*/

impl Drop for SessionImpl {
    fn drop(&mut self) {
        debug!("Dropping ValidatorSession...");

        self.stop_impl(false);
    }
}

/*
    Implementation internals of SessionImpl
*/

impl SessionImpl {
    /*
        Session stopping
    */

    fn stop_impl(&self, destroy_catchain_db: bool) {
        self.stop_flag.store(true, Ordering::Release);

        debug!(
            "...stopping Catchain (session_id is {})",
            self.session_id.to_hex_string()
        );

        self.catchain.stop(destroy_catchain_db);

        loop {
            if self
                .session_callbacks_processing_thread_stopped
                .load(Ordering::Relaxed)
                && self.main_processing_thread_stopped.load(Ordering::Relaxed)
            {
                break;
            }

            info!(
                "...waiting for ValidatorSession threads (session_id is {})",
                self.session_id.to_hex_string()
            );

            const CHECKING_INTERVAL: std::time::Duration = std::time::Duration::from_millis(300);

            std::thread::sleep(CHECKING_INTERVAL);
        }

        info!(
            "ValidatorSession has been stopped (session_id is {})",
            self.session_id.to_hex_string()
        );
    }

    /*
        Main loop & session callbacks processing loop
    */

    fn main_loop(
        should_stop_flag: Arc<AtomicBool>,
        is_stopped_flag: Arc<AtomicBool>,
        task_queue: TaskQueuePtr,
        completion_task_queue: TaskQueuePtr,
        callbacks_task_queue: CallbackTaskQueuePtr,
        options: SessionOptions,
        session_id: SessionId,
        ids: Vec<SessionNode>,
        local_key: PrivateKey,
        listener: SessionListenerPtr,
        catchain: CatchainPtr,
        session_activity_node: ActivityNodePtr,
        session_creation_time: std::time::SystemTime,
        metrics_receiver: Arc<metrics_runtime::Receiver>,
    ) {
        info!(
            "ValidatorSession main loop is started (session_id is {}); session thread creation time is {:.3}ms",
            session_id.to_hex_string(),
            session_creation_time.elapsed().unwrap().as_secs_f64() * 1000.0,
        );

        //configure metrics

        let loop_counter = metrics_receiver
            .sink()
            .counter("validator_session_main_loop_iterations");
        let loop_overloads_counter = metrics_receiver
            .sink()
            .counter("validator_session_main_loop_overloads");

        //create session processor

        let processor = SessionFactory::create_session_processor(
            options,
            session_id.clone(),
            ids,
            local_key,
            listener,
            catchain,
            completion_task_queue.clone(),
            callbacks_task_queue.clone(),
            session_creation_time,
            Some(metrics_receiver),
        );

        //create metrics dumper

        let mut metrics_dumper = MetricsDumper::new();

        metrics_dumper
            .add_compute_handler("sent_blocks.total".to_string(), &compute_instance_counter);
        metrics_dumper.add_compute_handler(
            "block_candidates_signatures.total".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "block_candidates.total".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "vote_candidates.total".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "round_attempts.total".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler("rounds.total".to_string(), &compute_instance_counter);
        metrics_dumper
            .add_compute_handler("old_rounds.total".to_string(), &compute_instance_counter);
        metrics_dumper.add_compute_handler(
            "session_states.total".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "integer_vectors.total".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper
            .add_compute_handler("bool_vectors.total".to_string(), &compute_instance_counter);
        metrics_dumper.add_compute_handler(
            "block_candidate_vectors.total".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "block_candidate_signature_vectors.total".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "vote_candidate_vectors.total".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "round_attempt_vectors.total".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "old_round_vectors.total".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_derivative_metric("session_states.total".to_string());
        metrics_dumper.add_derivative_metric("old_rounds.total".to_string());
        metrics_dumper.add_derivative_metric("rounds.total".to_string());
        metrics_dumper.add_derivative_metric("round_attempts.total".to_string());
        metrics_dumper.add_derivative_metric("block_candidates.total".to_string());
        metrics_dumper.add_derivative_metric("vote_candidates.total".to_string());
        metrics_dumper.add_derivative_metric("block_candidates_signatures.total".to_string());
        metrics_dumper.add_derivative_metric("sent_blocks.total".to_string());
        metrics_dumper.add_derivative_metric("old_round_vectors.total".to_string());
        metrics_dumper.add_derivative_metric("round_vectors.total".to_string());
        metrics_dumper.add_derivative_metric("round_attempt_vectors.total".to_string());
        metrics_dumper.add_derivative_metric("block_candidate_vectors.total".to_string());
        metrics_dumper.add_derivative_metric("vote_candidate_vectors.total".to_string());
        metrics_dumper.add_derivative_metric("block_candidate_signature_vectors.total".to_string());
        metrics_dumper.add_derivative_metric("sent_block_vectors.total".to_string());
        metrics_dumper.add_derivative_metric("bool_vectors.total".to_string());
        metrics_dumper.add_derivative_metric("integer_vectors.total".to_string());

        metrics_dumper.add_compute_handler(
            "sent_blocks.persistent".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "block_candidates_signatures.persistent".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "block_candidates.persistent".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "vote_candidates.persistent".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "round_attempts.persistent".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper
            .add_compute_handler("rounds.persistent".to_string(), &compute_instance_counter);
        metrics_dumper.add_compute_handler(
            "old_rounds.persistent".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "session_states.persistent".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "integer_vectors.persistent".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "bool_vectors.persistent".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "block_candidate_vectors.persistent".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "block_candidate_signature_vectors.persistent".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "vote_candidate_vectors.persistent".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "round_attempt_vectors.persistent".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "old_round_vectors.persistent".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_derivative_metric("session_states.persistent".to_string());
        metrics_dumper.add_derivative_metric("old_rounds.persistent".to_string());
        metrics_dumper.add_derivative_metric("rounds.persistent".to_string());
        metrics_dumper.add_derivative_metric("round_attempts.persistent".to_string());
        metrics_dumper.add_derivative_metric("block_candidates.persistent".to_string());
        metrics_dumper.add_derivative_metric("vote_candidates.persistent".to_string());
        metrics_dumper.add_derivative_metric("block_candidates_signatures.persistent".to_string());
        metrics_dumper.add_derivative_metric("sent_blocks.persistent".to_string());
        metrics_dumper.add_derivative_metric("old_round_vectors.persistent".to_string());
        metrics_dumper.add_derivative_metric("round_vectors.persistent".to_string());
        metrics_dumper.add_derivative_metric("round_attempt_vectors.persistent".to_string());
        metrics_dumper.add_derivative_metric("block_candidate_vectors.persistent".to_string());
        metrics_dumper.add_derivative_metric("vote_candidate_vectors.persistent".to_string());
        metrics_dumper
            .add_derivative_metric("block_candidate_signature_vectors.persistent".to_string());
        metrics_dumper.add_derivative_metric("sent_block_vectors.persistent".to_string());
        metrics_dumper.add_derivative_metric("bool_vectors.persistent".to_string());
        metrics_dumper.add_derivative_metric("integer_vectors.persistent".to_string());

        metrics_dumper
            .add_compute_handler("sent_blocks.temp".to_string(), &compute_instance_counter);
        metrics_dumper.add_compute_handler(
            "block_candidates_signatures.temp".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "block_candidates.temp".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "vote_candidates.temp".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper
            .add_compute_handler("round_attempts.temp".to_string(), &compute_instance_counter);
        metrics_dumper.add_compute_handler("rounds.temp".to_string(), &compute_instance_counter);
        metrics_dumper
            .add_compute_handler("old_rounds.temp".to_string(), &compute_instance_counter);
        metrics_dumper
            .add_compute_handler("session_states.temp".to_string(), &compute_instance_counter);
        metrics_dumper.add_compute_handler(
            "integer_vectors.temp".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper
            .add_compute_handler("bool_vectors.temp".to_string(), &compute_instance_counter);
        metrics_dumper.add_compute_handler(
            "block_candidate_vectors.temp".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "block_candidate_signature_vectors.temp".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "vote_candidate_vectors.temp".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "round_attempt_vectors.temp".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_compute_handler(
            "old_round_vectors.temp".to_string(),
            &compute_instance_counter,
        );
        metrics_dumper.add_derivative_metric("session_states.temp".to_string());
        metrics_dumper.add_derivative_metric("old_rounds.temp".to_string());
        metrics_dumper.add_derivative_metric("rounds.temp".to_string());
        metrics_dumper.add_derivative_metric("round_attempts.temp".to_string());
        metrics_dumper.add_derivative_metric("block_candidates.temp".to_string());
        metrics_dumper.add_derivative_metric("vote_candidates.temp".to_string());
        metrics_dumper.add_derivative_metric("block_candidates_signatures.temp".to_string());
        metrics_dumper.add_derivative_metric("sent_blocks.temp".to_string());
        metrics_dumper.add_derivative_metric("old_round_vectors.temp".to_string());
        metrics_dumper.add_derivative_metric("round_vectors.temp".to_string());
        metrics_dumper.add_derivative_metric("round_attempt_vectors.temp".to_string());
        metrics_dumper.add_derivative_metric("block_candidate_vectors.temp".to_string());
        metrics_dumper.add_derivative_metric("vote_candidate_vectors.temp".to_string());
        metrics_dumper.add_derivative_metric("block_candidate_signature_vectors.temp".to_string());
        metrics_dumper.add_derivative_metric("sent_block_vectors.temp".to_string());
        metrics_dumper.add_derivative_metric("bool_vectors.temp".to_string());
        metrics_dumper.add_derivative_metric("integer_vectors.temp".to_string());

        use catchain::utils::add_compute_percentage_metric;
        use catchain::utils::add_compute_relative_metric;
        use catchain::utils::add_compute_result_metric;

        metrics_dumper.add_derivative_metric("validator_session_main_loop_iterations".to_string());
        metrics_dumper.add_derivative_metric("validator_session_main_loop_overloads".to_string());
        metrics_dumper
            .add_derivative_metric("validator_session_callbacks_loop_iterations".to_string());
        metrics_dumper
            .add_derivative_metric("validator_session_callbacks_loop_overloads".to_string());
        add_compute_percentage_metric(
            &mut metrics_dumper,
            &"validator_session_main_loop_load".to_string(),
            &"validator_session_main_loop_overloads".to_string(),
            &"validator_session_main_loop_iterations".to_string(),
            0.0,
        );
        add_compute_percentage_metric(
            &mut metrics_dumper,
            &"validator_session_callbacks_loop_load".to_string(),
            &"validator_session_callbacks_loop_overloads".to_string(),
            &"validator_session_callbacks_loop_iterations".to_string(),
            0.0,
        );
        add_compute_percentage_metric(
            &mut metrics_dumper,
            &"active_nodes".to_string(),
            &"active_weight".to_string(),
            &"total_weight".to_string(),
            0.0,
        );

        add_compute_result_metric(&mut metrics_dumper, &"collate_requests".to_string());
        add_compute_result_metric(&mut metrics_dumper, &"collate_requests_expire".to_string());
        add_compute_result_metric(&mut metrics_dumper, &"validate_requests".to_string());
        add_compute_result_metric(&mut metrics_dumper, &"commit_requests".to_string());
        add_compute_result_metric(&mut metrics_dumper, &"rldp_queries".to_string());
        add_compute_result_metric(&mut metrics_dumper, &"temp_cache_reuse".to_string());
        add_compute_result_metric(&mut metrics_dumper, &"persistent_cache_reuse".to_string());
        metrics_dumper.add_derivative_metric("rldp_queries.total".to_string());
        metrics_dumper.add_derivative_metric("validate_requests.total".to_string());
        metrics_dumper.add_derivative_metric("validate_requests.failure".to_string());
        metrics_dumper.add_derivative_metric("validate_requests.success".to_string());
        metrics_dumper.add_derivative_metric("collate_requests.total".to_string());
        metrics_dumper.add_derivative_metric("collate_requests.failure".to_string());
        metrics_dumper.add_derivative_metric("collate_requests.success".to_string());
        metrics_dumper.add_derivative_metric("commit_requests.total".to_string());
        metrics_dumper.add_derivative_metric("commit_requests.failure".to_string());
        metrics_dumper.add_derivative_metric("commit_requests.success".to_string());

        metrics_dumper.add_derivative_metric("process_blocks_calls".to_string());
        metrics_dumper.add_derivative_metric("preprocess_block_calls".to_string());
        metrics_dumper.add_derivative_metric("request_new_block_calls".to_string());
        metrics_dumper.add_derivative_metric("check_all_calls".to_string());
        add_compute_relative_metric(
            &mut metrics_dumper,
            &"process_blocks_avg_capacity".to_string(),
            &"preprocess_block_calls".to_string(),
            &"process_blocks_calls".to_string(),
            0.0,
        );
        add_compute_relative_metric(
            &mut metrics_dumper,
            &"request_new_block_capacity".to_string(),
            &"preprocess_block_calls".to_string(),
            &"request_new_block_calls".to_string(),
            0.0,
        );
        add_compute_relative_metric(
            &mut metrics_dumper,
            &"iterations_per_check_all".to_string(),
            &"validator_session_main_loop_iterations".to_string(),
            &"check_all_calls".to_string(),
            0.0,
        );
        add_compute_relative_metric(
            &mut metrics_dumper,
            &"check_all_per_request_new_block".to_string(),
            &"check_all_calls".to_string(),
            &"request_new_block_calls".to_string(),
            0.0,
        );
        add_compute_relative_metric(
            &mut metrics_dumper,
            &"candidates_per_round".to_string(),
            &"sent_blocks.persistent".to_string(),
            &"commit_requests.total".to_string(),
            0.0,
        );

        metrics_dumper.add_derivative_metric("processing_queue.pulls".to_string());
        metrics_dumper.add_derivative_metric("processing_queue.posts".to_string());
        metrics_dumper.add_derivative_metric("callbacks_queue.pulls".to_string());
        metrics_dumper.add_derivative_metric("callbacks_queue.posts".to_string());

        metrics_dumper.add_compute_handler(
            "processing_queue".to_string(),
            &catchain::utils::compute_queue_size_counter,
        );
        metrics_dumper.add_compute_handler(
            "callbacks_queue".to_string(),
            &catchain::utils::compute_queue_size_counter,
        );

        metrics_dumper.add_compute_handler(
            "state_merge_cache_items".to_string(),
            &catchain::utils::compute_instance_counter,
        );
        add_compute_result_metric(&mut metrics_dumper, &"state_merge_cache_reuse".to_string());
        metrics_dumper.add_compute_handler(
            "block_update_cache_items".to_string(),
            &catchain::utils::compute_instance_counter,
        );
        add_compute_result_metric(&mut metrics_dumper, &"block_update_cache_reuse".to_string());

        //main loop

        let mut last_warn_dump_time = std::time::SystemTime::now();
        let mut next_metrics_dump_time = last_warn_dump_time;
        let mut next_profiling_dump_time = last_warn_dump_time;
        let mut last_unprioritized_closure_pulled = last_warn_dump_time;

        loop {
            {
                instrument!();

                session_activity_node.tick();
                loop_counter.increment();

                //check if the main loop should be stopped

                if should_stop_flag.load(Ordering::Relaxed) {
                    processor.borrow_mut().stop();
                    break;
                }

                //check overload flag

                if task_queue.is_overloaded() {
                    loop_overloads_counter.increment();
                }

                //handle session event with timeout

                let now = SystemTime::now();
                let timeout = match processor.borrow().get_next_awake_time().duration_since(now) {
                    Ok(timeout) => timeout,
                    Err(_err) => Duration::default(),
                };

                const MAX_TIMEOUT: Duration = Duration::from_secs(2); //such little timeout is needed to check should_stop_flag and thread exiting
                const MAX_UNPRIORITIZED_PULLS_TIMEOUT: Duration = Duration::from_secs(2); //max timeout for processing only prioritized events

                let timeout = std::cmp::min(timeout, MAX_TIMEOUT);

                let task = {
                    instrument!();

                    if completion_task_queue.is_empty()
                        || last_unprioritized_closure_pulled.elapsed().unwrap()
                            > MAX_UNPRIORITIZED_PULLS_TIMEOUT
                    {
                        last_unprioritized_closure_pulled = now;
                        task_queue.pull_closure(timeout, &mut last_warn_dump_time)
                    } else {
                        completion_task_queue
                            .pull_closure(Duration::from_millis(1), &mut last_warn_dump_time)
                    }
                };

                if let Some(task) = task {
                    check_execution_time!(100_000);
                    instrument!();

                    task(&mut *processor.borrow_mut());
                }

                //do checks only for next awake time

                if processor.borrow().get_next_awake_time().elapsed().is_ok() {
                    //check & update session state

                    instrument!();
                    check_execution_time!(20_000);

                    processor.borrow_mut().reset_next_awake_time();
                    processor.borrow_mut().check_all();
                }
            }

            //dump metrics

            if let Ok(_elapsed) = next_metrics_dump_time.elapsed() {
                instrument!();
                check_execution_time!(50_000);

                metrics_dumper.update(&processor.borrow().get_description().get_metrics_receiver());

                if log_enabled!(log::Level::Debug) {
                    let session_id_str = session_id.to_hex_string();
                    debug!("ValidatorSession {} metrics:", &session_id_str);

                    metrics_dumper.dump(|string| {
                        debug!("{}{}", session_id_str, string);
                    });
                }

                next_metrics_dump_time = std::time::SystemTime::now()
                    + Duration::from_millis(SESSION_METRICS_DUMP_PERIOD_MS);
            }

            //dump profiling

            if let Ok(_elapsed) = next_profiling_dump_time.elapsed() {
                instrument!();
                check_execution_time!(50_000);

                if log_enabled!(log::Level::Debug) {
                    let profiling_dump = profiling::Profiler::local_instance()
                        .with(|profiler| profiler.borrow().dump());

                    debug!(
                        "ValidatorSession {} profiling: {}",
                        &session_id.to_hex_string(),
                        profiling_dump
                    );
                }

                next_profiling_dump_time = std::time::SystemTime::now()
                    + Duration::from_millis(SESSION_PROFILING_DUMP_PERIOD_MS);
            }
        }

        //finishing routines

        info!(
            "ValidatorSession main loop is finished (session_id is {})",
            session_id.to_hex_string()
        );

        is_stopped_flag.store(true, Ordering::Release);
    }

    fn session_callbacks_loop(
        should_stop_flag: Arc<AtomicBool>,
        is_stopped_flag: Arc<AtomicBool>,
        task_queue: CallbackTaskQueuePtr,
        session_id: SessionId,
        metrics_receiver: Arc<metrics_runtime::Receiver>,
    ) {
        info!(
            "ValidatorSession session callbacks processing loop is started (session_id is {})",
            session_id.to_hex_string()
        );

        let activity_node = catchain::CatchainFactory::create_activity_node(format!(
            "ValidatorSessionCallbacks_{}",
            session_id.to_hex_string()
        ));

        //configure metrics

        let loop_counter = metrics_receiver
            .sink()
            .counter("validator_session_callbacks_loop_iterations");
        let loop_overloads_counter = metrics_receiver
            .sink()
            .counter("validator_session_callbacks_loop_overloads");

        //session callbacks processing loop

        let mut last_warn_dump_time = std::time::SystemTime::now();

        loop {
            activity_node.tick();
            loop_counter.increment();

            //check if the loop should be stopped

            if should_stop_flag.load(Ordering::Relaxed) {
                break;
            }

            //check overload flag

            if task_queue.is_overloaded() {
                loop_overloads_counter.increment();
            }

            //handle session outgoing event with timeout

            const MAX_TIMEOUT: Duration = Duration::from_secs(1);

            let task = task_queue.pull_closure(MAX_TIMEOUT, &mut last_warn_dump_time);

            if let Some(task) = task {
                check_execution_time!(100_000);

                task();
            }
        }

        //finishing routines

        info!(
            "ValidatorSession session callbacks processing loop is finished (session_id is {})",
            session_id.to_hex_string()
        );

        is_stopped_flag.store(true, Ordering::Release);
    }

    /*
        Creation & stop
    */

    pub(crate) fn create_task_queue(
        name: String,
        linked_queue: Option<TaskQueuePtr>,
        metrics_receiver: Arc<metrics_runtime::Receiver>,
    ) -> TaskQueuePtr {
        type ChannelPair = (
            crossbeam::channel::Sender<Box<TaskDesc<TaskPtr>>>,
            crossbeam::channel::Receiver<Box<TaskDesc<TaskPtr>>>,
        );

        let (queue_sender, queue_receiver): ChannelPair = crossbeam::crossbeam_channel::unbounded();
        let task_queue: TaskQueuePtr = Arc::new(TaskQueueImpl::<TaskPtr>::new(
            name,
            queue_sender,
            queue_receiver,
            linked_queue,
            metrics_receiver,
        ));

        task_queue
    }

    pub(crate) fn create_callback_task_queue(
        metrics_receiver: Arc<metrics_runtime::Receiver>,
    ) -> CallbackTaskQueuePtr {
        type ChannelPair = (
            crossbeam::channel::Sender<Box<TaskDesc<CallbackTaskPtr>>>,
            crossbeam::channel::Receiver<Box<TaskDesc<CallbackTaskPtr>>>,
        );

        let (queue_sender, queue_receiver): ChannelPair = crossbeam::crossbeam_channel::unbounded();
        let task_queue: CallbackTaskQueuePtr = Arc::new(TaskQueueImpl::<CallbackTaskPtr>::new(
            "callbacks".to_string(),
            queue_sender,
            queue_receiver,
            None,
            metrics_receiver,
        ));

        task_queue
    }

    pub(crate) fn create(
        options: &SessionOptions,
        session_id: &SessionId,
        ids: &Vec<SessionNode>,
        local_key: &PrivateKey,
        path: String,
        db_suffix: String,
        allow_unsafe_self_blocks_resync: bool,
        overlay_manager: CatchainOverlayManagerPtr,
        listener: SessionListenerPtr,
    ) -> SessionPtr {
        debug!("Creating ValidatorSession...");

        let session_creation_time = std::time::SystemTime::now();

        //remove fraction part from all timeouts

        let mut options = options.clone();

        options.catchain_idle_timeout =
            Duration::from_secs(options.catchain_idle_timeout.as_secs());
        options.round_attempt_duration =
            Duration::from_secs(options.round_attempt_duration.as_secs());
        options.next_candidate_delay = Duration::from_secs(options.next_candidate_delay.as_secs());

        //create metrics receiver

        let metrics_receiver = Arc::new(
            metrics_runtime::Receiver::builder()
                .build()
                .expect("failed to create validator session metrics receiver"),
        );

        //create task queues

        let main_task_queue =
            Self::create_task_queue("processing".to_string(), None, metrics_receiver.clone());
        let session_callbacks_task_queue =
            Self::create_callback_task_queue(metrics_receiver.clone());
        let session_callbacks_responses_task_queue = Self::create_task_queue(
            "prioritized_processing".to_string(),
            Some(main_task_queue.clone()),
            metrics_receiver.clone(),
        );

        //create catchain

        let catchain_options = catchain::Options {
            idle_timeout: options.catchain_idle_timeout,
            max_deps: options.catchain_max_deps,
            skip_processed_blocks: options.catchain_skip_processed_blocks,
            debug_disable_db: false,
        };
        let catchain_nodes = ids
            .iter()
            .map(|source| CatchainNode {
                public_key: source.public_key.clone(),
                adnl_id: source.adnl_id.clone(),
            })
            .collect();
        let catchain_listener = CatchainListenerImpl::create(main_task_queue.clone());
        let catchain = catchain::CatchainFactory::create_catchain(
            &catchain_options,
            &session_id.clone(),
            &catchain_nodes,
            &local_key,
            path.to_string(),
            db_suffix,
            allow_unsafe_self_blocks_resync,
            overlay_manager,
            Arc::downgrade(&catchain_listener.clone()),
        );

        //create threads

        let stop_flag = Arc::new(AtomicBool::new(false));
        let main_processing_thread_stopped = Arc::new(AtomicBool::new(false));
        let session_callbacks_processing_thread_stopped = Arc::new(AtomicBool::new(false));
        let session_activity_node = catchain::CatchainFactory::create_activity_node(format!(
            "ValidatorSession_{}",
            session_id.to_hex_string()
        ));
        let body: SessionImpl = SessionImpl {
            stop_flag: stop_flag.clone(),
            main_processing_thread_stopped: main_processing_thread_stopped.clone(),
            session_callbacks_processing_thread_stopped:
                session_callbacks_processing_thread_stopped.clone(),
            _main_task_queue: main_task_queue.clone(),
            _session_callbacks_responses_task_queue: session_callbacks_responses_task_queue.clone(),
            _session_callbacks_task_queue: session_callbacks_task_queue.clone(),
            catchain: catchain.clone(),
            session_id: session_id.clone(),
            _catchain_listener: catchain_listener,
            _activity_node: session_activity_node.clone(),
        };

        let session = Arc::new(body);
        let stop_flag_for_main_loop = stop_flag.clone();
        let stop_flag_for_callbacks_loop = stop_flag.clone();
        let session_callbacks_task_queue_for_main_loop = session_callbacks_task_queue.clone();
        let session_callbacks_task_queue_for_callbacks_loop = session_callbacks_task_queue.clone();
        let ids_clone = ids.clone();
        let local_key_clone = local_key.clone();
        let session_id_clone = session_id.clone();

        //create processing threads

        let metrics_receiver_clone = metrics_receiver.clone();
        let _main_processing_thread = std::thread::Builder::new()
            .name(format!(
                "{}:{}",
                MAIN_LOOP_NAME.to_string(),
                session_id.to_hex_string()
            ))
            .spawn(move || {
                SessionImpl::main_loop(
                    stop_flag_for_main_loop,
                    main_processing_thread_stopped,
                    main_task_queue,
                    session_callbacks_responses_task_queue,
                    session_callbacks_task_queue_for_main_loop,
                    options,
                    session_id_clone,
                    ids_clone,
                    local_key_clone,
                    listener,
                    catchain,
                    session_activity_node,
                    session_creation_time,
                    metrics_receiver,
                );
            });

        let session_id_clone = session_id.clone();
        let _session_callbacks_processing_thread = std::thread::Builder::new()
            .name(format!(
                "{}:{}",
                CALLBACKS_LOOP_NAME.to_string(),
                session_id.to_hex_string()
            ))
            .spawn(move || {
                SessionImpl::session_callbacks_loop(
                    stop_flag_for_callbacks_loop,
                    session_callbacks_processing_thread_stopped,
                    session_callbacks_task_queue_for_callbacks_loop,
                    session_id_clone,
                    metrics_receiver_clone,
                );
            });

        session
    }

    pub fn create_replay(
        options: &SessionOptions,
        log_replay_options: &LogReplayOptions,
        session_listener: SessionListenerPtr,
        replay_listener: SessionReplayListenerPtr,
    ) -> Result<SessionPtr> {
        let player = catchain::CatchainFactory::create_log_player(log_replay_options)?;
        let catchain_nodes = player.get_nodes();
        let weights = player.get_weights();
        let mut nodes = Vec::new();

        nodes.reserve(catchain_nodes.len());

        for i in 0..catchain_nodes.len() {
            let node = &catchain_nodes[i];
            let weight = weights[i];

            nodes.push(SessionNode {
                adnl_id: node.adnl_id.clone(),
                public_key: node.public_key.clone(),
                weight,
            });
        }

        Ok(Self::create(
            options,
            player.get_session_id(),
            &nodes,
            player.get_local_key(),
            log_replay_options.db_path.clone(),
            log_replay_options.db_suffix.clone(),
            log_replay_options.allow_unsafe_self_blocks_resync,
            player.get_overlay_manager(replay_listener),
            session_listener,
        ))
    }
}
