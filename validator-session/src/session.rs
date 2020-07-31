pub use super::*;

use super::task_queue::*;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::SystemTime;

/*
    Constants
*/

const MAIN_LOOP_NAME: &str = "VS1"; //validator session main loop short name
const CALLBACKS_LOOP_NAME: &str = "VS2"; //validator session callbacks loop short name

/*
===================================================================================================
    TaskQueue
===================================================================================================
*/

struct TaskQueueImpl<FuncPtr> {
    queue_sender: crossbeam::channel::Sender<FuncPtr>, //queue sender from outer world to the ValidatorSession
    queue_receiver: crossbeam::channel::Receiver<FuncPtr>, //queue receiver from outer world to the ValidatorSession
}

/*
    Implementation for crate TaskQueue trait
*/

impl<FuncPtr> TaskQueue<FuncPtr> for TaskQueueImpl<FuncPtr>
where
    FuncPtr: Send + 'static,
{
    fn post_closure(&self, task: FuncPtr) {
        if let Err(send_error) = self.queue_sender.send(task) {
            error!("ValidatorSession method post closure error: {}", send_error);
        }
    }

    fn pull_closure(&self, timeout: std::time::Duration) -> Option<FuncPtr> {
        match self.queue_receiver.recv_timeout(timeout) {
            Ok(task) => Some(task),
            _ => None,
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
        queue_sender: crossbeam::channel::Sender<FuncPtr>,
        queue_receiver: crossbeam::channel::Receiver<FuncPtr>,
    ) -> Self {
        Self {
            queue_sender: queue_sender,
            queue_receiver: queue_receiver,
        }
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
    main_task_queue: TaskQueuePtr, //task queue for main thread tasks processing
    main_processing_thread: Option<JoinHandle<()>>, //main session processing thread
    session_callbacks_task_queue: CallbackTaskQueuePtr, //task queue for session callbacks processing
    session_callbacks_processing_thread: Option<JoinHandle<()>>, //session callbacks catchain processing thread
}

/*
    Implementation for public Session trait
*/

impl Session for SessionImpl {}

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

        self.stop();
    }
}

/*
    Implementation internals of SessionImpl
*/

impl SessionImpl {
    /*
        Main loop & session callbacks processing loop
    */

    fn main_loop(
        session: Arc<Mutex<SessionImpl>>,
        options: SessionOptions,
        session_id: SessionId,
        ids: Vec<SessionNode>,
        local_id: PublicKeyHash,
        db_root: &String,
        db_suffix: &String,
        allow_unsafe_self_blocks_resync: bool,
        overlay_creator: catchain::OverlayCreator,
        listener: SessionListenerPtr,
    ) {
        debug!("ValidatorSession main loop is started");

        //cache main loop data

        let (task_queue, callbacks_task_queue, stop_flag) = match session.lock() {
            Ok(session) => (
                session.main_task_queue.clone(),
                session.session_callbacks_task_queue.clone(),
                session.stop_flag.clone(),
            ),
            Err(err) => {
                error!("ValidatorSession main loop error: {:?}", err);
                return;
            }
        };

        let processor = SessionFactory::create_session_processor(
            options,
            session_id,
            ids,
            local_id,
            db_root,
            db_suffix,
            allow_unsafe_self_blocks_resync,
            overlay_creator,
            listener,
            task_queue.clone(),
            callbacks_task_queue.clone(),
        );

        //main loop

        loop {
            //check if the main loop should be stopped

            if stop_flag.load(Ordering::Relaxed) {
                break;
            }

            //handle session event with timeout

            let now = SystemTime::now();
            let timeout = match processor.borrow().get_next_awake_time().duration_since(now) {
                Ok(timeout) => timeout,
                Err(_err) => Duration::default(),
            };

            const MAX_TIMEOUT: Duration = Duration::from_secs(2); //such little timeout is needed to check stop_flag and thread exiting

            processor
                .borrow_mut()
                .set_next_awake_time(now + MAX_TIMEOUT);

            let task = task_queue.pull_closure(timeout);

            if let Some(task) = task {
                let start = SystemTime::now();

                task(&mut *processor.borrow_mut());

                if let Ok(duration) = start.elapsed() {
                    const WARNING_DURATION: Duration = Duration::from_millis(100);

                    if duration > WARNING_DURATION {
                        warn!(
                            "ValidatorSession main loop task time execution warning: {:?}",
                            duration
                        );
                    }
                }
            }

            //check & update session state

            processor.borrow_mut().check_all();
        }

        //finishing routines

        debug!("ValidatorSession main loop is finished");
    }

    fn session_callbacks_loop(session: Arc<Mutex<SessionImpl>>) {
        debug!("ValidatorSession session callbacks processing loop is started");

        //cache main loop data

        let (task_queue, stop_flag) = match session.lock() {
            Ok(session) => (
                session.session_callbacks_task_queue.clone(),
                session.stop_flag.clone(),
            ),
            Err(err) => {
                error!(
                    "ValidatorSession session callbacks processing loop error: {:?}",
                    err
                );
                return;
            }
        };

        //session callbacks processing loop

        loop {
            //check if the main loop should be stopped

            if stop_flag.load(Ordering::Relaxed) {
                break;
            }

            //handle session outgoing event with timeout

            const MAX_TIMEOUT: Duration = Duration::from_secs(1);

            let task = task_queue.pull_closure(MAX_TIMEOUT);

            if let Some(task) = task {
                let start = SystemTime::now();

                task();

                if let Ok(duration) = start.elapsed() {
                    const WARNING_DURATION: Duration = Duration::from_millis(100);

                    if duration > WARNING_DURATION {
                        warn!(
                            "ValidatorSession callback task time execution warning: {:?}",
                            duration
                        );
                    }
                }
            }
        }

        //finishing routines

        debug!("ValidatorSession session callbacks processing loop is finished");
    }

    /*
        Creation & stop
    */

    pub(crate) fn create_task_queue() -> TaskQueuePtr {
        type ChannelPair = (
            crossbeam::channel::Sender<TaskPtr>,
            crossbeam::channel::Receiver<TaskPtr>,
        );

        let (queue_sender, queue_receiver): ChannelPair = crossbeam::crossbeam_channel::unbounded();
        let task_queue: TaskQueuePtr =
            Arc::new(TaskQueueImpl::<TaskPtr>::new(queue_sender, queue_receiver));

        task_queue
    }

    pub(crate) fn create_callback_task_queue() -> CallbackTaskQueuePtr {
        type ChannelPair = (
            crossbeam::channel::Sender<CallbackTaskPtr>,
            crossbeam::channel::Receiver<CallbackTaskPtr>,
        );

        let (queue_sender, queue_receiver): ChannelPair = crossbeam::crossbeam_channel::unbounded();
        let task_queue: CallbackTaskQueuePtr = Arc::new(TaskQueueImpl::<CallbackTaskPtr>::new(
            queue_sender,
            queue_receiver,
        ));

        task_queue
    }

    pub(crate) fn create(
        options: &SessionOptions,
        session_id: &SessionId,
        ids: &Vec<SessionNode>,
        local_id: &PublicKeyHash,
        db_root: &String,
        db_suffix: &String,
        allow_unsafe_self_blocks_resync: bool,
        overlay_creator: catchain::OverlayCreator,
        listener: SessionListenerPtr,
    ) -> SessionPtr {
        debug!("Creating ValidatorSession...");

        let main_task_queue = Self::create_task_queue();
        let session_callbacks_task_queue = Self::create_callback_task_queue();
        let stop_flag = Arc::new(AtomicBool::new(false));
        let body: SessionImpl = SessionImpl {
            stop_flag: stop_flag.clone(),
            main_task_queue: main_task_queue.clone(),
            main_processing_thread: None,
            session_callbacks_task_queue: session_callbacks_task_queue.clone(),
            session_callbacks_processing_thread: None,
        };

        let session = Arc::new(Mutex::new(body));
        let session_clone_for_main_loop = session.clone();
        let session_clone_for_callbacks_processing_loop = session.clone();
        let session_result: SessionPtr = session.clone();
        let ids_clone = ids.clone();
        let local_id_clone = local_id.clone();
        let session_id_clone = session_id.clone();
        let options_clone = *options;
        let db_root = db_root.clone();
        let db_suffix = db_suffix.clone();

        if let Ok(mut session) = session.lock() {
            //create processing threads

            session.main_processing_thread = Some(
                std::thread::Builder::new()
                    .name(MAIN_LOOP_NAME.to_string())
                    .spawn(move || {
                        SessionImpl::main_loop(
                            session_clone_for_main_loop,
                            options_clone,
                            session_id_clone,
                            ids_clone,
                            local_id_clone,
                            &db_root,
                            &db_suffix,
                            allow_unsafe_self_blocks_resync,
                            overlay_creator,
                            listener,
                        );
                    })
                    .unwrap(),
            );

            session.session_callbacks_processing_thread = Some(
                std::thread::Builder::new()
                    .name(CALLBACKS_LOOP_NAME.to_string())
                    .spawn(move || {
                        SessionImpl::session_callbacks_loop(
                            session_clone_for_callbacks_processing_loop,
                        );
                    })
                    .unwrap(),
            );
        } else {
            unreachable!("Session::create: can't lock session object");
        }

        session_result
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
                weight: weight,
            });
        }

        Ok(Self::create(
            options,
            player.get_session_id(),
            &nodes,
            player.get_local_id(),
            &log_replay_options.db_root,
            &log_replay_options.db_suffix,
            log_replay_options.allow_unsafe_self_blocks_resync,
            player.get_overlay_creator(replay_listener),
            session_listener,
        ))
    }

    fn stop(&mut self) {
        if self.stop_flag.load(Ordering::Relaxed) {
            return;
        }

        self.stop_flag.store(true, Ordering::Release);

        debug!("...waiting for ValidatorSession main processing thread");

        if let Some(handle) = self.main_processing_thread.take() {
            handle
                .join()
                .expect("Failed to join ValidatorSession main loop thread");
        }

        debug!("...waiting for ValidatorSession session callbacks processing thread");

        if let Some(handle) = self.session_callbacks_processing_thread.take() {
            handle
                .join()
                .expect("Failed to join ValidatorSession session callbacks processing loop thread");
        }
    }
}
