use std::{
    collections::{HashMap, VecDeque},
    fmt, fmt::{Display, Formatter},
    sync::Arc,
    time::Instant,
};
use std::collections::HashSet;
use crossbeam_channel::{Sender, Receiver, unbounded, TryRecvError};

use ever_crypto::KeyId;
use failure::err_msg;

use ton_api::ton::ton_node::RempMessageStatus;
use ton_block::{ShardIdent, Message};
use ton_types::{UInt256, Result};
use crate::{
    config::RempConfig,
    engine_traits::{EngineOperations, RempCoreInterface, RempDuplicateStatus},
    validator::{
        validator_utils::get_shard_by_message,
        message_cache::{RmqMessage, MessageCache},
        remp_catchain::RempCatchainStore
}};
#[cfg(feature = "telemetry")]
use adnl::telemetry::Metric;
use crate::validator::mutex_wrapper::MutexWrapper;

pub struct RempInterfaceQueues {
    message_cache: Arc<MessageCache>,
    runtime: Arc<tokio::runtime::Handle>,
    pub engine: Arc<dyn EngineOperations>,
    pub incoming_sender: Sender<Arc<RmqMessage>>,
    pub response_receiver: Receiver<(UInt256, Arc<RmqMessage>, RempMessageStatus)>
}

#[async_trait::async_trait]
pub trait RempQueue<T: Display> {
    /// Function that checks status of RempQueue.
    /// Should return Ok(Some(x)) if new message x is present in the queue
    /// (message is removed from the queue),
    /// should return Ok(None) if no new messages are waiting at the moment,
    /// should return Err(_) if some error happens (e.g., the channel disconnects).
    fn receive_message (&self) -> Result<Option<Arc<T>>>;

    /// Auxiliary function: should give info about the message's shard.
    async fn compute_shard (&self, msg: Arc<T>) -> Result<ShardIdent>;
}

pub struct RempQueueDispatcher<T: Display+Sized, Q: RempQueue<T>> {
    pending_messages: MutexWrapper<HashMap<ShardIdent,VecDeque<Arc<T>>>>,
    actual_queues: MutexWrapper<HashSet<ShardIdent>>,
    pub queue: Q,
    name: String,
    #[cfg(feature = "telemetry")]
    mutex_awaiting_metric: Arc<Metric>
}

impl<T: Display,Q: RempQueue<T>> RempQueueDispatcher<T,Q> {
    pub fn with_metric(
        name: String,
        queue: Q,
        #[cfg(feature = "telemetry")]
        mutex_awaiting_metric: Arc<Metric>
    ) -> Self {
        Self {
            pending_messages: MutexWrapper::new(HashMap::new(), format!("REMP pending msgs {}", name)),
            actual_queues: MutexWrapper::new(HashSet::new(), format!("REMP actual queues {}", name)),
            queue,
            name,
            #[cfg(feature = "telemetry")]
            mutex_awaiting_metric
        }
    }

    async fn insert_to_pending_msgs(&self, msg: Arc<T>, shard: &ShardIdent) {
        self.pending_messages.execute_sync(|msgs| match msgs.get_mut(&shard) {
            Some(v) => v.push_back(msg),
            None => {
                msgs.insert(shard.clone(), VecDeque::from([msg]));
            }
        }).await
    }

    pub async fn poll(&self, shard: &ShardIdent) -> (Option<Arc<T>>, usize) {
        // TODO: what happens if we lose our right for shard XXX immediately
        // after we received the message?

        log::trace!(target: "remp", "Polling {} REMP messages for shard {}", self.name, shard);

        #[cfg(feature = "telemetry")]
        let started = Instant::now();

        //let mut pending_messages = self.pending_messages.lock().await;

        #[cfg(feature = "telemetry")]
        self.mutex_awaiting_metric.update(started.elapsed().as_micros() as u64);

        // 1. Check whether we already have the message received
        let mut result = self.pending_messages.execute_sync(|msgs| match msgs.get_mut(shard) {
            Some(queue) => {
                log::trace!(target: "remp",
                    "Taking {} REMP message for shard {} from {} pending_messages",
                    self.name, shard, queue.len()
                );
                queue.pop_front()
            },
            None => None
        }).await;

        // 2. Check queues if the message is not received yet
        while result.is_none() {
            if let Ok(Some(msg)) = self.queue.receive_message() {
                // TODO: recalculate shard id each time?
                let msg_shard = match self.queue.compute_shard(msg.clone()).await {
                    Ok(x) => x,
                    Err(e) => {
                        log::error!(target: "remp", "Cannot compute shard for {}: {}", msg, e);
                        continue
                    }
                };

                if msg_shard == *shard {
                    log::trace!(target: "remp", "Received {} message for REMP: {}", self.name, msg);
                    result = Some(msg)
                }
                else if self.actual_queues.execute_sync(|aq| aq.contains(&shard)).await {
                    // It's not the requested shard - postpone the message
                    log::trace!(target: "remp", "Received {} message for REMP: {}, wrong shard {}, expected {}", 
                        self.name, msg, msg_shard, shard
                    );
                    self.insert_to_pending_msgs(msg.clone(), &msg_shard).await
                }
            }
            else { break; }
        }

        log::trace!(target: "remp", "{} message for REMP fetched from the queue: {}", self.name,
            match &result {
                None => "None".to_string(),
                Some(x) => format!("{}",x)
            }
        );
        (result, self.pending_messages.execute_sync(|msgs| msgs.len()).await)
    }

    pub async fn return_back(&self, msg: Arc<T>, shard: &ShardIdent) {
        log::trace!(target: "remp", "REMP {}: putting message {} for shard {} back", 
            self.name, msg, shard
        );
        self.insert_to_pending_msgs(msg, shard).await
    }

    pub async fn add_actual_shard(&self, shard: &ShardIdent) {
        log::trace!(target: "remp", "REMP {}: adding actual shard {}", self.name, shard);
        self.actual_queues.execute_sync(|aq| aq.insert(shard.clone())).await;
    }

    pub async fn remove_actual_shard(&self, shard: &ShardIdent) {
        log::trace!(target: "remp", "REMP {}: removing actual shard {}", self.name, shard);
        self.actual_queues.execute_sync(|aq| aq.remove(shard)).await;
        self.pending_messages.execute_sync(|msgs| msgs.remove(shard)).await;
    }
}

pub struct RempIncomingQueue {
    engine: Arc<dyn EngineOperations>,
    pub incoming_receiver: Receiver<Arc<RmqMessage>>,
}

impl RempIncomingQueue {
    pub fn new(engine: Arc<dyn EngineOperations>, incoming_receiver: Receiver<Arc<RmqMessage>>) -> Self {
        RempIncomingQueue { engine, incoming_receiver }
    }
}

#[async_trait::async_trait]
impl RempQueue<RmqMessage> for RempIncomingQueue {
    fn receive_message(&self) -> Result<Option<Arc<RmqMessage>>> {
        match self.incoming_receiver.try_recv() {
            Ok(x) => Ok(Some(x)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(err_msg("REMP Incoming Queue is disconnected!"))
        }
    }

    async fn compute_shard(&self, msg: Arc<RmqMessage>) -> Result<ShardIdent> {
        get_shard_by_message(self.engine.clone(), msg.message.clone()).await
    }
}

pub struct CollatorInterfaceWrapper {
    engine: Arc<dyn EngineOperations>,
//  message_dispatcher: Mutex<HashMap<UInt256, ShardIdent>>
}

impl CollatorInterfaceWrapper {
    pub fn new(engine: Arc<dyn EngineOperations>) -> Self {
        CollatorInterfaceWrapper {
            engine,
//          message_dispatcher: Mutex::new(HashMap::new())
        }
    }

    pub async fn send_message_to_collator(&self, msg_id: UInt256, msg: Arc<Message>) -> Result<()> {
        self.engine.new_remp_message(msg_id.clone(), msg)
/*
        match self.engine.new_remp_message(msg_id.clone(), msg) {
            Ok(()) => {
                    let mut md = self.message_dispatcher.lock().await;
                    if let Some(s) = md.insert(msg_id, shard.clone()) {
                        log::error!(target: "remp",
                            "Message {:x} already sent to Collator for shard {}",
                            msg_id, shard
                        )
                    }
                Ok(())
            },
            Err(e) => Err(e)
        }
*/
    }
}

pub struct CollatorResult {
    pub message_id: UInt256,
    pub message: Arc<Message>,
    pub status: RempMessageStatus
}

#[async_trait::async_trait]
impl RempQueue<CollatorResult> for CollatorInterfaceWrapper {
    fn receive_message(&self) -> Result<Option<Arc<CollatorResult>>> {
        self.engine.dequeue_remp_message_status().map(|x| x.map(|(id,msg,status)|
            Arc::new(CollatorResult { message_id: id, message: msg, status } )
        ))
    }

    async fn compute_shard(&self, msg: Arc<CollatorResult>) -> Result<ShardIdent> {
        get_shard_by_message(self.engine.clone(), msg.message.clone()).await
    }
}

impl Display for CollatorResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "message_id: {:x}, status: {}", self.message_id, self.status)
    }
}

// REMP Message flow: 
// Point 0. incoming message                                     @ RempService
// Point 1. incoming message queue;                              @ RempManager
// Point 2. incoming message dispatcher (RempQueueDispatcher)    @ RempManager
// Point 3. pushing to RMQ                                       @ MessageQueue
// Point 4. fetching from RMQ -- pending messages                @ MessageQueue
// Point 5. collator queue;                                      @ MessageQueue
// Point 6. collator receipt queue -         with dispatcher     @ RempManager
// Point 7.          ... then returns back to step 5

pub struct RempManager {
    pub options: RempConfig,

    pub catchain_store: Arc<RempCatchainStore>,
    pub message_cache: Arc<MessageCache>,
    incoming_dispatcher: RempQueueDispatcher<RmqMessage, RempIncomingQueue>,
    pub collator_receipt_dispatcher: RempQueueDispatcher<CollatorResult, CollatorInterfaceWrapper>,
    pub response_sender: Sender<(UInt256, Arc<RmqMessage>, RempMessageStatus)>
}

impl RempManager {
    pub fn create_with_options(engine: Arc<dyn EngineOperations>, opt: RempConfig, runtime: Arc<tokio::runtime::Handle>) 
        -> (Self, RempInterfaceQueues) 
    {
        let (incoming_sender, incoming_receiver) = unbounded();
        let (response_sender, response_receiver) = unbounded();
        let message_cache = Arc::new(MessageCache::with_metrics(
            #[cfg(feature = "telemetry")]
            engine.remp_core_telemetry().cache_mutex_metric(),
            #[cfg(feature = "telemetry")]
            engine.remp_core_telemetry().cache_size_metric(),
        ));

        let collator_interface_wrapper = CollatorInterfaceWrapper::new(engine.clone());
        return (RempManager {
            options: opt,
            catchain_store: Arc::new(RempCatchainStore::new()),
            message_cache: message_cache.clone(),
            incoming_dispatcher: RempQueueDispatcher::with_metric(
                "incoming".to_string(),
                RempIncomingQueue::new(engine.clone(), incoming_receiver),
                #[cfg(feature = "telemetry")]
                engine.remp_core_telemetry().incoming_mutex_metric()
            ),
            collator_receipt_dispatcher: RempQueueDispatcher::with_metric(
                "collator receipt dispatcher".to_string(),
                collator_interface_wrapper,
                #[cfg(feature = "telemetry")]
                engine.remp_core_telemetry().collator_receipt_mutex_metric()
            ),
            response_sender: response_sender
        }, RempInterfaceQueues { 
            engine,
            runtime,
            message_cache: message_cache.clone(), 
            incoming_sender, 
            response_receiver 
        });
    }

    pub async fn add_active_shard(&self, shard: &ShardIdent) {
        self.incoming_dispatcher.add_actual_shard(shard).await;
        self.collator_receipt_dispatcher.add_actual_shard(shard).await;
    }

    pub async fn remove_active_shard(&self, shard: &ShardIdent) {
        self.incoming_dispatcher.remove_actual_shard(shard).await;
        self.collator_receipt_dispatcher.remove_actual_shard(shard).await;
    }

    pub async fn poll_incoming(&self, shard: &ShardIdent) -> (Option<Arc<RmqMessage>>, usize) {
        return self.incoming_dispatcher.poll(shard).await;
    }

    pub async fn return_to_incoming(&self, message: Arc<RmqMessage>, shard: &ShardIdent) {
        self.incoming_dispatcher.return_back(message, shard).await;
    }

    pub fn queue_response_to_fullnode(&self, local_key_id: UInt256, rmq_message: Arc<RmqMessage>, status: RempMessageStatus) -> Result<()> {
        self.response_sender.send((local_key_id, rmq_message, status))?;
        Ok(())
    }

    pub async fn gc_old_messages(&self, current_cc_seqno: u32) -> usize {
        let for_removal = self.message_cache.get_old_messages(current_cc_seqno).await;
        /*
        for (msg, _updated_status) in for_removal.iter() {
            self.message_cache.remove_message(&msg.message_id);
        }
        */
        for_removal.len()
    }
}

#[allow(dead_code)] 
impl RempInterfaceQueues {
    pub fn make_test_message(&self) -> Result<RmqMessage> {
        RmqMessage::make_test_message()
    }

    /**
     * Demo REMP Interface usage:
     * In this loop:
     * 1. random messages are generated each second, and sent to the REMP input queue
     * 2. responses after REMP processing are taken from REMP response queue and printed
     */
    pub async fn test_remp_messages_loop(&self) {
        log::info!(target: "remp", "Test REMP messages loop is started");
        loop {
            match self.make_test_message() {
                Err(e) => log::error!(target: "remp", "Cannot make test REMP message: `{}`", e),
                Ok(test_message) => {
                    if let Err(x) = self.incoming_sender.send(Arc::new(test_message)) {
                        log::error!(target: "remp", "Cannot send test REMP message to RMQ: {}",
                            x
                        );
                    }

                    while let Ok(msg) = self.response_receiver.try_recv() {
                        log::info!(target: "remp", "Received test REMP response: {:?}", msg);
                    }
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    pub async fn send_response_to_fullnode(
        &self, local_key_id: UInt256, rmq_message: Arc<RmqMessage>, status: RempMessageStatus
    ) {
        let receipt = ton_api::ton::ton_node::RempReceipt::TonNode_RempReceipt (
            ton_api::ton::ton_node::rempreceipt::RempReceipt {
                message_id: rmq_message.message_id.clone().into(),
                status: status.clone(),
                timestamp: 0,
                source_id: local_key_id.into()
            }
        );

        let (engine,runtime) = (self.engine.clone(), self.runtime.clone());
        runtime.clone().spawn(async move {
            if let Err(e) = engine.send_remp_receipt(rmq_message.source_key.clone(), receipt).await {
                log::error!(target: "remp",
                    "Cannot send {} response message {:x} to {}: {}",
                    status, rmq_message.message_id, rmq_message.source_key, e
                )
            }
            else {
                log::info!(target: "remp", "Sending {} response for message {:x} to {}",
                    status, rmq_message.message_id, rmq_message.source_idx
                )
            }
        });
    }

    pub async fn poll_responses_loop(&self) {
        loop {
            match self.response_receiver.try_recv() {
                Ok((local_key_id, msg,status)) => self.send_response_to_fullnode(local_key_id, msg, status).await,
                Err(TryRecvError::Empty) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
                Err(TryRecvError::Disconnected) => return
            }
        }
    }
}

#[async_trait::async_trait]
impl RempCoreInterface for RempInterfaceQueues {
    async fn process_incoming_message(&self, message_id: UInt256, message: Message, source: Arc<KeyId>) -> Result<()> {
        let arc_message = Arc::new(message.clone());

        // build message
        let remp_message = Arc::new(RmqMessage::new (
            arc_message,
            message_id.clone(),
            source,
            0
        )?);

        if self.message_cache.get_message(&message_id).is_some() {
            log::trace!(target: "remp",
                "Point 1. We already know about message {:x}, no forwarding to incoming queue is necessary",
                message_id
            );
        }
        else {
            log::trace!(target: "remp", "Point 1. Adding incoming message {} to incoming queue", remp_message);
            self.incoming_sender.send(remp_message)?;
            #[cfg(feature = "telemetry")]
            self.engine.remp_core_telemetry().in_channel_from_fullnode(self.incoming_sender.len());
        }
        Ok(())
    }

    fn check_remp_duplicate(&self, message_id: &UInt256) -> RempDuplicateStatus {
        log::trace!(target: "remp", "RempInterfraceQueues: checking duplicates for {:x}", message_id);
        let res = self.message_cache.check_message_duplicates(message_id);
        log::trace!(target: "remp", "RempInterfraceQueues: duplicate check for {:x} finished: {:?}", message_id, res);
        return res
    }
}
