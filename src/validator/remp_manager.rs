/*
* Copyright (C) 2019-2023 EverX. All Rights Reserved.
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
    config::RempConfig,
    engine_traits::{EngineOperations, RempCoreInterface, RempDuplicateStatus},
    validator::{
        message_cache::{RmqMessage, MessageCache}, mutex_wrapper::MutexWrapper,
        remp_catchain::RempCatchainStore,
        validator_utils::{get_message_uid, get_shard_by_message}
    }
};

#[cfg(feature = "telemetry")]
use adnl::telemetry::Metric;
use std::{collections::{HashMap, HashSet, VecDeque}, fmt, sync::Arc, time::Duration};
#[cfg(feature = "telemetry")]
use std::time::Instant;
use ton_api::ton::ton_node::RempMessageStatus;
use ton_block::{ShardIdent, Message};
use ton_types::{error, KeyId, Result, SliceData, UInt256};

pub struct RempInterfaceQueues {
    message_cache: Arc<MessageCache>,
    runtime: Arc<tokio::runtime::Handle>,
    pub engine: Arc<dyn EngineOperations>,
    pub incoming_sender: 
        crossbeam_channel::Sender<Arc<RmqMessage>>,
    pub response_receiver: 
        crossbeam_channel::Receiver<(UInt256, Arc<RmqMessage>, RempMessageStatus)>
}

#[async_trait::async_trait]
pub trait RempQueue<T: fmt::Display> {
    /// Function that checks status of RempQueue.
    /// Should return Ok(Some(x)) if new message x is present in the queue
    /// (message is removed from the queue),
    /// should return Ok(None) if no new messages are waiting at the moment,
    /// should return Err(_) if some error happens (e.g., the channel disconnects).
    fn receive_message (&self) -> Result<Option<Arc<T>>>;

    /// Auxiliary function: should give info about the message's shard.
    async fn compute_shard (&self, msg: Arc<T>) -> Result<ShardIdent>;
}

pub struct RempQueueDispatcher<T: fmt::Display+Sized, Q: RempQueue<T>> {
    pending_messages: MutexWrapper<HashMap<ShardIdent,VecDeque<Arc<T>>>>,
    actual_queues: MutexWrapper<HashSet<ShardIdent>>,
    pub queue: Q,
    name: String,
    #[cfg(feature = "telemetry")]
    dispatcher_queue_size_metric: Arc<Metric>,
    #[cfg(feature = "telemetry")]
    mutex_awaiting_metric: Arc<Metric>
}

impl<T: fmt::Display, Q: RempQueue<T>> RempQueueDispatcher<T,Q> {
    pub fn with_metric(
        name: String,
        queue: Q,
        #[cfg(feature = "telemetry")]
        dispatcher_queue_size_metric: Arc<Metric>,
        #[cfg(feature = "telemetry")]
        mutex_awaiting_metric: Arc<Metric>
    ) -> Self {
        Self {
            pending_messages: MutexWrapper::new(HashMap::new(), format!("REMP pending msgs {}", name)),
            actual_queues: MutexWrapper::new(HashSet::new(), format!("REMP actual queues {}", name)),
            queue,
            name,
            #[cfg(feature = "telemetry")]
            dispatcher_queue_size_metric,
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

    async fn recalculate_shard(&self, msg: Arc<T>) -> Option<ShardIdent> {
        match self.queue.compute_shard(msg.clone()).await {
            Ok(x) => Some(x),
            Err(e) => {
                log::error!(target: "remp", "Cannot compute shard for {}: {}", msg, e);
                None
            }
        }
    }

    /// The function postpone the message until it is requested by poll from proper shard
    async fn reroute_message(&self, msg: Arc<T>, msg_shard: &ShardIdent, required_shard: &ShardIdent) {
        if self.actual_queues.execute_sync(|aq| aq.contains(&msg_shard)).await {
            log::trace!(target: "remp",
                "Received {} message for REMP: {}, wrong message shard {}, required/old shard {}; postponed",
                self.name, msg, msg_shard, required_shard
            );
            self.insert_to_pending_msgs(msg.clone(), &msg_shard).await
        }
        else {
            log::warn!(target: "remp",
                "Received {} message for REMP: {}, wrong shard {}, not served by the current validator; dropping",
                self.name, msg, msg_shard
            );
        }
    }

    pub async fn poll(&self, shard: &ShardIdent) -> (Option<Arc<T>>, usize) {
        // TODO: what happens if we lose our right for shard XXX immediately
        // after we received the message?

        log::trace!(target: "remp", "Polling {} REMP messages for shard {}", self.name, shard);

        #[cfg(feature = "telemetry")]
        let started = Instant::now();

        #[cfg(feature = "telemetry")]
        self.mutex_awaiting_metric.update(started.elapsed().as_micros() as u64);

        // 1. Check whether we already have the message received
        let mut result = self.pending_messages.execute_sync(|msgs| {
            #[cfg(feature = "telemetry")]
            self.dispatcher_queue_size_metric.update(msgs.iter().map(|(_sh,qu)| qu.len() as u64).sum());

            match msgs.get_mut(shard) {
                Some(queue) => {
                    log::trace!(target: "remp",
                        "Taking {} REMP message for shard {} from {} pending_messages",
                        self.name, shard, queue.len()
                    );
                    queue.pop_front()
                },
                None => None
            }
        }).await;

        // 2. Check queues if the message is not received yet
        while result.is_none() {
            if let Ok(Some(msg)) = self.queue.receive_message() {
                if let Some(msg_shard) = self.recalculate_shard(msg.clone()).await {
                    if msg_shard == *shard {
                        log::trace!(target: "remp", "Received {} message for REMP: {}", self.name, msg);
                        result = Some(msg)
                    }
                    else {
                        self.reroute_message(msg, &msg_shard, shard).await;
                    }
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

    pub async fn reroute_messages(&self, msgs: &VecDeque<Arc<T>>, old_shard: &ShardIdent) {
        for msg in msgs.iter() {
            if let Some(msg_shard) = self.recalculate_shard(msg.clone()).await {
                self.reroute_message(msg.clone(), &msg_shard, old_shard).await;
            }
        }
    }

    pub async fn add_actual_shard(&self, shard: &ShardIdent) {
        log::trace!(target: "remp", "REMP {}: adding actual shard {}", self.name, shard);
        self.actual_queues.execute_sync(|aq| aq.insert(shard.clone())).await;
    }

    pub async fn remove_actual_shard(&self, shard: &ShardIdent) -> Option<VecDeque<Arc<T>>> {
        log::trace!(target: "remp", "REMP {}: removing actual shard {}", self.name, shard);
        self.actual_queues.execute_sync(|aq| aq.remove(shard)).await;
        self.pending_messages.execute_sync(|msgs| msgs.remove(shard)).await
    }
}

pub struct RempIncomingQueue {
    engine: Arc<dyn EngineOperations>,
    pub incoming_receiver: crossbeam_channel::Receiver<Arc<RmqMessage>>
}

impl RempIncomingQueue {
    pub fn new(
        engine: Arc<dyn EngineOperations>, 
        incoming_receiver: crossbeam_channel::Receiver<Arc<RmqMessage>>
    ) -> Self {
        RempIncomingQueue { engine, incoming_receiver }
    }
}

#[async_trait::async_trait]
impl RempQueue<RmqMessage> for RempIncomingQueue {
    fn receive_message(&self) -> Result<Option<Arc<RmqMessage>>> {
        match self.incoming_receiver.try_recv() {
            Ok(x) => Ok(Some(x)),
            Err(crossbeam_channel::TryRecvError::Empty) => 
                Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => 
                Err(error!("REMP Incoming Queue is disconnected!"))
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

impl fmt::Display for CollatorResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    pub response_sender: crossbeam_channel::Sender<(UInt256, Arc<RmqMessage>, RempMessageStatus)>
}

impl RempManager {
    pub fn create_with_options(engine: Arc<dyn EngineOperations>, opt: RempConfig, runtime: Arc<tokio::runtime::Handle>)
        -> (Self, RempInterfaceQueues) 
    {
        let (incoming_sender, incoming_receiver) = crossbeam_channel::unbounded();
        let (response_sender, response_receiver) = crossbeam_channel::unbounded();
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
                engine.remp_core_telemetry().incoming_queue_size_metric(),
                #[cfg(feature = "telemetry")]
                engine.remp_core_telemetry().incoming_mutex_metric()
            ),
            collator_receipt_dispatcher: RempQueueDispatcher::with_metric(
                "collator receipt dispatcher".to_string(),
                collator_interface_wrapper,
                #[cfg(feature = "telemetry")]
                engine.remp_core_telemetry().collator_receipt_queue_size_metric(),
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
        let remaining_incoming_msgs = self.incoming_dispatcher.remove_actual_shard(shard).await;
        if let Some(remaining) = remaining_incoming_msgs {
            self.incoming_dispatcher.reroute_messages(&remaining, shard).await;
        }
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

    pub async fn gc_old_messages(&self, current_master_cc_seqno: u32) -> usize {
        let (total, accepted, rejected, only_status, incorrect) = self.message_cache.get_old_messages(current_master_cc_seqno).await;
        log::info!(target: "remp", "GC old messages: {} total ({} finally accepted, {} finally rejected, {} lost), {} only status in cache, {} incorrect cache state",
            total, accepted, rejected, total - accepted - rejected, only_status, incorrect
        );
        total
    }
}

#[allow(dead_code)] 
impl RempInterfaceQueues {
    pub fn make_test_message(&self) -> Result<RmqMessage> {
        RmqMessage::make_test_message(&SliceData::new_empty())
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
                        log::trace!(target: "remp", "Received test REMP response: {:?}", msg);
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
                log::trace!(target: "remp", "Sending {} response for message {:x} to {}",
                    status, rmq_message.message_id, rmq_message.source_idx
                )
            }
        });
    }

    pub async fn poll_responses_loop(&self) {
        loop {
            match self.response_receiver.try_recv() {
                Ok((local_key_id, msg,status)) => 
                    self.send_response_to_fullnode(local_key_id, msg, status).await,
                Err(crossbeam_channel::TryRecvError::Empty) => 
                    tokio::time::sleep(Duration::from_millis(10)).await,
                Err(crossbeam_channel::TryRecvError::Disconnected) => return
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
            get_message_uid(&message),
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

    fn check_remp_duplicate(&self, message_id: &UInt256) -> Result<RempDuplicateStatus> {
        log::trace!(target: "remp", "RempInterfaceQueues: checking duplicates for {:x}", message_id);
        let res = self.message_cache.check_message_duplicates(message_id);
        match &res {
            Ok(x) =>
                log::trace!(target: "remp", "RempInterfaceQueues: duplicate check for {:x} finished: {}",
                    message_id, self.message_cache.duplicate_info(x)
                ),
            Err(e) =>
                log::error!(target: "remp", "RempInterfaceQueues: duplicate check for {:x} failed: {:?}", message_id, e)
        }
        return res
    }
}
