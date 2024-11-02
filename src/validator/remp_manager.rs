/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific EVERX DEV software governing permissions and
* limitations under the License.
*/

use std::{
    fmt, fmt::{Display, Formatter},
    collections::{HashMap, HashSet, VecDeque},
    ops::RangeInclusive,
    sync::Arc,
    time::Duration
};
use std::cmp::{max, Reverse};
use std::collections::BinaryHeap;

use ever_block::{BlockIdExt, CatchainConfig, ShardIdent, UnixTime32};
use ton_api::ton::ton_node::RempMessageStatus;
use ever_block::{error, fail, KeyId, Result, SliceData, UInt256};

use crate::{
    config::RempConfig,
    engine_traits::{EngineOperations, RempCoreInterface, RempDuplicateStatus},
    validator::{
        message_cache::{RmqMessage, RempMessageOrigin, RempMessageWithOrigin, MessageCache}, mutex_wrapper::MutexWrapper,
        remp_catchain::RempCatchainStore,
        validator_utils::get_shard_by_message
    }
};

#[cfg(feature = "telemetry")]
use std::time::Instant;
use std::time::SystemTime;
#[cfg(feature = "telemetry")]
use adnl::telemetry::Metric;
use chrono::{DateTime, Utc};
use crossbeam_channel::TryRecvError;
use rand::Rng;

pub struct RempInterfaceQueues {
    message_cache: Arc<MessageCache>,
    runtime: Arc<tokio::runtime::Handle>,
    pub engine: Arc<dyn EngineOperations>,
    pub incoming_sender: 
        crossbeam_channel::Sender<Arc<RempMessageWithOrigin>>,
    pub response_receiver: 
        crossbeam_channel::Receiver<(UInt256, UInt256, Arc<RempMessageOrigin>, RempMessageStatus)>
}

type DelayHeap = (BinaryHeap<(Reverse<SystemTime>, UInt256)>, HashMap<UInt256, Arc<RempMessageWithOrigin>>);

pub struct RempDelayer {
    max_incoming_broadcast_delay_millis: u32,
    random_seed: u64,

    pub incoming_receiver: crossbeam_channel::Receiver<Arc<RempMessageWithOrigin>>,
    pub delayed_incoming_sender: crossbeam_channel::Sender<Arc<RempMessageWithOrigin>>,
    delay_heap: MutexWrapper<DelayHeap>,
}

impl RempDelayer {
    pub fn new (random_seed: u64, options: &RempConfig,
                incoming_receiver: crossbeam_channel::Receiver<Arc<RempMessageWithOrigin>>,
                delayed_incoming_sender: crossbeam_channel::Sender<Arc<RempMessageWithOrigin>>) -> Self {
        Self {
            max_incoming_broadcast_delay_millis: options.get_max_incoming_broadcast_delay_millis(),
            random_seed,
            incoming_receiver,
            delayed_incoming_sender,
            delay_heap: MutexWrapper::new((BinaryHeap::new(), HashMap::new()), "DelayerHeap".to_string()),
        }
    }

    async fn push_delayer(&self, activation_time: SystemTime, msg: Arc<RempMessageWithOrigin>) {
        let tm: DateTime<Utc> = activation_time.into();
        self.delay_heap.execute_sync(|(h,m)| {
            let message_id = msg.get_message_id();
            if !m.contains_key(message_id) {
                log::trace!(target: "remp", "Delaying REMP message {} till {}", msg, tm.format("%T"));
                h.push((Reverse(activation_time), message_id.clone()));
                m.insert(message_id.clone(), msg);
            }
            else {
                log::trace!(target: "remp", "Delayer: REMP message {} is already waiting", msg);
            }
        }).await
    }

    async fn pop_delayer(&self) -> Option<Arc<RempMessageWithOrigin>> {
        let now = SystemTime::now();
        let res = self.delay_heap.execute_sync(|(h,m)| {
            match h.peek() {
                Some((Reverse(tm), _)) if tm > &now => return None,
                None => return None,
                _ => ()
            };

            match h.pop() {
                Some((tm, id)) => m.remove(&id).map(|msg| (tm,msg)),
                None => None
            }
        }).await;

        if let Some((tm,msg)) = res {
            let tm: DateTime<Utc> = tm.0.into();
            log::trace!(target: "remp", "Resuming REMP message {}, delayed till {}", msg, tm.format("%T"));
            Some(msg)
        }
        else {
            None
        }
    }

    pub async fn max_len(&self) -> usize {
        let (h,m) = self.delay_heap.execute_sync(|(h,m)| (h.len(),m.len())).await;
        if h != m {
            log::error!(target: "remp", "Different number of entries in Delay BinaryHeap and Delay HashMap: {} != {}", h, m);
        }
        max (h,m)
    }

    async fn poll_incoming_once(&self) -> Result<Option<Arc<RempMessageWithOrigin>>> {
        if let Some(msg) = self.pop_delayer().await {
            return Ok(Some(msg))
        }

        loop {
            match self.incoming_receiver.try_recv() {
                Ok(msg) => {
                    if msg.has_no_source_key() && self.max_incoming_broadcast_delay_millis > 0 {
                        let delay = (msg.get_message_id().first_u64() + self.random_seed) % (self.max_incoming_broadcast_delay_millis as u64);
                        self.push_delayer(SystemTime::now() + Duration::from_millis(delay), msg).await;
                    }
                    else {
                        // self.push_delayer(SystemTime::now() + Duration::from_millis(100), msg).await; // TODO: debugging code
                        return Ok(Some(msg));
                    }
                }
                Err(TryRecvError::Empty) => return Ok(None),
                Err(TryRecvError::Disconnected) => fail!("REMP incoming queue disconnected")
            }
        }
    }

    pub async fn poll_incoming(&self) -> Result<usize> {
        let mut cnt = 0;
        while let Some(msg) = self.poll_incoming_once().await? {
            cnt += 1;
            self.delayed_incoming_sender.send(msg)?;
        }
        log::debug!(target: "remp", "Polling incoming REMP messages delayer: {} forwarded, {} waiting", cnt, self.max_len().await);
        Ok(cnt)
    }
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
        self.pending_messages.execute_sync(|msgs| match msgs.get_mut(shard) {
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
        if self.actual_queues.execute_sync(|aq| aq.contains(msg_shard)).await {
            log::trace!(target: "remp",
                "Received {} message for REMP: {}, wrong message shard {}, required/old shard {}; postponed",
                self.name, msg, msg_shard, required_shard
            );
            self.insert_to_pending_msgs(msg.clone(), msg_shard).await
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
    pub incoming_receiver: crossbeam_channel::Receiver<Arc<RempMessageWithOrigin>>
}

impl RempIncomingQueue {
    pub fn new(
        engine: Arc<dyn EngineOperations>, 
        incoming_receiver: crossbeam_channel::Receiver<Arc<RempMessageWithOrigin>>
    ) -> Self {
        RempIncomingQueue { engine, incoming_receiver }
    }
}

#[async_trait::async_trait]
impl RempQueue<RempMessageWithOrigin> for RempIncomingQueue {
    fn receive_message(&self) -> Result<Option<Arc<RempMessageWithOrigin>>> {
        match self.incoming_receiver.try_recv() {
            Ok(x) => Ok(Some(x)),
            Err(crossbeam_channel::TryRecvError::Empty) => 
                Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => 
                Err(error!("REMP Incoming Queue is disconnected!"))
        }
    }

    async fn compute_shard(&self, msg: Arc<RempMessageWithOrigin>) -> Result<ShardIdent> {
        get_shard_by_message(self.engine.clone(), msg.message.message.clone()).await
    }
}

//pub struct CollatorResult {
//    pub message_id: UInt256,
//    pub message: Arc<Message>,
//    pub status: RempMessageStatus
//}
/*
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
*/
//impl fmt::Display for CollatorResult {
//    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//        write!(f, "message_id: {:x}, status: {}", self.message_id, self.status)
//    }
//}

// REMP Message flow: 
// Point 0. incoming message                                     @ RempService
// Point 1. incoming message queue;                              @ RempManager
// Point 2. incoming message dispatcher (RempQueueDispatcher)    @ RempManager
// Point 3. pushing to RMQ                                       @ MessageQueue
// Point 4. fetching from RMQ -- pending messages                @ MessageQueue
// Point 5. collator queue;                                      @ MessageQueue
// Point 6. collator receipt queue -         with dispatcher     @ RempManager
// Point 7.          ... then returns back to step 5

#[derive(Default)]
pub struct RempSessionStats {
    pub total: usize,
    pub accepted_in_session: usize,
    pub rejected_in_session: usize,
    pub has_only_header: usize,
    pub incorrect: usize
}

impl Display for RempSessionStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} total ({} finally accepted, {} finally rejected, {} lost), {} only status in cache, {} incorrect state",
               self.total, self.accepted_in_session, self.rejected_in_session,
               self.total - self.accepted_in_session - self.rejected_in_session,
               self.has_only_header,
               self.incorrect
        )
    }
}

impl RempSessionStats {
    pub fn add(&mut self, addtional: &RempSessionStats) {
        self.total += addtional.total;
        self.accepted_in_session += addtional.accepted_in_session;
        self.rejected_in_session += addtional.rejected_in_session;
        self.has_only_header += addtional.has_only_header;
        self.incorrect += addtional.incorrect;
    }
}

pub struct RempManager {
    pub options: RempConfig,

    pub catchain_store: Arc<RempCatchainStore>,
    pub message_cache: Arc<MessageCache>,
    incoming_delayer: RempDelayer,
    incoming_dispatcher: RempQueueDispatcher<RempMessageWithOrigin, RempIncomingQueue>,
    //pub collator_receipt_dispatcher: RempQueueDispatcher<CollatorResult, CollatorInterfaceWrapper>,
    pub response_sender: crossbeam_channel::Sender<(UInt256 /* local_id */, UInt256 /* message_id */, Arc<RempMessageOrigin>, RempMessageStatus)>
}

impl RempManager {
    pub fn create_with_options(engine: Arc<dyn EngineOperations>, opt: RempConfig, runtime: Arc<tokio::runtime::Handle>)
        -> (Self, RempInterfaceQueues) 
    {
        let (incoming_sender, incoming_receiver) = crossbeam_channel::unbounded();
        let (delayed_incoming_sender, delayed_incoming_receiver) = crossbeam_channel::unbounded();
        let (response_sender, response_receiver) = crossbeam_channel::unbounded();
        let message_cache = Arc::new(MessageCache::with_metrics(
            #[cfg(feature = "telemetry")]
            engine.remp_core_telemetry().cache_size_metric()
        ));

        let mut delay_random_rng = rand::thread_rng();
        let delay_random_seed: u64 = delay_random_rng.gen();
        return (RempManager {
            options: opt.clone(),
            catchain_store: Arc::new(RempCatchainStore::new()),
            message_cache: message_cache.clone(),
            incoming_delayer: RempDelayer::new(delay_random_seed, &opt, incoming_receiver, delayed_incoming_sender),
            incoming_dispatcher: RempQueueDispatcher::with_metric(
                "incoming".to_string(),
                RempIncomingQueue::new(engine.clone(), delayed_incoming_receiver),
                #[cfg(feature = "telemetry")]
                engine.remp_core_telemetry().incoming_queue_size_metric(),
                #[cfg(feature = "telemetry")]
                engine.remp_core_telemetry().incoming_mutex_metric()
            ),
            response_sender
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
        //self.collator_receipt_dispatcher.add_actual_shard(shard).await;
    }

    pub async fn remove_active_shard(&self, shard: &ShardIdent) {
        let remaining_incoming_msgs = self.incoming_dispatcher.remove_actual_shard(shard).await;
        if let Some(remaining) = remaining_incoming_msgs {
            self.incoming_dispatcher.reroute_messages(&remaining, shard).await;
        }
        //self.collator_receipt_dispatcher.remove_actual_shard(shard).await;
    }

    pub async fn poll_incoming(&self, shard: &ShardIdent) -> (Option<Arc<RempMessageWithOrigin>>, usize) {
        match self.incoming_delayer.poll_incoming().await {
            Ok(n) => {
                log::trace!(target: "remp", "Polling REMP incoming delayer queue: {} messages processed", n);
            },
            Err(e) => {
                log::error!(target: "remp", "Cannot poll REMP incoming delayer queue: {}", e);
            }
        }
        self.incoming_dispatcher.poll(shard).await
    }

    pub async fn return_to_incoming(&self, message: Arc<RempMessageWithOrigin>, shard: &ShardIdent) {
        self.incoming_dispatcher.return_back(message, shard).await;
    }

    pub fn queue_response_to_fullnode(&self, local_key_id: UInt256, message_id: UInt256, origin: Arc<RempMessageOrigin>, status: RempMessageStatus) -> Result<()> {
        self.response_sender.send((local_key_id, message_id, origin, status))?;
        Ok(())
    }

    /// Garbage collects all messages from message cache, which are older than master cc `actual_lwb`
    pub async fn gc_old_messages(&self, actual_lwb: u32) -> RempSessionStats {
        self.message_cache.gc_old_messages(actual_lwb).await
    }

    pub fn create_master_cc_session(&self, new_cc_seqno: u32, new_time: UnixTime32, inf_blocks: Vec<BlockIdExt>) -> Result<()> {
        self.message_cache.try_set_master_cc_start_time(new_cc_seqno, new_time, inf_blocks)
    }

    // Returns None if no reliable lwb was found
    pub fn compute_lwb_for_upb(&self, starting_lwb: u32, new_cc_seqno: u32, rp_guarantee: Duration) -> Result<Option<u32>> {
        self.message_cache.compute_lwb_for_upb(starting_lwb, new_cc_seqno, rp_guarantee)
    }

    pub fn set_master_cc_range(&self, new_range: &RangeInclusive<u32>) -> Result<()> {
        self.message_cache.set_master_cc_range(new_range)
    }

    pub fn advance_master_cc(&self, new_cc_seqno: u32, rp_guarantee: Duration) -> Result<RangeInclusive<u32>> {
        self.message_cache.update_master_cc_ranges(new_cc_seqno, rp_guarantee)
    }

    pub fn calc_rp_guarantee(&self, config: &CatchainConfig) -> Duration {
        Duration::from_secs(config.mc_catchain_lifetime as u64)
    }
}

#[allow(dead_code)] 
impl RempInterfaceQueues {
    pub fn make_test_message(&self) -> Result<RempMessageWithOrigin> {
        Ok(RempMessageWithOrigin {
            message: RmqMessage::make_test_message(&SliceData::new_empty())?,
            origin: RempMessageOrigin::create_empty()?
        })
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
                Ok(msg) => {
                    if let Err(x) = self.incoming_sender.send(Arc::new(msg)) {
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
        &self, local_key_id: UInt256, message_id: UInt256, origin: Arc<RempMessageOrigin>, status: RempMessageStatus
    ) {
        let receipt = ton_api::ton::ton_node::RempReceipt::TonNode_RempReceipt (
            ton_api::ton::ton_node::rempreceipt::RempReceipt {
                message_id: message_id.clone(),
                status: status.clone(),
                timestamp: 0,
                source_id: local_key_id
            }
        );

        let (engine,runtime) = (self.engine.clone(), self.runtime.clone());
        runtime.clone().spawn(async move {
            if let Err(e) = engine.send_remp_receipt(origin.source_key.clone(), receipt).await {
                log::error!(target: "remp",
                    "Cannot send {} response message {:x} to {}: {}",
                    status, message_id, origin.source_key, e
                )
            }
            else {
                log::trace!(target: "remp", "Sending {} response for message {:x} to {}",
                    status, message_id, origin.source_idx
                )
            }
        });
    }

    pub async fn poll_responses_loop(&self) {
        loop {
            match self.response_receiver.try_recv() {
                Ok((local_key_id, hdr, origin, status)) => 
                    self.send_response_to_fullnode(local_key_id, hdr, origin, status).await,
                Err(crossbeam_channel::TryRecvError::Empty) => 
                    tokio::time::sleep(Duration::from_millis(1)).await,
                Err(crossbeam_channel::TryRecvError::Disconnected) => return
            }
        }
    }
}

#[async_trait::async_trait]
impl RempCoreInterface for RempInterfaceQueues {
    async fn process_incoming_message(&self, message: &ton_api::ton::ton_node::RempMessage, source: Arc<KeyId>) -> Result<()> {
        // build message
        let remp_message = RmqMessage::from_raw_message(message.message())?;
        if message.id() != &remp_message.message_id {
            fail!("Message with computed id {:x} has different id {:x} in RempMessage struct, message will be ignored",
                remp_message.message_id, message.id()
            );
        }
        if self.message_cache.is_message_fully_known(&remp_message.message_id)? {
            log::trace!(target: "remp",
                "Point 1. Message {:x} is fully known, no forwarding to incoming queue is necessary",
                remp_message.message_id
            );
        }
        else {
            let remp_message_origin = RempMessageOrigin::new (
                source,
                0
            )?;

            log::trace!(target: "remp",
                "Point 1. Adding incoming message {} to incoming queue, known message info {}",
                remp_message, self.message_cache.get_message_info(&remp_message.message_id)?
            );
            self.incoming_sender.send(Arc::new(RempMessageWithOrigin {
                message: remp_message,
                origin: remp_message_origin
            }))?;
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
        res
    }
}
