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

use std::{
    collections::{BinaryHeap, HashMap},
    fmt, fmt::Formatter,
    sync::Arc,
    time::Duration, time::SystemTime
};
use std::cmp::Reverse;
use std::ops::RangeInclusive;
use dashmap::DashMap;

use ton_api::ton::ton_node::{
    RempMessageStatus, RempMessageLevel,
    rempmessagestatus::{RempAccepted, RempIgnored, RempRejected}, RempCatchainRecord
};
use ton_block::{ShardIdent, Message, BlockIdExt, ValidatorDescr};
use ton_types::{UInt256, Result, fail};
use catchain::{PrivateKey, PublicKey};
use crate::{
    engine_traits::EngineOperations,
    ext_messages::{get_level_and_level_change, is_finally_accepted, is_finally_rejected},
    validator::{
        mutex_wrapper::MutexWrapper,
        message_cache::RmqMessage,
        remp_manager::RempManager,
        remp_block_parser::{process_block_messages_by_blockid, BlockProcessor},
        remp_catchain::{RempCatchainInfo, RempCatchainInstance},
        sessions_computing::GeneralSessionInfo,
        validator_utils::ValidatorListHash
    }
};
use failure::err_msg;
use ton_api::ton::ton_node::rempcatchainrecord::{RempCatchainMessage, RempCatchainMessageDigest};
use ton_api::ton::ton_node::RempMessageStatus::TonNode_RempRejected;
use crate::block::BlockIdExtExtention;
use crate::engine_traits::RempDuplicateStatus;

#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Clone)]
enum MessageQueueStatus { Created, Starting, Active, Stopping }
const RMQ_STOP_POLLING_INTERVAL: Duration = Duration::from_millis(50);
const RMQ_REQUEST_NEW_BLOCK_INTERVAL: Duration = Duration::from_millis(50);

struct MessageQueueImpl {
    status: MessageQueueStatus,

    /// Messages in queue: true if message is not collated/in block, but waiting for collator
    /// invocation (in collation_order), false if message is already given to collator
    pending_collation_set: HashMap<UInt256, bool>,

    /// Messages, waiting for collator invocation
    pending_collation_order: BinaryHeap<(Reverse<u32>, UInt256)>,
}

pub struct MessageQueue {
    remp_manager: Arc<RempManager>,
    engine: Arc<dyn EngineOperations>,
    catchain_info: Arc<RempCatchainInfo>,
    catchain_instance: RempCatchainInstance,
    queues: MutexWrapper<MessageQueueImpl>,
}

impl MessageQueueImpl {
    pub fn add_to_collation_queue(&mut self, message_id: &UInt256, timestamp: u32, add_if_absent: bool) -> Result<(bool,usize)> {
        match self.pending_collation_set.get_mut(message_id) {
            Some(waiting_collation) if *waiting_collation => Ok((false, self.pending_collation_set.len())),
            Some(waiting_collation) => {
                self.pending_collation_order.push((Reverse(timestamp), message_id.clone()));
                *waiting_collation = true;
                Ok((true, self.pending_collation_set.len()))
            },
            None if add_if_absent => {
                self.pending_collation_set.insert(message_id.clone(), true);
                self.pending_collation_order.push((Reverse(timestamp), message_id.clone())); // Max heap --- need earliest message
                Ok((true, self.pending_collation_set.len()))
            }
            None =>
                fail!("Message {} is not present in collation set, cannot return it to collation queue", message_id)
        }
    }

    pub fn take_first_for_collation(&mut self) -> Result<Option<(UInt256, u32)>> {
        if let Some((timestamp, id)) = self.pending_collation_order.pop() {
            match self.pending_collation_set.insert(id.clone(), false) {
                None => fail!("Message {:?}: taken from collation queue, but not found in collation set", id),
                Some(false) => fail!("Message {:?}: already removed from collation queue and given to collator", id),
                Some(true) => Ok(Some((id, timestamp.0)))
            }
        }
        else {
            Ok(None)
        }
    }

    pub fn list_pending_for_forwarding(&mut self) -> Result<Vec<UInt256>> {
        return Ok(self.pending_collation_set.keys().cloned().collect())
    }
}

impl MessageQueue {
    pub fn create(
        engine: Arc<dyn EngineOperations>,
        remp_manager: Arc<RempManager>,
        remp_catchain_info: Arc<RempCatchainInfo>,
    ) -> Result<Self> {
        let remp_catchain_instance = RempCatchainInstance::new(remp_catchain_info.clone());

        let queues = MutexWrapper::with_metric(
            MessageQueueImpl {
                status: MessageQueueStatus::Created,
                pending_collation_set: HashMap::new(),
                pending_collation_order: BinaryHeap::new(),
            },
            format!("<<RMQ {}>>", remp_catchain_instance),
            #[cfg(feature = "telemetry")]
            engine.remp_core_telemetry().rmq_catchain_mutex_metric(&remp_catchain_info.general_session_info.shard),
        );

        log::trace!(target: "remp", "Creating MessageQueue {}", remp_catchain_instance);

        return Ok(Self {
            remp_manager,
            engine,
            catchain_info: remp_catchain_info,
            catchain_instance: remp_catchain_instance,
            queues,
        });
    }

    pub async fn create_and_start (
        engine: Arc<dyn EngineOperations>, manager: Arc<RempManager>, info: Arc<RempCatchainInfo>, local_key: PrivateKey
    ) -> Result<Arc<Self>> {
        let queue = Arc::new(Self::create(engine, manager, info)?);
        queue.clone().start(local_key.clone()).await?;
        Ok(queue)
    }

    async fn set_queue_status(&self, required_status: MessageQueueStatus, new_status: MessageQueueStatus) -> Result<()> {
        self.queues.execute_sync(|q| {
            if q.status != required_status {
                fail!("RMQ {}: MessageQueue status is {:?}, but required to be {:?}", self, q.status, required_status)
            }
            else {
                q.status = new_status;
                Ok(())
            }
        }).await
    }

    pub fn send_response_to_fullnode(&self, rmq_message: Arc<RmqMessage>, status: RempMessageStatus) {
        log::debug!(target: "remp", "RMQ {}: queueing response to fullnode {}, status {}",
            self, rmq_message, status
        );

        if rmq_message.has_no_source_key() {
            log::trace!(target: "remp", "RMQ {}: message {} was broadcast and has no source key, no response", self, rmq_message)
        }
        else if let Err(e) = self.remp_manager.queue_response_to_fullnode(
            self.catchain_info.local_key_id.clone(), rmq_message.clone(), status.clone()
        ) {
            log::error!(target: "remp", "RMQ {}: cannot queue response to fullnode: {}, {}, local key {:x}, error `{}`",
                self, rmq_message, status, self.catchain_info.local_key_id, e
            );
        }
    }

    pub fn update_status_send_response(&self, msgid: &UInt256, message: Arc<RmqMessage>, new_status: RempMessageStatus) {
        match self.remp_manager.message_cache.update_message_status(&msgid, new_status.clone()) {
            Ok(Some(final_status)) => self.send_response_to_fullnode(message.clone(), final_status),
            Ok(None) => (), // Send nothing, no status update is requested
            Err(e) => log::error!(target: "remp", 
                "RMQ {}: Cannot update status for {:x}, new status {}, error {}",
                self, msgid, new_status, e
            )
        }
    }

    pub async fn update_status_send_response_by_id(&self, msgid: &UInt256, new_status: RempMessageStatus) -> Result<Arc<RmqMessage>> {
        let message = self.get_message(msgid)?;
        match &message {
            Some(rm) => {
                self.update_status_send_response(msgid, rm.clone(), new_status.clone());
                Ok(rm.clone())
            },
            None => 
                fail!(
                    "RMQ {}: cannot find message {:x} in RMQ messages hash! (new status {})",
                    self, msgid, new_status
                )
        }
    }

    pub async fn start (self: Arc<MessageQueue>, local_key: PrivateKey) -> Result<()> {
        self.set_queue_status(MessageQueueStatus::Created, MessageQueueStatus::Starting).await?;
        log::trace!(target: "remp", "RMQ {}: starting", self);
        
        let catchain_instance_res = self.remp_manager.catchain_store.start_catchain(
            self.engine.clone(), self.remp_manager.clone(), self.catchain_info.clone(), local_key
        ).await;

        match catchain_instance_res {
            Ok(catchain_instance_impl) => {
                log::trace!(target: "remp", "RMQ {}: catchain started", self);
                self.catchain_instance.init_instance(catchain_instance_impl);
                self.set_queue_status(MessageQueueStatus::Starting, MessageQueueStatus::Active).await
            },
            Err(e) => {
                log::error!(target: "remp", "RMQ {}: cannot start catchain: {}", self, e);
                self.set_queue_status(MessageQueueStatus::Starting, MessageQueueStatus::Created).await?;
                Err(e)
            }
        }
    }

    pub async fn stop(&self) -> Result<()> {
        log::trace!(target: "remp", "RMQ {}: trying to stop catchain", self);
        loop {
            let (do_stop, do_break) = self.queues.execute_sync(|q| {
                match q.status {
                    MessageQueueStatus::Created  => { q.status = MessageQueueStatus::Stopping; Ok((false, true)) },
                    MessageQueueStatus::Starting => Ok((false, false)),
                    MessageQueueStatus::Stopping => fail!("RMQ {}: cannot stop queue with 'Stopping' status", self),
                    MessageQueueStatus::Active   => { q.status = MessageQueueStatus::Stopping; Ok((true, true)) },
                }
            }).await?;

            if do_stop {
                log::trace!(target: "remp", "RMQ {}: stopping catchain", self);
                return self.remp_manager.catchain_store.stop_catchain(&self.catchain_info.queue_id).await;
            }
            if do_break {
                log::trace!(target: "remp", "RMQ {}: catchain is not started -- skip stopping (catchain_instance present: {})",
                    self, self.catchain_instance.is_session_active()
                );
                return Ok(());
            }
            log::trace!(target: "remp", "RMQ {}: waiting for catchain to stop it", self);
            tokio::time::sleep(RMQ_STOP_POLLING_INTERVAL).await
        }
    }

    pub async fn put_message_to_rmq(&self, old_message: Arc<RmqMessage>) -> Result<()> {
        if self.queues.execute_sync(|q| q.pending_collation_set.contains_key(&old_message.message_id)).await {
            log::trace!(target: "remp", "Point 3. RMQ {}; computing message {} delay --- already have it in local queue, should be skipped", self, old_message);
            return Ok(())
        }

        let msg = Arc::new(old_message.new_with_updated_source_idx(self.catchain_info.local_idx as u32));
        log::trace!(target: "remp", "Point 3. Pushing to RMQ {}; message {}", self, msg);
        self.catchain_instance.pending_messages_queue_send(msg.as_rmq_record(self.catchain_info.get_master_cc_seqno()))?;

        #[cfg(feature = "telemetry")]
        self.engine.remp_core_telemetry().in_channel_to_catchain(
            &self.catchain_info.general_session_info.shard, self.catchain_instance.pending_messages_queue_len()?);

        if let Some(session) = &self.remp_manager.catchain_store.get_catchain_session(&self.catchain_info.queue_id).await {
            log::trace!(target: "remp", "Point 3. Activating RMQ {} processing", self);
            session.request_new_block(SystemTime::now() + RMQ_REQUEST_NEW_BLOCK_INTERVAL);

            // Temporary status "New" --- message is not registered yet
            self.send_response_to_fullnode(msg, RempMessageStatus::TonNode_RempNew);
            Ok(())
        }
        else {
            log::error!(target: "remp", "RMQ {} not started", self);
            Err(failure::err_msg("RMQ is not started"))
        }
    }

    async fn add_pending_collation(&self, rmq_message: Arc<RmqMessage>, status_to_send: Option<RempMessageStatus>) -> Result<()> {
        let (added_to_queue, _len) = self.queues.execute_sync(
            |catchain| catchain.add_to_collation_queue(
                &rmq_message.message_id, rmq_message.timestamp, true
            )
        ).await?;

        #[cfg(feature = "telemetry")]
        self.engine.remp_core_telemetry().pending_collation(
            &self.catchain_info.general_session_info.shard, 
            _len
        );

        if added_to_queue {
            log::trace!(target: "remp",
                "Point 5. RMQ {}: adding message {} to collator queue", self, rmq_message
            );
            self.remp_manager.message_cache.mark_collation_attempt(&rmq_message.message_id)?;
            if let Some(status) = status_to_send {
                self.send_response_to_fullnode(rmq_message.clone(), status);
            }
        }

        Ok(())
    }

    fn forwarded_rejected_status() -> RempMessageStatus {
        let reject = RempRejected {
            level: RempMessageLevel::TonNode_RempQueue,
            block_id: Default::default(),
            error: "reject received from digest".to_string()
        };
        RempMessageStatus::TonNode_RempRejected(reject)
    }

    fn forwarded_ignored_status() -> RempMessageStatus {
        let ignored = RempIgnored {
            level: RempMessageLevel::TonNode_RempQueue,
            block_id: Default::default()
        };
        RempMessageStatus::TonNode_RempIgnored(ignored)
    }

    fn is_forwarded_status(status: &RempMessageStatus) -> bool {
        match status {
            RempMessageStatus::TonNode_RempIgnored(_) => *status == Self::forwarded_ignored_status(),
            RempMessageStatus::TonNode_RempRejected(_) => *status == Self::forwarded_rejected_status(),
            _ => false
        }
    }

    pub(crate) fn is_final_status(status: &RempMessageStatus) -> bool {
        match status {
            RempMessageStatus::TonNode_RempRejected(_) |
            RempMessageStatus::TonNode_RempTimeout |
            RempMessageStatus::TonNode_RempAccepted(RempAccepted { level: RempMessageLevel::TonNode_RempMasterchain, .. }) => true,

            _ => false
        }
    }

    fn increment_status_level(level: &RempMessageLevel) -> Option<RempMessageLevel> {
        match level {
            RempMessageLevel::TonNode_RempFullnode => Some(RempMessageLevel::TonNode_RempQueue),
            RempMessageLevel::TonNode_RempQueue => Some(RempMessageLevel::TonNode_RempCollator),
            RempMessageLevel::TonNode_RempCollator => Some(RempMessageLevel::TonNode_RempShardchain),
            RempMessageLevel::TonNode_RempShardchain => Some(RempMessageLevel::TonNode_RempMasterchain),
            RempMessageLevel::TonNode_RempMasterchain => None
        }
    }

    pub fn is_session_active(&self) -> bool {
        self.catchain_instance.is_session_active()
    }

    async fn process_pending_remp_catchain_message(&self, rmq_record_message: &RempCatchainMessage) -> Result<()> {
        let rmq_message = Arc::new(RmqMessage::from_rmq_record(rmq_record_message)?);
        let rmq_message_master_seqno = rmq_record_message.masterchain_seqno as u32;
        let forwarded = self.catchain_info.get_master_cc_seqno() > rmq_message_master_seqno;

        log::trace!(target: "remp",
            "Point 4. RMQ {}: inserting pending message {} from RMQ into message_cache, forwarded {}, message_master_cc {}",
            self, rmq_message, forwarded, rmq_message_master_seqno
        );

        let status_if_new = if let Some((_msg, status)) = self.is_queue_overloaded().await {
            status
        }
        else {
            if forwarded { Self::forwarded_ignored_status().clone() } else { RempMessageStatus::TonNode_RempNew }
        };

        // New message arrived: we consider the arrived status as 'New'/forwarded 'Ignored'
        // (Rejected messages arrive via 'Digest' messages)
        // Variants are:
        // 1a. We've never met the message -- add as 'New'/forwarded 'Ignored'.
        // 1b. We know it with positive non-final status (Duplicate/Accepted) -- replace it with normal 'Ignored'
        // 1c. We know it with some normal status -- leave in place our knowledge.
        // 1d. We know only forwarded message status -- replace it with new status.
        let added = self.remp_manager.message_cache.add_external_message_status(
            &rmq_message.message_id,
            &rmq_message.message_uid,
            Some(rmq_message.clone()),
            status_if_new,
            |old_status, new_status| {
                if Self::is_forwarded_status(old_status) {
                    return new_status.clone()
                }
                else if !Self::is_final_status(old_status) && forwarded {
                    // If the message status is non-negative (that is, not reject, timeout, etc)
                    // And the message is non final, then at least we have not rejected it.
                    // So, we specify non-forwarding Ignored status, giving us
                    // an attempt to collate it again.
                    let (lvl, lvl_chg) = get_level_and_level_change(old_status);
                    if lvl_chg >= 0 {
                        let new_level = match Self::increment_status_level(&lvl) {
                            Some(x) => x,
                            None => {
                                log::error!(target: "remp",
                                    "RMQ {}: cannot increment level {:?} for message_id {:x}",
                                    self, old_status, rmq_message.message_id
                                );
                                RempMessageLevel::TonNode_RempMasterchain
                            }
                        };
                        let ignored = RempIgnored {
                            level: new_level,
                            block_id: old_status.block_id().cloned().unwrap_or_default()
                        };
                        return RempMessageStatus::TonNode_RempIgnored(ignored)
                    }
                }
                old_status.clone()
            },
            rmq_message_master_seqno
        ).await;

        match added {
            Err(e) => {
                log::error!(target: "remp",
                            "Point 4. RMQ {}: cannot insert new message {} into message_cache, error: `{}`",
                            self, rmq_message, e
                        );
            },
            Ok((Some(_),new_status)) if Self::is_final_status(&new_status) => {
                log::trace!(target: "remp",
                            "Point 4. RMQ {}. Message {:x} master_cc_seqno {} from validator {} has final status {}, skipping",
                            self, rmq_message.message_id, rmq_message_master_seqno, rmq_message.source_idx, new_status
                        );
                #[cfg(feature = "telemetry")]
                self.engine.remp_core_telemetry().add_to_cache_attempt(false);
            }
            Ok((old_status,new_status)) => {
                log::trace!(target: "remp",
                            "Point 4. RMQ {}. Message {:x} master_cc_seqno {} from validator {} has non-final status {}{}, will be collated",
                            self, rmq_message.message_id, rmq_message_master_seqno, rmq_message.source_idx, new_status,
                            match &old_status {
                                None => format!(" (no old status)"),
                                Some(x) => format!(" (old status {})", x)
                            }
                        );
                self.add_pending_collation(rmq_message, Some(new_status)).await?;
                #[cfg(feature = "telemetry")]
                self.engine.remp_core_telemetry().add_to_cache_attempt(true);
            }
        }
        Ok(())
    }

    async fn process_pending_remp_catchain_digest(&self, reject_digest: &RempCatchainMessageDigest) -> Result<()> {
        if !self.catchain_info.master_cc_range.contains(&(reject_digest.masterchain_seqno as u32)) {
            log::error!(target: "remp",
                "Point 4. RMQ {}. Message digest (masterchain_seqno = {}, len = {}) does not fit to RMQ master cc range {}",
                self, reject_digest.masterchain_seqno, reject_digest.messages.len(), self.catchain_info.master_cc_range_info()
            )
        }
        else {
            log::info!(target: "remp",
                "Point 4. RMQ {}. Message digest (masterchain_seqno = {}, len = {}) received",
                self, reject_digest.masterchain_seqno, reject_digest.messages.len()
            );
            for message_ids in reject_digest.messages.iter() {
                self.remp_manager.message_cache.add_external_message_status(
                    &message_ids.id, &message_ids.uid,
                    None,
                    Self::forwarded_rejected_status().clone(),
                    // Forwarded reject cannot replace any status: if we know something
                    // to grant re-collation for the message, then this knowledge is
                    // more important than some particular reject opinion of some other
                    // validator.
                    |old,_new| { old.clone() },
                    reject_digest.masterchain_seqno as u32
                ).await?;
            }
        }
        Ok(())
    }

    async fn process_pending_remp_catchain_record(&self, remp_catchain_record: &RempCatchainRecord) -> Result<()> {
        match remp_catchain_record {
            RempCatchainRecord::TonNode_RempCatchainMessage(msg) =>
                self.process_pending_remp_catchain_message(msg).await,
            RempCatchainRecord::TonNode_RempCatchainMessageDigest(digest) =>
                self.process_pending_remp_catchain_digest(digest).await
        }
    }

    /// Check received messages queue and put all received messages into
    /// hash map. Check status of all old messages in the hash map.
    pub async fn poll(&self) -> Result<()> {
        log::debug!(target: "remp", "Point 4. RMQ {}: polling; total {} messages in cache, {} messages in rmq_queue",
            self, 
            self.received_messages_count().await,
            self.catchain_instance.rmq_catchain_receiver_len()?
        );

        // Process new messages from RMQ catchain and send them to collator.
        // Knowledge hierarchy:
        // 1. Our final knowledge (we received it from masterblock/we rejected it);
        // 2. Our non-final knowledge (New, Ignored, etc);
        // 3. Our temporary knowledge (Duplicate replaced with Ignored if message is still actual in the new session);
        // 4. Forwarded non-final response (other validators inform us that the message yet to be collated);
        // 5. Forwarded final response (other validators inform us that the message was rejected).
        'queue_loop: loop {
            match self.catchain_instance.rmq_catchain_try_recv() {
                Err(e) => {
                    log::error!(target: "remp", "RMQ {}: error receiving from rmq_queue: `{}`", self, e);
                    break 'queue_loop;
                },
                Ok(None) => {
                    log::trace!(target: "remp", "RMQ {}: No more messages in rmq_queue", self);
                    break 'queue_loop;
                },
                Ok(Some(record)) => self.process_pending_remp_catchain_record(&record).await?
            }
        }

        Ok(())
    }

    /// Prepare messages for collation - to be called just before collator invocation.
    pub async fn collect_messages_for_collation (&self) -> Result<()> {
        log::trace!(target: "remp", "RMQ {}: collecting messages for collation", self);
        let mut cnt = 0;
        while let Some((msgid, _timestamp)) = self.queues.execute_sync(|x| x.take_first_for_collation()).await? {
            let (status, message) = match self.remp_manager.message_cache.get_message_with_status(&msgid) {
                Err(e) => {
                    log::error!(
                        target: "remp",
                        "Point 5. RMQ {}: message {:x} found in pending_collation queue, error retriving it from messages/message_statuses: {}",
                        self, msgid, e
                    );
                    continue
                }
                Ok(None) => {
                    log::error!(
                        target: "remp",
                        "Point 5. RMQ {}: message {:x} found in pending_collation queue, but not in messages/message_statuses",
                        self, msgid
                    );
                    continue
                },
                Ok(Some((m, s))) => (s, m)
            };

            match status.clone() {
                RempMessageStatus::TonNode_RempNew
                | RempMessageStatus::TonNode_RempIgnored(_) => (),
                _ => {
                    log::trace!(target: "remp", "Point 5. RMQ {}: Skipping message {:x}, status does not allow it to collate: {}",
                        self, msgid, status
                    );
                    continue
                }
            };

            match self.remp_manager.message_cache.check_message_duplicates(&message.message_id)? {
                RempDuplicateStatus::Absent => fail!("Message {:x} is present in cache, but check_message_duplicates = Absent", &message.message_id),
                RempDuplicateStatus::Fresh(_) =>
                    log::trace!(target: "remp", "Point 5. RMQ {}: sending message {:x} to collator queue", self, message.message_id),
                d @ RempDuplicateStatus::Duplicate(_, _, _) => {
                    let duplicate_info = self.remp_manager.message_cache.duplicate_info(&d);
                    if !Self::is_final_status(&status) {
                        log::trace!(target: "remp", "Point 5. RMQ {}: rejecting message {:x}: '{}'",
                            self, message.message_id, duplicate_info
                        );
                        let rejected = RempRejected {
                            level: RempMessageLevel::TonNode_RempQueue,
                            block_id: BlockIdExt::default(),
                            error: duplicate_info
                        };
                        self.update_status_send_response(&msgid, message.clone(), RempMessageStatus::TonNode_RempRejected(rejected));
                    }
                    else {
                        log::error!(target: "remp", "Point 5. RMQ {}: message {:x} must not have a final status {:?}",
                            self, message.message_id, status
                        )
                    }
                    continue
                }
            }

            match self.send_message_to_collator(msgid.clone(), message.message.clone()).await {
                Err(e) => {
                    let error = format!("{}", e);
                    log::error!(target: "remp",
                        "Point 5. RMQ {}: error sending message {:x} to collator: `{}`; message status is unknown till collation end",
                        self, msgid, &error
                    );

                    // No new status: failure inside collator does not say
                    // anything about the message. Let's wait till collation end.
                },
                Ok(()) => {
                    let new_status = RempMessageStatus::TonNode_RempAccepted (RempAccepted {
                        level: RempMessageLevel::TonNode_RempQueue,
                        block_id: BlockIdExt::default(),
                        master_id: BlockIdExt::default()
                    });
                    self.update_status_send_response(&msgid, message.clone(), new_status);
                    cnt = cnt + 1;
                }
            }
        }
        log::trace!(target: "remp", "Point 5. RMQ {}: total {} messages for collation", self, cnt);
        Ok(())
    }

    pub async fn all_accepted_by_collator_to_ignored(&self) -> Result<Vec<UInt256>> {
        let mut downgrading = Vec::new();

        let to_check: Vec<(UInt256, bool)> = self.queues.execute_sync(|mq| {
            mq.pending_collation_set.iter().map(|(x,out_of_queue)| (x.clone(),*out_of_queue)).collect()
        }).await;

        // If message is not removed from collation queue --- and not accepted by
        // shardchain/masterchain, it may not have status "accepted by collator"
        for (c, out_of_queue) in to_check {
            match self.remp_manager.message_cache.change_accepted_by_collator_to_ignored(&c) {
                Ok(true) => {
                    downgrading.push(c.clone());
                    if !out_of_queue {
                        log::error!(target: "remp",
                            "RMQ {}: message {:x} had status 'accepted by collator', however it is out_of_queue",
                            self, c
                        );
                    }
                },
                Ok(false) => (),
                Err(e) => log::error!(target: "remp", "RMQ {}: message {:x}, cannot change accepted to ignored: {}",
                    self, c, e
                )
            }
        }

        Ok(downgrading)
    }

    /// Returns message to collation queue of the current collator.
    /// Message must alreaedy present in the queue.
    pub async fn return_to_collation_queue(&self, message_id: &UInt256) -> Result<()> {
        if let Some(message) = self.remp_manager.message_cache.get_message(message_id)? {
            self.queues.execute_sync(
                |catchain| catchain.add_to_collation_queue(message_id, message.timestamp, false)
            ).await?;
            Ok(())
        }
        else {
            fail!(
                "Point 6. RMQ {}: message {:x} is ignored by collator but absent, cannot return to collation queue",
                self, message_id
            )
        }
    }

    /// Processes one collator response from engine.deque_remp_message_status().
    /// Returns Ok(status) if the response was sucessfully processed, Ok(None) if no responses remain
    pub async fn process_one_deque_remp_message_status(&self) -> Result<Option<RempMessageStatus>> {
        let (collator_result, _pending) =
            self.remp_manager.collator_receipt_dispatcher.poll(&self.catchain_info.general_session_info.shard).await;

        match collator_result {
            Some(collator_result) => {
                log::trace!(target: "remp", "Point 6. REMP {} message {:x}, processing result {:?}",
                    self, collator_result.message_id, collator_result.status
                );
                let status = &collator_result.status;
                match status {
                    RempMessageStatus::TonNode_RempRejected(r) if r.level == RempMessageLevel::TonNode_RempCollator => {
                        self.update_status_send_response_by_id(&collator_result.message_id, status.clone()).await?;
                    },
                    RempMessageStatus::TonNode_RempAccepted(a) if a.level == RempMessageLevel::TonNode_RempCollator => {
                        self.update_status_send_response_by_id(&collator_result.message_id, status.clone()).await?;
                    },
                    RempMessageStatus::TonNode_RempIgnored(i) if i.level == RempMessageLevel::TonNode_RempCollator => {
                        // Part 2. All messages, ignored by collator itself are also a subject for re-collation
                        self.update_status_send_response_by_id(&collator_result.message_id, status.clone()).await?;
                        log::trace!(target: "remp", 
                            "Point 6. RMQ {}: message {:x} ignored by collator, returning to collation queue", 
                            self, collator_result.message_id
                        );
                        self.return_to_collation_queue(&collator_result.message_id).await?;
                    },
                    _ => fail!(
                            "Point 6. RMQ {}: unexpected collator result {} for RMQ message {:x}",
                            self, status, collator_result.message_id
                        )
                };
                Ok(Some(status.clone()))
            },
            None => Ok(None)
        }
    }

    /// Check message status after collation - to be called immediately after collator invocation.
    pub async fn process_collation_result (&self) {
        log::trace!(target: "remp", "Point 6. RMQ {}: processing collation results", self);
        let mut accepted = 0;
        let mut rejected = 0;
        let mut ignored = 0;

        loop {
            match self.process_one_deque_remp_message_status().await {
                Err(e) => {
                    log::error!(target: "remp",
                        "RMQ {}: cannot query message status, error {}",
                        self, e
                    );
                    break
                },
                Ok(None) => break,

                Ok(Some(RempMessageStatus::TonNode_RempAccepted(_))) => accepted += 1,
                Ok(Some(RempMessageStatus::TonNode_RempRejected(_))) => rejected += 1,
                Ok(Some(RempMessageStatus::TonNode_RempIgnored(_))) => ignored += 1,

                Ok(Some(s)) => {
                    log::error!(target: "remp", "Point 6. RMQ {}: unexpected status {}", self, s);
                    break
                }
            }
        }

        log::trace!(target: "remp", "Point 6. RMQ {}: total {} results processed (accepted {}, rejected {}, ignored {})", 
            self, accepted + rejected + ignored, accepted, rejected, ignored
        );
    }

    pub fn get_message(&self, id: &UInt256) -> Result<Option<Arc<RmqMessage>>> {
        self.remp_manager.message_cache.get_message(id)
    }

    async fn get_queue_len(&self) -> usize {
        self.queues.execute_sync(|q| q.pending_collation_set.len()).await
    }

    fn rejected_due_to_overload_status(error: String) -> RempMessageStatus {
        let rejected = RempRejected {
            level: RempMessageLevel::TonNode_RempQueue,
            block_id: Default::default(),
            error
        };
        TonNode_RempRejected(rejected)
    }

    fn reject_due_to_overload_message_and_status(&self, queue_len: usize, max_len: usize) -> (String, RempMessageStatus) {
        let overload_message = format!(
            "Message queue for shard {} is overloaded ({} in the queue, {} max allowed)",
            self.catchain_info.general_session_info.shard, queue_len, max_len
        );
        (overload_message.clone(), Self::rejected_due_to_overload_status(overload_message))
    }

    pub async fn is_queue_overloaded(&self) -> Option<(String, RempMessageStatus)> {
        let queue_len = self.get_queue_len().await;
        match self.remp_manager.options.get_message_queue_max_len() {
            Some(max_len) if queue_len >= max_len =>
                Some(self.reject_due_to_overload_message_and_status(queue_len, max_len)),
            _ => None
        }
    }

    pub async fn received_messages_count (&self) -> usize {
        self.queues.execute_sync(|mq| mq.pending_collation_set.len()).await
    }

    async fn send_message_to_collator(&self, message_id: UInt256, message: Arc<Message>) -> Result<()> {
        self.remp_manager.collator_receipt_dispatcher.queue.send_message_to_collator(
            message_id, message
        ).await
    }

    //pub fn pack_payload
}

impl Drop for MessageQueue {
    fn drop(&mut self) {
        log::trace!(target: "remp", "Dropping MessageQueue session {}", self);
    }
}

impl fmt::Display for MessageQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}*{:x}*{}*{}@{}*{}",
               self.catchain_info.local_idx,
               self.catchain_info.queue_id,
               self.catchain_info.general_session_info.shard,
               self.catchain_info.get_master_cc_seqno(),
               self.catchain_instance.get_id(),
               if self.catchain_instance.is_session_active() { "active" } else { "inactive" }
        )
    }
}

struct StatusUpdater {
    queue: Arc<MessageQueue>,
    new_status: RempMessageStatus
}

#[async_trait::async_trait]
impl BlockProcessor for StatusUpdater {
    async fn process_message(&self, message_id: &UInt256, _message_uid: &UInt256) {
        match self.queue.get_message(message_id) {
            Err(e) => log::error!(target: "remp", "Cannot get message {:x} from cache: {}", message_id, e),
            Ok(None) => log::warn!(target: "remp", "Cannot find message {:x} in cache", message_id),
            Ok(Some(message)) => {
                log::trace!(target: "remp", "Point 7. RMQ {} shard accepted message {}, new status {}",
                    self.queue, message, self.new_status
                );
                self.queue.update_status_send_response(message_id, message, self.new_status.clone())
            }
        }
    }
}

/** Controls message queues - actual and next */
pub struct RmqQueueManager {
    engine: Arc<dyn EngineOperations>,
    remp_manager: Arc<RempManager>,
    shard: ShardIdent,
    cur_queue: Option<Arc<MessageQueue>>,
    next_queues: DashMap<UInt256, Arc<RempCatchainInfo>>,
    local_public_key: PublicKey
}

impl RmqQueueManager {
    pub fn new(
        engine: Arc<dyn EngineOperations>,
        remp_manager: Arc<RempManager>,
        shard: ShardIdent,
        local_public_key: &PublicKey
    ) -> Self {
        // If RMQ is not enabled, current & prev queues are not created.
        let manager = RmqQueueManager {
            engine,
            remp_manager,
            shard: shard.clone(),
            cur_queue: None,
            next_queues: DashMap::new(), //MutexWrapper::new(vec!(), format!("Next queues for {}", shard.clone())),
            local_public_key: local_public_key.clone(),
            //status: MessageQueueStatus::Active
        };

        manager
    }

    pub fn set_queues(&mut self,
                      session_params: Arc<GeneralSessionInfo>, node_list_id: ValidatorListHash,
                      master_cc_range: &RangeInclusive<u32>, curr: &Vec<ValidatorDescr>, next: &Vec<ValidatorDescr>,
    ) -> Result<()> {
        if self.remp_manager.options.is_service_enabled() {
            if self.cur_queue.is_some() {
                fail!("RMQ Queue {}: attempt to re-initialize", self);
            }

            let remp_catchain_info = Arc::new(RempCatchainInfo::create(
                session_params.clone(), master_cc_range, curr, next, &self.local_public_key, node_list_id
            )?);

            self.cur_queue = Some(Arc::new(MessageQueue::create(
                self.engine.clone(), self.remp_manager.clone(), remp_catchain_info)?
            ));
        }
        Ok(())
    }

    fn get_next_queues(&self) -> Vec<Arc<RempCatchainInfo>> {
        self.next_queues.iter().map(|p| p.value().clone()).collect()
    }

    pub async fn add_new_queue(&self,
        next_master_cc_range: &RangeInclusive<u32>,
        prev_validators: &Vec<ValidatorDescr>, next_validators: &Vec<ValidatorDescr>,
        general_new_session_info: Arc<GeneralSessionInfo>,
        node_list_id: ValidatorListHash
    ) -> Result<()> {
        if !self.remp_manager.options.is_service_enabled() {
            return Ok(());
        }

        if self.cur_queue.is_none() {
            fail!("RMQ {}: cannot add new queue: cur_queue is none!", self);
        }

        //self.ensure_status(MessageQueueStatus::NewQueues)?;
        log::trace!(target: "remp", "RMQ {}: adding next queue {}", self, general_new_session_info);
        let remp_catchain_info = Arc::new(RempCatchainInfo::create(
            general_new_session_info.clone(), next_master_cc_range,
            prev_validators, next_validators, &self.local_public_key,
            node_list_id
        )?);

        if self.next_queues.contains_key(&remp_catchain_info.queue_id) {
            log::trace!(target: "remp", "RMQ {}: next queue {} is already there", self, remp_catchain_info);
            return Ok(());
        }

        let added = self.next_queues.insert(remp_catchain_info.queue_id.clone(), remp_catchain_info.clone());
        if let Some(prev_added) = added {
            if !prev_added.is_same_catchain(remp_catchain_info.clone()) {
                fail!("RMQ {}: two different next catchains old {}, new {} with same id", self, prev_added, remp_catchain_info);
            }
        }

        Ok(())
    }

    pub async fn forward_messages(&self, new_cc_range: &RangeInclusive<u32>, local_key: PrivateKey) {
        if !self.remp_manager.options.is_service_enabled() {
            return;
        }

        log::info!(target: "remp", "RMQ {}: forwarding messages to new RMQ (cc_seqno range {}..={})",
            self, new_cc_range.start(), new_cc_range.end()
        );
        let mut sent = 0;
        let mut sent_rejects = 0;

        if let Some(queue) = &self.cur_queue {
            let next_queue_infos: Vec<Arc<RempCatchainInfo>> = self.get_next_queues();
            if next_queue_infos.len() == 0 {
                log::trace!(target: "remp", "RMQ {}: no next queues to forward messages", self);
                return
            }

            let to_forward = match queue.queues.execute_sync(|x| x.list_pending_for_forwarding()).await {
                Ok(f) => f,
                Err(e) => {
                    log::error!(
                        target: "remp",
                        "RMQ {}: cannot retrieve messages for forwarding to next REMP queue, error `{}`",
                        self, e
                    );
                    return
                }
            };

            log::info!(target: "remp", "RMQ {}: {} pending messages, starting next queues", self, to_forward.len());

            let mut next_queues = Vec::new();
            for info in next_queue_infos.iter() {
                log::debug!(target: "remp", "RMQ {}: Starting {}", self, info);

                match MessageQueue::create_and_start(
                    self.engine.clone(), self.remp_manager.clone(), info.clone(), local_key.clone()
                ).await {
                    Err(e) => log::error!(
                        target: "remp",
                        "RMQ {}: cannot start next queue {}: `{}`",
                        self, queue, e
                    ),
                    Ok(queue) => next_queues.push(queue)
                }
            }

            //let mut rejected_message_digests: HashMap<u32, ton_api::ton::ton_node::rempcatchainrecord::RempCatchainMessageDigest> = HashMap::default();
            let mut rejected_message_digests: Vec<RempCatchainMessageDigest> = Vec::new();

            for msgid in to_forward.iter() {
                let (message, message_status, message_cc) = match self.remp_manager.message_cache.get_message_with_status_cc(msgid) {
                    Err(e) => {
                        log::error!(
                            target: "remp",
                            "Point 5a. RMQ {}: message {:x} found in pending_collation queue, but not in messages/message_statuses, error: {}",
                            self, msgid, e
                        );
                        continue
                    },
                    Ok(None) => {
                        log::error!(
                            target: "remp",
                            "Point 5a. RMQ {}: message {:x} found in pending_collation queue, but not in messages/message_statuses",
                            self, msgid
                        );
                        continue
                    },
                    Ok(Some((_m, status, _cc))) if status == RempMessageStatus::TonNode_RempTimeout => continue,
                    Ok(Some((_m, _status, cc))) if !new_cc_range.contains(&cc) => {
                        if *new_cc_range.end() < cc {
                            log::error!(target: "remp", "Point 5a. RMQ {}: message {:x} is younger (cc={}) than next_cc_range end {}..={} -- impossible",
                                self, msgid, cc, new_cc_range.start(), new_cc_range.end()
                            )
                        }
                        continue
                    },
                    Ok(Some((m, status, cc))) => (m, status, cc)
                };

                // Forwarding:
                // 1. Rejected messages -- as plain id (message id, message uid)
                // 2. Non-final messages -- (full RmqRecord)
                // All other messages are not forwarded

                if is_finally_rejected(&message_status) {
/*
                    if let Some(digest) = rejected_message_digests.get_mut (&message_cc) {
                        digest.messages.0.push(ton_api::ton::ton_node::rempcatchainmessageids::RempCatchainMessageIds {
                            id: message.message_id.clone(),
                            uid: message.message_uid.clone()
                        });
                    }
                    else {
                        let mut digest = ton_api::ton::ton_node::rempcatchainrecord::RempCatchainMessageDigest::default();
                        digest.masterchain_seqno = message_cc as i32;
                        digest.messages.0.push(ton_api::ton::ton_node::rempcatchainmessageids::RempCatchainMessageIds {
                            id: message.message_id.clone(),
                            uid: message.message_uid.clone()
                        });
                        rejected_message_digests.insert(message_cc, digest);
                    }
*/
                    let mut digest = RempCatchainMessageDigest::default();
                    digest.masterchain_seqno = message_cc as i32;
                    digest.messages.0.push(ton_api::ton::ton_node::rempcatchainmessageids::RempCatchainMessageIds {
                        id: message.message_id.clone(),
                        uid: message.message_uid.clone()
                    });
                    rejected_message_digests.push(digest);
                }
                else if !is_finally_accepted(&message_status) {
                    if MessageQueue::is_final_status(&message_status) {
                        log::error!(target: "remp",
                            "Point 5a. RMQ {}: message {:x} status {} is final, but not finally accepted or rejected",
                            self, msgid, message_status
                        );
                    }

                    for new in next_queues.iter() {
                        if let Err(x) = new.catchain_instance.pending_messages_queue_send(message.as_rmq_record(message_cc)) {
                            log::error!(target: "remp",
                            "Point 5a. RMQ {}: message {:x} cannot be put to new queue {}: `{}`",
                            self, msgid, new, x
                        )
                        } else {
                            sent = sent + 1;
                        }
                    }
                }
            }

            for digest in rejected_message_digests.into_iter() {
/*
                if !digest.messages.0.is_empty() {
                    let digest_len = digest.messages.0.len();
                    let msg = ton_api::ton::ton_node::RempCatchainRecord::TonNode_RempCatchainMessageDigest(digest);
                    for new in next_queues.iter() {
                        if let Err(x) = new.catchain_instance.pending_messages_queue_send(msg.clone()) {
                            log::error!(target: "remp",
                            "Point 5a. RMQ {}: message digest (len={}) cannot be put to new queue {}: `{}`",
                            self, digest_len, new, x
                        )
                        } else {
                            sent = sent + 1;
                        }
                    }
 */
                let digest_len = digest.messages.0.len();
                let msg = ton_api::ton::ton_node::RempCatchainRecord::TonNode_RempCatchainMessageDigest(digest);
                for new in next_queues.iter() {
                    if let Err(x) = new.catchain_instance.pending_messages_queue_send(msg.clone()) {
                        log::error!(target: "remp",
                            "Point 5a. RMQ {}: message digest (len={}) cannot be put to new queue {}: `{}`",
                            self, digest_len, new, x
                        )
                    } else {
                        sent = sent + 1;
                        sent_rejects = sent_rejects + 1;
                    }
                }
 /*
                else {
                    log::error!(target: "remp", "RMQ {}: rejected message digest for {} is empty, but present in cache!", self, master_cc)
                }
 */
            }

            log::info!(target: "remp", "RMQ {}: forwarding messages to new RMQ, total {}, actually sent {} (with {} rejects of them)",
                self, to_forward.len(), sent, sent_rejects
            );
        }
        else {
            log::warn!(target: "remp", "RMQ {}: cannot forward messages from non-existing queue", self);
        }
    }

    pub async fn start(&self, local_key: PrivateKey) -> Result<()> {
        if let Some(cur_queue) = &self.cur_queue {
            cur_queue.clone().start(local_key).await
        }
        else {
            log::warn!(target: "remp", "Cannot start RMQ queue for {} -- no current queue",
                self.info_string().await
            );
            Ok(())
        }
    }

    #[allow(dead_code)]
    pub async fn stop(&self) {
        log::trace!(target: "remp", "Stopping RMQ {}. First, stopping qurrent queue", self);
        if let Some(q) = &self.cur_queue {
            if let Err(e) = q.stop().await {
                log::error!(target: "remp", "Cannot stop RMQ {} current queue {}: `{}`", self, q, e);
            }
        }
        let next_queues : String = self.get_next_queues().iter().map(|c| format!("{} ", c.queue_id)).collect();
        log::trace!(target: "remp", "Stopping RMQ {} finished, next queues [{}] will be stopped by GC", self, next_queues);
    }

    pub async fn put_message_to_rmq(&self, message: Arc<RmqMessage>) -> Result<()> {
        if let Some(cur_queue) = &self.cur_queue {
            cur_queue.clone().put_message_to_rmq(message).await
        }
        else {
            fail!("Cannot put message to RMQ {}: queue is not started yet", self)
        }
    }

    pub async fn collect_messages_for_collation (&self) -> Result<()> {
        if let Some(queue) = &self.cur_queue {
            queue.collect_messages_for_collation().await?;
            Ok(())
        }
        else {
            fail!("Collecting messages for collation: RMQ {} is not started", self)
        }
    }

    pub async fn process_collation_result (&self) -> Result<()> {
        if let Some(queue) = &self.cur_queue {
            queue.process_collation_result().await;
            Ok(())
        }
        else {
            Err(err_msg(format!("Processing collation result: RMQ {} is not started", self)))
        }
    }

    pub async fn process_messages_from_committed_block(&self, id: BlockIdExt) -> Result<()> {
        if let Some(queue) = &self.cur_queue {
            let proc = Arc::new(StatusUpdater {
                queue: queue.clone(),
                new_status: RempMessageStatus::TonNode_RempAccepted(
                    if id.is_masterchain() {
                        ton_api::ton::ton_node::rempmessagestatus::RempAccepted {
                            level: RempMessageLevel::TonNode_RempMasterchain,
                            block_id: id.clone(),
                            master_id: id.clone(),
                        }
                    }
                    else {
                        ton_api::ton::ton_node::rempmessagestatus::RempAccepted {
                            level: RempMessageLevel::TonNode_RempShardchain,
                            block_id: id.clone(),
                            master_id: BlockIdExt::default(),
                        }
                    }
                )
            });
            process_block_messages_by_blockid(self.engine.clone(), &self.remp_manager.message_cache, id, proc).await?;

            // Point 7, Part 1. Collect and restart collation for all accepted by collator, but ignored in shardchain.
            let returned_msgs = queue.all_accepted_by_collator_to_ignored().await?;
            for msg_id in returned_msgs.iter() {
                log::trace!(target: "remp", "Point 7. RMQ {} returning message {:x} to collation queue", self, msg_id);
                if let Err(e) = queue.return_to_collation_queue(msg_id).await {
                    log::error!(target: "remp", "Point 7. RMQ {}: error returning message {:x} to collation queue: {}",
                        self, msg_id, e
                    )
                }
            };
            Ok(())
        }
        else {
            fail!("Applying message block {}: RMQ {} is not started", id, self)
        }
    }

    pub async fn poll(&self) {
        log::trace!(target: "remp", "Point 2. RMQ {} manager: polling incoming messages", self);
        if let Some(cur_queue) = &self.cur_queue {
            if !cur_queue.is_session_active() {
                log::warn!(target: "remp", "RMQ {} is not active yet, waiting...", self);
            }
            else if let Err(e) = cur_queue.poll().await {
                log::error!(target: "remp", "Error polling RMQ {} incoming messages: `{}`", self, e);
            }

            let mut cnt = 0;
            let mut cnt_rejected_overload = 0;
            'a: loop {
                match self.remp_manager.poll_incoming(&self.shard).await {
                    (Some(rmq_message), _) => {
                        if let Some((overload_message, status)) = cur_queue.is_queue_overloaded().await {
                            log::warn!(target: "remp", "Point 3. RMQ {}: {}, ignoring incoming message {}", self, overload_message, rmq_message);
                            cur_queue.send_response_to_fullnode(rmq_message, status);
                            cnt_rejected_overload+=1;
                        }
                        else if let Err(e) = self.put_message_to_rmq(rmq_message.clone()).await {
                            log::warn!(target: "remp", "Point 3. Error sending RMQ {} message {:?}: {}; returning back to incoming queue",
                                self, rmq_message, e
                            );
                            self.remp_manager.return_to_incoming(rmq_message, &self.shard).await;
                            break 'a;
                        }
                        else {
                            cnt = cnt+1;
                        }
                    }
                    (None, _pending) => {
                        #[cfg(feature = "telemetry")]
                        self.engine.remp_core_telemetry().pending_from_fullnode(_pending);
                        break 'a;
                    }
                }
            }

            #[cfg(feature = "telemetry")]
            {
                self.engine.remp_core_telemetry().messages_from_fullnode_for_shard(&self.shard, cnt);
                self.engine.remp_core_telemetry().rejected_overload_from_fullnode(cnt_rejected_overload);
            }

            log::trace!(target: "remp",
                "RMQ {} manager: finished polling incoming messages, {} messages processed, {} messages rejected due to overload",
                self, cnt, cnt_rejected_overload
            );
        }
        else {
            log::warn!(target: "remp", "Cannot poll RMQ {}: current queue is not defined", self.info_string().await)
        }
    }

    pub fn get_master_cc_range(&self) -> Option<RangeInclusive<u32>> {
        self.cur_queue.as_ref().map(|c| c.catchain_info.master_cc_range.clone())
    }

    pub async fn info_string(&self) -> String {
        if self.remp_manager.options.is_service_enabled() {
            format!("ReliableMessageQueue for shard {}: {}", self.shard, self)
        }
        else {
            format!("ReliableMessageQueue for shard {}: disabled", self.shard)
        }
    }

    pub fn get_sessions(&self) -> Vec<UInt256> {
        let mut sessions = Vec::new();
        if let Some(cur) = &self.cur_queue {
            sessions.push(cur.catchain_info.queue_id.clone());
        }
        for s in self.next_queues.iter() {
            sessions.push(s.key().clone());
        }
        return sessions;
    }
}

impl Drop for RmqQueueManager {
    fn drop(&mut self) {
        log::trace!(target: "remp", "RMQ session {} dropped", self);
    }
}

impl fmt::Display for RmqQueueManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let cur = if let Some(q) = &self.cur_queue { format!("{}", q) } else { format!("*none*") };
/*
        let new = if self.new_queues.len() > 0 { 
            self.new_queues.iter().map( |q| format!(" {}",q) ).collect() 
        } else { 
            format!(" *none*")
        };
        write!(f, "{}, new:{}", cur, new)
 */
        write!(f, "{}", cur)
    }
}

