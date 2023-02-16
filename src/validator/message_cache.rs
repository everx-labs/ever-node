use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc, fmt, fmt::{Display, Formatter}, time::SystemTime};
use std::sync::atomic::{AtomicUsize, Ordering, Ordering::Relaxed};
use lockfree::map::Map;
use ever_crypto::KeyId;
use ton_api::{
    IntoBoxed,
    ton::ton_node::{
        rempmessagestatus::{RempAccepted, RempIgnored, RempDuplicate},
        RempMessageStatus, RempMessageLevel
    }
};
use ton_block::{Deserializable, Message, ShardIdent, Serializable, MsgAddressInt, MsgAddrStd, ExternalInboundMessageHeader};
use ton_types::{UInt256, Result, BuilderData, SliceData, fail, error};
use crate::validator::mutex_wrapper::MutexWrapper;
use crate::ext_messages::{is_finally_accepted, is_finally_rejected, validate_status_change};
use crate::engine_traits::RempDuplicateStatus;
#[cfg(feature = "telemetry")]
use adnl::telemetry::Metric;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RmqMessage {
    pub message: Arc<Message>,
    pub message_id: UInt256,
    pub source_key: Arc<KeyId>,
    pub source_idx: u32,
    pub timestamp: u32,
}

impl RmqMessage {
    pub fn new(message: Arc<Message>, message_id: UInt256, source_key: Arc<KeyId>, source_idx: u32) -> Result<Self> {
        return Ok(RmqMessage { message, message_id, source_key, source_idx, timestamp: Self::timestamp_now()? })
    }

    pub fn from_rmq_record(record: &ton_api::ton::ton_node::rmqrecord::RmqMessage) -> Result<Self> {
        Ok(RmqMessage {
            message: Arc::new(Message::construct_from_bytes(&record.message)?),
            message_id: record.message_id.clone(),
            source_key: KeyId::from_data(record.source_key_id.as_slice().clone()),
            source_idx: record.source_idx as u32,
            timestamp: Self::timestamp_now()?
        })
    }

    fn timestamp_now() -> Result<u32> {
        Ok(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs() as u32)
    }

    pub fn new_with_updated_source_idx(&self, source_idx: u32) -> Self {
        RmqMessage {
            message: self.message.clone(),
            message_id: self.message_id.clone(),
            source_key: self.source_key.clone(),
            source_idx,
            timestamp: self.timestamp
        }
    }

    pub fn deserialize(raw: &ton_api::ton::bytes) -> Result<ton_api::ton::ton_node::RmqRecord> {
        let rmq_record: ton_api::ton::ton_node::RmqRecord = catchain::utils::deserialize_tl_boxed_object(&raw)?;
        Ok(rmq_record)
/*
        let rmq_message = RmqMessage {
            message: Arc::new(Message::construct_from_bytes(rmq_record.message())?),
            message_id: rmq_record.message_id().clone(),
            source_key: KeyId::from_data(rmq_record.source_key_id().as_slice().clone()),
            source_idx: *rmq_record.source_idx() as u32,
            timestamp: Self::timestamp_now()?,
        };

        let rmq_message_status = match rmq_record.status() {
            RmqRecordStatus::TonNode_RmqNew => RempMessageStatus::TonNode_RempNew,
            RmqRecordStatus::TonNode_RmqAccepted(acc) =>
                RempMessageStatus::TonNode_RempAccepted(
                    ton_api::ton::ton_node::rempmessagestatus::RempAccepted{
                        level: RempMessageLevel::TonNode_RempCollator,
                        block_id: acc.block_id.clone(),
                        master_id: BlockIdExt::default()
                    }
                ),
            RmqRecordStatus::TonNode_RmqRejected(rej) => // TODO: change RmqRecordStatus
                RempMessageStatus::TonNode_RempIgnored(
                    ton_api::ton::ton_node::rempmessagestatus::RempIgnored{
                        level: RempMessageLevel::TonNode_RempCollator,
                        block_id: rej.block_id.clone()
                    }
                )
        };

        Ok((Arc::new(rmq_message), rmq_message_status))
 */
    }

    pub fn as_rmq_record(&self, masterchain_seqno: u32) -> ton_api::ton::ton_node::RmqRecord {
        ton_api::ton::ton_node::rmqrecord::RmqMessage {
            message: self.message.write_to_bytes().unwrap().into(),
            message_id: self.message_id.clone().into(),
            source_key_id: UInt256::from(self.source_key.data()),
            source_idx: self.source_idx as i32,
            masterchain_seqno: masterchain_seqno as i32
        }.into_boxed()
    }

    pub fn serialize(rmq_record: &ton_api::ton::ton_node::RmqRecord) -> Result<ton_api::ton::bytes> {
/*
        let rmq_status = match status {
            RempMessageStatus::TonNode_RempNew => ton_api::ton::ton_node::RmqRecordStatus::TonNode_RmqNew,
            RempMessageStatus::TonNode_RempAccepted(a) if a.level == RempMessageLevel::TonNode_RempCollator =>
                ton_api::ton::ton_node::RmqRecordStatus::TonNode_RmqAccepted(
                    ton_api::ton::ton_node::rmqrecordstatus::RmqAccepted {
                        block_id: a.block_id
                    }
                ),
            RempMessageStatus::TonNode_RempRejected(ref r) if r.level == RempMessageLevel::TonNode_RempCollator =>
                ton_api::ton::ton_node::RmqRecordStatus::TonNode_RmqRejected(
                    ton_api::ton::ton_node::rmqrecordstatus::RmqRejected {
                        block_id: r.block_id.clone(),
                        error: format!("{:?}", status)
                    }
                ),
            RempMessageStatus::TonNode_RempIgnored(ref r) =>
                ton_api::ton::ton_node::RmqRecordStatus::TonNode_RmqRejected(
                    ton_api::ton::ton_node::rmqrecordstatus::RmqRejected {
                        block_id: r.block_id.clone(),
                        error: format!("{:?}", status)
                    }
                ),
            _ => {
                log::error!(target: "remp",
                    "RMQ {}: impossible status {} for writing", self, status
                );
                ton_api::ton::ton_node::RmqRecordStatus::TonNode_RmqNew
            }
        };
*/
        //let rmq_record = self.as_rmq_record();
        let rmq_record_serialized = catchain::utils::serialize_tl_boxed_object!(rmq_record);
        return Ok(rmq_record_serialized)
    }

    #[allow(dead_code)]
    pub fn make_test_message() -> Result<Self> {
        let address = UInt256::rand();
        let msg = ton_block::Message::with_ext_in_header(ExternalInboundMessageHeader {
            src: Default::default(),
            dst: MsgAddressInt::AddrStd(MsgAddrStd {
                anycast: None,
                workchain_id: -1,
                address: SliceData::from(address.clone())
            }),
            import_fee: Default::default()
        });

        let mut builder = BuilderData::new();
        msg.write_to(&mut builder).unwrap();

        let mut reader: SliceData = SliceData::load_builder(builder)?;
        let mut msg = Message::default();
        msg.read_from(&mut reader).unwrap();

        let msg_cell = msg.serialize().unwrap();
        //let msg_id = UInt256::rand();
        log::trace!(target: "remp", "Account: {}, Message: {:?}, serialized: {:?}, hash code: {}",
            address.to_hex_string(),
            msg, msg_cell.data(),
            msg_cell.repr_hash().to_hex_string()
        );
        let (msg_id, msg) = (msg_cell.repr_hash(), msg);

        RmqMessage::new (Arc::new(msg), msg_id, KeyId::from_data([0; 32]), 0)
    }
}

impl Display for RmqMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "id {:x}, source {}, source_idx {}, ts {}",
               self.message_id, self.source_key, self.source_idx, self.timestamp
        )
    }
}

pub struct MessageCacheImpl {
    message_master_cc_order: BinaryHeap<(Reverse<u32>, UInt256)>,
}

struct MessageCacheMessages {
    messages: Map<UInt256, Arc<RmqMessage>>,
    message_shards: Map<UInt256, ShardIdent>,
    message_statuses: Map<UInt256, RempMessageStatus>,
    message_master_cc: Map<UInt256, u32>,
    message_count: AtomicUsize,
    message_events: Map<UInt256, Vec<SystemTime>>,

    master_cc_start_time: Map<u32, SystemTime>,

    #[cfg(feature = "telemetry")]
    cache_size_metric: Arc<Metric>,
}

impl MessageCacheMessages {
    pub fn new(
        #[cfg(feature = "telemetry")]
        cache_size_metric: Arc<Metric>
    ) -> Self {
        MessageCacheMessages {
            messages: Map::default(),
            message_shards: Map::default(),
            message_statuses: Map::default(),
            message_master_cc: Map::default(),
            message_count: AtomicUsize::new(0),
            message_events: Map::default(),

            master_cc_start_time: Map::default(),

            #[cfg(feature = "telemetry")]
            cache_size_metric
        }
    }

    pub fn all_messages_count(&self) -> usize {
        self.message_count.load(Relaxed)
    }

    fn in_messages(&self, id: &UInt256) -> bool {
        match self.messages.get(id) {
            None => false,
            Some(_) => true
        }
    }

    fn in_message_statuses(&self, id: &UInt256) -> bool {
        match self.message_statuses.get(id) {
            None => false,
            Some(_) => true
        }
    }

    fn in_message_shards(&self, id: &UInt256) -> bool {
        match self.message_shards.get(id) {
            None => false,
            Some(_) => true
        }
    }

    fn in_message_master_cc(&self, id: &UInt256) -> bool {
        match self.message_master_cc.get(id) {
            None => false,
            Some(_) => true
        }
    }

    /// There are two possible consistent message statuses:
    /// 1. Message present will all structures
    /// 2. Only message status present (for futures)
    fn is_message_consistent(&self, id: &UInt256) -> bool {
        if self.in_messages(id) {
            self.in_message_statuses(id) && self.in_message_shards(id) && self.in_message_master_cc(id)
        }
        else {
            (!self.in_message_shards(id)) && self.in_message_master_cc(id)
        }
    }

    pub fn insert_message(&self, message: Arc<RmqMessage>, shard: ShardIdent, status: RempMessageStatus, master_cc: u32) -> Result<()> {
        let message_id = message.message_id.clone();
        if self.in_messages(&message_id) || self.in_message_shards(&message_id) || self.in_message_statuses(&message_id) || self.in_message_master_cc(&message_id) {
            fail!("Inconsistent message cache contents: message {} present in cache, although should not", message_id)
        }

        self.message_count.fetch_add(1, Ordering::Relaxed);
        self.messages.insert(message_id.clone(), message.clone());
        self.message_statuses.insert(message_id.clone(), status);
        self.message_shards.insert(message_id.clone(), shard);
        self.message_master_cc.insert(message_id.clone(), master_cc);
        self.message_events.insert(message_id.clone(), Vec::new());
        Ok(())
    }

    pub fn insert_message_status(&self, message_id: &UInt256, shard: ShardIdent, status: RempMessageStatus, master_cc: u32) -> Result<()> {
        if self.in_messages(message_id) || self.in_message_shards(message_id) || self.in_message_statuses(message_id) || self.in_message_master_cc(message_id) {
            fail!("Inconsistent message cache contents: message status {} present in cache, although should not", message_id)
        }

        self.message_count.fetch_add(1, Ordering::Relaxed);
        self.message_statuses.insert(message_id.clone(), status);
        self.message_shards.insert(message_id.clone(), shard);
        self.message_master_cc.insert(message_id.clone(), master_cc);
        self.message_events.insert(message_id.clone(), Vec::new());
        Ok(())
    }

    pub fn cc_expired(old_cc_seqno: u32, new_cc_seqno: u32) -> bool {
        old_cc_seqno+2 <= new_cc_seqno
    }

    pub fn is_expired(&self, message_id: &UInt256, new_cc_seqno: u32) -> Result<bool> {
        match self.message_master_cc.get(message_id) {
            None => fail!("Message {:x} was not found: cannot check its expiration time", message_id),
            Some(old_cc_seqno) => Ok(Self::cc_expired(old_cc_seqno.1, new_cc_seqno))
        }
    }

    pub fn update_message_shard(&self, message_id: &UInt256, new_shard: ShardIdent) -> Result<()> {
        let old_shard = self.message_shards.get(message_id);
        match &old_shard {
            None => fail!("Message shard {:x} not found", message_id),
            Some(x) if x.1 == new_shard => Ok(()),
            Some(_) => {
                self.message_shards.insert(message_id.clone(), new_shard);
                Ok(())
            }
        }
    }

    pub fn update_message_status(&self, message_id: &UInt256, new_status: RempMessageStatus,
                                 info_if_insered: Option<(ShardIdent, u32)>
    ) -> Result<()> {
        let old_status = self.message_statuses.get(&message_id);
        match &old_status {
            None => {
                if let Some((shard, master_cc)) = info_if_insered {
                    log::trace!(target: "remp",
                        "Message {:x}: absent from cache, adding status {}, shard {}, master_cc {}",
                        message_id, new_status, shard, master_cc
                    );
                    self.insert_message_status(message_id, shard, new_status, master_cc)
                }
                else {
                    fail!("Message {:x} not found", message_id)
                }
            },
            Some(old_status) =>
                if !validate_status_change(&old_status.1, &new_status) {
                    fail!("Message {:x}: cannot change status from {} to {}",
                        message_id, old_status.1, new_status
                    )
                }
                else {
                    log::trace!(target: "remp",
                        "Message {:x}: changing status {} => {}",
                        message_id, old_status.1, new_status
                    );
                    self.message_statuses.insert(message_id.clone(), new_status);
                    Ok(())
                }
        }
    }

    pub fn mark_collation_attempt(&self, msg_id: &UInt256) -> Result<()> {
        let mut events = self.message_events
            .remove(msg_id)
            .ok_or_else(|| error!("mark_collation_attempt: message {:x} has no message_events field", msg_id))?
            .val().clone();

        events.push(SystemTime::now());
        match self.message_events.insert(msg_id.clone(), events) {
            None => Ok(()),
            Some(x) => {
                fail!("mark_collation_attempt: events stats {} for message {:x} are lost",
                    self.message_events_to_string(self.message_master_cc.get(msg_id).map(|cc| cc.1), &x.1), msg_id
                )
            }
        }
    }

    pub fn change_accepted_by_collator_to_ignored(&self, msg_id: &UInt256) -> Option<u32> {
        match (self.message_statuses.get(msg_id), self.messages.get(msg_id)) {
            (Some(status), Some(msg)) => {
                if let RempMessageStatus::TonNode_RempAccepted(acc) = &status.1 {
                    if acc.level == RempMessageLevel::TonNode_RempCollator {
                        let ign = RempIgnored { block_id: acc.block_id.clone(), level: acc.level.clone() };
                        self.message_statuses.insert(msg_id.clone(), RempMessageStatus::TonNode_RempIgnored(ign));
                        return Some(msg.1.timestamp)
                    }
                }
                None
            },
            (Some(_), None) | (None, Some(_)) => {
                log::error!(target: "remp",
                    "Incorrect message cache state for message {:x}: either message or status is missing", msg_id);
                None
            },
            _ => None,
        }
    }

    pub fn message_events_to_string(&self, master_cc_opt: Option<u32>, events: &Vec<SystemTime>) -> String {
        if let Some(master_cc) = master_cc_opt {
            if let Some(start_time) = self.master_cc_start_time.get(&master_cc) {
                return events.iter().map(|x| {
                    match x.duration_since(start_time.1) {
                        Ok(dd) => format!("{} ", dd.as_secs()),
                        Err(e) => format!("`{}`", e)
                    }
                }).collect()
            }
        }

        "*???: no master_cc start time*".to_string()
    }

    pub fn set_master_cc_start_time(&self, master_cc: u32, start_time: SystemTime) {
        self.master_cc_start_time.insert(master_cc, start_time);
    }
}

#[allow(dead_code)]
impl MessageCacheImpl {
    pub fn new () -> Self {
        MessageCacheImpl {
            message_master_cc_order: BinaryHeap::new(),
        }
    }

    fn insert_message(&mut self, mc: Arc<MessageCacheMessages>, message: Arc<RmqMessage>, shard: ShardIdent, status: RempMessageStatus, master_cc: u32) -> Result<()> {
        mc.insert_message(message.clone(), shard, status, master_cc)?;
        self.message_master_cc_order.push((Reverse(master_cc), message.message_id.clone()));
        Ok(())
    }

    fn insert_message_status(&mut self, mc: Arc<MessageCacheMessages>, message_id: &UInt256, shard: ShardIdent, status: RempMessageStatus, master_cc: u32) -> Result<()> {
        mc.insert_message_status(message_id, shard, status, master_cc)?;
        self.message_master_cc_order.push((Reverse(master_cc), message_id.clone()));
        Ok(())
    }

    fn remove_old_message(&mut self, mc: Arc<MessageCacheMessages>, current_cc: u32) -> Option<(UInt256, Option<Arc<RmqMessage>>, Option<RempMessageStatus>, Option<ShardIdent>, Option<u32>)> {
        if let Some((old_cc, msg_id)) = self.message_master_cc_order.pop() {
            if !MessageCacheMessages::cc_expired(old_cc.0, current_cc) {
                self.message_master_cc_order.push((old_cc, msg_id));
                return None
            }

            let msg_opt = mc.messages.remove(&msg_id).map(|x| x.1.clone());
            let status_opt = mc.message_statuses.remove(&msg_id).map(|x| x.1.clone());
            let shard_opt = mc.message_shards.remove(&msg_id).map(|x| x.1.clone());
            let master_cc_opt = mc.message_master_cc.remove(&msg_id).map(|x| x.1);
            let msg_events = mc.message_events.remove(&msg_id).map(|x| x.val().clone());

            match master_cc_opt {
                None =>
                    log::error!(target: "remp", "Removing old message: message {:x} has no master_cc", msg_id),
                Some(m_cc) if m_cc != old_cc.0 =>
                    log::error!(target: "remp",
                        "Removing old message: message {:x} master_cc is {}, but master_cc_order is {}",
                        msg_id, m_cc, old_cc.0
                    ),
                Some(_) => ()
            }

            if msg_opt.is_some() || status_opt.is_some() {
                if msg_opt.is_some() {
                    mc.message_count.fetch_sub(1, Ordering::Relaxed);
                }

                #[cfg(feature = "telemetry")]
                mc.cache_size_metric.update(mc.all_messages_count() as u64);

                log::debug!(target: "remp", "Removing old message: current master cc {}, old master cc {:?}, msg_id {:x}, final status {:?}, collation history `{}`",
                    current_cc, master_cc_opt, msg_id, status_opt,
                    msg_events.iter().map(|x| mc.message_events_to_string(Some(old_cc.0), x)).collect::<String>()
                );
                return Some((msg_id, msg_opt, status_opt, shard_opt, master_cc_opt))
            }
            else {
                log::error!(target: "remp",
                    "MessageCache inconsistent: message {:x} is scheduled for removal, but not present in message cache",
                    msg_id
                );
            }
        }

        None
    }
}

pub struct MessageCache {
    messages: Arc<MessageCacheMessages>,
    cache: Arc<MutexWrapper<MessageCacheImpl>>
}

#[allow(dead_code)]
impl MessageCache {
    pub fn cc_expired(old_cc_seqno: u32, new_cc_seqno: u32) -> bool {
        MessageCacheMessages::cc_expired(old_cc_seqno, new_cc_seqno)
    }

    pub fn all_messages_count(&self) -> usize {
        self.messages.all_messages_count()
    }

    fn do_update_message_status(&self, message_id: &UInt256, new_status: RempMessageStatus, if_absent: Option<(ShardIdent, u32)>) -> Result<()> {
        self.messages.update_message_status(message_id, new_status, if_absent)
    }

    /// Checks for duplicate message:
    /// ... if new is Shardchain Accept -- it's duplicate (except it's accept for Collator with the same block).
    /// ... if new is final Accept -- it is applied anyway
    /// Returns new message status, if it worths reporting (final statuses do not need to be reported)
    pub fn update_message_status(&self, message_id: &UInt256, new_status: RempMessageStatus) -> Result<Option<RempMessageStatus>> {
        if let RempMessageStatus::TonNode_RempAccepted(acc_new) = &new_status {
            if acc_new.level == RempMessageLevel::TonNode_RempShardchain {
                let old_status = self.messages.message_statuses.get(message_id).map(|x| x.1.clone());
                let new_status = match old_status {
                    Some(RempMessageStatus::TonNode_RempAccepted(acc_old)) if
                        acc_old.level == RempMessageLevel::TonNode_RempCollator && acc_old.block_id == acc_new.block_id
                    => new_status.clone(),
                    _ => {
                        RempMessageStatus::TonNode_RempDuplicate(
                            ton_api::ton::ton_node::rempmessagestatus::RempDuplicate {
                                block_id: acc_new.block_id.clone()
                            }
                        )
                    }
                };

                self.messages.update_message_status(message_id, new_status.clone(), None)?;
                return Ok(Some(new_status))
            }
            else if acc_new.level == RempMessageLevel::TonNode_RempMasterchain {
                self.do_update_message_status(message_id, new_status.clone(), None)?;
                return Ok(None)
            }
        }

        self.do_update_message_status(message_id, new_status.clone(), None)?;
        Ok(Some(new_status))
    }

    pub fn get_message(&self, message_id: &UInt256) -> Option<Arc<RmqMessage>> {
        self.messages.messages.get(message_id).map(|m| m.1.clone())
    }

    pub fn get_message_status(&self, message_id: &UInt256) -> Option<RempMessageStatus> {
        self.messages.message_statuses.get(message_id).map(|m| m.1.clone())
    }

    pub fn get_message_with_status(&self, message_id: &UInt256) -> Option<(Arc<RmqMessage>, RempMessageStatus)> {
        self.get_message_with_status_and_master_cc(message_id).map(|(m,s,_e)| (m,s))
    }

    pub fn get_message_with_status_and_master_cc(&self, message_id: &UInt256) -> Option<(Arc<RmqMessage>, RempMessageStatus, u32)> {
        let (msg, status, expiration) = (
            self.messages.messages.get(message_id).map(|m| m.1.clone()),
            self.messages.message_statuses.get(message_id).map(|m| m.1.clone()),
            self.messages.message_master_cc.get(message_id).map(|m| m.1.clone())
        );

        match (msg, status, expiration) {
            (None, None, None) => None, // Not-existing message
            (None, Some(_), Some(_)) => None, // Bare message info (retrieved from finalized block)
            (Some(m), Some (s), Some(e)) => Some((m,s,e)), // Full message info
            (None, s, e) => { log::error!(target: "remp", "Message {:x} has no body, status = {:?}, master_cc = {:?}", message_id, s, e); None },
            (m, None, e) => { log::error!(target: "remp", "Message {:x} has no status, body = {:?}, master_cc = {:?}", message_id, m, e); None },
            (m, s, None) => { log::error!(target: "remp", "Message {:x} has no master_cc, body = {:?}, status = {:?}", message_id, m, s); None }
        }
    }

    /// Inserts message with given status, if it is not there
    /// If we know something about message -- that's more important than anything we discover from RMQ
    /// If we do not know anything -- TODO: if >= 2/3 rejects, then 'Rejected'. Otherwise 'New'
    /// Actual -- get it as granted ("imprinting")
    pub async fn add_external_message_status(&self, message_id: &UInt256, message: Option<Arc<RmqMessage>>, shard: ShardIdent, status: RempMessageStatus, master_cc: u32) -> Result<Option<RempMessageStatus>> {
        self.cache.execute_sync(|c| {
            let old_status = self.messages.message_statuses.get(message_id);
            match old_status {
                None => {
                    match message {
                        None => c.insert_message_status(self.messages.clone(), message_id, shard, status, master_cc)?,
                        Some(message) => c.insert_message(self.messages.clone(), message, shard, status, master_cc)?
                    };
                    Ok(None)
                },
                Some(r) => {
                    let r_clone = r.1.clone();
                    self.messages.update_message_shard(&message_id, shard)?;
                    Ok(Some(r_clone))
                },
            }
        }).await
    }

    pub fn insert_masterchain_message_status(&self, message_id: &UInt256, new_status: RempMessageStatus, masterchain_seqno: u32) -> Result<()> {
        if let RempMessageStatus::TonNode_RempAccepted(acc_new) = &new_status {
            if acc_new.level == RempMessageLevel::TonNode_RempMasterchain {
                self.do_update_message_status(
                    message_id, new_status.clone(),
                    Some((acc_new.block_id.shard_id.clone(), masterchain_seqno))
                )?;
                return Ok(())
            }
        }

        fail!("insert_masterchain_message_status for message {:x}: requested {}, however can update only to accepted by masterchain level",
            message_id, new_status
        )
    }

    pub fn mark_collation_attempt(&self, message_id: &UInt256) -> Result<()> {
        self.messages.mark_collation_attempt(message_id)
    }

    pub fn check_message_duplicates(&self, message_id: &UInt256) -> RempDuplicateStatus {
        match self.get_message_status(message_id) {
            Some(RempMessageStatus::TonNode_RempAccepted(RempAccepted {level: RempMessageLevel::TonNode_RempShardchain, block_id:blk,..})) |
            Some(RempMessageStatus::TonNode_RempAccepted(RempAccepted {level: RempMessageLevel::TonNode_RempMasterchain, block_id:blk,..})) |
            Some(RempMessageStatus::TonNode_RempDuplicate(RempDuplicate {block_id:blk,..})) =>
                RempDuplicateStatus::Duplicate(blk.clone()),
            Some(_) => RempDuplicateStatus::Fresh,
            None => RempDuplicateStatus::Absent,
        }
    }

    /// Checks whether message msg_id is accepted by collator; if true, changes its status to
    /// ignored and returns its timestamp
    pub fn change_accepted_by_collator_to_ignored(&self, msg_id: &UInt256) -> Option<u32> {
        self.messages.change_accepted_by_collator_to_ignored(msg_id)
    }

    pub async fn print_all_messages(&self, count_only: bool) {
        if count_only {
            log::trace!(target: "remp", "All REMP messages count {}", self.all_messages_count());
        }
        else {
            log::trace!(target: "remp", "All REMP messages -- not allowed, count {}", self.all_messages_count());
/*
            let msgs = self.cache.execute_sync(|c|
                c.list_all_messages()
            ).await;
            let mut idx = 1;
            for (shard,msg,status) in msgs.iter() {
                log::trace!(target: "remp", "Msg {}. shard {}, msg {}, status {}", idx, shard, msg, status);
                idx = idx+1;
            }
            log::trace!(target: "remp", "All REMP messages; list over");
 */
        }
    }

    pub fn set_master_cc_start_time(&self, master_cc: u32, start_time: SystemTime) {
        self.messages.set_master_cc_start_time(master_cc, start_time);
    }

    /// Collect all old messages; return all messages stats
    /// (total messages removed, accepted messages, rejected messages, messages that have status only, messages with incorrect status)
    /// total - (accepted + rejected) = lost;
    pub async fn get_old_messages(&self, current_cc: u32) -> (usize, usize, usize, usize, usize)
    {
        log::trace!(target: "remp", "Removing old messages from message_cache: masterchain_cc: {}", current_cc);
        let mut total = 0;
        let mut accepted = 0;
        let mut rejected = 0;
        let mut only_status = 0;
        let mut incorrect = 0;

        while let Some((id, m,s,_x,_master_cc)) =
            self.cache.execute_sync(|c| c.remove_old_message(self.messages.clone(), current_cc)).await
        {
            total += 1;
            match (m,s) {
                (Some(_m), Some(s)) => {
                    if is_finally_accepted(&s) { accepted += 1 }
                    else if is_finally_rejected(&s) { rejected += 1 }
                },
                (None, Some(s)) =>
                    if is_finally_accepted(&s) { only_status += 1 },
                (m, status) => {
                    log::error!(target: "remp",
                        "Record for message {:?} is in incorrect state: msg = {:?}, status = {:?}",
                        id, m, status
                    );
                    incorrect += 1
                }
            }
        }

        (total, accepted, rejected, only_status, incorrect)
    }

    pub fn with_metrics(
        #[cfg(feature = "telemetry")]
        mutex_awaiting_metric: Arc<Metric>,
        #[cfg(feature = "telemetry")]
        cache_size_metric: Arc<Metric>
    ) -> Self {
        MessageCache {
            messages: Arc::new(MessageCacheMessages::new(
                #[cfg(feature = "telemetry")]
                cache_size_metric
            )),
            cache: Arc::new(MutexWrapper::with_metric (
                MessageCacheImpl::new(),
                "Message cache".to_string(),
                #[cfg(feature = "telemetry")]
                mutex_awaiting_metric
            )) 
        }
    }
}
