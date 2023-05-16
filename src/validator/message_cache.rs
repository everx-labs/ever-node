use std::{
    cmp::Reverse, collections::BinaryHeap, sync::Arc, fmt, fmt::{Display, Formatter},
    time::SystemTime
};
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
use crate::validator::validator_utils::{get_message_uid, LockfreeMapSet};


#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RmqMessage {
    pub message: Arc<Message>,
    pub message_id: UInt256,
    pub message_uid: UInt256,
    pub source_key: Arc<KeyId>,
    pub source_idx: u32,
    pub timestamp: u32,
}

impl RmqMessage {
    pub fn new(message: Arc<Message>, message_id: UInt256, message_uid: UInt256, source_key: Arc<KeyId>, source_idx: u32) -> Result<Self> {
        return Ok(RmqMessage { message, message_id, message_uid, source_key, source_idx, timestamp: Self::timestamp_now()? })
    }

    pub fn from_rmq_record(record: &ton_api::ton::ton_node::rempcatchainrecord::RempCatchainMessage) -> Result<Self> {
        let message= Arc::new(Message::construct_from_bytes(&record.message)?);
        Ok(RmqMessage {
            message: message.clone(),
            message_id: record.message_id.clone(),
            message_uid: get_message_uid(&message),
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
            message_uid: self.message_uid.clone(),
            source_key: self.source_key.clone(),
            source_idx,
            timestamp: self.timestamp
        }
    }

    pub fn deserialize(raw: &ton_api::ton::bytes) -> Result<ton_api::ton::ton_node::RempCatchainRecord> {
        let rmq_record: ton_api::ton::ton_node::RempCatchainRecord = catchain::utils::deserialize_tl_boxed_object(&raw)?;
        Ok(rmq_record)
    }

    pub fn as_rmq_record(&self, master_cc: u32) -> ton_api::ton::ton_node::RempCatchainRecord {
        ton_api::ton::ton_node::rempcatchainrecord::RempCatchainMessage {
            message: self.message.write_to_bytes().unwrap().into(),
            message_id: self.message_id.clone().into(),
            source_key_id: UInt256::from(self.source_key.data()),
            source_idx: self.source_idx as i32,
            masterchain_seqno: master_cc as i32
        }.into_boxed()
    }

    pub fn serialize(rmq_record: &ton_api::ton::ton_node::RempCatchainRecord) -> Result<ton_api::ton::bytes> {
        let rmq_record_serialized = catchain::utils::serialize_tl_boxed_object!(rmq_record);
        return Ok(rmq_record_serialized)
    }

    #[allow(dead_code)]
    pub fn make_test_message(body: &SliceData) -> Result<Self> {
        let address = UInt256::rand();
        let msg = ton_block::Message::with_ext_in_header_and_body(ExternalInboundMessageHeader {
            src: Default::default(),
            dst: MsgAddressInt::AddrStd(MsgAddrStd {
                anycast: None,
                workchain_id: -1,
                address: SliceData::from(address.clone())
            }),
            import_fee: Default::default()
        }, body.clone());

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
        let (msg_id, msg_uid, msg) = (msg_cell.repr_hash(), get_message_uid(&msg), msg);

        RmqMessage::new (Arc::new(msg), msg_id, msg_uid,KeyId::from_data([0; 32]), 0)
    }
}

impl Display for RmqMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "id {:x}, uid {:x} source {}, source_idx {}, ts {}",
               self.message_id, self.message_uid, self.source_key, self.source_idx, self.timestamp
        )
    }
}

#[derive(Debug)]
pub struct RempMessageHeader {
    pub shard: ShardIdent,
    pub status: RempMessageStatus,
    pub master_cc: u32,

    pub message_id: UInt256,
    pub message_uid: UInt256
}

impl RempMessageHeader {
    pub fn new_arc(shard: &ShardIdent, status: RempMessageStatus, master_cc: u32, message_id: &UInt256, message_uid: &UInt256) -> Arc<Self> {
        Arc::new(RempMessageHeader { shard: shard.clone(), status, master_cc, message_id: message_id.clone(), message_uid: message_uid.clone() })
    }

    #[allow(dead_code)]
    pub fn from_rmq_record(record: &ton_api::ton::ton_node::rmqrecord::RmqMessage, message_uid: &UInt256, shard: &ShardIdent, status: &RempMessageStatus) -> Self {
        RempMessageHeader {
            shard: shard.clone(),
            status: status.clone(),
            master_cc: record.masterchain_seqno as u32,
            message_id: record.message_id.clone(),
            message_uid: message_uid.clone()
        }
    }

    pub fn new_replace_shard(&self, shard: &ShardIdent) -> Arc<Self> {
        Self::new_arc(shard, self.status.clone(), self.master_cc, &self.message_id, &self.message_uid)
    }

    pub fn new_replace_status(&self, status: RempMessageStatus) -> Arc<Self> {
        Self::new_arc(&self.shard, status, self.master_cc, &self.message_id, &self.message_uid)
    }
}

impl Display for RempMessageHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "id {}, uid {}, {}, {:?}, master_cc {}",
               self.message_id, self.message_uid, self.shard, self.status, self.master_cc
        )
    }
}

pub struct MessageCacheImpl {
    message_master_cc_order: BinaryHeap<(Reverse<u32>, UInt256)>,
}

struct MessageCacheMessages {
    messages: Map<UInt256, Arc<RmqMessage>>,
    message_headers: Map<UInt256, Arc<RempMessageHeader>>,
    message_events: Map<UInt256, Vec<SystemTime>>,
    message_by_uid: LockfreeMapSet<UInt256, UInt256>,

    message_count: AtomicUsize,

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
            message_headers: Map::default(),
            message_by_uid: LockfreeMapSet::default(),
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

    fn in_message_headers(&self, id: &UInt256) -> bool {
        match self.message_headers.get(id) {
            None => false,
            Some(_) => true
        }
    }

    /// There are two possible consistent message statuses:
    /// 1. Message present will all structures
    /// 2. Only message status present (for futures)
    fn _is_message_consistent(&self, id: &UInt256) -> bool {
        self.in_message_headers(id) || !self.in_messages(id)
    }

    fn is_message_known(&self, id: &UInt256) -> bool {
        self.in_messages(id) || self.in_message_headers(id)
    }

    pub fn insert_message(&self, message: Arc<RmqMessage>, message_header: Arc<RempMessageHeader>) -> Result<()> {
        if message.message_id != message_header.message_id {
            fail!("Inconsistent message: message {} and message_header {} have different message_id", message, message_header)
        }

        let message_id = message.message_id.clone();

        if self.is_message_known(&message_id) {
            fail!("Inconsistent message cache contents: message {} present in cache, although should not", message_id)
        }

        self.attach_id_to_uid(&message_id, &message_header.message_uid)?;
        self.message_count.fetch_add(1, Ordering::Relaxed);
        self.messages.insert(message_id.clone(), message);
        self.message_headers.insert(message_id.clone(), message_header);
        self.message_events.insert(message_id.clone(), Vec::new());
        Ok(())
    }

    pub fn insert_message_header(&self, message_header: Arc<RempMessageHeader>) -> Result<()> {
        let message_id = message_header.message_id.clone();
        if self.is_message_known(&message_id) {
            fail!("Inconsistent message cache contents: message status {} present in cache, although should not", message_id)
        }

        self.attach_id_to_uid(&message_id, &message_header.message_uid)?;
        self.message_count.fetch_add(1, Ordering::Relaxed);
        self.message_headers.insert(message_id.clone(), message_header);
        self.message_events.insert(message_id, Vec::new());
        Ok(())
    }

    pub fn cc_expired(old_cc_seqno: u32, new_cc_seqno: u32) -> bool {
        old_cc_seqno+2 <= new_cc_seqno
    }

    pub fn _is_expired(&self, message_id: &UInt256, new_cc_seqno: u32) -> Result<bool> {
        match self.message_headers.get(message_id) {
            None => fail!("Message {:x} was not found: cannot check its expiration time", message_id),
            Some(header) => Ok(Self::cc_expired(header.1.master_cc, new_cc_seqno))
        }
    }

    fn update_message_header<F>(&self, message_id: &UInt256, updater: F) -> Result<()>
        where F: FnOnce(Arc<RempMessageHeader>) -> Arc<RempMessageHeader>
    {
        let old_header = self.message_headers.get(message_id);
        match &old_header {
            None => fail!("Message shard {:x} not found", message_id),
            Some(x) => {
                self.message_headers.insert(message_id.clone(), updater(x.1.clone()));
                Ok(())
            }
        }
    }

    pub fn update_message_shard(&self, message_id: &UInt256, new_shard: &ShardIdent) -> Result<()> {
        self.update_message_header(message_id, |hdr: Arc<RempMessageHeader>| hdr.new_replace_shard(new_shard))
    }

    pub fn update_message_status(&self, message_id: &UInt256, new_status: RempMessageStatus,
                                 info_if_insered: Option<(&ShardIdent, u32, &UInt256)>
    ) -> Result<()> {
        let old_header_opt = self.message_headers.get(&message_id);
        match &old_header_opt {
            None => {
                if let Some((shard, master_cc, message_uid)) = info_if_insered {
                    let message_header = RempMessageHeader::new_arc(shard, new_status, master_cc, message_id, message_uid);
                    log::trace!(target: "remp",
                        "Message {:x}: absent from cache, adding {}",
                        message_id, message_header
                    );
                    self.insert_message_header(message_header)
                }
                else {
                    fail!("Updating message {:X} status to {:?}: message is not known", message_id, new_status)
                }
            },
            Some(old_header) =>
                // TODO: move status change validation into update
                if !validate_status_change(&old_header.1.status, &new_status) {
                    fail!("Message {:x}: cannot change status from {} to {}",
                        message_id, old_header.1, new_status
                    )
                }
                else {
                    log::trace!(target: "remp",
                        "Message {:x}: changing status {} => {}",
                        message_id, old_header.1, new_status
                    );
                    self.update_message_header(&message_id, |hdr: Arc<RempMessageHeader>| hdr.new_replace_status(new_status))?;
                    Ok(())
                }
        }
    }

    pub fn attach_id_to_uid(&self, msg_id: &UInt256, msg_uid: &UInt256) -> Result<()> {
        self.message_by_uid.append_to_set(msg_uid, msg_id)
    }

    pub fn detach_id_from_uid(&self, msg_id: &UInt256, msg_uid: &UInt256) -> Result<()> {
        self.message_by_uid.remove_from_set(msg_uid, msg_id)
    }

/*
    pub fn attach_id_to_uid(&self, msg_id: &UInt256, msg_uid: &UInt256) -> Result<()> {
        let mut start = match self.message_by_uid.get(msg_uid) {
            None => Vec::new(),
            Some (v) => v.1.clone()
        };
        let mut replacement = insert_and_sort(start, msg_id);

        loop {
            let old = self.message_by_uid.insert(msg_uid.clone(), replacement.clone());
            match old {
                None => break,
                Some(rm) if insert_and_sort(rm.val(), &msg_uid) == replacement => break,
                Some(rm) => start = rm.val().clone()
            }
        };

        Ok(())
    }
*/

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
                    self.message_events_to_string(self.message_headers.get(msg_id).map(|cc| cc.1.master_cc), &x.1), msg_id
                )
            }
        }
    }

    pub fn change_accepted_by_collator_to_ignored(&self, msg_id: &UInt256) -> Option<u32> {
        match (self.message_headers.get(msg_id), self.messages.get(msg_id)) {
            (Some(hdr), Some(msg)) => {
                if let RempMessageStatus::TonNode_RempAccepted(acc) = &hdr.1.status {
                    if acc.level == RempMessageLevel::TonNode_RempCollator {
                        let ign = RempIgnored { block_id: acc.block_id.clone(), level: acc.level.clone() };
                        self.message_headers.insert(msg_id.clone(), hdr.1.new_replace_status(RempMessageStatus::TonNode_RempIgnored(ign)));
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

    fn insert_message(&mut self, mc: Arc<MessageCacheMessages>, message: Arc<RmqMessage>, message_uid: &UInt256, shard: &ShardIdent, status: RempMessageStatus, master_cc: u32) -> Result<()> {
        mc.insert_message(message.clone(), RempMessageHeader::new_arc(shard, status, master_cc, &message.message_id, message_uid))?;
        self.message_master_cc_order.push((Reverse(master_cc), message.message_id.clone()));
        Ok(())
    }

    fn insert_message_status(&mut self, mc: Arc<MessageCacheMessages>, message_id: &UInt256, message_uid: &UInt256, shard: &ShardIdent, status: RempMessageStatus, master_cc: u32) -> Result<()> {
        mc.insert_message_header(RempMessageHeader::new_arc(shard, status, master_cc, message_id, message_uid))?;
        self.message_master_cc_order.push((Reverse(master_cc), message_id.clone()));
        Ok(())
    }

    fn remove_old_message(&mut self, mc: Arc<MessageCacheMessages>, current_cc: u32) -> Option<(UInt256, Option<Arc<RmqMessage>>, Option<Arc<RempMessageHeader>>)> {
        if let Some((old_cc, msg_id)) = self.message_master_cc_order.pop() {
            if !MessageCacheMessages::cc_expired(old_cc.0, current_cc) {
                self.message_master_cc_order.push((old_cc, msg_id));
                return None
            }

            let msg_opt = mc.messages.remove(&msg_id).map(|x| x.1.clone());
            let msg_header_opt = mc.message_headers.remove(&msg_id).map(|x| x.1.clone());
            let msg_events = mc.message_events.remove(&msg_id).map(|x| x.val().clone());

            if let Some(msg_header) = &msg_header_opt {
                if let Err(e) = mc.detach_id_from_uid(&msg_id, &msg_header.message_uid) {
                    log::error!(target: "remp", "Error detaching message {:x} from uid {:x}: `{}`",
                        msg_id, msg_header.message_uid, e
                    );
                }

                if msg_header.master_cc != old_cc.0 {
                    log::error!(target: "remp",
                        "Removing old message: message {:x} master_cc is {}, but master_cc_order is {}",
                        msg_id,
                        msg_header.master_cc,
                        old_cc.0
                    );
                }

                if msg_opt.is_some() {
                    mc.message_count.fetch_sub(1, Ordering::Relaxed);
                }

                #[cfg(feature = "telemetry")]
                mc.cache_size_metric.update(mc.all_messages_count() as u64);

                log::debug!(target: "remp", "Removing old message: current master cc {}, old master cc {:?}, msg_id {:x}, final status {:?}, collation history `{}`",
                    current_cc, msg_header.master_cc, msg_id, msg_header.status,
                    msg_events.iter().map(|x| mc.message_events_to_string(Some(old_cc.0), x)).collect::<String>()
                );
            }
            else {
                log::error!(target: "remp", "Message is inconsistent: message {:x} has no message header", msg_id);
            }
            return Some((msg_id, msg_opt, msg_header_opt));
/*
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
 */
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

    fn do_update_message_status(&self, message_id: &UInt256, new_status: RempMessageStatus, if_absent: Option<(&ShardIdent, u32, &UInt256)>) -> Result<()> {
        self.messages.update_message_status(message_id, new_status, if_absent)
    }

    /// Checks for duplicate message:
    /// ... if new is Shardchain Accept -- it's duplicate (except it's accept for Collator with the same block).
    /// ... if new is final Accept -- it is applied anyway
    /// Returns new message status, if it worths reporting (final statuses do not need to be reported)
    pub fn update_message_status(&self, message_id: &UInt256, new_status: RempMessageStatus) -> Result<Option<RempMessageStatus>> {
        if let RempMessageStatus::TonNode_RempAccepted(acc_new) = &new_status {
            if acc_new.level == RempMessageLevel::TonNode_RempShardchain {
                let old_status = self.messages.message_headers.get(message_id).map(|x| x.1.status.clone());
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
        self.messages.message_headers.get(message_id).map(|m| m.1.status.clone())
    }

    pub fn get_message_with_status(&self, message_id: &UInt256) -> Option<(Arc<RmqMessage>, RempMessageStatus)> {
        self.get_message_with_header(message_id).map(|(m,h)| (m,h.status.clone()))
        //self.get_message_with_status_and_master_cc(message_id).map(|(m,s,_e)| (m,s))
    }

    pub fn get_message_with_header(&self, message_id: &UInt256) -> Option<(Arc<RmqMessage>, Arc<RempMessageHeader>)> {
        let (msg, header) = (
            self.messages.messages.get(message_id).map(|m| m.1.clone()),
            self.messages.message_headers.get(message_id).map(|m| m.1.clone())
        );

        match (msg, header) {
            (None, None) => None, // Not-existing message
            (None, Some(_)) => None, // Bare message info (retrieved from finalized block)
            (Some(m), Some (h)) => Some((m.clone(),h.clone())), // Full message info
            (Some(m), None) => {
                log::error!(target: "remp", "Message {:x} has no status, body = {}", message_id, m);
                None
            }
        }
    }

    /// Inserts message with given status, if it is not there
    /// If we know something about message -- that's more important than anything we discover from RMQ
    /// If we do not know anything -- TODO: if >= 2/3 rejects, then 'Rejected'. Otherwise 'New'
    /// Actual -- get it as granted ("imprinting")
    pub async fn add_external_message_status(&self, message_id: &UInt256, message_uid: &UInt256, message: Option<Arc<RmqMessage>>, shard: &ShardIdent, status: RempMessageStatus, master_cc: u32) -> Result<Option<RempMessageStatus>> {
        self.cache.execute_sync(|c| {
            let old_header = self.messages.message_headers.get(message_id);
            match old_header {
                None => {
                    match message {
                        None => c.insert_message_status(self.messages.clone(), message_id, message_uid, shard, status, master_cc)?,
                        Some(message) => c.insert_message(self.messages.clone(), message, message_uid, shard, status, master_cc)?
                    };
                    Ok(None)
                },
                Some(r) => {
                    // TODO: checks about status -- whether we may downgrade it
                    let r_clone = r.1.status.clone();
                    self.messages.update_message_shard(&message_id, shard)?;
                    Ok(Some(r_clone))
                },
            }
        }).await
    }

    pub fn insert_masterchain_message_status(&self, message_id: &UInt256, message_uid: &UInt256, new_status: RempMessageStatus, masterchain_seqno: u32) -> Result<()> {
        if let RempMessageStatus::TonNode_RempAccepted(acc_new) = &new_status {
            if acc_new.level == RempMessageLevel::TonNode_RempMasterchain {
                self.do_update_message_status(
                    message_id, new_status.clone(),
                    Some((&acc_new.block_id.shard_id, masterchain_seqno, message_uid))
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

    pub fn get_messages_for_uid(&self, msg_uid: &UInt256) -> Vec<UInt256> {
        self.messages.message_by_uid.get_set(msg_uid)
    }

    pub fn check_message_duplicates(&self, message_id: &UInt256) -> Result<RempDuplicateStatus> {
        let uid = match self.messages.message_headers.get(message_id) {
            None => return Ok(RempDuplicateStatus::Absent),
            Some(hdr) => hdr.val().message_uid.clone()
        };

        let equivalent_msgs = self.get_messages_for_uid(&uid);
        if !equivalent_msgs.contains(message_id) {
            fail!("Message cache: message id {} is not associated with message uid {}", message_id, uid);
        }

        match equivalent_msgs.iter().map(|previous_msg_id| {
            match self.get_message_status(previous_msg_id) {
                Some(RempMessageStatus::TonNode_RempAccepted(RempAccepted { level: RempMessageLevel::TonNode_RempShardchain, block_id: blk, .. })) |
                Some(RempMessageStatus::TonNode_RempAccepted(RempAccepted { level: RempMessageLevel::TonNode_RempMasterchain, block_id: blk, .. })) |
                Some(RempMessageStatus::TonNode_RempDuplicate(RempDuplicate { block_id: blk, .. })) =>
                    RempDuplicateStatus::Duplicate(blk.clone(), uid.clone(), previous_msg_id.clone()),
                Some(_) => RempDuplicateStatus::Fresh(uid.clone()),
                None => RempDuplicateStatus::Absent,
            }
        }).max() {
            None => fail!("Message cache: empty list of messages for uid {}", uid),
            Some(x) => Ok(x)
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

        while let Some((id, m, hdr)) =
            self.cache.execute_sync(|c| c.remove_old_message(self.messages.clone(), current_cc)).await
        {
            total += 1;
            match (m,hdr) {
                (Some(_m),Some(hdr)) => {
                    if is_finally_accepted(&hdr.status) { accepted += 1 }
                    else if is_finally_rejected(&hdr.status) { rejected += 1 }
                },
                (None,Some(hdr)) =>
                    if is_finally_accepted(&hdr.status) { only_status += 1 },
                (m, h) => {
                    log::error!(target: "remp",
                        "Record for message {:?} is in incorrect state: msg = {:?}, status = {:?}",
                        id, m, h
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

