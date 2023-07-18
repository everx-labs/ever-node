use crate::{
    engine_traits::RempDuplicateStatus,
    ext_messages::{
        get_level_and_level_change, get_level_numeric_value, is_finally_accepted, 
        is_finally_rejected, validate_status_change
    },
    validator::{
        mutex_wrapper::MutexWrapper, validator_utils::{get_message_uid, LockfreeMapSet}
    }
};

#[cfg(feature = "telemetry")]
use adnl::telemetry::Metric;
use catchain::serialize_tl_boxed_object;
use std::{
    cmp::Reverse, collections::BinaryHeap, fmt, 
    sync::{Arc, atomic::{AtomicU32, AtomicUsize, Ordering}}, time::SystemTime
};
use ton_api::{
    IntoBoxed,
    ton::ton_node::{
        rempmessagestatus::{RempAccepted, RempIgnored, RempDuplicate},
        RempMessageStatus, RempMessageLevel
    }
};
use ton_block::{
    Deserializable, Message, Serializable, MsgAddressInt, MsgAddrStd, 
    ExternalInboundMessageHeader, BlockIdExt
};
use ton_types::{error, fail, BuilderData, KeyId, SliceData, Result, UInt256};

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
        let rmq_record_serialized = serialize_tl_boxed_object!(rmq_record);
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

        RmqMessage::new (Arc::new(msg), msg_id, msg_uid, KeyId::from_data([0; 32]), 0)
    }
}

impl fmt::Display for RmqMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "id {:x}, uid {:x} source {}, source_idx {}, ts {}",
               self.message_id, self.message_uid, self.source_key, self.source_idx, self.timestamp
        )
    }
}

#[derive(Debug)]
pub struct RempMessageHeader {
    //pub shard: ShardIdent,
    pub status: RempMessageStatus,
    pub master_cc: u32,

    pub message_id: UInt256,
    pub message_uid: UInt256
}

impl RempMessageHeader {
    pub fn new_arc(status: RempMessageStatus, master_cc: u32, message_id: &UInt256, message_uid: &UInt256) -> Arc<Self> {
        Arc::new(RempMessageHeader { status, master_cc, message_id: message_id.clone(), message_uid: message_uid.clone() })
    }

    #[allow(dead_code)]
    pub fn from_rmq_record(record: &ton_api::ton::ton_node::rmqrecord::RmqMessage, message_uid: &UInt256, status: &RempMessageStatus) -> Self {
        RempMessageHeader {
            status: status.clone(),
            master_cc: record.masterchain_seqno as u32,
            message_id: record.message_id.clone(),
            message_uid: message_uid.clone()
        }
    }
/*
    pub fn new_replace_shard(&self, shard: &ShardIdent) -> Arc<Self> {
        Self::new_arc(shard, self.status.clone(), self.master_cc, &self.message_id, &self.message_uid)
    }
*/
    pub fn new_replace_status(&self, status: RempMessageStatus) -> Arc<Self> {
        Self::new_arc(status, self.master_cc, &self.message_id, &self.message_uid)
    }
}

impl fmt::Display for RempMessageHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "id {:x}, uid {:x}, {:?}, master_cc {}",
               self.message_id, self.message_uid, /*self.shard,*/ self.status, self.master_cc
        )
    }
}

pub struct MessageCacheImpl {
    message_master_cc_order: BinaryHeap<(Reverse<u32>, UInt256)>,
}

struct MessageCacheMessages {
    messages: lockfree::map::Map<UInt256, Arc<RmqMessage>>,
    message_headers: lockfree::map::Map<UInt256, Arc<RempMessageHeader>>,
    message_events: lockfree::map::Map<UInt256, Vec<SystemTime>>,
    message_by_uid: LockfreeMapSet<UInt256, UInt256>,

    message_count: AtomicUsize,

    master_cc: AtomicU32,
    master_cc_start_time: lockfree::map::Map<u32, SystemTime>,

    #[cfg(feature = "telemetry")]
    cache_size_metric: Arc<Metric>,
}

impl MessageCacheMessages {
    pub fn new(
        #[cfg(feature = "telemetry")]
        cache_size_metric: Arc<Metric>
    ) -> Self {
        MessageCacheMessages {
            messages: lockfree::map::Map::default(),
            message_headers: lockfree::map::Map::default(),
            message_by_uid: LockfreeMapSet::default(),
            message_count: AtomicUsize::new(0),
            message_events: lockfree::map::Map::default(),

            master_cc_start_time: lockfree::map::Map::default(),
            master_cc: AtomicU32::new(0),

            #[cfg(feature = "telemetry")]
            cache_size_metric,
        }
    }

    pub fn all_messages_count(&self) -> usize {
        self.message_count.load(Ordering::Relaxed)
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
/*
    pub fn update_message_shard(&self, message_id: &UInt256, new_shard: &ShardIdent) -> Result<()> {
        self.update_message_header(message_id, |hdr: Arc<RempMessageHeader>| hdr.new_replace_shard(new_shard))
    }
*/
    pub fn update_message_status(&self, message_id: &UInt256, new_status: RempMessageStatus,
                                 info_if_insered: Option<(u32, &UInt256)>
    ) -> Result<()> {
        let old_header_opt = self.message_headers.get(&message_id);
        match &old_header_opt {
            None => {
                if let Some((master_cc, message_uid)) = info_if_insered {
                    let message_header = RempMessageHeader::new_arc(new_status, master_cc, message_id, message_uid);
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
        log::trace!(target: "remp", "Attaching id {:x} to uid {:x}", msg_id, msg_uid);
        self.message_by_uid.append_to_set(msg_uid, msg_id)?;
        if !self.message_by_uid.contains_in_set(msg_uid, msg_id) {
            fail!("id {:x} was added to uid {:x}, but addition didn't happen: [{:?}]",
                msg_id, msg_uid, self.message_by_uid.get_set(msg_uid)
            );
        }
        Ok(())
    }

    pub fn detach_id_from_uid(&self, msg_id: &UInt256, msg_uid: &UInt256) -> Result<()> {
        log::trace!(target: "remp", "Detaching id {:x} from uid {:x}", msg_id, msg_uid);
        self.message_by_uid.remove_from_set(msg_uid, msg_id)?;
        if self.message_by_uid.contains_in_set(msg_uid, msg_id) {
            fail!("id {:x} was removed from uid {:x}, but removal didn't happen: [{:?}]",
                msg_id, msg_uid, self.message_by_uid.get_set(msg_uid)
            );
        }
        Ok(())
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

    fn insert_message(&mut self, mc: Arc<MessageCacheMessages>, message: Arc<RmqMessage>, message_uid: &UInt256, status: RempMessageStatus, master_cc: u32) -> Result<()> {
        mc.insert_message(message.clone(), RempMessageHeader::new_arc(status, master_cc, &message.message_id, message_uid))?;
        self.message_master_cc_order.push((Reverse(master_cc), message.message_id.clone()));
        Ok(())
    }

    fn insert_message_status(&mut self, mc: Arc<MessageCacheMessages>, message_id: &UInt256, message_uid: &UInt256, status: RempMessageStatus, master_cc: u32) -> Result<()> {
        mc.insert_message_header(RempMessageHeader::new_arc(status, master_cc, message_id, message_uid))?;
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
                else {
                    log::trace!(target: "remp", "Successfully detached message {:x} from uid {:x}: [{:?}]",
                        msg_id, msg_header.message_uid, mc.message_by_uid.get_set(&msg_header.message_uid)
                    )
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

    fn do_update_message_status(&self, message_id: &UInt256, new_status: RempMessageStatus, if_absent: Option<(u32, &UInt256)>) -> Result<()> {
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
    /// If we do not know anything -- TODO: if all reject, then 'Rejected'. Otherwise 'New'
    /// Actual -- get it as granted ("imprinting")
    /// Returns old status and new (added) status
    pub async fn add_external_message_status<F>(&self,
        message_id: &UInt256, message_uid: &UInt256, message: Option<Arc<RmqMessage>>,
        status_if_new: RempMessageStatus, status_updater: F,
        master_cc: u32
    ) -> Result<(Option<RempMessageStatus>,RempMessageStatus)>
        where F: FnOnce(&RempMessageStatus, &RempMessageStatus) -> RempMessageStatus
    {
        self.cache.execute_sync(|c| {
            match self.messages.message_headers.get(message_id) {
                None => {
                    match message {
                        None => c.insert_message_status(self.messages.clone(), message_id, message_uid, status_if_new.clone(), master_cc)?,
                        Some(message) => c.insert_message(self.messages.clone(), message, message_uid, status_if_new.clone(), master_cc)?
                    };
                    Ok((None, status_if_new))
                },
                Some(old_header_rg) => {
                    let old_header = old_header_rg.1.clone();
                    let updated_status = status_updater (&old_header.status,&status_if_new);
                    self.messages.update_message_status(&message_id, updated_status.clone(), None)?;
                    Ok((Some(old_header.status.clone()), updated_status.clone()))
                },
            }
        }).await
    }

    pub fn mark_collation_attempt(&self, message_id: &UInt256) -> Result<()> {
        self.messages.mark_collation_attempt(message_id)
    }

    pub fn get_messages_for_uid(&self, msg_uid: &UInt256) -> Vec<UInt256> {
        self.messages.message_by_uid.get_set(msg_uid)
    }

    /// Returns None if `id` is the lowest message id for `uid`
    /// Returns minimal message id for `uid` otherwise
    pub fn get_lower_id_for_uid(&self, id: &UInt256, uid: &UInt256) -> Result<Option<UInt256>> {
        let equivalent_msgs = self.get_messages_for_uid(uid);
        log::trace!(target: "remp", "Looking for lower id for uid {:x}, ids {:?}", uid, equivalent_msgs);

        if !equivalent_msgs.contains(id) {
            fail!("Message cache: messages for uid {:x} do not contain id {:x}", uid, id);
        }

        match equivalent_msgs.iter().min() {
            None => fail!("Message cache: empty list of messages for uid {:x}", uid),
            Some(lowest_msg_id) if lowest_msg_id == id => Ok(None),
            Some(lowest_msg_id) => Ok(Some(lowest_msg_id.clone()))
        }
    }

    /// Checks, whether `message_id` can be collated or validated. There are three possible outcomes:
    /// * Absent: `message_id` is absent from cache --- cannot be collated/validated.
    /// * Fresh: `message_id` is smallest among messages with same uid and there are no
    /// other accepted by shardchain or masterchain messages with the same uid.
    /// * Duplicate: `message_id` is not smallest among messages with the same uid,
    /// or a message with the same uid is accedpted by shardchain or masterchain.
    pub fn check_message_duplicates(&self, message_id: &UInt256) -> Result<RempDuplicateStatus> {
        let uid = match self.messages.message_headers.get(message_id) {
            None => return Ok(RempDuplicateStatus::Absent),
            Some(hdr) => hdr.val().message_uid.clone()
        };

        let equivalent_msgs = self.get_messages_for_uid(&uid);
        log::trace!(target: "remp", "Attached to uid {:x}, ids {:?}", uid, equivalent_msgs);

        // Check whether a message with same uid was already accepted by shardchain or masterchain
        let fresh_duplicate_status = match equivalent_msgs.iter().map(|previous_msg_id| {
            match &self.get_message_status(previous_msg_id) {
                Some(s @ RempMessageStatus::TonNode_RempAccepted(RempAccepted { level: RempMessageLevel::TonNode_RempShardchain, block_id: blk, .. })) |
                Some(s @ RempMessageStatus::TonNode_RempAccepted(RempAccepted { level: RempMessageLevel::TonNode_RempMasterchain, block_id: blk, .. })) |
                Some(s @ RempMessageStatus::TonNode_RempDuplicate(RempDuplicate { block_id: blk, .. })) => {
                    let (lvl,_) = get_level_and_level_change(s);
                    let lvl_numeric = get_level_numeric_value(&lvl);
                    (lvl_numeric, RempDuplicateStatus::Duplicate(blk.clone(), uid.clone(), previous_msg_id.clone()))
                }
                Some(_) => (get_level_numeric_value(&RempMessageLevel::TonNode_RempQueue), RempDuplicateStatus::Fresh(uid.clone())),
                None => (get_level_numeric_value(&RempMessageLevel::TonNode_RempQueue), RempDuplicateStatus::Absent),
            }
        }).max() {
            None => fail!("Message cache: empty list of messages for uid {:x}", uid),
            Some((_lvl, RempDuplicateStatus::Absent)) => fail!("Message cache: no actual messages in list for uid {:x}", uid),
            Some((_lvl, d @ RempDuplicateStatus::Duplicate(_,_,_))) => return Ok(d),

            Some((_lvl, d @ RempDuplicateStatus::Fresh(_))) => d,
        };

        // Check whether message_id is minimal among other messages with same uid
        match self.get_lower_id_for_uid(&message_id, &uid)? {
            None => Ok(fresh_duplicate_status),
            Some(lowest_msg_id) => {
                match self.messages.message_headers.get(&lowest_msg_id) {
                    None => log::error!(target: "remp",
                        "Message id {:x}, duplicate for {:x}, uid {:x} is not found in cache",
                        lowest_msg_id, message_id, uid
                    ),
                    Some(m)
                        if Self::cc_expired(m.val().master_cc, self.messages.master_cc.load(Ordering::Relaxed)) =>
                            log::error!(target: "remp",
                                "Duplicate message for {:x} is expired {}",
                                lowest_msg_id, m.val()
                            ),
                    _ => ()
                }
                Ok(RempDuplicateStatus::Duplicate(BlockIdExt::default(), uid.clone(), lowest_msg_id))
            }
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

    pub fn set_master_cc(&self, new_current_cc: u32) {
        self.messages.master_cc.store(new_current_cc, Ordering::Relaxed);
    }

    pub fn set_master_cc_start_time(&self, master_cc: u32, start_time: SystemTime) {
        self.messages.set_master_cc_start_time(master_cc, start_time);
    }

    fn extend_info_for_uids(&self, uid: &UInt256) -> String {
        self.get_messages_for_uid(&uid).iter()
            .map(|x| match self.messages.message_headers.get(x) {
                    Some(v) => format!("{:x}:master cc {}; ", x, v.val().master_cc),
                    None => format!("{:x}:None; ", x)
                }
            )
            .collect::<String>()
    }

    pub fn duplicate_info(&self, status: &RempDuplicateStatus) -> String {
        match status {
            RempDuplicateStatus::Absent => "Absent".to_string(),
            RempDuplicateStatus::Fresh(uid) => format!("Fresh uid: {:x}, ids: [{}]", uid, self.extend_info_for_uids(uid)),
            RempDuplicateStatus::Duplicate(_,uid,lw) =>
                format!("Duplicate uid: {:x}, ids: [{}], id: {:x}", uid, self.extend_info_for_uids(uid), lw)
        }
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

