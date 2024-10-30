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
    cmp::max, 
    collections::HashSet,
    fmt, fmt::{Display, Formatter, Write},
    ops::RangeInclusive,
    sync::{Arc, atomic::{AtomicU32, Ordering, Ordering::Relaxed}},
    time::{Duration, SystemTime}
};
use lockfree::map::Map;
use dashmap::{DashMap, DashSet};

#[cfg(feature = "telemetry")]
use adnl::telemetry::Metric;

use crate::{
    engine_traits::RempDuplicateStatus,
    ext_messages::{
        create_ext_message,
        get_level_and_level_change, get_level_numeric_value, is_finally_accepted, 
        is_finally_rejected
    },
    validator::{
        remp_manager::RempSessionStats,
        validator_utils::{get_message_uid, LockfreeMapSet}
    }
};

use catchain::serialize_tl_boxed_object;

use ton_api::{
    IntoBoxed,
    ton::ton_node::{
        rempmessagestatus::{RempAccepted, RempIgnored},
        RempMessageStatus, RempMessageLevel,
        rempcatchainrecordv2::RempCatchainMessageHeaderV2
    }
};

use ever_block::{
    error, fail, BlockIdExt, Deserializable, ExternalInboundMessageHeader, 
    GetRepresentationHash,
    KeyId, Message,
    MsgAddressInt, MsgAddrStd, Result, Serializable, SliceData, UInt256, UnixTime32
};
use ever_block_json::unix_time_to_system_time;

#[cfg(test)]
#[path = "tests/test_message_cache.rs"]
mod tests;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RmqMessage {
    pub message: Arc<Message>,
    pub message_id: UInt256,
    pub message_uid: UInt256,
}

impl RmqMessage {
    pub fn new(message: Arc<Message>) -> Result<Self> {
        let message_id = message.hash()?;
        Self::new_with_id (message, message_id)
    }

    fn new_with_id(message: Arc<Message>, message_id: UInt256) -> Result<Self> {
        let message_uid = get_message_uid(&message);
        Ok(RmqMessage { message, message_id, message_uid })
    }

    pub fn from_raw_message(raw_msg: &ton_api::ton::bytes) -> Result<Self> {
        let (message_id, message) = create_ext_message(raw_msg)?;
        Self::new_with_id (Arc::new(message), message_id)
    }

    pub fn as_remp_message_body(&self) -> ton_api::ton::ton_node::RempMessageBody {
        ton_api::ton::ton_node::rempmessagebody::RempMessageBody {
            message: self.message.write_to_bytes().unwrap()
        }.into_boxed()
    }

    pub fn serialize_message_body(body: &ton_api::ton::ton_node::RempMessageBody) -> ton_api::ton::bytes {
        serialize_tl_boxed_object!(body)
    }

    pub fn deserialize_message_body(raw: &ton_api::ton::bytes) -> Result<ton_api::ton::ton_node::RempMessageBody> {
        let message_body: ton_api::ton::ton_node::RempMessageBody = catchain::utils::deserialize_tl_boxed_object(raw)?;
        Ok(message_body)
    }

    pub fn from_message_body(body: &ton_api::ton::ton_node::RempMessageBody) -> Result<Self> {
        let (message_id, message) = create_ext_message(body.message())?;
        Self::new_with_id (Arc::new(message), message_id)
    }

    #[allow(dead_code)]
    pub fn make_test_message(body: &SliceData) -> Result<Self> {
        let address = UInt256::rand();
        let msg = Message::with_ext_in_header_and_body(ExternalInboundMessageHeader {
            src: Default::default(),
            dst: MsgAddressInt::AddrStd(MsgAddrStd {
                anycast: None,
                workchain_id: -1,
                address: SliceData::from(address.clone())
            }),
            import_fee: Default::default()
        }, body.clone());

        let builder = msg.write_to_new_cell().unwrap();

        let mut reader = SliceData::load_builder(builder)?;
        let mut msg = Message::default();
        msg.read_from(&mut reader).unwrap();

        let msg_cell = msg.serialize().unwrap();
        //let msg_id = UInt256::rand();
        log::trace!(target: "remp", "Account: {}, Message: {:?}, serialized: {:?}, hash code: {}",
            address.to_hex_string(),
            msg, msg_cell.data(),
            msg_cell.repr_hash().to_hex_string()
        );

        RmqMessage::new (Arc::new(msg))
    }
}

impl fmt::Display for RmqMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fullmsg, id {:x}, uid {:x}",
               self.message_id, self.message_uid
        )
    }
}

#[derive(Debug,PartialEq,Eq)]
pub struct RempMessageHeader {
    pub message_id: UInt256,
    pub message_uid: UInt256,
}

impl RempMessageHeader {
    pub fn new(message_id: &UInt256, message_uid: &UInt256) -> Self {
        RempMessageHeader {
            message_id: message_id.clone(),
            message_uid: message_uid.clone()
        }
    }

    pub fn new_arc(message_id: &UInt256, message_uid: &UInt256) -> Arc<Self> {
        Arc::new(Self::new(message_id, message_uid))
    }
/*
    pub fn from_rmq_record(record: &ton_api::ton::ton_node::rempcatchainrecordv2::RempCatchainMessageHeaderV2) -> Result<(RempMessageHeader, RempMessageOrigin)> {
        let header = RempMessageHeader {
            message_id: record.message_id.clone(),
            message_uid: record.message_uid.clone(),
        };
        let origin = RempMessageOrigin {
            source_key: KeyId::from_data(record.source_key_id.as_slice().clone()),
            source_idx: record.source_idx as u32,
            timestamp: RempMessageOrigin::timestamp_now()?
        };
        Ok((header, origin))
    }
*/
    pub fn deserialize(raw: &ton_api::ton::bytes) -> Result<ton_api::ton::ton_node::RempCatchainRecordV2> {
        let rmq_record: ton_api::ton::ton_node::RempCatchainRecordV2 = catchain::utils::deserialize_tl_boxed_object(raw)?;
        Ok(rmq_record)
    }
/*
    pub fn as_remp_catchain_record(&self, master_cc: u32, origin: &RempMessageOrigin) -> ton_api::ton::ton_node::RempCatchainRecordV2 {
        ton_api::ton::ton_node::rempcatchainrecordv2::RempCatchainMessageHeaderV2 {
            message_id: self.message_id.clone().into(),
            message_uid: self.message_uid.clone().into(),
            source_key_id: UInt256::from(origin.source_key.data()),
            source_idx: origin.source_idx as i32,
            masterchain_seqno: master_cc as i32
        }.into_boxed()
    }
*/
    pub fn serialize(rmq_record: &ton_api::ton::ton_node::RempCatchainRecordV2) -> Result<ton_api::ton::bytes> {
        let rmq_record_serialized = serialize_tl_boxed_object!(rmq_record);
        Ok(rmq_record_serialized)
    }

    pub fn from_remp_catchain(record: &RempCatchainMessageHeaderV2) -> Result<Self> {
        Ok(Self::new(&record.message_id, &record.message_uid))
    }

    pub fn serialize_query(query: &ton_api::ton::ton_node::RempMessageQuery) -> Result<ton_api::ton::bytes> {
        let query_serialized = serialize_tl_boxed_object!(query);
        Ok(query_serialized)
    }

    pub fn deserialize_query(raw: &ton_api::ton::bytes) -> Result<ton_api::ton::ton_node::RempMessageQuery> {
        let query = catchain::utils::deserialize_tl_boxed_object(raw)?;
        Ok(query)
    }

    pub fn as_remp_message_query(&self) -> ton_api::ton::ton_node::RempMessageQuery {
        let message_id = self.message_id.clone();
        ton_api::ton::ton_node::rempmessagequery::RempMessageQuery { message_id }.into_boxed()
    }
}

impl Display for RempMessageHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "id {:x}, uid {:x}",
               self.message_id, self.message_uid
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RempMessageOrigin {
    pub source_key: Arc<KeyId>,
    pub source_idx: u32,
    pub timestamp: u32,
}

impl RempMessageOrigin {
    pub fn new(source_key: Arc<KeyId>, source_idx: u32) -> Result<Self> {
        Ok(RempMessageOrigin { 
            source_key,
            source_idx,
            timestamp: Self::timestamp_now()?
        })
    }

    pub fn new_with_updated_source_idx(&self, source_idx: u32) -> Self {
        RempMessageOrigin { 
            source_key: self.source_key.clone(),
            source_idx,
            timestamp: self.timestamp
        }
    }

    pub fn as_remp_catchain_record(&self, message_id: &UInt256, message_uid: &UInt256, master_cc: u32) -> ton_api::ton::ton_node::RempCatchainRecordV2 {
        ton_api::ton::ton_node::rempcatchainrecordv2::RempCatchainMessageHeaderV2 {
            message_id: message_id.clone(),
            message_uid: message_uid.clone(),
            source_key_id: UInt256::from(self.source_key.data()),
            source_idx: self.source_idx as i32,
            masterchain_seqno: master_cc as i32
        }.into_boxed()
    }

    pub fn from_remp_catchain(record: &RempCatchainMessageHeaderV2) -> Result<Self> {
        Self::new(
            KeyId::from_data(*record.source_key_id.as_slice()),
            record.source_idx as u32
        )
    }

    pub fn has_no_source_key(&self) -> bool {
        self.source_key.data().to_vec().iter().all(|x| *x == 0)
    }

    fn timestamp_now() -> Result<u32> {
        Ok(UnixTime32::now().as_u32())
    }

    pub fn system_time(&self) -> Result<SystemTime> {
        unix_time_to_system_time(UnixTime32::new(self.timestamp).as_u32() as u64)
    }

    pub fn create_empty() -> Result<Self> {
        Ok(RempMessageOrigin {
            source_key: KeyId::from_data([0; 32]),
            source_idx: 0,
            timestamp: RempMessageOrigin::timestamp_now()?
        })
    }
}

impl Display for RempMessageOrigin {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let source_key = if self.has_no_source_key() {
            "(broadcast)".to_owned()
        }
        else {
            format!("{}", self.source_key)
        };

        write!(f, "source_key {}, source_idx {}, timestamp {}",
               source_key, self.source_idx, self.timestamp
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RempMessageWithOrigin {
    pub message: RmqMessage,
    pub origin: RempMessageOrigin
}

impl RempMessageWithOrigin {
    pub fn get_message_id(&self) -> &UInt256 {
        &self.message.message_id
    }

    pub fn has_no_source_key(&self) -> bool {
        self.origin.has_no_source_key()
    }

    pub fn as_remp_catchain_record(&self, master_cc: u32) -> ton_api::ton::ton_node::RempCatchainRecordV2 {
        self.origin.as_remp_catchain_record(&self.message.message_id, &self.message.message_uid, master_cc)
    }

    pub fn as_remp_message_body(&self) -> ton_api::ton::ton_node::RempMessageBody {
        self.message.as_remp_message_body()
    }
}

impl Display for RempMessageWithOrigin {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}, {}", self.message, self.origin)
    }
}

pub struct MessageCacheSession {
    master_cc: u32,

    // Time of the leading masterblock (the block by itself belongs to previous session, but everything
    // after its issue belongs to the current one).
    start_time: UnixTime32,

    // Last master and shard blocks of previous session.
    // This should contain info from top_blocks of the catchain session leading masterblock
    // The leading masterblock belongs to previous cc, so it is also added to top_blocks
    // In case of the first session in blockchain (cc=0) this field should be emtpy.
    inf_shards: HashSet<BlockIdExt>,

    ids_for_uid: LockfreeMapSet<UInt256, UInt256>,
    message_headers: DashMap<UInt256, Arc<RempMessageHeader>>,
    message_origins: DashMap<UInt256, Arc<RempMessageOrigin>>,
    messages: DashMap<UInt256, Arc<RmqMessage>>,
    message_events: LockfreeMapSet<UInt256, u32>, //Map<UInt256, Vec<UnixTime32>>,
    message_status: DashMap<UInt256, RempMessageStatus>,
    message_finally_accepted: DashMap<UInt256, RempMessageStatus>,

    blocks_processed: DashSet<BlockIdExt>
}

impl Display for MessageCacheSession {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "MasterCacheSession {}", self.master_cc)
    }
}

impl MessageCacheSession {
    fn insert_message_header(&self, msg_id: &UInt256, msg_hdr: Arc<RempMessageHeader>, msg_origin: Option<Arc<RempMessageOrigin>>) -> Result<()> {
        if msg_id != &msg_hdr.message_id {
            fail!("Message with id {:x} and its header {} have different ids", msg_id, msg_hdr);
        }

        self.ids_for_uid.append_to_set(&msg_hdr.message_uid, msg_id)?;
        if !self.ids_for_uid.contains_in_set(&msg_hdr.message_uid, msg_id) {
            fail!("id {:x} was added to uid {:x}, but addition didn't happen: [{:?}]",
                msg_id, &msg_hdr.message_uid, self.ids_for_uid.get_set(&msg_hdr.message_uid)
            );
        }

        if let Some(old_hdr) = self.message_headers.insert(msg_id.clone(), msg_hdr.clone()) {
            if *old_hdr != *msg_hdr {
                fail!("Message with id {:x} changed its header from {} to {}", msg_id, old_hdr, msg_hdr);
            }
        }

        match msg_origin {
            Some(origin) => if let Some(old_origin) = self.message_origins.insert(msg_id.clone(), origin.clone()) {
                fail!("Message with id {:x} changed its origin from {} to {}", msg_id, origin, old_origin)
            },
            None => if let Some(old_origin) = self.message_origins.get(msg_id) {
                fail!("Message with id {:x} already has its origin in cache: {}, although we do not know it at insertion", msg_id, old_origin.value())
            }
        }

        Ok(())
    }

    fn insert_message(&self, msg: Arc<RmqMessage>, msg_hdr: Arc<RempMessageHeader>, msg_origin: Option<Arc<RempMessageOrigin>>) -> Result<bool> {
        if msg.message_uid != msg_hdr.message_uid || msg.message_id != msg_hdr.message_id {
            fail!("Message with id {:x} and uid {:x} and its header {} have different uids or ids", msg.message_id, msg.message_uid, msg_hdr);
        }

        self.insert_message_header(&msg.message_id, msg_hdr, msg_origin)?;
        match self.messages.insert(msg.message_id.clone(), msg.clone()) {
            None => Ok(true),
            Some(prev) if prev == msg => Ok(false),
            Some(p) => fail!("Different messages for same id {:x}, replacing {} with {}",
                msg.message_id, p, msg
            )
        }
    }

    /// Returns body_updated flag
    fn update_missing_fields(&self, message_id: &UInt256, message: Option<Arc<RmqMessage>>, origin: Option<Arc<RempMessageOrigin>>) -> Result<bool> {
        let mut body_updated = false;
        if let Some(m1) = &message {
            if let Some(m2) = self.messages.get(message_id) {
                if m1 != m2.value() {
                    fail!("Different message body: new body '{}', current cached body '{}'", m1, m2.value());
                }
            }
            else if let Some(m2) = self.messages.insert(message_id.clone(), m1.clone()) {
                if m1 != &m2 {
                    fail!("Parallel updating of message body: new body '{}', another body '{}'", m1, m2)
                }
                log::warn!(target: "remp",
                    "Update_missing_fields: body for message {:x} inserted as new, although it is present already in cache, considering non-updated",
                    message_id
                )
            }
            else {
                body_updated = true
            }
        };

        // If origin is different from the stored one, that's not a big deal.
        if let Some(o1) = &origin {
            if let Some(o2) = self.message_origins.get(message_id) {
                if o1 != o2.value() {
                    log::trace!("Different message origin: new origin '{}', current cached origin '{}'", o1, o2.value());
                }
            }
            else if let Some(o2) = self.message_origins.insert(message_id.clone(), o1.clone()) {
                if o1 != &o2 {
                    log::trace!("Different message origin: new origin '{}', current cached origin '{}'", o1, o2);
                }
            }
        }

        Ok(body_updated)
    }

    fn is_message_present(&self, msg_id: &UInt256) -> bool {
        self.message_headers.contains_key(msg_id)
    }

    fn is_message_fully_known(&self, msg_id: &UInt256) -> Result<bool> {
        if self.message_headers.contains_key(msg_id) &&
            self.message_origins.contains_key(msg_id) &&
            self.messages.contains_key(msg_id)
        {
            if self.get_message_status(msg_id)?.is_some() {
                Ok(true)
            }
            else {
                fail!("Inconsistent message info in cache: {}", self.message_info(msg_id))
            }
        }
        else {
            Ok(false)
        }
    }

    fn starts_before_block(&self, blk: &BlockIdExt) -> bool {
        for inf in &self.inf_shards {
            if inf.shard().intersect_with(blk.shard()) && inf.seq_no() >= blk.seq_no() {
                return false
            }
        }
        true
    }

    fn all_messages_count(&self) -> (usize,usize,usize) {
        (self.message_headers.len(), self.messages.len(), self.message_origins.len())
    }

    fn alter_message_status<F>(&self, message_id: &UInt256, status_updater: F)
        -> Result<(RempMessageStatus,RempMessageStatus)>
        where F: FnOnce(&RempMessageStatus) -> RempMessageStatus
    {
        match &mut self.message_status.get_mut(message_id) {
            None => {
                fail!("Changing status: no message {:x} in message cache session {}", message_id, self)
            },
            Some(status) => {
                let old_status = status.value().clone();
                let new_status = status_updater(&old_status);
                if is_finally_accepted(&new_status) {
                    if let Some(prev) = self.set_finally_accepted_status(message_id, new_status.clone())? {
                        Ok((prev, new_status))
                    }
                    else {
                        Ok((status.value().clone(), new_status))
                    }
                }
                else {
                    *status.value_mut() = new_status.clone();
                    Ok((old_status, status.value().clone()))
                }
            }
        }
    }

    fn update_message_status(&self, message_id: &UInt256, new_status: RempMessageStatus) -> Result<()> {
        if is_finally_accepted (&new_status) {
            log::trace!(target: "remp", "Setting finally accepting status for {:x}: {}", message_id, new_status);
            self.set_finally_accepted_status(message_id, new_status)?;
        }
        else {
            let old_status = self.message_status.insert(message_id.clone(), new_status.clone());
            log::trace!(target: "remp", "Changing message status for {:x}: {:?} => {}", message_id, old_status, new_status);

            if let Some(actual_status) = self.get_message_status(message_id)? {
                if is_finally_accepted(&actual_status) {
                    log::error!(target: "remp", "Changing status for {:x} to {}, although it has final status {}", message_id, new_status, actual_status);
                }
            }
        }
        Ok(())
    }

    fn set_finally_accepted_status(&self, message_id: &UInt256, new_status: RempMessageStatus) -> Result<Option<RempMessageStatus>> {
        if !is_finally_accepted(&new_status) {
            fail!("Set finally accepted status for {:x}: status {} is not final.", message_id, new_status)
        }
        let old = self.message_finally_accepted.insert(message_id.clone(), new_status);
        Ok(old)
    }

/*
    fn update_message_status(&self, message_id: &UInt256, new_status: RempMessageStatus) -> Result<RempMessageStatus> {
        match self.message_status.insert(message_id.clone(), new_status.clone()) {
            None => fail!("Changing status to {}: no message {:x} in message cache session {}", new_status, message_id, self),
            Some(old) => {
                log::trace!(target: "remp", "Message {:x}: changing status {} => {}", message_id, old, new_status);
                return Ok(old)
            }
        }
    }
 */
    fn get_message_status(&self, message_id: &UInt256) -> Result<Option<RempMessageStatus>> {
        if let Some(status) = self.message_finally_accepted.get(message_id) {
            if !is_finally_accepted(status.value()) {
                fail!("Only finally accepted status may be stored in message_finally_accepted, for id {:x} found {}", message_id, status.value())
            }
            Ok(Some(status.value().clone()))
        }
        else if let Some(status) = self.message_status.get(message_id) {
            if is_finally_accepted(status.value()) {
                fail!("No finally accepted status may be stored in message_status, for id {:x} found {}", message_id, status.value())
            }
            Ok(Some(status.value().clone()))
        }
        else {
            Ok(None)
        }
    }

    fn message_events_to_string(&self, message_id: &UInt256) -> String {
        let events = self.message_events.get_set(message_id);
        let result = if let Some(msg) = self.message_origins.get(message_id) {
            let base = msg.value().timestamp as i64;
            events.iter().fold(String::new(), |mut res, x| {
                let _ = write!(res, "{} ", *x as i64 - base);
                res
            })
        } else {
            "*events: no message origin*".to_string()
        };
        format!("{} ({} events)", result, events.len())
    }

    fn message_info(&self, message_id: &UInt256) -> String {
        let header = self.message_headers
            .get(message_id).map(|x| format!("uid: {:x}", x.value().message_uid))
            .unwrap_or_else(|| "*error: no header in cache*".to_owned());

        let status1 = self.message_status.get(message_id).map(|x| x.value().clone());
        let status2 = self.message_finally_accepted.get(message_id).map(|x| x.value().clone());
        let status = match (status1, status2) {
            (Some(x), None) => format!("{:?}/**", x),
            (None, Some(x)) => format!("**/{:?}", x),
            (Some(x), Some(y)) => format!("{:?}/{:?}", x, y),
            (None, None) => "*error: no status in cache*".to_owned()
        };

        let has_additional_info = self.messages.contains_key(message_id)
            || self.message_origins.contains_key(message_id)
            || !self.message_events.get_set(message_id).is_empty();

        let additional_info = if has_additional_info {
            let body = self.messages
                .get(message_id).map(|_| "has body".to_owned())
                .unwrap_or_else(|| "*error: no body*".to_owned());

            let origin = self.message_origins
                .get(message_id).map(|x| x.value().to_string())
                .unwrap_or_else(|| "*error: no origin*".to_owned());

            let collation_history = self.message_events_to_string(message_id);

            format!("{}, {}, {}", body, origin, collation_history)
        }
        else {
            "header only".to_owned()
        };

        format!("id {:x}, cc {}, {}, status: {}, {}",
                message_id, self.master_cc, header, status, additional_info
        )
    }

    fn mark_collation_attempt(&self, msg_id: &UInt256) -> Result<()> {
        self.message_events.append_to_set(msg_id, &UnixTime32::now().as_u32())
    }

    fn list_ids(&self) -> Vec<UInt256> {
        self.message_headers.iter().map(|v| v.key().clone()).collect()
    }

    fn gc_all(&self) -> RempSessionStats {
        let mut stats = RempSessionStats::default();
        for id in self.list_ids() {
            stats.total += 1;

            log::debug!(target: "remp", "Removing old message: {}", self.message_info(&id));

            let message_status = match self.get_message_status(&id) {
                Err(e) => {
                    log::error!(target: "remp", "Record for message {:?} is incorrect: err: {}", id, e);
                    stats.incorrect += 1;
                    continue;
                }
                Ok(s) => s
            };

            match (self.messages.get(&id), message_status) {
                (Some(_m),Some(status)) => {
                    if is_finally_accepted(&status) { stats.accepted_in_session += 1 }
                    else if is_finally_rejected(&status) { stats.rejected_in_session += 1 }
                },
                (None,Some(_status)) => stats.has_only_header += 1,
                (m, h) => {
                    log::error!(target: "remp",
                        "Record for message {:?} is in incorrect state: msg = {:?}, status = {:?}",
                        id, m.map(|x| x.value().clone()), h
                    );
                    stats.incorrect += 1
                }
            }
        }
        stats
    }

    fn new(master_cc: u32, start_time: UnixTime32, inf_shards: Vec<BlockIdExt>) -> Self {
        Self {
            master_cc,
            start_time,
            ids_for_uid: LockfreeMapSet::default(),
            message_headers: DashMap::new(),
            message_origins: DashMap::new(),
            message_events: LockfreeMapSet::default(),
            messages: DashMap::default(),
            message_status: DashMap::default(),
            message_finally_accepted: DashMap::default(),
            inf_shards: HashSet::from_iter(inf_shards),
            blocks_processed: DashSet::default(),
        }
    }
}

pub type MessageWithStatus = (Option<Arc<RmqMessage>>, Arc<RempMessageHeader>, Arc<RempMessageOrigin>, RempMessageStatus, u32);

pub struct MessageCache {
    sessions: Map<u32,Arc<MessageCacheSession>>,

    master_cc_seqno_stored: AtomicU32, // Minimal master_cc_seqno, for which we have messages
    master_cc_seqno_lwb: AtomicU32, // Minimal actual master_cc_seqno
    master_cc_seqno_curr: AtomicU32, // Current (that is, maximal) master_cc_seqno

    #[cfg(feature = "telemetry")]
    cache_size_metric: Arc<Metric>,
}

impl MessageCache {
    /// Returns stats about available messages' info in cache, three counts:
    /// (total, with_bodies, with_origins).
    /// total --- total messages in cache count;
    /// with_bodies --- messages that have bodies (that is, broadcasted/received from full node);
    /// with_origins --- messages that have origins (that is, received through catchain/from full node).
    ///
    /// Some messages never have body/origin (those that were taken from accepted master blocks),
    /// others may have either body or origin missing due to delays or losses in network.
    pub fn all_messages_count(&self) -> (usize,usize,usize) {
        let range = self.get_master_cc_stored_range();
        let mut result: usize = 0;
        let mut with_bodies: usize = 0;
        let mut with_origins: usize = 0;

        for cc in range {
            if let Some(s) = self.sessions.get(&cc) {
                let (r,b,o) = s.val().all_messages_count();
                result += r;
                with_bodies += b;
                with_origins += o;
            }
        }

        (result, with_bodies, with_origins)
    }

    /// Updates message status: changes status to the given value
    pub fn update_message_status(&self, message_id: &UInt256, new_status: RempMessageStatus) -> Result<()> {
        let session = self.get_session_for_message(message_id).ok_or_else(
            || error!("Cannot find message {:x} to change its status to {:?}", message_id, new_status)
        )?;

        session.update_message_status(message_id, new_status)
    }

    fn get_session_for_message(&self, message_id: &UInt256) -> Option<Arc<MessageCacheSession>> {
        let range = self.get_master_cc_stored_range();
        for cc in range {
            if let Some(s) = self.sessions.get(&cc) {
                if s.val().is_message_present(message_id) {
                    return Some(s.val().clone());
                }
            }
        }
        None
    }

    fn get_session_for_block(&self, blk: &BlockIdExt) -> Option<Arc<MessageCacheSession>> {
        let range = self.get_master_cc_stored_range();
        for cc in range.rev() {
            if let Some(s) = self.sessions.get(&cc) {
                log::trace!(target: "remp", "Looking for session of block {} in cc {}: {:?}",
                    blk, cc, s.val().inf_shards
                );

                if s.val().starts_before_block(blk) {
                    return Some(s.val().clone())
                }
            }
        }
        None
    }

    pub fn get_message(&self, message_id: &UInt256) -> Result<Option<Arc<RmqMessage>>> {
        let msg = self.get_session_for_message(message_id).map(
            |session| session.messages.get(message_id).map(
                |m| m.value().clone()
            )
        );
        Ok(msg.flatten())
    }

    pub fn get_message_status(&self, message_id: &UInt256) -> Result<Option<RempMessageStatus>> {
        match self.get_session_for_message(message_id) {
            None => Ok(None),
            Some(s) => s.get_message_status(message_id)
        }
    }

    pub fn get_message_uid(&self, message_id: &UInt256) -> Result<Option<UInt256>> {
        match self.get_session_for_message(message_id) {
            None => Ok(None),
            Some(s) =>
                Ok(Some(s.message_headers.get(message_id).ok_or_else(|| error!("No header for message {:x}, {}",
                    message_id, s
                ))?.value().message_uid.clone()))
        }
    }

    pub fn get_message_with_origin_status_cc(&self, message_id: &UInt256) -> Result<Option<MessageWithStatus>> {
        let session = match self.get_session_for_message(message_id) {
            None => return Ok(None),
            Some(s) => s
        };

        let (header, msg, origin, status) = (
            session.message_headers.get(message_id).ok_or_else(|| error!("Message {:x} has no header", message_id))?.value().clone(),
            session.messages.get(message_id).map(|m| m.value().clone()),
            session.message_origins.get(message_id).map(|m| m.value().clone()),
            session.get_message_status(message_id)?
        );

        match (msg, origin, status) {
            (_, None, Some(_)) => Ok(None), // Bare message info (retrieved from finalized block/not received from broadcast)
            (m, Some(o), Some (s)) =>
                Ok(Some((m.clone(), header.clone(), o.clone(), s.clone(), session.master_cc))), // Full message info
            (m, o, None) =>
                fail!("Message {:x} has no status, body = {:?}, origin = {:?}", message_id, m, o),
        }
    }

    pub fn get_message_origin(&self, message_id: &UInt256) -> Result<Option<Arc<RempMessageOrigin>>> {
        match self.get_session_for_message(message_id) {
            None => Ok(None),
            Some(s) => Ok(s.message_origins.get(message_id).map(|m| m.value().clone()))
        }
    }

    pub fn get_message_info(&self, message_id: &UInt256) -> Result<String> {
        match self.get_session_for_message(message_id) {
            None => Ok("absent".to_string()),
            Some(s) => Ok(s.message_info(message_id))
        }
    }

    pub fn is_message_fully_known(&self, message_id: &UInt256) -> Result<bool> {
        match self.get_session_for_message(message_id) {
            None => Ok(false),
            Some(s) => s.is_message_fully_known(message_id)
        }
    }

    fn insert_message(&self, session: Arc<MessageCacheSession>, message: Arc<RmqMessage>, message_header: Arc<RempMessageHeader>, message_origin: Option<Arc<RempMessageOrigin>>, status: &RempMessageStatus) -> Result<bool> {
        if message.message_id != message_header.message_id {
            fail!("Inconsistent message: message {} and message_header {} have different message_id", message, message_header)
        }

        let message_id = message.message_id.clone();

        //if session.is_message_present(&message_id) {
        //    fail!("Inconsistent message cache contents: message {} present in cache, although should not", message_id)
        //}

        if is_finally_accepted(status) {
            session.message_finally_accepted.insert(message_id.clone(), status.clone());
        }
        else {
            session.message_status.insert(message_id.clone(), status.clone());
        }
        session.insert_message(message, message_header, message_origin)
    }

    fn insert_message_header(&self, session: Arc<MessageCacheSession>, message_header: Arc<RempMessageHeader>, message_origin: Option<Arc<RempMessageOrigin>>, status: &RempMessageStatus) -> Result<()> {
        let message_id = message_header.message_id.clone();
        //if session.is_message_present(&message_id) {
        //    fail!("Inconsistent message cache contents: message header {:x} present in cache, although should not", message_id)
        //}

        if is_finally_accepted(status) {
            session.message_finally_accepted.insert(message_id.clone(), status.clone());
        }
        else {
            session.message_status.insert(message_id.clone(), status.clone());
        }
        session.insert_message_header(&message_id, message_header, message_origin)?;
        Ok(())
    }

    /// Inserts message with given status, if it is not there
    /// If we know something about message -- that's more important than anything we discover from RMQ
    /// If we do not know anything -- TODO: if all reject, then 'Rejected'. Otherwise 'New'
    /// Actual -- get it as granted ("imprinting")
    /// Returns old status, new (added) status, and body_updated flag
    #[allow(clippy::too_many_arguments)]
    pub fn add_external_message_status<F>(&self,
        message_id: &UInt256,
        message_uid: &UInt256,
        message: Option<Arc<RmqMessage>>,
        message_origin: Option<Arc<RempMessageOrigin>>,
        status_if_new: RempMessageStatus, status_updater: F,
        master_cc: u32
    ) -> Result<(Option<RempMessageStatus>,RempMessageStatus,bool)>
        where F: FnOnce(&RempMessageStatus, &RempMessageStatus) -> RempMessageStatus
    {
        match self.get_session_for_message(message_id) {
            None => {
                let session = self.sessions
                    .get(&master_cc)
                    .ok_or_else(|| error!("Master cc session {} is not created; current master cc ranges {:?}",
                        master_cc, self.get_master_cc_stored_range()
                    ))?.val().clone();

                let header = RempMessageHeader::new_arc(
                    message_id,
                    message_uid
                );

                let body_updated = match message {
                    None => {
                        self.insert_message_header( session, header, message_origin, &status_if_new)?;
                        false
                    },
                    Some(message) =>
                        self.insert_message(session, message, header, message_origin.clone(), &status_if_new)?
                };
                Ok((None, status_if_new, body_updated))
            },
            Some(session) => {
                let body_updated = session
                    .update_missing_fields(message_id, message, message_origin)
                    .unwrap_or_else(|e| {
                        log::error!(target: "remp", "Different cache contents for external message {}: {}", message_id, e);
                        false
                    });

                let (old_status, final_status) =
                    session.alter_message_status(message_id, |old| status_updater(old,&status_if_new))?;

                Ok((Some(old_status), final_status, body_updated))
            },
        }
    }

    /// Updates message body for the message
    /// `data` --- Message body with ids
    /// `return` --- Whether the body added or not
    pub fn update_message_body(&self, data: Arc<RmqMessage>) -> Result<bool> {
        let (old_status, new_status, body_updated) = self.add_external_message_status(
            &data.message_id,
            &data.message_uid,
            Some(data.clone()),
            None,
            RempMessageStatus::TonNode_RempNew, |old,_new| old.clone(),
            self.master_cc_seqno_curr.load(Ordering::Relaxed)
        )?;
        let info = self.get_message_info(&data.message_id)?;
        log::trace!(target: "remp", "Message {}, tried to update message body: old status {}, new status {}, full message cache info {}",
            &data,
            old_status.map_or_else(|| "None".to_string(), |m| m.to_string()),
            new_status,
            info
        );
        Ok(body_updated)
    }

    /// Checks whether message msg_id is accepted by collator;
    /// if true, changes its status to ignored
    pub fn change_accepted_by_collator_to_ignored(&self, msg_id: &UInt256) -> Result<bool> {
        let session = self.get_session_for_message(msg_id)
            .ok_or_else(|| error!("Cannot find message {:x} in message cache", msg_id))?;

        let (before,after) = session.alter_message_status(msg_id, |old_status| {
            if let RempMessageStatus::TonNode_RempAccepted(acc) = old_status {
                if acc.level == RempMessageLevel::TonNode_RempCollator {
                    let ign = RempIgnored { block_id: acc.block_id.clone(), level: acc.level.clone() };
                    return RempMessageStatus::TonNode_RempIgnored(ign)
                }
            };
            old_status.clone()
        })?;

        Ok(before != after)
    }

    fn get_master_cc_stored_range(&self) -> RangeInclusive<u32> {
        let lwb = self.master_cc_seqno_stored.load(Ordering::Relaxed);
        let curr = self.master_cc_seqno_curr.load(Ordering::Relaxed);
        lwb ..= curr
    }

    pub fn mark_collation_attempt(&self, message_id: &UInt256) -> Result<()> {
        let session = self
            .get_session_for_message(message_id)
            .ok_or_else(|| error!("Cannot find message {:x}", message_id))?;
        session.mark_collation_attempt(message_id)
    }

    pub fn get_messages_for_uid(&self, msg_uid: &UInt256) -> Vec<UInt256> {
        let mut res = Vec::new();
        for cc in self.get_master_cc_stored_range() {
            if let Some(session) = self.sessions.get(&cc) {
                let mut curr = session.val().ids_for_uid.get_set(msg_uid);
                res.append(&mut curr);
            }
        }
        res.sort();
        res
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
        let uid = match self.get_message_uid(message_id)? {
            None => return Ok(RempDuplicateStatus::Absent),
            Some(hdr) => hdr
        };

        let equivalent_msgs = self.get_messages_for_uid(&uid);
        //log::trace!(target: "remp", "Attached to uid {:x}, ids {:?}", uid, equivalent_msgs);

        // Check whether a message with same uid was already accepted by shardchain or masterchain
        let fresh_duplicate_status = match equivalent_msgs.iter().map(|previous_msg_id| {
            match &self.get_message_status(previous_msg_id)? {
                Some(s @ RempMessageStatus::TonNode_RempAccepted(RempAccepted { level: RempMessageLevel::TonNode_RempShardchain, block_id: blk, .. })) |
                Some(s @ RempMessageStatus::TonNode_RempAccepted(RempAccepted { level: RempMessageLevel::TonNode_RempMasterchain, block_id: blk, .. })) => {
                    let (lvl,_) = get_level_and_level_change(s);
                    let lvl_numeric = get_level_numeric_value(&lvl);
                    Ok((lvl_numeric, RempDuplicateStatus::Duplicate(blk.clone(), uid.clone(), previous_msg_id.clone())))
                }
                Some(_) => Ok((get_level_numeric_value(&RempMessageLevel::TonNode_RempQueue), RempDuplicateStatus::Fresh(uid.clone()))),
                None => Ok((get_level_numeric_value(&RempMessageLevel::TonNode_RempQueue), RempDuplicateStatus::Absent)),
            }
        }).try_fold(None, |acc: Option<(i32, RempDuplicateStatus)>, curr: Result<(i32, RempDuplicateStatus)>|
            Result::Ok(max(acc, Some(curr?)))
        )? {
            None => fail!("Message cache: empty list of messages for uid {:x}", uid),
            Some((_lvl, RempDuplicateStatus::Absent)) => fail!("Message cache: no actual messages in list for uid {:x}", uid),
            Some((_lvl, d @ RempDuplicateStatus::Duplicate(_,_,_))) => return Ok(d),
            Some((_lvl, d @ RempDuplicateStatus::Fresh(_))) => d,
        };

        // Check whether message_id is minimal among other messages with same uid
        match self.get_lower_id_for_uid(message_id, &uid)? {
            None => Ok(fresh_duplicate_status),
            Some(lowest_msg_id) => {
                if self.get_session_for_message(&lowest_msg_id).is_none() {
                    fail!("Message id {:x}, duplicate for {:x} uid {:x}, is not found in cache",
                        lowest_msg_id, message_id, uid
                    )
                }
                Ok(RempDuplicateStatus::Duplicate(BlockIdExt::default(), uid.clone(), lowest_msg_id))
            }
        }
    }

    pub fn message_stats(&self) -> String {
        let (count,with_origins,with_bodies) = self.all_messages_count();
        format!("All REMP messages count = {}, of them: with origins (via catchain) = {}, with bodies (broadcasted) = {}", count, with_origins, with_bodies)
    }

    pub fn try_set_master_cc_start_time(&self, master_cc: u32, start_time: UnixTime32, inf_blocks: Vec<BlockIdExt>) -> Result<()> {
        if let Some(session) = self.sessions.get(&master_cc) {
            let session = session.val().clone();
            let cc_stored = self.master_cc_seqno_stored.load(Relaxed);
            if cc_stored > master_cc {
                fail!("MessageCacheSession {} is already created, but not counted in master_cc_seqno_stored {}", master_cc, cc_stored)
            }

            if session.start_time.as_u32() != start_time.as_u32() {
                fail!("Session with cc {} start_time is different: {} != {}",
                    master_cc, session.start_time, start_time
                )
            }

            let shards = &session.inf_shards;
            if shards.len() != inf_blocks.len() || !inf_blocks.iter().all(|b| shards.contains(b)) {
                fail!("MessageCacheSession {} has different infimum blocks: old {:?}, new {:?}",
                    master_cc, shards, inf_blocks
                )
            }
            return Ok(())
        }

        log::info!(target: "remp", "Creating MessageCacheSession: master_cc {}, start time {}, inf blocks {:?}",
            master_cc, start_time.as_u32(), inf_blocks
        );

        if let Some(_old) = self.sessions.insert(master_cc, Arc::new(MessageCacheSession::new(master_cc, start_time, inf_blocks))) {
            fail!("MessageCacheSession {} is created in parallel!", master_cc)
        }
        self.master_cc_seqno_stored.fetch_min(master_cc, Relaxed);
        Ok(())
    }

    pub fn is_block_processed(&self, blk: &BlockIdExt) -> Result<bool> {
        let session = self
            .get_session_for_block(blk)
            .ok_or_else(|| error!("Block {} has no corresponding message cache session", blk))?;
        Ok(session.blocks_processed.contains(blk))
    }

    pub fn mark_block_processed(&self, blk: &BlockIdExt) -> Result<bool> {
        let session = self
            .get_session_for_block(blk)
            .ok_or_else(|| error!("Block {} has no corresponding message cache session", blk))?;
        Ok(session.blocks_processed.insert(blk.clone()))
    }

    pub fn get_inf_shards(&self, cc: u32) -> Result<HashSet<BlockIdExt>> {
        let session = self.sessions.get(&cc)
            .ok_or_else(|| error!("Session {} is unkonwn", cc))?;
        Ok(session.val().inf_shards.clone())
    }

    pub fn compute_lwb_for_upb(&self, starting_lwb: u32, new_current_master_cc: u32, rp_guarantee: Duration) -> Result<Option<u32>> {
        let new_time = match self.sessions.get(&new_current_master_cc) {
            None => fail!("update_master_cc_ranges: start time for master cc {} must be known", new_current_master_cc),
            Some(t) => t.val().start_time.as_u32() as i64
        };
        let cutoff = new_time - rp_guarantee.as_secs() as i64;

        log::trace!(target: "remp", "Computing lwb from old lwb {}, new master cc {}, new time {}, cutoff {}",
            starting_lwb, new_current_master_cc, new_time, cutoff
        );

        let lwb_candidates: Vec<(u32,i64)> = (starting_lwb.. new_current_master_cc)
            .filter_map(|cc|
                self.sessions.get(&cc)
                    .map(|session| (cc, session.val().start_time.as_u32() as i64))
            )
            .collect();

        let mut prev_time = 0;
        for (_cc,time) in lwb_candidates.iter() {
            if *time < prev_time {
                fail!("Incorrect master_cc_start_time contents: not properly sorted: {:?}", lwb_candidates);
            }
            if *time > new_time {
                fail!("Older session start time {} is greater than new time {}, contents: {:?}", *time, new_time, lwb_candidates);
            }
            prev_time = *time;
        }

        // start[max_expired] <= cutoff, start[max_expired+1] > cutoff
        let max_expired = lwb_candidates.iter()
            .filter(|(_cc, start_time)| *start_time <= cutoff)
            .max();

        match max_expired {
            None => {
                log::info!(target: "remp",
                    "Computing lwb for upb {}: not enough time info, leaving range lwb in place",
                    new_current_master_cc
                );
                Ok(None)
            },
            Some((new_lwb, new_lwb_time)) => {
                log::info!(target: "remp", "Computed lwb for upb: {}..={} [{}s..={}s]",
                    new_lwb, new_current_master_cc, new_lwb_time, new_time
                );
                Ok(Some(*new_lwb))
            }
        }
    }

    pub fn set_master_cc_range(&self, new_range: &RangeInclusive<u32>) -> Result<()> {
        let old_upb = self.master_cc_seqno_curr.load(Relaxed);
        if old_upb > *new_range.end() {
            fail!("Cannot move master_cc_range backwards: old range ends with {}, new range is {}..={}",
                old_upb, new_range.start(), new_range.end()
            )
        }

        self.master_cc_seqno_lwb.store(*new_range.start(), Ordering::Relaxed);
        for cc in new_range.clone() {
            if self.sessions.get(&cc).is_none() {
                fail!("Setting master cc range {:?}: session {} is not created", new_range, cc)
            }
            self.master_cc_seqno_curr.fetch_max(cc, Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn update_master_cc_ranges(&self, new_current_master_cc: u32, rp_guarantee: Duration) -> Result<RangeInclusive<u32>> {
        let old_lwb = self.master_cc_seqno_lwb.load(Ordering::Relaxed);
        let old_upb = self.master_cc_seqno_curr.load(Ordering::Relaxed);

        log::trace!(target: "remp", "Advancing master cc range: old {}..={}, new upb {}",
            old_lwb, old_upb, new_current_master_cc
        );

        if !(old_lwb <= new_current_master_cc && old_upb <= new_current_master_cc) {
            fail!("update_master_cc_ranges: incorrect master cc relations, new_current_master_cc {}, old lwb {}, old current cc {}",
                new_current_master_cc, old_lwb, old_upb
            );
        }

        let new_lwb = match self.compute_lwb_for_upb(old_lwb, new_current_master_cc, rp_guarantee)? {
            None => {
                log::info!(target: "remp",
                    "Advancing master cc range: not enough time info, leaving range lwb in place: {}..={} => {}..={}",
                    old_lwb, old_upb, old_lwb, new_current_master_cc
                );
                old_lwb
            },
            Some(new_lwb) => {
                log::info!(target: "remp", "Advancing master cc range: {}..={} => {}..={}",
                    old_lwb, old_upb, new_lwb, new_current_master_cc
                );
                new_lwb
            }
        };

        let new_range = new_lwb..=new_current_master_cc;
        self.set_master_cc_range(&new_range)?;
        Ok(new_range)
    }

    fn extend_info_for_uids(&self, uid: &UInt256) -> String {
        self.get_messages_for_uid(uid).iter()
            .map(|x| match self.get_session_for_message(x) {
                    Some(v) => format!("{:x}:master cc {}; ", x, v.master_cc),
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

    /// 1. Update master_cc_seqno_lwb and master_cc_seqno_curr
    /// 2. Collect all old messages; return all messages stats
    /// (total messages removed, accepted messages, rejected messages, messages that have status only, messages with incorrect status)
    /// total - (accepted + rejected) = lost;
    pub async fn gc_old_messages(&self, actual_cc: u32) -> RempSessionStats
    {
        let mut stats: RempSessionStats = Default::default();

        let gc_lwb = self.master_cc_seqno_stored.load(Ordering::Relaxed);
        for cc_to_remove in gc_lwb..actual_cc {
            if let Some(session) = self.sessions.remove(&cc_to_remove) {
                log::debug!(target: "remp", "Removing & gc MessageCacheSession {}", session.val());
                stats.add(&session.val().gc_all());

                #[cfg(feature = "telemetry")]
                self.cache_size_metric.update(self.all_messages_count().0 as u64);
            }
            self.master_cc_seqno_stored.store(cc_to_remove+1, Relaxed);
        }

        stats
    }

    pub fn with_metrics(
        #[cfg(feature = "telemetry")]
        cache_size_metric: Arc<Metric>,
    ) -> Self {
        MessageCache {
            sessions: Map::new(),

            master_cc_seqno_stored: AtomicU32::new(u32::MAX),
            master_cc_seqno_lwb: AtomicU32::new(1),
            master_cc_seqno_curr: AtomicU32::new(0),
            #[cfg(feature = "telemetry")]
            cache_size_metric,
        }
    }
}
