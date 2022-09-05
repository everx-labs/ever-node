use std::{collections::HashMap, sync::Arc, fmt, fmt::{Display, Formatter}};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use ever_crypto::KeyId;
use ton_api::ton::ton_node::{RempMessageStatus, RmqRecordStatus, RempMessageLevel};
use ton_block::{BlockIdExt, Deserializable, Message, ShardIdent, Serializable, MsgAddressInt, MsgAddrStd, ExternalInboundMessageHeader};
use ton_types::{UInt256, Result, BuilderData, SliceData, fail};
use ton_api::IntoBoxed;
use crate::validator::mutex_wrapper::MutexWrapper;
use crate::ext_messages::validate_status_change;
use ton_api::ton::ton_node::rempmessagestatus::RempAccepted;
use ton_api::ton::ton_node::rempmessagestatus::RempIgnored;
use crate::engine_traits::RempDuplicateStatus;
use std::time::SystemTime;
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

    pub fn deserialize(raw: &ton_api::ton::bytes) -> Result<(Arc<RmqMessage>, RempMessageStatus)> {
        let rmq_record: ton_api::ton::ton_node::RmqRecord = catchain::utils::deserialize_tl_boxed_object(&raw)?;

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
    }

    pub fn serialize(&self, status: RempMessageStatus) -> Result<ton_api::ton::bytes> {
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

        let rmq_record = ton_api::ton::ton_node::rmqrecord::RmqRecord {
            message: self.message.write_to_bytes().unwrap().into(),
            message_id: self.message_id.into(),
            source_key_id: UInt256::from(self.source_key.data()),
            source_idx: self.source_idx as i32,
            status: rmq_status,
        }.into_boxed();

        let rmq_record_serialized = catchain::utils::serialize_tl_boxed_object!(&rmq_record);
        return Ok(rmq_record_serialized)
    }

    #[allow(dead_code)]
    pub fn make_test_message() -> Self {
        let address = UInt256::rand();
        let msg = ton_block::Message::with_ext_in_header(ExternalInboundMessageHeader {
            src: Default::default(),
            dst: MsgAddressInt::AddrStd(MsgAddrStd {
                anycast: None,
                workchain_id: -1,
                address: SliceData::from(address)
            }),
            import_fee: Default::default()
        });

        let mut builder = BuilderData::new();
        msg.write_to(&mut builder).unwrap();

        let mut reader: SliceData = SliceData::from(builder.data());
        let mut msg = Message::default();
        msg.read_from(&mut reader).unwrap();

        let msg_cell = msg.serialize().unwrap();
        //let msg_id = UInt256::rand();
        log::info!(target: "remp", "Account: {}, Message: {:?}, serialized: {:?}, hash code: {}",
            address.to_hex_string(),
            msg, msg_cell.data(),
            msg_cell.repr_hash().to_hex_string()
        );
        let (msg_id, msg) = (msg_cell.repr_hash(), msg);

        RmqMessage::new (Arc::new(msg), msg_id, KeyId::from_data([0; 32]), 0).unwrap()
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
    messages: HashMap<UInt256, Arc<RmqMessage>>,
    message_shards: HashMap<UInt256, ShardIdent>,
    message_statuses: HashMap<UInt256, RempMessageStatus>,
    message_master_cc: HashMap<UInt256, u32>,
    message_master_cc_order: BinaryHeap<(Reverse<u32>, UInt256)>,
    #[cfg(feature = "telemetry")]
    cache_size_metric: Arc<Metric>,
}

#[allow(dead_code)]
impl MessageCacheImpl {
    pub fn new (
        #[cfg(feature = "telemetry")]
        cache_size_metric: Arc<Metric>,
    ) -> Self {
        MessageCacheImpl {
            messages: HashMap::new(),
            message_shards: HashMap::new(),
            message_statuses: HashMap::new(),
            message_master_cc: HashMap::new(),
            message_master_cc_order: BinaryHeap::new(),
            #[cfg(feature = "telemetry")]
            cache_size_metric,
        }
    }

    /// There are two possible consistent message statuses:
    /// 1. Message present will all structures
    /// 2. Only message status present (for futures)
    fn is_message_consistent(&self, id: &UInt256) -> bool {
        if self.messages.contains_key(id) {
            self.message_statuses.contains_key(id) && self.message_shards.contains_key(id) && self.message_master_cc.contains_key(id)
        }
        else {
            (!self.message_shards.contains_key(id)) && self.message_master_cc.contains_key(id)
        }
    }

    pub fn insert_message(&mut self, message: Arc<RmqMessage>, shard: ShardIdent, status: RempMessageStatus, master_cc: u32) -> Result<()> {
        let message_id = message.message_id;
        if self.messages.contains_key(&message_id) || self.message_shards.contains_key(&message_id) || self.message_statuses.contains_key(&message_id) {
            fail!("Inconsistent message cache contents: message {} present in cache, although should not", message_id)
        }

        self.messages.insert(message.message_id.clone(), message.clone());
        self.message_statuses.insert(message.message_id.clone(), status);
        self.message_shards.insert(message.message_id.clone(), shard);
        self.message_master_cc.insert(message.message_id.clone(), master_cc);
        self.message_master_cc_order.push((Reverse(master_cc), message.message_id.clone()));
        Ok(())
    }

    pub fn remove_message(&mut self, message_id: &UInt256) {
        self.messages.remove(message_id);
        self.message_shards.remove(message_id);
        self.message_statuses.remove(message_id);
        self.message_master_cc.remove(message_id);

        #[cfg(feature = "telemetry")]
        self.cache_size_metric.update(self.messages.len() as u64);
    }

    pub fn cc_expired(old_cc_seqno: u32, new_cc_seqno: u32) -> bool {
        old_cc_seqno >= new_cc_seqno+2
    }

    pub fn is_expired(&mut self, message_id: &UInt256, new_cc_seqno: u32) -> Result<bool> {
        match self.message_master_cc.get(message_id) {
            None => fail!("Message {:x} was not found: cannot check its expiration time", message_id),
            Some(old_cc_seqno) => Ok(Self::cc_expired(*old_cc_seqno, new_cc_seqno))
        }
    }

    pub fn update_message_shard(&mut self, message_id: &UInt256, new_shard: ShardIdent) -> Result<()> {
        let old_shard = self.message_shards.get(message_id);
        match &old_shard {
            None => fail!("Message shard {:x} not found", message_id),
            Some(x) if **x == new_shard => Ok(()),
            Some(_) => {
                self.message_shards.insert(message_id.clone(), new_shard);
                Ok(())
            }
        }
    }

    pub fn update_message_status(&mut self, message_id: &UInt256, new_status: RempMessageStatus) -> Result<()> {
        let old_status = self.message_statuses.get(&message_id);
        match &old_status {
            None => fail!("Message {:x} not found", message_id),
            Some(old_status) =>
                if !validate_status_change(&old_status, &new_status) {
                    fail!("Message {:x}: cannot change status from {} to {}",
                        message_id, old_status, new_status
                    )
                }
                else {
                    log::trace!(target: "remp",
                        "Message {:x}: changing status {} => {}",
                        message_id, old_status, new_status
                    );
                    self.message_statuses.insert(message_id.clone(), new_status);
                    Ok(())
                }
        }
    }

    pub fn for_all_messages_in_shard(
        &self, shard: &ShardIdent, f: &mut dyn FnMut(&UInt256, &Arc<RmqMessage>, &RempMessageStatus)->()
    ) {
        for (id,msg) in self.messages.iter() {
            if let Some(msg_shard) = self.message_shards.get(id) {
                if *msg_shard == *shard {
                    if let Some(status) = self.message_statuses.get(id) {
                        f(id,msg,status);
                    }
                    else {
                        log::error!(target: "remp", "Status for message {} is missing!", id);
                    }
                }
            }
            else {
                log::error!(target: "remp", "Shard for message {} is missing!", id);
            }
        }
    }

    pub fn received_messages_to_vector(&self, shard: &ShardIdent) -> Vec<(Arc<RmqMessage>, RempMessageStatus)> {
        let mut messages = Vec::new();
        self.for_all_messages_in_shard(shard, &mut |_id,msg: &Arc<RmqMessage>,status| messages.push((msg.clone(), status.clone())));
        return messages;
    }

    pub fn received_messages_count(&self, shard: &ShardIdent) -> u32 {
        let mut count = 0;
        self.for_all_messages_in_shard(shard, &mut |_id,_msg: &Arc<RmqMessage>,_status| count += 1);
        count
    }

    pub fn downgrade_accepted_by_collator(&mut self, shard: &ShardIdent) -> Vec<(Arc<RmqMessage>, RempMessageStatus)> {
        let mut downgrading = Vec::new();
        self.for_all_messages_in_shard(shard, &mut |_id,msg: &Arc<RmqMessage>,status|
            match status {
                RempMessageStatus::TonNode_RempAccepted(acc) if acc.level == RempMessageLevel::TonNode_RempCollator => {
                    let ign = RempIgnored { block_id: acc.block_id.clone(), level: acc.level.clone() };
                    downgrading.push((msg.clone(), RempMessageStatus::TonNode_RempIgnored(ign)));
                },
                _ => (),
            }
        );
        for (msg,status) in downgrading.iter() {
            if let Err(e) = self.update_message_status(&msg.message_id, status.clone()) {
                log::error!(target: "remp", "Error updating message status: {}", e);
            }
        }
        downgrading
    }

    pub fn remove_old_message(&mut self, current_cc: u32) -> Option<(UInt256, Option<Arc<RmqMessage>>, Option<RempMessageStatus>, Option<ShardIdent>, Option<u32>)> {
        if let Some((old_cc, msg_id)) = self.message_master_cc_order.pop() {
            if !Self::cc_expired(old_cc.0, current_cc) {
                self.message_master_cc_order.push((old_cc, msg_id));
                return None
            }

            let msg_opt = self.messages.remove(&msg_id);
            let status_opt = self.message_statuses.remove(&msg_id);
            let shard_opt = self.message_shards.remove(&msg_id);
            let master_cc_opt = self.message_master_cc.remove(&msg_id);
            return Some((msg_id, msg_opt, status_opt, shard_opt, master_cc_opt))
        }

        None
    }
    
/*
                if let Some(status) = self.message_statuses.get(msg_id) {
                   let ns = match status {
                        RempMessageStatus::TonNode_RempAccepted(a) if a.level == RempMessageLevel::TonNode_RempMasterchain => return None,
                        RempMessageStatus::TonNode_RempRejected(_) => return None,
                        RempMessageStatus::TonNode_RempDuplicate(_) => return None,
                        _ => return Some((msg, RempMessageStatus::TonNode_RempTimeout))
                    };
                    return Some ((msg.clone(),ns))
                }
                else {
                    log::error!(target: "remp", "Status for message {} is missing, although master_cc_order = {}!",
                        id, old_cc.0
                    );
                    removing.push((msg.clone(),None))
                }
            }
        }

        return None;
    }
*/
    pub fn all_messages_count(&self) -> usize {
        self.messages.len()
    }

    pub fn list_all_messages(&self) -> Vec<(ShardIdent, Arc<RmqMessage>, RempMessageStatus)> {
        let mut list = Vec::new();
        for (id,msg) in self.messages.iter() {
            if let Some(msg_shard) = self.message_shards.get(id) {
                if let Some(status) = self.message_statuses.get(id) {
                    list.push((msg_shard.clone(), msg.clone(), status.clone()));
                }
                else {
                    log::error!(target: "remp", "Status for message {} is missing!", id);
                }
            }
            else {
                log::error!(target: "remp", "Shard for message {} is missing!", id);
            }
        }
        list
    }
}

pub struct MessageCache {
    cache: Arc<MutexWrapper<MessageCacheImpl>>
}

#[allow(dead_code)]
impl MessageCache {
    pub async fn received_messages_to_vector(&self, shard: &ShardIdent) -> Vec<(Arc<RmqMessage>, RempMessageStatus)> {
        self.cache.execute_sync(|cache| cache.received_messages_to_vector(shard)).await
    }

    pub async fn received_messages_count(&self, shard: &ShardIdent) -> u32 {
        self.cache.execute_sync(|cache| cache.received_messages_count(shard)).await
    }

    pub async fn all_messages_count(&self) -> usize {
        self.cache.execute_sync(|cache| cache.all_messages_count()).await
    }

    async fn do_update_message_status(&self, message_id: &UInt256, new_status: RempMessageStatus) -> Result<()> {
        self.cache.execute_sync(|cache| cache.update_message_status(message_id, new_status)).await
    }

    /// Returns new message status
    pub async fn update_message_status(&self, message_id: &UInt256, new_status: RempMessageStatus) -> Result<Option<RempMessageStatus>> {
        if let RempMessageStatus::TonNode_RempAccepted(acc_new) = &new_status {
            // If new is Accept for different block -- it's duplicate!
            let mut old_block_id : BlockIdExt = Default::default();
            if self.cache.execute_sync(|c|
                match c.message_statuses.get(message_id) {
                    Some(RempMessageStatus::TonNode_RempAccepted(acc)) if acc.level == RempMessageLevel::TonNode_RempCollator => {
                        old_block_id = acc.block_id.clone();
                        true
                    },
                    _ => false
                }
            ).await {
                if (acc_new.level == RempMessageLevel::TonNode_RempShardchain ||
                    acc_new.level == RempMessageLevel::TonNode_RempMasterchain) &&
                    acc_new.block_id != old_block_id
                {
                    log::trace! (target: "remp", "Message {:x} is duplicate for {}", message_id, new_status);

                    let duplicate_status = RempMessageStatus::TonNode_RempDuplicate(
                        ton_api::ton::ton_node::rempmessagestatus::RempDuplicate {
                            block_id: acc_new.block_id.clone()
                        }
                    );
                    self.do_update_message_status(message_id, duplicate_status.clone()).await?;
                    return Ok(Some(duplicate_status));
                }
            }
            // If new is Accept and old is Duplicate -- ignore it, updates are about someone's else block
            else if let Some(RempMessageStatus::TonNode_RempDuplicate(_)) = self.get_message_status(message_id).await {
                return Ok(None);
            }
        };

        self.do_update_message_status(message_id, new_status.clone()).await?;
        return Ok(Some(new_status))
    }

    pub async fn get_message(&self, message_id: &UInt256) -> Option<Arc<RmqMessage>> {
        self.cache.execute_sync(|c|
            c.messages.get(message_id).map(|m| m.clone())
        ).await
    }

    pub async fn get_message_status(&self, message_id: &UInt256) -> Option<RempMessageStatus> {
        self.cache.execute_sync(|c|
            c.message_statuses.get(message_id).map(|m| m.clone())
        ).await
    }

    pub async fn get_message_with_status(&self, message_id: &UInt256) -> Option<(Arc<RmqMessage>, RempMessageStatus)> {
        self.get_message_with_status_and_master_cc(message_id).await.map(|(m,s,_e)| (m,s))
    }

    pub async fn get_message_with_status_and_master_cc(&self, message_id: &UInt256) -> Option<(Arc<RmqMessage>, RempMessageStatus, u32)> {
        let (msg, status, expiration) = self.cache.execute_sync(|c|
            (c.messages.get(message_id).map(|m| m.clone()),
             c.message_statuses.get(message_id).map(|m| m.clone()),
             c.message_master_cc.get(message_id).map(|m| m.clone()))
        ).await;

        match (msg, status, expiration) {
            (None, None, None) => None, // Not-existing message
            (None, Some(_), Some(_)) => None, // Bare message info (retrieved from finalized block)
            (Some(m), Some (s), Some(e)) => Some((m,s,e)), // Full message info
            (None, s, e) => { log::error!("Message {:x} has no body, status = {:?}, master_cc = {:?}", message_id, s, e); None },
            (m, None, e) => { log::error!("Message {:x} has no status, body = {:?}, master_cc = {:?}", message_id, m, e); None },
            (m, s, None) => { log::error!("Message {:x} has no master_cc, body = {:?}, status = {:?}", message_id, m, s); None }
        }
    }
/*
    /// Inserts message with 'New' status, returns false if message is already there
    /// and true if message is new for the message_cache
    pub async fn new_message_with_shard(&self, message: Arc<RmqMessage>, shard: ShardIdent) -> Result<(bool, usize)> {
        self.cache.execute_sync(|c| {
            if c.messages.contains_key(&message.message_id) {
                Ok((false, c.messages.len()))
            }
            else {
                c.insert_message(message, shard, RempMessageStatus::TonNode_RempNew)?;
                #[cfg(feature = "telemetry")]
                c.cache_size_metric.update(c.messages.len() as u64);
                Ok((true, c.messages.len()))
            }
        }).await
    }
*/
    /// Inserts message with given status, if it is not there
    /// If we know something about message -- that's more important than anything we discover from RMQ
    /// If we do not know anything -- TODO: if >= 2/3 rejects, then 'Rejected'. Otherwise 'New'
    /// Actual -- get it as granted ("imprinting")
    pub async fn add_external_message_status(&self, message: Arc<RmqMessage>, shard: ShardIdent, status: RempMessageStatus, master_cc: u32) -> Result<Option<RempMessageStatus>> {
        self.cache.execute_sync(|c| {
            let old_status = c.message_statuses.get(&message.message_id);
            match old_status {
                None => {
                    c.insert_message(message, shard, status, master_cc)?;
                    Ok(None)
                },
                Some(r) => {
                    let r_clone = r.clone();
                    c.update_message_shard(&message.message_id, shard)?;
                    Ok(Some(r_clone))
                },
            }
        }).await
    }

    pub async fn remove_message(&self, message_id: &UInt256) {
        self.cache.execute_sync(|cache| cache.remove_message(message_id)).await
    }

    pub async fn check_message_duplicates(&self, message_id: &UInt256) -> RempDuplicateStatus {
        match self.get_message_status(message_id).await {
            Some(RempMessageStatus::TonNode_RempAccepted(RempAccepted {level: RempMessageLevel::TonNode_RempShardchain, block_id:blk,..})) |
            Some(RempMessageStatus::TonNode_RempAccepted(RempAccepted {level: RempMessageLevel::TonNode_RempMasterchain, block_id:blk,..})) =>
                RempDuplicateStatus::Duplicate(blk.clone()),
            Some(_) => RempDuplicateStatus::Fresh,
            None => RempDuplicateStatus::Absent,
        }
    }

    pub async fn downgrade_accepted_by_collator(&self, shard: &ShardIdent) -> Vec<(Arc<RmqMessage>, RempMessageStatus)> {
        self.cache.execute_sync(|c|
            c.downgrade_accepted_by_collator(shard)
        ).await
    }

//  pub async fn update_shards(&self, ) {
//  }

    pub async fn print_all_messages(&self, count_only: bool) {
        if count_only {
            log::trace!(target: "remp", "All REMP messages count {}", self.all_messages_count().await);
        }
        else {
            log::trace!(target: "remp", "All REMP messages:");
            let msgs = self.cache.execute_sync(|c|
                c.list_all_messages()
            ).await;
            let mut idx = 1;
            for (shard,msg,status) in msgs.iter() {
                log::trace!(target: "remp", "Msg {}. shard {}, msg {}, status {}", idx, shard, msg, status);
                idx = idx+1;
            }
            log::trace!(target: "remp", "All REMP messages; list over");
        }
    }

    /// Collect all old messages; return all messages to be timeout-rejected
    pub async fn get_old_messages(&self, current_cc: u32) -> Vec<(Arc<RmqMessage>, RempMessageStatus)> {
        let mut old_messages = Vec::new();

        while let Some((id, m,s,_x,_master_cc)) = self.cache.execute_sync(|c| c.remove_old_message(current_cc)).await {
            match (m,s) {
                (Some(m), Some(s)) => old_messages.push((m,s)),
                (None, Some(_s)) => (),
                (m, s) => log::error!(target: "remp",
                    "Record for message {:?} is in incorrect state: msg = {:?}, status = {:?}",
                    id, m, s
                )
            }
        }

        old_messages
    }

    pub fn with_metrics(
        #[cfg(feature = "telemetry")]
        mutex_awaiting_metric: Arc<Metric>,
        #[cfg(feature = "telemetry")]
        cache_size_metric: Arc<Metric>
    ) -> Self {
        MessageCache { 
            cache: Arc::new(MutexWrapper::with_metric (
                MessageCacheImpl::new(
                    #[cfg(feature = "telemetry")]
                    cache_size_metric
                ),
                "Message cache".to_string(),
                #[cfg(feature = "telemetry")]
                mutex_awaiting_metric
            )) 
        }
    }
}
