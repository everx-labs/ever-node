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

use std::sync::{Arc, atomic::{AtomicU64, Ordering, AtomicU32}};
use lockfree::map::Map;
use ton_block::{Deserializable, ShardIdent, Message, AccountIdPrefixFull, BlockIdExt};
use ton_types::{Result, types::UInt256, fail, read_boc};
use adnl::common::{add_unbound_object_to_map, add_unbound_object_to_map_with_update};
use ton_api::ton::ton_node::{RempMessageStatus, RempMessageLevel};

#[cfg(test)]
#[path = "tests/test_ext_messages.rs"]
mod tests;

#[cfg(not(feature = "fast_finality"))]
const MESSAGE_LIFETIME: u32 = 600; // seconds
#[cfg(not(feature = "fast_finality"))]
const MESSAGE_MAX_GENERATIONS: u8 = 3;

#[cfg(feature = "fast_finality")]
const MESSAGE_LIFETIME: u32 = 60; // seconds
#[cfg(feature = "fast_finality")]
const MESSAGE_MAX_GENERATIONS: u8 = 0;
const MAX_EXTERNAL_MESSAGE_DEPTH: u16 = 512;
const MAX_EXTERNAL_MESSAGE_SIZE: usize = 65535;

pub const EXT_MESSAGES_TRACE_TARGET: &str = "ext_messages";

#[derive(Clone)]
struct MessageKeeper {
    message: Arc<Message>,

    // active: bool,            0x1_00_00000000
    // generation: u8,          0x0_ff_00000000
    // reactivate_at: u32,      0x0_00_ffffffff
    atomic_storage: Arc<AtomicU64>,
}

impl MessageKeeper {

    fn new(message: Arc<Message>) -> Self {
        let mut atomic_storage = 0;
        Self::set_active(&mut atomic_storage, true);
        
        Self {
            message,
            atomic_storage: Arc::new(AtomicU64::new(atomic_storage)),
        }
    }

    fn message(&self) -> &Arc<Message> {
        &self.message
    }

    fn check_active(&self, now: u32) -> bool {
        let mut atomic_storage = self.atomic_storage.load(Ordering::Relaxed);
        let active = Self::fetch_active(atomic_storage);
        let generation = Self::fetch_generation(atomic_storage);
        let reactivate_at = Self::fetch_reactivate_at(atomic_storage);

        if !active && reactivate_at <= now {
            Self::set_active(&mut atomic_storage, true);
            Self::set_generation(&mut atomic_storage, generation + 1);
            self.atomic_storage.store(atomic_storage, Ordering::Relaxed);
            true
        } else {
            active
        }
    }

    fn can_postpone(&self) -> bool {
        let atomic_storage = self.atomic_storage.load(Ordering::Relaxed);
        Self::fetch_generation(atomic_storage) < MESSAGE_MAX_GENERATIONS
    }

    fn postpone(&self, now: u32) {
        let mut atomic_storage = self.atomic_storage.load(Ordering::Relaxed);
        let active = Self::fetch_active(atomic_storage);

        if active {
            let generation = Self::fetch_generation(atomic_storage);
            Self::set_active(&mut atomic_storage, false);
            Self::set_reactivate_at(&mut atomic_storage, now + generation as u32 * 5);
            self.atomic_storage.store(atomic_storage, Ordering::Relaxed);
        }
    }

    fn fetch_active(atomic_storage: u64) -> bool { 
        atomic_storage & 0x1_00_00000000 != 0
    }
    fn set_active(atomic_storage: &mut u64, active: bool) {
        if active {
            *atomic_storage |= 0x1_00_00000000;
        } else {
            *atomic_storage &= 0x0_ff_ffffffff;
        }
    }

    fn fetch_generation(atomic_storage: u64) -> u8 { 
        ((atomic_storage & 0x0_ff_00000000) >> 32) as u8
    }
    fn set_generation(atomic_storage: &mut u64, generation: u8) {
        *atomic_storage &= 0x1_00_ffffffff;
        *atomic_storage |= (generation as u64) << 32;
    }

    fn fetch_reactivate_at(atomic_storage: u64) -> u32 { 
        (atomic_storage & 0x0_00_ffffffff) as u32
    }
    fn set_reactivate_at(atomic_storage: &mut u64, reactivate_at: u32) {
        *atomic_storage &= 0x1_ff_00000000;
        *atomic_storage |= reactivate_at as u64;
    }
}

#[derive(Clone)]
struct MessageDescription {
    id: UInt256,
    workchain_id: i32,
    prefix: u64,
}

struct OrderMap {
    seqno: Arc<AtomicU32>,
    map: Map<u32, MessageDescription>,
}

impl OrderMap {
    fn new(id: UInt256, workchain_id: i32, prefix: u64) -> Self {
        let seqno = Arc::new(AtomicU32::new(1));
        let map = Map::new();
        map.insert(0, MessageDescription { id, workchain_id, prefix });
        Self { seqno, map }
    }
    fn insert(&self, id: UInt256, workchain_id: i32, prefix: u64) {
        let seqno = self.seqno.fetch_add(1, Ordering::Relaxed);
        self.map.insert(seqno, MessageDescription { id, workchain_id, prefix });
    }
}

pub struct MessagesPool {
    // map by hash of message
    messages: Map<UInt256, MessageKeeper>,
    // map by timestamp, inside map by seqno for hash of message, workchain_id and prefix of dst address
    order: Map<u32, Arc<OrderMap>>,
    // minimal timestamp
    min_timestamp: AtomicU32,

    #[cfg(test)]
    total_messages: AtomicU32,
    #[cfg(test)]
    total_in_order: AtomicU32,
}

impl MessagesPool {

    pub fn new(now: u32) -> Self {
        metrics::gauge!("ext_messages_len", 0f64);
        metrics::gauge!("ext_messages_expired", 0f64);
        Self {
            messages: Map::with_hasher(Default::default()),
            order: Map::with_hasher(Default::default()),
            min_timestamp: AtomicU32::new(now),
            #[cfg(test)]
            total_messages: AtomicU32::new(0),
            #[cfg(test)]
            total_in_order: AtomicU32::new(0),
        }
    }

    pub fn new_message_raw(&self, data: &[u8], now: u32) -> Result<()> {
        let (id, message) = create_ext_message(data)?;
        let message = Arc::new(message);

        self.new_message(id.clone(), message, now)?;
        Ok(())
    }

    pub fn new_message(&self, id: UInt256, message: Arc<Message>, now: u32) -> Result<()> {
        let timestamp = self.min_timestamp.load(Ordering::Relaxed);
        if now < timestamp {
            fail!("now {} is less than minimum {} for {:x}", now, timestamp, id)
        }
        if self.messages.get(&id).is_some() {
            return Ok(());
        }
        log::debug!(target: EXT_MESSAGES_TRACE_TARGET, "adding external message {:x}", id);
        let workchain_id = message.dst_workchain_id().unwrap_or_default();
        let prefix = message.int_dst_account_id().map_or(0, |mut slice| slice.get_next_u64().unwrap_or_default());
        self.messages.insert(id.clone(), MessageKeeper::new(message));
        #[cfg(test)]
        self.total_messages.fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        self.total_in_order.fetch_add(1, Ordering::Relaxed);
        #[cfg(not(feature = "statsd"))]
        metrics::increment_gauge!("ext_messages_len", 1f64);

        add_unbound_object_to_map_with_update(&self.order, now, |map| {
            if let Some(map) = map {
                map.insert(id.clone(), workchain_id, prefix);
                Ok(None)
            } else {
                let entry = Arc::new(OrderMap::new(id.clone(), workchain_id, prefix));
                Ok(Some(entry))
            }
        })?;
        Ok(())
    }

    pub fn iter(self: Arc<MessagesPool>, shard: ShardIdent, now: u32) -> MessagePoolIter {
        MessagePoolIter::new(self, shard, now)
    }

    pub fn complete_messages(&self, to_delay: Vec<UInt256>, _to_delete: Vec<UInt256>, now: u32) -> Result<()> {
        #[cfg(feature = "fast_finality")]
        for id in &_to_delete {
            let result = self.messages.remove_with(id, |_| {
                log::debug!(
                    target: EXT_MESSAGES_TRACE_TARGET,
                    "complete_messages: removing external message {:x} while enumerating to_delete list",
                    id,
                );
                true
            });
            if result.is_some() {
                #[cfg(not(feature = "statsd"))]
                metrics::decrement_gauge!("ext_messages_len", 1f64);
                #[cfg(test)]
                self.total_messages.fetch_sub(1, Ordering::Relaxed);
            }
        }
        for id in &to_delay {
            let result = self.messages.remove_with(id, |(_, keeper)| {
                if keeper.can_postpone() {
                    log::debug!(
                        target: EXT_MESSAGES_TRACE_TARGET,
                        "complete_messages: postponed external message {:x} while enumerating to_delay list",
                        id,
                    );
                    keeper.postpone(now);
                    false
                } else {
                    true
                }
            });
            if result.is_some() {
                log::debug!(
                    target: EXT_MESSAGES_TRACE_TARGET,
                    "complete_messages: removing external message {:x} because can't postpone",
                    id,
                );
                #[cfg(not(feature = "statsd"))]
                metrics::decrement_gauge!("ext_messages_len", 1f64);
                #[cfg(test)]
                self.total_messages.fetch_sub(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
impl MessagesPool {
    #[cfg(not(feature = "fast_finality"))]
    fn get_messages(self: &Arc<Self>, shard: &ShardIdent, now: u32) -> Result<Vec<(Arc<Message>, UInt256)>> {
        Ok(self.clone().iter(shard.clone(), now).collect())
    }

    pub fn has_messages(&self) -> bool {
        self.messages.iter().next().is_some()
    }

    pub fn clear(&mut self) {
        self.messages.clear()
    }
}

pub struct MessagePoolIter {
    pool: Arc<MessagesPool>,
    shard: ShardIdent,
    now: u32,
    timestamp: u32,
    seqno: u32,
}

impl MessagePoolIter {
    fn new(pool: Arc<MessagesPool>, shard: ShardIdent, now: u32) -> Self {
        let timestamp = pool.min_timestamp.load(Ordering::Relaxed);
        Self {
            pool,
            shard,
            now,
            timestamp,
            seqno: 0,
        }
    }

    fn clear_expired_messages(&mut self) {
        let order = match self.pool.order.remove(&self.timestamp) {
            Some(guard) => guard.val().clone(),
            None => return
        };
        log::debug!(
            target: EXT_MESSAGES_TRACE_TARGET,
            "removing order map for timestamp {} because it is expired", self.timestamp
        );
        while self.seqno < order.seqno.load(Ordering::Relaxed) {
            if let Some(guard) = order.map.remove(&self.seqno) {
                if let Some(guard) = self.pool.messages.remove(&guard.val().id) {
                    #[cfg(not(feature = "statsd"))]
                    metrics::increment_gauge!("ext_messages_expired", 1f64);
                    #[cfg(not(feature = "statsd"))]
                    metrics::decrement_gauge!("ext_messages_len", 1f64);
                    log::debug!(
                        target: EXT_MESSAGES_TRACE_TARGET,
                        "removing external message {:x} because it is expired", guard.key()
                    );
                    #[cfg(test)]
                    self.pool.total_messages.fetch_sub(1, Ordering::Relaxed);
                }
                #[cfg(test)]
                self.pool.total_in_order.fetch_sub(1, Ordering::Relaxed);
            }
            self.seqno += 1;
        }
    }

    fn find_in_map(&mut self, map: &Map<u32, MessageDescription>) -> Option<(Arc<Message>, UInt256)> {
        // if link is valid we check if message is for desired shard and is active
        let descr = map.get(&self.seqno)?;
        let keeper = self.pool.messages.get(&descr.val().id)?;
        if self.shard.contains_prefix(descr.val().workchain_id, descr.val().prefix) && keeper.val().check_active(self.now) {
            return Some((keeper.val().message().clone(), descr.val().id.clone()));
        }
        // let descr = map.get(&self.seqno)?.1.clone();
        // let keeper = self.pool.messages.get(&descr.id)?.1.clone();
        // if self.shard.contains_prefix(descr.workchain_id, descr.prefix) && keeper.check_active(self.now) {
        //     return Some((keeper.message().clone(), descr.id));
        // }
        None
    }
}

impl Iterator for MessagePoolIter {
    type Item = (Arc<Message>, UInt256);

    fn next(&mut self) -> Option<Self::Item> {
        // iterate timestamp
        while self.timestamp <= self.now {
            // check if this order map is expired
            if self.timestamp + MESSAGE_LIFETIME < self.now {
                self.clear_expired_messages();
                // level was removed or not present try to move bottom margin
                let _ = self.pool.min_timestamp.compare_exchange(self.timestamp, self.timestamp + 1, Ordering::Relaxed, Ordering::Relaxed);
            } else if let Some(order) = self.pool.order.get(&self.timestamp).map(|guard| guard.val().clone()) {
                while self.seqno < order.seqno.load(Ordering::Relaxed) {
                    let result = self.find_in_map(&order.map);
                    self.seqno += 1;
                    if result.is_some() {
                        return result;
                    }
                }
            } else if self.timestamp < self.now {
                // level is not present try to move bottom margin
                let _ = self.pool.min_timestamp.compare_exchange(self.timestamp, self.timestamp + 1, Ordering::Relaxed, Ordering::Relaxed);
            }
            self.timestamp += 1;
            self.seqno = 0;
        }
        None
    }
}

pub fn create_ext_message(data: &[u8]) -> Result<(UInt256, Message)> {

    if data.len() > MAX_EXTERNAL_MESSAGE_SIZE {
        fail!("External message is too large: {}", data.len())
    }

    let read_result = read_boc(&data)?;
    if read_result.header.big_cells_count > 0 {
        fail!("External message contains big cells")
    }
    let root = read_result.withdraw_single_root()?;
    if root.level() != 0 {
        fail!("External message must have zero level, but has {}", root.level())
    }
    if root.repr_depth() >= MAX_EXTERNAL_MESSAGE_DEPTH {
        fail!("External message {:x} is too deep: {}", root.repr_hash(), root.repr_depth())
    }
    let message = Message::construct_from_cell(root.clone())?;
    if let Some(header) = message.ext_in_header() {
        if header.dst.rewrite_pfx().is_some() {
            fail!("External inbound message {:x} contains anycast info - it is not supported", root.repr_hash())
        }
        Ok((root.repr_hash(), message))
    } else {
        fail!("External inbound message {:x} doesn't have proper header", root.repr_hash())
    }
}

pub fn get_level_and_level_change(status: &RempMessageStatus) -> (RempMessageLevel, i32) {
    match status {
        RempMessageStatus::TonNode_RempAccepted(a) => (a.level.clone(), 1),
        RempMessageStatus::TonNode_RempRejected(r) => (r.level.clone(), -1),
        RempMessageStatus::TonNode_RempIgnored(i) => (i.level.clone(), -1),
        RempMessageStatus::TonNode_RempTimeout => (RempMessageLevel::TonNode_RempQueue, -1),
        RempMessageStatus::TonNode_RempSentToValidators(_) => (RempMessageLevel::TonNode_RempFullnode, 0),
        /*RempMessageStatus::TonNode_RempDuplicate*/
        _ /*RempMessageStatus::TonNode_RempNew*/ => (RempMessageLevel::TonNode_RempQueue, 0)
    }
}

pub fn get_level_numeric_value(lvl: &RempMessageLevel) -> i32 {
    match lvl {
        RempMessageLevel::TonNode_RempFullnode => 0,
        RempMessageLevel::TonNode_RempQueue => 1,
        RempMessageLevel::TonNode_RempCollator => 2,
        RempMessageLevel::TonNode_RempShardchain => 3,
        RempMessageLevel::TonNode_RempMasterchain => 4
    }
}

/// A message with "rejected" status or a timed-out message are finally rejected
pub fn is_finally_rejected(status: &RempMessageStatus) -> bool {
    match status {
        RempMessageStatus::TonNode_RempRejected(_) | RempMessageStatus::TonNode_RempTimeout => true,
        _ => false
    }
}

pub fn is_finally_accepted(status: &RempMessageStatus) -> bool {
    match get_level_and_level_change(status) {
        (RempMessageLevel::TonNode_RempMasterchain, chg) => chg > 0,
        _ => false
    }
}

pub struct RempMessagesPool {
    messages: Map<UInt256, Arc<Message>>,
    statuses_queue: lockfree::queue::Queue<(UInt256, Arc<Message>, RempMessageStatus)>,
}

impl RempMessagesPool {

    pub fn new() -> Self {
        Self {
            messages: Map::new(),
            statuses_queue: lockfree::queue::Queue::new(),
        }
    }

    pub fn new_message(&self, id: UInt256, message: Arc<Message>) -> Result<()> {
        if !add_unbound_object_to_map(&self.messages, id.clone(), || Ok(message.clone()))? {
            fail!("External message {:x} is already added", id)
        }
        Ok(())
    }

    // Important! If call get_messages with same shard two times in row (without finalize_messages between)
    // the messages returned first call will return second time too.
    pub fn get_messages(&self, shard: &ShardIdent) -> Result<Vec<(Arc<Message>, UInt256)>> {
        let mut result = vec!();
        let mut ids = String::new();
        for guard in self.messages.iter() {
            if let Some(dst) = guard.val().dst_ref() {
                if let Ok(prefix) = AccountIdPrefixFull::prefix(dst) {
                    if shard.contains_full_prefix(&prefix) {
                        result.push((guard.val().clone(), guard.key().clone()));
                        if log::log_enabled!(log::Level::Debug) {
                            ids.push_str(&format!("{:x} ", guard.key()));
                        }
                    }
                }
            }
        }

        log::debug!(
            target: EXT_MESSAGES_TRACE_TARGET,
            "get_messages(remp): shard {}, messages ({}pcs.): {}",
            result.len(), shard, ids
        );

        Ok(result)
    }

    pub fn finalize_messages(
        &self,
        block: BlockIdExt,
        accepted: Vec<UInt256>,
        rejected: Vec<(UInt256, String)>,
        ignored: Vec<UInt256>,
    ) -> Result<()> {
        for id in accepted {
            if let Some(pair) = self.messages.remove(&id) {
                self.statuses_queue.push((
                    id,
                    pair.val().clone(),
                    RempMessageStatus::TonNode_RempAccepted(
                        ton_api::ton::ton_node::rempmessagestatus::RempAccepted{
                            level: RempMessageLevel::TonNode_RempCollator,
                            block_id: block.clone(),
                            master_id: BlockIdExt::default()
                        }
                    )
                ));
            } else {
                log::warn!("finalize_messages: unknown accepted message {}", id);
            }
        }
        for (id, error) in rejected {
            if let Some(pair) = self.messages.remove(&id) {
                self.statuses_queue.push((
                    id,
                    pair.val().clone(),
                    RempMessageStatus::TonNode_RempRejected(
                        ton_api::ton::ton_node::rempmessagestatus::RempRejected{
                            level: RempMessageLevel::TonNode_RempCollator,
                            block_id: block.clone(),
                            error
                        }
                    )
                ));
            } else {
                log::warn!("finalize_messages: unknown rejected message {}", id);
            }
        }
        for id in ignored {
            if let Some(pair) = self.messages.remove(&id) {
                self.statuses_queue.push((
                    id,
                    pair.val().clone(),
                    RempMessageStatus::TonNode_RempIgnored(
                        ton_api::ton::ton_node::rempmessagestatus::RempIgnored{
                            level: RempMessageLevel::TonNode_RempCollator,
                            block_id: block.clone(),
                        }
                    )
                ));
            } else {
                log::warn!("finalize_messages: unknown rejected message {}", id);
            }
        }
        Ok(())
    }

    pub fn finalize_remp_messages_as_ignored(&self, block_id: &BlockIdExt)
    -> Result<()> {
        let mut ignored = vec!();
        for guard in self.messages.iter() {
            if let Some(dst) = guard.val().dst_ref() {
                if let Ok(prefix) = AccountIdPrefixFull::prefix(dst) {
                    if block_id.shard().contains_full_prefix(&prefix) {
                        ignored.push(guard.key().clone());
                    }
                }
            }
        }
        self.finalize_messages(block_id.clone(), vec!(), vec!(), ignored)?;
        Ok(())
    }

    pub fn dequeue_message_status(&self) -> Result<Option<(UInt256, Arc<Message>, RempMessageStatus)>> {
        Ok(self.statuses_queue.pop())
    }
}
