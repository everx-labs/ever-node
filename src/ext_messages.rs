/*
* Copyright (C) 2019-2021 TON Labs. All Rights Reserved.
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

use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use ton_block::{Deserializable, ShardIdent, Message, AccountIdPrefixFull, BlockIdExt};
use ton_types::{Result, types::UInt256, fail, read_single_root_boc};
use adnl::common::add_unbound_object_to_map;
use ton_api::ton::ton_node::{RempMessageStatus, RempMessageLevel};

const MESSAGE_LIFETIME: u32 = 600; // seconds
const MESSAGE_MAX_GENERATIONS: u8 = 2;
const MAX_EXTERNAL_MESSAGE_DEPTH: u16 = 512;
const MAX_EXTERNAL_MESSAGE_SIZE: usize = 65535;

pub const EXT_MESSAGES_TRACE_TARGET: &str = "ext_messages";

struct MessageKeeper {
    message: Arc<Message>,

    // active: bool,            0x1_00_00000000
    // generation: u8,          0x0_ff_00000000
    // reactivate_at: u32,      0x0_00_ffffffff
    atomic_storage: AtomicU64,

    delete_at: u32,
}

impl MessageKeeper {

    pub fn new(message: Arc<Message>, now: u32) -> Self {
        let mut atomic_storage = 0;
        Self::set_active(&mut atomic_storage, true);
        
        Self {
            message,
            atomic_storage: AtomicU64::new(atomic_storage),
            delete_at: now + MESSAGE_LIFETIME
        }
    }

    pub fn message(&self) -> &Message {
        &self.message
    }

    pub fn clone_message(&self) -> Arc<Message> {
        Arc::clone(&self.message)
    }

    pub fn check_active(&self, now: u32) -> bool {
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

    pub fn can_postpone(&self) -> bool {
        let atomic_storage = self.atomic_storage.load(Ordering::Relaxed);
        Self::fetch_generation(atomic_storage) <= MESSAGE_MAX_GENERATIONS
    }

    pub fn postpone(&self, now: u32) {
        let mut atomic_storage = self.atomic_storage.load(Ordering::Relaxed);
        let active = Self::fetch_active(atomic_storage);

        if active {
            let generation = Self::fetch_generation(atomic_storage);
            Self::set_active(&mut atomic_storage, false);
            Self::set_reactivate_at(&mut atomic_storage, now + generation as u32 * 5);
            self.atomic_storage.store(atomic_storage, Ordering::Relaxed);
        }
    }

    pub fn expired(&self, now: u32) -> bool {
        self.delete_at <= now
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

pub struct MessagesPool {
    messages: lockfree::map::Map<UInt256, MessageKeeper>
}

impl MessagesPool {

    pub fn new() -> Self {
        Self{ messages: lockfree::map::Map::new() }
    }


    pub fn new_message_raw(&self, data: &[u8], now: u32) -> Result<UInt256> {
        let (id, message) = create_ext_message(data)?;
        let message = Arc::new(message);

        self.new_message(id.clone(), message, now)?;
        Ok(id)
    }

    pub fn new_message(&self, id: UInt256, message: Arc<Message>, now: u32) -> Result<()> {
        add_unbound_object_to_map(
            &self.messages,
            id,
            || Ok(MessageKeeper::new(message.clone(), now))
        )?;
        Ok(())
    }

    pub fn get_messages(&self, shard: &ShardIdent, now: u32) -> Result<Vec<(Arc<Message>, UInt256)>> {
        let mut result = vec!();
        let mut ids = String::new();
        let mut total = 0;
        for guard in self.messages.iter() {
            total += 1;
            if guard.val().expired(now) {
                log::debug!(
                    target: EXT_MESSAGES_TRACE_TARGET,
                    "get_messages: removing external message {:x} because it is expired",
                    guard.key(),
                );
                self.messages.remove(guard.key());
            } else {
                if let Some(dst) = guard.val().message().dst_ref() {
                    if let Ok(prefix) = AccountIdPrefixFull::prefix(dst) {
                        if shard.contains_full_prefix(&prefix) {
                            if guard.val().check_active(now) {
                                result.push((guard.val().clone_message(), guard.key().clone()));
                                ids.push_str(&format!("{:x} ", guard.key()));
                            }
                        }
                    }
                }
            }
        }
        log::debug!(
            target: EXT_MESSAGES_TRACE_TARGET,
            "get_messages: total (all shardes): {}; found for {} ({}): {}",
            total, shard, result.len(), ids
        );
        Ok(result)
    }

    pub fn complete_messages(&self, to_delay: Vec<UInt256>, to_delete: Vec<UInt256>, now: u32) -> Result<()> {
        for id in to_delete.iter() {
            log::debug!(
                target: EXT_MESSAGES_TRACE_TARGET,
                "complete_messages: removing external message {:x} while enumerating to_delete list",
                id,
            );
            self.messages.remove(id);
        }
        for id in to_delay.iter() {
            if let Some(guard) = self.messages.get(id) {
                if guard.val().can_postpone() {
                    log::debug!(
                        target: EXT_MESSAGES_TRACE_TARGET,
                        "complete_messages: postponed external message {:x} while enumerating to_delay list",
                        id,
                    );
                    guard.val().postpone(now);
                } else {
                    log::debug!(
                        target: EXT_MESSAGES_TRACE_TARGET,
                        "complete_messages: removing external message {:x} because can't postpone",
                        id,
                    );
                    self.messages.remove(id);
                }
            }
        }
        Ok(())
    }

}

pub fn create_ext_message(data: &[u8]) -> Result<(UInt256, Message)> {

    if data.len() > MAX_EXTERNAL_MESSAGE_SIZE {
        fail!("External message is too large: {}", data.len())
    }
    let root = read_single_root_boc(&data)?;
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
        RempMessageStatus::TonNode_RempNew => (RempMessageLevel::TonNode_RempQueue, 0),
        RempMessageStatus::TonNode_RempAccepted(a) => (a.level.clone(), 1),
        RempMessageStatus::TonNode_RempRejected(r) => (r.level.clone(), -1),
        RempMessageStatus::TonNode_RempIgnored(i) => (i.level.clone(), -1),
        RempMessageStatus::TonNode_RempTimeout => (RempMessageLevel::TonNode_RempQueue, -1),
        RempMessageStatus::TonNode_RempDuplicate(_) => (RempMessageLevel::TonNode_RempQueue, 0),
        RempMessageStatus::TonNode_RempSentToValidators(_) => (RempMessageLevel::TonNode_RempFullnode, 0),
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

pub fn validate_status_change(old_status: &RempMessageStatus, new_status: &RempMessageStatus) -> bool {
    let (_old_lvl, _old_chg) = get_level_and_level_change(old_status);
    let (_new_lvl, _) = get_level_and_level_change(new_status);

    return true // TODO: proper check
}

pub struct RempMessagesPool {
    messages: lockfree::map::Map<UInt256, Arc<Message>>,
    statuses_queue: lockfree::queue::Queue<(UInt256, Arc<Message>, RempMessageStatus)>,
}

impl RempMessagesPool {

    pub fn new() -> Self {
        Self {
            messages: lockfree::map::Map::new(),
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