use std::{
    io::Cursor,
    sync::{Arc, atomic::{AtomicU64, Ordering}}
};
use ton_block::{Deserializable, ShardIdent, Message, MsgAddressInt};
use ton_types::{Result, types::UInt256, deserialize_tree_of_cells, fail};


const MESSAGE_LIFETIME: u32 = 600; // seconds
const MESSAGE_MAX_GENERATIONS: u8 = 2;
const MAX_EXTERNAL_MESSAGE_DEPTH: u16 = 512;
const MAX_EXTERNAL_MESSAGE_SIZE: usize = 65535;

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


    pub fn new_message_raw(&self, data: &[u8], now: u32) -> Result<()> {
        let (id, message) = create_ext_message(data)?;
        let message = Arc::new(message);

        self.new_message(id, message, now)
    }

    pub fn new_message(&self, id: UInt256, message: Arc<Message>, now: u32) -> Result<()> {
        self.messages.insert_with(id, |_key, prev_gen_val, updated_pair | {
            if updated_pair.is_some() {
                // someone already added the value into map
                // so discard this insertion attempt
                lockfree::map::Preview::Discard
            } else if prev_gen_val.is_some() {
                // it is value we inserted just now
                lockfree::map::Preview::Keep
            } else {
                // there is not the value in the map - try to add.
                // If other thread adding value the same time - the closure will be recalled
                lockfree::map::Preview::New(MessageKeeper::new(Arc::clone(&message), now))
            }
        });

        Ok(())
    }

    pub fn get_messages(&self, shard: &ShardIdent, now: u32) -> Result<Vec<(Arc<Message>, UInt256)>> {
        let mut result = vec!();
        for guard in self.messages.iter() {
            if let Some(MsgAddressInt::AddrStd(addr_std)) = guard.val().message().dst() {
                if addr_std.workchain_id as i32 == shard.workchain_id() && 
                   shard.contains_account(addr_std.address.clone())? {
                    if guard.val().expired(now) {
                        self.messages.remove(guard.key());
                    }
                    else if guard.val().check_active(now) {
                        result.push((guard.val().clone_message(), guard.key().clone()));
                    }
                }
            }
        }
        Ok(result)
    }

    pub fn complete_messages(&self, to_delay: Vec<UInt256>, to_delete: Vec<UInt256>, now: u32) -> Result<()> {
        for id in to_delete.iter() {
            self.messages.remove(id);
        }
        for id in to_delay.iter() {
            if let Some(guard) = self.messages.get(id) {
                if guard.val().can_postpone() {
                    guard.val().postpone(now);
                } else {
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
    let root = deserialize_tree_of_cells(&mut Cursor::new(data))?;
    if root.level() != 0 {
        fail!("External message must have zero level, but has {}", root.level())
    }
    if root.repr_depth() >= MAX_EXTERNAL_MESSAGE_DEPTH {
        fail!("External message is too deep: {}", root.repr_depth())
    }
    let message = Message::construct_from(&mut root.clone().into())?;
    if let Some(_header) = message.ext_in_header() {
        Ok((root.repr_hash(), message))
    } else {
        fail!("External inbound message {} doesn't have proper header", root.repr_hash())
    }
}
