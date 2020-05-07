use crate::{
    block::{convert_block_id_ext_blk2api, convert_block_id_ext_api2blk, BlockStuff},
    error::NodeError, 
    shard_state::ShardStateStuff,
    block_proof::BlockProofStuff,
};

use std::{
    path::Path, sync::Arc, sync::atomic::{AtomicU32, Ordering}
};
use ton_block::{BlockIdExt, BlockInfo, AccountIdPrefixFull};
use ton_node_storage::{
    block_db::BlockDb, block_info_db::BlockInfoDb, 
    node_state_db::NodeStateDb, shardstate_db::ShardStateDb,
    db::{rocksdb::RocksDb, traits::{KvcWriteable, DbKey}},
};
use ton_types::{error, fail, Result};

#[cfg(test)]
#[path = "tests/test_db.rs"]
mod tests;

pub trait NodeState: serde::Serialize + serde::de::DeserializeOwned {
    fn get_key() -> &'static str;
    fn load_from_db(db: &dyn InternalDb) -> Result<Self> {
        let value = db.load_node_state(Self::get_key())?;
        Ok(bincode::deserialize::<Self>(&value)?)
    }
    fn store_to_db(&self, db: &dyn InternalDb) -> Result<()> {
        let value = bincode::serialize(self)?;
        db.store_node_state(Self::get_key(), value)
    }
}

#[macro_export]
macro_rules! define_db_prop {
    ($name:ident, $key:literal, $type:ty) => {
        #[derive(serde::Serialize, serde::Deserialize)]
        pub struct $name(pub $type);
        impl NodeState for $name {
            fn get_key() -> &'static str { $key }
        }
    };
}

const FLAG_DATA: u32 = 1;
const FLAG_PROOF: u32 = 1 << 1;
const FLAG_PROOF_LINK: u32 = 1 << 2;
const FLAG_EXT_DB: u32 = 1 << 3;
const FLAG_STATE: u32 = 1 << 4;
const FLAG_PERSISTENT_STATE: u32 = 1 << 5;
const FLAG_NEXT_1: u32 = 1 << 6;
const FLAG_NEXT_2: u32 = 1 << 7;
const FLAG_PREV_1: u32 = 1 << 8;
const FLAG_PREV_2: u32 = 1 << 9;
const FLAG_APPLIED: u32 = 1 << 10;
const FLAG_KEY_BLOCK: u32 = 1 << 11;

/// Meta information related to block
#[derive(Default)]
pub struct BlockHandle {
    id: BlockIdExt,
    flags: AtomicU32,
    gen_utime: AtomicU32,
}

impl BlockHandle {

    pub fn new(id: &BlockIdExt) -> Self {
        let mut res = Self::default();
        res.id = id.clone();
        res
    }

    pub fn with_data(id: &BlockIdExt, data: &[u8]) -> Result<Self> {
        let mut gen_utime = 0;
        let flags = match data.len() {
            1 => data[0] as u32,
            4 => u32::from_le_bytes(arrayref::array_ref!(&data, 0, 4).clone()),
            8 => {
                gen_utime = u32::from_le_bytes(arrayref::array_ref!(&data, 4, 4).clone());
                u32::from_le_bytes(arrayref::array_ref!(&data, 0, 4).clone())
            }
            _ => fail!(NodeError::InvalidData("wrong data length".to_string()))
        };
        Ok(Self {
            id: id.clone(),
            flags: AtomicU32::new(flags),
            gen_utime: AtomicU32::new(gen_utime),

        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut data = self.flags.load(Ordering::SeqCst).to_le_bytes().to_vec();
        data.extend_from_slice(&self.flags.load(Ordering::SeqCst).to_le_bytes());
        data
    }

    // This flags might be set into true only. So flush only after transform false -> true.

    fn fetch_info(&self, info: &BlockInfo) -> Result<()> {
        self.gen_utime.store(info.gen_utime().0, Ordering::SeqCst);
        if info.key_block() {
            self.flags.fetch_or(FLAG_KEY_BLOCK, Ordering::SeqCst);
        }
        Ok(())
    }
    pub(self) fn set_data_inited(&self, block: &BlockStuff) -> Result<()> {
        self.fetch_info(&block.block().read_info()?)?;
        self.flags.fetch_or(FLAG_DATA, Ordering::SeqCst);
        Ok(())
    }
    pub(self) fn set_proof_inited(&self, proof: &BlockProofStuff) -> Result<()> {
        self.fetch_info(&proof.virtualize_block()?.0.read_info()?)?;
        self.flags.fetch_or(FLAG_PROOF, Ordering::SeqCst);
        Ok(())
    }
    pub(self) fn set_proof_link_inited(&self, proof: &BlockProofStuff) -> Result<()> {
        self.fetch_info(&proof.virtualize_block()?.0.read_info()?)?;
        self.flags.fetch_or(FLAG_PROOF, Ordering::SeqCst);
        Ok(())
    }
    pub(self) fn set_processed_in_ext_db(&self) {
        self.flags.fetch_or(FLAG_EXT_DB, Ordering::SeqCst);
    }
    pub(self) fn set_state_inited(&self) {
        self.flags.fetch_or(FLAG_STATE, Ordering::SeqCst);
    }
    pub(self) fn set_persistent_state_inited(&self) {
        self.flags.fetch_or(FLAG_PERSISTENT_STATE, Ordering::SeqCst);
    }
    pub(self) fn set_next1_inited(&self) {
        self.flags.fetch_or(FLAG_NEXT_1, Ordering::SeqCst);
    }
    pub(self) fn set_next2_inited(&self) {
        self.flags.fetch_or(FLAG_NEXT_2, Ordering::SeqCst);
    }
    pub(self) fn set_prev1_inited(&self) {
        self.flags.fetch_or(FLAG_PREV_1, Ordering::SeqCst);
    }
    pub(self) fn set_prev2_inited(&self) {
        self.flags.fetch_or(FLAG_PREV_2, Ordering::SeqCst);
    }
    pub(self) fn set_applied(&self) {
        self.flags.fetch_or(FLAG_APPLIED, Ordering::SeqCst);
    }

    pub fn id(&self) -> &BlockIdExt { &self.id }
    pub fn data_inited(&self) -> bool { (self.flags.load(Ordering::SeqCst) & FLAG_DATA) != 0 }
    pub fn proof_inited(&self) -> bool { (self.flags.load(Ordering::SeqCst) & FLAG_PROOF) != 0 }
    pub fn proof_link_inited(&self) -> bool { (self.flags.load(Ordering::SeqCst) & FLAG_PROOF_LINK) != 0 }
    pub fn processed_in_ext_db(&self) -> bool { (self.flags.load(Ordering::SeqCst) & FLAG_EXT_DB) != 0 }
    pub fn state_inited(&self) -> bool { (self.flags.load(Ordering::SeqCst) & FLAG_STATE) != 0 }
    pub fn persistent_state_inited(&self) -> bool { (self.flags.load(Ordering::SeqCst) & FLAG_PERSISTENT_STATE) != 0 }
    pub fn next1_inited(&self) -> bool { (self.flags.load(Ordering::SeqCst) & FLAG_NEXT_1) != 0 }
    pub fn next2_inited(&self) -> bool { (self.flags.load(Ordering::SeqCst) & FLAG_NEXT_2) != 0 }
    pub fn prev1_inited(&self) -> bool { (self.flags.load(Ordering::SeqCst) & FLAG_PREV_1) != 0 }
    pub fn prev2_inited(&self) -> bool { (self.flags.load(Ordering::SeqCst) & FLAG_PREV_2) != 0 }
    pub fn applied(&self) -> bool { (self.flags.load(Ordering::SeqCst) & FLAG_APPLIED) != 0 }

    pub fn gen_utime(&self) -> Result<u32> {
        if self.data_inited() {
            Ok(self.gen_utime.load(Ordering::Relaxed))
        } else {
            fail!("block data not inited yet")
        }
    }
    pub fn is_key_block(&self) -> Result<bool> {
        if self.data_inited() {
            Ok((self.flags.load(Ordering::Relaxed) & FLAG_KEY_BLOCK) != 0)
        } else {
            fail!("block data not inited yet")
        }
    }
}

pub trait InternalDb : Sync + Send {
    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Arc<BlockHandle>>;

    fn store_block_data(&self, handle: &BlockHandle, block: &BlockStuff) -> Result<()>;
    fn load_block_data(&self, id: &BlockIdExt) -> Result<BlockStuff>;

    fn find_block_by_seq_no(&self, acc_pfx: &AccountIdPrefixFull, seqno: u32) -> Result<Arc<BlockHandle>>;
    fn find_block_by_unix_time(&self, acc_pfx: &AccountIdPrefixFull, utime: u32) -> Result<Arc<BlockHandle>>;
    fn find_block_by_lt(&self, acc_pfx: &AccountIdPrefixFull, lt: u64) -> Result<Arc<BlockHandle>>;

    fn store_block_proof(&self, handle: &BlockHandle, proof: &BlockProofStuff) -> Result<()>;
    fn load_block_proof(&self, id: &BlockIdExt, is_link: bool) -> Result<BlockProofStuff>;

    fn store_shard_state_dynamic(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<()>;
    fn load_shard_state_dynamic(&self, id: &BlockIdExt) -> Result<ShardStateStuff>;

    fn store_shard_state_persistent(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<()>;
    fn load_shard_state_persistent(&self, id: &BlockIdExt) -> Result<ShardStateStuff>;

    fn store_block_prev(&self, handle: &BlockHandle, prev: &BlockIdExt) -> Result<()>;
    fn load_block_prev(&self, id: &BlockIdExt) -> Result<BlockIdExt>;

    fn store_block_prev2(&self, handle: &BlockHandle, prev2: &BlockIdExt) -> Result<()>;
    fn load_block_prev2(&self, id: &BlockIdExt) -> Result<BlockIdExt>;

    fn store_block_next1(&self, handle: &BlockHandle, next: &BlockIdExt) -> Result<()>;
    fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt>;

    fn store_block_next2(&self, handle: &BlockHandle, next2: &BlockIdExt) -> Result<()>;
    fn load_block_next2(&self, id: &BlockIdExt) -> Result<BlockIdExt>;

    fn store_block_processed_in_ext_db(&self, handle: &BlockHandle) -> Result<()>;
    fn store_block_applied(&self, id: &BlockIdExt) -> Result<()>;

    fn store_node_state(&self, key: &'static str, value: Vec<u8>) -> Result<()>;
    fn load_node_state(&self, key: &'static str) -> Result<Vec<u8>>;
}

#[derive(serde::Deserialize)]
pub struct InternalDbConfig {
    pub db_directory: String,
}

struct BlockSeqno([u8; 4]);
impl BlockSeqno {
    pub fn new(seqno: u32) -> Self {
        Self(seqno.to_le_bytes())
    }
}
impl DbKey for BlockSeqno {
    fn key(&self) -> &[u8] {
        &self.0
    }
}

pub struct InternalDbImpl {
    block_db: BlockDb,
    block_handle_db: BlockInfoDb,
    block_proof_db: BlockInfoDb,
    block_proof_link_db: BlockInfoDb,
    prev_block_db: BlockInfoDb,
    prev2_block_db: BlockInfoDb,
    next_block_db: BlockInfoDb,
    next2_block_db: BlockInfoDb,
    node_state_db: NodeStateDb,
    shard_state_persistent_db: BlockInfoDb,
    shard_state_dynamic_db: ShardStateDb,
    block_handle_cache: lockfree::map::Map<ton_api::ton::ton_node::blockidext::BlockIdExt, Arc<BlockHandle>>,
    mc_blocks_id_index: Box<dyn KvcWriteable<BlockSeqno>>,
    //ss_test_map: lockfree::map::Map<BlockIdExt, ShardStateStuff>,
}

impl InternalDbImpl {
    pub fn new(config: InternalDbConfig) -> Result<Self> {
        Ok(
            Self {
                block_db: BlockDb::with_path(&Self::build_name(&config.db_directory, "block_db")),
                block_handle_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "block_handle_db")),
                block_proof_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "block_proof_db")),
                block_proof_link_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "block_proof_link_db")),
                prev_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "prev1_block_db")),
                prev2_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "prev2_block_db")),
                next_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "next1_block_db")),
                next2_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "next2_block_db")),
                node_state_db: NodeStateDb::with_path(&Self::build_name(&config.db_directory, "node_state_db")),
                shard_state_persistent_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "shard_state_persistent_db")),
                shard_state_dynamic_db: ShardStateDb::with_paths(
                    &Self::build_name(&config.db_directory, "states_index_db"),
                    &Self::build_name(&config.db_directory, "cells_db")
                ),
                block_handle_cache: lockfree::map::Map::new(),
                mc_blocks_id_index: Box::new(RocksDb::with_path(&Self::build_name(&config.db_directory, "mc_blocks_id_index"))),
                //ss_test_map: lockfree::map::Map::new(),
            }
            // Self {
            //     block_db: BlockDb::in_memory(),
            //     block_handle_db: BlockInfoDb::in_memory(),
            //     prev_block_db: BlockInfoDb::in_memory(),
            //     prev2_block_db: BlockInfoDb::in_memory(),
            //     next_block_db: BlockInfoDb::in_memory(),
            //     next2_block_db: BlockInfoDb::in_memory(),
            //     node_state_db: NodeStateDb::with_path(&Self::build_name(&config.db_directory, "node_state_db")),
            //     shard_state_persistent_db: BlockInfoDb::in_memory(),
            //     shard_state_dynamic_db: ShardStateDb::in_memory(),
            //     block_handle_cache: lockfree::map::Map::new(),
            //     ss_test_map: lockfree::map::Map::new(),
            // }
        )
    }

    fn build_name(dir: &str, name: &str) -> String {
        let dir = Path::new(dir);
        let name = Path::new(name);
        // `dir` and `name` are correct utf-8 string so `dir.join(name)` too and `unwrap()` is safe.
        dir.join(name).to_str().unwrap().to_owned()
    }

    fn store_block_handle(&self, handle: &BlockHandle) -> Result<()> {
        let value = handle.serialize();
        // TODO: fix DB's interface to avoid cloning.
        self.block_handle_db.put(&handle.id().into(), &value)?;
        Ok(())
    }

    fn load_block_handle_impl(&self, id: &BlockIdExt) -> Result<Arc<BlockHandle>> {
        log::trace!("load_block_handle_impl {}", id);
        let ton_api_id = convert_block_id_ext_blk2api(id);
        if let Some(pair) = self.block_handle_cache.get(&ton_api_id) {
            return Ok(pair.val().clone())
        } else {
            // Load from DB or create new if not exist
            let loaded_block_handle = Arc::new(
                if let Ok(raw_block_handle) = self.block_handle_db.get(&id.into()) {
                    BlockHandle::with_data(id, &raw_block_handle)?
                } else {
                    BlockHandle::new(id)
                });

            // This is so-called "interactive insertion"
                let result = self.block_handle_cache.insert_with(ton_api_id.clone(), |_key, prev_gen_val, updated_pair | {
                    if updated_pair.is_some() {
                        // other thread already added the value into map
                        // so discard this insertion attempt
                        lockfree::map::Preview::Discard
                    } else if prev_gen_val.is_some() {
                        // we added the value just now - keep this value
                        lockfree::map::Preview::Keep
                    } else {
                        // there is not the value in the map - try to add.
                        // If other thread adding value the same time - the closure will be recalled
                        lockfree::map::Preview::New(loaded_block_handle.clone())
                    }
                });
                let block_handle = match result {
                    lockfree::map::Insertion::Created => {
                        // block handle we loaded now was added - use it
                        loaded_block_handle
                    },
                    lockfree::map::Insertion::Updated(_) => {
                        // unreachable situation - all updates must be discarded
                        unreachable!("load_block_handle: unreachable Insertion::Updated")
                    },
                    lockfree::map::Insertion::Failed(_) => {
                        // othre thread's block handle was added - get it and use
                        self.block_handle_cache.get(&ton_api_id).unwrap().val().clone()
                    }
                };

            Ok(block_handle)
        }
    }
}

impl InternalDb for InternalDbImpl {

    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Arc<BlockHandle>> {
        log::trace!("load_block_handle {}", id);
        let handle = self.load_block_handle_impl(id)?;
        Ok(handle)
    }

    fn store_block_data(&self, handle: &BlockHandle, block: &BlockStuff) -> Result<()> {
        log::trace!("store_block_data {}", block.id());
        if handle.id() != block.id() {
            fail!(NodeError::InvalidArg("`block` and `handle` mismatch".to_string()))
        }
        if !handle.data_inited() {
            self.block_db.put(&block.id().into(), block.data())?;
            if block.id().shard().is_masterchain() {
                let id_bytes = bincode::serialize(
                    &convert_block_id_ext_blk2api(block.id())
                )?;
                self.mc_blocks_id_index.put(&BlockSeqno::new(block.id().seq_no()), &id_bytes)?;
            }
            handle.set_data_inited(block)?;
            self.store_block_handle(handle)?;
        }
        Ok(())
    }

    fn load_block_data(&self, id: &BlockIdExt) -> Result<BlockStuff> {
        log::trace!("load_block_data {}", id);
        let raw_block = self.block_db.get(&id.into())?;
        let block = BlockStuff::deserialize(&raw_block)?;
        if block.id() != id {
            fail!(NodeError::InvalidData("Downloaded block has invalid id".to_string()))
        } else {
            Ok(block)
        }
    }

    fn find_block_by_seq_no(&self, acc_pfx: &AccountIdPrefixFull, seq_no: u32) -> Result<Arc<BlockHandle>> {
        log::trace!("find_block_by_seq_no {} {}", acc_pfx, seq_no);
        if !acc_pfx.is_masterchain() {
            unimplemented!();
        } else {
            let id_bytes = self.mc_blocks_id_index.get(&BlockSeqno::new(seq_no))?;
            let id = convert_block_id_ext_api2blk(
                &bincode::deserialize::<ton_api::ton::ton_node::blockidext::BlockIdExt>(&id_bytes)?
            )?;
            self.load_block_handle(&id)
        }
    }

    fn find_block_by_unix_time(&self, _acc_pfx: &AccountIdPrefixFull, _utime: u32) -> Result<Arc<BlockHandle>> {
        unimplemented!();
    }

    fn find_block_by_lt(&self, _acc_pfx: &AccountIdPrefixFull, _lt: u64) -> Result<Arc<BlockHandle>> {
        unimplemented!();
    }

    fn store_block_proof(&self, handle: &BlockHandle, proof: &BlockProofStuff) -> Result<()> {
        log::trace!("store_block_proof {}", proof.id());

        if handle.id() != proof.id() {
            fail!(NodeError::InvalidArg("`proof` and `handle` mismatch".to_string()))
        }

        if proof.is_link() {
            if !handle.proof_link_inited() {
                self.block_proof_link_db.put(&proof.id().into(), &proof.serialize()?)?;
                handle.set_proof_link_inited(proof)?;
                self.store_block_handle(handle)?;
            }
        } else {
            if !handle.proof_inited() {
                self.block_proof_db.put(&proof.id().into(), &proof.serialize()?)?;
                handle.set_proof_inited(proof)?;
                self.store_block_handle(handle)?;
            }
        }
        Ok(())
    }

    fn load_block_proof(&self, id: &BlockIdExt, is_link: bool) -> Result<BlockProofStuff> {
        log::trace!("load_block_proof {}", id);
        let raw_proof = if is_link {
            self.block_proof_link_db.get(&id.into())?
        } else {
            self.block_proof_db.get(&id.into())?
        };
        let proof = BlockProofStuff::deserialize(id, &raw_proof, is_link)?;
        Ok(proof)
    }

    fn store_shard_state_dynamic(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<()> {
        log::trace!("store_shard_state_dynamic {}", state.block_id());
        if handle.id() != state.block_id() {
            fail!(NodeError::InvalidArg("`state` and `handle` mismatch".to_string()))
        }
        if !handle.state_inited() {
            self.shard_state_dynamic_db.put(&state.block_id().into(), state.root_cell().clone())?;
            //self.ss_test_map.insert(state.block_id().clone(), state.clone());

            handle.set_state_inited();
            self.store_block_handle(handle)?;
        }
        Ok(())
    }

    fn load_shard_state_dynamic(&self, id: &BlockIdExt) -> Result<ShardStateStuff> {
        log::trace!("load_shard_state_dynamic {}", id);

        Ok(ShardStateStuff::new(id.clone(), self.shard_state_dynamic_db.get(&id.into())?)?)
        //Ok(self.ss_test_map.get(id).ok_or_else(|| error!("no state found"))?.val().clone()
    }

    fn store_shard_state_persistent(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<()> {
        log::trace!("store_shard_state_persistent {}", state.block_id());
        if handle.id() != state.block_id() {
            fail!(NodeError::InvalidArg("`state` and `handle` mismatch".to_string()))
        }
        if !handle.persistent_state_inited() {
            self.shard_state_persistent_db.put(&state.block_id().into(), &state.serialize()?)?;
            handle.set_persistent_state_inited();
            self.store_block_handle(handle)?;
        }
        Ok(())
    }

    fn load_shard_state_persistent(&self, id: &BlockIdExt) -> Result<ShardStateStuff>{
        log::trace!("load_shard_state_persistent {}", id);
        let raw_state = self.shard_state_persistent_db.get(&id.into())?;
        let state = ShardStateStuff::deserialize(id.clone(), &raw_state)?;
        Ok(state)
    }

    fn store_block_prev(&self, handle: &BlockHandle, prev: &BlockIdExt) -> Result<()> {
        log::trace!("store_block_prev {}", handle.id());
        if !handle.prev1_inited() {
            let value = bincode::serialize(&convert_block_id_ext_blk2api(prev))?;
            self.prev_block_db.put(&handle.id().into(), &value)?;

            handle.set_prev1_inited();
            self.store_block_handle(handle)?;
        }
        Ok(())
    }

    fn load_block_prev(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        log::trace!("load_block_prev {}", id);
        let bytes = self.prev_block_db.get(&id.into())?;
        let prev = bincode::deserialize::<ton_api::ton::ton_node::blockidext::BlockIdExt>(&bytes)?;
        convert_block_id_ext_api2blk(&prev)
    }

    fn store_block_prev2(&self, handle: &BlockHandle, prev2: &BlockIdExt) -> Result<()> {
        log::trace!("store_block_prev2 {}", handle.id());
        if !handle.prev2_inited() {
            let value = bincode::serialize(&convert_block_id_ext_blk2api(prev2))?;
            self.prev2_block_db.put(&handle.id().into(), &value)?;

            handle.set_prev2_inited();
            self.store_block_handle(handle)?;
        }
        Ok(())
    }

    fn load_block_prev2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        log::trace!("load_block_prev2 {}", id);
        let bytes = self.prev2_block_db.get(&id.into())?;
        let prev2 = bincode::deserialize::<ton_api::ton::ton_node::blockidext::BlockIdExt>(&bytes)?;
        convert_block_id_ext_api2blk(&prev2)
    }

    fn store_block_next1(&self, handle: &BlockHandle, next: &BlockIdExt) -> Result<()> {
        log::trace!("store_block_next1 {}", handle.id());
        if !handle.next1_inited() {
            let value = bincode::serialize(&convert_block_id_ext_blk2api(next))?;
            self.next_block_db.put(&handle.id().into(), &value)?;

            handle.set_next1_inited();
            self.store_block_handle(handle)?;
        }
        Ok(())
    }

    fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        log::trace!("load_block_next1 {}", id);
        let bytes = self.next_block_db.get(&id.into())?;
        let next = bincode::deserialize::<ton_api::ton::ton_node::blockidext::BlockIdExt>(&bytes)?;
        convert_block_id_ext_api2blk(&next)
    }

    fn store_block_next2(&self, handle: &BlockHandle, next2: &BlockIdExt) -> Result<()> {
        log::trace!("store_block_next2 {}", handle.id());
        if !handle.next2_inited() {
            let value = bincode::serialize(&convert_block_id_ext_blk2api(next2))?;
            self.next2_block_db.put(&handle.id().into(), &value)?;

            handle.set_next2_inited();
            self.store_block_handle(handle)?;
        }
        Ok(())
    }

    fn load_block_next2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        log::trace!("load_block_next2 {}", id);
        let bytes = self.next2_block_db.get(&id.into())?;
        let next2 = bincode::deserialize::<ton_api::ton::ton_node::blockidext::BlockIdExt>(&bytes)?;
        convert_block_id_ext_api2blk(&next2)
    }

    fn store_block_applied(&self, id: &BlockIdExt) -> Result<()> {
        log::trace!("store_block_applied {}", id);

        let handle = self.load_block_handle_impl(id)?;
        if !handle.applied() {
            handle.set_applied();
            self.store_block_handle(&handle)?;
        }
        Ok(())
    }

    fn store_block_processed_in_ext_db(&self, handle: &BlockHandle) -> Result<()> {
        log::trace!("store_block_processed_in_ext_db {}", handle.id);
        if !handle.processed_in_ext_db() {
            handle.set_processed_in_ext_db();
            self.store_block_handle(handle)?;
        }
        Ok(())
    }

    fn store_node_state(&self, key: &'static str, value: Vec<u8>) -> Result<()> {
        log::trace!("store_node_state {}", key);
        self.node_state_db.put(&key, &value)?;
        Ok(())
    }

    fn load_node_state(&self, key: &'static str) -> Result<Vec<u8>> {
        log::trace!("load_node_state {}", key);
        let value = self.node_state_db.get(&key)?;
        Ok(value.as_ref().to_vec()) // TODO try not to copy
    }
}
