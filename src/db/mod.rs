use crate::{
    block::{convert_block_id_ext_blk2api, convert_block_id_ext_api2blk, BlockStuff},
    error::NodeError, 
    shard_state::ShardStateStuff,
    block_proof::BlockProofStuff,
};

use std::{
    path::Path, sync::Arc, cmp::max
};
use ton_block::{BlockIdExt, AccountIdPrefixFull};
use ton_node_storage::{
    block_db::BlockDb, block_info_db::BlockInfoDb, shardstate_persistent_db::ShardStatePersistentDb,
    node_state_db::NodeStateDb, shardstate_db::ShardStateDb, shardstate_db,
    db::{rocksdb::RocksDb, traits::{KvcWriteable, DbKey}},
};
use ton_types::{error, fail, Result};

pub mod block_handle;
pub use block_handle::BlockHandle;


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
        #[derive(Default, serde::Serialize, serde::Deserialize)]
        pub struct $name(pub $type);
        impl NodeState for $name {
            fn get_key() -> &'static str { $key }
        }
    };
}

#[async_trait::async_trait]
pub trait InternalDb : Sync + Send {
    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Arc<BlockHandle>>;

    fn store_block_data(&self, handle: &BlockHandle, block: &BlockStuff) -> Result<()>;
    fn load_block_data(&self, id: &BlockIdExt) -> Result<BlockStuff>;
    fn load_block_data_raw(&self, id: &BlockIdExt) -> Result<Vec<u8>>;

    fn find_block_by_seq_no(&self, acc_pfx: &AccountIdPrefixFull, seqno: u32) -> Result<Arc<BlockHandle>>;
    fn find_block_by_unix_time(&self, acc_pfx: &AccountIdPrefixFull, utime: u32) -> Result<Arc<BlockHandle>>;
    fn find_block_by_lt(&self, acc_pfx: &AccountIdPrefixFull, lt: u64) -> Result<Arc<BlockHandle>>;

    fn store_block_proof(&self, handle: &BlockHandle, proof: &BlockProofStuff) -> Result<()>;
    fn load_block_proof(&self, id: &BlockIdExt, is_link: bool) -> Result<BlockProofStuff>;
    fn load_block_proof_raw(&self, id: &BlockIdExt, is_link: bool) -> Result<Vec<u8>>;

    fn store_shard_state_dynamic(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<()>;
    fn load_shard_state_dynamic(&self, id: &BlockIdExt) -> Result<ShardStateStuff>;
    fn gc_shard_state_dynamic_db(&self) -> Result<usize>;

    async fn store_shard_state_persistent(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<()>;
    async fn store_shard_state_persistent_raw(&self, handle: &BlockHandle, state_data: &Vec<u8>) -> Result<()>;
    async fn load_shard_state_persistent_slice(&self, id: &BlockIdExt, offset: u64, length: u64) -> Result<Vec<u8>>;
    async fn load_shard_state_persistent_size(&self, id: &BlockIdExt) -> Result<u64>;

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
    block_handle_db: Arc<BlockInfoDb>,
    block_proof_db: BlockInfoDb,
    block_proof_link_db: BlockInfoDb,
    prev_block_db: BlockInfoDb,
    prev2_block_db: BlockInfoDb,
    next_block_db: BlockInfoDb,
    next2_block_db: BlockInfoDb,
    node_state_db: NodeStateDb,
    shard_state_persistent_db: ShardStatePersistentDb,
    shard_state_dynamic_db: ShardStateDb,
    block_handle_cache: lockfree::map::Map<ton_api::ton::ton_node::blockidext::BlockIdExt, Arc<BlockHandle>>,
    mc_blocks_id_index: Box<dyn KvcWriteable<BlockSeqno>>,
    //ss_test_map: lockfree::map::Map<BlockIdExt, ShardStateStuff>,
    shardstate_db_gc: shardstate_db::GC,
}

impl InternalDbImpl {
    pub fn new(config: InternalDbConfig) -> Result<Self> {
        let block_handle_db = Arc::new(BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "block_handle_db")));
        let shard_state_dynamic_db = ShardStateDb::with_paths(
            &Self::build_name(&config.db_directory, "states_index_db"),
            &Self::build_name(&config.db_directory, "cells_db")
        );
        let shardstate_db_gc = shardstate_db::GC::new(&shard_state_dynamic_db, Arc::clone(&block_handle_db));
        Ok(
            Self {
                block_db: BlockDb::with_path(&Self::build_name(&config.db_directory, "block_db")),
                block_handle_db,
                block_proof_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "block_proof_db")),
                block_proof_link_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "block_proof_link_db")),
                prev_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "prev1_block_db")),
                prev2_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "prev2_block_db")),
                next_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "next1_block_db")),
                next2_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "next2_block_db")),
                node_state_db: NodeStateDb::with_path(&Self::build_name(&config.db_directory, "node_state_db")),
                shard_state_persistent_db: ShardStatePersistentDb::with_path(
                    &Self::build_name(&config.db_directory, "shard_state_persistent_db")),
                shard_state_dynamic_db,
                block_handle_cache: lockfree::map::Map::new(),
                mc_blocks_id_index: Box::new(RocksDb::with_path(&Self::build_name(&config.db_directory, "mc_blocks_id_index"))),
                //ss_test_map: lockfree::map::Map::new(),
                shardstate_db_gc,
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

#[async_trait::async_trait]
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
        let raw_block = self.load_block_data_raw(id)?;
        BlockStuff::deserialize(id.clone(), raw_block.to_vec())
    }

    fn load_block_data_raw(&self, id: &BlockIdExt) -> Result<Vec<u8>> {
        log::trace!("load_block_data_raw {}", id);
        let raw_block = self.block_db.get(&id.into())?;
        Ok(raw_block.to_vec())
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
        if !cfg!(feature = "local_test") {
            log::trace!("store_block_proof {}", proof.id());

            if handle.id() != proof.id() {
                fail!(NodeError::InvalidArg("`proof` and `handle` mismatch".to_string()))
            }

            if proof.is_link() {
                if !handle.proof_link_inited() {
                    self.block_proof_link_db.put(&proof.id().into(), proof.data())?;
                    handle.set_proof_link_inited(proof)?;
                    self.store_block_handle(handle)?;
                }
            } else {
                if !handle.proof_inited() {
                    self.block_proof_db.put(&proof.id().into(), proof.data())?;
                    handle.set_proof_inited(proof)?;
                    self.store_block_handle(handle)?;
                }
            }
        }
        Ok(())
    }

    fn load_block_proof(&self, id: &BlockIdExt, is_link: bool) -> Result<BlockProofStuff> {
        log::trace!("load_block_proof {} {}", if is_link {"link"} else {""}, id);
        let raw_proof = self.load_block_proof_raw(id, is_link)?;
        let proof = BlockProofStuff::deserialize(id, raw_proof, is_link)?;
        Ok(proof)
    }

    fn load_block_proof_raw(&self, id: &BlockIdExt, is_link: bool) -> Result<Vec<u8>> {
        log::trace!("load_block_proof_raw {} {}", if is_link {"link"} else {""}, id);
        let raw_proof = if is_link {
            self.block_proof_link_db.get(&id.into())?
        } else {
            self.block_proof_db.get(&id.into())?
        };
        Ok(raw_proof.to_vec())
    }

    fn store_shard_state_dynamic(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<()> {
        log::trace!("store_shard_state_dynamic {}", state.block_id());
        if handle.id() != state.block_id() {
            fail!(NodeError::InvalidArg("`state` and `handle` mismatch".to_string()))
        }
        if !handle.state_inited() {
            self.shard_state_dynamic_db.put(&state.block_id().into(), state.root_cell().clone())?;
            //self.ss_test_map.insert(state.block_id().clone(), state.clone());

            handle.set_gen_utime(state.shard_state().gen_time())?;
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

    async fn store_shard_state_persistent(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<()> {
        log::trace!("store_shard_state_persistent {}", state.block_id());
        if handle.id() != state.block_id() {
            fail!(NodeError::InvalidArg("`state` and `handle` mismatch".to_string()))
        }
        if !handle.persistent_state_inited() {
            self.shard_state_persistent_db.put(&state.block_id().into(), &state.serialize()?).await?;
            handle.set_persistent_state_inited();
            self.store_block_handle(handle)?;
        }
        Ok(())
    }

    async fn store_shard_state_persistent_raw(&self, handle: &BlockHandle, state_data: &Vec<u8>) -> Result<()> {
        log::trace!("store_shard_state_persistent_raw {}", handle.id());
        if !handle.persistent_state_inited() {
            self.shard_state_persistent_db.put(&handle.id().into(), state_data).await?;
            handle.set_persistent_state_inited();
            self.store_block_handle(handle)?;
        }
        Ok(())
    }

    async fn load_shard_state_persistent_slice(&self, id: &BlockIdExt, offset: u64, length: u64) -> Result<Vec<u8>> {
        log::trace!("load_shard_state_persistent_slice {}", id);
        let full_lenth = self.load_shard_state_persistent_size(id).await?;
        if offset > full_lenth {
            fail!("offset is greater than full length");
        }
        if offset == full_lenth {
            Ok(vec![])
        } else {
            let length = max(length, full_lenth - offset);
            let db_slice = self.shard_state_persistent_db.get_slice(&id.into(), offset, length).await?;
            Ok(db_slice.to_vec())
        }
    }

    async fn load_shard_state_persistent_size(&self, id: &BlockIdExt) -> Result<u64> {
        log::trace!("load_shard_state_persistent_slice {}", id);
        self.shard_state_persistent_db.get_size(&id.into()).await
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
        log::trace!("store_block_processed_in_ext_db {}", handle.id());
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

    fn gc_shard_state_dynamic_db(&self) -> Result<usize> {
        self.shardstate_db_gc.collect()
    }
}
