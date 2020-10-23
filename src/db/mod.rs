use crate::{
    block::{convert_block_id_ext_blk2api, convert_block_id_ext_api2blk, BlockStuff},
    error::NodeError,
    shard_state::ShardStateStuff,
    block_proof::BlockProofStuff,
};

use std::{
    path::{Path, PathBuf}, sync::Arc, cmp::max
};
use ton_api::ton::PublicKey;
use ton_block::{BlockIdExt, AccountIdPrefixFull, UnixTime32};
use ton_node_storage::{
    block_handle_db::BlockHandleDb,
    block_index_db::BlockIndexDb,
    block_info_db::BlockInfoDb,
    node_state_db::NodeStateDb,
    shardstate_db::ShardStateDb,
    status_db::StatusDb,
    shardstate_db,
    shardstate_persistent_db::ShardStatePersistentDb,
};
use ton_types::{error, fail, Result, UInt256};

pub mod block_handle;
pub use block_handle::BlockHandle;
use ton_node_storage::archives::{archive_manager::ArchiveManager, package_entry_id::PackageEntryId};

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

    async fn store_block_data(&self, handle: &BlockHandle, block: &BlockStuff) -> Result<()>;
    async fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff>;
    async fn load_block_data_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>>;

    fn find_block_by_seq_no(&self, acc_pfx: &AccountIdPrefixFull, seqno: u32) -> Result<Arc<BlockHandle>>;
    fn find_block_by_unix_time(&self, acc_pfx: &AccountIdPrefixFull, utime: u32) -> Result<Arc<BlockHandle>>;
    fn find_block_by_lt(&self, acc_pfx: &AccountIdPrefixFull, lt: u64) -> Result<Arc<BlockHandle>>;

    async fn store_block_proof(&self, handle: &BlockHandle, proof: &BlockProofStuff) -> Result<()>;
    async fn load_block_proof(&self, handle: &BlockHandle, is_link: bool) -> Result<BlockProofStuff>;
    async fn load_block_proof_raw(&self, handle: &BlockHandle, is_link: bool) -> Result<Vec<u8>>;

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
    async fn store_block_applied(&self, handle: &BlockHandle) -> Result<()>;

    async fn archive_block(&self, id: &BlockIdExt) -> Result<()>;

    fn store_node_state(&self, key: &'static str, value: Vec<u8>) -> Result<()>;
    fn load_node_state(&self, key: &'static str) -> Result<Vec<u8>>;

    fn archive_manager(&self) -> &Arc<ArchiveManager>;

    fn index_handle(&self, handle: &BlockHandle) -> Result<()>;
    fn assign_mc_ref_seq_no(&self, handle: &BlockHandle, mc_seq_no: u32) -> Result<()>;
}

type BlockHandleCache = Arc<lockfree::map::Map<BlockIdExt, Arc<BlockHandle>>>;

#[derive(serde::Deserialize)]
pub struct InternalDbConfig {
    pub db_directory: String,
}

pub struct InternalDbImpl {
    block_handle_db: Arc<BlockHandleDb>,
    block_index_db: Arc<BlockIndexDb>,
    prev_block_db: BlockInfoDb,
    prev2_block_db: BlockInfoDb,
    next_block_db: BlockInfoDb,
    next2_block_db: BlockInfoDb,
    node_state_db: NodeStateDb,
    shard_state_persistent_db: ShardStatePersistentDb,
    shard_state_dynamic_db: ShardStateDb,
    block_handle_cache: BlockHandleCache,
    //ss_test_map: lockfree::map::Map<BlockIdExt, ShardStateStuff>,
    shardstate_db_gc: shardstate_db::GC,
    archive_manager: Arc<ArchiveManager>,
}

impl InternalDbImpl {
    pub async fn new(config: InternalDbConfig) -> Result<Self> {
        let status_db = Arc::new(StatusDb::with_path(&Self::build_name(&config.db_directory, "status_db")));
        let block_index_db = Arc::new(BlockIndexDb::with_paths(
            &Self::build_name(&config.db_directory, "index_db/lt_desc_db"),
            &Self::build_name(&config.db_directory, "index_db/lt_db"),
            &Self::build_name(&config.db_directory, "index_db/lt_shard_db"),
            Arc::clone(&status_db),
        ));
        let block_handle_db = Arc::new(
            BlockHandleDb::with_path(
                &Self::build_name(&config.db_directory, "block_handle_db"),
            )
        );
        let shard_state_dynamic_db = ShardStateDb::with_paths(
            &Self::build_name(&config.db_directory, "shardstate_db"),
            &Self::build_name(&config.db_directory, "cells_db")
        );
        let shardstate_db_gc = shardstate_db::GC::new(&shard_state_dynamic_db, Arc::clone(&block_handle_db));
        let archive_manager = Arc::new(ArchiveManager::with_data(Arc::new(PathBuf::from(&config.db_directory))).await?);
        Ok(
            Self {
                block_handle_db,
                block_index_db,
                prev_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "prev1_block_db")),
                prev2_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "prev2_block_db")),
                next_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "next1_block_db")),
                next2_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "next2_block_db")),
                node_state_db: NodeStateDb::with_path(&Self::build_name(&config.db_directory, "node_state_db")),
                shard_state_persistent_db: ShardStatePersistentDb::with_path(
                    &Self::build_name(&config.db_directory, "shard_state_persistent_db")),
                shard_state_dynamic_db,
                block_handle_cache: BlockHandleCache::default(),
                //ss_test_map: lockfree::map::Map::new(),
                shardstate_db_gc,
                archive_manager,
            }
            // Self {
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
        self.block_handle_db.put_value(&handle.id().into(), handle.meta())?;
        Ok(())
    }

    fn load_or_create_handle(&self, id: &BlockIdExt) -> Result<Arc<BlockHandle>> {
        Ok(Arc::new(match self.block_handle_db.try_get_value(&id.into())? {
            None => BlockHandle::new(id.clone(), Arc::clone(&self.block_handle_cache)),
            Some(block_meta) => BlockHandle::with_values(id.clone(), block_meta, Arc::clone(&self.block_handle_cache)),
        }))
    }


    fn load_block_handle_impl(&self, id: &BlockIdExt) -> Result<Arc<BlockHandle>> {
        log::trace!("load_block_handle_impl {}", id);

        adnl::common::add_object_to_map_with_update(&self.block_handle_cache, id.clone(), |val| {
            if val.is_some() {
                Ok(None)
            } else {
                Some(self.load_or_create_handle(id)).transpose()
            }
        })?;

        Ok(self.block_handle_cache
            .get(id)
            .ok_or_else(|| error!("unexpected error in load_block_handle_impl"))?
            .1.clone()
        )
    }
}

#[async_trait::async_trait]
impl InternalDb for InternalDbImpl {

    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Arc<BlockHandle>> {
        log::trace!("load_block_handle {}", id);
        let handle = self.load_block_handle_impl(id)?;
        Ok(handle)
    }

    async fn store_block_data(&self, handle: &BlockHandle, block: &BlockStuff) -> Result<()> {
        log::trace!("store_block_data {}", block.id());
        if handle.id() != block.id() {
            fail!(NodeError::InvalidArg("`block` and `handle` mismatch".to_string()))
        }
        if !handle.data_inited() {
            handle.fetch_block_info(block)?;
            self.store_block_handle(handle)?;
            let entry_id = PackageEntryId::<_, UInt256, PublicKey>::Block(block.id());
            self.archive_manager.add_file(&entry_id, block.data().to_vec()).await?;
            if !handle.set_data_inited() {
                self.store_block_handle(handle)?;
            }
        }
        Ok(())
    }

    async fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        log::trace!("load_block_data {}", handle.id());
        let raw_block = self.load_block_data_raw(handle).await?;
        BlockStuff::deserialize(handle.id().clone(), raw_block.to_vec())
    }

    async fn load_block_data_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        log::trace!("load_block_data_raw {}", handle.id());
        if !handle.data_inited() {
            fail!("This block is not stored yet: {:?}", handle);
        }
        let entry_id = PackageEntryId::<_, UInt256, PublicKey>::Block(handle.id());
        self.archive_manager.get_file(handle.id(), handle.meta(), &entry_id).await
    }

    fn find_block_by_seq_no(&self, acc_pfx: &AccountIdPrefixFull, seq_no: u32) -> Result<Arc<BlockHandle>> {
        self.load_block_handle(&self.block_index_db.get_block_by_seq_no(acc_pfx, seq_no)?)
    }

    fn find_block_by_unix_time(&self, acc_pfx: &AccountIdPrefixFull, utime: u32) -> Result<Arc<BlockHandle>> {
        self.load_block_handle(&self.block_index_db.get_block_by_ut(acc_pfx, UnixTime32(utime))?)
    }

    fn find_block_by_lt(&self, acc_pfx: &AccountIdPrefixFull, lt: u64) -> Result<Arc<BlockHandle>> {
        self.load_block_handle(&self.block_index_db.get_block_by_lt(acc_pfx, lt)?)
    }

    async fn store_block_proof(&self, handle: &BlockHandle, proof: &BlockProofStuff) -> Result<()> {
        if !cfg!(feature = "local_test") {
            log::trace!("store_block_proof {}", proof.id());

            if handle.id() != proof.id() {
                fail!(NodeError::InvalidArg("`proof` and `handle` mismatch".to_string()))
            }

            if proof.is_link() {
                if !handle.proof_link_inited() {
                    handle.fetch_proof_info(proof)?;
                    self.store_block_handle(handle)?;
                    let entry_id = PackageEntryId::<_, UInt256, PublicKey>::ProofLink(handle.id());
                    self.archive_manager.add_file(&entry_id, proof.data().to_vec()).await?;
                    if !handle.set_proof_link_inited() {
                        self.store_block_handle(handle)?;
                    }
                }
            } else {
                if !handle.proof_inited() {
                    handle.fetch_proof_info(proof)?;
                    self.store_block_handle(handle)?;
                    let entry_id = PackageEntryId::<_, UInt256, PublicKey>::Proof(handle.id());
                    self.archive_manager.add_file(&entry_id, proof.data().to_vec()).await?;
                    if !handle.set_proof_inited() {
                        self.store_block_handle(handle)?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn load_block_proof(&self, handle: &BlockHandle, is_link: bool) -> Result<BlockProofStuff> {
        log::trace!("load_block_proof {} {}", if is_link {"link"} else {""}, handle.id());
        let raw_proof = self.load_block_proof_raw(handle, is_link).await?;
        BlockProofStuff::deserialize(handle.id(), raw_proof, is_link)
    }

    async fn load_block_proof_raw(&self, handle: &BlockHandle, is_link: bool) -> Result<Vec<u8>> {
        log::trace!("load_block_proof_raw {} {}", if is_link {"link"} else {""}, handle.id());
        let (entry_id, inited) = if is_link {
            (PackageEntryId::<_, UInt256, PublicKey>::ProofLink(handle.id()), handle.proof_link_inited())
        } else {
            (PackageEntryId::<_, UInt256, PublicKey>::Proof(handle.id()), handle.proof_inited())
        };
        if !inited {
            fail!("This proof{} is not in the archive: {:?}", if is_link { "link" } else { "" }, handle);
        }
        self.archive_manager.get_file(handle.id(), handle.meta(), &entry_id).await
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
    }

    fn gc_shard_state_dynamic_db(&self) -> Result<usize> {
        self.shardstate_db_gc.collect()
    }

    async fn store_shard_state_persistent(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<()> {
        log::trace!("store_shard_state_persistent {}", state.block_id());
        if handle.id() != state.block_id() {
            fail!(NodeError::InvalidArg("`state` and `handle` mismatch".to_string()))
        }
        if !handle.persistent_state_inited() {
            self.shard_state_persistent_db.put(&state.block_id().into(), &state.serialize()?).await?;
            if !handle.set_persistent_state_inited() {
                self.store_block_handle(handle)?;
            }
        }
        Ok(())
    }

    async fn store_shard_state_persistent_raw(&self, handle: &BlockHandle, state_data: &Vec<u8>) -> Result<()> {
        log::trace!("store_shard_state_persistent_raw {}", handle.id());
        if !handle.persistent_state_inited() {
            self.shard_state_persistent_db.put(&handle.id().into(), state_data).await?;
            if !handle.set_persistent_state_inited() {
                self.store_block_handle(handle)?;
            }
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

            if !handle.set_prev1_inited() {
                self.store_block_handle(handle)?;
            }
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

            if !handle.set_prev2_inited() {
                self.store_block_handle(handle)?;
            }
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

            if !handle.set_next1_inited() {
                self.store_block_handle(handle)?;
            }
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

            if !handle.set_next2_inited() {
                self.store_block_handle(handle)?;
            }
        }
        Ok(())
    }

    fn load_block_next2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        log::trace!("load_block_next2 {}", id);
        let bytes = self.next2_block_db.get(&id.into())?;
        let next2 = bincode::deserialize::<ton_api::ton::ton_node::blockidext::BlockIdExt>(&bytes)?;
        convert_block_id_ext_api2blk(&next2)
    }

    fn store_block_processed_in_ext_db(&self, handle: &BlockHandle) -> Result<()> {
        log::trace!("store_block_processed_in_ext_db {}", handle.id());
        if !handle.set_processed_in_ext_db() {
            self.store_block_handle(handle)?;
        }
        Ok(())
    }

    async fn store_block_applied(&self, handle: &BlockHandle) -> Result<()> {
        log::trace!("store_block_applied {}", handle.id());

        if !handle.set_applied() {
            self.store_block_handle(&handle)?;
        }
        Ok(())
    }

    async fn archive_block(&self, id: &BlockIdExt) -> Result<()> {
        log::trace!("store_block_applied {}", id);

        let handle = self.load_block_handle_impl(id)?;
        if handle.moved_to_archive() {
            return Ok(());
        }

        self.archive_manager.move_to_archive(
            id,
            handle.meta(),
            || {
                if !handle.set_moved_to_archive() {
                    self.store_block_handle(&handle)?;
                }
                Ok(())
            }
        ).await.unwrap_or_else(|err|
            log::error!(
                target: "storage",
                "Failed to move block to archive: {}. Error: {}",
                id,
                err
            )
        );

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

    fn archive_manager(&self) -> &Arc<ArchiveManager> {
        &self.archive_manager
    }

    fn index_handle(&self, handle: &BlockHandle) -> Result<()> {
        if !handle.set_indexed() {
            self.block_index_db.add_handle(&handle.id().into(), handle.meta())?;
            self.store_block_handle(&handle)?;
        }

        Ok(())
    }

    fn assign_mc_ref_seq_no(&self, handle: &BlockHandle, mc_seq_no: u32) -> Result<()> {
        if handle.set_masterchain_ref_seq_no(mc_seq_no) != mc_seq_no {
            self.store_block_handle(handle)?;
        }

        Ok(())
    }
}

