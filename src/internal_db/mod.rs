use crate::{
    block::{convert_block_id_ext_blk2api, convert_block_id_ext_api2blk, BlockStuff},
    block_proof::BlockProofStuff, error::NodeError, shard_state::ShardStateStuff,
    types::top_block_descr::{TopBlockDescrId, TopBlockDescrStuff},
};
use std::{path::PathBuf, sync::Arc, cmp::min, collections::HashMap};
use storage::{
    archives::{archive_manager::ArchiveManager, package_entry_id::PackageEntryId},
    block_handle_db::{BlockHandleDb, BlockHandleStorage}, block_index_db::BlockIndexDb, 
    block_info_db::BlockInfoDb, node_state_db::NodeStateDb, shardstate_db::{GC, ShardStateDb},
    shardstate_persistent_db::ShardStatePersistentDb, types::{BlockHandle, BlockMeta},
    shard_top_blocks_db::ShardTopBlocksDb,
};
#[cfg(feature = "read_old_db")]
use storage::block_db::BlockDb;
use ton_api::ton::PublicKey;
use ton_block::{Block, BlockIdExt, AccountIdPrefixFull, UnixTime32};
use ton_types::{error, fail, Result, UInt256};

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
    fn create_or_load_block_handle(
        &self, 
        id: &BlockIdExt, 
        block: Option<&Block>,
        utime: Option<u32>
    ) -> Result<Arc<BlockHandle>>;
    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>>;

    async fn store_block_data(&self, block: &BlockStuff) -> Result<Arc<BlockHandle>>;
    async fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff>;
    async fn load_block_data_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>>;

    fn find_block_by_seq_no(&self, acc_pfx: &AccountIdPrefixFull, seqno: u32) -> Result<Arc<BlockHandle>>;
    fn find_block_by_unix_time(&self, acc_pfx: &AccountIdPrefixFull, utime: u32) -> Result<Arc<BlockHandle>>;
    fn find_block_by_lt(&self, acc_pfx: &AccountIdPrefixFull, lt: u64) -> Result<Arc<BlockHandle>>;

    async fn store_block_proof(
        &self, 
        id: &BlockIdExt, 
        handle: Option<Arc<BlockHandle>>, 
        proof: &BlockProofStuff
    ) -> Result<Arc<BlockHandle>>;
    async fn load_block_proof(&self, handle: &BlockHandle, is_link: bool) -> Result<BlockProofStuff>;
    async fn load_block_proof_raw(&self, handle: &BlockHandle, is_link: bool) -> Result<Vec<u8>>;

    fn store_shard_state_dynamic(
        &self, 
        handle: &Arc<BlockHandle>, 
        state: &ShardStateStuff
    ) -> Result<()>;
    fn load_shard_state_dynamic(&self, id: &BlockIdExt) -> Result<ShardStateStuff>;
    fn gc_shard_state_dynamic_db(&self) -> Result<usize>;

    async fn store_shard_state_persistent(&self, handle: &Arc<BlockHandle>, state: &ShardStateStuff) -> Result<()>;
    async fn store_shard_state_persistent_raw(&self, handle: &Arc<BlockHandle>, state_data: &[u8]) -> Result<()>;
    async fn load_shard_state_persistent_slice(&self, id: &BlockIdExt, offset: u64, length: u64) -> Result<Vec<u8>>;
    async fn load_shard_state_persistent_size(&self, id: &BlockIdExt) -> Result<u64>;

    fn store_block_prev1(&self, handle: &Arc<BlockHandle>, prev: &BlockIdExt) -> Result<()>;
    fn load_block_prev1(&self, id: &BlockIdExt) -> Result<BlockIdExt>;

    fn store_block_prev2(&self, handle: &Arc<BlockHandle>, prev2: &BlockIdExt) -> Result<()>;
    fn load_block_prev2(&self, id: &BlockIdExt) -> Result<BlockIdExt>;

    fn store_block_next1(&self, handle: &Arc<BlockHandle>, next: &BlockIdExt) -> Result<()>;
    fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt>;

    fn store_block_next2(&self, handle: &Arc<BlockHandle>, next2: &BlockIdExt) -> Result<()>;
    fn load_block_next2(&self, id: &BlockIdExt) -> Result<BlockIdExt>;

//    fn store_block_processed_in_ext_db(&self, handle: &Arc<BlockHandle>) -> Result<()>;
    fn store_block_applied(&self, handle: &Arc<BlockHandle>) -> Result<()>;

    async fn archive_block(&self, id: &BlockIdExt) -> Result<()>;

    fn store_node_state(&self, key: &'static str, value: Vec<u8>) -> Result<()>;
    fn load_node_state(&self, key: &'static str) -> Result<Vec<u8>>;

    fn archive_manager(&self) -> &Arc<ArchiveManager>;

    fn index_handle(&self, handle: &Arc<BlockHandle>) -> Result<()>;
    fn assign_mc_ref_seq_no(&self, handle: &Arc<BlockHandle>, mc_seq_no: u32) -> Result<()>;


    fn save_top_shard_block(&self, id: &TopBlockDescrId, tsb: &TopBlockDescrStuff) -> Result<()>;
    fn load_all_top_shard_blocks(&self) -> Result<HashMap<TopBlockDescrId, TopBlockDescrStuff>>;
    fn load_all_top_shard_blocks_raw(&self) -> Result<HashMap<TopBlockDescrId, Vec<u8>>>;
    fn remove_top_shard_block(&self, id: &TopBlockDescrId) -> Result<()>;
}

#[derive(serde::Deserialize)]
pub struct InternalDbConfig {
    pub db_directory: String,
}

pub struct InternalDbImpl {
    block_handle_storage: Arc<BlockHandleStorage>,
    block_index_db: Arc<BlockIndexDb>,
    prev_block_db: BlockInfoDb,
    prev2_block_db: BlockInfoDb,
    next_block_db: BlockInfoDb,
    next2_block_db: BlockInfoDb,
    node_state_db: NodeStateDb,
    shard_state_persistent_db: ShardStatePersistentDb,
    shard_state_dynamic_db: ShardStateDb,
    //ss_test_map: lockfree::map::Map<BlockIdExt, ShardStateStuff>,
    shardstate_db_gc: GC,
    archive_manager: Arc<ArchiveManager>,
    shard_top_blocks_db: ShardTopBlocksDb,

    #[cfg(feature = "read_old_db")]
    old_block_db: BlockDb,
    #[cfg(feature = "read_old_db")]
    old_block_proof_db: BlockInfoDb,
    #[cfg(feature = "read_old_db")]
    old_block_proof_link_db: BlockInfoDb,
    
}

impl InternalDbImpl {

    pub async fn new(config: InternalDbConfig) -> Result<Self> {
        let block_index_db = Arc::new(BlockIndexDb::with_paths(
            &Self::build_name(&config.db_directory, "index_db/lt_desc_db"),
            &Self::build_name(&config.db_directory, "index_db/lt_db"),
        ));
        let block_handle_db = Arc::new(
            BlockHandleDb::with_path(
                &Self::build_name(&config.db_directory, "block_handle_db"),
            )
        );
        let block_handle_storage = Arc::new(
            BlockHandleStorage::with_db(Arc::clone(&block_handle_db))
        );
        let shard_state_dynamic_db = ShardStateDb::with_paths(
            &Self::build_name(&config.db_directory, "shardstate_db"),
            &Self::build_name(&config.db_directory, "cells_db")
        );
        let shardstate_db_gc = GC::new(&shard_state_dynamic_db, Arc::clone(&block_handle_db));
        let archive_manager = Arc::new(ArchiveManager::with_data(Arc::new(PathBuf::from(&config.db_directory))).await?);
        Ok(
            Self {
                block_handle_storage,
                block_index_db,
                prev_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "prev1_block_db")),
                prev2_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "prev2_block_db")),
                next_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "next1_block_db")),
                next2_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "next2_block_db")),
                node_state_db: NodeStateDb::with_path(&Self::build_name(&config.db_directory, "node_state_db")),
                shard_state_persistent_db: ShardStatePersistentDb::with_path(
                    &Self::build_name(&config.db_directory, "shard_state_persistent_db")),
                shard_state_dynamic_db,
                //ss_test_map: lockfree::map::Map::new(),
                shardstate_db_gc,
                archive_manager,
                shard_top_blocks_db: ShardTopBlocksDb::with_path(&Self::build_name(&config.db_directory, "shard_top_blocks_db")),

                #[cfg(feature = "read_old_db")]
                old_block_db: BlockDb::with_path(&Self::build_name(&config.db_directory, "block_db")),
                #[cfg(feature = "read_old_db")]
                old_block_proof_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "block_proof_db")),
                #[cfg(feature = "read_old_db")]
                old_block_proof_link_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "block_proof_link_db")),
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

    pub fn build_name(dir: &str, name: &str) -> String {
        format!("{}/{}", dir, name)
    }

    fn store_block_handle(&self, handle: &Arc<BlockHandle>) -> Result<()> {
        self.block_handle_storage.store_handle(handle)
    }

/*
    fn create_trusted_block_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>> {
        log::trace!("create_trusted_block_handle {}", id);
        self.block_handle_storage.create_handle(id.clone(), BlockMeta::with_data(0, 0, 0, 0, true))
    }
*/

}


#[async_trait::async_trait]
impl InternalDb for InternalDbImpl {

    fn create_or_load_block_handle(
        &self, 
        id: &BlockIdExt, 
        block: Option<&Block>,
        utime: Option<u32>
    ) -> Result<Arc<BlockHandle>> {
        if let Some(handle) = self.load_block_handle(id)? {
            return Ok(handle) 
        }
        let meta = if let Some(block) = block {
            BlockMeta::from_block(block)?
        } else if id.seq_no == 0 {
            if let Some(utime) = utime {
                BlockMeta::with_data(0, utime, 0, 0)
            } else {
                fail!("Cannot create handle for zero block {} without UNIX time")
            }
        } else {
            fail!("Cannot create handle for block {} without data", id)
        };
        if let Some(handle) = self.block_handle_storage.create_handle(id.clone(), meta)? {
            return Ok(handle)
        }
        if let Some(handle) = self.load_block_handle(id)? {
            return Ok(handle) 
        }
        fail!("Cannot create handle for block {}", id)
    }

    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>> {
        log::trace!("load_block_handle {}", id);
        self.block_handle_storage.load_handle(id)
    }

    async fn store_block_data(
        &self, 
        block: &BlockStuff
    ) -> Result<Arc<BlockHandle>> {
        log::trace!("store_block_data {}", block.id());
        let handle = self.create_or_load_block_handle(block.id(), Some(block.block()), None)?;
        if !handle.has_data() {
            let _ = handle.block_file_lock().write().await;
            if !handle.has_data() {
                let entry_id = PackageEntryId::<_, UInt256, PublicKey>::Block(block.id());
                self.archive_manager.add_file(&entry_id, block.data().to_vec()).await?;
                if handle.set_data() {
                    self.store_block_handle(&handle)?;
                }
            }
        }
        Ok(handle) 
    }

    async fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        log::trace!("load_block_data {}", handle.id());
        let raw_block = self.load_block_data_raw(handle).await?;
        BlockStuff::deserialize(handle.id().clone(), raw_block)
    }

    async fn load_block_data_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        log::trace!("load_block_data_raw {}", handle.id());
        if !handle.has_data() {
            fail!("This block is not stored yet: {:?}", handle);
        }
        #[cfg(feature = "read_old_db")] {
            let raw_block = self.old_block_db.get(&handle.id().into())?;
            return Ok(raw_block.to_vec());
        }
        #[cfg(not(feature = "read_old_db"))] {
            let entry_id = PackageEntryId::<_, UInt256, PublicKey>::Block(handle.id());
            self.archive_manager.get_file(handle, &entry_id).await
        }
    }

    fn find_block_by_seq_no(&self, acc_pfx: &AccountIdPrefixFull, seq_no: u32) -> Result<Arc<BlockHandle>> {
        log::trace!("find_block_by_seq_no {} {}", acc_pfx, seq_no);
        let id = self.block_index_db.get_block_by_seq_no(acc_pfx, seq_no)?;
        self.load_block_handle(&id)?.ok_or_else(
            || error!("Cannot find handle for block {}", id) 
        )
    }

    fn find_block_by_unix_time(&self, acc_pfx: &AccountIdPrefixFull, utime: u32) -> Result<Arc<BlockHandle>> {
        log::trace!("find_block_by_unix_time {} {}", acc_pfx, utime);
        let id = self.block_index_db.get_block_by_ut(acc_pfx, UnixTime32(utime))?;
        self.load_block_handle(&id)?.ok_or_else(
            || error!("Cannot find handle for block {}", id) 
        )
    }

    fn find_block_by_lt(&self, acc_pfx: &AccountIdPrefixFull, lt: u64) -> Result<Arc<BlockHandle>> {
        log::trace!("find_block_by_lt {} {}", acc_pfx, lt);
        let id = self.block_index_db.get_block_by_lt(acc_pfx, lt)?;
        self.load_block_handle(&id)?.ok_or_else(
            || error!("Cannot find handle for block {}", id) 
        )
    }

    async fn store_block_proof(
        &self, 
        id: &BlockIdExt,
        handle: Option<Arc<BlockHandle>>, 
        proof: &BlockProofStuff
    ) -> Result<Arc<BlockHandle>> {

        if let Some(handle) = &handle {
            if handle.id() != id {
                fail!("Block handle and id mismatch: {} vs {}", handle.id(), id)
            }
        }
        if cfg!(feature = "local_test") {
            if let Some(handle) = handle {
                Ok(handle)
            } else if let Some(handle) = self.load_block_handle(id)? {
                Ok(handle)
            } else {
                fail!("Cannot load handle for block {} during test", id)
            }
        } else {

            log::trace!("store_block_proof {}", proof.id());
            if id != proof.id() {
                fail!(NodeError::InvalidArg("`proof` and `id` mismatch".to_string()))
            }
              
            let handle = if let Some(handle) = handle {
                handle
            } else {
                self.create_or_load_block_handle(id, Some(&proof.virtualize_block()?.0), None)?
            };

            if proof.is_link() {
                if !handle.has_proof_link() {
                    let _ = handle.proof_file_lock().write().await;
                    if !handle.has_proof_link() {
                        let entry_id = PackageEntryId::<_, UInt256, PublicKey>::ProofLink(id);
                        self.archive_manager.add_file(&entry_id, proof.data().to_vec()).await?;
                        if handle.set_proof_link() {
                            self.store_block_handle(&handle)?;
                        }
                    }
                }
            } else {
                if !handle.has_proof() {
                    let _lock = handle.proof_file_lock().write().await;
                    if !handle.has_proof() {
                        let entry_id = PackageEntryId::<_, UInt256, PublicKey>::Proof(id);
                        self.archive_manager.add_file(&entry_id, proof.data().to_vec()).await?;
                        if handle.set_proof() {
                            self.store_block_handle(&handle)?;
                        }
                    }
                }
            }
            Ok(handle)
        }
    }

    async fn load_block_proof(&self, handle: &BlockHandle, is_link: bool) -> Result<BlockProofStuff> {
        log::trace!("load_block_proof {} {}", if is_link {"link"} else {""}, handle.id());
        let raw_proof = self.load_block_proof_raw(handle, is_link).await?;
        BlockProofStuff::deserialize(handle.id(), raw_proof, is_link)
    }

    #[cfg(feature = "read_old_db")]
    async fn load_block_proof_raw(&self, handle: &BlockHandle, is_link: bool) -> Result<Vec<u8>> {
        log::trace!("load_block_proof_raw {} {}", if is_link {"link"} else {""}, handle.id());
        let raw_proof = if is_link {
            self.old_block_proof_link_db.get(&handle.id().into())?
        } else {
            self.old_block_proof_db.get(&handle.id().into())?
        };
        Ok(raw_proof.to_vec())
    }

    #[cfg(not(feature = "read_old_db"))]
    async fn load_block_proof_raw(&self, handle: &BlockHandle, is_link: bool) -> Result<Vec<u8>> {
        log::trace!("load_block_proof_raw {} {}", if is_link {"link"} else {""}, handle.id());
        let (entry_id, inited) = if is_link {
            (PackageEntryId::<_, UInt256, PublicKey>::ProofLink(handle.id()), handle.has_proof_link())
        } else {
            (PackageEntryId::<_, UInt256, PublicKey>::Proof(handle.id()), handle.has_proof())
        };
        if !inited {
            fail!("This proof{} is not in the archive: {:?}", if is_link { "link" } else { "" }, handle);
        }
        self.archive_manager.get_file(handle, &entry_id).await
    }

    fn store_shard_state_dynamic(
        &self, 
        handle: &Arc<BlockHandle>, 
        state: &ShardStateStuff
    ) -> Result<()> {
        log::trace!("store_shard_state_dynamic {}", state.block_id());
        if handle.id() != state.block_id() {
            fail!(NodeError::InvalidArg("`state` and `handle` mismatch".to_string()))
        }
        if !handle.has_state() {
            self.shard_state_dynamic_db.put(state.block_id(), state.root_cell().clone())?;
            //self.ss_test_map.insert(state.block_id().clone(), state.clone());
            if handle.set_state() {
                self.store_block_handle(handle)?;
            }
        }
        Ok(())
    }

    fn load_shard_state_dynamic(&self, id: &BlockIdExt) -> Result<ShardStateStuff> {
        log::trace!("load_shard_state_dynamic {}", id);

        Ok(ShardStateStuff::new(id.clone(), self.shard_state_dynamic_db.get(id)?)?)
    }

    fn gc_shard_state_dynamic_db(&self) -> Result<usize> {
        self.shardstate_db_gc.collect()
    }

    async fn store_shard_state_persistent(
        &self, 
        handle: &Arc<BlockHandle>, 
        state: &ShardStateStuff
    ) -> Result<()> {
        log::trace!("store_shard_state_persistent {}", state.block_id());
        if handle.id() != state.block_id() {
            fail!(NodeError::InvalidArg("`state` and `handle` mismatch".to_string()))
        }
        if !handle.has_persistent_state() {
            self.shard_state_persistent_db.put(state.block_id(), &state.serialize()?).await?;
            if handle.set_persistent_state() {
                self.store_block_handle(handle)?;
            }
        }
        Ok(())
    }

    async fn store_shard_state_persistent_raw(
        &self, 
        handle: &Arc<BlockHandle>, 
        state_data: &[u8]
    ) -> Result<()> {
        log::trace!("store_shard_state_persistent_raw {}", handle.id());
        if !handle.has_persistent_state() {
            self.shard_state_persistent_db.put(handle.id(), state_data).await?;
            if handle.set_persistent_state() {
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
            let length = min(length, full_lenth - offset);
            let db_slice = self.shard_state_persistent_db.get_slice(id, offset, length).await?;
            Ok(db_slice.to_vec())
        }
    }

    async fn load_shard_state_persistent_size(&self, id: &BlockIdExt) -> Result<u64> {
        log::trace!("load_shard_state_persistent_slice {}", id);
        self.shard_state_persistent_db.get_size(id).await
    }

    fn store_block_prev1(&self, handle: &Arc<BlockHandle>, prev: &BlockIdExt) -> Result<()> {
        log::trace!("store_block_prev {}", handle.id());
        if !handle.has_prev1() {
            let value = bincode::serialize(&convert_block_id_ext_blk2api(prev))?;
            self.prev_block_db.put(handle.id(), &value)?;
            if handle.set_prev1() {
                self.store_block_handle(handle)?;
            }
        }
        Ok(())
    }

    fn load_block_prev1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        log::trace!("load_block_prev {}", id);
        let bytes = self.prev_block_db.get(id)?;
        let prev = bincode::deserialize::<ton_api::ton::ton_node::blockidext::BlockIdExt>(&bytes)?;
        convert_block_id_ext_api2blk(&prev)
    }

    fn store_block_prev2(&self, handle: &Arc<BlockHandle>, prev2: &BlockIdExt) -> Result<()> {
        log::trace!("store_block_prev2 {}", handle.id());
        if !handle.has_prev2() {
            let value = bincode::serialize(&convert_block_id_ext_blk2api(prev2))?;
            self.prev2_block_db.put(handle.id(), &value)?;
            if handle.set_prev2() {
                self.store_block_handle(handle)?;
            }
        }
        Ok(())
    }

    fn load_block_prev2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        log::trace!("load_block_prev2 {}", id);
        let bytes = self.prev2_block_db.get(id)?;
        let prev2 = bincode::deserialize::<ton_api::ton::ton_node::blockidext::BlockIdExt>(&bytes)?;
        convert_block_id_ext_api2blk(&prev2)
    }

    fn store_block_next1(&self, handle: &Arc<BlockHandle>, next: &BlockIdExt) -> Result<()> {
        log::trace!("store_block_next1 {}", handle.id());
        if !handle.has_next1() {
            let value = bincode::serialize(&convert_block_id_ext_blk2api(next))?;
            self.next_block_db.put(handle.id(), &value)?;
            if handle.set_next1() {
                self.store_block_handle(handle)?;
            }
        }
        Ok(())
    }

    fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        log::trace!("load_block_next1 {}", id);
        let bytes = self.next_block_db.get(id)?;
        let next = bincode::deserialize::<ton_api::ton::ton_node::blockidext::BlockIdExt>(&bytes)?;
        convert_block_id_ext_api2blk(&next)
    }

    fn store_block_next2(&self, handle: &Arc<BlockHandle>, next2: &BlockIdExt) -> Result<()> {
        log::trace!("store_block_next2 {}", handle.id());
        if !handle.has_next2() {
            let value = bincode::serialize(&convert_block_id_ext_blk2api(next2))?;
            self.next2_block_db.put(handle.id(), &value)?;
            if handle.set_next2() {
                self.store_block_handle(handle)?;
            }
        }
        Ok(())
    }

    fn load_block_next2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        log::trace!("load_block_next2 {}", id);
        let bytes = self.next2_block_db.get(id)?;
        let next2 = bincode::deserialize::<ton_api::ton::ton_node::blockidext::BlockIdExt>(&bytes)?;
        convert_block_id_ext_api2blk(&next2)
    }

/*
    fn store_block_processed_in_ext_db(&self, handle: &Arc<BlockHandle>) -> Result<()> {
        log::trace!("store_block_processed_in_ext_db {}", handle.id());
        if handle.set_processed_in_ext_db() {
            self.store_block_handle(handle)?;
        }
        Ok(())
    }
*/

    fn store_block_applied(&self, handle: &Arc<BlockHandle>) -> Result<()> {
        log::trace!("store_block_applied {}", handle.id());
        if handle.set_block_applied() {
            self.store_block_handle(&handle)?;
        }
        Ok(())
    }

    async fn archive_block(&self, id: &BlockIdExt) -> Result<()> {
        log::trace!("archive_block {}", id);

        let handle = self.load_block_handle(id)?.ok_or_else(
            || error!("Cannot load handle for archiving block {}", id)
        )?;
        if handle.is_archived() {
            return Ok(());
        }

        self.archive_manager.move_to_archive(
            &handle,
            || {
                if handle.set_archived() {
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

    fn index_handle(&self, handle: &Arc<BlockHandle>) -> Result<()> {
        if handle.set_block_indexed() {
            self.block_index_db.add_handle(handle)?;
            self.store_block_handle(&handle)?;
        }
        Ok(())
    }

    fn assign_mc_ref_seq_no(&self, handle: &Arc<BlockHandle>, mc_seq_no: u32) -> Result<()> {
        if handle.set_masterchain_ref_seq_no(mc_seq_no)? {
            self.store_block_handle(handle)?;
        }
        Ok(())
    }

    fn save_top_shard_block(&self, id: &TopBlockDescrId, tsb: &TopBlockDescrStuff) -> Result<()> {
        log::trace!("save_top_shard_block {}", id);
        self.shard_top_blocks_db.put(&id.to_bytes()?, &tsb.to_bytes()?)
    }

    fn load_all_top_shard_blocks(&self) -> Result<HashMap<TopBlockDescrId, TopBlockDescrStuff>> {
        log::trace!("load_all_top_shard_blocks");
        let mut result = HashMap::<TopBlockDescrId, TopBlockDescrStuff>::new();
        self.shard_top_blocks_db.for_each(&mut |id_bytes, tsb_bytes| {
            let id = TopBlockDescrId::from_bytes(&id_bytes)?;
            let tsb = TopBlockDescrStuff::from_bytes(tsb_bytes, false)?;
            result.insert(id, tsb);
            Ok(true)
        })?;
        Ok(result)
    }

    fn load_all_top_shard_blocks_raw(&self) -> Result<HashMap<TopBlockDescrId, Vec<u8>>> {
        log::trace!("load_all_top_shard_blocks_raw");
        let mut result = HashMap::<TopBlockDescrId, Vec<u8>>::new();
        self.shard_top_blocks_db.for_each(&mut |id_bytes, tsb_bytes| {
            let id = TopBlockDescrId::from_bytes(&id_bytes)?;
            result.insert(id, tsb_bytes.to_vec());
            Ok(true)
        })?;
        Ok(result)
    }

    fn remove_top_shard_block(&self, id: &TopBlockDescrId) -> Result<()> {
        log::trace!("remove_top_shard_block {}", id);
        self.shard_top_blocks_db.delete(&id.to_bytes()?)
    }
}

