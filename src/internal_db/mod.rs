use crate::{
    block::{convert_block_id_ext_blk2api, convert_block_id_ext_api2blk, BlockStuff},
    block_proof::BlockProofStuff, error::NodeError, shard_state::ShardStateStuff,
    types::top_block_descr::{TopBlockDescrId, TopBlockDescrStuff},
};
use std::{
    path::PathBuf, sync::Arc, cmp::min, collections::HashMap,
    sync::atomic::{AtomicU32, Ordering}
};
use storage::{
    TimeChecker,
    archives::{archive_manager::ArchiveManager, package_entry_id::PackageEntryId},
    block_handle_db::{BlockHandleDb, BlockHandleStorage, Callback}, 
    block_index_db::BlockIndexDb, block_info_db::BlockInfoDb, node_state_db::NodeStateDb, 
    shardstate_db::{AllowStateGcResolver, ShardStateDb}, 
    shardstate_persistent_db::ShardStatePersistentDb, 
    types::{BlockHandle, BlockMeta}, shard_top_blocks_db::ShardTopBlocksDb,
};
#[cfg(feature = "read_old_db")]
use storage::block_db::BlockDb;
use ton_api::ton::PublicKey;
use ton_block::{Block, BlockIdExt, AccountIdPrefixFull, UnixTime32};
use ton_types::{error, fail, Result, UInt256};

/// Node state keys
pub(crate) const INITIAL_MC_BLOCK: &str      = "InitMcBlockId";
pub(crate) const LAST_APPLIED_MC_BLOCK: &str = "LastMcBlockId";
pub(crate) const PSS_KEEPER_MC_BLOCK: &str   = "PssKeeperBlockId";
pub(crate) const SHARD_CLIENT_MC_BLOCK: &str = "ShardsClientMcBlockId";

#[derive(Clone, Debug)]
pub enum DataStatus {
    Created,  // Just created
    Fetched,  // Read from DB as is
    Updated   // Read from DB or created and then updated with new data
}

#[derive(Clone, Debug)]
pub struct BlockResult {
    handle: Arc<BlockHandle>,
    status: DataStatus
}

impl BlockResult {

    /// Constructor
    pub fn with_status(handle: Arc<BlockHandle>, status: DataStatus) -> Self {
        Self {
            handle, 
            status
        }    
    }

    /// Any result 
    pub fn as_any(self) -> Arc<BlockHandle> {
        self.handle
    }

    /// Assert creation
    pub fn as_created(self) -> Option<Arc<BlockHandle>> {
        match self.status {
            DataStatus::Created => Some(self.handle),
            _ => None
        }
    }

    /// Assert non-creation
    pub fn as_non_created(self) -> Option<Arc<BlockHandle>> {
        match self.status {
            DataStatus::Created => None,
            _ => Some(self.handle)
        }
    }

    /// Assert non-update
    pub fn as_non_updated(self) -> Option<Arc<BlockHandle>> {
        match self.status {
            DataStatus::Updated => None,
            _ => Some(self.handle)
        }
    }

    /// Assert update
    pub fn as_updated(self) -> Option<Arc<BlockHandle>> {
        match self.status {
            DataStatus::Updated => Some(self.handle),
            _ => None
        }
    }

    /// Check update
    pub fn is_updated(&self) -> bool{
        match self.status {
            DataStatus::Updated => true,
            _ => false
        }
    }

}

pub mod state_gc_resolver;

#[async_trait::async_trait]
pub trait InternalDb : Sync + Send {
    fn create_or_load_block_handle(
        &self, 
        id: &BlockIdExt, 
        block: Option<&Block>,
        utime: Option<u32>,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<BlockResult>;
    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>>;

    async fn store_block_data(
        &self, 
        block: &BlockStuff, 
        callback: Option<Arc<dyn Callback>>
    ) -> Result<BlockResult>;
    async fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff>;
    async fn load_block_data_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>>;

    fn find_block_by_seq_no(&self, acc_pfx: &AccountIdPrefixFull, seqno: u32) -> Result<Arc<BlockHandle>>;
    fn find_block_by_unix_time(&self, acc_pfx: &AccountIdPrefixFull, utime: u32) -> Result<Arc<BlockHandle>>;
    fn find_block_by_lt(&self, acc_pfx: &AccountIdPrefixFull, lt: u64) -> Result<Arc<BlockHandle>>;

    async fn store_block_proof(
        &self, 
        id: &BlockIdExt, 
        handle: Option<Arc<BlockHandle>>, 
        proof: &BlockProofStuff,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<BlockResult>;
    async fn load_block_proof(&self, handle: &BlockHandle, is_link: bool) -> Result<BlockProofStuff>;
    async fn load_block_proof_raw(&self, handle: &BlockHandle, is_link: bool) -> Result<Vec<u8>>;

    fn store_shard_state_dynamic(
        &self, 
        handle: &Arc<BlockHandle>, 
        state: ShardStateStuff,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<(ShardStateStuff, bool)>;
    fn load_shard_state_dynamic(&self, id: &BlockIdExt) -> Result<ShardStateStuff>;
   // fn gc_shard_state_dynamic_db(&self) -> Result<usize>;

    async fn store_shard_state_persistent(
        &self, 
        handle: &Arc<BlockHandle>, 
        state: &ShardStateStuff,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()>;
    async fn store_shard_state_persistent_raw(
        &self, 
        handle: &Arc<BlockHandle>, 
        state_data: &[u8],
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()>;
    async fn load_shard_state_persistent_slice(&self, id: &BlockIdExt, offset: u64, length: u64) -> Result<Vec<u8>>;
    async fn load_shard_state_persistent_size(&self, id: &BlockIdExt) -> Result<u64>;

    fn store_block_prev1(
        &self, 
        handle: &Arc<BlockHandle>, 
        prev: &BlockIdExt,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()>;
    fn load_block_prev1(&self, id: &BlockIdExt) -> Result<BlockIdExt>;

    fn store_block_prev2(
        &self, 
        handle: &Arc<BlockHandle>, 
        prev2: &BlockIdExt,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()>;
    fn load_block_prev2(&self, id: &BlockIdExt) -> Result<BlockIdExt>;

    fn store_block_next1(
        &self, 
        handle: &Arc<BlockHandle>, 
        next: &BlockIdExt,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()>;
    fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt>;

    fn store_block_next2(
        &self, 
        handle: &Arc<BlockHandle>, 
        next2: &BlockIdExt,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()>;
    fn load_block_next2(&self, id: &BlockIdExt) -> Result<BlockIdExt>;

//    fn store_block_processed_in_ext_db(&self, handle: &Arc<BlockHandle>) -> Result<()>;
    fn store_block_applied(
        &self, 
        handle: &Arc<BlockHandle>,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<bool>;

    async fn archive_block(
        &self, 
        id: &BlockIdExt,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()>;

    fn load_node_state(&self, key: &'static str) -> Result<Option<Arc<BlockIdExt>>>;
    fn save_node_state(&self, key: &'static str, value: &BlockIdExt) -> Result<()>;

    fn archive_manager(&self) -> &Arc<ArchiveManager>;

    fn index_handle(
        &self, 
        handle: &Arc<BlockHandle>,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()>;

    fn assign_mc_ref_seq_no(
        &self, 
        handle: &Arc<BlockHandle>, 
        mc_seq_no: u32,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()>;

    fn save_top_shard_block(&self, id: &TopBlockDescrId, tsb: &TopBlockDescrStuff) -> Result<()>;
    fn load_all_top_shard_blocks(&self) -> Result<HashMap<TopBlockDescrId, TopBlockDescrStuff>>;
    fn load_all_top_shard_blocks_raw(&self) -> Result<HashMap<TopBlockDescrId, Vec<u8>>>;
    fn remove_top_shard_block(&self, id: &TopBlockDescrId) -> Result<()>;

    fn db_root_dir(&self) -> Result<&str>;

    fn adjust_states_gc_interval(&self, interval_ms: u32);
}

#[derive(serde::Deserialize)]
pub struct InternalDbConfig {
    pub db_directory: String,
    pub cells_gc_interval_ms: u32,
}

pub struct InternalDbImpl {
    block_handle_storage: Arc<BlockHandleStorage>,
    block_index_db: Arc<BlockIndexDb>,
    prev_block_db: BlockInfoDb,
    prev2_block_db: BlockInfoDb,
    next_block_db: BlockInfoDb,
    next2_block_db: BlockInfoDb,
    shard_state_persistent_db: ShardStatePersistentDb,
    shard_state_dynamic_db: Arc<ShardStateDb>,
    //ss_test_map: lockfree::map::Map<BlockIdExt, ShardStateStuff>,
    //shardstate_db_gc: GC,
    archive_manager: Arc<ArchiveManager>,
    shard_top_blocks_db: ShardTopBlocksDb,

    #[cfg(feature = "read_old_db")]
    old_block_db: BlockDb,
    #[cfg(feature = "read_old_db")]
    old_block_proof_db: BlockInfoDb,
    #[cfg(feature = "read_old_db")]
    old_block_proof_link_db: BlockInfoDb,

    config: InternalDbConfig,
    cells_gc_interval: Arc<AtomicU32>,
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
        let node_state_db = Arc::new(
            NodeStateDb::with_path(
                &Self::build_name(&config.db_directory, "node_state_db")
            )
        );
        let block_handle_storage = Arc::new(
            BlockHandleStorage::with_dbs(block_handle_db.clone(), node_state_db)
        );
        let shard_state_dynamic_db = ShardStateDb::with_paths(
            &Self::build_name(&config.db_directory, "shardstate_db"),
            &Self::build_name(&config.db_directory, "cells_db"),
            &Self::build_name(&config.db_directory, "cells_db1"),
        )?;
        //let shardstate_db_gc = GC::new(&shard_state_dynamic_db, Arc::clone(&block_handle_db))?;
        let archive_manager = Arc::new(ArchiveManager::with_data(Arc::new(PathBuf::from(&config.db_directory))).await?);
        let db = Self {
            block_handle_storage,
            block_index_db,
            prev_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "prev1_block_db")),
            prev2_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "prev2_block_db")),
            next_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "next1_block_db")),
            next2_block_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "next2_block_db")),
            shard_state_persistent_db: ShardStatePersistentDb::with_path(
                &Self::build_name(&config.db_directory, "shard_state_persistent_db")),
            shard_state_dynamic_db,
            //ss_test_map: lockfree::map::Map::new(),
            //shardstate_db_gc,
            archive_manager,
            shard_top_blocks_db: ShardTopBlocksDb::with_path(&Self::build_name(&config.db_directory, "shard_top_blocks_db")),

            #[cfg(feature = "read_old_db")]
            old_block_db: BlockDb::with_path(&Self::build_name(&config.db_directory, "block_db")),
            #[cfg(feature = "read_old_db")]
            old_block_proof_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "block_proof_db")),
            #[cfg(feature = "read_old_db")]
            old_block_proof_link_db: BlockInfoDb::with_path(&Self::build_name(&config.db_directory, "block_proof_link_db")),

            cells_gc_interval: Arc::new(AtomicU32::new(config.cells_gc_interval_ms)),
            config,
        };

        Ok(db)
    }

    pub fn start_states_gc(&self, resolver: Arc<dyn AllowStateGcResolver>) {
        self.shard_state_dynamic_db.clone().start_gc(resolver, self.cells_gc_interval.clone())
    }

    pub fn build_name(dir: &str, name: &str) -> String {
        format!("{}/{}", dir, name)
    }

    fn store_block_handle(
        &self, 
        handle: &Arc<BlockHandle>,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        self.block_handle_storage.store_handle(handle, callback)
    }

}


#[async_trait::async_trait]
impl InternalDb for InternalDbImpl {

    fn create_or_load_block_handle(
        &self, 
        id: &BlockIdExt, 
        block: Option<&Block>,
        utime: Option<u32>,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<BlockResult> {
        if let Some(handle) = self.load_block_handle(id)? {
            return Ok(BlockResult::with_status(handle, DataStatus::Fetched))
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
        if let Some(handle) = 
            self.block_handle_storage.create_handle(id.clone(), meta, callback)? {
                return Ok(BlockResult::with_status(handle, DataStatus::Created))
            }
        if let Some(handle) = self.load_block_handle(id)? {
            return Ok(BlockResult::with_status(handle, DataStatus::Fetched))
        }
        fail!("Cannot create handle for block {}", id)
    }

    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>> {
        let _tc = TimeChecker::new(format!("load_block_handle {}", id), 10);
        self.block_handle_storage.load_handle(id)
    }

    async fn store_block_data(
        &self, 
        block: &BlockStuff,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<BlockResult> {
        let _tc = TimeChecker::new(format!("store_block_data {}", block.id()), 100);
        let mut result = self.create_or_load_block_handle(
            block.id(), Some(block.block()), None, callback.clone()
        )?;
        let handle = result.clone().as_non_updated().ok_or_else(
            || error!("INTERNAL ERROR: block {} result mismatch in store_block_data {:?}", block.id(), result)
        )?;
        let entry_id = PackageEntryId::<_, UInt256, PublicKey>::Block(block.id());
        if !handle.has_data() || !self.archive_manager.check_file(&handle, &entry_id) {
            let _lock = handle.block_file_lock().write().await;
            if !handle.has_data() || !self.archive_manager.check_file(&handle, &entry_id) {
                self.archive_manager.add_file(&entry_id, block.data().to_vec()).await?;
                if handle.set_data() {
                    self.store_block_handle(&handle, callback)?;
                    result = BlockResult::with_status(handle.clone(), DataStatus::Updated)
                }
            }
        }
        Ok(result)
    }

    async fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        let _tc = TimeChecker::new(format!("load_block_data {}", handle.id()), 100);
        let raw_block = self.load_block_data_raw(handle).await?;
        BlockStuff::deserialize(handle.id().clone(), raw_block)
    }

    async fn load_block_data_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        let _tc = TimeChecker::new(format!("load_block_data_raw {}", handle.id()), 100);
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
        let _tc = TimeChecker::new(format!("find_block_by_seq_no {} {}", acc_pfx, seq_no), 100);
        let id = self.block_index_db.get_block_by_seq_no(acc_pfx, seq_no)?;
        self.load_block_handle(&id)?.ok_or_else(
            || error!("Cannot find handle for block {}", id) 
        )
    }

    fn find_block_by_unix_time(&self, acc_pfx: &AccountIdPrefixFull, utime: u32) -> Result<Arc<BlockHandle>> {
        let _tc = TimeChecker::new(format!("find_block_by_unix_time {} {}", acc_pfx, utime), 100);
        let id = self.block_index_db.get_block_by_ut(acc_pfx, UnixTime32(utime))?;
        self.load_block_handle(&id)?.ok_or_else(
            || error!("Cannot find handle for block {}", id) 
        )
    }

    fn find_block_by_lt(&self, acc_pfx: &AccountIdPrefixFull, lt: u64) -> Result<Arc<BlockHandle>> {
        let _tc = TimeChecker::new(format!("find_block_by_lt {} {}", acc_pfx, lt), 100);
        let id = self.block_index_db.get_block_by_lt(acc_pfx, lt)?;
        self.load_block_handle(&id)?.ok_or_else(
            || error!("Cannot find handle for block {}", id) 
        )
    }

    async fn store_block_proof(
        &self, 
        id: &BlockIdExt,
        handle: Option<Arc<BlockHandle>>, 
        proof: &BlockProofStuff,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<BlockResult> {

        if let Some(handle) = &handle {
            if handle.id() != id {
                fail!("Block handle and id mismatch: {} vs {}", handle.id(), id)
            }
        }
        if cfg!(feature = "local_test") {
            let handle = if let Some(handle) = handle {
                handle
            } else if let Some(handle) = self.load_block_handle(id)? {
                handle
            } else {
                fail!("Cannot load handle for block {} during test", id)
            };
            Ok(BlockResult::with_status(handle, DataStatus::Fetched))
        } else {

            let _tc = TimeChecker::new(format!("store_block_proof {}", proof.id()), 200);
            if id != proof.id() {
                fail!(NodeError::InvalidArg("`proof` and `id` mismatch".to_string()))
            }
              
            let mut result = if let Some(handle) = handle {
                BlockResult::with_status(handle, DataStatus::Fetched)
            } else {
                let (virt_block, _) = proof.virtualize_block()?;
                self.create_or_load_block_handle(id, Some(&virt_block), None, callback.clone())?
            };
            let handle = result.clone().as_non_updated().ok_or_else(
                || error!("INTERNAL ERROR: block {} result mismatch in store_block_proof", id)
            )?;
            if proof.is_link() {
                let entry_id = PackageEntryId::<_, UInt256, PublicKey>::ProofLink(id);
                if !handle.has_proof_link() || 
                   !self.archive_manager.check_file(&handle, &entry_id) 
                {
                    let _lock = handle.proof_file_lock().write().await;
                    if !handle.has_proof_link() || 
                       !self.archive_manager.check_file(&handle, &entry_id) 
                    {
                        let entry_id = PackageEntryId::<_, UInt256, PublicKey>::ProofLink(id);
                        self.archive_manager.add_file(&entry_id, proof.data().to_vec()).await?;
                        if handle.set_proof_link() {
                            self.store_block_handle(&handle, callback)?;
                            result = BlockResult::with_status(handle.clone(), DataStatus::Updated)
                        }
                    }
                }
            } else {
                let entry_id = PackageEntryId::<_, UInt256, PublicKey>::Proof(id);
                if !handle.has_proof() || 
                   !self.archive_manager.check_file(&handle, &entry_id) 
                {
                    let _lock = handle.proof_file_lock().write().await;
                    if !handle.has_proof() || 
                       !self.archive_manager.check_file(&handle, &entry_id) 
                    {
                        let entry_id = PackageEntryId::<_, UInt256, PublicKey>::Proof(id);
                        self.archive_manager.add_file(&entry_id, proof.data().to_vec()).await?;
                        if handle.set_proof() {
                            self.store_block_handle(&handle, callback)?;
                            result = BlockResult::with_status(handle.clone(), DataStatus::Updated)
                        }
                    }
                }
            }
            Ok(result)

        }

    }

    async fn load_block_proof(&self, handle: &BlockHandle, is_link: bool) -> Result<BlockProofStuff> {
        let _tc = TimeChecker::new(format!("load_block_proof {} {}", if is_link {"link"} else {""}, handle.id()), 100);
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
        mut state: ShardStateStuff,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<(ShardStateStuff, bool)> {
        let _tc = TimeChecker::new(format!("store_shard_state_dynamic {}", state.block_id()), 300);
        if handle.id() != state.block_id() {
            fail!(NodeError::InvalidArg("`state` and `handle` mismatch".to_string()))
        }
        if !handle.has_state() {
            let saved_root = self.shard_state_dynamic_db.put(state.block_id(), state.root_cell().clone())?;
            state = ShardStateStuff::new(handle.id().clone(), saved_root)?;
            //self.ss_test_map.insert(state.block_id().clone(), state.clone());
            if handle.set_state() {
                self.store_block_handle(handle, callback)?;
                return Ok((state, true));
            }
        }
        Ok((state, false))
    }

    fn load_shard_state_dynamic(&self, id: &BlockIdExt) -> Result<ShardStateStuff> {
        let _tc = TimeChecker::new(format!("load_shard_state_dynamic {}", id), 10);        
        Ok(ShardStateStuff::new(id.clone(), self.shard_state_dynamic_db.get(id)?)?)
    }

    /*
    fn gc_shard_state_dynamic_db(&self) -> Result<usize> {
        self.shardstate_db_gc.collect()
    }*/

    async fn store_shard_state_persistent(
        &self, 
        handle: &Arc<BlockHandle>, 
        state: &ShardStateStuff,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        let _tc = TimeChecker::new(format!("store_shard_state_persistent {}", state.block_id()), 10_000);
        if handle.id() != state.block_id() {
            fail!(NodeError::InvalidArg("`state` and `handle` mismatch".to_string()))
        }
        if !handle.has_persistent_state() {
            self.shard_state_persistent_db.put(state.block_id(), &state.serialize()?).await?;
            if handle.set_persistent_state() {
                self.store_block_handle(handle, callback)?;
            }
        }
        Ok(())
    }

    async fn store_shard_state_persistent_raw(
        &self, 
        handle: &Arc<BlockHandle>, 
        state_data: &[u8],
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        let _tc = TimeChecker::new(
            format!("store_shard_state_persistent_raw {}", handle.id()),
            state_data.len() as u64 / 1000 + 10
        );
        if !handle.has_persistent_state() {
            self.shard_state_persistent_db.put(handle.id(), state_data).await?;
            if handle.set_persistent_state() {
                self.store_block_handle(handle, callback)?;
            }
        }
        Ok(())
    }

    async fn load_shard_state_persistent_slice(&self, id: &BlockIdExt, offset: u64, length: u64) -> Result<Vec<u8>> {
        let _tc = TimeChecker::new(format!("load_shard_state_persistent_slice {}", id), 200);
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
        let _tc = TimeChecker::new(format!("load_shard_state_persistent_size {}", id), 50);
        self.shard_state_persistent_db.get_size(id).await
    }

    fn store_block_prev1(
        &self, 
        handle: &Arc<BlockHandle>, 
        prev: &BlockIdExt,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        let _tc = TimeChecker::new(format!("store_block_prev {}", handle.id()), 10);
        if !handle.has_prev1() {
            let value = bincode::serialize(&convert_block_id_ext_blk2api(prev))?;
            self.prev_block_db.put(handle.id(), &value)?;
            if handle.set_prev1() {
                self.store_block_handle(handle, callback)?;
            }
        }
        Ok(())
    }

    fn load_block_prev1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        let _tc = TimeChecker::new(format!("load_block_prev {}", id), 10);
        let bytes = self.prev_block_db.get(id)?;
        let prev = bincode::deserialize::<ton_api::ton::ton_node::blockidext::BlockIdExt>(&bytes)?;
        convert_block_id_ext_api2blk(&prev)
    }

    fn store_block_prev2(
        &self, 
        handle: &Arc<BlockHandle>, 
        prev2: &BlockIdExt,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        let _tc = TimeChecker::new(format!("store_block_prev2 {}", handle.id()), 10);
        if !handle.has_prev2() {
            let value = bincode::serialize(&convert_block_id_ext_blk2api(prev2))?;
            self.prev2_block_db.put(handle.id(), &value)?;
            if handle.set_prev2() {
                self.store_block_handle(handle, callback)?;
            }
        }
        Ok(())
    }

    fn load_block_prev2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        let _tc = TimeChecker::new(format!("load_block_prev2 {}", id), 10);
        let bytes = self.prev2_block_db.get(id)?;
        let prev2 = bincode::deserialize::<ton_api::ton::ton_node::blockidext::BlockIdExt>(&bytes)?;
        convert_block_id_ext_api2blk(&prev2)
    }

    fn store_block_next1(
        &self, 
        handle: &Arc<BlockHandle>, 
        next: &BlockIdExt,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        let _tc = TimeChecker::new(format!("store_block_next1 {}", handle.id()), 10);
        if !handle.has_next1() {
            let value = bincode::serialize(&convert_block_id_ext_blk2api(next))?;
            self.next_block_db.put(handle.id(), &value)?;
            if handle.set_next1() {
                self.store_block_handle(handle, callback)?;
            }
        }
        Ok(())
    }

    fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        let _tc = TimeChecker::new(format!("load_block_next1 {}", id), 10);
        let bytes = self.next_block_db.get(id)?;
        let next = bincode::deserialize::<ton_api::ton::ton_node::blockidext::BlockIdExt>(&bytes)?;
        convert_block_id_ext_api2blk(&next)
    }

    fn store_block_next2(
        &self, 
        handle: &Arc<BlockHandle>, 
        next2: &BlockIdExt,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        let _tc = TimeChecker::new(format!("store_block_next2 {}", handle.id()), 10);
        if !handle.has_next2() {
            let value = bincode::serialize(&convert_block_id_ext_blk2api(next2))?;
            self.next2_block_db.put(handle.id(), &value)?;
            if handle.set_next2() {
                self.store_block_handle(handle, callback)?;
            }
        }
        Ok(())
    }

    fn load_block_next2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        let _tc = TimeChecker::new(format!("load_block_next2 {}", id), 10);
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

    fn store_block_applied(
        &self, 
        handle: &Arc<BlockHandle>,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<bool> {
        let _tc = TimeChecker::new(format!("store_block_applied {}", handle.id()), 10);
        if handle.set_block_applied() {
            self.store_block_handle(&handle, callback)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn archive_block(
        &self, 
        id: &BlockIdExt,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        let _tc = TimeChecker::new(format!("archive_block {}", id), 200);
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
                    self.store_block_handle(&handle, callback.clone())?;
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

    fn load_node_state(&self, key: &'static str) -> Result<Option<Arc<BlockIdExt>>> {
        let _tc = TimeChecker::new(format!("load_node_state {}", key), 10);
        self.block_handle_storage.load_state(key)
    }

    fn save_node_state(&self, key: &'static str, block_id: &BlockIdExt) -> Result<()> {
        let _tc = TimeChecker::new(format!("store_node_state {}", key), 10);
        self.block_handle_storage.store_state(key, block_id)
    }

    fn archive_manager(&self) -> &Arc<ArchiveManager> {
        &self.archive_manager
    }

    fn index_handle(
        &self, 
        handle: &Arc<BlockHandle>,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        if !handle.is_indexed() {
            self.block_index_db.add_handle(handle)?;
            if handle.set_block_indexed() {
                self.store_block_handle(&handle, callback)?;
            }
        }
        Ok(())
    }

    fn assign_mc_ref_seq_no(
        &self, 
        handle: &Arc<BlockHandle>, 
        mc_seq_no: u32,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        if handle.set_masterchain_ref_seq_no(mc_seq_no)? {
            self.store_block_handle(handle, callback)?;
        }
        Ok(())
    }

    fn save_top_shard_block(&self, id: &TopBlockDescrId, tsb: &TopBlockDescrStuff) -> Result<()> {
        let _tc = TimeChecker::new(format!("save_top_shard_block {}", id), 50);
        self.shard_top_blocks_db.put(&id.to_bytes()?, &tsb.to_bytes()?)
    }

    fn load_all_top_shard_blocks(&self) -> Result<HashMap<TopBlockDescrId, TopBlockDescrStuff>> {
        let _tc = TimeChecker::new(format!("load_all_top_shard_blocks"), 100);
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
        let _tc = TimeChecker::new(format!("load_all_top_shard_blocks_raw"), 100);
        let mut result = HashMap::<TopBlockDescrId, Vec<u8>>::new();
        self.shard_top_blocks_db.for_each(&mut |id_bytes, tsb_bytes| {
            let id = TopBlockDescrId::from_bytes(&id_bytes)?;
            result.insert(id, tsb_bytes.to_vec());
            Ok(true)
        })?;
        Ok(result)
    }

    fn remove_top_shard_block(&self, id: &TopBlockDescrId) -> Result<()> {
        let _tc = TimeChecker::new(format!("remove_top_shard_block {}", id), 50);
        self.shard_top_blocks_db.delete(&id.to_bytes()?)
    }

    fn db_root_dir(&self) -> Result<&str> {
        Ok(&self.config.db_directory)
    }

    fn adjust_states_gc_interval(&self, interval_ms: u32) {
        let prev = self.cells_gc_interval.swap(interval_ms, Ordering::Relaxed);
        log::info!("Adjusted states gc interval {} -> {}", prev, interval_ms);
    }
}

