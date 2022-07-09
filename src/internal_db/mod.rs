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

use crate::{
    block::BlockStuff, block_proof::BlockProofStuff, engine_traits::EngineAlloc, error::NodeError,
    shard_state::ShardStateStuff, types::top_block_descr::{TopBlockDescrId, TopBlockDescrStuff},
};
#[cfg(feature = "telemetry")]
use crate::engine_traits::EngineTelemetry;

use std::{
    cmp::min, collections::{HashMap, HashSet}, io::Cursor, mem::size_of, path::{Path, PathBuf},
    sync::{Arc, atomic::{AtomicBool, AtomicU32, Ordering}}, time::{UNIX_EPOCH, Duration}
};
use storage::{
    TimeChecker,
    archives::{archive_manager::ArchiveManager, package_entry_id::PackageEntryId},
    block_handle_db::{BlockHandle, BlockHandleDb, BlockHandleStorage, Callback}, 
    block_info_db::BlockInfoDb, db::rocksdb::RocksDb, node_state_db::NodeStateDb, 
    shardstate_db::{AllowStateGcResolver, ShardStateDb}, 
    shardstate_persistent_db::ShardStatePersistentDb, types::BlockMeta, 
    shard_top_blocks_db::ShardTopBlocksDb, StorageAlloc, traits::Serializable,
};
#[cfg(feature = "telemetry")]
use storage::StorageTelemetry;
use ton_block::{Block, BlockIdExt};
use ton_types::{error, fail, Result, UInt256, Cell};

/// Full node state keys
pub const INITIAL_MC_BLOCK: &str       = "InitMcBlockId";
pub const LAST_APPLIED_MC_BLOCK: &str  = "LastMcBlockId";
pub const PSS_KEEPER_MC_BLOCK: &str    = "PssKeeperBlockId";
pub const SHARD_CLIENT_MC_BLOCK: &str  = "ShardsClientMcBlockId";
pub const DB_VERSION: &str  = "DbVersion";

pub const DB_VERSION_0: u32  = 0;
pub const DB_VERSION_1: u32  = 1; // with fixed cells/bits counter in StorageCell
pub const CURRENT_DB_VERSION: u32 = DB_VERSION_1;

/// Validator state keys
pub(crate) const LAST_ROTATION_MC_BLOCK: &str = "LastRotationBlockId";

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
    pub fn _as_any(self) -> Arc<BlockHandle> {
        self.handle
    }

    /// Assert creation
    pub fn _as_created(self) -> Option<Arc<BlockHandle>> {
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
pub mod restore;
mod update;

#[derive(serde::Deserialize)]
pub struct InternalDbConfig {
    pub db_directory: String,
    pub cells_gc_interval_sec: u32,
}

pub struct InternalDb {
    db: Arc<RocksDb>,
    block_handle_storage: Arc<BlockHandleStorage>,
    prev1_block_db: BlockInfoDb,
    prev2_block_db: BlockInfoDb,
    next1_block_db: BlockInfoDb,
    next2_block_db: BlockInfoDb,
    shard_state_persistent_db: ShardStatePersistentDb,
    shard_state_dynamic_db: Arc<ShardStateDb>,
    //ss_test_map: lockfree::map::Map<BlockIdExt, ShardStateStuff>,
    //shardstate_db_gc: GC,
    archive_manager: Arc<ArchiveManager>,
    shard_top_blocks_db: ShardTopBlocksDb,
    full_node_state_db: Arc<NodeStateDb>,

    config: InternalDbConfig,
    cells_gc_interval: Arc<AtomicU32>,
    #[cfg(feature = "telemetry")]
    telemetry: Arc<EngineTelemetry>,
    allocated: Arc<EngineAlloc>,
}

impl InternalDb {

    pub async fn with_update(
        config: InternalDbConfig,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<EngineTelemetry>,
        allocated: Arc<EngineAlloc>,
        check_stop: Option<&(dyn Fn() -> Result<()> + Sync)>,
        is_broken: Option<&AtomicBool>
    ) -> Result<Self> {
        let mut db = Self::construct(
            config,
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated,
        ).await?;
        let version = db.resolve_db_version()?;
        if version != CURRENT_DB_VERSION {
            if let Some(check_stop) = check_stop {
                db = update::update(db, version, check_stop, is_broken).await?
            } else {
                fail!(
                    "DB version {} does not correspond to current supported one {}.", 
                    version, 
                    CURRENT_DB_VERSION
                )
            }
        }
        Ok(db)
    }

    async fn construct(
        config: InternalDbConfig,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<EngineTelemetry>,
        allocated: Arc<EngineAlloc>,
    ) -> Result<Self> {
        let db = RocksDb::with_path(config.db_directory.as_str(), "db");
        let db_catchain = RocksDb::with_path(config.db_directory.as_str(), "catchains");
        let block_handle_db = Arc::new(
            BlockHandleDb::with_db(db.clone(), "block_handle_db")?
        );
        let full_node_state_db = Arc::new(
            NodeStateDb::with_db(db.clone(), "node_state_db")?
        );
        let validator_state_db = Arc::new(
            NodeStateDb::with_db(db_catchain, "validator_state_db")?
        );
        let block_handle_storage = Arc::new(
            BlockHandleStorage::with_dbs(
                block_handle_db.clone(), 
                full_node_state_db.clone(), 
                validator_state_db,
                #[cfg(feature = "telemetry")]
                telemetry.storage.clone(),
                allocated.storage.clone()
            )
        );
        let shard_state_dynamic_db = Self::create_shard_state_dynamic_db(
            db.clone(),
            #[cfg(feature = "telemetry")]
            telemetry.storage.clone(),
            allocated.storage.clone()
        )?;
        //let shardstate_db_gc = GC::new(&shard_state_dynamic_db, Arc::clone(&block_handle_db))?;
        let archive_manager = Arc::new(
            ArchiveManager::with_data(
                db.clone(),
                Arc::new(PathBuf::from(&config.db_directory)),
                #[cfg(feature = "telemetry")]
                telemetry.storage.clone(),
                allocated.storage.clone()
            ).await?
        );

        let db = Self {
            db: db.clone(),
            block_handle_storage,
            prev1_block_db: BlockInfoDb::with_db(db.clone(), "prev1_block_db")?,
            prev2_block_db: BlockInfoDb::with_db(db.clone(), "prev2_block_db")?,
            next1_block_db: BlockInfoDb::with_db(db.clone(), "next1_block_db")?,
            next2_block_db: BlockInfoDb::with_db(db.clone(), "next2_block_db")?,
            shard_state_persistent_db: ShardStatePersistentDb::with_path(
                Path::new(config.db_directory.as_str()).join("shard_state_persistent_db")
            ),
            shard_state_dynamic_db,
            //ss_test_map: lockfree::map::Map::new(),
            //shardstate_db_gc,
            archive_manager,
            shard_top_blocks_db: ShardTopBlocksDb::with_db(db.clone(), "shard_top_blocks_db")?,
            full_node_state_db,

            cells_gc_interval: Arc::new(AtomicU32::new(config.cells_gc_interval_sec)),
            config,
            #[cfg(feature = "telemetry")]
            telemetry, 
            allocated
        };

        Ok(db)
    }

    pub fn resolve_db_version(&self) -> Result<u32> {
        if self.block_handle_storage.is_empty()? {
            self.store_db_version(CURRENT_DB_VERSION)?;
            Ok(CURRENT_DB_VERSION)
        } else {
            self.load_db_version()
        }
    }

    fn store_db_version(&self, v: u32) -> Result<()> {
        let mut bytes = Vec::with_capacity(size_of::<u32>());
        v.serialize(&mut bytes)?;
        self.full_node_state_db.put(&DB_VERSION, &bytes)
    }

    fn load_db_version(&self) -> Result<u32> {
        if let Some(db_slice) = self.full_node_state_db.try_get(&DB_VERSION)? {
            let mut cursor = Cursor::new(db_slice.as_ref());
            u32::deserialize(&mut cursor)
        } else {
            Ok(DB_VERSION_0)
        }
    }

    fn create_shard_state_dynamic_db(
        db: Arc<RocksDb>,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<StorageTelemetry>,
        allocated: Arc<StorageAlloc>
    ) -> Result<Arc<ShardStateDb>> {
        ShardStateDb::with_db(
            db,
            "shardstate_db",
            "cells_db",
            "cells_db1",
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated
        )
    }

    pub fn clean_shard_state_dynamic_db(&mut self) -> Result<()> {
        if self.shard_state_dynamic_db.is_gc_run() {
            fail!("It is forbidden to clear shard_state_dynamic_db while cells GC is running")
        }

        self.db.drop_table("shardstate_db")?;
        self.db.drop_table("cells_db")?;
        self.db.drop_table("cells_db1")?;

        self.shard_state_dynamic_db = Self::create_shard_state_dynamic_db(
            self.db.clone(),
            #[cfg(feature = "telemetry")]
            self.telemetry.storage.clone(),
            self.allocated.storage.clone()
        )?;

        Ok(())
    }

    //#[allow(dead_code)]                                                                                 /
    pub fn start_states_gc(&self, resolver: Arc<dyn AllowStateGcResolver>) {
        self.shard_state_dynamic_db.clone().start_gc(resolver, self.cells_gc_interval.clone())
    }

    pub async fn stop_states_gc(&self) {
        self.shard_state_dynamic_db.stop_gc().await
    }

    fn store_block_handle(
        &self, 
        handle: &Arc<BlockHandle>,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        self.block_handle_storage.save_handle(handle, callback)
    }

    fn load_block_linkage(
        &self, 
        id: &BlockIdExt,
        db: &BlockInfoDb,
        msg: &str
    ) -> Result<BlockIdExt> {
        let _tc = TimeChecker::new(format!("{} {}", msg, id), 10);
        let bytes = db.get(id)?;
        let mut cursor = Cursor::new(&bytes);
        BlockIdExt::deserialize(&mut cursor)
    }

    fn store_block_linkage(
        &self, 
        handle: &Arc<BlockHandle>, 
        linkage: &BlockIdExt,
        db: &BlockInfoDb,
        msg: &str,
        check_has: impl Fn(&Arc<BlockHandle>) -> bool,
        check_set: impl Fn(&Arc<BlockHandle>) -> bool,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        let _tc = TimeChecker::new(format!("{} {}", msg, handle.id()), 10);
        if !check_has(handle) {
            let mut value = Vec::new();
            linkage.serialize(&mut value)?;
            db.put(handle.id(), &value)?;
            if check_set(handle) {
                self.store_block_handle(handle, callback)?;
            }
        }
        Ok(())
    }

    pub fn create_or_load_block_handle(
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
        if let Some(handle) = self.block_handle_storage.create_handle(id.clone(), meta, callback)? {
            Ok(BlockResult::with_status(handle, DataStatus::Created))
        } else if let Some(handle) = self.load_block_handle(id)? {
            Ok(BlockResult::with_status(handle, DataStatus::Fetched))
        } else {
            fail!("Cannot create handle for block {}", id)
        }
    }

    pub fn load_block_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>> {
        let _tc = TimeChecker::new(format!("load_block_handle {}", id), 10);
        self.block_handle_storage.load_handle(id)
    }

    pub async fn store_block_data(
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
        let entry_id = PackageEntryId::<_, UInt256, UInt256>::Block(block.id());
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

    pub async fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        let _tc = TimeChecker::new(format!("load_block_data {}", handle.id()), 100);
        let raw_block = self.load_block_data_raw(handle).await?;
        BlockStuff::deserialize(handle.id().clone(), raw_block)
    }

    pub async fn load_block_data_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        let _tc = TimeChecker::new(format!("load_block_data_raw {}", handle.id()), 100);
        if !handle.has_data() {
            fail!("This block is not stored yet: {:?}", handle);
        }
        let entry_id = PackageEntryId::<_, UInt256, UInt256>::Block(handle.id());
        self.archive_manager.get_file(handle, &entry_id).await
    }

    pub fn find_mc_block_by_seq_no(&self, seqno: u32) -> Result<Arc<BlockHandle>> {
        let _tc = TimeChecker::new(format!("find_mc_block_by_seq_no {}", seqno), 100);
        let last_id = self.load_full_node_state(LAST_APPLIED_MC_BLOCK)?.ok_or_else(
            || error!("Cannot find MC block {} because no MC blocks applied", seqno)
        )?;
        let last_state = self.load_shard_state_dynamic(&last_id)?;
        let id = last_state.find_block_id(seqno)?;        
        self.load_block_handle(&id)?.ok_or_else(
            || error!("Cannot load handle for master block {}", id)
        )          
    }

    pub async fn store_block_proof(
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
        {
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
                let entry_id = PackageEntryId::<_, UInt256, UInt256>::ProofLink(id);
                if !handle.has_proof_link() || 
                    !self.archive_manager.check_file(&handle, &entry_id) 
                {
                    let _lock = handle.proof_file_lock().write().await;
                    if !handle.has_proof_link() || 
                        !self.archive_manager.check_file(&handle, &entry_id) 
                    {
                        let entry_id = PackageEntryId::<_, UInt256, UInt256>::ProofLink(id);
                        self.archive_manager.add_file(&entry_id, proof.data().to_vec()).await?;
                        if handle.set_proof_link() {
                            self.store_block_handle(&handle, callback)?;
                            result = BlockResult::with_status(handle.clone(), DataStatus::Updated)
                        }
                    }
                }
            } else {
                let entry_id = PackageEntryId::<_, UInt256, UInt256>::Proof(id);
                if !handle.has_proof() || 
                    !self.archive_manager.check_file(&handle, &entry_id) 
                {
                    let _lock = handle.proof_file_lock().write().await;
                    if !handle.has_proof() || 
                        !self.archive_manager.check_file(&handle, &entry_id) 
                    {
                        let entry_id = PackageEntryId::<_, UInt256, UInt256>::Proof(id);
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

    pub async fn load_block_proof(&self, handle: &BlockHandle, is_link: bool) -> Result<BlockProofStuff> {
        let _tc = TimeChecker::new(format!("load_block_proof {} {}", if is_link {"link"} else {""}, handle.id()), 100);
        let raw_proof = self.load_block_proof_raw(handle, is_link).await?;
        BlockProofStuff::deserialize(handle.id(), raw_proof, is_link)
    }

    pub async fn load_block_proof_raw(&self, handle: &BlockHandle, is_link: bool) -> Result<Vec<u8>> {
        log::trace!("load_block_proof_raw {} {}", if is_link {"link"} else {""}, handle.id());
        let (entry_id, inited) = if is_link {
            (PackageEntryId::<_, UInt256, UInt256>::ProofLink(handle.id()), handle.has_proof_link())
        } else {
            (PackageEntryId::<_, UInt256, UInt256>::Proof(handle.id()), handle.has_proof())
        };
        if !inited {
            fail!("This proof{} is not in the archive: {:?}", if is_link { "link" } else { "" }, handle);
        }
        self.archive_manager.get_file(handle, &entry_id).await
    }

    pub async fn store_shard_state_dynamic(
        &self,
        handle: &Arc<BlockHandle>, 
        state: &Arc<ShardStateStuff>,
        callback: Option<Arc<dyn Callback>>,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
    ) -> Result<(Arc<ShardStateStuff>, bool)> {

        let _tc = TimeChecker::new(format!("store_shard_state_dynamic {}", state.block_id()), 300);
        if handle.id() != state.block_id() {
            fail!(NodeError::InvalidArg("`state` and `handle` mismatch".to_string()))
        }
        let _lock = handle.saving_state_lock().lock().await;
        if !handle.has_state() {
            let saved_root = self.shard_state_dynamic_db.put(
                state.block_id(), 
                state.root_cell().clone(),
                check_stop,
            )?;
            if handle.set_state() {
                self.store_block_handle(handle, callback)?;
                let state = ShardStateStuff::from_root_cell(
                    handle.id().clone(), 
                    saved_root,
                    #[cfg(feature = "telemetry")]
                    &self.telemetry,
                    &self.allocated
                )?;
                return Ok((state, true));
            }
        }
        Ok((self.load_shard_state_dynamic(handle.id())?, false))
    }

    pub async fn store_shard_state_dynamic_raw_force(
        &self,
        handle: &Arc<BlockHandle>, 
        state_root: Cell,
        callback: Option<Arc<dyn Callback>>,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
    ) -> Result<Cell> {
        let _tc = TimeChecker::new(format!("store_shard_state_dynamic_raw_force {}", handle.id()), 300);
        let saved_root = self.shard_state_dynamic_db.put(handle.id(), state_root, check_stop)?;
        let _lock = handle.saving_state_lock().lock().await;
        self.store_block_handle(handle, callback)?;
        return Ok(saved_root);
    }

    pub fn load_shard_state_dynamic(&self, id: &BlockIdExt) -> Result<Arc<ShardStateStuff>> {
        let _tc = TimeChecker::new(format!("load_shard_state_dynamic {}", id), 10);        
        Ok(
            ShardStateStuff::from_root_cell(
                id.clone(), 
                self.shard_state_dynamic_db.get(id)?,
                #[cfg(feature = "telemetry")]
                &self.telemetry,
                &self.allocated
            )?
        )
    }

    pub async fn store_shard_state_persistent(
        &self, 
        handle: &Arc<BlockHandle>, 
        state: &Arc<ShardStateStuff>,
        callback: Option<Arc<dyn Callback>>,
        abort: Box<dyn Fn() -> bool + Send + Sync>
    ) -> Result<()> {
        let _tc = TimeChecker::new(format!("store_shard_state_persistent {}", state.block_id()), 10_000);
        if handle.id() != state.block_id() {
            fail!(NodeError::InvalidArg("`state` and `handle` mismatch".to_string()))
        }
        if !handle.has_persistent_state() {
            let state1 = state.clone();
            let bytes = tokio::task::spawn_blocking(move || {
                state1.serialize_with_abort(&abort)
            }).await??;
            self.shard_state_persistent_db.put(state.block_id(), &bytes).await?;
            if handle.set_persistent_state() {
                self.store_block_handle(handle, callback)?;
            }
        }
        Ok(())
    }

    pub async fn store_shard_state_persistent_raw(
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

            // TODO write directly into file without huge vector

            self.shard_state_persistent_db.put(handle.id(), state_data).await?;
            if handle.set_persistent_state() {
                self.store_block_handle(handle, callback)?;
            }
        }
        Ok(())
    }

    pub async fn load_shard_state_persistent_slice(&self, id: &BlockIdExt, offset: u64, length: u64) -> Result<Vec<u8>> {
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

    pub async fn load_shard_state_persistent(&self, id: &BlockIdExt) -> Result<Arc<ShardStateStuff>> {
        let _tc = TimeChecker::new(format!("load_shard_state_persistent {}", id), 200);
        let full_lenth = self.load_shard_state_persistent_size(id).await?;

        // TODO read directly from file without huge vector

        let data = self.shard_state_persistent_db.get_vec(id, 0, full_lenth).await?;
        ShardStateStuff::deserialize_inmem(
            id.clone(),
            Arc::new(data),
            #[cfg(feature = "telemetry")]
            &self.telemetry,
            &self.allocated
        )
    }

    pub async fn load_shard_state_persistent_size(&self, id: &BlockIdExt) -> Result<u64> {
        let _tc = TimeChecker::new(format!("load_shard_state_persistent_size {}", id), 50);
        self.shard_state_persistent_db.get_size(id).await
    }

    pub async fn shard_state_persistent_gc(
        &self,
        calc_ttl: impl Fn(u32) -> (u32, bool),
        zerostate_id: &BlockIdExt,
    ) -> Result<()> {
        let _tc = TimeChecker::new(format!("shard_state_persistent_gc"), 5000);
        let mut for_delete = HashSet::new();
        self.shard_state_persistent_db.for_each_key(&mut |key| {

            // In `impl DbKey for BlockIdExt` you can see that only root hash is used, 
            // so it is correct to build id next way, because key in shard_state_persistent_db
            // is a block's root hash.
            let id = BlockIdExt {
                root_hash: UInt256::from(key),
                ..Default::default()
            };

            if id.root_hash() == zerostate_id.root_hash() {
                log::info!("  Zerostate: {:x}", zerostate_id.root_hash());
                return Ok(true);
            }

            let convert_to_utc = |t| {
                chrono::prelude::DateTime::<chrono::Utc>::from(
                    UNIX_EPOCH + Duration::from_secs(t as u64)
                ).naive_utc()
            };

            match self.load_block_handle(&id)? {
                None => log::warn!("shard_state_persistent_gc: can't load handle for {:x}", id.root_hash()),
                Some(handle) => {
                    let gen_utime = handle.gen_utime()?;
                    let (ttl, expired) = calc_ttl(gen_utime);
                    log::info!(
                        "{} Persistent state: {:x}, mc block: {}, gen_utime: {} UTC ({}), expired at: {} UTC ({})",
                        if expired {"X"} else {" "},
                        handle.id().root_hash(),
                        handle.masterchain_ref_seq_no(),
                        convert_to_utc(gen_utime),
                        handle.gen_utime()?,
                        convert_to_utc(ttl),
                        ttl);
                    if expired {
                        for_delete.insert(id);
                    }
                }
            }
            Ok(true)
        })?;

        for id in for_delete {
            match self.shard_state_persistent_db.delete(&id).await {
                Ok(_) => log::debug!("shard_state_persistent_gc: {:x} deleted", id.root_hash()),
                Err(e) => log::warn!("shard_state_persistent_gc: can't delete {:x}: {}", id.root_hash(), e)
            }
        }

        Ok(())
    }

    pub fn store_block_prev1(
        &self, 
        handle: &Arc<BlockHandle>, 
        prev: &BlockIdExt,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        self.store_block_linkage(
            handle, prev, &self.prev1_block_db, "store_block_prev1", 
            |handle| handle.has_prev1(),
            |handle| handle.set_prev1(),
            callback
        )
    }

    pub fn load_block_prev1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.load_block_linkage(id, &self.prev1_block_db, "load_block_prev1")
    }

    pub fn store_block_prev2(
        &self, 
        handle: &Arc<BlockHandle>, 
        prev2: &BlockIdExt,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        self.store_block_linkage(
            handle, prev2, &self.prev2_block_db, "store_block_prev2", 
            |handle| handle.has_prev2(),
            |handle| handle.set_prev2(),
            callback
        )
    }

    pub fn load_block_prev2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.load_block_linkage(id, &self.prev2_block_db, "load_block_prev2")
    }

    pub fn store_block_next1(
        &self, 
        handle: &Arc<BlockHandle>, 
        next: &BlockIdExt,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        self.store_block_linkage(
            handle, next, &self.next1_block_db, "store_block_next1", 
            |handle| handle.has_next1(),
            |handle| handle.set_next1(),
            callback
        )
    }

    pub fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.load_block_linkage(id, &self.next1_block_db, "load_block_next1")
    }

    pub fn store_block_next2(
        &self, 
        handle: &Arc<BlockHandle>, 
        next2: &BlockIdExt,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        self.store_block_linkage(
            handle, next2, &self.next2_block_db, "store_block_next2", 
            |handle| handle.has_next2(),
            |handle| handle.set_next2(),
            callback
        )
    }

    pub fn load_block_next2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.load_block_linkage(id, &self.next2_block_db, "load_block_next2")
    }

    pub fn store_block_applied(
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

    pub async fn archive_block(
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
    
    pub fn load_full_node_state(&self, key: &'static str) -> Result<Option<Arc<BlockIdExt>>> {
        let _tc = TimeChecker::new(format!("load_full_node_state {}", key), 10);
        self.block_handle_storage.load_full_node_state(key)
    }

    pub fn save_full_node_state(&self, key: &'static str, block_id: &BlockIdExt) -> Result<()> {
        let _tc = TimeChecker::new(format!("save_full_node_state {}", key), 10);
        self.block_handle_storage.save_full_node_state(key, block_id)
    }

    pub fn drop_validator_state(&self, key: &'static str) -> Result<()> {
        let _tc = TimeChecker::new(format!("drop_validator_state {}", key), 10);
        self.block_handle_storage.drop_validator_state(key)
    }

    pub fn load_validator_state(&self, key: &'static str) -> Result<Option<Arc<BlockIdExt>>> {
        let _tc = TimeChecker::new(format!("load_validator_state {}", key), 10);
        self.block_handle_storage.load_validator_state(key)
    }

    pub fn save_validator_state(&self, key: &'static str, block_id: &BlockIdExt) -> Result<()> {
        let _tc = TimeChecker::new(format!("save_validator_state {}", key), 10);
        self.block_handle_storage.save_validator_state(key, block_id)
    }

    pub fn archive_manager(&self) -> &Arc<ArchiveManager> {
        &self.archive_manager
    }

    pub fn assign_mc_ref_seq_no(
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

    pub fn save_top_shard_block(&self, id: &TopBlockDescrId, tsb: &TopBlockDescrStuff) -> Result<()> {
        let _tc = TimeChecker::new(format!("save_top_shard_block {}", id), 50);
        self.shard_top_blocks_db.put(&id.to_bytes()?, &tsb.to_bytes()?)
    }

    pub fn load_all_top_shard_blocks(&self) -> Result<HashMap<TopBlockDescrId, TopBlockDescrStuff>> {
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


    pub fn remove_top_shard_block(&self, id: &TopBlockDescrId) -> Result<()> {
        let _tc = TimeChecker::new(format!("remove_top_shard_block {}", id), 50);
        self.shard_top_blocks_db.delete(&id.to_bytes()?)
    }

    pub fn db_root_dir(&self) -> Result<&str> {
        Ok(&self.config.db_directory)
    }

    pub fn adjust_states_gc_interval(&self, interval_ms: u32) {
        let prev = self.cells_gc_interval.swap(interval_ms, Ordering::Relaxed);
        log::info!("Adjusted states gc interval {} -> {}", prev, interval_ms);
    }

    pub async fn truncate_database(&self, mc_block_id: &BlockIdExt, processed_wc: i32) -> Result<()> {
        // store shard blocks to truncate
        let prev_id = self.load_block_prev1(mc_block_id)?;
        let prev_handle = self.load_block_handle(&prev_id)?
            .ok_or_else(|| error!("there is no handle for block {}", prev_id))?;
        let prev_block = self.load_block_data(&prev_handle).await?;
        let top_blocks = prev_block.shard_hashes()?.top_blocks(&[processed_wc])?;

        // truncate archives
        self.archive_manager.trunc(mc_block_id, &|id: &BlockIdExt| {
            if id.shard().is_masterchain() {
                return id.seq_no() >= mc_block_id.seq_no();
            } else {
                for tb in &top_blocks {
                    if id.shard().intersect_with(tb.shard()) && id.seq_no() > tb.seq_no() {
                        return true;
                    }
                }
            }
            false
        }).await?;

        // truncate handles and prev/next links
        fn clear_dbs(db: &InternalDb, id: BlockIdExt) {
            log::trace!("truncate_database: trying to drop handle {}", id);
            let _ = db.block_handle_storage.drop_handle(id.clone(), None);
            let _ = db.prev2_block_db.delete(&id);
            let _ = db.prev1_block_db.delete(&id);
            let _ = db.next2_block_db.delete(&id);
            let _ = db.next1_block_db.delete(&id);
        }

        self.next1_block_db.for_each(&mut |_key, val| {
            let id = BlockIdExt::deserialize(&mut Cursor::new(&val))?;
            if id.shard().is_masterchain() && id.seq_no() >= mc_block_id.seq_no() {
                clear_dbs(self, id);
            } else {
                if id.shard().workchain_id() == processed_wc {
                    for tb in &top_blocks {
                        if id.shard().intersect_with(tb.shard()) {
                            if id.seq_no() > tb.seq_no() {
                                clear_dbs(self, id);
                                break;
                            }
                        }
                    }
                }
            }
            Ok(true)
        })?;
        self.next2_block_db.for_each(&mut |_key, val| {
            let id = BlockIdExt::deserialize(&mut Cursor::new(&val))?;
            if id.shard().workchain_id() == processed_wc {
                for tb in &top_blocks {
                    if id.shard().intersect_with(tb.shard()) {
                        if id.seq_no() > tb.seq_no() {
                            clear_dbs(self, id);
                            break;
                        }
                    }
                }
            }
            Ok(true)
        })?;

        // truncate info related with last handles
        fn clear_last_handle(db: &InternalDb, id: &BlockIdExt) {
            log::trace!("truncate_database: clear_last_handle {}", id);
            let _ = db.next1_block_db.delete(id);
            if let Ok(Some(handle)) = db.load_block_handle(id) {
                handle.reset_next1();
                handle.reset_next2();
                let _ = db.store_block_handle(&handle, None);
            }
        }

        clear_last_handle(self, &prev_id);
        for id in &top_blocks {
            clear_last_handle(self, &id);
        }

        Ok(())
    }

    pub fn reset_unapplied_handles(&self) -> Result<()> {
        let _tc = TimeChecker::new(format!("reset_unapplied_handles"), 1000);
        self.block_handle_storage.for_each_keys(&mut |id| {
            if let Ok(Some(handle)) = self.load_block_handle(&id) {
                if !handle.is_applied() {
                    handle.reset_state();
                    let _ = self.store_block_handle(&handle, None);
                }
            }
            Ok(true)
        })?;
        Ok(())
    }
}

