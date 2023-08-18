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

use crate::{
    block::BlockStuff, block_proof::BlockProofStuff, engine_traits::EngineAlloc, error::NodeError,
    shard_state::ShardStateStuff, types::top_block_descr::{TopBlockDescrId, TopBlockDescrStuff},
    internal_db::restore::check_db, config::CellsDbConfig,

};
#[cfg(feature = "telemetry")]
use crate::engine_traits::EngineTelemetry;

use std::{
    cmp::min, collections::{HashMap, HashSet}, io::Cursor, mem::size_of, path::{Path, PathBuf},
    sync::{Arc, atomic::{AtomicBool, AtomicU32, Ordering}}, time::{UNIX_EPOCH, Duration}, ops::Deref
};
use storage::{
    TimeChecker,
    archives::{archive_manager::ArchiveManager, package_entry_id::PackageEntryId},
    block_handle_db::{self, BlockHandle, BlockHandleDb, BlockHandleStorage}, 
    block_info_db::BlockInfoDb, db::rocksdb::RocksDb, node_state_db::NodeStateDb, 
    types::BlockMeta, db::filedb::FileDb,
    shard_top_blocks_db::ShardTopBlocksDb, StorageAlloc, traits::Serializable,
};
use storage::shardstate_db_async::{self, AllowStateGcResolver, ShardStateDb};
#[cfg(feature = "telemetry")]
use storage::StorageTelemetry;
use ton_block::{Block, BlockIdExt, INVALID_WORKCHAIN_ID};
use ton_types::{error, fail, Result, UInt256, Cell, BocWriter, MAX_SAFE_DEPTH};
use ton_types::{DoneCellsStorage};

/// Full node state keys
pub const INITIAL_MC_BLOCK: &str         = "InitMcBlockId";
pub const LAST_APPLIED_MC_BLOCK: &str    = "LastMcBlockId";
pub const PSS_KEEPER_MC_BLOCK: &str      = "PssKeeperBlockId";
pub const SHARD_CLIENT_MC_BLOCK: &str    = "ShardsClientMcBlockId";
pub const ARCHIVES_GC_BLOCK: &str        = "ArchivesGcMcBlockId";
pub const ASSUME_OLD_FORMAT_CELLS: &str  = "AssumeOldFormatCells";
pub const LAST_UNNEEDED_KEY_BLOCK: &str  = storage::db::rocksdb::LAST_UNNEEDED_KEY_BLOCK;
pub const DB_VERSION: &str  = "DbVersion";

pub const DB_VERSION_0: u32  = 0;
pub const _DB_VERSION_1: u32  = 1; // with fixed cells/bits counter in StorageCell
pub const DB_VERSION_2: u32  = 2; // with async cells storage
pub const DB_VERSION_3: u32  = 3; // with faster cells storage (separated counters)
pub const DB_VERSION_4: u32  = 4; // with updated cells (with counter) and correct allow_old_cells property
pub const CURRENT_DB_VERSION: u32 = DB_VERSION_4;

const CELLS_CF_NAME: &str = "cells_db";

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
    pub fn _to_any(self) -> Arc<BlockHandle> {
        self.handle
    }

    /// Assert creation
    pub fn _to_created(self) -> Option<Arc<BlockHandle>> {
        match self.status {
            DataStatus::Created => Some(self.handle),
            _ => None
        }
    }

    /// Assert non-creation
    pub fn to_non_created(self) -> Option<Arc<BlockHandle>> {
        match self.status {
            DataStatus::Created => None,
            _ => Some(self.handle)
        }
    }

    /// Assert non-update
    pub fn to_non_updated(self) -> Option<Arc<BlockHandle>> {
        match self.status {
            DataStatus::Updated => None,
            _ => Some(self.handle)
        }
    }

    /// Assert update
    pub fn to_updated(self) -> Option<Arc<BlockHandle>> {
        match self.status {
            DataStatus::Updated => Some(self.handle),
            _ => None
        }
    }

    /// Check update
    pub fn _is_updated(&self) -> bool {
        match self.status {
            DataStatus::Updated => true,
            _ => false
        }
    }

}

pub mod state_gc_resolver;
pub mod restore;
mod update;

#[derive(serde::Deserialize, Default)]
pub struct InternalDbConfig {
    pub db_directory: String,
    pub cells_gc_interval_sec: u32,
    pub cells_db_config: CellsDbConfig,
}

pub struct InternalDb {
    db: Arc<RocksDb>,
    block_handle_storage: Arc<BlockHandleStorage>,
    prev1_block_db: BlockInfoDb,
    prev2_block_db: BlockInfoDb,
    next1_block_db: BlockInfoDb,
    next2_block_db: BlockInfoDb,
    shard_state_persistent_db: Arc<FileDb>,
    shard_state_dynamic_db: Arc<ShardStateDb>,
    archive_manager: Arc<ArchiveManager>,
    shard_top_blocks_db: ShardTopBlocksDb,
    full_node_state_db: Arc<NodeStateDb>,

    config: InternalDbConfig,
    cells_gc_interval: Arc<AtomicU32>,
    #[cfg(feature = "telemetry")]
    telemetry: Arc<EngineTelemetry>,
    allocated: Arc<EngineAlloc>,
}

#[allow(dead_code)]
impl InternalDb {

    pub async fn with_update(
        config: InternalDbConfig,
        restore_db_enabled: bool,
        force_check_db: bool,
        allow_update: bool,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
        is_broken: Option<&AtomicBool>,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<EngineTelemetry>,
        allocated: Arc<EngineAlloc>,
    ) -> Result<Self> {
        let mut db = Self::construct(
            config,
            allow_update,
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated,
        ).await?;
        let version = db.resolve_db_version()?;
        if version != CURRENT_DB_VERSION {
            if allow_update {
                db = update::update(db, version, check_stop, is_broken, force_check_db, 
                    restore_db_enabled).await?
            } else {
                fail!(
                    "DB version {} does not correspond to current supported one {}.", 
                    version, 
                    CURRENT_DB_VERSION
                )
            }
        } else {
            log::info!("DB VERSION {}", version);
            // TODO correct workchain id needed here, but it will be known later
            db = check_db(db, 0, restore_db_enabled, force_check_db, check_stop, is_broken).await?;
        }
        Ok(db)
    }

    async fn construct(
        config: InternalDbConfig,
        allow_update: bool,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<EngineTelemetry>,
        allocated: Arc<EngineAlloc>,
    ) -> Result<Self> {
        let mut hi_perf_cfs = HashSet::new();
        hi_perf_cfs.insert(CELLS_CF_NAME.to_string());
        let db = RocksDb::with_options(config.db_directory.as_str(), "db", hi_perf_cfs, false)?;
        let db_catchain = RocksDb::with_path(config.db_directory.as_str(), "catchains")?;
        let block_handle_db = Arc::new(
            BlockHandleDb::with_db(db.clone(), "block_handle_db", true)?
        );
        let full_node_state_db = Arc::new(
            NodeStateDb::with_db(db.clone(), storage::db::rocksdb::NODE_STATE_DB_NAME, true)?
        );
        let validator_state_db = Arc::new(
            NodeStateDb::with_db(db_catchain, "validator_state_db", true)?
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

        let mut assume_old_cells = false;
        if let Some(db_slice) = full_node_state_db.try_get(&ASSUME_OLD_FORMAT_CELLS)? {
            assume_old_cells = 1 == *db_slice.first()
                .ok_or_else(|| error!("Empty value for ASSUME_OLD_FORMAT_CELLS property"))?;
        } else if allow_update {
            let version = if let Some(db_slice) = full_node_state_db.try_get(&DB_VERSION)? {
                let mut cursor = Cursor::new(db_slice.as_ref());
                u32::deserialize(&mut cursor)?
            } else {
                if block_handle_storage.is_empty()? {
                    CURRENT_DB_VERSION
                } else {
                    0
                }
            };
            if version < DB_VERSION_3 {
                assume_old_cells = true;
            }
            if allow_update {
                full_node_state_db.put(&ASSUME_OLD_FORMAT_CELLS, &[assume_old_cells as u8])?;
            }
        }
        log::info!("Cells db - assume old cells: {}", assume_old_cells);
        let shard_state_dynamic_db = Self::create_shard_state_dynamic_db(
            db.clone(),
            &config,
            assume_old_cells,
            false,
            #[cfg(feature = "telemetry")]
            telemetry.storage.clone(),
            allocated.storage.clone()
        )?;
        let last_unneeded_key_block = block_handle_storage
            .load_full_node_state(LAST_UNNEEDED_KEY_BLOCK)?.unwrap_or_default();
        let archive_manager = Arc::new(
            ArchiveManager::with_data(
                db.clone(),
                Arc::new(PathBuf::from(&config.db_directory)),
                last_unneeded_key_block.seq_no(),
                #[cfg(feature = "telemetry")]
                telemetry.storage.clone(),
                allocated.storage.clone()
            ).await?
        );

        let db = Self {
            db: db.clone(),
            block_handle_storage,
            prev1_block_db: BlockInfoDb::with_db(db.clone(), "prev1_block_db", true)?,
            prev2_block_db: BlockInfoDb::with_db(db.clone(), "prev2_block_db", true)?,
            next1_block_db: BlockInfoDb::with_db(db.clone(), "next1_block_db", true)?,
            next2_block_db: BlockInfoDb::with_db(db.clone(), "next2_block_db", true)?,
            shard_state_persistent_db: Arc::new(FileDb::with_path(
                Path::new(config.db_directory.as_str()).join("shard_state_persistent_db")
            )),
            shard_state_dynamic_db,
            archive_manager,
            shard_top_blocks_db: ShardTopBlocksDb::with_db(db.clone(), "shard_top_blocks_db", true)?,
            full_node_state_db,

            cells_gc_interval: Arc::new(AtomicU32::new(config.cells_gc_interval_sec)),
            config,
            #[cfg(feature = "telemetry")]
            telemetry, 
            allocated
        };

        Ok(db)
    }

    fn resolve_db_version(&self) -> Result<u32> {
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

    pub fn load_db_version(&self) -> Result<u32> {
        if let Some(db_slice) = self.full_node_state_db.try_get(&DB_VERSION)? {
            let mut cursor = Cursor::new(db_slice.as_ref());
            u32::deserialize(&mut cursor)
        } else {
            Ok(DB_VERSION_0)
        }
    }

    fn create_shard_state_dynamic_db(
        db: Arc<RocksDb>,
        config: &InternalDbConfig,
        assume_old_cells: bool,
        update_cells: bool,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<StorageTelemetry>,
        allocated: Arc<StorageAlloc>,
    ) -> Result<Arc<ShardStateDb>> {
        ShardStateDb::new(
            db,
            "shardstate_db",
            CELLS_CF_NAME,
            &config.db_directory,
            assume_old_cells,
            update_cells,
            config.cells_db_config.states_db_queue_len,
            config.cells_db_config.max_pss_slowdown_mcs,
            config.cells_db_config.prefill_cells_counters,
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated
        )
    }

    pub fn clean_shard_state_dynamic_db(&mut self) -> Result<()> {
        if self.shard_state_dynamic_db.is_gc_run() {
            fail!("It is forbidden to clear shard_state_dynamic_db while cells GC is running")
        }

        if let Err(e) = self.db.drop_table_force("shardstate_db") {
            log::warn!("Can't drop table \"shardstate_db\": {}", e);
        }
        if let Err(e) = self.db.drop_table_force(CELLS_CF_NAME) {
            log::warn!("Can't drop table \"cells_db\": {}", e);
        }
        let _ = self.db.drop_table_force("cells_db1"); // depricated table, used in db versions 1 & 2
        self.full_node_state_db.put(&ASSUME_OLD_FORMAT_CELLS, &[0])?;

        self.shard_state_dynamic_db = Self::create_shard_state_dynamic_db(
            self.db.clone(),
            &self.config,
            false,
            false,
            #[cfg(feature = "telemetry")]
            self.telemetry.storage.clone(),
            self.allocated.storage.clone()
        )?;

        Ok(())
    }

    pub async fn update_cells_db_upto_4(&mut self) -> Result<()> {
        // Node is just started when we call this function, so it is ok to block one worker here.
        self.shard_state_dynamic_db = tokio::task::block_in_place(|| {
            Self::create_shard_state_dynamic_db(
                self.db.clone(),
                &self.config,
                true,
                true,
                #[cfg(feature = "telemetry")]
                self.telemetry.storage.clone(),
                self.allocated.storage.clone()
            )
        })?;
        self.full_node_state_db.put(&ASSUME_OLD_FORMAT_CELLS, &[0])?;
        Ok(())
    }

    pub fn start_states_gc(&self, resolver: Arc<dyn AllowStateGcResolver>) {
        self.shard_state_dynamic_db.clone().start_gc(resolver, self.cells_gc_interval.clone())
    }

    pub async fn stop_states_db(&self) {
        self.shard_state_dynamic_db.stop().await
    }

    fn store_block_handle(
        &self, 
        handle: &Arc<BlockHandle>,
        callback: Option<Arc<dyn block_handle_db::Callback>>
    ) -> Result<()> {
        let _tc = TimeChecker::new(format!("store_block_handle {}", handle.id()), 30);
        self.block_handle_storage.save_handle(handle, callback)
    }

    fn load_block_linkage(
        &self, 
        id: &BlockIdExt,
        db: &BlockInfoDb,
        msg: &str
    ) -> Result<Option<BlockIdExt>> {
        let _tc = TimeChecker::new(format!("{} {}", msg, id), 30);
        let Some(bytes) = db.try_get(id)? else {
            return Ok(None)
        };
        let mut cursor = Cursor::new(&bytes);
        let ret = BlockIdExt::deserialize(&mut cursor)?;
        Ok(Some(ret))
    }

    fn store_block_linkage(
        &self, 
        handle: &Arc<BlockHandle>, 
        linkage: &BlockIdExt,
        db: &BlockInfoDb,
        msg: &str,
        check_has: impl Fn(&Arc<BlockHandle>) -> bool,
        check_set: impl Fn(&Arc<BlockHandle>) -> bool,
        callback: Option<Arc<dyn block_handle_db::Callback>>
    ) -> Result<()> {
        let _tc = TimeChecker::new(format!("{} {}", msg, handle.id()), 30);
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
        is_queue_update: Option<(i32, bool)>, // (update for, is_empty)
        utime: Option<u32>,
        callback: Option<Arc<dyn block_handle_db::Callback>>
    ) -> Result<BlockResult> {
        let _tc = TimeChecker::new(format!("create_or_load_block_handle {}", id), 30);

        if let Some(handle) = self.load_block_handle(id)? {
            return Ok(BlockResult::with_status(handle, DataStatus::Fetched))
        }
        let meta = if let Some(block) = block {
            if let Some((target_wc, is_empty)) = is_queue_update {
                BlockMeta::from_queue_update(block, target_wc, is_empty)?
            } else {
                BlockMeta::from_block(block)?
            }
        } else if id.seq_no == 0 {
            if let Some(utime) = utime {
                BlockMeta::with_data(0, utime, 0, 0, INVALID_WORKCHAIN_ID)
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
        let _tc = TimeChecker::new(format!("load_block_handle {}", id), 30);
        self.block_handle_storage.load_handle(id)
    }

    pub async fn store_block_data(
        &self, 
        block: &BlockStuff,
        callback: Option<Arc<dyn block_handle_db::Callback>>
    ) -> Result<BlockResult> {
        let _tc = TimeChecker::new(format!("store_block_data {}", block.id()), 100);
        let mut result = self.create_or_load_block_handle(
            block.id(),
            Some(block.block_or_queue_update()?),
            block.is_queue_update_for().map(|o| (o, false)),
            None,
            callback.clone()
        )?;
        let handle = result.clone().to_non_updated().ok_or_else(
            || error!(
                "INTERNAL ERROR: block {} result mismatch in store_block_data {:?}", 
                block.id(), result
            )
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
        if let Some(target_wc)= handle.is_queue_update_for() {
            BlockStuff::deserialize_queue_update(handle.id().clone(), target_wc, raw_block)
        } else {
            BlockStuff::deserialize_block(handle.id().clone(), raw_block)
        }
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

    pub fn find_mc_block_by_seq_no_without_state(&self, seqno: u32) -> Result<Arc<BlockHandle>> {
        let _tc = TimeChecker::new(format!("find_mc_block_by_seq_no_without_state {}", seqno), 300);
        let mut found = None;
        self.next1_block_db.for_each(&mut |_key, val| {
            let id = BlockIdExt::deserialize(&mut Cursor::new(&val))?;
            if id.shard().is_masterchain() && id.seq_no() == seqno {
                found = Some(id);
                Ok(false)
            } else {
                Ok(true)
            }
        })?;
        if let Some(id) = found {
            self.load_block_handle(&id)?.ok_or_else(
                || error!("Cannot load handle for master block {}", id)
            )
        } else {
            fail!("Can't find mc block with seqno {}", seqno)
        }
    }

    pub async fn store_block_proof(
        &self, 
        id: &BlockIdExt,
        handle: Option<Arc<BlockHandle>>, 
        proof: &BlockProofStuff,
        callback: Option<Arc<dyn block_handle_db::Callback>>
    ) -> Result<BlockResult> {

        let _tc = TimeChecker::new(format!("store_block_proof {}", proof.id()), 100);

        if let Some(handle) = &handle {
            if handle.id() != id {
                fail!("Block handle and id mismatch: {} vs {}", handle.id(), id)
            }
        }
        if id != proof.id() {
            fail!(NodeError::InvalidArg("`proof` and `id` mismatch".to_string()))
        }

        let mut result = if let Some(handle) = handle {
            BlockResult::with_status(handle, DataStatus::Fetched)
        } else {
            let (virt_block, _) = proof.virtualize_block()?;
            self.create_or_load_block_handle(id, Some(&virt_block), None, None, callback.clone())?
        };
        let handle = result.clone().to_non_updated().ok_or_else(
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

    pub async fn load_block_proof(&self, handle: &BlockHandle, is_link: bool) -> Result<BlockProofStuff> {
        let _tc = TimeChecker::new(format!("load_block_proof {} {}", if is_link {"link"} else {""}, handle.id()), 100);
        let raw_proof = self.load_block_proof_raw_(handle, is_link).await?;
        BlockProofStuff::deserialize(handle.id(), raw_proof, is_link)
    }

    pub async fn load_block_proof_raw(&self, handle: &BlockHandle, is_link: bool) -> Result<Vec<u8>> {
        let _tc = TimeChecker::new(format!("load_block_proof_raw {} {}", if is_link {"link"} else {""}, handle.id()), 100);
        self.load_block_proof_raw_(handle, is_link).await
    }

    async fn load_block_proof_raw_(&self, handle: &BlockHandle, is_link: bool) -> Result<Vec<u8>> {
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
        callback_handle: Option<Arc<dyn block_handle_db::Callback>>,
        callback_ss: Option<Arc<dyn shardstate_db_async::Callback>>,
        force: bool,
    ) -> Result<(Arc<ShardStateStuff>, bool)> {

        let timeout = 30;
        let _tc = TimeChecker::new(format!("store_shard_state_dynamic {}", state.block_id()), timeout);

        if handle.id() != state.block_id() {
            fail!(NodeError::InvalidArg("`state` and `handle` mismatch".to_string()))
        }
        let _lock = handle.saving_state_lock().lock().await;
        if force || !handle.has_state() {
            self.shard_state_dynamic_db.put(
                state.block_id(), 
                state.root_cell().clone(),
                callback_ss
            ).await?;
            if handle.set_state() {
                self.store_block_handle(handle, callback_handle)?;
            }
            return Ok((state.clone(), true))
        } else {
            Ok((self.load_shard_state_dynamic(handle.id())?, false))
        }
    }

    pub async fn store_shard_state_dynamic_raw_force(
        &self,
        handle: &Arc<BlockHandle>, 
        state_root: Cell,
        callback_ss: Option<Arc<dyn shardstate_db_async::Callback>>,
    ) -> Result<Cell> {

        let timeout = 30;
        let _tc = TimeChecker::new(format!("store_shard_state_dynamic_raw_force {}", handle.id()), timeout);
        let _lock = handle.saving_state_lock().lock().await;

        self.shard_state_dynamic_db.put(handle.id(), state_root.clone(), callback_ss).await?;
        Ok(state_root)
    }

    pub fn load_shard_state_dynamic_ex(
        &self,
        id: &BlockIdExt,
        use_cache: bool
    ) -> Result<Arc<ShardStateStuff>> {
        let _tc = TimeChecker::new(format!("load_shard_state_dynamic {}  use", id), 30);

        let handle = self.load_block_handle(id)?.ok_or_else(
            || error!("Cannot load handle for block {}", id)
        )?;

        let root_cell = self.shard_state_dynamic_db.get(handle.id(), use_cache)?;

        let ss = if let Some (target_wc) = handle.is_queue_update_for() {
            ShardStateStuff::from_out_msg_queue_root_cell(
                handle.id().clone(), 
                root_cell,
                target_wc,
                #[cfg(feature = "telemetry")]
                &self.telemetry,
                &self.allocated
            )?
        } else {
            ShardStateStuff::from_state_root_cell(
                handle.id().clone(), 
                root_cell,
                #[cfg(feature = "telemetry")]
                &self.telemetry,
                &self.allocated
            )?
        };
        Ok(ss)
    }

    pub fn load_shard_state_dynamic(&self, id: &BlockIdExt) -> Result<Arc<ShardStateStuff>> {
        self.load_shard_state_dynamic_ex(
            id, 
            true
        )
    }

    pub async fn store_shard_state_persistent(
        &self, 
        handle: &Arc<BlockHandle>, 
        state: Arc<ShardStateStuff>,
        callback: Option<Arc<dyn block_handle_db::Callback>>,
        abort: Arc<dyn Fn() -> bool + Send + Sync>
    ) -> Result<()> {
        let root_hash = state.root_cell().repr_hash();
        log::info!("store_shard_state_persistent block id: {}, state root {:x}",
            state.block_id(), root_hash);
        if handle.id() != state.block_id() {
            fail!(NodeError::InvalidArg("`state` and `handle` mismatch".to_string()))
        }
        if handle.has_persistent_state() {
            log::info!("store_shard_state_persistent {:x}: already saved", root_hash);
        } else {
            let id = handle.id().clone();
            let shard_state_dynamic_db = self.shard_state_dynamic_db.clone();
            let shard_state_persistent_db = self.shard_state_persistent_db.clone();
            tokio::task::spawn_blocking(move || -> Result<()> {
                let root_cell = state.root_cell().clone();
                // Drop state - don't keep in memory a root cell that keeps full tree!
                std::mem::drop(state);

                log::debug!("store_shard_state_persistent {}", id);
                let cells_storage = shard_state_dynamic_db.create_ordered_cells_storage(&root_hash)?;
                let now1 = std::time::Instant::now();
                let boc = BocWriter::with_params(
                    [root_cell], MAX_SAFE_DEPTH, cells_storage, abort.deref())?;
                log::info!("store_shard_state_persistent {:x} building boc TIME {}sec", root_hash, now1.elapsed().as_secs());

                let mut dest = shard_state_persistent_db.get_write_object(&id)?;
                let now2 = std::time::Instant::now();
                boc.write(&mut dest)?;
                log::info!(
                    "store_shard_state_persistent {:x} DONE; write boc TIME {}sec, total TIME {}sec",
                    root_hash, now2.elapsed().as_secs(), now1.elapsed().as_secs()
                );
                Ok(())
            }).await??;

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
        callback: Option<Arc<dyn block_handle_db::Callback>>
    ) -> Result<()> {
        let _tc = TimeChecker::new(
            format!("store_shard_state_persistent_raw {}", handle.id()),
            state_data.len() as u64 / 1000 + 10
        );
        if !handle.has_persistent_state() {
            self.shard_state_persistent_db.write_whole_file(handle.id(), state_data).await?;
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
            let data = self.shard_state_persistent_db.read_file_part(id, offset, length).await?;
            Ok(data)
        }
    }

    pub async fn load_shard_state_persistent(
        &self, 
        id: &BlockIdExt,
        abort: &dyn Fn() -> bool,
    ) -> Result<Arc<ShardStateStuff>> {
        let _tc = TimeChecker::new(format!("load_shard_state_persistent {}", id), 1000);

        // Fast (in-memory) version
        let data = self.shard_state_persistent_db.read_whole_file(id).await?;
        ShardStateStuff::deserialize_state_inmem(
            id.clone(),
            Arc::new(data),
            #[cfg(feature = "telemetry")]
            &self.telemetry,
            &self.allocated,
            abort
        )
    }

    pub async fn load_shard_state_persistent_size(&self, id: &BlockIdExt) -> Result<u64> {
        let _tc = TimeChecker::new(format!("load_shard_state_persistent_size {}", id), 50);
        self.shard_state_persistent_db.get_file_size(id).await
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
            match self.shard_state_persistent_db.delete_file(&id).await {
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
        callback: Option<Arc<dyn block_handle_db::Callback>>
    ) -> Result<()> {
        self.store_block_linkage(
            handle, prev, &self.prev1_block_db, "store_block_prev1", 
            |handle| handle.has_prev1(),
            |handle| handle.set_prev1(),
            callback
        )
    }

    pub fn load_block_prev1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.load_block_linkage(id, &self.prev1_block_db, "load_block_prev1")?.ok_or_else(
            || error!("No prev1 block for {}", id)
        )
    }

    pub fn store_block_prev2(
        &self, 
        handle: &Arc<BlockHandle>, 
        prev2: &BlockIdExt,
        callback: Option<Arc<dyn block_handle_db::Callback>>
    ) -> Result<()> {
        self.store_block_linkage(
            handle, prev2, &self.prev2_block_db, "store_block_prev2", 
            |handle| handle.has_prev2(),
            |handle| handle.set_prev2(),
            callback
        )
    }

    pub fn load_block_prev2(&self, id: &BlockIdExt) -> Result<Option<BlockIdExt>> {
        self.load_block_linkage(id, &self.prev2_block_db, "load_block_prev2")
    }

    pub fn store_block_next1(
        &self, 
        handle: &Arc<BlockHandle>, 
        next: &BlockIdExt,
        callback: Option<Arc<dyn block_handle_db::Callback>>
    ) -> Result<()> {
        self.store_block_linkage(
            handle, next, &self.next1_block_db, "store_block_next1", 
            |handle| handle.has_next1(),
            |handle| handle.set_next1(),
            callback
        )
    }

    pub fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.load_block_linkage(id, &self.next1_block_db, "load_block_next1")?.ok_or_else(
            || error!("No next1 block for {}", id)
        )
    }

    pub fn store_block_next2(
        &self, 
        handle: &Arc<BlockHandle>, 
        next2: &BlockIdExt,
        callback: Option<Arc<dyn block_handle_db::Callback>>
    ) -> Result<()> {
        self.store_block_linkage(
            handle, next2, &self.next2_block_db, "store_block_next2", 
            |handle| handle.has_next2(),
            |handle| handle.set_next2(),
            callback
        )
    }

    pub fn load_block_next2(&self, id: &BlockIdExt) -> Result<Option<BlockIdExt>> {
        self.load_block_linkage(id, &self.next2_block_db, "load_block_next2")
    }

    pub fn store_block_applied(
        &self, 
        handle: &Arc<BlockHandle>,
        callback: Option<Arc<dyn block_handle_db::Callback>>
    ) -> Result<bool> {
        let _tc = TimeChecker::new(format!("store_block_applied {}", handle.id()), 30);
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
        callback: Option<Arc<dyn block_handle_db::Callback>>
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
        ).await.or_else(|err| {
            log::error!(
                target: "storage",
                "Failed to move block to archive: {}. Error: {}",
                id,
                err
            );
            Ok(())
        })
    }
    
    pub fn load_full_node_state(&self, key: &'static str) -> Result<Option<Arc<BlockIdExt>>> {
        let _tc = TimeChecker::new(format!("load_full_node_state {}", key), 30);
        self.block_handle_storage.load_full_node_state(key)
    }

    pub fn save_full_node_state(&self, key: &'static str, block_id: &BlockIdExt) -> Result<()> {
        let _tc = TimeChecker::new(format!("save_full_node_state {}", key), 30);
        self.block_handle_storage.save_full_node_state(key, block_id)
    }

    pub fn drop_validator_state(&self, key: &'static str) -> Result<()> {
        let _tc = TimeChecker::new(format!("drop_validator_state {}", key), 30);
        self.block_handle_storage.drop_validator_state(key)
    }

    pub fn load_validator_state(&self, key: &'static str) -> Result<Option<Arc<BlockIdExt>>> {
        let _tc = TimeChecker::new(format!("load_validator_state {}", key), 30);
        self.block_handle_storage.load_validator_state(key)
    }

    pub fn save_validator_state(&self, key: &'static str, block_id: &BlockIdExt) -> Result<()> {
        let _tc = TimeChecker::new(format!("save_validator_state {}", key), 30);
        self.block_handle_storage.save_validator_state(key, block_id)
    }

    pub async fn get_archive_id(&self, mc_seq_no: u32) -> Option<u64> {
        let _tc = TimeChecker::new(format!("get_archive_id {}", mc_seq_no), 30);
        self.archive_manager.get_archive_id(mc_seq_no).await
    }

    pub async fn get_archive_slice(&self, archive_id: u64, offset: u64, limit: u32) -> Result<Vec<u8>> {
        let _tc = TimeChecker::new(
            format!("get_archive_slice id: {}, offset: {}, limit: {}", archive_id, offset, limit),
            300
        );
        self.archive_manager.get_archive_slice(archive_id, offset, limit).await
    }

    pub async fn clean_unapplied_files(&self, ids: &[BlockIdExt]) {
        let _tc = TimeChecker::new("clean_unapplied_files".to_owned(), 300);
        self.archive_manager.clean_unapplied_files(ids).await;
    }

    pub async fn archive_gc(&self, last_unneeded_key_block: &BlockIdExt) -> Result<()> {
        let _tc = TimeChecker::new(format!("archive_gc {}", last_unneeded_key_block), 300);
        self.archive_manager.gc(last_unneeded_key_block).await;
        self.save_full_node_state(LAST_UNNEEDED_KEY_BLOCK, last_unneeded_key_block)
    }

    pub fn assign_mc_ref_seq_no(
        &self, 
        handle: &Arc<BlockHandle>, 
        mc_seq_no: u32,
        callback: Option<Arc<dyn block_handle_db::Callback>>
    ) -> Result<()> {
        let _tc = TimeChecker::new(format!("assign_mc_ref_seq_no {}", handle.id()), 30);
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

        let mut invalid_entries = Vec::new();
        self.shard_top_blocks_db.for_each(&mut |id_bytes, tsb_bytes| {
            let id = TopBlockDescrId::from_bytes(&id_bytes);
            let tbds = TopBlockDescrStuff::from_bytes(tsb_bytes, false);

            match &id {
                Ok(id) => {
                    if let Err(e) = &tbds {
                        log::error!("Skipping invalid top block description for {id}: {e:?}");
                    }
                }
                Err(e) => log::error!("Skipping invalid top block description: {e:?}"),
            }

            if let (Ok(id), Ok(tbds)) = (id, tbds) {
                result.insert(id, tbds);
            } else {
                invalid_entries.push(id_bytes.to_owned());
            }
            Ok(true)
        })?;

        for id in invalid_entries {
            self.shard_top_blocks_db.delete(&id)?;
        }

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

    pub async fn truncate_database(&self, mc_block_id: &BlockIdExt) -> Result<()> {
        // store shard blocks to truncate
        let prev_id = self.load_block_prev1(mc_block_id)?;
        let prev_handle = self.load_block_handle(&prev_id)?
            .ok_or_else(|| error!("there is no handle for block {}", prev_id))?;
        let prev_block = self.load_block_data(&prev_handle).await?;
        let top_blocks = prev_block.shard_hashes()?.top_blocks_all()?;

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
        self.next2_block_db.for_each(&mut |_key, val| {
            let id = BlockIdExt::deserialize(&mut Cursor::new(&val))?;
            for tb in &top_blocks {
                if id.shard().intersect_with(tb.shard()) {
                    if id.seq_no() > tb.seq_no() {
                        clear_dbs(self, id);
                        break;
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

    pub fn create_done_cells_storage(
        &self, 
        root_cell_id: &UInt256
    ) -> Result<Box<dyn DoneCellsStorage>> {
        self.shard_state_dynamic_db.create_done_cells_storage(root_cell_id)
    }
}

