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
    StorageAlloc, cell_db::CellDb, 
    db::{rocksdb::RocksDbTable, traits::{DbKey, KvcWriteable}},
    dynamic_boc_rc_db::{DynamicBocDb, DoneCellsStorageAdapter, OrderedCellsStorageAdapter, CellsCounters, CellByHashStorageAdapter},
    traits::Serializable,
    TARGET, error::StorageError,
};
#[cfg(feature = "telemetry")]
use crate::StorageTelemetry;
use crate::db::rocksdb::RocksDb;
use std::{
    io::{Cursor, Read, Write},
    sync::{Arc, atomic::{AtomicU8, AtomicU32, Ordering}}, 
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use ton_block::BlockIdExt;
use ton_types::{
    ByteOrderRead, Cell, Result, fail, error, DoneCellsStorage, OrderedCellsStorage, 
    CellByHashStorage, UInt256,
};

pub trait AllowStateGcResolver: Send + Sync {
    fn allow_state_gc(&self, block_id: &BlockIdExt, save_utime: u64, gc_utime: u64) -> Result<bool>;
}

pub(crate) struct DbEntry {
    // Because key in db is not a full BlockIdExt it's need to store it here to use while GC.
    pub block_id: BlockIdExt,
    pub cell_id: UInt256,
    pub save_utime: u64,
}

impl DbEntry {
    pub fn with_params(block_id: BlockIdExt, cell_id: UInt256, save_utime: u64) -> Self {
        Self {block_id, cell_id, save_utime }
    }
}

impl Serializable for DbEntry {
    fn serialize<T: Write>(&self, writer: &mut T) -> Result<()> {
        self.block_id.serialize(writer)?;
        writer.write_all(self.cell_id.key())?;
        writer.write_all(&self.save_utime.to_le_bytes())?;
        Ok(())
    }

    fn deserialize<T: Read>(reader: &mut T) -> Result<Self> {
        let block_id = BlockIdExt::deserialize(reader)?;
        let mut buf = [0; 32];
        reader.read_exact(&mut buf)?;
        let cell_id = buf.into();
        let save_utime = reader.read_le_u64().unwrap_or(0);

        Ok(Self { block_id, cell_id, save_utime })
    }
}

pub enum Job {
    PutState(Cell, BlockIdExt),
    DeleteState(BlockIdExt),
}

#[async_trait::async_trait]
pub trait Callback: Sync + Send {
    async fn invoke(&self, job: Job, ok: bool);
}

pub struct SsNotificationCallback(tokio::sync::Notify);

#[async_trait::async_trait]
impl Callback for SsNotificationCallback {
    async fn invoke(&self, _job: Job, _ok: bool) {
        self.0.notify_one();
    }
}

impl SsNotificationCallback {
    pub fn new() -> Arc<Self> {
        Arc::new(Self(tokio::sync::Notify::new()))
    }
    pub async fn wait(&self) {
        self.0.notified().await;
    }
}

impl Job {
    pub fn block_id(&self) -> &BlockIdExt {
        match self {
            Job::PutState(_cell, id) => &id,
            Job::DeleteState(id) => &id,
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub struct CellsDbConfig {
    pub states_db_queue_len: u32,
    pub max_pss_slowdown_mcs: u32,
    pub prefill_cells_counters: bool,
    pub cache_cells_counters: bool,
    pub cells_lru_size: usize,
}

impl Default for CellsDbConfig {
    fn default() -> Self {
        Self {
            states_db_queue_len: 1000,
            max_pss_slowdown_mcs: 750,
            prefill_cells_counters: false,
            cache_cells_counters: false,
            cells_lru_size: 1_000_000,
        }
    }
}

pub struct ShardStateDb {
    db: Arc<RocksDb>,
    shardstate_db: Arc<dyn KvcWriteable<BlockIdExt>>,
    dynamic_boc_db: Arc<DynamicBocDb>,
    storer: tokio::sync::mpsc::UnboundedSender<(Job, Option<Arc<dyn Callback>>)>,
    in_queue: AtomicU32,
    stop: AtomicU8,
    config: CellsDbConfig,
    pss_slowdown_mcs: Arc<AtomicU32>,
    gc_resolver: tokio::sync::OnceCell<Arc<dyn AllowStateGcResolver>>,
    #[cfg(feature = "telemetry")]
    telemetry: Arc<StorageTelemetry>,
}

impl ShardStateDb {

    const MASK_GC_STARTED: u8 = 0x01;
    const MASK_WORKER: u8 = 0x02;
    const MASK_STOPPED: u8 = 0x80;

    pub fn new(
        db: Arc<RocksDb>,
        shardstate_db_path: &str,
        cell_db_path: &str,
        db_root_path: &str,
        assume_old_cells: bool,
        update_cells: bool,
        config: CellsDbConfig,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<StorageTelemetry>,
        allocated: Arc<StorageAlloc>
    ) -> Result<Arc<Self>> {

        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

        if assume_old_cells && config.prefill_cells_counters && !update_cells {
            fail!("assume_old_cells and prefill_cells_counters can't be enabled at the same time")
        }
        if !config.cache_cells_counters && config.prefill_cells_counters {
            fail!("cache_counters must be enabled if prefill_cells_counters is enabled")
        }

        let mut dynamic_boc_db = DynamicBocDb::with_db(
            Arc::new(CellDb::with_db(db.clone(), cell_db_path, true)?),
            db_root_path,
            assume_old_cells,
            config.cells_lru_size,
            #[cfg(feature = "telemetry")]
            telemetry.clone(),
            allocated.clone()
        );
        if update_cells {
            if assume_old_cells {
                dynamic_boc_db.check_and_update_cells()?;
            }
        }

        let ss_db = Arc::new(Self {
            db: db.clone(),
            shardstate_db: Arc::new(RocksDbTable::with_db(db.clone(), shardstate_db_path, true)?),
            dynamic_boc_db: Arc::new(dynamic_boc_db),
            storer: sender,
            in_queue: AtomicU32::new(0),
            stop: AtomicU8::new(0),
            config,
            pss_slowdown_mcs: Arc::new(AtomicU32::new(0)),
            gc_resolver: tokio::sync::OnceCell::new(),
            #[cfg(feature = "telemetry")]
            telemetry,
        });

        tokio::spawn({
            let ss_db = ss_db.clone();
            async move {
                ss_db.worker(receiver).await;
            }
        });

        Ok(ss_db)
    }

    pub fn start_gc(
        self: Arc<Self>,
        gc_resolver: Arc<dyn AllowStateGcResolver>,
        run_interval_adjustable_sec: Arc<AtomicU32>,
    ) {
        if self.gc_resolver.set(gc_resolver.clone()).is_err() {
            log::error!(target: TARGET, "INTERNAL ERROR: Attempt to set GC resolver twice");
        }
        self.stop.fetch_or(Self::MASK_GC_STARTED, Ordering::Relaxed);
        tokio::spawn(async move {

            fn check_and_stop(stop: &AtomicU8) -> bool {
                if (stop.load(Ordering::Relaxed) & ShardStateDb::MASK_STOPPED) != 0 {
                    stop.fetch_and(!ShardStateDb::MASK_GC_STARTED, Ordering::Relaxed);
                    log::warn!(target: TARGET, "ShardStateDb GC: stopped");
                    true
                } else {
                    false
                }
            }

            async fn sleep_nicely(stop: &AtomicU8, mut sleep_for_ms: u64) -> bool {
                const TIMEOUT_STEP: u64 = 1000;
                loop {
                    let interval = if sleep_for_ms > TIMEOUT_STEP {
                        TIMEOUT_STEP
                    } else {
                        sleep_for_ms
                    };
                    tokio::time::sleep(Duration::from_millis(interval)).await;
                    if check_and_stop(stop) {
                        return false
                    }
                    if sleep_for_ms <= TIMEOUT_STEP {
                        return true;
                    }
                    sleep_for_ms -= TIMEOUT_STEP;
                }
            }

            async fn wait_queue(in_queue: &AtomicU32, stop: &AtomicU8, max_queue_len: u32) -> bool {
                loop {
                    let in_queue = in_queue.load(Ordering::Relaxed);
                    metrics::gauge!("db_shardstate_queue", in_queue as f64);
                    if in_queue >= max_queue_len {
                        log::warn!(
                            target: TARGET, 
                            "ShardStateDb GC: waiting for queue (current queue length: {})",
                            in_queue
                        );
                        if !sleep_nicely(stop, 1000).await {
                            return false;
                        }
                    } else {
                        return true;
                    }
                }
            }

            log::debug!(target: TARGET, "ShardStateDb GC: started worker");

            let mut to_delete: Vec<BlockIdExt> = vec!();
            loop {
                let run_gc_interval = run_interval_adjustable_sec.load(Ordering::Relaxed) as u64;
                if to_delete.len() == 0 {
                    log::debug!(target: TARGET, "ShardStateDb GC: waiting for {run_gc_interval}sec...");
                    if !sleep_nicely(&self.stop, run_gc_interval * 1000).await {
                        return;
                    }
                } else {
                    let interval_ms = (run_gc_interval * 1000) / (to_delete.len() + 1) as u64;
                    while let Some(id) = to_delete.pop() {
                        if !wait_queue(&self.in_queue, &self.stop, self.config.states_db_queue_len).await {
                            return;
                        }
                        let in_queue = self.in_queue.fetch_add(1, Ordering::Relaxed) + 1;
                        metrics::gauge!("db_shardstate_queue", in_queue as f64);

                        let now = std::time::Instant::now();
                        let callback = SsNotificationCallback::new();
                        if let Err(e) = self.storer.send((Job::DeleteState(id.clone()), Some(callback.clone()))) {
                            log::error!(
                                target: TARGET, 
                                "Can't send state to delete from db, id {}",
                                e.0.0.block_id()
                            );
                        } else {
                            #[cfg(feature = "telemetry")]
                            self.telemetry.shardstates_queue.update(std::cmp::max(0, in_queue) as u64);
                            log::trace!(target: TARGET, "ShardStateDb GC: in_queue {}", in_queue);

                            loop {
                                let result = tokio::time::timeout(
                                    Duration::from_millis(1000),
                                    callback.wait()
                                ).await;
                                if let Err(tokio::time::error::Elapsed{..}) = result {
                                    if check_and_stop(&self.stop) {
                                        return;
                                    }
                                } else {
                                    break;
                                }
                            }

                            let elapsed = now.elapsed();
                            metrics::histogram!("db_shardstate_gc_time", elapsed);
                            let elapsed = elapsed.as_millis() as u64;
                            if elapsed > interval_ms {
                                log::warn!(
                                    target: TARGET,
                                    "ShardStateDb GC: deleting state {} was slower then given \
                                    interval, TIME {} ms, slot {} ms",
                                    id, elapsed, interval_ms,
                                );
                                if check_and_stop(&self.stop) {
                                    return;
                                }
                            } else {
                                if !sleep_nicely(&self.stop, interval_ms - elapsed).await {
                                    return;
                                }
                            }
                        }
                    }
                }

                log::debug!(target: TARGET, "ShardStateDb GC: collecting states to delete");

                let mut kept = 0;
                self.shardstate_db.for_each(&mut |_key, value| {
                    if check_and_stop(&self.stop) {
                        return Ok(false);
                    }
                    let entry = DbEntry::from_slice(value)?;
                    let time = 
                        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
                    match gc_resolver.allow_state_gc(&entry.block_id, entry.save_utime, time) {
                        Ok(true) => {
                            log::debug!(
                                target: TARGET, "ShardStateDb GC: delete  id {}", entry.block_id);
                            to_delete.push(entry.block_id);
                        },
                        Ok(false) => {
                            kept += 1;
                            log::debug!(
                                target: TARGET, "ShardStateDb GC: keep  id {}", entry.block_id);
                        },
                        Err(e) => log::warn!(
                            target: TARGET, 
                            "ShardStateDb  allow_state_gc  id {}  root_cell_id {:x}  error {}",
                            entry.block_id, entry.cell_id, e
                        ),
                    }
                    Ok(true)
                }).expect("Can't return error");

                if check_and_stop(&self.stop) {
                    return;
                }

                log::info!(
                    target: TARGET,
                    "ShardStateDb GC: collected {} states to delete, kept {}",
                    to_delete.len(), kept
                );

                // Sort ids by decreasing seqno. This way differences between 
                // states will be smaller, so each delete operation will be faster
                // (last in the vector - the earliest state - will be deleted first)
                to_delete.sort_by(|a, b| b.seq_no().cmp(&a.seq_no()));
            }
        });
    }

    pub async fn stop(&self) {
        self.stop.fetch_or(Self::MASK_STOPPED, Ordering::Relaxed);
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if !self.is_run() {
                tokio::time::sleep(Duration::from_secs(1)).await;
                break;
            }
        }
    }

    pub fn is_gc_run(&self) -> bool {
        self.stop.load(Ordering::Relaxed) & Self::MASK_GC_STARTED != 0
    }

    fn is_run(&self) -> bool {
        self.stop.load(Ordering::Relaxed) & (Self::MASK_GC_STARTED | Self::MASK_WORKER) != 0
    }

    pub fn shardstate_db(&self) -> Arc<dyn KvcWriteable<BlockIdExt>> {
        Arc::clone(&self.shardstate_db)
    }

    pub async fn put(
        &self, 
        id: &BlockIdExt, 
        state_root: Cell, 
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        let root_id = state_root.repr_hash();
        log::debug!(
            target: TARGET, 
            "ShardStateDb::put  id {}  root_cell_id {:x}",
            id, root_id
        );
        loop {
            let in_queue = self.in_queue.load(Ordering::Relaxed);
            if in_queue >= self.config.states_db_queue_len {
                log::warn!(
                    target: TARGET, 
                    "ShardStateDb::put  id {}  root_cell_id {:x}  waiting for queue (current queue length: {})",
                    id, root_id, in_queue
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else {
                break;
            }
        }

        let in_queue = self.in_queue.fetch_add(1, Ordering::Relaxed) + 1;

        self.storer.send((Job::PutState(state_root, id.clone()), callback))
            .map_err(|_| error!("Can't send state to put into db, id {}, root {}", id, root_id))?;

        metrics::gauge!("db_shardstate_queue", in_queue as f64);
        #[cfg(feature = "telemetry")]
        self.telemetry.shardstates_queue.update(std::cmp::max(0, in_queue) as u64);
        log::trace!("ShardStateDb put: in_queue {}", in_queue);

        Ok(())
    }

    pub fn get(&self, id: &BlockIdExt, use_cache: bool) -> Result<Cell> {
        let data = match self.shardstate_db.get(id) {
            Ok(data) => data,
            Err(e) => {
                if let Some(gc_resolver) = self.gc_resolver.get() {
                    if gc_resolver.allow_state_gc(id, 0, 0)? {
                        fail!(StorageError::StateIsAllowedToGc(id.clone()))
                    }
                }
                return Err(e);
            },
        };
        let db_entry = DbEntry::from_slice(&data)?;

        log::debug!(
            target: TARGET,
            "ShardStateDb::get  id {}  cell_id {:x}  use_cache {}",
            id, db_entry.cell_id, use_cache
        );

        if let Some(gc_resolver) = self.gc_resolver.get() {
            let utime_now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            if gc_resolver.allow_state_gc(&db_entry.block_id, db_entry.save_utime, utime_now)? {
                fail!(StorageError::StateIsAllowedToGc(db_entry.block_id))
            }
        }

        let root_cell = self.dynamic_boc_db.load_boc(&db_entry.cell_id, use_cache)?;
        Ok(root_cell)
    }

    pub fn create_done_cells_storage(
        &self, 
        root_cell_id: &UInt256
    ) -> Result<Box<dyn DoneCellsStorage>> {
        Ok(Box::new(DoneCellsStorageAdapter::new(
            self.db.clone(),
            self.dynamic_boc_db.clone(),
            format!("{:x}", root_cell_id),
        )?))
    }

    pub fn create_ordered_cells_storage(
        &self, 
        root_cell_id: &UInt256
    ) -> Result<impl OrderedCellsStorage> {
        Ok(OrderedCellsStorageAdapter::new(
            self.db.clone(),
            self.dynamic_boc_db.clone(),
            format!("{:x}", root_cell_id),
            self.pss_slowdown_mcs.clone(),
        )?)
    }

    pub fn create_hashed_cell_storage(
        &self,) -> Result<impl CellByHashStorage> {
        Ok(CellByHashStorageAdapter::new(
            self.dynamic_boc_db.clone(),
            false
        )?)
    }
    
    pub fn enumerate_ids(&self, callback: &mut dyn FnMut(&BlockIdExt) -> Result<bool>) -> Result<()> {
        self.shardstate_db.for_each(&mut |_key, value| {
            let entry = DbEntry::from_slice(value)?;
            callback(&entry.block_id)
        })?;
        Ok(())
    }

    async fn worker(
        self: Arc<Self>,
        mut receiver: tokio::sync::mpsc::UnboundedReceiver<(Job, Option<Arc<dyn Callback>>)>
    ) {
        self.stop.fetch_or(Self::MASK_WORKER, Ordering::Relaxed);

        let check_stop = || {
            if self.stop.load(Ordering::Relaxed) & Self::MASK_STOPPED != 0 {
                self.stop.fetch_and(!ShardStateDb::MASK_WORKER, Ordering::Relaxed);
                true
            } else {
                false
            }
        };

        let ss_db = Arc::clone(&self);

        let mut cells_counters = if !self.config.cache_cells_counters {
            None
        } else {
            let mut cells_counters = CellsCounters::default();
            if self.config.prefill_cells_counters {
                let now = std::time::Instant::now();
                if let Err(e) = tokio::task::block_in_place(|| {
                    let check_stop = || {
                        if ss_db.stop.load(Ordering::Relaxed) & Self::MASK_STOPPED != 0 {
                            fail!("Stopped")
                        } else {
                            Ok(())
                        }
                    };
                    ss_db.dynamic_boc_db.fill_counters(&check_stop, &mut cells_counters)
                }) {
                    log::error!("ShardStateDb worker: fill_counters error {}", e);
                }
                log::info!(
                    "ShardStateDb worker: fill_counters TIME {}ms  counters {}", 
                    now.elapsed().as_millis(), cells_counters.len()
                );
            }
            Some(cells_counters)
        };

        loop {
            if check_stop() {
                return;
            }
            match tokio::time::timeout(Duration::from_millis(500), receiver.recv()).await {
                Ok(Some((mut job, callback))) => {

                    let in_queue = self.in_queue.fetch_sub(1, Ordering::Relaxed) - 1;
                    metrics::gauge!("db_shardstate_queue", in_queue as f64);
                    #[cfg(feature = "telemetry")] {
                        self.telemetry.shardstates_queue.update(std::cmp::max(0, in_queue) as u64);
                        self.telemetry.cells_counters.update(
                            cells_counters.as_ref().map(|c| c.len()).unwrap_or_default() as u64);
                    }
                    let slowdown = (self.config.max_pss_slowdown_mcs * in_queue) / self.config.states_db_queue_len;
                    self.pss_slowdown_mcs.store(slowdown, Ordering::Relaxed);
                    log::debug!("ShardStateDb worker: in_queue {}, pss slowdown {}mcs", in_queue, slowdown);

                    let mut ok = false;
                    match &mut job {
                        Job::PutState(cell, id) => {
                            match self.clone().put_internal(id, cell.clone(), 
                                &mut cells_counters, self.config.prefill_cells_counters)
                            {
                                Err(e) => {
                                    if check_stop() {
                                        return;
                                    }
                                    log::error!(
                                        target: TARGET, "CRITICAL! ShardStateDb::put_internal  {}", e
                                    );
                                }
                                Ok(saved_root) => {
                                    *cell = saved_root;
                                    ok = true;
                                }
                            }
                        }
                        Job::DeleteState(id) => {
                            if let Err(e) = self.clone().delete_internal(id,
                                &mut cells_counters, self.config.prefill_cells_counters)
                            {
                                if check_stop() {
                                    return;
                                }
                                log::error!(
                                    target: TARGET, "CRITICAL! ShardStateDb::delete_internal  {}", e
                                );
                            } else {
                                ok = true
                            }
                        }
                    }
                    if let Some(callback) = callback {
                        let _ = callback.invoke(job, ok).await;
                    }
                }
                _ => ()
            }
        }
    }

    pub fn put_internal(
        self: Arc<Self>,
        id: &BlockIdExt,
        state_root: Cell,
        cells_counters: &mut Option<CellsCounters>,
        full_filled_cells: bool,
    ) -> Result<Cell> {
        let cell_id = UInt256::from(state_root.repr_hash());

        log::trace!(
            target: TARGET, 
            "ShardStateDb::put_internal  id {}  root_cell_id {:x}",
            id, cell_id
        );

        if self.shardstate_db.contains(id)? {
            log::warn!(
                target: TARGET, 
                "ShardStateDb::put_internal  ALREADY EXISTS  id {}  root_cell_id {:x}",
                id, cell_id
            );
            return self.dynamic_boc_db.load_boc(&cell_id, true);
        }

        let ss_db = self.clone();
        let saved_root = tokio::task::block_in_place(|| {
            let check_stop = || {
                if ss_db.stop.load(Ordering::Relaxed) & Self::MASK_STOPPED != 0 {
                    fail!("Stopped")
                } else {
                    Ok(())
                }
            };
            ss_db.dynamic_boc_db.save_boc(state_root, true, &check_stop, cells_counters, full_filled_cells)
        })?;

        let save_utime = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let db_entry = DbEntry::with_params(id.clone(), cell_id.clone(), save_utime);
        let mut buf = Vec::new();
        db_entry.serialize(&mut Cursor::new(&mut buf))?;

        self.shardstate_db.put(id, &buf)?;

        log::trace!(
            target: TARGET, 
            "ShardStateDb::put_internal DONE  id {}  root_cell_id {:x}",
            id, cell_id
        );

        Ok(saved_root)
    }

    pub fn delete_internal(
        self: Arc<Self>,
        id: &BlockIdExt,
        cells_counters: &mut Option<CellsCounters>,
        full_filled_cells: bool,
    ) -> Result<()> {
        log::trace!(target: TARGET, "ShardStateDb::delete_internal  id {}", id);

        let db_entry = DbEntry::from_slice(&self.shardstate_db.get(id)?)?;

        let ss_db = self.clone();
        tokio::task::block_in_place(|| {
            let check_stop = || {
                if ss_db.stop.load(Ordering::Relaxed) & Self::MASK_STOPPED != 0 {
                    fail!("Stopped")
                } else {
                    Ok(())
                }
            };
            ss_db.dynamic_boc_db.delete_boc(&db_entry.cell_id, &check_stop, cells_counters, full_filled_cells)
        })?;

        self.shardstate_db.delete(id)?;

        log::trace!(target: TARGET, "ShardStateDb::delete_internal  DONE  id {}", id);
        Ok(())
    }
}   
