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
    dynamic_boc_rc_db::{DynamicBocDb, DoneCellsStorageAdapter, OrderedCellsStorageAdapter},
    traits::Serializable,
    TARGET,
};
#[cfg(feature = "telemetry")]
use crate::StorageTelemetry;
#[cfg(all(test, feature = "telemetry"))]
use crate::tests::utils::create_storage_telemetry;
use crate::db::rocksdb::RocksDb;
use std::{
    io::{Cursor, Read, Write},
    sync::{Arc, atomic::{AtomicU8, AtomicU32, AtomicI32, Ordering}}, 
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use ton_block::BlockIdExt;
use ton_types::{
    ByteOrderRead, Cell, Result, fail, error, DoneCellsStorage, OrderedCellsStorage, UInt256
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

impl Job {
    pub fn block_id(&self) -> &BlockIdExt {
        match self {
            Job::PutState(_cell, id) => &id,
            Job::DeleteState(id) => &id,
        }
    }
}

pub struct ShardStateDb {
    db: Arc<RocksDb>,
    shardstate_db: Arc<dyn KvcWriteable<BlockIdExt>>,
    dynamic_boc_db: Arc<DynamicBocDb>,
    storer: tokio::sync::mpsc::UnboundedSender<(Job, Option<Arc<dyn Callback>>)>,
    in_queue: AtomicI32,
    stop: AtomicU8,
}

impl ShardStateDb {

    const MASK_GC_STARTED: u8 = 0x01;
    const MASK_STOPPED: u8 = 0x80;

    pub fn new(
        db: Arc<RocksDb>,
        shardstate_db_path: impl ToString,
        cell_db_path: impl ToString,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<StorageTelemetry>,
        allocated: Arc<StorageAlloc>
    ) -> Result<Arc<Self>> {

        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

        let ss_db = Arc::new(Self {
            db: db.clone(),
            shardstate_db: Arc::new(RocksDbTable::with_db(db.clone(), shardstate_db_path, true)?),
            dynamic_boc_db: Arc::new(DynamicBocDb::with_db(
                Arc::new(CellDb::with_db(db.clone(), cell_db_path, true)?),
                #[cfg(feature = "telemetry")]
                telemetry.clone(),
                allocated.clone()
            )),
            storer: sender,
            in_queue: AtomicI32::new(0),
            stop: AtomicU8::new(0)
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
        self.stop.fetch_or(Self::MASK_GC_STARTED, Ordering::Relaxed);
        tokio::spawn(async move {

            fn check_and_stop(stop: &AtomicU8) -> bool {
                if (stop.load(Ordering::Relaxed) & ShardStateDb::MASK_STOPPED) != 0 {
                    stop.fetch_and(!ShardStateDb::MASK_GC_STARTED, Ordering::Relaxed);
                    true
                } else {
                    false
                }
            }

            async fn sleep_nicely(stop: &AtomicU8, sleep_for_sec: u64) -> bool {
                let start = Instant::now();
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if check_and_stop(stop) {
                        return false
                    }
                    if start.elapsed().as_secs() > sleep_for_sec {
                        return true
                    } 
                }
            }

            loop {
                let run_gc_interval = run_interval_adjustable_sec.load(Ordering::Relaxed) as u64;

                if !sleep_nicely(&self.stop, run_gc_interval).await {
                    return;
                }

                let mut to_delete = vec!();
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

                for id in to_delete {
                    if check_and_stop(&self.stop) {
                        return;
                    }
                    tokio::task::yield_now().await;
                    if let Err(e) = self.storer.send((Job::DeleteState(id), None)) {
                        log::error!(
                            target: TARGET, 
                            "Can't send state to delete from db, id {}",
                            e.0.0.block_id()
                        );
                    } else {
                        let in_queue = self.in_queue.fetch_add(1, Ordering::Relaxed) + 1;
                        log::trace!("ShardStateDb GC: in_queue {}", in_queue);
                    }
                }
            }
        });
    }

    pub async fn stop(&self) {
        self.stop.fetch_or(Self::MASK_STOPPED, Ordering::Relaxed);
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if !self.is_gc_run() {
                break;
            }
        }
    }

    pub fn is_gc_run(&self) -> bool {
        self.stop.load(Ordering::Relaxed) & Self::MASK_GC_STARTED != 0
    }

    pub fn shardstate_db(&self) -> Arc<dyn KvcWriteable<BlockIdExt>> {
        Arc::clone(&self.shardstate_db)
    }

    pub fn put(
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
        self.storer.send((Job::PutState(state_root, id.clone()), callback))
            .map_err(|_| error!("Can't send state to put into db, id {}, root {}", id, root_id))?;

        let in_queue = self.in_queue.fetch_add(1, Ordering::Relaxed) + 1;
        log::trace!("ShardStateDb put: in_queue {}", in_queue);

        Ok(())
    }

    pub fn get(&self, id: &BlockIdExt, use_cache: bool) -> Result<Cell> {
        let db_entry = DbEntry::from_slice(&self.shardstate_db.get(id)?)?;
        log::debug!(
            target: TARGET,
            "ShardStateDb::get  id {}  cell_id {:x}  use_cache {}",
            id, db_entry.cell_id, use_cache
        );
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
        )?)
    }

    async fn worker(
        self: Arc<Self>,
        mut receiver: tokio::sync::mpsc::UnboundedReceiver<(Job, Option<Arc<dyn Callback>>)>
    ) {
        let check_stop = || self.stop.load(Ordering::Relaxed) & Self::MASK_STOPPED != 0;
        loop {
            if check_stop() {
                return;
            }
            match tokio::time::timeout(Duration::from_millis(500), receiver.recv()).await {
                Ok(Some((job, callback))) => {

                    let in_queue = self.in_queue.fetch_sub(1, Ordering::Relaxed) - 1;
                    log::debug!("ShardStateDb worker: in_queue {}", in_queue);

                    match &job {
                        Job::PutState(cell, id) => {
                            let ok = if let Err(e) = self.clone().put_internal(id, cell.clone()).await {
                                if check_stop() {
                                    return;
                                }
                                log::error!(
                                    target: TARGET, "CRITICAL! ShardStateDb::put_internal  {}", e
                                );
                                false
                            } else {
                                true
                            };
                            if let Some(callback) = callback {
                                let _ = callback.invoke(job, ok).await;
                            }
                        }
                        Job::DeleteState(id) => {
                            let ok = if let Err(e) = self.clone().delete_internal(id).await {
                                if check_stop() {
                                    return;
                                }
                                log::error!(
                                    target: TARGET, "CRITICAL! ShardStateDb::delete_internal  {}", e
                                );
                                false
                            } else {
                                true
                            };
                            if let Some(callback) = callback {
                                let _ = callback.invoke(job, ok).await;
                            }
                        }
                    }
                }
                _ => ()
            }
        }
    }

    pub async fn put_internal(self: Arc<Self>, id: &BlockIdExt, state_root: Cell) -> Result<()> {
        let cell_id = UInt256::from(state_root.repr_hash());

        log::debug!(
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
            return Ok(())
        }

        let ss_db = self.clone();
        tokio::task::spawn_blocking(move || {
            let check_stop = || {
                if ss_db.stop.load(Ordering::Relaxed) & Self::MASK_STOPPED != 0 {
                    fail!("Stopped")
                } else {
                    Ok(())
                }
            };
            ss_db.dynamic_boc_db.save_boc(state_root, true, &check_stop)
        }).await??;

        let save_utime = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let db_entry = DbEntry::with_params(id.clone(), cell_id.clone(), save_utime);
        let mut buf = Vec::new();
        db_entry.serialize(&mut Cursor::new(&mut buf))?;

        self.shardstate_db.put(id, &buf)?;

        log::debug!(
            target: TARGET, 
            "ShardStateDb::put_internal DONE  id {}  root_cell_id {:x}",
            id, cell_id
        );

        Ok(())
    }

    pub async fn delete_internal(self: Arc<Self>, id: &BlockIdExt) -> Result<()> {
        log::debug!(target: TARGET, "ShardStateDb::delete_internal  id {}", id);

        let db_entry = DbEntry::from_slice(&self.shardstate_db.get(id)?)?;

        let ss_db = self.clone();
        tokio::task::spawn_blocking(move || {
            let check_stop = || {
                if ss_db.stop.load(Ordering::Relaxed) & Self::MASK_STOPPED != 0 {
                    fail!("Stopped")
                } else {
                    Ok(())
                }
            };
            ss_db.dynamic_boc_db.delete_boc(&db_entry.cell_id, &check_stop)
        }).await??;

        self.shardstate_db.delete(id)?;

        log::debug!(target: TARGET, "ShardStateDb::delete_internal  DONE  id {}", id);
        Ok(())
    }
}   
