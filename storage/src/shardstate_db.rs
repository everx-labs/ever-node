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
    db::{rocksdb::RocksDbTable, traits::{DbKey, KvcWriteable, KvcTransaction}},
    dynamic_boc_db::DynamicBocDb, /*dynamic_boc_diff_writer::DynamicBocDiffWriter,*/
    traits::Serializable, types::{CellId, Reference, StorageCell},
    TARGET,
};
#[cfg(feature = "telemetry")]
use crate::StorageTelemetry;
#[cfg(all(test, feature = "telemetry"))]
use crate::tests::utils::create_storage_telemetry;
use fnv::FnvHashSet;
use crate::db::rocksdb::RocksDb;
use std::{
    io::{Cursor, Read, Write}, ops::Deref, 
    sync::{Arc, atomic::{AtomicU8, AtomicU32, Ordering}}, 
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use ton_block::BlockIdExt;
use ton_types::{ByteOrderRead, Cell, Result, fail};

pub(crate) struct DbEntry {
    pub cell_id: CellId, // TODO: remove this id
    pub block_id: BlockIdExt,
    pub db_index: u32,
    pub save_utime: u64,
}

impl DbEntry {
    pub fn with_params(cell_id: CellId, block_id: BlockIdExt, db_index: u32, save_utime: u64) -> Self {
        Self { cell_id, block_id, db_index, save_utime }
    }
}

impl Serializable for DbEntry {
    fn serialize<T: Write>(&self, writer: &mut T) -> Result<()> {
        writer.write_all(self.cell_id.key())?;
        self.block_id.serialize(writer)?;
        writer.write_all(&self.db_index.to_le_bytes())?;
        writer.write_all(&self.save_utime.to_le_bytes())?;
        Ok(())
    }

    fn deserialize<T: Read>(reader: &mut T) -> Result<Self> {
        let mut buf = [0; 32];
        reader.read_exact(&mut buf)?;
        let cell_id = CellId::new(buf.into());
        let block_id = BlockIdExt::deserialize(reader)?;
        let db_index = reader.read_le_u32().unwrap_or(0); // use 0 db by default
        let save_utime = reader.read_le_u64().unwrap_or(0);

        Ok(Self { cell_id, block_id, db_index, save_utime })
    }
}

pub struct ShardStateDb {
    shardstate_db: Arc<dyn KvcWriteable<BlockIdExt>>,
    current_dynamic_boc_db_index: AtomicU32,
    dynamic_boc_db_0: Arc<DynamicBocDb>,
    dynamic_boc_db_0_writers: AtomicU32,
    dynamic_boc_db_1: Arc<DynamicBocDb>,
    dynamic_boc_db_1_writers: AtomicU32,
    stop: AtomicU8
}

const SHARDSTATES_GC_LAG_SEC: u64 = 300;

impl ShardStateDb {

    const MASK_GC_STARTED: u8 = 0x01;
    const MASK_GC_STOPPED: u8 = 0x80;

    /// Constructs new instance using in-memory key-value collections

    /// Constructs new instance using RocksDB with given paths
    pub fn with_db(
        db: Arc<RocksDb>,
        shardstate_db_path: impl ToString,
        cell_db_path: impl ToString,
        cell_db_path_additional: impl ToString,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<StorageTelemetry>,
        allocated: Arc<StorageAlloc>
    ) -> Result<Arc<Self>> {
        let ret = Self::with_dbs(
            Arc::new(RocksDbTable::with_db(db.clone(), shardstate_db_path)?),
            Arc::new(CellDb::with_db(db.clone(), cell_db_path)?),
            Arc::new(CellDb::with_db(db.clone(), cell_db_path_additional)?),
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated
        );
        Ok(ret)
    }

    /// Constructs new instance using given key-value collection implementations
    fn with_dbs(
        shardstate_db: Arc<dyn KvcWriteable<BlockIdExt>>,
        cell_db_0: Arc<CellDb>,
        cell_db_1: Arc<CellDb>,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<StorageTelemetry>,
        allocated: Arc<StorageAlloc>
    ) -> Arc<Self> {
        Arc::new(Self {
            shardstate_db,
            current_dynamic_boc_db_index: AtomicU32::new(0),
            dynamic_boc_db_0: Arc::new(DynamicBocDb::with_db(
                cell_db_0.clone(), 
                0,
                Some(cell_db_1.clone()),
                #[cfg(feature = "telemetry")]
                telemetry.clone(),
                allocated.clone()
            )),
            dynamic_boc_db_0_writers: AtomicU32::new(0),
            dynamic_boc_db_1: Arc::new(DynamicBocDb::with_db(
                cell_db_1,
                1,
                Some(cell_db_0),
                #[cfg(feature = "telemetry")]
                telemetry,
                allocated
            )),
            dynamic_boc_db_1_writers: AtomicU32::new(0),
            stop: AtomicU8::new(0)
        })
    }

    pub fn start_gc(
        self: Arc<Self>,
        gc_resolver: Arc<dyn AllowStateGcResolver>,
        run_interval_adjustable_sec: Arc<AtomicU32>,
    ) {
        // When test-blockchains deploys all validator-nodes start approx at one time, 
        // so all GC start at one time all nodes. It can reduce whole blockchain's performans,
        // so let's mix start time next way
        let run_gc_interval = run_interval_adjustable_sec.load(Ordering::Relaxed) as u64;
        let mut rng = rand::thread_rng();
        let rand_initial_sleep = rand::Rng::gen_range(&mut rng, 0..run_gc_interval);

        self.stop.fetch_or(Self::MASK_GC_STARTED, Ordering::Relaxed);
        tokio::spawn(async move {

            fn check_and_stop(stop: &AtomicU8) -> bool {
                if (stop.load(Ordering::Relaxed) & ShardStateDb::MASK_GC_STOPPED) != 0 {
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
        
            if !sleep_nicely(&self.stop, rand_initial_sleep).await {
                return
            }
 
            let mut last_gc_duration = Duration::from_secs(0);
            loop {
                let run_gc_interval = 
                    Duration::from_secs(run_interval_adjustable_sec.load(Ordering::Relaxed) as u64);
                if run_gc_interval > last_gc_duration {
                    if !sleep_nicely(
                        &self.stop, 
                        (run_gc_interval - last_gc_duration).as_secs()
                    ).await {
                        return
                    }
                }

                // Take unused db's index
                let current_db_index = self.current_dynamic_boc_db_index.load(Ordering::Relaxed);
                let (collected_db, _another_db, collected_db_writers) =
                    match current_db_index {
                    1 => {
                        (&self.dynamic_boc_db_0, &self.dynamic_boc_db_1, &self.dynamic_boc_db_0_writers)
                    },
                    0 => {
                        (&self.dynamic_boc_db_1, &self.dynamic_boc_db_0, &self.dynamic_boc_db_1_writers)
                    },
                    _ => {
                        log::error!(target: TARGET, "Invalid `current_dynamic_boc_db_index` while GC");
                        last_gc_duration = Duration::from_secs(0);
                        continue;
                    }
                };

                // wait while all wtirers finish operation with unused db
                let writers_waiting_start = Instant::now();
                while collected_db_writers.load(Ordering::Relaxed) > 0 {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let time = writers_waiting_start.elapsed().as_millis();
                    if time > 1000 {
                        log::warn!(target: TARGET, "Too long awaiting of dynamic boc db writing {}ms", time);
                    }
                }

                // start GC
                log::info!(target: TARGET, "Statring GC for db {}", collected_db.db_index());
                let collecting_start = Instant::now();
                let self_ = self.clone();
                let collected_db_ = collected_db.clone();
                let gc_resolver_ = gc_resolver.clone();
                let result = tokio::task::spawn_blocking(move || 
                    gc(
                        self_.shardstate_db(),
                        collected_db_,
                        gc_resolver_,
                        &|| {
                            if (self_.stop.load(Ordering::Relaxed) & ShardStateDb::MASK_GC_STOPPED) != 0 {
                                fail!("GC was stopped")
                            }
                            Ok(())
                        }
                    )
                ).await;
                last_gc_duration = collecting_start.elapsed();
                match result {
                    Err(e) => {
                        log::error!(target: TARGET, "Panic while GC for db {}: {}, TIME: {}ms",
                            collected_db.db_index(), e, last_gc_duration.as_millis());
                    },
                    Ok(Err(e)) => {
                        if check_and_stop(&self.stop) {
                            log::info!(target: TARGET, "GC for db {}: was stopped",
                                collected_db.db_index());
                            return;
                        }
                        log::error!(target: TARGET, "Error while GC for db {}: {}, TIME: {}ms",
                            collected_db.db_index(), e, last_gc_duration.as_millis());
                    },
                    Ok(Ok(gc_stat)) => {
                        log::info!(
                            target: TARGET, 
                            "Finished GC for db {}\n\
                            collected cells     {:>8}\n\
                            marked cells        {:>8}\n\
                            total cells         {:>8}\n\
                            roots to sweep      {:>8}\n\
                            marked roots        {:>8}\n\
                            mark time           {:>8} ms\n\
                            sweep time          {:>8} ms\n\
                            total time          {:>8} ms",
                            collected_db.db_index(),
                            gc_stat.collected_cells,
                            gc_stat.marked_cells,
                            gc_stat.collected_cells + gc_stat.marked_cells,
                            gc_stat.roots_to_sweep,
                            gc_stat.marked_roots,
                            gc_stat.mark_time.as_millis(),
                            gc_stat.sweep_time.as_millis(),
                            (gc_stat.mark_time + gc_stat.sweep_time).as_millis()
                        );
                    }
                }

                // Change current db's index
                self.current_dynamic_boc_db_index.store(collected_db.db_index(), Ordering::Relaxed);
            };
        });
    }

    pub async fn stop_gc(&self) {
        self.stop.fetch_or(Self::MASK_GC_STOPPED, Ordering::Relaxed);
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

    /// Returns reference to shardstates database
    pub fn shardstate_db(&self) -> Arc<dyn KvcWriteable<BlockIdExt>> {
        Arc::clone(&self.shardstate_db)
    }

    /// Stores cells from given tree which don't exist in the storage.
    /// Returns root cell which is implemented as StorageCell.
    /// So after store() origin shard state's cells might be dropped.
    pub fn put(
        &self,
        id: &BlockIdExt,
        state_root: Cell,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
    ) -> Result<Cell> {
        let saved_root = match self.current_dynamic_boc_db_index.load(Ordering::Relaxed) {
            0 => {
                self.dynamic_boc_db_0_writers.fetch_add(1, Ordering::Relaxed);
                let r = self.put_internal(id, state_root, &self.dynamic_boc_db_0, check_stop);
                self.dynamic_boc_db_0_writers.fetch_sub(1, Ordering::Relaxed);
                r?
            },
            1 => {
                self.dynamic_boc_db_1_writers.fetch_add(1, Ordering::Relaxed);
                let r = self.put_internal(id, state_root, &self.dynamic_boc_db_1, check_stop);
                self.dynamic_boc_db_1_writers.fetch_sub(1, Ordering::Relaxed);
                r?
            },
            _ => fail!("Invalid `current_dynamic_boc_db_index`")
        };

        Ok(saved_root)
    }

    pub fn put_internal(
        &self,
        id: &BlockIdExt,
        state_root: Cell,
        boc_db: &Arc<DynamicBocDb>,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
    ) -> Result<Cell> {
        let cell_id = CellId::from(state_root.repr_hash());
        
        log::trace!(target: TARGET, "ShardStateDb::put_internal  start  id {}  root_cell_id {}  db_index {}",
            id, cell_id, boc_db.db_index());

        let c = tokio::task::block_in_place(
            || boc_db.save_as_dynamic_boc(state_root.deref(), check_stop)
        )?;

        let save_utime = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let db_entry = DbEntry::with_params(cell_id.clone(), id.clone(), boc_db.db_index(), save_utime);
        let mut buf = Vec::new();
        db_entry.serialize(&mut Cursor::new(&mut buf))?;

        self.shardstate_db.put(id, buf.as_slice())?;
        // (self.shardstate_db as dyn KvcWriteable::<BlockIdExt>).put(id, buf.as_slice())?;

        log::trace!(target: TARGET, "ShardStateDb::put_internal  finish  id {}  root_cell_id {}  db_index {}  written: {} cells",
            id, cell_id, boc_db.db_index(), c);

        let root_cell = boc_db.load_dynamic_boc(&db_entry.cell_id)?;

        Ok(root_cell)
    }

    /// Loads previously stored root cell
    pub fn get(&self, id: &BlockIdExt) -> Result<Cell> {
        let db_entry = DbEntry::from_slice(&self.shardstate_db.get(id)?)?;

        log::trace!(target: TARGET, "ShardStateDb::get  id {}  cell_id {}  db_index {}", 
            id, db_entry.cell_id, db_entry.db_index);

        let boc_db = match db_entry.db_index {
            1 => &self.dynamic_boc_db_1,
            0 => &self.dynamic_boc_db_0,
            index => fail!("Invalid db's index {}", index)
        };
        let root_cell = boc_db.load_dynamic_boc(&db_entry.cell_id)?;

        Ok(root_cell)
    }

}

pub trait AllowStateGcResolver: Send + Sync {
    fn allow_state_gc(&self, block_id: &BlockIdExt, save_utime: u64, gc_utime: u64) -> Result<bool>;
}

#[derive(Default)]
pub struct GcStatistic {
    pub collected_cells: usize,
    pub marked_cells: usize,
    pub roots_to_sweep: usize,
    pub marked_roots: usize,
    pub mark_time: Duration,
    pub sweep_time: Duration,
}

pub fn gc(
    shardstate_db: Arc<dyn KvcWriteable<BlockIdExt>>,
    cleaned_boc_db: Arc<DynamicBocDb>,
    gc_resolver: Arc<dyn AllowStateGcResolver>,
    check_stopped: &dyn Fn() -> Result<()>,
) -> Result<GcStatistic> {

    let mark_started = Instant::now();
    let shardstate_db_ = shardstate_db.clone();
    let cleaned_boc_db_ = cleaned_boc_db.clone();
    let gc_resolver_ = gc_resolver.clone();
    let (marked, to_sweep, marked_roots) = mark(
        shardstate_db_.deref(),
        cleaned_boc_db_.deref(),
        gc_resolver_.deref(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        check_stopped,
    )?;

    let marked_cells = marked.len();
    let mark_time = mark_started.elapsed();
    let roots_to_sweep = to_sweep.len();

    if to_sweep.is_empty() {
        Ok(GcStatistic {
            collected_cells: 0,
            marked_cells,
            roots_to_sweep,
            marked_roots,
            mark_time,
            sweep_time: Duration::default(),
        })
    } else {

        let sweep_started = Instant::now();
        let collected_cells = sweep(
            shardstate_db,
            cleaned_boc_db,
            to_sweep,
            marked,
            check_stopped,
        )?;

        Ok(GcStatistic {
            collected_cells,
            marked_cells,
            roots_to_sweep,
            marked_roots,
            mark_time,
            sweep_time: sweep_started.elapsed(),
        })
    }
}
type MarkResult = Result<(FnvHashSet<CellId>, Vec<(BlockIdExt, CellId)>, usize)>;

fn mark(
    shardstate_db: &dyn KvcWriteable<BlockIdExt>,
    dynamic_boc_db: &DynamicBocDb,
    gc_resolver: &dyn AllowStateGcResolver,
    gc_time: u64,
    check_stopped: &dyn Fn() -> Result<()>,
) -> MarkResult {
    // We should leave last states in DB to avoid full state saving when DB will active.
    // 1) enumerate all roots with their save-time
    // 2) remember max time and max masterchain root
    // 3) do not sweep last masterchain root and shardchains roots 
    //    for last SHARDSTATES_GC_LAG_SEC seconds

    let mut last_mc_seqno = 0;
    let mut roots = Vec::new();
    let mut max_shard_utime = 0;

    shardstate_db.for_each(&mut |_key, value| {
        check_stopped()?;
        let db_entry = DbEntry::from_slice(value)?;
        if db_entry.db_index == dynamic_boc_db.db_index() {
            if db_entry.block_id.shard().is_masterchain() {
                if db_entry.block_id.seq_no() > last_mc_seqno {
                    last_mc_seqno = db_entry.block_id.seq_no();
                }
            } else {
                if db_entry.save_utime > max_shard_utime{
                    max_shard_utime = db_entry.save_utime;
                }
            }
            roots.push(db_entry);
        }
        Ok(true)
    })?;

    let mut to_mark = Vec::new();
    let mut to_sweep = Vec::new();
    for root in roots {
        check_stopped()?;
        let is_mc = root.block_id.shard().is_masterchain();
        if gc_resolver.allow_state_gc(&root.block_id, root.save_utime, gc_time)?
            && (!is_mc || root.block_id.seq_no() < last_mc_seqno)
            && (is_mc || root.save_utime + SHARDSTATES_GC_LAG_SEC < max_shard_utime)
        {
            log::trace!(target: TARGET, "mark  to_sweep  block_id {}  cell_id {}  db_index {}  utime {}", 
                root.block_id, root.cell_id, dynamic_boc_db.db_index(), root.save_utime);
            to_sweep.push((root.block_id, root.cell_id));
        } else {
            log::trace!(target: TARGET, "mark  to_mark  block_id {}  cell_id {}  db_index {}  utime {}", 
                root.block_id, root.cell_id, dynamic_boc_db.db_index(), root.save_utime);
            to_mark.push(root.cell_id);
        }
    }

    let marked_roots = to_mark.len();
    let mut marked = FnvHashSet::default();
    if !to_sweep.is_empty() {
        for cell_id in to_mark {
            mark_subtree_recursive(dynamic_boc_db, cell_id.clone(), cell_id, &mut marked, check_stopped)?;
        }
    }

    Ok((marked, to_sweep, marked_roots))
}

fn mark_subtree_recursive(
    dynamic_boc_db: &DynamicBocDb,
    cell_id: CellId,
    root_id: CellId,
    marked: &mut FnvHashSet<CellId>,
    check_stopped: &dyn Fn() -> Result<()>,
) -> Result<()> {
    check_stopped()?;
    if !marked.contains(&cell_id) {
        match load_cell_references(dynamic_boc_db, &cell_id) {
            Ok(references) => {
                marked.insert(cell_id.clone());

                log::trace!(
                    target: TARGET,
                    "mark_subtree_recursive  marked  cell {}  root: {}  db_index {}",
                    cell_id, root_id, dynamic_boc_db.db_index(),
                );

                for reference in references {
                    mark_subtree_recursive(dynamic_boc_db, reference.hash().into(), root_id.clone(),
                        marked, check_stopped)?;
                }
            },
            Err(err) => {
                log::error!(
                    target: TARGET,
                    "mark_subtree_recursive  can't load cell  cell {}  root: {}  db_index {}  error: {}",
                    cell_id, root_id, dynamic_boc_db.db_index(), err,
                );
            }
        }
    }
    Ok(())
}

fn sweep(
    shardstate_db: Arc<dyn KvcWriteable<BlockIdExt>>,
    dynamic_boc_db: Arc<DynamicBocDb>,
    to_sweep: Vec<(BlockIdExt, CellId)>,
    marked: FnvHashSet<CellId>,
    check_stopped: &dyn Fn() -> Result<()>,
) -> Result<usize> {
    if to_sweep.is_empty() {
        return Ok(0);
    }

    let mut deleted_count = 0;
    let mut sweeped = FnvHashSet::default();

    let mut transaction = dynamic_boc_db.begin_transaction()?;

    for (block_id, cell_id) in to_sweep {

        deleted_count += sweep_cells_recursive(
            dynamic_boc_db.as_ref(),
            transaction.as_mut(), 
            &cell_id,
            &cell_id,
            &marked,
            &mut sweeped,
            check_stopped,
        )?;
        log::trace!(target: TARGET, "GC::sweep  block_id {}", block_id);

        shardstate_db.delete(&block_id)?;
    }

    transaction.commit()?;

    Ok(deleted_count)
}

fn sweep_cells_recursive(
    dynamic_boc_db: &DynamicBocDb,
    transaction: &mut dyn KvcTransaction<CellId>,
    cell_id: &CellId,
    root_id: &CellId,
    marked: &FnvHashSet<CellId>,
    sweeped: &mut FnvHashSet<CellId>,
    check_stopped: &dyn Fn() -> Result<()>,
) -> Result<usize> {
    check_stopped()?;
    if marked.contains(cell_id) || sweeped.contains(cell_id) {
        Ok(0)
    } else {
        let mut deleted_count = 0;
        sweeped.insert(cell_id.clone());

        match load_cell_references(dynamic_boc_db, cell_id) {
            Ok(references) => {
                for reference in references {
                    deleted_count += sweep_cells_recursive(
                        dynamic_boc_db,
                        transaction,
                        &reference.hash().into(), 
                        root_id,
                        marked,
                        sweeped,
                        check_stopped
                    )?;
                }

                log::trace!(
                    target: TARGET,
                    "sweep_cells_recursive  delete cell  id: {}  root: {}  db_index: {}",
                    cell_id, root_id, dynamic_boc_db.db_index(),
                );

                transaction.delete(cell_id);

                deleted_count += 1;
            }
            Err(err) => {
                log::error!(
                    target: TARGET,
                    "sweep_cells_recursive  can't load cell  id: {}  root: {}  db_index: {}  error: {}",
                    cell_id, root_id, dynamic_boc_db.db_index(), err,
                );
            }
        }
        Ok(deleted_count)
    }
}

fn load_cell_references(dynamic_boc_db: &DynamicBocDb, cell_id: &CellId) -> Result<Vec<Reference>> {
    let slice = dynamic_boc_db.cell_db().get(cell_id)?;
    StorageCell::deserialize_references(slice.as_ref())
}
