use crate::{
    cell_db::CellDb, TARGET,
    db::{rocksdb::RocksDb, traits::{DbKey, KvcSnapshotable, KvcTransaction}},
    dynamic_boc_db::DynamicBocDb, /*dynamic_boc_diff_writer::DynamicBocDiffWriter,*/
    traits::Serializable, types::{CellId, Reference, StorageCell}
};
use fnv::FnvHashSet;
use std::{
    io::{Cursor, Read, Write},
    ops::Deref, 
    path::Path, 
    sync::{Arc, atomic::{AtomicU32, Ordering}}, 
    time::{Duration, Instant}
};
use ton_block::{BlockIdExt, UnixTime32};
use ton_types::{ByteOrderRead, Cell, Result, fail};

pub(crate) struct DbEntry {
    pub cell_id: CellId,
    pub block_id_ext: BlockIdExt,
    db_index: u32,
}

impl DbEntry {
    pub fn with_params(cell_id: CellId, block_id_ext: BlockIdExt, db_index: u32) -> Self {
        Self { cell_id, block_id_ext, db_index }
    }
}

impl Serializable for DbEntry {
    fn serialize<T: Write>(&self, writer: &mut T) -> Result<()> {
        writer.write_all(&self.db_index.to_le_bytes())?;
        writer.write_all(self.cell_id.key())?;
        self.block_id_ext.serialize(writer)
    }

    fn deserialize<T: Read>(reader: &mut T) -> Result<Self> {
        let db_index = reader.read_le_u32()?;
        let mut buf = [0; 32];
        reader.read_exact(&mut buf)?;
        let cell_id = CellId::new(buf.into());
        let block_id_ext = BlockIdExt::deserialize(reader)?;

        Ok(Self { cell_id, block_id_ext, db_index })
    }
}

pub struct ShardStateDb {
    shardstate_db: Arc<dyn KvcSnapshotable<BlockIdExt>>,
    current_dynamic_boc_db_index: AtomicU32,
    dynamic_boc_db_0: Arc<DynamicBocDb>,
    dynamic_boc_db_0_writers: AtomicU32,
    dynamic_boc_db_1: Arc<DynamicBocDb>,
    dynamic_boc_db_1_writers: AtomicU32,
}

impl ShardStateDb {

    /// Constructs new instance using in-memory key-value collections

    /// Constructs new instance using RocksDB with given paths
    pub fn with_paths<P1: AsRef<Path>, P2: Clone + AsRef<Path>>(
        shardstate_db_path: P1,
        cell_db_path: P2,
        cell_db_path_additional: P2,
    ) -> Result<Arc<Self>> {
        Self::with_dbs(
            Arc::new(RocksDb::with_path(shardstate_db_path)),
            CellDb::with_path(cell_db_path),
            CellDb::with_path(cell_db_path_additional),
        )
    }

    /// Constructs new instance using given key-value collection implementations
    fn with_dbs(
        shardstate_db: Arc<dyn KvcSnapshotable<BlockIdExt>>,
        cell_db_0: CellDb,
        cell_db_1: CellDb,
    )-> Result<Arc<Self>> {
        let instance = Arc::new(Self {
            shardstate_db,
            current_dynamic_boc_db_index: AtomicU32::new(0),
            dynamic_boc_db_0: Arc::new(DynamicBocDb::with_db(cell_db_0)),
            dynamic_boc_db_0_writers: AtomicU32::new(0),
            dynamic_boc_db_1: Arc::new(DynamicBocDb::with_db(cell_db_1)),
            dynamic_boc_db_1_writers: AtomicU32::new(0),
        });
        Ok(instance)
    }

    pub fn start_gc(
        self: Arc<Self>,
        gc_resolver: Arc<dyn AllowStateGcResolver>,
        run_gc_interval: Duration,
    ) {
        tokio::spawn(async move {
            let mut last_gc_duration = Duration::from_secs(0);
            loop {
                if run_gc_interval > last_gc_duration {
                    tokio::time::sleep(run_gc_interval - last_gc_duration).await;
                }

                // Take unused db's index
                let current_db_index = self.current_dynamic_boc_db_index.load(Ordering::Relaxed);
                let (collected_db, another_db, collected_db_writers, collected_db_index) =
                    match current_db_index {
                    1 => {
                        (&self.dynamic_boc_db_0, &self.dynamic_boc_db_1, &self.dynamic_boc_db_0_writers, 0)
                    },
                    0 => {
                        (&self.dynamic_boc_db_1, &self.dynamic_boc_db_0, &self.dynamic_boc_db_1_writers, 1)
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
                log::info!(target: TARGET, "Statring GC for db {}", collected_db_index);
                let collecting_start = Instant::now();
                let result = gc(self.shardstate_db(),
                    collected_db.clone(),
                    another_db.clone(),
                    gc_resolver.deref(),
                    collected_db_index
                ).await;
                last_gc_duration = collecting_start.elapsed();
                match result {
                    Err(e) => {
                        log::error!(target: TARGET, "Error while GC for db {}: {}, TIME: {}ms",
                        collected_db_index, e, last_gc_duration.as_millis());
                    },
                    Ok(gc_stat) => {
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
                            collected_db_index,
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
                self.current_dynamic_boc_db_index.store(collected_db_index, Ordering::Relaxed);
            };
        });
    }

    /// Returns reference to shardstates database
    pub fn shardstate_db(&self) -> Arc<dyn KvcSnapshotable<BlockIdExt>> {
        Arc::clone(&self.shardstate_db)
    }

    /// Stores cells from given tree which don't exist in the storage.
    /// Returns root cell which is implemented as StorageCell.
    /// So after store() origin shard state's cells might be dropped.
    pub fn put(&self, id: &BlockIdExt, state_root: Cell) -> Result<()> {
        match self.current_dynamic_boc_db_index.load(Ordering::Relaxed) {
            0 => {
                self.dynamic_boc_db_0_writers.fetch_add(1, Ordering::Relaxed);
                let r = self.put_internal(id, state_root, 0, &self.dynamic_boc_db_0);
                self.dynamic_boc_db_0_writers.fetch_sub(1, Ordering::Relaxed);
                r?;
            },
            1 => {
                self.dynamic_boc_db_1_writers.fetch_add(1, Ordering::Relaxed);
                let r = self.put_internal(id, state_root, 1, &self.dynamic_boc_db_1);
                self.dynamic_boc_db_1_writers.fetch_sub(1, Ordering::Relaxed);
                r?;
            },
            _ => fail!("Invalid `current_dynamic_boc_db_index`")
        }

        Ok(())
    }

    pub fn put_internal(
        &self,
        id: &BlockIdExt,
        state_root: Cell,
        db_index: u32,
        boc_db: &Arc<DynamicBocDb>
    ) -> Result<()> {
        let cell_id = CellId::from(state_root.repr_hash());
        
        log::trace!(target: TARGET, "ShardStateDb::put_internal  id {}  cell_id {}  db_index {}",
            id, cell_id, db_index);

        let c = boc_db.save_as_dynamic_boc(state_root.deref())?;

        let db_entry = DbEntry::with_params(cell_id, id.clone(), db_index);
        let mut buf = Vec::new();
        db_entry.serialize(&mut Cursor::new(&mut buf))?;

        self.shardstate_db.put(id, buf.as_slice())?;

        log::trace!(target: TARGET, "ShardStateDb::put_internal  id {}, written: {} cells", id, c);

        Ok(())
    }

    /// Loads previously stored root cell
    pub fn get(&self, id: &BlockIdExt) -> Result<Cell> {
        let db_entry = DbEntry::from_slice(self.shardstate_db.get(id)?.as_ref())?;

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
    fn allow_state_gc(&self, block_id_ext: &BlockIdExt, gc_utime: UnixTime32) -> Result<bool>;
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

pub async fn gc(
    shardstate_db: Arc<dyn KvcSnapshotable<BlockIdExt>>,
    cleaned_boc_db: Arc<DynamicBocDb>,
    another_boc_db: Arc<DynamicBocDb>,
    gc_resolver: &dyn AllowStateGcResolver,
    cleaned_db_index: u32,
) -> Result<GcStatistic> {

    let mark_started = Instant::now();
    let (mut marked, to_sweep, marked_roots) = mark_cleaned_db(
        shardstate_db.deref(),
        cleaned_boc_db.deref(),
        gc_resolver,
        cleaned_db_index,
        UnixTime32::now()
    )?;

    let mark_time = mark_started.elapsed();
    let marked_cells = marked.len();
    let roots_to_sweep = to_sweep.len();

    if to_sweep.is_empty() {
        Ok(GcStatistic {
            collected_cells: 0,
            marked_cells,
            roots_to_sweep: 0,
            marked_roots: marked_roots,
            mark_time,
            sweep_time: Duration::default(),
        })
    } else {

        let marked_roots2 = mark_another_db(
            shardstate_db.deref(),
            another_boc_db.deref(),
            gc_resolver,
            cleaned_db_index,
            UnixTime32::now(),
            &mut marked
        )?;

        
        let sweep_started = Instant::now();
        let collected_cells = sweep(
            shardstate_db,
            cleaned_boc_db,
            cleaned_db_index,
            to_sweep,
            marked
        ).await?;
        let sweep_time = sweep_started.elapsed();

        Ok(GcStatistic {
            collected_cells,
            marked_cells,
            roots_to_sweep,
            marked_roots: marked_roots + marked_roots2,
            mark_time,
            sweep_time,
        })
    }
}

fn mark_cleaned_db(
    shardstate_db: &dyn KvcSnapshotable<BlockIdExt>,
    dynamic_boc_db: &DynamicBocDb,
    gc_resolver: &dyn AllowStateGcResolver,
    db_index: u32,
    gc_utime: UnixTime32
) -> Result<(FnvHashSet<CellId>, Vec<(BlockIdExt, CellId)>, usize)> {
    let mut to_mark = Vec::new();
    let mut to_sweep = Vec::new();
    shardstate_db.for_each(&mut |_key, value| {
        let db_entry = DbEntry::from_slice(value)?;
        let cell_id = db_entry.cell_id;
        let block_id_ext = db_entry.block_id_ext;
        if db_entry.db_index == db_index {
            if gc_resolver.allow_state_gc(&block_id_ext, gc_utime)? {
                log::trace!(target: TARGET, "GC::mark  block_id {}", block_id_ext);
                to_sweep.push((block_id_ext, cell_id));
            } else {
                to_mark.push(cell_id);
            }
        }
        Ok(true)
    })?;

    let marked_roots = to_mark.len();
    let mut marked = FnvHashSet::default();
    if to_sweep.len() > 0 {
        for cell_id in to_mark {
            mark_subtree_recursive(dynamic_boc_db, cell_id, &mut marked)?;
        }
    }

    Ok((marked, to_sweep, marked_roots))
}

fn mark_another_db(
    shardstate_db: &dyn KvcSnapshotable<BlockIdExt>,
    dynamic_boc_db: &DynamicBocDb,
    gc_resolver: &dyn AllowStateGcResolver,
    db_index: u32,
    gc_utime: UnixTime32,
    marked: &mut FnvHashSet<CellId>,
) -> Result<usize> {
    let mut to_mark = Vec::new();
    shardstate_db.for_each(&mut |_key, value| {
        let db_entry = DbEntry::from_slice(value)?;
        let cell_id = db_entry.cell_id;
        let block_id_ext = db_entry.block_id_ext;
        if db_entry.db_index != db_index {
            if !gc_resolver.allow_state_gc(&block_id_ext, gc_utime)? {
                to_mark.push(cell_id);
            }
        }
        Ok(true)
    })?;

    let marked_roots = to_mark.len();
    for cell_id in to_mark {
        mark_subtree_recursive(dynamic_boc_db, cell_id, marked)?;
    }

    Ok(marked_roots)
}

fn mark_subtree_recursive(
    dynamic_boc_db: &DynamicBocDb,
    cell_id: CellId,
    marked: &mut FnvHashSet<CellId>
) -> Result<()> {
    if !marked.contains(&cell_id) {
        let references = load_cell_references(dynamic_boc_db, &cell_id)?;
        marked.insert(cell_id);
        for reference in references {
            mark_subtree_recursive(dynamic_boc_db, reference.hash().into(), marked)?;
        }
    }
    Ok(())
}

async fn sweep(
    shardstate_db: Arc<dyn KvcSnapshotable<BlockIdExt>>,
    dynamic_boc_db: Arc<DynamicBocDb>,
    db_index: u32,
    to_sweep: Vec<(BlockIdExt, CellId)>,
    marked: FnvHashSet<CellId>
) -> Result<usize> {
    if to_sweep.len() < 1 {
        return Ok(0);
    }

    let marked = Arc::new(marked);

    let mut deleted_count = 0;
    let sweeping = Arc::new(lockfree::set::Set::new());


    let mut tasks = vec!();
    for (block_id, cell_id) in to_sweep {

        let dynamic_boc_db = dynamic_boc_db.clone();
        let shardstate_db = shardstate_db.clone();
        let marked = marked.clone();
        let sweeping = sweeping.clone();

        let t = tokio::task::spawn_blocking(move || -> Result<usize> {
        
            let mut transaction = dynamic_boc_db.begin_transaction()?;

            let deleted_count1 = sweep_cells_recursive(
                dynamic_boc_db.as_ref(),
                transaction.as_mut(), 
                cell_id, 
                marked.as_ref(),
                &sweeping,
            )?;
            log::trace!(target: TARGET, "GC::sweep  block_id {}", block_id);

            deleted_count += deleted_count1;

            transaction.commit()?;

            shardstate_db.delete(&block_id)?;
            Ok(deleted_count)
        });
        tasks.push(t);
    }

    let mut err = false;
    for r in futures::future::join_all(tasks).await {
        match r {
            Ok(Ok(d)) => deleted_count += d,
            Ok(Err(e)) => {
                err = true;
                log::error!("Error while GC for base {}: {}", db_index, e);
            }
            Err(e) => {
                err = true;
                log::error!("Panic while GC for base {}: {}", db_index, e);
            }
        }
    }
    if err {
        fail!("GC for db {} was failed (see log)", db_index);
    } else {
        Ok(deleted_count)
    }
}

fn sweep_cells_recursive(
    dynamic_boc_db: &DynamicBocDb,
    transaction: &mut dyn KvcTransaction<CellId>,
    cell_id: CellId,
    marked: &FnvHashSet<CellId>,
    sweeping: &lockfree::set::Set<CellId>,
) -> Result<usize> {

    if marked.contains(&cell_id) || sweeping.insert(cell_id.clone()).is_err() {
        Ok(0)
    } else {

        let mut deleted_count = 0;

        let references = load_cell_references(dynamic_boc_db, &cell_id)?;

        for reference in references {
            deleted_count += sweep_cells_recursive(
                dynamic_boc_db,
                transaction,
                reference.hash().into(), 
                marked,
                sweeping,
            )?;
        }

        transaction.delete(&cell_id);

        deleted_count += 1;
        Ok(deleted_count)
    }
}

fn load_cell_references(dynamic_boc_db: &DynamicBocDb, cell_id: &CellId) -> Result<Vec<Reference>> {
    let slice = dynamic_boc_db.cell_db().get(cell_id)?;
    StorageCell::deserialize_references(slice.as_ref())
}
