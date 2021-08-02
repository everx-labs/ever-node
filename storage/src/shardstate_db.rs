use crate::{
    block_handle_db::BlockHandleDb, cell_db::CellDb, 
    db::{rocksdb::RocksDb, traits::{DbKey, KvcSnapshotable}},
    dynamic_boc_db::DynamicBocDb, dynamic_boc_diff_writer::DynamicBocDiffWriter,
    traits::Serializable, types::{CellId, Reference, StorageCell}
};
use fnv::FnvHashSet;
use std::{
    io::{Cursor, Read, Write}, path::Path, 
    sync::{Arc, atomic::{AtomicU32, Ordering}}
};
use std::ops::Deref;
use ton_block::{BlockIdExt, UnixTime32};
use ton_types::{Cell, Result};

pub struct ShardStateDb {
    shardstate_db: Arc<dyn KvcSnapshotable<BlockIdExt>>,
    dynamic_boc_db: Arc<DynamicBocDb>,
}

pub(crate) struct DbEntry {
    pub cell_id: CellId,
    pub block_id_ext: BlockIdExt,
}

impl DbEntry {
    pub fn with_params(cell_id: CellId, block_id_ext: BlockIdExt) -> Self {
        Self { cell_id, block_id_ext }
    }
}

impl Serializable for DbEntry {
    fn serialize<T: Write>(&self, writer: &mut T) -> Result<()> {
        writer.write_all(self.cell_id.key())?;
        self.block_id_ext.serialize(writer)
    }

    fn deserialize<T: Read>(reader: &mut T) -> Result<Self> {
        let mut buf = [0; 32];
        reader.read_exact(&mut buf)?;
        let cell_id = CellId::new(buf.into());
        let block_id_ext = BlockIdExt::deserialize(reader)?;

        Ok(Self { cell_id, block_id_ext })
    }
}

impl ShardStateDb {

    /// Constructs new instance using in-memory key-value collections

    /// Constructs new instance using RocksDB with given paths
    pub fn with_paths<P1: AsRef<Path>, P2: AsRef<Path>>(shardstate_db_path: P1, cell_db_path: P2) -> Self {
        Self::with_dbs(
            Arc::new(RocksDb::with_path(shardstate_db_path)),
            CellDb::with_path(cell_db_path),
        )
    }

    /// Constructs new instance using given key-value collection implementations
    fn with_dbs(shardstate_db: Arc<dyn KvcSnapshotable<BlockIdExt>>, cell_db: CellDb) -> Self {
        Self {
            shardstate_db,
            dynamic_boc_db: Arc::new(DynamicBocDb::with_db(cell_db)),
        }
    }

    /// Returns reference to shardstates database
    pub fn shardstate_db(&self) -> Arc<dyn KvcSnapshotable<BlockIdExt>> {
        Arc::clone(&self.shardstate_db)
    }

    /// Returns reference to dynamic BOC database
    pub fn dynamic_boc_db(&self) -> Arc<DynamicBocDb> {
        Arc::clone(&self.dynamic_boc_db)
    }

    /// Returns reference to cell_db database

    /// Stores cells from given tree which don't exist in the storage.
    /// Returns root cell which is implemented as StorageCell.
    /// So after store() origin shard state's cells might be dropped.
    pub fn put(&self, id: &BlockIdExt, state_root: Cell) -> Result<()> {
        let cell_id = CellId::from(state_root.repr_hash());
        self.dynamic_boc_db.save_as_dynamic_boc(state_root.deref())?;

        let db_entry = DbEntry::with_params(cell_id, id.clone());
        let mut buf = Vec::new();
        db_entry.serialize(&mut Cursor::new(&mut buf))?;

        self.shardstate_db.put(id, buf.as_slice())?;

        Ok(())
    }

    /// Loads previously stored root cell
    pub fn get(&self, id: &BlockIdExt) -> Result<Cell> {
        let db_entry = DbEntry::from_slice(self.shardstate_db.get(id)?.as_ref())?;
        let root_cell = self.dynamic_boc_db.load_dynamic_boc(&db_entry.cell_id)?;

        Ok(root_cell)
    }
}

pub(crate) trait AllowStateGcResolver: Send + Sync {
    fn allow_state_gc(&self, block_id_ext: &BlockIdExt, gc_utime: UnixTime32) -> Result<bool>;
}

struct AllowStateGcResolverImpl {
    // dynamic_boc_db: Arc<DynamicBocDb>,
    block_handle_db: Arc<BlockHandleDb>,
    shard_state_ttl: AtomicU32,
}

impl AllowStateGcResolverImpl {
    pub fn with_data(/*dynamic_boc_db: Arc<DynamicBocDb>,*/ block_handle_db: Arc<BlockHandleDb>) -> Self {
        Self {
            // dynamic_boc_db,
            block_handle_db,
            shard_state_ttl: AtomicU32::new(3600 * 24),
        }
    }

    #[allow(dead_code)]
    pub fn shard_state_ttl(&self) -> u32 {
        self.shard_state_ttl.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    pub fn set_shard_state_ttl(&self, value: u32) {
        self.shard_state_ttl.store(value, Ordering::SeqCst)
    }
}

impl AllowStateGcResolver for AllowStateGcResolverImpl {
    fn allow_state_gc(&self, block_id_ext: &BlockIdExt, gc_utime: UnixTime32) -> Result<bool> {
        let block_meta = self.block_handle_db.get_value(block_id_ext)?;

        // TODO: Implement more sophisticated logic of decision shard state garbage collecting

        Ok(block_meta.gen_utime + self.shard_state_ttl() < gc_utime.0)
    }
}

pub struct GC {
    shardstate_db: Arc<dyn KvcSnapshotable<BlockIdExt>>,
    dynamic_boc_db: Arc<DynamicBocDb>,
    allow_state_gc_resolver: Arc<dyn AllowStateGcResolver>,
}

impl GC {
    pub fn new(db: &ShardStateDb, block_handle_db: Arc<BlockHandleDb>) -> Self {
        Self::with_data(
            db.shardstate_db(),
            db.dynamic_boc_db(),
            Arc::new(
                AllowStateGcResolverImpl::with_data(
                    // db.dynamic_boc_db(),
                    block_handle_db
                )
            )
        )
    }

    pub(crate) fn with_data(
        shardstate_db: Arc<dyn KvcSnapshotable<BlockIdExt>>,
        dynamic_boc_db: Arc<DynamicBocDb>,
        allow_state_gc_resolver: Arc<dyn AllowStateGcResolver>
    ) -> Self {
        Self {
            shardstate_db,
            dynamic_boc_db,
            allow_state_gc_resolver,
        }
    }

    pub fn collect(&self) -> Result<usize> {
        let (marked, to_sweep) = self.mark(UnixTime32::now())?;
        let result = self.sweep(to_sweep, marked);

        result
    }

    fn mark(
        &self, 
        gc_utime: UnixTime32
    ) -> Result<(FnvHashSet<CellId>, Vec<(BlockIdExt, CellId)>)> {
        let mut to_mark = Vec::new();
        let mut to_sweep = Vec::new();
        let shardstates = self.shardstate_db.snapshot()?;
        shardstates.for_each(&mut |_key, value| {
            let db_entry = DbEntry::from_slice(value)?;
            let cell_id = db_entry.cell_id;
            let block_id_ext = db_entry.block_id_ext;
            if (!self.dynamic_boc_db.cells_map().read()
                .expect("Poisoned RwLock")
                .contains_key(&cell_id))
                && self.allow_state_gc_resolver.allow_state_gc(&block_id_ext, gc_utime)?
            {
                to_sweep.push((block_id_ext, cell_id));
            } else {
                to_mark.push(cell_id);
            }

            Ok(true)
        })?;

        let mut marked = FnvHashSet::default();
        if to_sweep.len() > 0 {
            for cell_id in to_mark {
                self.mark_subtree_recursive(cell_id, &mut marked)?;
            }
        }

        Ok((marked, to_sweep))
    }

    fn mark_subtree_recursive(&self, cell_id: CellId, marked: &mut FnvHashSet<CellId>) -> Result<()> {
        if marked.contains(&cell_id) {
            return Ok(());
        }

        let references = self.load_cell_references(&cell_id)?;
        marked.insert(cell_id);

        for reference in references {
            self.mark_subtree_recursive(reference.hash().into(), marked)?;
        }

        Ok(())
    }

    fn sweep(
        &self, 
        to_sweep: Vec<(BlockIdExt, CellId)>, 
        marked: FnvHashSet<CellId>
    ) -> Result<usize> {
        if to_sweep.len() < 1 {
            return Ok(0);
        }

        let diff_writer = self.dynamic_boc_db.diff_factory().construct();
        let mut deleted_count = 0;
        for (block_id, cell_id) in to_sweep {
            deleted_count += self.sweep_cells_recursive(&diff_writer, cell_id, &marked)?;
            self.shardstate_db.delete(&block_id)?;
        }
        diff_writer.apply()?;

        Ok(deleted_count)
    }

    fn sweep_cells_recursive(
        &self,
        diff_writer: &DynamicBocDiffWriter,
        cell_id: CellId,
        marked: &FnvHashSet<CellId>,
    ) -> Result<usize> {
        if marked.contains(&cell_id) {
            return Ok(0);
        }

        let mut deleted_count = 0;
        let references = self.load_cell_references(&cell_id)?;
        for reference in references {
            deleted_count += self.sweep_cells_recursive(diff_writer, reference.hash().into(), marked)?;
        }

        diff_writer.delete_cell(&cell_id);
        deleted_count += 1;

        Ok(deleted_count)
    }

    fn load_cell_references(&self, cell_id: &CellId) -> Result<Vec<Reference>> {
        let slice = self.dynamic_boc_db.cell_db().get(cell_id)?;

        StorageCell::deserialize_references(slice.as_ref())
    }
}
