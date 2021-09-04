use crate::{
    cell_db::CellDb, /*dynamic_boc_diff_writer::{DynamicBocDiffFactory, DynamicBocDiffWriter},*/
    types::{CellId, StorageCell},
    db::traits::KvcTransaction,
    TARGET,
};
use fnv::{FnvHashMap, FnvHashSet};
use std::{ops::{Deref, DerefMut}, sync::{Arc, RwLock, Weak}};
//#[cfg(test)]
//use std::path::Path;
use ton_types::{Cell, Result, error, CellImpl, MAX_LEVEL};

#[derive(Debug)]
pub struct DynamicBocDb {
    db: Arc<CellDb>,
    cells: Arc<RwLock<FnvHashMap<CellId, Weak<StorageCell>>>>,
    db_index: u32,
}

impl DynamicBocDb {

    /// Constructs new instance using in-memory key-value collection

/*
    /// Constructs new instance using RocksDB with given path
*/

    /// Constructs new instance using given key-value collection implementation
    pub(crate) fn with_db(db: CellDb, db_index: u32) -> Self {
        let db = Arc::new(db);
        Self {
            db: Arc::clone(&db),
            cells: Arc::new(RwLock::new(FnvHashMap::default())),
            db_index,
        }
    }

    pub fn cell_db(&self) -> &Arc<CellDb> {
        &self.db
    }

    pub fn cells_map(&self) -> Arc<RwLock<FnvHashMap<CellId, Weak<StorageCell>>>> {
        Arc::clone(&self.cells)
    }

    pub fn db_index(&self) -> u32 {
        self.db_index
    }

    /// Converts tree of cells into DynamicBoc
    pub fn save_as_dynamic_boc(self: &Arc<Self>, root_cell: &dyn CellImpl) -> Result<usize> {
        let mut transaction = self.db.begin_transaction()?;
        let mut visited = FnvHashSet::default();
        let root_id = CellId::new(root_cell.hash(MAX_LEVEL));

        let written_count = self.save_tree_of_cells_recursive(
            root_cell,
            Arc::clone(&self.db),
            transaction.as_mut(),
            &mut visited,
            &root_id
        )?;

        transaction.commit()?;
        for h in visited {
            log::trace!(target: TARGET, "DynamicBocDb::save_as_dynamic_boc  id {}  root_cell_id {}  db_index {}",
                h, root_id, self.db_index);
        }

        Ok(written_count)
    }

    /// Gets root cell from key-value storage
    pub fn load_dynamic_boc(self: &Arc<Self>, root_cell_id: &CellId) -> Result<Cell> {
        let storage_cell = self.load_cell(root_cell_id)?;

        Ok(Cell::with_cell_impl_arc(storage_cell))
    }

    pub(crate) fn load_cell(self: &Arc<Self>, cell_id: &CellId) -> Result<Arc<StorageCell>> {
        let in_cache = if let Some(cell) = self.cells.read()
            .expect("Poisoned RwLock")
            .get(&cell_id)
        {
            if let Some(cell) = Weak::upgrade(&cell) {
                log::trace!(target: TARGET, "DynamicBocDb::load_cell  from cache  id {}  db_index {}",
                    cell_id, self.db_index);
                return Ok(cell);
            }
            // Even if the cell is disposed, we will load and store it later,
            // so we don't need to remove garbage here.
            true
        } else {
            false
        };
        let storage_cell = Arc::new(
            CellDb::get_cell(self.db.deref(), &cell_id, Arc::clone(self))
                .map_err(|e| error!("Can't load cell  id {}  db_index {}  in_cache {}  error: {}", cell_id, self.db_index, in_cache, e))?
        );
        self.cells.write()
            .expect("Poisoned RwLock")
            .insert(cell_id.clone(), Arc::downgrade(&storage_cell));

        log::trace!(target: TARGET, "DynamicBocDb::load_cell  from DB  id {}  db_index {}  in_cache {}",
            cell_id, self.db_index, in_cache);

        Ok(storage_cell)
    }

    fn save_tree_of_cells_recursive(
        self: &Arc<Self>,
        cell: &dyn CellImpl,
        cell_db: Arc<CellDb>,
        transaction: &mut dyn KvcTransaction<CellId>,
        visited: &mut FnvHashSet<CellId>,
        root_id: &CellId,
    ) -> Result<usize> {
        let cell_id = CellId::new(cell.hash(MAX_LEVEL));
        if visited.contains(&cell_id) {
            Ok(0)
        } else if cell_db.contains(&cell_id)? {
            log::trace!(
                target: TARGET,
                "DynamicBocDb::save_tree_of_cells_recursive  already in DB  id {}  root_cell_id {}  db_index {}",
                cell_id, root_id, self.db_index);
            Ok(0)
        } else {
            transaction.put(&cell_id, &StorageCell::serialize(cell)?);
            visited.insert(cell_id);

            let mut count = 1;
            for i in 0..cell.references_count() {
                count += self.save_tree_of_cells_recursive(
                    cell.reference(i)?.deref(),
                    Arc::clone(&cell_db),
                    transaction,
                    visited,
                    root_id
                )?;
            }

            Ok(count)
        }
    }
}

impl Deref for DynamicBocDb {
    type Target = Arc<CellDb>;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl DerefMut for DynamicBocDb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.db
    }
}
