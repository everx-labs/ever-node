use crate::{
    cell_db::CellDb, dynamic_boc_diff_writer::{DynamicBocDiffFactory, DynamicBocDiffWriter},
    types::{CellId, StorageCell}
};
use fnv::FnvHashMap;
use std::{ops::{Deref, DerefMut}, sync::{Arc, RwLock, Weak}};
//#[cfg(test)]
//use std::path::Path;
use ton_types::{Cell, Result};

#[derive(Debug)]
pub struct DynamicBocDb {
    db: Arc<CellDb>,
    cells: Arc<RwLock<FnvHashMap<CellId, Weak<StorageCell>>>>,
    diff_factory: DynamicBocDiffFactory,
}

impl DynamicBocDb {

    /// Constructs new instance using in-memory key-value collection

/*
    /// Constructs new instance using RocksDB with given path
*/

    /// Constructs new instance using given key-value collection implementation
    pub(crate) fn with_db(db: CellDb) -> Self {
        let db = Arc::new(db);
        Self {
            db: Arc::clone(&db),
            cells: Arc::new(RwLock::new(FnvHashMap::default())),
            diff_factory: DynamicBocDiffFactory::new(db),
        }
    }

    pub fn cell_db(&self) -> &Arc<CellDb> {
        &self.db
    }

    pub fn cells_map(&self) -> Arc<RwLock<FnvHashMap<CellId, Weak<StorageCell>>>> {
        Arc::clone(&self.cells)
    }

    /// Converts tree of cells into DynamicBoc
    pub fn save_as_dynamic_boc(self: &Arc<Self>, root_cell: Cell) -> Result<usize> {
        let diff_writer = self.diff_factory.construct();

        let written_count = self.save_tree_of_cells_recursive(
            root_cell.clone(),
            Arc::clone(&self.db),
            &diff_writer)?;

        diff_writer.apply()?;

        Ok(written_count)
    }

    /// Gets root cell from key-value storage
    pub fn load_dynamic_boc(self: &Arc<Self>, root_cell_id: &CellId) -> Result<Cell> {
        let storage_cell = self.load_cell(root_cell_id)?;

        Ok(Cell::with_cell_impl_arc(storage_cell))
    }

    pub(super) fn diff_factory(&self) -> &DynamicBocDiffFactory {
        &self.diff_factory
    }

    pub(crate) fn load_cell(self: &Arc<Self>, cell_id: &CellId) -> Result<Arc<StorageCell>> {
        if let Some(cell) = self.cells.read()
            .expect("Poisoned RwLock")
            .get(&cell_id)
        {
            if let Some(ref cell) = Weak::upgrade(&cell) {
                return Ok(Arc::clone(cell));
            }
            // Even if the cell is disposed, we will load and store it later,
            // so we don't need to remove garbage here.
        }
        let storage_cell = Arc::new(
            CellDb::get_cell(&*self.db, &cell_id, Arc::clone(self))?
        );
        self.cells.write()
            .expect("Poisoned RwLock")
            .insert(cell_id.clone(), Arc::downgrade(&storage_cell));

        Ok(storage_cell)
    }

    fn save_tree_of_cells_recursive(
        self: &Arc<Self>,
        cell: Cell,
        cell_db: Arc<CellDb>,
        diff_writer: &DynamicBocDiffWriter
    ) -> Result<usize> {
        let cell_id = CellId::new(cell.repr_hash());
        if cell_db.contains(&cell_id)? || diff_writer.contains_cell(&cell_id) {
            return Ok(0);
        }

        diff_writer.add_cell(cell_id, cell.clone());

        let mut count = 1;
        for i in 0..cell.references_count() {
            count += self.save_tree_of_cells_recursive(
                cell.reference(i)?,
                Arc::clone(&cell_db),
                diff_writer
            )?;
        }

        Ok(count)
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
