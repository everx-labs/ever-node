use crate::{dynamic_boc_db::DynamicBocDb, types::{CellId, Reference}};
use std::sync::{Arc, RwLock};
use ton_types::{Cell, CellData, CellImpl, CellType, LevelMask, MAX_LEVEL, Result};
use ton_types::types::UInt256;

#[derive(Debug)]
pub struct StorageCell {
    cell_data: CellData,
    references: RwLock<Vec<Reference>>,
    boc_db: Arc<DynamicBocDb>,
}

/// Represents Cell for storing in persistent storage
impl StorageCell {
    /// Constructs StorageCell with given parameters
    pub fn with_params(
        cell_data: CellData,
        references: Vec<Reference>,
        boc_db: Arc<DynamicBocDb>,
    ) -> Self {
        Self {
            cell_data,
            references: RwLock::new(references),
            boc_db,
        }
    }

    /// Gets cell's id
    pub fn id(&self) -> CellId {
        CellId::new(self.repr_hash())
    }

    /// Gets representation hash
    pub fn repr_hash(&self) -> UInt256 {
        self.hash(MAX_LEVEL as usize)
    }

    pub(crate) fn reference(&self, index: usize) -> Result<Arc<StorageCell>> {
        let hash = match &self.references.read().expect("Poisoned RwLock")[index]
        {
            Reference::Loaded(cell) => return Ok(Arc::clone(cell)),
            Reference::NeedToLoad(hash) => hash.clone()
        };

        let cell_id = CellId::from(hash.clone());
        let storage_cell = self.boc_db.load_cell(&cell_id)?;
        self.references.write().expect("Poisoned RwLock")[index] = Reference::Loaded(Arc::clone(&storage_cell));

        Ok(storage_cell)
    }
}

impl CellImpl for StorageCell {
    fn data(&self) -> &[u8] {
        self.cell_data.data()
    }

    fn cell_data(&self) -> &CellData {
        &self.cell_data
    }

    fn bit_length(&self) -> usize {
        self.cell_data.bit_length() as usize
    }

    fn references_count(&self) -> usize {
        self.references.read().expect("Poisoned RwLock").len()
    }

    fn reference(&self, index: usize) -> Result<Cell> {
        Ok(Cell::with_cell_impl_arc(self.reference(index)?))
    }

    fn cell_type(&self) -> CellType {
        self.cell_data.cell_type()
    }

    fn level_mask(&self) -> LevelMask {
        self.cell_data.level_mask()
    }

    fn hash(&self, index: usize) -> UInt256 {
        self.cell_data.hash(index)
    }

    fn depth(&self, index: usize) -> u16 {
        self.cell_data.depth(index)
    }

    fn store_hashes(&self) -> bool {
        self.cell_data.store_hashes()
    }
}

fn references_hashes_equal(left: &Vec<Reference>, right: &Vec<Reference>) -> bool {
    for i in 0..left.len() {
        if left[i].hash() != right[i].hash() {
            return false;
        }
    }
    true
}

impl Drop for StorageCell {
    fn drop(&mut self) {
        self.boc_db.cells_map().write()
            .expect("Poisoned RwLock")
            .remove(&self.id());
    }
}

impl PartialEq for StorageCell {
    fn eq(&self, other: &Self) -> bool {
        if self.cell_data != other.cell_data {
            return false;
        }

        let self_guard = self.references.read().expect("Poisoned RwLock");
        let other_guard = other.references.read().expect("Poisoned RwLock");
        self_guard.len() == other_guard.len()
            && references_hashes_equal(&self_guard, &other_guard)
    }
}
