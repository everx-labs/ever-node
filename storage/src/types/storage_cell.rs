use crate::{dynamic_boc_db::DynamicBocDb, types::{CellId, Reference}};
use std::{io::{Cursor, Write}, sync::{Arc, RwLock, atomic::{AtomicU64, Ordering}}};
use ton_types::{
    ByteOrderRead, Cell, CellData, CellImpl, CellType, LevelMask,
    Result, UInt256,
    MAX_LEVEL, MAX_REFERENCES_COUNT
};

#[derive(Debug)]
pub struct StorageCell {
    cell_data: CellData,
    references: RwLock<Vec<Reference>>,
    boc_db: Arc<DynamicBocDb>,
    tree_bits_count: Arc<AtomicU64>,
    tree_cell_count: Arc<AtomicU64>,
}

/// Represents Cell for storing in persistent storage
impl StorageCell {
    /// Constructs StorageCell by deserialization
    pub fn deserialize(boc_db: Arc<DynamicBocDb>, data: &[u8]) -> Result<Self> {
        assert!(!data.is_empty());

        let mut reader = Cursor::new(data);
        let references_count = reader.read_byte()?;
        let mut references = Vec::with_capacity(references_count as usize);
        for _ in 0..references_count {
            let hash = UInt256::from(reader.read_u256()?);
            references.push(Reference::NeedToLoad(hash));
        }
        let cell_data = CellData::deserialize(&mut reader)?;
        let (tree_bits_count, tree_cell_count) = reader
            .read_be_u64()
            .and_then(|first| Ok((first, reader.read_be_u64()?)))
            .unwrap_or((0, 0));
        Ok(Self {
            cell_data,
            references: RwLock::new(references),
            boc_db,
            tree_bits_count: Arc::new(AtomicU64::new(tree_bits_count)),
            tree_cell_count: Arc::new(AtomicU64::new(tree_cell_count)),
        })
    }

    pub fn deserialize_references(data: &[u8]) -> Result<Vec<Reference>> {
        assert!(!data.is_empty());

        let mut reader = Cursor::new(data);
        let references_count = reader.read_byte()?;
        let mut references = Vec::with_capacity(references_count as usize);
        for _ in 0..references_count {
            let hash = UInt256::from(reader.read_u256()?);
            references.push(Reference::NeedToLoad(hash));
        }
        Ok(references)
    }

    pub fn serialize(cell: &dyn CellImpl) -> Result<Vec<u8>> {
        let references_count = cell.references_count() as u8;

        assert!(references_count as usize <= MAX_REFERENCES_COUNT);

        let mut data = Vec::new();

        data.write(&[references_count])?;

        for i in 0..references_count {
            data.write(cell.reference(i as usize)?.repr_hash().as_slice())?;
        }

        cell.cell_data().serialize(&mut data)?;

        data.write(&cell.tree_bits_count().to_be_bytes())?;
        data.write(&cell.tree_cell_count().to_be_bytes())?;

        assert!(!data.is_empty());

        Ok(data)
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
        let hash = match &self.references.read().expect("Poisoned RwLock")[index] {
            Reference::Loaded(cell) => return Ok(Arc::clone(cell)),
            Reference::NeedToLoad(hash) => hash.clone()
        };

        let cell_id = CellId::new(hash);
        let storage_cell = self.boc_db.load_cell(&cell_id)?;
        self.references.write().expect("Poisoned RwLock")[index] = Reference::Loaded(Arc::clone(&storage_cell));

        Ok(storage_cell)
    }

    // need to patch database after update
    #[allow(dead_code)]
    fn update_tree_stat_if_need(&self) -> Result<()> {
        if self.tree_cell_count.load(Ordering::Relaxed) == 0 {
            let mut tree_bits_count = self.cell_data.bit_length() as u64;
            let mut tree_cell_count = 1;
            for index in 0..self.references_count() {
                if let Ok(reference) = self.reference(index) {
                    tree_bits_count += reference.tree_bits_count();
                    tree_cell_count += reference.tree_cell_count();
                }
            }
            self.tree_bits_count.store(tree_bits_count, Ordering::Relaxed);
            self.tree_cell_count.store(tree_cell_count, Ordering::Relaxed);
            self.boc_db.save_as_dynamic_boc(self)?;
        }
        Ok(())
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

    fn tree_bits_count(&self) -> u64 {
        // self.update_tree_stat_if_need().ok();
        self.tree_bits_count.load(Ordering::Relaxed)
    }

    fn tree_cell_count(&self) -> u64 {
        // self.update_tree_stat_if_need().ok();
        self.tree_cell_count.load(Ordering::Relaxed)
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
