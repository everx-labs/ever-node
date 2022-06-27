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

use crate::{dynamic_boc_db::DynamicBocDb, types::{CellId, Reference}};
use std::{io::{Cursor, Write}, sync::{Arc, RwLock, atomic::{AtomicU64, Ordering}}};
use ton_types::{
    ByteOrderRead, Cell, CellData, CellImpl, CellType, LevelMask,
    Result, UInt256,
    MAX_LEVEL, MAX_REFERENCES_COUNT
};

//#[derive(Debug)]
pub struct StorageCell {
    cell_data: CellData,
    references: RwLock<Vec<Reference>>,
    boc_db: Arc<DynamicBocDb>,
    tree_bits_count: u64,
    tree_cell_count: u64,
}

lazy_static::lazy_static!{
    static ref CELL_COUNT: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
}

/// Represents Cell for storing in persistent storage
impl StorageCell {

    /// Constructs StorageCell by deserialization
    pub fn deserialize(boc_db: Arc<DynamicBocDb>, data: &[u8]) -> Result<Self> {
        assert!(!data.is_empty());

        let mut reader = Cursor::new(data);
        let cell_data = CellData::deserialize(&mut reader)?;
        let references_count = cell_data.references_count();
        let mut references = Vec::with_capacity(references_count as usize);
        for _ in 0..references_count {
            let hash = UInt256::from(reader.read_u256()?);
            references.push(Reference::NeedToLoad(hash));
        }
        let (tree_bits_count, tree_cell_count) = reader
            .read_be_u64()
            .and_then(|first| Ok((first, reader.read_be_u64()?)))
            .unwrap_or((0, 0));
        let ret = Self {
            cell_data,
            references: RwLock::new(references),
            boc_db,
            tree_bits_count,
            tree_cell_count,
        };
        CELL_COUNT.fetch_add(1, Ordering::Relaxed);
        Ok(ret)
    }

    pub fn cell_count() -> u64 {
        CELL_COUNT.load(Ordering::Relaxed)
    }

    pub fn deserialize_references(data: &[u8]) -> Result<Vec<Reference>> {
        assert!(!data.is_empty());

        let mut reader = Cursor::new(data);
        let cell_data = CellData::deserialize(&mut reader)?;
        let references_count = cell_data.references_count();
        let mut references = Vec::with_capacity(references_count);
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

        cell.cell_data().serialize(&mut data)?;

        for i in 0..references_count {
            data.write_all(cell.reference(i as usize)?.repr_hash().as_slice())?;
        }

        data.write_all(&cell.tree_bits_count().to_be_bytes())?;
        data.write_all(&cell.tree_cell_count().to_be_bytes())?;

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
}

impl CellImpl for StorageCell {
    fn data(&self) -> &[u8] {
        self.cell_data.data()
    }

    fn raw_data(&self) -> Result<&[u8]> {
        Ok(self.cell_data.raw_data())
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
        self.tree_bits_count
    }

    fn tree_cell_count(&self) -> u64 {
        self.tree_cell_count as u64
    }
}

fn references_hashes_equal(left: &[Reference], right: &[Reference]) -> bool {
    for i in 0..left.len() {
        if left[i].hash() != right[i].hash() {
            return false;
        }
    }
    true
}

impl Drop for StorageCell {
    fn drop(&mut self) {
        CELL_COUNT.fetch_sub(1, Ordering::Relaxed);
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
