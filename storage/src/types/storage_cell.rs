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

use crate::{types::{Reference}};
use std::{io::{Cursor, Write}, sync::{Arc, RwLock, atomic::{AtomicU64, Ordering}}};
use ton_types::{
    ByteOrderRead, Cell, CellData, CellImpl, CellType, LevelMask,
    Result, UInt256,
    MAX_LEVEL, MAX_REFERENCES_COUNT
};

use ton_types::fail;

use crate::dynamic_boc_rc_db::DynamicBocDb;

pub struct StorageCell {
    cell_data: CellData,
    references: RwLock<Vec<Reference>>,
    boc_db: Arc<DynamicBocDb>,
    tree_bits_count: u64,
    tree_cell_count: u64,
    use_cache: bool,
}

lazy_static::lazy_static!{
    static ref CELL_COUNT: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
}

/// Represents Cell for storing in persistent storage
impl StorageCell {

    /// Constructs StorageCell by deserialization
    pub fn deserialize(
        boc_db: Arc<DynamicBocDb>, 
        data: &[u8], 
        use_cache: bool,
        with_parents_count: bool,
    ) -> Result<(Self, u32)> {
        debug_assert!(!data.is_empty());

        let mut reader = Cursor::new(data);
        let parents_count = if with_parents_count {
            reader.read_le_u32()?
        } else {
            0
        };
        let cell_data = CellData::deserialize(&mut reader)?;
        let references_count = cell_data.references_count();
        let mut references = Vec::with_capacity(references_count as usize);
        for _ in 0..references_count {
            let hash = UInt256::from(reader.read_u256()?);
            references.push(Reference::NeedToLoad(hash));
        }
        let tree_bits_count = reader.read_le_u64()?;
        let tree_cell_count = reader.read_le_u64()?;

        let read = reader.position();
        let data_len = data.len();
        if read != data_len as u64 {
            fail!("There is more data after storage cell deserialisation (read: {}, data len: {})",
                read, data_len);
        }

        CELL_COUNT.fetch_add(1, Ordering::Relaxed);
        boc_db.allocated().storage_cells.fetch_add(1, Ordering::Relaxed);
        let cell = Self {
            cell_data,
            references: RwLock::new(references),
            boc_db,
            tree_bits_count,
            tree_cell_count,
            use_cache,
        };
        Ok((cell, parents_count))
    }

    pub fn cell_count() -> u64 {
        CELL_COUNT.load(Ordering::Relaxed)
    }

    pub fn deserialize_parents_count(data: &[u8]) -> Result<u32> {
        let parents_count = Cursor::new(data).read_le_u32()?;
        Ok(parents_count)
    }

    pub fn deserialize_references(
        data: &[u8],
        with_parents_count: bool,
    ) -> Result<Vec<Reference>> {
        debug_assert!(!data.is_empty());

        let mut reader = Cursor::new(data);
        if with_parents_count {
            let _parents_count = reader.read_le_u32()?;
        }
        let cell_data = CellData::deserialize(&mut reader)?;
        let references_count = cell_data.references_count();
        let mut references = Vec::with_capacity(references_count);
        for _ in 0..references_count {
            let hash = UInt256::from(reader.read_u256()?);
            references.push(Reference::NeedToLoad(hash));
        }
        Ok(references)
    }

    pub fn serialize_self(&self) -> Result<Vec<u8>> {

        let mut data = Vec::new();

        self.cell_data.serialize(&mut data)?;

        let references = &self.references.read().expect("Poisoned RwLock");
        for r in references.iter() {
            data.write_all(r.hash().as_slice())?;
        }

        data.write_all(&self.tree_bits_count().to_le_bytes())?;
        data.write_all(&self.tree_cell_count().to_le_bytes())?;

        debug_assert!(!data.is_empty());

        Ok(data)
    }

    pub fn serialize(
        cell: &dyn CellImpl
    ) -> Result<Vec<u8>> {
        let references_count = cell.references_count() as u8;

        debug_assert!(references_count as usize <= MAX_REFERENCES_COUNT);

        let mut data = Vec::new();

        cell.cell_data().serialize(&mut data)?;

        for i in 0..references_count {
            data.write_all(cell.reference(i as usize)?.repr_hash().as_slice())?;
        }

        data.write_all(&cell.tree_bits_count().to_le_bytes())?;
        data.write_all(&cell.tree_cell_count().to_le_bytes())?;

        debug_assert!(!data.is_empty());

        Ok(data)
    }

    pub fn with_cell(
        cell: &dyn CellImpl,
        boc_db: Arc<DynamicBocDb>,
        use_cache: bool,
    ) -> Result<Self> {
        let references_count = cell.references_count();
        let mut references = Vec::with_capacity(references_count);
        for i in 0..references_count {
            let hash = cell.reference(i)?.repr_hash();
            references.push(Reference::NeedToLoad(hash));
        }
        CELL_COUNT.fetch_add(1, Ordering::Relaxed);
        boc_db.allocated().storage_cells.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            cell_data: cell.cell_data().clone(),
            references: RwLock::new(references),
            boc_db,
            tree_bits_count: cell.tree_bits_count(),
            tree_cell_count: cell.tree_bits_count(),
            use_cache
        })
    }

    /// Gets cell's id
    pub fn id(&self) -> UInt256 {
        self.repr_hash()
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

        let cell_id = hash;
        let storage_cell = self.boc_db.load_cell(
            &cell_id,
            self.use_cache
        )?;
        self.references.write().expect("Poisoned RwLock")[index] = Reference::Loaded(Arc::clone(&storage_cell));

        Ok(storage_cell)
    }

    pub(crate) fn reference_id(&self, index: usize) -> UInt256 {
        self.references.read().expect("Poisoned RwLock")[index].hash()
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

    fn reference_repr_hash(&self, index: usize) -> Result<UInt256> {
        Ok(self.reference_id(index))
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
        self.boc_db.allocated().storage_cells.fetch_sub(1, Ordering::Relaxed);
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
