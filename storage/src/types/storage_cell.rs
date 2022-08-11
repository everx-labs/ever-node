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

use crate::{types::{CellId, Reference}};
use std::{io::{Cursor, Write}, sync::{Arc, RwLock, atomic::{AtomicU64, Ordering}}};
use ton_types::{
    ByteOrderRead, Cell, CellData, CellImpl, CellType, LevelMask,
    Result, UInt256,
    MAX_LEVEL, MAX_REFERENCES_COUNT
};

#[cfg(not(feature = "ref_count_gc"))]
use crate::dynamic_boc_db::DynamicBocDb;
#[cfg(feature = "ref_count_gc")]
use ton_types::fail;

#[cfg(feature = "ref_count_gc")]
use crate::dynamic_boc_rc_db::DynamicBocDb;

pub struct StorageCell {
    cell_data: CellData,
    references: RwLock<Vec<Reference>>,
    boc_db: Arc<DynamicBocDb>,
    tree_bits_count: u64,
    tree_cell_count: u64,
    #[cfg(feature = "ref_count_gc")]
    parents_count: u32,
    #[cfg(feature = "ref_count_gc")]
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
        #[cfg(feature = "ref_count_gc")]
        use_cache: bool
    ) -> Result<Self> {
        debug_assert!(!data.is_empty());

        let mut reader = Cursor::new(data);
        #[cfg(feature = "ref_count_gc")]
        let parents_count = reader.read_le_u32()?;
        let cell_data = CellData::deserialize(&mut reader)?;
        let references_count = cell_data.references_count();
        let mut references = Vec::with_capacity(references_count as usize);
        for _ in 0..references_count {
            let hash = UInt256::from(reader.read_u256()?);
            references.push(Reference::NeedToLoad(hash));
        }
        let tree_bits_count = reader.read_le_u64()?;
        let tree_cell_count = reader.read_le_u64()?;

        CELL_COUNT.fetch_add(1, Ordering::Relaxed);
        boc_db.allocated().storage_cells.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            cell_data,
            references: RwLock::new(references),
            boc_db,
            tree_bits_count,
            tree_cell_count,
            #[cfg(feature = "ref_count_gc")]
            parents_count,
            #[cfg(feature = "ref_count_gc")]
            use_cache,
        })
    }

    pub fn cell_count() -> u64 {
        CELL_COUNT.load(Ordering::Relaxed)
    }

    #[cfg(feature = "ref_count_gc")]
    pub fn deserialize_parents_count(data: &[u8]) -> Result<u32> {
        let parents_count = Cursor::new(data).read_le_u32()?;
        Ok(parents_count)
    }

    pub fn deserialize_references(data: &[u8]) -> Result<Vec<Reference>> {
        debug_assert!(!data.is_empty());

        let mut reader = Cursor::new(data);
        #[cfg(feature = "ref_count_gc")]
        let _parents_count = reader.read_le_u32()?;
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

        #[cfg(feature = "ref_count_gc")]
        data.write_all(&self.parents_count.to_le_bytes())?;

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
        cell: &dyn CellImpl,
        #[cfg(feature = "ref_count_gc")]
        parents_count: u32,
    ) -> Result<Vec<u8>> {
        let references_count = cell.references_count() as u8;

        debug_assert!(references_count as usize <= MAX_REFERENCES_COUNT);

        let mut data = Vec::new();

        #[cfg(feature = "ref_count_gc")]
        data.write_all(&parents_count.to_le_bytes())?;

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
        #[cfg(feature = "ref_count_gc")]
        parents_count: u32,
        boc_db: Arc<DynamicBocDb>,
        #[cfg(feature = "ref_count_gc")]
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
            #[cfg(feature = "ref_count_gc")]
            parents_count,
            #[cfg(feature = "ref_count_gc")]
            use_cache
        })
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
        let storage_cell = self.boc_db.load_cell(
            &cell_id,
            #[cfg(feature = "ref_count_gc")]
            self.use_cache
        )?;
        self.references.write().expect("Poisoned RwLock")[index] = Reference::Loaded(Arc::clone(&storage_cell));

        Ok(storage_cell)
    }

    #[cfg(feature = "ref_count_gc")]
    pub(crate) fn reference_id(&self, index: usize) -> CellId {
        let hash = self.references.read().expect("Poisoned RwLock")[index].hash();
        CellId::new(hash)
    }

    #[cfg(feature = "ref_count_gc")]
    pub(crate) fn parents_count(&self) -> u32 {
        self.parents_count
    }

    #[cfg(feature = "ref_count_gc")]
    pub(crate) fn inc_parents_count(&mut self) -> Result<u32> {
        if self.parents_count == u32::MAX {
            fail!("Parents count has reached the maximum value");
        }
        self.parents_count += 1;
        Ok(self.parents_count)
    }

    #[cfg(feature = "ref_count_gc")]
    pub(crate) fn dec_parents_count(&mut self) -> Result<u32> {
        if self.parents_count == 0 {
            fail!("Parents count has reached zero value");
        }
        self.parents_count -= 1;
        Ok(self.parents_count)
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
