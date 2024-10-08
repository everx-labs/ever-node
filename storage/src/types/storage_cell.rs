/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific EVERX DEV software governing permissions and
* limitations under the License.
*/

use crate::{dynamic_boc_rc_db::DynamicBocDb, TARGET};
use std::{io::Cursor, sync::{Arc,Weak, atomic::{AtomicU64, Ordering}}};
use ever_block::{
    error, fail, ByteOrderRead, Cell, CellData, CellImpl, CellType, LevelMask, Result, UInt256, 
    MAX_LEVEL,
};

#[cfg(test)]
#[path = "tests/test_storage_cell.rs"]
mod tests;

const NOT_INITIALIZED_DEPTH: u16 = u16::MAX;

struct Reference {
    hash: UInt256,
    depth: u16,
    cell: Option<Weak<dyn CellImpl>>,
}

pub struct StorageCell {
    cell_data: CellData,
    references: parking_lot::RwLock<Vec<Reference>>,
    tree_bits_count: u64,
    tree_cell_count: u64,
    boc_db: Weak<DynamicBocDb>,
    use_cache: bool,
}

lazy_static::lazy_static!{
    static ref CELL_COUNT: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
}

struct SliceReader<'a> {
    data: &'a [u8],
    position: usize,
}
impl<'a> SliceReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }
    fn read(&mut self, size: usize) -> Result<&'a [u8]> {
        if self.data.len() < self.position + size {
            fail!("Buffer is too small to read {} bytes", size);
        }
        let slice = &self.data[self.position..self.position+size];
        self.position += size;
        Ok(slice)
    }
}

/// Represents Cell for storing in persistent storage
impl StorageCell {

    pub fn deserialize(
        boc_db: &Arc<DynamicBocDb>,
        repr_hash: &UInt256,
        data: &[u8],
        use_cache: bool,
    ) -> Result<Self> {

        if data.len() < 2 {
            fail!("Buffer is too small to read description bytes");
        }

        // Cell data (same as in BOC)
        let mut cell_data = CellData::with_unbounded_raw_data_slice(data)?;
        let mut reader = SliceReader::new(&data[cell_data.raw_data().len()..]);

        // If the cell data isn't contain stored high hashes - read it now
        let level = cell_data.level();
        let store_hashes = cell_data.store_hashes();
        let mut hash_array_index = 0;
        if level > 0 && // there are high hashes
           cell_data.cell_type() != CellType::PrunedBranch && // pruned branche stores high hashes in the data
           !store_hashes // some cells store high hashes in the raw data
        {
            for _ in 0..level {
                let hash = reader.read(32)?;
                let depth = u16::from_le_bytes(reader.read(2)?.try_into()?);
                cell_data.set_hash_depth(hash_array_index, hash, depth)?;
                hash_array_index += 1;
            }
        }

        if !store_hashes {
            // Representation depth without hash, because DB key is repr hash
            let depth = u16::from_le_bytes(reader.read(2)?.try_into()?);
            cell_data.set_hash_depth(hash_array_index, repr_hash.as_slice(), depth)?;
        }

        // References (child repr hash + child repr depth)
        let references_count = cell_data.references_count();
        let mut references = Vec::with_capacity(references_count);
        for _ in 0..references_count {
            let hash = UInt256::from_slice(reader.read(32)?);
            let depth = u16::from_le_bytes(reader.read(2)?.try_into()?);
            references.push(Reference { hash, depth, cell: None });
        }

        // Tree bits & cells count
        let tree_bits_count = u64::from_le_bytes(reader.read(8)?.try_into()?);
        let tree_cell_count = u64::from_le_bytes(reader.read(8)?.try_into()?);

        let read = cell_data.raw_data().len() + reader.position;
        if read != data.len() {
            fail!("There is more data after storage cell deserialisation (read: {}, data len: {})",
                read, data.len());
        }

        CELL_COUNT.fetch_add(1, Ordering::Relaxed);
        boc_db.allocated().storage_cells.fetch_add(1, Ordering::Relaxed);

        Ok(Self {
            cell_data,
            references: parking_lot::RwLock::new(references),
            boc_db: Arc::downgrade(boc_db),
            tree_bits_count,
            tree_cell_count,
            use_cache,
        })

    }

    pub fn migrate_to_v6(data: &[u8]) -> Result<Vec<u8>> {

        let mut reader = Cursor::new(data);
        let cell_data = CellData::deserialize(&mut reader)?;
        let references_count = cell_data.references_count();
        let store_hashes = cell_data.store_hashes();
        let new_data_size = Self::calc_serialized_size(
            cell_data.raw_data().len(),
            store_hashes,
            cell_data.level(),
            references_count,
            cell_data.cell_type(),
        );
        let mut new_data = Vec::with_capacity(new_data_size);

        // Cell data (same as in BOC)
        new_data.extend_from_slice(cell_data.raw_data());

        // Hashes/depths
        if !store_hashes && cell_data.cell_type() != CellType::PrunedBranch {
            let level_mask = cell_data.level_mask().mask();
            if level_mask != 0 {
                for i in 0..MAX_LEVEL {
                    if (1 << i) & level_mask != 0 {
                        new_data.extend_from_slice(cell_data.hash(i).as_slice());
                        new_data.extend_from_slice(&cell_data.depth(i).to_le_bytes());
                    }
                }
            }
        }
        if !store_hashes {
            new_data.extend_from_slice(&cell_data.depth(MAX_LEVEL).to_le_bytes());
        }

        // References
        for _ in 0..references_count {
            let ref_hash = reader.read_u256()?;
            new_data.extend_from_slice(&ref_hash.as_slice());
            new_data.extend_from_slice(&NOT_INITIALIZED_DEPTH.to_le_bytes());
        }

        // Tree bits/cells count
        let pos = reader.position() as usize;
        new_data.extend_from_slice(&data[pos..pos + 16]);

        let read = reader.position() + 16;
        let data_len = data.len();
        if read != data_len as u64 {
            fail!("There is more data after storage cell deserialisation (read: {}, data len: {})",
                read, data_len);
        }

        Ok(new_data)
    }

    pub fn cell_count() -> u64 {
        CELL_COUNT.load(Ordering::Relaxed)
    }

    pub fn calc_serialized_size(
        raw_data_len: usize,
        store_hashes: bool,
        level: u8,
        references_count: usize,
        cell_type: CellType,
    ) -> usize {
        let mut data_size = raw_data_len; // data
        if !store_hashes {
            if cell_type != CellType::PrunedBranch {
                data_size += level as usize * 34; // hash + depth
            }
            data_size += 2; // repr depth
        }
        data_size += 16; // tree bits + tree cell count
        data_size += references_count * 34; // reference hash + depth
        data_size
    }

    pub fn serialize(cell: &dyn CellImpl) -> Result<Vec<u8>> {
        let store_hashes = cell.store_hashes();
        let data_size = Self::calc_serialized_size(
            cell.raw_data()?.len(),
            store_hashes,
            cell.level(),
            cell.references_count(),
            cell.cell_type(),
        );

        let mut data = Vec::with_capacity(data_size);
        data.extend_from_slice(cell.raw_data()?);

        if !store_hashes {
            if cell.cell_type() != CellType::PrunedBranch {
                let level_mask = cell.level_mask().mask();
                if level_mask != 0 {
                    for i in 0..MAX_LEVEL {
                        if (1 << i) & level_mask != 0 {
                            data.extend_from_slice(cell.hash(i).as_slice());
                            data.extend_from_slice(&cell.depth(i).to_le_bytes());
                        }
                    }
                }
            }
            data.extend_from_slice(&cell.depth(MAX_LEVEL).to_le_bytes());
        }

        for i in 0..cell.references_count() {
            data.extend_from_slice(cell.reference_repr_hash(i)?.as_slice());
            data.extend_from_slice(&cell.reference_repr_depth(i)?.to_le_bytes());
        }

        data.extend_from_slice(&cell.tree_bits_count().to_le_bytes());
        data.extend_from_slice(&cell.tree_cell_count().to_le_bytes());

        Ok(data)
    }

    pub fn with_cell(
        cell: &dyn CellImpl,
        boc_db: &Arc<DynamicBocDb>,
        take_refs: bool,
        use_cache: bool,
    ) -> Result<Self> {
        let references_count = cell.references_count();
        let mut references = Vec::with_capacity(references_count);
        for i in 0..references_count {
            let hash = cell.reference_repr_hash(i)?;
            let depth = cell.reference_repr_depth(i)?;
            if !take_refs {
                log::trace!(target: TARGET, "Cell {:x} - reference [{}] {:x} is not taken", 
                    cell.hash(MAX_LEVEL), i, hash);
                references.push(Reference {hash, depth, cell: None} );
            } else {
                log::trace!(target: TARGET, "Cell {:x} - reference [{}] {:x} is taken", 
                    cell.hash(MAX_LEVEL), i, hash);
                references.push(Reference {
                    hash,
                    depth,
                    cell: Some(Arc::downgrade(&cell.reference(i)?.cell_impl()))
                });
            }
        }
        let mut cell_data = CellData::with_raw_data(cell.raw_data()?.to_vec())?;
        if !cell.store_hashes() {
            let mut hash_index = 0;
            let level_mask = cell.level_mask().mask();
            if level_mask != 0 {
                for i in 0..MAX_LEVEL {
                    if (1 << i) & level_mask != 0 {
                        cell_data.set_hash_depth(hash_index, cell.hash(i).as_slice(), cell.depth(i))?;
                        hash_index += 1;
                    }
                }
            }
            cell_data.set_hash_depth(hash_index, cell.hash(MAX_LEVEL).as_slice(), cell.depth(MAX_LEVEL))?;
        }
        CELL_COUNT.fetch_add(1, Ordering::Relaxed);
        boc_db.allocated().storage_cells.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            cell_data,
            references: parking_lot::RwLock::new(references),
            boc_db: Arc::downgrade(boc_db),
            tree_bits_count: cell.tree_bits_count(),
            tree_cell_count: cell.tree_cell_count(),
            use_cache
        })
    }

    pub(crate) fn reference(&self, index: usize) -> Result<Arc<dyn CellImpl>> {
        let hash = {
            let references = self.references.read();
            let reference = references.get(index).ok_or_else(|| error!("Reference #{index} not found"))?;
            if let Some(weak) = &reference.cell {
                if let Some(cell) = weak.upgrade() {
                    return Ok(cell);
                } else {
                    log::trace!(target: TARGET, "Cell {:x} - reference [{}] {:x} was freed", 
                        self.hash(MAX_LEVEL), index, reference.hash);
                }
            } else {
                log::trace!(target: TARGET, "Cell {:x} - reference [{}] {:x} is None", 
                    self.hash(MAX_LEVEL), index, reference.hash);
            }
            reference.hash.clone()
        };

        let boc_db = self.boc_db.upgrade().ok_or_else(|| error!("BocDb is dropped"))?;
        let cell = boc_db.load_cell(
            &hash,
            self.use_cache,
            true
        )?;

        if self.use_cache {
            self.references.write()[index].cell = 
                Some(Arc::downgrade(cell.cell_impl()) as Weak<dyn CellImpl>);
        }

        Ok(cell.cell_impl().clone())
    }

}

impl CellImpl for StorageCell {
    fn data(&self) -> &[u8] {
        self.cell_data.data()
    }

    fn raw_data(&self) -> Result<&[u8]> {
        Ok(self.cell_data.raw_data())
    }

    fn bit_length(&self) -> usize {
        self.cell_data.bit_length()
    }

    fn references_count(&self) -> usize {
        self.cell_data.references_count()
    }

    fn reference(&self, index: usize) -> Result<Cell> {
        Ok(Cell::with_cell_impl_arc(self.reference(index)?))
    }

    fn reference_repr_hash(&self, index: usize) -> Result<UInt256> {
        Ok(self.references.read()
            .get(index).ok_or_else(|| error!("There is no reference #{}", index))?
            .hash.clone()
        )
    }

    fn reference_repr_depth(&self, index: usize) -> Result<u16> {
        let guard = self.references.read();
        let r = guard.get(index).ok_or_else(|| error!("There is no reference #{}", index))?;

        if r.depth != NOT_INITIALIZED_DEPTH {
            Ok(r.depth)
        } else {
            drop(guard);
            let cell = self.reference(index)?;
            let depth = cell.depth(MAX_LEVEL);
            self.references.write()[index].depth = depth;
            Ok(depth)
        }
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
        self.tree_cell_count
    }
}

impl Drop for StorageCell {
    fn drop(&mut self) {
        CELL_COUNT.fetch_sub(1, Ordering::Relaxed);
        if let Some(boc_db) = self.boc_db.upgrade() {
            boc_db.allocated().storage_cells.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl PartialEq for StorageCell {
    fn eq(&self, other: &Self) -> bool {
        self.cell_data.raw_hash(MAX_LEVEL) == other.cell_data.raw_hash(MAX_LEVEL)
    }
}