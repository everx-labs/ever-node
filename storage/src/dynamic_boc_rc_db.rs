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

use crate::{
    StorageAlloc, cell_db::CellDb, db_impl_base, types::{StorageCell}, TARGET,
    db::{traits::{KvcTransactional, U32Key}, rocksdb::RocksDb},
};
#[cfg(feature = "telemetry")]
use crate::StorageTelemetry;
#[cfg(all(test, feature = "telemetry"))]
use crate::tests::utils::create_storage_telemetry;
use std::{ops::{Deref, DerefMut}, sync::{Arc, RwLock, Weak}, io::Cursor};
#[cfg(feature = "telemetry")]
use std::{
    sync::atomic::Ordering
};
//#[cfg(test)]
//use std::path::Path;
use ton_types::{
    Cell, Result, CellImpl, MAX_LEVEL, fail, DoneCellsStorage, UInt256, OrderedCellsStorage,
    ByteOrderRead,
};

//#[derive(Debug)]
pub struct DynamicBocDb {
    db: Arc<CellDb>,
    cells: Arc<RwLock<fnv::FnvHashMap<UInt256, Weak<StorageCell>>>>,
    #[cfg(feature = "telemetry")]
    telemetry: Arc<StorageTelemetry>,
    allocated: Arc<StorageAlloc>
}

enum VisitedCell {
    New {
        cell: Cell,
        parents_count: u32,
    },
    Updated(StorageCell)
}

impl VisitedCell {
    fn with_storage_cell(cell: StorageCell) -> Self {
        Self::Updated(cell)
    }

    fn with_new_cell(cell: Cell) -> Self {
        Self::New{
            cell,
            parents_count: 1
        }
    }

    fn inc_parents_count(&mut self) -> Result<u32> {
        match self {
            VisitedCell::New{parents_count, ..} => {
                if *parents_count == u32::MAX {
                    fail!("Parents count has reached the maximum value");
                }
                *parents_count += 1;
                Ok(*parents_count)
            }
            VisitedCell::Updated(sc) => sc.inc_parents_count()
        }
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        match self {
            VisitedCell::Updated(cell) => {
                cell.serialize_self()
            },
            VisitedCell::New{cell, parents_count} => {
                StorageCell::serialize(cell.deref(), *parents_count)
            }
        }
    }
}

impl DynamicBocDb {

    pub(crate) fn with_db(
        db: Arc<CellDb>, 
        #[cfg(feature = "telemetry")]
        telemetry: Arc<StorageTelemetry>,
        allocated: Arc<StorageAlloc>
    ) -> Self {
        Self {
            db: Arc::clone(&db),
            cells: Arc::new(RwLock::new(fnv::FnvHashMap::default())),
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated
        }
    }

    pub fn cell_db(&self) -> &Arc<CellDb> {
        &self.db
    }

    pub fn cells_map(&self) -> Arc<RwLock<fnv::FnvHashMap<UInt256, Weak<StorageCell>>>> {
        Arc::clone(&self.cells)
    }

    // Is not thread-safe!
    pub fn save_boc(
        self: &Arc<Self>,
        root_cell: Cell,
        is_state_root: bool,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
    ) -> Result<()> {
        let root_id = root_cell.hash(MAX_LEVEL);
        log::debug!(target: TARGET, "DynamicBocDb::save_boc  {:x}", root_id);

        if is_state_root && self.db.contains(&root_id)? {
            log::warn!(target: TARGET, "DynamicBocDb::save_boc  ALREADY EXISTS  {}", root_id);
            return Ok(());
        }

        let now = std::time::Instant::now();
        let mut visited = fnv::FnvHashMap::default();
        self.save_cells_recursive(
            root_cell,
            &mut visited,
            &root_id,
            check_stop
        )?;

        log::debug!(
            target: TARGET, 
            "DynamicBocDb::save_boc  {:x}  save_cells_recursive TIME {}", root_id,
            now.elapsed().as_millis()
        );

        let now = std::time::Instant::now();
        let mut transaction = self.db.begin_transaction()?;
        for (id, cell) in visited.iter() {
            transaction.put(id, &cell.serialize()?)?;
        }
        log::debug!(
            target: TARGET, 
            "DynamicBocDb::save_boc  {:x}  transaction build TIME {}", root_id,
            now.elapsed().as_millis()
        );

        let now = std::time::Instant::now();
        transaction.commit()?;
        log::debug!(
            target: TARGET, 
            "DynamicBocDb::save_boc  {:x}  transaction commit TIME {}", root_id,
            now.elapsed().as_millis()
        );



        log::debug!(target: TARGET, "DynamicBocDb::save_boc  {:x}  saved {}", root_id, visited.len());

        Ok(())
    }

    // Is thread-safe
    pub fn load_boc(self: &Arc<Self>, root_cell_id: &UInt256, use_cache: bool) -> Result<Cell> {
        let storage_cell = self.load_cell(root_cell_id, use_cache)?;

        Ok(Cell::with_cell_impl_arc(storage_cell))
    }

    // Is not thread-safe!
    pub fn delete_boc(
        self: &Arc<Self>,
        root_cell_id: &UInt256,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
    ) -> Result<()> {
        log::debug!(target: TARGET, "DynamicBocDb::delete_boc  {}", root_cell_id);
        let mut visited = fnv::FnvHashMap::default();
        self.delete_cells_recursive(
            root_cell_id,
            &mut visited,
            root_cell_id,
            check_stop,
        )?;

        let mut deleted = 0;
        let mut transaction = self.db.begin_transaction()?;
        for (id, cell) in visited.iter() {
            if cell.parents_count() == 0 {
                transaction.delete(id)?;
                deleted += 1;
            } else {
                transaction.put(id, &StorageCell::serialize(cell, cell.parents_count())?)?;
            }
        }
        transaction.commit()?;

        log::debug!(
            target: TARGET,
            "DynamicBocDb::delete_boc  {}  deleted {}  updated {}",
            root_cell_id, deleted, visited.len() - deleted
        );
        Ok(())
    }

    pub(crate) fn load_cell(self: &Arc<Self>, cell_id: &UInt256, use_cache: bool) -> Result<Arc<StorageCell>> {
        let in_cache = use_cache && if let Some(cell) = self.cells.read()
            .expect("Poisoned RwLock")
            .get(cell_id)
        {
            if let Some(cell) = Weak::upgrade(&cell) {
                log::trace!(
                    target: TARGET, 
                    "DynamicBocDb::load_cell  from cache  id {:x}",
                    cell_id,
                );
                return Ok(cell);
            }
            // Even if the cell is disposed, we will load and store it later,
            // so we don't need to remove garbage here.
            true
        } else {
            false
        };
        let storage_cell = Arc::new(
            match CellDb::get_cell(self.db.deref(), cell_id, Arc::clone(self), use_cache) {
                Ok(cell) => cell,
                Err(e) => {
                    fail!("Can't load cell {:x} from db, error: {}", cell_id, e);
                    // log::error!("FATAL! Can't load cell {:x} from db, error: {}", cell_id, e);
                    // std::thread::sleep(time::Duration::from_millis(2_000));
                    // std::process::exit(0xFF);
                }
            }
        );

        #[cfg(feature = "telemetry")]
        self.telemetry.storage_cells.update(self.allocated.storage_cells.load(Ordering::Relaxed));

        if use_cache {
            self.cells.write()
                .expect("Poisoned RwLock")
                .insert(cell_id.clone(), Arc::downgrade(&storage_cell));
        }

        log::trace!(
            target: TARGET, 
            "DynamicBocDb::load_cell  from DB  id {:x}  in_cache {}  use_cache {}",
            cell_id, in_cache, use_cache
        );

        Ok(storage_cell)
    }

    pub(crate) fn allocated(&self) -> &StorageAlloc {
        &self.allocated
    }

    fn save_cells_recursive(
        self: &Arc<Self>,
        cell: Cell,
        visited: &mut fnv::FnvHashMap<UInt256, VisitedCell>,
        root_id: &UInt256,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
    ) -> Result<()> {
        check_stop()?;
        let cell_id = cell.hash(MAX_LEVEL);

        if let Some(c) = visited.get_mut(&cell_id) {
            // Cell is new or was load from base, so need to inc counter one more time.
            let pc = c.inc_parents_count()?;
            log::trace!(
                target: TARGET,
                "DynamicBocDb::save_cells_recursive  updated cell {:x}  inc counter {}  root_cell_id {:x}",
                cell_id, pc, root_id
            );
            Ok(())
        } else if let Some(mut c) = self.db.try_get_cell(&cell_id, Arc::clone(self), false)? {
            // Cell was load from base first time.
            let pc = c.inc_parents_count()?;
            log::trace!(
                target: TARGET,
                "DynamicBocDb::save_cells_recursive  updated new cell {:x}  inc counter {}  root_cell_id {:x}",
                cell_id, pc, root_id
            );
            visited.insert(cell_id, VisitedCell::with_storage_cell(c));
            Ok(())
        } else {
            // New cell.
            let c = VisitedCell::with_new_cell(cell.clone());
            log::trace!(
                target: TARGET,
                "DynamicBocDb::save_cells_recursive  added new cell {:x}  root_cell_id {:x}",
                cell_id, root_id
            );
            visited.insert(cell_id, c);
            for i in 0..cell.references_count() {
                self.save_cells_recursive(
                    cell.reference(i)?,
                    visited,
                    root_id,
                    check_stop
                )?;
            }
            Ok(())
        }
    }

    fn delete_cells_recursive(
        self: &Arc<Self>,
        cell_id: &UInt256,
        visited: &mut fnv::FnvHashMap<UInt256, StorageCell>,
        root_id: &UInt256,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
    ) -> Result<()> {
        check_stop()?;

        let cell = loop {
            if let Some(c) = visited.get_mut(cell_id) {
                break c;
            } else {
                match self.db.get_cell(&cell_id, Arc::clone(self), false) {
                    Ok(c) => {
                        // This is a nightly-only experimental API. Uncomment and delete loop when will stable.
                        //visited.try_insert(cell_id.clone(), c)
                        //    .map_err(|_| error!("Internal errror in DynamicBocDb::delete_cells_recursive"))?
                        visited.insert(cell_id.clone(), c);
                        continue;
                    },
                    Err(e) => {
                        log::warn!(
                            "DynamicBocDb::delete_cells_recursive  unknown cell with id {:x}  {}", 
                            cell_id, e
                        );
                        return Ok(())
                    }
                }
            };
        };

        let parents_count = cell.dec_parents_count()?;

        log::trace!(
            target: TARGET,
            "DynamicBocDb::delete_cells_recursive  update cell {:x}  parents_count {}  root_cell_id {:x}",
            cell_id, parents_count, root_id
        );

        if parents_count == 0 {
            let references_count = cell.references_count();
            let mut references = Vec::with_capacity(references_count);
            for i in 0..cell.references_count() {
                references.push(cell.reference_id(i));
            }
            for r in references {
                self.delete_cells_recursive(
                    &r,
                    visited,
                    root_id,
                    check_stop
                )?;
            }
        }

        Ok(())
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

db_impl_base!(IndexedUint256Db, KvcTransactional, U32Key);

pub struct DoneCellsStorageAdapter {
    boc_db: Arc<DynamicBocDb>,
    index: IndexedUint256Db, // index in boc (u32) -> cell id (u256)
}

impl DoneCellsStorageAdapter {
    pub fn new(
        db: Arc<RocksDb>,
        boc_db: Arc<DynamicBocDb>,
        index_db_path: impl ToString,
    ) -> Result<Self> {
        let path = index_db_path.to_string();
        let _ = db.drop_table_force(&path);
        Ok(Self {
            boc_db,
            index: IndexedUint256Db::with_db(db.clone(), index_db_path, true)?,
        })
    }
}

impl DoneCellsStorage for DoneCellsStorageAdapter {
    fn insert(&mut self, index: u32, cell: Cell) -> Result<()> {
        self.index.put(&index.into(), cell.repr_hash().as_slice())?;
        self.boc_db.clone().save_boc(cell, false, &|| Ok(()))?;
        Ok(())
    }

    fn get(&self, index: u32) -> Result<Cell> {
        let id = UInt256::from_slice(self.index.get(&index.into())?.as_ref()).into();
        Ok(Cell::with_cell_impl_arc(self.boc_db.clone().load_cell(&id, false)?))
    }

    fn cleanup(&mut self) -> Result<()> {
        self.index.destroy()?;
        Ok(())
    }
}

db_impl_base!(IndexedUint32Db, KvcTransactional, UInt256);

// This is adapter for DynamicBocDb wich allows to use it as OrderedCellsStorage 
// while serialising BOC. All cells sent to 'push_cell' should be already saved into DynamicBocDb!
pub struct OrderedCellsStorageAdapter {
    boc_db: Arc<DynamicBocDb>,
    index1: IndexedUint256Db, // reverted index in boc (u32) -> cell id (u256)
    index2: IndexedUint32Db, // cell id (u256) -> reverted index in boc (u32)
    cells_count: u32,
}

impl OrderedCellsStorageAdapter {
    pub fn new(
        db: Arc<RocksDb>,
        boc_db: Arc<DynamicBocDb>,
        index_db_path: impl ToString,
    ) -> Result<Self> {
        let index_db_path = index_db_path.to_string();
        let path1 = format!("{}_1", index_db_path);
        let path2 = format!("{}_2", index_db_path);
        let _ = db.drop_table_force(&path1);
        let _ = db.drop_table_force(&path2);
        Ok(Self {
            boc_db,
            index1: IndexedUint256Db::with_db(db.clone(), path1, true)?,
            index2: IndexedUint32Db::with_db(db.clone(), path2, true)?,
            cells_count: 0,
        })
    }
}

impl OrderedCellsStorage for OrderedCellsStorageAdapter {
    // All cells sent to 'store_cell' should be already saved into DynamicBocDb! The method doesn't
    // check it because of performance requirements
    fn store_cell(&mut self, _cell: Cell) -> Result<()> {
        Ok(())
    }

    fn push_cell(&mut self, hash: &UInt256) -> Result<()> {
        let index = self.cells_count;
        self.index1.put(&index.into(), hash.as_slice())?;
        self.index2.put(&hash, &index.to_le_bytes())?;
        self.cells_count += 1;
        Ok(())
    }

    fn get_cell_by_index(&self, index: u32) -> Result<Cell> {
        let id = UInt256::from_slice(self.index1.get(&index.into())?.as_ref()).into();
        Ok(Cell::with_cell_impl_arc(self.boc_db.clone().load_cell(&id, false)?))
    }
    fn get_rev_index_by_hash(&self, hash: &UInt256) -> Result<u32> {
        let data = self.index2.get(hash)?;
        let mut reader = Cursor::new(&data);
        let index = reader.read_le_u32()?;
        Ok(index)
    }
    fn contains_hash(&self, hash: &UInt256) -> Result<bool> {
        self.index2.contains(hash)
    }
    fn cleanup(&mut self) -> Result<()> {
        self.index1.destroy()?;
        self.index2.destroy()?;
        Ok(())
    }
}