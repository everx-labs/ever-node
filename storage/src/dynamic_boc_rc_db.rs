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
use std::{
    ops::{Deref, DerefMut}, 
    sync::{Arc, RwLock, Weak, atomic::{AtomicU32, Ordering}}, 
    io::Cursor,
    path::Path,
    borrow::Cow,
    time::Duration,
    fs::write,
};
//#[cfg(test)]
//use std::path::Path;
use ton_types::{
    Cell, Result, MAX_LEVEL, fail, DoneCellsStorage, UInt256, OrderedCellsStorage,
    ByteOrderRead, error, CellData,
};

pub const BROKEN_CELL_BEACON_FILE: &str = "ton_node.broken_cell";

//#[derive(Debug)]
pub struct DynamicBocDb {
    db: Arc<CellDb>,
    db_root_path: String,
    assume_old_cells: bool,
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
    Updated {
        cell_id: UInt256,
        parents_count: u32,
    },
    UpdatedOldFormat {
        cell: Cell,
        parents_count: u32,
    },
}

impl VisitedCell {
    fn with_raw_counter(cell_id: UInt256, parents_count: &[u8]) -> Result<Self> {
        let mut reader = Cursor::new(parents_count);
        Ok(Self::Updated {
            cell_id,
            parents_count: reader.read_le_u32()?,
        })
    }

    fn with_counter(cell_id: UInt256, parents_count: u32) -> Self {
        Self::Updated {
            cell_id,
            parents_count,
        }
    }

    fn with_new_cell(cell: Cell) -> Self {
        Self::New{
            cell,
            parents_count: 1
        }
    }

    fn with_old_format_cell(cell: Cell, parents_count: u32) -> Self {
        Self::UpdatedOldFormat{
            cell,
            parents_count
        }
    }

    fn inc_parents_count(&mut self) -> Result<u32> {
        let parents_count = match self {
            VisitedCell::New{parents_count, ..} => parents_count,
            VisitedCell::Updated{parents_count, ..} => parents_count,
            VisitedCell::UpdatedOldFormat{parents_count, ..} => parents_count,
        };
        if *parents_count == u32::MAX {
            fail!("Parents count has reached the maximum value");
        }
        *parents_count += 1;
        Ok(*parents_count)
    }

    fn dec_parents_count(&mut self) -> Result<u32> {
        let parents_count = match self {
            VisitedCell::New{parents_count, ..} => parents_count,
            VisitedCell::Updated{parents_count, ..} => parents_count,
            VisitedCell::UpdatedOldFormat{parents_count, ..} => parents_count,
        };
        if *parents_count == 0 {
            fail!("Can't decrement - parents count is already zero");
        }
        *parents_count -= 1;
        Ok(*parents_count)
    }

    fn parents_count(&self) -> u32 {
        match self {
            VisitedCell::New{parents_count, ..} => *parents_count,
            VisitedCell::Updated{parents_count, ..} => *parents_count,
            VisitedCell::UpdatedOldFormat{parents_count, ..} => *parents_count,
         }
    }

    fn serialize_counter(&self) -> ([u8; 33], [u8; 4]) {
        let (id, counter) = match self {
            VisitedCell::New{cell, parents_count} => {
                (Cow::Owned(cell.repr_hash()), parents_count)
            }
            VisitedCell::Updated{cell_id, parents_count} => {
                (Cow::Borrowed(cell_id), parents_count)
            }
            VisitedCell::UpdatedOldFormat{cell, parents_count} => {
                (Cow::Owned(cell.repr_hash()), parents_count)
            }
        };
        (build_counter_key(id.as_slice()), counter.to_le_bytes())
    }

    fn serialize_cell(&self) -> Result<Option<Vec<u8>>> {
        match self {
            VisitedCell::Updated{..} => Ok(None),
            VisitedCell::New{cell, ..} => {
                let data = StorageCell::serialize(cell.deref())?;
                Ok(Some(data))
            }
            VisitedCell::UpdatedOldFormat{cell, ..} => {
                let data = StorageCell::serialize(cell.deref())?;
                Ok(Some(data))
            }
        }
    }

    fn cell(&self) -> Option<&Cell> {
        match self {
            VisitedCell::New{cell, ..} => Some(cell),
            VisitedCell::Updated{..} => None,
            VisitedCell::UpdatedOldFormat{cell, ..} => Some(cell),
        }
    }
}

fn build_counter_key(cell_id: &[u8]) -> [u8; 33] {
    let mut key = [0_u8; 33];
    key[..32].copy_from_slice(cell_id);
    key[32] = 0;
    key
}

impl DynamicBocDb {

    pub(crate) fn with_db(
        db: Arc<CellDb>,
        db_root_path: &str,
        assume_old_cells: bool,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<StorageTelemetry>,
        allocated: Arc<StorageAlloc>
    ) -> Self {
        Self {
            db: Arc::clone(&db),
            db_root_path: db_root_path.to_string(),
            assume_old_cells,
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
        cells_counters: &mut Option<fnv::FnvHashMap<UInt256, u32>>,
        full_filled_counters: bool,
    ) -> Result<()> {
        let root_id = root_cell.hash(MAX_LEVEL);
        log::debug!(target: TARGET, "DynamicBocDb::save_boc  {:x}", root_id);

        if full_filled_counters && self.assume_old_cells {
            fail!("Full filled counters is not supported with assume_old_cells");
        }

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
            check_stop,
            cells_counters,
            full_filled_counters,
        )?;

        log::debug!(
            target: TARGET, 
            "DynamicBocDb::save_boc  {:x}  save_cells_recursive TIME {}", root_id,
            now.elapsed().as_millis()
        );

        let now = std::time::Instant::now();
        let mut created = 0;
        let mut transaction = self.db.begin_transaction()?;
        for (id, vc) in visited.iter() {
            // cell
            if let Some(data) = vc.serialize_cell()? {
                transaction.put(id, &data)?;
                created += 1;
            }

            // counter
            let (k, v) = vc.serialize_counter();
            transaction.put_raw(&k, &v)?;
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

        let updated = visited.len() - created;
        #[cfg(feature = "telemetry")] {
            self.telemetry.new_cells.update(created as u64);
            self.telemetry.updated_cells.update(updated as u64);
        }

        log::debug!(target: TARGET, "DynamicBocDb::save_boc  {:x}  created {}  updated {}", root_id, created, updated);

        Ok(())
    }

    // Is thread-safe
    pub fn load_boc(self: &Arc<Self>, root_cell_id: &UInt256, use_cache: bool) -> Result<Cell> {
        let storage_cell = self.load_cell(root_cell_id, use_cache)?;

        Ok(Cell::with_cell_impl_arc(storage_cell))
    }

    pub fn check_and_update_cells(&mut self) -> Result<()> {
        log::debug!(
            target: TARGET, 
            "DynamicBocDb::check_and_update_cells  started",
        );

        let mut transaction = self.db.begin_transaction()?;
        let mut total_cells = 0;
        let mut updated_cells = 0;
        let now = std::time::Instant::now();

        self.db.for_each(&mut |key, value| {
            if key.len() == 32 {
                // try to load cell in old format
                let mut reader = Cursor::new(value);
                let counter = reader.read_le_u32()?;
                if let Ok(cell_data) = CellData::deserialize(&mut reader) {
                    let references_count = cell_data.references_count();
                    let tail_len = 32 * references_count +  // references
                                   2 * 8;  // tree_bits_count, tree_cell_count
                    if value.len() - reader.position() as usize == tail_len {
                        // check if there is no counter in new format
                        let counter_key = build_counter_key(key);
                        if self.db.try_get_raw(&counter_key)?.is_none() {
                            // save cell in new format (without counter)
                            transaction.put_raw(key, &value[4..])?;
                            // counter
                            let counter = counter.to_le_bytes();
                            transaction.put_raw(&counter_key, &counter)?;
                            updated_cells += 1;
                        }
                    }
                }
                total_cells += 1;

                if total_cells % 100_000 == 0 {
                    log::info!(
                        target: TARGET, 
                        "DynamicBocDb::check_and_update_cells  processed {}, updated {}",
                        total_cells, updated_cells,
                    );
                }
            }
            Ok(true)
        })?;

        let enum_time = now.elapsed().as_millis();

        transaction.commit()?;
        self.assume_old_cells = false;

        log::info!(
            target: TARGET, 
            "DynamicBocDb::check_and_update_cells  processed {}, updated {}, enum TIME {}, commit TIME {}",
            total_cells, updated_cells, enum_time, now.elapsed().as_millis() - enum_time,
        );

        Ok(())
    }

    pub fn fill_counters(
        self: &Arc<Self>,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
        cells_counters: &mut fnv::FnvHashMap<UInt256, u32>,
    ) -> Result<()> {
        self.db.for_each(&mut |key, value| {
            if key.len() == 33 && key[32] == 0 {
                let cell_id = UInt256::from_slice(&key[0..32]);
                let mut reader = Cursor::new(value);
                let counter = reader.read_le_u32()?;
                cells_counters.insert(cell_id, counter);
            }
            check_stop()?;
            Ok(true)
        })?;
        Ok(())
    }

    // Is not thread-safe!
    pub fn delete_boc(
        self: &Arc<Self>,
        root_cell_id: &UInt256,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
        cells_counters: &mut Option<fnv::FnvHashMap<UInt256, u32>>,
        full_filled_counters: bool,
    ) -> Result<()> {
        log::debug!(target: TARGET, "DynamicBocDb::delete_boc  {:x}", root_cell_id);
        
        if full_filled_counters && self.assume_old_cells {
            fail!("Full filled counters is not supported with assume_old_cells");
        }
        
        let mut visited = fnv::FnvHashMap::default();
        self.delete_cells_recursive(
            root_cell_id,
            &mut visited,
            root_cell_id,
            check_stop,
            cells_counters,
            full_filled_counters,
        )?;

        let mut deleted = 0;
        let mut transaction = self.db.begin_transaction()?;
        for (id, cell) in visited.iter() {
            let counter_key = build_counter_key(id.as_slice());
            let counter = cell.parents_count();
            if counter == 0 {
                transaction.delete(id)?;
                // if there is no counter with the key, then it will be just ignored
                transaction.delete_raw(&counter_key)?;
                deleted += 1;
            } else {
                transaction.put_raw(&counter_key, &counter.to_le_bytes())?;

                // update old format cell
                if let Some(cell) = cell.serialize_cell()? {
                    transaction.put(id, &cell)?;
                }
            }
        }
        transaction.commit()?;

        let updated = visited.len() - deleted;
        #[cfg(feature = "telemetry")] {
            self.telemetry.deleted_cells.update(deleted as u64);
            self.telemetry.updated_cells.update(updated as u64);
        }

        log::debug!(
            target: TARGET,
            "DynamicBocDb::delete_boc  {:x}  deleted {}  updated {}",
            root_cell_id, deleted, updated
        );
        Ok(())
    }

    pub(crate) fn load_cell(
        self: &Arc<Self>,
        cell_id: &UInt256,
        use_cache: bool,
    ) -> Result<Arc<StorageCell>> {
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

        let storage_cell_data = match self.db.get(cell_id) {
            Ok(data) => data,
            Err(e) => {
                log::error!("FATAL!");
                log::error!("FATAL! Can't load cell {:x} from db, error: {:?}", cell_id, e);
                log::error!("FATAL!");
                
                let path = Path::new(&self.db_root_path).join(BROKEN_CELL_BEACON_FILE);
                write(&path, "")?;
                
                std::thread::sleep(Duration::from_millis(100));
                std::process::exit(0xFF);
            }
        };

        let storage_cell = match StorageCell::deserialize(self.clone(), &storage_cell_data, use_cache, false)
            .or_else(|_| StorageCell::deserialize(self.clone(), &storage_cell_data, use_cache, true))
        {
            Ok((cell, _)) => Arc::new(cell),
            Err(e) => {
                log::error!("FATAL!");
                log::error!(
                    "FATAL! Can't deserialize cell {:x} from db, data: {}, error: {:?}",
                    cell_id, hex::encode(storage_cell_data), e
                );
                log::error!("FATAL!");
                
                let path = Path::new(&self.db_root_path).join(BROKEN_CELL_BEACON_FILE);
                write(&path, "")?;
                
                std::thread::sleep(Duration::from_millis(100));
                std::process::exit(0xFF);
            }
        };

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
        cells_counters: &mut Option<fnv::FnvHashMap<UInt256, u32>>,
        full_filled_counters: bool,
    ) -> Result<()> {

        check_stop()?;
        let cell_id = cell.repr_hash();

        let (counter, _cell) = self.load_and_update_cell(
            &cell_id,
            visited,
            root_id,
            cells_counters,
            full_filled_counters,
            |visited_cell| visited_cell.inc_parents_count(),
            "DynamicBocDb::save_cells_recursive"
        )?;
        if counter.is_none() {
            // New cell.
            let c = VisitedCell::with_new_cell(cell.clone());
            visited.insert(cell_id.clone(), c);
            if let Some(counters) = cells_counters.as_mut() {
                counters.insert(cell_id.clone(), 1);
            }
            log::trace!(
                target: TARGET,
                "DynamicBocDb::save_cells_recursive  {:x}  new cell  root_cell_id {:x}",
                cell_id, root_id
            );

            for i in 0..cell.references_count() {
                self.save_cells_recursive(
                    cell.reference(i)?,
                    visited,
                    root_id,
                    check_stop,
                    cells_counters,
                    full_filled_counters,
                )?;
            }
        }

        Ok(())
    }

    fn delete_cells_recursive(
        self: &Arc<Self>,
        cell_id: &UInt256,
        visited: &mut fnv::FnvHashMap<UInt256, VisitedCell>,
        root_id: &UInt256,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
        cells_counters: &mut Option<fnv::FnvHashMap<UInt256, u32>>,
        full_filled_counters: bool,
    ) -> Result<()> {

        check_stop()?;

        if let (Some(counter), cell) = self.load_and_update_cell(
            cell_id,
            visited,
            root_id,
            cells_counters,
            full_filled_counters,
            |visited_cell| visited_cell.dec_parents_count(),
            "DynamicBocDb::delete_cells_recursive",
        )? {
            if counter == 0 {
                if let Some(counters) = cells_counters.as_mut() {
                    counters.remove(cell_id);
                }

                let cell = if let Some(c) = cell {
                    c
                } else {
                    Cell::with_cell_impl(self.db.get_cell(&cell_id, Arc::clone(self), false, false)?.0)
                };

                for i in 0..cell.references_count() {
                    self.delete_cells_recursive(
                        &cell.reference_repr_hash(i)?,
                        visited,
                        root_id,
                        check_stop,
                        cells_counters,
                        full_filled_counters,
                    )?;
                }
            }
        } else {
            log::warn!(
                "DynamicBocDb::delete_cells_recursive  unknown cell with id {:x}  root_cell_id {:x}", 
                cell_id, root_id
            );
        }

        Ok(())
    }

    fn load_and_update_cell(
        self: &Arc<Self>,
        cell_id: &UInt256,
        visited: &mut fnv::FnvHashMap<UInt256, VisitedCell>,
        root_id: &UInt256,
        cells_counters: &mut Option<fnv::FnvHashMap<UInt256, u32>>,
        full_filled_counters: bool,
        update_cell: impl Fn(&mut VisitedCell) -> Result<u32>,
        op_name: &str,
    ) -> Result<(Option<u32>, Option<Cell>)> {

        if let Some(visited_cell) = visited.get_mut(&cell_id) {
            // Cell was already updated while this operation, just update counter
            let new_counter = update_cell(visited_cell)?;
            if let Some(counters) = cells_counters.as_mut() {
                let counter = counters.get_mut(&cell_id).ok_or_else(
                    || error!("INTERNAL ERROR: cell from 'visited' is not presented in `cells_counters`")
                )?;
                *counter = new_counter;
            }
            log::trace!(
                target: TARGET,
                "{}  {:x}  update visited {}  root_cell_id {:x}",
                op_name, cell_id, new_counter, root_id
            );
            return Ok((Some(new_counter), visited_cell.cell().cloned()));
        }

        if let Some(counter) = cells_counters.as_mut().and_then(|cc| cc.get_mut(&cell_id)) {
            // Cell's counter is in cache - update it
            let mut visited_cell = VisitedCell::with_counter(cell_id.clone(), *counter);
            *counter = update_cell(&mut visited_cell)?;
            visited.insert(cell_id.clone(), visited_cell);
            log::trace!(
                target: TARGET,
                "{}  {:x}  update counter {}  root_cell_id {:x}",
                op_name, cell_id, counter, root_id
            );
            #[cfg(feature = "telemetry")]
            self.telemetry.cell_counter_from_cache.update(1);

            return Ok((Some(*counter), None));
        }

        if !full_filled_counters {
            if let Some(counter_raw) = self.db.try_get_raw(&build_counter_key(cell_id.as_slice()))? {
                // Cell's counter is in DB - load it and update
                let mut visited_cell = VisitedCell::with_raw_counter(cell_id.clone(), &counter_raw)?;
                let counter = update_cell(&mut visited_cell)?;
                visited.insert(cell_id.clone(), visited_cell);
                if let Some(counters) = cells_counters.as_mut() {
                    counters.insert(cell_id.clone(), counter);
                }
                log::trace!(
                    target: TARGET,
                    "{}  {:x}  load counter {}  root_cell_id {:x}",
                    op_name, cell_id, counter, root_id
                );
                #[cfg(feature = "telemetry")]
                self.telemetry.cell_counter_from_db.update(1);

                return Ok((Some(counter), None));
            }
        }

        if self.assume_old_cells {
            if let Some((cell, counter)) = self.db.try_get_cell(&cell_id, Arc::clone(self), false, true)? {
                // Old cell without external counter
                let cell = Cell::with_cell_impl(cell);
                let mut visited_cell = VisitedCell::with_old_format_cell(cell.clone(), counter);
                let counter = update_cell(&mut visited_cell)?;
                visited.insert(cell_id.clone(), visited_cell);
                if let Some(counters) = cells_counters.as_mut() {
                    counters.insert(cell_id.clone(), counter);
                }
                log::trace!(
                    target: TARGET,
                    "{}  {:x}  update old cell {}  root_cell_id {:x}",
                    op_name, cell_id, counter, root_id
                );
                #[cfg(feature = "telemetry")]
                self.telemetry.updated_old_cells.update(1);

                return Ok((Some(counter), Some(cell)));
            }
        }

        Ok((None, None))
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
        self.boc_db.clone().save_boc(cell, false, &|| Ok(()), &mut None, false)?;
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
    slowdown_mcs: Arc<AtomicU32>,
    index_db_path: String,
}

impl OrderedCellsStorageAdapter {
    pub fn new(
        db: Arc<RocksDb>,
        boc_db: Arc<DynamicBocDb>,
        index_db_path: impl ToString,
        slowdown_mcs: Arc<AtomicU32>,
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
            slowdown_mcs,
            index_db_path: index_db_path.to_string(),
        })
    }
    fn slowdown(&self) -> u32 {
        let timeout = self.slowdown_mcs.load(Ordering::Relaxed);
        if timeout > 0 {
            std::thread::sleep(Duration::from_micros(timeout as u64));
        }
        timeout
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

        let slowdown = self.slowdown();
        if self.cells_count % 1000 == 0 {
            log::info!(
                "OrderedCellsStorage::push_cell {} cells: {}, slowdown mcs: {}",
                self.index_db_path, self.cells_count, slowdown
            );
        }
        Ok(())
    }

    fn get_cell_by_index(&self, index: u32) -> Result<Cell> {
        let id = UInt256::from_slice(self.index1.get(&index.into())?.as_ref()).into();
        let cell = Cell::with_cell_impl_arc(self.boc_db.clone().load_cell(&id, false)?);

        let slowdown = self.slowdown();
        if index % 1000 == 0 {
            log::info!(
                "OrderedCellsStorage::get_cell_by_index {} index: {}, total cells: {}, slowdown mcs: {}",
                self.index_db_path, index, self.cells_count, slowdown
            );
        }
        Ok(cell)
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