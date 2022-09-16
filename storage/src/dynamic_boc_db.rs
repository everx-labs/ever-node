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
    StorageAlloc, cell_db::CellDb, 
    /*dynamic_boc_diff_writer::{DynamicBocDiffFactory, DynamicBocDiffWriter},*/
    types::{StorageCell}, db::traits::KvcTransaction, TARGET,
};
#[cfg(feature = "telemetry")]
use crate::StorageTelemetry;
#[cfg(test)]
use crate::tests::utils::create_storage_allocated;
#[cfg(all(test, feature = "telemetry"))]
use crate::tests::utils::create_storage_telemetry;
use std::{ops::{Deref, DerefMut}, sync::{Arc, RwLock, Weak}, time};
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;
//#[cfg(test)]
//use std::path::Path;
use ton_types::{Cell, Result, CellImpl, MAX_LEVEL, fail, error, UInt256};

//#[derive(Debug)]
pub struct DynamicBocDb {
    db: Arc<CellDb>,
    another_db: Option<Arc<CellDb>>,
    cells: Arc<RwLock<fnv::FnvHashMap<UInt256, Weak<StorageCell>>>>,
    db_index: u32,
    #[cfg(feature = "telemetry")]
    telemetry: Arc<StorageTelemetry>,
    allocated: Arc<StorageAlloc> 
}

impl DynamicBocDb {

    /// Constructs new instance using in-memory key-value collection
    #[cfg(test)]
    pub fn in_memory() -> Self {
        Self::with_db(
            Arc::new(CellDb::in_memory()),
            0,
            None,
            #[cfg(feature = "telemetry")]
            create_storage_telemetry(),
            create_storage_allocated()
        )
    }

/*
    /// Constructs new instance using RocksDB with given path
    #[cfg(test)]
    pub fn with_path<P: AsRef<Path>>(path: P) -> Self {
        Self::with_db(CellDb::with_path(path))
    }
*/

    /// Constructs new instance using given key-value collection implementation
    pub(crate) fn with_db(
        db: Arc<CellDb>, 
        db_index: u32,
        another_db: Option<Arc<CellDb>>,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<StorageTelemetry>,
        allocated: Arc<StorageAlloc>
    ) -> Self {
        Self {
            db: Arc::clone(&db),
            cells: Arc::new(RwLock::new(fnv::FnvHashMap::default())),
            db_index,
            another_db,
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

    pub fn db_index(&self) -> u32 {
        self.db_index
    }

    /// Converts tree of cells into DynamicBoc
    pub fn save_as_dynamic_boc(
        self: &Arc<Self>,
        root_cell: &dyn CellImpl,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
    ) -> Result<usize> {
        let mut transaction = self.db.begin_transaction()?;
        let mut visited = fnv::FnvHashSet::default();
        let root_id = root_cell.hash(MAX_LEVEL);

        let written_count = self.save_tree_of_cells_recursive(
            root_cell,
            transaction.as_mut(),
            &mut visited,
            &root_id,
            check_stop
        )?;

        transaction.commit()?;
        for h in visited {
            log::trace!(target: TARGET, "DynamicBocDb::save_as_dynamic_boc  id {:x}  root_cell_id {:x}  db_index {}",
                h, root_id, self.db_index);
        }

        Ok(written_count)
    }

    /// Gets root cell from key-value storage
    pub fn load_dynamic_boc(self: &Arc<Self>, root_cell_id: &UInt256) -> Result<Cell> {
        let storage_cell = self.load_cell(root_cell_id)?;

        Ok(Cell::with_cell_impl_arc(storage_cell))
    }

    pub(crate) fn load_cell(self: &Arc<Self>, cell_id: &UInt256) -> Result<Arc<StorageCell>> {
        let in_cache = if let Some(cell) = self.cells.read()
            .expect("Poisoned RwLock")
            .get(cell_id)
        {
            if let Some(cell) = Weak::upgrade(cell) {
                log::trace!(
                    target: TARGET, 
                    "DynamicBocDb::load_cell  from cache  id {:x}  db_index {}",
                    cell_id, self.db_index
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
            match self.load_cell_from_db(cell_id) {
                Ok(cell) => cell,
                Err(e) => {
                    log::error!("FATAL! {}", e);
                    std::thread::sleep(time::Duration::from_millis(2_000));
                    std::process::exit(0xFF);
                }
            }
        );
        #[cfg(feature = "telemetry")]
        self.telemetry.storage_cells.update(self.allocated.storage_cells.load(Ordering::Relaxed));
        self.cells.write()
            .expect("Poisoned RwLock")
            .insert(cell_id.clone(), Arc::downgrade(&storage_cell));

        log::trace!(target: TARGET, "DynamicBocDb::load_cell  from DB  id {:x}  db_index {}  in_cache {}",
            cell_id, self.db_index, in_cache);

        Ok(storage_cell)
    }

    pub(crate) fn allocated(&self) -> &StorageAlloc {
        &self.allocated
    }

    fn load_cell_from_db(self: &Arc<Self>, cell_id: &UInt256) -> Result<StorageCell> {
        match CellDb::get_cell(self.db.deref(), cell_id, Arc::clone(self)) {
            Ok(cell) => Ok(cell),
            Err(e) => {
                if let Some(another_db) = self.another_db.as_ref() {
                    let cell = CellDb::get_cell(another_db.deref(), cell_id, Arc::clone(self))
                        .map_err(|e| error!(
                            "Can't load cell from both dbs  id {:x}  db_index {} in_cache false  error: {}",
                            cell_id, self.db_index, e
                        ))?;

                    // Restore only one cell. If caller requests referenced cell - we will restore it the same way.
                    self.db.put(cell_id, &StorageCell::serialize(&cell)?)?;

                    log::warn!("A cell was restored from another db  id {:x}  db_index {}",
                        cell_id, self.db_index);

                    Ok(cell)
                } else {
                    fail!(
                        "Can't load cell  id {:x}  db_index {}  in_cache false  error: {}",
                        cell_id, self.db_index, e
                    );
                }
            }
        }
    }

    fn save_tree_of_cells_recursive(
        &self,
        cell: &dyn CellImpl,
        transaction: &mut dyn KvcTransaction<UInt256>,
        visited: &mut fnv::FnvHashSet<UInt256>,
        root_id: &UInt256,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
    ) -> Result<usize> {
        check_stop()?;
        let cell_id = cell.hash(MAX_LEVEL);
        if visited.contains(&cell_id) {
            Ok(0)
        } else if self.db.contains(&cell_id)? {
            log::trace!(
                target: TARGET,
                "DynamicBocDb::save_tree_of_cells_recursive  already in DB  id {:x}  root_cell_id {:x}  db_index {}",
                cell_id, root_id, self.db_index);
            Ok(0)
        } else {
            transaction.put(&cell_id, &StorageCell::serialize(cell)?);
            visited.insert(cell_id);

            let mut count = 1;
            for i in 0..cell.references_count() {
                count += self.save_tree_of_cells_recursive(
                    cell.reference(i)?.deref(),
                    transaction,
                    visited,
                    root_id,
                    check_stop
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
