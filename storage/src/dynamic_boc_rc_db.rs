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

use crate::{
    db_impl_base,
    StorageAlloc, types::StorageCell, TARGET,
    db::{U32Key, rocksdb::RocksDb}, shardstate_db_async::ShardStateDb,
};
#[cfg(feature = "telemetry")]
use crate::StorageTelemetry;
use std::{
    fs::write, io::Cursor, mem::size_of, ops::Deref, path::{Path, PathBuf},
    sync::{atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering}, Arc}, time::{Duration, Instant}
};
use ever_block::{
    error, fail, BuilderData, ByteOrderRead, Cell, CellByHashStorage, DoneCellsStorage, 
    OrderedCellsStorage, Result, UInt256, MAX_LEVEL 
};
use ever_block::merkle_update::CellsFactory;

pub const BROKEN_CELL_BEACON_FILE: &str = "ton_node.broken_cell";

// FnvHashMap is a standard HashMap with FNV hasher. This hasher is bit faster than default one.
pub(crate) type CellsCounters = fnv::FnvHashMap<UInt256, u32>;

enum VisitedCell {
    New {
        cell: Cell,
        parents_count: u32,
    },
    Updated {
        parents_count: u32,
    },
}

impl VisitedCell {
    fn with_raw_counter(parents_count: &[u8]) -> Result<Self> {
        let mut reader = Cursor::new(parents_count);
        Ok(Self::Updated {
            parents_count: reader.read_le_u32()?,
        })
    }

    fn with_counter(parents_count: u32) -> Self {
        Self::Updated {
            parents_count,
        }
    }

    fn with_new_cell(cell: Cell) -> Self {
        Self::New{
            cell,
            parents_count: 1
        }
    }

    fn inc_parents_count(&mut self) -> Result<u32> {
        let parents_count = match self {
            VisitedCell::New{parents_count, ..} => parents_count,
            VisitedCell::Updated{parents_count, ..} => parents_count,
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
        }
    }

    fn serialize_counter(&self) -> [u8; 4] {
        self.parents_count().to_le_bytes()
    }

    fn serialize_cell(&self) -> Result<Option<Vec<u8>>> {
        match self {
            VisitedCell::Updated{..} => Ok(None),
            VisitedCell::New{cell, ..} => {
                let data = StorageCell::serialize(cell.deref())?;
                Ok(Some(data))
            }
        }
    }

    fn cell(&self) -> Option<&Cell> {
        match self {
            VisitedCell::New{cell, ..} => Some(cell),
            VisitedCell::Updated{..} => None,
        }
    }
}

pub struct DynamicBocDb {
    db: Arc<RocksDb>,
    cells_cf_name: String,
    counters_cf_name: String,
    db_root_path: PathBuf,
    raw_cells_cache: RawCellsCache,
    storing_cells: Arc<lockfree::map::Map<UInt256, Cell>>,
    storing_cells_count: AtomicU64,
    #[cfg(feature = "telemetry")]
    telemetry: Arc<StorageTelemetry>,
    allocated: Arc<StorageAlloc>
}

impl DynamicBocDb {
    pub(crate) fn with_db(
        db: Arc<RocksDb>,
        cell_db_cf: &str,
        db_root_path: impl AsRef<Path>,
        cache_size_bytes: u64,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<StorageTelemetry>,
        allocated: Arc<StorageAlloc>
    ) -> Result<Self> {
        let raw_cells_cache = RawCellsCache::new(cache_size_bytes);
        if db.cf_handle(cell_db_cf).is_none() {
            db.create_cf(cell_db_cf, &Self::build_cf_options())?;
        }
        let counters_cf_name = format!("{}_counters", cell_db_cf);
        if db.cf_handle(&counters_cf_name).is_none() {
            db.create_cf(&counters_cf_name, &Self::build_cf_options())?;
        }
        Ok(Self {
            db,
            cells_cf_name: cell_db_cf.to_string(),
            counters_cf_name,
            db_root_path: db_root_path.as_ref().to_path_buf(),
            raw_cells_cache,
            storing_cells: Arc::new(lockfree::map::Map::new()),
            storing_cells_count: AtomicU64::new(0),
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated
        })
    }

    fn build_cf_options() -> rocksdb::Options {

        let mut options = rocksdb::Options::default();
        let mut block_opts = rocksdb::BlockBasedOptions::default();

        // specified cache for blocks.
        let cache = rocksdb::Cache::new_lru_cache(1024 * 1024 * 1024);
        block_opts.set_block_cache(&cache);

        // save in LRU block cache also indexes and bloom filters
        block_opts.set_cache_index_and_filter_blocks(true);

        // keep indexes and filters in block cache until tablereader freed
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);

        // Setup bloom filter with length of 10 bits per key.
        // This length provides less than 1% false positive rate.
        block_opts.set_bloom_filter(10.0, false);

        options.set_block_based_table_factory(&block_opts);

        // Enable whole key bloom filter in memtable.
        options.set_memtable_whole_key_filtering(true);

        // Amount of data to build up in memory (backed by an unsorted log
        // on disk) before converting to a sorted on-disk file.
        //
        // Larger values increase performance, especially during bulk loads.
        // Up to max_write_buffer_number write buffers may be held in memory
        // at the same time,
        // so you may wish to adjust this parameter to control memory usage.
        // Also, a larger write buffer will result in a longer recovery time
        // the next time the database is opened.
        options.set_write_buffer_size(1024 * 1024 * 1024);

        // The maximum number of write buffers that are built up in memory.
        // The default and the minimum number is 2, so that when 1 write buffer
        // is being flushed to storage, new writes can continue to the other
        // write buffer.
        // If max_write_buffer_number > 3, writing will be slowed down to
        // options.delayed_write_rate if we are writing to the last write buffer
        // allowed.
        options.set_max_write_buffer_number(4);

        // if prefix_extractor is set and memtable_prefix_bloom_size_ratio is not 0,
        // create prefix bloom for memtable with the size of
        // write_buffer_size * memtable_prefix_bloom_size_ratio.
        // If it is larger than 0.25, it is sanitized to 0.25.
        let transform = rocksdb::SliceTransform::create_fixed_prefix(10);
        options.set_prefix_extractor(transform);
        options.set_memtable_prefix_bloom_ratio(0.1);

        options
    }

    pub async fn update_to_v6(
        &self,
        old_cell_cf_name: &str,
        stop: Arc<AtomicU8>,
    ) -> Result<()> {
        log::info!(target: TARGET, "Started cells DB migration to new format");

        let (tx, rx) = tokio::sync::mpsc::channel(100_000);

        let reader = {
            let db = self.db.clone();
            let old_cell_cf_name = old_cell_cf_name.to_string();
            let stop = stop.clone();
            tokio::task::spawn_blocking(move || -> Result<()> {

                // This worker gets cells and counters from old db and puts them into channel

                let old_cells_cf = db.cf_handle(&old_cell_cf_name)
                    .ok_or_else(|| error!("Can't get `{}` cf handle", old_cell_cf_name))?;
                let now = std::time::Instant::now();
                let mut read = 0;
                for kv in db.iterator_cf(&old_cells_cf, rocksdb::IteratorMode::Start) {

                    if stop.load(Ordering::Relaxed) & ShardStateDb::MASK_STOPPED != 0 {
                        log::warn!(target: TARGET, "Cells DB migration: STOPPED");
                        return Ok(());
                    }

                    let (key, value) = kv?;
                    tx.blocking_send((key, value))?;
                    read += 1;
                    if read % 1_000_000 == 0 {
                        log::info!(
                            target: TARGET,
                            "Cells DB migration: read {} items, speed {} items/sec",
                            read, read / now.elapsed().as_secs()
                        );
                    }
                }

                Ok(())
            })
        };

        #[allow(clippy::type_complexity)]
        async fn writer(
            db: &DynamicBocDb, 
            mut rx: tokio::sync::mpsc::Receiver<(Box<[u8]>, Box<[u8]>)>
        ) -> Result<()> {
            let now = std::time::Instant::now();
            let mut total_cells = 0;
            let mut total_counters = 0;
            let counters_cf = db.counters_cf()?;
            let cells_cf = db.cells_cf()?;
            while let Some((key, value)) = rx.recv().await {
                // save cell or counter
                match key.len() {
                    33 => {
                        // Counter. Save it as is into separated column family
                        db.db.put_cf(&counters_cf, &key[..32], value)?;
                        total_counters += 1;
                    }
                    32 => {
                        // Cell. Transform cell from old format to new
                        let new_value = StorageCell::migrate_to_v6(&value)?;
                        db.db.put_cf(&cells_cf, &key, new_value)?;
                        total_cells += 1;
                    }
                    _ => {
                        log::warn!(
                            target: TARGET,
                            "Cells DB migration: skipped a key with unknown length {} {}",
                            key.len(), hex::encode(&key)
                        );
                    }
                }
                let el = now.elapsed().as_secs();
                if el != 0 && (total_counters % 1_000_000 == 0 || total_cells % 1_000_000 == 0) {
                    log::info!(
                        target: TARGET,
                        "Cells DB migration: processed {} cells and {} counters, speed {} cells/sec, {} counters/sec",
                        total_cells, total_counters, total_cells / el, total_counters / el
                    );
                }
            }

            log::info!(
                target: TARGET,
                "Cells DB migration: processed {} cells and {} counters, TIME {} sec",
                total_cells, total_counters, now.elapsed().as_secs()
            );
            Ok(())
        }

        let (r1, r2) = tokio::join!(reader, writer(self, rx));
        r1??;
        r2?;

        self.db.drop_cf(old_cell_cf_name)?;

        log::info!(target: TARGET, "Cells DB migration to new format is finished");

        Ok(())
    }

    pub fn cells_cache_len(&self) -> usize {
        self.raw_cells_cache.0.len()
    }

    #[cfg(test)]
    pub fn count(&self) -> usize {
        if let Ok(cf) = self.counters_cf() {
            self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start).count()
        } else {
            0
        }
    }

    // Is not thread-safe!
    pub fn save_boc(
        self: &Arc<Self>,
        root_cell: Cell,
        is_state_root: bool,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
        cells_counters: &mut Option<CellsCounters>,
        full_filled_counters: bool,
    ) -> Result<Cell> {
        let root_id = root_cell.hash(MAX_LEVEL);
        log::debug!(target: TARGET, "DynamicBocDb::save_boc  {:x}", root_id);

        let cells_cf = self.cells_cf()?;

        if is_state_root {
            if let Some(val) = self.db.get_pinned_cf(&cells_cf, root_id.as_slice())? {
                log::warn!(target: TARGET, "DynamicBocDb::save_boc  ALREADY EXISTS  {}", root_id);
                let cell = StorageCell::deserialize(self, &root_id, &val, true)?;
                #[cfg(feature = "telemetry")]
                self.telemetry.storage_cells.update(self.allocated.storage_cells.load(Ordering::Relaxed));
                return Ok(Cell::with_cell_impl(cell))
            }
        }

        let now = std::time::Instant::now();
        let counters_cf = self.counters_cf()?;
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

        let now2 = std::time::Instant::now();
        let mut created = 0;
        let mut transaction = rocksdb::WriteBatch::default();
        for (id, vc) in visited.iter() {
            // cell
            if let Some(data) = vc.serialize_cell()? {
                transaction.put_cf(&cells_cf, id.as_slice(), &data);
                created += 1;
            }

            // counter
            transaction.put_cf(&counters_cf, id.as_slice(), vc.serialize_counter());
        }
        log::debug!(
            target: TARGET,
            "DynamicBocDb::save_boc  {:x}  transaction build TIME {}", root_id,
            now2.elapsed().as_millis()
        );

        let now3 = Instant::now();
        self.db.write(transaction)?;
        #[cfg(feature = "telemetry")]
        self.telemetry.boc_db_element_write_nanos.update(
            now.elapsed().as_nanos() as u64 / (visited.len() as u64 + created as u64));

        log::debug!(
            target: TARGET,
            "DynamicBocDb::save_boc  {:x}  transaction commit TIME {}", root_id,
            now3.elapsed().as_millis()
        );

        for (id, _) in visited.iter() {
            if self.storing_cells.remove(id).is_some() {
                log::trace!(
                    target: TARGET,
                    "DynamicBocDb::save_boc  {:x}  cell removed from storing_cells", id
                );
                let _storing_cells_count = self.storing_cells_count.fetch_sub(1, Ordering::Relaxed);
                #[cfg(feature = "telemetry")]
                self.telemetry.storing_cells.update(_storing_cells_count - 1);
            }
        }

        let saved_root = if let Some(c) = visited.get(&root_id).and_then(|vc| vc.cell()) {
            c.clone()
        } else {
            // only if the root cell was already saved (just updated counter) - we need to load it here
            self.load_boc(&root_id, true)?
        };

        let updated = visited.len() - created;
        #[cfg(feature = "telemetry")] {
            self.telemetry.new_cells.update(created as u64);
            self.telemetry.updated_cells_speed.update(updated as u64);
        }

        log::debug!(target: TARGET, "DynamicBocDb::save_boc  {:x}  created {}  updated {}", root_id, created, updated);

        Ok(saved_root)
    }

    // Is thread-safe
    pub fn load_boc(self: &Arc<Self>, root_cell_id: &UInt256, use_cache: bool) -> Result<Cell> {
        self.load_cell(root_cell_id, use_cache, true)
    }

    pub fn fill_counters(
        self: &Arc<Self>,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
        cells_counters: &mut CellsCounters,
    ) -> Result<()> {
        let counters_cf = self.counters_cf()?;
        for kv in self.db.iterator_cf(&counters_cf, rocksdb::IteratorMode::Start) {
            let (key, value) = kv?;
            if key.len() == 33 && key[32] == 0 {
                let cell_id = UInt256::from_slice(&key[0..32]);
                let mut reader = Cursor::new(value);
                let counter = reader.read_le_u32()?;
                cells_counters.insert(cell_id, counter);
                if cells_counters.len() % 1_000_000 == 0 {
                    log::info!(
                        target: TARGET,
                        "DynamicBocDb::fill_counters  processed {}",
                        cells_counters.len(),
                    );
                }
            }
            check_stop()?;
        }
        Ok(())
    }

    // Is not thread-safe!
    pub fn delete_boc(
        self: &Arc<Self>,
        root_cell_id: &UInt256,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
        cells_counters: &mut Option<CellsCounters>,
        full_filled_counters: bool,
    ) -> Result<()> {
        log::debug!(target: TARGET, "DynamicBocDb::delete_boc  {:x}", root_cell_id);

        #[cfg(feature = "telemetry")] 
        let now = Instant::now();
        let mut visited = fnv::FnvHashMap::default();
        self.delete_cells_recursive(
            root_cell_id,
            &mut visited,
            root_cell_id,
            check_stop,
            cells_counters,
            full_filled_counters,
        )?;

        let cells_cf = self.cells_cf()?;
        let counters_cf = self.counters_cf()?;
        let mut deleted = 0;
        let mut transaction = rocksdb::WriteBatch::default();
        for (id, cell) in visited.iter() {
            let counter = cell.parents_count();
            if counter == 0 {
                transaction.delete_cf(&cells_cf, id.as_slice());
                // if there is no counter with the key, then it will be just ignored
                transaction.delete_cf(&counters_cf, id.as_slice());
                deleted += 1;
            } else {
                transaction.put_cf(&counters_cf, id.as_slice(), counter.to_le_bytes());

                // update old format cell
                if let Some(cell) = cell.serialize_cell()? {
                    transaction.put(id, &cell);
                }
            }
        }

        self.db.write(transaction)?;

        let updated = visited.len() - deleted;
        #[cfg(feature = "telemetry")] {
            self.telemetry.deleted_cells_speed.update(deleted as u64);
            self.telemetry.updated_cells_speed.update(updated as u64);
            self.telemetry.boc_db_element_write_nanos.update(
                now.elapsed().as_nanos() as u64 / (visited.len() as u64 + deleted as u64));
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
        fill_cache: bool,
        panic: bool,
    ) -> Result<Cell> {
        #[cfg(feature = "telemetry")]
        let now = Instant::now();
        if let Some(data) = self.raw_cells_cache.0.get(cell_id) {
            if let Ok(cell) = StorageCell::deserialize(self, cell_id, &data, fill_cache) {
                log::trace!(
                    target: TARGET, 
                    "DynamicBocDb::load_cell from cache id {cell_id:x}"
                );
                #[cfg(feature = "telemetry")] {
                    self.telemetry.storage_cells.update(
                        self.allocated.storage_cells.load(Ordering::Relaxed));
                    self.telemetry.cell_loading_from_cache_nanos.update(
                        now.elapsed().as_nanos() as u64);
                    self.telemetry.cells_loaded_from_cache.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(Cell::with_cell_impl(cell))
            }
        }

        #[cfg(feature = "telemetry")]
        let now = Instant::now();
        let storage_cell_data = match self.db.get_pinned_cf(&self.cells_cf()?, cell_id.as_slice()) {
            Ok(Some(data)) => data,
            _ => {

                if let Some(guard) = self.storing_cells.get(cell_id) {
                    log::error!(
                        target: TARGET,
                        "DynamicBocDb::load_cell from storing_cells by id {cell_id:x}",
                    );
                    return Ok(guard.val().clone());
                }

                if !panic {
                    fail!("Can't load cell {:x} from db", cell_id);
                }

                log::error!("FATAL!");
                log::error!("FATAL! Can't load cell {:x} from db", cell_id);
                log::error!("FATAL!");

                let path = Path::new(&self.db_root_path).join(BROKEN_CELL_BEACON_FILE);
                write(path, "")?;

                std::thread::sleep(Duration::from_millis(100));
                std::process::exit(0xFF);
            }
        };

        if fill_cache {
            self.raw_cells_cache.0.insert(cell_id.clone(), bytes::Bytes::copy_from_slice(&storage_cell_data));
        }

        let storage_cell = match StorageCell::deserialize(self, cell_id, &storage_cell_data, fill_cache) {
            Ok(cell) => Arc::new(cell),
            Err(e) => {

                if !panic {
                    fail!("Can't deserialize cell {:x} from db, error: {:?}", cell_id, e);
                }

                log::error!("FATAL!");
                log::error!(
                    "FATAL! Can't deserialize cell {:x} from db, data: {}, error: {:?}",
                    cell_id, hex::encode(&storage_cell_data), e
                );
                log::error!("FATAL!");

                let path = Path::new(&self.db_root_path).join(BROKEN_CELL_BEACON_FILE);
                write(path, "")?;

                std::thread::sleep(Duration::from_millis(100));
                std::process::exit(0xFF);
            }
        };

        #[cfg(feature = "telemetry")] {
            self.telemetry.storage_cells.update(self.allocated.storage_cells.load(Ordering::Relaxed));
            self.telemetry.cell_loading_from_db_nanos.update(now.elapsed().as_nanos() as u64);
            self.telemetry.cells_loaded_from_db.fetch_add(1, Ordering::Relaxed);
        }

        log::trace!(
            target: TARGET,
            "DynamicBocDb::load_cell from DB id {cell_id:x} fill_cache {fill_cache}"
        );

        Ok(Cell::with_cell_impl_arc(storage_cell))
    }

    pub(crate) fn allocated(&self) -> &StorageAlloc {
        &self.allocated
    }

    fn cells_cf(&self) -> Result<Arc<rocksdb::BoundColumnFamily>> {
        self.db.cf_handle(&self.cells_cf_name)
            .ok_or_else(|| error!("Can't get `{}` cf handle", self.cells_cf_name))
    }

    fn counters_cf(&self) -> Result<Arc<rocksdb::BoundColumnFamily>> {
        self.db.cf_handle(&self.counters_cf_name)
            .ok_or_else(|| error!("Can't get `{}` cf handle", self.counters_cf_name))
    }

    fn save_cells_recursive(
        self: &Arc<Self>,
        cell: Cell,
        visited: &mut fnv::FnvHashMap<UInt256, VisitedCell>,
        root_id: &UInt256,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
        cells_counters: &mut Option<CellsCounters>,
        full_filled_counters: bool,
    ) -> Result<()> {

        let counters_cf = self.counters_cf()?;
        let mut stack = vec![cell.clone()];

        while let Some(cell) = stack.pop() {

            check_stop()?;
            let cell_id = cell.repr_hash();

            let (counter, _cell) = self.load_and_update_cell(
                &counters_cf,
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
                    stack.push(cell.reference(i)?);
                }
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
        cells_counters: &mut Option<CellsCounters>,
        full_filled_counters: bool,
    ) -> Result<()> {

        let counters_cf = self.counters_cf()?;
        let mut stack = vec![cell_id.clone()];
        while let Some(cell_id) = stack.pop() {

            check_stop()?;

            if let (Some(counter), cell) = self.load_and_update_cell(
                &counters_cf,
                &cell_id,
                visited,
                root_id,
                cells_counters,
                full_filled_counters,
                |visited_cell| visited_cell.dec_parents_count(),
                "DynamicBocDb::delete_cells_recursive",
            )? {
                if counter == 0 {
                    if let Some(counters) = cells_counters.as_mut() {
                        counters.remove(&cell_id);
                    }

                    let cell = if let Some(c) = cell {
                        c
                    } else {
                        match self.load_cell(&cell_id, true, false) {
                            Ok(cell) => cell,
                            Err(e) => {
                                log::warn!("DynamicBocDb::delete_cells_recursive  {:?}", e
                                );
                                continue;
                            }
                        }
                    };

                    for i in 0..cell.references_count() {
                        stack.push(cell.reference_repr_hash(i)?);
                    }
                }
            } else {
                log::warn!(
                    "DynamicBocDb::delete_cells_recursive  unknown cell with id {:x}  root_cell_id {:x}",
                    cell_id, root_id
                );
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn load_and_update_cell(
        self: &Arc<Self>,
        counters_cf: &impl rocksdb::AsColumnFamilyRef,
        cell_id: &UInt256,
        visited: &mut fnv::FnvHashMap<UInt256, VisitedCell>,
        root_id: &UInt256,
        cells_counters: &mut Option<CellsCounters>,
        full_filled_counters: bool,
        update_cell: impl Fn(&mut VisitedCell) -> Result<u32>,
        op_name: &str,
    ) -> Result<(Option<u32>, Option<Cell>)> {
        #[cfg(feature = "telemetry")]
        let now = Instant::now();
        if let Some(visited_cell) = visited.get_mut(cell_id) {
            // Cell was already updated while this operation, just update counter
            let new_counter = update_cell(visited_cell)?;
            if let Some(counters) = cells_counters.as_mut() {
                let counter = counters.get_mut(cell_id).ok_or_else(
                    || error!("INTERNAL ERROR: cell from 'visited' is not presented in `cells_counters`")
                )?;
                *counter = new_counter;
                #[cfg(feature = "telemetry")] {
                    self.telemetry.counter_loading_from_cache_nanos.update(now.elapsed().as_nanos() as u64);
                }
            }
            log::trace!(
                target: TARGET,
                "{}  {:x}  update visited {}  root_cell_id {:x}",
                op_name, cell_id, new_counter, root_id
            );
            return Ok((Some(new_counter), visited_cell.cell().cloned()));
        }

        #[cfg(feature = "telemetry")]
        let now = Instant::now();
        if let Some(counter) = cells_counters.as_mut().and_then(|cc| cc.get_mut(cell_id)) {
            // Cell's counter is in cache - update it

            #[cfg(feature = "telemetry")] {
                self.telemetry.counter_loading_from_cache_nanos.update(now.elapsed().as_nanos() as u64);
                self.telemetry.counters_loaded_from_cache.fetch_add(1, Ordering::Relaxed);
                self.telemetry.cell_counter_from_cache_speed.update(1);
            }

            let mut visited_cell = VisitedCell::with_counter(*counter);
            *counter = update_cell(&mut visited_cell)?;
            visited.insert(cell_id.clone(), visited_cell);
            log::trace!(
                target: TARGET,
                "{}  {:x}  update counter {}  root_cell_id {:x}",
                op_name, cell_id, counter, root_id
            );

            return Ok((Some(*counter), None));
        }

        if !full_filled_counters {
            #[cfg(feature = "telemetry")]
            let now = Instant::now();
            if let Some(counter_raw) = self.db.get_pinned_cf(counters_cf, cell_id.as_slice())? {
                // Cell's counter is in DB - load it and update

                #[cfg(feature = "telemetry")] {
                    self.telemetry.counter_loading_from_db_nanos.update(now.elapsed().as_nanos() as u64);
                    self.telemetry.counters_loaded_from_db.fetch_add(1, Ordering::Relaxed);
                    self.telemetry.cell_counter_from_db_speed.update(1);
                }

                let mut visited_cell = VisitedCell::with_raw_counter(&counter_raw)?;
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

                return Ok((Some(counter), None));
            }
        }

        Ok((None, None))
    }
}

impl CellsFactory for DynamicBocDb {
    fn create_cell(self: Arc<Self>, builder: BuilderData) -> Result<Cell> {

        let cell = StorageCell::with_cell(&*builder.into_cell()?, &self, true, true)?;
        let cell = Cell::with_cell_impl(cell);
        let repr_hash = cell.repr_hash();

        let mut result_cell = None;

        let result = self.storing_cells.insert_with(
            repr_hash,
            |_, inserted, found| {
                if let Some((_, found)) = found {
                    result_cell = Some(found.clone());
                    lockfree::map::Preview::Discard
                } else if let Some(inserted) = inserted {
                    result_cell = Some(inserted.clone());
                    lockfree::map::Preview::Keep
                } else {
                    result_cell = Some(cell.clone());
                    lockfree::map::Preview::New(cell.clone())
                }
            }
        );

        let result_cell = result_cell
            .ok_or_else(|| error!("INTERNAL ERROR: result_cell {:x} is None", cell.repr_hash()))?;

        match result {
            lockfree::map::Insertion::Created => {
                log::trace!(target: TARGET, "DynamicBocDb::create_cell {:x} - created new", cell.repr_hash());
                #[cfg(feature = "telemetry")] {
                    let storing_cells_count = self.storing_cells_count.fetch_add(1, Ordering::Relaxed);
                    self.telemetry.storing_cells.update(storing_cells_count + 1);
                }
            }
            lockfree::map::Insertion::Failed(_) => {
                log::trace!(target: TARGET, "DynamicBocDb::create_cell {:x} - already exists", cell.repr_hash());
            }
            lockfree::map::Insertion::Updated(old) => {
                fail!("INTERNAL ERROR: storing_cells.insert_with {:x} returned Updated({:?})", cell.repr_hash(), old)
            }
        }

        Ok(result_cell)
    }
}

db_impl_base!(IndexedUInt256Db, U32Key);

pub struct DoneCellsStorageAdapter {
    boc_db: Arc<DynamicBocDb>,
    index: IndexedUInt256Db, // index in boc (u32) -> cell id (u256)
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
            index: IndexedUInt256Db::with_db(db.clone(), index_db_path, true)?,
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
        let id = UInt256::from_slice(self.index.get(&index.into())?.as_ref());
        self.boc_db.clone().load_cell(&id, false, true)
    }

    fn cleanup(&mut self) -> Result<()> {
        self.index.destroy()?;
        Ok(())
    }
}

db_impl_base!(IndexedUInt32Db, UInt256);

pub struct CellByHashStorageAdapter {
    boc_db: Arc<DynamicBocDb>,
    use_cache: bool,
}

impl CellByHashStorageAdapter {
    pub fn new(
        boc_db: Arc<DynamicBocDb>,
        use_cache: bool,
    ) -> Result<Self> {
        Ok(Self {
            boc_db,
            use_cache,
        })
    }
}

impl CellByHashStorage for CellByHashStorageAdapter {
    fn get_cell_by_hash(&self, hash: &UInt256) -> Result<Cell> {
        self.boc_db.clone().load_cell(hash, self.use_cache, true)
    }
}

// This is adapter for DynamicBocDb wich allows to use it as OrderedCellsStorage
// while serialising BOC. All cells sent to 'push_cell' should be already saved into DynamicBocDb!
pub struct OrderedCellsStorageAdapter {
    boc_db: Arc<DynamicBocDb>,
    index1: IndexedUInt256Db, // reverted index in boc (u32) -> cell id (u256)
    index2: IndexedUInt32Db, // cell id (u256) -> reverted index in boc (u32)
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
            index1: IndexedUInt256Db::with_db(db.clone(), path1, true)?,
            index2: IndexedUInt32Db::with_db(db.clone(), path2, true)?,
            cells_count: 0,
            slowdown_mcs,
            index_db_path: index_db_path.to_string(),
        })
    }
    fn slowdown(&self) -> u32 {
        let timeout = self.slowdown_mcs.load(Ordering::Relaxed);
        if timeout > 0 {
            std::thread::sleep(Duration::from_nanos(timeout as u64));
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
        self.index2.put(hash, &index.to_le_bytes())?;
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
        let id = UInt256::from_slice(self.index1.get(&index.into())?.as_ref());
        let cell = self.boc_db.clone().load_cell(&id, false, true)?;

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

struct RawCellsCache(
    quick_cache::sync::Cache<UInt256, bytes::Bytes, CellSizeEstimator, ahash::RandomState>
);

#[derive(Clone, Copy)]
pub struct CellSizeEstimator;
impl quick_cache::Weighter<UInt256, bytes::Bytes> for CellSizeEstimator {
    fn weight(&self, _key: &UInt256, val: &bytes::Bytes) -> u32 {
        const BYTES_SIZE: usize = size_of::<usize>() * 4;
        let len = size_of::<UInt256>() + val.len() + BYTES_SIZE;
        len as u32
    }
}

impl RawCellsCache {

    fn new(size_in_bytes: u64) -> Self {

        // Percentile 0.1%    from  96 to 127  => 1725119 count
        // Percentile 10%     from 128 to 191  => 82838849 count
        // Percentile 25%     from 128 to 191  => 82838849 count
        // Percentile 50%     from 128 to 191  => 82838849 count
        // Percentile 75%     from 128 to 191  => 82838849 count
        // Percentile 90%     from 192 to 255  => 22775080 count
        // Percentile 95%     from 192 to 255  => 22775080 count
        // Percentile 99%     from 192 to 255  => 22775080 count
        // Percentile 99.9%   from 256 to 383  => 484002 count
        // Percentile 99.99%  from 256 to 383  => 484002 count
        // Percentile 99.999% from 256 to 383  => 484002 count

        // from 64  to 95  - 15_267
        // from 96  to 127 - 1_725_119
        // from 128 to 191 - 82_838_849
        // from 192 to 255 - 22_775_080
        // from 256 to 383 - 484_002

        // we assume that 75% of cells are in range 128..191
        // so we can use use 192 as size for value in cache

        const MAX_CELL_SIZE: u64 = 192;
        const KEY_SIZE: u64 = 32;

        let estimated_cell_cache_capacity = size_in_bytes / (KEY_SIZE + MAX_CELL_SIZE);
        log::trace!("{estimated_cell_cache_capacity},{size_in_bytes}");
        let raw_cache = quick_cache::sync::Cache::with(
            estimated_cell_cache_capacity as usize,
            size_in_bytes,
            CellSizeEstimator,
            ahash::RandomState::default(),
            quick_cache::sync::DefaultLifecycle::default()
        );
        Self(raw_cache)

    }
}
