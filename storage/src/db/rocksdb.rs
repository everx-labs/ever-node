/*
* Copyright (C) 2019-2023 TON Labs. All Rights Reserved.
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
    db::traits::{
        DbKey, Kvc, KvcReadable, KvcSnapshotable, KvcTransaction, KvcTransactional, KvcWriteable,
    },
    traits::Serializable,
    types::DbSlice
};
use adnl::common::add_unbound_object_to_map;
use rocksdb::{
    BlockBasedOptions, BoundColumnFamily, Cache, DBWithThreadMode, IteratorMode, MultiThreaded, 
    Options, SnapshotWithThreadMode, WriteBatch
};
use std::{
    fmt::{Debug, Formatter}, ops::Deref, path::Path, sync::{Arc, atomic::{AtomicI32, Ordering}},
    io::Cursor, collections::HashSet,
};
use ton_types::{fail, error, Result};
use ton_block::BlockIdExt;

pub const LAST_UNNEEDED_KEY_BLOCK: &str = "LastUnneededKeyBlockId"; // Latest key block we can delete in archives GC
pub const NODE_STATE_DB_NAME: &str = "node_state_db";

#[derive(Debug)]
pub struct RocksDb {
    db: Option<DBWithThreadMode<MultiThreaded>>,
    locks: lockfree::map::Map<String, AtomicI32>,
    hi_perf_cfs: HashSet<String>
}

impl RocksDb {

    /// Creates new instance with given path
    pub fn with_path(path: &str, name: &str) -> Result<Arc<Self>> {
        Self::with_options(path, name, HashSet::new(), false)
    }

    /// Creates new instance read only with given path
    pub fn read_only(path: &str, name: &str) -> Result<Arc<Self>> {
        Self::with_options(path, name, HashSet::new(), true)
    }

    /// Creates new instance with given path and ability to additionally configure options
    pub fn with_options(
        path: &str, 
        name: &str,
        hi_perf_cfs: HashSet<String>,
        read_only: bool
    ) -> Result<Arc<Self>> {

        let path = Path::new(path);
        let path = path.join(name);

        let options = Self::build_db_options();

        let mut iteration = 1;
        loop {

            let cfs = DBWithThreadMode::<MultiThreaded>::list_cf(&options, &path).unwrap_or_default();

            log::info!(
                target: "storage",
                "Opening DB {} (read only: {}) with {} cfs (iteration {})",
                name, read_only, cfs.len(), iteration
            );
            iteration += 1;

            let cfs_opt = cfs.clone().into_iter()
                .map(|cf| {
                    let opt = if hi_perf_cfs.contains(&cf) {
                        Self::build_hi_perf_cf_options()
                    } else {
                        Options::default()
                    };
                    rocksdb::ColumnFamilyDescriptor::new(cf, opt)
                });
            let db = if read_only {
                DBWithThreadMode::<MultiThreaded>::open_cf_descriptors_read_only(&options, &path, cfs_opt, false)?
            } else {
                DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(&options, &path, cfs_opt)?
            };


            // 
            // Clean up CF from old archives
            //
            if !read_only && cfs.len() > 100 && iteration <= 3 {
                if Self::clean_up_old_cf(&db, &cfs)
                    .map_err(|e| error!("Error while clean_up_old_cf: {}", e))? 
                {
                    drop(db);
                    continue;
                }
            }

            let db = Self {
                db: Some(db),
                locks: lockfree::map::Map::new(),
                hi_perf_cfs,
            };
            return Ok(Arc::new(db))
        }
    }

    fn build_hi_perf_cf_options() -> Options {

        let mut options = Options::default();
        let mut block_opts = BlockBasedOptions::default();

        // specified cache for blocks.
        let cache = Cache::new_lru_cache(1024 * 1024 * 1024);
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

    fn build_db_options() -> Options {

        let mut options = Options::default();

        // If true, the database will be created if it is missing.
        options.create_if_missing(true);

        // By default, RocksDB uses only one background thread for flush and
        // compaction. Calling this function will set it up such that total of
        // `total_threads` is used. Good value for `total_threads` is the number of
        // cores.
        let num_cpus = std::thread::available_parallelism().unwrap().get();
        options.set_max_subcompactions(std::cmp::max(num_cpus as u32 / 2, 1));
        options.set_max_background_jobs(std::cmp::max(num_cpus as i32 / 2, 2));
        options.increase_parallelism(num_cpus as i32);

        // If true, missing column families will be automatically created.
        options.create_missing_column_families(true);
        
        options.enable_statistics();
        options.set_dump_malloc_stats(true);

        options
    }

    fn clean_up_old_cf(db: &DBWithThreadMode<MultiThreaded>, cfs: &[String]) -> Result<bool> {

        if let Some(cf) = db.cf_handle(NODE_STATE_DB_NAME) {
            if let Ok(Some(db_slice)) = db.get_pinned_cf(&cf, LAST_UNNEEDED_KEY_BLOCK) {
                let mut cursor = Cursor::new(db_slice.as_ref());
                let id = BlockIdExt::deserialize(&mut cursor)?;
                let prefixes = ["entry_meta_db_", "offsets_db_", "status_db_"];

                log::info!(target: "storage", "Read last unneeded key block: {}", id.seq_no());

                for cf_name in cfs {
                    for pfx in &prefixes {
                        if cf_name.contains(pfx) {
                            let num = u32::from_str_radix(&cf_name.replace(pfx, ""), 10).unwrap_or(u32::MAX);
                            if num < id.seq_no() {
                                log::warn!(target: "storage", "Dropping old CF {}", cf_name);
                                let result = db.drop_cf(cf_name);
                                log::warn!(target: "storage", "Dropped old CF {}: {:?}", cf_name, result);
                            }
                            break;
                        }
                    }
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn table(
        self: Arc<Self>,
        family: impl ToString,
        create_if_not_exist: bool,
    ) -> Result<RocksDbTable> {
        RocksDbTable::with_db(self, family, create_if_not_exist)
    }

    fn db(&self) -> &DBWithThreadMode<MultiThreaded> {
        self.db.as_ref().expect("rocksdb was occasionaly destroyed")
    }

    // Error is occured if column family is already created
    fn create_cf(&self, name: &str) -> Result<()> {
        let opt = if self.hi_perf_cfs.contains(name) {
            Self::build_hi_perf_cf_options()
        } else {
            Options::default()
        };
        self.db().create_cf(name, &opt)?;
        Ok(())
    }

    pub fn drop_table(&self, name: &str) -> Result<bool> {
        if let Some(lock) = self.locks.get(name) {
            let lock = lock.val();
            if lock.compare_exchange(0, -1000000, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                self.db().drop_cf(name)?;
                self.locks.remove(name);
                return Ok(true)
            } else {
                return Ok(false)
            }
        }
        fail!("Attempt to drop already dropped table {}", name)
    }

    pub fn drop_table_force(&self, name: &str) -> Result<()> {
        if self.drop_table(name).is_err() {
            self.db().drop_cf(name)?;
            self.locks.remove(name);
        }
        Ok(())
    }

    fn cf(&self, name: &str) -> Result<Arc<BoundColumnFamily>> {
        self.db().cf_handle(name)
            .ok_or_else(|| error!("no handle for column family {} in rocksdb", name))
    }

    pub fn destroy_db(path: impl AsRef<Path>) -> Result<bool> {
        let opts = Options::default();
        match DBWithThreadMode::<MultiThreaded>::destroy(&opts, path.as_ref()) {
            Ok(_) => Ok(true),
            Err(err) => fail!("cannot destroy database {:?} : {}", path.as_ref(), err)
        }
    }
}

/// Implementation of key-value collection for RocksDB
impl Kvc for RocksDb {

    fn len(&self) -> Result<usize> {
        // be careful in usual code
        Ok(self.db().iterator(IteratorMode::Start).count())
    }

    fn destroy(&mut self) -> Result<bool> {
        match self.db.take() {
            Some(db) => {
                let path = db.path().to_path_buf();
                drop(db);
                Self::destroy_db(path)
            }
            None => fail!("rocksdb already destroyed")
        }
    }
}

impl<K: DbKey + Send + Sync> KvcReadable<K> for RocksDb {

    fn get_meta(&self) -> &str {
        self.db().path().to_str().unwrap()
    }

    fn try_get_raw(&self, key: &[u8]) -> Result<Option<DbSlice>> {
        let ret = self.db().get_pinned(key)?;
        Ok(ret.map(|value| value.into()))
    }

    fn for_each(&self, predicate: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool>) -> Result<bool> {
        for iter in self.db().iterator(IteratorMode::Start) {
            let (key, value) = iter?;
            if !predicate(key.as_ref(), value.as_ref())? {
                return Ok(false)
            }
        }
        Ok(true)
    }

}

impl<K: DbKey + Send + Sync> KvcWriteable<K> for RocksDb {
    fn put_raw(&self, key: &[u8], value: &[u8]) -> Result<()> {
        Ok(self.db().put(key, value)?)
    }

    fn delete_raw(&self, key: &[u8]) -> Result<()> {
        Ok(self.db().delete(key)?)
    }
}

/// Implementation of transaction support for key-value collection for RocksDB.
impl<K: DbKey + Send + Sync> KvcTransactional<K> for RocksDb {
    fn begin_transaction(&self) -> Result<Box<dyn KvcTransaction<K>>> {
        unreachable!()
    }
}

impl Deref for RocksDb {
    type Target = DBWithThreadMode<MultiThreaded>;
    fn deref(&self) -> &Self::Target {
        self.db.as_ref().unwrap()
    }
}

impl AsRef<DBWithThreadMode<MultiThreaded>> for RocksDb {
    fn as_ref(&self) -> &DBWithThreadMode<MultiThreaded> {
        self.db()
    }
}

#[derive(Debug)]
pub struct RocksDbTable {
    db: Arc<RocksDb>,
    family: String,
}

impl RocksDbTable {

    pub(crate) fn with_db(
        db: Arc<RocksDb>,
        family: impl ToString,
        create_if_not_exist: bool,
    ) -> Result<Self> {
        let family = family.to_string();
        loop {
            if db.locks.get(&family).is_some() {
                break
            }
            if let Err(e) = db.cf(&family) {
                if create_if_not_exist {
                    db.create_cf(&family)?;
                } else {
                    fail!(e)
                }
            }
            add_unbound_object_to_map(
                &db.locks, 
                family.clone(), 
                || Ok(AtomicI32::new(0))
            )?;
        }
        let ret = Self {
            db,
            family
        };
        Ok(ret)
    }

    fn cf(&self) -> Result<Arc<BoundColumnFamily>> {
        self.db.cf(&self.family)
    }

}

impl AsRef<DBWithThreadMode<MultiThreaded>> for RocksDbTable {
    fn as_ref(&self) -> &DBWithThreadMode<MultiThreaded> {
        self.db.db()
    }               
}

impl Deref for RocksDbTable {
    type Target = DBWithThreadMode<MultiThreaded>;
    fn deref(&self) -> &Self::Target {
        self.db.db()
    }
}

/// Implementation of key-value collection for RocksDB
impl Kvc for RocksDbTable {

    fn len(&self) -> Result<usize> {
        // be careful in usual code
        Ok(self.db.iterator_cf(&self.cf()?, IteratorMode::Start).count())
    }

    fn destroy(&mut self) -> Result<bool> {
        self.db.drop_table(&self.family)
    }
}

/// Implementation of readable key-value collection for RocksDB. Actual implementation is blocking.
impl<K: DbKey + Send + Sync> KvcReadable<K> for RocksDbTable {

    fn get_meta(&self) -> &str {
        self.family.as_str()
    }

    fn try_get_raw(&self, key: &[u8]) -> Result<Option<DbSlice>> {
        if let Some(lock) = self.db.locks.get(&self.family) {
            let lock = lock.val();
            if lock.fetch_add(1, Ordering::Relaxed) >= 0 {
                let ret = self.db.get_pinned_cf(&self.cf()?, key);
                lock.fetch_sub(1, Ordering::Relaxed);
                return Ok(ret?.map(|value| value.into()))
            }
        }
        fail!("Attempt to read from dropped table {}", self.family)
    }

    fn for_each(&self, predicate: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool>) -> Result<bool> {
        if let Some(lock) = self.db.locks.get(&self.family) {
            let lock = lock.val();
            if lock.fetch_add(1, Ordering::Relaxed) >= 0 {
                for iter in self.db.iterator_cf(&self.cf()?, IteratorMode::Start) {
                    let (key, value) = iter?;
                    match predicate(key.as_ref(), value.as_ref()) {
                        Ok(false) => {
                            lock.fetch_sub(1, Ordering::Relaxed);
                            return Ok(false)
                        },
                        Ok(true) => (),
                        Err(e) => {
                            lock.fetch_sub(1, Ordering::Relaxed);
                            return Err(e)
                        }
                    }
                }
                lock.fetch_sub(1, Ordering::Relaxed);
                return Ok(true)
            }
        }
        fail!("Attempt to iterate over dropped table {}", self.family)
    }

}

/// Implementation of writable key-value collection for RocksDB. Actual implementation is blocking.
impl<K: DbKey + Send + Sync> KvcWriteable<K> for RocksDbTable {

    fn put_raw(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if let Some(lock) = self.db.locks.get(&self.family) {
            let lock = lock.val();
            if lock.fetch_add(1, Ordering::Relaxed) >= 0 {
                let ret = self.db.put_cf(&self.cf()?, key, value);
                lock.fetch_sub(1, Ordering::Relaxed);
                return Ok(ret?)
            }
        }
        fail!("Attempt to write into dropped table {}", self.family)
    }

    fn delete_raw(&self, key: &[u8]) -> Result<()> {
        if let Some(lock) = self.db.locks.get(&self.family) {
            let lock = lock.val();
            if lock.fetch_add(1, Ordering::Relaxed) >= 0 {
                let ret = self.db.delete_cf(&self.cf()?, key);
                lock.fetch_sub(1, Ordering::Relaxed);
                return Ok(ret?)
            }
        }
        fail!("Attempt to delete from dropped table {}", self.family)
    }

}

/// Implementation of support for take snapshots for RocksDB.
impl<K: DbKey + Send + Sync> KvcSnapshotable<K> for RocksDbTable {
    fn snapshot<'db>(&'db self) -> Result<Arc<dyn KvcReadable<K> + 'db>> {
        Ok(Arc::new(RocksDbSnapshot::new(self.db.clone(), self.db.snapshot(), self.family.clone())))
    }
}

// TODO: snapshot without family by RocksDb
struct RocksDbSnapshot<'db> {
    db: Arc<RocksDb>,
    snapshot: SnapshotWithThreadMode<'db, DBWithThreadMode<MultiThreaded>>,
    family: String,
}

impl<'db> RocksDbSnapshot<'db> {
    pub(crate) fn new(
        db: Arc<RocksDb>, 
        snapshot: SnapshotWithThreadMode<'db, DBWithThreadMode<MultiThreaded>>, 
        family: String
    ) -> Self {
        Self {
            db,
            snapshot,
            family,
        }
    }
    fn cf(&self) -> Result<Arc<BoundColumnFamily>> {
        self.db.cf(&self.family)
    }
}

impl Debug for RocksDbSnapshot<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[snapshot] for {}", self.family)
    }
}

impl Kvc for RocksDbSnapshot<'_> {
    fn len(&self) -> Result<usize> {
        fail!("len() is not supported for snapshots")
    }
    fn destroy(&mut self) -> Result<bool> {
        fail!("destroy() is not supported for snapshots")
    }
}

impl<K: DbKey + Send + Sync> KvcReadable<K> for RocksDbSnapshot<'_> {
    fn try_get_raw(&self, key: &[u8]) -> Result<Option<DbSlice>> {
        Ok(self.snapshot.get_cf(&self.cf()?, key)?.map(|value| value.into()))
    }
    fn for_each(&self, predicate: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool>) -> Result<bool> {
        for iter in self.snapshot.iterator_cf(&self.cf()?, IteratorMode::Start) {
            let (key, value) = iter?;
            if !predicate(key.as_ref(), value.as_ref())? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// Implementation of transaction support for key-value collection for RocksDB.
impl<K: DbKey + Send + Sync> KvcTransactional<K> for RocksDbTable {
    fn begin_transaction(&self) -> Result<Box<dyn KvcTransaction<K>>> {
        Ok(Box::new(RocksDbTransaction::new(self.db.clone(), self.family.clone())))
    }
}

// TODO: Batch for RockDb

pub struct RocksDbTransaction {
    db: Arc<RocksDb>,
    batch: Option<WriteBatch>,
    family: String
}

/// Implementation of transaction for key-value collection for RocksDB.
impl RocksDbTransaction {
    fn new(db: Arc<RocksDb>, family: String) -> Self {
        Self {
            db,
            batch: Some(WriteBatch::default()),
            family,
        }
    }
    fn cf(&self) -> Result<Arc<BoundColumnFamily>> {
        self.db.cf(&self.family)
    }
}

impl<'db, K: DbKey + Send + Sync> KvcTransaction<K> for RocksDbTransaction {
    fn put_raw(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut batch = self.batch.take().unwrap();
        batch.put_cf(&self.cf()?, key, value);
        self.batch = Some(batch);
        Ok(())
    }

    fn delete_raw(&mut self, key: &[u8]) -> Result<()> {
        let mut batch = self.batch.take().unwrap();
        batch.delete_cf(&self.cf()?, key);
        self.batch = Some(batch);
        Ok(())
    }

    fn clear(&mut self) {
        self.batch.as_mut().unwrap().clear();
    }

    fn commit(self: Box<Self>) -> Result<()> {
        Ok(self.db.write(self.batch.unwrap())?)
    }

    fn len(&self) -> usize {
        self.batch.as_ref().unwrap().len()
    }

    fn is_empty(&self) -> bool {
        self.batch.as_ref().unwrap().is_empty()
    }
}
