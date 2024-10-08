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
    db::DbKey,
    traits::Serializable,
    types::DbSlice, error::StorageError
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
use ever_block::{fail, error, Result};
use ever_block::BlockIdExt;

pub const LAST_UNNEEDED_KEY_BLOCK: &str = "LastUnneededKeyBlockId"; // Latest key block we can delete in archives GC
pub const NODE_STATE_DB_NAME: &str = "node_state_db";

pub type DbPredicateMut<'a> = &'a mut dyn FnMut(&[u8], &[u8]) -> Result<bool>;

#[derive(Debug)]
pub struct RocksDb {
    db: Option<DBWithThreadMode<MultiThreaded>>,
    locks: lockfree::map::Map<String, AtomicI32>,
    hi_perf_cfs: HashSet<String>
}

impl RocksDb {

    /// Creates new instance with given path
    pub fn with_path(path: impl AsRef<Path>, name: &str) -> Result<Arc<Self>> {
        Self::with_options(path, name, HashSet::new(), false)
    }

    /// Creates new instance read only with given path
    pub fn read_only(path: impl AsRef<Path>, name: &str) -> Result<Arc<Self>> {
        Self::with_options(path, name, HashSet::new(), true)
    }

    /// Creates new instance with given path and ability to additionally configure options
    pub fn with_options(
        path: impl AsRef<Path>,
        name: &str,
        hi_perf_cfs: HashSet<String>,
        read_only: bool
    ) -> Result<Arc<Self>> {

        let path = path.as_ref().join(name);

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
            if !read_only && cfs.len() > 100 && iteration <= 3 &&
                Self::clean_up_old_cf(&db, &cfs)
                    .map_err(|e| error!("Error while clean_up_old_cf: {}", e))?
            {
                drop(db);
                continue;
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

        options.set_max_total_wal_size(1024 * 1024 * 1024);

        options.enable_statistics();
        options.set_dump_malloc_stats(true);

        // Specify the maximal size of the info log file. If the log file
        // is larger than `max_log_file_size`, a new info log file will
        // be created.
        // If max_log_file_size == 0, all logs will be written to one log file.
        options.set_max_log_file_size(1024 * 1024 * 100);

        // Maximal info log files to be kept.
        // Default: 1000
        options.set_keep_log_file_num(3);

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
                            let num = cf_name.replace(pfx, "").parse::<u32>().unwrap_or(u32::MAX);
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

    pub fn table<K: DbKey + Send + Sync>(
        self: Arc<Self>,
        family: impl ToString,
        create_if_not_exist: bool,
    ) -> Result<RocksDbTable<K>> {
        RocksDbTable::with_db(self, family, create_if_not_exist)
    }

    pub fn db(&self) -> &DBWithThreadMode<MultiThreaded> {
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

    pub fn destroy(&mut self) -> Result<bool> {
        match self.db.take() {
            Some(db) => {
                let path = db.path().to_path_buf();
                drop(db);
                Self::destroy_db(path)
            }
            None => fail!("rocksdb already destroyed")
        }
    }

    fn get_meta(&self) -> &str {
        self.db().path().to_str().unwrap()
    }

    fn try_get_raw(&self, key: &[u8]) -> Result<Option<DbSlice>> {
        let ret = self.db().get_pinned(key)?;
        Ok(ret.map(|value| value.into()))
    }

    pub fn try_get<K: DbKey + Send + Sync>(&self, key: &K) -> Result<Option<DbSlice>> {
        self.try_get_raw(key.key())
    }

    /// Gets value from collection by the key
    pub fn get<K: DbKey + Send + Sync>(&self, key: &K) -> Result<DbSlice> {
        self.try_get(key)?.ok_or_else(|| {
            let meta = self.get_meta();
            let what = if meta.is_empty() {
                key.as_string()
            } else {
                format!("{} ({})", key.as_string(), meta)
            };
            StorageError::KeyNotFound(key.key_name(), what).into()
        })
    }

    fn put_raw(&self, key: &[u8], value: &[u8]) -> Result<()> {
        Ok(self.db().put(key, value)?)
    }

    /// Puts value into collection by the key
    pub fn put<K: DbKey + Send + Sync>(&self, key: &K, value: &[u8]) -> Result<()> {
        self.put_raw(key.key(), value)
    }

    pub fn delete_raw(&self, key: &[u8]) -> Result<()> {
        Ok(self.db().delete(key)?)
    }

    /// Deletes value from collection by the key
    pub fn delete<K: DbKey + Send + Sync>(&self, key: &K) -> Result<()> {
        self.delete_raw(key.key())
    }
    
    pub fn contains<K: DbKey + Send + Sync>(&self, key: &K) -> Result<bool> {
        Ok(self.try_get(key)?.is_some())
    }
}

#[derive(Debug)]
pub struct RocksDbTable<K> {
    db: Arc<RocksDb>,
    family: String,
    phantom: std::marker::PhantomData<K>,
}

impl<K: DbKey + Send + Sync> RocksDbTable<K> {

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
            family,
            phantom: std::marker::PhantomData
        };
        Ok(ret)
    }

    fn cf(&self) -> Result<Arc<BoundColumnFamily>> {
        self.db.cf(&self.family)
    }

    pub fn len(&self) -> Result<usize> {
        // be careful in usual code
        Ok(self.db.iterator_cf(&self.cf()?, IteratorMode::Start).count())
    }

    /// Returns true, if collection is empty; false otherwise
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    pub fn destroy(&mut self) -> Result<bool> {
        self.db.drop_table(&self.family)
    }

    fn get_meta(&self) -> &str {
        self.family.as_str()
    }

    pub fn try_get_raw(&self, key: &[u8]) -> Result<Option<DbSlice>> {
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

    pub fn for_each(&self, predicate: DbPredicateMut) -> Result<bool> {
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

    pub fn put_raw(&self, key: &[u8], value: &[u8]) -> Result<()> {
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

    /// Puts value into collection by the key
    pub fn put(&self, key: &K, value: &[u8]) -> Result<()> {
        self.put_raw(key.key(), value)
    }

    pub fn delete_raw(&self, key: &[u8]) -> Result<()> {
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

    /// Deletes value from collection by the key
    pub fn delete(&self, key: &K) -> Result<()> {
        self.delete_raw(key.key())
    }

    pub fn try_get(&self, key: &K) -> Result<Option<DbSlice>> {
        self.try_get_raw(key.key())
    }

    /// Gets value from collection by the key
    pub fn get(&self, key: &K) -> Result<DbSlice> {
        self.try_get(key)?.ok_or_else(|| {
            let meta = self.get_meta();
            let what = if meta.is_empty() {
                key.as_string()
            } else {
                format!("{} ({})", key.as_string(), meta)
            };
            StorageError::KeyNotFound(key.key_name(), what).into()
        })
    }

    /// Gets slice with given size starting from given offset from collection by the key
    pub fn get_slice(&self, key: &K, offset: u64, size: u64) -> Result<DbSlice> {
        self.get_vec(key, offset, size).map(DbSlice::Vector)
    }

    fn get_vec(&self, key: &K, offset: u64, size: u64) -> Result<Vec<u8>> {
        self.get(key).and_then(|value| {
            if offset >= value.len() as u64 || offset + size > value.as_ref().len() as u64 {
                return Err(StorageError::OutOfRange.into());
            }

            let mut result = Vec::new();
            result.extend_from_slice(&value[offset as usize..(offset + size) as usize]);
            Ok(result)
        })
    }

    /// Determines, is key exists in key-value collection
    pub fn contains(&self, key: &K) -> Result<bool> {
        Ok(self.try_get(key)?.is_some())
    }

    pub fn snapshot(&self) -> Result<Arc<RocksDbSnapshot>> {
        Ok(Arc::new(RocksDbSnapshot::new(self.db.clone(), self.db.snapshot(), self.family.clone())))
    }

    pub fn begin_transaction(&self) -> Result<Box<RocksDbTransaction>> {
        Ok(Box::new(RocksDbTransaction::new(self.db.clone(), self.family.clone())))
    }
}

impl Deref for RocksDb {
    type Target = rocksdb::DBWithThreadMode<MultiThreaded>;

    fn deref(&self) -> &Self::Target {
        self.db.as_ref().unwrap()
    }
}

// TODO: snapshot without family by RocksDb
pub struct RocksDbSnapshot<'db> {
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

    /// Get meta information (like DB name or something)
    fn get_meta(&self) -> &str {
        ""
    }

    fn cf(&self) -> Result<Arc<BoundColumnFamily>> {
        self.db.cf(&self.family)
    }

    fn try_get_raw(&self, key: &[u8]) -> Result<Option<DbSlice>> {
        Ok(self.snapshot.get_cf(&self.cf()?, key)?.map(|value| value.into()))
    }

    fn try_get<K: DbKey + Send + Sync>(&self, key: &K) -> Result<Option<DbSlice>> {
        self.try_get_raw(key.key())
    }

    /// Gets value from collection by the key
    pub fn get<K: DbKey + Send + Sync>(&self, key: &K) -> Result<DbSlice> {
        self.try_get(key)?.ok_or_else(|| {
            let meta = self.get_meta();
            let what = if meta.is_empty() {
                key.as_string()
            } else {
                format!("{} ({})", key.as_string(), meta)
            };
            StorageError::KeyNotFound(key.key_name(), what).into()
        })
    }

    pub fn for_each(&self, predicate: DbPredicateMut) -> Result<bool> {
        for iter in self.snapshot.iterator_cf(&self.cf()?, IteratorMode::Start) {
            let (key, value) = iter?;
            if !predicate(key.as_ref(), value.as_ref())? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl Debug for RocksDbSnapshot<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[snapshot] for {}", self.family)
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

    pub fn put_raw(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut batch = self.batch.take().unwrap();
        batch.put_cf(&self.cf()?, key, value);
        self.batch = Some(batch);
        Ok(())
    }

    /// Puts value into collection by the key
    pub fn put<K: DbKey + Send + Sync>(&mut self, key: &K, value: &[u8]) -> Result<()> {
        self.put_raw(key.key(), value)
    }

    pub fn delete_raw(&mut self, key: &[u8]) -> Result<()> {
        let mut batch = self.batch.take().unwrap();
        batch.delete_cf(&self.cf()?, key);
        self.batch = Some(batch);
        Ok(())
    }

    /// Deletes value from collection by the key
    pub fn delete<K: DbKey + Send + Sync>(&mut self, key: &K) -> Result<()> {
        self.delete_raw(key.key())
    }

    pub fn commit(self) -> Result<()> {
        Ok(self.db.write(self.batch.unwrap())?)
    }
}

pub async fn destroy_rocks_db(path: &str, name: &str) -> ever_block::Result<()> {
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    let mut path = std::path::Path::new(path);
    let db_path = path.join(name);
    // Clean up DB
    if db_path.exists() {
        let opts = rocksdb::Options::default();
        while let Err(e) = rocksdb::DBWithThreadMode::<rocksdb::MultiThreaded>::destroy(
                &opts, 
                db_path.as_path()
        ) {
            println!("Can't destroy DB: {}", e);
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        }
    }
    // Clean up DB folder
    if db_path.exists() {
        std::fs::remove_dir_all(db_path).map_err(
            |e| ever_block::error!("Can't clean DB folder: {}", e)
        )?
    }
    // Clean up upper folder if empty
    while path.exists() {
        if std::fs::read_dir(path)?.count() > 0 {
            break
        }
        std::fs::remove_dir_all(path).map_err(
            |e| ever_block::error!("Can't clean DB enclosing folder: {}", e)
        )?;
        path = if let Some(path) = path.parent() {
            path
        } else { 
            break
        }
    }
    Ok(())
}