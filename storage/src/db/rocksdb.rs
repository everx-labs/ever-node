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
    db::traits::{
        DbKey, Kvc, KvcReadable, KvcSnapshotable, KvcTransaction, KvcTransactional, KvcWriteable
    },
    types::DbSlice
};
use adnl::common::add_unbound_object_to_map;
use rocksdb::{
    BlockBasedOptions, BoundColumnFamily, Cache, DBWithThreadMode, IteratorMode, MultiThreaded, 
    Options, SnapshotWithThreadMode, WriteBatch
};
use std::{
    fmt::{Debug, Formatter}, ops::Deref, path::Path, sync::{Arc, atomic::{AtomicI32, Ordering}}
};
use ton_types::{fail, Result};

#[derive(Debug)]
pub struct RocksDb {
    db: Option<DBWithThreadMode<MultiThreaded>>,
    locks: lockfree::map::Map<String, AtomicI32>
}

impl RocksDb {

    /// Creates new instance with given path
    pub fn with_path(path: &str, name: &str) -> Arc<Self> {
        Self::with_options(path, name, |_| {}, false)
    }

    /// Creates new instance read only with given path
    pub fn read_only(path: &str, name: &str) -> Arc<Self> {
        Self::with_options(path, name, |_| {}, true)
    }

    /// Creates new instance with given path and ability to additionally configure options
    pub fn with_options(
        path: &str, 
        name: &str,
        configure_options: impl Fn(&mut Options), 
        read_only: bool
    ) -> Arc<Self> {

        let path = Path::new(path);
        let path = path.join(name);

        let cache = Cache::new_lru_cache(1 << 30).unwrap(); //1Gb block cache for one instance
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_cache(&cache);
        // save in LRU block cache also indexes and bloom filters
        block_opts.set_cache_index_and_filter_blocks(true); 
        // keep indexes and filters in block cache until tablereader freed
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true); 
        block_opts.set_block_size(16 << 10);
        // use latest available format version with new implementation of bloom filters
        block_opts.set_format_version(5); 

        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_max_total_wal_size(512 << 20); // 512Mb for one instance
        options.set_max_background_jobs(4);
        // allow background async incrementall file sync to disk by 1Mb per sync
        options.set_bytes_per_sync(1 << 20); 
        options.set_block_based_table_factory(&block_opts);
        options.create_missing_column_families(true);
        options.enable_statistics();
        options.set_dump_malloc_stats(true);

        configure_options(&mut options);

        let cfs = DBWithThreadMode::<MultiThreaded>::list_cf(&options, &path)
            .unwrap_or(Vec::new());
/*
        if cfs.is_empty() {
            // we must add at least default column to add other columns in future
            cfs.push("default".to_string());
        }
        let db = DBWithThreadMode::<MultiThreaded>::open_cf(&options, &path, cfs);
*/
        let db = if read_only {
            DBWithThreadMode::<MultiThreaded>::open_cf_for_read_only(&options, &path, cfs, false)
        } else {
            DBWithThreadMode::<MultiThreaded>::open_cf(&options, &path, cfs)
        };
        let db = db.unwrap_or_else(
            |err| panic!("Cannot open DB {:?}: {}", path, err)
        );
        let db = Self {
            db: Some(db),
            locks: lockfree::map::Map::new()
        };
        Arc::new(db)
    }

    pub fn table(self: Arc<Self>, family: impl ToString) -> Result<RocksDbTable> {
        RocksDbTable::with_db(self, family)
    }

    fn db(&self) -> &DBWithThreadMode<MultiThreaded> {
        self.db.as_ref().expect("rocksdb was occasionaly destroyed")
    }

    fn create_cf(&self, name: &str) {
        let opts = Options::default();
        self.db().create_cf(name, &opts).ok();
        // error is occured if column family is created twice
        //    .unwrap_or_else(|err| panic!("unable to create column family {} for rocksdb : {}", name, err));
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

    fn cf(&self, name: &str) -> Arc<BoundColumnFamily> {
        self.db().cf_handle(name)
            .unwrap_or_else(|| panic!("no handle for column family {} in rocksdb", name))
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

    fn try_get(&self, key: &K) -> Result<Option<DbSlice>> {
        let ret = self.db().get_pinned(key.key())?;
        Ok(ret.map(|value| value.into()))
    }

    fn for_each(&self, predicate: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool>) -> Result<bool> {
        for (key, value) in self.db().iterator(IteratorMode::Start) {
            if !predicate(key.as_ref(), value.as_ref())? {
                return Ok(false)
            }
        }
        Ok(true)
    }

}

impl<K: DbKey + Send + Sync> KvcWriteable<K> for RocksDb {
    fn put(&self, key: &K, value: &[u8]) -> Result<()> {
        Ok(self.db().put(key.key(), value)?)
    }

    fn delete(&self, key: &K) -> Result<()> {
        Ok(self.db().delete(key.key())?)
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

    pub(crate) fn with_db(db: Arc<RocksDb>, family: impl ToString) -> Result<Self> {
        let family = family.to_string();
        loop {
            if db.locks.get(&family).is_some() {
                break
            }
            add_unbound_object_to_map(
                &db.locks, 
                family.clone(), 
                || Ok(AtomicI32::new(0))
            )?;
            db.create_cf(&family);
        }
        let ret = Self {
            db,
            family
        };
        Ok(ret)
    }

    fn cf(&self) -> Arc<BoundColumnFamily> {
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
        Ok(self.db.iterator_cf(&self.cf(), IteratorMode::Start).count())
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

    fn try_get(&self, key: &K) -> Result<Option<DbSlice>> {
        if let Some(lock) = self.db.locks.get(&self.family) {
            let lock = lock.val();
            if lock.fetch_add(1, Ordering::Relaxed) >= 0 {
                let ret = self.db.get_pinned_cf(&self.cf(), key.key());
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
                for (key, value) in self.db.iterator_cf(&self.cf(), IteratorMode::Start) {
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

    fn put(&self, key: &K, value: &[u8]) -> Result<()> {
        if let Some(lock) = self.db.locks.get(&self.family) {
            let lock = lock.val();
            if lock.fetch_add(1, Ordering::Relaxed) >= 0 {
                let ret = self.db.put_cf(&self.cf(), key.key(), value);
                lock.fetch_sub(1, Ordering::Relaxed);
                return Ok(ret?)
            }
        }
        fail!("Attempt to write into dropped table {}", self.family)
    }

    fn delete(&self, key: &K) -> Result<()> {
        if let Some(lock) = self.db.locks.get(&self.family) {
            let lock = lock.val();
            if lock.fetch_add(1, Ordering::Relaxed) >= 0 {
                let ret = self.db.delete_cf(&self.cf(), key.key());
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
    fn cf(&self) -> Arc<BoundColumnFamily> {
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
    fn try_get(&self, key: &K) -> Result<Option<DbSlice>> {
        Ok(self.snapshot.get_cf(&self.cf(), key.key())?.map(|value| value.into()))
    }

    fn for_each(&self, predicate: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool>) -> Result<bool> {
        for (key, value) in self.snapshot.iterator_cf(&self.cf(), IteratorMode::Start) {
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
    fn cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf(&self.family)
    }
}

impl<'db, K: DbKey + Send + Sync> KvcTransaction<K> for RocksDbTransaction {
    fn put(&mut self, key: &K, value: &[u8]) {
        let mut batch = self.batch.take().unwrap();
        batch.put_cf(&self.cf(), key.key(), value);
        self.batch = Some(batch);
    }

    fn delete(&mut self, key: &K) {
        let mut batch = self.batch.take().unwrap();
        batch.delete_cf(&self.cf(), key.key());
        self.batch = Some(batch);
    }

    fn clear(&mut self) {
        self.batch.as_mut().unwrap().clear();
    }

    fn commit(self: Box<Self>) -> Result<()> {
        self.db.create_cf(&self.family);
        Ok(self.db.write(self.batch.unwrap())?)
    }

    fn len(&self) -> usize {
        self.batch.as_ref().unwrap().len()
    }

    fn is_empty(&self) -> bool {
        self.batch.as_ref().unwrap().is_empty()
    }
}
