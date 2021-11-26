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
    error::StorageError, types::DbSlice
};
use rocksdb::{DB, IteratorMode, Options, Snapshot, WriteBatch};
use ton_types::{fail, Result};
use std::{fmt::{Debug, Formatter}, path::{Path, PathBuf}, sync::Arc};

#[derive(Debug)]
pub struct RocksDb {
    db: Arc<Option<DB>>,
    path: PathBuf,
}

impl RocksDb {
    /// Creates new instance with given path
    pub fn with_path(path: impl AsRef<Path>) -> Self {
        Self::with_options(path, |_| {})
    }

    /// Creates new instance with given path and ability to additionally configure options
    pub fn with_options(path: impl AsRef<Path>, configure_options: impl Fn(&mut Options)) -> Self {
        let pathbuf = path.as_ref().to_path_buf();

        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_max_total_wal_size(1024 * 1024 * 1024);

        configure_options(&mut options);

        Self {
            db: Arc::new(Some(DB::open(&options, path)
                .unwrap_or_else(|err| panic!("Cannot open DB {:?}: {}", pathbuf, err)))),
            path: pathbuf
        }
    }

    pub(crate) fn db(&self) -> Result<&DB> {
        if let Some(ref db) = *self.db {
            Ok(db)
        } else {
            Err(StorageError::DbIsDropped.into())
        }
    }
}

/// Implementation of key-value collection for RocksDB
impl Kvc for RocksDb {
    fn len(&self) -> Result<usize> {
        fail!("len() is not supported for RocksDb")
    }

    fn destroy(&mut self) -> Result<()> {
        if Arc::get_mut(&mut self.db)
            .ok_or(StorageError::HasActiveTransactions)?
            .is_some()
        {
            self.db = Arc::new(None);
        }

        Ok(DB::destroy(&Options::default(), &self.path)?)
    }
}

/// Implementation of readable key-value collection for RocksDB. Actual implementation is blocking.
impl<K: DbKey + Send + Sync> KvcReadable<K> for RocksDb {

    fn get_meta(&self) -> &str {
        self.path.to_str().unwrap_or_default()
    }

    fn try_get(&self, key: &K) -> Result<Option<DbSlice>> {
        Ok(self.db()?.get_pinned(key.key())?
            .map(|value| value.into()))
    }

    fn for_each(&self, predicate: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool>) -> Result<bool> {
        for (key, value) in self.db()?.iterator(IteratorMode::Start) {
            if !predicate(key.as_ref(), value.as_ref())? {
                return Ok(false);
            }
        }
        Ok(true)
    }

}

/// Implementation of writable key-value collection for RocksDB. Actual implementation is blocking.
impl<K: DbKey + Send + Sync> KvcWriteable<K> for RocksDb {
    fn put(&self, key: &K, value: &[u8]) -> Result<()> {
        self.db()?.put(key.key(), value)
            .map_err(|err| err.into())
    }

    fn delete(&self, key: &K) -> Result<()> {
        self.db()?.delete(key.key())
            .map_err(|err| err.into())
    }
}

/// Implementation of support for take snapshots for RocksDB.
impl<K: DbKey + Send + Sync> KvcSnapshotable<K> for RocksDb {
    fn snapshot<'db>(&'db self) -> Result<Arc<dyn KvcReadable<K> + 'db>> {
        Ok(Arc::new(RocksDbSnapshot(self.db()?.snapshot())))
    }
}

struct RocksDbSnapshot<'db>(Snapshot<'db>);

impl Debug for RocksDbSnapshot<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("[snapshot]")
    }
}

impl Kvc for RocksDbSnapshot<'_> {
    fn len(&self) -> Result<usize> {
        fail!("len() is not supported for RocksDb")
    }

    fn destroy(&mut self) -> Result<()> {
        fail!("destroy() is not supported for snapshots")
    }
}

impl<K: DbKey + Send + Sync> KvcReadable<K> for RocksDbSnapshot<'_> {
    fn try_get(&self, key: &K) -> Result<Option<DbSlice>> {
        Ok(self.0.get(key.key())?
            .map(|value| value.into()))
    }

    fn for_each(&self, predicate: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool>) -> Result<bool> {
        for (key, value) in self.0.iterator(IteratorMode::Start) {
            if !predicate(key.as_ref(), value.as_ref())? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// Implementation of transaction support for key-value collection for RocksDB.
impl<K: DbKey + Send + Sync> KvcTransactional<K> for RocksDb {
    fn begin_transaction(&self) -> Result<Box<dyn KvcTransaction<K>>> {
        Ok(Box::new(RocksDbTransaction::new(Arc::clone(&self.db))))
    }
}

pub struct RocksDbTransaction {
    db: Arc<Option<DB>>,
    batch: WriteBatch,
}

/// Implementation of transaction for key-value collection for RocksDB.
impl RocksDbTransaction {
    fn new(db: Arc<Option<DB>>) -> Self {
        Self {
            db,
            batch: WriteBatch::default()
        }
    }
}

impl<K: DbKey + Send + Sync> KvcTransaction<K> for RocksDbTransaction {
    fn put(&mut self, key: &K, value: &[u8]) {
        self.batch.put(key.key(), value);
    }

    fn delete(&mut self, key: &K) {
        self.batch.delete(key.key());
    }

    fn clear(&mut self) {
        self.batch.clear();
    }

    fn commit(self: Box<Self>) -> Result<()> {
        if let Some(ref db) = *self.db {
            db.write(self.batch)
            .map_err(|err| err.into())
        } else {
            Err(StorageError::DbIsDropped.into())
        }
    }

    fn len(&self) -> usize {
        self.batch.len()
    }

    fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }
}
