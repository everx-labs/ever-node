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
use std::sync::{Arc, Mutex};
use fnv::FnvHashMap;
use ton_types::Result;

pub type MemoryDbMap = FnvHashMap<Vec<u8>, Vec<u8>>;

/// In-memory key-value collection
#[derive(Debug, Clone)]
pub struct MemoryDb {
    map: Arc<Option<Mutex<MemoryDbMap>>>
}

impl Default for MemoryDb {
    fn default() -> Self {
        Self::new()
    }
}

/// Implementation of in-memory key-value collection
impl MemoryDb {
    /// Constructs empty collection
    pub fn new() -> Self {
        Self::with_map(FnvHashMap::default())
    }

    fn with_map(map: FnvHashMap<Vec<u8>, Vec<u8>>) -> Self {
        Self {
            map: Arc::new(Some(Mutex::new(map)))
        }
    }

    fn map(&self) -> Result<&Mutex<MemoryDbMap>> {
        if let Some(ref map) = *self.map {
            Ok(map)
        } else {
            Err(StorageError::DbIsDropped.into())
        }
    }
}

/// Implementation of key-value collection for MemoryDb
impl Kvc for MemoryDb {
    fn len(&self) -> Result<usize> {
        Ok(self.map()?
            .lock().unwrap()
            .len())
    }

    fn is_empty(&self) -> Result<bool> {
        Ok(self.map()?
            .lock().unwrap()
            .is_empty())
    }

    fn destroy(&mut self) -> Result<bool> {
        if Arc::get_mut(&mut self.map)
            .ok_or(StorageError::HasActiveTransactions)?
            .is_some()
        {
            self.map = Arc::new(None);
        }
        Ok(true)
    }
}

/// Implementation of readable key-value collection for MemoryDb. Actual implementation is blocking.
impl<K: DbKey + Send + Sync> KvcReadable<K> for MemoryDb {
    fn try_get(&self, key: &K) -> Result<Option<DbSlice>> {
        Ok(self.map()?
            .lock().unwrap()
            .get(key.key())
            .map(|vec| vec.clone().into()))
    }

    fn contains(&self, key: &K) -> Result<bool> {
        Ok(self.map()?
            .lock().unwrap()
            .contains_key(key.key()))
    }

    fn for_each(&self, predicate: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool>) -> Result<bool> {
        let mut pairs = Vec::with_capacity(self.map()?.lock().unwrap().len());
        for (key, value) in self.map()?.lock().unwrap().iter() {
            pairs.push((key.clone(), value.clone()));
        }

        for (key, value) in pairs {
            if !predicate(&key[..], &value[..])? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// Implementation of wriatable key-value collection for MemoryDb. Actual implementation is blocking.
impl<K: DbKey + Send + Sync> KvcWriteable<K> for MemoryDb {
    fn put(&self, key: &K, value: &[u8]) -> Result<()> {
        self.map()?
            .lock().unwrap()
            .insert(key.key().to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&self, key: &K) -> Result<()> {
        self.map()?
            .lock().unwrap()
            .remove(key.key());
        Ok(())
    }
}

/// Implementation of support for take snapshots for MemoryDb.
impl<K: DbKey + Send + Sync> KvcSnapshotable<K> for MemoryDb {
    fn snapshot<'db>(&'db self) -> Result<Arc<dyn KvcReadable<K> + 'db>> {
        Ok(Arc::new(Self::with_map(self.map()?.lock().unwrap().clone())))
    }
}

/// Implementation of transaction support for key-value collection for MemoryDb.
impl<K: DbKey + Send + Sync> KvcTransactional<K> for MemoryDb {
    fn begin_transaction(&self) -> Result<Box<dyn KvcTransaction<K>>> {
        Ok(Box::new(MemoryDbTransaction::new(Arc::clone(&self.map))))
    }
}

#[derive(Debug)]
struct Pair {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Debug)]
enum PendingOperation {
    Put(Pair),
    Delete(Vec<u8>),
}

#[derive(Debug)]
pub struct MemoryDbTransaction {
    db_map: Arc<Option<Mutex<MemoryDbMap>>>,
    pending: Mutex<Vec<PendingOperation>>,
}

/// Implementation of transaction for MemoryDb.
impl MemoryDbTransaction {
    fn new(db_map: Arc<Option<Mutex<MemoryDbMap>>>) -> Self {
        Self {
            db_map,
            pending: Mutex::new(Vec::new()),
        }
    }
}

impl<K: DbKey + Send + Sync> KvcTransaction<K> for MemoryDbTransaction {
    fn put(&mut self, key: &K, value: &[u8]) {
        self.pending.lock().unwrap().push(
            PendingOperation::Put(
                Pair {
                    key: key.key().to_vec(),
                    value: value.to_vec(),
                }
            )
        );
    }

    fn delete(&mut self, key: &K) {
        self.pending.lock().unwrap().push(
            PendingOperation::Delete(key.key().to_vec())
        );
    }

    fn clear(&mut self) {
        self.pending.lock().unwrap().clear();
    }

    fn commit(self: Box<Self>) -> Result<()> {
        let mut guard = self.db_map.as_ref().as_ref()
            .ok_or(StorageError::DbIsDropped)?
            .lock().unwrap();
        for operation in self.pending.lock().unwrap().drain(..) {
            match operation {
                PendingOperation::Put(pair) => guard.insert(pair.key, pair.value),
                PendingOperation::Delete(key) => guard.remove(&key),
            };
        }

        Ok(())
    }

    fn len(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    fn is_empty(&self) -> bool {
        self.pending.lock().unwrap().is_empty()
    }
}
