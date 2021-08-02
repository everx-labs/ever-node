use crate::{db::traits::DbKey, error::StorageError, types::DbSlice};
use std::{fmt::Debug, sync::Arc};
use ton_types::Result;

/// Trait for key-value collections
pub trait Kvc: Debug + Send + Sync {
    /// Element count of collection
    fn len(&self) -> Result<usize>;

    /// Returns true, if collection is empty; false otherwise
    fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Destroys this key-value collection and underlying database
    fn destroy(&mut self) -> Result<()>;
}

/// Trait for readable key-value collections
pub trait KvcReadable<K: DbKey + Send + Sync>: Kvc {
    /// Tries to get value from collection by the key; returns Ok(None) if the key not found
    fn try_get(&self, key: &K) -> Result<Option<DbSlice>>;

    /// Gets value from collection by the key
    fn get(&self, key: &K) -> Result<DbSlice> {
        self.try_get(key)?
            .ok_or_else(|| StorageError::KeyNotFound(key.key_name(), key.as_string()).into())
    }

    /// Gets slice with given size starting from given offset from collection by the key
    fn get_slice(&self, key: &K, offset: u64, size: u64) -> Result<DbSlice> {
        self.get(key).and_then(|value| {
            if offset >= value.len() as u64 || offset + size > value.as_ref().len() as u64 {
                return Err(StorageError::OutOfRange.into());
            }

            let mut result = vec![0u8; size as usize];
            result.copy_from_slice(&value[offset as usize..(offset + size) as usize]);
            Ok(result.into())
        })
    }

    /// Gets the size of value by the key
    fn get_size(&self, key: &K) -> Result<u64> {
        self.get(key).map(|value| value.len() as u64)
    }

    /// Determines, is key exists in key-value collection
    fn contains(&self, key: &K) -> Result<bool> {
        Ok(self.try_get(key)?.is_some())
    }

    /// Iterates over items in key-value collection, running predicate for each key-value pair
    fn for_each(&self, predicate: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool>) -> Result<bool>;
}

/// Trait for writable key-value collections
pub trait KvcWriteable<K: DbKey + Send + Sync>: KvcReadable<K> {
    /// Puts value into collection by the key
    fn put(&self, key: &K, value: &[u8]) -> Result<()>;

    /// Deletes value from collection by the key
    fn delete(&self, key: &K) -> Result<()>;
}

/// Trait for key-value collections with the ability of take snapshots
pub trait KvcSnapshotable<K: DbKey + Send + Sync>: KvcWriteable<K> {
    /// Takes snapshot from key-value collection
    fn snapshot<'db>(&'db self) -> Result<Arc<dyn KvcReadable<K> + 'db>>;
}

/// Trait for transactional key-value collections
pub trait KvcTransactional<K: DbKey + Send + Sync>: KvcSnapshotable<K> {
    /// Creates new transaction (batch)
    fn begin_transaction(&self) -> Result<Box<dyn KvcTransaction<K>>>;
}

/// Trait for transaction on key-value collection. The transaction must be committed before the
/// data actually being written into the collection. The transaction is automatically being aborted
/// on destroy, if not committed.
pub trait KvcTransaction<K: DbKey + Send + Sync> {
    /// Adds put operation into transaction (batch)
    fn put(&mut self, key: &K, value: &[u8]);

    /// Adds delete operation into transaction (batch)
    fn delete(&mut self, key: &K);

    /// Removes all pending operations from transaction (batch)
    fn clear(&mut self);

    /// Commits the transaction (batch)
    fn commit(self: Box<Self>) -> Result<()>;

    /// Gets pending operations count
    fn len(&self) -> usize;

    /// Returns true if pending operation count is zero; otherwise false
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
