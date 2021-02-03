use crate::{db::traits::DbKey, types::DbSlice};
use async_trait::async_trait;
use std::fmt::Debug;
use ton_types::Result;

/// Trait for key-value collections
#[async_trait]
pub trait KvcAsync: Debug + Send + Sync {
    /// Element count of collection
    async fn len(&self) -> Result<usize>;

    /// Returns true, if collection is empty; false otherwise
    async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Destroys this key-value collection and underlying database
    async fn destroy(&mut self) -> Result<()>;
}

/// Trait for readable key-value collections
#[async_trait]
pub trait KvcReadableAsync<K: DbKey>: KvcAsync {
    /// Tries to get value from collection by the key; returns Ok(None) if key not found
    async fn try_get(&self, key: &K) -> Result<Option<DbSlice>>;

    /// Gets value from collection by the key
    async fn get(&self, key: &K) -> Result<DbSlice>;

    /// Gets slice with given size starting from given offset from collection by the key
    async fn get_slice(&self, key: &K, offset: u64, size: u64) -> Result<DbSlice>;

    /// Gets the size of value by the key
    async fn get_size(&self, key: &K) -> Result<u64>;

    /// Determines, is key exists in key-value collection
    async fn contains(&self, key: &K) -> Result<bool>;
}

/// Trait for writable key-value collections
#[async_trait]
pub trait KvcWriteableAsync<K: DbKey>: KvcReadableAsync<K> {
    /// Puts value into collection by the key
    async fn put(&self, key: &K, value: &[u8]) -> Result<()>;

    /// Deletes value from collection by the key
    async fn delete(&self, key: &K) -> Result<()>;
}
