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
    async fn destroy(&mut self) -> Result<bool>;
}

/// Trait for readable key-value collections
#[async_trait]
pub trait KvcReadableAsync<K: DbKey + Sync>: KvcAsync {
    /// Tries to get value from collection by the key; returns Ok(None) if key not found
    async fn try_get(&self, key: &K) -> Result<Option<DbSlice>>;

    /// Gets value from collection by the key
    async fn get(&self, key: &K) -> Result<DbSlice>;

    /// Gets slice with given size starting from given offset from collection by the key
    async fn get_slice(&self, key: &K, offset: u64, size: u64) -> Result<DbSlice>;

    /// Gets vector with copy of data with given size starting from given offset 
    /// from collection by the key
    async fn get_vec(&self, key: &K, offset: u64, size: u64) -> Result<Vec<u8>>;

    /// Gets the size of value by the key
    async fn get_size(&self, key: &K) -> Result<u64>;

    /// Determines, is key exists in key-value collection
    async fn contains(&self, key: &K) -> Result<bool>;

    /// Iterates over items in key-value collection, running predicate for each key
    fn for_each_key(&self, predicate: &mut dyn FnMut(&[u8]) -> Result<bool>) -> Result<bool>;
}

/// Trait for writable key-value collections
#[async_trait]
pub trait KvcWriteableAsync<K: DbKey + Sync>: KvcReadableAsync<K> {
    /// Puts value into collection by the key
    async fn put(&self, key: &K, value: &[u8]) -> Result<()>;

    /// Deletes value from collection by the key
    async fn delete(&self, key: &K) -> Result<()>;
}
