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

#![cfg(test)]

use crate::{
    db::traits::{DbKey, KvcAsync, KvcReadableAsync, KvcWriteable, KvcWriteableAsync},
    types::DbSlice
};
use async_trait::async_trait;
use std::{fmt::Debug, marker::PhantomData, ops::Deref};
use ton_types::Result;

/// This facade wraps key-value collections implementing sync traits into async traits
#[derive(Debug)]
pub struct KvcWriteableAsyncAdapter<K: DbKey + Debug + Send + Sync, T: KvcWriteable<K>> {
    kvc: T,
    phantom: PhantomData<K>,
}

impl<K: DbKey + Debug + Send + Sync, T: KvcWriteable<K>> KvcWriteableAsyncAdapter<K, T> {
/*
    pub fn new(kvc: T) -> Self {
        Self { kvc, phantom: PhantomData::default() }
    }
*/

    pub fn kvc(&self) -> &T {
        &self.kvc
    }
}

impl<K: DbKey + Debug + Send + Sync, T: KvcWriteable<K>> Deref for KvcWriteableAsyncAdapter<K, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.kvc()
    }
}

#[async_trait]
impl<K: DbKey + Debug + Send + Sync, T: KvcWriteable<K>> KvcAsync for KvcWriteableAsyncAdapter<K, T> {
    async fn len(&self) -> Result<usize> {
        self.kvc.len()
    }
    async fn is_empty(&self) -> Result<bool> {
        self.kvc.is_empty()
    }
    async fn destroy(&mut self) -> Result<bool> {
        self.kvc.destroy()
    }
}

#[async_trait]
impl<K: DbKey + Debug + Send + Sync, T: KvcWriteable<K>> KvcReadableAsync<K> for KvcWriteableAsyncAdapter<K, T> {
    async fn try_get<'a>(&'a self, key: &K) -> Result<Option<DbSlice<'a>>> {
        self.kvc.try_get(key)
    }

    async fn get<'a>(&'a self, key: &K) -> Result<DbSlice<'a>> {
        self.kvc.get(key)
    }

    async fn get_slice<'a>(&'a self, key: &K, offset: u64, size: u64) -> Result<DbSlice<'a>> {
        self.kvc.get_slice(key, offset, size)
    }

    async fn get_vec(&self, key: &K, offset: u64, size: u64) -> Result<Vec<u8>> {
        self.kvc.get_vec(key, offset, size)
    }

    async fn get_size(&self, key: &K) -> Result<u64> {
        self.kvc.get_size(key)
    }

    async fn contains(&self, key: &K) -> Result<bool> {
        self.kvc.contains(key)
    }

    fn for_each_key(&self, _predicate: &mut dyn FnMut(&[u8]) -> Result<bool>) -> Result<bool> {
        unimplemented!()
    }
}

#[async_trait]
impl<K: DbKey + Debug + Send + Sync, T: KvcWriteable<K>> KvcWriteableAsync<K> for KvcWriteableAsyncAdapter<K, T> {
    async fn put(&self, key: &K, value: &[u8]) -> Result<()> {
        self.kvc.put(key, value)
    }

    async fn delete(&self, key: &K) -> Result<()> {
        self.kvc.delete(key)
    }
}
