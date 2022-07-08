/*
* Copyright (C) 2019-2022 TON Labs. All Rights Reserved.
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

use std::{collections::HashMap, sync::Arc, ops::Deref};

use crate::db::{
    rocksdb::{RocksDb, RocksDbTable},
    traits::KvcTransactional,
};
use ton_types::Result;

/// The table contains only keys (values are empty) with has next struct (see prepare_key)
/// - hash (32 bytes)
/// - workchain id (i32, 4 bytes, big endian)
/// - adderess (32 bytes)
#[derive(Clone)]
pub struct U256Index(Arc<RocksDbTable>);

impl U256Index {
    pub fn with_db(db: Arc<RocksDb>, family: impl ToString) -> Result<Self> {
        let table = Arc::new(db.table(family)?);
        Ok(Self(table))
    }

    fn table(&self) -> &RocksDbTable {
        self.0.deref()
    }

    pub fn get_by_prefix(&self, prefix: impl AsRef<[u8]>) -> Vec<Box<[u8]>> {
        self.table().get_by_prefix(&prefix)
    }

    pub fn clear(&self) -> Result<()> {
        self.table().drop_data()
    }
}

#[derive(Eq, PartialEq, Clone, Copy)]
enum Action {
    Add,
    Delete
}

/// The batch contains unapplied operations to the index table (add or delete row)
/// Get (see get_by_prefix) operation returns list of keys with given prefix (with same hash)
pub struct U256IndexBatch {
    index: U256Index,
    // TODO: need to have more progressive structure with optimization by prefix
    batch: HashMap<Vec<u8>, Action>,
}

impl U256IndexBatch {
    #[doc = "Constructs new index table using RocksDB with given family"]
    pub fn with_index(index: U256Index) -> Self {
        let batch = HashMap::new();
        Self { index, batch }
    }

    pub fn merge(&mut self, other: &Self) {
        for (key, add) in other.batch.iter() {
            match self.batch.get_mut(key) {
                Some(value) => {
                    if value != add {
                        *value = *add
                    }
                }
                None => {
                    self.batch.insert(key.clone(), *add);
                }
            }
        }
    }

    pub fn commit(&self) -> Result<()> {
        let mut batch = self.index.table().begin_transaction()?;
        for (key, action) in self.batch.iter() {
            match action {
                Action::Add => batch.put(&key.as_slice(), &[]),
                Action::Delete => batch.delete(&key.as_slice()),
            }
        }
        batch.commit()
    }

    pub fn put(&mut self, key: Vec<u8>, workchain_id: i32, address: impl AsRef<[u8]>) {
        let key = Self::prepare_key(key, workchain_id, address);
        self.batch.insert(key, Action::Add);
    }

    pub fn delete(&mut self, key: Vec<u8>, workchain_id: i32, address: impl AsRef<[u8]>) {
        let key = Self::prepare_key(key, workchain_id, address);
        self.batch.insert(key, Action::Delete);
    }

    pub fn get_by_prefix(&self, prefix: impl AsRef<[u8]>) -> Vec<Box<[u8]>> {
        let prefix = prefix.as_ref();
        let prefix_len = prefix.len();
        let mut keys = self.index.get_by_prefix(&prefix);
        // need to get only with prefix
        for (key, add) in self.batch.iter() {
            if &key[..prefix_len] != prefix {
                continue;
            }
            let pos = keys.iter().position(|v| key.as_slice() == v.as_ref());
            match add {
                Action::Add => {
                    if pos.is_none() {
                        keys.push(key.clone().into_boxed_slice());
                    }
                }
                Action::Delete => {
                    if let Some(pos) = pos {
                        keys.swap_remove(pos);
                    }
                }
            }
        }
        keys
    }

    fn prepare_key(mut key: Vec<u8>, workchain_id: i32, address: impl AsRef<[u8]>) -> Vec<u8> {
        debug_assert_eq!(key.len(), 32);
        key.extend_from_slice(&workchain_id.to_be_bytes());
        key.extend_from_slice(address.as_ref());
        key
    }
}

