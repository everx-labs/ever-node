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

use rocksdb::DBPinnableSlice;
use std::ops::Deref;

/// Represents memory slice, returned by database (in a case of RocksDB), or vector, in a case of MemoryDb
pub enum DbSlice<'a> {
    RocksDbTable(DBPinnableSlice<'a>),
    Vector(Vec<u8>),
    Slice(&'a [u8]),
}

impl AsRef<[u8]> for DbSlice<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            DbSlice::RocksDbTable(slice) => slice.as_ref(),
            DbSlice::Vector(vector) => vector.as_slice(),
            DbSlice::Slice(slice) => *slice,
        }
    }
}

impl Deref for DbSlice<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<'a> From<DBPinnableSlice<'a>> for DbSlice<'a> {
    fn from(slice: DBPinnableSlice<'a>) -> Self {
        DbSlice::RocksDbTable(slice)
    }
}

impl<'a> From<&'a [u8]> for DbSlice<'a> {
    fn from(slice: &'a [u8]) -> Self {
        DbSlice::Slice(slice)
    }
}

impl<'a> From<Vec<u8>> for DbSlice<'a> {
    fn from(vector: Vec<u8>) -> Self {
        DbSlice::Vector(vector)
    }
}
