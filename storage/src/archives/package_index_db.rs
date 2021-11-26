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

use crate::{db_impl_cbor, db::traits::{KvcWriteable, U32Key}};
use std::convert::TryInto;
use ton_types::Result;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct PackageIndexEntry {
    deleted: bool,
    finalized: bool,
}

impl PackageIndexEntry {
    pub const fn new() -> Self {
        Self::with_data(false, false)
    }

    pub const fn with_data(deleted: bool, finalized: bool) -> Self {
        Self { deleted, finalized }
    }

    pub const fn deleted(&self) -> bool {
        self.deleted
    }

    pub const fn finalized(&self) -> bool {
        self.finalized
    }
}

db_impl_cbor!(PackageIndexDb, KvcWriteable, U32Key, PackageIndexEntry);

impl PackageIndexDb {
    pub fn for_each_deserialized(&self, mut predicate: impl FnMut(u32, PackageIndexEntry) -> Result<bool>) -> Result<bool> {
        self.for_each(&mut |key_data, data| {
            let key = u32::from_le_bytes(key_data.try_into()?);
            let value = serde_cbor::from_slice(data)?;
            predicate(key, value)
        })
    }
}
