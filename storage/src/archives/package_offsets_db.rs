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
    db_impl_cbor, archives::package_entry_id::PackageEntryId, 
    db::traits::{DbKey, KvcWriteable}
};
use std::{borrow::Borrow, collections::hash_map::DefaultHasher, hash::{Hash, Hasher}};
use ton_block::BlockIdExt;
use ton_types::UInt256;

pub struct PackageOffsetKey {
    entry_id_hash: [u8; 8],
}

impl PackageOffsetKey {
    pub fn from_entry_type<B, U256, PK>(entry_id: &PackageEntryId<B, U256, PK>) -> Self
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<UInt256> + Hash
    {
        let mut hasher = DefaultHasher::new();
        entry_id.hash(&mut hasher);

        Self { entry_id_hash: hasher.finish().to_le_bytes() }
    }
}

impl<B, U256, PK> From<&PackageEntryId<B, U256, PK>> for PackageOffsetKey
where
    B: Borrow<BlockIdExt> + Hash,
    U256: Borrow<UInt256> + Hash,
    PK: Borrow<UInt256> + Hash
{
    fn from(entry_id: &PackageEntryId<B, U256, PK>) -> Self {
        Self::from_entry_type(entry_id)
    }
}

impl DbKey for PackageOffsetKey {
    fn key_name(&self) -> &'static str {
        "PackageOffsetKey"
    }

    fn key(&self) -> &[u8] {
        &self.entry_id_hash
    }
}

db_impl_cbor!(PackageOffsetsDb, KvcWriteable, PackageOffsetKey, u64);
