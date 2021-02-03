use crate::{
    db_impl_cbor, archives::package_entry_id::PackageEntryId, 
    db::traits::{DbKey, KvcWriteable}
};
use std::{borrow::Borrow, collections::hash_map::DefaultHasher, hash::{Hash, Hasher}};
use ton_api::ton::PublicKey;
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
        PK: Borrow<PublicKey> + Hash
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
    PK: Borrow<PublicKey> + Hash
{
    fn from(entry_id: &PackageEntryId<B, U256, PK>) -> Self {
        Self::from_entry_type(&entry_id)
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
