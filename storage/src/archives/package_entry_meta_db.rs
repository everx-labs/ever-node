use crate::{
    db_impl_cbor, archives::package_entry_meta::PackageEntryMeta, 
    db::traits::{KvcWriteable, U32Key}
};

db_impl_cbor!(PackageEntryMetaDb, KvcWriteable, U32Key, PackageEntryMeta);
