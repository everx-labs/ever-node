use crate::{db_impl_cbor, db::traits::KvcWriteable, types::{LtDesc, ShardIdentKey}};

db_impl_cbor!(LtDescDb, KvcWriteable, ShardIdentKey, LtDesc);
