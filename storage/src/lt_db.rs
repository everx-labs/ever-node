use crate::{db_impl_cbor, db::traits::KvcWriteable, types::{LtDbEntry, LtDbKey}};

db_impl_cbor!(LtDb, KvcWriteable, LtDbKey, LtDbEntry);
