use crate::{db_impl_base, db::traits::KvcWriteable};

db_impl_base!(CatchainPersistentDb, KvcWriteable, ton_types::types::UInt256);
