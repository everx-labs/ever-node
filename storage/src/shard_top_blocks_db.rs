use crate::db_impl_base;
use crate::db::traits::KvcWriteable;

db_impl_base!(ShardTopBlocksDb, KvcWriteable, Vec<u8>);
