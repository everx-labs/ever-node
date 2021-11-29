use crate::{db_impl_base, db::traits::KvcWriteable};
use ton_block::BlockIdExt;

db_impl_base!(BlockInfoDb, KvcWriteable, BlockIdExt);
