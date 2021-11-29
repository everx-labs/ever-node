use crate::{db_impl_base, db::traits::KvcWriteable};

db_impl_base!(NodeStateDb, KvcWriteable, &'static str);
