use crate::{db::{filedb::FileDb, traits::KvcWriteableAsync}};
//#[cfg(test)]
//use crate::db::db::{async_adapter::KvcWriteableAsyncAdapter, memorydb::MemoryDb}; 
use std::{ops::{Deref, DerefMut}, path::Path};
use ton_block::BlockIdExt;

#[derive(Debug)]
pub struct ShardStatePersistentDb {
    db: Box<dyn KvcWriteableAsync<BlockIdExt>>,
}

impl ShardStatePersistentDb {
   
/*
    /// Constructs new instance using in-memory key-value collection
*/

    /// Constructs new instance using FileDb with given path
    pub fn with_path<P: AsRef<Path>>(path: P) -> Self {
        Self {
            db: Box::new(FileDb::with_path(path))
        }
    }

}

impl Deref for ShardStatePersistentDb {
    type Target = Box<dyn KvcWriteableAsync<BlockIdExt>>;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl DerefMut for ShardStatePersistentDb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.db
    }
}
