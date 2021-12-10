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
    /// Constructs new instance using FileDb with given path
    pub fn with_path(path: impl AsRef<Path>) -> Self {
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
