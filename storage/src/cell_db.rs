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

use crate::{
    db_impl_base, db::traits::{KvcTransaction, KvcTransactional},
    dynamic_boc_db::DynamicBocDb, types::{CellId, StorageCell}
};
use std::sync::Arc;
use ton_types::Result;

db_impl_base!(CellDb, KvcTransactional, CellId);

impl CellDb {
    /// Gets cell from key-value storage by cell id
    pub fn get_cell(&self, cell_id: &CellId, boc_db: Arc<DynamicBocDb>) -> Result<StorageCell> {
        StorageCell::deserialize(boc_db, self.db.get(cell_id)?.as_ref())
    }

    /// Puts cell into transaction
    pub fn put_cell<T: KvcTransaction<CellId> + ?Sized>(transaction: &mut T, cell_id: &CellId, cell_data: &[u8]) -> Result<()> {
        transaction.put(cell_id, cell_data);
        Ok(())
    }
}
