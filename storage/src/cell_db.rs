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
    db_impl_base, 
    db::traits::{KvcTransaction, KvcTransactional, U32Key},
    types::{StorageCell},
};

use crate::dynamic_boc_rc_db::DynamicBocDb;

use std::{sync::Arc, io::{Cursor, Write}};
use ton_types::{Result, IndexedCellsStorage, RawCell, MAX_REFERENCES_COUNT, ByteOrderRead, UInt256};

db_impl_base!(CellDb, KvcTransactional, UInt256);

impl CellDb {
    /// Gets cell from key-value storage by cell id
    pub fn get_cell(
        &self, 
        cell_id: &UInt256,
        boc_db: Arc<DynamicBocDb>, 
        use_cache: bool,
        with_parents_count: bool,
    ) -> Result<(StorageCell, u32)> {
        StorageCell::deserialize(
            boc_db, 
            self.db.get(cell_id)?.as_ref(), 
            use_cache,
            with_parents_count,
        )
    }

    pub fn try_get_cell(
        &self,
        cell_id: &UInt256,
        boc_db: Arc<DynamicBocDb>,
        use_cache: bool,
        with_parents_count: bool,
    ) -> Result<Option<(StorageCell, u32)>> {
        if let Some(data) = self.db.try_get(cell_id)? {
            Ok(Some(StorageCell::deserialize(
                boc_db, 
                data.as_ref(), 
                use_cache,
                with_parents_count,
            )?))
        } else {
            Ok(None)
        }
    }

    /// Puts cell into transaction
    pub fn put_cell<T: KvcTransaction<UInt256> + ?Sized>(transaction: &mut T, cell_id: &UInt256, cell_data: &[u8]) -> Result<()> {
        transaction.put(cell_id, cell_data)?;
        Ok(())
    }
}

db_impl_base!(IndexedCellDb, KvcTransactional, U32Key);

impl IndexedCellsStorage for IndexedCellDb {

    fn insert(&mut self, index: u32, cell: RawCell) -> Result<()> {
        let mut data = Vec::with_capacity(cell.data.len() + 4 * MAX_REFERENCES_COUNT);
        for i in 0..MAX_REFERENCES_COUNT {
            data.write_all(&cell.refs[i].to_le_bytes())?;
        }
        data.write_all(&cell.data)?;
        self.db.put(&index.into(), &data)
    }
    fn remove(&mut self, index: u32) -> Result<RawCell> {
        let data = self.db.get(&index.into())?;
        let mut reader = Cursor::new(data.as_ref());
        let mut refs = [0_u32; MAX_REFERENCES_COUNT];
        for i in 0..MAX_REFERENCES_COUNT {
            refs[i] = reader.read_le_u32()?;
        }
        Ok(RawCell {
            data: data.as_ref()[MAX_REFERENCES_COUNT * 4..].into(),
            refs,
        })
    }
    fn cleanup(&mut self) -> Result<()> {
        self.db.destroy()?;
        Ok(())
    }
}