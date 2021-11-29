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
