use crate::{cell_db::CellDb, types::CellId};
use fnv::FnvHashMap;
use std::sync::{Arc, RwLock};
use ton_types::Result;

#[derive(Debug)]
pub(super) struct DynamicBocDiff {
    db: Arc<CellDb>,
    diff: RwLock<FnvHashMap<CellId, Option<Vec<u8>>>>,
}

impl DynamicBocDiff {
    pub fn new(db: Arc<CellDb>) -> Self {
        Self {
            db,
            diff: RwLock::new(FnvHashMap::default()),
        }
    }

    pub fn add_cell(&self, cell_id: CellId, cell_data: Vec<u8>) {
        self.diff.write()
            .expect("Poisoned RwLock")
            .insert(cell_id, Some(cell_data));
    }

    pub fn contains_cell(&self, cell_id: &CellId) -> bool {
        self.diff.read()
            .expect("Poisoned RwLock")
            .contains_key(cell_id)
    }

    pub fn delete_cell(&self, cell_id: &CellId) {
        let mut write_guard = self.diff.write()
            .expect("Poisoned RwLock");
        if !write_guard.contains_key(cell_id) {
            write_guard.insert(cell_id.clone(), None);
        }
    }

    pub fn apply(self) -> Result<()> {
        let transaction = self.db.begin_transaction()?;

        for (cell_id, cell_data_opt) in self.diff.write()
            .expect("Poisoned RwLock")
            .drain()
        {
            match cell_data_opt {
                Some(cell_data) => CellDb::put_cell(&*transaction, &cell_id, &cell_data)?,
                None => transaction.delete(&cell_id),
            }
        }

        transaction.commit()
    }
}
