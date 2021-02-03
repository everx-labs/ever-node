use crate::{cell_db::CellDb, types::CellId};
use fnv::FnvHashMap;
use std::sync::{Arc, RwLock};
use ton_types::{Cell, Result};

#[derive(Debug)]
pub(super) struct DynamicBocDiff {
    db: Arc<CellDb>,
    diff: RwLock<FnvHashMap<CellId, Option<Cell>>>,
}

impl DynamicBocDiff {
    pub fn new(db: Arc<CellDb>) -> Self {
        Self {
            db,
            diff: RwLock::new(FnvHashMap::default()),
        }
    }

    pub fn add_cell(&self, cell_id: CellId, cell: Cell) {
        self.diff.write()
            .expect("Poisoned RwLock")
            .insert(cell_id, Some(cell));
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

        for (cell_id, cell_opt) in self.diff.write()
            .expect("Poisoned RwLock")
            .drain()
        {
            match cell_opt {
                Some(cell) => CellDb::put_cell(&*transaction, &cell_id, cell)?,
                None => transaction.delete(&cell_id),
            }
        }

        transaction.commit()
    }
}
