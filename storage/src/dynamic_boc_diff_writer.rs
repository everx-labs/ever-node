use crate::{cell_db::CellDb, dynamic_boc_diff::DynamicBocDiff, types::CellId};
use std::sync::{Arc, RwLock, Weak};
use ton_types::{Cell, Result};

#[derive(Debug)]
pub(super) struct DynamicBocDiffFactory {
    db: Arc<CellDb>,
    diff: RwLock<Weak<DynamicBocDiff>>,
}

impl DynamicBocDiffFactory {
    pub fn new(db: Arc<CellDb>) -> Self {
        Self {
            db,
            diff: RwLock::new(Weak::new()),
        }
    }

    pub fn construct(&self) -> DynamicBocDiffWriter {
        // TODO: Temporary disabled behavior because of issues with saving under high load
        DynamicBocDiffWriter::new({
            // let mut guard = self.diff.write()
            //     .expect("Poisoned RwLock");
            // match Weak::upgrade(&guard) {
                // Some(diff) => diff,
                // None => {
                    let diff = Arc::new(DynamicBocDiff::new(Arc::clone(&self.db)));
                    // *guard = Arc::downgrade(&diff);
                    diff
                // }
            // }
        })
    }
}

pub struct DynamicBocDiffWriter {
    diff: Arc<DynamicBocDiff>,
}

impl DynamicBocDiffWriter {
    fn new(diff: Arc<DynamicBocDiff>) -> Self {
        Self { diff }
    }

    pub fn add_cell(&self, cell_id: CellId, cell: Cell) {
        self.diff.add_cell(cell_id, cell)
    }

    pub fn delete_cell(&self, cell_id: &CellId) {
        self.diff.delete_cell(cell_id)
    }

    pub fn apply(self) -> Result<()> {
        if let Ok(diff) = Arc::try_unwrap(self.diff) {
            return diff.apply();
        }

        // TODO: Make function async and do not return until data is saved

        Ok(())
    }
}
