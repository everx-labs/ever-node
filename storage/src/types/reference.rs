use crate::{/*error::StorageError,*/ types::StorageCell};
use std::sync::Arc;
use ton_types::{types::UInt256/*, Result*/};

#[derive(Clone, PartialEq, Debug)]
pub enum Reference {
    Loaded(Arc<StorageCell>),
    NeedToLoad(UInt256),
}

impl Reference {

/*
    pub fn unwrap(&self) -> Arc<StorageCell> {
        if let Reference::Loaded(cell) = self {
            Arc::clone(&cell)
        } else {
            panic!("Cell is not loaded. Need to load.")
        }
    }
*/

/*
    pub fn as_result(&self) -> Result<Arc<StorageCell>> {
        if let Reference::Loaded(ref cell) = self {
            Ok(Arc::clone(&cell))
        } else {
            Err(StorageError::ReferenceNotLoaded)?
        }
    }
*/

    pub fn hash(&self) -> UInt256 {
        match self {
            Reference::Loaded(cell) => cell.repr_hash(),
            Reference::NeedToLoad(hash) => hash.clone(),
        }
    }
}
