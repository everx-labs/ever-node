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

use crate::{/*error::StorageError,*/ types::StorageCell};
use std::sync::Arc;
use ton_types::{types::UInt256/*, Result*/};

#[derive(Clone, PartialEq)]
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
