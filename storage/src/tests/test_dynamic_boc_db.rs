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

use crate::{dynamic_boc_db::DynamicBocDb, tests::utils::*};
use std::sync::Arc;
use ton_types::Result;
use std::ops::Deref;

#[test]
fn test_dynamic_boc_tree_creation() -> Result<()> {
    let root_cell = get_test_tree_of_cells();
    let dynamic_boc_db = Arc::new(DynamicBocDb::in_memory());
    let _written_count = dynamic_boc_db.save_as_dynamic_boc(root_cell.deref(), &|| Ok(()))?;
    let boc_root = dynamic_boc_db.load_dynamic_boc(&root_cell.repr_hash())?;
    compare_trees(root_cell, boc_root)?;

    Ok(())
}

#[test]
fn test_scenario() -> Result<()> {
    let db = Arc::new(DynamicBocDb::in_memory());
    assert!(db.is_empty()?);

    let root_cell = get_test_tree_of_cells();
    assert!(db.is_empty()?);
    let initial_cell_count = count_tree_unique_cells(root_cell.clone());
    let _written_count = db.save_as_dynamic_boc(root_cell.deref(), &|| Ok(()))?;
    assert_eq!(db.len()?, initial_cell_count);

    let loaded_boc = db.load_dynamic_boc(&root_cell.repr_hash().into())?;
    let fetched_count = count_tree_unique_cells(loaded_boc.clone());
    assert_eq!(fetched_count, initial_cell_count);
    assert_eq!(db.cells_map().read().len(), initial_cell_count);

    compare_trees(root_cell.clone(), loaded_boc)?;

    Ok(())
}