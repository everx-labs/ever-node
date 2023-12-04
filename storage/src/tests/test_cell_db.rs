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
    cell_db::CellDb, tests::utils::*, types::{StorageCell}
};
use std::{io::Cursor, sync::Arc};
use ton_types::{Cell, Result, cells_serialization::deserialize_tree_of_cells, UInt256};
use std::ops::Deref;

use crate::dynamic_boc_db::DynamicBocDb;

fn get_test_cell() -> Result<(UInt256, Cell)> {
    let data = get_test_raw_boc();
    let root = deserialize_tree_of_cells(&mut Cursor::new(data))?;
    let cell_id = root.repr_hash();

    Ok((cell_id, root))
}

#[test]
fn test_get_put_cell() -> Result<()> {
    let boc_db = Arc::new(DynamicBocDb::in_memory());
    assert!(boc_db.is_empty()?);

    let (cell_id, cell) = get_test_cell()?;

    let mut transaction = boc_db.begin_transaction()?;

    CellDb::put_cell(transaction.as_mut(), &cell_id, &StorageCell::serialize(cell.deref())?)?;
    assert_eq!(transaction.len(), 1);
    assert_eq!(boc_db.len()?, 0);

    transaction.commit()?;
    assert_eq!(boc_db.len()?, 1);

    let result = Cell::with_cell_impl(
        boc_db.get_cell(&cell_id, Arc::clone(&boc_db),
        true
    )?);
    assert_eq!(result, cell);

    assert_eq!(result.tree_cell_count(), cell.tree_cell_count());
    assert_eq!(result.tree_bits_count(), cell.tree_bits_count());

    Ok(())
}
