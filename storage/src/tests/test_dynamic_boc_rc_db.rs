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
    cell_db::CellDb, db::rocksdb::RocksDb, dynamic_boc_rc_db::DynamicBocDb, 
    tests::utils::*, StorageAlloc
};
#[cfg(feature = "telemetry")]
use crate::StorageTelemetry;
use std::sync::Arc;
use ton_types::Result;

//include!("../../../common/src/log.rs");

#[test]
fn test_dynamic_boc_rc_db() -> Result<()> {

    //init_log("../common/config/log_cfg.yml");

    let testname = "test_dynamic_boc_rc_db";
    let _ = std::fs::remove_dir_all(testname);

    let db = RocksDb::with_path(testname, testname)?;

    let boc_db = Arc::new(DynamicBocDb::with_db(
        Arc::new(CellDb::with_db(db.clone(), testname, true)?), 
        "",
        false,
        1_000_000,
        #[cfg(feature = "telemetry")]
        Arc::new(StorageTelemetry::default()),
        Arc::new(StorageAlloc::default()),
    ));

    let root_cell = get_test_tree_of_cells();
    let mut cells_counters = Some(fnv::FnvHashMap::default());

    assert!(boc_db.is_empty()?);
    let initial_cell_count = count_tree_unique_cells(root_cell.clone());
    boc_db.save_boc(root_cell.clone(), true, &|| Ok(()), &mut cells_counters, false)?;
    assert_eq!(boc_db.len()?, initial_cell_count * 2);

    let loaded_boc = boc_db.load_boc(&root_cell.repr_hash().into(), true)?;
    let fetched_count = count_tree_unique_cells(loaded_boc.clone());
    assert_eq!(fetched_count, initial_cell_count);
    assert_eq!(boc_db.cells_cache().lock().len(), initial_cell_count);

    let root_cell_2 = get_another_test_tree_of_cells();
    boc_db.save_boc(root_cell_2.clone(), true, &|| Ok(()), &mut cells_counters, false)?;

    boc_db.delete_boc(&root_cell.repr_hash().into(), &|| Ok(()), &mut cells_counters, false)?;
    assert!(boc_db.len()? > 0);

    boc_db.delete_boc(&root_cell_2.repr_hash().into(), &|| Ok(()), &mut cells_counters, false)?;
    assert_eq!(boc_db.len()?, 0);

    let _ = std::fs::remove_dir_all(testname);
    Ok(())
}

