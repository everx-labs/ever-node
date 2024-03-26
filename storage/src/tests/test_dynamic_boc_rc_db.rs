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
use ton_block::CellsFactory;
use ton_types::{BuilderData, Cell, IBitstring, Result};

include!("../db/tests/destroy_db.rs");
include!("../../../common/src/log.rs");

const DB_PATH: &str = "../target/test";

#[tokio::test(flavor = "multi_thread")]
async fn test_dynamic_boc_rc_db() -> Result<()> {

    //init_log("../common/config/log_cfg.yml");

    const DB_NAME: &str = "test_dynamic_boc_rc_db";

    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();

    let db = RocksDb::with_path(DB_PATH, DB_NAME)?;
    let boc_db = Arc::new(DynamicBocDb::with_db(
        Arc::new(CellDb::with_db(db.clone(), DB_NAME, true)?), 
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
    assert_eq!(boc_db.cells_cache_len(), initial_cell_count);

    let root_cell_2 = get_another_test_tree_of_cells();
    boc_db.save_boc(root_cell_2.clone(), true, &|| Ok(()), &mut cells_counters, false)?;

    boc_db.delete_boc(&root_cell.repr_hash().into(), &|| Ok(()), &mut cells_counters, false)?;
    assert!(boc_db.len()? > 0);

    boc_db.delete_boc(&root_cell_2.repr_hash().into(), &|| Ok(()), &mut cells_counters, false)?;
    assert_eq!(boc_db.len()?, 0);

    drop(boc_db);
    drop(db);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();
    Ok(())

}

#[tokio::test(flavor = "multi_thread")]
async fn test_dynamic_boc_rc_db_2() -> Result<()> {

    init_log("../common/config/log_cfg_debug.yml");
    println!();

    const DB_NAME: &str = "test_dynamic_boc_rc_db_2";

    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();

    let db = RocksDb::with_path(DB_PATH, DB_NAME)?;
    let boc_db = Arc::new(DynamicBocDb::with_db(
        Arc::new(CellDb::with_db(db.clone(), DB_NAME, true)?), 
        "",
        false,
        1_000_000,
        #[cfg(feature = "telemetry")]
        Arc::new(StorageTelemetry::default()),
        Arc::new(StorageAlloc::default()),
    ));

    let cells_factory = boc_db.clone() as Arc<dyn CellsFactory>;
    let create_ss = |cells_chain: Vec<&str>| -> Cell {
        let mut child = None;
        let mut cell = Cell::default();
        for data in cells_chain.iter().rev() {
            let mut builder = BuilderData::new();
            let mut data = data.as_bytes().to_vec();
            data.push(0x80);
            builder.append_bitstring(&data).unwrap();
            if let Some(child) = child {
                builder.checked_append_reference(child).unwrap();
            }
            cell = cells_factory.clone().create_cell(builder).unwrap();
            child = Some(cell.clone());
        }
        cell
    };
    let check_stop = || Ok(());

    let r1 = create_ss(vec!["r1", "c1", "A", "B"]);
    let r1_id = r1.repr_hash();
    boc_db.save_boc(r1, true, &check_stop, &mut None, false).unwrap();

    let r2 = create_ss(vec!["r2", "c2", "A", "B"]);
    let r2_id = r2.repr_hash();
    boc_db.save_boc(r2, true, &check_stop, &mut None, false).unwrap();

    boc_db.delete_boc(&r1_id, &check_stop, &mut None, false).unwrap();
    boc_db.delete_boc(&r2_id, &check_stop, &mut None, false).unwrap();

    let r3 = create_ss(vec!["r3", "c3", "B"]);
    let r3_id = r3.repr_hash();
    boc_db.save_boc(r3, true, &check_stop, &mut None, false).unwrap();

    boc_db.delete_boc(&r3_id, &check_stop, &mut None, false).unwrap();

    let r4 = create_ss(vec!["r4", "c4", "A", "B"]);
    boc_db.save_boc(r4, true, &check_stop, &mut None, false).unwrap();

    drop(cells_factory);
    drop(boc_db);
    drop(db);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();
    Ok(())

}

