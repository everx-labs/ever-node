/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific EVERX DEV software governing permissions and
* limitations under the License.
*/

use crate::{
    db::rocksdb::{RocksDb, destroy_rocks_db},
    shardstate_db_async::{AllowStateGcResolver, CellsDbConfig, ShardStateDb}, StorageAlloc,
};
#[cfg(feature = "telemetry")]
use crate::StorageTelemetry;
use std::{
    fs::{read, File}, io::copy, path::Path, sync::{atomic::AtomicU32, Arc}, time::Duration
};
use ever_block::{BlockIdExt, ShardIdent};
use ever_block::{Cell, Result, UInt256, read_single_root_boc};

include!("../../../common/src/log.rs");

const DB_PATH: &str = "../target/test";

fn ss_from_file(index: u32) -> Cell {
    let path = format!("src/tests/testdata/{}", index);
    let orig_bytes = read(Path::new(&path))
        .unwrap_or_else(|_| panic!("Error reading file {}", path));

    read_single_root_boc(orig_bytes)
        .expect("Error deserializing shard-state")
}

struct MockedResolver;

impl AllowStateGcResolver for MockedResolver {
    fn allow_state_gc(
        &self, 
        _nw_id: i32, 
        block_id: &BlockIdExt, 
        _saved_at: u64, 
        _gc_utime: u64
    ) -> Result<bool> {
        Ok(block_id.seq_no() > 2_467_100)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shardstate_db_async() -> Result<()> {

    init_log("../common/config/log_cfg_debug.yml");

    const DB_NAME: &str = "test_shardstate_db_async";

    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();

    let db = RocksDb::with_path(DB_PATH, DB_NAME)?;
    let ss_db = ShardStateDb::new(
        db.clone(),
        "shardstate_db",
        "cell_db",
        DB_NAME,
        CellsDbConfig {
            states_db_queue_len: 1000,
            max_pss_slowdown_mcs: 1000,
            prefill_cells_counters: false,
            cache_cells_counters: true,
            cache_size_bytes: 10000000,
        },
        #[cfg(feature = "telemetry")]
        Arc::new(StorageTelemetry::default()),
        Arc::new(StorageAlloc::default()),
    )?;

    ss_db.clone().start_gc(Arc::new(MockedResolver), Arc::new(AtomicU32::new(1)));

    let range = 2_467_080..2_467_119;
    for i in range.clone() {
        let root_cell = ss_from_file(i);
        let block_id_ext = BlockIdExt::with_params(
            ShardIdent::with_tagged_prefix(-1, 0x8000_0000_0000_0000)?,
            i,
            root_cell.repr_hash(),
            UInt256::default()
        );
        ss_db.put(&block_id_ext, root_cell.clone(), None).await?;
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    for i in range {
        let root_cell = ss_from_file(i);
        let block_id_ext = BlockIdExt::with_params(
            ShardIdent::with_tagged_prefix(-1, 0x8000_0000_0000_0000)?,
            i,
            root_cell.repr_hash(),
            UInt256::default()
        );
        let res = ss_db.get(&block_id_ext, true);

        if block_id_ext.seq_no() > 2_467_100 {
            if res.is_ok() {
                panic!("Should be error");
            }
        } else {
            let loaded_root_cell = res.unwrap();
            assert_eq!(root_cell, loaded_root_cell);
        }
    }

    ss_db.stop().await;

    drop(ss_db);
    drop(db);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();
    Ok(())

}

fn unzip_folder(zip_path: &str, target_path: &str) -> Result<()> {
    let zip_file = File::open(zip_path)?;
    let mut archive = zip::ZipArchive::new(zip_file)?;

    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let outpath = Path::new(target_path).join(file.mangled_name());

        if file.name().ends_with('/') {
            std::fs::create_dir_all(&outpath)?;
        } else {
            if let Some(p) = outpath.parent() {
                if !p.exists() {
                    std::fs::create_dir_all(&p)?;
                }
            }
            let mut outfile = File::create(&outpath)?;
            copy(&mut file, &mut outfile)?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shardstate_db_migration() -> Result<()> {

    std::env::set_var("RUST_BACKTRACE", "full");

    init_log("../common/config/log_cfg_debug.yml");

    const DB_NAME: &str = "shardstate_db_v5";

    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();

    unzip_folder("src/tests/testdata/shardstate_db_v5.zip", "../target/test")?;

    let db = RocksDb::with_path(DB_PATH, DB_NAME)?;
    let ss_db = ShardStateDb::new(
        db.clone(),
        "shardstate_db",
        "cell_db_v5",
        DB_NAME,
        CellsDbConfig {
            states_db_queue_len: 1000,
            max_pss_slowdown_mcs: 1000,
            prefill_cells_counters: false,
            cache_cells_counters: true,
            cache_size_bytes: 10000000,
        },
        #[cfg(feature = "telemetry")]
        Arc::new(StorageTelemetry::default()),
        Arc::new(StorageAlloc::default()),
    )?;

    ss_db.update_cells_db_to_v6("cell_db").await?;

    let mut visited = std::collections::HashSet::new();
    ss_db.enumerate_ids(&mut |id| {
        let cell = ss_db.get(id, true)?;
        visited.insert(cell.repr_hash());
        let mut stack = vec!(cell);
        while let Some(cell) = stack.pop() {
            for i in 0..cell.references_count() {
                let ref_cell_hash = cell.reference_repr_hash(i)?;
                if visited.insert(ref_cell_hash) {
                    let ref_cell = cell.reference(i)?;
                    stack.push(ref_cell);
                }
            }
        }
        Ok(true)
    })?;

    Ok(())
}