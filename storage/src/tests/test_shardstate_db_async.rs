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
    db::rocksdb::RocksDb,
    shardstate_db_async::{AllowStateGcResolver, CellsDbConfig, ShardStateDb}, StorageAlloc,
};
#[cfg(feature = "telemetry")]
use crate::StorageTelemetry;
use std::{
    fs::read, path::Path, sync::{Arc, atomic::{AtomicU32}}, time::Duration
};
use ton_block::{BlockIdExt, ShardIdent};
use ton_types::{Cell, Result, UInt256, read_single_root_boc};

include!("../db/tests/destroy_db.rs");
include!("../../../common/src/log.rs");

const DB_PATH: &str = "../target/test";

fn ss_from_file(index: u32) -> Cell {
    let path = format!("src/tests/testdata/{}", index);
    let orig_bytes = read(Path::new(&path))
        .expect(&format!("Error reading file {}", path));

    read_single_root_boc(&orig_bytes)
        .expect("Error deserializing shard-state")
}

struct MockedResolver;

impl AllowStateGcResolver for MockedResolver {
    fn allow_state_gc(&self, block_id: &BlockIdExt, _saved_at: u64, _gc_utime: u64) -> Result<bool> {
        Ok(block_id.seq_no() > 2_467_100)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shardstate_db_async() -> Result<()> {

    init_log("../common/config/log_cfg_debug.yml");
    println!();

    const DB_NAME: &str = "test_shardstate_db_async";

    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();

    let db = RocksDb::with_path(DB_PATH, DB_NAME)?;
    let ss_db = ShardStateDb::new(
        db.clone(),
        DB_NAME,
        "shardstate_db",
        "cell_db",
        false,        
        false,
        CellsDbConfig {
            states_db_queue_len: 1000,
            max_pss_slowdown_mcs: 1000,
            prefill_cells_counters: false,
            cache_cells_counters: true,
            cells_lru_size: 1000000,
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
        let block_id = block_id_ext.into();
        ss_db.put(&block_id, root_cell.clone(), None).await?;
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
        let block_id = block_id_ext.into();
        let res = ss_db.get(&block_id, true);

        if block_id.seq_no() > 2_467_100 {
            if !res.is_err() {
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
