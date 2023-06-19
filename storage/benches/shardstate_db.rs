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

use std::{sync::Arc, collections::HashSet};
use storage::{
    db::rocksdb::RocksDb,
    shardstate_db_async::{AllowStateGcResolver, ShardStateDb, SsNotificationCallback},
    StorageAlloc,
};
#[cfg(feature = "telemetry")]
use storage::StorageTelemetry;
use ton_block::{BlockIdExt, ShardIdent};
use ton_types::{Cell, Result, BuilderData};
use rand::{Rng, SeedableRng};

include!("../src/db/tests/destroy_db.rs");
include!("../../common/src/log.rs");

const DB_PATH: &str = "../target/test";

struct MockedResolver;

impl AllowStateGcResolver for MockedResolver {
    fn allow_state_gc(&self, block_id: &BlockIdExt, _saved_at: u64, _gc_utime: u64) -> Result<bool> {
        Ok(block_id.seq_no() > 2_467_100)
    }
}

fn update_boc(old_root: &Cell, need_cells: u32, new_cells: &mut u32, rng: &mut impl rand::RngCore) -> Cell {
    let mut new_root = old_root.clone();
    while *new_cells < need_cells {
        new_root = update_boc_(&new_root, (need_cells as f32 * 2.5) as u32, new_cells, rng);
        //println!("update_boc iteration, new_cells: {new_cells}");
    }
    new_root
}

fn update_boc_(old_root: &Cell, need_cells: u32, new_cells: &mut u32, rng: &mut impl rand::RngCore) -> Cell {
    let mut builder = BuilderData::new();
    let bits = rng.gen_range(0..=1023);
    let mut data = vec!(0_u8; (bits + 7) / 8);
    rng.fill(&mut data[..]);
    builder.append_raw(&data, bits).unwrap();

    for child in old_root.clone_references() {
        if rng.gen_range(0..need_cells) < *new_cells {
            builder.checked_append_reference(child).unwrap();
        } else {
            *new_cells += 1;
            let child = update_boc_(&child, need_cells, new_cells, rng);
            builder.checked_append_reference(child).unwrap();
        }
    }

    builder.into_cell().unwrap()
}

/// Creates a BOC with random topology and specifed number of cells
fn generate_boc(need_cells: u32, rng: &mut impl rand::RngCore) -> Cell {
    println!("generate BOC with about {} cells", need_cells);

    let bottom_level_cells = (need_cells as f32 * 0.08) as usize;

    // starting from the bottom level
    let mut cells: Vec<Cell> = vec!();
    let mut bottom_level = true;
    let mut level = 0;
    let mut total_cells = 0;
    let mut hashes = HashSet::new();
    loop {
        let mut next_level_cells = vec!();
        level += 1;
        println!("Next level #{level} started; prev level cells: {cells}", cells = cells.len());

        while cells.len() > 0 || bottom_level && next_level_cells.len() < bottom_level_cells {
            if rng.gen_range(1..5) == 1 && cells.len() > 0 {
                next_level_cells.push(cells.pop().unwrap());
                continue;
            }
            let mut builder = BuilderData::new();
            let bits = rng.gen_range(0..=1023);
            let mut data = vec!(0_u8; (bits + 7) / 8);
            rng.fill(&mut data[..]);
            builder.append_raw(&data, bits).unwrap();
            
            let rc = match rng.gen_range(0..100) {
                0..=40 => 2,
                41..=80 => 3,
                81..=95 => 1,
                _ => 0
            };

            for _ in 0..rc {
                if cells.len() == 0 {
                    break;
                }
                let child = if rng.gen_range(1..3) == 1 {
                    cells[rng.gen_range(0..cells.len())].clone()
                } else {
                    cells.pop().unwrap()
                };
                builder.checked_append_reference(child).unwrap();
            }
            
            total_cells += 1;
            let cell = builder.into_cell().unwrap();
            hashes.insert(cell.repr_hash());
            next_level_cells.push(cell);

        }
        bottom_level = false;
        cells = next_level_cells;

        if cells.len() == 1 {
            println!("Tree done; needed cells {need_cells}  uniq cells {}  total cells: {total_cells}", hashes.len());
            return cells.pop().unwrap();
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {

    let mut rng = rand::rngs::SmallRng::from_seed([123; 32]);
    let cells = 3_000_000;
    let number_of_bocs = 1;
    let update = 5_000;
    let updates = 500;

    init_log("../common/config/log_cfg_debug.yml");

    const DB_NAME: &str = "test_shardstate_db_async_2";

    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();

    let open_db = || {
        let mut hi_perf_cfs = HashSet::new();
        hi_perf_cfs.insert("cells_db".to_string());
        let db = RocksDb::with_options(DB_PATH, DB_NAME, hi_perf_cfs, false).unwrap();
        let ss_db = ShardStateDb::new(
            db.clone(),
            DB_NAME,
            "shardstate_db",
            "cell_db",
            false,
            1000,
            1000,
            false,
            #[cfg(feature = "telemetry")]
            Arc::new(StorageTelemetry::default()),
            Arc::new(StorageAlloc::default()),
        ).unwrap();
        (db, ss_db)
    };

    let (db, ss_db) = open_db();
    let mut cell = Cell::default();
    let mut first_put_time = vec!();
    let mut block_id = BlockIdExt::default();

    for _ in 0..number_of_bocs {
        cell = generate_boc(cells, &mut rng);
        // print the cell
        // println!("{:#.2}", cell);

        block_id = BlockIdExt {
            shard_id: ShardIdent::masterchain(),
            seq_no: 0,
            root_hash: cell.repr_hash(),
            file_hash: Default::default(),
        };

        let cb = SsNotificationCallback::new();
        let first_put = std::time::Instant::now();

        ss_db.put(&block_id, cell.clone(), Some(cb.clone())).await.unwrap();
        cb.wait().await;

        first_put_time.push(first_put.elapsed().as_millis());
    }
    ss_db.stop().await;
    drop(ss_db);
    drop(db);


    let (_db, ss_db) = open_db();

    let before_updates = std::time::Instant::now();

    for i in 0..updates {
        let mut new_cells = 0;
        cell = update_boc(&cell, update, &mut new_cells, &mut rng);
        println!("updated {new_cells}");

        block_id = BlockIdExt {
            shard_id: ShardIdent::masterchain(),
            seq_no: block_id.seq_no + 1,
            root_hash: cell.repr_hash(),
            file_hash: Default::default(),
        };

        if i != updates - 1 {
            ss_db.put(&block_id, cell.clone(), None).await.unwrap();
        } else {
            let cb = SsNotificationCallback::new();
            ss_db.put(&block_id, cell.clone(), Some(cb.clone())).await.unwrap();
            cb.wait().await;
        }
    }

    println!("Cells: {cells}");
    println!("Update cells: {update}");
    println!("Updates: {updates}");
    println!("Bocs: {number_of_bocs}");
    print!("first put time: ");
    for t in first_put_time {
        print!("{} ", t);
    }
    print!("\n");
    let update_total = before_updates.elapsed().as_millis();
    println!("updates total time {}, per update {}", update_total, update_total / updates as u128);

    ss_db.stop().await;

    Ok(())
}
