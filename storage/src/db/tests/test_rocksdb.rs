/*
* Copyright (C) 2019-2023 EverX. All Rights Reserved.
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
    db::{
        rocksdb::{RocksDb, RocksDbTable},
        tests::utils::{
            expect_error, expect_key_not_found_error, KEY0, KEY1,
        },
        traits::{
            KvcReadable, KvcSnapshotable, KvcTransactional, KvcWriteable
        }
    },
    error::StorageError
};
use rocksdb::{DB, Options, WriteBatch, Cache, BlockBasedOptions, MultiThreaded, DBWithThreadMode};
use std::{path::Path, sync::Arc};
use ton_types::Result;

include!("destroy_db.rs");

const DB_PATH: &str = "../target/test";

#[tokio::test]
async fn test_get_put_delete() -> Result<()> {

    const DB_NAME: &str = "test_get_put_delete";

    let db = RocksDb::with_path(DB_PATH, DB_NAME)?;
    let tb = RocksDbTable::with_db(db.clone(), "test", true)?;

    tb.delete(&KEY0)?;
    expect_key_not_found_error(tb.get(&KEY0), KEY0);
    tb.put(&KEY0, &[0])?;
    assert_eq!(tb.get(&KEY0)?.as_ref(), &[0]);
    expect_key_not_found_error(tb.get(&KEY1), KEY1);
    tb.put(&KEY1, &[1])?;
    assert_eq!(tb.get(&KEY1)?.as_ref(), &[1]);

    tb.delete(&KEY0)?;
    expect_key_not_found_error(tb.get(&KEY0), KEY0);
    assert_eq!(tb.get(&KEY1)?.as_ref(), &[1]);
    tb.delete(&KEY1)?;
    expect_key_not_found_error(tb.get(&KEY1), KEY1);

    drop(tb);
    drop(db);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();
    Ok(())

}

#[tokio::test]
async fn test_transactions() -> Result<()> {

    const DB_NAME: &str = "test_transactions";

    let db = RocksDb::with_path(DB_PATH, DB_NAME)?;
    let tb = RocksDbTable::with_db(db.clone(), "test", true)?;
    
    {
        let mut transaction = tb.begin_transaction()?;
        transaction.put(&KEY0, &[0])?;
        transaction.put(&KEY1, &[1])?;
        assert!(tb.get(&KEY0).is_err());
        assert!(tb.get(&KEY1).is_err());
        // Transaction must be rolled back on drop
    }
    assert!(tb.get(&KEY0).is_err());
    assert!(tb.get(&KEY1).is_err());

    {
        let mut transaction = tb.begin_transaction()?;
        transaction.put(&KEY0, &[0])?;
        transaction.put(&KEY1, &[1])?;
        assert!(tb.get(&KEY0).is_err());
        assert!(tb.get(&KEY1).is_err());
        transaction.commit()?;
    }
    assert_eq!(tb.get(&KEY0)?.as_ref(), &[0]);
    assert_eq!(tb.get(&KEY1)?.as_ref(), &[1]);

    drop(tb);
    drop(db);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();
    Ok(())

}

#[tokio::test]
async fn test_snapshots() -> Result<()> {
  
    const DB_NAME: &str = "test_snapshots";

    let db = RocksDb::with_path(DB_PATH, DB_NAME)?;
    let tb = RocksDbTable::with_db(db.clone(), "test", true)?;

    {
        tb.put(&KEY0, &[0])?;
        let snapshot = tb.snapshot()?;
        tb.put(&KEY1, &[1])?;
        assert_eq!(snapshot.get(&KEY0)?.as_ref(), &[0]);
        assert!(snapshot.get(&KEY1).is_err());
        assert_eq!(tb.get(&KEY0)?.as_ref(), &[0]);
        assert_eq!(tb.get(&KEY1)?.as_ref(), &[1]);
    }

    drop(tb);
    drop(db);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();
    Ok(())

}

#[tokio::test]
async fn test_foreach() -> Result<()> {

    const DB_NAME: &str = "test_foreach";

    let db = RocksDb::with_path(DB_PATH, DB_NAME)?;
    let tb = RocksDbTable::with_db(db.clone(), "test", true)?;

    let vec = vec![(KEY0, 0), (KEY1, 1)];
    for (key, value) in vec.iter() {
        tb.put(key, &[*value])?;
    }

    let mut index = 0;
    let result = KvcReadable::<&[u8]>::for_each(
        &tb, 
        &mut |key, value| {
            let (key0, value0) = vec.get(index).unwrap();
            index += 1;
            assert_eq!(key, *key0);
            assert_eq!(value, &[*value0]);
            Ok(true)
        }
    )?;

    assert!(result);

    let snapshot = tb.snapshot()?;
    index = 0;
    let result = KvcReadable::<&[u8]>::for_each(
        snapshot.as_ref(), 
        &mut |key, value| {
            let (key0, value0) = vec.get(index).unwrap();
            index += 1;
            assert_eq!(key, *key0);
            assert_eq!(value, &[*value0]);
            Ok(true)
        }
    )?;

    assert!(result);

    drop(snapshot);
    drop(tb);
    drop(db);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();
    Ok(())

}

#[tokio::test]
async fn test_get_slice() -> Result<()> {

    const DB_NAME: &str = "test_get_slice";

    let db = RocksDb::with_path(DB_PATH, DB_NAME)?;
    let tb = RocksDbTable::with_db(db.clone(), "test", true)?;

    expect_key_not_found_error(tb.get_slice(&KEY0, 0, 5), KEY0);

    tb.put(&KEY0, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])?;
    assert_eq!(tb.get_slice(&KEY0, 0, 1)?.as_ref(), &[0]);
    assert_eq!(tb.get_slice(&KEY0, 0, 10)?.as_ref(), &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    assert_eq!(tb.get_slice(&KEY0, 5, 5)?.as_ref(), &[5, 6, 7, 8, 9]);
    assert_eq!(tb.get_slice(&KEY0, 5, 2)?.as_ref(), &[5, 6]);

    expect_error(tb.get_slice(&KEY0, 7, 5), StorageError::OutOfRange);
    expect_error(tb.get_slice(&KEY0, 7, 4), StorageError::OutOfRange);
    expect_error(tb.get_slice(&KEY0, 11, 1), StorageError::OutOfRange);

    drop(tb);
    drop(db);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();
    Ok(())

}

#[tokio::test]
async fn test_db_open_and_create_column_family() {

    const DB_NAME: &str = "family_db";

    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_max_total_wal_size(1024 * 1024 * 1024);
    options.create_missing_column_families(true);

    let cfs = vec!["first"];
    let path = Path::new(DB_PATH).join(DB_NAME);
    destroy_rocks_db(DB_PATH, DB_NAME).await.ok();
    let mut db = DB::open_cf(&options, path.as_path(), cfs).unwrap();
    db.create_cf("second", &Default::default()).unwrap();

    assert!(db.cf_handle("third").is_none(), "we didn't create this table");
    assert!(db.cf_handle("default").is_some(), "this table is always created by rocks");

    let cf1 = db.cf_handle("first").unwrap();
    db.put_cf(&cf1, b"key1", b"value1").unwrap();
    assert_eq!(db.get_cf(&cf1, b"key1").unwrap().unwrap().as_slice(), b"value1");
    assert!(db.get_cf(&cf1, b"key2").unwrap().is_none());

    assert!(db.get(b"key1").unwrap().is_none());

    let cf2 = db.cf_handle("second").unwrap();
    assert!(db.get_cf(&cf2, b"key1").unwrap().is_none());
    db.put_cf(&cf2, b"key2", b"value2").unwrap();
    assert_eq!(db.get_cf(&cf2, b"key2").unwrap().unwrap().as_slice(), b"value2");

    let mut batch = WriteBatch::default();
    batch.delete_cf(&cf2, b"key2");
    batch.put_cf(&cf2, b"key3", b"value3");
    db.write(batch).unwrap();

    assert!(db.get_cf(&cf2, b"key2").unwrap().is_none());
    assert_eq!(db.get_cf(&cf2, b"key3").unwrap().unwrap().as_slice(), b"value3");

    //drop(cf1); // Warning by compiler
    //drop(cf2); // Warning by compiler
    drop(db);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();

}

#[tokio::test]
async fn test_double_create() {

    const DB_NAME: &str = "family_db";

    let cache = Cache::new_lru_cache(1 << 30); //1Gb block cache for one instance
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_cache(&cache);
    // save in LRU block cache also indexes and bloom filters
    block_opts.set_cache_index_and_filter_blocks(true); 
    // keep indexes and filters in block cache until tablereader freed
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true); 
    block_opts.set_block_size(16 << 10);
    // use latest available format version with new implementation of bloom filters
    block_opts.set_format_version(5); 

    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_max_total_wal_size(512 << 20); // 512Mb for one instance
    options.set_max_background_jobs(4);
    // allow background async incrementall file sync to disk by 1Mb per sync
    options.set_bytes_per_sync(1 << 20); 
    options.set_block_based_table_factory(&block_opts);
    options.create_missing_column_families(true);
    options.enable_statistics();
    options.set_dump_malloc_stats(true);

    let path = Path::new(DB_PATH).join(DB_NAME);
    destroy_rocks_db(DB_PATH, DB_NAME).await.ok();
    let db = Arc::new(DBWithThreadMode::<MultiThreaded>::open(&options, path.as_path()).unwrap());
    let name0 = "session0";
    let name1 = "session1";
    db.create_cf(name0, &Default::default()).unwrap();
    db.cf_handle(name0).unwrap();
    db.create_cf(name1, &Default::default()).unwrap();
    db.cf_handle(name1).unwrap();
    assert!(db.create_cf(name0, &Default::default()).is_err());
    assert!(db.create_cf(name1, &Default::default()).is_err());
    drop(db);
 
    let cfs = DBWithThreadMode::<MultiThreaded>::list_cf(&options, &path.as_path()).unwrap();
    dbg!(cfs);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();

}
