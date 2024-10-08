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

use crate::{block_db::BlockDb, tests::utils::*, db::rocksdb::RocksDb};
use ever_block::Result;

const DB_PATH: &str = "../target/test";

#[test]
fn test_get_put_raw_block() -> Result<()> {
    const DB_NAME: &str = "test_get_put_raw_block";

    let db = RocksDb::with_path(DB_PATH, DB_NAME).unwrap();
    let block_db = BlockDb::with_db(db, "block_db", true)?;

    let block_id = get_test_block_id();
    let data = get_test_raw_boc();

    block_db.put(&block_id, &data)?;
    assert_eq!(block_db.len()?, 1);

    let result = block_db.get(&block_id)?;
    assert_eq!(result.as_ref(), data.as_slice());

    Ok(())
}
