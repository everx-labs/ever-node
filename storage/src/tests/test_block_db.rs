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

use crate::{block_db::BlockDb, tests::utils::*};
use ton_types::Result;

#[test]
fn test_get_put_raw_block() -> Result<()> {
    let db = BlockDb::in_memory();
    assert!(db.is_empty()?);

    let block_id= get_test_block_id().into();
    let data = get_test_raw_boc();

    db.put(&block_id, &data)?;
    assert_eq!(db.len()?, 1);

    let result = db.get(&block_id)?;
    assert_eq!(result.as_ref(), data.as_slice());

    Ok(())
}
