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

use crate::{db::traits::DbKey, tests::utils::{FILE_HASH, ROOT_HASH}};
use ton_block::{BlockIdExt, ShardIdent};
use ton_types::UInt256;

fn create_block_id() -> BlockIdExt {
    BlockIdExt::with_params(
        ShardIdent::with_tagged_prefix(-1, 0x8000_0000_0000_0000).unwrap(),
        1,
        UInt256::from(ROOT_HASH),
        UInt256::from(FILE_HASH)
    )
}

#[test]
fn test_block_id_formatting() {
    let block_id = create_block_id();
    assert_eq!(
        block_id.to_string().to_lowercase(), 
        format!(
            "(-1:8000000000000000, 1, rh {}, fh {})", 
            hex::encode(ROOT_HASH), 
            hex::encode(FILE_HASH)
        ).to_lowercase()
    );
}

#[test]
fn test_block_key_formatting() {
    let block_id = create_block_id();
    assert_eq!(block_id.key(), ROOT_HASH);
}