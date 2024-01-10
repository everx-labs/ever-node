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
    block_handle_db::FLAG_KEY_BLOCK,
    db::rocksdb::RocksDb,
    tests::utils::create_block_handle_storage, 
    types::BlockMeta
};
use ton_block::{BlockIdExt, ShardIdent};
use ton_types::UInt256;

include!("../db/tests/destroy_db.rs");

const DB_PATH: &str = "../target/test";

#[tokio::test]
async fn test_block_handle_specific_flags() {

    let (db, _) = create_block_handle_storage(None); 
    let handle = db
        .create_handle(BlockIdExt::default(), BlockMeta::default(), None)
        .unwrap()
        .unwrap();

//    assert_eq!(handle.flags(), 0);
//    assert_eq!(handle.fetched(), false);

    assert_eq!(handle.has_data(), false);
    assert_eq!(handle.set_data(), true);
    assert_eq!(handle.has_data(), true);
    assert_eq!(handle.set_data(), false);

    assert_eq!(handle.has_proof(), false);
    assert_eq!(handle.set_proof(), true);
    assert_eq!(handle.has_proof(), true);
    assert_eq!(handle.set_proof(), false);

    assert_eq!(handle.has_proof_link(), false);
    assert_eq!(handle.set_proof_link(), true);
    assert_eq!(handle.has_proof_link(), true);
    assert_eq!(handle.set_proof_link(), false);

/*
    assert_eq!(handle.processed_in_ext_db(), false);
    assert_eq!(handle.set_processed_in_ext_db(), false);
    assert_eq!(handle.processed_in_ext_db(), true);
*/

    assert_eq!(handle.has_state(), false);
    assert_eq!(handle.set_state(), true);
    assert_eq!(handle.has_state(), true);
    assert_eq!(handle.set_state(), false);

    assert_eq!(handle.has_persistent_state(), false);
    assert_eq!(handle.set_persistent_state(), true);
    assert_eq!(handle.has_persistent_state(), true);
    assert_eq!(handle.set_persistent_state(), false);

    assert_eq!(handle.has_next1(), false);
    assert_eq!(handle.set_next1(), true);
    assert_eq!(handle.has_next1(), true);
    assert_eq!(handle.set_next1(), false);

    assert_eq!(handle.has_next2(), false);
    assert_eq!(handle.set_next2(), true);
    assert_eq!(handle.has_next2(), true);
    assert_eq!(handle.set_next2(), false);

    assert_eq!(handle.has_prev1(), false);
    assert_eq!(handle.set_prev1(), true);
    assert_eq!(handle.has_prev1(), true);
    assert_eq!(handle.set_prev1(), false);

    assert_eq!(handle.has_prev2(), false);
    assert_eq!(handle.set_prev2(), true);
    assert_eq!(handle.has_prev2(), true);
    assert_eq!(handle.set_prev2(), false);

    assert_eq!(handle.is_applied(), false);
    assert_eq!(handle.set_block_applied(), true);
    assert_eq!(handle.is_applied(), true);
    assert_eq!(handle.set_block_applied(), false);

    assert_eq!(handle.is_archived(), false);
    assert_eq!(handle.set_archived(), true);
    assert_eq!(handle.is_archived(), true);
    assert_eq!(handle.set_archived(), false);

}

#[tokio::test]
async fn test_handle_db() {

    const DB_NAME: &str = "test_handle_db";

    let db = RocksDb::with_path(DB_PATH, DB_NAME).unwrap();
    let (block_handle_storage, _) = create_block_handle_storage(Some(db.clone()));
    
    for mc_seq_no in 0..20_u32 {
        let block_id = BlockIdExt::with_params(ShardIdent::masterchain(), mc_seq_no, 
            UInt256::from_le_bytes(&mc_seq_no.to_le_bytes()), UInt256::default());
        let flags = if mc_seq_no % 13 == 0 {
            FLAG_KEY_BLOCK
        } else {
            0
        };
        let meta = BlockMeta::with_data(flags, 0, 0, 0, 0);
        let handle = block_handle_storage
            .create_handle(block_id.clone(), meta, None)
            .unwrap()
            .unwrap();
        handle.set_data();
        //tokio::task::yield_now().await;
        handle.set_archived();
        block_handle_storage.save_handle(&handle, None).unwrap();
        if mc_seq_no % 13 == 0 {
            assert!(handle.is_key_block().unwrap());
        }
        assert!(handle.has_data());
        assert!(handle.is_archived());
    }

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    for mc_seq_no in 0..20 {
        let block_id = BlockIdExt::with_params(ShardIdent::masterchain(), mc_seq_no, 
            UInt256::from_le_bytes(&mc_seq_no.to_le_bytes()), UInt256::default());
        let handle = block_handle_storage.load_handle(&block_id).unwrap().unwrap();
        if mc_seq_no % 13 == 0 {
            assert!(handle.is_key_block().unwrap());
        }
        assert!(handle.has_data());
        assert!(handle.is_archived());
    }

    drop(block_handle_storage);
    drop(db);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();

}

