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
    archives::{
        archive_slice::ArchiveSlice, package::PKG_HEADER_SIZE,
        package_entry::PKG_ENTRY_HEADER_SIZE, package_entry_id::{GetFileName, PackageEntryId},
        package_id::PackageType,
    },
    block_handle_db::{BlockHandleStorage, FLAG_KEY_BLOCK}, db::rocksdb::RocksDb,
    tests::utils::create_block_handle_storage, types::BlockMeta,
    StorageAlloc,
};
#[cfg(feature = "telemetry")]
use crate::StorageTelemetry;
use std::{future::Future, path::Path, pin::Pin, sync::Arc};
use ton_block::{BlockIdExt, ShardIdent};
use ton_types::{error, Result, UInt256};

include!("../../db/tests/destroy_db.rs");

const DB_PATH: &str = "../target/test";

const ARCHIVE_PATH: &str = "archive/packages/arch0000/";
const ARCHIVE_00000_GOLD_PATH: &str = "src/archives/tests/testdata/archive.00000.pack.gold";
const ARCHIVE_00100_GOLD_PATH: &str = "src/archives/tests/testdata/archive.00100.pack.gold";
const ARCHIVE_00200_GOLD_PATH: &str = "src/archives/tests/testdata/archive.00200.pack.gold";

struct TestContext {
    archive_slice: ArchiveSlice, 
    block_handle_storage: BlockHandleStorage
}

async fn prepare_test(
    name: &str, 
    package_type: PackageType
) -> Result<(Arc<RocksDb>, TestContext)> {
    let db_root = Path::new(DB_PATH).join(name);
    let _ = std::fs::remove_dir_all(&db_root);
    let db = RocksDb::with_path(DB_PATH, name)?;
    let archive_slice = ArchiveSlice::new_empty(
        db.clone(),
        Arc::new(db_root),
        0,
        package_type,
        #[cfg(feature = "telemetry")]
        Arc::new(StorageTelemetry::default()),
        Arc::new(StorageAlloc::default()),
    ).await?;
    let (block_handle_storage, _) = create_block_handle_storage(None);
    let test_context = TestContext {
        archive_slice, 
        block_handle_storage
    };
    Ok((db, test_context))
}

async fn destroy_db(db: Arc<RocksDb>, name: &str) {
    drop(db);
    destroy_rocks_db(DB_PATH, name).await.unwrap();
}

type Pinned = Pin<Box<dyn Future<Output = Result<()>>>>;

async fn run_test(
    name: &str, 
    package_type: PackageType,
    scenario: impl Fn(TestContext) -> Pinned
) -> Result<()> {
    let (db, test_context) = prepare_test(name, package_type).await?;
    scenario(test_context).await?;
    destroy_db(db, name).await;
    Ok(())
}

#[tokio::test]
async fn test_scenario_gold() -> Result<()> {
                                 
    async fn scenario(mut test_context: TestContext) -> Result<()> {
                              
        // Populating...
        let entry_id = PackageEntryId::<BlockIdExt, UInt256, UInt256>::Empty;
        test_context.archive_slice.add_file(None, &entry_id, vec![1, 2, 3]).await?;

        let data = vec![1, 2, 3, 4, 5];   
        for mc_seq_no in 0..250 {
            let block_id = BlockIdExt::with_params(
                ShardIdent::masterchain(), 
                mc_seq_no, 
                UInt256::with_array([mc_seq_no as u8; 32]), 
                UInt256::default()
            );
            let meta = BlockMeta::with_data(0, 0, 0, 0, 0);
            let handle = test_context.block_handle_storage.create_handle(
                block_id.clone(), meta, None
            )?.ok_or_else(
                || error!("Cannot create handle for block {}", block_id)
            )?;
            let entry_id = PackageEntryId::<_, UInt256, UInt256>::Proof(&block_id);
            test_context.archive_slice.add_file(Some(&handle), &entry_id, data.clone()).await?;
            let file = test_context.archive_slice.get_file(
                Some(&handle), &entry_id
            ).await?.ok_or_else(
                || error!("Cannot get file from archive for block {}", block_id)
            )?;
           
            assert_eq!(file.filename(), &entry_id.filename());
            assert_eq!(file.data(), &data);
        }

        // Comparing...

        let archive_path = test_context.archive_slice.db_root_path.join(ARCHIVE_PATH);
        assert_eq!(
            tokio::fs::read(archive_path.join("archive.00000.pack")).await.unwrap(), 
            tokio::fs::read(ARCHIVE_00000_GOLD_PATH).await.unwrap()
        );
        assert_eq!(
            tokio::fs::read(archive_path.join("archive.00100.pack")).await.unwrap(), 
            tokio::fs::read(ARCHIVE_00100_GOLD_PATH).await.unwrap()
        );
        assert_eq!(
            tokio::fs::read(archive_path.join("archive.00200.pack")).await.unwrap(), 
            tokio::fs::read(ARCHIVE_00200_GOLD_PATH).await.unwrap()
        );
    
        let hdr = PKG_HEADER_SIZE + PKG_ENTRY_HEADER_SIZE; 
        let read = test_context.archive_slice.get_slice(
            0,
            (hdr + entry_id.filename().as_bytes().len()) as u64 + 1,
            2
        ).await?;
        assert_eq!(read, vec![2, 3]);

        // Deleting...
        
        test_context.archive_slice.destroy().await?;
        assert!(!archive_path.join("archive.00000.pack").exists());
        assert!(!archive_path.join("archive.00100.pack").exists());
        assert!(!archive_path.join("archive.00200.pack").exists());
        drop(test_context);
        Ok(())

    }

    run_test(
        "test_archive_slice_scenario_gold",
        PackageType::Blocks,
        |ctx| Box::pin(scenario(ctx))
    ).await 

}

#[tokio::test]
async fn test_key_blocks_slice() -> Result<()> {

    async fn scenario(test_context: TestContext) -> Result<()> {
        let data = vec![1, 2, 3, 4, 5];
        let key_blocks = vec![456, 777, 1976, 5456, 7324, 10345, 15822, 24054, 27000];
        for mc_seq_no in key_blocks {
            let block_id = BlockIdExt::with_params(
                ShardIdent::masterchain(), 
                mc_seq_no,
                UInt256::rand(),
                UInt256::rand(),
            );
            let meta = BlockMeta::with_data(FLAG_KEY_BLOCK, 0, 0, 0, 0);
            let handle = test_context.block_handle_storage.create_handle(
                block_id.clone(), meta, None
            )?.ok_or_else(
                || error!("Cannot create handle for block {}", block_id)
            )?;
            let entry_id = PackageEntryId::<_, UInt256, UInt256>::Block(&block_id);
            test_context.archive_slice.add_file(Some(&handle), &entry_id, data.clone()).await?;
            let file = test_context.archive_slice.get_file(
                Some(&handle), &entry_id
            ).await?.ok_or_else(
                || error!("Cannot get file from archive for block {}", block_id)
            )?;
            assert_eq!(file.filename(), &entry_id.filename());
            assert_eq!(file.data(), &data);
        }
        drop(test_context);
        Ok(())
    }

    run_test(
        "test_key_blocks_slice",
        PackageType::KeyBlocks,
        |ctx| Box::pin(scenario(ctx))
    ).await 

}
