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
        ARCHIVE_PACKAGE_SIZE, archive_manager::ArchiveManager, 
        package_entry_id::{GetFileNameShort, PackageEntryId},
    },
    block_handle_db::{FLAG_KEY_BLOCK, BlockHandleStorage}, db::rocksdb::RocksDb,
    tests::utils::create_block_handle_storage, types::BlockMeta, StorageAlloc,
};
#[cfg(feature = "telemetry")]
use crate::StorageTelemetry;
use std::{path::Path, sync::Arc};
use ton_block::{BlockIdExt, ShardIdent};
use ton_types::{error, Result, UInt256};

include!("../../db/tests/destroy_db.rs");

const DB_PATH: &str = "../target/test";

// include!("../../../../common/src/log.rs");

const TESTDATA_PATH: &str = "src/archives/tests/testdata/";
const ARCHIVE_00000_GOLD: &str = "archive.00000-2.pack.gold";
const ARCHIVE_00100_GOLD: &str = "archive.00100.pack.gold";
const ARCHIVE_00200_GOLD: &str = "archive.00200.pack.gold";

#[tokio::test]
async fn test_scenario() -> Result<()> {
   
    const DB_NAME: &str = "test_archive_manager_scenario";

    let db_root = Path::new(DB_PATH).join(DB_NAME);
    let _ = std::fs::remove_dir_all(&db_root);
    let db = RocksDb::with_path(DB_PATH, DB_NAME)?;
    let manager = ArchiveManager::with_data(
        db.clone(),
        Arc::new(db_root),
        0,
        #[cfg(feature = "telemetry")]
        Arc::new(StorageTelemetry::default()),
        Arc::new(StorageAlloc::default()),
    ).await?;
    let (block_handle_storage, _) = create_block_handle_storage(None);

    let data = vec![1, 2, 3, 4, 5];
    for mc_seq_no in 0..250 {
        let block_id = BlockIdExt::with_params(
            ShardIdent::masterchain(), 
            mc_seq_no, 
            UInt256::with_array([mc_seq_no as u8; 32]), 
            UInt256::default()
        );
        let meta = BlockMeta::with_data(0, 0, 0, 0, 0);
        let handle = block_handle_storage.create_handle(block_id.clone(), meta, None)?.ok_or_else(
            || error!("Cannot create handle for block {}", block_id)
        )?;

        let entry_id = PackageEntryId::<_, &UInt256, &UInt256>::Proof(&block_id);
        manager.add_file(&entry_id, data.clone()).await?;
        handle.set_proof();
        handle.set_block_applied();
        manager.move_to_archive(&handle, || Ok(())).await?;
    }

    for mc_seq_no in 0..300 {
        assert_eq!(
            manager.get_archive_id(mc_seq_no).await,
            Some(((mc_seq_no - mc_seq_no % ARCHIVE_PACKAGE_SIZE) as u64) << 32),
            "mc_seq_no = {}",
            mc_seq_no
        );
    }

    assert!(manager.get_archive_id(300).await.is_none());

    test_downloading(&manager, 0, ARCHIVE_00000_GOLD).await?;
    test_downloading(&manager, 100, ARCHIVE_00100_GOLD).await?;
    test_downloading(&manager, 222, ARCHIVE_00200_GOLD).await?;

    drop(block_handle_storage);
    drop(manager);
    drop(db);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();
    Ok(())

}

async fn test_downloading(manager: &ArchiveManager, mc_seq_no: u32, gold_path: &str) -> Result<()> {
    let data = simulate_downloading(&manager, mc_seq_no).await?;
    assert_eq!(
        &data[..], 
        tokio::fs::read(Path::new(TESTDATA_PATH).join(gold_path)).await.unwrap().as_slice()
    );
    Ok(())
}

async fn simulate_downloading(manager: &ArchiveManager, mc_seq_no: u32) -> Result<Vec<u8>> {
    let archive_id = manager.get_archive_id(mc_seq_no).await.unwrap();
    let mut data = Vec::new();
    let mut offset = 0;
    const ARCHIVE_PACKAGE_SIZE: u32 = 1024;
    loop {
        let slice = manager.get_archive_slice(archive_id, offset, ARCHIVE_PACKAGE_SIZE).await?;
        data.extend_from_slice(&slice[..]);
        if slice.len() < ARCHIVE_PACKAGE_SIZE as usize {
            break;
        }
        offset += slice.len() as u64;
    }    
    Ok(data)
}

#[tokio::test]
async fn test_scenario_keyblocks_10m() -> Result<()> {

    const DB_NAME: &str = "test_scenario_keyblocks_10m";

    // init_log("./../common/config/log_cfg.yml");
    let path = Path::new(DB_PATH).join(DB_NAME);
    let db = RocksDb::with_path(DB_PATH, DB_NAME)?;
    let (block_handle_storage, _) = create_block_handle_storage(Some(db.clone()));
    let manager = ArchiveManager::with_data(
        db.clone(),
        Arc::new(path),
        0,
        #[cfg(feature = "telemetry")]
        Arc::new(StorageTelemetry::default()),
        Arc::new(StorageAlloc::default()),
    ).await?;

    let data = vec![1, 2, 3, 4, 5];
    let numbers = vec![
        7,
        43,
        560,
        5480,
        95480,
        134354,
        3548755,
        10174254,
        101574254,
        1343548755,
    ];

    for mc_seq_no in numbers {
        let block_id = BlockIdExt::with_params(
            ShardIdent::masterchain(),
            mc_seq_no,
            UInt256::from_le_bytes(&mc_seq_no.to_le_bytes()),
            UInt256::default()
        );
        let meta = BlockMeta::with_data(FLAG_KEY_BLOCK, 0, 0, 0, 0);

        let entry_id = PackageEntryId::<_, &UInt256, &UInt256>::Proof(&block_id);
        
        manager.add_file(&entry_id, data.clone()).await?;
        
        let handle = block_handle_storage
            .create_handle(block_id.clone(), meta, None)?
            .ok_or_else(|| error!("Cannot create handle for block {}", block_id))?;

        handle.set_proof();
        handle.set_block_applied();
        manager.move_to_archive(&handle, || Ok(())).await?;
        handle.set_archived();
        assert!(handle.is_key_block()?);
        assert!(handle.has_proof());
        assert!(handle.is_archived());
        block_handle_storage.save_handle(&handle, None)?;

        tokio::time::sleep(std::time::Duration::from_millis(1)).await;

        let block_id = BlockIdExt::with_params(ShardIdent::masterchain(), mc_seq_no, 
            UInt256::from_le_bytes(&mc_seq_no.to_le_bytes()), UInt256::default());
        let handle = block_handle_storage.load_handle(&block_id)?
            .ok_or_else(|| error!("Cannot load handle for block {}", block_id))?;
        assert!(handle.has_proof());
        assert!(handle.is_archived());
        assert!(handle.is_key_block()?);

        let entry_id = PackageEntryId::<_, &UInt256, &UInt256>::Proof(&block_id);
        manager.get_file(&handle, &entry_id).await?;
    }
    
    drop(block_handle_storage);
    drop(manager);
    drop(db);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();
    Ok(())

}

fn generate_block_id(workchain_id: i32, shard_prefix_tagged: u64, seq_no: u32) -> BlockIdExt {
    let shard_id = ShardIdent::with_tagged_prefix(workchain_id, shard_prefix_tagged).unwrap();
    BlockIdExt::with_params(shard_id, seq_no, UInt256::rand(), UInt256::rand())
}

fn generate_short_filename(id: &BlockIdExt) -> String {
    let entry_id = PackageEntryId::<_, UInt256, UInt256>::ProofLink(id);
    entry_id.filename_short()
}

#[tokio::test]
async fn test_clean_unapplied_files() {

    const DB_NAME: &str = "clean_db";

    // init_log("./../common/config/log_cfg.yml");
    let path = Path::new(DB_PATH).join(DB_NAME);
    let db = RocksDb::with_path(DB_PATH, DB_NAME).unwrap();
    let manager = ArchiveManager::with_data(
        db.clone(),
        Arc::new(path),
        0,
        #[cfg(feature = "telemetry")]
        Arc::new(StorageTelemetry::default()),
        Arc::new(StorageAlloc::default()),
    ).await.unwrap();
    
    let id = generate_block_id(555, 0xF8000000_00000000, 100);
    let filename = generate_short_filename(&id);
    let path = manager.unapplied_files_path().join(filename);

    std::fs::write(&path, "test").unwrap();

    let id = generate_block_id(555, 0xF8000000_00000000, 99);
    manager.clean_unapplied_files(&vec![id]).await;

    assert!(path.exists(), "file {:?} must not be removed", path);

    let id = generate_block_id(333, 0xF8000000_00000000, 101);
    manager.clean_unapplied_files(&vec![id]).await;

    assert!(path.exists(), "file {:?} must not be removed", path);

    let id = generate_block_id(555, 0xE8000000_00000000, 101);
    manager.clean_unapplied_files(&vec![id]).await;

    assert!(path.exists(), "file {:?} must not be removed", path);

    let id = generate_block_id(555, 0xF8000000_00000000, 101);
    manager.clean_unapplied_files(&vec![id]).await;

    assert!(!path.exists(), "file {:?} must be removed", path);

    drop(manager);
    drop(db);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap();

}

#[allow(dead_code)]
struct TestArchiveManager {
    db: Arc<RocksDb>,
    archive_manager: ArchiveManager,
    block_handle_storage: BlockHandleStorage
}

impl TestArchiveManager {

    async fn new(root: &str, name: &str) -> Result<Self> {
        let path = Path::new(root).join(name);
        let db = RocksDb::with_path(root, name)?;
        let archive_manager = ArchiveManager::with_data(
            db.clone(),
            Arc::new(path),
            0,
            #[cfg(feature = "telemetry")]
            Arc::new(StorageTelemetry::default()),
            Arc::new(StorageAlloc::default()),
        ).await.unwrap();
        let (block_handle_storage, _) = create_block_handle_storage(Some(db.clone()));
        Ok(Self {
            db,
            archive_manager,
            block_handle_storage
        })
    }

    fn master_block_id(mc_seq_no: u32, version: u8) -> BlockIdExt {
        BlockIdExt::with_params(
            ShardIdent::masterchain(),
            mc_seq_no,
            UInt256::from_le_bytes(&mc_seq_no.to_le_bytes()),
            UInt256::from_le_bytes(&[version])
        )
    }

    async fn add_entries(&self, range: impl IntoIterator<Item = u32>, version: u8, key_blocks: &[u32]) {
        for mc_seq_no in range {
            let block_id = Self::master_block_id(mc_seq_no, version);
            let flags = if key_blocks.contains(&mc_seq_no) {
                FLAG_KEY_BLOCK
            } else {
                0
            };
            let meta = BlockMeta::with_data(flags, 0, 0, 0, 0);
            let handle = self.block_handle_storage.create_handle(block_id.clone(), meta, None).unwrap().unwrap();
            let entry_id = PackageEntryId::<&BlockIdExt, &UInt256, &UInt256>::Block(&block_id);
            self.archive_manager.add_file(&entry_id, vec![1, 2, 3, 4, 5]).await.unwrap();
            handle.set_data();
            let entry_id = PackageEntryId::<&BlockIdExt, &UInt256, &UInt256>::Proof(&block_id);
            self.archive_manager.add_file(&entry_id, vec![6, 7, 8, 9]).await.unwrap();
            handle.set_proof();
            handle.set_block_applied();
            self.archive_manager.move_to_archive(&handle, || Ok(())).await.unwrap();
            handle.set_archived();
            assert!(handle.is_archived());
            self.block_handle_storage.save_handle(&handle, None).unwrap()
        }
    }

    async fn trunc_with_check(&self, mc_seq_no: u32, version: u8) {
        let trunc_block_id = Self::master_block_id(mc_seq_no, version);
        println!("trunc on {}", trunc_block_id);
        self.archive_manager.trunc(&trunc_block_id, &|id: &BlockIdExt| id.seq_no() >= trunc_block_id.seq_no()).await.unwrap();

        let block_id = Self::master_block_id(mc_seq_no - 1, version);
        let handle = self.block_handle_storage.load_handle(&block_id).unwrap().unwrap();
        let entry_id = PackageEntryId::<&BlockIdExt, &UInt256, &UInt256>::Block(&block_id);
        self.archive_manager.get_file(&handle, &entry_id).await.unwrap();
        let entry_id = PackageEntryId::<&BlockIdExt, &UInt256, &UInt256>::Proof(&block_id);
        self.archive_manager.get_file(&handle, &entry_id).await.unwrap();

        let handle = self.block_handle_storage.load_handle(&trunc_block_id).unwrap().unwrap();
        let entry_id = PackageEntryId::<&BlockIdExt, &UInt256, &UInt256>::Block(&trunc_block_id);
        self.archive_manager.get_file(&handle, &entry_id).await.expect_err("block should not be read");
        let entry_id = PackageEntryId::<&BlockIdExt, &UInt256, &UInt256>::Proof(&trunc_block_id);
        self.archive_manager.get_file(&handle, &entry_id).await.expect_err("proof should not be read");

        let block_id = Self::master_block_id(mc_seq_no + 1, version);
        let handle = self.block_handle_storage.load_handle(&block_id).unwrap().unwrap();
        let entry_id = PackageEntryId::<&BlockIdExt, &UInt256, &UInt256>::Block(&block_id);
        self.archive_manager.get_file(&handle, &entry_id).await.expect_err("block should not be read");
        let entry_id = PackageEntryId::<&BlockIdExt, &UInt256, &UInt256>::Proof(&block_id);
        self.archive_manager.get_file(&handle, &entry_id).await.expect_err("proof should not be read");
    }

}

#[tokio::test]
async fn test_archive_truncate() {

    const DB_NAME: &str = "node_db_slices";

    // init_log("../common/config/log_cfg.yml");
    let manager = TestArchiveManager::new(DB_PATH, DB_NAME).await.unwrap();

    manager.add_entries(1..306, 0, &[53]).await;
    manager.trunc_with_check(54, 0).await;
    manager.trunc_with_check(53, 0).await;
    manager.trunc_with_check(52, 0).await;

    manager.add_entries(500..700, 1, &[500]).await;

    manager.add_entries(10_000_000..10_000_070, 1, &[10_000_000, 10_000_053]).await;
    manager.trunc_with_check(10_000_054, 1).await;
    manager.trunc_with_check(10_000_053, 1).await;
    manager.trunc_with_check(10_000_001, 1).await;

    drop(manager);   
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap()

}

