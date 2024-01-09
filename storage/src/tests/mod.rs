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

mod test_block_db;
mod test_catchain_persistent_db;
mod test_dynamic_boc_rc_db;
mod test_shardstate_db_async;

pub mod utils {

    use crate::{
        db::rocksdb::RocksDb,
        StorageAlloc, block_handle_db::{BlockHandleDb, BlockHandleStorage}, 
        node_state_db::NodeStateDb
    };
    #[cfg(feature = "telemetry")]
    use crate::StorageTelemetry;
    use fnv::FnvHashSet;
    use std::sync::Arc;
    use ton_block::{BlockIdExt, SHARD_FULL, ShardIdent};
    use ton_types::{Cell, Result, UInt256, read_single_root_boc};

    pub fn get_test_raw_boc() -> Vec<u8> {
        include_bytes!("testdata/2467080").to_vec()
    }

    pub fn get_test_shard_ident() -> ShardIdent {
        ShardIdent::with_tagged_prefix(-1, SHARD_FULL).unwrap()
    }

    pub static ROOT_HASH: [u8; 32] = [
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF,
        0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10,
    ];

    pub static FILE_HASH: [u8; 32] = [
        0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10,
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF,
    ];

    pub fn get_test_block_id() -> BlockIdExt {
        // -1,8000000000000000,1830539
        BlockIdExt::with_params(
            get_test_shard_ident(),
            1830539,
            UInt256::from(&ROOT_HASH),
            UInt256::from(&FILE_HASH)
        )
    }

    pub fn get_test_tree_of_cells() -> Cell {
        let data = include_bytes!("testdata/2467080").to_vec();
        read_single_root_boc(&data).unwrap()
    }

    pub fn get_another_test_tree_of_cells() -> Cell {
        let data = include_bytes!("testdata/2467119").to_vec();
        read_single_root_boc(&data).unwrap()
    }

    #[allow(dead_code)]
    pub fn compare_trees(cell_left: Cell, cell_right: Cell) -> Result<()> {
        assert_eq!(cell_left.cell_data(), cell_right.cell_data());
        assert_eq!(cell_left.cell_type(), cell_right.cell_type());
        assert_eq!(cell_left.is_pruned(), cell_right.is_pruned());
        assert_eq!(cell_left.is_merkle(), cell_right.is_merkle());
        assert_eq!(cell_left.bit_length(), cell_right.bit_length());
        assert_eq!(cell_left.data(), cell_right.data());
        assert_eq!(cell_left.repr_hash(), cell_right.repr_hash());
        assert_eq!(cell_left.repr_depth(), cell_right.repr_depth());
        assert_eq!(cell_left.level(), cell_right.level());
        for i in 0..(cell_left.level() + 1) as usize {
            assert_eq!(cell_left.hash(i), cell_right.hash(i));
            assert_eq!(cell_left.depth(i), cell_right.depth(i));
        }
        assert_eq!(cell_left.references_count(), cell_right.references_count());
        for i in 0..cell_left.references_count() {
            compare_trees(cell_left.reference(i)?, cell_left.reference(i)?)?;
        }
        Ok(())
    }

    pub fn count_tree_unique_cells(root_cell: Cell) -> usize {
        let mut unique_cells = FnvHashSet::default();
        count_tree_unique_cells_recursive(root_cell, &mut unique_cells);
        unique_cells.len()
    }

    fn count_tree_unique_cells_recursive(cell: Cell, unique_cells: &mut FnvHashSet<UInt256>) {
        if unique_cells.insert(cell.repr_hash().into()) {
            for i in 0..cell.references_count() {
                count_tree_unique_cells_recursive(cell.reference(i).unwrap(), unique_cells);
            }
        }
    }

    pub fn create_block_handle_storage(
        db: Option<Arc<RocksDb>>
    ) -> (BlockHandleStorage, Arc<BlockHandleDb>) {
        let block_handle_db = if let Some(db) = db.clone() {
            Arc::new(BlockHandleDb::with_db(db, "block_handles", true).unwrap())
        } else {
            Arc::new(BlockHandleDb::in_memory())
        };
        let block_handle_storage = BlockHandleStorage::with_dbs(
            block_handle_db.clone(),
            if let Some(db) = db.clone() {
                Arc::new(NodeStateDb::with_db(db, "full_node_states", true).unwrap())
            } else {
                Arc::new(NodeStateDb::in_memory())
            },
            if let Some(db) = db {
                Arc::new(NodeStateDb::with_db(db, "validator_states", true).unwrap())
            } else {
                Arc::new(NodeStateDb::in_memory())
            },
            #[cfg(feature = "telemetry")]
            Arc::new(StorageTelemetry::default()),
            Arc::new(StorageAlloc::default()),
        );
        (block_handle_storage, block_handle_db)
    }
}
