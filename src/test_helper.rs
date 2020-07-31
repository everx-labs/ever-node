#![allow(dead_code)]
#![cfg(test)]
use crate::{
    block::BlockStuff,
    db::{BlockHandle, InternalDb, InternalDbConfig, InternalDbImpl},
    shard_state::ShardStateStuff,
};
use sha2::Digest;
use std::{io::Cursor, ops::Deref, str::FromStr, sync::Arc};
use ton_block::*;
use ton_types::{
    deserialize_tree_of_cells, 
    Result, SliceData, UInt256
};

include!("../common/src/log.rs");

pub fn trace_object(name: &str) -> Result<()> {
    let bytes = std::fs::read(name)?;
    let file_hash = UInt256::from(sha2::Sha256::digest(&bytes).as_slice());
    let cell = deserialize_tree_of_cells(&mut Cursor::new(&bytes))?;
    let root_hash = cell.repr_hash();
    println!("root_hash: UInt256::from_str(\"{}\"),\n\
        file_hash: UInt256::from_str(\"{}\"),", root_hash.to_hex_string(), file_hash.to_hex_string());
    match SliceData::from(cell.clone()).get_next_u32()? {
        0x9023afe2 => println!("{}:\n{:?}", name, ShardStateUnsplit::construct_from(&mut cell.into())?),
        0x5f327da5 => println!("{}:\n{:?}", name, ShardStateSplit::construct_from(&mut cell.into())?),
        0x9bc7a987 => println!("{}:\n{:?}", name, BlockInfo::construct_from(&mut cell.into())?),
        0x11ef55aa => {
            let block = Block::construct_from(&mut cell.into())?;
            println!("{}:\n{:?}", name, block.read_info()?);
            println!("{}", ton_block_json::debug_block(block)?);
        }
        tag => println!("unknown tag x{:x}", tag),
    }
    Ok(())
}

#[test]
fn trace_block() {
    // trace_object("d:\\work\\block_boc_8e0900aa7d0faac5f3751bee0394868f089f1745935a8a30f6eac83dbab3fc3d.boc").unwrap();
}

pub fn test_zero_state_block_id() -> BlockIdExt {
    BlockIdExt {
        shard_id: ShardIdent::masterchain(),
        seq_no: 0,
        root_hash: UInt256::from_str("17a3a92992aabea785a7a090985a265cd31f323d849da51239737e321fb05569").unwrap(),
        file_hash: UInt256::from_str("5e994fcf4d425c0a6ce6a792594b7173205f740a39cd56f537defd28b48a0f6e").unwrap(),
    }
}

pub fn test_mc_state_block_id() -> BlockIdExt {
    BlockIdExt {
        shard_id: ShardIdent::masterchain(),
        seq_no: 3258710,
        root_hash: UInt256::from_str("e4b7037fb3095c0304cdb21587fb2de398830d751804b68d11f3ae4dc35a1f4d").unwrap(),
        file_hash: UInt256::from_str("2b9cacf89b0092b221fde550ce5581940993baf4a24765460272420470773411").unwrap(),
    }
}

pub fn test_next_key_block() -> BlockIdExt {
    BlockIdExt {
        shard_id: ShardIdent::masterchain(),
        seq_no: 3259221,
        root_hash: UInt256::from_str("7b0e729c9faba9e62b4a6a7bbfa124cd1bc50a0722370e52826564e04ea78ec6").unwrap(),
        file_hash: UInt256::from_str("72acd259523ab12c902e54cbb995014b3fab013e46488c243a9aa547e47a8ec5").unwrap()
    }
}

pub fn test_download_next_key_blocks_ids(block_id: &BlockIdExt) -> Result<Vec<BlockIdExt>> {
    if block_id == &test_mc_state_block_id() {
        Ok(vec!(test_next_key_block()))
    } else {
        Ok(Vec::new())
    }
}

pub fn import_block(shard_prefix: u64, seq_no: u32, db: &dyn InternalDb) -> Result<Arc<BlockHandle>> {
    let path = dirs::home_dir().unwrap().into_os_string().into_string().unwrap();
    let handle;
    if seq_no == 0 {
        handle = db.load_block_handle(&test_zero_state_block_id())?;
    } else {
        let name = format!("{}/full-node-test/blocks/{:016X}/{}", path, shard_prefix, seq_no);
        println!("file name: {}", name);
        let block_stuff = BlockStuff::read_from_file(&name)?;
        handle = db.load_block_handle(&block_stuff.id())?;
        if handle.data_inited() {
            println!("block: {} already in base", name);
        } else {
            println!("import block: {} with block_id: {}", name, handle.id());
            db.store_block_data(&handle, &block_stuff)?;
        }
    };
    let name = format!("{}/full-node-test/states/{:16X}/{}", path, shard_prefix, seq_no);
    if handle.state_inited() {
        println!("state: {} already in base", name);
    } else if let Ok(data) = std::fs::read(&name) {
        println!("state: {} reading", name);
        let state = ShardStateStuff::deserialize(handle.id().clone(), &data)?;
        println!("import state: {} with block_id: {}", name, handle.id());
        db.store_shard_state_dynamic(&handle, &state)?;
    }
    Ok(handle)
}

pub fn prepare_key_block_state(db: &dyn InternalDb) {
    let key_block_id = test_next_key_block();
    let key_handle = db.load_block_handle(&key_block_id).unwrap();
    let block_id = test_mc_state_block_id();
    import_block(block_id.shard().shard_prefix_with_tag(), block_id.seq_no, db).unwrap();
    if db.load_shard_state_dynamic(key_handle.id()).is_err() {
        let state = db.load_shard_state_dynamic(&block_id).expect("start state should present");
        let mut root = state.root_cell().clone();
        for seq_no in block_id.seq_no() + 1..=key_handle.id().seq_no() {
            let handle = import_block(block_id.shard().shard_prefix_with_tag(), seq_no, db).unwrap();
            let block = db.load_block_data(handle.id()).unwrap();
            let merkle_update = block.block().read_state_update().expect("bad StateUpdate");
            assert_eq!(root.repr_hash(), merkle_update.old_hash);
            root = merkle_update.apply_for(&root).unwrap().clone();
        }
        let state = ShardStateStuff::new(key_block_id, root).unwrap();
        db.store_shard_state_dynamic(&key_handle, &state).unwrap();
    }
}

pub fn prepare_test_db() -> Result<Arc<dyn InternalDb>> {
    let db_config = InternalDbConfig { db_directory: format!("node_db") };
    let db = Arc::new(InternalDbImpl::new(db_config)?);
    prepare_key_block_state(db.deref());
    Ok(db)
}
