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
use ton_block_json::*;
use ton_types::{
    deserialize_tree_of_cells, serialize_toc,
    AccountId, Result, Cell, SliceData, HashmapType, UInt256
};
use ton_executor::TransactionExecutor;
use ton_vm::stack::StackItem;

include!("../common/src/log.rs");

// replace assert_eq for compare not to get panic
macro_rules! assert_eq {
    ($left:expr , $right:expr,) => ({
        assert_eq!($left, $right)
    });
    ($left:expr , $right:expr) => ({
        match (&($left), &($right)) {
            (left_val, right_val) => {
                if !(*left_val == *right_val) {
                    return Err(failure::err_msg(format!("{}, file {}:{}",
                        pretty_assertions::Comparison::new(left_val, right_val), file!(), line!())))
                }
            }
        }
    });
    ($left:expr , $right:expr, $($arg:tt)*) => ({
        match (&($left), &($right)) {
            (left_val, right_val) => {
                if !(*left_val == *right_val) {
                    return Err(failure::err_msg(format!("{}, {} file {}:{}",
                        pretty_assertions::Comparison::new(left_val, right_val),
                        format_args!($($arg)*), file!(), line!())))
                }
            }
        }
    });
}

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
            let extra = block.read_extra()?;
            let acc_blocks = extra.read_account_blocks()?;
            acc_blocks.iterate_with_keys(|key, acc_block| {
                println!("ACCOUNT: {}", key.to_hex_string());
                acc_block.transaction_iterate_full(|lt, tr_cell, _cur| {
                    let tr = Transaction::construct_from(&mut tr_cell.into())?;
                    println!("LT: {} tr: {}", lt, ton_block_json::debug_transaction(tr)?);
                    Ok(true)
                })
            })?;
            println!("{}", ton_block_json::debug_block(block)?);
        }
        tag => println!("unknown tag x{:x}", tag),
    }
    Ok(())
}

pub fn compute_file_hash(data: &[u8]) -> UInt256 {
    UInt256::from(sha2::Sha256::digest(data).as_slice())
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

pub fn import_block(path: &str, shard_prefix: u64, seq_no: u32, db: &dyn InternalDb) -> Result<Arc<BlockHandle>> {
    let handle;
    if seq_no == 0 {
        unimplemented!("need to import state 0 itself")
        // handle = db.load_block_handle(&test_zero_state_block_id())?;
    } else {
        let name = format!("{}/blocks/{:016X}/{}", path, shard_prefix, seq_no);
        println!("file name: {}", name);
        let block_stuff = BlockStuff::read_from_file(&name)?;
        handle = db.load_block_handle(&block_stuff.id())?;
        if handle.data_inited() {
            log::info!("block: {} already in base", name);
        } else {
            log::info!("import block: {} with block_id: {}", name, handle.id());
            db.store_block_data(&handle, &block_stuff)?;
        }
    };
    let name = format!("{}/states/{:16X}/{}", path, shard_prefix, seq_no);
    if handle.state_inited() {
        log::info!("state: {} already in base", name);
    } else if let Ok(data) = std::fs::read(&name) {
        log::info!("state: {} reading", name);
        let state = ShardStateStuff::deserialize(handle.id().clone(), &data)?;
        log::info!("import state: {} with block_id: {}", name, handle.id());
        db.store_shard_state_dynamic(&handle, &state)?;
    }
    Ok(handle)
}

// imports from replication folders persistent state and blocks and apply one by one
pub fn import_replica(db: Arc<dyn InternalDb>, path: &str, _workchain_id: i32, prefix: u64, mut seq_no: u32, mut need_seqno: u32) -> Result<()> {
    let path = format!("{}/{}", dirs::home_dir().unwrap().into_os_string().into_string().unwrap(), path);
    let acc_pfx = AccountIdPrefixFull::any_masterchain();
    // let acc_pfx = AccountIdPrefixFull::workchain(workchain_id, prefix);
    if let Ok(_handle) = db.find_block_by_seq_no(&acc_pfx, need_seqno) {
        // uncoment this to store state as a separate file
        // println!("load state {}", need_seqno);
        // let state = db.load_shard_state_dynamic(_handle.id())?;
        // println!("serialize state {}", need_seqno);
        // let data = serialize_toc(state.root_cell())?;
        // println!("store state {}", need_seqno);
        // std::fs::write(format!("{}/states/{:16X}/{}", path, prefix, need_seqno), data)?;
    } else {
        let result = db
            .find_block_by_seq_no(&acc_pfx, seq_no)
            .or_else(|_err| import_block(&path, prefix, seq_no, db.deref()));
        let handle = match result {
            Ok(handle) => handle,
            Err(_) => return Ok(())
        };
        let state = db.load_shard_state_dynamic(handle.id()).expect("start state should present");
        let mut root = state.root_cell().clone();
        while seq_no <= need_seqno {
            seq_no += 1;
            let handle = import_block(&path, prefix, seq_no, db.deref()).unwrap();
            let block = db.load_block_data(handle.id()).unwrap();
            let merkle_update = block.block().read_state_update().expect("bad StateUpdate");
            assert!(root.repr_hash() == merkle_update.old_hash);
            root = merkle_update.apply_for(&root).unwrap().clone();
            log::info!("Store state for {}", handle.id());
            let state = ShardStateStuff::new(handle.id().clone(), root.clone()).unwrap();
            db.store_shard_state_dynamic(&handle, &state).unwrap();
            if handle.is_key_block().unwrap() && seq_no > need_seqno {
                println!("new key block found: {}", handle.id());
                need_seqno = seq_no + 1; // apply one extra block after new key_block
            }
        }
    }
    Ok(())
}

pub fn prepare_key_block_state(db: &dyn InternalDb) {
    let path = dirs::home_dir().unwrap().into_os_string().into_string().unwrap();
    let key_block_id = test_next_key_block();
    let key_handle = db.load_block_handle(&key_block_id).unwrap();
    let block_id = test_mc_state_block_id();
    import_block(&path, block_id.shard().shard_prefix_with_tag(), block_id.seq_no, db).unwrap();
    if db.load_shard_state_dynamic(key_handle.id()).is_err() {
        let state = db.load_shard_state_dynamic(&block_id).expect("start state should present");
        let mut root = state.root_cell().clone();
        for seq_no in block_id.seq_no() + 1..=key_handle.id().seq_no() {
            let handle = import_block(&path, block_id.shard().shard_prefix_with_tag(), seq_no, db).unwrap();
            let block = db.load_block_data(handle.id()).unwrap();
            let merkle_update = block.block().read_state_update().expect("bad StateUpdate");
            std::assert_eq!(root.repr_hash(), merkle_update.old_hash);
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

pub fn compare_accounts(acc1: &Account, acc2: &Account) -> Result<()> {
    assert_eq!(acc1, acc2);
    Ok(())
}

fn compare_block_accounts(acc1: &ShardAccountBlocks, acc2: &ShardAccountBlocks) -> Result<()> {
    acc1.scan_diff_with_aug(&acc2, |key, acc_cur_1, acc_cur_2| {
        dbg!(&key);
        if let (Some((acc1, cur1)), Some((acc2, cur2))) = (&acc_cur_1, &acc_cur_2) {
            acc1.transactions().scan_diff_with_aug(acc2.transactions(), |lt, tr_cur1, tr_cur2| {
                dbg!(&lt);
                if let (Some((InRefValue(tr1), cur1)), Some((InRefValue(tr2), cur2))) = (&tr_cur1, &tr_cur2) {
                    compare_transactions(tr1, tr2, true)?;
                    assert_eq!(cur1, cur2);
                } else {
                    if let Some((InRefValue(tr), _cur)) = &tr_cur1 {
                        log::info!("{}", debug_transaction(tr.clone())?);
                    }
                    assert_eq!(tr_cur1, tr_cur2);
                }
                Ok(true)
            })?;
            println!("{:#.3}", acc1.transactions().data().unwrap());
            println!("{:#.3}", acc1.transactions().data().unwrap());
            assert_eq!(acc1.transactions(), acc2.transactions());
            assert_eq!(cur1, cur2);
            assert_eq!(acc1.read_state_update()?, acc2.read_state_update()?);
        } else {
            assert_eq!(acc_cur_1, acc_cur_2);
        }
        Ok(true)
    })?;
    std::fs::write("./target/cmp/1.txt", &format!("{:#.3}", acc1.data().unwrap())).unwrap();
    std::fs::write("./target/cmp/2.txt", &format!("{:#.3}", acc2.data().unwrap())).unwrap();
    assert_eq!(acc1, acc2);
    Ok(())
}

pub fn compare_transactions(tr1: &Transaction, tr2: &Transaction, check_messages: bool) -> Result<()> {
    dbg!(tr1.logical_time());
    assert_eq!(tr1.read_description()?, tr2.read_description()?);
    if check_messages {
        if let (Some(msg1), Some(msg2)) = (&tr1.in_msg, &tr2.in_msg) {
            compare_messages(&msg1.read_struct()?, &msg2.read_struct()?, false)?;
        } else {
            assert_eq!(tr1.in_msg, tr2.in_msg);
        }
    }
    tr1.out_msgs.scan_diff(&tr2.out_msgs, |key: U15, msg1, msg2| {
        dbg!(&key.0);
        if let (Some(InRefValue(msg1)), Some(InRefValue(msg2))) = (&msg1, &msg2) {
            compare_messages(msg1, msg2, false)?;
        } else {
            assert_eq!(tr1.in_msg, tr2.in_msg);
        }
        Ok(true)
    })?;
    assert_eq!(tr1.read_state_update()?, tr2.read_state_update()?);
    assert_eq!(tr1, tr2);
    Ok(())
}

fn compare_in_msgs(msgs1: &InMsgDescr, msgs2: &InMsgDescr) -> Result<()> {
    msgs1.scan_diff_with_aug(msgs2, |key, msg_aug_1, msg_aug_2| {
        dbg!(&key);
        dbg!(&msg_aug_1);
        dbg!(&msg_aug_2);
        let tr = msg_aug_1.as_ref().unwrap().0.read_transaction()?.unwrap();
        println!("Transaction: {}", debug_transaction(tr)?);
        if let (Some((msg1, aug1)), Some((msg2, aug2))) = (&msg_aug_1, &msg_aug_2) {
            if let (Some(tr1), Some(tr2)) = (msg1.read_transaction()?, msg2.read_transaction()?) {
                compare_transactions(&tr1, &tr2, false)?;
                compare_messages(&msg1.read_message()?, &msg2.read_message()?, false)?;
            } else {
                compare_messages(&msg1.read_message()?, &msg2.read_message()?, true)?;
            }
            assert_eq!(aug1, aug2);
        } else {
            assert_eq!(msg_aug_1, msg_aug_2);
        }
        Ok(true)
    })?;
    assert_eq!(msgs1, msgs2);
    Ok(())
}

fn compare_messages(msg1: &Message, msg2: &Message, _check_transaction: bool) -> Result<()> {
    assert_eq!(msg1, msg2);
    Ok(())
}

fn compare_out_msgs(msgs1: &OutMsgDescr, msgs2: &OutMsgDescr) -> Result<()> {
    msgs1.scan_diff_with_aug(msgs2, |key, msg_aug_1, msg_aug_2| {
        dbg!(&key);
        dbg!(&msg_aug_1);
        dbg!(&msg_aug_2);
        assert_eq!(msg_aug_1, msg_aug_2);
        Ok(true)
    })?;
    assert_eq!(msgs1, msgs2);
    Ok(())
}

pub fn compare_mc_blocks(block1: &Block, block2: &mut Block, full: bool) -> Result<()> {
    assert_eq!(block1.global_id(), block2.global_id(), "global_id");
    let info1 = block1.read_info()?;
    let info2 = block2.read_info()?;
    assert_eq!(info1.read_prev_ref()?, info2.read_prev_ref()?, "info");
    let extra1 = block1.read_extra()?;
    let extra2 = block2.read_extra()?;
    compare_in_msgs(&extra1.read_in_msg_descr()?, &extra2.read_in_msg_descr()?)?;
    compare_block_accounts(&extra1.read_account_blocks()?, &extra2.read_account_blocks()?)?;
    compare_out_msgs(&extra1.read_out_msg_descr()?, &extra2.read_out_msg_descr()?)?;

    let custom1 = extra1.read_custom()?.unwrap();
    let custom2 = extra2.read_custom()?.unwrap();
    let msg1 = custom1.read_recover_create_msg()?;
    let msg2 = custom2.read_recover_create_msg()?;
    // assert_eq!(msg1.read_message()?, msg2.read_message()?, "recover_create_msg");
    assert_eq!(msg1, msg2, "recover_create_msg");
    let msg1 = custom1.read_mint_msg()?;
    let msg2 = custom2.read_mint_msg()?;
    // assert_eq!(msg1.read_message()?, msg2.read_message()?, "mint_msg");
    assert_eq!(msg1, msg2, "mint_msg");
    assert_eq!(info1, info2, "info");
    assert_eq!(extra1, extra2, "extra");
    let value_flow1 = block1.read_value_flow()?;
    let value_flow2 = block2.read_value_flow()?;
    if full { // it does not work yet
        assert_eq!(value_flow1, value_flow2, "value_flow");
        assert_eq!(block1.state_update_cell(), block2.state_update_cell(), "state_update");
    } else {
        // Сan't calculate the ValueFlow, need previous block, check some fileds
        assert_eq!(value_flow1.imported, value_flow2.imported, "imported");
        assert_eq!(value_flow1.exported, value_flow2.exported, "exported");
        assert_eq!(value_flow1.fees_collected, value_flow2.fees_collected, "fees_collected");
        assert_eq!(value_flow1.fees_imported, value_flow2.fees_imported, "fees_imported");
        assert_eq!(value_flow1.recovered, value_flow2.recovered, "recovered");
        assert_eq!(value_flow1.created, value_flow2.created, "created");
        assert_eq!(value_flow1.minted, value_flow2.minted, "minted");
        // Сan't calculate the MerkleUpdate, need the original ShardState, skipped
    }
    Ok(())
}

pub fn compare_states(state1: &ShardStateUnsplit, state2: &ShardStateUnsplit)  -> Result<()> {
    let sh_acc1 = state1.read_accounts()?;
    let sh_acc2 = state2.read_accounts()?;
    sh_acc1.scan_diff_with_aug(&sh_acc2, |account_id, sh_acc_db1, sh_acc_db2| {
        dbg!(&account_id);
        // if account_id == AccountId::from([0x33; 32]) {
        //     return Ok(true)
        // }
        // if account_id == AccountId::from([0x55; 32]) {
        //     return Ok(true)
        // }
        // if account_id == AccountId::from_str("34517c7bdf5187c55af4f8b61fdc321588c7ab768dee24b006df29106458d7cf")? {
        //     return Ok(true)
        // }
        if let (Some((sh_acc1, db1)), Some((sh_acc2, db2))) = (&sh_acc_db1, &sh_acc_db2) {
            let acc1 = sh_acc1.read_account()?;
            let acc2 = sh_acc2.read_account()?;
            assert_eq!(acc1, acc2);
            assert_eq!(sh_acc1.last_trans_hash(), sh_acc2.last_trans_hash());
            assert_eq!(sh_acc1.last_trans_lt(), sh_acc2.last_trans_lt());
            assert_eq!(db1, db2);
        } else {
            assert_eq!(sh_acc_db1, sh_acc_db2);
        }
        Ok(true)
    })?;
    let msgs1 = state1.read_out_msg_queue_info()?;
    let msgs2 = state2.read_out_msg_queue_info()?;
    msgs1.out_queue().scan_diff_with_aug(msgs2.out_queue(), |key, enq1, enq2| {
        dbg!(key);
        assert_eq!(enq1, enq2);
        Ok(true)
    })?;
    assert_eq!(msgs1.out_queue(), msgs2.out_queue());
    // msgs1.proc_info().scan_diff(msgs2.proc_info(), |key: ProcessedInfoKey, info1, info2| {
    //     dbg!(key);
    //     dbg!(info1, info2);
    //     Ok(true)
    // })?;
    // assert_eq!(msgs1.proc_info(), msgs2.proc_info());
    // msgs1.ihr_pending().scan_diff(msgs2.ihr_pending(), |_key, ihr1, ihr2| {
    //     // dbg!(key);
    //     assert_eq!(ihr1, ihr2);
    //     Ok(true)
    // })?;
    assert_eq!(msgs1.ihr_pending().len()?, 0);
    assert_eq!(msgs2.ihr_pending().len()?, 0);
    assert_eq!(msgs1.ihr_pending(), msgs2.ihr_pending());
    // assert_eq!(msgs1, msgs2);
    // assert_eq!(state1.out_msg_queue_info_cell(), state2.out_msg_queue_info_cell());
    assert_eq!(state1, state2);
    Ok(())
}

/// need to rerun
/// account, smartcontrainfo(config, message)
pub fn check_transaction(
    path: &str,
    executor: &dyn TransactionExecutor,
    account: &Account,
    msg_opt: Option<&Message>,
    at: u32,
    block_lt: u64,
    address: &AccountId,
    tr: &mut Transaction,
    shard_account: &AccountBlock,
) -> Result<()> {
    log::info!(target: "collator", "checking transaction {} for account {}",
        tr.logical_time(), address.to_hex_string());

    let ethalon_tr = shard_account.transaction(tr.logical_time())?.unwrap();

    // first check hash of accounts
    assert_eq!(ethalon_tr.read_state_update()?.old_hash, tr.read_state_update()?.old_hash);
    if let Err(err) = compare_transactions(&ethalon_tr, tr, true) {
        prepare_transaction_boc(path, executor, account, msg_opt, at, block_lt, tr.logical_time())?;
        panic!("{}", err)
    }
    Ok(())
}

fn serialize_boc(cell: &Cell, path: &str, name: &str) -> Result<()> {
    let data = serialize_toc(cell)?;
    std::fs::write(format!("{}/{}", path, name), data)?;
    Ok(())
}

pub fn prepare_transaction_boc(
    path: &str,
    executor: &dyn TransactionExecutor,
    account: &Account,
    msg_opt: Option<&Message>,
    at: u32,
    block_lt: u64,
    lt: u64,
) -> Result<()> {
    std::fs::create_dir_all(path).ok();
    let mut script = "\
        #!/usr/bin/env fift -s\n\
        { char } word x>B 1 'nop } ::_ B{\n\
        { 0 word drop 0 'nop } :: //\n\n\
        // prepare stack for execute\n\
    ".to_string();
    let stack = executor.build_stack(msg_opt, &account);
    for item in stack.iter() {
        match item {
            StackItem::Integer(x) => script += &format!("{}\n", x),
            StackItem::Slice(x) => if x.remaining_references() == 0 {
                script += &format!("x{{{:X}}}\n", x)
            } else {
                let bytes = serialize_toc(&x.into_cell())?;
                script += &format!("x{{{}}} B>boc <s\n", hex::encode_upper(&bytes))
            }
            StackItem::Cell(x) => if x.references_count() == 0 {
                script += &format!("<b x{{{:X}}} s, b>\n", x)
            } else {
                let bytes = serialize_toc(x)?;
                script += &format!("x{{{}}} B>boc\n", hex::encode_upper(&bytes))
            }
            _ => unimplemented!("{:?}", item)
        }
    }
    let account_addr = if let Some(msg) = msg_opt {
        msg.write_to_file(&format!("{}/message.boc", path))?;
        // script += "\"message.boc\" file>B B>boc <s 1000000 gasrunvmcode .s";
        msg.dst().unwrap()
    } else {
        account.get_addr().cloned().unwrap()
    };

    if let Some(cell) = account.get_code() {
        serialize_boc(&cell, path, "code.boc")?;
        script += "// load code to slice\n\"code.boc\" file>B B>boc <s\n"; // code slice
    }

    if let Some(cell) = account.get_data() {
        serialize_boc(&cell, path, "data.boc")?;
        script += "// load data to cell\n\"data.boc\" file>B B>boc\n"; // data cell
    } else {
        script += "<b b>"; // empty cell
    }

    let balance = account.get_balance().cloned().unwrap_or_default();
    let other_balance = if let Some(cell) = balance.other_as_hashmap().data() {
        serialize_boc(cell, path, "balance.boc")?;
        "\"balance.boc\" file>B B>boc"
    } else {
        "dictnew"
    };
    let rand_seed = UInt256::default();

    let account_addr_cell = account_addr.serialize()?;
    serialize_boc(executor.config().raw_config().config_params.data().unwrap(), path, "config_params.boc")?;
    script += &format!("// prepare smart contract info tuple\n\
        // magic actions msgs at block_lt tr_lt\n\
        0x076ef1ea 0 0 {} {} {}\n\
        // rand seed\n\
        0x{}\n\
        // balance\n\
        {} {} 2 tuple\n\
        // address addr_std w/o anycast\n\
        x{{{:X}}}\n\
        // config\n\
        \"config_params.boc\" file>B B>boc\n\
        10 tuple\n\
        1 tuple\n\
        // run vm contract\n\
        0x55 runvmx .s\n\
    ", at, block_lt, lt, rand_seed.to_hex_string(),
        balance.grams.0, other_balance, account_addr_cell);

    std::fs::write(format!("{}/run.fif", path), script)?;
    Ok(())
}

pub fn prepare_data_for_executor_test(path: &str, acc_before: &Cell, acc_after: &Cell, msg: &Cell, trans: &Cell, config: &Cell) -> Result<()> {
    std::fs::create_dir_all(path).ok();
    serialize_boc(acc_before, path, "account_old.boc")?;
    serialize_boc(acc_after,  path, "account_new.boc")?;
    serialize_boc(msg,        path, "message.boc"    )?;
    serialize_boc(trans,      path, "transaction.boc")?;
    serialize_boc(config,     path, "config.boc"     )?;
    Ok(())

}

pub fn format_block_id(id: &BlockIdExt) -> String {
    format!(r#"
        let block_id = BlockIdExt {{
            shard_id: ShardIdent::with_tagged_prefix({}, 0x{}).unwrap(),
            seq_no: {},
            root_hash: UInt256::from_str("{:x}").unwrap(),
            file_hash: UInt256::from_str("{:x}").unwrap(),
        }};"#,
        id.shard().workchain_id(), id.shard().shard_prefix_as_str_with_tag(), id.seq_no(), 
        id.root_hash(), id.file_hash()
    )
}

#[ignore]
#[test]
fn show_content() {
    // trace_object("C:\\Users\\Sergey\\Downloads\\block_boc_0c473247dab568b81a9b44b731dec781f0c2349b43614cedcc824d0b4454f1d6.boc").unwrap();
    // trace_object("C:\\full-node-test\\blocks\\8000000000000000\\3258710").unwrap();
    // trace_object("C:\\full-node-test\\blocks\\8000000000000000\\3258711").unwrap();
    panic!("need to read");
}

use std::time::Instant;
use std::sync::atomic::{AtomicU64, Ordering};
use ton_block::TrComputePhase;
use ton_executor::{BlockchainConfig, OrdinaryTransactionExecutor};

#[ignore]
#[test]
fn uncover_block() {
    // prepare config
    let block = Block::construct_from_file("D:\\block_boc_382c76d2202ea3840bb1729449522b4eea7cd6b194ae4e2d21b1aecd83d0c59e.boc").unwrap();
    let config = block.read_extra().unwrap().read_custom().unwrap().unwrap().config().unwrap().clone();

    // prepare state
    let state = ShardStateUnsplit::construct_from_file("D:\\state_125190_0_3800000000000000_1C9186AB8650F24F3BE3C927A337C2BF45869EE148A850F78AD7AD19E4FCE2A2").unwrap();
    let shard_accounts = state.read_accounts().unwrap();

    // prepare block candidate
    let data = std::fs::read("D:\\38").unwrap();
    let data = hex::decode(data).unwrap();
    let block = Block::construct_from_bytes(&data).unwrap();
    // panic!("block {}", debug_block(block.clone()).unwrap());
    let info = block.read_info().unwrap();
    assert!(state.shard() == info.shard());
    assert!(state.seq_no() + 1 == info.seq_no());
    let extra = block.read_extra().unwrap();
    let account_blocks = extra.read_account_blocks().unwrap();
    // let in_msgs = extra.read_in_msg_descr().unwrap();
    // in_msgs.dump();
    let config_cell = config.serialize().unwrap();
    let config = BlockchainConfig::with_config(config).unwrap();
    println!("block info: {:?}", info);
    let mut min = (std::u128::MAX, 0, UInt256::default());
    let mut max = (std::u128::MIN, 0, UInt256::default());
    let mut avg = 0;
    let mut cnt = 0;
    let mut gas = 0;
    let now = Instant::now();
    // init_test_log();
    let debug_hash = UInt256::default();
    // let debug_hash = UInt256::from_str("2df4b47898ff61fec2a6f01384dff264fbc07e6404a7d924bf93395ed932fe89").unwrap();
    let _debug_account_id = AccountId::from_string("3521430decf7eb0e3849a4e62646a7186964c86376042772cf1798a5c2e52511").unwrap();
    account_blocks.iterate_with_keys(|account_addr, acc_block| {
        println!("account id: {}", account_addr.to_hex_string());
        let account_id = AccountId::from(account_addr);
        let shard_account = shard_accounts.account(&account_id)?.unwrap_or_default();
        let mut account_root = shard_account.account_cell().clone();
        acc_block.transactions().iterate_slices(|_lt, trans| {
            let trans_cell = trans.reference(0).unwrap();
            println!("transaction id: {}", trans_cell.repr_hash().to_hex_string());
            let trans = Transaction::construct_from(&mut trans_cell.clone().into()).unwrap();
            let msg_cell = trans.in_msg_cell().unwrap().clone();
            let msg_hash = msg_cell.repr_hash();
            println!("input message id: {}", msg_hash.to_hex_string());
            let msg = Message::construct_from_cell(msg_cell.clone()).unwrap();
            let at = info.gen_utime().0;
            let block_lt = info.start_lt();
            let lt = Arc::new(AtomicU64::new(trans.logical_time()));
            let executor = OrdinaryTransactionExecutor::new(config.clone());
            let debug = msg_hash == debug_hash;
            let account = Account::construct_from_cell(account_root.clone()).unwrap();
            let old_account_root = account_root.clone();
            if let Some(TrComputePhase::Vm(vm_phase)) = trans.read_description().unwrap().compute_phase_ref() {
                gas += vm_phase.gas_used.0;
            }
            let now = Instant::now();
            let _trans2 = executor.execute(Some(&msg), &mut account_root, at, block_lt, lt.clone(), debug).unwrap();
            let time = now.elapsed().as_micros();
            if debug {
                let path = "target/check";
                crate::test_helper::prepare_transaction_boc(path, &executor, &account, Some(&msg), at, block_lt, lt.load(Ordering::Relaxed)).unwrap();
                crate::test_helper::prepare_data_for_executor_test(path, &old_account_root, &account_root, &msg_cell, &trans_cell, &config_cell).unwrap();
                if let Err(err) = crate::test_helper::compare_transactions(&trans, &_trans2, true) {
                    panic!("\n{}", err)
                }
                return Ok(false)
            }
            cnt += 1;
            avg += time;
            if time > max.0 {
                max.0 = time;
                max.1 = cnt;
                max.2 = msg_hash.clone();
            }
            if time < min.0 {
                min.0 = time;
                min.1 = cnt;
                min.2 = msg_hash;
            }
            Ok(true)
        })
    }).unwrap();
    println!("total gas: {}, total time: {} for {}", gas, now.elapsed().as_micros(), cnt);
    println!("min time: {} hash: {}", min.0, min.2.to_hex_string());
    println!("max time: {} hash: {}", max.0, max.2.to_hex_string());
    println!("avg: {}", avg / cnt);
    panic!("all right");
    // blocks.iterate_with_keys()
}
