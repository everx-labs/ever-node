/*
* Copyright (C) 2019-2023 EverX Labs. All Rights Reserved.
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

#![allow(dead_code)]
use crate::{
    block::BlockStuff, block_proof::BlockProofStuff, config::{CollatorConfig, TonNodeConfig}, 
    collator_test_bundle::create_engine_allocated,
    full_node::apply_block::apply_block,
    internal_db::{
        LAST_APPLIED_MC_BLOCK, SHARD_CLIENT_MC_BLOCK,
        BlockResult, InternalDb, InternalDbConfig,
    },
    engine_traits::{EngineAlloc, EngineOperations}, ext_messages::MessagesPool, 
    network::node_network::NodeNetwork, shard_blocks::ShardBlocksPool, 
    shard_state::ShardStateStuff,
    types::top_block_descr::TopBlockDescrStuff,
    validator::{collator::Collator, CollatorSettings, validate_query::ValidateQuery}
};
use crate::validator::{
    accept_block::create_top_shard_block_description, validator_utils::compute_validator_set_cc
};
#[cfg(feature = "telemetry")]
use crate::{collator_test_bundle::create_engine_telemetry, engine_traits::EngineTelemetry};

use std::{path::Path, sync::{{Arc, RwLock}, atomic::{AtomicU32, Ordering}}, time::Duration};
use storage::block_handle_db::{BlockHandle, Callback, StoreJob};
use ton_api::ton::ton_node::broadcast::BlockBroadcast;
use ton_block::*;
use ton_block_json::*;
use ton_types::{error, write_boc, Cell, UInt256};

include!("../../common/src/config.rs");
include!("../../common/src/test.rs");

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

pub fn are_shard_states_equal(ss1: &ShardStateStuff, ss2: &ShardStateStuff) -> bool {
    (ss1.block_id() == ss2.block_id()) &&
    (ss1.state().unwrap() == ss2.state().unwrap()) &&
    (ss1.root_cell() == ss2.root_cell()) &&
    if let Ok(extra1) = ss1.shard_state_extra() {
        if let Ok(extra2) = ss2.shard_state_extra() {
            extra1 == extra2
        } else {
            false
        }
    } else {
        if ss2.shard_state_extra().is_ok() {
            false
        } else {
            true
        }
    } 
}

#[cfg(feature = "fast_finality")]
fn ref_shard_blocks(ref_shard_blocks: &RefShardBlocks) -> Result<Vec<BlockIdExt>> {
    let mut shards = Vec::new();
    ref_shard_blocks.iterate_shard_block_refs(&mut |id, _end_lt| {
        shards.push(id);
        Ok(true)
    })?;
    Ok(shards)
}

#[cfg(feature = "fast_finality")]
async fn check_block_refs_loops(engine: &Arc<dyn EngineOperations>, block_stuff: &BlockStuff) -> Result<()> {
    let shards = ref_shard_blocks(block_stuff.block()?.read_extra()?.ref_shard_blocks())?;
    for block_id in &shards {
        for id in shards.iter().filter(|id| *id != block_id) {
            let handle = engine.load_block_handle(id)?.ok_or_else(
                || error!("Cannot load handle for block {}", id)
            )?;
            let block = engine.load_block(&handle).await?;
            let shards = ref_shard_blocks(block.block()?.read_extra()?.ref_shard_blocks())?;
            for ref_id in &shards {
                if ref_id.seq_no() > block_id.seq_no() && ref_id.shard().intersect_with(block_id.shard()) {
                    fail!("for {} ref shard {} has reference to newer shard block {}", block_stuff.id(), block_id, ref_id)
                }
            }
        }
    }
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
            // println!("{:#.3}", acc1.transactions().data().unwrap());
            // println!("{:#.3}", acc1.transactions().data().unwrap());
            assert_eq!(acc1.transactions(), acc2.transactions());
            assert_eq!(cur1, cur2);
            assert_eq!(acc1.read_state_update()?, acc2.read_state_update()?);
        } else {
            assert_eq!(acc_cur_1, acc_cur_2);
        }
        Ok(true)
    })?;
    // std::fs::write("./target/cmp/1.txt", &format!("{:#.3}", acc1.data().unwrap())).unwrap();
    // std::fs::write("./target/cmp/2.txt", &format!("{:#.3}", acc2.data().unwrap())).unwrap();
    assert_eq!(acc1, acc2);
    Ok(())
}

pub fn compare_blocks(block1: &Block, block2: &mut Block) -> Result<()> {
    assert_eq!(block1.global_id(), block2.global_id(), "global_id");
    let info1 = block1.read_info()?;
    let info2 = block2.read_info()?;
    assert_eq!(info1.read_prev_ref()?, info2.read_prev_ref()?, "info");
    let extra1 = block1.read_extra()?;
    let extra2 = block2.read_extra()?;
    compare_in_msgs(&extra1.read_in_msg_descr()?, &extra2.read_in_msg_descr()?)?;
    compare_out_msgs(&extra1.read_out_msg_descr()?, &extra2.read_out_msg_descr()?)?;
    compare_block_accounts(&extra1.read_account_blocks()?, &extra2.read_account_blocks()?)?;

    let custom1 = extra1.read_custom()?;
    let custom2 = extra2.read_custom()?;
    if custom1.is_some() && custom2.is_some() {
        let custom1 = custom1.unwrap();
        let custom2 = custom2.unwrap();
        let msg1 = custom1.read_recover_create_msg()?;
        let msg2 = custom2.read_recover_create_msg()?;
        // assert_eq!(msg1.read_message()?, msg2.read_message()?, "recover_create_msg");
        assert_eq!(msg1, msg2, "recover_create_msg");
        let msg1 = custom1.read_mint_msg()?;
        let msg2 = custom2.read_mint_msg()?;
        // assert_eq!(msg1.read_message()?, msg2.read_message()?, "mint_msg");
        assert_eq!(msg1, msg2, "mint_msg");
        assert_eq!(custom1, custom2);
    } else {
        assert_eq!(custom1, custom2);
    }
    assert_eq!(info1, info2, "info");
    assert_eq!(extra1, extra2, "extra");
    let value_flow1 = block1.read_value_flow()?;
    let value_flow2 = block2.read_value_flow()?;
    assert_eq!(value_flow1, value_flow2, "value_flow");
    assert_eq!(block1.read_state_update()?.new_hash, block2.read_state_update()?.new_hash, "state_update new hash");
    Ok(())
}

fn compare_in_msgs(msgs1: &InMsgDescr, msgs2: &InMsgDescr) -> Result<()> {
    msgs1.scan_diff_with_aug(msgs2, |_key, msg_aug_1, msg_aug_2| {
        dbg!(&_key);
        // dbg!(&msg_aug_1);
        // dbg!(&msg_aug_2);
        // let _tr = msg_aug_1.as_ref().unwrap().0.read_transaction()?.unwrap();
        // dbg!(debug_transaction(_tr)?);
        if let (Some((msg1, aug1)), Some((msg2, aug2))) = (&msg_aug_1, &msg_aug_2) {
            if let (Some(tr1), Some(tr2)) = (msg1.read_transaction()?, msg2.read_transaction()?) {
                compare_transactions(&tr1, &tr2, false)?;
                compare_messages(&msg1.read_message()?, &msg2.read_message()?, false)?;
            } else {
                compare_messages(&msg1.read_message()?, &msg2.read_message()?, true)?;
            }
            assert_eq!(aug1, aug2);
        } else if let Some(msg_aug_2) = msg_aug_2 {
            println!("{}", debug_message(msg_aug_2.0.read_message()?.clone())?);
            assert_eq!(msg_aug_1, Some(msg_aug_2));
        } else {
            if let Some((msg1, _aug1)) = msg_aug_1.clone() {
                println!("only in msgs1 {:?}", msg1.read_message()?);
                println!("only in msgs1 {:?}", msg1.read_transaction()?);
            }
            if let Some((msg2, _aug2)) = msg_aug_2.clone() {
                println!("only in msgs2 {:?}", msg2.read_message()?);
                println!("only in msgs2 {:?}", msg2.read_transaction()?);
            }
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
        // assert_eq!(msg_aug_1, msg_aug_2);
        Ok(true)
    })?;
    assert_eq!(msgs1, msgs2);
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

pub fn full_trace_block(name: &str, block: &Block) -> Result<()> {
    let mut text = format!("Block: {}\n", debug_block(block.clone())?);
    let extra = block.read_extra()?;
    let in_msgs = extra.read_in_msg_descr()?;
    in_msgs.iterate_objects(|in_msg| {
        let msg = in_msg.read_message()?;
        text += &format!("InMsg: {}\n", debug_message(msg)?);
        Ok(true)
    })?;
    let out_msgs = extra.read_out_msg_descr()?;
    out_msgs.iterate_objects(|out_msg| {
        if let Some(msg) = out_msg.read_message()? {
            text += &format!("OutMsg: {}\n", debug_message(msg)?);
        }
        Ok(true)
    })?;
    let acc_blocks = extra.read_account_blocks()?;
    acc_blocks.iterate_objects(|block| {
        block.transactions().iterate_objects(|InRefValue(tr)| {
            text += &format!("Transaction: {}\n", debug_transaction(tr)?);
            Ok(true)
        })
    })?;
    std::fs::write(name, text)?;
    Ok(())
}

pub fn gen_master_state(
    config: Option<ConfigParams>,
    shard_state_id: Option<BlockIdExt>,
    master_state_id: Option<BlockIdExt>,
    accounts: &[&Account],
    #[cfg(feature = "telemetry")]
    telemetry: Option<Arc<EngineTelemetry>>,
    allocated: Option<Arc<EngineAlloc>>
) -> (BlockIdExt, Arc<ShardStateStuff>) {
    let mut ss = ShardStateUnsplit::with_ident(ShardIdent::masterchain());
    for account in accounts {
        let account_id = UInt256::from(account.get_id().unwrap().get_next_hash().unwrap());
        ss.insert_account(
            &account_id, 
            &ShardAccount::with_params(&account, UInt256::default(), 0).unwrap()
        ).unwrap();
    }
    let mut ms = McStateExtra::default();
    if let Some(config) = config {
        ms.config = config;
    } else {
        let mut param = ConfigParam0::new();
        param.config_addr = UInt256::from([1;32]); 
        ms.config.set_config(ConfigParamEnum::ConfigParam0(param)).unwrap();
        let mut param = ConfigParam34::new();
        param.cur_validators = ValidatorSet::new(
            1600000000,
            1610000000,
            1,
            vec![ValidatorDescr::default()]
        ).unwrap();
        ms.config.set_config(ConfigParamEnum::ConfigParam34(param)).unwrap();
    }

    if let Some(shard_state_id) = shard_state_id {
        ms.shards.add_workchain(
            0, 
            0,
            shard_state_id.root_hash.clone(),
            shard_state_id.file_hash.clone(),
            #[cfg(feature = "fast_finality")] Some(ton_block::ShardCollators::default()),
            #[cfg(not(feature = "fast_finality"))] None
        ).unwrap();
    }
    if let Some(master_state_id) = &master_state_id {
        ss.set_seq_no(master_state_id.seq_no());
        ss.set_shard(master_state_id.shard().clone());
    }
    ss.write_custom(Some(&ms)).unwrap();
    let cell = ss.serialize().unwrap();
    let bytes = ton_types::write_boc(&cell).unwrap();
    #[cfg(feature = "telemetry")]
    let telemetry = telemetry.unwrap_or_else(|| create_engine_telemetry());
    let allocated = allocated.unwrap_or_else(|| create_engine_allocated());
    if let Some(master_state_id) = master_state_id {
        let master_state = ShardStateStuff::deserialize_state(
            master_state_id.clone(), 
            &bytes,
            #[cfg(feature = "telemetry")]
            &telemetry,
            &allocated
        ).unwrap();
        (master_state_id, master_state)
    } else {
        let master_state_id = BlockIdExt::with_params(
            ShardIdent::masterchain(),
            0,
            cell.repr_hash(),
            UInt256::calc_file_hash(&bytes)
        );
        let master_state = ShardStateStuff::deserialize_zerostate(
            master_state_id.clone(), 
            &bytes,
            #[cfg(feature = "telemetry")]
            &telemetry,
            &allocated
        ).unwrap();
        (master_state_id, master_state)
    }
}

pub fn gen_shard_state(
    shard_state_id: Option<BlockIdExt>,
    accounts: &[&Account],
    #[cfg(feature = "telemetry")]
    telemetry: Option<Arc<EngineTelemetry>>,
    allocated: Option<Arc<EngineAlloc>>,
    master_ref: Option<BlkMasterInfo>,
) -> (BlockIdExt, Arc<ShardStateStuff>) {
    let mut ss = ShardStateUnsplit::with_ident(ShardIdent::full(0));
    ss.set_master_ref(master_ref);
    for account in accounts {
        let account_id = UInt256::from(account.get_id().unwrap().get_next_hash().unwrap());
        ss.insert_account(
            &account_id, 
            &ShardAccount::with_params(&account, UInt256::default(), 0).unwrap()
        ).unwrap();
    }
    let cell = ss.serialize().unwrap();
    let bytes = ton_types::write_boc(&cell).unwrap();
    #[cfg(feature = "telemetry")]
    let telemetry = telemetry.unwrap_or_else(|| create_engine_telemetry());
    let allocated = allocated.unwrap_or_else(|| create_engine_allocated());
    if let Some(shard_state_id) = shard_state_id {
        let shard_state = ShardStateStuff::deserialize_state(
            shard_state_id.clone(), 
            &bytes,
            #[cfg(feature = "telemetry")]
            &telemetry,
            &allocated
        ).unwrap();
        (shard_state_id, shard_state)
    } else {
        let shard_state_id = BlockIdExt::with_params(
            ShardIdent::full(0),
            0,
            cell.repr_hash(),
            UInt256::calc_file_hash(&bytes)
        );
        let shard_state = ShardStateStuff::deserialize_zerostate(
            shard_state_id.clone(), 
            &bytes,
            #[cfg(feature = "telemetry")]
            &telemetry,
            &allocated
        ).unwrap();
        (shard_state_id, shard_state)
    }
}

pub async fn get_config(
    ip: &str, 
    config_dir: Option<&str>, 
    default: &str
) -> Result<TonNodeConfig> {
    let resolved_ip = resolve_ip(ip).await?;
    let config_path = get_test_config_path("node", &resolved_ip)?;
    let config_dir = if let Some(config_dir) = config_dir {
        Path::new(config_dir)
    } else if let Some(config_dir) = config_path.parent() {
        config_dir
    } else {
        fail!("No parent in config path {}", config_path.display())
    };
    let Some(config_dir) = config_dir.to_str() else {
        fail!("Cannot use config dir {}", config_dir.display())
    };
    let Some(config_file) = config_path.file_name() else {
        fail!("No file name in config path {}", config_path.display())
    };
    let Some(config_file) = config_file.to_str() else {
        fail!("Cannot use config file {:?}", config_file)
    };
    let (adnl_config, _) = generate_adnl_configs(
        ip, 
        vec![NodeNetwork::TAG_DHT_KEY, NodeNetwork::TAG_OVERLAY_KEY], 
        Some(resolved_ip)                                                                      
    )?;
    TonNodeConfig::from_file(
        config_dir,
        config_file,
        Some(adnl_config), 
        format!("../configs/{}", default).as_str(),
        None
    )
}

pub fn prepare_data_for_executor_test(
    path: &str, 
    acc_before: &Cell, 
    acc_after: &Cell, 
    msg: &Cell, 
    trans: &Cell, 
    config: &Cell
) -> Result<()> {
    std::fs::create_dir_all(path).ok();
    serialize_boc(acc_before, path, "account_old.boc")?;
    serialize_boc(acc_after,  path, "account_new.boc")?;
    serialize_boc(msg,        path, "message.boc"    )?;
    serialize_boc(trans,      path, "transaction.boc")?;
    serialize_boc(config,     path, "config.boc"     )?;
    Ok(())

}

fn serialize_boc(cell: &Cell, path: &str, name: &str) -> Result<()> {
    let data = write_boc(cell)?;
    std::fs::write(format!("{}/{}", path, name), data)?;
    Ok(())
}

pub struct TestEngine {
    pub res_path: Option<String>,
    pub db: Arc<InternalDb>,
    pub now: Arc<AtomicU32>,
    pub ext_messages: Arc<MessagesPool>,
    pub shard_states: lockfree::map::Map<ShardIdent, ShardStateStuff>,
    pub check_only_transactions: bool,
    pub check_only_masterchain: bool,
    pub check_only_msg_merger: bool,
    pub shard_blocks: ShardBlocksPool,
    last_applied_mc_block_id: RwLock<Option<Arc<BlockIdExt>>>,
    #[cfg(feature = "telemetry")]
    engine_telemetry: Arc<EngineTelemetry>,
    engine_allocated: Arc<EngineAlloc>
}

impl TestEngine {

    pub async fn new_db_dir(db_dir: &str, res_path: Option<&str>) -> Result<Self> {
        if let Err(err) = std::fs::read_dir(db_dir) {
            fail!("Directory not found: {} {}", db_dir, err);
        }
        if let Some(res_path) = &res_path {
            std::fs::create_dir_all(res_path).ok();
        }
        let db_config = InternalDbConfig { 
            db_directory: db_dir.to_string(),
            ..Default::default()
        };
        #[cfg(feature = "telemetry")]
        let telemetry = create_engine_telemetry();
        let allocated = create_engine_allocated();
        let db = Arc::new(
            InternalDb::with_update(
                db_config, 
                false,
                false,
                false,
                &|| Ok(()),
                None,
                #[cfg(feature = "telemetry")]
                telemetry.clone(),
                allocated.clone(),
            ).await?
        );
        let shard_blocks = db.load_all_top_shard_blocks().unwrap_or_default();
        let last_mc_seqno = db
            .load_full_node_state(LAST_APPLIED_MC_BLOCK)?
            .map(|id| id.seq_no as u32)
            .unwrap_or_default();
        let (shard_blocks, _) = ShardBlocksPool::new(
            shard_blocks, 
            last_mc_seqno, 
            true,
            #[cfg(feature = "telemetry")]
            &telemetry,
            &allocated
        )?;
        Ok(Self {
            db,
            res_path: res_path.map(|s| s.to_string()),
            now: Arc::new(AtomicU32::new(0)),
            ext_messages: Arc::new(MessagesPool::new(0)),
            shard_states: Default::default(),
            check_only_transactions: false,
            check_only_masterchain: false,
            check_only_msg_merger: false,
            shard_blocks,
            last_applied_mc_block_id: RwLock::new(None),
            #[cfg(feature = "telemetry")]
            engine_telemetry: telemetry,
            engine_allocated: allocated
        })
    }

    pub async fn change_mc_state(&self, mc_state_id: &BlockIdExt) -> Result<()> {
        let mc_state = self.db.load_shard_state_dynamic(&mc_state_id)?;
        self.save_last_applied_mc_block_id(&mc_state_id)?;
        self.shard_blocks.update_shard_blocks(&mc_state).await?;
        Ok(())
    }

    pub async fn change_mc_state_by_seqno(&self, mc_seq_no: u32) -> Result<BlockIdExt> {
        let mc_state_id = self.db.load_full_node_state(LAST_APPLIED_MC_BLOCK)?.unwrap();
        let mc_state = self.load_state(&mc_state_id).await?;
        let (_, mc_state_id, _) = mc_state.shard_state_extra()?.prev_blocks.get(&mc_seq_no)?.unwrap().master_block_id();
        log::debug!("Changing last masterchain state to {}", mc_state_id);
        self.change_mc_state(&mc_state_id).await?;
        Ok(mc_state_id)
    }

    pub async fn load_block_by_id(&self, id: &BlockIdExt) -> Result<BlockStuff> {
        let handle = self.load_block_handle(id)?.ok_or_else(
            || error!("Cannot load handle for block {}", id)
        )?;
        self.load_block(&handle).await
    }

    pub async fn prepare_for_block(&mut self, block_stuff: &BlockStuff) -> Result<()> {
        let info = block_stuff.block()?.read_info()?;
        let master_ref_seq_no = if let Some(master_ref) = info.read_master_ref()? {
            master_ref.master.seq_no
        } else {
            assert!(block_stuff.id().shard().is_masterchain());
            block_stuff.id().seq_no() - 1
        };
        if let Err(err) = self.change_mc_state_by_seqno(master_ref_seq_no).await {
            log::error!("DB do not have MC state for block: {} {}", block_stuff.id(), err);
            return Ok(())
        }
        self.now.store(block_stuff.gen_utime()?, Ordering::Relaxed);
        let extra = block_stuff.block()?.read_extra()?;
        let in_msgs = extra.read_in_msg_descr()?;
        // self.ext_messages.clear();
        in_msgs.iterate_with_keys(|key, in_msg| {
            let msg = in_msg.read_message()?;
            if msg.is_inbound_external() {
                self.ext_messages.new_message(key, Arc::new(msg), self.now())?;
            }
            Ok(true)
        })?;
        Ok(())
    }

    pub fn has_external_messages(&self) -> bool {
        self.ext_messages.has_messages()
    }

    async fn get_prev_mc_state(self: &Arc<Self>, block_id: &BlockIdExt) -> Result<(Arc<ShardStateStuff>, Vec<BlockIdExt>)> {
        let block_stuff = self.load_block_by_id(block_id).await?;
        let (mc_state, shard_blocks);
        if block_id.shard().is_masterchain() {
            let state = self.load_state(block_id).await?;
            let mc_seq_no = block_stuff.id().seq_no() - 1;
            let (_, mc_state_id, _) = state.shard_state_extra()?.prev_blocks.get(&mc_seq_no)?.unwrap().master_block_id();
            mc_state = self.load_state(&mc_state_id).await?;
            shard_blocks = mc_state.top_blocks_all()?;
        } else {
            let block = block_stuff.block()?;
            #[cfg(feature = "fast_finality")] {
                shard_blocks = ref_shard_blocks(block.read_extra()?.ref_shard_blocks())?;
                // check_block_refs_loops(&engine, &block_stuff).await?;
            }
            #[cfg(not(feature = "fast_finality"))] {
                shard_blocks = Vec::new();
            }
            let info = block.read_info()?;
            let (_, mc_state_id) =  info.read_master_id()?.master_block_id();
            mc_state = self.load_state(&mc_state_id).await?;
        }
        Ok((mc_state, shard_blocks))
    }

    pub async fn check_block(self: &Arc<Self>, block_id: &BlockIdExt) -> Result<()> {
        let engine = self.clone() as Arc<dyn EngineOperations>;
        let (mc_state, shard_blocks) = self.get_prev_mc_state(block_id).await?;

        let block_stuff = self.load_block_by_id(block_id).await?;
        let info = block_stuff.block()?.read_info()?;
        let prev_blocks_ids = info.read_prev_ids()?;

        let mc_state_extra = mc_state.shard_state_extra()?;
        let cc_seqno_from_state = if block_id.shard().is_masterchain() {
            mc_state_extra.validator_info.catchain_seqno
        } else {
            mc_state_extra.shards.calc_shard_cc_seqno(block_id.shard())?
        };
        // let (validator_set, _) = mc_state.read_cur_validator_set_and_cc_conf()?;
        let mut cc_seqno_with_delta = 0;
        let nodes = compute_validator_set_cc(
            &*mc_state,
            &block_id.shard(),
            block_id.seq_no(),
            cc_seqno_from_state,
            &mut cc_seqno_with_delta
        )?;
        let validator_set = ValidatorSet::with_cc_seqno(0, 0, 0, cc_seqno_with_delta, nodes)?;
    
        // build TSBD for each shard block
        // let mut descr = TopBlockDescrSet::default();
        for block_id in &shard_blocks {
            let block_stuff = match self.load_block_by_id(block_id).await {
                Ok(block_stuff) => block_stuff,
                Err(err) => {
                    log::error!("{}", err);
                    continue;
                }
            };
            let info = block_stuff.block()?.read_info()?;
            let prev_blocks_ids = info.read_prev_ids()?; // TODO: this should be chain of ids not prev ids
            let base_info = ValidatorBaseInfo::with_params(
                info.gen_validator_list_hash_short(),
                info.gen_catchain_seqno()
            );
            // sometimes some shards don't have states in not full database
            let signatures = BlockSignatures::with_params(base_info, Default::default());
            let tbd_opt = create_top_shard_block_description(
                &block_stuff,
                signatures,
                &mc_state,
                &prev_blocks_ids,
                &*engine
            ).await?;
            if let Some(tbd) = tbd_opt {
                assert_ne!(0, tbd.chain().len());
                let tbds = Arc::new(TopBlockDescrStuff::new(tbd, block_id, true, true)?);
                self.shard_blocks.process_shard_block(
                    block_id,
                    info.gen_catchain_seqno(),
                    || Ok(tbds.clone()),
                    false,
                    &*engine
                ).await?;
            }
        }
        self.try_collate_with_compare(validator_set, block_stuff, prev_blocks_ids).await
    }
    
    async fn try_collate_with_compare(
        self: &Arc<Self>,
        validator_set: ValidatorSet,
        block_stuff: BlockStuff,
        prev_blocks_ids: Vec<BlockIdExt>,
    ) -> Result<()> {

        let info = block_stuff.block()?.read_info()?;
        let extra = block_stuff.block()?.read_extra()?;
        let (_, block_id) = info.read_master_id()?.master_block_id();
        self.save_last_applied_mc_block_id(&block_id)?;
    
        let min_mc_seqno = info.min_ref_mc_seqno() - 1;
    
        log::info!("TRY COLLATE block {}, min_mc_seqno {}", block_stuff.id(), min_mc_seqno);
    
        let collator = Collator::new(
            block_stuff.id().shard().clone(),
            min_mc_seqno,
            prev_blocks_ids.clone(),
            validator_set.clone(),
            extra.created_by().clone(),
            self.clone(),
            Some(extra.rand_seed().clone()),
            CollatorSettings::default(),
        )?;

        let (block_candidate, new_state) = collator.collate().await?;   
            
        if let Some(res_path) = &self.res_path {

            let new_block = Block::construct_from_bytes(&block_candidate.data)?;
            let su1 = block_stuff.block()?.read_state_update()?;
            let su2 = new_block.read_state_update()?;

            std::fs::write(
                &format!("{}/update.txt", res_path), 
                format!("old: {:#.1024}\nnew: {:#.1024}", su1.old, su1.new)
            )?;
            std::fs::write(
                &format!("{}/update_candidate.txt", res_path), 
                format!("old: {:#.1024}\nnew: {:#.1024}", su2.old, su2.new)
            )?;

            // let shard = block_stuff.id().shard().shard_key(false);
            // let shard = format!("{:x}-{}", shard, block_stuff.id().seq_no());

            block_stuff.block()?.write_to_file(&format!("{}/block_real.boc", res_path))?;
            new_block.write_to_file(&format!("{}/block_candidate.boc", res_path))?;
            std::fs::write(
                &format!("{}/collated_data.bin", res_path), 
                &block_candidate.collated_data
            )?;

            full_trace_block(
                &format!("{}/block_real.txt", res_path), 
                block_stuff.block()?
            )?;
            full_trace_block(
                &format!("{}/block_candidate.txt", res_path), 
                &new_block
            )?;
            // full_trace_block(
            //     &format!("{}/{}-block_real.txt", res_path, shard), 
            //     block_stuff.block()?
            // )?;
            // full_trace_block(
            //     &format!("{}/{}-block_candidate.txt", res_path, shard), 
            //     &new_block
            // )?;

            let state_stuff = self.load_state(block_stuff.id()).await?;
            std::fs::write(
                &format!("{}/state_real.txt", res_path), 
                ton_block_json::debug_state(state_stuff.state()?.clone())?
            )?;
            std::fs::write(
                &format!("{}/state_candidate.txt", res_path), 
                ton_block_json::debug_state(new_state.clone())?
            )?;

            // std::fs::write(
            //     &format!("{}/{}-state_real.txt", res_path, shard), 
            //     ton_block_json::debug_state(state_stuff.state()?.clone())?
            //)?;
            // std::fs::write(
            //     &format!("{}/{}-state_candidate.txt", res_path, shard), 
            //     ton_block_json::debug_state(new_state.clone())?
            //)?;
    
            // let cell = ton_types::deserialize_tree_of_cells(
            //     &mut std::io::Cursor::new(&block_candidate.data)
            // )?;
            // std::fs::write(
            //     &format!("{}/boc_real.txt", res_path), 
            //     format!("{:#.1024}", block_stuff.root_cell())
            // )?;
            // std::fs::write(
            //     &format!("{}/boc_candidate.txt", res_path), 
            //     format!("{:#.1024}", &cell) 
            // )?;
    
            // let cell = new_state.serialize()?;
            // std::fs::write(
            //     &format!("{}/toc_real.txt", res_path), 
            //     format!("{:#.1024}", state_stuff.root_cell())
            // )?;
            // std::fs::write(
            //     &format!("{}/toc_candidate.txt", res_path), 
            //     format!("{:#.1024}", &cell)
            // )?;

        }
    
        let validator_query = ValidateQuery::new(
            block_stuff.id().shard().clone(),
            min_mc_seqno,
            prev_blocks_ids,
            block_candidate.clone(),
            validator_set,
            self.clone(),
            true,
            true,
        );
        validator_query.try_validate().await?;
    
        // let mut error = String::new();
        // if let Err(err) = compare_states(state_stuff.state()?, &new_state) {
        //     writeln!(error, "{}", err)?;
        // }
        // if let Err(err) = compare_blocks(block_stuff.block()?, &mut new_block) {
        //     writeln!(error, "{}", err)?;
        // }
        // if !error.is_empty() {
        //     block_stuff.block()?.write_to_file(&format!("{}/block_real.boc", self.res_path))?;
        //     new_block.write_to_file(&format!("{}/block_candidate.boc", self.res_path))?;
        //     panic!("{}", error)
        // }
    
        Ok(())
    }

}

#[async_trait::async_trait]
impl EngineOperations for TestEngine {

    fn now(&self) -> u32 {
        self.now.load(Ordering::Relaxed)
    }

    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>> {
        self.db.load_block_handle(id)
    }

    async fn load_state(&self, block_id: &BlockIdExt) -> Result<Arc<ShardStateStuff>> {
        self.db.load_shard_state_dynamic(block_id)
    }

    async fn load_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        self.db.load_block_data(handle).await
    }

    async fn load_block_proof(
        &self, 
        handle: &Arc<BlockHandle>, 
        is_link: bool
    ) -> Result<BlockProofStuff> {
        self.db.load_block_proof(handle, is_link).await
    }

    fn load_last_applied_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
        match self.last_applied_mc_block_id.read().unwrap().to_owned() {
            Some(id) => Ok(Some(id)),
            None => self.db.load_full_node_state(LAST_APPLIED_MC_BLOCK)
        }
    }
    fn save_last_applied_mc_block_id(&self, last_mc_block: &BlockIdExt) -> Result<()> {
        *self.last_applied_mc_block_id.write().unwrap() = Some(Arc::new(last_mc_block.clone()));
        Ok(())
    }
    async fn load_last_applied_mc_state(&self) -> Result<Arc<ShardStateStuff>> {
        let id = if let Some(id) = self.load_last_applied_mc_block_id()? {
            id
        } else {
            fail!("No last applied MC block set")
        };
        self.load_state(&id).await
    }
    fn load_shard_client_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
        self.db.load_full_node_state(SHARD_CLIENT_MC_BLOCK)
    }

    async fn send_block_broadcast(&self, _broadcast: BlockBroadcast) -> Result<()> {
        Ok(())
    }

    async fn send_top_shard_block_description(
        &self,
        _tbd: Arc<TopBlockDescrStuff>,
        _cc_seqno: u32,
        _resend: bool,
    ) -> Result<()> {
        Ok(())
    }

    async fn store_block_proof(
        &self, 
        id: &BlockIdExt, 
        handle: Option<Arc<BlockHandle>>, 
        proof: &BlockProofStuff
    ) -> Result<BlockResult> {
        self.db.store_block_proof(id, handle, proof, None).await
    }

    async fn store_block(
        &self, 
        block: &BlockStuff
    ) -> Result<BlockResult> {
        let tr_count = block.block()?.read_extra()?.read_account_blocks()?.count_transactions()?;
        log::trace!(
            target: "nodese", 
            "block: {}:{}, transactions: {}", 
            block.id().shard(), block.id().seq_no(), tr_count
        );
        self.db.store_block_data(block, None).await
    }

    fn store_block_prev1(&self, handle: &Arc<BlockHandle>, prev: &BlockIdExt) -> Result<()> {
        self.db.store_block_prev1(handle, prev, None)
    }

    fn store_block_prev2(&self, handle: &Arc<BlockHandle>, prev2: &BlockIdExt) -> Result<()> {
        self.db.store_block_prev2(handle, prev2, None)
    }

    async fn store_zerostate(
        &self, 
        mut state: Arc<ShardStateStuff>, 
        state_bytes: &[u8]
    ) -> Result<(Arc<ShardStateStuff>, Arc<BlockHandle>)> {
        let handle = self.db.create_or_load_block_handle(
            state.block_id(), 
            None,
            None,
            Some(state.state()?.gen_time()),
            None
        )?.to_non_updated().ok_or_else(
            || error!("Bad result in create or load block handle")
        )?;
        state = self.store_state(&handle, state).await?;
        self.db.store_shard_state_persistent_raw(&handle, state_bytes, None).await?;
        Ok((state, handle))
    }

    async fn set_applied(
        &self, 
        handle: &Arc<BlockHandle>, 
        mc_seq_no: u32
    ) -> Result<bool> {
        if handle.is_applied() {
            return Ok(false);
        }
        log::trace!(target: "nodese", "set_applied: {}:{}", handle.id().shard(), handle.id().seq_no());
        self.db.assign_mc_ref_seq_no(handle, mc_seq_no, None)?;
        self.db.archive_block(handle.id(), None).await?;
        self.db.store_block_applied(handle, None)
    }

    fn load_block_prev1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db.load_block_prev1(id)
    }

    fn load_block_prev2(&self, id: &BlockIdExt) -> Result<Option<BlockIdExt>> {
        self.db.load_block_prev2(id)
    }

    fn store_block_next1(&self, handle: &Arc<BlockHandle>, next: &BlockIdExt) -> Result<()> {
        self.db.store_block_next1(handle, next, None)
    }

    async fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db.load_block_next1(id)
    }

    fn store_block_next2(&self, handle: &Arc<BlockHandle>, next2: &BlockIdExt) -> Result<()> {
        self.db.store_block_next2(handle, next2, None)
    }

    async fn load_block_next2(&self, id: &BlockIdExt) -> Result<Option<BlockIdExt>> {
        self.db.load_block_next2(id)
    }

    async fn process_block_in_ext_db(
        &self,
        _handle: &Arc<BlockHandle>,
        _block: &BlockStuff,
        _proof: Option<&BlockProofStuff>,
        _state: &Arc<ShardStateStuff>,
        _prev_states: (&Arc<ShardStateStuff>, Option<&Arc<ShardStateStuff>>),
        _mc_seq_no: u32,
    )
    -> Result<()> {
        Ok(())
    }

    async fn wait_state(
        self: Arc<Self>,
        id: &BlockIdExt,
        _timeout_ms: Option<u64>,
        _allow_block_downloading: bool
    ) -> Result<Arc<ShardStateStuff>> {
        self.load_state(id).await
    }

    async fn store_state(
        &self, 
        handle: &Arc<BlockHandle>, 
        state: Arc<ShardStateStuff>,
    ) -> Result<Arc<ShardStateStuff>> {
        let (state, _ ) = self.db.store_shard_state_dynamic(
            handle,
            &state,
            None,
            None,
            false
            
        ).await?;
        Ok(state)
    }

    async fn apply_block_internal(
        self: Arc<Self>, 
        handle: &Arc<BlockHandle>, 
        block: &BlockStuff, 
        mc_seq_no: u32, 
        pre_apply: bool,
        recursion_depth: u32,
    ) -> Result<()> {
        debug_assert!(!pre_apply);
        log::debug!(target: "nodese", "apply_block {}", handle.id());
        apply_block(
            &handle, 
            &block,  
            mc_seq_no, 
            &(self.clone() as Arc<dyn EngineOperations>), 
            pre_apply,
            recursion_depth
        ).await?;
        self.set_applied(handle, mc_seq_no).await?;
        Ok(())
    }

    async fn download_and_apply_block(
        self: Arc<Self>, 
        _id: &BlockIdExt, 
        _mc_seq_no: u32, 
        _pre_apply: bool
    ) -> Result<()> {
        Ok(())
    }

    async fn wait_applied_block(&self, id: &BlockIdExt, _timeout_ms: Option<u64>) -> Result<Arc<BlockHandle>> {
        self.load_block_handle(id)?.ok_or_else(
            || error!("Cannot load handle for block {}", id)
        )
    }

    async fn download_block(&self, id: &BlockIdExt, _limit: Option<u32>) -> Result<(BlockStuff, Option<BlockProofStuff>)> {
        let handle = self.load_block_handle(id)?.ok_or_else(
            || error!("Cannot load handle for block {}", id)
        )?;
        Ok((
            self.db.load_block_data(&handle).await?,
            Some(self.db.load_block_proof(&handle, !id.shard().is_masterchain()).await?)
        ))
    }
    fn new_external_message(&self, id: UInt256, message: Arc<Message>) -> Result<()> {
        self.ext_messages.new_message(id, message, self.now())
    }
    fn get_external_messages_iterator(
        &self, 
        shard: ShardIdent
    ) -> Box<dyn Iterator<Item = (Arc<Message>, UInt256)> + Send + Sync> {
        Box::new(self.ext_messages.clone().iter(shard, self.now()))
    }
    fn complete_external_messages(&self, to_delay: Vec<UInt256>, to_delete: Vec<UInt256>) -> Result<()> {
        self.ext_messages.complete_messages(to_delay, to_delete, self.now())
    }
    async fn get_shard_blocks(
        &self,
        last_mc_state: &Arc<ShardStateStuff>,
        actual_last_mc_seqno: Option<&mut u32>,
    ) -> Result<Vec<Arc<TopBlockDescrStuff>>> {
        self.shard_blocks.get_shard_blocks(last_mc_state, self, true, actual_last_mc_seqno).await
    }

    #[cfg(feature = "telemetry")]
    fn engine_telemetry(&self) -> &Arc<EngineTelemetry> {
        &self.engine_telemetry
    }

    fn engine_allocated(&self) -> &Arc<EngineAlloc> {
        &self.engine_allocated
    }

    fn collator_config(&self) -> &CollatorConfig {
        lazy_static::lazy_static!{
            static ref COLLATOR_CONFIG: CollatorConfig = CollatorConfig {
                cutoff_timeout_ms: 100_000,
                stop_timeout_ms: 100_000,
                max_collate_threads: 6,
                ..Default::default()
            };
        };
        &*COLLATOR_CONFIG
    }

}

pub struct WaitForHandle {
    count: AtomicU32,
    delay: u32, 
    max_count: u32,
    ping: tokio::sync::Barrier
}

impl WaitForHandle {
    pub fn with_max_count(max_count: u32) -> Arc<Self> {
        Self::with_max_count_and_delay(max_count, 0)
    }
    pub fn with_max_count_and_delay(max_count: u32, delay: u32) -> Arc<Self> {
        let ret = Self {
            count: AtomicU32::new(0),
            delay,
            max_count,
            ping: tokio::sync::Barrier::new(2)
        };
        Arc::new(ret)
    }
    pub async fn wait(&self) {
        self.ping.wait().await;
        if self.delay > 0 {
            tokio::time::sleep(Duration::from_millis(self.delay as u64)).await
        }
    }
}

#[async_trait::async_trait]
impl Callback for WaitForHandle {
    async fn invoke(&self, _job: StoreJob, ok: bool) {
        if ok {
            let count = self.count.fetch_add(1, Ordering::Relaxed);
            if count + 1 == self.max_count {
                self.ping.wait().await;
            }
        }
    }
}
