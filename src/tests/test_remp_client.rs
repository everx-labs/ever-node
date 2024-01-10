use crate::{
    block::BlockStuff, engine_traits::EngineOperations,
    full_node::remp_client::{RempClient}, shard_state::ShardStateStuff, 
    validator::validator_utils::get_adnl_id,
};
#[cfg(feature = "telemetry")]
use crate::full_node::telemetry::RempClientTelemetry;

use catchain::CatchainNode;
use std::{
    collections::{HashSet, HashMap}, str::FromStr,
    sync::{Arc, atomic::{AtomicU32, Ordering}},
    time::{Duration, SystemTime, UNIX_EPOCH}
};
use storage::{types::BlockMeta, block_handle_db::{BlockHandle, BlockHandleStorage}};
use ton_api::ton::ton_node::{
    RempMessage, RempMessageLevel, RempMessageStatus, RempReceipt
};
use ton_block::{
    ConfigParams, FutureSplitMerge, ShardHashes, ShardIdent, ShardStateUnsplit, ValidatorDescr,
    ValidatorSet, SigPubKey, ShardDescr, CatchainConfig, ConfigParamEnum, ConfigParam34, 
    ConfigParam36, BinTree, InRefValue, McStateExtra, Block, BlockExtra, InMsg, McBlockExtra,
    BlockIdExt, Message, Transaction, ExternalInboundMessageHeader, MsgAddressExt, MsgAddressInt,
    InMsgDescr, GetRepresentationHash, Serializable, ShardAccount,
};
use ton_types::{
    error, fail, AccountId, Ed25519KeyOption, KeyId, Result, SliceData, UInt256
};

const TOTAL_BLOCKS: usize = 40;
const START_BLOCK_SEQNO: u32 = 100;
const NEXT_BLOCK_TIMEOUT: u64 = 500;
const CATCHAIN_LIFETIME: u32 = 250;

fn prepare_ss(
    current_vset_utime_until: u32,
    add_next_vset: bool,
    before_split: bool,
    before_merge: bool,
    future_split_merge: Option<FutureSplitMerge>
) -> Result<Arc<ShardStateStuff>> {
    
    // current vset
    let mut list = vec!();
    for n in 0..200 {
        let keypair = Ed25519KeyOption::generate()?;
        let key = SigPubKey::from_bytes(keypair.pub_key()?)?;
        let vd = ValidatorDescr::with_params(key, n, None, None);
        list.push(vd);
    }
    let vset = ValidatorSet::new(0, current_vset_utime_until, 30, list).unwrap();

    // next vset
    let mut list = vec!();
    for n in 0..200 {
        let keypair = Ed25519KeyOption::generate()?;
        let key = SigPubKey::from_bytes(keypair.pub_key()?)?;
        let vd = ValidatorDescr::with_params(key, n, None, None);
        list.push(vd);
    }
    let next_vset = ValidatorSet::new(0, current_vset_utime_until, 30, list).unwrap();

    // shard descr
    let shard_prefix = 0xc000000000000000_u64;
    let sd_c = ShardDescr {
        seq_no: START_BLOCK_SEQNO,
        reg_mc_seqno: START_BLOCK_SEQNO,
        before_split,
        before_merge,
        next_catchain_seqno: 10,
        next_validator_shard: shard_prefix,
        split_merge_at: future_split_merge.unwrap_or_default(),
        ..Default::default()
    };
    let mut sd_4 = sd_c.clone();
    sd_4.next_validator_shard = 0x4000000000000000_u64;

    // cc_config
    let cc_config = CatchainConfig {
        isolate_mc_validators: true,
        shuffle_mc_validators: true,
        mc_catchain_lifetime: CATCHAIN_LIFETIME,
        shard_catchain_lifetime: CATCHAIN_LIFETIME,
        shard_validators_lifetime: 100,
        shard_validators_num: 7,
    };

    // shard state building
    let mut config = ConfigParams::new();
    config.set_config(ConfigParamEnum::ConfigParam28(cc_config))?;
    config.set_config(ConfigParamEnum::ConfigParam34(ConfigParam34 { cur_validators: vset }))?;
    if add_next_vset {
        config.set_config(ConfigParamEnum::ConfigParam36(ConfigParam36 { next_validators: next_vset }))?;
    }

    let shards = BinTree::with_item(&ShardDescr::default())?;
    let mut shard_hashes = ShardHashes::default();
    shard_hashes.set(&0, &InRefValue(shards))?;
    shard_hashes.split_shard(
        &ShardIdent::with_tagged_prefix(0, 0x8000000000000000_u64)?,
        |_| {Ok((sd_c, sd_4))}
    )?;

    let extra = McStateExtra {
        shards: shard_hashes,
        config,
        ..Default::default()
    };

    let mut ss = ShardStateUnsplit::with_ident(ShardIdent::masterchain());
    ss.write_custom(Some(&extra))?;

    ss.insert_account(
        &UInt256::from_str("4000000000000000000000000000000000000000000000000000000000000000")?,
        &ShardAccount::default()
    )?;

    ShardStateStuff::from_state(
        prepare_id(START_BLOCK_SEQNO, &ShardIdent::masterchain()),
        ss,
        #[cfg(feature = "telemetry")]
        &crate::collator_test_bundle::create_engine_telemetry(),
        &crate::collator_test_bundle::create_engine_allocated()
    )
}

fn prepare_validators(
    cc_config: &CatchainConfig,
    subset_params: &[(ShardIdent, u32, &ValidatorSet)]
) -> Result<HashSet<Arc<KeyId>>> {
    let mut validators = HashSet::new();
    for (shard, cc_seqno, validator_set) in subset_params {
        let (subset, _hash_short) = validator_set.calc_subset(
            cc_config, 
            shard.shard_prefix_with_tag(), 
            shard.workchain_id(), 
            *cc_seqno,
            0.into())?;
        for v in &subset {
            validators.insert(get_adnl_id(v));
        }
    }

    Ok(validators)
}

#[test]
fn test_calculate_validators_current_mc() -> Result<()> {
    // current mc validators

    let ss = prepare_ss(10_000, false, false, false, None)?;

    let (v1, _) = RempClient::calculate_validators(
        ss.clone(),
        ShardIdent::masterchain(),
        AccountId::from_string("3521430decf7eb0e3849a4e62646a7186964c86376042772cf1798a5c2e52511")?,
        100,
    )?;
    let mut vs1 = HashSet::new();
    v1.iter().for_each(|(k, _)| { vs1.insert(k.clone()); } );


    let last_mc_state_extra = ss.state()?.read_custom()?.ok_or_else(|| error!("Can't read_custom"))?;
    let cc_config = last_mc_state_extra.config.catchain_config()?;
    let cur_validator_set = last_mc_state_extra.config.validator_set()?;
    let v2 = prepare_validators(
        &cc_config,
        &vec![(ShardIdent::masterchain(), 0, &cur_validator_set)]
    )?;

    assert_eq!(vs1, v2);
    Ok(())
}

#[test]
fn test_calculate_validators_next_mc() -> Result<()> {
    // current and next mc validators
    let now = 9_990;
    let ss = prepare_ss(now + super::NEXT_SET_LAG / 2, true, false, false, None)?;

    let (v1, _) = RempClient::calculate_validators(
        ss.clone(),
        ShardIdent::masterchain(),
        AccountId::from_string("3521430decf7eb0e3849a4e62646a7186964c86376042772cf1798a5c2e52511")?,
        now,
    )?;
    let mut vs1 = HashSet::new();
    v1.iter().for_each(|(k, _)| { vs1.insert(k.clone()); } );

    let last_mc_state_extra = ss.state()?.read_custom()?.ok_or_else(|| error!("Can't read_custom"))?;
    let cc_config = last_mc_state_extra.config.catchain_config()?;
    let cur_validator_set = last_mc_state_extra.config.validator_set()?;
    let next_validator_set = last_mc_state_extra.config.next_validator_set()?;
    let v2 = prepare_validators(
        &cc_config,
        &vec![
            (ShardIdent::masterchain(), 0, &cur_validator_set),
            (ShardIdent::masterchain(), 1, &next_validator_set)
        ]
    )?;

    assert_eq!(vs1, v2);
    Ok(())
}

#[test]
fn test_calculate_validators_mc_no_next_vset() -> Result<()> {
    // current mc validators
    
    let ss = prepare_ss(10_000, false, false, false, None)?;

    let (v1, _) = RempClient::calculate_validators(
        ss.clone(),
        ShardIdent::masterchain(),
        AccountId::from_string("3521430decf7eb0e3849a4e62646a7186964c86376042772cf1798a5c2e52511")?,
        9_980,
    )?;
    let mut vs1 = HashSet::new();
    v1.iter().for_each(|(k, _)| { vs1.insert(k.clone()); } );

    let last_mc_state_extra = ss.state()?.read_custom()?.ok_or_else(|| error!("Can't read_custom"))?;
    let cc_config = last_mc_state_extra.config.catchain_config()?;
    let cur_validator_set = last_mc_state_extra.config.validator_set()?;
    let v2 = prepare_validators(
        &cc_config,
        &vec![(ShardIdent::masterchain(), 0, &cur_validator_set)]
    )?;

    assert_eq!(vs1, v2);
    Ok(())
}

#[test]
fn test_calculate_validators_current_cc() -> Result<()> {
    // current shard validators
    
    let ss = prepare_ss(10_000, false, false, false, None)?;
    let shard = ShardIdent::with_tagged_prefix(0, 0x4000000000000000_u64)?;

    let (v1, _) = RempClient::calculate_validators(
        ss.clone(),
        shard.clone(),
        AccountId::from_string("3521430decf7eb0e3849a4e62646a7186964c86376042772cf1798a5c2e52511")?,
        100,
    )?;
    let mut vs1 = HashSet::new();
    v1.iter().for_each(|(k, _)| { vs1.insert(k.clone()); } );

    let last_mc_state_extra = ss.state()?.read_custom()?.ok_or_else(|| error!("Can't read_custom"))?;
    let cc_config = last_mc_state_extra.config.catchain_config()?;
    let cur_validator_set = last_mc_state_extra.config.validator_set()?;
    let v2 = prepare_validators(
        &cc_config,
        &vec![(shard, 10, &cur_validator_set)]
    )?;

    assert_eq!(vs1, v2);
    Ok(())
}

#[test]
fn test_calculate_validators_next_cc() -> Result<()> {
    // current and next catchain validators

    let ss = prepare_ss(10_000, false, false, false, None)?;
    let shard = ShardIdent::with_tagged_prefix(0, 0x4000000000000000_u64)?;

    let (v1, _) = RempClient::calculate_validators(
        ss.clone(),
        shard.clone(),
        AccountId::from_string("3521430decf7eb0e3849a4e62646a7186964c86376042772cf1798a5c2e52511")?,
        CATCHAIN_LIFETIME - super::NEXT_SET_LAG / 2,
    )?;
    let mut vs1 = HashSet::new();
    v1.iter().for_each(|(k, _)| { vs1.insert(k.clone()); } );

    let last_mc_state_extra = ss.state()?.read_custom()?.ok_or_else(|| error!("Can't read_custom"))?;
    let cc_config = last_mc_state_extra.config.catchain_config()?;
    let cur_validator_set = last_mc_state_extra.config.validator_set()?;
    //let next_validator_set = last_mc_state_extra.config.next_validator_set()?;
    let v2 = prepare_validators(
        &cc_config,
        &vec![
            (shard.clone(), 10, &cur_validator_set),
            (shard, 11, &cur_validator_set)
        ]
    )?;

    assert_eq!(vs1, v2);
    Ok(())
}

#[test]
fn test_calculate_validators_next_val_set() -> Result<()> {
    // current and next val set validators
    
    let now = 9_910;
    let ss = prepare_ss(now + super::NEXT_SET_LAG / 2, true, false, false, None)?;
    let shard = ShardIdent::with_tagged_prefix(0, 0x4000000000000000_u64)?;

    let (v1, _) = RempClient::calculate_validators(
        ss.clone(),
        shard.clone(),
        AccountId::from_string("3521430decf7eb0e3849a4e62646a7186964c86376042772cf1798a5c2e52511")?,
        now,
    )?;
    let mut vs1 = HashSet::new();
    v1.iter().for_each(|(k, _)| { vs1.insert(k.clone()); } );

    let last_mc_state_extra = ss.state()?.read_custom()?.ok_or_else(|| error!("Can't read_custom"))?;
    let cc_config = last_mc_state_extra.config.catchain_config()?;
    let cur_validator_set = last_mc_state_extra.config.validator_set()?;
    let next_validator_set = last_mc_state_extra.config.next_validator_set()?;
    let v2 = prepare_validators(
        &cc_config,
        &vec![
            (shard.clone(), 10, &cur_validator_set),
            (shard, 11, &next_validator_set)
        ]
    )?;

    assert_eq!(vs1, v2);
    Ok(())
}

#[test]
fn test_calculate_validators_before_merge() -> Result<()> {
    // validators for merged shard
    
    let ss = prepare_ss(10_000, false, false, true, None)?;
    let shard = ShardIdent::with_tagged_prefix(0, 0x4000000000000000_u64)?;

    let (v1, _) = RempClient::calculate_validators(
        ss.clone(),
        shard.clone(),
        AccountId::from_string("3521430decf7eb0e3849a4e62646a7186964c86376042772cf1798a5c2e52511")?,
        10,
    )?;
    let mut vs1 = HashSet::new();
    v1.iter().for_each(|(k, _)| { vs1.insert(k.clone()); } );

    let last_mc_state_extra = ss.state()?.read_custom()?.ok_or_else(|| error!("Can't read_custom"))?;
    let cc_config = last_mc_state_extra.config.catchain_config()?;
    let cur_validator_set = last_mc_state_extra.config.validator_set()?;
    let merged_shard = ShardIdent::with_tagged_prefix(0, 0x8000000000000000_u64)?;
    let v2 = prepare_validators(
        &cc_config,
        &vec![(merged_shard, 11, &cur_validator_set)]
    )?;

    assert_eq!(vs1, v2);
    Ok(())
}

#[test]
fn test_calculate_validators_before_split() -> Result<()> {
    // validators for splitted shard

    let ss = prepare_ss(10_000, false, true, false, None)?;
    let shard = ShardIdent::with_tagged_prefix(0, 0x4000000000000000_u64)?;

    let (v1, _) = RempClient::calculate_validators(
        ss.clone(),
        shard.clone(),
        AccountId::from_string("3521430decf7eb0e3849a4e62646a7186964c86376042772cf1798a5c2e52511")?,
        10,
    )?;
    let mut vs1 = HashSet::new();
    v1.iter().for_each(|(k, _)| { vs1.insert(k.clone()); } );

    let last_mc_state_extra = ss.state()?.read_custom()?.ok_or_else(|| error!("Can't read_custom"))?;
    let cc_config = last_mc_state_extra.config.catchain_config()?;
    let cur_validator_set = last_mc_state_extra.config.validator_set()?;
    let splitted_shard = ShardIdent::with_tagged_prefix(0, 0x2000000000000000_u64)?;
    let v2 = prepare_validators(
        &cc_config,
        &vec![(splitted_shard, 11, &cur_validator_set)]
    )?;

    assert_eq!(vs1, v2);
    Ok(())
}

#[test]
fn test_calculate_validators_future_merge() -> Result<()> {
    // current validators for current and merged shard
    
    let fsm = FutureSplitMerge::Merge {
        merge_utime: 10, 
        interval: 100,
    };
    let ss = prepare_ss(10_000, false, false, false, Some(fsm))?;
    let shard = ShardIdent::with_tagged_prefix(0, 0x4000000000000000_u64)?;

    let (v1, _) = RempClient::calculate_validators(
        ss.clone(),
        shard.clone(),
        AccountId::from_string("3521430decf7eb0e3849a4e62646a7186964c86376042772cf1798a5c2e52511")?,
        20,
    )?;
    let mut vs1 = HashSet::new();
    v1.iter().for_each(|(k, _)| { vs1.insert(k.clone()); } );

    let last_mc_state_extra = ss.state()?.read_custom()?.ok_or_else(|| error!("Can't read_custom"))?;
    let cc_config = last_mc_state_extra.config.catchain_config()?;
    let cur_validator_set = last_mc_state_extra.config.validator_set()?;
    let merged_shard = ShardIdent::with_tagged_prefix(0, 0x8000000000000000_u64)?;
    let v2 = prepare_validators(
        &cc_config,
        &vec![
            (shard, 10, &cur_validator_set),
            (merged_shard, 11, &cur_validator_set)
        ]
    )?;

    assert_eq!(vs1, v2);
    Ok(())
}

#[test]
fn test_calculate_validators_future_split() -> Result<()> {
    // current validators for current and splitted shard
    
    let fsm = FutureSplitMerge::Split {
        split_utime: 10, 
        interval: 100,
    };
    let ss = prepare_ss(10_000, false, false, false, Some(fsm))?;
    let shard = ShardIdent::with_tagged_prefix(0, 0x4000000000000000_u64)?;

    let (v1, _) = RempClient::calculate_validators(
        ss.clone(),
        shard.clone(),
        AccountId::from_string("3521430decf7eb0e3849a4e62646a7186964c86376042772cf1798a5c2e52511")?,
        20,
    )?;
    let mut vs1 = HashSet::new();
    v1.iter().for_each(|(k, _)| { vs1.insert(k.clone()); } );

    let last_mc_state_extra = ss.state()?.read_custom()?.ok_or_else(|| error!("Can't read_custom"))?;
    let cc_config = last_mc_state_extra.config.catchain_config()?;
    let cur_validator_set = last_mc_state_extra.config.validator_set()?;
    let splitted_shard = ShardIdent::with_tagged_prefix(0, 0x2000000000000000_u64)?;
    let v2 = prepare_validators(
        &cc_config,
        &vec![
            (shard, 10, &cur_validator_set),
            (splitted_shard, 11, &cur_validator_set)
        ]
    )?;

    assert_eq!(vs1, v2);
    Ok(())
}

#[test]
fn test_calculate_validators_supermix() -> Result<()> {
    // next catchain + set + furute split
    let now = 10_236;
    let fsm = FutureSplitMerge::Split {
        split_utime: now + super::NEXT_SET_LAG / 3, 
        interval: 300,
    };
    let ss = prepare_ss(now + super::NEXT_SET_LAG / 4, true, false, false, Some(fsm))?;
    let shard = ShardIdent::with_tagged_prefix(0, 0x4000000000000000_u64)?;

    let (v1, _) = RempClient::calculate_validators(
        ss.clone(),
        shard.clone(),
        AccountId::from_string("3521430decf7eb0e3849a4e62646a7186964c86376042772cf1798a5c2e52511")?,
        now,
    )?;
    let mut vs1 = HashSet::new();
    v1.iter().for_each(|(k, _)| { vs1.insert(k.clone()); } );

    let last_mc_state_extra = ss.state()?.read_custom()?.ok_or_else(|| error!("Can't read_custom"))?;
    let cc_config = last_mc_state_extra.config.catchain_config()?;
    let cur_validator_set = last_mc_state_extra.config.validator_set()?;
    let next_validator_set = last_mc_state_extra.config.next_validator_set()?;
    let splitted_shard = ShardIdent::with_tagged_prefix(0, 0x2000000000000000_u64)?;
    let v2 = prepare_validators(
        &cc_config,
        &vec![
            (shard.clone(), 10, &cur_validator_set),
            (shard.clone(), 11, &cur_validator_set),
            (shard.clone(), 11, &next_validator_set),
            (splitted_shard.clone(), 11, &cur_validator_set),
            (splitted_shard.clone(), 12, &cur_validator_set),
            (splitted_shard.clone(), 12, &next_validator_set),
        ]
    )?;

    assert_eq!(vs1, v2);
    Ok(())
}

fn prepare_id(seq_no: u32, shard_id: &ShardIdent) -> BlockIdExt {
    let mut root_hash = [0; 32];
    root_hash[0..=3].copy_from_slice(&seq_no.to_le_bytes());
    root_hash[5..=8].copy_from_slice(&shard_id.workchain_id().to_le_bytes());
    root_hash[9..=16].copy_from_slice(&shard_id.shard_prefix_with_tag().to_le_bytes());

    BlockIdExt {
        shard_id: shard_id.clone(),
        seq_no,
        root_hash: UInt256::with_array(root_hash),
        file_hash: UInt256::default(),
    }
}

fn prepare_block(seq_no: u32, shard_id: ShardIdent, messages: Vec<Message>) -> Result<(BlockIdExt, BlockStuff)> {
    let mut in_messages = InMsgDescr::default();
    for m in messages.iter() {
        in_messages.insert(&InMsg::external(
            m.serialize()?, 
            Transaction::default().serialize()?
        ))?;
    }

    let mut extra = BlockExtra::default();
    extra.write_in_msg_descr(&in_messages)?;

    let id = prepare_id(seq_no, &shard_id);

    let mut block = Block::default();

    if shard_id.is_masterchain() {
        let shard_prefix = 0xc000000000000000_u64;
        let shard_block_id = prepare_id(seq_no, &ShardIdent::with_tagged_prefix(0, 0xc000_0000_0000_0000_u64)?);
        let sd_c = ShardDescr {
            seq_no,
            reg_mc_seqno: seq_no - 1,
            next_catchain_seqno: 10,
            next_validator_shard: shard_prefix,
            root_hash: shard_block_id.root_hash,
            file_hash: shard_block_id.file_hash,
            ..Default::default()
        };
        let shard_block_id = prepare_id(seq_no, &ShardIdent::with_tagged_prefix(0, 0x4000_0000_0000_0000_u64)?);
        let mut sd_4 = sd_c.clone();
        sd_4.root_hash = shard_block_id.root_hash;
        sd_4.file_hash = shard_block_id.file_hash;
        sd_4.next_validator_shard = 0x4000000000000000_u64;

        let shards = BinTree::with_item(&ShardDescr::default())?;
        let mut shard_hashes = ShardHashes::default();
        shard_hashes.set(&0, &InRefValue(shards))?;
        shard_hashes.split_shard(
            &ShardIdent::with_tagged_prefix(0, 0x8000000000000000_u64)?,
            |_| {Ok((sd_4, sd_c))}
        )?;

        let mut mc_extra = McBlockExtra::default();
        *mc_extra.shards_mut() = shard_hashes;

        extra.write_custom(Some(&mc_extra))?;
    }

    block.write_extra(&extra)?;

    let block = BlockStuff::fake_with_block(id.clone(), block);

    Ok((id, block))
}

fn prepare_message(magic: u8, to: &str) -> Message {
    let body = SliceData::new(vec![magic; 8]);
    let header = ExternalInboundMessageHeader::new(
        MsgAddressExt::default(),
        MsgAddressInt::from_str(to).unwrap()
    );
    Message::with_ext_in_header_and_body(header, body)
}

struct TestRempClientEngine {
    pub state: Arc<ShardStateStuff>,
    pub blocks: HashMap<BlockIdExt, BlockStuff>,
    pub block_handle_storage: BlockHandleStorage,
    pub sent_remp_messages: AtomicU32,
    pub signed_remp_messages: AtomicU32,
    #[cfg(feature = "telemetry")]
    pub telemetry: RempClientTelemetry,
}

impl TestRempClientEngine {
    fn find_block(&self, shard: &ShardIdent, seq_no: u32) -> Option<BlockIdExt> {
        for id in self.blocks.keys() {
            if id.shard() == shard && id.seq_no() == seq_no {
                return Some(id.clone())
            }
        }
        None
    }
}

#[async_trait::async_trait]
impl EngineOperations for TestRempClientEngine {
    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>> {
        let handle = self.block_handle_storage.create_handle(
            id.clone(), 
            BlockMeta::default(), 
            None
        )?;
        if let Some(handle) = handle {
            if self.blocks.contains_key(id) {
                handle.set_data();
                handle.set_state();
                handle.set_block_applied();
                if id.seq_no() < START_BLOCK_SEQNO + TOTAL_BLOCKS as u32 {
                    handle.set_next1();
                }
            }
            Ok(Some(handle))
        } else {
            self.block_handle_storage.load_handle(id)
        }
    }

    async fn wait_next_applied_mc_block(
        &self, 
        prev_handle: &BlockHandle,
        _timeout_ms: Option<u64>
    ) -> Result<(Arc<BlockHandle>, BlockStuff)> {

        tokio::time::sleep(Duration::from_millis(NEXT_BLOCK_TIMEOUT)).await;

        let id = self.load_block_next1(prev_handle.id()).await?;
        let handle = self.load_block_handle(&id)?.ok_or_else(|| error!("Can't load block handle {}", id))?;
        let block = self.blocks.get(&id).ok_or_else(|| error!("Can't load block {}", id))?;
        Ok((handle, block.clone()))
    }

    fn processed_workchain(&self) -> Option<i32> {
        None
    }

    async fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.find_block(id.shard(), id.seq_no() + 1)
            .ok_or_else(|| error!("The is no next block for {}", id))
    }

    async fn load_block_next2(&self, _id: &BlockIdExt) -> Result<Option<BlockIdExt>> {
        Ok(None)
        //fail!("The is no next 2 block")
    }

    async fn check_sync(&self) -> Result<bool> {
        Ok(true)
    }

    async fn load_last_applied_mc_state(&self) -> Result<Arc<ShardStateStuff>> {
        Ok(self.state.clone())
    }

    fn send_remp_message(&self, _to: Arc<KeyId>, _message: &RempMessage) -> Result<()> {
        self.sent_remp_messages.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn sign_remp_receipt(&self, _receipt: &RempReceipt) -> Result<Vec<u8>> {
        const SIGNATURE_LEN: usize = 64;
        self.signed_remp_messages.fetch_add(1, Ordering::Relaxed);
        let mut signature = vec![0; SIGNATURE_LEN];
        for i in 0..signature.len() {
            signature[i] = rand::random::<u8>();
        }                                                           
        Ok(signature)
    }

    fn load_last_applied_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
        Ok(Some(Arc::new(self.state.block_id().clone())))
    }

    async fn load_state(&self, _block_id: &BlockIdExt) -> Result<Arc<ShardStateStuff>> {
        //if self.state.block_id() == block_id {
            Ok(self.state.clone())
        //} else {
        //    fail!("Can't load state {}", block_id)
        //}
    }

    async fn wait_state(
        self: Arc<Self>,
        id: &BlockIdExt,
        _timeout_ms: Option<u64>,
        _allow_block_downloading: bool
    ) -> Result<Arc<ShardStateStuff>> {
        self.load_state(id).await
    }

    async fn update_validators(
        &self,
        _to_resolve: Vec<CatchainNode>,
        _to_delete: Vec<CatchainNode>
    ) -> Result<()> {
        Ok(())
    }

    async fn load_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        Ok(self
            .blocks
            .get(handle.id())
            .ok_or_else(|| error!("Can't load block {}", handle.id()))?
            .clone()
        )
    }

    fn check_stop(&self) -> bool {
        false
    }

    async fn process_remp_msg_status_in_ext_db(
        &self,
        _id: &UInt256,
        _status: &RempReceipt,
        _signature: &[u8],
    ) -> Result<()> {
        Ok(())
    }

    #[cfg(feature = "telemetry")]
    fn remp_client_telemetry(&self) -> &RempClientTelemetry {
        &self.telemetry
    }
}

#[tokio::test]
async fn test_remp_client() -> Result<()> {

    // init_test_log();

    let msg1 = prepare_message(1, "-1:4000000000000000000000000000000000000000000000000000000000000000");
    let msg1_id = msg1.hash()?;

    let msg2 = prepare_message(2, "0:4000000000000000000000000000000000000000000000000000000000000000");
    let msg2_id = msg2.hash()?;

    let msg3 = prepare_message(3, "0:4000000000000000000000000000000000000000000000000000000000000000");
    let msg3_id = msg3.hash()?;

    let mut mc_block_9 = None;

    let mut blocks = HashMap::new();
    for seq_no in START_BLOCK_SEQNO..=START_BLOCK_SEQNO + TOTAL_BLOCKS as u32 {

        let mut messages = vec!();
        if seq_no == 104 {
            messages.push(msg1.clone());
        }
        let (id, block) = prepare_block(seq_no as u32, ShardIdent::masterchain(), messages)?;
        if seq_no == 104 {
            mc_block_9 = Some(block.clone());
        }
        blocks.insert(id, block);

        let mut messages = vec!();
        if seq_no == 103 {
            messages.push(msg2.clone());
        }
        let (id, block) = prepare_block(seq_no as u32, ShardIdent::with_tagged_prefix(0, 0x4000000000000000_u64)?, messages)?;
        blocks.insert(id, block);

        let messages = vec!();
        let (id, block) = prepare_block(seq_no as u32, ShardIdent::with_tagged_prefix(0, 0xc000000000000000_u64)?, messages)?;
        blocks.insert(id, block);
    }

    let state = prepare_ss(
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as u32,
        false, false, false, None
    )?;

    let engine = Arc::new(TestRempClientEngine {
        state,
        blocks,
        block_handle_storage: crate::collator_test_bundle::create_block_handle_storage(),
        sent_remp_messages: AtomicU32::new(0),
        signed_remp_messages: AtomicU32::new(0),
        telemetry: RempClientTelemetry::default(),
    });

    let remp_client = Arc::new(RempClient::with_params(1000, NEXT_BLOCK_TIMEOUT * 10, true, UInt256::rand()));
    remp_client.clone().start(engine.clone(), None).await?;

    remp_client.clone().process_remp_message(msg2.write_to_bytes()?, msg2_id.clone());
    
    tokio::time::sleep(Duration::from_millis(NEXT_BLOCK_TIMEOUT * 1)).await;
    
    remp_client.clone().process_remp_message(msg1.write_to_bytes()?, msg1_id.clone());
    remp_client.clone().process_remp_message(msg3.write_to_bytes()?, msg3_id.clone());

    remp_client.clone().process_new_block(mc_block_9.unwrap());

    tokio::time::sleep(Duration::from_millis(NEXT_BLOCK_TIMEOUT * 5)).await;

    // 1
    let m1_history = remp_client
        .messages_history()
        .get(&msg1_id)
        .ok_or_else(|| error!("Can't load message {}", msg1_id))?;
    // let mut sent = false;
    let mut included = false;
    let mut finalized = false;
    assert!(m1_history.val().time_to_die.load(Ordering::Relaxed) != 0);
    for status in m1_history.val().statuses.iter() {
        match &status.val().status() {
            //RempMessageStatus::TonNode_RempSentToValidators(_) => sent = true,
            RempMessageStatus::TonNode_RempAccepted(acc) => { 
                if acc.master_id.seq_no() == 0 {
                    included = true;
                    assert!(acc.level == RempMessageLevel::TonNode_RempShardchain);
                    let block = acc.block_id.clone();
                    assert!(block.shard().is_masterchain());
                    assert_eq!(block.seq_no(), 104);
                } else {
                    finalized = true;
                    assert!(acc.level == RempMessageLevel::TonNode_RempMasterchain);
                    let block = acc.block_id.clone();
                    let mc_block = acc.master_id.clone();
                    assert!(block.shard().is_masterchain());
                    assert_eq!(block.seq_no(), 104);
                    assert_eq!(block, mc_block);
                }
            }
            s => fail!("Unexpected status found {:?}", s),
        }
    }
    //assert!(sent);
    assert!(included);
    assert!(finalized);

    // 2
    let m2_history = remp_client
        .messages_history()
        .get(&msg2_id)
        .ok_or_else(|| error!("Can't load message {}", msg2_id))?;
    // let mut sent = false;
    let mut finalized = false;
    assert!(m2_history.val().time_to_die.load(Ordering::Relaxed) != 0);
    for status in m2_history.val().statuses.iter() {
        match &status.val().status() {
            // RempMessageStatus::TonNode_RempSentToValidators(_) => sent = true,
            RempMessageStatus::TonNode_RempAccepted(acc) => { 
                finalized = true;
                assert!(acc.level == RempMessageLevel::TonNode_RempMasterchain);
                println!("acc.level {:?}", acc.level);
                let block: BlockIdExt = acc.block_id.clone();
                assert!(block.shard().shard_prefix_with_tag() == 0x4000000000000000_u64);
                assert_eq!(block.seq_no(), 103);
            }
            s => fail!("Unexpected status found {:?}", s),
        }
    }
    // assert!(sent);
    assert!(finalized);

    // 3
    let m3_history = remp_client
        .messages_history()
        .get(&msg3_id)
        .ok_or_else(|| error!("Can't load message {}", msg3_id))?;
    // let mut sent = false;
    assert!(m3_history.val().time_to_die.load(Ordering::Relaxed) == 0);
    for status in m3_history.val().statuses.iter() {
        match &status.val().status() {
            // RempMessageStatus::TonNode_RempSentToValidators(_) => sent = true,
            s => fail!("Unexpected status found {:?}", s),
        }
    }
    //assert!(sent);

    tokio::time::sleep(Duration::from_millis(NEXT_BLOCK_TIMEOUT * 10)).await;

    assert!(remp_client.messages_history().iter().count() == 1);

    Ok(())
}