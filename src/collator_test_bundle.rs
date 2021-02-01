use crate::{
    types::top_block_descr::TopBlockDescrStuff,
    shard_state::ShardStateStuff,
    db::BlockHandle,
    engine_traits::EngineOperations,
    block::BlockStuff,
    validator::{accept_block::create_top_shard_block_description, BlockCandidate},
    out_msg_queue::OutMsgQueueInfoStuff,
};
use ton_types::{UInt256, fail, error, Result, CellType, deserialize_cells_tree};
use ton_block::{
    BlockIdExt, Message, ShardIdent, AccountIdPrefixFull, Serializable, MerkleUpdate, TopBlockDescr,
    Deserializable, ValidatorBaseInfo, BlockSignaturesPure, BlockSignatures, HashmapAugType, 
    ShardStateUnsplit, TopBlockDescrSet,
};
use std::{
    convert::{TryFrom, TryInto},
    sync::{Arc, Weak}, 
    collections::HashMap, 
    fs::{File, read, write}, 
    ops::Deref,
    io::Cursor,
};


#[derive(serde::Deserialize, serde::Serialize)]
struct CollatorTestBundleIndexJson {
    id: String,
    top_shard_blocks: Vec<String>,
    external_messages: Vec<String>,
    last_mc_state: String,
    min_ref_mc_seqno: u32,
    mc_states: Vec<String>,
    neighbors: Vec<String>,
    prev_blocks: Vec<String>,
    created_by: String,
    rand_seed: String,
    now: u32,
    fake: bool,
    contains_ethalon: bool,
    #[serde(default)]
    contains_candidate: bool,
    #[serde(default)]
    notes: String,
}

impl TryFrom<CollatorTestBundleIndexJson> for CollatorTestBundleIndex {
    type Error = failure::Error;
    fn try_from(value: CollatorTestBundleIndexJson) -> Result<Self> {
        let mut shard_blocks = vec!();
        for s in value.top_shard_blocks {
            shard_blocks.push(s.parse()?);
        }
        let mut external_messages = vec!();
        for s in value.external_messages {
            external_messages.push(UInt256::from_str(&s)?);
        }
        let mut mc_states = vec!();
        for s in value.mc_states {
            mc_states.push(s.parse()?);
        }
        let mut neighbors = vec!();
        for s in value.neighbors {
            neighbors.push(s.parse()?);
        }
        let mut prev_blocks = vec!();
        for s in value.prev_blocks {
            prev_blocks.push(s.parse()?);
        }
        Ok(CollatorTestBundleIndex {
            id: value.id.parse()?,
            top_shard_blocks: shard_blocks,
            external_messages,
            last_mc_state: value.last_mc_state.parse()?,
            min_ref_mc_seqno: value.min_ref_mc_seqno,
            mc_states,
            neighbors,
            prev_blocks,
            created_by: UInt256::from_str(&value.created_by)?,
            rand_seed: Some(UInt256::from_str(&value.rand_seed)?),
            now: value.now,
            fake: value.fake,
            contains_ethalon: value.contains_ethalon,
            contains_candidate: value.contains_candidate,
            notes: value.notes,
        })
    }
}

impl From<&CollatorTestBundleIndex> for CollatorTestBundleIndexJson {
    fn from(value: &CollatorTestBundleIndex) -> Self {
        CollatorTestBundleIndexJson {
            id: value.id.to_string(),
            top_shard_blocks: value.top_shard_blocks.iter().map(|v| v.to_string()).collect(),
            external_messages: value.external_messages.iter().map(|v| v.to_hex_string()).collect(),
            last_mc_state: value.last_mc_state.to_string(),
            min_ref_mc_seqno: value.min_ref_mc_seqno,
            mc_states: value.mc_states.iter().map(|v| v.to_string()).collect(),
            neighbors: value.neighbors.iter().map(|v| v.to_string()).collect(),
            prev_blocks: value.prev_blocks.iter().map(|v| v.to_string()).collect(),
            created_by: value.created_by.to_hex_string(),
            rand_seed: match &value.rand_seed {
                Some(rand_seed) => rand_seed.to_hex_string(),
                None => UInt256::default().to_hex_string(),
            },
            now: value.now,
            fake: value.fake,
            contains_ethalon: value.contains_ethalon,
            contains_candidate: value.contains_candidate,
            notes: Default::default(),
        }
    }
}

struct CollatorTestBundleIndex {
    id: BlockIdExt,
    top_shard_blocks: Vec<BlockIdExt>,
    external_messages: Vec<UInt256>,
    last_mc_state: BlockIdExt,
    min_ref_mc_seqno: u32,
    mc_states: Vec<BlockIdExt>,
    neighbors: Vec<BlockIdExt>,
    prev_blocks: Vec<BlockIdExt>,
    created_by: UInt256,
    rand_seed: Option<UInt256>,
    now: u32,
    fake: bool,
    contains_ethalon: bool,
    contains_candidate: bool,
    notes: String,
}

impl CollatorTestBundleIndex {
    pub fn oldest_mc_state(&self) -> BlockIdExt {
        let mut oldest_mc_state = self.last_mc_state.clone();
        for id in self.mc_states.iter() {
            if id.seq_no < oldest_mc_state.seq_no {
                oldest_mc_state = id.clone();
            }
        }
        oldest_mc_state
    }
}

fn construct_from_file<T: Deserializable>(path: &str) -> Result<(T, UInt256, UInt256)> {
    let bytes = std::fs::read(path)?;
    let fh = UInt256::calc_file_hash(&bytes);
    let cell = ton_types::deserialize_tree_of_cells(&mut std::io::Cursor::new(bytes))?;
    let rh = cell.repr_hash();
    Ok((T::construct_from_cell(cell)?, fh, rh))
}

pub struct CollatorTestBundle {
    index: CollatorTestBundleIndex,
    top_shard_blocks: Vec<Arc<TopBlockDescrStuff>>,
    external_messages: Vec<(Arc<Message>, UInt256)>,
    states: HashMap<BlockIdExt, ShardStateStuff>,
    mc_merkle_updates: HashMap<BlockIdExt, MerkleUpdate>,
    blocks: HashMap<BlockIdExt, BlockStuff>,
    candidate: Option<BlockCandidate>,

    aux_mc_shard_states: lockfree::map::Map<u32, ShardStateStuff>,
    shard_states_cache: lockfree::map::Map<ShardIdent, ShardStateStuff>,
    block_handle_cache: Arc<lockfree::map::Map<BlockIdExt, Weak<BlockHandle>>>,
}

impl CollatorTestBundle {

    pub async fn build_with_zero_state(mc_zero_state_name: &str, wc_zero_state_name: &str) -> Result<Self> {
        log::info!("Building with zerostate from {} and {}", mc_zero_state_name, wc_zero_state_name);

        let (mc_state, mc_fh, mc_rh) = construct_from_file::<ShardStateUnsplit>(mc_zero_state_name)?;
        let (wc_state, wc_fh, wc_rh) = construct_from_file::<ShardStateUnsplit>(wc_zero_state_name)?;

        let now = std::cmp::max(mc_state.gen_time(), wc_state.gen_time()) + 1;

        let last_mc_state = BlockIdExt::with_params(mc_state.shard().clone(), 0, mc_rh, mc_fh);
        let mc_state = ShardStateStuff::with_state(last_mc_state.clone(), mc_state)?;
        let mut states = HashMap::new();
        states.insert(last_mc_state.clone(), mc_state);
        
        let block_id = BlockIdExt::with_params(wc_state.shard().clone(), 0, wc_rh, wc_fh);
        let wc_state = ShardStateStuff::with_state(block_id.clone(), wc_state)?;
        states.insert(block_id.clone(), wc_state);

        let prev_blocks = vec![last_mc_state.clone()];
        let mut id = last_mc_state.clone();
        id.seq_no += 1;

        let index = CollatorTestBundleIndex {
            id,
            top_shard_blocks: vec![],
            external_messages: vec![],
            mc_states: vec![last_mc_state.clone()],
            last_mc_state,
            min_ref_mc_seqno: 0,
            neighbors: vec![],
            prev_blocks,
            created_by: UInt256::default(),
            rand_seed: None,
            now,
            fake: true,
            contains_ethalon: false,
            contains_candidate: false,
            notes: Default::default(),
        };


        Ok(Self {
            index,
            top_shard_blocks: Default::default(),
            external_messages: Default::default(),
            states,
            mc_merkle_updates: Default::default(),
            blocks: Default::default(),
            aux_mc_shard_states: lockfree::map::Map::new(),
            shard_states_cache: lockfree::map::Map::new(),
            block_handle_cache: Arc::new(lockfree::map::Map::new()),
            candidate: None,
        })
    }

    // build bundle for a collating (just now) block. 
    // Uses real engine for top shard blocks and external messages.
    pub async fn build_for_collating_block(
        prev_blocks_ids: Vec<BlockIdExt>,
        engine: &dyn EngineOperations,
    ) -> Result<Self> {

        log::info!("Building for furure block, prev[0]: {}", prev_blocks_ids[0]);

        let mut states = HashMap::new();
        let shard = if prev_blocks_ids.len() > 1 { prev_blocks_ids[0].shard().merge()? } else { prev_blocks_ids[0].shard().clone() };
        let is_master = shard.is_masterchain();

        //
        // last mc state
        //
        let mc_state = engine.load_last_applied_mc_state().await?;
        let last_mc_id = mc_state.block_id();
        let mut oldest_mc_seq_no = last_mc_id.seq_no();
        let mut newest_mc_seq_no = last_mc_id.seq_no();

        //
        // top shard blocks
        //
        let top_shard_blocks = if is_master {
            engine.get_shard_blocks(last_mc_id.seq_no())?
        } else {
            vec![]
        };

        //
        // external messages
        //
        let external_messages = engine.get_external_messages(&shard)?;

        // 
        // neighbors
        //
        let mut neighbors = vec!();
        let shards = mc_state.shard_state_extra()?.shards();
        let neighbor_list = shards.get_neighbours(&shard)?;
        for shard in neighbor_list.iter() {
            states.insert(shard.block_id().clone(), engine.load_state(shard.block_id()).await?);
            neighbors.push(shard.block_id().clone());
        }

        if shards.is_empty() || mc_state.block_id().seq_no() != 0 {
            states.insert(mc_state.block_id().clone(), mc_state.clone());
        }
        // master blocks's collator uses new neighbours, based on new shaedes config.
        // It is difficult to calculate new config there. So add states for all new shard blocks.
        for tsb in top_shard_blocks.iter() {
            let id = tsb.proof_for();
            if !states.contains_key(id) {
                states.insert(id.clone(), engine.load_state(id).await?);
                neighbors.push(id.clone());
            }
        }

        //
        // prev_blocks & states
        //
        let mut blocks = HashMap::new();
        let prev1 = engine.load_block(engine.load_block_handle(&prev_blocks_ids[0])?.deref()).await?;
        states.insert(prev_blocks_ids[0].clone(), engine.load_state(&prev_blocks_ids[0]).await?);
        blocks.insert(prev_blocks_ids[0].clone(), prev1);
        if prev_blocks_ids.len() > 1 {
            let prev2 = engine.load_block(engine.load_block_handle(&prev_blocks_ids[1])?.deref()).await?;
            states.insert(prev_blocks_ids[1].clone(), engine.load_state(&prev_blocks_ids[1]).await?);
            blocks.insert(prev_blocks_ids[1].clone(), prev2);
        }

        // collect needed mc states
        for (_, state) in states.iter() {
            let nb = OutMsgQueueInfoStuff::from_shard_state(state)?;
            for entry in nb.entries() {
                if entry.mc_seqno < oldest_mc_seq_no {
                    oldest_mc_seq_no = entry.mc_seqno;
                } else if entry.mc_seqno > newest_mc_seq_no {
                    newest_mc_seq_no = entry.mc_seqno;
                }
            }
        }

        // mc states and merkle updates
        let mc_pfx = AccountIdPrefixFull::any_masterchain();
        let oldest_mc_state = engine.load_state(
            engine.find_block_by_seq_no(&mc_pfx, oldest_mc_seq_no).await?.id()
        ).await?;
        let mut mc_states = vec!(oldest_mc_state.block_id().clone());
        states.insert(oldest_mc_state.block_id().clone(), oldest_mc_state);
        let mut mc_merkle_updates = HashMap::new();

        for mc_seq_no in oldest_mc_seq_no + 1..=newest_mc_seq_no {
            let handle = engine.find_block_by_seq_no(&mc_pfx, mc_seq_no).await?;
            let block = engine.load_block(&handle).await?;
            mc_merkle_updates.insert(block.id().clone(), block.block().read_state_update()?);
            states.insert(block.id().clone(), engine.load_state(block.id()).await?);
            mc_states.push(block.id().clone());
        }

        let id = BlockIdExt {
            shard_id: shard,
            seq_no: prev_blocks_ids.iter().max_by_key(|id| id.seq_no()).unwrap().seq_no() + 1,
            root_hash: UInt256::default(),
            file_hash: UInt256::default(),
        };

        let index = CollatorTestBundleIndex {
            id,
            top_shard_blocks: top_shard_blocks.iter().map(|tsb| tsb.proof_for().clone()).collect(),
            external_messages: external_messages.iter().map(|(_, id)| id.clone()).collect(),
            last_mc_state: last_mc_id.clone(),
            min_ref_mc_seqno: oldest_mc_seq_no,
            mc_states,
            neighbors,
            prev_blocks: prev_blocks_ids,
            created_by: UInt256::default(),
            rand_seed: None,
            now: engine.now(),
            fake: true,
            contains_ethalon: false,
            contains_candidate: false,
            notes: Default::default(),
        };

        Ok(Self {
            index,
            top_shard_blocks,
            external_messages,
            states,
            mc_merkle_updates,
            blocks,
            aux_mc_shard_states: lockfree::map::Map::new(),
            shard_states_cache: lockfree::map::Map::new(),
            block_handle_cache: Arc::new(lockfree::map::Map::new()),
            candidate: None,
        })
    }

    // build bundle for a validating (just now) block. 
    // Uses real engine for top shard blocks and external messages.
    pub async fn build_for_validating_block(
        shard: ShardIdent,
        _min_masterchain_block_id: BlockIdExt,
        prev_blocks_ids: Vec<BlockIdExt>,
        candidate: BlockCandidate,
        engine: Arc<dyn EngineOperations>,
    ) -> Result<Self> {

        log::info!("Building for validating block, candidate: {}", candidate.block_id);

        let mut states = HashMap::new();
        let is_master = shard.is_masterchain();

        //
        // last mc state
        //
        let mc_state = engine.load_last_applied_mc_state().await?;
        let last_mc_id = mc_state.block_id();
        let mut oldest_mc_seq_no = last_mc_id.seq_no();
        let mut newest_mc_seq_no = last_mc_id.seq_no();

        //
        // top shard blocks
        //
        let top_shard_blocks = if is_master {
            engine.get_shard_blocks(last_mc_id.seq_no())?
        } else {
            vec![]
        };

        //
        // external messages
        //
        let external_messages = engine.get_external_messages(&shard)?;

        // 
        // neighbors
        //
        let mut neighbors = vec!();
        let shards = if shard.is_masterchain() {
            let block = BlockStuff::new(candidate.block_id.clone(), candidate.data.clone())?;
            block
                .block()
                .read_extra()?
                .read_custom()?
                .ok_or_else(|| error!("Given block is not a master block."))?
                .shards()
                .clone()
        } else {
            mc_state.shard_state_extra()?.shards().clone()
        };
        let neighbor_list = shards.get_neighbours(&shard)?;
        for shard in neighbor_list.iter() {
            states.insert(shard.block_id().clone(), engine.load_state(shard.block_id()).await?);
            neighbors.push(shard.block_id().clone());
        }

        if shards.is_empty() || mc_state.block_id().seq_no() != 0 {
            states.insert(mc_state.block_id().clone(), mc_state.clone());
        }

        //
        // prev_blocks & states
        //
        let mut blocks = HashMap::new();
        let prev1 = engine.load_block(engine.load_block_handle(&prev_blocks_ids[0])?.deref()).await?;
        states.insert(prev_blocks_ids[0].clone(), engine.load_state(&prev_blocks_ids[0]).await?);
        blocks.insert(prev_blocks_ids[0].clone(), prev1);
        if prev_blocks_ids.len() > 1 {
            let prev2 = engine.load_block(engine.load_block_handle(&prev_blocks_ids[1])?.deref()).await?;
            states.insert(prev_blocks_ids[1].clone(), engine.load_state(&prev_blocks_ids[1]).await?);
            blocks.insert(prev_blocks_ids[1].clone(), prev2);
        }

        // collect needed mc states
        for (_, state) in states.iter() {
            let nb = OutMsgQueueInfoStuff::from_shard_state(state)?;
            for entry in nb.entries() {
                if entry.mc_seqno < oldest_mc_seq_no {
                    oldest_mc_seq_no = entry.mc_seqno;
                } else if entry.mc_seqno > newest_mc_seq_no {
                    newest_mc_seq_no = entry.mc_seqno;
                }
            }
        }

        // mc states and merkle updates
        let mc_pfx = AccountIdPrefixFull::any_masterchain();
        let oldest_mc_state = engine.load_state(
            engine.find_block_by_seq_no(&mc_pfx, oldest_mc_seq_no).await?.id()
        ).await?;
        let mut mc_states = vec!(oldest_mc_state.block_id().clone());
        states.insert(oldest_mc_state.block_id().clone(), oldest_mc_state);
        let mut mc_merkle_updates = HashMap::new();

        for mc_seq_no in oldest_mc_seq_no + 1..=newest_mc_seq_no {
            let handle = engine.find_block_by_seq_no(&mc_pfx, mc_seq_no).await?;
            let block = engine.load_block(&handle).await?;
            mc_merkle_updates.insert(block.id().clone(), block.block().read_state_update()?);
            states.insert(block.id().clone(), engine.load_state(block.id()).await?);
            mc_states.push(block.id().clone());
        }

        let b = BlockStuff::new(candidate.block_id.clone(), candidate.data.clone())?;

        let index = CollatorTestBundleIndex {
            id: candidate.block_id.clone(),
            top_shard_blocks: top_shard_blocks.iter().map(|tsb| tsb.proof_for().clone()).collect(),
            external_messages: external_messages.iter().map(|(_, id)| id.clone()).collect(),
            last_mc_state: last_mc_id.clone(),
            min_ref_mc_seqno: oldest_mc_seq_no,
            mc_states,
            neighbors,
            prev_blocks: prev_blocks_ids,
            created_by: candidate.created_by.clone(),
            rand_seed: None,
            now: b.block().read_info()?.gen_utime().0,
            fake: true,
            contains_ethalon: false,
            contains_candidate: true,
            notes: Default::default(),
        };

        Ok(Self {
            index,
            top_shard_blocks,
            external_messages,
            states,
            mc_merkle_updates,
            blocks,
            aux_mc_shard_states: lockfree::map::Map::new(),
            shard_states_cache: lockfree::map::Map::new(),
            block_handle_cache: Arc::new(lockfree::map::Map::new()),
            candidate: Some(candidate),
        })
    }

    // Build partially fake bundle using data from node's database. Top shard blocks are built 
    // without signatures. Ethalon block is included, external messages are taken 
    // from ethalon block
    pub async fn build_with_ethalon(
        block_id: &BlockIdExt,
        engine: &dyn EngineOperations,
    ) -> Result<Self> {

        log::info!("Building with ethalon {}", block_id);

        let handle = engine.load_block_handle(block_id)?;
        let block = engine.load_block(&handle).await?;
        let info = block.block().read_info()?;
        let extra = block.block().read_extra()?;
        let mut states = HashMap::new();

        //
        // last mc state
        //
        let last_mc_id = if let Some(master_ref) = info.read_master_ref()? {
            BlockIdExt::from_ext_blk(master_ref.master)
        } else {
            block.construct_prev_id()?.0
        };
        let mut oldest_mc_seq_no = last_mc_id.seq_no();
        let mut newest_mc_seq_no = last_mc_id.seq_no();

        //
        // top shard blocks (fake)
        //
        let mut shard_blocks_ids = vec![];
        let mc_block_extra = extra.read_custom()?;
        if let Some(mc_block_extra) = mc_block_extra.as_ref() {
            mc_block_extra.shards().iterate_shards(|shard_id, descr| {
                shard_blocks_ids.push(BlockIdExt {
                    shard_id,
                    seq_no: descr.seq_no,
                    root_hash: descr.root_hash,
                    file_hash: descr.file_hash,
                });
                Ok(true)
            })?;
        }
        let mut top_shard_blocks = vec![];
        let mut top_shard_blocks_ids = vec![];
        let mc_state = engine.load_state(&last_mc_id).await?;
        for shard_block_id in shard_blocks_ids.iter().filter(|id| id.seq_no() != 0) {
            let block = engine.load_block(engine.load_block_handle(shard_block_id)?.deref()).await?;
            let info = block.block().read_info()?;
            let prev_blocks_ids = info.read_prev_ids()?;
            let base_info = ValidatorBaseInfo::with_params(
                info.gen_validator_list_hash_short(),
                info.gen_catchain_seqno()
            );
            let signatures = BlockSignaturesPure::default();

            // sometimes some shards don't have new blocks to create TSBD
            if let Some(tbd) = create_top_shard_block_description(
                    &block,
                    BlockSignatures::with_params(base_info, signatures),
                    &mc_state, // TODO
                    &prev_blocks_ids,
                    engine,
                ).await? {
                let tbd = TopBlockDescrStuff::new(tbd, block_id, true).unwrap();
                top_shard_blocks_ids.push(tbd.proof_for().clone());
                top_shard_blocks.push(Arc::new(tbd));
            }
        }

        //
        // external messages
        //
        let mut external_messages = vec![];
        let mut external_messages_ids = vec![];
        let in_msgs = extra.read_in_msg_descr()?;
        in_msgs.iterate_with_keys(|key, in_msg| {
            let msg = in_msg.read_message()?;
            if msg.is_inbound_external() {
                external_messages_ids.push(key.clone());
                external_messages.push((Arc::new(msg), key));
            }
            Ok(true)
        })?;

        // 
        // neighbors
        //
        let mut neighbors = vec!();
        let shards = if let Some(mc_block_extra) = mc_block_extra.as_ref() {
            mc_block_extra.shards()
        } else {
            mc_state.shard_state_extra()?.shards()
        };

        let neighbor_list = shards.get_neighbours(block_id.shard())?;
        for shard in neighbor_list.iter() {
            states.insert(shard.block_id().clone(), engine.load_state(shard.block_id()).await?);
            neighbors.push(shard.block_id().clone());
        }

        if shards.is_empty() || mc_state.block_id().seq_no() != 0 {
            states.insert(mc_state.block_id().clone(), mc_state);
        }

        //
        // prev_blocks & states
        //
        let mut blocks = HashMap::new();
        let mut prev_blocks_ids = vec!();
        let prev = block.construct_prev_id()?;
        let prev1 = engine.load_block_handle(&prev.0)?;
        prev_blocks_ids.push(prev1.id().clone());
        states.insert(prev1.id().clone(), engine.load_state(prev1.id()).await?);
        if let Ok(block) = engine.load_block(&prev1).await {
            blocks.insert(prev1.id().clone(), block);
        }
        if let Some(prev2) = prev.1 {
            let prev2 = engine.load_block(engine.load_block_handle(&prev2)?.deref()).await?;
            prev_blocks_ids.push(prev2.id().clone());
            states.insert(prev2.id().clone(), engine.load_state(prev2.id()).await?);
            blocks.insert(prev2.id().clone(), prev2);
        }

        // collect needed mc states
        for (_, state) in states.iter() {
            let nb = OutMsgQueueInfoStuff::from_shard_state(state)?;
            for entry in nb.entries() {
                if entry.mc_seqno < oldest_mc_seq_no {
                    oldest_mc_seq_no = entry.mc_seqno;
                } else if entry.mc_seqno > newest_mc_seq_no {
                    newest_mc_seq_no = entry.mc_seqno;
                }
            }
        }

        // ethalon block and state
        blocks.insert(block_id.clone(), block.clone());
        if block_id.shard().is_masterchain() {
            if block_id.seq_no() < oldest_mc_seq_no {
                oldest_mc_seq_no = block_id.seq_no();
            } else if block_id.seq_no() > newest_mc_seq_no {
                newest_mc_seq_no = block_id.seq_no();
            }
        } else {
            states.insert(block_id.clone(), engine.load_state(block_id).await?);
        }
        // mc states and merkle updates
        let mc_pfx = AccountIdPrefixFull::any_masterchain();
        let oldest_mc_state = engine.load_state(
            engine.find_block_by_seq_no(&mc_pfx, oldest_mc_seq_no).await?.id()
        ).await?;
        let mut mc_states = vec!(oldest_mc_state.block_id().clone());
        states.insert(oldest_mc_state.block_id().clone(), oldest_mc_state);
        let mut mc_merkle_updates = HashMap::new();

        for mc_seq_no in oldest_mc_seq_no + 1..=newest_mc_seq_no {
            let handle = engine.find_block_by_seq_no(&mc_pfx, mc_seq_no).await?;
            let block = engine.load_block(&handle).await?;
            mc_merkle_updates.insert(block.id().clone(), block.block().read_state_update()?);
            states.insert(block.id().clone(), engine.load_state(block.id()).await?);
            mc_states.push(block.id().clone());
        }

        let index = CollatorTestBundleIndex {
            id: block_id.clone(),
            top_shard_blocks: top_shard_blocks_ids,
            external_messages: external_messages_ids,
            last_mc_state: last_mc_id,
            min_ref_mc_seqno: info.min_ref_mc_seqno(),
            mc_states,
            neighbors,
            prev_blocks: prev_blocks_ids,
            created_by: extra.created_by().clone(),
            rand_seed: Some(extra.rand_seed().clone()),
            now: info.gen_utime().0,
            fake: true,
            contains_ethalon: true,
            contains_candidate: false,
            notes: Default::default(),
        };

        Ok(Self {
            index,
            top_shard_blocks,
            external_messages,
            states,
            mc_merkle_updates,
            blocks,
            aux_mc_shard_states: lockfree::map::Map::new(),
            shard_states_cache: lockfree::map::Map::new(),
            block_handle_cache: Arc::new(lockfree::map::Map::new()),
            candidate: None,
        })
    }

    pub fn save(&self, path: &str) -> Result<()> {
        // 📂 root directory
        let path = Self::build_filename(path, &self.index.id);
        log::info!("Saving {}", path);
        std::fs::create_dir_all(&path)?;

        // ├─📂 top_shard_blocks
        for tbd in self.top_shard_blocks.iter() {
            let path = format!("{}/top_shard_blocks/", path);
            std::fs::create_dir_all(&path)?;
            let filename = format!("{}/{:x}", path, tbd.proof_for().root_hash());
            tbd.top_block_descr().write_to_file(filename)?;
        }

        // ├─📂 external_messages
        for (m, id) in self.external_messages.iter() {
            let path = format!("{}/external_messages/", path);
            std::fs::create_dir_all(&path)?;
            let filename = format!("{}/{:x}", path, id);
            m.write_to_file(filename)?;
        }

        // ├─📂 states
        // all shardes states
        let path1 = format!("{}/states/", path);
        std::fs::create_dir_all(&path1)?;
        for ss_id in self.index.neighbors.iter().chain(self.index.prev_blocks.iter()) {
            let filename = format!("{}/{:x}", path1, ss_id.root_hash());
            self.states.get(ss_id)
                .ok_or_else(|| error!("Bundle's internal error (state {})", ss_id))?
                .write_to(&mut File::create(filename)?)?;
        }
        // ethalon state
        if self.index.contains_ethalon && !self.index.id.shard().is_masterchain() {
            let filename = format!("{}/{:x}", path1, self.index.id.root_hash());
            self.states.get(&self.index.id)
                .ok_or_else(|| error!("Bundle's internal error (state {})", self.index.id))?
                .write_to(&mut File::create(filename)?)?;
        }
        // oldest mc state is saved full 
        let oldest_mc_state = self.index.oldest_mc_state();
        let filename = format!("{}/{:x}", path1, oldest_mc_state.root_hash());
        self.states.get(&oldest_mc_state)
            .ok_or_else(|| error!("Bundle's internal error (state {})", oldest_mc_state))?
            .write_to(&mut File::create(filename)?)?;

        // merkle updates for all other mc states
        let path1 = format!("{}/states/mc_merkle_updates/", path);
        std::fs::create_dir_all(&path1)?;
        for (id, mu) in self.mc_merkle_updates.iter() {
            let filename = format!("{}/{:x}", path1, id.root_hash());
            mu.write_to_file(filename)?;
        }

        // ├─📂 blocks
        for (id, b) in self.blocks.iter() {
            let path = format!("{}/blocks/", path);
            std::fs::create_dir_all(&path)?;
            let filename = format!("{}/{:x}", path, id.root_hash());
            b.write_to(&mut File::create(filename)?)?;
        }

        // candidate
        if let Some(candidate) = &self.candidate {
            if candidate.block_id != self.index.id {
                fail!("Candidate's id mismatch")
            }
            if candidate.created_by != self.index.created_by {
                fail!("Candidate's created_by mismatch")
            }
            let path = format!("{}/candidate/", path);
            std::fs::create_dir_all(&path)?;
            write(format!("{}/data", path), &candidate.data)?;
            write(format!("{}/collated_data", path), &candidate.collated_data)?;
        }

        // 🗂 index
        let file = std::fs::File::create(format!("{}/index.json", path))?;
        serde_json::to_writer_pretty(file, &CollatorTestBundleIndexJson::from(&self.index))?;

        Ok(())
    }

    pub fn load(path: &str) -> Result<Self> {
        if !std::path::Path::new(path).is_dir() {
            fail!("Directory not found: {}", path);
        }
        // 🗂 index
        let file = std::fs::File::open(format!("{}/index.json", path))?;
        let index: CollatorTestBundleIndexJson = serde_json::from_reader(file)?;
        let mut index: CollatorTestBundleIndex = index.try_into()?;

        // ├─📂 top_shard_blocks
        let mut top_shard_blocks = vec!();
        for id in index.top_shard_blocks.iter() {
            let filename = format!("{}/top_shard_blocks/{:x}", path, id.root_hash());
            let tbd = TopBlockDescr::construct_from_file(filename)?;
            top_shard_blocks.push(Arc::new(TopBlockDescrStuff::new(tbd, id, index.fake)?));
        }

        // to add simple external message:
        // uncomment this block, and change dst address then run test
        // add id (new filename of message) to external messages in index.json
        // std::fs::create_dir_all(format!("{}/external_messages", path)).ok();
        // let src = ton_block::MsgAddressExt::with_extern([0x77; 32].into())?;
        // let dst = hex::decode("b1219502b825ef2345f49fc9065e485e7f478bddafa63039d00c63e494ab7090")?;
        // let dst = ton_block::MsgAddressInt::with_standart(None, 0, dst.into())?;
        // let h = ton_block::ExternalInboundMessageHeader::new(src, dst);
        // let msg = Message::with_ext_in_header(h);
        // let id = msg.serialize()?.repr_hash();
        // let filename = format!("{}/external_messages/{:x}", path, id);
        // msg.write_to_file(filename)?;

        // ├─📂 external_messages
        let mut external_messages = vec!();
        for id in index.external_messages.iter() {
            let filename = format!("{}/external_messages/{:x}", path, id);
            external_messages.push((
                Arc::new(Message::construct_from_file(filename)?),
                id.clone()
            ));
        }

        // ├─📂 states
        let mut states = HashMap::new();

        // all shardes states
        for ss_id in index.neighbors.iter().chain(index.prev_blocks.iter()) {
            let filename = format!("{}/states/{:x}", path, ss_id.root_hash());
            let ss = if ss_id.seq_no() == 0 {
                ShardStateStuff::deserialize_zerostate(ss_id.clone(), &read(filename)?)?
            } else {
                ShardStateStuff::deserialize(ss_id.clone(), &read(filename)?)?
            };
            states.insert(ss_id.clone(), ss);

        }
        if index.contains_ethalon && !index.id.shard().is_masterchain() {
            let filename = format!("{}/states/{:x}", path, index.id.root_hash());
            states.insert(
                index.id.clone(),
                ShardStateStuff::deserialize(index.id.clone(), &read(filename)?)?
            );
        }

        // oldest mc state is saved full 
        let oldest_mc_state_id = index.oldest_mc_state();
        let filename = format!("{}/states/{:x}", path, oldest_mc_state_id.root_hash());
        let oldest_mc_state = if oldest_mc_state_id.seq_no() == 0 {
            ShardStateStuff::deserialize_zerostate(oldest_mc_state_id.clone(), &read(filename)?)?
        } else {
            ShardStateStuff::deserialize(oldest_mc_state_id.clone(), &read(filename)?)?
        };
        let mut prev_state_root = oldest_mc_state.root_cell().clone();
        states.insert(oldest_mc_state_id.clone(), oldest_mc_state);

        // other states are culculated by merkle updates
        let mut mc_merkle_updates = HashMap::new();
        for id in index.mc_states.iter() {
            if id != &oldest_mc_state_id {
                let filename = format!("{}/states/mc_merkle_updates/{:x}", path, id.root_hash());
                mc_merkle_updates.insert(
                    id.clone(),
                    MerkleUpdate::construct_from_file(filename)?,
                );
            }
        }
        index.mc_states.sort_by_key(|id| id.seq_no);
        for id in index.mc_states.iter() {
            if id != &oldest_mc_state_id {
                let mu = mc_merkle_updates.get(id).ok_or_else(|| error!("Can't get merkle update {}", id))?;
                let new_root = mu.apply_for(&prev_state_root)?;
                states.insert(id.clone(), ShardStateStuff::new(id.clone(), new_root.clone())?);
                prev_state_root = new_root;
            }
        }

        // ├─📂 blocks
        let mut blocks = HashMap::new();
        if index.contains_ethalon {
            let filename = format!("{}/blocks/{:x}", path, index.id.root_hash());
            blocks.insert(
                index.id.clone(),
                BlockStuff::deserialize(index.id.clone(), read(filename)?)?
            );
        }
        for id in index.prev_blocks.iter() {
            if id.seq_no() != 0 {
                let filename = format!("{}/blocks/{:x}", path, id.root_hash());
                blocks.insert(
                    id.clone(),
                    BlockStuff::deserialize(id.clone(), read(filename)?)?
                );
            }
        }

        let aux_mc_shard_states = lockfree::map::Map::new();
        aux_mc_shard_states.insert(
            index.last_mc_state.seq_no(),
            states.get(&index.last_mc_state).unwrap().clone()
        );

        let candidate = if !index.contains_candidate {
            None
        } else {
            let path = format!("{}/candidate/", path);
            let data = ton_api::ton::bytes(read(format!("{}/data", path))?);
            Some(BlockCandidate {
                block_id: index.id.clone(),
                collated_file_hash: catchain::utils::get_hash(&data),
                data: data.0,
                collated_data: read(format!("{}/collated_data", path))?,
                created_by: index.created_by.clone(),
            })
        };

        Ok(CollatorTestBundle {
            index,
            top_shard_blocks,
            external_messages,
            states,
            mc_merkle_updates,
            blocks,
            aux_mc_shard_states,
            shard_states_cache: lockfree::map::Map::new(),
            block_handle_cache: Arc::new(lockfree::map::Map::new()),
            candidate
        })
    }

    pub fn ethalon_block(&self) -> Result<Option<BlockStuff>> {
        if self.index.contains_ethalon {
            Ok(Some(
                self.blocks.get(&self.index.id).ok_or_else(|| error!("Index declares contains_ethalon=true but the block is not found"))?.clone()
            ))
        } else if let Some(candidate) = self.candidate() {
            Ok(Some(BlockStuff::new(self.index.id.clone(), candidate.data.clone())?))
        } else {
            Ok(None)
        }
    }

    pub fn ethalon_state(&self) -> Result<Option<ShardStateStuff>> {
        if self.index.contains_ethalon {
            Ok(self.states.get(&self.index.id).cloned())
        } else if let Some(block) = self.ethalon_block()? {
            let prev_ss_root = match block.construct_prev_id()? {
                (prev1, Some(prev2)) => {
                    let ss1 = self.states.get(&prev1).ok_or_else(|| error!("Prev state is not found"))?.root_cell().clone();
                    let ss2 = self.states.get(&prev2).ok_or_else(|| error!("Prev state is not found"))?.root_cell().clone();
                    ShardStateStuff::construct_split_root(ss1, ss2)?
                },
                (prev, None) => {
                    self.states.get(&prev).ok_or_else(|| error!("Prev state is not found"))?.root_cell().clone()
                }
            };
            let merkle_update = block
                .block()
                .read_state_update()?;
            let block_id = block.id().clone();
            let ss_root = merkle_update.apply_for(&prev_ss_root)?;
            Ok(Some(ShardStateStuff::new(block_id.clone(), ss_root)?))
        } else {
            Ok(None)
        }
    }

    pub fn block_id(&self) -> &BlockIdExt { &self.index.id }
    pub fn prev_blocks_ids(&self) -> &Vec<BlockIdExt> { &self.index.prev_blocks }
    pub fn min_ref_mc_seqno(&self) -> u32 { self.index.min_ref_mc_seqno }
    pub fn created_by(&self) -> &UInt256 { &self.index.created_by }
    pub fn rand_seed(&self) -> Option<&UInt256> { self.index.rand_seed.as_ref() }
    pub fn candidate(&self) -> Option<&BlockCandidate> { self.candidate.as_ref() }
    pub fn notes(&self) -> &str { &self.index.notes }
    pub fn set_notes(&mut self, notes: String) { self.index.notes = notes }

    pub fn exists(path: &str, block_id: &BlockIdExt) -> bool {
        let path = Self::build_filename(path, block_id);
        std::path::Path::new(&path).exists()
    }

    fn build_filename(prefix: &str, block_id: &BlockIdExt) -> String {
        format!(
            "{}/{}.{}_{}_{:x}{:x}{:x}{:x}_collator_test_bundle",
            prefix,
            block_id.shard().workchain_id(),
            block_id.shard().shard_prefix_as_str_with_tag(),
            block_id.seq_no(),
            block_id.root_hash().as_slice()[0],
            block_id.root_hash().as_slice()[1],
            block_id.root_hash().as_slice()[2],
            block_id.root_hash().as_slice()[3],
        )
    }
}

// Is used instead full node's engine for run tests
#[async_trait::async_trait]
impl EngineOperations for CollatorTestBundle {
    fn now(&self) -> u32 {
        self.index.now
    }
    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Arc<BlockHandle>> {
        let handle = BlockHandle::new(id.clone(), self.block_handle_cache.clone());
        if self.blocks.contains_key(id) && *id != self.index.id {
            handle.set_data_inited();
            handle.set_state_inited();
            handle.set_applied();
        }
        Ok(Arc::new(handle))
    }

    async fn load_state(&self, block_id: &BlockIdExt) -> Result<ShardStateStuff> {
        if *block_id != self.index.id {
            if let Some(s) = self.states.get(block_id) {
                return Ok(s.clone());
            }
        }
        fail!("bundle doesn't contain state for block {}", block_id)
    }

    async fn load_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        if *handle.id() != self.index.id {
            if let Some(s) = self.blocks.get(handle.id()) {
                return Ok(s.clone());
            }
        }
        fail!("bundle doesn't contain block {}", handle.id())
    }

    async fn load_last_applied_mc_state(&self) -> Result<ShardStateStuff> {
        if let Some(s) = self.states.get(&self.index.last_mc_state) {
            Ok(s.clone())
        } else {
            fail!("bundle doesn't contain state for block {}", &self.index.last_mc_state)
        }
    }

    fn aux_mc_shard_states(&self) -> &lockfree::map::Map<u32, ShardStateStuff> {
        &self.aux_mc_shard_states
    }

    fn shard_states(&self) -> &lockfree::map::Map<ShardIdent, ShardStateStuff> {
        &self.shard_states_cache
    }

    async fn wait_state(&self, handle: &BlockHandle) -> Result<ShardStateStuff> {
        self.load_state(handle.id()).await
    }

    async fn find_block_by_seq_no(&self, acc_pfx: &AccountIdPrefixFull, seq_no: u32) -> Result<Arc<BlockHandle>> {
        for (id, _block) in self.blocks.iter() {
            if id.seq_no() == seq_no && id.shard().contains_full_prefix(acc_pfx) {
                return self.load_block_handle(id)
            }
        }
        fail!("Block with seq_no {} and acc prefix {} is not found in the bundle", seq_no, acc_pfx);
    }

    fn get_external_messages(&self, _shard: &ShardIdent) -> Result<Vec<(Arc<Message>, UInt256)>> {
        Ok(self.external_messages.clone())
    }

    fn get_shard_blocks(&self, _mc_seq_no: u32) -> Result<Vec<Arc<TopBlockDescrStuff>>> {
        if self.top_shard_blocks.len() > 0 {
            return Ok(self.top_shard_blocks.clone());
        } else if let Some(candidate) = self.candidate() {
            let collated_roots = deserialize_cells_tree(&mut Cursor::new(&candidate.collated_data))?;
            for i in 0..collated_roots.len() {
                let croot = collated_roots[i].clone();
                if croot.cell_type() == CellType::Ordinary {
                    let mut res = vec!();
                    let top_shard_descr_dict = TopBlockDescrSet::construct_from_cell(croot)?;
                    top_shard_descr_dict.collection().iterate(|tbd| {
                        let id = tbd.0.proof_for().clone();
                        res.push(Arc::new(TopBlockDescrStuff::new(tbd.0, &id, true)?));
                        Ok(true)
                    })?;
                    return Ok(res);
                }
            }
        }
        Ok(vec!())
    }

    fn complete_external_messages(&self, _to_delay: Vec<UInt256>, _to_delete: Vec<UInt256>) -> Result<()> {
        Ok(())
    }
}
