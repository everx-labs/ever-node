/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific EVERX DEV software governing permissions and
* limitations under the License.
*/
#![allow(unused_imports, unused_assignments, unused_variables, dead_code)]

use crate::{
    block::BlockStuff,
    config::CollatorConfig,
    engine_traits::{EngineAlloc, EngineOperations}, 
    shard_state::ShardStateStuff,
    types::top_block_descr::TopBlockDescrStuff,
    validator::{
        accept_block::create_top_shard_block_description,
        collator::{CollateResult, Collator},
        out_msg_queue::{CachedStates, OutMsgQueueInfoStuff},
        validate_query::ValidateQuery,
        validator_utils::{compute_validator_set_cc, PrevBlockHistory},
        BlockCandidate,
        CollatorSettings,
    },
};
#[cfg(feature = "telemetry")]
use crate::engine_traits::EngineTelemetry;

#[cfg(feature = "telemetry")]
use adnl::telemetry::Metric;
use std::{
    collections::{HashMap, HashSet}, convert::{TryFrom, TryInto}, fs::{File, read, write}, 
    ops::Deref, sync::{Arc, atomic::AtomicU64} 
};
use storage::{
    StorageAlloc,
    block_handle_db::{BlockHandle, BlockHandleDb, BlockHandleStorage}, 
    block_handle_db::NodeStateDb,
    types::BlockMeta,
};
#[cfg(feature = "telemetry")]
use storage::StorageTelemetry;
use ever_block::{
    error, fail, read_boc, read_single_root_boc, Error,
    BlockIdExt, BlockSignatures, BlockSignaturesPure, Cell, CellType, Deserializable,
    GlobalCapabilities, HashmapAugType, HashmapType, MerkleProof, Message, OutMsgQueue,
    Result, Serializable, ShardIdent, ShardStateUnsplit, TopBlockDescr, TopBlockDescrSet,
    UInt256, UsageTree, ValidatorBaseInfo, ValidatorSet, FundamentalSmcAddresses,
};
use crate::engine_traits::RempDuplicateStatus;

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
    #[serde(default, skip_serializing)]
    now: u32,
    #[serde(default)]
    now_ms: u64,
    contains_ethalon: bool,
    #[serde(default)]
    contains_candidate: bool,
    #[serde(default)]
    notes: String,
}

impl TryFrom<CollatorTestBundleIndexJson> for CollatorTestBundleIndex {
    type Error = Error;
    fn try_from(value: CollatorTestBundleIndexJson) -> Result<Self> {
        let mut shard_blocks = vec!();
        for s in value.top_shard_blocks {
            shard_blocks.push(s.parse()?);
        }
        let mut external_messages = vec!();
        for s in value.external_messages {
            external_messages.push(s.parse()?);
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
            created_by: value.created_by.parse()?,
            rand_seed: Some(value.rand_seed.parse()?),
            now_ms: if value.now_ms == 0 { (value.now as u64) * 1000 } else { value.now_ms },
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
            now: (value.now_ms / 1000) as u32,
            now_ms: value.now_ms,
            contains_ethalon: value.contains_ethalon,
            contains_candidate: value.contains_candidate,
            notes: String::new(),
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
    now_ms: u64,
    contains_ethalon: bool,
    contains_candidate: bool,
    notes: String,
}

fn construct_from_file<T: Deserializable>(path: &str) -> Result<(T, UInt256, UInt256)> {
    let bytes = std::fs::read(path)?;
    let fh = UInt256::calc_file_hash(&bytes);
    let cell = read_single_root_boc(&bytes)?;
    let rh = cell.repr_hash();
    Ok((T::construct_from_cell(cell)?, fh, rh))
}

pub fn create_block_handle_storage() -> BlockHandleStorage {
    BlockHandleStorage::with_dbs(
        Arc::new(BlockHandleDb::in_memory()),
        Arc::new(NodeStateDb::in_memory()),
        Arc::new(NodeStateDb::in_memory()),
        #[cfg(feature = "telemetry")]
        Arc::new(StorageTelemetry::default()),
        Arc::new(StorageAlloc::default()),
    )
}

#[cfg(feature = "telemetry")]
pub fn create_engine_telemetry() -> Arc<EngineTelemetry> {
    Arc::new(
        EngineTelemetry {
            storage: Arc::new(StorageTelemetry::default()),
            awaiters: Metric::without_totals("", 1),
            catchain_clients: Metric::without_totals("", 1),
            cells: Metric::without_totals("", 1),
            overlay_clients: Metric::without_totals("", 1),
            peer_stats: Metric::without_totals("", 1),
            shard_states: Metric::without_totals("", 1),
            top_blocks: Metric::without_totals("", 1),
            validator_peers: Metric::without_totals("", 1),
            validator_sets: Metric::without_totals("", 1),
            old_state_cell_load_time: Metric::with_total_average("", 10),
        }
    )
}

pub fn create_engine_allocated() -> Arc<EngineAlloc> {
    Arc::new(
        EngineAlloc {
            storage: Arc::new(StorageAlloc::default()),
            awaiters: Arc::new(AtomicU64::new(0)),
            catchain_clients: Arc::new(AtomicU64::new(0)),
            overlay_clients: Arc::new(AtomicU64::new(0)),
            peer_stats: Arc::new(AtomicU64::new(0)),
            shard_states: Arc::new(AtomicU64::new(0)),
            top_blocks: Arc::new(AtomicU64::new(0)),
            validator_peers: Arc::new(AtomicU64::new(0)),
            validator_sets: Arc::new(AtomicU64::new(0))
        }
    )
}

pub struct CollatorTestBundle {
    index: CollatorTestBundleIndex,
    top_shard_blocks: Vec<Arc<TopBlockDescrStuff>>,
    external_messages: Vec<(Arc<Message>, UInt256)>,
    states: HashMap<BlockIdExt, Arc<ShardStateStuff>>, // used for loading purposes
    state_proofs: HashMap<BlockIdExt, MerkleProof>, // merkle proofs for states to lower their size
    ethalon_block: Option<BlockStuff>,
    candidate: Option<BlockCandidate>,
    block_handle_storage: BlockHandleStorage,
    #[cfg(feature = "telemetry")]
    telemetry: Arc<EngineTelemetry>,
    allocated: Arc<EngineAlloc>,
    collator_config: CollatorConfig,
    split_queues_cache: lockfree::map::Map<BlockIdExt, Option<(OutMsgQueue, OutMsgQueue, HashSet<UInt256>)>>,
}

#[allow(dead_code)]
impl CollatorTestBundle {

    pub async fn build_with_zero_state(mc_zero_state_name: &str, wc_zero_state_names: &[&str]) -> Result<Self> {
        log::info!("Building with zerostate from {} and {}", mc_zero_state_name, wc_zero_state_names.join(", "));

        #[cfg(feature = "telemetry")]
        let telemetry = create_engine_telemetry(); 
        let allocated = create_engine_allocated();

        let (mc_state, mc_fh, mc_rh) = construct_from_file::<ShardStateUnsplit>(mc_zero_state_name)?;
        let last_mc_state = BlockIdExt::with_params(mc_state.shard().clone(), 0, mc_rh, mc_fh);
        let mc_state = ShardStateStuff::from_state(
            last_mc_state.clone(), 
            mc_state,
            #[cfg(feature = "telemetry")]
            &telemetry,
            &allocated
        )?;

        let mut now_ms = mc_state.state()?.gen_time_ms() + 1;
        let mut states = HashMap::new();
        states.insert(last_mc_state.clone(), mc_state);
        for wc_zero_state_name in wc_zero_state_names {
            let (wc_state, wc_fh, wc_rh) = construct_from_file::<ShardStateUnsplit>(wc_zero_state_name)?;
            now_ms = now_ms.max(wc_state.gen_time_ms() + 1);
            let block_id = BlockIdExt::with_params(wc_state.shard().clone(), 0, wc_rh, wc_fh);
            let wc_state = ShardStateStuff::from_state(
                block_id.clone(), 
                wc_state,
                #[cfg(feature = "telemetry")]
                &telemetry,
                &allocated
            )?;
            states.insert(block_id.clone(), wc_state);
        }

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
            now_ms,
            contains_ethalon: false,
            contains_candidate: false,
            notes: String::new(),
        };

        Ok(Self {
            index,
            top_shard_blocks: Default::default(),
            external_messages: Default::default(),
            states,
            state_proofs: Default::default(),
            ethalon_block: None,
            // blocks: Default::default(),
            block_handle_storage: create_block_handle_storage(),
            candidate: None,
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated,
            collator_config: CollatorConfig::default(),
            split_queues_cache: lockfree::map::Map::new(),
        })
    }

    fn deserialize_state(
        path: &str,
        ss_id: &BlockIdExt,
        #[cfg(feature = "telemetry")]
        telemetry: &EngineTelemetry,
        allocated: &EngineAlloc,
    ) -> Result<Arc<ShardStateStuff>> {
        let filename = format!("{}/states/{:x}", path, ss_id.root_hash());
        log::info!("Loading state {} from {}", ss_id, filename);
        let data = read(&filename).map_err(|_| error!("cannot read file {}", filename))?;
        if ss_id.seq_no() == 0 {
            ShardStateStuff::deserialize_zerostate(
                ss_id.clone(), 
                &data,
                #[cfg(feature = "telemetry")]
                &telemetry,
                &allocated 
            )
        } else if let Ok(proof) = MerkleProof::construct_from_bytes(&data) {
            ShardStateStuff::from_state_root_cell(
                ss_id.clone(),
                proof.proof.virtualize(1),
                #[cfg(feature = "telemetry")]
                &telemetry,
                &allocated
            )
        } else {
            ShardStateStuff::deserialize_state_inmem(
                ss_id.clone(),
                Arc::new(data),
                #[cfg(feature = "telemetry")]
                &telemetry,
                &allocated,
                &|| false
            )
        }
    }

    pub fn load(path: impl AsRef<std::path::Path>) -> Result<Self> {

        let path = path.as_ref();
        if !path.is_dir() {
            fail!("Directory not found: {:?}", path);
        }
        let path = path.to_str().unwrap();

        #[cfg(feature = "telemetry")]
        let telemetry = create_engine_telemetry(); 
        let allocated = create_engine_allocated();

        // 🗂 index
        // let file = std::fs::File::open(path.join("index.json"))?;
        let file = std::fs::File::open(format!("{}/index.json", path))?;
        let index: CollatorTestBundleIndexJson = serde_json::from_reader(file)?;
        let index: CollatorTestBundleIndex = index.try_into()?;

        // ├─📂 top_shard_blocks
        let mut top_shard_blocks = vec!();
        for id in index.top_shard_blocks.iter() {
            let filename = format!("{}/top_shard_blocks/{:x}", path, id.root_hash());
            let tbd = TopBlockDescr::construct_from_file(filename)?;
            top_shard_blocks.push(Arc::new(TopBlockDescrStuff::new(tbd, id, true, false)?));
        }

        // to add simple external message:
        // uncomment this block, and change dst address then run test
        // add id (new filename of message) to external messages in index.json
        // std::fs::create_dir_all(format!("{}/external_messages", path)).ok();
        // let src = ever_block::MsgAddressExt::with_extern([0x77; 32].into())?;
        // let dst = hex::decode("b1219502b825ef2345f49fc9065e485e7f478bddafa63039d00c63e494ab7090")?;
        // let dst = ever_block::MsgAddressInt::with_standart(None, 0, dst.into())?;
        // let h = ever_block::ExternalInboundMessageHeader::new(src, dst);
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

        // all shards and mc states
        let iter = index.neighbors.iter()
            .chain(index.prev_blocks.iter())
            .chain(index.mc_states.iter());
        for ss_id in iter {
            let filename = format!("{}/states/{:x}", path, ss_id.root_hash());
            log::info!("Loading state {} from {}", ss_id, filename);
            let data = read(&filename).map_err(|_| error!("cannot read file {}", filename))?;
            let ss = if ss_id.seq_no() == 0 {
                ShardStateStuff::deserialize_zerostate(
                    ss_id.clone(), 
                    &data,
                    #[cfg(feature = "telemetry")]
                    &telemetry,
                    &allocated 
                )?
            } else if let Ok(proof) = MerkleProof::construct_from_bytes(&data) {
                ShardStateStuff::from_state_root_cell(
                    ss_id.clone(),
                    proof.proof.virtualize(1),
                    #[cfg(feature = "telemetry")]
                    &telemetry,
                    &allocated
                )?
            } else {
                ShardStateStuff::deserialize_state_inmem(
                    ss_id.clone(),
                    Arc::new(data),
                    #[cfg(feature = "telemetry")]
                    &telemetry,
                    &allocated,
                    &|| false
                )?
            };
            states.insert(ss_id.clone(), ss);
        }

        // ├─📂 blocks
        let ethalon_block = if !index.contains_ethalon {
            None
        } else {
            let filename = format!("{}/blocks/{:x}", path, index.id.root_hash());
            let data = read(&filename).map_err(|_| error!("cannot read file {}", filename))?;
            Some(BlockStuff::deserialize_block(index.id.clone(), data)?)
        };

        let candidate = if !index.contains_candidate {
            None
        } else {
            let path = format!("{}/candidate/", path);
            let data = read(format!("{}/data", path))?;
            Some(BlockCandidate {
                block_id: index.id.clone(),
                collated_file_hash: catchain::utils::get_hash(&data),
                data,
                collated_data: read(format!("{}/collated_data", path))?,
                created_by: index.created_by.clone(),
            })
        };

        Ok(CollatorTestBundle {
            index,
            top_shard_blocks,
            external_messages,
            states,
            state_proofs: Default::default(),
            ethalon_block,
            block_handle_storage: create_block_handle_storage(),
            candidate,
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated,
            collator_config: CollatorConfig {
                cutoff_timeout_ms: 1000,
                stop_timeout_ms: 3000,
                max_collate_threads: 1,
                retry_if_empty: false,
                finalize_empty_after_ms: 0,
                empty_collation_sleep_ms: 0,
                ..Default::default()
            },
            split_queues_cache: lockfree::map::Map::new(),
        })
    }

    // returns ethalon block or desrialize it from candidate if present
    pub fn ethalon_block(&self) -> Result<Option<BlockStuff>> {
        if self.index.contains_ethalon {
            Ok(self.ethalon_block.clone())
        } else if let Some(candidate) = self.candidate() {
            Ok(Some(BlockStuff::deserialize_block_checked(self.index.id.clone(), candidate.data.clone())?))
        } else {
            Ok(None)
        }
    }

    pub fn block_id(&self) -> &BlockIdExt { &self.index.id }
    pub fn prev_blocks_ids(&self) -> &Vec<BlockIdExt> { &self.index.prev_blocks }
    pub fn min_ref_mc_seqno(&self) -> u32 { self.index.min_ref_mc_seqno }
    pub fn created_by(&self) -> &UInt256 { &self.index.created_by }
    pub fn rand_seed(&self) -> Option<&UInt256> { self.index.rand_seed.as_ref() }
// UNUSED
//    pub fn notes(&self) -> &str { &self.index.notes }

}

impl CollatorTestBundle {
    fn load_state_internal(&self, block_id: &BlockIdExt) -> Result<Arc<ShardStateStuff>> {
        if let Some(state) = self.states.get(block_id) {
            Ok(state.clone())
        } else if let Some(proof) = self.state_proofs.get(block_id) {
            ShardStateStuff::from_state_root_cell(
                block_id.clone(),
                proof.proof.clone().virtualize(1),
                #[cfg(feature = "telemetry")]
                &self.telemetry,
                &self.allocated,
            )
        } else {
            fail!("bundle doesn't contain state for block {}", block_id)
        }
    }

    async fn load_and_simplify_state(
        engine: &Arc<dyn EngineOperations>,
        state_proofs: &mut HashMap<BlockIdExt, MerkleProof>,
        id: &BlockIdExt,
        block_opt: Option<&BlockStuff>,
    ) -> Result<()> {
        Self::add_simplified_state(
            engine.load_state(id).await?.root_cell(),
            state_proofs,
            id,
            block_opt,
            None,
            None
        )
    }
    fn add_simplified_state(
        state_root: &Cell,
        state_proofs: &mut HashMap<BlockIdExt, MerkleProof>,
        id: &BlockIdExt,
        block_opt: Option<&BlockStuff>,
        usage_tree_opt: Option<&UsageTree>,
        min_ref_mc_seqno: Option<u32>,
    ) -> Result<()> {
        if state_proofs.get(id).is_some() {
            assert!(min_ref_mc_seqno.is_none());
            assert!(block_opt.is_none());
            assert!(usage_tree_opt.is_none());
            log::debug!("state proof already exists {}", id);
            return Ok(());
        }
        log::debug!("prepare simplified state for {}", id);
        // let root_hash = root.repr_hash();
        let usage_tree_local = UsageTree::default();
        let usage_tree = usage_tree_opt.unwrap_or(&usage_tree_local);
        let state_root = usage_tree.use_cell(state_root.clone(), false);
        let state = ShardStateUnsplit::construct_from_cell(state_root.clone())?;
        let mut sub_trees = HashSet::new();
        let accounts = state.read_accounts()?;
        let mut smc_addresses = FundamentalSmcAddresses::default();
        if let Some(mut custom) = state.read_custom()? {
            if let Some(min_ref_mc_seqno) = min_ref_mc_seqno {
                for mc_seqno in min_ref_mc_seqno..id.seq_no {
                    custom.prev_blocks.get_raw(&mc_seqno)?.unwrap();
                }
                // add fake for new block to avoid pruned access
                custom.prev_blocks.set(
                    &id.seq_no,
                    &Default::default(),
                    &Default::default()
                )?;
        
                // get all system contracts
                smc_addresses = custom.config().fundamental_smc_addr()?;
                smc_addresses.add_key(&custom.config().minter_address()?)?;
                smc_addresses.add_key(&custom.config().config_address()?)?;
                smc_addresses.add_key(&custom.config().elector_address()?)?;
            }
            // here clear all unnecessary data
            custom.prev_blocks = Default::default();
            // serialize struct and store all sub-trees
            let cell = custom.serialize()?;
            for i in 0..cell.references_count() {
                let child = cell.reference(i)?;
                for j in 0..child.references_count() {
                    sub_trees.insert(child.reference(j)?.repr_hash());
                }
            }
        }
        // read all accounts affected in block
        if let Some(block) = block_opt {
            let extra = block.block()?.read_extra()?;
            extra.read_account_blocks()?.iterate_slices(|account_id, _| {
                smc_addresses.add_key_serialized(account_id)?;
                Ok(true)
            })?;
            // load all work cells
            // log::trace!("traverse accounts");
            // accounts.len()?;
        }
        smc_addresses.iterate_slices_with_keys(|account_id, _| {
            if let (Some(leaf), _) = accounts.clone().set_builder_serialized(account_id, &Default::default(), &Default::default())? {
                // if let Some(leaf) = accounts.get_serialized_raw(account_id)? {
                sub_trees.insert(leaf.cell().repr_hash());
            }
            Ok(true)
        })?;

        // don't prune out_msg_queue_info - it could be very big
        let hash = state.out_msg_queue_info_cell().repr_hash();
        sub_trees.insert(hash);
        let proof = MerkleProof::create_with_subtrees(
            &state_root,
            |hash| usage_tree.contains(hash),
            |hash| sub_trees.contains(hash)
        )?;
        state_proofs.insert(id.clone(), proof);
        Ok(())
    }

    // build bundle for a collating (just now) block. 
    // Uses real engine for top shard blocks and external messages.
    // If usage_tree is not present, try to collate block
    pub async fn build_for_collating_block(
        engine: &Arc<dyn EngineOperations>,
        prev_blocks_ids: Vec<BlockIdExt>,
        usage_tree_opt: Option<UsageTree>,
    ) -> Result<Self> {
        log::info!("Building for furure block, prev[0]: {}", prev_blocks_ids[0]);

        // TODO: fill caches states
        let mut cached_states = CachedStates::new(engine);

        let mut state_proofs = HashMap::new();
        let is_master = prev_blocks_ids[0].shard().is_masterchain();
        let shard = if let Some(merge_block_id) = prev_blocks_ids.get(1) {
            merge_block_id.shard().merge()?
        } else if engine.load_state(&prev_blocks_ids[0]).await?.state()?.before_split() {
            prev_blocks_ids[0].shard().split()?.0
        } else {
            prev_blocks_ids[0].shard().clone()
        };

        //
        // last mc state
        //
        let mc_state = engine.load_last_applied_mc_state().await?;
        let last_mc_id = mc_state.block_id().clone();

        //
        // top shard blocks
        //
        let top_shard_blocks = if is_master {
            engine.get_shard_blocks(&mc_state, None).await?
        } else {
            vec![]
        };

        //
        // external messages
        //
        let external_messages = engine.get_external_messages_iterator(
            shard.clone(),
            0
        ).collect::<Vec<_>>();

        //
        // prev states
        //
        let (usage_tree, candidate) = if let Some(usage_tree) = usage_tree_opt {
            (usage_tree, None)
        } else {
            // try to collate block
            let collate_result = try_collate(
                engine,
                shard.clone(),
                prev_blocks_ids.clone(),
                None,
                None,
                true,
                false
            ).await?;
            (collate_result.usage_tree, collate_result.candidate)
        };
        let (id, now_ms, block_opt);
        if let Some(candidate) = &candidate {
            let block = BlockStuff::deserialize_block(candidate.block_id.clone(), candidate.data.clone())?;
            now_ms = block.block()?.read_info()?.gen_utime_ms();
            id = candidate.block_id.clone();
            block_opt = Some(block);
        } else {
            now_ms = engine.now_ms();
            // now_ms = engine.load_state(&prev_blocks_ids[0]).await?.state_or_queue()?.gen_time_ms() + 1; // TODO: merge?
            id = BlockIdExt {
                shard_id: shard.clone(),
                seq_no: prev_blocks_ids.iter().map(|id| id.seq_no()).max().unwrap() + 1,
                root_hash: UInt256::default(),
                file_hash: UInt256::default(),
            };
            block_opt = None;
        }
        if let Some(merge_block_id) = prev_blocks_ids.get(1) {
            let proof = MerkleProof::create(
                engine.load_state(merge_block_id).await?.root_cell(),
                |h| usage_tree.contains(h)
            )?;
            state_proofs.insert(merge_block_id.clone(), proof);
        }
        if !is_master {
            let proof = MerkleProof::create(
                engine.load_state(&prev_blocks_ids[0]).await?.root_cell(),
                |h| usage_tree.contains(h)
            )?;
            state_proofs.insert(prev_blocks_ids[0].clone(), proof);
        }

        //
        // neighbors
        //
        let mut neighbors = vec!();
        let shards = mc_state.shard_hashes()?;
        // TODO: this can be improved later by collated block
        let neighbor_list = shards.neighbours_for(&shard)?;
        for shard in neighbor_list.iter() {
            Self::load_and_simplify_state(engine, &mut state_proofs, shard.block_id(), None).await?;
            neighbors.push(shard.block_id().clone());
        }

        // master blocks's collator uses new neighbours, based on new shards config.
        // It is difficult to calculate new config there. So add states for all new shard blocks.
        for tsb in top_shard_blocks.iter() {
            let id = tsb.proof_for();
            if !state_proofs.contains_key(id) {
                Self::load_and_simplify_state(engine, &mut state_proofs, id, None).await?;
                neighbors.push(id.clone());
            }
        }

        // collect needed mc states
        let mut oldest_mc_seq_no = last_mc_id.seq_no();
        let mut newest_mc_seq_no = last_mc_id.seq_no();
        for (block_id, _state_root) in state_proofs.iter() {
            let state = engine.load_state(block_id).await?;
            let nb = OutMsgQueueInfoStuff::from_shard_state(&state, &mut cached_states).await?;
            for entry in nb.entries() {
                if entry.mc_seqno() < oldest_mc_seq_no {
                    oldest_mc_seq_no = entry.mc_seqno();
                } else if entry.mc_seqno() > newest_mc_seq_no {
                    newest_mc_seq_no = entry.mc_seqno();
                }
            }
        }

        //
        // mc states
        //
        Self::add_simplified_state(
            mc_state.root_cell(),
            &mut state_proofs,
            mc_state.block_id(),
            if is_master { block_opt.as_ref() } else { None },
            if is_master { Some(&usage_tree) } else { None },
            Some(oldest_mc_seq_no),
        )?;
        let mut mc_states = vec!(mc_state.block_id().clone());
        for mc_seq_no in oldest_mc_seq_no..newest_mc_seq_no {
            let handle = engine.find_mc_block_by_seq_no(mc_seq_no).await?;
            Self::load_and_simplify_state(engine, &mut state_proofs, handle.id(), None).await?;
            mc_states.push(handle.id().clone());
        }

        let index = CollatorTestBundleIndex {
            id,
            top_shard_blocks: top_shard_blocks.iter().map(|tsb| tsb.proof_for().clone()).collect(),
            external_messages: external_messages.iter().map(|(_, id)| id.clone()).collect(),
            last_mc_state: last_mc_id,
            min_ref_mc_seqno: oldest_mc_seq_no,
            mc_states,
            neighbors,
            prev_blocks: prev_blocks_ids,
            created_by: UInt256::default(),
            rand_seed: None,
            now_ms,
            contains_ethalon: false,
            contains_candidate: candidate.is_some(),
            notes: String::new(),
        };

        Ok(Self {
            index,
            top_shard_blocks,
            external_messages,
            states: Default::default(),
            state_proofs,
            ethalon_block: None,
            block_handle_storage: create_block_handle_storage(),
            candidate,
            #[cfg(feature = "telemetry")]
            telemetry: create_engine_telemetry(),
            allocated: create_engine_allocated(),
            collator_config: CollatorConfig::default(),
            split_queues_cache: lockfree::map::Map::new(),
        })
    }

    // build bundle for a validating (just now) block. 
    // Uses real engine for top shard blocks and external messages.
    // Blocks data loading is optional because we sometimes create bundles using a cut database (without blocks). 
    // Such a bundle will work, but creating merkle updates could be long

    pub async fn build_for_validating_block(
        engine: &Arc<dyn EngineOperations>,
        prev: &PrevBlockHistory,
        candidate: BlockCandidate,
    ) -> Result<Self> {
        log::info!("Building for validating block, candidate: {}", candidate.block_id);

        // TODO: fill caches states
        let mut cached_states = CachedStates::new(engine);

        let mut state_proofs = HashMap::new();
        let is_master = candidate.block_id.shard().is_masterchain();

        let block = BlockStuff::deserialize_block_checked(candidate.block_id.clone(), candidate.data.clone())?;
        let now_ms = block.block()?.read_info()?.gen_utime_ms();

        //
        // last mc state
        //
        let mc_state = engine.load_last_applied_mc_state().await?;
        let last_mc_id = mc_state.block_id().clone();

        //
        // top shard blocks
        //
        let top_shard_blocks = if is_master {
            engine.get_shard_blocks(&mc_state, None).await?
        } else {
            vec![]
        };

        //
        // external messages
        //
        let external_messages = engine.get_external_messages_iterator(candidate.block_id.shard().clone(), 0).collect::<Vec<_>>();

        //
        // prev states
        //
        if let Some(merge_block_id) = prev.get_prev(1) {
            let key = candidate.block_id.shard().shard_key(false);
            let usage_tree = UsageTree::default();
            let state = engine.load_state(merge_block_id).await?;
            let state_root = usage_tree.use_cell(state.root_cell().clone(), false);
            let mut accounts = ShardStateUnsplit::construct_from_cell(state_root)?.read_accounts()?;

            let other = engine.load_state(&prev.get_prevs()[0]).await?;
            let state_root = usage_tree.use_cell(other.root_cell().clone(), false);
            let other_accounts = ShardStateUnsplit::construct_from_cell(state_root)?.read_accounts()?;
            accounts.merge(&other_accounts, &key)?;

            Self::add_simplified_state(state.root_cell(), &mut state_proofs, merge_block_id, Some(&block), Some(&usage_tree), None)?;
            Self::add_simplified_state(other.root_cell(), &mut state_proofs, &prev.get_prevs()[0], Some(&block), Some(&usage_tree), None)?;
        } else if !is_master {
            Self::load_and_simplify_state(engine, &mut state_proofs, &prev.get_prevs()[0], Some(&block)).await?;
        }

        // 
        // neighbors
        //
        let mut neighbors = vec!();
        let shards = if is_master {
            block.shard_hashes()?
        } else {
            mc_state.shard_hashes()?
        };
        let neighbor_list = shards.neighbours_for(&candidate.block_id.shard())?;
        for shard in neighbor_list.iter() {
            Self::load_and_simplify_state(engine, &mut state_proofs, shard.block_id(), None).await?;
            neighbors.push(shard.block_id().clone());
        }

        // master blocks's collator uses new neighbours, based on new shards config.
        // It is difficult to calculate new config there. So add states for all new shard blocks.
        for tsb in top_shard_blocks.iter() {
            let id = tsb.proof_for();
            if !state_proofs.contains_key(id) {
                Self::load_and_simplify_state(engine, &mut state_proofs, id, None).await?;
                neighbors.push(id.clone());
            }
        }

        // collect needed mc states
        let mut oldest_mc_seq_no = last_mc_id.seq_no();
        let mut newest_mc_seq_no = last_mc_id.seq_no();
        for (block_id, _state_root) in state_proofs.iter() {
            let state = engine.load_state(block_id).await?;
            let nb = OutMsgQueueInfoStuff::from_shard_state(&state, &mut cached_states).await?;
            for entry in nb.entries() {
                if entry.mc_seqno() < oldest_mc_seq_no {
                    oldest_mc_seq_no = entry.mc_seqno();
                } else if entry.mc_seqno() > newest_mc_seq_no {
                    newest_mc_seq_no = entry.mc_seqno();
                }
            }
        }

        //
        // mc states
        //
        Self::add_simplified_state(
            mc_state.root_cell(),
            &mut state_proofs,
            mc_state.block_id(),
            if is_master { Some(&block) } else { None },
            None,
            Some(oldest_mc_seq_no),
        )?;
        let mut mc_states = vec!(mc_state.block_id().clone());
        for mc_seq_no in oldest_mc_seq_no..newest_mc_seq_no {
            let handle = engine.find_mc_block_by_seq_no(mc_seq_no).await?;
            Self::load_and_simplify_state(engine, &mut state_proofs, handle.id(), None).await?;
            mc_states.push(handle.id().clone());
        }

        // let mut blocks = HashMap::new();
        // blocks.insert(candidate.block_id.clone(), block);

        let index = CollatorTestBundleIndex {
            id: candidate.block_id.clone(),
            top_shard_blocks: top_shard_blocks.iter().map(|tsb| tsb.proof_for().clone()).collect(),
            external_messages: external_messages.iter().map(|(_, id)| id.clone()).collect(),
            last_mc_state: last_mc_id,
            min_ref_mc_seqno: oldest_mc_seq_no,
            mc_states,
            neighbors,
            prev_blocks: prev.get_prevs().to_vec(),
            created_by: candidate.created_by.clone(),
            rand_seed: None,
            now_ms,
            contains_ethalon: false,
            contains_candidate: true,
            notes: String::new(),
        };

        Ok(Self {
            index,
            top_shard_blocks,
            external_messages,
            states: Default::default(),
            state_proofs,
            ethalon_block: None,
            block_handle_storage: create_block_handle_storage(),
            candidate: Some(candidate),
            #[cfg(feature = "telemetry")]
            telemetry: create_engine_telemetry(),
            allocated: create_engine_allocated(),
            collator_config: CollatorConfig::default(),
            split_queues_cache: lockfree::map::Map::new(),
        })
    }

    // Build partially fake bundle using data from node's database. Top shard blocks are built 
    // without signatures. Ethalon block is included, external messages are taken 
    // from ethalon block
    pub async fn build_with_ethalon(
        engine: &Arc<dyn EngineOperations>,
        block: BlockStuff,
    ) -> Result<Self> {
        log::info!("Building with ethalon {}", block.id());

        let info = block.block()?.read_info()?;
        let extra = block.block()?.read_extra()?;

        // TODO: fill caches states
        let mut cached_states = CachedStates::new(engine);

        let mut state_proofs = HashMap::new();
        let is_master = block.id().shard().is_masterchain();

        //
        // last mc state
        //
        let (prev, merge_block_id) = block.construct_prev_id()?;
        let last_mc_id = if let Some(master_ref) = info.read_master_ref()? {
            BlockIdExt::from_ext_blk(master_ref.master)
        } else {
            prev.clone()
        };
        let mc_state = engine.load_state(&last_mc_id).await?;

        //
        // prev states
        //
        let mut prev_blocks_ids = vec!(prev);
        if let Some(merge_block_id) = merge_block_id {
            let key = block.id().shard().shard_key(false);
            let usage_tree = UsageTree::default();
            let state = engine.load_state(&merge_block_id).await?;
            let state_root = usage_tree.use_cell(state.root_cell().clone(), false);
            let mut accounts = ShardStateUnsplit::construct_from_cell(state_root)?.read_accounts()?;

            let other = engine.load_state(&prev_blocks_ids[0]).await?;
            let state_root = usage_tree.use_cell(other.root_cell().clone(), false);
            let other_accounts = ShardStateUnsplit::construct_from_cell(state_root)?.read_accounts()?;
            accounts.merge(&other_accounts, &key)?;

            Self::add_simplified_state(state.root_cell(), &mut state_proofs, &merge_block_id, Some(&block), Some(&usage_tree), None)?;
            Self::add_simplified_state(other.root_cell(), &mut state_proofs, &prev_blocks_ids[0], Some(&block), Some(&usage_tree), None)?;
            prev_blocks_ids.push(merge_block_id);
        } else if !is_master {
            Self::load_and_simplify_state(engine, &mut state_proofs, &prev_blocks_ids[0], Some(&block)).await?;
        }

        //
        // top shard blocks (fake)
        //
        let shard_blocks_ids = block.top_blocks_all().unwrap_or_default();
        let mut top_shard_blocks = vec![];
        let mut top_shard_blocks_ids = vec![];
        for shard_block_id in shard_blocks_ids.iter().filter(|id| id.seq_no() != 0) {
            let handle = engine.load_block_handle(shard_block_id)?
                .ok_or_else(|| error!("Cannot load handle for shard block {}", shard_block_id))?;
            if let Ok(block) = engine.load_block(&handle).await {
                let info = block.block()?.read_info()?;
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
                    engine.deref(),
                ).await? {
                    let tbd = TopBlockDescrStuff::new(tbd, block.id(), true, false).unwrap();
                    top_shard_blocks_ids.push(tbd.proof_for().clone());
                    top_shard_blocks.push(Arc::new(tbd));
                }
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
        let shards = block.shard_hashes().or_else(|_| mc_state.shard_hashes())?;
        let neighbor_list = shards.neighbours_for(block.id().shard())?;
        for shard in neighbor_list.iter() {
            Self::load_and_simplify_state(engine, &mut state_proofs, shard.block_id(), None).await?;
            neighbors.push(shard.block_id().clone());
        }

        // collect needed mc states
        let mut oldest_mc_seq_no = last_mc_id.seq_no();
        let mut newest_mc_seq_no = last_mc_id.seq_no();
        for (block_id, _state) in state_proofs.iter() {
            let state = engine.load_state(block_id).await?;
            let nb = OutMsgQueueInfoStuff::from_shard_state(&state, &mut cached_states).await?;
            for entry in nb.entries() {
                if entry.mc_seqno() < oldest_mc_seq_no {
                    oldest_mc_seq_no = entry.mc_seqno();
                } else if entry.mc_seqno() > newest_mc_seq_no {
                    newest_mc_seq_no = entry.mc_seqno();
                }
            }
        }

        //
        // mc states
        //
        Self::add_simplified_state(
            mc_state.root_cell(),
            &mut state_proofs,
            mc_state.block_id(),
            None,
            None,
            Some(oldest_mc_seq_no)
        )?;
        let mut mc_states = vec!(mc_state.block_id().clone());
        for mc_seq_no in oldest_mc_seq_no..newest_mc_seq_no {
            let handle = engine.find_mc_block_by_seq_no(mc_seq_no).await?;
            Self::load_and_simplify_state(engine, &mut state_proofs, handle.id(), None).await?;
            mc_states.push(handle.id().clone());
        }

        let index = CollatorTestBundleIndex {
            id: block.id().clone(),
            top_shard_blocks: top_shard_blocks_ids,
            external_messages: external_messages_ids,
            last_mc_state: last_mc_id,
            min_ref_mc_seqno: info.min_ref_mc_seqno(),
            mc_states,
            neighbors,
            prev_blocks: prev_blocks_ids,
            created_by: extra.created_by().clone(),
            rand_seed: Some(extra.rand_seed().clone()),
            now_ms: info.gen_utime_ms(),
            contains_ethalon: true,
            contains_candidate: false,
            notes: String::new(),
        };

        Ok(Self {
            index,
            top_shard_blocks,
            external_messages,
            states: Default::default(),
            state_proofs,
            ethalon_block: Some(block),
            block_handle_storage: create_block_handle_storage(),
            candidate: None,
            #[cfg(feature = "telemetry")]
            telemetry: create_engine_telemetry(),
            allocated: create_engine_allocated(),
            collator_config: CollatorConfig::default(),
            split_queues_cache: lockfree::map::Map::new(),
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
            log::info!("Saving top_shard_blocks {}", filename);
            tbd.top_block_descr().write_to_file(filename)?;
        }

        // ├─📂 external_messages
        for (m, id) in self.external_messages.iter() {
            let path = format!("{}/external_messages/", path);
            std::fs::create_dir_all(&path)?;
            let filename = format!("{}/{:x}", path, id);
            log::info!("Saving external message {}", filename);
            m.write_to_file(filename)?;
        }

        // ├─📂 states
        // all states ptoofs
        let path1 = format!("{}/states/", path);
        std::fs::create_dir_all(&path1)?;
        let iter = self.index.neighbors.iter()
            .chain(self.index.prev_blocks.iter())
            .chain(self.index.mc_states.iter());
        for ss_id in iter {
            let filename = format!("{}/{:x}", path1, ss_id.root_hash());
            log::debug!("Saving {} state to {}", ss_id, filename);
            let now = std::time::Instant::now();
            self.state_proofs.get(ss_id)
                .ok_or_else(|| error!("Bundle's internal error (state {})", ss_id))?
                .write_to_file(&filename)?;
            log::debug!("Saved {} state to {} in {} ms", ss_id, filename, now.elapsed().as_millis());
        }

        // ├─📂 blocks
        if let Some(block) = &self.ethalon_block {
            let path = format!("{}/blocks/", path);
            std::fs::create_dir_all(&path)?;
            let filename = format!("{}/{:x}", path, block.id().root_hash());
            log::info!("Saving ethalon block {}", filename);
            block.write_to(&mut File::create(filename)?)?;
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

    pub fn candidate(&self) -> Option<&BlockCandidate> { self.candidate.as_ref() }
    pub fn set_notes(&mut self, notes: String) { self.index.notes = notes }
}

// Is used instead full node's engine for run tests
#[async_trait::async_trait]
impl EngineOperations for CollatorTestBundle {

    fn now(&self) -> u32 {
        (self.index.now_ms / 1000) as u32
    }

    fn now_ms(&self) -> u64 {
        self.index.now_ms
    }

    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>> {
        let handle = self.block_handle_storage.create_handle(
            id.clone(), 
            BlockMeta::default(), 
            None
        )?;
        if let Some(handle) = handle {
            if self.states.contains_key(id) {
                handle.set_state();
                handle.set_block_applied();
            }
            Ok(Some(handle))
        } else {
            Ok(None)
        }
    }

    async fn load_state(&self, block_id: &BlockIdExt) -> Result<Arc<ShardStateStuff>> {
        if *block_id != self.index.id {
            return self.load_state_internal(&block_id)
        }
        fail!("bundle doesn't contain state for block {}", block_id)
    }

    async fn load_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        if *handle.id() != self.index.id {
            if let Some(s) = &self.ethalon_block {
                return Ok(s.clone());
            }
        }
        fail!("bundle doesn't contain block {}", handle.id())
    }

    async fn load_last_applied_mc_state(&self) -> Result<Arc<ShardStateStuff>> {
        self.load_state_internal(&self.index.last_mc_state)
    }

    async fn wait_state(
        self: Arc<Self>,
        id: &BlockIdExt,
        _timeout_ms: Option<u64>,
        _allow_block_downloading: bool
    ) -> Result<Arc<ShardStateStuff>> {
        self.load_state(id).await
    }

    async fn find_mc_block_by_seq_no(&self, seq_no: u32) -> Result<Arc<BlockHandle>> {
        for (id, _block) in self.states.iter() {
            if (id.seq_no() != seq_no) || !id.shard().is_masterchain() {
                continue
            }
            return self.load_block_handle(id)?.ok_or_else(
                || error!("Cannot load handle for block {}", id)
            )
        }
        fail!("Masterblock with seq_no {} is not found in the bundle", seq_no);
    }

    fn get_external_messages_iterator(
        &self,
        _shard: ShardIdent,
        _finish_time_ms: u64
    ) -> Box<dyn Iterator<Item = (Arc<Message>, UInt256)> + Send + Sync> {
        Box::new(self.external_messages.clone().into_iter())
    }

    async fn get_shard_blocks(&self,
        _: &Arc<ShardStateStuff>,
        _: Option<&mut u32>,
    ) -> Result<Vec<Arc<TopBlockDescrStuff>>> {
        if !self.top_shard_blocks.is_empty() {
            return Ok(self.top_shard_blocks.clone());
        } else if let Some(candidate) = self.candidate() {
            log::info!("candidate.collated_data.len(): {}", candidate.collated_data.len());
            if !candidate.collated_data.is_empty() {
                let collated_roots = read_boc(&candidate.collated_data)?.roots;
                for i in 0..collated_roots.len() {
                    let croot = collated_roots[i].clone();
                    if croot.cell_type() == CellType::Ordinary {
                        let mut res = vec!();
                        let top_shard_descr_dict = TopBlockDescrSet::construct_from_cell(croot)?;
                        top_shard_descr_dict.collection().iterate(|tbd| {
                            let id = tbd.0.proof_for().clone();
                            res.push(Arc::new(TopBlockDescrStuff::new(tbd.0, &id, true, false)?));
                            Ok(true)
                        })?;
                        return Ok(res);
                    }
                }
            }
        }
        Ok(vec!())
    }

    fn complete_external_messages(&self, _to_delay: Vec<(UInt256, String)>, _to_delete: Vec<(UInt256, i32)>) -> Result<()> {
        Ok(())
    }

    #[cfg(feature = "telemetry")]
    fn engine_telemetry(&self) -> &Arc<EngineTelemetry> {
        &self.telemetry
    }

    fn engine_allocated(&self) -> &Arc<EngineAlloc> {
        &self.allocated
    }

    async fn check_remp_duplicate(&self, _message_id: &UInt256) -> Result<RempDuplicateStatus> {
        Ok(RempDuplicateStatus::Fresh(UInt256::default()))
    }

    fn collator_config(&self) -> &CollatorConfig {
        &self.collator_config
    }

    fn set_split_queues_calculating(&self, _before_split_block: &BlockIdExt) -> bool {
        true
    }

    fn set_split_queues(
        &self,
        before_split_block: &BlockIdExt,
        queue0: OutMsgQueue,
        queue1: OutMsgQueue,
        visited_cells: HashSet<UInt256>,
    ) {
        self.split_queues_cache.insert(
            before_split_block.clone(),
            Some((queue0, queue1, visited_cells))
        );
    }

    fn get_split_queues(
        &self,
        before_split_block: &BlockIdExt
    ) -> Option<(OutMsgQueue, OutMsgQueue, HashSet<UInt256>)> {
        if let Some(guard) = self.split_queues_cache.get(before_split_block) {
            if let Some(q) = guard.val() {
                return Some(q.clone())
            }
        }
        None
    }
}

pub async fn try_collate(
    engine: &Arc<dyn EngineOperations>,
    shard: ShardIdent,
    prev_blocks_ids: Vec<BlockIdExt>,
    created_by_opt: Option<UInt256>,
    rand_seed_opt: Option<UInt256>,
    is_bundle: bool,
    check_validation: bool,
) -> Result<CollateResult> {
    let mc_state = engine.load_last_applied_mc_state().await?;
    let mc_state_extra = mc_state.shard_state_extra()?;
    let prev_blocks_history = PrevBlockHistory::with_prevs(&shard, prev_blocks_ids);
    let mut cc_seqno_with_delta = 0;
    let cc_seqno_from_state = if shard.is_masterchain() {
        mc_state_extra.validator_info.catchain_seqno
    } else {
        mc_state_extra.shards.calc_shard_cc_seqno(&shard)?
    };
    let nodes = compute_validator_set_cc(
        &mc_state,
        &shard,
        prev_blocks_history.get_next_seqno().unwrap_or_default(),
        cc_seqno_from_state,
        &mut cc_seqno_with_delta
    )?;
    let validator_set = ValidatorSet::with_cc_seqno(0, 0, 0, cc_seqno_with_delta, nodes)?;

    // log::debug!("{}", block_stuff.id());

    log::info!("TRY COLLATE block {}", shard);

    let min_mc_seqno = if prev_blocks_history.get_prevs()[0].seq_no() == 0 {
        0
    } else {
        let state = engine.load_state(&prev_blocks_history.get_prevs()[0]).await?;
        state.state()?.min_ref_mc_seqno()
    };

    let collator_settings = CollatorSettings {
        #[cfg(test)]
        is_bundle,
        ..Default::default()
    };
    let collator = Collator::new(
        shard.clone(),
        min_mc_seqno,
        &prev_blocks_history,
        validator_set.clone(),
        created_by_opt.unwrap_or_default(),
        engine.clone(),
        rand_seed_opt,
        None,
    )?;
    let collate_result = collator.collate(collator_settings).await?;

    if let Some(candidate) = &collate_result.candidate {
        if check_validation {
            // let new_block = Block::construct_from_bytes(&candidate.data).unwrap();

            // std::fs::write(&format!("{}/state_candidate.json", RES_PATH), ever_block_json::debug_state(new_state.clone())?)?;
            // std::fs::write(&format!("{}/block_candidate.json", RES_PATH), ever_block_json::debug_block_full(new_block)?)?;
            
            let validator_query = ValidateQuery::new(
                shard.clone(),
                min_mc_seqno,
                prev_blocks_history.get_prevs().to_vec(),
                candidate.clone(),
                validator_set.clone(),
                engine.clone(),
                true,
                true,
                None,
            );
            validator_query.try_validate().await?;
        }
        Ok(collate_result)
    } else {
        Err(collate_result.error.unwrap())
    }
}
