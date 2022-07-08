/*
* Copyright (C) 2019-2022 TON Labs. All Rights Reserved.
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

use std::{
    collections::HashMap,
    convert::TryInto,
    sync::{Arc, RwLock},
};
use storage::{db::rocksdb::RocksDb, index::{U256Index, U256IndexBatch}};
use ton_block::{Account, BlockIdExt, HashmapAugType, ShardAccount, MASTERCHAIN_ID};
use ton_types::{error, fail, Result, UInt256};
use ton_vm::executor::IndexProvider;

use crate::{
    block::BlockStuff,
    internal_db::{InternalDb, LAST_INDEX_MC_BLOCK},
    shard_state::{ShardHashesStuff, ShardStateStuff},
};

pub struct AccHashesIndex {
    pub init_code_hash: U256Index,
    pub code_hash: U256Index,
    pub data_hash: U256Index,
}

impl AccHashesIndex {
    fn with_db(db: Arc<RocksDb>) -> Result<Self> {
        Ok(Self {
            init_code_hash: U256Index::with_db(db.clone(), "index_init_code_hash_prefix_32")?,
            code_hash: U256Index::with_db(db.clone(), "index_code_hash_prefix_32")?,
            data_hash: U256Index::with_db(db.clone(), "index_data_hash_prefix_32")?,
        })
    }

    fn new_batch(&self, mc_seqno: u32) -> AccHashesIndexBatch {
        AccHashesIndexBatch {
            mc_seqno,
            init_code_hash: U256IndexBatch::with_index(self.init_code_hash.clone()),
            code_hash: U256IndexBatch::with_index(self.code_hash.clone()),
            data_hash: U256IndexBatch::with_index(self.data_hash.clone()),
        }
    }

    fn clear(&self) -> Result<()> {
        self.init_code_hash.clear()?;
        self.code_hash.clear()?;
        self.data_hash.clear()?;
        Ok(())
    }
}

pub struct AccHashesIndexBatch {
    pub mc_seqno: u32,
    pub init_code_hash: U256IndexBatch,
    pub code_hash: U256IndexBatch,
    pub data_hash: U256IndexBatch,
}

impl AccHashesIndexBatch {

    fn merge(&mut self, other: &AccHashesIndexBatch) {
        self.init_code_hash.merge(&other.init_code_hash);
        self.code_hash.merge(&other.code_hash);
        self.data_hash.merge(&other.data_hash);
    }

    fn commit(&self) -> Result<()> {
        self.init_code_hash.commit()?;
        self.code_hash.commit()?;
        self.data_hash.commit()?;
        Ok(())
    }

    /// compare account's previous and new state and update hash if need
    fn update_account_index<'a, T: AsRef<[u8]> + PartialEq>(
        index_batch: &mut U256IndexBatch,
        workchain_id: i32,
        account_id: impl AsRef<[u8]>,
        prev: Option<&'a Account>,
        next: Option<&'a Account>,
        extract_hash: impl Fn(&'a Account) -> Option<T>,
    ) {
        let prev = prev.map(|acc| extract_hash(acc)).flatten();
        let next = next.map(|acc| extract_hash(acc)).flatten();
        if prev != next {
            if let Some(hash) = prev {
                index_batch.delete(hash.as_ref().to_vec(), workchain_id, &account_id);
            }
            if let Some(hash) = next {
                index_batch.put(hash.as_ref().to_vec(), workchain_id, &account_id);
            }
        }
    }

    fn update_by_block(
        &mut self,
        block: &BlockStuff,
        prevs: &[Arc<ShardStateStuff>],
        next: &Arc<ShardStateStuff>,
    ) -> Result<()> {
        let workchain_id = block.id().shard().workchain_id();
        let extra = block.block().read_extra()?;
        let accounts = extra.read_account_blocks()?;
        accounts.iterate_with_keys(|account_id, _block_acc| {
            let first = match prevs[0].find_account(&account_id)? {
                None => match prevs.get(1) {
                    Some(p) => p.find_account(&account_id)?,
                    None => None,
                },
                first => first,
            };
            let last = next.find_account(&account_id)?;
            Self::update_account_index(
                &mut self.init_code_hash,
                workchain_id,
                &account_id,
                first.as_ref(),
                last.as_ref(),
                Account::init_code_hash,
            );
            Self::update_account_index(
                &mut self.code_hash,
                workchain_id,
                &account_id,
                first.as_ref(),
                last.as_ref(),
                Account::get_code_hash,
            );
            Self::update_account_index(
                &mut self.data_hash,
                workchain_id,
                &account_id,
                first.as_ref(),
                last.as_ref(),
                Account::get_data_hash,
            );
            Ok(true)
        })?;
        Ok(())
    }

    /// build index by masterchain or workchain state
    fn build_by_state(&mut self, state: &ShardStateStuff) -> Result<()> {
        let accounts = state.state().read_accounts()?;
        accounts.iterate_with_keys(|account_id, shard_acc| {
            let acc = shard_acc.read_account()?;
            if let Some(init_code_hash) = acc.init_code_hash() {
                self.init_code_hash.put(
                    init_code_hash.as_slice().to_vec(),
                    state.shard().workchain_id(),
                    account_id.as_slice(),
                );
            }
            if let Some(code) = acc.get_code() {
                self.code_hash.put(
                    code.repr_hash().as_slice().to_vec(),
                    state.shard().workchain_id(),
                    account_id.as_slice(),
                );
            }
            if let Some(data) = acc.get_data() {
                self.data_hash.put(
                    data.repr_hash().as_slice().to_vec(),
                    state.shard().workchain_id(),
                    account_id.as_slice(),
                );
            }
            Ok(true)
        })?;
        Ok(())
    }
}

struct AccHashesIndexProvider {
    db: Arc<InternalDb>,
    shards: ShardHashesStuff,
    workchain_id: i32,
    batch: Arc<AccHashesIndexBatch>,
}

impl AccHashesIndexProvider {
    fn get_accounts_by_hash<'a>(
        &'a self,
        hash: &'a UInt256,
        get_needed_batch: impl FnOnce(&AccHashesIndexBatch) -> &U256IndexBatch,
    ) -> Result<Vec<ShardAccount>> {
        let len = hash.as_slice().len();
        let ids = &get_needed_batch(&self.batch).get_by_prefix(hash.as_slice());
        let mut result = Vec::new();
        let last_id = self
            .db
            .load_full_node_state(LAST_INDEX_MC_BLOCK)?
            .ok_or_else(|| error!("problem with indeces"))?;
        let mc_state = self.db.load_shard_state_dynamic(&last_id)?;
        // TODO: we can also merge all accounts in one state and find - it can be faster
        let shards = self.shards.top_blocks(&[self.workchain_id])?;
        let mut states = Vec::new();
        for block_id in &shards {
            states.push(self.db.load_shard_state_dynamic(block_id)?);
        }
        for id in ids {
            let id = &id[len..];
            let workchain_id = i32::from_be_bytes(id[0..4].try_into()?);
            // TODO: it can be long, we need to make work with byte buffer
            let account_id = UInt256::from_slice(&id[4..]);
            if workchain_id == MASTERCHAIN_ID {
                if let Some(shard_acc) = mc_state.state().read_accounts()?.get(&account_id)? {
                    result.push(shard_acc);
                }
            } else {
                for state in &states {
                    if let Some(shard_acc) = state.state().read_accounts()?.get(&account_id)? {
                        result.push(shard_acc);
                        break;
                    }
                }
            }
        }
        Ok(result)
    }
}

impl IndexProvider for AccHashesIndexProvider {
    fn get_accounts_by_init_code_hash(&self, hash: &UInt256) -> Result<Vec<ShardAccount>> {
        self.get_accounts_by_hash(hash, |storage| &storage.init_code_hash)
    }
    fn get_accounts_by_code_hash(&self, hash: &UInt256) -> Result<Vec<ShardAccount>> {
        self.get_accounts_by_hash(hash, |storage| &storage.code_hash)
    }
    fn get_accounts_by_data_hash(&self, hash: &UInt256) -> Result<Vec<ShardAccount>> {
        self.get_accounts_by_hash(hash, |storage| &storage.data_hash)
    }
}

pub struct AccHashesIndexManager {
    db: Arc<InternalDb>,
    index: AccHashesIndex,
    batches: RwLock<HashMap<UInt256, Arc<AccHashesIndexBatch>>>,
    workchain_id: i32,
}

impl AccHashesIndexManager {
    /// main constructor
    pub fn with_db(db: Arc<InternalDb>, workchain_id: i32) -> Result<Self> {
        let index = AccHashesIndex::with_db(db.db.clone())?;
        Ok(Self {
            db,
            index,
            batches: RwLock::new(HashMap::new()),
            workchain_id,
        })
    }

    /// prepare special structure for providing indexing accounts
    /// if temporary index was not built for this key - it will be built here
    pub fn index_provider(
        &self,
        shards: ShardHashesStuff,
        prev_mc_block_id: &BlockIdExt,
        root_hash_opt: Option<&UInt256>,
    ) -> Result<Option<Arc<dyn IndexProvider>>> {
        let batch = match root_hash_opt {
            Some(root_hash) => {
                let mut batches = self.batches.write().unwrap();
                match batches.get(root_hash) {
                    None => {
                        let batch = Arc::new(self.construct_batch(&shards, prev_mc_block_id.seq_no())?);
                        batches.insert(root_hash.clone(), batch.clone());
                        batch
                    }
                    Some(batch) => batch.clone()
                }
            }
            None => {
                Arc::new(self.construct_batch(&shards, prev_mc_block_id.seq_no())?)
            }
        };
        Ok(Some(Arc::new(AccHashesIndexProvider {
            db: self.db.clone(),
            batch,
            shards,
            workchain_id: self.workchain_id,
        })))
    }

    fn new_batch(&self, mc_seqno: u32) -> AccHashesIndexBatch {
        self.index.new_batch(mc_seqno)
    }

    fn retain(&self, mc_seqno: u32) {
        self.batches
            .write()
            .unwrap()
            .retain(|_, batch| batch.mc_seqno + 2 >= mc_seqno);
    }

    // fn clear(&self) -> Result<()> {
    //     self.index.clear()
    // }

    /// creates temporary index by master of shard state
    fn build_index_by_state(&self, state: &ShardStateStuff) -> Result<AccHashesIndexBatch> {
        let mut batch = self.new_batch(state.block_id().seq_no());
        batch.build_by_state(state)?;
        Ok(batch)
    }

    /// build index after boot
    pub(crate) fn build_index(&self, mc_state: &ShardStateStuff) -> Result<()> {
        self.index.clear()?;
        let batch = self.build_index_by_state(mc_state)?;
        batch.commit()?;

        let shards = mc_state.shard_hashes()?;
        let shards = shards.top_blocks(&[self.workchain_id])?;
        for block_id in &shards {
            let state = self.db.load_shard_state_dynamic(block_id)?;
            let batch = self.build_index_by_state(&state)?;
            batch.commit()?;
        }
        log::info!(target: "index", "constructed account index {}", mc_state.block_id());
        self.db
            .save_full_node_state(LAST_INDEX_MC_BLOCK, mc_state.block_id())
    }

    /// find previous block ids which belong to current masterchain block
    fn load_shard_blocks(
        &self,
        id: BlockIdExt,
        blocks: &mut Vec<BlockIdExt>,
        prev_mc_seqno: u32,
    ) -> Result<()> {
        let handle = self
            .db
            .load_block_handle(&id)?
            .ok_or_else(|| error!("no block handle for {}", id))?;
        let mc_seqno = handle.masterchain_ref_seq_no();
        if mc_seqno == prev_mc_seqno {
            if let Ok(prev) = self.db.load_block_prev1(&id) {
                self.load_shard_blocks(prev, blocks, prev_mc_seqno)?;
            }
            if let Ok(prev) = self.db.load_block_prev2(&id) {
                self.load_shard_blocks(prev, blocks, prev_mc_seqno)?;
            }
            blocks.push(id);
        }
        Ok(())
    }

    /// creates temporary index for shards
    /// it will not be written to disk
    /// and updates for shardblock must be ready
    fn construct_batch(
        &self,
        shards: &ShardHashesStuff,
        prev_mc_seqno: u32,
    ) -> Result<AccHashesIndexBatch> {
        let shards = shards.top_blocks(&[self.workchain_id])?;
        let mut blocks = Vec::with_capacity(shards.len());
        for id in shards.into_iter() {
            self.load_shard_blocks(id, &mut blocks, prev_mc_seqno)?;
        }
        let mut batch = self.new_batch(prev_mc_seqno + 1);
        for id in blocks.iter().rev() {
            // we use precreated update for shard blocks
            if let Some(update) = self.batches.read().unwrap().get(id.root_hash()) {
                batch.merge(update)
            } else {
                log::error!(target: "index", "shard block {} was not appllied previously or race condition?", id)
            }
        }
        Ok(batch)
    }

    fn update_index_by_master_block(&self, block: &BlockStuff) -> Result<()> {
        // check if index already updated
        if let Some(id) = self.db.load_full_node_state(LAST_INDEX_MC_BLOCK)? {
            if id.seq_no() >= block.id().seq_no() {
                fail!("index already applied for {}", block.id().seq_no())
            }
            // if batch is not present - get it, update, then write
            let key = block.id().root_hash();
            if let Some(batch) = self.batches.write().unwrap().remove(key) {
                // write to disk previously prepared batch
                batch.commit()?;
            } else {
                let (prev, _) = block.construct_prev_id()?;
                let prev = self.db.load_shard_state_dynamic(&prev)?;
                let next = self.db.load_shard_state_dynamic(block.id())?;
                let shards = block.shard_hashes()?;
                // construct compulative batch by shards
                let mut batch = self.construct_batch(&shards, prev.seq_no())?;
                // update batch with new block
                batch.update_by_block(block, &[prev], &next)?;
                // write to disk
                batch.commit()?;
            }
            // remove old batches
            self.retain(block.id().seq_no());
        } else {
            let mc_state = self.db.load_shard_state_dynamic(block.id())?;
            self.build_index(&mc_state)?;
        }
        self.db
            .save_full_node_state(LAST_INDEX_MC_BLOCK, block.id())
    }

    /// update index by master or shard block
    pub(crate) fn update_index(&self, block: &BlockStuff) -> Result<()> {
        let key = block.id().root_hash();
        if block.id().shard().is_masterchain() {
            self.update_index_by_master_block(block)?;
        } else if self.batches.read().unwrap().get(key).is_none() {
            let (prev_block_id1, prev_block_id2_opt) = block.construct_prev_id()?;
            let mut prevs = vec![self.db.load_shard_state_dynamic(&prev_block_id1)?];
            if let Some(prev) = prev_block_id2_opt {
                prevs.push(self.db.load_shard_state_dynamic(&prev)?)
            }
            let next = self.db.load_shard_state_dynamic(block.id())?;
            let mut batch = self.new_batch(block.id().seq_no());
            batch.update_by_block(block, &prevs, &next)?;
            if self
                .batches
                .write()
                .unwrap()
                .insert(key.clone(), Arc::new(batch))
                .is_some()
            {
                log::warn!(target: "index", "updated by block: {} twice", block.id());
            }
        }
        log::trace!(target: "index", "updated by block: {}", block.id());
        Ok(())
    }
}
