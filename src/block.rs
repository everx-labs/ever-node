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

use std::{cmp::max, sync::Arc};
use std::io::Write;
use ton_block::{
    Block, BlockIdExt, BlkPrevInfo, Deserializable, ExtBlkRef, ShardDescr, ShardIdent, 
    ShardHashes, HashmapAugType, MerkleProof, Serializable, ConfigParams, OutQueueUpdate,
};
use ton_types::{
    Cell, Result, types::UInt256, BocReader, error, fail, HashmapType, UsageTree,
};

use crate::{
    shard_state::ShardHashesStuff,
    validator::accept_block::visit_block_for_proof,
};

pub struct BlockPrevStuff {
    pub mc_block_id: BlockIdExt,
    pub prev: Vec<BlockIdExt>,
    pub after_split: bool
}

/// It is a wrapper around various block's representations and properties.
/// # Remark
/// Because of no deterministic of a bag of cells's serialization need to store `data` 
/// to make deserialization and serialization functions symmetric.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct BlockStuff {
    id: BlockIdExt,
    block: Option<Block>,
    queue_update: Option<Block>, // virtual block with queue update, info, ...
    queue_update_for: i32,
    root: Cell,
    data: Arc<Vec<u8>>, // Arc is used to make cloning more lightweight
}

impl BlockStuff {

    pub fn deserialize_block_checked(id: BlockIdExt, data: Vec<u8>) -> Result<Self> {
        let file_hash = UInt256::calc_file_hash(&data);
        if id.file_hash() != &file_hash {
            fail!("wrong file_hash for {}", id)
        }
        Self::deserialize_block(id, data)
    }

    pub fn deserialize_block(id: BlockIdExt, data: Vec<u8>) -> Result<Self> {
        let data = Arc::new(data);
        let root = BocReader::new().read_inmem(data.clone())?.withdraw_single_root()?;
        if id.root_hash != root.repr_hash() {
            fail!("wrong root hash for {}", id)
        }
        let block = Some(Block::construct_from_cell(root.clone())?);
        Ok(Self { id, block, root, data, ..Default::default() })
    }

    pub fn deserialize_queue_update(id: BlockIdExt, queue_update_for: i32, data: Vec<u8>) -> Result<Self> {
        let data = Arc::new(data);
        let root = BocReader::new().read_inmem(data.clone())?.withdraw_single_root()?;
        let merkle_proof = MerkleProof::construct_from_cell(root.clone())?;
        if id.root_hash != merkle_proof.hash {
            fail!("wrong merkle proof hash for {}", id)
        }
        let queue_update = Some(merkle_proof.virtualize()?);
        Ok(Self { id, queue_update, queue_update_for, root, data, ..Default::default() })
    }

    pub fn block(&self) -> Result<&Block> {
        self.block.as_ref().ok_or_else(|| {
            error!("the block {} is not full, it contains only queue update for WC {}", 
                self.id, self.queue_update_for)
        })
    }

    pub fn block_or_queue_update(&self) -> Result<&Block> {
        if let Some(block) = self.block.as_ref() {
            Ok(block)
        } else if let Some(virt_block) = self.queue_update.as_ref() {
            Ok(virt_block)
        } else {
            fail!("INTERNAL ERROR: both block and queue_update fields are None in {}", self.id)
        }
    }

    pub fn get_queue_update_for(&self, workchain_id: i32) -> Result<OutQueueUpdate> {
        if self.is_queue_update() && self.queue_update_for != workchain_id {
            fail!("This queue update is for wc {}, not {}", self.queue_update_for, workchain_id)
        }
        self
            .block_or_queue_update()?
            .out_msg_queue_updates.as_ref().ok_or_else(|| {
                error!("Block {} doesn't contain queue updates at all", self.id())
            })?
            .get(&workchain_id)?.ok_or_else(|| {
                error!("Block {} doesn't contain queue update for wc {}", self.id(), workchain_id)
            })
    }

    pub fn is_queue_update(&self) -> bool { self.queue_update.is_some() }

    pub fn is_queue_update_for(&self) -> Option<i32> {
        if self.is_queue_update() {
            Some(self.queue_update_for)
        } else {
            None
        }
    }
    
    pub fn id(&self) -> &BlockIdExt { &self.id }

// Unused
//    pub fn shard(&self) -> &ShardIdent { 
//        self.id.shard() 
//    }

    pub fn root_cell(&self) -> &Cell { &self.root }

    pub fn data(&self) -> &[u8] { &self.data }

// Unused
//    pub fn is_masterchain(&self) -> bool {
//        self.id.shard().is_masterchain()
//    }

    pub fn gen_utime(&self) -> Result<u32> {
        Ok(self.block_or_queue_update()?.read_info()?.gen_utime().as_u32())
    }

   pub fn is_key_block(&self) -> Result<bool> {
       Ok(self.block_or_queue_update()?.read_info()?.key_block())
   }

    pub fn construct_prev_id(&self) -> Result<(BlockIdExt, Option<BlockIdExt>)> {
        let header = self.block_or_queue_update()?.read_info()?;
        match header.read_prev_ref()? {
            BlkPrevInfo::Block{prev} => {
                let shard_id = if header.after_split() {
                    header.shard().merge()?
                } else {
                    header.shard().clone()
                };
                let id = BlockIdExt {
                    shard_id,
                    seq_no: prev.seq_no,
                    root_hash: prev.root_hash,
                    file_hash: prev.file_hash
                };

                Ok((id, None))
            },
            BlkPrevInfo::Blocks{prev1, prev2} => {
                let prev1 = prev1.read_struct()?;
                let prev2 = prev2.read_struct()?;
                let (shard1, shard2) = header.shard().split()?;
                let id1 = BlockIdExt {
                    shard_id: shard1,
                    seq_no: prev1.seq_no,
                    root_hash: prev1.root_hash,
                    file_hash: prev1.file_hash
                };
                let id2 = BlockIdExt {
                    shard_id: shard2,
                    seq_no: prev2.seq_no,
                    root_hash: prev2.root_hash,
                    file_hash: prev2.file_hash
                };
                Ok((id1, Some(id2)))
            }
        }
    }

    pub fn construct_master_id(&self) -> Result<BlockIdExt> {
        let mc_id = self.get_master_id()?;
        Ok(BlockIdExt::from_ext_blk(mc_id))
    }

    pub fn get_master_id(&self) -> Result<ExtBlkRef> {
        Ok(self
            .block_or_queue_update()?
            .read_info()?
            .read_master_ref()?
            .ok_or_else(|| error!("Can't get master ref: given block is a master block"))?
            .master)
    }

    pub fn write_to<T: Write>(&self, dst: &mut T) -> Result<()> {
        dst.write(&self.data)?;
        Ok(())
    }

    pub fn shards(&self) -> Result<ShardHashes> {
        Ok(self
            .block()?
            .read_extra()?
            .read_custom()?
            .ok_or_else(|| error!("Given block is not a master block."))?
            .shards()
            .clone()
        )
    }

    pub fn shard_hashes(&self) -> Result<ShardHashesStuff> {
        Ok(ShardHashesStuff::from(self
            .block()?
            .read_extra()?
            .read_custom()?
            .ok_or_else(|| error!("Given block is not a master block."))?
            .shards()
            .clone()
        ))
    }

    pub fn top_blocks(&self, workchain_id: i32) -> Result<Vec<BlockIdExt>> {
        let mut shards = Vec::new();
        self
            .block()?
            .read_extra()?
            .read_custom()?
            .ok_or_else(|| error!("Given block is not a master block."))?
            .shards()
            .iterate_shards_for_workchain(workchain_id, |ident: ShardIdent, descr: ShardDescr| {
                let last_shard_block = BlockIdExt {
                    shard_id: ident.clone(),
                    seq_no: descr.seq_no,
                    root_hash: descr.root_hash,
                    file_hash: descr.file_hash
                };
                shards.push(last_shard_block);
                Ok(true)
            })?;

        Ok(shards)
    }

    pub fn top_blocks_all_headers(&self) -> Result<Vec<(BlockIdExt, ShardDescr)>> {
        let mut shards = Vec::new();
        self
            .block()?
            .read_extra()?
            .read_custom()?
            .ok_or_else(|| error!("Given block is not a master block."))?
            .shards()
            .iterate_shards(|ident: ShardIdent, descr: ShardDescr| {
                let id = BlockIdExt {
                    shard_id: ident.clone(),
                    seq_no: descr.seq_no,
                    root_hash: descr.root_hash.clone(),
                    file_hash: descr.file_hash.clone()
                };
                shards.push((id, descr));
                Ok(true)
            })?;

        Ok(shards)
    }

    pub fn top_blocks_all(&self) -> Result<Vec<BlockIdExt>> {
        let mut shards = Vec::new();
        self
            .block()?
            .read_extra()?
            .read_custom()?
            .ok_or_else(|| error!("Given block is not a master block."))?
            .shards()
            .iterate_shards(|ident: ShardIdent, descr: ShardDescr| {
                let last_shard_block = BlockIdExt {
                    shard_id: ident.clone(),
                    seq_no: descr.seq_no,
                    root_hash: descr.root_hash,
                    file_hash: descr.file_hash
                };
                shards.push(last_shard_block);
                Ok(true)
            })?;

        Ok(shards)
    }

   pub fn get_config_params(&self) -> Result<ConfigParams> {
       self.block()?
           .read_extra()?
           .read_custom()?
           .ok_or_else(|| error!("Block {} doesn't contain `custom` field", self.id))?
           .config_mut().take()
           .ok_or_else(|| error!("Block {} doesn't contain `config` field", self.id))
   }

// Unused
//    pub fn read_cur_validator_set_and_cc_conf(&self) -> Result<(ValidatorSet, CatchainConfig)> {
//        self.block()?.read_cur_validator_set_and_cc_conf()
//    }

    pub fn calculate_tr_count(&self) -> Result<usize> {
        let now = std::time::Instant::now();
        let mut tr_count = 0;

        self.block()?.read_extra()?.read_account_blocks()?.iterate_objects(|account_block| {
            tr_count += account_block.transactions().len()?;
            Ok(true)
        })?;
        log::trace!("calculate_tr_count: transactions {}, TIME: {}ms, block: {}", tr_count, now.elapsed().as_millis(), self.id());
        Ok(tr_count)
    }
}

pub trait BlockIdExtExtention {
    fn is_masterchain(&self) -> bool;
}
impl BlockIdExtExtention for BlockIdExt {
    fn is_masterchain(&self) -> bool {
        self.shard().is_masterchain()
    }
}

// unpack_block_prev_blk_try in t-node
pub fn construct_and_check_prev_stuff(
    block_root: &Cell,
    id: &BlockIdExt,
    fetch_blkid: bool)
-> Result<(BlockIdExt, BlockPrevStuff)> {

    let block = Block::construct_from_cell(block_root.clone())?;
    let info = block.read_info()?;

    if info.version() != 0 {
        fail!("Block -> info -> version should be zero (found {})", info.version())
    }

    let out_block_id = if fetch_blkid {
        BlockIdExt {
            shard_id: info.shard().clone(),
            seq_no: info.seq_no(),
            root_hash: block_root.repr_hash(),
            file_hash: UInt256::default(),
        }
    } else {
        if id.shard() != info.shard() {
            fail!("block header contains shard ident: {}, but expected: {}", info.shard(), id.shard())
        }
        if id.seq_no() != info.seq_no() {
            fail!("block header contains seq_no: {}, but expected: {}", info.seq_no(), id.seq_no())
        }
        if *id.root_hash() != block_root.repr_hash() {
            fail!(
                "block header has incorrect root hash: {:x}, but expected: {:x}",
                    block_root.repr_hash(), id.root_hash()
            )
        }
        BlockIdExt::default()
    };

    let master_ref = info.read_master_ref()?;
    if master_ref.is_some() == info.shard().is_masterchain() {
        fail!("Block info: `info.is_master()` and `info.shard().is_masterchain()` mismatch");
    }

    let out_after_split = info.after_split();

    let mut out_prev = vec!();
    let prev_seqno;
    match info.read_prev_ref()? {
        BlkPrevInfo::Block { prev } => {
            out_prev.push(BlockIdExt {
                shard_id: if info.after_split() { info.shard().merge()? } else { info.shard().clone() },
                seq_no: prev.seq_no,
                root_hash: prev.root_hash,
                file_hash: prev.file_hash,
            });
            prev_seqno = prev.seq_no;
        }
        BlkPrevInfo::Blocks { prev1, prev2 } => {
            if info.after_split() {
                fail!("shardchains cannot be simultaneously split and merged at the same block")
            }
            let prev1 = prev1.read_struct()?;
            let prev2 = prev2.read_struct()?;
            if prev1.seq_no == 0 || prev2.seq_no == 0 {
                fail!("shardchains cannot be merged immediately after initial state")
            }
            let (shard1, shard2) = info.shard().split()?;
            out_prev.push(BlockIdExt {
                shard_id: shard1,
                seq_no: prev1.seq_no,
                root_hash: prev1.root_hash,
                file_hash: prev1.file_hash,
            });
            out_prev.push(BlockIdExt {
                shard_id: shard2,
                seq_no: prev2.seq_no,
                root_hash: prev2.root_hash,
                file_hash: prev2.file_hash,
            });
            prev_seqno = max(prev1.seq_no, prev2.seq_no);
        }
    }

    if id.seq_no() != prev_seqno + 1 {
        fail!(
            "new block has invalid seqno (not equal to one plus maximum of seqnos of its ancestors)"
        );
    }
    
    let out_mc_block_id = if info.shard().is_masterchain() {
        out_prev[0].clone()
    } else {
        let master_ref = master_ref.ok_or_else(|| error!(
            "non masterchain block doesn't contain mc block ref"))?;
        BlockIdExt {
            shard_id: ShardIdent::masterchain(),
            seq_no: master_ref.master.seq_no,
            root_hash: master_ref.master.root_hash,
            file_hash: master_ref.master.file_hash,
        }
    };

    if info.shard().is_masterchain() && (info.vert_seqno_incr() != 0) && !info.key_block() {
        fail!("non-key masterchain block cannot have vert_seqno_incr set")
    }

    Ok((
        out_block_id,
        BlockPrevStuff {
            mc_block_id: out_mc_block_id,
            prev: out_prev,
            after_split: out_after_split
        }
    ))

}

#[allow(dead_code)]
pub fn make_queue_update_from_block(block_stuff: &BlockStuff, update_for_wc: i32) -> Result<BlockStuff> {
    let queue_update_bytes = make_queue_update_from_block_raw(block_stuff, update_for_wc)?;
    BlockStuff::deserialize_queue_update(block_stuff.id().clone(), update_for_wc, queue_update_bytes)
}

pub fn make_queue_update_from_block_raw(block_stuff: &BlockStuff, update_for_wc: i32) -> Result<Vec<u8>> {
    let usage_tree = UsageTree::with_root(block_stuff.root_cell().clone());
    let block = Block::construct_from_cell(usage_tree.root_cell())?;

    visit_block_for_proof(&block, block_stuff.id())?;

    let update_root = block
        .out_msg_queue_updates.as_ref()
        .ok_or_else(|| error!("Block {} doesn't contain out_msg_queue_updates", block_stuff.id()))?
        .get_as_slice(&update_for_wc)?
        .ok_or_else(|| error!("Block {} doesn't contain out msg queue update for wc {}", block_stuff.id(), update_for_wc))?
        .cell().clone();

    let update_root_hash = update_root.repr_hash();

    let queue_update = MerkleProof::create_with_subtrees(
        block_stuff.root_cell(),
        |h| usage_tree.contains(h),
        |h| h == &update_root_hash
    )?;
    queue_update.write_to_bytes()
}
