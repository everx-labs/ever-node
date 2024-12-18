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

use crate::{
    shard_state::ShardHashesStuff,
    validator::accept_block::visit_block_for_proof,
    block_proof::BlockProofStuff, engine_traits::EngineOperations,
};

use ever_block::{
    error, fail, BlkPrevInfo, BlockProof, Block, BlockIdExt, BlockSignatures, BocReader, Cell,
    ConfigParams, ConnectedNwOutDescr, ConnectedNwDescrExt, Deserializable, ExtBlkRef,
    GetRepresentationHash, HashmapAugType, HashmapType, MeshKit, MeshMsgQueueUpdates,
    MeshMsgQueuesKit, MeshUpdate, MerkleProof, MerkleUpdate, OutMsgQueueInfo, OutQueueUpdate,
    Result, Serializable, ShardDescr, ShardIdent, ShardHashes, UInt256, UsageTree
};
#[cfg(test)]
use ever_block::{BlockInfo, BlkMasterInfo, write_boc};
use std::{cmp::max, io::Write, sync::Arc,collections::{HashMap, HashSet}};

#[cfg(test)]
#[path = "tests/test_block.rs"]
mod tests;

pub struct BlockPrevStuff {
    pub mc_block_id: BlockIdExt,
    pub prev: Vec<BlockIdExt>,
    pub _after_split: bool
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BlockKind {
    Block,
    QueueUpdate { queue_update_for: i32, empty: bool },
    MeshKit { network_id: i32 },
    MeshUpdate { network_id: i32 },
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum BlockOrigin {
    Block(Block),
    QueueUpdate {
        block_part: Block,
        queue_update_for: i32,
        empty: bool,
    },
    MeshUpdate {
        block_part: Block,
        queue_updates: MeshMsgQueueUpdates,
        network_id: i32,
    },
    MeshKit {
        block_part: Block,
        queues: MeshMsgQueuesKit,
        network_id: i32,
    }
}
impl Default for BlockOrigin {
    fn default() -> Self {
        Self::Block(Block::default())
    }
}

/// It is a wrapper around various block's representations and properties.
/// # Remark
/// Because of no deterministic of a bag of cells's serialization need to store `data` 
/// to make deserialization and serialization functions symmetric.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct BlockStuff {
    id: BlockIdExt,
    block: BlockOrigin,
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
        let block = BlockOrigin::Block(Block::construct_from_cell(root.clone())?);
        Ok(Self { id, block, root, data })
    }

    pub fn deserialize_queue_update(
        id: BlockIdExt,
        queue_update_for: i32,
        empty: bool,
        data: Vec<u8>
    ) -> Result<Self> {
        let data = Arc::new(data);
        let root = BocReader::new().read_inmem(data.clone())?.withdraw_single_root()?;
        let merkle_proof = MerkleProof::construct_from_cell(root.clone())?;
        if id.root_hash != merkle_proof.hash {
            fail!("wrong merkle proof hash for {}", id)
        }
        let block = BlockOrigin::QueueUpdate{
            block_part: merkle_proof.virtualize()?, 
            queue_update_for,
            empty
        };
        Ok(Self { id, block, root, data })
    }

    pub fn deserialize_mesh_update(
        nw_id: i32,
        id: BlockIdExt,
        target_nw_id: i32,
        data: Vec<u8>
    ) -> Result<(Self, BlockProofStuff)> {

        fn check_update(
            nw_descr: &ConnectedNwOutDescr,
            nw_id: i32,
            mesh_update: &MeshUpdate,
            id: &BlockIdExt,
            shard: &ShardIdent,
        ) -> Result<bool> {
            if nw_descr.out_queue_update.new_hash != nw_descr.out_queue_update.old_hash {
                let update = mesh_update.queue_updates.get_queue_update(shard)?
                    .ok_or_else(|| error!("Can't load update from shard {} block {} {}",
                        shard, nw_id, id))?;
                if update.old_hash != nw_descr.out_queue_update.old_hash {
                    fail!("Old update hash mismatch. Shard {} block {} {}", shard, nw_id, id);
                }
                if update.new_hash != nw_descr.out_queue_update.new_hash {
                    fail!("New update hash mismatch. Shard {} block {} {}", shard, nw_id, id);
                }
                Ok(true)
            } else {
                Ok(false)
            }
        }

        let data = Arc::new(data);
        let root = BocReader::new().read_inmem(data.clone())?.withdraw_single_root()?;
        let mesh_update = MeshUpdate::construct_from_cell(root.clone())?;
        if id.root_hash != mesh_update.mc_block_part.hash {
            fail!("wrong merkle proof hash for {}", id)
        }
        let block_part: Block = mesh_update.mc_block_part.virtualize()?;
        let mc_extra = block_part
            .read_extra()?
            .read_custom()?
            .ok_or_else(|| error!("Given block is not a master block."))?;

        // Check update for masterchain
        let nw_descr = mc_extra.mesh_descr().get(&target_nw_id)?
            .ok_or_else(|| error!("Can't load descr for {} from masterchain, mesh kit {} {}", 
                target_nw_id, nw_id, id)
        )?;
        let mut total_updates = 0;
        if check_update(&nw_descr.queue_descr, nw_id, &mesh_update, &id, &ShardIdent::masterchain())? {
            total_updates += 1;
        }

        // Check updates for shards
        mc_extra.shards().iterate_shards(|ident: ShardIdent, descr: ShardDescr| {
            let nw_descr = descr.mesh_msg_queues.get(&target_nw_id)?
                .ok_or_else(|| error!("Can't load descr for {} from shard {} block {} {}",
                    target_nw_id, ident, nw_id, id)
            )?;
            if check_update(&nw_descr, nw_id, &mesh_update, &id, &ident)? {
                total_updates += 1;
            }
            Ok(true)
        })?;

        let real_len = mesh_update.queue_updates.len()?;
        if real_len != total_updates {
            fail!("Mesh updates count mismatch {} != {}. Block {} {}", 
                real_len, total_updates, nw_id, id);
        }

        let block = BlockOrigin::MeshUpdate {
            block_part,
            queue_updates: mesh_update.queue_updates,
            network_id: nw_id,
        };
        let block_stuff = Self { id, block, root, data };

        let proof = BlockProof {
            proof_for: block_stuff.id.clone(),
            root: mesh_update.mc_block_part.write_to_new_cell()?.into_cell()?,
            signatures: Some(mesh_update.signatures),
        };
        let proof_stuff = BlockProofStuff::new(proof, false)?;

        Ok((block_stuff, proof_stuff))
    }

    pub fn deserialize_mesh_kit(
        nw_id: i32,
        id: BlockIdExt,
        target_nw_id: i32,
        data: Vec<u8>
    ) -> Result<(Self, BlockProofStuff)> {
        let data = Arc::new(data);
        let root = BocReader::new().read_inmem(data.clone())?.withdraw_single_root()?;
        
        let mesh_kit = MeshKit::construct_from_cell(root.clone())?;
        if id.root_hash != mesh_kit.mc_block_part.hash {
            fail!("wrong merkle proof hash for {}", id)
        }

        let block_part: Block = mesh_kit.mc_block_part.virtualize()?;
        let custom = block_part
            .read_extra()?
            .read_custom()?.ok_or_else(|| error!("Given block is not a master block."))?;
        
        let nw_descr = custom.mesh_descr().get(&target_nw_id)?
            .ok_or_else(|| error!("Can't load descr for {} from masterchain, mesh kit {} {}", 
                target_nw_id, nw_id, id))?;

        let queue = mesh_kit.queues.get_queue(&ShardIdent::masterchain())?
            .ok_or_else(|| error!("Can't load queue for {} from masterchain, mesh kit {} {}", 
                target_nw_id, nw_id, id))?;

        if queue.hash()? != nw_descr.queue_descr.out_queue_update.new_hash {
            fail!("Old update hash mismatch. Masterchain, mesh kit {} {}", nw_id, id);
        }

        let mut total_queues = 1;
        custom.shards().iterate_shards(|ident: ShardIdent, descr: ShardDescr| {

            let nw_descr = descr.mesh_msg_queues.get(&target_nw_id)?
                .ok_or_else(|| error!("Can't load descr for {} from shard {} mesh kit {} {}",
                    target_nw_id, ident, nw_id, id))?;

                    total_queues += 1;

            let queue = mesh_kit.queues.get_queue(&ident)?
                .ok_or_else(|| error!("Can't load queue from shard {} mesh kit {} {}",
                    ident, nw_id, id))?;

            if queue.hash()? != nw_descr.out_queue_update.new_hash {
                fail!("Old update hash mismatch. Shard {} mesh kit {} {}", ident, nw_id, id);
            }
            
            Ok(true)
        })?;

        let real_len = mesh_kit.queues.len()?;
        if real_len != total_queues {
            fail!("Mesh out queues count mismatch {} != {}. Block {} {}", 
                real_len, total_queues, nw_id, id);
        }

        let block = BlockOrigin::MeshKit {
            block_part,
            queues: mesh_kit.queues,
            network_id: nw_id,
        };
        let block_stuff = Self { id, block, root, data };

        let proof = BlockProof {
            proof_for: block_stuff.id.clone(),
            root: mesh_kit.mc_block_part.write_to_new_cell()?.into_cell()?,
            signatures: Some(mesh_kit.signatures),
        };
        let proof_stuff = BlockProofStuff::new(proof, false)?;

        Ok((block_stuff, proof_stuff))
    }

    #[cfg(test)]
    pub fn from_block(block: Block) -> Result<Self> {
        let root = block.serialize()?;
        let data = write_boc(&root)?;
        let file_hash = UInt256::calc_file_hash(&data);
        let block_info = block.read_info()?;
        let id = BlockIdExt {
            shard_id: block_info.shard().clone(),
            seq_no: block_info.seq_no(),
            root_hash: root.repr_hash(),
            file_hash,
        };
        Ok(Self { id, block: BlockOrigin::Block(block), root, data: Arc::new(data) })
    }

    #[cfg(test)]
    pub fn read_block_from_file(filename: &str) -> Result<Self> {
        let data = Arc::new(std::fs::read(filename)?);
        let file_hash = UInt256::calc_file_hash(&data);
        let root = BocReader::new().read_inmem(data.clone())?.withdraw_single_root()?;
        let block = Block::construct_from_cell(root.clone())?;
        let block_info = block.read_info()?;
        let id = BlockIdExt {
            shard_id: block_info.shard().clone(),
            seq_no: block_info.seq_no(),
            root_hash: root.repr_hash(),
            file_hash,
        };
        let block = BlockOrigin::Block(block);
        Ok(Self { id, block, root, data })
    }

    #[cfg(test)]
    pub fn fake_block(id: BlockIdExt, mc_block_id: Option<BlockIdExt>, is_key_block: bool) -> Result<Self> {
        let mut block = Block::default();
        if let Some(mc_block_id) = mc_block_id {
            let mut info = BlockInfo::default();
            info.write_master_ref(Some(&BlkMasterInfo {
                master: ExtBlkRef {
                    end_lt: 0,
                    seq_no: mc_block_id.seq_no,
                    root_hash: mc_block_id.root_hash,
                    file_hash: mc_block_id.file_hash,
                }
            }))?;
            info.set_key_block(is_key_block);
            block.write_info(&info)?;
        }
        Ok(BlockStuff {
            id,
            block: BlockOrigin::Block(block),
            root: Cell::default(),
            data: Arc::new(vec!(0xfe; 10_000)),
        })
    }

    pub fn kind(&self) -> BlockKind {
        match &self.block {
            BlockOrigin::Block(_) => BlockKind::Block,
            BlockOrigin::QueueUpdate{queue_update_for, empty, ..} =>
                BlockKind::QueueUpdate{queue_update_for: *queue_update_for, empty: *empty },
            BlockOrigin::MeshUpdate{network_id, ..} => BlockKind::MeshUpdate{network_id: *network_id},
            BlockOrigin::MeshKit{network_id, ..} => BlockKind::MeshKit{network_id: *network_id},
        }
    }

    pub fn block(&self) -> Result<&Block> {
        if let BlockOrigin::Block(block) = &self.block {
            Ok(block)
        } else {
            fail!("Block {} is not a BlockKind::Block", self.id)
        }
    }

    pub fn virt_block(&self) -> Result<&Block> {
        match &self.block {
            BlockOrigin::Block(block) => Ok(block),
            BlockOrigin::QueueUpdate{block_part, ..} => Ok(block_part),
            BlockOrigin::MeshUpdate{block_part, ..} => Ok(block_part),
            BlockOrigin::MeshKit{block_part, ..} => Ok(block_part),
        }
    }

    pub fn get_queue_update_for(&self, workchain_id: i32) -> Result<OutQueueUpdate> {
        if let Some(wc) = self.is_queue_update_for() {
            if wc != workchain_id {
                fail!("{} is not queue update for wc {}", self.id(), workchain_id)
            }
        }
        self
            .virt_block()?
            .out_msg_queue_updates.as_ref().ok_or_else(|| {
                error!("Block {} doesn't contain queue updates at all", self.id())
            })?
            .get(&workchain_id)?.ok_or_else(|| {
                error!("Block {} doesn't contain queue update for wc {}", self.id(), workchain_id)
            })
    }

    #[cfg(test)]
    pub fn fake_with_block(id: BlockIdExt, block: Block) -> Self {
        BlockStuff {
            id,
            block: BlockOrigin::Block(block),
            root: Cell::default(),
            data: Arc::new(vec!(0xfe; 10_000)),
        }
    }

    pub fn is_queue_update(&self) -> bool {
        matches!(self.block, BlockOrigin::QueueUpdate{..})
    }

    pub fn is_queue_update_for(&self) -> Option<i32> {
        match &self.block {
            BlockOrigin::QueueUpdate{queue_update_for, ..} => Some(*queue_update_for),
            _ => None
        }
    }

    pub fn is_mesh(&self) -> bool {
        matches!(self.block, BlockOrigin::MeshUpdate{..} | BlockOrigin::MeshKit{..})
    }

    // pub fn is_mesh_kit(&self) -> bool {
    //     matches!(self.block, BlockOrigin::MeshKit{..})
    // }

    // pub fn is_mesh_update(&self) -> bool {
    //     matches!(self.block, BlockOrigin::MeshUpdate{..})
    // }

    pub fn is_usual_block(&self) -> bool {
        matches!(self.block, BlockOrigin::Block(_))
    }

    pub fn id(&self) -> &BlockIdExt { &self.id }

    pub fn network_global_id(&self) -> i32 {
        match &self.block {
            BlockOrigin::MeshUpdate{network_id, ..} => *network_id,
            BlockOrigin::MeshKit{network_id, ..} => *network_id,
            _ => 0
        }
    }

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
        Ok(self.virt_block()?.read_info()?.gen_utime().as_u32())
    }

   pub fn is_key_block(&self) -> Result<bool> {
       Ok(self.virt_block()?.read_info()?.key_block())
   }

    pub fn construct_prev_id(&self) -> Result<(BlockIdExt, Option<BlockIdExt>)> {
        let header = self.virt_block()?.read_info()?;
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
            .virt_block()?
            .read_info()?
            .read_master_ref()?
            .ok_or_else(|| error!("Can't get master ref: given block is a master block"))?
            .master)
    }

    pub fn write_to<T: Write>(&self, dst: &mut T) -> Result<()> {
        dst.write_all(&self.data)?;
        Ok(())
    }

    pub fn shards(&self) -> Result<ShardHashes> {
        Ok(self
            .virt_block()?
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

    // Commited master block for each connected network
    pub fn mesh_top_blocks(&self) -> Result<HashMap<i32, BlockIdExt>> {
        let mut tbs = HashMap::new();
        self
            .block()?
            .read_extra()?
            .read_custom()?
            .ok_or_else(|| error!("Given block is not a master block."))?
            .mesh_descr()
            .iterate_with_keys(|id: i32, descr: ConnectedNwDescrExt| {
                if let Some(descr) = descr.descr {
                    let block = BlockIdExt {
                        shard_id: ShardIdent::masterchain(),
                        seq_no: descr.seq_no,
                        root_hash: descr.root_hash,
                        file_hash: descr.file_hash
                    };
                    tbs.insert(id, block);
                }
                Ok(true)
            })?;
        Ok(tbs)
    }

    pub fn top_blocks_all(&self) -> Result<Vec<BlockIdExt>> {
        let mut shards = Vec::new();
        self
            .virt_block()?
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
//       self.block()?.read_cur_validator_set_and_cc_conf()
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

    pub fn mesh_update(&self, src_shard: &ShardIdent) -> Result<MerkleUpdate> {
        match &self.block {
            BlockOrigin::MeshUpdate{queue_updates, ..} => {
                let update = queue_updates.get_queue_update(src_shard)?
                    .ok_or_else(|| error!("Block {} doesn't contain queue update for shard {}", self.id(), src_shard))?;
                Ok(update)
            },
            _ => fail!("Block {} is not a mesh update", self.id())
        }
    }

    pub fn mesh_queue(&self, src_shard: &ShardIdent) -> Result<Arc<OutMsgQueueInfo>> {
        match &self.block {
            BlockOrigin::MeshKit{queues, ..} => {
                let q = queues.get_queue(src_shard)?
                    .ok_or_else(|| error!("Block {} doesn't contain queue from shard {}", self.id(), src_shard))?;
                Ok(Arc::new(q))
            },
            _ => fail!("Block {} is not a mesh kit", self.id())
        }
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
    let prev_seqno = match info.read_prev_ref()? {
        BlkPrevInfo::Block { prev } => {
            out_prev.push(BlockIdExt {
                shard_id: if info.after_split() { info.shard().merge()? } else { info.shard().clone() },
                seq_no: prev.seq_no,
                root_hash: prev.root_hash,
                file_hash: prev.file_hash,
            });
            prev.seq_no
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
            max(prev1.seq_no, prev2.seq_no)
        }
    };

    if id.seq_no() != prev_seqno + 1 {
        fail!(
            "new block has invalid seqno (not equal to one plus maximum of seqnos of its ancestors)"
        );
    }
    
    let out_mc_block_id = if info.shard().is_masterchain() {
        out_prev[0].clone()
    } else {
        master_ref
            .ok_or_else(|| error!("non masterchain block doesn't contain mc block ref"))?
            .master.master_block_id().1
    };

    if info.shard().is_masterchain() && (info.vert_seqno_incr() != 0) && !info.key_block() {
        fail!("non-key masterchain block cannot have vert_seqno_incr set")
    }

    Ok((
        out_block_id,
        BlockPrevStuff {
            mc_block_id: out_mc_block_id,
            prev: out_prev,
            _after_split: out_after_split
        }
    ))

}

#[allow(dead_code)]
pub fn make_queue_update_from_block(block_stuff: &BlockStuff, update_for_wc: i32) -> Result<BlockStuff> {
    let queue_update_bytes = make_queue_update_from_block_raw(block_stuff, update_for_wc)?;
    BlockStuff::deserialize_queue_update(block_stuff.id().clone(), update_for_wc, false, queue_update_bytes)
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

fn make_mesh_proof(
    block_stuff: &BlockStuff,
    target_nw: i32,
) -> Result<(MerkleProof, HashMap<BlockIdExt, ConnectedNwOutDescr>)> {
    if !block_stuff.id().shard().is_masterchain() {
        fail!("Can't make mesh proof for non-masterchain block {}", block_stuff.id())
    }

    let usage_tree = UsageTree::with_root(block_stuff.root_cell().clone());
    let block = Block::construct_from_cell(usage_tree.root_cell())?;

    visit_block_for_proof(&block, block_stuff.id())?;

    let mut descr_root_hashes = HashSet::new();
    let mut descrs = HashMap::new();

    // Traget network description (last commited block of the network info)
    // Queue info from this mc to the target network
    let mut mc_descr_root = block
        .read_extra()?
        .read_custom()?
        .ok_or_else(|| error!("Block {} doesn't contain `custom` field", block_stuff.id()))?
        .mesh_descr()
        .get_as_slice(&target_nw)?
        .ok_or_else(|| error!("Block {} doesn't contain description for network {}",
                        block_stuff.id(), target_nw))?;
    descr_root_hashes.insert(mc_descr_root.repr_hash());
    descrs.insert(
        block_stuff.id().clone(),
        ConnectedNwDescrExt::construct_from(&mut mc_descr_root)?.queue_descr
    );

    // Queue info from shardes to the target network
    block
        .read_extra()?
        .read_custom()?
        .ok_or_else(|| error!("Given block is not a master block."))?
        .shards()
        .iterate_shards(|ident: ShardIdent, descr: ShardDescr| {
            let mut descr_root = descr.mesh_msg_queues.get_as_slice(&target_nw)?
                .ok_or_else(|| error!("Can't load description for nw {} from shard {} mc_block {}",
                    target_nw, ident, block_stuff.id()))?;
                descr_root_hashes.insert(descr_root.repr_hash());
                let id = BlockIdExt {
                    shard_id: ident.clone(),
                    seq_no: descr.seq_no,
                    root_hash: descr.root_hash,
                    file_hash: descr.file_hash
                };
                descrs.insert(
                    id,
                    ConnectedNwOutDescr::construct_from(&mut descr_root)?
                );
            Ok(true)
        })?;

    let proof = MerkleProof::create_with_subtrees(
        block_stuff.root_cell(),
        |h| usage_tree.contains(h),
        |h| descr_root_hashes.contains(h)
    )?;

    Ok((proof, descrs))
}

pub async fn make_mesh_kit_raw(
    block_stuff: &BlockStuff,
    target_nw: i32,
    signatures: BlockSignatures,
    engine: &dyn EngineOperations // for loading needed states
) -> Result<Vec<u8>> {

    let (proof, descrs) = make_mesh_proof(block_stuff, target_nw)?;

    let mut queues = MeshMsgQueuesKit::new();

    for (id, out_descr) in descrs {
        let state = engine.load_state(&id).await?;

        let queue = state.state()?.read_out_msg_queues_info()?.1
            .get(&target_nw)?
            .ok_or_else(|| error!("Can't load out queue for {} from {}", target_nw, id))?
            .inner();

        if queue.hash()? != out_descr.out_queue_update.new_hash {
            fail!("INTERNAL ERROR: new queue hash for {} from {} mismatch", target_nw, id);
        }

        queues.add_queue(id.shard(), queue)?;
    }

    let mesh_kit = MeshKit {
        mc_block_part: proof,
        queues,
        signatures,
    };

    mesh_kit.write_to_bytes()
}

pub async fn make_mesh_update_raw(
    block_stuff: &BlockStuff,
    target_nw: i32,
    signatures: BlockSignatures,
    engine: &dyn EngineOperations // for loading needed states
) -> Result<Vec<u8>> {

    let (proof, descrs) = make_mesh_proof(block_stuff, target_nw)?;

    let mut queue_updates = MeshMsgQueueUpdates::new();
    for (id, out_descr) in descrs {
        if out_descr.out_queue_update.new_hash != out_descr.out_queue_update.old_hash {
            
            let new_state = engine.load_state(&id).await?;
            let old_state = engine.load_state(&block_stuff.construct_prev_id()?.0).await?;

            let new_queue_root = new_state.state()?.read_out_msg_queues_info()?.1
                .get_as_slice(&target_nw)?
                .ok_or_else(|| error!("Can't load out queue for {} from {}", target_nw, id))?;
                
            let old_queue_root = old_state.state()?.read_out_msg_queues_info()?.1
                .get_as_slice(&target_nw)?
                .ok_or_else(|| error!("Can't load out queue for {} from {}", target_nw, id))?;

            let now = std::time::Instant::now();
            let update = MerkleUpdate::create(&old_queue_root.cell(), &new_queue_root.cell())?;
            let elapsed = now.elapsed().as_millis();
            if elapsed > 50 {
                log::warn!(
                    "make_mesh_update_raw(target nw: {}): MerkleUpdate::create from state {} took {}ms", 
                    target_nw, id, elapsed
                );
            }
            queue_updates.add_queue_update(id.shard(), update)?;
        }
    }

    let mesh_update = MeshUpdate {
        mc_block_part: proof,
        queue_updates,
        signatures,
    };

    mesh_update.write_to_bytes()
}