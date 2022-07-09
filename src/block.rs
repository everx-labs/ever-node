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

use std::{io::Cursor, collections::HashMap, cmp::max, sync::Arc};
use std::io::Write;
use ton_block::{
    Block, BlockIdExt, BlkPrevInfo, Deserializable, ExtBlkRef, ShardDescr, ShardIdent, 
    ShardHashes, HashmapAugType
};
use ton_types::{Cell, Result, types::UInt256, deserialize_tree_of_cells, error, fail, HashmapType};

use crate::shard_state::ShardHashesStuff;


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
    block: Block,
    root: Cell,
    data: Arc<Vec<u8>>, // Arc is used to make cloning more lightweight
}

impl BlockStuff {
    pub fn new(id: BlockIdExt, data: Vec<u8>) -> Result<Self> {
        let file_hash = UInt256::calc_file_hash(&data);
        if file_hash != id.file_hash {
            fail!("block candidate has invalid file hash: declared {:x}, actual {:x}",
                id.file_hash, file_hash)
        }
        let root = deserialize_tree_of_cells(&mut Cursor::new(&data))?;
        if root.repr_hash() != id.root_hash {
            fail!("block candidate has invalid root hash: declared {:x}, actual {:x}",
                id.root_hash, root.repr_hash())
        }
        let block = Block::construct_from(&mut root.clone().into())?;
        Ok(Self { id, block, root, data: Arc::new(data) })
    }

    pub fn deserialize_checked(id: BlockIdExt, data: Vec<u8>) -> Result<Self> {
        let file_hash = UInt256::calc_file_hash(&data);
        if id.file_hash() != &file_hash {
            fail!("wrong file_hash for {}", id)
        }
        Self::deserialize(id, data)
    }

    pub fn deserialize(id: BlockIdExt, data: Vec<u8>) -> Result<Self> {
        let root = deserialize_tree_of_cells(&mut Cursor::new(&data))?;
        if id.root_hash != root.repr_hash() {
            fail!("wrong root hash for {}", id)
        }
        let block = Block::construct_from(&mut root.clone().into())?;
        Ok(Self{ id, block, root, data: Arc::new(data), })
    }

    #[cfg(any(test))]
    pub fn read_from_file(filename: &str) -> Result<Self> {
        let data = std::fs::read(filename)?;
        let file_hash = UInt256::calc_file_hash(&data);

        let root = deserialize_tree_of_cells(&mut Cursor::new(&data))?;
        let block = Block::construct_from(&mut root.clone().into())?;
        let block_info = block.read_info()?;
        let id = BlockIdExt {
            shard_id: block_info.shard().clone(),
            seq_no: block_info.seq_no(),
            root_hash: root.repr_hash(),
            file_hash,
        };
        Ok(Self{ id, block, root, data: Arc::new(data), })
    }


    pub fn block(&self) -> &Block { &self.block }
  
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
        Ok(self.block.read_info()?.gen_utime().as_u32())
    }

// Unused
//    pub fn is_key_block(&self) -> Result<bool> {
//        Ok(self.block.read_info()?.key_block())
//    }

    pub fn construct_prev_id(&self) -> Result<(BlockIdExt, Option<BlockIdExt>)> {
        let header = self.block.read_info()?;
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
            .block
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
            .block()
            .read_extra()?
            .read_custom()?
            .ok_or_else(|| error!("Given block is not a master block."))?
            .shards()
            .clone()
        )
    }

    pub fn shard_hashes(&self) -> Result<ShardHashesStuff> {
        Ok(ShardHashesStuff::from(self
            .block()
            .read_extra()?
            .read_custom()?
            .ok_or_else(|| error!("Given block is not a master block."))?
            .shards()
            .clone()
        ))
    }

    pub fn shards_blocks(&self, workchain_id: i32) -> Result<HashMap<ShardIdent, BlockIdExt>> {
        let mut shards = HashMap::new();
        self
            .block()
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
                shards.insert(ident, last_shard_block);
                Ok(true)
            })?;

        Ok(shards)
    }

// Unused
//    pub fn config(&self) -> Result<ConfigParams> {
//        self
//            .block()
//            .read_extra()?
//            .read_custom()?
//            .and_then(|custom| custom.config().cloned())
//            .ok_or_else(|| error!(NodeError::InvalidArg(
//                "State doesn't contain `custom` field".to_string()
//            )))
//    }

// Unused
//    pub fn read_cur_validator_set_and_cc_conf(&self) -> Result<(ValidatorSet, CatchainConfig)> {
//        self.block().read_cur_validator_set_and_cc_conf()
//    }

    pub fn calculate_tr_count(&self) -> Result<usize> {
        let now = std::time::Instant::now();
        let mut tr_count = 0;

        self.block.read_extra()?.read_account_blocks()?.iterate_objects(|account_block| {
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

