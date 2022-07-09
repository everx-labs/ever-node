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

use crate::{engine_traits::EngineAlloc, error::NodeError};
#[cfg(feature = "telemetry")]
use crate::engine_traits::EngineTelemetry;

use adnl::{declare_counted, common::{CountedObject, Counter}};
use std::{io::{Write, Cursor}, sync::{Arc, atomic::Ordering}};
use ton_block::{
    BlockIdExt, ShardAccount, ShardIdent, ShardStateUnsplit, ShardStateSplit, ValidatorSet, 
    CatchainConfig, Serializable, Deserializable, ConfigParams, McShardRecord, 
    McStateExtra, ShardDescr, ShardHashes, HashmapAugType, InRefValue, BinTree, 
    BinTreeType, WorkchainDescr
};
use ton_types::{
    AccountId, Cell, Result, deserialize_tree_of_cells, deserialize_tree_of_cells_inmem,
    UInt256, BagOfCells, BocSerialiseMode, SliceData, error, fail,
};

//    #[derive(Debug, Default, Clone, Eq, PartialEq)]
// It is a wrapper around various shard state's representations and properties.
declare_counted!(
    pub struct ShardStateStuff {
        block_id: BlockIdExt,
        shard_state: ShardStateUnsplit,
        shard_state_extra: Option<McStateExtra>,
        root: Cell
    }
);

impl ShardStateStuff {

    pub fn from_root_cell(
        block_id: BlockIdExt, 
        root: Cell,
        #[cfg(feature = "telemetry")]
        telemetry: &EngineTelemetry,
        allocated: &EngineAlloc
    ) -> Result<Arc<Self>> {
        let shard_state = ShardStateUnsplit::construct_from_cell(root.clone())?;
        if shard_state.shard() != block_id.shard() {
            fail!(NodeError::InvalidData("State's shard block_id is not equal to given one".to_string()))
        }
        if shard_state.shard().shard_prefix_with_tag() != block_id.shard().shard_prefix_with_tag() {
            fail!(
                NodeError::InvalidData("State's shard id is not equal to given one".to_string())
            )
        } else if shard_state.seq_no() != block_id.seq_no {
            fail!(
                NodeError::InvalidData("State's seqno is not equal to given one".to_string())
            )

        }
        Self::with_params(
            block_id, shard_state, root, 
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated
        )
    }

    pub fn from_state(
        block_id: BlockIdExt, 
        shard_state: ShardStateUnsplit,
        #[cfg(feature = "telemetry")]
        telemetry: &EngineTelemetry,
        allocated: &EngineAlloc
    ) -> Result<Arc<Self>> {
        let root = shard_state.serialize()?;
        Self::with_params(
            block_id, shard_state, root, 
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated
        )
    }

    pub fn deserialize_zerostate(
        block_id: BlockIdExt, 
        bytes: &[u8],
        #[cfg(feature = "telemetry")]
        telemetry: &EngineTelemetry,
        allocated: &EngineAlloc
    ) -> Result<Arc<Self>> {
        if block_id.seq_no() != 0 {
            fail!("Given block id has non-zero seq number");
        }        
        let file_hash = UInt256::calc_file_hash(&bytes);
        if file_hash != block_id.file_hash {
            fail!("Wrong zero state's {} file hash", block_id);
        }
        let root = deserialize_tree_of_cells(&mut Cursor::new(bytes))?;
        if &root.repr_hash() != block_id.root_hash() {
            fail!("Wrong zero state's {} root hash", block_id);
        }
        Self::from_root_cell(
            block_id, root, 
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated
        )
    }

    pub fn deserialize_inmem(
        block_id: BlockIdExt, 
        bytes: Arc<Vec<u8>>,
        #[cfg(feature = "telemetry")]
        telemetry: &EngineTelemetry,
        allocated: &EngineAlloc
    ) -> Result<Arc<Self>> {
        if block_id.seq_no() == 0 {
            fail!("Use `deserialize_zerostate` method for zerostate");
        }
        let root = deserialize_tree_of_cells_inmem(bytes)?;
        Self::from_root_cell(
            block_id, root, 
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated
        )
    }

    pub fn deserialize(
        block_id: BlockIdExt, 
        bytes: &[u8],
        #[cfg(feature = "telemetry")]
        telemetry: &EngineTelemetry,
        allocated: &EngineAlloc
    ) -> Result<Arc<Self>> {
        if block_id.seq_no() == 0 {
            fail!("Use `deserialize_zerostate` method for zerostate");
        }
        let root = deserialize_tree_of_cells(&mut Cursor::new(bytes))?;
        Self::from_root_cell(
            block_id, root, 
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated
        )
    }

/*
*/

    fn with_params(
        block_id: BlockIdExt,
        shard_state: ShardStateUnsplit,
        root: Cell,  
        #[cfg(feature = "telemetry")]
        telemetry: &EngineTelemetry,
        allocated: &EngineAlloc
    ) -> Result<Arc<Self>> {
        let ret = Self {
            block_id,
            shard_state_extra: shard_state.read_custom()?,
            shard_state,
            root,
            counter: allocated.shard_states.clone().into()
        };
        #[cfg(feature = "telemetry")]
        telemetry.shard_states.update(allocated.shard_states.load(Ordering::Relaxed));
        Ok(Arc::new(ret))
    }

    pub fn construct_split_root(left: Cell, right: Cell) -> Result<Cell> {
        let ss_split = ShardStateSplit { left, right };
        ss_split.serialize()
    }

    pub fn state(&self) -> &ShardStateUnsplit { &self.shard_state }
    pub fn shard_account(&self, address: &AccountId) -> Result<Option<ShardAccount>> {
        self.state().read_accounts()?.account(address)
    }
// Unused
//    pub fn withdraw_state(self) -> ShardStateUnsplit { 
//        self.shard_state 
//    }
    pub fn shard_state_extra(&self) -> Result<&McStateExtra> {
        self.shard_state_extra
            .as_ref()
            .ok_or_else(|| error!("Masterchain state of {} must contain McStateExtra", self.block_id()))
    }
    pub fn shards(&self) -> Result<&ShardHashes> { Ok(self.shard_state_extra()?.shards()) }

    pub fn shard_hashes(&self) -> Result<ShardHashesStuff> {
        Ok(ShardHashesStuff::from(self.shard_state_extra()?.shards().clone()))
    }
                       
    pub fn root_cell(&self) -> &Cell { &self.root }

    pub fn shard(&self) -> &ShardIdent { &self.block_id.shard() }

    pub fn top_blocks(&self, workchain_id: i32) -> Result<Vec<BlockIdExt>> {
        self.shard_hashes()?.top_blocks(&[workchain_id])
    }

// Unused
//    pub fn set_shard(&mut self, shard: ShardIdent) { 
//        self.block_id.shard_id = shard; 
//    }

// Unused
//    pub fn id(&self) -> ton_api::ton::ton_node::blockidext::BlockIdExt { 
//        convert_block_id_ext_blk2api(&self.block_id) 
//    }

    pub fn block_id(&self) -> &BlockIdExt { &self.block_id }

    pub fn seq_no(&self) -> u32 {
        self.state().seq_no()
    }

    pub fn write_to<T: Write>(&self, dst: &mut T) -> Result<()> {
        ton_types::cells_serialization::serialize_tree_of_cells(&self.root, dst)?;
        Ok(())
    }

// Unused
//    pub fn serialize(&self) -> Result<Vec<u8>> {
//        let mut bytes = Cursor::new(Vec::<u8>::new());
//        self.write_to(&mut bytes)?;
//        Ok(bytes.into_inner())
//    }

    pub fn serialize_with_abort(&self, abort: &dyn Fn() -> bool) -> Result<Vec<u8>> {
        let mut bytes = Vec::<u8>::new();
        let boc = BagOfCells::with_params(vec!(&self.root), vec!(), abort)?;
        boc.write_to_with_abort(
            &mut bytes,
            BocSerialiseMode::Generic{
                index: true,
                crc: false,
                cache_bits: false,
                flags: 0 
            },
            None,
            None,
            abort
        )?;
        Ok(bytes)
    }

    pub fn config_params(&self) -> Result<&ConfigParams> {
        Ok(&self.shard_state_extra()?.config)
    }

    pub fn has_prev_block(&self, block_id: &BlockIdExt) -> Result<bool> {
        Ok(self.shard_state_extra()?
            .prev_blocks.get(&block_id.seq_no())?
            .map(|id| &id.blk_ref().root_hash == block_id.root_hash() && &id.blk_ref().file_hash == block_id.file_hash())
            .unwrap_or_default())
    }

// Unused
//    pub fn prev_key_block_id(&self) -> BlockIdExt {
//        if let Some(seq_no) = self.block_id().seq_no().checked_sub(1) {
//            if let Ok(extra) = self.shard_state_extra() {
//                if let Ok(Some(id)) = extra.prev_blocks.get_prev_key_block(seq_no) {
//                    return id.master_block_id().1
//                }
//            }
//        }
//        log::error!("prev_key_block_id error for shard_state {}", self.block_id());
//        BlockIdExt::with_params(ShardIdent::masterchain(), 0, UInt256::default(), UInt256::default())
//    }

    pub fn find_block_id(&self, mc_seq_no: u32) -> Result<BlockIdExt> {
        match self.seq_no().cmp(&mc_seq_no) {
            std::cmp::Ordering::Equal => Ok(self.block_id().clone()),
            std::cmp::Ordering::Greater => {
                let (_end_lt, block_id, _key_block) = self.shard_state_extra()?
                    .prev_blocks.get(&mc_seq_no)?
                    .ok_or_else(|| error!("no master block id for {}", mc_seq_no))?
                    .master_block_id();
                Ok(block_id)
            }
            std::cmp::Ordering::Less => fail!("cannot get block id for future block, because {} < {}", self.seq_no(), mc_seq_no)
        }
    }

    pub fn workchains(&self) -> Result<Vec<(i32, WorkchainDescr)>> {
        let mut vec = Vec::new();
        self.config_params()?.workchains()?.iterate_with_keys(|workchain_id, descr| {
            vec.push((workchain_id, descr));
            Ok(true)
        })?;
        Ok(vec)
    }

    pub fn read_cur_validator_set_and_cc_conf(&self) -> Result<(ValidatorSet, CatchainConfig)> {
        self.config_params()?.read_cur_validator_set_and_cc_conf()
    }

// Unused
//    pub fn find_account(&self, account_id: &UInt256) -> Result<Option<Account>> {
//        self
//            .state()
//            .read_accounts()?
//            .get(account_id)?
//            .map(|shard_acc| shard_acc.read_account())
//            .transpose()
//    }

}


#[derive(Clone, Debug, Default)]
pub struct ShardHashesStuff {
    shards: ShardHashes
}

impl ShardHashesStuff {
    pub fn top_blocks(&self, workchains: &[i32]) -> Result<Vec<BlockIdExt>> {
        let mut shards = Vec::new();
        for workchain_id in workchains {
            if let Some(InRefValue(bintree)) = self.shards.get(workchain_id)? {
                bintree.iterate(|prefix, shard_descr| {
                    let shard_ident = ShardIdent::with_prefix_slice(*workchain_id, prefix)?;
                    let block_id = BlockIdExt::with_params(shard_ident, shard_descr.seq_no, shard_descr.root_hash, shard_descr.file_hash);
                    shards.push(block_id);
                    Ok(true)
                })?;
            }
        }
        Ok(shards)
    }

    fn get_shards(bintree: &BinTree<ShardDescr>, _shard: &ShardIdent, workchain_id: i32, vec: &mut Vec<McShardRecord>) -> Result<bool> {
        bintree.iterate(|prefix, shard_descr| {
            let shard_ident = ShardIdent::with_prefix_slice(workchain_id, prefix)?;
            // turn off hypercube routing
            // if shard.is_neighbor_for(&shard_ident) {}
            vec.push(McShardRecord::from_shard_descr(shard_ident, shard_descr));
            Ok(true)
        })
    }

    pub fn neighbours_for(&self, shard: &ShardIdent, workchain_id: i32) -> Result<Vec<McShardRecord>> {
        let mut vec = Vec::new();
        if let Some(InRefValue(bintree)) = self.shards.get(&workchain_id)? {
            Self::get_shards(&bintree, shard, workchain_id, &mut vec)?;
        }
        Ok(vec)
    }

    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }
}

impl AsRef<ShardHashes> for ShardHashesStuff {
    fn as_ref(&self) -> &ShardHashes {
        &self.shards
    }
}

impl From<ShardHashes> for ShardHashesStuff {
    fn from(shards: ShardHashes) -> Self {
        Self{ shards }
    }
}

impl Deserializable for ShardHashesStuff {
    fn construct_from(slice: &mut SliceData) -> Result<Self> {
        let shards = ShardHashes::construct_from(slice)?;
        Ok(Self { shards })
    }
}

