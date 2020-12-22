use crate::error::NodeError;
use crate::block::convert_block_id_ext_blk2api;

use std::{io::{Write, Cursor}};
use ton_block::{
    BlockIdExt, ShardIdent, ShardStateUnsplit, ShardStateSplit, Serializable, Deserializable, 
    ConfigParams, McStateExtra, ShardHashes, HashmapAugType
};
use ton_types::{Cell, error, fail, Result, deserialize_tree_of_cells, UInt256};


/// It is a wrapper around various shard state's representations and properties.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct ShardStateStuff {
    block_id: BlockIdExt,
    shard_state: ShardStateUnsplit,
    shard_state_extra: Option<McStateExtra>,
    root: Cell,
}

impl ShardStateStuff {
    pub fn new(block_id: BlockIdExt, root: Cell) -> Result<Self> {
        let shard_state = ShardStateUnsplit::construct_from(&mut root.clone().into())?;
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
        let mut stuff = Self::default();
        stuff.block_id = block_id;
        stuff.shard_state_extra = shard_state.read_custom()?;
        stuff.shard_state = shard_state;
        stuff.root = root;
        Ok(stuff)
    }

    pub fn from_state(shard_state: ShardStateUnsplit) -> Result<Self> {
        let block_id = BlockIdExt::with_params(shard_state.shard().clone(),
            shard_state.seq_no(), Default::default(), Default::default());
        Self::with_state(block_id, shard_state)
    }

    pub fn with_state(block_id: BlockIdExt, shard_state: ShardStateUnsplit) -> Result<Self> {
        let mut stuff = Self::default();
        stuff.block_id = block_id;
        stuff.root = shard_state.serialize()?;
        stuff.shard_state = shard_state;
        stuff.shard_state_extra = stuff.shard_state.read_custom()?;
        Ok(stuff)
    }

    pub fn construct_split_root(left: Cell, right: Cell) -> Result<Cell> {
        let ss_split = ShardStateSplit { left, right };
        ss_split.serialize()
    }

    pub fn deserialize_zerostate(id: BlockIdExt, bytes: &[u8]) -> Result<Self> {
        if id.seq_no() != 0 {
            fail!("Given id has non-zero seq number");
        }        
        let file_hash = UInt256::calc_file_hash(&bytes);
        if file_hash != id.file_hash {
            fail!("Wrong zero state's {} file hash", id);
        }
        let root = deserialize_tree_of_cells(&mut Cursor::new(bytes))?;
        if root.repr_hash() != id.root_hash() {
            fail!("Wrong zero state's {} root hash", id);
        }
        Self::new(id, root)
    }

    pub fn deserialize(id: BlockIdExt, bytes: &[u8]) -> Result<Self> {
        if id.seq_no() == 0 {
            fail!("Use `deserialize_zerostate` method for zerostate");
        }
        let root = deserialize_tree_of_cells(&mut Cursor::new(bytes))?;
        #[cfg(feature = "store_copy")]
        {
            let path = format!("./target/replication/states/{}", id.shard().shard_prefix_as_str_with_tag());
            std::fs::create_dir_all(&path).ok();
            std::fs::write(format!("{}/{}", path, id.seq_no()), bytes).ok();
        }

        Self::new(id, root)
    }

    pub fn shard_state(&self) -> &ShardStateUnsplit { &self.shard_state }
    pub fn state(&self) -> &ShardStateUnsplit { &self.shard_state }
    pub fn withdraw_state(self) -> ShardStateUnsplit { self.shard_state }
    pub fn shard_state_extra(&self) -> Result<&McStateExtra> {
        self.shard_state_extra
            .as_ref()
            .ok_or_else(|| error!("Masterchain state of {} must contain McStateExtra", self.block_id()))
    }
    pub fn shards(&self) -> Result<&ShardHashes> { Ok(self.shard_state_extra()?.shards()) }

    pub fn root_cell(&self) -> &Cell { &self.root }

    pub fn shard(&self) -> &ShardIdent { &self.block_id.shard() }

    pub fn set_shard(&mut self, shard: ShardIdent) { self.block_id.shard_id = shard; }

    pub fn id(&self) -> ton_api::ton::ton_node::blockidext::BlockIdExt { convert_block_id_ext_blk2api(&self.block_id) }

    pub fn block_id(&self) -> &BlockIdExt { &self.block_id }

    pub fn write_to<T: Write>(&self, dst: &mut T) -> Result<()> {
        ton_types::cells_serialization::serialize_tree_of_cells(&self.root, dst)?;
        Ok(())
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut bytes = Cursor::new(Vec::<u8>::new());
        self.write_to(&mut bytes)?;
        Ok(bytes.into_inner())
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
}

