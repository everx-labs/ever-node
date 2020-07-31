use crate::error::NodeError;
use crate::block::convert_block_id_ext_blk2api;

use sha2::Digest;
use std::io::{Write, Cursor};
use ton_block::{BlockIdExt};
use ton_block::{
    ShardStateUnsplit, ShardStateSplit, Serializable, Deserializable, ConfigParams, McStateExtra, HashmapAugType,
};
use ton_types::{Cell, error, fail, Result, deserialize_tree_of_cells, UInt256};


/// It is a wrapper around various shard state's representations and properties.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct ShardStateStuff {
    id: BlockIdExt,
    shard_state: ShardStateUnsplit,
    shard_state_extra: Option<McStateExtra>,
    root: Cell
}

impl ShardStateStuff {

    pub fn new(id: BlockIdExt, root: Cell) -> Result<Self> {
        let shard_state: ShardStateUnsplit = ShardStateUnsplit::construct_from(&mut root.clone().into())?;
        if shard_state.shard().shard_prefix_with_tag() != id.shard().shard_prefix_with_tag() {
            fail!(
                NodeError::InvalidData("State's shard id is not equal to given one".to_string())
            )
        } else if shard_state.seq_no() != id.seq_no {
            fail!(
                NodeError::InvalidData("State's seqno is not equal to given one".to_string())
            )
        } else {
            let shard_state_extra = shard_state.read_custom()?;
            Ok(Self{ id, shard_state, shard_state_extra, root } )
        }
    }

    pub fn construct_split_root(left: Self, right: Self) -> Result<Cell> {
        let ss_split = ShardStateSplit {
            left: left.shard_state,
            right: right.shard_state,
        };
        Ok(ss_split.write_to_new_cell()?.into())
    }

    pub fn deserialize(id: BlockIdExt, bytes: &[u8]) -> Result<Self> {
        let root = deserialize_tree_of_cells(&mut Cursor::new(bytes))?;
        if id.seq_no() == 0 {
            let file_hash = UInt256::from(sha2::Sha256::digest(bytes).as_slice());
            if file_hash != id.file_hash && root.repr_hash() != id.root_hash {
                fail!("Wrong zero state {}. Hashes doesn't correspond", id)
            }
        }
        #[cfg(feature = "store_copy")]
        {
            let path = format!("./target/replication/states/{}", id.shard().shard_prefix_as_str_with_tag());
            std::fs::create_dir_all(&path).ok();
            std::fs::write(format!("{}/{}", path, id.seq_no()), bytes).ok();
        }

        Self::new(id, root)
    }

    #[cfg(test)]
    pub fn read_from_file(id: BlockIdExt, filename: &str) -> Result<Self> {
        Self::deserialize(id, &mut std::fs::read(filename)?)
    }

    pub fn shard_state(&self) -> &ShardStateUnsplit { &self.shard_state }
    pub fn shard_state_extra(&self) -> Result<&McStateExtra> {
        self.shard_state_extra
            .as_ref()
            .ok_or_else(|| error!("Masterchain state of {} must contain McStateExtra", self.block_id()))
    }

    pub fn root_cell(&self) -> &Cell { &self.root }

    pub fn block_id_api(&self) -> ton_api::ton::ton_node::blockidext::BlockIdExt { convert_block_id_ext_blk2api(&self.id) }
    pub fn block_id(&self) -> &BlockIdExt { &self.id }

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
