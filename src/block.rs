use sha2::Digest;
use std::{io::{Write, Cursor}, collections::HashMap};
use ton_block::{Block, BlockIdExt, BlkPrevInfo, Deserializable, ShardIdent, ShardDescr};
use ton_types::{Cell, Result, types::UInt256, deserialize_tree_of_cells, error};

#[cfg(test)]
#[path = "tests/test_block.rs"]
mod tests;

/// It is a wrapper around various block's representations and properties.
/// # Remark
/// Because of no deterministic of a bag of cells's serialization need to store `data` 
/// to make deserialization and serialization functions symmetric.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct BlockStuff {
    id: BlockIdExt,
    block: Block,
    root: Cell,
    data: Vec<u8>,
}

impl BlockStuff {

/*
    pub fn new(id: BlockIdExt, root: Cell) -> Result<Self> {
        let mut dst = Cursor::new(vec!());
        ton_types::cells_serialization::serialize_tree_of_cells(&root, &mut dst)?;
        Ok(
            Self{
                id,
                block: Block::construct_from(&mut root.clone().into())?,
                root,
                data: dst.into_inner(),
            }
        )
    }
*/

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        let mut hasher = sha2::Sha256::new();
        hasher.input(bytes);
        let file_hash: [u8; 32] = hasher.result().into();

        let root = deserialize_tree_of_cells(&mut Cursor::new(bytes))?;
        let block = Block::construct_from(&mut root.clone().into())?;
        let block_info = block.read_info()?;
        let id = BlockIdExt {
            shard_id: *block_info.shard(),
            seq_no: block_info.seq_no(),
            root_hash: root.repr_hash(),
            file_hash: file_hash.into(),
        };
        Ok(Self{ id, block, root, data: bytes.to_owned(), })
    }

    #[cfg(test)]
    pub fn read_from_file(filename: &str) -> Result<Self> {
        let bytes = std::fs::read(filename)?;
        Self::deserialize(&bytes)
    }

    pub fn block(&self) -> &Block { &self.block }

    pub fn id_api(&self) -> ton_api::ton::ton_node::blockidext::BlockIdExt { convert_block_id_ext_blk2api(&self.id) }
    pub fn id(&self) -> &BlockIdExt { &self.id }

    pub fn shard(&self) -> &ShardIdent { self.id.shard() }

    //#[cfg(feature = "external_db")]
    pub fn root_cell(&self) -> &Cell { &self.root }

    pub fn data(&self) -> &[u8] { &self.data }

    pub fn is_masterchain(&self) -> bool {
        self.id.shard().is_masterchain()
    }

    pub fn gen_utime(&self) -> Result<u32> {
        Ok(self.block.read_info()?.gen_utime().0)
    }

    pub fn is_key_block(&self) -> Result<bool> {
        Ok(self.block.read_info()?.key_block())
    }

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

    pub fn write_to<T: Write>(&self, dst: &mut T) -> Result<()> {
        dst.write(&self.data)?;
        Ok(())
    }

/*
    pub fn serialize(&self) -> Result<Vec<u8>> {
        Ok(self.data().to_vec())
    }
*/

    pub fn shards_blocks(&self) -> Result<HashMap<ShardIdent, BlockIdExt>> {
        let mut shardes = HashMap::new();
        self
            .block()
            .read_extra()?
            .read_custom()?
            .ok_or_else(|| error!("Given block is not a master block."))?
            .hashes()
            .iterate_shards(&mut |ident: ShardIdent, descr: ShardDescr| {
                let last_shard_block = BlockIdExt {
                    shard_id: ident,
                    seq_no: descr.seq_no,
                    root_hash: descr.root_hash,
                    file_hash: descr.file_hash
                };
                shardes.insert(ident, last_shard_block);
                Ok(true)
            })?;

        Ok(shardes)
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

pub fn convert_block_id_ext_api2blk(id: &ton_api::ton::ton_node::blockidext::BlockIdExt) -> Result<BlockIdExt> {
    Ok(
        ton_block::BlockIdExt::with_params(
            ton_block::ShardIdent::with_tagged_prefix(id.workchain, id.shard as u64)?,
            id.seqno as u32,
            UInt256::from(&id.root_hash.0),
            UInt256::from(&id.file_hash.0),
        )
    )
}

#[allow(dead_code)]
pub fn convert_block_id_ext_blk2api(id: &BlockIdExt) -> ton_api::ton::ton_node::blockidext::BlockIdExt {
    ton_api::ton::ton_node::blockidext::BlockIdExt {
        workchain: id.shard_id.workchain_id(),
        shard: id.shard_id.shard_prefix_with_tag() as i64, // FIXME: why signed int???
        seqno: if id.seq_no <= std::i32::MAX as u32 {
                id.seq_no as i32
            } else {
                panic!("too big block seq_no") // FIXME: what to do?
            },
        root_hash: ton_api::ton::int256(id.root_hash.as_slice().to_owned()),
        file_hash: ton_api::ton::int256(id.file_hash.as_slice().to_owned()),
    }
}

#[allow(dead_code)]
pub fn convert_block_id_ext_blk_vec(vec: &Vec<BlockIdExt>) -> Vec<ton_api::ton::ton_node::blockidext::BlockIdExt> {
    vec.iter().fold(Vec::new(), |mut vec, item| {
        vec.push(convert_block_id_ext_blk2api(item));
        vec
    })
}

#[allow(dead_code)]
pub fn compare_block_ids(id: &BlockIdExt, id_api: &ton_api::ton::ton_node::blockidext::BlockIdExt) -> bool {
    id.shard_id.shard_prefix_with_tag() == id_api.shard as u64
        && id.shard_id.workchain_id() == id_api.workchain
        && id.root_hash.as_slice() == &id_api.root_hash.0
        && id.file_hash.as_slice() == &id_api.file_hash.0
}
