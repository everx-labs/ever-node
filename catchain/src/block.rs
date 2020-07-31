pub use super::*;
use std::fmt;
use std::str::FromStr;
use utils::*;

/*
    Implementation details for Block
*/

pub(crate) struct BlockImpl {
    source_id: usize, //receiver source which has generated & signed this block
    fork_id: usize,   //fork ID for this block inside current node
    source_public_key_hash: PublicKeyHash, //public key hash of the source
    height: BlockHeight, //height of the block
    hash: BlockHash,  //hash of the block
    prev: Option<BlockPtr>, //previous block in a fork chain
    payload: BlockPayload, //block's payload (for validator session)
    block_deps: Vec<BlockPtr>, //dependencies for this block
    forks_dep_heights: Vec<BlockHeight>, //heights of each fork which is used in prev & dependency blocks for this block
    extra_id: BlockExtraId,              //block extra data identifier
}

/*
    Implementation for public Block trait
*/

impl Block for BlockImpl {
    /*
        General purpose methods & accessors
    */

    fn get_extra_id(&self) -> BlockExtraId {
        self.extra_id
    }

    fn get_payload(&self) -> &BlockPayload {
        &self.payload
    }

    fn get_source_id(&self) -> usize {
        self.source_id
    }

    fn get_fork_id(&self) -> usize {
        self.fork_id
    }

    fn get_source_public_key_hash(&self) -> &PublicKeyHash {
        &self.source_public_key_hash
    }

    fn get_hash(&self) -> &BlockHash {
        &self.hash
    }

    fn get_height(&self) -> BlockHeight {
        self.height
    }

    fn get_prev(&self) -> Option<BlockPtr> {
        self.prev.clone()
    }

    fn get_deps(&self) -> &Vec<BlockPtr> {
        &self.block_deps
    }

    fn get_forks_dep_heights(&self) -> &Vec<BlockHeight> {
        &self.forks_dep_heights
    }

    fn is_descendant_of(&self, block: &dyn Block) -> bool {
        let fork = block.get_fork_id();

        if fork >= self.forks_dep_heights.len() {
            return false;
        }

        block.get_height() <= self.forks_dep_heights[fork]
    }
}

impl fmt::Display for BlockImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Block(hash={:?}, source_id={}, height={})",
            &self.hash, self.source_id, self.height
        )
    }
}

impl fmt::Debug for BlockImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Block")
            .field("hash", &self.hash)
            .field("source_id", &self.source_id)
            .field("height", &self.height)
            .finish()
    }
}

impl BlockImpl {
    /*
        Block creation
    */

    pub(crate) fn create(
        source_id: usize,
        fork_id: usize,
        source_public_key_hash: PublicKeyHash,
        height: BlockHeight,
        hash: BlockHash,
        payload: BlockPayload,
        prev_block: Option<BlockPtr>,
        block_deps: Vec<BlockPtr>,
        forks_dep_heights: Vec<BlockHeight>,
        extra_id: BlockExtraId,
    ) -> BlockPtr {
        let body = BlockImpl {
            source_id: source_id,
            fork_id: fork_id,
            source_public_key_hash: source_public_key_hash,
            height: height,
            hash: hash,
            prev: prev_block,
            payload: payload,
            block_deps: block_deps,
            forks_dep_heights: forks_dep_heights,
            extra_id: extra_id,
        };

        Arc::new(body)
    }

    pub(crate) fn create_from_string_dump(dump: &str, extra_id: BlockExtraId) -> BlockPtr {
        let mut body = BlockImpl {
            source_id: 0,
            fork_id: 0,
            source_public_key_hash: parse_hex_as_public_key_hash(
                "0000000000000000000000000000000000000000000000000000000000000000",
            ),
            height: 0,
            hash: parse_hex_as_int256(
                "0000000000000000000000000000000000000000000000000000000000000000",
            ),
            prev: None,
            payload: parse_hex_as_bytes(""),
            block_deps: [].to_vec(),
            forks_dep_heights: [].to_vec(),
            extra_id: extra_id,
        };

        for line in dump.lines() {
            let v: Vec<&str> = line.split('=').collect();
            if v.len() == 2 {
                let id = v.get(0).unwrap().trim();
                let value = v.get(1).unwrap().trim();

                if id == "src" {
                    body.source_id = FromStr::from_str(value).unwrap();
                }
                if id == "fork" {
                    body.fork_id = FromStr::from_str(value).unwrap();
                }
                if id == "height" {
                    body.height = FromStr::from_str(value).unwrap();
                }
                if id == "hash" {
                    body.hash = parse_hex_as_int256(value);
                }
                if id == "src_hash" {
                    body.source_public_key_hash = parse_hex_as_public_key_hash(value);
                }
                if id == "payload" {
                    body.payload = parse_hex_as_bytes(value);
                }
            }
        }

        Arc::new(body)
    }
}
