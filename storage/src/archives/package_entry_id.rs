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

use std::borrow::Borrow;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use ton_block::{BlockIdExt, ShardIdent};
use ton_types::{error, fail, base64_encode, Result, UInt256};

#[derive(Debug, Hash, PartialEq, Eq)]
pub enum PackageEntryId<B, U256, PK>
where
    B: Borrow<BlockIdExt> + Hash,
    U256: Borrow<UInt256> + Hash,
    PK: Borrow<UInt256> + Hash
{
    Empty,
    Block(B),
    ZeroState(B),
    PersistentState { mc_block_id: B, block_id: B },
    Proof(B),
    ProofLink(B),
    Signatures(B),
    Candidate { block_id: B, collated_data_hash: U256, source: PK },
    BlockInfo(B),
}

impl PackageEntryId<BlockIdExt, UInt256, UInt256> {

    pub fn from_filename(filename: &str) -> Result<Self> {

        if filename == PackageEntryId::<BlockIdExt, UInt256, UInt256>::Empty.filename_prefix() {
            return Ok(PackageEntryId::Empty);
        }

        let dummy = BlockIdExt::default();

        if let Some(mut block_ids) = Self::parse_block_ids(
            filename,
            PackageEntryId::Block(&dummy),
            1
        )? {
            return Ok(PackageEntryId::Block(block_ids.remove(0)));
        }

        if let Some(mut block_ids) = Self::parse_block_ids(
            filename,
            PackageEntryId::ZeroState(&dummy),
            1
        )? {
            return Ok(PackageEntryId::ZeroState(block_ids.remove(0)));
        }

        if let Some(mut block_ids) = Self::parse_block_ids(
            filename,
            PackageEntryId::Proof(&dummy),
            1
        )? {
            return Ok(PackageEntryId::Proof(block_ids.remove(0)));
        }

        if let Some(mut block_ids) = Self::parse_block_ids(
            filename,
            PackageEntryId::ProofLink(&dummy),
            1)?
        {
            return Ok(PackageEntryId::ProofLink(block_ids.remove(0)));
        }

        if let Some(mut block_ids) = Self::parse_block_ids(
            filename,
            PackageEntryId::Signatures(&dummy),
            1
        )? {
            return Ok(PackageEntryId::Signatures(block_ids.remove(0)));
        }

        if let Some(mut block_ids) = Self::parse_block_ids(
            filename,
            PackageEntryId::BlockInfo(&dummy),
            1
        )? {
            return Ok(PackageEntryId::BlockInfo(block_ids.remove(0)));
        }

        if let Some(mut block_ids) = Self::parse_block_ids(
            filename,
            PackageEntryId::PersistentState {
                mc_block_id: &dummy,
                block_id: &dummy,
            },
            2
        )? {
            return Ok(PackageEntryId::PersistentState {
                mc_block_id: block_ids.remove(0),
                block_id: block_ids.remove(0),
            });
        }

        if filename.starts_with(
            PackageEntryId::<&BlockIdExt, UInt256, UInt256>::Candidate {
                block_id: &dummy,
                collated_data_hash: UInt256::default(),
                source: UInt256::default()
            }.filename_prefix()
        ) {
            fail!("Unsupported from_filename() for PackageEntryId::Candidate");
        }

        fail!("Cannot parse filename: {}", filename)
    
    }

    fn parse_block_ids(filename: &str, dummy: PackageEntryId<&BlockIdExt, UInt256, UInt256>, count: usize) -> Result<Option<Vec<BlockIdExt>>> {
        let prefix = dummy.filename_prefix();
        if !filename.starts_with(&(prefix.to_string() + "_")) {
            return Ok(None);
        }

        let mut result = Vec::new();
        let mut pos = prefix.len() + 1;
        for _ in 0..count {
            let (block_id, len) = parse_block_id(&filename[pos..filename.len()])?;
            result.push(block_id);
            pos += len + 1;
        }

        Ok(Some(result))
    }
}

impl<B, U256, PK> PackageEntryId<B, U256, PK>
where
    B: Borrow<BlockIdExt> + Hash,
    U256: Borrow<UInt256> + Hash,
    PK: Borrow<UInt256> + Hash
{
    fn filename_prefix(&self) -> &'static str {
        match self {
            PackageEntryId::Empty => "empty",
            PackageEntryId::Block(_) => "block",
            PackageEntryId::ZeroState(_) => "zerostate",
            PackageEntryId::PersistentState { mc_block_id: _, block_id: _ } => "state",
            PackageEntryId::Proof(_) => "proof",
            PackageEntryId::ProofLink(_) => "prooflink",
            PackageEntryId::Signatures(_) => "signatures",
            PackageEntryId::Candidate { block_id: _, collated_data_hash: _, source: _ } => "candidate",
            PackageEntryId::BlockInfo(_) => "info",
        }
    }
}

pub trait GetFileName {
    fn filename(&self) -> String;
}

pub trait FromFileName {
    fn from_filename(filename: &str) -> Result<Self> where Self: Sized;
}

impl GetFileName for BlockIdExt {
    fn filename(&self) -> String {
        format!("({wc_id},{shard_id:016x},{seq_no}):{root_hash:064X}:{file_hash:064X}",
                wc_id = self.shard().workchain_id(),
                shard_id = self.shard().shard_prefix_with_tag(),
                seq_no = self.seq_no(),
                root_hash = self.root_hash(),
                file_hash = self.file_hash(),
        )
    }
}

// parse block ID: (wc,shard,seqno):rh:fh, for example (-1,F800000000000000,10):19..68:5C..DC
fn parse_block_id(filename: &str) -> Result<(BlockIdExt, usize)> {
    fn find_id(ids: &str, upto: char) -> Result<usize> {
        let ret = ids.find(upto).ok_or_else(|| error!(""))?;
        if ids.len() == ret + 1 {
            fail!("too short")
        }
        Ok(ret)
    }
    fn parse_ids(ids: &str) -> Result<(BlockIdExt, usize)> {
        if find_id(ids, '(')? != 0 {
            fail!("bad format")
        }
        let pos_a = find_id(&ids[1..], ',')? + 1;
        let pos_b = find_id(&ids[pos_a + 1..], ',')? + pos_a + 1;
        let pos_c = find_id(&ids[pos_b + 1..], ')')? + pos_b + 1;
        if find_id(&ids[pos_c + 1..], ':')? != 0 {
            fail!("bad format")
        }
        let pos_d = find_id(&ids[pos_c + 2..], ':')? + pos_c + 2;
        if ids.len() <= pos_d + 64 {
            fail!("bad format")
        }
        let id = BlockIdExt {
            shard_id: ShardIdent::with_tagged_prefix(
                i32::from_str(&ids[1..pos_a])?,
                u64::from_str_radix(&ids[pos_a + 1..pos_b], 16)?
            )?,
            seq_no: u32::from_str(&ids[pos_b + 1..pos_c])?,
            root_hash: UInt256::from_str(&ids[pos_c + 2..pos_d])?,
            file_hash: UInt256::from_str(&ids[pos_d + 1..pos_d + 65])?
        };
        Ok((id, pos_d + 65))
    }
    parse_ids(filename).map_err(|e| error!("Invalid block ID {}: {}", filename, e))
}

/// parse file name for example block_555_F800000000000000_100_19685CDC8B64BBB5
pub fn parse_short_filename(filename: &str) -> Result<(i32, u64, u32)> {
    enum Id { 
        Wc = 1, 
        Shard, 
        SeqNo, 
        Count = 5
    }
    fn parse_ids(ids: &Vec<&str>) -> Result<(i32, u64, u32)> {
        if ids.len() != Id::Count as usize {
            fail!("too short")
        } else {
            let ret = (
                i32::from_str(&ids[Id::Wc as usize])?,
                u64::from_str_radix(&ids[Id::Shard as usize], 16)?,
                u32::from_str(&ids[Id::SeqNo as usize])?
            );
            Ok(ret)
        }
    }
    parse_ids(&filename.split('_').collect()).map_err(
        |e| error!("Invalid block file name {}: {}", filename, e)
    )
}

impl FromFileName for BlockIdExt {
    fn from_filename(filename: &str) -> Result<Self> {
        parse_block_id(filename)
            .map(|(block_id, _len)| block_id)
    }
}

impl GetFileName for UInt256 {
    fn filename(&self) -> String {
        base64_encode(self)
    }
}

impl<B, U256, PK> GetFileName for PackageEntryId<B, U256, PK>
where
    B: Borrow<BlockIdExt> + Hash,
    U256: Borrow<UInt256> + Hash,
    PK: Borrow<UInt256> + Hash
{
    fn filename(&self) -> String {
        match self {
            PackageEntryId::Empty => self.filename_prefix().to_string(),

            PackageEntryId::Block(block_id) |
            PackageEntryId::ZeroState(block_id) |
            PackageEntryId::Proof(block_id) |
            PackageEntryId::ProofLink(block_id) |
            PackageEntryId::Signatures(block_id) |
            PackageEntryId::BlockInfo(block_id) => 
                format!("{}_{}", self.filename_prefix(), block_id.borrow().filename()),

            PackageEntryId::PersistentState { mc_block_id, block_id } =>
                format!("{}_{}_{}",
                    self.filename_prefix(),
                    mc_block_id.borrow().filename(),
                    block_id.borrow().filename()
                ),

            PackageEntryId::Candidate { block_id, collated_data_hash, source } =>
                format!("{}_{}_{:X}_{}",
                    self.filename_prefix(),
                    block_id.borrow().filename(),
                    collated_data_hash.borrow(),
                    source.borrow().filename()
                ),

        }
    }
}

pub trait GetFileNameShort {
    fn filename_short(&self) -> String;
}

impl GetFileNameShort for BlockIdExt {
    fn filename_short(&self) -> String {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        format!("{wc_id}_{shard_id:016X}_{seq_no}_{hash:016X}",
            wc_id = self.shard().workchain_id(),
            shard_id = self.shard().shard_prefix_with_tag(),
            seq_no = self.seq_no(),
            hash = hasher.finish()
        )
    }
}

impl<B, U256, PK> GetFileNameShort for PackageEntryId<B, U256, PK>
where
    B: Borrow<BlockIdExt> + Hash,
    U256: Borrow<UInt256> + Hash,
    PK: Borrow<UInt256> + Hash
{
    fn filename_short(&self) -> String {
        match self {
            PackageEntryId::Empty => self.filename_prefix().to_string(),

            PackageEntryId::Block(block_id) |
            PackageEntryId::ZeroState(block_id) |
            PackageEntryId::Proof(block_id) |
            PackageEntryId::ProofLink(block_id) |
            PackageEntryId::Signatures(block_id) |
            PackageEntryId::BlockInfo(block_id) => 
                format!("{}_{}", self.filename_prefix(), block_id.borrow().filename_short()),

            PackageEntryId::PersistentState { mc_block_id, block_id } =>
                format!("{}_{}_{}",
                    self.filename_prefix(),
                    mc_block_id.borrow().filename_short(),
                    block_id.borrow().filename_short()
                ),

            PackageEntryId::Candidate { block_id, collated_data_hash, source } =>
                format!("{}_{}_{:X}_{}",
                    self.filename_prefix(),
                    block_id.borrow().filename_short(),
                    collated_data_hash.borrow(),
                    source.borrow().filename()
                ),

        }
    }
}

impl<B, U256, PK> Display for PackageEntryId<B, U256, PK>
where
    B: Borrow<BlockIdExt> + Hash,
    U256: Borrow<UInt256> + Hash,
    PK: Borrow<UInt256> + Hash
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.filename().as_str())
    }
}
