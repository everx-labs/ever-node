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

use lazy_static::lazy_static;
use regex::Regex;

use ton_block::{BlockIdExt, ShardIdent};
use ton_types::{error, fail, Result, UInt256};


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

fn parse_block_id(filename: &str) -> Result<(BlockIdExt, usize)> {
    lazy_static! {
            static ref REGEX: Regex = Regex::new(r"^\((-?\d+),([0-9a-f]{16}),(\d+)\):([0-9A-F]{64}):([0-9A-F]{64})")
                .expect("Failed to compile regular expression");
        }

    let captures = REGEX.captures(filename)
        .ok_or_else(|| error!("Incorrect BlockIdExt format: {}", filename))?;

    enum Groups { Chain = 1, Shard, SeqNo, RootHash, FileHash }

    let workchain_id = i32::from_str(&captures[Groups::Chain as usize])?;
    let shard_prefix_tagged = u64::from_str_radix(&captures[Groups::Shard as usize], 16)?;
    let seq_no = u32::from_str(&captures[Groups::SeqNo as usize])?;
    let root_hash = UInt256::from_str(&captures[Groups::RootHash as usize])?;
    let file_hash = UInt256::from_str(&captures[Groups::FileHash as usize])?;

    let shard_id = ShardIdent::with_tagged_prefix(workchain_id, shard_prefix_tagged)?;

    Ok((BlockIdExt {
        shard_id,
        seq_no,
        root_hash,
        file_hash
    }, captures[0].len()))
}

impl FromFileName for BlockIdExt {
    fn from_filename(filename: &str) -> Result<Self> {
        parse_block_id(filename)
            .map(|(block_id, _len)| block_id)
    }
}

impl GetFileName for UInt256 {
    fn filename(&self) -> String {
        base64::encode(self)
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
            PackageEntryId::BlockInfo(block_id) => format!("{}_{}", self.filename_prefix(), block_id.borrow().filename()),

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
                hash = hasher.finish(),
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
            PackageEntryId::BlockInfo(block_id) => format!("{}_{}", self.filename_prefix(), block_id.borrow().filename_short()),

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
