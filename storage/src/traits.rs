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

use std::io::{Cursor, Read, Write};

use ton_block::{BlockIdExt, ShardIdent};
use ton_types::{ByteOrderRead, Result, UInt256};

pub trait Serializable {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()>;

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self> where Self: Sized;

    fn from_slice(data: &[u8]) -> Result<Self> where Self: Sized {
        Self::deserialize(&mut Cursor::new(data))
    }

    fn to_vec(&self) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        self.serialize(&mut result)?;

        Ok(result)
    }
}

impl Serializable for ShardIdent {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.workchain_id().to_le_bytes())?;
        writer.write_all(&self.shard_prefix_with_tag().to_le_bytes())?;

        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
        let workchain_id = reader.read_le_u32()? as i32;
        let shard_prefix_tagged = reader.read_le_u64()?;

        Self::with_tagged_prefix(workchain_id, shard_prefix_tagged)
    }
}

impl Serializable for BlockIdExt {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.shard_id.serialize(writer)?;
        writer.write_all(&self.seq_no().to_le_bytes())?;
        writer.write_all(self.root_hash().as_ref())?;
        writer.write_all(self.file_hash().as_ref())?;

        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self> where Self: Sized {
        let shard_id = ShardIdent::deserialize(reader)?;
        let seq_no = reader.read_le_u32()?;
        let root_hash = UInt256::from(reader.read_u256()?);
        let file_hash = UInt256::from(reader.read_u256()?);

        Ok(Self::with_params(shard_id, seq_no, root_hash, file_hash))
    }
}

impl Serializable for u32 {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        Ok(writer.write_all(&self.to_le_bytes())?)
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self> where Self: Sized {
        Ok(reader.read_le_u32()?)
    }
}

impl Serializable for u64 {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        Ok(writer.write_all(&self.to_le_bytes())?)
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self> where Self: Sized {
        Ok(reader.read_le_u64()?)
    }
}

impl Serializable for bool {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        Ok(writer.write_all(&[*self as u8])?)
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self> where Self: Sized {
        Ok(reader.read_byte()? != 0)
    }
}
