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

use crate::{block_handle_db, traits::Serializable};
use std::{io::{Read, Write}, sync::atomic::{AtomicU64, Ordering}};
use ton_block::Block;
use ton_types::{ByteOrderRead, Result};

#[derive(Debug, Default)]
pub struct BlockMeta {
    flags: AtomicU64,
    pub gen_utime: u32,
    pub gen_lt: u64,
}

impl BlockMeta {

    pub fn from_block(block: &Block) -> Result<Self> {
        let info = block.read_info()?;
        let flags = if info.key_block() {
            block_handle_db::FLAG_KEY_BLOCK
        } else {
            0
        };
        Ok(Self::with_data(flags, info.gen_utime().as_u32(), info.start_lt(), 0))
    }

    pub fn with_data(
        flags: u32, 
        gen_utime: u32, 
        gen_lt: u64, 
        masterchain_ref_seq_no: u32
    ) -> Self {
        Self {
            flags: AtomicU64::new(((flags as u64) << 32) | masterchain_ref_seq_no as u64),
            gen_utime,
            gen_lt,
        }
    }

    pub fn flags(&self) -> u32 {
        (self.flags.load(Ordering::Relaxed) >> 32) as u32
    }

    pub fn set_flags(&self, flags: u32) -> u32 {
        (self.flags.fetch_or((flags as u64) << 32, Ordering::Relaxed) >> 32) as u32
    }

    pub fn reset(&self, flags: u32, reset_mc_ref_seq_no: bool) {
        if reset_mc_ref_seq_no {
            self.flags.fetch_and((!flags as u64) << 32, Ordering::Relaxed);
        } else {
            self.flags.fetch_and(!((flags as u64) << 32), Ordering::Relaxed);

        }
    }

    pub fn masterchain_ref_seq_no(&self) -> u32 {
        self.flags.load(Ordering::Relaxed) as u32
    }

    pub fn set_masterchain_ref_seq_no(&self, masterchain_ref_seq_no: u32) -> u32 {
        self.flags.fetch_or(masterchain_ref_seq_no as u64, Ordering::Relaxed) as u32
    }

}

impl Serializable for BlockMeta {

    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        const FLAG_MASK: u64 = 0x0000_FFFF_FFFF_FFFF;
        let flags = self.flags.load(Ordering::Relaxed) & FLAG_MASK;
        writer.write_all(&flags.to_le_bytes())?;
        writer.write_all(&self.gen_utime.to_le_bytes())?;
        writer.write_all(&self.gen_lt.to_le_bytes())?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
        let flags = reader.read_le_u64()?;
        let gen_utime = reader.read_le_u32()?;
        let gen_lt = reader.read_le_u64()?;
        let masterchain_ref_seq_no = flags as u32;
        let flags = (flags >> 32) as u32;
        let bm = Self::with_data(flags, gen_utime, gen_lt, masterchain_ref_seq_no);
        Ok(bm)
    }

}
