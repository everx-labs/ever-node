use crate::{traits::Serializable, types::block_handle};
use std::{io::{Read, Write}, sync::atomic::{AtomicU64, Ordering}};
use ton_block::Block;
use ton_types::{ByteOrderRead, Result};

#[derive(Debug, Default)]
pub struct BlockMeta {
    flags: AtomicU64,
    pub gen_utime: u32,
    pub gen_lt: u64,
//    gen_utime: AtomicU32,
//    gen_lt: AtomicU64,
//    masterchain_ref_seq_no: AtomicU32,
//    fetched: AtomicBool,
}

impl BlockMeta {

    pub fn from_block(block: &Block) -> Result<Self> {
        let info = block.read_info()?;
        let flags = if info.key_block() {
            block_handle::FLAG_KEY_BLOCK
        } else {
            0
        };
        Ok(Self::with_data(flags, info.gen_utime().0, info.start_lt(), 0/*, true*/))
    }

    pub fn with_data(
        flags: u32, 
        gen_utime: u32, 
        gen_lt: u64, 
        masterchain_ref_seq_no: u32
/*        fetched: bool */
    ) -> Self {
        Self {
            flags: AtomicU64::new(((flags as u64) << 32) | masterchain_ref_seq_no as u64),
            gen_utime,//: AtomicU32::new(gen_utime),
            gen_lt,
//            masterchain_ref_seq_no: AtomicU32::new(masterchain_ref_seq_no),
//            fetched: AtomicBool::new(fetched),
        }
    }

    pub fn flags(&self) -> u32 {
        (self.flags.load(Ordering::Relaxed) >> 32) as u32
    }

    pub fn set_flags(&self, flags: u32) -> u32 {
        (self.flags.fetch_or((flags as u64) << 32, Ordering::Relaxed) >> 32) as u32
    }

/*
    pub const fn gen_utime(&self) -> &AtomicU32 {
        &self.gen_utime
    }

    pub const fn gen_lt(&self) -> &AtomicU64 {
        &self.gen_lt
    }
*/

    pub fn masterchain_ref_seq_no(&self) -> u32 {
        self.flags.load(Ordering::Relaxed) as u32
    }

    pub fn set_masterchain_ref_seq_no(&self, masterchain_ref_seq_no: u32) -> u32 {
        self.flags.fetch_or(masterchain_ref_seq_no as u64, Ordering::Relaxed) as u32
    }

/*
    pub fn fetched(&self) -> bool {
        self.fetched.load(Ordering::SeqCst)
    }

    pub fn set_fetched(&self) -> bool {
        self.fetched.swap(true, Ordering::SeqCst)
    }
*/

}

impl Serializable for BlockMeta {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.flags.load(Ordering::SeqCst).to_le_bytes())?;
        writer.write_all(&self.gen_utime/*.load(Ordering::SeqCst)*/.to_le_bytes())?;
        writer.write_all(&self.gen_lt/*.load(Ordering::SeqCst)*/.to_le_bytes())?;
//        writer.write_all(&self.masterchain_ref_seq_no.load(Ordering::SeqCst).to_le_bytes())?;
//        writer.write_all(&[self.fetched() as u8])?;

        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {

        let flags = reader.read_le_u64()?;
        let gen_utime = reader.read_le_u32()?;
        let gen_lt = reader.read_le_u64()?;
        let masterchain_ref_seq_no = flags as u32;
        let flags = (flags >> 32) as u32;
//        let fetched = reader.read_byte()? != 0;
        let bm = Self::with_data(flags, gen_utime, gen_lt, masterchain_ref_seq_no /*fetched*/);


        Ok(bm)
    }

}
