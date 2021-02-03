use crate::{block_handle_db::BlockHandleCache, traits::Serializable, types::BlockMeta};
use std::{io::Write, sync::{Arc, atomic::{AtomicBool, Ordering}}};
use ton_block::BlockIdExt;
use ton_types::{fail, Result};

const FLAG_DATA: u32                 = 1;
const FLAG_PROOF: u32                = 1 << 1;
const FLAG_PROOF_LINK: u32           = 1 << 2;
//const FLAG_EXT_DB: u32               = 1 << 3;
const FLAG_STATE: u32                = 1 << 4;
const FLAG_PERSISTENT_STATE: u32     = 1 << 5;
const FLAG_NEXT_1: u32               = 1 << 6;
const FLAG_NEXT_2: u32               = 1 << 7;
const FLAG_PREV_1: u32               = 1 << 8;
const FLAG_PREV_2: u32               = 1 << 9;
const FLAG_APPLIED: u32              = 1 << 10;
pub(crate) const FLAG_KEY_BLOCK: u32 = 1 << 11;
const FLAG_MOVED_TO_ARCHIVE: u32     = 1 << 13;
const FLAG_INDEXED: u32              = 1 << 14;

/// Meta information related to block
#[derive(Debug)]
pub struct BlockHandle {
    id: BlockIdExt,
    meta: BlockMeta,
    moving_to_archive_started: AtomicBool,
    temp_lock: tokio::sync::RwLock<()>,
    block_handle_cache: Arc<BlockHandleCache>,
}

impl BlockHandle {

    pub fn new(id: BlockIdExt, block_handle_cache: Arc<BlockHandleCache>) -> Self {
        Self::with_values(id, BlockMeta::default(), block_handle_cache)
    }

    pub fn with_values(id: BlockIdExt, meta: BlockMeta, block_handle_cache: Arc<BlockHandleCache>) -> Self {
        Self {
            id,
            meta,
            moving_to_archive_started: AtomicBool::new(false),
            temp_lock: tokio::sync::RwLock::new(()),
            block_handle_cache
        }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.meta.serialize(writer)
    }

/*
    pub fn fetch_shard_state(&self, ss: &ShardStateUnsplit) -> Result<()> {
        self.meta.gen_utime().store(ss.gen_time(), Ordering::SeqCst);
        if ss.read_custom()?.map(|c| c.after_key_block).unwrap_or(false) {
            self.set_flags(FLAG_KEY_BLOCK);
        }
        self.meta.set_fetched();
        Ok(())
    }
*/

/*
    fn fetch_info(&self, info: &BlockInfo) -> Result<()> {
        self.meta.gen_utime().store(info.gen_utime().0, Ordering::SeqCst);
        if info.key_block() {
            self.set_flags(FLAG_KEY_BLOCK);
        }
        self.meta.set_fetched();
        Ok(())
    }
*/

/*
    pub fn fetch_block_info(&self, block: &Block) -> Result<()> {
        self.fetch_info(&block.read_info()?)
    }
*/

    // This flags might be set into true only. So flush only after transform false -> true.

    pub fn set_data(&self) -> bool {
        self.set_flag(FLAG_DATA)
    }

    pub fn set_proof(&self) -> bool {
        self.set_flag(FLAG_PROOF)
    }

    pub fn set_proof_link(&self) -> bool {
        self.set_flag(FLAG_PROOF_LINK)
    }

/*
    pub fn set_processed_in_ext_db(&self) -> bool {
        self.set_flag(FLAG_EXT_DB)
    }
*/

    pub fn set_state(&self) -> bool {
        self.set_flag(FLAG_STATE)
    }

    pub fn set_persistent_state(&self) -> bool {
        self.set_flag(FLAG_PERSISTENT_STATE)
    }

    pub fn set_next1(&self) -> bool {
        self.set_flag(FLAG_NEXT_1)
    }

    pub fn set_next2(&self) -> bool {
        self.set_flag(FLAG_NEXT_2)
    }

    pub fn set_prev1(&self) -> bool {
        self.set_flag(FLAG_PREV_1)
    }

    pub fn set_prev2(&self) -> bool {
        self.set_flag(FLAG_PREV_2)
    }

    pub fn set_block_applied(&self) -> bool {
        self.set_flag(FLAG_APPLIED)
    }

    pub fn id(&self) -> &BlockIdExt {
        &self.id
    }

    pub fn meta(&self) -> &BlockMeta {
        &self.meta
    }

    pub fn set_block_indexed(&self) -> bool {
        self.set_flag(FLAG_INDEXED)
    }

    pub fn has_data(&self) -> bool {
        self.is_flag_set(FLAG_DATA)
    }

    pub fn has_proof(&self) -> bool {
        if cfg!(feature = "local_test") {
            true
        } else {
            self.is_flag_set(FLAG_PROOF)
        }
    }

    pub fn has_proof_link(&self) -> bool {
        if cfg!(feature = "local_test") {
            true
        } else {
            self.is_flag_set(FLAG_PROOF_LINK)
        }
    }

    pub fn has_proof_or_link(&self, is_link: &mut bool) -> bool {
        *is_link = !self.id.shard().is_masterchain();
        if *is_link {
            self.has_proof_link()
        } else {
            self.has_proof()
        }
    }

/*
    pub fn is_processed_in_ext_db(&self) -> bool {
        self.is_flag_set(FLAG_EXT_DB)
    }
*/

    pub fn has_state(&self) -> bool {
        self.is_flag_set(FLAG_STATE)
    }

    pub fn has_persistent_state(&self) -> bool {
        self.is_flag_set(FLAG_PERSISTENT_STATE)
    }

    pub fn has_next1(&self) -> bool {
        self.is_flag_set(FLAG_NEXT_1)
    }

    pub fn has_next2(&self) -> bool {
        self.is_flag_set(FLAG_NEXT_2)
    }

    pub fn has_prev1(&self) -> bool {
        self.is_flag_set(FLAG_PREV_1)
    }

    pub fn has_prev2(&self) -> bool {
        self.is_flag_set(FLAG_PREV_2)
    }

    pub fn is_applied(&self) -> bool {
        self.is_flag_set(FLAG_APPLIED)
    }

    pub fn is_indexed(&self) -> bool {
        self.is_flag_set(FLAG_INDEXED)
    }

    pub fn gen_lt(&self) -> u64 {
        self.meta.gen_lt
    }

    pub fn gen_utime(&self) -> Result<u32> {
//        if self.fetched() || self.state_inited() {
            Ok(self.meta.gen_utime)
//        } else {
//            fail!("Data is not inited yet")
//        }
    }

/*
    pub fn set_gen_utime(&self, time: u32) -> Result<()> {
        if self.fetched() || self.state_inited() {
            if time != self.meta.gen_utime().load(Ordering::Relaxed) {
                fail!("gen_utime was already set with another value")
            } else {
                Ok(())
            }
        } else {
            self.meta.gen_utime().store(time, Ordering::SeqCst);
            Ok(())
        }
    }
*/

    pub fn masterchain_ref_seq_no(&self) -> u32 {
        if self.id.shard().is_masterchain() {
            self.id.seq_no()
        } else {
            self.meta.masterchain_ref_seq_no()
        }
    }

    pub fn set_masterchain_ref_seq_no(&self, masterchain_ref_seq_no: u32) -> Result<bool> {
        let prev = self.meta.set_masterchain_ref_seq_no(masterchain_ref_seq_no);
        if prev == 0 {
            Ok(true)
        } else if prev == masterchain_ref_seq_no {
            Ok(false)
        } else {
            fail!("INTERNAL ERROR: set different masterchain ref seqno")
        }
    }

    pub fn is_archived(&self) -> bool {
        self.is_flag_set(FLAG_MOVED_TO_ARCHIVE)
    }

    pub fn set_archived(&self) -> bool {
        self.set_flag(FLAG_MOVED_TO_ARCHIVE)
    }

/*
    pub fn fetched(&self) -> bool {
        self.meta().fetched()
    }
*/

    pub fn is_key_block(&self) -> Result<bool> {
//        if self.fetched() {
            Ok(self.is_flag_set(FLAG_KEY_BLOCK))
/*
        } else {
            fail!("Data is not inited yet")
        }
*/
    }

    pub fn start_moving_to_archive(&self) -> bool {
        self.moving_to_archive_started.swap(true, Ordering::SeqCst)
    }

    pub(crate) fn temp_lock(&self) -> &tokio::sync::RwLock<()>  {
        &self.temp_lock
    }

//    #[inline]
//    fn flags(&self) -> u32 {
//        self.meta.flags()
//    }

    #[inline]
    fn is_flag_set(&self, flag: u32) -> bool {
        (self.meta.flags() & flag) == flag
    }

    #[inline]
    fn set_flag(&self, flag: u32) -> bool {
        (self.meta.set_flags(flag) & flag) != flag
    }

}

impl Drop for BlockHandle {
    fn drop(&mut self) {
        self.block_handle_cache.remove_with(&self.id, |(_id, weak)| {
            weak.strong_count() == 0
        });
    }
}

