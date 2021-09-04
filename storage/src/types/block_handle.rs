use crate::{block_handle_db::BlockHandleCache, traits::Serializable, types::BlockMeta};
use std::{io::Write, sync::Arc};
use ton_block::BlockIdExt;
use ton_types::{fail, Result};
#[cfg(feature = "telemetry")]
use std::sync::atomic::{AtomicBool, Ordering};

const FLAG_DATA: u32                 = 0x00000001;
const FLAG_PROOF: u32                = 0x00000002;
const FLAG_PROOF_LINK: u32           = 0x00000004;
//const FLAG_EXT_DB: u32               = 1 << 3;
const FLAG_STATE: u32                = 0x00000010;
const FLAG_PERSISTENT_STATE: u32     = 0x00000020;
const FLAG_NEXT_1: u32               = 0x00000040;
const FLAG_NEXT_2: u32               = 0x00000080;
const FLAG_PREV_1: u32               = 0x00000100;
const FLAG_PREV_2: u32               = 0x00000200;
const FLAG_APPLIED: u32              = 0x00000400;
pub(crate) const FLAG_KEY_BLOCK: u32 = 0x00000800;
const FLAG_MOVED_TO_ARCHIVE: u32     = 0x00002000;
const FLAG_INDEXED: u32              = 0x00004000;

// not serializing flags
const FLAG_ARCHIVING: u32            = 0x00010000;

/// Meta information related to block
#[derive(Debug)]
pub struct BlockHandle {
    id: BlockIdExt,
    meta: BlockMeta,
    block_file_lock: tokio::sync::RwLock<()>,
    proof_file_lock: tokio::sync::RwLock<()>,
    block_handle_cache: Arc<BlockHandleCache>,
    #[cfg(feature = "telemetry")]
    got_by_broadcast: AtomicBool,
}

impl BlockHandle {

    pub fn new(id: BlockIdExt, block_handle_cache: Arc<BlockHandleCache>) -> Self {
        Self::with_values(id, BlockMeta::default(), block_handle_cache)
    }

    pub fn with_values(id: BlockIdExt, meta: BlockMeta, block_handle_cache: Arc<BlockHandleCache>) -> Self {
        Self {
            id,
            meta,
            block_file_lock: tokio::sync::RwLock::new(()),
            proof_file_lock: tokio::sync::RwLock::new(()),
            block_handle_cache,
            #[cfg(feature = "telemetry")]
            got_by_broadcast: AtomicBool::new(false),
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
            fail!(
                "INTERNAL ERROR: set different masterchain ref seqno for block {}: {} -> {}",
                self.id,
                prev,
                masterchain_ref_seq_no
            )
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

    pub fn set_moving_to_archive(&self) -> bool {
        self.set_flag(FLAG_ARCHIVING)
    }

    pub fn block_file_lock(&self) -> &tokio::sync::RwLock<()>  {
        &self.block_file_lock
    }

    pub fn proof_file_lock(&self) -> &tokio::sync::RwLock<()>  {
        &self.proof_file_lock
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

#[cfg(feature = "telemetry")]
impl BlockHandle {
    pub fn set_got_by_broadcast(&self, value: bool) {
        self.got_by_broadcast.store(value, Ordering::Relaxed);
    }

    pub fn got_by_broadcast(&self) -> bool {
        self.got_by_broadcast.load(Ordering::Relaxed)
    }
}

impl Drop for BlockHandle {
    fn drop(&mut self) {
        self.block_handle_cache.remove_with(&self.id, |(_id, weak)| {
            weak.strong_count() == 0
        });
    }
}

