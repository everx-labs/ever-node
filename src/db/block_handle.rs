use std::io::Write;
use std::sync::atomic::Ordering;

use ton_block::{BlockIdExt, BlockInfo};
use ton_node_storage::traits::Serializable;
use ton_node_storage::types::BlockMeta;
use ton_types::{fail, Result};

use crate::block::{BlockIdExtExtention, BlockStuff};
use crate::block_proof::BlockProofStuff;
use crate::db::BlockHandleCache;

const FLAG_DATA: u32 = 1;
const FLAG_PROOF: u32 = 1 << 1;
const FLAG_PROOF_LINK: u32 = 1 << 2;
const FLAG_EXT_DB: u32 = 1 << 3;
const FLAG_STATE: u32 = 1 << 4;
const FLAG_PERSISTENT_STATE: u32 = 1 << 5;
const FLAG_NEXT_1: u32 = 1 << 6;
const FLAG_NEXT_2: u32 = 1 << 7;
const FLAG_PREV_1: u32 = 1 << 8;
const FLAG_PREV_2: u32 = 1 << 9;
const FLAG_APPLIED: u32 = 1 << 10;
const FLAG_KEY_BLOCK: u32 = 1 << 11;
const FLAG_MOVING_TO_ARCHIVE: u32 = 1 << 12;
const FLAG_MOVED_TO_ARCHIVE: u32 = 1 << 13;

/// Meta information related to block
#[derive(Debug)]
pub struct BlockHandle {
    id: BlockIdExt,
    meta: BlockMeta,
    block_handle_cache: BlockHandleCache,
}

impl BlockHandle {
    pub(super) fn new(id: BlockIdExt, block_handle_cache: BlockHandleCache) -> Self {
        Self::with_values(id, BlockMeta::default(), block_handle_cache)
    }

    pub(super) const fn with_values(id: BlockIdExt, meta: BlockMeta, block_handle_cache: BlockHandleCache) -> Self {
        Self { id, meta, block_handle_cache }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.meta.serialize(writer)
    }

    // This flags might be set into true only. So flush only after transform false -> true.

    fn fetch_info(&self, info: &BlockInfo) -> Result<()> {
        self.meta.gen_utime().store(info.gen_utime().0, Ordering::SeqCst);
        let masterchain_ref_seq_no = if info.shard().is_masterchain() {
            info.seq_no()
        } else {
            info.read_master_id()?.seq_no
        };
        self.meta.masterchain_ref_seq_no().store(masterchain_ref_seq_no, Ordering::SeqCst);
        if info.key_block() {
            self.set_flag(FLAG_KEY_BLOCK);
        }
        self.meta.set_fetched();
        Ok(())
    }

    pub(super) fn fetch_block_info(&self, block: &BlockStuff) -> Result<()> {
        self.fetch_info(&block.block().read_info()?)
    }

    pub(super) fn fetch_proof_info(&self, proof: &BlockProofStuff) -> Result<()> {
        self.fetch_info(&proof.virtualize_block()?.0.read_info()?)
    }

    // TODO: Give correct name due to actual meaning (not "inited", but "saved" or "stored")
    pub(super) fn set_data_inited(&self) -> bool {
        self.set_flag(FLAG_DATA)
    }

    // TODO: Give correct name due to actual meaning (not "inited", but "saved" or "stored")
    pub(super) fn set_proof_inited(&self) -> bool {
        self.set_flag(FLAG_PROOF)
    }

    // TODO: Give correct name due to actual meaning (not "inited", but "saved" or "stored")
    pub(super) fn set_proof_link_inited(&self) -> bool {
        self.set_flag(FLAG_PROOF_LINK)
    }

    pub(super) fn set_processed_in_ext_db(&self) -> bool {
        self.set_flag(FLAG_EXT_DB)
    }

    pub(super) fn set_state_inited(&self) -> bool {
        self.set_flag(FLAG_STATE)
    }

    pub(super) fn set_persistent_state_inited(&self) -> bool {
        self.set_flag(FLAG_PERSISTENT_STATE)
    }

    pub(super) fn set_next1_inited(&self) -> bool {
        self.set_flag(FLAG_NEXT_1)
    }

    pub(super) fn set_next2_inited(&self) -> bool {
        self.set_flag(FLAG_NEXT_2)
    }

    pub(super) fn set_prev1_inited(&self) -> bool {
        self.set_flag(FLAG_PREV_1)
    }

    pub(super) fn set_prev2_inited(&self) -> bool {
        self.set_flag(FLAG_PREV_2)
    }

    pub(super) fn set_applied(&self) -> bool {
        self.set_flag(FLAG_APPLIED)
    }

    pub fn id(&self) -> &BlockIdExt {
        &self.id
    }

    pub fn meta(&self) -> &BlockMeta {
        &self.meta
    }

    // TODO: Give correct name due to actual meaning (not "inited", but "saved" or "stored")
    pub fn data_inited(&self) -> bool {
        self.flag(FLAG_DATA)
    }

    // TODO: Give correct name due to actual meaning (not "inited", but "saved" or "stored")
    pub fn proof_inited(&self) -> bool {
        if cfg!(feature = "local_test") {
            true
        } else {
            self.flag(FLAG_PROOF)
        }
    }

    // TODO: Give correct name due to actual meaning (not "inited", but "saved" or "stored")
    pub fn proof_link_inited(&self) -> bool {
        if cfg!(feature = "local_test") {
            true
        } else {
            self.flag(FLAG_PROOF_LINK)
        }
    }

    // TODO: Give correct name due to actual meaning (not "inited", but "saved" or "stored")
    pub fn proof_or_link_inited(&self, is_link: &mut bool) -> bool {
        *is_link = self.id.shard().is_masterchain();
        if *is_link {
            self.proof_link_inited()
        } else {
            self.proof_inited()
        }
    }

    pub fn processed_in_ext_db(&self) -> bool {
        self.flag(FLAG_EXT_DB)
    }

    pub fn state_inited(&self) -> bool {
        self.flag(FLAG_STATE)
    }

    pub fn persistent_state_inited(&self) -> bool {
        self.flag(FLAG_PERSISTENT_STATE)
    }

    pub fn next1_inited(&self) -> bool {
        self.flag(FLAG_NEXT_1)
    }

    pub fn next2_inited(&self) -> bool {
        self.flag(FLAG_NEXT_2)
    }

    pub fn prev1_inited(&self) -> bool {
        self.flag(FLAG_PREV_1)
    }

    pub fn prev2_inited(&self) -> bool {
        self.flag(FLAG_PREV_2)
    }

    pub fn applied(&self) -> bool {
        self.flag(FLAG_APPLIED)
    }

    pub fn gen_utime(&self) -> Result<u32> {
        if self.fetched() || self.state_inited() {
            Ok(self.meta.gen_utime().load(Ordering::Relaxed))
        } else {
            fail!("Data is not inited yet")
        }
    }

    pub(super) fn set_gen_utime(&self, time: u32) -> Result<()> {
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

    pub fn masterchain_ref_seq_no(&self) -> Result<u32> {
        if self.id.is_masterchain() {
            return Ok(self.id.seq_no());
        }

        if !self.fetched() {
            fail!("Data is not inited yet")
        }

        Ok(self.meta.masterchain_ref_seq_no().load(Ordering::SeqCst))
    }

    pub fn stored(&self) -> bool {
        self.meta().handle_stored()
    }

    pub fn set_stored(&self) -> bool {
        self.meta().set_handle_stored()
    }

    pub fn moving_to_archive(&self) -> bool {
        self.flag(FLAG_MOVING_TO_ARCHIVE)
    }

    pub fn set_moving_to_archive(&self) -> bool {
        self.set_flag(FLAG_MOVING_TO_ARCHIVE)
    }

    pub fn reset_moving_to_archive(&self) -> bool {
        self.reset_flag(FLAG_MOVING_TO_ARCHIVE)
    }

    pub fn moved_to_archive(&self) -> bool {
        self.flag(FLAG_MOVED_TO_ARCHIVE)
    }

    pub fn set_moved_to_archive(&self) -> bool {
        self.set_flag(FLAG_MOVED_TO_ARCHIVE)
    }

    pub fn fetched(&self) -> bool {
        self.meta().fetched()
    }

    pub fn is_key_block(&self) -> Result<bool> {
        if self.fetched() {
            Ok(self.flag(FLAG_KEY_BLOCK))
        } else {
            fail!("Data is not inited yet")
        }
    }

    #[inline]
    fn flags(&self) -> u32 {
        self.meta.flags().load(Ordering::SeqCst)
    }

    #[inline]
    fn flag(&self, flag: u32) -> bool {
        self.flags() & flag != 0
    }

    #[inline]
    fn set_flag(&self, flag: u32) -> bool {
        self.meta.flags().fetch_or(flag, Ordering::SeqCst) & flag == flag
    }

    #[inline]
    fn reset_flag(&self, flag: u32) -> bool {
        self.meta.flags().fetch_and(!flag, Ordering::SeqCst) & flag == flag
    }
}

impl Drop for BlockHandle {
    fn drop(&mut self) {
        self.block_handle_cache.remove(self.id());
    }
}

