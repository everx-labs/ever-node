use std::sync::atomic::Ordering;

use ton_block::{BlockIdExt, BlockInfo};
use ton_node_storage::types::BlockMeta;
use ton_types::{fail, Result};

use crate::block::BlockStuff;
use crate::block_proof::BlockProofStuff;

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

/// Meta information related to block
pub struct BlockHandle {
    id: BlockIdExt,
    meta: BlockMeta,
}

impl BlockHandle {
    pub fn new(id: &BlockIdExt) -> Self {
        Self::with_values(id.clone(), BlockMeta::default())
    }

    pub fn with_data(id: &BlockIdExt, data: &[u8]) -> Result<Self> {
        Ok(Self::with_values(id.clone(), BlockMeta::deserialize(data)?))
    }

    fn with_values(id: BlockIdExt, meta: BlockMeta) -> Self {
        Self { id, meta }
    }

    pub fn serialize(&self) -> Vec<u8> {
        self.meta.serialize()
    }

    // This flags might be set into true only. So flush only after transform false -> true.

    fn fetch_info(&self, info: &BlockInfo) -> Result<()> {
        self.meta.gen_utime().store(info.gen_utime().0, Ordering::SeqCst);
        if info.key_block() {
            self.set_flag(FLAG_KEY_BLOCK);
        }
        Ok(())
    }

    pub(super) fn set_data_inited(&self, block: &BlockStuff) -> Result<()> {
        self.fetch_info(&block.block().read_info()?)?;
        self.set_flag(FLAG_DATA);
        Ok(())
    }

    pub(super) fn set_proof_inited(&self, proof: &BlockProofStuff) -> Result<()> {
        self.fetch_info(&proof.virtualize_block()?.0.read_info()?)?;
        self.set_flag(FLAG_PROOF);
        Ok(())
    }

    pub(super) fn set_proof_link_inited(&self, proof: &BlockProofStuff) -> Result<()> {
        self.fetch_info(&proof.virtualize_block()?.0.read_info()?)?;
        self.set_flag(FLAG_PROOF);
        Ok(())
    }

    pub(super) fn set_processed_in_ext_db(&self) {
        self.set_flag(FLAG_EXT_DB);
    }

    pub(super) fn set_state_inited(&self) {
        self.set_flag(FLAG_STATE);
    }

    pub(super) fn set_persistent_state_inited(&self) {
        self.set_flag(FLAG_PERSISTENT_STATE);
    }

    pub(super) fn set_next1_inited(&self) {
        self.set_flag(FLAG_NEXT_1);
    }

    pub(super) fn set_next2_inited(&self) {
        self.set_flag(FLAG_NEXT_2);
    }

    pub(super) fn set_prev1_inited(&self) {
        self.set_flag(FLAG_PREV_1);
    }

    pub(super) fn set_prev2_inited(&self) {
        self.set_flag(FLAG_PREV_2);
    }

    pub(super) fn set_applied(&self) {
        self.set_flag(FLAG_APPLIED);
    }

    pub fn id(&self) -> &BlockIdExt {
        &self.id
    }

    pub fn data_inited(&self) -> bool {
        self.flag(FLAG_DATA)
    }

    pub fn proof_inited(&self) -> bool {
        if cfg!(feature = "local_test") {
            true
        } else {
            self.flag(FLAG_PROOF)
        }
    }

    pub fn proof_link_inited(&self) -> bool {
        if cfg!(feature = "local_test") {
            true
        } else {
            self.flag(FLAG_PROOF_LINK)
        }
    }

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
        if self.data_inited() || self.proof_inited() || self.proof_link_inited() || self.state_inited() {
            Ok(self.meta.gen_utime().load(Ordering::Relaxed))
        } else {
            fail!("Data is not inited yet")
        }
    }

    pub(super) fn set_gen_utime(&self, time: u32) -> Result<()> {
        if self.data_inited() || self.proof_inited() || self.proof_link_inited() || self.state_inited() {
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

    pub fn is_key_block(&self) -> Result<bool> {
        if self.data_inited() || self.proof_inited() || self.proof_link_inited() {
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
    fn set_flag(&self, flag: u32) {
        self.meta.flags().fetch_or(flag, Ordering::SeqCst);
    }
}