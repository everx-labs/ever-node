use crate::{
    block::{BlockStuff}, engine::Engine, 
    shard_state::ShardStateStuff,
    network::node_network::FullNodeOverlayClient,
    block_proof::BlockProofStuff,
    db::BlockHandle,
};

use std::{io::Cursor, sync::Arc};
use ton_types::{deserialize_tree_of_cells, fail, Result};
use overlay::OverlayShortId;
use ton_block::{AccountIdPrefixFull, BlockIdExt, Deserializable, Message};

#[async_trait::async_trait]
pub trait GetOverlay : Sync + Send {
    async fn start(&self) -> Result<Arc<dyn FullNodeOverlayClient>>;
    async fn get_overlay(&self, overlay_id: &Arc<OverlayShortId>) -> Result<Arc<dyn FullNodeOverlayClient>>;
    fn calc_overlay_short_id(&self, workchain: i32, shard: u64) -> Result<Arc<OverlayShortId>>;
}

// TODO make separate traits for read and write operations (may be critical and not etc.)
#[async_trait::async_trait]
#[allow(unused)]
pub trait EngineOperations : Sync + Send {

    // Block related operations

    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Arc<BlockHandle>> {
        unimplemented!()
    }
    async fn load_applied_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        unimplemented!()
    }
    async fn wait_applied_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        unimplemented!()
    }
    async fn load_last_applied_mc_block(&self) -> Result<BlockStuff> {
        unimplemented!()
    }
    async fn load_last_applied_mc_block_id(&self) -> Result<BlockIdExt> {
        unimplemented!()
    }
    async fn find_block_by_seq_no(&self, acc_pfx: &AccountIdPrefixFull, seqno: u32) -> Result<Arc<BlockHandle>> {
        unimplemented!()
    }
    async fn find_block_by_unix_time(&self, acc_pfx: &AccountIdPrefixFull, utime: u32) -> Result<Arc<BlockHandle>> {
        unimplemented!()
    }
    async fn find_block_by_lt(&self, acc_pfx: &AccountIdPrefixFull, lt: u64) -> Result<Arc<BlockHandle>> {
        unimplemented!()
    }
    async fn apply_block(self: Arc<Self>, handle: &BlockHandle, block: Option<&BlockStuff>) -> Result<()> {
        unimplemented!()
    }
    async fn check_block_proof(&self, proof: &BlockProofStuff) -> Result<()> {
        unimplemented!()
    }
    async fn download_block(&self, id: &BlockIdExt) -> Result<(BlockStuff, BlockProofStuff)> {
        unimplemented!()
    }
    async fn download_block_proof_link(&self, id: &BlockIdExt) -> Result<BlockProofStuff> {
        unimplemented!()
    }
    async fn download_next_block(&self, prev_id: &BlockIdExt) -> Result<(BlockStuff, BlockProofStuff)> {
        unimplemented!()
    }
    async fn get_next_key_blocks_ids(&self, block_id: &BlockIdExt, priority: u32) -> Result<Vec<BlockIdExt>> {
        unimplemented!()
    }
    async fn store_block(&self, handle: &BlockHandle, block: &BlockStuff) -> Result<()> {
        unimplemented!()
    }
    async fn store_block_proof(&self, handle: &BlockHandle, proof: &BlockProofStuff) -> Result<()> {
        unimplemented!()
    }
    async fn load_block_proof(&self, handle: &BlockHandle, is_link: bool) -> Result<BlockProofStuff> {
        unimplemented!()
    }
    async fn process_block_in_ext_db(
        &self,
        handle: &BlockHandle,
        block: &BlockStuff,
        proof: Option<&BlockProofStuff>,
        state: &ShardStateStuff)
    -> Result<()> {
        unimplemented!()
    }


    // State related operations

    async fn download_state(&self, block_id: &BlockIdExt, master_id: &BlockIdExt) -> Result<ShardStateStuff> {
        unimplemented!()
    }
    async fn load_mc_zero_state(&self) -> Result<ShardStateStuff> {
        unimplemented!()
    }
    async fn load_state(&self, handle: &BlockHandle) -> Result<ShardStateStuff> {
        unimplemented!()
    }
    async fn wait_state(&self, handle: &BlockHandle) -> Result<ShardStateStuff> {
        unimplemented!()
    }
    async fn store_state(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<()> {
        unimplemented!()
    }
    async fn process_full_state_in_ext_db(&self, state: &ShardStateStuff)-> Result<()> {
        unimplemented!()
    }


    // Block next prev links

    async fn store_block_prev(&self, handle: &BlockHandle, prev: &BlockIdExt) -> Result<()> {
        unimplemented!()
    }
    async fn load_block_prev(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        unimplemented!()
    }
    async fn store_block_prev2(&self, handle: &BlockHandle, prev2: &BlockIdExt) -> Result<()> {
        unimplemented!()
    }
    async fn load_block_prev2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        unimplemented!()
    }
    async fn store_block_next1(&self, handle: &BlockHandle, next: &BlockIdExt) -> Result<()> {
        unimplemented!()
    }
    async fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        unimplemented!()
    }
    async fn store_block_next2(&self, handle: &BlockHandle, next2: &BlockIdExt) -> Result<()> {
        unimplemented!()
    }
    async fn load_block_next2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        unimplemented!()
    }

    // I/O

    async fn broadcast_to_public_overlay(
        &self, 
        to: &AccountIdPrefixFull, 
        data: &[u8]
    ) -> Result<u32> {
        unimplemented!()    
    }

    async fn redirect_external_message(&self, msg: &[u8]) -> Result<u32> {
        if msg.len() > Engine::MAX_EXTERNAL_MESSAGE_SIZE {
            fail!("External message is too large: {}", msg.len())
        }
        let root = deserialize_tree_of_cells(&mut Cursor::new(msg))?;
        if root.level() != 0 {
            fail!("External message must have zero level, but has {}", root.level())
        }
        if root.repr_depth() >= Engine::MAX_EXTERNAL_MESSAGE_DEPTH {
            fail!("External message is too deep: {}", root.repr_depth())
        }
        let message = Message::construct_from(&mut root.clone().into())?;
        if let Some(header) = message.ext_in_header() {
            self.broadcast_to_public_overlay(
                &AccountIdPrefixFull::checked_prefix(&header.dst)?,
                msg
            ).await
        } else {
            fail!("External message is not properly formatted: {}", message)
        }
    }

}

/// External DB should implement this trait and put itself into engine's new function
#[async_trait::async_trait]
pub trait ExternalDb : Sync + Send {
    async fn process_block(
        &self,
        block: &BlockStuff,
        proof: Option<&BlockProofStuff>,
        state: &ShardStateStuff
    ) -> Result<()>;
    async fn process_full_state(&self, state: &ShardStateStuff) -> Result<()>;
}
