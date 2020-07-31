use crate::{
    block::{BlockStuff}, engine::Engine, 
    shard_state::ShardStateStuff,
    network::full_node_client::FullNodeOverlayClient,
    block_proof::BlockProofStuff,
    db::BlockHandle,
    jaeger
};

use std::{
    io::Cursor,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use ton_types::{deserialize_tree_of_cells, fail, Result};
use overlay::{OverlayShortId, QueriesConsumer};
use ton_block::{AccountIdPrefixFull, BlockIdExt, Deserializable, Message, ShardIdent};

#[async_trait::async_trait]
pub trait OverlayOperations : Sync + Send {
    async fn start(self: Arc<Self>) -> Result<Arc<dyn FullNodeOverlayClient>>;
    async fn get_overlay(self: Arc<Self>, overlay_id: &Arc<OverlayShortId>) -> Result<Arc<dyn FullNodeOverlayClient>>;
    fn add_consumer(&self, overlay_id: &Arc<OverlayShortId>, consumer: Arc<dyn QueriesConsumer>) -> Result<()>;
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
    async fn load_block_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        unimplemented!()
    }
    async fn wait_applied_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        unimplemented!()
    }
    async fn wait_next_applied_mc_block(&self, prev_handle: &BlockHandle) -> Result<(Arc<BlockHandle>, BlockStuff)> {
        unimplemented!()
    }
    async fn load_last_applied_mc_block(&self) -> Result<BlockStuff> {
        unimplemented!()
    }
    async fn load_last_applied_mc_block_id(&self) -> Result<BlockIdExt> {
        unimplemented!()
    }
    async fn load_last_applied_mc_state(&self) -> Result<ShardStateStuff> {
        unimplemented!()
    }
    async fn load_shards_client_mc_block_id(&self) -> Result<BlockIdExt> {
        unimplemented!()
    }
    async fn store_shards_client_mc_block_id(&self, id: &BlockIdExt) -> Result<()> {
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
    async fn download_block(&self, id: &BlockIdExt) -> Result<(BlockStuff, BlockProofStuff)> {
        unimplemented!()
    }
    async fn download_block_proof(&self, id: &BlockIdExt, is_link: bool, key_block: bool) -> Result<BlockProofStuff> {
        unimplemented!()
    }
    async fn download_next_block(&self, prev_id: &BlockIdExt) -> Result<(BlockStuff, BlockProofStuff)> {
        unimplemented!()
    }
    async fn download_next_key_blocks_ids(&self, block_id: &BlockIdExt, priority: u32) -> Result<Vec<BlockIdExt>> {
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
    async fn load_block_proof_raw(&self, handle: &BlockHandle, is_link: bool) -> Result<Vec<u8>> {
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
    async fn load_state(&self, block_id: &BlockIdExt) -> Result<ShardStateStuff> {
        unimplemented!()
    }
    async fn load_persistent_state_size(&self, block_id: &BlockIdExt) -> Result<u64> {
        unimplemented!()
    }
    async fn load_persistent_state_slice(
        &self,
        handle: &BlockHandle,
        offset: u64,
        length: u64
    ) -> Result<Vec<u8>> {
        unimplemented!()
    }
    async fn wait_state(&self, handle: &BlockHandle) -> Result<ShardStateStuff> {
        unimplemented!()
    }
    async fn store_state(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<()> {
        unimplemented!()
    }
    async fn store_persistent_state(self: Arc<Self>, state: ShardStateStuff) -> Result<()> {
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

    // Utils

    fn now(&self) -> u32 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as u32
    }

    fn is_persistent_state(&self, block_time: u32, prev_time: u32) -> bool {
        block_time >> 17 != prev_time >> 17
    }

    fn persistent_state_ttl(&self, block_time: u32) -> u32 {
        if cfg!(feature = "local_test") {
            !0
        } else {
            let x = block_time >> 17;
            debug_assert!(x != 0);
            block_time + ((1 << 18) << x.trailing_zeros())
        }
    }

    // Options

    fn get_last_fork_masterchain_seqno(&self) -> u32 { 0 }

    fn get_hardforks(&self) { todo!("WTF") }

    // True to allow sync from initial block, but it fail if it is not key block
    fn initial_sync_disabled(&self) -> bool { false } 

    // False to allow infinite init
    fn allow_blockchain_init(&self) -> bool { false }

    // Time in past to get blocks in
    fn sync_blocks_before(&self) -> u32  { 0 }

    fn key_block_utime_step(&self) -> u32 {
        if cfg!(feature = "local_test") {
            !0 >> 2 // allow to sync with test data
        } else {
            86400 // One day period
        }
    }

    fn need_db_truncate(&self) -> bool { false }

    // Parameter outside of node
    fn truncate_seqno(&self) -> u32 { 0 } 

    fn need_monitor(&self, _shard: &ShardIdent) -> bool { false }

    // Is got from global config
    fn init_mc_block_id(&self) -> &BlockIdExt {
        unimplemented!()
    }

    fn set_init_mc_block_id(&self, _init_block_id: &BlockIdExt) {
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
            let res = self.broadcast_to_public_overlay(
                &AccountIdPrefixFull::checked_prefix(&header.dst)?,
                msg
            ).await;
            jaeger::broadcast_sended(root.repr_hash().to_hex_string());
            res
        } else {
            fail!("External message is not properly formatted: {}", message)
        }
    }
    fn set_applied(&self, block_id: &BlockIdExt) -> Result<()> {unimplemented!()}
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
