use crate::{
    block::{BlockStuff}, 
    shard_state::ShardStateStuff,
    network::{full_node_client::FullNodeOverlayClient},
    block_proof::BlockProofStuff,
    types::top_block_descr::{TopBlockDescrStuff, TopBlockDescrId},
    ext_messages::create_ext_message,
    jaeger,
    config::CollatorTestBundlesGeneralConfig,
};
use adnl::common::KeyOption;
use catchain::{
    CatchainNode, CatchainOverlay, CatchainOverlayListenerPtr, 
    CatchainOverlayLogReplayListenerPtr
};
use overlay::{OverlayId, OverlayShortId, QueriesConsumer, PrivateOverlayShortId};
use std::{sync::Arc, time::{SystemTime, UNIX_EPOCH}};
use storage::types::BlockHandle;
use ton_api::ton::ton_node::broadcast::BlockBroadcast;
use ton_block::{AccountIdPrefixFull, BlockIdExt, Message, ShardIdent, HashmapAugType};
use ton_types::{fail, Result, UInt256};

#[async_trait::async_trait]
pub trait OverlayOperations : Sync + Send {
    async fn start(self: Arc<Self>) -> Result<Arc<dyn FullNodeOverlayClient>>;
    async fn get_peers_count(&self, masterchain_zero_state_id: &BlockIdExt) -> Result<usize>;
    async fn get_overlay(
        self: Arc<Self>, 
        overlay_id: (Arc<OverlayShortId>, OverlayId)
    ) -> Result<Arc<dyn FullNodeOverlayClient>>;
    fn add_consumer(&self, overlay_id: &Arc<OverlayShortId>, consumer: Arc<dyn QueriesConsumer>) -> Result<()>;
    fn calc_overlay_id(&self, workchain: i32, shard: u64) -> Result<(Arc<OverlayShortId>, OverlayId)> ;
}

#[async_trait::async_trait]
pub trait PrivateOverlayOperations: Sync + Send {
    async fn set_validator_list(
        &self, 
        validator_list_id: UInt256,
        validators: &Vec<CatchainNode>
    ) -> Result<Option<Arc<KeyOption>>>;

    async fn remove_validator_list(&self, validator_list_id: UInt256) -> Result<bool>;

    fn create_catchain_client(
        &self,
        validator_list_id: UInt256,
        overlay_short_id : &Arc<PrivateOverlayShortId>,
        nodes_public_keys : &Vec<CatchainNode>,
        listener : CatchainOverlayListenerPtr,
        _log_replay_listener: CatchainOverlayLogReplayListenerPtr
    ) -> Result<Arc<dyn CatchainOverlay + Send>>;

    fn stop_catchain_client(&self, overlay_short_id: &Arc<PrivateOverlayShortId>);
}

// TODO make separate traits for read and write operations (may be critical and not etc.)
#[async_trait::async_trait]
#[allow(unused)]
pub trait EngineOperations : Sync + Send {

    fn validator_network(&self) -> Arc<dyn PrivateOverlayOperations> {
        unimplemented!()
    }

    // Validator specific operations
    async fn set_validator_list(
        &self, 
        validator_list_id: UInt256,
        validators: &Vec<CatchainNode>
    ) -> Result<Option<Arc<KeyOption>>> {
        unimplemented!()
    }

    async fn remove_validator_list(&self, validator_list_id: UInt256) -> Result<bool> {
        unimplemented!()
    }

    fn create_catchain_client(
        &self,
        validator_list_id: UInt256,
        overlay_short_id : &Arc<PrivateOverlayShortId>,
        nodes_public_keys : &Vec<CatchainNode>,
        listener : CatchainOverlayListenerPtr,
        _log_replay_listener: CatchainOverlayLogReplayListenerPtr
    ) -> Result<Arc<dyn CatchainOverlay + Send>> {
        unimplemented!()
    }

    fn stop_catchain_client(&self, overlay_short_id: &Arc<PrivateOverlayShortId>) {
        unimplemented!()
    }

    // Block related operations

    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>> {
        unimplemented!()
    }
    async fn load_applied_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        unimplemented!()
    }
    async fn wait_applied_block(&self, id: &BlockIdExt, timeout_ms: Option<u64>) -> Result<(Arc<BlockHandle>, BlockStuff)> {
        unimplemented!()
    }
    async fn load_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        unimplemented!()
    }
    async fn load_block_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        unimplemented!()
    }
    async fn wait_next_applied_mc_block(&self, prev_handle: &BlockHandle, timeout_ms: Option<u64>) -> Result<(Arc<BlockHandle>, BlockStuff)> {
        unimplemented!()
    }
    async fn load_last_applied_mc_block(&self) -> Result<BlockStuff> {
        unimplemented!()
    }
    async fn load_last_applied_mc_block_id(&self) -> Result<BlockIdExt> {
        unimplemented!()
    }
    async fn store_last_applied_mc_block_id(&self, last_mc_block: &BlockIdExt) -> Result<()> {
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
    async fn apply_block(
        self: Arc<Self>, 
        handle: &Arc<BlockHandle>, 
        block: &BlockStuff, 
        mc_seq_no: u32, 
        pre_apply: bool
    ) -> Result<()> {
        unimplemented!()
    }
    async fn download_and_apply_block(
        self: Arc<Self>, 
        id: &BlockIdExt, 
        mc_seq_no: u32, 
        pre_apply: bool
    ) -> Result<()> {
        unimplemented!()
    }
    async fn download_block(&self, id: &BlockIdExt, limit: Option<u32>) -> Result<(BlockStuff, BlockProofStuff)> {
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
    async fn store_block(&self, block: &BlockStuff) -> Result<Arc<BlockHandle>> {
        unimplemented!()
    }
    async fn store_block_proof(
        &self, 
        id: &BlockIdExt, 
        handle: Option<Arc<BlockHandle>>, 
        proof: &BlockProofStuff
    ) -> Result<Arc<BlockHandle>> {
        unimplemented!()
    }
    async fn load_block_proof(&self, handle: &Arc<BlockHandle>, is_link: bool) -> Result<BlockProofStuff> {
        unimplemented!()
    }
    async fn load_block_proof_raw(&self, handle: &BlockHandle, is_link: bool) -> Result<Vec<u8>> {
        unimplemented!()
    }
    async fn process_block_in_ext_db(
        &self,
        handle: &Arc<BlockHandle>,
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
    async fn download_zerostate(&self, id: &BlockIdExt) -> Result<(ShardStateStuff, Vec<u8>)> {
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
    async fn wait_state(self: Arc<Self>, id: &BlockIdExt, timeout_ms: Option<u64>) -> Result<ShardStateStuff> {
        unimplemented!()
    }
    async fn store_state(
        &self, 
        handle: &Arc<BlockHandle>, 
        state: &ShardStateStuff
    ) -> Result<()> {
        unimplemented!()
    }
    async fn store_zerostate(
        &self, 
        id: &BlockIdExt, 
        state: &ShardStateStuff, 
        state_bytes: &[u8]
    ) -> Result<Arc<BlockHandle>> {
        unimplemented!()
    }
    async fn process_full_state_in_ext_db(&self, state: &ShardStateStuff)-> Result<()> {
        unimplemented!()
    }

    // Block next prev links

    fn store_block_prev1(&self, handle: &Arc<BlockHandle>, prev: &BlockIdExt) -> Result<()> {
        unimplemented!()
    }
    fn load_block_prev1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        unimplemented!()
    }
    fn store_block_prev2(&self, handle: &Arc<BlockHandle>, prev2: &BlockIdExt) -> Result<()> {
        unimplemented!()
    }
    fn load_block_prev2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        unimplemented!()
    }
    fn store_block_next1(&self, handle: &Arc<BlockHandle>, next: &BlockIdExt) -> Result<()> {
        unimplemented!()
    }
    async fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        unimplemented!()
    }
    fn store_block_next2(&self, handle: &Arc<BlockHandle>, next2: &BlockIdExt) -> Result<()> {
        unimplemented!()
    }
    async fn load_block_next2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        unimplemented!()
    }

    // Global node's state

    async fn check_sync(&self) -> Result<bool> {
        unimplemented!()
    }
    fn set_will_validate(&self, will_validate: bool) {
        unimplemented!()
    }
    fn is_validator(&self) -> bool {
        unimplemented!()
    }

    // Top shard blocks

    // Get current list of new shard blocks with respect to last mc block.
    // If given mc_seq_no is not equal to last mc seq_no - function fails.
    fn get_shard_blocks(&self, mc_seq_no: u32) -> Result<Vec<Arc<TopBlockDescrStuff>>> {
        unimplemented!()
    }

    // Save tsb into persistent storage
    fn save_top_shard_block(&self, id: &TopBlockDescrId, tsb: &TopBlockDescrStuff) -> Result<()> {
        unimplemented!()
    }

    // Remove tsb from persistent storage
    fn remove_top_shard_block(&self, id: &TopBlockDescrId) -> Result<()> {
        unimplemented!()
    }

    // External messages

    fn new_external_message_raw(&self, data: &[u8]) -> Result<()> {
        unimplemented!()
    }
    fn new_external_message(&self, id: UInt256, message: Arc<Message>) -> Result<()> {
        unimplemented!()
    }
    fn get_external_messages(&self, shard: &ShardIdent) -> Result<Vec<(Arc<Message>, UInt256)>> {
        unimplemented!()
    }
    fn complete_external_messages(&self, to_delay: Vec<UInt256>, to_delete: Vec<UInt256>) -> Result<()> {
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

    fn test_bundles_config(&self) -> &CollatorTestBundlesGeneralConfig {
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

    async fn send_block_broadcast(&self, broadcast: BlockBroadcast) -> Result<()> {
        unimplemented!()
    }

    async fn send_top_shard_block_description(&self, tbd: &TopBlockDescrStuff) -> Result<()> {
        unimplemented!()
    }

    async fn redirect_external_message(&self, message_data: &[u8]) -> Result<u32> {
        let (id, message) = create_ext_message(message_data)?;
        let message = Arc::new(message);
        self.new_external_message(id.clone(), message.clone())?;
        if let Some(header) = message.ext_in_header() {
            let res = self.broadcast_to_public_overlay(
                &AccountIdPrefixFull::checked_prefix(&header.dst)?,
                message_data
            ).await;
            jaeger::broadcast_sended(id.to_hex_string());
            res
        } else {
            fail!("External message is not properly formatted: {}", message)
        }
    }

    // Boot specific operations

    async fn set_applied(&self, handle: &Arc<BlockHandle>, mc_seq_no: u32) -> Result<()> {
        unimplemented!()
    }

    async fn get_archive_id(&self, mc_seq_no: u32) -> Option<u64> {
        unimplemented!()
    }

    async fn get_archive_slice(&self, archive_id: u64, offset: u64, limit: u32) -> Result<Vec<u8>> {
        unimplemented!()
    }

    async fn download_archive(&self, masterchain_seqno: u32) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    fn assign_mc_ref_seq_no(&self, handle: &Arc<BlockHandle>, mc_seq_no: u32) -> Result<()> {
        unimplemented!()
    }

    fn aux_mc_shard_states(&self) -> &lockfree::map::Map<u32, ShardStateStuff> {unimplemented!()}
    fn shard_states(&self) -> &lockfree::map::Map<ShardIdent, ShardStateStuff> {unimplemented!()}

    async fn request_aux_mc_state(self: Arc<Self>, seq_no: u32, timeout_ms: Option<u64>) -> Result<bool> {
        log::debug!("requesting mc state for seq_no {}", seq_no);
        if self.aux_mc_shard_states().get(&seq_no).is_some() {
            return Ok(true)
        }
        let state = self.load_last_applied_mc_state().await?;
        if seq_no >= state.state().seq_no() {
            return Ok(true)
        }
        let block_id = match state.shard_state_extra()?.prev_blocks.get(&seq_no) {
            Ok(Some(result)) => result.master_block_id().1,
            _ => fail!("cannot find masterchain block with seqno {} \
                to load corresponding state as required", seq_no)
        };
        self.set_aux_mc_state(&self.clone().wait_state(&block_id, timeout_ms).await?)
    }
    fn set_aux_mc_state(&self, state: &ShardStateStuff) -> Result<bool> {
        adnl::common::add_object_to_map_with_update(
            self.aux_mc_shard_states(),
            state.block_id().seq_no,
            |other| match other {
                Some(other) if other.block_id() != state.block_id() => {
                    fail!("got two masterchain states of same height \
                        corresponding to different blocks {} and {}",
                            other.block_id(), state.block_id()
                    )
                }
                Some(_) => Ok(None),
                _ => Ok(Some(state.clone()))
            }
        )
    }
    fn get_aux_mc_state(&self, seq_no: u32) -> Option<ShardStateStuff> {
        self.aux_mc_shard_states().get(&seq_no).map(|k_v| k_v.1.clone())
    }
    fn set_shard_state(&self, state: &ShardStateStuff) {
        self.shard_states().insert(state.shard().clone(), state.clone());
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
