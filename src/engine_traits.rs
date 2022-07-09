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

use crate::{
    block::BlockStuff, config::CollatorTestBundlesGeneralConfig, 
    block_proof::BlockProofStuff, config::TonNodeConfig, internal_db::BlockResult,
    network::{control::ControlServer, full_node_client::FullNodeOverlayClient},
    shard_state::ShardStateStuff, types::top_block_descr::{TopBlockDescrStuff, TopBlockDescrId}
};
#[cfg(feature = "telemetry")]
use crate::{
    full_node::telemetry::FullNodeTelemetry, network::telemetry::FullNodeNetworkTelemetry,
    validator::telemetry::CollatorValidatorTelemetry,
};

use ever_crypto::{KeyId, KeyOption};
#[cfg(feature = "telemetry")]
use adnl::telemetry::Metric;
use catchain::{
    CatchainNode, CatchainOverlay, CatchainOverlayListenerPtr, 
    CatchainOverlayLogReplayListenerPtr
};
use overlay::{
    BroadcastSendInfo, OverlayId, OverlayShortId, QueriesConsumer, PrivateOverlayShortId
};
use std::{sync::{Arc, atomic::AtomicU64}, time::{SystemTime, UNIX_EPOCH}};
#[cfg(feature = "telemetry")]
use storage::{StorageAlloc, block_handle_db::BlockHandle};
#[cfg(feature = "telemetry")]
use storage::StorageTelemetry;
use ton_api::ton::ton_node::broadcast::BlockBroadcast;
use ton_block::{AccountIdPrefixFull, BlockIdExt, Message, ShardIdent, signature::SigPubKey};
use ton_types::{Result, UInt256};
use validator_session::{BlockHash, SessionId, ValidatorBlockCandidate};

pub struct ValidatedBlockStatNode {
    pub public_key : SigPubKey,
    pub signed : bool,
    pub collated : bool,
}

pub struct ValidatedBlockStat {
    pub nodes : Vec<ValidatedBlockStatNode>,
}

#[cfg(feature = "telemetry")]
pub struct EngineTelemetry {
    pub storage: Arc<StorageTelemetry>,
    pub awaiters: Arc<Metric>,
    pub catchain_clients: Arc<Metric>,
    pub cells: Arc<Metric>,
    pub overlay_clients: Arc<Metric>,
    pub peer_stats: Arc<Metric>,
    pub shard_states: Arc<Metric>,
    pub top_blocks: Arc<Metric>,
    pub validator_peers: Arc<Metric>,
    pub validator_sets: Arc<Metric>
}

pub struct EngineAlloc {
    pub storage: Arc<StorageAlloc>,
    pub awaiters: Arc<AtomicU64>,
    pub catchain_clients: Arc<AtomicU64>,
    pub overlay_clients: Arc<AtomicU64>,
    pub peer_stats: Arc<AtomicU64>,
    pub shard_states: Arc<AtomicU64>,
    pub top_blocks: Arc<AtomicU64>,
    pub validator_peers: Arc<AtomicU64>,
    pub validator_sets: Arc<AtomicU64>
}

#[async_trait::async_trait]
pub trait OverlayOperations : Sync + Send {
    async fn start(&self) -> Result<()>;
    async fn get_peers_count(&self, masterchain_zero_state_id: &BlockIdExt) -> Result<usize>;
    async fn get_overlay(
        self: Arc<Self>, 
        overlay_id: (Arc<OverlayShortId>, OverlayId)
    ) -> Result<Arc<dyn FullNodeOverlayClient>>;
    async fn get_masterchain_overlay(self: Arc<Self>) -> Result<Arc<dyn FullNodeOverlayClient>> {
        let overlay_id = self.calc_overlay_id(ton_block::MASTERCHAIN_ID, ton_block::SHARD_FULL)?;
        self.get_overlay(overlay_id).await
    }
    fn add_consumer(&self, overlay_id: &Arc<OverlayShortId>, consumer: Arc<dyn QueriesConsumer>) -> Result<()>;
    fn calc_overlay_id(&self, workchain: i32, shard: u64) -> Result<(Arc<OverlayShortId>, OverlayId)> ;
}

#[async_trait::async_trait]
pub trait PrivateOverlayOperations: Sync + Send {
    async fn set_validator_list(
        &self, 
        validator_list_id: UInt256,
        validators: &Vec<CatchainNode>
    ) -> Result<Option<Arc<dyn KeyOption>>>;

    fn activate_validator_list(&self, validator_list_id: UInt256) -> Result<()>;

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

    async fn processed_workchain(&self) -> Result<(bool, i32)> { Ok((true, 0)) }

    fn get_validator_status(&self) -> bool { unimplemented!() }

    fn validator_network(&self) -> Arc<dyn PrivateOverlayOperations> {
        unimplemented!()
    }

    fn validation_status(&self) -> &lockfree::map::Map<ShardIdent, u64> {
        unimplemented!()
    }

    fn collation_status(&self) -> &lockfree::map::Map<ShardIdent, u64> {
        unimplemented!()
    }

    // Validator specific operations
    async fn set_validator_list(
        &self, 
        validator_list_id: UInt256,
        validators: &Vec<CatchainNode>
    ) -> Result<Option<Arc<dyn KeyOption>>> {
        unimplemented!()
    }

    fn activate_validator_list(&self, validator_list_id: UInt256) -> Result<()> {
        unimplemented!()
    }

    async fn remove_validator_list(&self, validator_list_id: UInt256) -> Result<bool> {
        unimplemented!()
    }

    fn set_sync_status(&self, status: u32) {
        unimplemented!()
    }

    fn get_sync_status(&self) -> u32 {
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
    fn load_last_applied_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
        unimplemented!()
    }
    fn save_last_applied_mc_block_id(&self, last_mc_block: &BlockIdExt) -> Result<()> {
        unimplemented!()
    }
    async fn load_last_applied_mc_state_or_zerostate(&self) -> Result<Arc<ShardStateStuff>> {
        match self.load_last_applied_mc_block_id()? {
            Some(block_id) => self.load_state(&block_id).await,
            None => self.load_mc_zero_state().await
        }
    }
    async fn load_last_applied_mc_state(&self) -> Result<Arc<ShardStateStuff>> {
        unimplemented!()
    }
    fn load_shard_client_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
        unimplemented!()
    }
    fn save_shard_client_mc_block_id(&self, id: &BlockIdExt) -> Result<()> {
        unimplemented!()
    }
    fn load_last_rotation_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
        unimplemented!()
    }
    fn save_last_rotation_block_id(&self, info: &BlockIdExt) -> Result<()> {
        unimplemented!()
    }
    fn clear_last_rotation_block_id(&self) -> Result<()> {
        unimplemented!()
    }
    fn save_block_candidate(
        &self, 
        session_id: &SessionId, 
        candidate: ValidatorBlockCandidate
    ) -> Result<()> {
        unimplemented!()
    }
    fn load_block_candidate(
        &self, 
        session_id: &SessionId, 
        root_hash: &BlockHash
    ) -> Result<Arc<ValidatorBlockCandidate>> {
        unimplemented!()
    }
    fn destroy_block_candidates(&self, session_id: &SessionId) -> Result<bool> {
        unimplemented!()
    }
    async fn find_mc_block_by_seq_no(&self, seqno: u32) -> Result<Arc<BlockHandle>> {
        unimplemented!()
    }
    async fn apply_block(
        self: Arc<Self>, 
        handle: &Arc<BlockHandle>, 
        block: &BlockStuff, 
        mc_seq_no: u32, 
        pre_apply: bool
    ) -> Result<()> {
        self.apply_block_internal(handle, block, mc_seq_no, pre_apply, 0).await
    }
    async fn apply_block_internal(
        self: Arc<Self>, 
        handle: &Arc<BlockHandle>, 
        block: &BlockStuff, 
        mc_seq_no: u32, 
        pre_apply: bool,
        recursion_depth: u32
    ) -> Result<()> {
        unimplemented!()
    }
    async fn download_and_apply_block(
        self: Arc<Self>, 
        id: &BlockIdExt, 
        mc_seq_no: u32, 
        pre_apply: bool
    ) -> Result<()> {
        self.download_and_apply_block_internal(id, mc_seq_no, pre_apply, 0).await
    }
    async fn download_and_apply_block_internal(
        self: Arc<Self>, 
        id: &BlockIdExt, 
        mc_seq_no: u32, 
        pre_apply: bool,
        recursion_depth: u32
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
    async fn download_next_key_blocks_ids(&self, block_id: &BlockIdExt, priority: u32) -> Result<(Vec<BlockIdExt>, bool)> {
        unimplemented!()
    }
    async fn store_block(
        &self, 
        block: &BlockStuff
    ) -> Result<BlockResult> {
        unimplemented!()
    }
    async fn store_block_proof(
        &self, 
        id: &BlockIdExt, 
        handle: Option<Arc<BlockHandle>>, 
        proof: &BlockProofStuff
    ) -> Result<BlockResult> {
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
        state: &Arc<ShardStateStuff>,
        prev_states: (&Arc<ShardStateStuff>, Option<&Arc<ShardStateStuff>>),
        mc_seq_no: u32,
    )
    -> Result<()> {
        unimplemented!()
    }

    async fn process_chain_range_in_ext_db(
        &self,
        chain_range: &ChainRange)
    -> Result<()> {
        unimplemented!()
    }

    // State related operations

    async fn download_state(
        &self, 
        block_id: &BlockIdExt, 
        master_id: &BlockIdExt,
        active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>,
        attempts: Option<usize>
    ) -> Result<(Arc<ShardStateStuff>, Arc<Vec<u8>>)> {
        unimplemented!()
    }
    async fn download_zerostate(
        &self, 
        id: &BlockIdExt
    ) -> Result<(Arc<ShardStateStuff>, Vec<u8>)> {
        unimplemented!()
    }
    async fn load_mc_zero_state(&self) -> Result<Arc<ShardStateStuff>> {
        unimplemented!()
    }
    async fn load_state(&self, block_id: &BlockIdExt) -> Result<Arc<ShardStateStuff>> {
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
    async fn wait_state(
        self: Arc<Self>,
        id: &BlockIdExt,
        timeout_ms: Option<u64>,
        allow_block_downloading: bool
    ) -> Result<Arc<ShardStateStuff>> {
        unimplemented!()
    }
    async fn store_state(
        &self, 
        handle: &Arc<BlockHandle>, 
        state: Arc<ShardStateStuff>,
        state_bytes: Option<&[u8]>,
    ) -> Result<Arc<ShardStateStuff>> {
        unimplemented!()
    }
    async fn store_zerostate(
        &self, 
        state: Arc<ShardStateStuff>, 
        state_bytes: &[u8]
    ) -> Result<(Arc<ShardStateStuff>, Arc<BlockHandle>)> {
        unimplemented!()
    }
    async fn process_full_state_in_ext_db(&self, state: &Arc<ShardStateStuff>)-> Result<()> {
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
    fn get_own_shard_blocks(&self, mc_seq_no: u32) -> Result<Vec<Arc<TopBlockDescrStuff>>> {
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

    fn is_persistent_state(&self, block_time: u32, prev_time: u32, pss_period_bits: u32) -> bool {
        block_time >> pss_period_bits != prev_time >> pss_period_bits
    }

    fn persistent_state_ttl(&self, block_time: u32, pss_period_bits: u32) -> u32 {
        let x = block_time >> pss_period_bits;
        debug_assert!(x != 0);
        block_time + ((1 << (pss_period_bits + 1)) << x.trailing_zeros())
    }

    // Options

    fn get_last_fork_masterchain_seqno(&self) -> u32 { 0 }

    fn get_hardforks(&self) { todo!("WTF") }

    // True to allow sync from initial block, but it fail if it is not key block
    fn initial_sync_disabled(&self) -> bool { false } 

    // time for loading key blocks chain
    fn time_for_blockchain_init(&self) -> u32 { 600 }

    // Time in past to get blocks in
    fn sync_blocks_before(&self) -> u32  { 0 }

    fn key_block_utime_step(&self) -> u32 {
        86400 // One day period
    }

    fn need_db_truncate(&self) -> bool { false }

    // Parameter outside of node
    fn truncate_seqno(&self) -> u32 { 0 } 

    fn need_monitor(&self, _shard: &ShardIdent) -> bool { false }

    // Is got from global config
    fn init_mc_block_id(&self) -> &BlockIdExt {
        unimplemented!()
    }

    fn save_init_mc_block_id(&self, _init_block_id: &BlockIdExt) -> Result<()> {
        unimplemented!()
    }

    fn test_bundles_config(&self) -> &CollatorTestBundlesGeneralConfig {
        unimplemented!()
    }

    fn db_root_dir(&self) -> Result<&str> {
        Ok(TonNodeConfig::DEFAULT_DB_ROOT)
    }

    fn produce_chain_ranges_enabled(&self) -> bool {
        unimplemented!()
    }

    fn adjust_states_gc_interval(&self, interval_ms: u32) {
        unimplemented!()
    }

    // I/O

    async fn broadcast_to_public_overlay(
        &self, 
        to: &AccountIdPrefixFull, 
        data: &[u8]
    ) -> Result<BroadcastSendInfo> {
        unimplemented!()    
    }

    async fn send_block_broadcast(&self, broadcast: BlockBroadcast) -> Result<()> {
        unimplemented!()
    }

    async fn send_top_shard_block_description(
        &self,
        tbd: Arc<TopBlockDescrStuff>,
        cc_seqno: u32,
        resend: bool,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn redirect_external_message(&self, message_data: &[u8]) -> Result<BroadcastSendInfo> {
        unimplemented!()
    }

    // Boot specific operations

    async fn set_applied(
        &self, 
        handle: &Arc<BlockHandle>, 
        mc_seq_no: u32
    ) -> Result<bool> {
        unimplemented!()
    }

    async fn get_archive_id(&self, mc_seq_no: u32) -> Option<u64> {
        unimplemented!()
    }

    async fn get_archive_slice(&self, archive_id: u64, offset: u64, limit: u32) -> Result<Vec<u8>> {
        unimplemented!()
    }

    async fn download_archive(
        &self, 
        masterchain_seqno: u32,
        active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>
    ) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    #[cfg(feature = "telemetry")]
    fn full_node_telemetry(&self) -> &FullNodeTelemetry {
        unimplemented!()
    }

    #[cfg(feature = "telemetry")]
    fn collator_telemetry(&self) -> &CollatorValidatorTelemetry {
        unimplemented!()
    }

    #[cfg(feature = "telemetry")]
    fn validator_telemetry(&self) -> &CollatorValidatorTelemetry {
        unimplemented!()
    }

    #[cfg(feature = "telemetry")]
    fn full_node_service_telemetry(&self) -> &FullNodeNetworkTelemetry {
        unimplemented!()
    }

    #[cfg(feature = "telemetry")]
    fn engine_telemetry(&self) -> &Arc<EngineTelemetry> {
        unimplemented!()
    }

    fn engine_allocated(&self) -> &Arc<EngineAlloc> {
        unimplemented!()
    }

    fn calc_tps(&self, period: u64) -> Result<u32> {
        unimplemented!()
    }

    // Slashing related functions

    fn push_validated_block_stat(&self, stat : ValidatedBlockStat) -> Result<()> {
        unimplemented!();
    }

    fn pop_validated_block_stat(&self) -> Result<ValidatedBlockStat> {
        unimplemented!();
    }

    // Engine stopping

    fn acquire_stop(&self, mask: u32) {
        unimplemented!();
    }

    fn check_stop(&self) -> bool {
        unimplemented!();
    }

    fn release_stop(&self, mask: u32) {
        unimplemented!();
    }

    fn register_server(&self, server: Server) {
        unimplemented!();
    }

}

pub struct ChainRange {
    pub master_block: BlockIdExt,
    pub shard_blocks: Vec<BlockIdExt>
}

/// External DB should implement this trait and put itself into engine's new function
#[async_trait::async_trait]
pub trait ExternalDb : Sync + Send {
    async fn process_block(
        &self,
        block: &BlockStuff,
        proof: Option<&BlockProofStuff>,
        state: &Arc<ShardStateStuff>,
        prev_states: (&Arc<ShardStateStuff>, Option<&Arc<ShardStateStuff>>),
        mc_seq_no: u32,
    ) -> Result<()>;
    async fn process_full_state(&self, state: &Arc<ShardStateStuff>) -> Result<()>;
    fn process_chain_range_enabled(&self) -> bool;
    async fn process_chain_range(&self, range: &ChainRange) -> Result<()>;
}

pub enum Server {
    ControlServer(ControlServer),
    #[cfg(feature = "external_db")]
    KafkaConsumer(stream_cancel::Trigger)
}
