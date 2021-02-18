use crate::{
    block::BlockStuff, block_proof::BlockProofStuff, 
    config::CollatorTestBundlesGeneralConfig,
    engine::{Engine, LastMcBlockId, ShardsClientMcBlockId, STATSD},
    engine_traits::{EngineOperations, PrivateOverlayOperations}, error::NodeError,
    internal_db::NodeState, 
    shard_state::ShardStateStuff, types::top_block_descr::{TopBlockDescrStuff, TopBlockDescrId},
};
use adnl::common::KeyOption;
use catchain::{
    CatchainNode, CatchainOverlay, CatchainOverlayListenerPtr, CatchainOverlayLogReplayListenerPtr
};
use overlay::PrivateOverlayShortId;
use std::{sync::Arc, ops::Deref, convert::TryInto};
use storage::types::BlockHandle;
use ton_api::ton::ton_node::broadcast::BlockBroadcast;
use ton_block::{BlockIdExt, AccountIdPrefixFull, ShardIdent, Message, SHARD_FULL, MASTERCHAIN_ID};
use ton_types::{fail, error, Result, UInt256};

#[async_trait::async_trait]
impl EngineOperations for Engine {

    fn validator_network(&self) -> Arc<dyn PrivateOverlayOperations> {
        Engine::validator_network(self)
    }

    async fn set_validator_list(
        &self, 
        validator_list_id: UInt256,
        validators: &Vec<CatchainNode>
    ) -> Result<Option<Arc<KeyOption>>> {
        self.validator_network().set_validator_list(validator_list_id, validators).await
    }

    async fn remove_validator_list(&self, validator_list_id: UInt256) -> Result<bool> {
        self.validator_network().remove_validator_list(validator_list_id).await
    }

    fn create_catchain_client(
        &self,
        validator_list_id: UInt256,
        overlay_short_id : &Arc<PrivateOverlayShortId>,
        nodes_public_keys : &Vec<CatchainNode>,
        listener : CatchainOverlayListenerPtr,
        _log_replay_listener: CatchainOverlayLogReplayListenerPtr
    ) -> Result<Arc<dyn CatchainOverlay + Send>> {
        self.validator_network().create_catchain_client(
            validator_list_id,
            overlay_short_id,
            nodes_public_keys,
            listener,
            _log_replay_listener
        )
    }

    fn stop_catchain_client(&self, overlay_short_id: &Arc<PrivateOverlayShortId>) {
        self.validator_network().stop_catchain_client(overlay_short_id)
    }

    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>> {
        self.db().load_block_handle(id)
    }

    async fn load_applied_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        // TODO make cache?
        if handle.is_applied() {
            self.load_block(handle).await
        } else if handle.has_data() {
            fail!("Block is not applied yet")
        } else {
            fail!("No block")
        }
    }

    async fn load_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        self.db().load_block_data(handle).await
    }

    async fn load_block_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        self.db().load_block_data_raw(handle).await
    }

    async fn wait_applied_block(&self, id: &BlockIdExt, timeout_ms: Option<u64>) -> Result<(Arc<BlockHandle>, BlockStuff)> {
        loop {
            if let Some(handle) = self.load_block_handle(id)? {
                if handle.is_applied() {
                    let block = self.load_block(&handle).await?;
                    return Ok((handle, block))
                }
            }
            self.block_applying_awaiters().wait(id, timeout_ms).await?;
        }
    }

    async fn wait_next_applied_mc_block(
        &self, 
        prev_handle: &BlockHandle,
        timeout_ms: Option<u64>
    ) -> Result<(Arc<BlockHandle>, BlockStuff)> {
        if !prev_handle.id().shard().is_masterchain() {
            fail!(NodeError::InvalidArg("`prev_handle` doesn't belong masterchain".to_string()))
        }
        loop {
            if prev_handle.has_next1() {
                let id = self.load_block_next1(prev_handle.id()).await?;
                return self.wait_applied_block(&id, timeout_ms).await
            } else {
                if let Some(id) = self.next_block_applying_awaiters().wait(prev_handle.id(), timeout_ms).await? {
                    if let Some(handle) = self.load_block_handle(&id)? {
                        let block = self.load_block(&handle).await?;
                        return Ok((handle, block))
                    }
                }
            }
        }
    }

    async fn find_block_by_seq_no(&self, acc_pfx: &AccountIdPrefixFull, seqno: u32) -> Result<Arc<BlockHandle>> {
        self.db().find_block_by_seq_no(acc_pfx, seqno)
    }

    async fn find_block_by_unix_time(&self, acc_pfx: &AccountIdPrefixFull, utime: u32) -> Result<Arc<BlockHandle>> {
        self.db().find_block_by_unix_time(acc_pfx, utime)
    }

    async fn find_block_by_lt(&self, acc_pfx: &AccountIdPrefixFull, lt: u64) -> Result<Arc<BlockHandle>> {
        self.db().find_block_by_lt(acc_pfx, lt)
    }

    async fn load_last_applied_mc_block(&self) -> Result<BlockStuff> {
        let block_id = &self.load_last_applied_mc_block_id().await?;
        let handle = self.load_block_handle(&block_id)?.ok_or_else(
            || error!("Cannot load handle for last applied master block {}", block_id)
        )?;
        self.load_applied_block(&handle).await
    }

    async fn load_last_applied_mc_state(&self) -> Result<ShardStateStuff> {
        self.load_state(&self.load_last_applied_mc_block_id().await?).await
    }

    async fn load_last_applied_mc_block_id(&self) -> Result<BlockIdExt> {
        (&LastMcBlockId::load_from_db(self.db().deref())?.0).try_into()
    }

    async fn store_last_applied_mc_block_id(&self, last_mc_block: &BlockIdExt) -> Result<()> {
        LastMcBlockId(last_mc_block.into()).store_to_db(self.db().deref())
    }

    async fn load_shards_client_mc_block_id(&self) -> Result<BlockIdExt> {
        (&ShardsClientMcBlockId::load_from_db(self.db().deref())?.0).try_into()
    }

    async fn store_shards_client_mc_block_id(&self, id: &BlockIdExt) -> Result<()> {
        STATSD.gauge("shards_client_mc_block", id.seq_no() as f64);
        ShardsClientMcBlockId(id.into()).store_to_db(self.db().deref())
    }

    async fn apply_block(
        self: Arc<Self>, 
        handle: &Arc<BlockHandle>, 
        block: &BlockStuff, 
        mc_seq_no: u32, 
        pre_apply: bool
    ) -> Result<()> {
        // if it is pre-apply we are waiting for `state_inited` or `applied`
        // otherwise - only for applied
        while !((pre_apply && handle.has_state()) || handle.is_applied()) {
            if self.block_applying_awaiters().do_or_wait(
                handle.id(),
                None,
                self.clone().apply_block_worker(handle, block, mc_seq_no, pre_apply)
            ).await?.is_some() {
                break;
            }
        }
        Ok(())
    }

    async fn download_and_apply_block(
        self: Arc<Self>, 
        id: &BlockIdExt, 
        mc_seq_no: u32, 
        pre_apply: bool
    ) -> Result<()> {
        self.download_and_apply_block_worker(id, mc_seq_no, pre_apply).await
    }

    async fn download_block(
        &self, 
        id: &BlockIdExt, 
        limit: Option<u32>
    ) -> Result<(BlockStuff, BlockProofStuff)> {
        loop {
            if let Some(handle) = self.load_block_handle(id)? {
                if handle.has_data() {
                    let ret = (
                        self.load_block(&handle).await?,
                        self.load_block_proof(&handle, !id.shard().is_masterchain()).await?
                    );
                    return Ok(ret)
                }
            }

            if let Some(data) = self.download_block_awaiters().do_or_wait(
                id,
                None,
                self.download_block_worker(id, limit, None)
            ).await? {
                return Ok(data)
            }
        }
    }

    async fn download_block_proof(&self, id: &BlockIdExt, is_link: bool, key_block: bool) -> Result<BlockProofStuff> {
        self.download_block_proof_worker(id, is_link, key_block, None).await
    }

    async fn download_next_block(&self, prev_id: &BlockIdExt) -> Result<(BlockStuff, BlockProofStuff)> {
        self.download_next_block_worker(prev_id, None).await
    }

    async fn download_state(&self, block_id: &BlockIdExt, master_id: &BlockIdExt) -> Result<ShardStateStuff> {
        let overlay = self.get_full_node_overlay(block_id.shard().workchain_id(), block_id.shard().shard_prefix_with_tag()).await?;
        crate::full_node::state_helper::download_persistent_state(block_id, master_id, overlay.deref()).await
    }

    async fn download_zerostate(&self, id: &BlockIdExt) -> Result<(ShardStateStuff, Vec<u8>)> {
        self.download_zerostate_worker(id, None).await
    }

    async fn store_block(&self, block: &BlockStuff) -> Result<Arc<BlockHandle>> {
        let handle = self.db().store_block_data(block).await?;
        if handle.id().shard().is_masterchain() {
            if handle.is_key_block()? {
                self.update_last_known_keyblock_seqno(handle.id().seq_no());
            }
            self.update_last_known_mc_block_seqno(handle.id().seq_no());
        }
        Ok(handle)
    }

    async fn store_block_proof(
        &self, 
        id: &BlockIdExt,
        handle: Option<Arc<BlockHandle>>, 
        proof: &BlockProofStuff
    ) -> Result<Arc<BlockHandle>> {
        self.db().store_block_proof(id, handle, proof).await
    }

    async fn load_block_proof(
        &self, 
        handle: &Arc<BlockHandle>, 
        is_link: bool
    ) -> Result<BlockProofStuff> {
        // TODO make cache?
        self.db().load_block_proof(handle, is_link).await
    }

    async fn load_block_proof_raw(&self, handle: &BlockHandle, is_link: bool) -> Result<Vec<u8>> {
        self.db().load_block_proof_raw(handle, is_link).await
    }

    async fn load_mc_zero_state(&self) -> Result<ShardStateStuff> {
        let id = self.zero_state_id();
        let len = self.db().load_shard_state_persistent_size(id).await?;
        let data = self.db().load_shard_state_persistent_slice(id, 0, len).await?;

        ShardStateStuff::deserialize_zerostate(self.zero_state_id().clone(), &data)
    }

    async fn load_state(&self, block_id: &BlockIdExt) -> Result<ShardStateStuff> {
        if let Some (kv) = self.shard_states_cache().get(block_id) {
            Ok(kv.val().clone())
        } else {
            let state = self.db().load_shard_state_dynamic(block_id)?;
            self.shard_states_cache().insert(block_id.clone(), state.clone());
            Ok(state)
        }
    }

    async fn load_persistent_state_size(&self, block_id: &BlockIdExt) -> Result<u64> {
        self.db().load_shard_state_persistent_size(block_id).await
    }

    async fn load_persistent_state_slice(
        &self,
        handle: &BlockHandle,
        offset: u64,
        length: u64
    ) -> Result<Vec<u8>> {
        self.db().load_shard_state_persistent_slice(handle.id(), offset, length).await
    }

    async fn wait_state(self: Arc<Self>, id: &BlockIdExt, timeout_ms: Option<u64>) -> Result<ShardStateStuff> {
        loop {
            if let Some(handle) = self.load_block_handle(id)? {
                if handle.has_state() {
                    break self.load_state(id).await
                }
            }
            let id1 = id.clone();
            let engine = self.clone();
            tokio::spawn(async move {
                if let Err(e) = engine.download_and_apply_block(&id1, 0, true).await {
                    log::error!("Error while pre-apply block (while wait_state) {}: {}", id1, e);
                }
            });
            if let Some(ss) = self.shard_states_awaiters().wait(id, timeout_ms).await? {
                break Ok(ss)
            }
        }
    }

    async fn store_state(
        &self, 
        handle: &Arc<BlockHandle>, 
        state: &ShardStateStuff
    ) -> Result<()> {
        self.shard_states_awaiters().do_or_wait(
            state.block_id(),
            None,
            async { Ok(state.clone()) }
        ).await?;
        if self.shard_states_cache().get(handle.id()).is_none() {
            self.shard_states_cache().insert(handle.id().clone(), state.clone());
        }
        self.db().store_shard_state_dynamic(handle, state)
    }

    async fn store_zerostate(
        &self, 
        id: &BlockIdExt, 
        state: &ShardStateStuff, 
        state_bytes: &[u8]
    ) -> Result<Arc<BlockHandle>> {
        let handle = self.db().create_or_load_block_handle(
            id, 
            None, 
            Some(state.state().gen_time())
        )?;
        self.store_state(&handle, state).await?;
        self.db().store_shard_state_persistent_raw(&handle, state_bytes).await?;
        Ok(handle)
    }

    fn store_block_prev1(&self, handle: &Arc<BlockHandle>, prev: &BlockIdExt) -> Result<()> {
        self.db().store_block_prev1(handle, prev)
    }

    fn load_block_prev1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db().load_block_prev1(id)
    }

    fn store_block_prev2(&self, handle: &Arc<BlockHandle>, prev2: &BlockIdExt) -> Result<()> {
        self.db().store_block_prev2(handle, prev2)
    }

    fn load_block_prev2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db().load_block_prev2(id)
    }

    fn store_block_next1(&self, handle: &Arc<BlockHandle>, next: &BlockIdExt) -> Result<()> {
        self.db().store_block_next1(handle, next)
    }

    async fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db().load_block_next1(id)
    }

    fn store_block_next2(&self, handle: &Arc<BlockHandle>, next2: &BlockIdExt) -> Result<()> {
        self.db().store_block_next2(handle, next2)
    }

    async fn load_block_next2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db().load_block_next2(id)
    }

    async fn process_block_in_ext_db(
        &self,
        handle: &Arc<BlockHandle>,
        block: &BlockStuff,
        proof: Option<&BlockProofStuff>,
        state: &ShardStateStuff)
    -> Result<()> {
        if self.ext_db().len() > 0 {
            if proof.is_some() && !handle.id().shard().is_masterchain() {
                fail!("Non master blocks should be processed without proof")
            }
            if proof.is_none() && handle.id().shard().is_masterchain() {
                let proof = self.load_block_proof(handle, false).await?;
                for db in self.ext_db() {
                    db.process_block(block, Some(&proof), state).await?;
                }
            } else {
                for db in self.ext_db() {
                    db.process_block(block, proof, state).await?;
                }
            }
        }
//        self.db().store_block_processed_in_ext_db(handle)?;
        Ok(())
    }

    async fn process_full_state_in_ext_db(&self, state: &ShardStateStuff)-> Result<()> {
        for db in self.ext_db() {
            db.process_full_state(state).await?;
        }
        Ok(())
    }

    async fn download_next_key_blocks_ids(
        &self, 
        block_id: &BlockIdExt, 
        _priority: u32
    ) -> Result<Vec<BlockIdExt>> {
        let mc_overlay = self.get_masterchain_overlay().await?;
        mc_overlay.download_next_key_blocks_ids(block_id, 5).await
    }

    async fn set_applied(&self, handle: &Arc<BlockHandle>, mc_seq_no: u32) -> Result<()> {
        if handle.is_applied() {
            return Ok(());
        }
        self.db().assign_mc_ref_seq_no(handle, mc_seq_no)?;
        self.db().index_handle(handle)?;
        self.db().archive_block(handle.id()).await?;
        self.db().store_block_applied(handle)?;
        Ok(())
    }

    fn initial_sync_disabled(&self) -> bool { Engine::initial_sync_disabled(self) }

    fn init_mc_block_id(&self) -> &BlockIdExt { (self as &Engine).init_mc_block_id() }

    fn set_init_mc_block_id(&self, init_mc_block_id: &BlockIdExt) {
        (self as &Engine).set_init_mc_block_id(init_mc_block_id)
    }

    async fn broadcast_to_public_overlay(&self, to: &AccountIdPrefixFull, data: &[u8]) -> Result<u32> {
        let overlay = self.get_full_node_overlay(to.workchain_id, to.prefix).await?;
        let res = overlay.broadcast_external_message(data).await?;

        Ok(res)
    }

    async fn get_archive_id(&self, mc_seq_no: u32) -> Option<u64> {
        self.db().archive_manager().get_archive_id(mc_seq_no).await
    }

    async fn get_archive_slice(&self, archive_id: u64, offset: u64, limit: u32) -> Result<Vec<u8>> {
        self.db().archive_manager().get_archive_slice(archive_id, offset, limit).await
    }

    async fn download_archive(&self, masterchain_seqno: u32) -> Result<Option<Vec<u8>>> {
        let client = self.get_masterchain_overlay().await?;
        client.download_archive(masterchain_seqno).await
    }

    fn assign_mc_ref_seq_no(&self, handle: &Arc<BlockHandle>, mc_seq_no: u32) -> Result<()> {
        self.db().assign_mc_ref_seq_no(handle, mc_seq_no)
    }

    async fn send_block_broadcast(&self, broadcast: BlockBroadcast) -> Result<()> {
        let overlay = self.get_full_node_overlay(
            MASTERCHAIN_ID, //broadcast.id.workchain, by t-node all broadcast are sending into masterchain overlay
            SHARD_FULL, //broadcast.id.shard as u64
        ).await?;
        overlay.send_block_broadcast(broadcast).await
    }

    async fn send_top_shard_block_description(&self, tbd: &TopBlockDescrStuff) -> Result<()> {
        let overlay = self.get_full_node_overlay(
            MASTERCHAIN_ID, //tbd.proof_for().shard().workchain_id(), by t-node all broadcast are sending into masterchain overlay
            SHARD_FULL, //tbd.proof_for().shard().shard_prefix_with_tag()
        ).await?;

        overlay.send_top_shard_block_description(tbd).await
    }

    async fn check_sync(&self) -> Result<bool> {
        Engine::check_sync(self).await
    }

    fn set_will_validate(&self, will_validate: bool) {
        Engine::set_will_validate(self, will_validate);
    }

    fn is_validator(&self) -> bool {
        self.will_validate()
    }

    fn new_external_message(&self, id: UInt256, message: Arc<Message>) -> Result<()> {
        if !self.is_validator() {
            return Ok(());
        }
        self.external_messages().new_message(id, message, self.now())
    }

    fn new_external_message_raw(&self, data: &[u8]) -> Result<()> {
        if !self.is_validator() {
            return Ok(());
        }
        self.external_messages().new_message_raw(data, self.now())
    }

    fn get_external_messages(&self, shard: &ShardIdent) -> Result<Vec<(Arc<Message>, UInt256)>> {
        self.external_messages().get_messages(shard, self.now())
    }

    fn complete_external_messages(&self, to_delay: Vec<UInt256>, to_delete: Vec<UInt256>) -> Result<()> {
        self.external_messages().complete_messages(to_delay, to_delete, self.now())
    }

    fn aux_mc_shard_states(&self) -> &lockfree::map::Map<u32, ShardStateStuff> {
        Engine::aux_mc_shard_states(self)
    }
    fn shard_states(&self) -> &lockfree::map::Map<ShardIdent, ShardStateStuff> {
        Engine::shard_states(self)
    }

    // Get current list of new shard blocks with respect to last mc block.
    // If given mc_seq_no is not equal to last mc seq_no - function fails.
    fn get_shard_blocks(&self, mc_seq_no: u32) -> Result<Vec<Arc<TopBlockDescrStuff>>> {
        self.shard_blocks().get_shard_blocks(mc_seq_no)
    }

    // Save tsb into persistent storage
    fn save_top_shard_block(&self, id: &TopBlockDescrId, tsb: &TopBlockDescrStuff) -> Result<()> {
        self.db().save_top_shard_block(id, tsb)
    }

    // Remove tsb from persistent storage
    fn remove_top_shard_block(&self, id: &TopBlockDescrId) -> Result<()> {
        self.db().remove_top_shard_block(id)
    }
    
    fn test_bundles_config(&self) -> &CollatorTestBundlesGeneralConfig {
        Engine::test_bundles_config(self)
    }
}
