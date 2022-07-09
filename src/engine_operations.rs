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
    block::BlockStuff, block_proof::BlockProofStuff, 
    config::CollatorTestBundlesGeneralConfig,
    engine::{Engine, STATSD},
    engine_traits::{
        ChainRange, EngineAlloc, EngineOperations, PrivateOverlayOperations, Server, 
        ValidatedBlockStat
    },
    error::NodeError,
    internal_db::{
        INITIAL_MC_BLOCK, LAST_APPLIED_MC_BLOCK, LAST_ROTATION_MC_BLOCK, SHARD_CLIENT_MC_BLOCK,
        BlockResult,
    },
    shard_state::ShardStateStuff,
    types::top_block_descr::{TopBlockDescrStuff, TopBlockDescrId},
    ext_messages::{create_ext_message, EXT_MESSAGES_TRACE_TARGET},
    jaeger,
    validator::candidate_db::CandidateDb,
};
#[cfg(feature = "telemetry")]
use crate::{
    engine_traits::EngineTelemetry, full_node::telemetry::FullNodeTelemetry, 
    network::telemetry::FullNodeNetworkTelemetry, validator::telemetry::CollatorValidatorTelemetry
};

use ever_crypto::{KeyId, KeyOption};
use catchain::{
    CatchainNode, CatchainOverlay, CatchainOverlayListenerPtr, CatchainOverlayLogReplayListenerPtr
};
use overlay::{BroadcastSendInfo, PrivateOverlayShortId};
#[cfg(feature="workchains")]
use rand::Rng;
#[cfg(feature="workchains")]
use std::sync::atomic::Ordering;
use std::{ops::Deref, sync::Arc};
use storage::block_handle_db::BlockHandle;
use ton_api::ton::ton_node::broadcast::BlockBroadcast;
#[cfg(feature="workchains")]
use ton_block::{BASE_WORKCHAIN_ID, INVALID_WORKCHAIN_ID};
use ton_block::{
    MASTERCHAIN_ID, SHARD_FULL,
    BlockIdExt, AccountIdPrefixFull, ShardIdent, Message
};
use ton_types::{fail, error, Result, UInt256};
use validator_session::{BlockHash, SessionId, ValidatorBlockCandidate};

#[async_trait::async_trait]
impl EngineOperations for Engine {
    #[cfg(feature="workchains")]
    async fn processed_workchain(&self) -> Result<(bool, i32)> {
        match self.workchain_id.load(Ordering::Relaxed) {
            INVALID_WORKCHAIN_ID => {
                if let Ok(mc_state) = self.load_last_applied_mc_state_or_zerostate().await {
                    let workchains = mc_state.workchains()?;
                    match workchains.len() {
                        0 => fail!("no workchains in config in {}", mc_state.block_id()),
                        1 => {
                            log::info!("single workchain configuration - old rules");
                            let workchain_id = workchains[0].0;
                            self.workchain_id.store(workchain_id, Ordering::Relaxed);
                            Ok((false, workchain_id))
                        }
                        count => {
                            match rand::thread_rng().gen_range(0, count as usize + 1) {
                                0 => {
                                    log::info!("random assign for masterchain");
                                    self.workchain_id.store(MASTERCHAIN_ID, Ordering::Relaxed);
                                    self.network().config_handler().store_workchain(MASTERCHAIN_ID);
                                    Ok((true, BASE_WORKCHAIN_ID))
                                }
                                index => {
                                    log::info!("random assign for workchain {}", workchains[index - 1].0);
                                    let workchain_id = workchains[index - 1].0;
                                    self.network().config_handler().store_workchain(workchain_id);
                                    Ok((false, workchain_id))
                                }
                            }
                        }
                    }
                } else {
                    log::trace!("mc state was not found so workchains were not determined");
                    Ok((false, BASE_WORKCHAIN_ID))
                }
            }
            MASTERCHAIN_ID => Ok((true, BASE_WORKCHAIN_ID)),
            workchain_id => Ok((false, workchain_id))
        }
    }

    fn get_validator_status(&self) -> bool {
        self.network.config_handler().get_validator_status()
    }

    fn validator_network(&self) -> Arc<dyn PrivateOverlayOperations> {
        Engine::validator_network(self)
    }

    async fn set_validator_list(
        &self, 
        validator_list_id: UInt256,
        validators: &Vec<CatchainNode>
    ) -> Result<Option<Arc<dyn KeyOption>>> {
        self.validator_network().set_validator_list(validator_list_id, validators).await
    }

    fn activate_validator_list(&self, validator_list_id: UInt256) -> Result<()> {
        self.network().activate_validator_list(validator_list_id)
    }

    fn set_sync_status(&self, status: u32) {
        self.set_sync_status(status);
    }

    fn get_sync_status(&self) -> u32 {
        self.get_sync_status()
    }

    fn validation_status(&self) -> &lockfree::map::Map<ShardIdent, u64> {
        self.validation_status()
    }

    fn collation_status(&self) -> &lockfree::map::Map<ShardIdent, u64> {
        self.collation_status()
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
            let is_applied = || {
                if let Some(handle) = self.load_block_handle(id)? {
                    Ok(handle.is_applied())
                } else {
                    Ok(false)
                }
            };

            if let Some(handle) = self.load_block_handle(id)? {
                if handle.is_applied() {
                    let block = self.load_block(&handle).await?;
                    return Ok((handle, block))
                }
            }

            self.block_applying_awaiters().wait(id, timeout_ms, &is_applied).await?;
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
                if let Some(id) = self.next_block_applying_awaiters()
                    .wait(prev_handle.id(), timeout_ms, || Ok(prev_handle.has_next1())).await? {
                    if let Some(handle) = self.load_block_handle(&id)? {
                        let block = self.load_block(&handle).await?;
                        return Ok((handle, block))
                    }
                }
            }
        }
    }

    async fn find_mc_block_by_seq_no(&self, seqno: u32) -> Result<Arc<BlockHandle>> {
        let last_state = self.load_last_applied_mc_state_or_zerostate().await?;
        let id = last_state.find_block_id(seqno)?;        
        self.load_block_handle(&id)?.ok_or_else(
            || error!("Cannot load handle for master block {}", id)
        )    
    }

    async fn load_last_applied_mc_block(&self) -> Result<BlockStuff> {
        match self.load_last_applied_mc_block_id()? {
            Some(block_id) => {
                let handle = self.load_block_handle(&block_id)?.ok_or_else(
                    || error!("Cannot load handle for last applied master block {}", block_id)
                )?;
                self.load_applied_block(&handle).await
            }
            None => fail!("INTERNAL ERROR: No last applied MC block set")
        }
    }

    async fn load_last_applied_mc_state(&self) -> Result<Arc<ShardStateStuff>> {
        match self.load_last_applied_mc_block_id()? {
            Some(block_id) => {
                self.load_state(&block_id).await
            }
            None => fail!("INTERNAL ERROR: No last applied MC block set")
        }
    }

    fn load_last_applied_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
        self.db().load_full_node_state(LAST_APPLIED_MC_BLOCK)
    }

    fn save_last_applied_mc_block_id(&self, id: &BlockIdExt) -> Result<()> {
        self.db().save_full_node_state(LAST_APPLIED_MC_BLOCK, id)
    }

    fn load_shard_client_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
        self.db().load_full_node_state(SHARD_CLIENT_MC_BLOCK)
    }

    fn save_shard_client_mc_block_id(&self, id: &BlockIdExt) -> Result<()> {
        STATSD.gauge("shards_client_mc_block", id.seq_no() as f64);
        self.db().save_full_node_state(SHARD_CLIENT_MC_BLOCK, id)
    }

    fn load_last_rotation_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
        self.db().load_validator_state(LAST_ROTATION_MC_BLOCK)
    }

    fn save_last_rotation_block_id(&self, id: &BlockIdExt) -> Result<()> {
        self.db().save_validator_state(LAST_ROTATION_MC_BLOCK, id)
    }

    fn clear_last_rotation_block_id(&self) -> Result<()> {
        self.db().drop_validator_state(LAST_ROTATION_MC_BLOCK)
    }

    fn save_block_candidate(
        &self, 
        session_id: &SessionId, 
        candidate: ValidatorBlockCandidate
    ) -> Result<()> {
        CandidateDb::with_path(
            self.db_root_dir()?,
            &format!("catchains/candidates{:x}", session_id)
        )?.save(candidate)
    }

    fn load_block_candidate(
        &self, 
        session_id: &SessionId, 
        root_hash: &BlockHash
    ) -> Result<Arc<ValidatorBlockCandidate>> {
        CandidateDb::with_path(
            self.db_root_dir()?,
            &format!("catchains/candidates{:x}", session_id)
        )?.load(root_hash)
    }

    fn destroy_block_candidates(&self, session_id: &SessionId) -> Result<bool> {
        CandidateDb::with_path(
            self.db_root_dir()?,
            &format!("catchains/candidates{:x}", session_id)
        )?.destroy()
    }

    async fn apply_block_internal(
        self: Arc<Self>, 
        handle: &Arc<BlockHandle>, 
        block: &BlockStuff, 
        mc_seq_no: u32, 
        pre_apply: bool,
        recursion_depth: u32
    ) -> Result<()> {
        // if it is pre-apply we are waiting for `state_inited` or `applied`
        // otherwise - only for applied
        while !((pre_apply && handle.has_state()) || handle.is_applied()) {
            self.block_applying_awaiters().do_or_wait(
                handle.id(),
                None,
                self.clone().apply_block_worker(handle, block, mc_seq_no, pre_apply, recursion_depth)
            ).await?;
        }
        Ok(())
    }

    async fn download_and_apply_block_internal(
        self: Arc<Self>, 
        id: &BlockIdExt, 
        mc_seq_no: u32, 
        pre_apply: bool,
        recursion_depth: u32
    ) -> Result<()> {
        self.download_and_apply_block_worker(id, mc_seq_no, pre_apply, recursion_depth).await
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

    async fn download_state(
        &self, 
        block_id: &BlockIdExt, 
        master_id: &BlockIdExt,
        active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>,
        attempts: Option<usize>
    ) -> Result<(Arc<ShardStateStuff>, Arc<Vec<u8>>)> {
        let overlay = self.get_full_node_overlay(
            block_id.shard().workchain_id(), 
            block_id.shard().shard_prefix_with_tag()
        ).await?;
        crate::full_node::state_helper::download_persistent_state(
            block_id,
            master_id,
            overlay.deref(),
            self.clone(),
            active_peers,
            attempts,
            &|| {
                if self.check_stop() {
                    fail!("Persistent state downloading was stopped")
                }
                Ok(())
            }
        ).await
    }

    async fn download_zerostate(
        &self, 
        id: &BlockIdExt
    ) -> Result<(Arc<ShardStateStuff>, Vec<u8>)> {
        self.download_zerostate_worker(id, None).await
    }

    async fn store_block(&self, block: &BlockStuff) -> Result<BlockResult> {
        let result = self.db().store_block_data(block, None).await?;
        if let Some(handle) = result.clone().as_updated() {
            let id = block.id();
            if id.shard().is_masterchain() {
                let seq_no = id.seq_no();
                if handle.is_key_block()? {
                    self.update_last_known_keyblock_seqno(seq_no);
                }
                self.update_last_known_mc_block_seqno(seq_no);
            }
        }
        Ok(result)
    }

    async fn store_block_proof(
        &self, 
        id: &BlockIdExt,
        handle: Option<Arc<BlockHandle>>, 
        proof: &BlockProofStuff,
    ) -> Result<BlockResult> {
        self.db().store_block_proof(id, handle, proof, None).await
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

    async fn load_mc_zero_state(&self) -> Result<Arc<ShardStateStuff>> {
        let block_id = self.zero_state_id();
        self.load_state(block_id).await
    }

    async fn load_state(&self, block_id: &BlockIdExt) -> Result<Arc<ShardStateStuff>> {
        if let Some(state) = self.shard_states_cache().get(block_id) {
            self.update_shard_states_cache_stat(true);
            Ok(state)
        } else {
            let state = self.db().load_shard_state_dynamic(block_id)?;
            self.shard_states_cache().set(block_id.clone(), |_| Some(state.clone()))?;
            self.update_shard_states_cache_stat(false);
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

    async fn wait_state(
        self: Arc<Self>,
        id: &BlockIdExt,
        timeout_ms: Option<u64>,
        allow_block_downloading: bool
    ) -> Result<Arc<ShardStateStuff>> {
        loop {
            let has_state = || {
                Ok(self.load_block_handle(id)?.map(|h| h.has_state()).unwrap_or(false))
            };

            if has_state()? {
                break self.load_state(id).await
            }
            let id1 = id.clone();
            let engine = self.clone();
            if allow_block_downloading {
                tokio::spawn(async move {
                    if let Err(e) = engine.download_and_apply_block(&id1, 0, true).await {
                        log::error!("Error while pre-apply block (while wait_state) {}: {}", id1, e);
                    }
                });
            }
            if let Some(ss) = self.shard_states_awaiters().wait(id, timeout_ms, &has_state).await? {
                break Ok(ss)
            }
        }
    }

    async fn store_state(
        &self, 
        handle: &Arc<BlockHandle>, 
        state: Arc<ShardStateStuff>,
        state_bytes: Option<&[u8]>,
    ) -> Result<Arc<ShardStateStuff>> {
        let check_stop = || {
            if self.stopper().check_stop() {
                fail!("Stopped")
            }
            Ok(())
        };
        let (state, saved) = self.db().store_shard_state_dynamic(handle, &state, None, &check_stop).await?;
        if saved {
            #[cfg(feature = "telemetry")]
            self.full_node_telemetry().new_pre_applied_block(handle.got_by_broadcast());
        }
        if self.shard_states_cache().get(handle.id()).is_none() {
            self.shard_states_cache().set(handle.id().clone(), |_| Some(state.clone()))?;
        }
        self.shard_states_awaiters().do_or_wait(
            state.block_id(),
            None,
            async { Ok(state.clone()) }
        ).await?;
        if let Some(state_data) = state_bytes {
            self.db().store_shard_state_persistent_raw(&handle, state_data, None).await?;
        }
        Ok(state)
    }

    async fn store_zerostate(
        &self, 
        mut state: Arc<ShardStateStuff>, 
        state_bytes: &[u8]
    ) -> Result<(Arc<ShardStateStuff>, Arc<BlockHandle>)> {
        let handle = self.db().create_or_load_block_handle(
            state.block_id(), 
            None, 
            Some(state.state().gen_time()),
            None
        )?.as_non_updated().ok_or_else(
            || error!("INTERNAL ERROR: mismatch in zerostate storing")
        )?;
        state = self.store_state(&handle, state, Some(state_bytes)).await?;
        Ok((state, handle))
    }

    fn store_block_prev1(&self, handle: &Arc<BlockHandle>, prev: &BlockIdExt) -> Result<()> {
        self.db().store_block_prev1(handle, prev, None)
    }

    fn load_block_prev1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db().load_block_prev1(id)
    }

    fn store_block_prev2(&self, handle: &Arc<BlockHandle>, prev2: &BlockIdExt) -> Result<()> {
        self.db().store_block_prev2(handle, prev2, None)
    }

    fn load_block_prev2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db().load_block_prev2(id)
    }

    fn store_block_next1(&self, handle: &Arc<BlockHandle>, next: &BlockIdExt) -> Result<()> {
        self.db().store_block_next1(handle, next, None)
    }

    async fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db().load_block_next1(id)
    }

    fn store_block_next2(&self, handle: &Arc<BlockHandle>, next2: &BlockIdExt) -> Result<()> {
        self.db().store_block_next2(handle, next2, None)
    }

    async fn load_block_next2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db().load_block_next2(id)
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
        if self.ext_db().len() > 0 {
            if proof.is_some() && !handle.id().shard().is_masterchain() {
                fail!("Non master blocks should be processed without proof")
            }
            if proof.is_none() && handle.id().shard().is_masterchain() {
                let proof = self.load_block_proof(handle, false).await?;
                for db in self.ext_db() {
                    db.process_block(block, Some(&proof), state, prev_states, mc_seq_no).await?;
                }
            } else {
                for db in self.ext_db() {
                    db.process_block(block, proof, state, prev_states, mc_seq_no).await?;
                }
            }
        }
//        self.db().store_block_processed_in_ext_db(handle)?;
        Ok(())
    }

    async fn process_chain_range_in_ext_db(&self, chain_range: &ChainRange) -> Result<()> {
        for db in self.ext_db() {
            db.process_chain_range(chain_range).await?;
        }

        Ok(())
    }

    async fn process_full_state_in_ext_db(&self, state: &Arc<ShardStateStuff>)-> Result<()> {
        for db in self.ext_db() {
            db.process_full_state(state).await?;
        }
        Ok(())
    }

    async fn download_next_key_blocks_ids(
        &self, 
        block_id: &BlockIdExt, 
        _priority: u32
    ) -> Result<(Vec<BlockIdExt>, bool)> {
        let mc_overlay = self.get_masterchain_overlay().await?;
        mc_overlay.download_next_key_blocks_ids(block_id, 5).await
    }

    async fn set_applied(
        &self, 
        handle: &Arc<BlockHandle>, 
        mc_seq_no: u32
    ) -> Result<bool> {
        if handle.is_applied() {
            return Ok(false);
        }
        self.db().assign_mc_ref_seq_no(handle, mc_seq_no, None)?;
        self.db().archive_block(handle.id(), None).await?;
        if self.db().store_block_applied(handle, None)? {
            #[cfg(feature = "telemetry")]
            self.full_node_telemetry().new_applied_block();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn initial_sync_disabled(&self) -> bool { Engine::initial_sync_disabled(self) }

    fn init_mc_block_id(&self) -> &BlockIdExt { 
        (self as &Engine).init_mc_block_id() 
    }

    fn save_init_mc_block_id(&self, id: &BlockIdExt) -> Result<()> {
        self.db().save_full_node_state(INITIAL_MC_BLOCK, id)
    }

    async fn broadcast_to_public_overlay(
        &self, 
        to: &AccountIdPrefixFull, 
        data: &[u8]
    ) -> Result<BroadcastSendInfo> {
        let overlay = self.get_full_node_overlay(to.workchain_id, to.prefix).await?;
        overlay.broadcast_external_message(data).await
    }

    async fn redirect_external_message(&self, message_data: &[u8]) -> Result<BroadcastSendInfo> {
        match create_ext_message(message_data) {
            Err(e) => {
                let err = format!(
                    "Can't deserialize external message with len {}: {}",
                    message_data.len(), e,
                );
                log::warn!(target: EXT_MESSAGES_TRACE_TARGET, "{}", &err);
                fail!("{}", err);
            }
            Ok((id, message)) => {
                match redirect_external_message(self, message, id.clone(), message_data).await {
                    Err(e) => {
                        let err = format!(
                            "Can't redirect external message {:x}: {}",
                            id, e,
                        );
                        log::error!(target: EXT_MESSAGES_TRACE_TARGET, "{}", &err);
                        fail!("{}", err);
                    }
                    Ok(info) => {
                        log::debug!(
                            target: EXT_MESSAGES_TRACE_TARGET,
                            "Redirected external message {:x} to {} nodes by {} packages",
                            id, info.send_to, info.packets,
                        );
                        return Ok(info);
                    }
                }
            }
        }
    }

    async fn get_archive_id(&self, mc_seq_no: u32) -> Option<u64> {
        self.db().archive_manager().get_archive_id(mc_seq_no).await
    }

    async fn get_archive_slice(&self, archive_id: u64, offset: u64, limit: u32) -> Result<Vec<u8>> {
        self.db().archive_manager().get_archive_slice(archive_id, offset, limit).await
    }

    async fn download_archive(
        &self, 
        masterchain_seqno: u32,
        active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>
    ) -> Result<Option<Vec<u8>>> {
        let client = self.get_masterchain_overlay().await?;
        client.download_archive(masterchain_seqno, active_peers).await
    }

    async fn send_block_broadcast(&self, broadcast: BlockBroadcast) -> Result<()> {
        let overlay = self.get_full_node_overlay(
            broadcast.id.shard().workchain_id(),
            SHARD_FULL, //broadcast.id.shard as u64
        ).await?;
        overlay.send_block_broadcast(broadcast).await?;
        #[cfg(feature = "telemetry")]
        self.full_node_telemetry().sent_block_broadcast();
        Ok(())
    }

    async fn send_top_shard_block_description(
        &self,
        tbd: Arc<TopBlockDescrStuff>,
        cc_seqno: u32,
        resend: bool,
    ) -> Result<()> {
        let overlay = self.get_full_node_overlay(
            MASTERCHAIN_ID, // tbd.proof_for().shard().workchain_id(),
            SHARD_FULL, //tbd.proof_for().shard().shard_prefix_with_tag()
        ).await?;

        if !resend {
            let id = tbd.proof_for();
            if let Err(e) = self.shard_blocks().process_shard_block(
                id, cc_seqno, || Ok(tbd.clone()), false, false, self.deref()).await {
                log::error!("Can't add own shard top block {}: {}", id, e);
            }
        }
        
        overlay.send_top_shard_block_description(&tbd).await?;
        #[cfg(feature = "telemetry")]
        self.full_node_telemetry().sent_top_block_broadcast();
        Ok(())
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

    fn get_external_messages(&self, shard: &ShardIdent) -> Result<Vec<(Arc<Message>, UInt256)>> {
        self.external_messages().get_messages(shard, self.now())
    }

    fn complete_external_messages(&self, to_delay: Vec<UInt256>, to_delete: Vec<UInt256>) -> Result<()> {
        self.external_messages().complete_messages(to_delay, to_delete, self.now())
    }

    // Get current list of new shard blocks with respect to last mc block.
    // If given mc_seq_no is not equal to last mc seq_no - function fails.
    fn get_shard_blocks(&self, mc_seq_no: u32) -> Result<Vec<Arc<TopBlockDescrStuff>>> {
        self.shard_blocks().get_shard_blocks(mc_seq_no, false)
    }
    fn get_own_shard_blocks(&self, mc_seq_no: u32) -> Result<Vec<Arc<TopBlockDescrStuff>>> {
        self.shard_blocks().get_shard_blocks(mc_seq_no, true)
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

    fn db_root_dir(&self) -> Result<&str> {
        self.db().db_root_dir()
    }

    fn produce_chain_ranges_enabled(&self) -> bool {
        self.ext_db().iter().any(|ext_db| ext_db.process_chain_range_enabled())
    }

    #[cfg(feature = "telemetry")]
    fn full_node_telemetry(&self) -> &FullNodeTelemetry {
        Engine::full_node_telemetry(self)
    }

    #[cfg(feature = "telemetry")]
    fn collator_telemetry(&self) -> &CollatorValidatorTelemetry {
        Engine::collator_telemetry(self)
    }

    #[cfg(feature = "telemetry")]
    fn validator_telemetry(&self) -> &CollatorValidatorTelemetry {
        Engine::validator_telemetry(self)
    }

    #[cfg(feature = "telemetry")]
    fn full_node_service_telemetry(&self) -> &FullNodeNetworkTelemetry {
        Engine::full_node_service_telemetry(self)
    }

    #[cfg(feature = "telemetry")]
    fn engine_telemetry(&self) -> &Arc<EngineTelemetry> {
        Engine::engine_telemetry(self)
    }

    fn engine_allocated(&self) -> &Arc<EngineAlloc> {
        Engine::engine_allocated(self)
    }

    fn calc_tps(&self, period: u64) -> Result<u32> {
        self.tps_counter().calc_tps(period)
    }

    fn push_validated_block_stat(&self, stat: ValidatedBlockStat) -> Result<()> {
        self.validated_block_stats_sender().try_send(stat)?;
        Ok(())
    }

    fn pop_validated_block_stat(&self) -> Result<ValidatedBlockStat> {
        let result = self.validated_block_stats_receiver().try_recv()?;
        Ok(result)
    }

    fn adjust_states_gc_interval(&self, interval_ms: u32) {
        self.db().adjust_states_gc_interval(interval_ms)
    }

    fn acquire_stop(&self, mask: u32) {
        self.stopper().acquire_stop(mask);
    }

    fn check_stop(&self) -> bool {
        self.stopper().check_stop()
    }

    fn release_stop(&self, mask: u32) {
        self.stopper().release_stop(mask);
    }

    fn register_server(&self, server: Server) {
        Engine::register_server(self, server)
    }

}

async fn redirect_external_message(
    engine: &dyn EngineOperations, 
    message: Message, 
    id: UInt256,
    message_data: &[u8]
) -> Result<BroadcastSendInfo> {
    let message = Arc::new(message);
    engine.new_external_message(id.clone(), message.clone())?;
    if let Some(header) = message.ext_in_header() {
        let res = engine.broadcast_to_public_overlay(
            &AccountIdPrefixFull::checked_prefix(&header.dst)?,
            message_data
        ).await;
        #[cfg(feature = "telemetry")]
        engine.full_node_telemetry().sent_ext_msg_broadcast();
        jaeger::broadcast_sended(id.to_hex_string());
        res
    } else {
        fail!("External message is not properly formatted: {}", message)
    }
}
