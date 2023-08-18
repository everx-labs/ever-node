/*
* Copyright (C) 2019-2023 EverX. All Rights Reserved.
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
    config::{CollatorTestBundlesGeneralConfig, CollatorConfig},
    engine::{Engine, STATSD},
    engine_traits::{
        ChainRange, EngineAlloc, EngineOperations, PrivateOverlayOperations, Server,
        RempCoreInterface, RempDuplicateStatus
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
    validator::{
        candidate_db::CandidateDb,
        validator_manager::ValidationStatus,
        validator_utils::validatordescr_to_catchain_node,
    }
};
#[cfg(feature = "slashing")]
use crate::validator::slashing::ValidatedBlockStat;
#[cfg(feature = "telemetry")]
use crate::{
    engine_traits::EngineTelemetry, full_node::telemetry::{FullNodeTelemetry, RempClientTelemetry}, 
    network::telemetry::FullNodeNetworkTelemetry, 
    validator::telemetry::{CollatorValidatorTelemetry, RempCoreTelemetry},
};

use catchain::{
    CatchainNode, CatchainOverlay, CatchainOverlayListenerPtr, CatchainOverlayLogReplayListenerPtr
};
use overlay::{BroadcastSendInfo, PrivateOverlayShortId};
use std::{collections::HashSet, ops::Deref, sync::Arc};
use storage::block_handle_db::BlockHandle;
use ton_api::{
    serialize_boxed, 
    ton::ton_node::{
        RempMessage, RempMessageStatus, RempReceipt, 
        broadcast::{BlockBroadcast, QueueUpdateBroadcast}
    }
};
use ton_block::{
    MASTERCHAIN_ID, SHARD_FULL, GlobalCapabilities, OutMsgQueue,
    BlockIdExt, AccountIdPrefixFull, ShardIdent, Message
};
#[cfg(feature="workchains")]
use ton_block::{BASE_WORKCHAIN_ID, INVALID_WORKCHAIN_ID};
use ton_types::{error, fail, KeyId, KeyOption, Result, UInt256};
use validator_session::{BlockHash, SessionId, ValidatorBlockCandidate};

#[async_trait::async_trait]
impl EngineOperations for Engine {

    fn processed_workchain(&self) -> Option<i32> {
        self.processed_workchain()
    }

    async fn is_foreign_wc(&self, workchain_id: i32) -> Result<(bool, i32)> {
        let cap_workchains = self.load_actual_config_params().await?.has_capability(GlobalCapabilities::CapWorkchains);
        if let Some(own_workchain_id) = self.processed_workchain() {
            if !cap_workchains {
                Ok((false, own_workchain_id))
            } else if workchain_id == MASTERCHAIN_ID {
                Ok((false, own_workchain_id)) // masterchain is always no foreign
            } else {
                Ok((own_workchain_id != workchain_id, own_workchain_id))
            }
        } else {
            if cap_workchains {
                fail!("CapWorkchains is set but workchain was not specified in node's config")
            }
            Ok((false, workchain_id))
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

    fn validation_status(&self) -> ValidationStatus {
        self.validation_status()
    }

    fn set_validation_status(&self, status: ValidationStatus) {
        self.set_validation_status(status)
    }

    fn last_validation_time(&self) -> &lockfree::map::Map<ShardIdent, u64> {
        self.last_validation_time()
    }

    fn set_last_validation_time(&self, shard: ShardIdent, time: u64) {
        self.set_last_validation_time(shard, time)
    }

    fn remove_last_validation_time(&self, shard: &ShardIdent) {
        self.remove_last_validation_time(shard)
    }

    fn last_collation_time(&self) -> &lockfree::map::Map<ShardIdent, u64> {
        self.last_collation_time()
    }

    fn set_last_collation_time(&self, shard: ShardIdent, time: u64) {
        self.set_last_collation_time(shard, time)
    }
    
    fn remove_last_collation_time(&self, shard: &ShardIdent) {
        self.remove_last_collation_time(shard)
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

    async fn wait_applied_block(&self, id: &BlockIdExt, timeout_ms: Option<u64>) -> Result<Arc<BlockHandle>> {
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
                    return Ok(handle)
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
        let handle = loop {
            if prev_handle.has_next1() {
                let id = self.load_block_next1(prev_handle.id()).await?;
                break self.wait_applied_block(&id, timeout_ms).await?;
            } else {
                if let Some(id) = self.next_block_applying_awaiters()
                    .wait(prev_handle.id(), timeout_ms, || Ok(prev_handle.has_next1())).await? {
                    if let Some(handle) = self.load_block_handle(&id)? {
                        break handle;
                    }
                }
            }
        };
        let block = self.load_block(&handle).await?;
        Ok((handle, block))
    }

    async fn find_mc_block_by_seq_no(&self, seqno: u32) -> Result<Arc<BlockHandle>> {
        let last_state = self.load_last_applied_mc_state().await?;
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
    ) -> Result<(BlockStuff, Option<BlockProofStuff>)> {
        loop {
            if let Some(handle) = self.load_block_handle(id)? {
                if handle.has_data() {
                    let block = self.load_block(&handle).await?;
                    let proof = if !handle.is_queue_update() {
                        Some(self.load_block_proof(&handle, !id.shard().is_masterchain()).await?)
                    } else {
                        None
                    };
                    return Ok((block, proof))
                }
            }

            let (foreign_block, own_wc) = self.is_foreign_wc(id.shard().workchain_id()).await?;

            if foreign_block {
                if let Some(queue_update) = self.download_queue_update_awaiters().do_or_wait(
                    id,
                    None,
                    self.download_queue_update_worker(id, own_wc, limit, None)
                ).await? {
                    return Ok((queue_update, None))
                }
            } else {
                if let Some((block, proof)) = self.download_block_awaiters().do_or_wait(
                    id,
                    None,
                    self.download_block_worker(id, limit, None)
                ).await? {
                    return Ok((block, Some(proof)))
                }
            }
        }
    }

    async fn download_block_proof(&self, id: &BlockIdExt, is_link: bool, key_block: bool) -> Result<BlockProofStuff> {
        self.download_block_proof_worker(id, is_link, key_block, None).await
    }

    async fn download_next_block(&self, prev_id: &BlockIdExt) -> Result<(BlockStuff, BlockProofStuff)> {
        self.download_next_block_worker(prev_id, None).await
    }

    async fn download_and_store_state(
        &self, 
        handle: &Arc<BlockHandle>,
        root_hash: &UInt256,
        master_id: &BlockIdExt,
        active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>,
        attempts: Option<usize>
    ) -> Result<Arc<ShardStateStuff>> {


        let (is_foreign_block, own_wc) = self.is_foreign_wc(handle.id().shard().workchain_id()).await?;
        let (queue_for_wc, overlay_wc) = if is_foreign_block {
            (Some(own_wc), own_wc)
        } else {
            (None, handle.id().shard().workchain_id())
        };

        let overlay = self.get_full_node_overlay(overlay_wc, SHARD_FULL).await?;

        let data = crate::full_node::state_helper::download_persistent_state(
            handle.id(),
            queue_for_wc,
            master_id,
            overlay.deref(),
            active_peers,
            attempts,
            &|| {
                if self.check_stop() {
                    fail!("Persistent state downloading was stopped")
                }
                Ok(())
            }
        ).await?;

        let state = self.shard_states_keeper().check_and_store_state(
            handle, root_hash, data, self.low_memory_mode()).await?;

        Ok(state)
    }

    async fn download_zerostate(
        &self, 
        id: &BlockIdExt
    ) -> Result<(Arc<ShardStateStuff>, Vec<u8>)> {
        self.download_zerostate_worker(id, None).await
    }

    async fn store_block(&self, block: &BlockStuff) -> Result<BlockResult> {
        let result = self.db().store_block_data(block, None).await?;
        if let Some(handle) = result.clone().to_updated() {
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

    fn create_handle_for_empty_queue_update(
        &self,
        block: &BlockStuff // virt block constructed from proof of update 
                           // (block's BOC contains only queue update, other cells are pruned)
    ) -> Result<BlockResult> {
        let target_wc = block.is_queue_update_for()
            .ok_or_else(|| error!("Block {} is not a queue update", block.id()))?;
        self.db().create_or_load_block_handle(
            block.id(),
            Some(block.block_or_queue_update()?),
            Some((target_wc, true)),
            None,
            None
        )
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
        self.shard_states_keeper().load_state(block_id).await
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
                        log::error!("Error while pre-apply block (while wait_state) {}: {:?}", id1, e);
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
    ) -> Result<Arc<ShardStateStuff>> {
        let (state, saved) =
            self.shard_states_keeper().store_state(handle, state, None, false).await?;
        if saved {
            #[cfg(feature = "telemetry")]
            self.full_node_telemetry().new_pre_applied_block(handle.got_by_broadcast());
        }
        self.shard_states_awaiters().do_or_wait(
            state.block_id(),
            None,
            async { Ok(state.clone()) }
        ).await?;
        Ok(state)
    }

    async fn store_zerostate(
        &self, 
        state: Arc<ShardStateStuff>, 
        state_bytes: &[u8]
    ) -> Result<(Arc<ShardStateStuff>, Arc<BlockHandle>)> {
        let handle = self.db().create_or_load_block_handle(
            state.block_id(), 
            None,
            None,
            Some(state.state()?.gen_time()),
            None
        )?.to_non_updated().ok_or_else(
            || error!("INTERNAL ERROR: mismatch in zerostate storing")
        )?;
        let (state, _) =
            self.shard_states_keeper().store_state(&handle, state, Some(state_bytes), false).await?;
        self.shard_states_awaiters().do_or_wait(
            state.block_id(),
            None,
            async { Ok(state.clone()) }
        ).await?;
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

    fn load_block_prev2(&self, id: &BlockIdExt) -> Result<Option<BlockIdExt>> {
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

    async fn load_block_next2(&self, id: &BlockIdExt) -> Result<Option<BlockIdExt>> {
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

        // Initialisation of remp_capability after cold boot by first processed master state
        if state.block_id().shard().is_masterchain() {
            self.set_remp_capability(
                state.config_params()?.has_capability(GlobalCapabilities::CapRemp),
            );
        }
        Ok(())
    }

    async fn process_remp_msg_status_in_ext_db(
        &self,
        id: &UInt256,
        status: &RempReceipt,
        signature: &[u8],
    ) -> Result<()> {
        for db in self.ext_db() {
            db.process_remp_msg_status(id, status, signature).await?;
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
        if handle.id().seq_no() != 0 {
            self.db().archive_block(handle.id(), None).await?;
        }
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

    async fn redirect_external_message(&self, message_data: &[u8], id: UInt256) -> Result<()> {
        if !self.check_sync().await? {
            fail!("Can't process external message because node is out of sync");
        }

        let remp_way = self.remp_capability();
        if remp_way {
            self.remp_client()
                .ok_or_else(|| error!("redirect_external_message: remp client is not set"))?
                .clone()
                .process_remp_message(message_data.into(), id.clone());
            log::debug!(
                target: EXT_MESSAGES_TRACE_TARGET,
                "Redirected external message {:x} to REMP",
                id,
            );
            Ok(())
        } else {
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
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    async fn get_archive_id(&self, mc_seq_no: u32) -> Option<u64> {
        self.db().get_archive_id(mc_seq_no).await
    }

    async fn get_archive_slice(&self, archive_id: u64, offset: u64, limit: u32) -> Result<Vec<u8>> {
        self.db().get_archive_slice(archive_id, offset, limit).await
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
        let mut target_wcs = vec!();

        // If Wc2WcQueueUpdates enabled - send master block to all WCs
        if broadcast.id.shard().is_masterchain() {
            let mc_state = self.load_last_applied_mc_state().await?;
            if mc_state.config_params()?.has_capability(GlobalCapabilities::CapWorkchains) {
                mc_state.shard_state_extra()?.shards().iterate_with_keys(|workchain_id: i32, _| {
                    target_wcs.push(workchain_id);
                    Ok(true)
                })?;
                target_wcs.push(MASTERCHAIN_ID);
            } else {
                target_wcs.push(broadcast.id.shard().workchain_id());
            }
        } else {
            target_wcs.push(broadcast.id.shard().workchain_id());
        }

        for wc in target_wcs {
            log::trace!("send_block_broadcast {} to {}", broadcast.id, wc);
            let overlay = self.get_full_node_overlay(wc, SHARD_FULL).await?;
            overlay.send_block_broadcast(broadcast.clone()).await?;
        }

        #[cfg(feature = "telemetry")]
        self.full_node_telemetry().sent_block_broadcast();

        Ok(())
    }

    async fn send_queue_update_broadcast(&self, broadcast: QueueUpdateBroadcast) -> Result<()> {
        // TODO select right overlay
        let overlay = self.get_full_node_overlay(
            broadcast.target_wc,
            SHARD_FULL, //broadcast.id.shard as u64
        ).await?;
        
        log::trace!("send_queue_update_broadcast {} to {}", broadcast.id, broadcast.target_wc);
        overlay.send_queue_update_broadcast(broadcast).await?;
        #[cfg(feature = "telemetry")]
        self.full_node_telemetry().sent_block_broadcast(); // TODO
        Ok(())
    }

    async fn send_top_shard_block_description(
        &self,
        tbd: Arc<TopBlockDescrStuff>,
        cc_seqno: u32,
        is_resend: bool,
    ) -> Result<()> {

        if !is_resend {
            let id = tbd.proof_for();
            if let Err(e) = self.shard_blocks().process_shard_block(
                id, cc_seqno, || Ok(tbd.clone()), false, self.deref()).await {
                log::error!("Can't add own shard top block {}: {}", id, e);
            }
        }

        let mut target_wcs = vec!();
        if self.load_actual_config_params().await?.has_capability(GlobalCapabilities::CapWorkchains) {
            target_wcs.push(tbd.proof_for().shard().workchain_id());
            target_wcs.push(MASTERCHAIN_ID);
        } else {
            target_wcs.push(MASTERCHAIN_ID);
        }

        for wc in target_wcs {
            let overlay = self.get_full_node_overlay(wc, SHARD_FULL).await?;
            overlay.send_top_shard_block_description(&tbd).await?;
        }

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

    // Remp messages
    fn new_remp_message(&self, id: UInt256, message: Arc<Message>) -> Result<()> {
        self.remp_messages()?.new_message(id, message)
    }
    fn get_remp_messages(&self, shard: &ShardIdent) -> Result<Vec<(Arc<Message>, UInt256)>> {
        self.remp_messages()?.get_messages(shard)
    }
    fn finalize_remp_messages(
        &self,
        block: BlockIdExt,
        accepted: Vec<UInt256>,
        rejected: Vec<(UInt256, String)>,
        ignored: Vec<UInt256>,
    ) -> Result<()> {
        self.remp_messages()?.finalize_messages(block, accepted, rejected, ignored)
    }
    fn finalize_remp_messages_as_ignored(&self, block_id: &BlockIdExt)
    -> Result<()> {
        self.remp_messages()?.finalize_remp_messages_as_ignored(block_id)
    }
    fn dequeue_remp_message_status(&self) -> Result<Option<(UInt256, Arc<Message>, RempMessageStatus)>> {
        self.remp_messages()?.dequeue_message_status()
    }

    async fn check_remp_duplicate(&self, message_id: &UInt256) -> Result<RempDuplicateStatus> {
        self.remp_service()
            .ok_or_else(|| error!("Can't get message status because remp service was not set"))?
            .remp_core_interface()?
            .check_remp_duplicate(message_id)
    }

    // Get current list of new shard blocks with respect to last mc block.
    // If given mc_seq_no is not equal to last mc seq_no - function fails.
    async fn get_shard_blocks(
        &self,
        mc_state: &Arc<ShardStateStuff>,
        actual_last_mc_seqno: Option<&mut u32>,
    ) -> Result<Vec<Arc<TopBlockDescrStuff>>> {
        self.shard_blocks().get_shard_blocks(mc_state, self, false, actual_last_mc_seqno).await
    }
    async fn get_own_shard_blocks(
        &self, 
        mc_state: &Arc<ShardStateStuff>
    ) -> Result<Vec<Arc<TopBlockDescrStuff>>> {
        self.shard_blocks().get_shard_blocks(mc_state, self, true, None).await
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

    fn collator_config(&self) -> &CollatorConfig {
        Engine::collator_config(self)
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
    fn remp_core_telemetry(&self) -> &RempCoreTelemetry {
        Engine::remp_core_telemetry(self)
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
    fn remp_client_telemetry(&self) -> &RempClientTelemetry {
        Engine::remp_client_telemetry(self)
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

    #[cfg(feature = "slashing")]
    fn push_validated_block_stat(&self, stat: ValidatedBlockStat) -> Result<()> {
        Ok(self.validated_block_stats_sender().try_send(stat)?)
    }

    #[cfg(feature = "slashing")]
    fn pop_validated_block_stat(&self) -> Result<ValidatedBlockStat> {
        Ok(self.validated_block_stats_receiver().try_recv()?)
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

    fn send_remp_message(&self, to: Arc<KeyId>, message: &RempMessage) -> Result<()> {
        self.network().remp().send_message(to, message)
    }

    /*async fn sign_and_send_remp_receipt(&self, to: Arc<KeyId>, receipt: RempReceipt) -> Result<()> {
        let validators: Vec<CatchainNode> = self.load_actual_config_params().await?
            .validator_set()?.list()
            .iter().map(|vd| validatordescr_to_catchain_node(vd)).collect();
        let (key, adnl_id) = self.network
            .get_validator_key(&validators).await?
            .ok_or_else(|| error!("Can't get validator's key"))?;
        if key.id().data() != receipt.source_id().as_slice() {
            fail!("given source_id {} is not correspond to key {}", hex::encode(receipt.source_id().as_slice()), hex::encode(key.id().data()))
        }
        let receipt_bytes = serialize_boxed(&receipt)?;
        let signature = key.sign(&receipt_bytes)?.as_slice().try_into()?;
        // let signed_receipt = RempSignedReceipt::TonNode_RempSignedReceipt (
        //     ton_api::ton::ton_node::rempsignedreceipt::RempSignedReceipt {
        //         receipt: receipt_bytes.into(),
        //         signature: ton_api::ton::int512(signature),
        //     }
        // );

        // self.network().remp().send_receipt(to, receipt.message_id(), signed_receipt).await
        self.network().remp().combine_and_send_receipt(to, receipt, signature, adnl_id).await

    }*/

    async fn send_remp_receipt(&self, to: Arc<KeyId>, receipt: RempReceipt) -> Result<()> {
        let validators: Vec<CatchainNode> = self.load_actual_config_params().await?
            .validator_set()?.list()
            .iter().map(|vd| validatordescr_to_catchain_node(vd)).collect();
        let (key, adnl_id) = self.network
            .get_validator_key(&validators).await?
            .ok_or_else(|| error!("Can't get validator's key"))?;
        if key.id().data() != receipt.source_id().as_slice() {
            fail!("given source_id {} is not correspond to key {}", hex::encode(receipt.source_id().as_slice()), hex::encode(key.id().data()))
        }
        self.network().remp().combine_and_send_receipt(to, receipt, adnl_id).await
    }

    fn sign_remp_receipt(&self, receipt: &RempReceipt) -> Result<Vec<u8>> {
        let receipt_bytes = serialize_boxed(receipt)?;
        let key = self.network.public_overlay_key()?;
        if key.id().data() != receipt.source_id().as_slice() {
            fail!("given source_id {} is not correspond to key {}", hex::encode(receipt.source_id().as_slice()), hex::encode(key.id().data()))
        }
        let signature = key.sign(&receipt_bytes)?;
        Ok(signature)
    }

    async fn update_validators(
        &self,
        to_resolve: Vec<CatchainNode>,
        to_delete: Vec<CatchainNode>
    ) -> Result<()> {

        log::info!("Validators resolving for remp was started");

        if to_resolve.len() > 0 {
            // TODO support callback and breaker flag
            self.network().search_validator_keys_for_full_node(to_resolve, Arc::new(|_| {}))?;
        }
        if to_delete.len() > 0 {
            self.network().delete_validator_keys_for_full_node(to_delete)?;
        }

        log::info!("Validators resolving for remp has finished");
        Ok(())
    }

    fn set_remp_core_interface(&self, rci: Arc<dyn RempCoreInterface>) -> Result<()> {
        if let Some(rs) = self.remp_service() {
            rs.set_remp_core_interface(rci)?;
        } else {
            log::warn!("Attempt to set remp_core_interface while remp service is disabled");
        }
        Ok(())
    }

    // returns true if there were no either calculating or done queues before
    fn set_split_queues_calculating(&self, before_split_block: &BlockIdExt) -> bool {
        // insert None is there was not value before and return true
        // return false if there was any value (None or Some) before
        adnl::common::add_unbound_object_to_map_with_update(
            self.split_queues_cache(),
            before_split_block.clone(),
            |v| {
                if v.is_some() {
                    fail!("");
                }
                Ok(Some(None))
            }
        ).is_ok()
    }

    fn set_split_queues(
        &self,
        before_split_block: &BlockIdExt,
        queue0: OutMsgQueue,
        queue1: OutMsgQueue,
        visited_cells: HashSet<UInt256>,
    ) {
        self.split_queues_cache().insert(
            before_split_block.clone(),
            Some((queue0, queue1, visited_cells))
        );
    }

    fn get_split_queues(
        &self,
        before_split_block: &BlockIdExt
    ) -> Option<(OutMsgQueue, OutMsgQueue, HashSet<UInt256>)> {
        if let Some(guard) = self.split_queues_cache().get(before_split_block) {
            if let Some(q) = guard.val() {
                return Some(q.clone())
            }
        }
        None
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
