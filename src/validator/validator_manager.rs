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
    config::{RempConfig, ValidatorManagerConfig},
    engine::Engine,
    engine_traits::EngineOperations,
    shard_state::ShardStateStuff,
    validator::{
        remp_block_parser::{RempMasterblockObserver, find_previous_sessions},
        remp_manager::{RempManager, RempInterfaceQueues},
        sessions_computing::{GeneralSessionInfo, SessionValidatorsInfo, SessionValidatorsList, SessionValidatorsCache},
        fabric::run_validate_query_any_candidate,
        validator_group::{ValidatorGroup, ValidatorGroupStatus},
        validator_utils::{
            get_first_block_seqno_after_prevs,
            get_masterchain_seqno, get_block_info_by_id,
            compute_validator_list_id, get_group_members_by_validator_descrs, 
            is_remp_enabled, try_calc_subset_for_workchain,
            validatordescr_to_catchain_node, validatorset_to_string,
            ValidatorListHash, ValidatorSubsetInfo,
            try_calc_vset_for_workchain,
        },
        out_msg_queue::OutMsgQueueInfoStuff,
    },
};
use crate::validator::validator_utils::try_calc_subset_for_workchain_standard;

#[cfg(feature = "slashing")]
use crate::validator::slashing::{SlashingManager, SlashingManagerPtr};

use catchain::{CatchainNode, PublicKey, serialize_tl_boxed_object};
use std::{
    cmp::min,
    collections::{HashMap, HashSet}, convert::TryFrom,
    ops::RangeInclusive,
    sync::Arc,
    time::{Duration, SystemTime}
};
use crate::validator::verification::{VerificationFactory, VerificationListener, VerificationManagerPtr};
use crate::validator::verification::GENERATE_MISSING_BLS_KEY;
use crate::validator::BlockCandidate;
use spin::mutex::SpinMutex;
use tokio::time::timeout;
use ton_api::IntoBoxed;
use ton_block::{
    BlockIdExt, ConfigParamEnum, ConsensusConfig, FutureSplitMerge, McStateExtra, ShardDescr, ShardIdent, 
    ValidatorDescr, ValidatorSet, BASE_WORKCHAIN_ID, MASTERCHAIN_ID, GlobalCapabilities, CatchainConfig, 
    UnixTime32
};
use ton_types::{error, fail, Result, UInt256};

use crate::block::BlockIdExtExtention;

fn get_session_id_serialize(
    session_info: Arc<GeneralSessionInfo>,
    vals: &Vec<ValidatorDescr>,
    new_catchain_ids: bool
) -> catchain::RawBuffer {
    let mut members = Vec::new();
    get_group_members_by_validator_descrs(vals, &mut members);

    if !new_catchain_ids {
        unimplemented!("Old catchain ids format is not supported")
    } else {
        serialize_tl_boxed_object!(&ton_api::ton::validator::group::GroupNew {
            workchain: session_info.shard.workchain_id(),
            shard: session_info.shard.shard_prefix_with_tag() as i64,
            vertical_seqno: session_info.max_vertical_seqno as i32,
            last_key_block_seqno: session_info.key_seqno as i32,
            catchain_seqno: session_info.catchain_seqno as i32,
            config_hash: session_info.opts_hash.clone(),
            members: members.into()
        }
        .into_boxed())
    }
}

/// serialize data and calc sha256
fn get_session_id(
    session_info: Arc<GeneralSessionInfo>,
    val_set: &Vec<ValidatorDescr>,
    new_catchain_ids: bool,
) -> UInt256 {
    let serialized = get_session_id_serialize(
        session_info,
        val_set,
        new_catchain_ids
    );
    UInt256::calc_file_hash(&serialized)
}

fn compute_session_unsafe_serialized(session_id: &UInt256, rotate_id: u32) -> Vec<u8> {
    let mut unsafe_id_serialized: Vec<u8> = session_id.as_slice().to_vec();
    let mut rotate_id_serialized: Vec<u8> = rotate_id.to_le_bytes().to_vec();
    unsafe_id_serialized.append(&mut rotate_id_serialized);
    return unsafe_id_serialized
}

/// Computes session_id and if unsafe rotation is taking place,
/// replaces session_id with unsafe rotation session id.
fn get_session_unsafe_id(
    session_info: Arc<GeneralSessionInfo>,
    val_set: &Vec<ValidatorDescr>,
    new_catchain_ids: bool,
    prev_block_opt: Option<u32>,
    vm_config: &ValidatorManagerConfig,
) -> UInt256 {
    let session_id = get_session_id(session_info.clone(), val_set, new_catchain_ids);

    if session_info.shard.is_masterchain() {
        if let Some(rotate_id) = vm_config.check_unsafe_catchain_rotation(prev_block_opt, session_info.catchain_seqno) {
            let unsafe_serialized = compute_session_unsafe_serialized(&session_id, rotate_id);
            let unsafe_id = UInt256::calc_file_hash(unsafe_serialized.as_slice());

            log::warn!(
                target: "validator",
                "Unsafe master session rotation: session {} at block={:?}, cc={} -> rotate_id={}, new session {}",
                session_id.to_hex_string(),
                prev_block_opt,
                session_info.catchain_seqno,
                rotate_id,
                unsafe_id.to_hex_string()
            );
            return unsafe_id;
        }
    }
    return session_id;
}

fn validator_session_options_serialize(
    opts: &validator_session::SessionOptions,
) -> catchain::RawBuffer {
    serialize_tl_boxed_object!(&ton_api::ton::validator_session::config::ConfigNew {
        catchain_idle_timeout: opts.catchain_idle_timeout.as_secs_f64().into(),
        catchain_max_deps: i32::try_from(opts.catchain_max_deps).unwrap(),
        round_candidates: i32::try_from(opts.round_candidates).unwrap(),
        next_candidate_delay: opts.next_candidate_delay.as_secs_f64().into(),
        round_attempt_duration: i32::try_from(opts.round_attempt_duration.as_secs()).unwrap(),
        max_round_attempts: i32::try_from(opts.max_round_attempts).unwrap(),
        max_block_size: i32::try_from(opts.max_block_size).unwrap(),
        max_collated_data_size: i32::try_from(opts.max_collated_data_size).unwrap(),
        new_catchain_ids: ton_api::ton::Bool::from(opts.new_catchain_ids)
    }
    .into_boxed())
}

fn get_validator_session_options_hash(opts: &validator_session::SessionOptions) -> (UInt256, catchain::RawBuffer) {
    let serialized = validator_session_options_serialize(opts);
    (UInt256::calc_file_hash(&serialized), serialized)
}

fn get_session_options(opts: &ConsensusConfig) -> validator_session::SessionOptions {
    let default_opts = validator_session::SessionOptions::default();

    validator_session::SessionOptions {
        catchain_idle_timeout: std::time::Duration::from_millis(opts.consensus_timeout_ms.into()),
        catchain_max_deps: opts.catchain_max_deps,
        catchain_skip_processed_blocks: false, // Debugging option, not found in consensus config
        round_candidates: opts.round_candidates,
        next_candidate_delay: std::time::Duration::from_millis(opts.next_candidate_delay_ms.into()),
        round_attempt_duration: std::time::Duration::from_secs(opts.attempt_duration.into()),
        max_round_attempts: opts.fast_attempts,
        max_block_size: opts.max_block_bytes,
        max_collated_data_size: opts.max_collated_bytes,
        new_catchain_ids: opts.new_catchain_ids,
        skip_single_node_session_validations: false, // This should be set to true for single-node sessions
        catchain_receiver_max_neighbours_count: default_opts.catchain_receiver_max_neighbours_count,
        catchain_receiver_neighbours_sync_min_period: default_opts.catchain_receiver_neighbours_sync_min_period,
        catchain_receiver_neighbours_sync_max_period: default_opts.catchain_receiver_neighbours_sync_max_period,
        catchain_receiver_max_sources_sync_attempts: default_opts.catchain_receiver_max_sources_sync_attempts,
        catchain_receiver_neighbours_rotate_min_period: default_opts.catchain_receiver_neighbours_rotate_min_period,
        catchain_receiver_neighbours_rotate_max_period: default_opts.catchain_receiver_neighbours_rotate_max_period,    
    }
}

async fn clear_catchains_cache(path_str: String) -> Result<()>{
    log::info!(target: "validator_manager", "Clearing catchains cache...");
    let removed = tokio::task::spawn_blocking(move || -> Result<u32> {
        let entries = std::fs::read_dir(path_str)?;
        let mut removed = 0;
        for entry in entries {
            let path = entry?.path();
            if path.is_dir() {
                if let Err(err) = std::fs::remove_dir_all(path.clone()) {
                    log::warn!("Error clearing catchains cache {}: {}", path.display(), err);
                } else {
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }).await??;
    log::info!(target: "validator_manager", "Cleared catchains cache, removed {} entries", removed);
    Ok(())
}

#[repr(u8)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
pub enum ValidationStatus {
    Disabled = 0,
    Waiting = 1,
    Countdown = 2,
    Active = 3
}

impl ValidationStatus {
    fn allows_validate(&self) -> bool {
        match self {
            Self::Disabled | Self::Waiting => false,
            Self::Countdown | Self::Active => true
        }
    }
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => ValidationStatus::Waiting,
            2 => ValidationStatus::Countdown,
            3 => ValidationStatus::Active,
            _ => ValidationStatus::Disabled,
        }
    }
}

struct ValidatorListStatus {
    known_lists: HashMap<ValidatorListHash, PublicKey>,
    curr: Option<ValidatorListHash>,
    next: Option<ValidatorListHash>,
    curr_utime_since: Option<u32>,
    next_utime_since: Option<u32>,
}

impl ValidatorListStatus {
    fn add_list (&mut self, list_id: ValidatorListHash, key: PublicKey) {
        self.known_lists.insert(list_id, key);
    }

    fn contains_list (&self, list_id: &ValidatorListHash) -> bool {
        return self.known_lists.contains_key(list_id);
    }

    fn remove_list (&mut self, list_id: &ValidatorListHash) {
        self.known_lists.remove(list_id);
    }

    fn get_list (&self, list_id: &ValidatorListHash) -> Option<PublicKey> {
        return match self.known_lists.get(list_id) {
            None => None,
            Some(ch) => Some(ch.clone())
        }
    }

    fn get_local_key (&self) -> Option<PublicKey> {
        return match &self.curr {
            None => None,
            Some(ch) => self.get_list (&ch)
        }
    }

    fn actual_or_coming (&self, list_id: &ValidatorListHash) -> bool {
        match &self.curr {
            Some(curr_id) if list_id == curr_id => return true,
            _ => ()
        };

        match &self.next {
            Some(next_id) if list_id == next_id => return true,
            _ => return false
        }
    }

    fn known_hashes (&self) -> HashSet<ValidatorListHash> {
        return self.known_lists.keys().cloned().collect();
    }

    fn get_curr_utime_since(&self) -> &Option<u32> {
        &self.curr_utime_since
    }
}

impl Default for ValidatorListStatus {
    fn default() -> ValidatorListStatus {
        return ValidatorListStatus {
            known_lists: HashMap::default(),
            curr: None,
            next: None,
            curr_utime_since: None,
            next_utime_since: None,
        }
    }
}

fn rotate_all_shards(mc_state_extra: &McStateExtra) -> bool {
    mc_state_extra.validator_info.nx_cc_updated
}

struct VerificationListenerImpl {
    engine: Arc<dyn EngineOperations>,
}

#[async_trait::async_trait]
impl VerificationListener for VerificationListenerImpl {
    async fn verify(&self, block_candidate: &BlockCandidate) -> bool {
        ValidatorManagerImpl::verify_block_candidate(block_candidate, self.engine.clone()).await
    }
}

impl VerificationListenerImpl {
    fn new(engine: Arc<dyn EngineOperations>) -> Self {
        Self {
            engine
        }
    }
}

struct ValidatorManagerImpl {
    engine: Arc<dyn EngineOperations>,
    rt: tokio::runtime::Handle,
    validator_sessions: HashMap<UInt256, Arc<ValidatorGroup>>, // Sessions: both actual (started) and future
    validator_list_status: ValidatorListStatus,
    session_info_cache: SessionValidatorsCache,
    config: ValidatorManagerConfig,
    remp_config: RempConfig,

    remp_manager: Option<Arc<RempManager>>,
    block_observer: Option<RempMasterblockObserver>,

    #[cfg(feature = "slashing")]
    slashing_manager: SlashingManagerPtr,
    verification_manager: SpinMutex<Option<VerificationManagerPtr>>,
    verification_listener: SpinMutex<Option<Arc<VerificationListenerImpl>>>,
}

impl ValidatorManagerImpl {
    fn create(
        engine: Arc<dyn EngineOperations>,
        rt: tokio::runtime::Handle,
        config: ValidatorManagerConfig,
        remp_config: RempConfig,
    ) -> (Self, Option<Arc<RempInterfaceQueues>>) {
        let (remp_manager, remp_interface_queues) = if remp_config.is_service_enabled() {
            let (m, i) = RempManager::create_with_options(engine.clone(), remp_config.clone(), Arc::new(rt.clone()));
            (Some(Arc::new(m)), Some(Arc::new(i)))
        } else {
            (None, None)
        };

        engine.set_validation_status(ValidationStatus::Disabled);
        (ValidatorManagerImpl {
            engine: engine.clone(),
            rt: rt.clone(),
            validator_sessions: HashMap::default(),
            validator_list_status: ValidatorListStatus::default(),
            session_info_cache: SessionValidatorsCache::new(),
            config,
            remp_config,
            remp_manager,
            block_observer: None,
            #[cfg(feature = "slashing")]
            slashing_manager: SlashingManager::create(),
            verification_manager: SpinMutex::new(None),
            verification_listener: SpinMutex::new(None),
        }, remp_interface_queues)
    }

    /// Create/destroy verification manager
    fn update_verification_manager_usage(&self, mc_state_extra: &McStateExtra) {
        let smft_enabled = !self.config.smft_disabled && crate::validator::validator_utils::is_smft_enabled(self.engine.clone(), mc_state_extra.config());
        let verification_manager = self.verification_manager.lock().clone();
        let has_verification_manager = verification_manager.is_some();

        if smft_enabled && has_verification_manager {
            return; //verification manager is enabled and created
        }
        if !smft_enabled && !has_verification_manager {
            return; //verification manager is disabled and destroyed
        }

        if !smft_enabled && has_verification_manager {
            //SMFT is disabled, but verification manager is created
            log::info!(target: "verificator", "Destroy verification manager for validator manager");

            *self.verification_manager.lock() = None;
            *self.verification_listener.lock() = None;

            return;
        }

        if smft_enabled && !has_verification_manager {
            //SMFT is enabled, but verification manager has not been created

            log::info!(target: "verificator", "Initialize verification manager for validator manager");

            let verification_manager = VerificationFactory::create_manager(self.engine.clone(), self.rt.clone());
            let verification_listener = Arc::new(VerificationListenerImpl::new(self.engine.clone()));

            *self.verification_manager.lock() = Some(verification_manager);
            *self.verification_listener.lock() = Some(verification_listener);

            log::info!(target: "verificator", "Verification manager for validator manager has been initialized");

            return;
        }
    }

    /// find own key in validator subset
    fn find_us(&self, validators: &[ValidatorDescr]) -> Option<PublicKey> {
        if let Some(lk) = self.validator_list_status.get_local_key() {
            let local_keyhash = lk.id().data();
            for val in validators {
                let pkhash = val.compute_node_id_short();
                if pkhash.as_slice() == local_keyhash {
                    //log::info!(target: "validator_manager", "Comparing {} with {}", pkhash, local_keyhash);
                    //log::info!(target: "validator_manager", "({:?})", pk.pub_key().unwrap());
                    //compute public key hash
                    return Some(lk)
                }
            }
        }
        None
    }

    async fn update_single_validator_list(&mut self, validator_list: &[ValidatorDescr], name: &str)
    -> Result<Option<ValidatorListHash>> {
        let list_id = match compute_validator_list_id(validator_list, None)? {
            None => return Ok(None),
            Some(l) if self.validator_list_status.contains_list(&l) => return Ok(Some(l)),
            Some(l) => l,
        };

        let nodes_res: Vec<CatchainNode> = validator_list
            .iter()
            .map(validatordescr_to_catchain_node)
            .collect::<Vec<CatchainNode>>();

        log::info!(target: "validator_manager", "Updating {} validator list (id {:x}):", name, list_id);
        for x in &nodes_res {
            log::debug!(target: "validator_manager", "pk: {}, pk_id: {}, andl_id: {}",
                hex::encode(x.public_key.pub_key().unwrap()),
                hex::encode(x.public_key.id().data()),
                hex::encode(x.adnl_id.data())
            );
        }

        match self.engine.set_validator_list(list_id.clone(), &nodes_res).await? {
            Some(key) => {
                self.validator_list_status.add_list(list_id.clone(), key.clone());
                log::info!(target: "validator_manager", "Local node: pk_id: {} id: {}",
                    hex::encode(key.pub_key().unwrap()),
                    hex::encode(key.id().data())
                );
                return Ok(Some(list_id));
            },
            None => {
                log::info!(target: "validator_manager", "Local node is not a {} validator", name);
                return Ok(None);
            }
        }
    }

    async fn update_validator_lists(&mut self, mc_state: &ShardStateStuff) -> Result<bool> {
        let (validator_set, next_validator_set) = match mc_state.state()?.read_custom()? {
            None => return Ok(false),
            Some(state) => (state.config.validator_set()?, state.config.next_validator_set()?)
        };

        self.validator_list_status.curr = self.update_single_validator_list(validator_set.list(), "current").await?;
        self.validator_list_status.curr_utime_since = Some(validator_set.utime_since());
        if let Some(id) = self.validator_list_status.curr.as_ref() {
            self.engine.activate_validator_list(id.clone())?;
        }
        self.validator_list_status.next = self.update_single_validator_list(next_validator_set.list(), "next").await?;
        self.validator_list_status.next_utime_since = Some(next_validator_set.utime_since());

        metrics::gauge!("in_current_vset_p34", if self.validator_list_status.curr.is_some() { 1 } else { 0 } as f64);
        metrics::gauge!("in_next_vset_p36", if self.validator_list_status.next.is_some() { 1 } else { 0 } as f64);
        return Ok(!self.validator_list_status.curr.is_none() || !self.validator_list_status.next.is_none());
    }

    async fn is_active_shard(&self, shard: &ShardIdent) -> bool {
        for (_id, group) in self.validator_sessions.iter() {
            if group.shard() == shard {
                match group.get_status().await {
                    ValidatorGroupStatus::Sync | ValidatorGroupStatus::Active | ValidatorGroupStatus::Countdown { .. } => return true,
                    _ => ()
                }
            }
        }
        return false;
    }

    async fn garbage_collect_lists(&mut self) -> Result<()> {
        log::trace!(target: "validator_manager", "Garbage collect lists");
        let mut lists_gc = self.validator_list_status.known_hashes();

        for id in self.validator_sessions.values().into_iter() {
            lists_gc.remove(&id.get_validator_list_id());
        }

        for id in lists_gc {
            if !self.validator_list_status.actual_or_coming (&id) {
                log::trace!(target: "validator_manager", "Removing validator list: {:x}", id);
                self.validator_list_status.remove_list(&id);
                self.engine.remove_validator_list(id.clone()).await?;
                log::trace!(target: "validator_manager", "Validator list removed: {:x}", id);
            } else {
                log::trace!(target: "validator_manager", "Validator list is still actual: {:x}", id);
            }
        }
        log::trace!(target: "validator_manager", "Garbage collect lists -- ok");

        Ok(())
    }

    async fn garbage_collect_message_cache(&mut self) {
        if let Some(remp) = &self.remp_manager {
            let mut min_start = None;
            for vg in self.validator_sessions.iter() {
                if let Some(vg_range) = vg.1.get_master_cc_range().await {
                    let ms = min_start.unwrap_or(*vg_range.start());
                    min_start = Some(min(ms, *vg_range.start()));
                }
            }

            if let Some(min_actual) = min_start {
                let stats = remp.gc_old_messages(min_actual).await;
                log::info!(target: "remp", "GC old REMP messages (cc < {}): {}", min_actual, stats);
                #[cfg(feature = "telemetry")]
                self.engine.remp_core_telemetry().deleted_from_cache(stats.total);
            }
        }
    }

    async fn garbage_collect_remp_sessions(&mut self) {
        let remp = match &self.remp_manager {
            Some(remp) => remp,
            None => return
        };

        let mut active_remp_sessions = HashSet::new();
        for (_group_id,group) in &self.validator_sessions {
            if let Some(qm) = group.get_reliable_message_queue().await {
                active_remp_sessions.extend(qm.get_sessions().into_iter());
            }
        }

        remp.catchain_store.clone().gc_catchain_sessions(self.rt.clone(), active_remp_sessions).await;
    }

    async fn garbage_collect(&mut self) {
        if let Err(e) = self.garbage_collect_lists().await {
            log::error!(target: "validator_manager", "Error while garbage collecting validator lists: `{}`", e);
        }
        self.garbage_collect_message_cache().await;
        self.garbage_collect_remp_sessions().await;
    }

    async fn stop_and_remove_sessions(&mut self, sessions_to_remove: &HashSet<UInt256>, new_master_cc_range: Option<RangeInclusive<u32>>) {
        for id in sessions_to_remove.iter() {
            log::trace!(target: "validator_manager", "stop&remove: removing {:x}", id);
            match self.validator_sessions.get(id) {
                None => {
                    log::error!(target: "validator_manager",
                        "Session stopping error: {:x} already removed from hash", id
                    )
                }
                Some(session) => {
                    match session.get_status().await {
                        ValidatorGroupStatus::Stopping => {}
                        ValidatorGroupStatus::Stopped => {
                            if let Some(group) = self.validator_sessions.remove(id) {
                                if !self.is_active_shard(group.shard()).await {
                                    self.engine.remove_last_validation_time(group.shard());
                                    self.engine.remove_last_collation_time(group.shard());
                                    if let Some(remp_manager) = &self.remp_manager {
                                        remp_manager.remove_active_shard(group.shard()).await;
                                    }
                                }
                            }
                        }
                        _ => {
                            if let Err(e) = session.clone().stop(self.rt.clone(), new_master_cc_range.clone()).await {
                                log::error!(target: "validator_manager",
                                    "Could not stop session {:x}: `{}`", id, e);
                                    self.validator_sessions.remove(id);
                            }
                        }
                    }
                }
            }
        }
    }

    async fn compute_session_options(&mut self, mc_state_extra: &McStateExtra)
    -> Result<(validator_session::SessionOptions, UInt256)> {
        let consensus_config = match mc_state_extra.config.config(29)? {
            Some(ConfigParamEnum::ConfigParam29(ccc)) => ccc.consensus_config,
            _ => fail!("no CatchainConfig in config_params"),
        };
        let session_options = get_session_options(&consensus_config);
        let (opts_hash, session_options_serialized) = get_validator_session_options_hash(&session_options);
        log::debug!(target: "validator_manager", "SessionOptions from config.29: {:?}", session_options);
        log::debug!(
            target: "validator_manager",
            "SessionOptions from config.29 serialized: {} hash: {:x}",
            hex::encode(session_options_serialized),
            opts_hash
        );
        Ok((session_options, opts_hash))
    }

    async fn compute_prev_session_by_prev_cc(
        &self,
        general_session_info: Arc<GeneralSessionInfo>,
        full_validator_set: &ValidatorSet,
        mc_state: &ShardStateStuff,
        prev_blocks: &Vec<BlockIdExt>,
    ) -> Result<SessionValidatorsList> {
        let catchain_config = self.read_catchain_config(mc_state)?;
        if general_session_info.catchain_seqno <= 1 /* && check for actual validator set */ {
            return Ok(SessionValidatorsList::new())
        }

        let mut list = SessionValidatorsList::new();

        for blk in prev_blocks.iter() {
            let prev_session_info = Arc::new(general_session_info.with_shard_cc_seqno(
                blk.shard().clone(), general_session_info.catchain_seqno - 1
            ));
            let prev_subset = match try_calc_subset_for_workchain(
                &full_validator_set,
                &mc_state,
                &prev_session_info.shard,
                prev_session_info.catchain_seqno,
                blk.seq_no(),
            )? {
                Some(x) => x,
                None => {
                    log::error!(
                        target: "validator_manager",
                        "Cannot compute validator set for workchain {}: less than {} of {}",
                        prev_session_info.shard,
                        catchain_config.shard_validators_num,
                        full_validator_set.list().len()
                    );
                    continue
                }
            };

            let prev_block_seqno_opt = prev_blocks.get(0).map(|x| x.seq_no);
            let prev_session_id = get_session_unsafe_id(
                prev_session_info.clone(),
                &prev_subset.validators,
                true,
                prev_block_seqno_opt.clone(),
                &self.config
            );

            let prev_validator_set = prev_subset.compute_validator_set(prev_session_info.catchain_seqno)?;
            list.add_session(Arc::new(
                SessionValidatorsInfo::new(prev_session_info.clone(), prev_session_id, prev_validator_set, prev_block_seqno_opt)
            ))?;
        }

        Ok(list)
    }

    async fn update_validator_groups_with_next_list(&self,
        next_session_id: UInt256,
        next_session_info: Arc<GeneralSessionInfo>,
        next_subset: &ValidatorSet,
        next_master_cc_range: &RangeInclusive<u32>,
        prev_sessions: &SessionValidatorsList) -> Result<()>
    {
        let prev_validator_list = prev_sessions.get_validator_list()?;
        for prev_session_shard in prev_sessions.get_shards_sorted().iter() {
            let prev_session = prev_sessions
                .get_session_for_shard(prev_session_shard)
                .ok_or_else(|| error!("Shard {} is absent from {}, although mentioned by get_shards_sorted()",
                    prev_session_shard, prev_sessions)
                )?;

            match self.validator_sessions.get(&prev_session.session_id) {
                Some(prev_group) =>
                    prev_group.clone().add_next_validators(
                        &prev_validator_list,
                        next_subset,
                        next_session_info.clone(),
                        next_master_cc_range
                    ).await?,

                None => log::trace!(target: "validator_manager",
                     "New session id {:x}, shard {}, previous session id {:x} shard {} has no validator_group",
                     next_session_id, next_session_info.shard, prev_session.session_id, prev_session_shard
                )
            }
        }

        Ok(())
    }

    async fn compute_prev_sessions_list(
        &self,
        full_validator_set: &ValidatorSet,
        mc_state: &ShardStateStuff,
        prev_blocks: &Vec<BlockIdExt>,
        next_session_info: Arc<GeneralSessionInfo>,
        next_session_id: &UInt256
    ) -> Result<Arc<SessionValidatorsList>> {
        if next_session_info.catchain_seqno <= 1 {
            return Ok(Arc::new(SessionValidatorsList::new()))
        }

        // Find previous sessions according to stored info
        let prev_sessions_from_cache = match find_previous_sessions(
            self.engine.clone(), &self.session_info_cache,
            prev_blocks, next_session_info.catchain_seqno, &next_session_info.shard
        ).await {
            Err(e) => {
                log::warn!(target: "validator_manager", "Cannot compute old sessions by prev blocks for {:x}: '{}'", next_session_id, e);
                Arc::new(SessionValidatorsList::new())
            },
            Ok(p) => p
        };

        // Compute previous sessions according to previous cc
        let prev_sessions_by_cc = self.compute_prev_session_by_prev_cc(
            next_session_info.clone(), full_validator_set, mc_state, prev_blocks
        ).await?;

        if &prev_sessions_by_cc != prev_sessions_from_cache.as_ref() {
            log::warn!(target: "validator_manager", "Different previous sessions sets for shards [{:?}]: history sessions {}, prev cc sessions {}",
                prev_sessions_by_cc.get_shards_sorted(), prev_sessions_from_cache, prev_sessions_by_cc
            );
        }

        let prev_sessions = if prev_sessions_from_cache.is_empty() {
            Arc::new(prev_sessions_by_cc)
        }
        else {
            prev_sessions_from_cache
        };

        Ok(prev_sessions)
    }

    async fn update_validation_status(&mut self, mc_state: &ShardStateStuff, mc_state_extra: &McStateExtra) -> Result<()> {
        match self.engine.validation_status() {
            ValidationStatus::Waiting => {
                let last_masterchain_block = mc_state.block_id();
                if last_masterchain_block.seq_no == 0 || rotate_all_shards(mc_state_extra) {
                    let later_than_hardfork = self.engine.get_last_fork_masterchain_seqno() <= last_masterchain_block.seq_no;
                    let good_history = self.remp_manager.is_none()
                        || self.has_long_enough_replay_protection_history(last_masterchain_block).await?;

                    if self.engine.check_sync().await?
                        && later_than_hardfork
                        && good_history
                    {
                        self.block_observer = self.initialize_replay_protection_history(last_masterchain_block).await?;
                        if last_masterchain_block.seq_no == 0 && self.config.no_countdown_for_zerostate {
                            self.engine.set_validation_status(ValidationStatus::Active);
                        } else {
                            self.engine.set_validation_status(ValidationStatus::Countdown);
                        }
                    }
                }
            }
            ValidationStatus::Countdown => {
                for (_, group) in self.validator_sessions.iter() {
                    let status = group.get_status().await;
                    if status == ValidatorGroupStatus::Sync || status == ValidatorGroupStatus::Active {
                        let path_str: String = self.engine.db_root_dir()?.to_owned()+"/catchains";
                        tokio::spawn( async move {
                            if let Err(err) = clear_catchains_cache(path_str).await {
                                log::warn!("Error clearing catchains cache: {}", err);
                            }
                        });
                        self.engine.set_validation_status(ValidationStatus::Active);
                        break
                    }
                }
            }
            ValidationStatus::Disabled | ValidationStatus::Active => {}
        }
        Ok(())
    }

    async fn disable_validation(&mut self) -> Result<()> {
        self.engine.set_validation_status(ValidationStatus::Disabled);

        let existing_validator_sessions: HashSet<UInt256> =
            self.validator_sessions.keys().cloned().collect();
        self.stop_and_remove_sessions(&existing_validator_sessions, None).await;
        self.garbage_collect().await;
        self.engine.set_will_validate(false);
        self.engine.clear_last_rotation_block_id()?;
        log::info!(target: "validator_manager", "All sessions were removed, validation disabled");
        Ok(())
    }

    fn enable_validation(&mut self) {
        self.engine.set_will_validate(true);
        let validation_status = std::cmp::max(self.engine.validation_status(), ValidationStatus::Waiting);
        self.engine.set_validation_status(validation_status);
        log::debug!(target: "validator_manager", "Validation enabled: status {:?}", validation_status);
    }

    async fn start_sessions(&mut self,
        new_shards: &HashMap<ShardIdent, Vec<BlockIdExt>>,
        our_current_shards: &mut HashMap<ShardIdent, ValidatorSet>,
        keyblock_seqno: u32,
        session_options: validator_session::SessionOptions,
        opts_hash: &UInt256,
        gc_validator_sessions: &mut HashSet<UInt256>,
        mc_now: UnixTime32,
        mc_state: &ShardStateStuff,
        mc_state_extra: &McStateExtra,
        master_cc_range: &RangeInclusive<u32>,
        last_masterchain_block: &BlockIdExt
    ) -> Result<()> {
        let validator_list_id = match &self.validator_list_status.curr {
            Some(list_id) => list_id,
            None => return Ok(())
        };
        let full_validator_set = mc_state_extra.config.validator_set()?;

        let validation_status = self.engine.validation_status();
        let catchain_config = self.read_catchain_config(mc_state)?;
        let group_start_status = if validation_status == ValidationStatus::Countdown {
            let session_lifetime = std::cmp::min(catchain_config.mc_catchain_lifetime,
                                                 catchain_config.shard_catchain_lifetime);
            let start_at = tokio::time::Instant::now() + Duration::from_secs((session_lifetime / 2).into());
            ValidatorGroupStatus::Countdown { start_at }
        } else {
            ValidatorGroupStatus::Sync
        };

        let do_unsafe_catchain_rotate = self.config.check_unsafe_catchain_rotation(
            Some(last_masterchain_block.seq_no), mc_state_extra.validator_info.catchain_seqno
        ) != None;

        log::trace!(target: "validator_manager", "Starting/updating sessions {}",
            if do_unsafe_catchain_rotate {"(unsafe rotate)"} else {""}
        );

        let remp_enabled = is_remp_enabled(self.engine.clone(), mc_state_extra.config());

        for (ident, prev_blocks) in new_shards.iter() {
            let cc_seqno_from_state = if ident.is_masterchain() {
                *master_cc_range.end()
            } else {
                mc_state_extra.shards().calc_shard_cc_seqno(&ident)?
            };

            let cc_seqno = cc_seqno_from_state;

            log::trace!(target: "validator_manager", "Trying to start/update session for shard {}, cc_seqno {}",
                ident, cc_seqno_from_state
            );

            let subset = match try_calc_subset_for_workchain(
                &full_validator_set,
                &mc_state,
                ident,
                cc_seqno,
                get_first_block_seqno_after_prevs(prev_blocks).unwrap_or_default()
            )? {
                Some(x) => x,
                None => {
                    log::debug!(
                        target: "validator_manager", 
                        "Cannot compute validator set for workchain {}: less than {} of {}",
                        ident,
                        catchain_config.shard_validators_num,
                        full_validator_set.list().len()
                    );
                    continue
                }
            };

            let vsubset = ValidatorSet::with_cc_seqno(0, 0, 0, cc_seqno, subset.validators.clone())?;
            let general_session_info = Arc::new(GeneralSessionInfo {
                shard: ident.clone(),
                opts_hash: opts_hash.clone(),
                catchain_seqno: cc_seqno,
                key_seqno: keyblock_seqno,
                max_vertical_seqno: 0
            });

            let prev_block_seqno_opt = prev_blocks.get(0).map(|x| x.seq_no);
            let session_id = get_session_unsafe_id(
                general_session_info.clone(),
                &vsubset.list().to_vec(),
                true,
                prev_block_seqno_opt.clone(),
                &self.config
            );

            let session_info = SessionValidatorsInfo::new(
                general_session_info.clone(), session_id.clone(), vsubset.clone(), prev_block_seqno_opt
            );
            log::trace!(target: "validator_manager", "Current subset for session: {}", session_info);
            self.session_info_cache.add_info(general_session_info.catchain_seqno, session_info)?;

            let prev_sessions = self.compute_prev_sessions_list(
                &full_validator_set,
                &mc_state,
                prev_blocks,
                general_session_info.clone(),
                &session_id
            ).await?;

            if prev_sessions.is_empty() {
                log::debug!(
                    target: "validator_manager",
                    "No previous subset for session: shard {}, cc_seqno {}, keyblock_seqno {}, skipping",
                    ident, general_session_info.catchain_seqno, keyblock_seqno
                );
            } else {
                log::trace!(target: "validator_manager", "Previous subset for session: {}", prev_sessions);
                if let Err(e) = self.update_validator_groups_with_next_list(
                    session_id.clone(),
                    general_session_info.clone(),
                    &vsubset,
                    master_cc_range,
                    &prev_sessions
                ).await {
                    log::error!(target: "validator_manager", "Error adding previous subset for session {}: {}", prev_sessions, e)
                }
            };

            let local_id_option = self.find_us(&subset.validators);

            if let Some(local_id) = &local_id_option {
                our_current_shards.insert (ident.clone(), vsubset.clone());
                
                log::debug!(
                    target: "validator_manager", 
                    "subset for session: shard {}, cc_seqno {}, keyblock_seqno {}, \
                    validator_set {}, session_id {:x}",
                    ident, cc_seqno, keyblock_seqno,
                    validatorset_to_string(&vsubset), session_id
                );

                gc_validator_sessions.remove(&session_id);

                // If blockchain works under unsafe_catchain_rotation, then do not change its status:
                // 1. Do not start new sessions
                // 2. Do not remove functioning old sessions
                if do_unsafe_catchain_rotate && !ident.is_masterchain() && local_id_option.is_none() {
                    log::trace!(
                        target: "validator", 
                        "Current shard {}, session {:x}: unsafe rotation skipping", 
                        ident, session_id
                    );
                    continue;
                }

                let engine = self.engine.clone();
                #[cfg(feature = "slashing")]
                let slashing_manager = self.slashing_manager.clone();
                let remp_manager = self.remp_manager.clone();
                let verification_manager = self.verification_manager.lock().clone();
                let allow_unsafe_self_blocks_resync = self.config.unsafe_resync_catchains.contains(&cc_seqno);
                let session = self.validator_sessions.entry(session_id.clone()).or_insert_with(||
                    Arc::new(ValidatorGroup::new(
                        general_session_info.clone(),
                        local_id.clone(),
                        session_id.clone(),
                        validator_list_id.clone(),
                        vsubset.clone(),
                        session_options,
                        remp_manager,
                        engine,
                        allow_unsafe_self_blocks_resync,
                        #[cfg(feature = "slashing")]
                        slashing_manager,
                        verification_manager,
                    ))
                );

                let session_status = session.get_status().await;
                let session_clone = session.clone();
                if session_status == ValidatorGroupStatus::Created {
                    log::trace!(target: "validator_manager", "Current shard {}, session {:x}: starting", ident, session_id);

                    if let Some(remp_manager) = &self.remp_manager {
                        remp_manager.add_active_shard(session.shard()).await;
                    }
                    ValidatorGroup::start_with_status(
                        session_clone,
                        &prev_sessions.get_validator_list()?,
                        &vsubset,
                        group_start_status,
                        prev_blocks.clone(),
                        last_masterchain_block.clone(),
                        SystemTime::UNIX_EPOCH + Duration::from_secs(mc_now.as_u32() as u64),
                        master_cc_range,
                        remp_enabled,
                        self.rt.clone()
                    ).await?;
                } else if session_status >= ValidatorGroupStatus::Stopping {
                    log::error!(target: "validator_manager", "Cannot start stopped session {}", session.info().await);
                } else {
                    log::trace!(target: "validator_manager", "Current shard {}, session {:x}: working", ident, session_id);
                }
            } else {
                log::trace!(target: "validator_manager", "We are not in subset for {}", ident);
            }
            log::trace!(target: "validator_manager", "Session {} started (if necessary)", ident);
        }
        log::trace!(target: "validator_manager", "Starting/updating sessions, end of list");
        Ok(())
    }

    async fn update_shards(&mut self, mc_state: Arc<ShardStateStuff>) -> Result<()> {
        if !self.update_validator_lists(&mc_state).await? {
            log::info!(target: "validator_manager", "Current validator list is empty, validation is disabled.");
            self.disable_validation().await?;
            return Ok(())
        }

        let mc_state_extra = mc_state.shard_state_extra()?;
        let last_masterchain_block = mc_state.block_id();

        let keyblock_seqno = if mc_state_extra.after_key_block {
            mc_state.block_id().seq_no
        } else {
            mc_state_extra.last_key_block.as_ref().map(|id| id.seq_no).expect("masterchain state must contain info about previous key block")
        };
        let mc_now: UnixTime32 = mc_state.state()?.gen_time().into();
        let (session_options, opts_hash) = self.compute_session_options(&mc_state_extra).await?;
        let catchain_config = self.read_catchain_config(&mc_state)?;

        self.enable_validation();
        if is_remp_enabled(self.engine.clone(), mc_state.config_params()?) && self.remp_manager.is_none() {
            log::error!(target: "validator_manager",
                "Remp capability is enabled in network config, but remp_manager is not started, network may not work properly. \
                Check `remp` section of config.json. Currently remp.service_enabled = {}",
                self.remp_config.is_service_enabled()
            );
        }

        // update verification manager (create / destroy depends on SMFT capabilities)
        self.update_verification_manager_usage(&mc_state_extra);

        let master_cc_seqno = get_masterchain_seqno(self.engine.clone(), &mc_state).await?;
        if let Some(remp) = &self.remp_manager {
            if last_masterchain_block.seq_no == 0 {
                remp.create_master_cc_session(master_cc_seqno, mc_now, vec!())?;
            }
            if last_masterchain_block.seq_no > 0 && rotate_all_shards(&mc_state_extra) {
                let (_block_info, tops) = get_block_info_by_id(self.engine.clone(), last_masterchain_block).await?;
                remp.create_master_cc_session(master_cc_seqno, mc_now, tops)?;
            }
        }

        self.update_validation_status(&mc_state, &mc_state_extra).await?;

        let mut master_cc_range = master_cc_seqno..=master_cc_seqno; // Todo: compute always
        if let Some(remp) = &self.remp_manager {
            if master_cc_seqno > 0 && self.engine.validation_status().allows_validate() {
                let rp_guarantee = remp.calc_rp_guarantee(&catchain_config);
                master_cc_range = remp.advance_master_cc(master_cc_seqno, rp_guarantee)?;
            }
        }

        // Collect info about shards
        let mut gc_validator_sessions: HashSet<UInt256> =
            self.validator_sessions.keys().cloned().collect();

        // Shards that are working or about to start (continue) in this masterstate: shard_ident -> prevs
        let mut new_shards = HashMap::new();
        // Validator sets for shards that are working or about to start
        let mut our_current_shards: HashMap<ShardIdent, ValidatorSet> = HashMap::new();

        // Shards that will eventually be started (in later masterstates): need to prepare
        let mut future_shards: HashSet<ShardIdent> = HashSet::new();
        // Validator sets for shards that will eventually be started
        let mut our_future_shards: 
            HashMap<ShardIdent, (ValidatorSubsetInfo, u32, ValidatorListHash)> = HashMap::new();
        let mut blocks_before_split: HashSet<BlockIdExt> = HashSet::new();

        new_shards.insert(ShardIdent::masterchain(), vec![last_masterchain_block.clone()]);
        future_shards.insert(ShardIdent::masterchain());
        let workchain_id = self.engine.processed_workchain().unwrap_or(BASE_WORKCHAIN_ID);
        let cap_workchains = mc_state.config_params()?.has_capability(GlobalCapabilities::CapWorkchains);
        if !cap_workchains || workchain_id != MASTERCHAIN_ID {
            mc_state_extra.shards().iterate_shards_for_workchain(workchain_id, |ident: ShardIdent, descr: ShardDescr| {
                // Add all shards that are effective from now
                // ValidatorGroups will be created and appropriate sessions started for these shards
                let top_block = BlockIdExt::with_params(
                    ident.clone(),
                    descr.seq_no,
                    descr.root_hash,
                    descr.file_hash
                );

                if descr.before_split {
                    let lr_shards = ident.split();
                    match lr_shards {
                        Err(e) => log::error!(target: "validator_manager", "Cannot split shard: `{}`", e),
                        Ok((l,r)) => {
                            new_shards.insert(l, vec![top_block.clone()]);
                            new_shards.insert(r, vec![top_block.clone()]);
                            blocks_before_split.insert(top_block);
                        }
                    }
                } else if descr.before_merge {
                    let parent_shard = ident.merge();
                    match parent_shard {
                        Err(e) => log::error!(target: "validator_manager", "Cannot merge shard: `{}`", e),
                        Ok(p) => {
                            let mut prev_blocks = match new_shards.get(&p) {
                                Some(pb) => pb.clone(),
                                None => vec![BlockIdExt::default(), BlockIdExt::default()]
                            };

                            // Add previous block for the shard: there are two parents for merge, so two prevs
                            let (_l,r) = p.split()?;
                            prev_blocks[(r == ident) as usize] = top_block;
                            new_shards.insert(p, prev_blocks);
                        }
                    }
                } else {
                    new_shards.insert(ident.clone(), vec![top_block]);
                }

                // Create list of shards which will be effective soon
                // ValidatorGroups will be created for these shards, but not started.
                let cur_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs();
                match descr.split_merge_at {
                    FutureSplitMerge::None => {
                        future_shards.insert(ident);
                    }
                    FutureSplitMerge::Split{split_utime: time, interval: _interval} => {
                        if (time as u64) < cur_time + 60 {
                            match ident.split() {
                                Ok((l,r)) => {
                                    future_shards.insert(l);
                                    future_shards.insert(r);
                                }
                                Err(e) => log::error!(target: "validator_manager", "Cannot split shard {}: `{}`", ident, e)
                            }
                        } else {
                            future_shards.insert(ident);
                        }
                    }
                    FutureSplitMerge::Merge{merge_utime: time, interval: _interval} => {
                        if (time as u64) < cur_time + 60 {
                            match ident.merge() {
                                Ok(p) => {
                                    future_shards.insert(p);
                                }
                                Err(e) => log::error!(target: "validator_manager", "Cannot merge shard {}: `{}`", ident, e)
                            }
                        } else {
                            future_shards.insert(ident);
                        }
                    }
                };

                Ok(true)
            })?;
        }

        // Initializing future shards
        log::debug!(target: "validator_manager", "Future shards initialization:");
        let next_validator_set = mc_state_extra.config.next_validator_set()?;
        let full_validator_set = mc_state_extra.config.validator_set()?;
        let possible_validator_change = next_validator_set.total() > 0;
        let mut mc_validators = Vec::new();

        mc_validators.reserve(full_validator_set.total() as usize);

        for ident in future_shards.iter() {
            log::trace!(target: "validator_manager", "Future shard {}", ident);
            let (cc_seqno_from_state, cc_lifetime) = if ident.is_masterchain() {
                (master_cc_seqno, catchain_config.mc_catchain_lifetime)
            } else {
                (mc_state_extra.shards().calc_shard_cc_seqno(&ident)?, catchain_config.shard_catchain_lifetime)
            };

            let near_validator_change = possible_validator_change &&
                next_validator_set.utime_since() <= (mc_now.as_u32() / cc_lifetime + 1) * cc_lifetime;
            let future_validator_set = if near_validator_change {
                log::info!(
                    target: "validator_manager", 
                    "Validator change will happen during catchain session lifetime \
                    for shard {}: cc_lifetime {}, now {}, next set since {}",
                    ident, cc_lifetime, mc_now, next_validator_set.utime_since()
                );
                &next_validator_set
            } else {
                &full_validator_set
            };

            let vnext_list_id = match compute_validator_list_id(&future_validator_set.list(), None)? {
                None => continue,
                Some(l) => l
            };

            let next_cc_seqno = cc_seqno_from_state + 1;

        //     #[cfg(feature = "fast_finality")]
        //     let next_subset_opt = if !ident.is_masterchain() {
        //         //let block_seqno = Self::get_first_block_seqno_after_prevs(new_shards.get(ident))?;
        //         try_calc_next_subset_for_workchain_fast_finality(
        //             &future_validator_set, 
        //             future_validator_set.cc_seqno(), 
        //             &mc_state, 
        //             ident
        //         )?
        //     } else {
        //         try_calc_subset_for_workchain_standard(
        //             &future_validator_set, 
        //             mc_state.config_params()?, 
        //             ident, 
        //             next_cc_seqno
        //         )?
        //     };

            let next_subset_opt = try_calc_subset_for_workchain_standard(
                &future_validator_set, mc_state.config_params()?, ident, next_cc_seqno)?;

            let next_subset = match next_subset_opt {
                Some(x) => x,
                None => {
                    log::error!(
                        target: "validator_manager", 
                        "Cannot compute validator set for workchain {}: less than {} of {}",
                        ident,
                        catchain_config.shard_validators_num,
                        future_validator_set.list().len()
                    );
                    continue
                }
            };

            our_future_shards.insert(ident.clone(), (next_subset, next_cc_seqno, vnext_list_id));
            log::trace!(
                target: "validator_manager", 
                "Future shard {}: computing next subset with cc_seqno {} -- done", 
                ident, next_cc_seqno
            );
        };

        // Iterate over shards and start all missing sessions
        log::trace!(target: "validator_manager", "Starting missing sessions");
        let validation_status = self.engine.validation_status();
        if validation_status.allows_validate() {
            self.start_sessions(
                &new_shards,
                &mut our_current_shards,
                keyblock_seqno,
                session_options,
                &opts_hash,
                &mut gc_validator_sessions,
                mc_now,
                &mc_state,
                &mc_state_extra,
                &master_cc_range,
                last_masterchain_block
            ).await?;
        }
        log::trace!(target: "validator_manager", "Missing sessions started. Current shards:");

        // Iterate over future shards and create all future sessions
        for (ident, (wc, next_cc_seqno, next_val_list_id)) in our_future_shards.iter() {
            if ident.is_masterchain() {
                mc_validators.append(&mut wc.validators.clone());
            }

            if let Some(local_id) = self.find_us(&wc.validators) {
                let new_session_info = Arc::new(GeneralSessionInfo {
                    shard: ident.clone(),
                    opts_hash: opts_hash.clone(),
                    catchain_seqno: *next_cc_seqno,
                    key_seqno: keyblock_seqno,
                    max_vertical_seqno: 0
                });

                let session_id = get_session_id(
                    new_session_info.clone(),
                    &wc.validators,
                    true,
                );
                gc_validator_sessions.remove(&session_id);
                if !self.validator_sessions.contains_key(&session_id) {
                    let verification_manager = self.verification_manager.lock().clone();
                    let session = Arc::new(ValidatorGroup::new(
                        new_session_info,
                        local_id,
                        session_id.clone(),
                        next_val_list_id.clone(),
                        wc.compute_validator_set(*next_cc_seqno)?,
                        session_options,
                        self.remp_manager.clone(),
                        self.engine.clone(),
                        self.config.unsafe_resync_catchains.contains(next_cc_seqno),
                        #[cfg(feature = "slashing")]
                        self.slashing_manager.clone(),
                        verification_manager,
                    ));
                    self.validator_sessions.insert(session_id, session);
                }
            }
        }
        if !mc_state.config_params()?.has_capability(GlobalCapabilities::CapNoSplitOutQueue) {
            let mut precalc_split_queues_for: HashSet<BlockIdExt> = HashSet::new();
            for (_, session) in &self.validator_sessions {
                for id in &blocks_before_split {
                    if id.shard().is_parent_for(session.shard()) {
                        log::trace!(
                            target: "validator_manager", "precalc_split_queues_for {}", id
                        );
                        precalc_split_queues_for.insert(id.clone());
                    }
                }
            }
            
            // start background tasks which will precalculate split out messages queues
            for id in precalc_split_queues_for {
                let engine = self.engine.clone();
                tokio::spawn(async move {
                    log::trace!(
                        target: "validator_manager", "Split queues precalculating for {}", id
                    );
                    match OutMsgQueueInfoStuff::precalc_split_queues(&engine, &id).await {
                        Ok(_) => log::trace!(
                            target: "validator_manager", "Split queues precalculated for {}", id
                        ),
                        Err(e) => log::error!(
                            target: "validator_manager", 
                            "Can't precalculate split queues for {}: {}", id, e
                        )
                    }
                });
            }
        }

        let verification_manager = self.verification_manager.lock().clone();
        if let Some(verification_manager) = verification_manager {
            if let Some(verification_listener) = self.verification_listener.lock().clone() {
                let config = &mc_state_extra.config;
                let mut are_workchains_updated = false;
                match try_calc_vset_for_workchain(
                    &full_validator_set, config, &catchain_config, workchain_id
                ) {
                    Ok(workchain_validators) => {
                        let found_in_wc = self.find_us(&workchain_validators).is_some();
                        let found_in_mc = self.find_us(&mc_validators).is_some();
                        let verification_listener: Arc<dyn VerificationListener> = 
                            verification_listener.clone();
                        let verification_listener = Arc::downgrade(&verification_listener);
                        if let Some(local_key) = self.validator_list_status.get_local_key() {
                            let local_key_id = local_key.id();

                            if let Some(utime_since) = 
                                self.validator_list_status.get_curr_utime_since() 
                            {
                                log::trace!(target: "verificator", "Request BLS key");
                                let mut local_bls_key = 
                                    self.engine.get_validator_bls_key(local_key_id).await;
                                log::trace!(target: "verificator", "Request BLS key done");
                                if local_bls_key.is_none() && GENERATE_MISSING_BLS_KEY {
                                    match VerificationFactory::generate_test_bls_key(&local_key) {
                                        Ok(bls_key) => local_bls_key = Some(bls_key),
                                        Err(err) => log::error!(
                                            target: "verificator", 
                                            "Can't generate test BLS key: {:?}", err
                                        ),
                                    }
                                }

                                match local_bls_key {
                                    Some(local_bls_key) => {
                                        if found_in_wc || found_in_mc {
                                            log::debug!(
                                                target: "verificator", "Update workchains start"
                                            );
                                            verification_manager.update_workchains(
                                                local_key_id.clone(),
                                                local_bls_key,
                                                workchain_id,
                                                *utime_since,
                                                &workchain_validators,
                                                &mc_validators,
                                                &verification_listener
                                            ).await;
                                            are_workchains_updated = true;
                                            log::debug!(
                                                target: "verificator", "Update workchains finish"
                                            );
                                        } else {
                                            log::debug!(
                                                target: "verificator", 
                                                "Skip workchains update because we are not present \
                                                in WC/MC sets for workchain_id={}", 
                                                workchain_id
                                            );
                                        }
                                    },
                                    None => log::error!(
                                        target: "verificator", 
                                        "Can't create verification workchains: \
                                         no BLS private key attached"
                                    ),
                                }
                            } else {
                                log::warn!(
                                    target: "verificator", "Validator curr_utime_since is not set"
                                )
                            }
                        } else {
                            log::warn!(target: "verificator", "Validator local key is not found");
                        }
                    }
                    Err(err) => {
                        log::error!(target: "validator", "Can't create verification workchains: {:?}", err);
                    }
                }

                if !are_workchains_updated {
                    log::debug!(target: "verificator", "Reset workchains start");
                    verification_manager.reset_workchains().await;
                    log::debug!(target: "verificator", "Reset workchains finish");
                }
            }
        }

        if rotate_all_shards(&mc_state_extra) {
            log::info!(target: "validator_manager", "New last rotation block: {}", last_masterchain_block);
            self.engine.save_last_rotation_block_id(last_masterchain_block)?;
        }

        log::trace!(target: "validator_manager", "starting stop&remove");
        self.stop_and_remove_sessions(&gc_validator_sessions, Some(master_cc_range)).await;
        log::trace!(target: "validator_manager", "starting garbage collect");
        self.garbage_collect().await;
        log::trace!(target: "validator_manager", "exiting");
        Ok(())
    }

    async fn stats(&mut self) {
        log::info!(target: "validator_manager", "{:32} {}", "session id", "st round shard");
        log::info!(target: "validator_manager", "{:-64}", "");

        let mut queue_lengths : HashMap<UInt256, usize> = HashMap::new();

        // Validation shards statistics
        for (_, group) in self.validator_sessions.iter() {
            if let Some(qm) = group.get_reliable_message_queue().await {
                match qm.get_messages_cnt().await {
                    Some((session, cnt)) => {
                        queue_lengths.insert(session, cnt);
                        metrics::gauge!("remp_queue_len", cnt as f64, &[("shard", group.shard().to_string())]);
                    }
                    None => log::warn!(target: "remp", "Validator group {} has no current REMP session", group.info().await)
                }
            };

            log::info!(target: "validator_manager", "{}", group.info().await);
            let status = group.get_status().await;
            if status == ValidatorGroupStatus::Sync || status == ValidatorGroupStatus::Active || status == ValidatorGroupStatus::Stopping {
                self.engine.set_last_validation_time(group.shard().clone(), group.last_validation_time());
                self.engine.set_last_collation_time(group.shard().clone(), group.last_collation_time());
            }
        }
 
        if let Some(rm) = &self.remp_manager {
            metrics::gauge!("remp_message_cache_size", rm.message_cache.all_messages_count().0 as f64);
            log::info!(target: "validator_manager", "Remp message cache stats: {}", rm.message_cache.message_stats());

            for (s, shard_ident) in rm.catchain_store.list_catchain_sessions().await.iter() {
                let queue_len = match queue_lengths.get(shard_ident) {
                    Some(len) => format!(", queue_len = {}", len),
                    None => "".to_owned()
                };
                log::info!(target: "validator_manager", "{}{}", s, queue_len);
            }
        }

        log::trace!(target: "validator_manager", "======= sessions stats over =======");
    }

    /// For block `upper_id` find master cc range and block id --- shortest range with proper history.
    /// Returns cc range and corresponding block, initiating cc start.
    async fn find_initial_mc_range(&self, upper_id: &BlockIdExt) -> Result<(RangeInclusive<u32>, BlockIdExt)> {
        if !upper_id.is_masterchain() {
            fail!("Can find initial mc range only for master chain blocks, not for {}", upper_id);
        }
        let remp_manager = self.remp_manager.as_ref().ok_or_else(|| error!("Should not read states back if no REMP is active"))?;

        if upper_id.seq_no <= 0 {
            return Ok((0..=0, upper_id.clone()));
        }

        let upper_state = self.engine.load_state(upper_id).await?;
        let rp_guarantee = remp_manager.calc_rp_guarantee(&self.read_catchain_config(&upper_state)?);

        let (upper_info, _loop_inf_shards) = get_block_info_by_id(self.engine.clone(), &upper_id).await?;
        let mut loop_info = upper_info.clone();
        let mut loop_id = upper_id.clone();
        let mut rotation_blocks = HashMap::new();

        loop {
            let new_loop_id = self.engine.load_block_prev1(&loop_id)?.clone();
            if new_loop_id.seq_no <= 1 {
                return Ok((0..=upper_info.gen_catchain_seqno(), new_loop_id.clone()));
            }

            let (new_loop_info, new_loop_inf_shards) = get_block_info_by_id(self.engine.clone(), &new_loop_id).await?;
            if new_loop_info.gen_catchain_seqno() < loop_info.gen_catchain_seqno() {
                // new_loop_info generated in catchain, but creates new catchain (loop_info)
                remp_manager.create_master_cc_session(loop_info.gen_catchain_seqno(), new_loop_info.gen_utime(), new_loop_inf_shards)?;
                rotation_blocks.insert(loop_info.gen_catchain_seqno(), new_loop_id.clone());

                if let Some(proposed_lwb) = remp_manager.compute_lwb_for_upb(loop_info.gen_catchain_seqno(), upper_info.gen_catchain_seqno(), rp_guarantee)? {
                    let lwb_rotation_block = rotation_blocks
                        .get(&proposed_lwb)
                        .ok_or_else(|| error!("No rotation block for cc {}", proposed_lwb))?;
                    return Ok((proposed_lwb..=upper_info.gen_catchain_seqno(), lwb_rotation_block.clone()));
                }
            }

            loop_id = new_loop_id;
            loop_info = new_loop_info;
        }
    }

    fn read_catchain_config(&self, state: &ShardStateStuff) -> Result<CatchainConfig> {
        let state_extra = state.shard_state_extra()?;
        state_extra.config.catchain_config()
    }

    async fn initialize_replay_protection_history(&self, initialization_upb_block: &BlockIdExt) -> Result<Option<RempMasterblockObserver>> {
        let remp_manager = match &self.remp_manager {
            Some(rm) => rm,
            None => return Ok(None)
        };

        let (cc_range, lwb_block) = self.find_initial_mc_range(initialization_upb_block).await?;
        log::info!(target: "validator_manager", "Initialization: processing blocks {}..={}, collecting messages with master cc range {}..={}",
            lwb_block, initialization_upb_block, cc_range.start(), cc_range.end()
        );
        if !cc_range.is_empty() {
            remp_manager.set_master_cc_range(&cc_range)?;
        }

        let handle = self.engine
            .load_block_handle(&lwb_block)?
            .ok_or_else(|| error!("Cannot load block handle {}, needed for initialization", lwb_block))?;

        let observer = RempMasterblockObserver::create_and_start(
            self.engine.clone(), remp_manager.message_cache.clone(), self.rt.clone(), handle.clone()
        )?;
        observer.process_master_block_range(&lwb_block, &initialization_upb_block).await?;

        Ok(Some(observer))
    }

    async fn has_long_enough_replay_protection_history(&mut self, initialization_upb_block: &BlockIdExt) -> Result<bool> {
        match {
            self.find_initial_mc_range(&initialization_upb_block).await
        } {
            Err(e) => {
                log::warn!(target: "validator_manager", "Error finding initial replay protection range: '{}'; waiting for more data ...", e);
                Ok(false)
            }
            Ok(_) => Ok(true)
        }
    }

    /// infinite loop with possible error cancellation
    async fn invoke(&mut self) -> Result<()> {
        let last_applied_block_id = self.engine.load_last_applied_mc_block_id()?.ok_or_else(
            || error!("Cannot run validator_manager if no last applied block is present")
        )?;
        let last_applied_block_handle = self.engine.load_block_handle(&last_applied_block_id)?.ok_or_else(
            || error!("Cannot load handle for last applied master block {}", last_applied_block_id)
        )?;
        let mut mc_handle = if let Some(id) = self.engine.load_last_rotation_block_id()? {
            log::info!(
                target: "validator_manager", 
                "Validator manager initialization: last rotation block: {}",
                id
            );
            let mc_handle = self.engine.load_block_handle(&id)?.ok_or_else(
                || error!("Cannot load handle for master block {}", id)
            )?;
            mc_handle
        } else {
            log::info!(target: "validator_manager",
                "Validator manager initialization: no last rotation block, using last applied block: {}", last_applied_block_id
            );
            last_applied_block_handle.clone()
        };

        //let block_observer = self.initialize_block_observer(&last_applied_block_handle).await?;

        while !self.engine.check_stop() {
            log::trace!(target: "validator_manager", "Trying to load state for masterblock {}", mc_handle.id().seq_no);

            match self.engine.load_state(mc_handle.id()).await {
                Ok(mc_state) => {
                    log::info!(target: "validator_manager", "Processing masterblock {}", mc_handle.id().seq_no);
                    #[cfg(feature = "slashing")]
                    if let Some(local_id) = self.validator_list_status.get_local_key() {
                        log::debug!(target: "validator_manager", "Processing slashing masterblock {}", mc_handle.id().seq_no);
                        self.slashing_manager.handle_masterchain_block(&mc_handle, &mc_state, &local_id, &self.engine).await;
                    }
                    log::trace!(target: "validator_manager", "Processing messages from masterblock {}", mc_handle.id().seq_no);
                    if let Some(bo) = &self.block_observer {
                        bo.process_master_block_handle(&mc_handle).await?;
                    }
                    log::trace!(target: "validator_manager", "Updating shards according to masterblock {}", mc_handle.id().seq_no);
                    self.update_shards(mc_state).await?;
                    log::trace!(target: "validator_manager", "Shards for masterblock {} updated", mc_handle.id().seq_no);
                },
                Err(e) => {
                    if self.engine.validation_status().allows_validate() {
                        fail!("State for {} lost while validating (status {:?}): error '{}'",
                            mc_handle.id(), self.engine.validation_status(), e
                        )
                    }
                    log::info!(target: "validator_manager", "Processing masterblock {}: state not available, going forward", mc_handle.id().seq_no);
                }
            }

            mc_handle = loop {
                log::trace!(target: "validator_manager", "Checking stop engine");
                if self.engine.check_stop() {
                    log::trace!(target: "validator_manager", "Engine is stoped. Exiting from invocation loop (while loading block)");
                    return Ok(())
                }
                log::trace!(target: "validator_manager", "Checked stop engine: going on");
                self.stats().await;
                log::trace!(target: "validator_manager", "Waiting next applied masterblock after {}", mc_handle.id().seq_no);
                match timeout(self.config.update_interval, self.engine.wait_next_applied_mc_block(&mc_handle, None)).await {
                    Ok(r_res) => {
                        log::trace!(target: "validator_manager", "Got next applied master block (result): {}",
                            match &r_res {
                                Err(e) => format!("Err({})", e),
                                Ok((h, _bs)) => format!("Ok({})", h.id())
                            }
                        );
                        break r_res?.0
                    },
                    Err(tokio::time::error::Elapsed{..}) => {
                        log::warn!(
                            target: "validator_manager", 
                            "Validator manager didn't receive next applied master block after {}",
                            mc_handle.id()
                        );
                    }
                }
            }
        }

        log::info!(target: "validator_manager", "Engine is stopped. Exiting from invocation loop (while applying state)");
        Ok(())
    }

    async fn verify_block_candidate(block_candidate: &BlockCandidate, engine: Arc<dyn EngineOperations>) -> bool {
        let candidate_id = block_candidate.block_id.clone();

        log::debug!(target: "verificator", "Verifying block candidate {:?}", candidate_id);

        match run_validate_query_any_candidate(block_candidate.clone(), engine).await {
            Ok(_time) => return true,
            Err(err) => {
                log::warn!(target: "verificator", "Block {:?} verification error: {:?}", candidate_id, err);
                return false;
            }
        }
    }
}

/// main entry point to validation process
pub fn start_validator_manager(
    engine: Arc<dyn EngineOperations>,
    runtime: tokio::runtime::Handle,
    config: ValidatorManagerConfig,
    remp_config: RempConfig,
) {
    const CHECK_VALIDATOR_TIMEOUT: u64 = 60;    //secs
    runtime.clone().spawn(async move {
        log::info!(target: "validator_manager", "checking if current node is a validator during {CHECK_VALIDATOR_TIMEOUT} secs");
        engine.acquire_stop(Engine::MASK_SERVICE_VALIDATOR_MANAGER);
        while !engine.get_validator_status() {
            log::trace!(target: "validator_manager", "Not a validator, waiting...");
            let _ = engine.clear_last_rotation_block_id();
            for _ in 0..CHECK_VALIDATOR_TIMEOUT {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if engine.check_stop() {
                    log::error!(target: "validator_manager", "Engine is stopped. exiting");
                    engine.release_stop(Engine::MASK_SERVICE_VALIDATOR_MANAGER);
                    return;
                }
            }
        }

        log::trace!(target: "validator_manager", "Starting validator manager");
        let (mut manager, remp_iface) = ValidatorManagerImpl::create(engine.clone(), runtime.clone(), config, remp_config);

        if let Some(remp_iface) = remp_iface {
            if let Err(e) = engine.set_remp_core_interface(remp_iface.clone()) {
                log::error!(target: "validator_manager", "set_remp_core_interface: {}", e);
            }

            runtime.clone().spawn(async move { 
                log::info!(target: "remp", "Starting REMP responses polling loop");
                remp_iface.poll_responses_loop().await; 
                log::info!(target: "remp", "Finishing REMP responses polling loop");
            });
        }

        if let Err(e) = manager.invoke().await {
            log::error!(target: "validator_manager", "FATAL!!! Unexpected error in validator manager: {:?}", e);
        }
        log::info!(target: "validator_manager", "Exiting, validator manager is stopped");
        engine.release_stop(Engine::MASK_SERVICE_VALIDATOR_MANAGER);
    });
}

#[cfg(test)]
#[path = "tests/test_session_id.rs"]
mod tests;
