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

use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
    sync::Arc,
    time::{Duration, SystemTime}
};

use crate::{
    config::{RempConfig, ValidatorManagerConfig},
    engine::{Engine, STATSD},
    engine_traits::EngineOperations,
    shard_state::ShardStateStuff,
    validator::{
        remp_block_parser::RempBlockObserverToplevel,
        remp_manager::{RempManager, RempInterfaceQueues},
        validator_group::{ValidatorGroup, ValidatorGroupStatus},
        validator_utils::{
            try_calc_subset_for_workchain,
            validatordescr_to_catchain_node,
            validatorset_to_string,
            compute_validator_list_id,
            ValidatorListHash,
            GeneralSessionInfo, get_group_members_by_validator_descrs, is_remp_enabled
        },
        out_msg_queue::OutMsgQueueInfoStuff,
    },
};

#[cfg(feature = "slashing")]
use crate::validator::slashing::{SlashingManager, SlashingManagerPtr};

use catchain::{CatchainNode, PublicKey, serialize_tl_boxed_object};
use tokio::time::timeout;
use ton_api::IntoBoxed;
use ton_block::{
    BlockIdExt, CatchainConfig, ConfigParamEnum, ConsensusConfig, FutureSplitMerge, McStateExtra, 
    ShardDescr, ShardIdent, ValidatorDescr, ValidatorSet, BASE_WORKCHAIN_ID, MASTERCHAIN_ID, 
    GlobalCapabilities
};
use ton_types::{error, fail, Result, UInt256};
use crate::validator::sessions_computing::{SessionHistory, SessionInfo};
use crate::validator::validator_utils::{
    ValidatorSubsetInfo, get_first_block_seqno_after_prevs, try_calc_subset_for_workchain_standard
};

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
    UInt256::calc_file_hash(&serialized.0)
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
    (UInt256::calc_file_hash(&serialized.0), serialized)
}

fn get_session_options(opts: &ConsensusConfig) -> validator_session::SessionOptions {
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
    }
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
    next: Option<ValidatorListHash>
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
}

impl Default for ValidatorListStatus {
    fn default() -> ValidatorListStatus {
        return ValidatorListStatus {
            known_lists: HashMap::default(),
            curr: None,
            next: None
        }
    }
}

fn rotate_all_shards(mc_state_extra: &McStateExtra) -> bool {
    mc_state_extra.validator_info.nx_cc_updated

}

struct ValidatorManagerImpl {
    engine: Arc<dyn EngineOperations>,
    rt: tokio::runtime::Handle,
    validator_sessions: HashMap<UInt256, Arc<ValidatorGroup>>, // Sessions: both actual (started) and future
    validator_list_status: ValidatorListStatus,
    session_history: SessionHistory,
    config: ValidatorManagerConfig,

//    shard_blocks_observer_for_remp: RempBlockObserver,
    remp_manager: Option<Arc<RempManager>>,

    #[cfg(feature = "slashing")]
    slashing_manager: SlashingManagerPtr,
}

impl ValidatorManagerImpl {
    fn create(
        engine: Arc<dyn EngineOperations>,
        rt: tokio::runtime::Handle,
        config: ValidatorManagerConfig,
        remp_config: RempConfig,
    ) -> (Self, Option<Arc<RempInterfaceQueues>>) {
        let (remp_manager, remp_interface_queues) = if remp_config.is_service_enabled() {
            let (m, i) = RempManager::create_with_options(engine.clone(), remp_config, Arc::new(rt.clone()));
            (Some(Arc::new(m)), Some(Arc::new(i)))
        } else {
            (None, None)
        };

        engine.set_validation_status(ValidationStatus::Disabled);
        (ValidatorManagerImpl {
            engine,
            rt,
            validator_sessions: HashMap::default(),
            validator_list_status: ValidatorListStatus::default(),
            session_history: SessionHistory::new(),
            config,
            remp_manager,
            //shard_blocks_observer_for_remp: None,
            #[cfg(feature = "slashing")]
            slashing_manager: SlashingManager::create(),
        }, remp_interface_queues)
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
        if let Some(id) = self.validator_list_status.curr.as_ref() {
            self.engine.activate_validator_list(id.clone())?;
        }
        self.validator_list_status.next = self.update_single_validator_list(next_validator_set.list(), "next").await?;

        STATSD.gauge("in_current_vset_p34", if self.validator_list_status.curr.is_some() { 1 } else { 0 } as f64);
        STATSD.gauge("in_next_vset_p36", if self.validator_list_status.next.is_some() { 1 } else { 0 } as f64);
        return Ok(!self.validator_list_status.curr.is_none() || !self.validator_list_status.next.is_none());
    }

    async fn is_active_shard(&self, shard: &ShardIdent) -> bool {
        for (_id, group) in self.validator_sessions.iter() {
            if group.shard() == shard {
                match group.get_status().await {
                    ValidatorGroupStatus::Active | ValidatorGroupStatus::Countdown { .. } => return true,
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

    async fn stop_and_remove_sessions(&mut self, sessions_to_remove: &HashSet<UInt256>, new_master_cc_seqno: Option<u32>) {
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
                            if let Err(e) = session.clone().stop(self.rt.clone(), new_master_cc_seqno).await {
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
            hex::encode(session_options_serialized.0),
            opts_hash
        );
        Ok((session_options, opts_hash))
    }

    async fn compute_prev_validator_list_impl(
        &self,
        full_validator_set: &ValidatorSet,
        current_subset: &ValidatorSet,
        mc_state: &ShardStateStuff,
        prev_blocks: &Vec<BlockIdExt>,
        general_session_info: Arc<GeneralSessionInfo>,
        session_id: &UInt256,
        master_cc_seqno: u32,
    ) -> Result<Option<Vec<ValidatorDescr>>> {
        let catchain_config = mc_state.config_params()?.catchain_config()?;
        if general_session_info.catchain_seqno > 1 {
            let prev_session_info = Arc::new(general_session_info.with_cc_seqno(general_session_info.catchain_seqno-1));
            let prev_subset = match try_calc_subset_for_workchain(
                &full_validator_set,
                &mc_state,
                &prev_session_info.shard,
                prev_session_info.catchain_seqno,
                0, // TODO: correct block necessary!
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
                    return Ok(None)
                }
            };

            let prev_session_id = get_session_unsafe_id(
                prev_session_info.clone(),
                &prev_subset.validators,
                true,
                prev_blocks.get(0).map(|x| x.seq_no),
                &self.config
            );

            for old_session_id in self.session_history.get_prev_sessions(&session_id)?.iter() {
                if *old_session_id == prev_session_id {
                    log::trace!(target: "validator_manager", "Old session id: {:x}, old session id from prev cc: {:x}",
                            old_session_id, prev_session_id
                        );
                }
                else {
                    log::warn!(target: "validator_manager", "Old session id: {:x} != old session id from prev cc: {:x}",
                            old_session_id, prev_session_id
                        );
                }
            }

            let old_session_id = &prev_session_id;
            match self.validator_sessions.get(old_session_id) {
                Some(old_session) =>
                    old_session.clone().add_next_validators(
                        master_cc_seqno, &prev_subset.validators, &current_subset, general_session_info.clone(),
                    ).await?,
                None => log::debug!(target: "validator_manager",
                         "Shard {}, adding info for session {:x}, previous session id {:x} has no validator_group!",
                         prev_session_info.shard, session_id, old_session_id
                    )
            }
            Ok(Some(prev_subset.validators))
        }
        else {
            let prev_validator_list = &self.session_history.get_prev_validator_list(&session_id)?;
            for old_session_id in self.session_history.get_prev_sessions(&session_id)?.iter() {
                match self.validator_sessions.get(old_session_id) {
                    Some(old_session) =>
                        old_session.clone().add_next_validators(
                            master_cc_seqno, prev_validator_list, &current_subset,
                            general_session_info.clone()
                        ).await?,
                    None => log::debug!(target: "validator_manager",
                             "Shard {}, adding info for session {:x}, previous session id {:x} has no validator_group!",
                             general_session_info.shard, session_id, old_session_id
                        )
                }
            }
            Ok(Some(prev_validator_list.clone()))
        }
    }

    async fn compute_prev_validator_list(
        &self,
        full_validator_set: &ValidatorSet,
        current_subset: &ValidatorSet,
        mc_state: &ShardStateStuff,
        prev_blocks: &Vec<BlockIdExt>,
        general_session_info: Arc<GeneralSessionInfo>,
        session_id: &UInt256,
        master_cc_seqno: u32,
    ) -> Result<Option<Vec<ValidatorDescr>>> {

        self.compute_prev_validator_list_impl(
            full_validator_set, current_subset, mc_state,
            prev_blocks, general_session_info,
            session_id, master_cc_seqno
        ).await
    }

    async fn update_validation_status(&mut self, mc_state: &ShardStateStuff, mc_state_extra: &McStateExtra) -> Result<()> {
        match self.engine.validation_status() {
            ValidationStatus::Waiting => {
                let rotate = rotate_all_shards(mc_state_extra);
                let last_masterchain_block = mc_state.block_id();
                let later_than_hardfork = self.engine.get_last_fork_masterchain_seqno() <= last_masterchain_block.seq_no;
                if self.engine.check_sync().await?
                    && (rotate || last_masterchain_block.seq_no == 0)
                    && later_than_hardfork
                {
                    if last_masterchain_block.seq_no == 0 && self.config.no_countdown_for_zerostate {
                        self.engine.set_validation_status(ValidationStatus::Active);
                    }
                    else {
                        self.engine.set_validation_status(ValidationStatus::Countdown);
                    }
                }
            }
            ValidationStatus::Countdown => {
                for (_, group) in self.validator_sessions.iter() {
                    if group.get_status().await == ValidatorGroupStatus::Active {
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
        catchain_config: &CatchainConfig,
        gc_validator_sessions: &mut HashSet<UInt256>,
        mc_now: u32,
        mc_state: &ShardStateStuff,
        mc_state_extra: &McStateExtra,
        last_masterchain_block: &BlockIdExt
    ) -> Result<()> {
        let validator_list_id = match &self.validator_list_status.curr {
            Some(list_id) => list_id,
            None => return Ok(())
        };
        let full_validator_set = mc_state_extra.config.validator_set()?;

        let validation_status = self.engine.validation_status();
        let group_start_status = if validation_status == ValidationStatus::Countdown {
            let session_lifetime = std::cmp::min(catchain_config.mc_catchain_lifetime,
                                                 catchain_config.shard_catchain_lifetime);
            let start_at = tokio::time::Instant::now() + Duration::from_secs((session_lifetime / 2).into());
            ValidatorGroupStatus::Countdown { start_at }
        } else {
            ValidatorGroupStatus::Active
        };

        let do_unsafe_catchain_rotate = self.config.check_unsafe_catchain_rotation(
            Some(last_masterchain_block.seq_no), mc_state_extra.validator_info.catchain_seqno
        ) != None;

        log::trace!(target: "validator_manager", "Starting/updating sessions {}",
            if do_unsafe_catchain_rotate {"(unsafe rotate)"} else {""}
        );

        let remp_enabled = is_remp_enabled(self.engine.clone(), mc_state_extra.config());

        for (ident, prev_blocks) in new_shards.iter() {
            let master_cc_seqno = mc_state_extra.validator_info.catchain_seqno;
            let cc_seqno_from_state = if ident.is_masterchain() {
                master_cc_seqno
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
                        "Cannot compute validator set for workchain {}:{:016X}: less than {} of {}",
                        ident.workchain_id(), 
                        ident.shard_prefix_with_tag(),
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

            let session_id = get_session_unsafe_id(
                general_session_info.clone(),
                &vsubset.list().to_vec(),
                true,
                prev_blocks.get(0).map(|x| x.seq_no),
                &self.config
            );

            let session_info = SessionInfo::new(ident.clone(), session_id.clone(), vsubset.clone());
            let old_shards: Vec<ShardIdent> = prev_blocks.iter().map(|blk| blk.shard_id.clone()).collect();
            self.session_history.new_session_after(session_info.clone(), old_shards.clone())?;

            let prev_validator_list =
                match self.compute_prev_validator_list(
                    &full_validator_set, &vsubset,
                    &mc_state, prev_blocks, general_session_info.clone(),
                    &session_id, master_cc_seqno
                ).await? {
                    None => {
                        log::debug!(
                            target: "validator_manager",
                            "No previous subset for session: \
                            shard {}, cc_seqno {}, keyblock_seqno {}, skipping",
                            ident, cc_seqno, keyblock_seqno,
                        );
                        Vec::new()
                    },
                    Some(x) => x
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
                let allow_unsafe_self_blocks_resync = self.config.unsafe_resync_catchains.contains(&cc_seqno);
                let session = self.validator_sessions.entry(session_id.clone()).or_insert_with(||
                    Arc::new(ValidatorGroup::new(
                        general_session_info.clone(),
                        local_id.clone(),
                        session_id.clone(),
                        master_cc_seqno,
                        validator_list_id.clone(),
                        vsubset.clone(),
                        session_options,
                        remp_manager,
                        engine,
                        allow_unsafe_self_blocks_resync,
                        #[cfg(feature = "slashing")]
                        slashing_manager,
                    ))
                );

                let session_status = session.get_status().await;
                let session_clone = session.clone();
                if session_status == ValidatorGroupStatus::Created {
                    log::trace!(target: "validator_manager", "Current shard {}, session {:x}: starting", ident, session_id);

                    self.session_history.set_as_actual_session(session_id)?;
                    if let Some(remp_manager) = &self.remp_manager {
                        remp_manager.add_active_shard(session.shard()).await;
                    }
                    ValidatorGroup::start_with_status(
                        session_clone,
                        &prev_validator_list,
                        &vsubset,
                        group_start_status,
                        prev_blocks.clone(),
                        last_masterchain_block.clone(),
                        SystemTime::UNIX_EPOCH + Duration::from_secs(mc_now.into()),
                        remp_enabled,
                        self.rt.clone()
                    ).await?;
                } else if session_status >= ValidatorGroupStatus::Stopping {
                    log::error!(target: "validator_manager", "Cannot start stopped session {}", session.info().await);
                } else {
                    log::trace!(target: "validator_manager", "Current shard {}, session {:x}: working", ident, session_id);
                }
            } else {
                log::trace!("We are not in subset for {}", ident);
            }
            log::trace!(target: "validator_manager", "Session {} started (if necessary)", ident);
        }
        log::trace!(target: "validator_manager", "Starting/updating sessions, end of list");
        Ok(())
    }

    fn get_masterchain_seqno(&self, mc_state: Arc<ShardStateStuff>) -> Result<u32> {
        let mc_state_extra = mc_state.shard_state_extra()?;
        Ok(mc_state_extra.validator_info.catchain_seqno)
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
        let mc_now = mc_state.state()?.gen_time();
        let (session_options, opts_hash) = self.compute_session_options(&mc_state_extra).await?;
        let catchain_config = mc_state_extra.config.catchain_config()?;

        self.enable_validation();
        self.update_validation_status(&mc_state, &mc_state_extra).await?;

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
        let mut our_future_shards: HashMap<ShardIdent, (ValidatorSubsetInfo, u32, ValidatorListHash)> = HashMap::new();
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
        let master_cc_seqno = self.get_masterchain_seqno(mc_state.clone())?;

        for ident in future_shards.iter() {
            log::trace!(target: "validator_manager", "Future shard {}", ident);
            let (cc_seqno_from_state, cc_lifetime) = if ident.is_masterchain() {
                (master_cc_seqno, catchain_config.mc_catchain_lifetime)
            } else {
                (mc_state_extra.shards().calc_shard_cc_seqno(&ident)?, catchain_config.shard_catchain_lifetime)
            };

            let near_validator_change = possible_validator_change &&
                next_validator_set.utime_since() <= (mc_now / cc_lifetime + 1) * cc_lifetime;
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
                &catchain_config,
                &mut gc_validator_sessions,
                mc_now,
                &mc_state,
                &mc_state_extra,
                last_masterchain_block
            ).await?;
        }
        log::trace!(target: "validator_manager", "Missing sessions started. Current shards:");

        // Iterate over future shards and create all future sessions
        for (ident, (wc, next_cc_seqno, next_val_list_id)) in our_future_shards.iter() {
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
                    let session = Arc::new(ValidatorGroup::new(
                        new_session_info,
                        local_id,
                        session_id.clone(),
                        master_cc_seqno + 1,
                        next_val_list_id.clone(),
                        wc.compute_validator_set(*next_cc_seqno)?,
                        session_options,
                        self.remp_manager.clone(),
                        self.engine.clone(),
                        self.config.unsafe_resync_catchains.contains(next_cc_seqno),
                        #[cfg(feature = "slashing")]
                        self.slashing_manager.clone()
                    ));
                    self.validator_sessions.insert(session_id, session);
                }
            }
        }

        let mut precalc_split_queues_for: HashSet<BlockIdExt> = HashSet::new();
        for (_, session) in &self.validator_sessions {
            for id in &blocks_before_split {
                if id.shard().is_parent_for(session.shard()) {
                    log::trace!(target: "validator_manager", "precalc_split_queues_for {}", id);
                    precalc_split_queues_for.insert(id.clone());
                }
            }
        }

        // start background tasks which will precalculate split out messages queues
        for id in precalc_split_queues_for {
            let engine = self.engine.clone();
            tokio::spawn(async move {
                log::trace!(target: "validator_manager", "Split queues precalculating for {}", id);
                match OutMsgQueueInfoStuff::precalc_split_queues(&engine, &id).await {
                    Ok(_) => log::trace!(target: "validator_manager", "Split queues precalculated for {}", id),
                    Err(e) => log::error!(target: "validator_manager", "Can't precalculate split queues for {}: {}", id, e)
                }
            });
        }

        if rotate_all_shards(&mc_state_extra) {
            log::info!(target: "validator_manager", "New last rotation block: {}", last_masterchain_block);
            self.engine.save_last_rotation_block_id(last_masterchain_block)?;

            if let Some(remp) = &self.remp_manager {
                remp.message_cache.set_master_cc(master_cc_seqno);
                remp.message_cache.set_master_cc_start_time(master_cc_seqno, SystemTime::now());
                let _deleted = remp.gc_old_messages(master_cc_seqno).await;
                #[cfg(feature = "telemetry")]
                self.engine.remp_core_telemetry().deleted_from_cache(_deleted);
            }
        }

        log::trace!(target: "validator_manager", "starting stop&remove");
        self.stop_and_remove_sessions(&gc_validator_sessions, Some(master_cc_seqno)).await;
        log::trace!(target: "validator_manager", "starting garbage collect");
        self.garbage_collect_lists().await?;
        log::trace!(target: "validator_manager", "exiting");
        Ok(())
    }

    async fn stats(&mut self) {
        log::info!(target: "validator_manager", "{:32} {}", "session id", "st round shard");
        log::info!(target: "validator_manager", "{:-64}", "");

        // Validation shards statistics
        for (_, group) in self.validator_sessions.iter() {
            log::info!(target: "validator_manager", "{}", group.info().await);
            let status = group.get_status().await;
            if status == ValidatorGroupStatus::Active || status == ValidatorGroupStatus::Stopping {
                self.engine.set_last_validation_time(group.shard().clone(), group.last_validation_time());
                self.engine.set_last_collation_time(group.shard().clone(), group.last_collation_time());
            }
        }
 
        for (_,group) in self.validator_sessions.iter() {
            group.print_messages(true).await;
        }

        if let Some(rm) = &self.remp_manager {
            rm.message_cache.print_all_messages(true).await;
        }
    }
/*
    fn init_shard_blocks_observer_for_remp(&mut self, init_mc_block: Arc<BlockHandle>) {
        self.shard_blocks_observer_for_remp.init(init_mc_block, self.engine.clone());
    }

    async fn process_new_blocks_for_remp(&mut self, mc_block: Arc<BlockStuff>) -> Result<()> {
        let blocks = self.shard_blocks_observer_for_remp.get_new_blocks(mc_block)?;
        Ok(())
    }
*/
    /// infinite loop with possible error cancelation
    async fn invoke(&mut self) -> Result<()> {
        let mc_block_id = if let Some(id) = self.engine.load_last_rotation_block_id()? {
            log::info!(
                target: "validator_manager", 
                "Validator manager initialization: last rotation block: {}",
                id
            );
            id
        } else if let Some(id) = self.engine.load_last_applied_mc_block_id()? {
            log::info!(
                target: "validator_manager",
                "Validator manager initialization: last applied block: {}, no last rotation block",
                id
            );
            id
        } else {
            fail!("Validator manager initialization neither last rotation nor applied block")
        };

        let mut mc_handle = self.engine.load_block_handle(&mc_block_id)?.ok_or_else(
            || error!("Cannot load handle for master block {}", mc_block_id)
        )?;

        let block_observer = if let Some(remp_manager) = &self.remp_manager {
            Some(RempBlockObserverToplevel::toplevel(
                self.engine.clone(), remp_manager.message_cache.clone(), self.rt.clone(), mc_handle.clone()
            )?)
        } else { None };

        loop {
            if self.engine.check_stop() {
                return Ok(())
            }
            log::trace!(target: "validator_manager", "Trying to load state for masterblock {}", mc_handle.id().seq_no);
            let mc_state = self.engine.load_state(mc_handle.id()).await?;
            log::info!(target: "validator_manager", "Processing masterblock {}", mc_handle.id().seq_no);
            #[cfg(feature = "slashing")]
            if let Some(local_id) = self.validator_list_status.get_local_key() {
                log::debug!(target: "validator_manager", "Processing slashing masterblock {}", mc_handle.id().seq_no);
                self.slashing_manager.handle_masterchain_block(&mc_handle, &mc_state, &local_id, &self.engine).await;
            }
            log::trace!(target: "validator_manager", "Updaing shards for masterblock {}", mc_handle.id().seq_no);
            if let Some(block_observer) = &block_observer {
                if mc_handle.id().seq_no() != 0 {
                    let mc_blockstuff = self.engine.load_block(&mc_handle).await?;
                    block_observer.send((mc_blockstuff, self.get_masterchain_seqno(mc_state.clone())?))?;
                }
            }
            self.update_shards(mc_state).await?;

            mc_handle = loop {
                if self.engine.check_stop() {
                    return Ok(())
                }
                self.stats().await;
                log::trace!(target: "validator_manager", "Waiting next applied masterblock after {}", mc_handle.id().seq_no);
                match timeout(self.config.update_interval, self.engine.wait_next_applied_mc_block(&mc_handle, None)).await {
                    Ok(r_res) => break r_res?.0,
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
        engine.acquire_stop(Engine::MASK_SERVICE_VALIDATOR_MANAGER);
        while !engine.get_validator_status() {
            log::trace!("is not a validator");
            let _ = engine.clear_last_rotation_block_id();
            for _ in 0..CHECK_VALIDATOR_TIMEOUT {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if engine.check_stop() {
                    engine.release_stop(Engine::MASK_SERVICE_VALIDATOR_MANAGER);
                    return;
                }
            }
        }
        log::info!("starting validator manager...");
        let (mut manager, remp_iface) = ValidatorManagerImpl::create(engine.clone(), runtime.clone(), config, remp_config);

        if let Some(remp_iface) = remp_iface {
            if let Err(e) = engine.set_remp_core_interface(remp_iface.clone()) {
                log::error!(target: "validator_manager", "set_remp_core_interface: {}", e);
            }

            runtime.clone().spawn(async move { 
                log::info!("Starting REMP responses polling loop");
                remp_iface.poll_responses_loop().await; 
                log::info!("Finishing REMP responses polling loop");
            });
        }

        if let Err(e) = manager.invoke().await {
            log::error!(target: "validator_manager", "FATAL!!! Unexpected error in validator manager: {:?}", e);
        }
        engine.release_stop(Engine::MASK_SERVICE_VALIDATOR_MANAGER);
    });
}

