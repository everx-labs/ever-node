/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific EVERX DEV software governing permissions and
* limitations under the License.
*/

use std::{cmp::max, ops::RangeInclusive, sync::{*, atomic::{Ordering, AtomicU64}}, time::*};
use crossbeam_channel::Receiver;

use catchain::utils::get_hash;
use ever_block::{BlockIdExt, ShardIdent, ValidatorSet, ValidatorDescr, UnixTime32};
use ever_block::{fail, error, Result, UInt256};
use validator_session::{
    BlockHash, BlockPayloadPtr, CatchainOverlayManagerPtr,
    SessionId, SessionPtr, SessionListenerPtr, SessionFactory,
    SessionListener, SessionNode, SessionOptions,
    PublicKey, PrivateKey, PublicKeyHash, ValidatorBlockCandidate,
    ValidatorBlockCandidateCallback, ValidatorBlockCandidateDecisionCallback
};

#[cfg(feature = "slashing")]
use validator_session::SlashingValidatorStat;

use validator_session_listener::{
    process_validation_queue,
    ValidatorSessionListener, ValidationAction,
};

use super::*;
use super::fabric::*;
use crate::{
    engine_traits::EngineOperations,
    validator::{
        catchain_overlay::CatchainOverlayManagerImpl,
        mutex_wrapper::MutexWrapper,
        reliable_message_queue::RmqQueueManager,
        remp_manager::RempManager,
        remp_block_parser::check_history_up_to_cc,
        sessions_computing::GeneralSessionInfo,
        validator_utils::{
            validatordescr_to_session_node,
            validator_query_candidate_to_validator_block_candidate, ValidatorListHash,
        }
    }
};
use crate::validator::reliable_message_queue::RempQueueCollatorInterfaceImpl;

#[cfg(feature = "slashing")]
use crate::validator::slashing::SlashingManagerPtr;
//#[cfg(feature = "fast_finality")]
use crate::validator::validator_utils::PrevBlockHistory;
// #[cfg(feature = "fast_finality")]
// use crate::validator::workchains_fast_finality::compute_actual_finish;

use crate::validator::verification::VerificationManagerPtr;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum ValidatorGroupStatus {
    Created, Countdown { start_at: tokio::time::Instant },
    Sync, Active, Stopping, Stopped
}

impl std::fmt::Display for ValidatorGroupStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidatorGroupStatus::Created => write!(f, "created"),
            ValidatorGroupStatus::Countdown {start_at: at} => {
                let now = tokio::time::Instant::now();
                write!(f, "cntdwn {}", at.saturating_duration_since(now).as_secs())
            },
            ValidatorGroupStatus::Sync => write!(f, "sync"),
            ValidatorGroupStatus::Active => write!(f, "active"),
            ValidatorGroupStatus::Stopping => write!(f, "stopping"),
            ValidatorGroupStatus::Stopped => write!(f, "stopped")
        }
    }
}

impl ValidatorGroupStatus {
    pub fn before (&self, of: &ValidatorGroupStatus) -> bool {
        match (&self, of) {
            (ValidatorGroupStatus::Countdown {..}, ValidatorGroupStatus::Countdown {..}) => false,
            _ => self <= of
        }
    }
}

pub struct ValidatorGroupImpl {
    prev_block_ids: PrevBlockHistory, //Vec<BlockIdExt>,
    last_known_round: u32,

    shard: ShardIdent,
    session_id: SessionId,
    session_ptr: Option<SessionPtr>,
    reliable_queue: Option<Arc<RmqQueueManager>>,

    min_masterchain_block_id: Option<BlockIdExt>,
    min_ts: SystemTime,

    #[allow(dead_code)]
    replay_finished: bool,
    on_generate_slot_invoked: bool,
    on_candidate_invoked: bool,

    status: ValidatorGroupStatus,
}

impl Drop for ValidatorGroupImpl {
    fn drop (&mut self) {
        // Important: does not stop the session -- to avoid database deletion,
        // which otherwise would happen each time the validator-manager crashes.
        log::info!(target: "validator", "ValidatorGroupImpl: dropping session {}", self.info());
    }
}

impl ValidatorGroupImpl {
    // Creates and starts session
    fn start(
        &mut self,
        session_listener: validator_session::SessionListenerPtr,
        prev: Vec<BlockIdExt>,
        min_masterchain_block_id: BlockIdExt,
        min_ts: SystemTime,
        g: Arc<ValidatorGroup>,
        prev_validators: &Vec<ValidatorDescr>,
        next_validator_set: &ValidatorSet,
        master_cc_range: &RangeInclusive<u32>,
        start_remp_session: bool,
        rt: tokio::runtime::Handle
    ) -> Result<()> {
        if self.status >= ValidatorGroupStatus::Stopping {
            fail!("Inactive session cannot be started! {}", self.info())
        }

        self.status = ValidatorGroupStatus::Sync;

        log::info!(target: "validator", "Starting session {} (start remp: {})", self.info(), start_remp_session);

        self.prev_block_ids.update_prev(&prev);
        self.min_masterchain_block_id = Some(min_masterchain_block_id.clone());
        self.min_ts = min_ts;

        let nodes_res: Result<Vec<SessionNode>> = g.validator_set.list().iter()
            .map(validatordescr_to_session_node)
            .collect();
        let nodes = nodes_res?;

        let overlay_manager: CatchainOverlayManagerPtr =
            Arc::new(CatchainOverlayManagerImpl::new(g.engine.validator_network(), g.validator_list_id.clone()));
        let db_path = format!("{}/catchains", g.engine.db_root_dir()?);
        let db_suffix = format!(
            "-{}.{}.{}.{}.", 
            g.shard().workchain_id(),
            g.shard().shard_prefix_as_str_with_tag(),
            min_masterchain_block_id.seq_no, 
            g.general_session_info.catchain_seqno
        );

        let session_ptr = 
        /* Skip single node mode due to instabilities 
        if nodes.len() == 1 {
            //special case for single node session

            let mut options = g.config.clone();

            options.skip_single_node_session_validations = true;

            SessionFactory::create_single_node_session(
                &options,
                &g.session_id,
                &g.local_key,
                db_path,
                db_suffix,
                session_listener,
            )
        } else {
        */
            SessionFactory::create_session(
                &g.config,
                &g.session_id,
                &nodes,
                &g.local_key,
                db_path,
                db_suffix,
                g.allow_unsafe_self_blocks_resync,
                overlay_manager,
                session_listener,
            );
        /*};*/

        if let Some(remp_manager) = &g.remp_manager {
            if start_remp_session {
                let mut rq = RmqQueueManager::new(
                    g.engine.clone(), remp_manager.clone(), g.shard().clone(),
                    &g.local_key
                );
                if let Err(error) = rq.set_queues(
                    g.general_session_info.clone(),
                    g.validator_list_id.clone(),
                    master_cc_range,
                    prev_validators,
                    &next_validator_set.list().to_vec()
                ) {
                    log::error!(target: "remp", "Cannot create queue for {}: {}", g.general_session_info, error);
                };
                let reliable_queue_clone = Arc::new(rq);
                self.reliable_queue = Some(reliable_queue_clone.clone());

                let local_key = g.local_key.clone();
                rt.clone().spawn(async move {
                    match reliable_queue_clone.start(local_key).await {
                        Ok(()) => (),
                        Err(e) => log::error!(target: "validator", "Cannot start RMQ {}: {}",
                           reliable_queue_clone.info_string().await, e
                        )
                    }
                });
            }
        }

        let g_clone = g.clone();
        rt.clone().spawn(async move {
            process_validation_queue (g_clone.receiver.clone(), g_clone.clone(), rt).await;
        });

        log::trace!(target: "validator", "Started session {}, options {:?}, ref.cnt = {}",
            self.info(), g.config, SessionPtr::strong_count(&session_ptr)
        );

        self.session_ptr = Some(session_ptr);
        return Ok(())
    }

    pub fn _session_ptr(&self) -> Option<SessionPtr> {
        return self.session_ptr.clone();
    }

    pub fn info_round(&self, round: u32) -> String {
        let next_seqno = self.prev_block_ids
            .get_next_seqno()
            .map_or("".to_owned(), |seqno| format!(", {} next seqno", seqno));
        return format!(
            "session_status: id {:x}, shard {}{}, {}, round {}, prevs {}",
            self.session_id, self.shard, next_seqno, self.status, round, self.prev_block_ids
        );
    }

    pub fn get_master_cc_range(&self) -> Option<RangeInclusive<u32>> {
        self.reliable_queue.as_ref().map(|q| q.get_master_cc_range()).flatten()
    }

    pub fn info(&self) -> String {
        return self.info_round(self.last_known_round);
    }

    // Initializes structure
    pub fn new(
        shard: ShardIdent,
        session_id: validator_session::SessionId,
    ) -> ValidatorGroupImpl {
        log::info!(target: "validator", "Initializing session {:x}, shard {}", session_id, shard);

        let prev_block_ids = PrevBlockHistory::new(&shard);
        ValidatorGroupImpl {
            min_masterchain_block_id: None,
            min_ts: SystemTime::now(),
            status: ValidatorGroupStatus::Created,
            last_known_round: 0,

            shard,
            session_id,
            session_ptr: None,
            reliable_queue: None,
            prev_block_ids,

            on_candidate_invoked: false,
            on_generate_slot_invoked: false,
            replay_finished: false,
        }
    }

/*
    pub fn update_next_validator_set(&mut self, catchain_seqno: u32, curr_set: &Vec<ValidatorDescr>, next_set: &Vec<ValidatorDescr>) {
        self.reliable_queue.switch_queue(catchain_seqno, curr_set, next_set);
    }
 */

    pub fn update_round(&mut self, round: u32) -> (u32, PrevBlockHistory, Option<BlockIdExt>, SystemTime)
    {
        self.last_known_round = max(self.last_known_round, round);
        return (self.last_known_round, self.prev_block_ids.clone(), self.min_masterchain_block_id.clone(), self.min_ts)
    }
}

pub struct ValidatorGroup {
    general_session_info: Arc<GeneralSessionInfo>,
    local_key: PrivateKey,
    config: SessionOptions,
    session_id: SessionId,
    //catchain_seqno: u32,
    validator_list_id: ValidatorListHash,

    //shard: ShardIdent,
    engine: Arc<dyn EngineOperations>,
    remp_manager: Option<Arc<RempManager>>,
    validator_set: ValidatorSet,
    #[allow(dead_code)]
    allow_unsafe_self_blocks_resync: bool,

    group_impl: Arc<MutexWrapper<ValidatorGroupImpl>>,
    callback: Arc<dyn SessionListener + Send + Sync>,
    receiver: Arc<Receiver<ValidationAction>>,

    #[cfg(feature = "slashing")]
    slashing_manager: SlashingManagerPtr,
    verification_manager: Option<VerificationManagerPtr>,
    last_validation_time: AtomicU64,
    last_collation_time: AtomicU64,
}

impl ValidatorGroup {
    pub fn new(
        general_session_info: Arc<GeneralSessionInfo>,
        local_key: PrivateKey,
        session_id: SessionId,
        validator_list_id: ValidatorListHash,
        validator_set: ValidatorSet,
        config: SessionOptions,
        remp_manager: Option<Arc<RempManager>>,
        engine: Arc<dyn EngineOperations>,
        allow_unsafe_self_blocks_resync: bool,
        #[cfg(feature = "slashing")]
        slashing_manager: SlashingManagerPtr,
        verification_manager: Option<VerificationManagerPtr>,
    ) -> Self {
        let group_impl = ValidatorGroupImpl::new(
            general_session_info.shard.clone(),
            session_id.clone(),
        );
        let id = format!("Val. group {} {:x}", general_session_info.shard, session_id);
        let (listener, receiver) = ValidatorSessionListener::create();

        log::trace!(target: "validator", "Creating validator group: {}", id);
        ValidatorGroup {
            general_session_info,
            local_key,
            validator_list_id,
            session_id,
            validator_set,
            config,
            engine,
            allow_unsafe_self_blocks_resync,
            remp_manager,
            group_impl: Arc::new(MutexWrapper::new(group_impl, id)),
            callback: Arc::new(listener),
            receiver: Arc::new(receiver),
            #[cfg(feature = "slashing")]
            slashing_manager,
            verification_manager,
            last_validation_time: AtomicU64::new(0),
            last_collation_time: AtomicU64::new(0)
        }
    }

    /// Mutex used inside. Needs to cache the result
    pub async fn get_next_block_descr(&self, root_hash: Option<&BlockHash>) -> String {
        self.group_impl.execute_sync(|group_impl| group_impl.prev_block_ids.get_next_block_descr(root_hash)).await
    }

    pub fn shard(&self) -> &ShardIdent {
        &self.general_session_info.shard
    }

    pub fn last_validation_time(&self) -> u64 {
        self.last_validation_time.load(Ordering::Relaxed)
    }

    pub fn last_collation_time(&self) -> u64 {
        self.last_collation_time.load(Ordering::Relaxed)
    }

    pub fn make_validator_session_callback(&self) -> SessionListenerPtr {
        Arc::downgrade(&self.callback)
    }

    pub async fn start_with_status(
        self: Arc<ValidatorGroup>,
        prev_validators: &Vec<ValidatorDescr>,
        next_validators: &ValidatorSet,
        validation_start_status: ValidatorGroupStatus,
        prev: Vec<BlockIdExt>,
        min_masterchain_block_id: BlockIdExt,
        min_ts: SystemTime,
        master_cc_range: &RangeInclusive<u32>,
        start_remp_session: bool,
        rt: tokio::runtime::Handle
    ) -> Result<()> {
        let prev_validators_cloned = prev_validators.clone();
        let next_validators_cloned = next_validators.clone();
        let master_cc_range_cloned = master_cc_range.clone();
        self.set_status(validation_start_status.clone()).await?;
        rt.clone().spawn (async move {
            if let ValidatorGroupStatus::Countdown { start_at } = validation_start_status {
                log::trace!(target: "validator", "Session delay started: {}", self.info().await);
                tokio::time::sleep_until(start_at).await;
            }

            let callback = self.make_validator_session_callback();
            self.group_impl.execute_sync(|group_impl|
            {
                if group_impl.status <= ValidatorGroupStatus::Active {
                    if let Err(e) = group_impl.start(
                        callback,
                        prev,
                        min_masterchain_block_id,
                        min_ts,
                        self.clone(),
                        &prev_validators_cloned,
                        &next_validators_cloned,
                        &master_cc_range_cloned,
                        start_remp_session,
                        rt
                    )
                    {
                        log::error!(target: "validator", "Cannot start group: {}", e);
                    }
                }
                else {
                    log::trace!(target: "validator", "Session deleted before countdown: {}", group_impl.info());
                }
            }).await;
        });
        Ok(())
    }

    pub async fn add_next_validators(
        self: Arc<ValidatorGroup>, 
        prev_validators: &Vec<ValidatorDescr>,
        next_validator_set: &ValidatorSet,
        new_session_info: Arc<GeneralSessionInfo>,
        next_master_cc_range: &RangeInclusive<u32>,
    ) -> Result<()> {
        log::debug!(target: "validator", "Adding next validators {}: next set {:?}", self.info().await, next_validator_set);
        let (rmq, group_status) =
            self.group_impl.execute_sync(|group_impl| (group_impl.reliable_queue.clone(), group_impl.status)).await;

        if group_status >= ValidatorGroupStatus::Stopping {
            return Ok(());
        }

        if let Some(rmq) = rmq {
            log::debug!(target: "validator", "Adding next validators (add_new_queue) {}", self.info().await);
            rmq.add_new_queue(
                next_master_cc_range,
                prev_validators,
                &next_validator_set.list().to_vec(),
                new_session_info,
                self.validator_list_id.clone()
            ).await?;
        }

        log::debug!(target: "validator", "Adding next validators (finished) {}", self.info().await);
        Ok(())
    }

    pub async fn stop(self: Arc<ValidatorGroup>, rt: tokio::runtime::Handle, new_master_cc_range: Option<RangeInclusive<u32>>) -> Result<()> {
        self.set_status(ValidatorGroupStatus::Stopping).await?;
        log::debug!(target: "validator", "Stopping group: {}", self.info().await);
        let group_impl = self.group_impl.clone();
        let self_clone = self.clone();
        rt.spawn({
            async move {
                log::debug!(target: "validator", "Stopping group (spawn): {}", self_clone.info().await);
                let (reliable_message_queue, session_ptr) =
                    group_impl.execute_sync(|group_impl|
                        (group_impl.reliable_queue.clone(), group_impl.session_ptr.clone())
                    ).await;
                if let Some(rmq) = reliable_message_queue {
                    log::debug!(target: "validator", "Stopping group (spawn) {}, rmq: {}", self_clone.info().await, rmq);
                    if let Some(new_cc_range) = new_master_cc_range {
                        log::debug!(target: "validator", "Forwarding messages, rmq: {}, new cc range: {:?}", rmq, new_cc_range);
                        rmq.forward_messages(&new_cc_range, self_clone.local_key.clone()).await;
                        log::debug!(target: "validator", "Messages forwarded, rmq: {}, new cc range: {:?}", rmq, new_cc_range);
                    }
                }
                if let Some(s_ptr) = session_ptr {
                    log::debug!(target: "validator", "Stopping catchain: {}", self_clone.info().await);
                    s_ptr.stop();
                }
                log::debug!(target: "validator", "Group stopped: {}", self_clone.info().await);
                let _ = self_clone.set_status(ValidatorGroupStatus::Stopped).await;
                log::info!(target: "validator", "Status set: {}", self_clone.info().await);
                let _ = self_clone.destroy_db().await;
                log::debug!(target: "validator", "Db destroyed: {}", self_clone.info().await);
            }
        });
        log::debug!(target: "validator", "Stopping group {}, stop spawned", self.info().await);
        Ok(())
    }

    async fn save_block_candidate(&self, vb_candidate: ValidatorBlockCandidate) -> Result<()> {
        self.engine.save_block_candidate(&self.session_id, vb_candidate)
    }

    async fn load_block_candidate(&self, root_hash: &UInt256) -> Result<Arc<ValidatorBlockCandidate>> {
        self.engine.load_block_candidate(&self.session_id, root_hash)
    }

    pub async fn destroy_db(&self) -> Result<()> {

        while !self.engine.destroy_block_candidates(&self.session_id)? {
            tokio::task::yield_now().await
        }
        Ok(())
    }

    pub async fn get_status(&self) -> ValidatorGroupStatus {
        self.group_impl.execute_sync(|group_impl| group_impl.status).await
    }

    pub async fn set_status(&self, status: ValidatorGroupStatus) -> Result<()> {
        self.group_impl.execute_sync(|group_impl|
        if group_impl.status.before(&status) {
            group_impl.status = status;
            Ok(())
        } else {
            fail!("Status cannot retreat, from {} to {}", group_impl.status, status)
        }).await
    }

    pub fn get_validator_list_id(&self) -> ValidatorListHash {
        self.validator_list_id.clone()
    }

    pub async fn get_master_cc_range(&self) -> Option<RangeInclusive<u32>> {
        self.group_impl.execute_sync(|group_impl| group_impl.get_master_cc_range()).await
    }

    pub async fn get_reliable_message_queue(&self) -> Option<Arc<RmqQueueManager>> {
        self.group_impl.execute_sync(|group_impl| group_impl.reliable_queue.clone()).await
    }

    pub async fn get_remp_queue_collator_interface(&self) -> Option<Arc<RempQueueCollatorInterfaceImpl>> {
        let queue_manager = self.get_reliable_message_queue().await;
        queue_manager.map(|x| {
            Arc::new(RempQueueCollatorInterfaceImpl::new(x))
        })
    }

    pub async fn info_round(&self, round: u32) -> String {
        self.group_impl.execute_sync(|group_impl| group_impl.info_round(round)).await
    }

    pub async fn info(&self) -> String {
        self.group_impl.execute_sync(|group_impl| group_impl.info()).await
    }

    pub async fn poll_rmq(&self) {
        if let Some(rmq) = self.get_reliable_message_queue().await {
            log::debug!(target: "validator", "Polling RMQ {}, group {}", rmq, self.info().await);
            rmq.poll().await;
        };
    }

    async fn check_in_sync(&self, mc_blocks: &PrevBlockHistory) -> Result<bool> {
        match self.get_status().await {
            ValidatorGroupStatus::Active => return Ok(true),
            ValidatorGroupStatus::Sync => (),
            s => fail!("Cannot validate in status {}", s)
        }

        let remp = match &self.remp_manager {
            Some(remp) => remp,
            None => {
                self.set_status(ValidatorGroupStatus::Active).await?;
                return Ok(true)
            }
        };

        match self.get_master_cc_range().await {
            None => {
                self.set_status(ValidatorGroupStatus::Active).await?;
                return Ok(true)
                //fail!("Shard {} history cannot be known: master_cc_range is unavailable")
            }
            Some(mc_range) => {
                if let Some(unknown_block) = check_history_up_to_cc(
                    self.engine.clone(), remp.message_cache.clone(), mc_blocks.get_prevs(), *mc_range.start()
                ).await? {
                    log::warn!(target: "validator", "Shard {} history from {:?} up to master cc {} is not fully known: block {} is not processed",
                        self.info().await, mc_blocks.get_prevs(), *mc_range.start(), unknown_block
                    );
                    Ok(false)
                }
                else {
                    self.set_status(ValidatorGroupStatus::Active).await?;
                    Ok(true)
                }
            }
        }
    }

    pub async fn on_generate_slot(
        &self, 
        round: u32, 
        callback: ValidatorBlockCandidateCallback, 
        rt: tokio::runtime::Handle
    ) {
        let next_block_descr = self.get_next_block_descr(None).await;
        log::info!(
            target: "validator", 
            "({}): ValidatorGroup::on_generate_slot: collator request, {}",
            next_block_descr,
            self.info_round(round).await
        );

        let (_lk_round, prev_block_ids, mm_block_id, min_ts) =
            self.group_impl.execute_sync(|group_impl| group_impl.update_round (round)).await;

        let remp_queue_collator_interface_impl = match self.check_in_sync(&prev_block_ids).await {
            Err(e) => {
                log::warn!(target: "validator", "({}): Error checking sync for {}: `{}`",
                    next_block_descr, self.info_round(round).await, e
                );
                callback(Err(e));
                return;
            }
            Ok(false) => None,
            Ok(true) => self.get_remp_queue_collator_interface().await
        };

        let result = match mm_block_id {
            Some(mc) => {
                match run_collate_query (
                    self.shard().clone(),
                    min_ts,
                    mc.seq_no,
                    &prev_block_ids,
                    remp_queue_collator_interface_impl.clone().map(|x| x.into_interface()),
                    self.local_key.clone(),
                    self.validator_set.clone(),
                    self.engine.clone(),
                    self.verification_manager.clone(),
                ).await {
                    Ok(b) => Ok(Arc::new(b)),
                    Err(x) => Err(x)
                }
            }
            None => Err(error!("Min masterchain block id missing")),
        };

        let candidate = match self.verification_manager.clone() {
            Some(_) => match &result {
                Ok(candidate) => Some(candidate.clone()),
                _ => None
            },
            None => None,
        };

        let (rh,fh) = match &result {
            Err(_) => (UInt256::default(), UInt256::default()),
            Ok(candidate) => (candidate.id.root_hash.clone(), get_hash(&candidate.data.data()))
        };

        let next_block_id = self.group_impl.execute_sync(|group_impl|
            group_impl.prev_block_ids.get_next_block_id(&rh, &fh)
        ).await;

        let return_result_message = if let Some(x) = remp_queue_collator_interface_impl {
            match x.update_queues_after_collation(&next_block_id, result.is_ok()).await {
                Ok((total, returned)) =>
                    format!("total external messages {}, processed {}, returned to queue {}", total, total as i64 - returned as i64, returned),
                Err(e) => format!("error returning non-processed external messages to queue `{}`", e)
            }
        }
        else {
            format!("no external messages processed")
        };

        let result_message = match &result {
            Ok(_) => {
                let now = UnixTime32::now().as_u32() as u64;
                self.last_collation_time.fetch_max(now, Ordering::Relaxed);

                format!("Collation successful")
            }
            Err(x) => {
                format!("Collation failed: `{}`", x)
            }
        };

        let result_message = format!("{}{}", result_message, return_result_message);
/*
        if let Some(rmq) = self.get_reliable_message_queue().await {
            if let Err(e) = rmq.process_collation_result().await {
                log::error!(target: "validator", "({}): Error processing collation results for {}: `{}`",
                    next_block_descr,
                    self.info_round(round).await,
                    e
                )
            }
        }
*/
        log::info!(target: "validator", "({}): ValidatorGroup::on_generate_slot: {}, {}",
            next_block_descr,
            self.info_round(round).await, result_message
        );

        self.group_impl.execute_sync(|group_impl| group_impl.on_generate_slot_invoked = true).await;

        callback(result);

        if let Some(verification_manager) = self.verification_manager.clone() {
            if let Some(candidate) = candidate {
                log::debug!(target:"verificator", "Received new candidate for round {} for shard {:?}", round, self.shard());
                let verification_manager = verification_manager.clone();

                let candidate = super::BlockCandidate {
                    block_id: next_block_id,
                    data: candidate.data.data().to_vec(),
                    collated_file_hash: candidate.collated_file_hash.clone(),
                    collated_data: candidate.collated_data.data().to_vec(),
                    created_by: self.local_key.pub_key().expect("source must contain pub_key").into(),
                };

                rt.clone().spawn(async move {
                    verification_manager.send_new_block_candidate(&candidate).await;
                });
            }
        }
    }

    async fn do_candidate_validation(&self, round: u32, source: &PublicKey, candidate: BlockCandidate) -> Result<SystemTime> {
        let (lk_round, prev_block_ids, mc_block_id_opt, min_ts) =
            self.group_impl.execute_sync(|group_impl| {
                group_impl.update_round(round)
            }).await;

        if round < lk_round {
            fail!("round {} < self.last_known_round {}", round, lk_round);
        }

        /* TODO: check collated messages
                    match self.check_in_sync(&prev_block_ids).await {
                        Err(e) => {
                            log::warn!(target: "validator", "ValidatorGroup::on_generate_slot: session {} shards are not in sync yet: {}", self.info_round(round).await, e);
                            callback(Err(e));
                            return;
                        }
                        Ok(()) => ()
                    }
        */

        prev_block_ids.ensure_next_block_new(&candidate.block_id.root_hash, &candidate.block_id.file_hash)?;

        let mc_block_id = mc_block_id_opt.ok_or_else(|| error!("Min masterchain block id missing"))?;

        let mc = self.engine.load_last_applied_mc_state().await?;

        let last_applied_block_opt = if self.general_session_info.shard.is_masterchain() {
            Some(mc.block_id().clone())
        }
        else {
            mc.shard_state_extra()?
                .shards().find_shard(&self.general_session_info.shard)?
                .map(|x| x.block_id().clone())
        };

        if let Some(last_applied_block) = last_applied_block_opt {
            if last_applied_block.seq_no >= candidate.block_id.seq_no {
                fail!(
                    "Attempting to validate obsolete candidate block {}, actual last shard block {}",
                    candidate.block_id, last_applied_block
                );
            }
        }

        let validation_completion_time = run_validate_query(
            self.shard().clone(),
            min_ts,
            mc_block_id,
            &prev_block_ids,
            candidate.clone(),
            self.validator_set.clone(),
            self.engine.clone(),
            SystemTime::now() + Duration::new(10, 0),
            self.verification_manager.clone(),
            false,
        ).await?;

        let vb_candidate = validator_query_candidate_to_validator_block_candidate(
            source.clone(), candidate
        );

        self.save_block_candidate(vb_candidate).await?;

        self.last_validation_time.fetch_max(
            validation_completion_time.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs(),
            Ordering::Relaxed
        );

        Ok(validation_completion_time)
    }

    // Validate_query
    pub async fn on_candidate(
        &self,
        round: u32,
        source: PublicKey,
        root_hash: BlockHash,
        data: BlockPayloadPtr,
        collated_data: BlockPayloadPtr,
        callback: ValidatorBlockCandidateDecisionCallback,
    ) {
        let next_block_descr = self.get_next_block_descr(Some(&root_hash)).await;

        let candidate_id = format!("source {}, rh {:x}", source.id(), root_hash);

        log::trace!(target: "validator", "({}): ValidatorGroup::on_candidate: {}, {}",
            next_block_descr,
            candidate_id, self.info_round(round).await);

        let candidate_block_id = self.group_impl.execute_sync(|group_impl|
            group_impl.prev_block_ids.get_next_block_id(&root_hash, &get_hash(&data.data()))
        ).await;

        let candidate = super::BlockCandidate {
            block_id: candidate_block_id, //BlockIdExt::with_params(self.shard().clone(), 0, root_hash, get_hash(&data.data())),
            data: data.data().to_vec(),
            collated_file_hash: catchain::utils::get_hash (&collated_data.data()),
            collated_data: collated_data.data().to_vec(),
            created_by: UInt256::from(source.pub_key().expect("source must contain pub_key")),
        };

        let validation_result = self.do_candidate_validation(round, &source, candidate).await;
        let validation_result_message = match &validation_result {
            Ok(completion_time) => format!("Validation successful: finished at {:?}", completion_time),
            Err(e) => format!("Validation failed with verdict `{}`", e),
        };

/*
        let result_message = match &result {
            Ok(x) => {
                let vb_candidate = validator_query_candidate_to_validator_block_candidate(
                    source.clone(), candidate
                );

                match self.save_block_candidate(vb_candidate).await {
                    Ok(()) => {
                        self.last_validation_time.fetch_max(
                            x.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs(),
                            Ordering::Relaxed
                        );
                        format!("Validation successful: finished at {:?}", x)
                    }
                    Err(x) => format!("Validation successful, db error `{}`", x)
                }
            }
            Err(x) => format!("Validation failed with verdict `{}`", x),
        };
 */
        self.group_impl.execute_sync(|group_impl| group_impl.on_candidate_invoked = true).await;

        log::info!(target: "validator", "({}): ValidatorGroup::on_candidate: {}, {}, {}",
            next_block_descr,
            candidate_id, self.info_round(round).await, validation_result_message
        );

        callback(validation_result);

        log::trace!(target: "validator", "({}): ValidatorGroup::on_candidate: {}, {}, {}, callback called",
            next_block_descr,
            candidate_id, self.info_round(round).await, validation_result_message
        );
    }

    // Accept_block
    //self.accept_block_candidate (round, source, root_hash, file_hash, data, signatures, approve_signatures);
    //signatures: Vec<(PublicKeyHash, BlockPayloadPtr)>,
    //approve_signatures: Vec<(PublicKeyHash, BlockPayloadPtr)>)
    pub async fn on_block_committed(
        &self,
        round: u32,
        source: PublicKey,
        root_hash: BlockHash,
        file_hash: BlockHash,
        data: BlockPayloadPtr,
        sig_set: Vec<(PublicKeyHash, BlockPayloadPtr)>,
        approve_sig_set: Vec<(PublicKeyHash, BlockPayloadPtr)>,
    ) {
        let next_block_descr = self.get_next_block_descr(Some(&root_hash)).await;

        let data_vec = data.data().to_vec();
        let we_generated = source.id() == self.local_key.id();

        log::info!(target: "validator", 
            "({}): ValidatorGroup::on_block_committed: source {}, data size = {}, {}" ,
            next_block_descr,
            source.id(), data_vec.len(), self.info_round(round).await
        );

        let prev_block_history = self.group_impl.execute_sync(|group_impl| {
            if round >= group_impl.last_known_round {
                group_impl.last_known_round = round + 1;
            };

            group_impl.prev_block_ids.clone()
        }).await;

        if let Err(e) = prev_block_history.ensure_next_block_new(&root_hash, &file_hash) {
            log::error!(target: "validator", "({}): ValidatorGroup::on_block_committed: source {}, `{}`, {}",
                next_block_descr, source.id(), e, self.info_round(round).await
            );
            return;
        }
        let next_block_id = prev_block_history.get_next_block_id(&root_hash, &file_hash);

        log::info!(target: "validator", 
            "({}): ValidatorGroup::on_block_committed: source {}, id {}, data size = {}, {}",
            next_block_descr,
            source.id(), next_block_id, data_vec.len(), self.info_round(round).await
        );

        let mut result = run_accept_block_query(
            next_block_id.clone(),
            if data_vec.len() > 0 {
                Some(data_vec)
            } else {
                None
            },
            prev_block_history.get_prevs().clone(),
            self.validator_set.clone(),
            sig_set,
            approve_sig_set,
            we_generated,
            self.engine.clone(),
        ).await;

        if let Ok(()) = result {
            if let Some(rmq) = self.get_reliable_message_queue().await {
                log::trace!(target: "remp", "Processing committed shardblock {}", next_block_id);
                result = rmq.process_messages_from_committed_block(next_block_id.clone()).await;
            }
        }

        let (full_result, new_prevs) = self.group_impl.execute_sync(|group_impl| {
            let full_result = match result {
                Ok(()) =>
                    if !group_impl.prev_block_ids.same_prevs(&prev_block_history) {
                        Err(error!("Sync error: two requests at a time, prevs have changed!!!"))
                    } else {
                        Ok(())
                    },
                err => err
                // TODO: retry block commit
            };

            group_impl.prev_block_ids.update_prev(&vec!(next_block_id));

            (full_result, group_impl.prev_block_ids.display_prevs())
        }).await;

        match full_result {
            Ok(()) => log::info!(
                target: "validator", 
                "({}): ValidatorGroup::on_block_committed: success!, source {}, {}, new prevs {}",
                next_block_descr,
                source.id(),
                self.info_round(round).await,
                new_prevs
            ),
            Err(err) => log::error!(
                target: "validator", 
                "({}): ValidatorGroup::on_block_committed: error!, source {}, error message: `{}`, {}, new prevs {}",
                next_block_descr,
                source.id(),
                err,
                self.info_round(round).await,
                new_prevs
            )
        }
    }

    pub async fn on_block_skipped(&self, round: u32) {
        log::info!(
            target: "validator", 
            "({}): ValidatorGroup::on_block_skipped, {}",
            self.get_next_block_descr(None).await,
            self.info_round(round).await
        );

        self.group_impl.execute_sync(|group_impl|
            if round > group_impl.last_known_round {
                group_impl.last_known_round = round + 1;
            }
        ).await;
    }

    pub async fn on_get_approved_candidate(
        &self,
        _source: PublicKey,
        root_hash: BlockHash,
        file_hash: BlockHash,
        _collated_data_hash: BlockHash,
        callback: ValidatorBlockCandidateCallback)
    {
        let next_block_descr = self.get_next_block_descr(Some(&root_hash)).await;

        log::info!(
            target: "validator", 
            "({}): ValidatorGroup::on_get_approved_candidate rh {:x}, fh {:x}, {}",
            next_block_descr,
            root_hash, file_hash, self.info().await
        );

        let result = self.load_block_candidate(&root_hash).await;
        let result_txt = match &result {
            Ok(_) => format!("Ok"),
            Err(err) => format!("Candidate not found: {}", err)
        };
        log::info!(
            target: "validator", 
            "({}): ValidatorGroup::on_get_approved_candidate {}, {}",
            next_block_descr,
            result_txt, self.info().await
        );
        callback(result);
    }

    #[cfg(feature = "slashing")]
    pub async fn on_slashing_statistics(&self, round: u32, stat: SlashingValidatorStat) {
        log::debug!(
            target: "validator", 
            "({}): ValidatorGroup::on_slashing_statistics round {}, stat {:?}",
            self.get_next_block_descr().await,
            round, stat
        );
        #[cfg(feature = "slashing")]
        self.slashing_manager.update_statistics(&stat);
    }

}

impl Drop for ValidatorGroup {
    fn drop (&mut self) {
        // Important: does not stop the session -- to avoid database deletion,
        // which otherwise would happen each time the validator-manager crashes.
        log::info!(target: "validator", "ValidatorGroup: dropping session {:x}", self.session_id);
    }
}
