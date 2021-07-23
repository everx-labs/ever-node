#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_macros)]

use std::cmp::max;
use std::sync::*;
use std::sync::atomic::{Ordering, AtomicU64};
use std::time::*;
use crossbeam_channel::{Sender, Receiver};
use tokio::{runtime::Runtime, sync::Mutex};

use crate::{engine_traits::{EngineOperations, PrivateOverlayOperations}};
use catchain::utils::get_hash;
use ton_block::{BlockIdExt, ShardIdent, ValidatorSet};
use ton_types::{Result, UInt256};
use validator_session::*;
use validator_utils::{
    sigpubkey_to_publickey, validatordescr_to_session_node,
    validator_query_candidate_to_validator_block_candidate, ValidatorListHash
};
use validator_session_listener::{ValidatorSessionListener, ValidationAction, OnBlockCommitted,
                                 process_validation_queue
};

use super::*;
use super::fabric::*;
use super::validate_query::*;
use super::candidate_db::CandidateDb;
use catchain::CatchainOverlay;
use failure::Error;
use crate::validator::slashing::SlashingManagerPtr;

struct CatchainOverlayManagerImpl {
    network: Weak<dyn PrivateOverlayOperations>,
    validator_list_id: UInt256
}

impl CatchainOverlayManagerImpl {
    fn new(network: Arc<dyn PrivateOverlayOperations>, validator_list_id: UInt256) -> Self {
        Self {
            network: Arc::downgrade(&network),
            validator_list_id
        }
    }
}

impl catchain::CatchainOverlayManager for CatchainOverlayManagerImpl {

    fn start_overlay(
        &self,
        local_id: &PublicKeyHash,
        overlay_short_id: &Arc<catchain::PrivateOverlayShortId>,
        nodes: &Vec<CatchainNode>,
        listener: catchain::CatchainOverlayListenerPtr,
        replay_listener: catchain::CatchainOverlayLogReplayListenerPtr,
    ) -> Result<CatchainOverlayPtr> {
        let engine_network = self.network.upgrade().unwrap();
        engine_network
            .create_catchain_client(
                self.validator_list_id.clone(), overlay_short_id, nodes, listener, replay_listener
            )
    }

    /// Stop existing overlay
    fn stop_overlay(
        &self,
        overlay_short_id: &Arc<catchain::PrivateOverlayShortId>,
        _overlay: &CatchainOverlayPtr,
    ) {
        let engine_network = self.network.upgrade().unwrap();

        engine_network.stop_catchain_client(overlay_short_id);
    }

}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum ValidatorGroupStatus {
    Created, Countdown { start_at: tokio::time::Instant }, Active, Stopping, Stopped
}

impl std::fmt::Display for ValidatorGroupStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidatorGroupStatus::Created => write!(f, "created"),
            ValidatorGroupStatus::Countdown {start_at: at} => {
                let now = tokio::time::Instant::now();
                write!(f, "cntdwn {}", at.saturating_duration_since(now).as_secs())
            },
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
            _ => (self <= of)
        }
    }
}

pub struct ValidatorGroupImpl {
    prev_block_ids: Vec<BlockIdExt>,
    last_known_round: u32,

    shard: ShardIdent,
    session_id: validator_session::SessionId,
    session_ptr: Option<SessionPtr>,
    candidate_db: CandidateDb,

    min_masterchain_block_id: Option<BlockIdExt>,
    min_ts: SystemTime,
    replay_finished: bool,
    on_generate_slot_invoked: bool,
    on_candidate_invoked: bool,

    status: ValidatorGroupStatus
}

impl Drop for ValidatorGroupImpl {
    fn drop (&mut self) {
        // Important: does not stop the session -- to avoid database deletion,
        // which otherwise would happen each time the validator-manager crashes.
        log::info!(target: "validator", "ValidatorGroupImpl: dropping session {}", self.info());
    }
}

pub fn prevs_to_string(prev_block_ids: &Vec<BlockIdExt>) -> String {
    prev_block_ids.iter().map(|x| format!(" {} ", x)).collect()
}

impl ValidatorGroupImpl {
    // Creates and starts session
    pub fn start(
        &mut self,
        session_listener: validator_session::SessionListenerPtr,
        prev: Vec<BlockIdExt>,
        min_masterchain_block_id: BlockIdExt,
        min_ts: SystemTime,
        g: Arc<ValidatorGroup>,
        rt: Arc<Runtime>,
    ) -> Result<()> {
        if self.status != ValidatorGroupStatus::Active {
            let msg = format!("Inactive session cannot be started! {}", self.info());
            log::error!(target: "validator", "{}", msg);
            return Err(failure::err_msg(msg));
        }
        log::info!(target: "validator", "Starting session {}", self.info());

        self.prev_block_ids = prev;
        self.min_masterchain_block_id = Some(min_masterchain_block_id);
        self.min_ts = min_ts;

        let nodes_res: Result<Vec<SessionNode>> = g.validator_set.list().iter()
            .map(validatordescr_to_session_node)
            .collect();
        let nodes = nodes_res?;

        let overlay_manager: CatchainOverlayManagerPtr =
            Arc::new(CatchainOverlayManagerImpl::new(g.engine.validator_network(), g.validator_list_id.clone()));
        let db_root = format!("{}/catchains", g.engine.db_root_dir()?);
        let db_suffix = "".to_string();
        let allow_unsafe_self_blocks_resync = false;

        let session_ptr = SessionFactory::create_session(
            &g.config,
            &g.session_id,
            &nodes,
            &g.local_key,
            &db_root,
            &db_suffix,
            allow_unsafe_self_blocks_resync,
            overlay_manager,
            session_listener,
        );

        let g_clone = g.clone();
        rt.clone().spawn(async move {
            process_validation_queue (g_clone.receiver.clone(), g_clone.clone(), rt.clone()).await;
        });

        log::trace!(target: "validator", "Started session {}, options {:?}, ref.cnt = {}",
            self.info(), g.config, SessionPtr::strong_count(&session_ptr)
        );

        self.session_ptr = Some(session_ptr);
        self.candidate_db.start()?;
        return Ok(())
    }

    pub fn get_session_ptr(&self) -> Option<SessionPtr> {
        return self.session_ptr.clone();
    }

    pub fn destroy_db(&mut self) -> Result<()> {
        self.candidate_db.destroy()
    }

    pub fn info_round(&self, round: u32) -> String {
        return format!("session_status: id {}, shard {}, {}, round {}, prevs {}",
                       self.session_id.to_hex_string(), self.shard, self.status,
                       round, prevs_to_string(&self.prev_block_ids)
        );
    }

    pub fn info(&self) -> String {
        return self.info_round(self.last_known_round);
    }

    // Initializes structure
    pub fn new(shard: ShardIdent, session_id: validator_session::SessionId, db_root_dir: &str) -> ValidatorGroupImpl {
        log::info!(target: "validator", "Initializing session {}, shard {}", session_id.to_hex_string(), shard);

        ValidatorGroupImpl {
            min_masterchain_block_id: None,
            min_ts: SystemTime::now(),
            status: ValidatorGroupStatus::Created,
            last_known_round: 0,
            candidate_db: CandidateDb::new(format!("{}/candidates/{}", db_root_dir, session_id.to_hex_string())),

            shard,
            session_id,
            session_ptr: None,
            prev_block_ids: Vec::new(),

            on_candidate_invoked: false,
            on_generate_slot_invoked: false,
            replay_finished: false
        }
    }

    // Advances block id counter
    pub fn create_next_block_id(&self, root_hash: BlockHash, file_hash: BlockHash, shard: ShardIdent) -> Result<BlockIdExt> {
        let mut seqno = 0;
        for x in &self.prev_block_ids {
            if seqno < x.seq_no {
                seqno = x.seq_no;
            }
            if x.root_hash == root_hash && x.file_hash == file_hash {
                return Err(failure::err_msg(format!(
                    "New block is equal with one of previous: new rh {}, fh {} and prev {}, {}",
                    root_hash.to_hex_string(), file_hash.to_hex_string(), x, self.info()
                )));
            }
        }

        Ok(BlockIdExt {
            shard_id: shard,
            seq_no: seqno + 1,
            root_hash: root_hash,
            file_hash: file_hash,
        })
    }

    pub fn update_round(&mut self, round: u32) -> (u32, Vec<BlockIdExt>, Option<BlockIdExt>, SystemTime)
    {
        self.last_known_round = max(self.last_known_round, round);
        return (self.last_known_round, self.prev_block_ids.clone(), self.min_masterchain_block_id.clone(), self.min_ts)
    }
}

pub struct ValidatorGroup {
    local_key: PublicKey,
    config: SessionOptions,
    session_id: validator_session::SessionId,
    validator_list_id: ValidatorListHash,

    shard: ShardIdent,
    engine: Arc<dyn EngineOperations>,
    validator_set: ValidatorSet,
    allow_unsafe_self_blocks_resync: bool,

    group_impl: Arc<tokio::sync::Mutex<ValidatorGroupImpl>>,
    callback: Arc<dyn validator_session::SessionListener + Send + Sync>,
    receiver: Arc<Receiver<ValidationAction>>,

    slashing_manager: SlashingManagerPtr,
    last_validation_time: AtomicU64,
    last_collation_time: AtomicU64,
}

impl ValidatorGroup {
    pub fn new(
        shard: ShardIdent,
        local_key: PublicKey,
        session_id: validator_session::SessionId,
        validator_list_id: ValidatorListHash,
        validator_set: ValidatorSet,
        config: SessionOptions,
        engine: Arc<dyn EngineOperations>,
        allow_unsafe_self_blocks_resync: bool,
        slashing_manager: SlashingManagerPtr,
    ) -> Self {
        let group_impl = ValidatorGroupImpl::new(shard.clone(), session_id.clone(), 
            engine.db_root_dir().expect("Can't get db_root_dir from engine"));
        let (listener, receiver) = ValidatorSessionListener::create();

        ValidatorGroup {
            shard,
            local_key,
            validator_list_id,
            session_id,
            validator_set,
            config,
            engine,
            allow_unsafe_self_blocks_resync,
            group_impl: Arc::new(tokio::sync::Mutex::new(group_impl)),
            callback: Arc::new(listener),
            receiver: Arc::new(receiver),
            slashing_manager: slashing_manager,
            last_validation_time: AtomicU64::new(0),
            last_collation_time: AtomicU64::new(0)
        }
    }

    pub fn shard(&self) -> &ShardIdent {
        &self.shard
    }

    pub fn last_validation_time(&self) -> u64 {
        self.last_validation_time.load(Ordering::Relaxed)
    }

    pub fn last_collation_time(&self) -> u64 {
        self.last_collation_time.load(Ordering::Relaxed)
    }

    pub fn make_validator_session_callback(&self) -> validator_session::SessionListenerPtr {
        Arc::downgrade(&self.callback)
    }

    pub async fn start_with_status(self_arc: Arc<ValidatorGroup>,
        validation_start_status: ValidatorGroupStatus,
        prev: Vec<BlockIdExt>,
        min_masterchain_block_id: BlockIdExt,
        min_ts: SystemTime,
        rt: Arc<Runtime>
    ) -> Result<()> {
        self_arc.set_status(validation_start_status.clone()).await?;

        rt.clone().spawn (
            async move {
                if let ValidatorGroupStatus::Countdown { start_at: at } = validation_start_status {
                    log::trace!(target: "validator", "Session delay started: {}", self_arc.info().await);
                    tokio::time::sleep_until(at).await;
                }

                let callback = self_arc.make_validator_session_callback();
                let mut group_impl = self_arc.group_impl.clone().lock_owned().await;
                if group_impl.status <= ValidatorGroupStatus::Active {
                    group_impl.status = ValidatorGroupStatus::Active;
                    group_impl.start(callback, prev, min_masterchain_block_id, min_ts, self_arc.clone(), rt)
                }
                else {
                    log::trace!(target: "validator", "Session deleted before countdown: {}", self_arc.info().await);
                    Ok(())
                }
            }
        );

        Ok(())
    }

    pub async fn stop(self: Arc<ValidatorGroup>, rt: Arc<Runtime>) -> Result<()> {
        self.set_status(ValidatorGroupStatus::Stopping).await?;
        let group_impl = self.group_impl.clone();
        rt.spawn({
            async move {
                log::trace!(target: "validator", "Stopping group: {}", self.info().await);
                let session_ptr = {
                    let gi = group_impl.clone().lock_owned().await;
                    gi.session_ptr.clone()
                };
                if let Some(s_ptr) = session_ptr {
                    s_ptr.stop();
                }
                log::info!(target: "validator", "Group stopped: {}", self.info().await);
                let _ = self.set_status(ValidatorGroupStatus::Stopped).await;
                let _ = self.destroy_db().await;
            }
        });
        Ok(())
    }

    pub async fn destroy_db(&self) -> Result<()> {
        let mut group_impl = self.group_impl.clone().lock_owned().await;
        group_impl.destroy_db()
    }

    pub async fn get_status(&self) -> ValidatorGroupStatus {
        let group_impl = self.group_impl.lock().await;
        return group_impl.status.clone();
    }

    pub async fn set_status(&self, status: ValidatorGroupStatus) -> Result<()> {
        let mut group_impl = self.group_impl.clone().lock_owned().await;
        if group_impl.status.before(&status) {
            group_impl.status = status;
            Ok(())
        }
        else {
            Err(failure::err_msg(format!("Status cannot retreat, from {} to {}",
                                         group_impl.status, status)))
        }
    }

    pub fn get_validator_list_id(&self) -> ValidatorListHash {
        return self.validator_list_id.clone();
    }


    pub async fn info_round(&self, round: u32) -> String {
        let group_impl = self.group_impl.lock().await;
        group_impl.info_round(round)
    }

    pub async fn info (&self) -> String {
        let group_impl = self.group_impl.lock().await;
        group_impl.info()
    }

    pub async fn on_generate_slot(&self, round: u32, callback: ValidatorBlockCandidateCallback) {
        log::info!(
            target: "validator", 
            "SessionListener::on_generate_slot: collator request, {}",
            self.info_round(round).await
        );

        let (_lk_round, prev_block_ids, mm_block_id, min_ts) = self.group_impl.lock().await.update_round (round);

        let result = match mm_block_id {
            Some(mc) => {
                match run_collate_query (
                    self.shard,
                    min_ts,
                    mc.clone(),
                    prev_block_ids,
                    self.local_key.clone(),
                    self.validator_set.clone(),
                    self.engine.clone(),
                    1000,
                ).await {
                    Ok(b) => Ok(Arc::new(b)),
                    Err(x) => Err(x)
                }
            }
            None => Err(failure::err_msg("Min masterchain block id missing")),
        };
        let result_message = match &result {
            Ok(x) => {
                let now = std::time::SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs();
                self.last_collation_time.fetch_max(now, Ordering::Relaxed);

                format!("Collation successful")
            },
            Err(x) => format!("Collation failed: `{}`", x),
        };
        log::info!(target: "validator", "SessionListener::on_generate_slot: {}, {}",
            self.info_round(round).await, result_message
        );
        self.group_impl.lock().await.on_generate_slot_invoked = true;

        callback(result)
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
        let candidate_id = format!("source {}, rh {}", source.id(), root_hash.to_hex_string());
        log::trace!(target: "validator", "SessionListener::on_candidate: {}, {}",
            candidate_id, self.info_round(round).await);

        let mut candidate = super::BlockCandidate {
            block_id: BlockIdExt::with_params(self.shard, 0, root_hash, get_hash(&data.data())),
            data: data.data().to_vec(),
            collated_file_hash: catchain::utils::get_hash (&collated_data.data()),
            collated_data: collated_data.data().to_vec(),
            created_by: UInt256::from(source.pub_key().unwrap()),
        };

        let result = {
            let (prev_block_ids, mm_block_id, min_ts) = {
                let mut group_impl = self.group_impl.clone().lock_owned().await;
                let (lk_round, prev_block_ids, mm_block_id, min_ts) = group_impl.update_round(round);
                if round < lk_round {
                    log::error!(target: "validator", "round {} < self.last_known_round {}", round, lk_round);
                    return;
                }
                let next_block_id = match group_impl.create_next_block_id(
                    candidate.block_id.root_hash, candidate.block_id.file_hash, self.shard
                ) {
                    Err(x) => { log::error!(target: "validator", "{}", x); return },
                    Ok(x) => x
                };
                candidate.block_id = next_block_id;
                (prev_block_ids, mm_block_id, min_ts)
            };

            match mm_block_id {
                Some(mc) => {
                    run_validate_query(
                        self.shard,
                        min_ts,
                        mc.clone(),
                        prev_block_ids,
                        candidate.clone(),
                        self.validator_set.clone(),
                        self.engine.clone(),
                        SystemTime::now() + Duration::new(10, 0),
                    ).await
                }
                None => Err(failure::err_msg("Min masterchain block id missing")),
            }
        };

        let result_message = match &result {
            Ok(x) => {
                let vb_candidate = validator_query_candidate_to_validator_block_candidate(
                    source.clone(), candidate
                );
                let group_impl = self.group_impl.clone().lock_owned().await;
                let res = group_impl.candidate_db.save(vb_candidate).await;

                match &res {
                    Ok(()) => {
                        self.last_validation_time.fetch_max(
                            x.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs(),
                            Ordering::Relaxed
                        );
                        format!("Validation successful: finished at {:?}", x)
                    },
                    Err(x) => format!("Validation successful, db error `{}`", x)
                }
            },
            Err(x) => format!("Validation failed with verdict `{}`", x),
        };
        self.group_impl.lock().await.on_candidate_invoked = true;

        log::info!(target: "validator", "SessionListener::on_candidate: {}, {}, {}",
            candidate_id, self.info_round(round).await, result_message
        );
        callback(result);
        log::trace!(target: "validator", "SessionListener::on_candidate: {}, {}, {}, callback called",
            candidate_id, self.info_round(round).await, result_message
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
        let data_vec = data.data().to_vec();
        let we_generated = source.id() == self.local_key.id();

        log::info!(target: "validator", 
            "SessionListener::on_block_committed: source {}, data size = {}, {}" ,
            source.id(), data_vec.len(), self.info_round(round).await
        );

        let (next_block_id, prev_block_ids) = {
            let mut group_impl = self.group_impl.clone().lock_owned().await;
            if round >= group_impl.last_known_round {
                group_impl.last_known_round = round + 1;
            };

            (
                match group_impl.create_next_block_id(root_hash, file_hash, self.shard) {
                    Ok(x) => x,
                    Err(x) => { log::error!(target: "validator", "{}",x); return }
                },
                group_impl.prev_block_ids.clone()
            )
        };

        log::info!(target: "validator", 
            "SessionListener::on_block_committed: source {}, id {}, data size = {}, {}" ,
            source.id(), next_block_id, data_vec.len(), self.info_round(round).await
        );

        let result = run_accept_block_query(
            next_block_id.clone(),
            if data_vec.len() > 0 {
                Some(data_vec)
            } else {
                None
            },
            prev_block_ids.clone(),
            self.validator_set.clone(),
            sig_set,
            approve_sig_set,
            we_generated,
            self.engine.clone(),
        ).await;

        let (result_txt, new_prevs) = {
            let mut group_impl = self.group_impl.clone().lock_owned().await;

            let result_txt = match result {
                Ok(()) =>
                    if group_impl.prev_block_ids != prev_block_ids {
                        format!("Sync error: two requests at a time, prevs have changed!!!")
                    } else {
                        "Ok".to_string()
                    },
                Err(x) => format!("Error: {}", x)
                // TODO: retry block commit
            };

            group_impl.prev_block_ids = vec![next_block_id];
            (result_txt, prevs_to_string(&group_impl.prev_block_ids))
        };

        log::info!(
            target: "validator", 
            "SessionListener::on_block_committed: source {}, commit result `{}`, {}, new prevs {}",
            source.id(),
            result_txt,
            self.info_round(round).await,
            new_prevs
        );
    }

    pub async fn on_block_skipped(&self, round: u32) {
        log::info!(target: "validator", "SessionListener::on_block_skipped, {}", self.info_round(round).await);
        let mut group_impl = self.group_impl.clone().lock_owned().await;
        if round > group_impl.last_known_round {
            group_impl.last_known_round = round + 1;
        }
    }

    pub async fn on_get_approved_candidate(
        &self,
        source: PublicKey,
        root_hash: BlockHash,
        file_hash: BlockHash,
        collated_data_hash: BlockHash,
        callback: ValidatorBlockCandidateCallback)
    {
        log::info!(target: "validator", "SessionListener::on_get_approved_candidate rh {}, fh {}, {}", root_hash.to_hex_string(), file_hash.to_hex_string(), self.info().await);
        let result = {
            let group_impl = self.group_impl.clone().lock_owned().await;
            group_impl.candidate_db.load(source, root_hash, file_hash, collated_data_hash).await
        };
        let result_txt = match &result {
            Ok(res) => format!("Ok"),
            Err(err) => format!("Candidate not found: {}", err)
        };
        log::info!(target: "validator", "SessionListener::on_get_approved_candidate {}, {}", result_txt, self.info().await);
        callback(result);
    }

    pub fn on_slashing_statistics(&self, round: u32, stat: SlashingValidatorStat) {
        log::debug!(target: "validator", "SessionListener::on_slashing_statistics round {}, stat {:?}", round, stat);

        self.slashing_manager.update_statistics(&stat);
    }
}

impl Drop for ValidatorGroup {
    fn drop (&mut self) {
        // Important: does not stop the session -- to avoid database deletion,
        // which otherwise would happen each time the validator-manager crashes.
        log::info!(target: "validator", "ValidatorGroup: dropping session {}",
            self.session_id.to_hex_string()
        );
    }
}

