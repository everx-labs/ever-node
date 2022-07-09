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

pub use super::*;

use super::session_description::SessionDescriptionImpl;
use crate::{task_queue::*, ton_api::IntoBoxed};
use backtrace::Backtrace;
use cache::FifoCache;
use catchain::{
    profiling::{check_execution_time, instrument, ResultStatusCounter},
    BlockPtr, ExternalQueryResponseCallback,
};
use ever_crypto::KeyId;
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, SystemTime},
};
use ton_types::UInt256;

/*
    Constants
*/

const DEBUG_IGNORE_PROPOSALS_PRIORITY: bool = false; //ignore proposals priority and generate block each round
const DEBUG_DUMP_BLOCKS: bool = false; //dump blocks with dependencies and actions for debugging
const DEBUG_DUMP_ON_NEW_ROUND: bool = false; //debug dump for each new round
const DEBUG_DUMP_AFTER_BLOCK_APPLYING: bool = false; //debug dump after each block applying
const DEBUG_REQUEST_NEW_BLOCKS_IMMEDIATELY: bool = false; //request new blocks immediately without waiting
const DEBUG_CHECK_ALL_BEFORE_ROUND_SWITCH: bool = false; //check updates before round switching
                                                         //TODO: remove this debug option after performance tuning
const DEBUG_DUMP_BACKTRACE_FOR_LATE_VALIDATIONS: bool = true; //dump all late validations backtrace
const DEBUG_EVENTS_LOG: bool = true; //dump consensus events
const COMPLETION_HANDLERS_MAX_WAIT_PERIOD: Duration = Duration::from_millis(60000); //max wait time for completion handlers
const COMPLETION_HANDLERS_CHECK_PERIOD: Duration = Duration::from_millis(5000); //period of completion handlers checking
const BLOCK_PREPROCESSING_WARN_LATENCY: Duration = Duration::from_millis(200); //max block processing latency
const BLOCK_PROCESSING_WARN_LATENCY: Duration = Duration::from_millis(600); //max block processing latency; expect to have up to 0.5s of natural algorithm latency between process_blocks (see request_new_block for details)
const MAX_NEXT_BLOCK_WAIT_DELAY: Duration = Duration::from_millis(500); //max next block wait delay
const WARN_DUMP_PERIOD: Duration = Duration::from_millis(2000); //warning dump period
const HANGED_CONSENSUS_UPDATE_TIME: Duration = MAX_NEXT_BLOCK_WAIT_DELAY; //update interval for hanged consensus

const STATES_RESERVED_COUNT: usize = 100000; //reserved states count for blocks
const ROUND_DEBUG_PERIOD: std::time::Duration = Duration::from_secs(15); //round debug time
const LONG_ROUND_PERIOD: std::time::Duration = Duration::from_secs(10); //catchain batching mode is forced enabled for long rounds
const VALIDATOR_IDLE_TIMEOUT: std::time::Duration = Duration::from_secs(10); //allowed inactivity time for validator

/*
    Implementation details for SessionProcessor
*/

type BlockCandidateTlPtr = Rc<::ton_api::ton::validator_session::Candidate>;
type BlockCandidateMap =
    Rc<RefCell<HashMap<BlockId, (BlockCandidateTlPtr, std::time::SystemTime)>>>;
type RoundBlockMap = HashMap<u32, BlockCandidateMap>;
type BlockApproveMap = HashMap<BlockId, (SystemTime, BlockPayloadPtr)>;
type BlockMap = HashMap<BlockId, BlockPayloadPtr>;
type BlockSet = HashSet<BlockId>;
type StateMergeCache = FifoCache<(HashType, HashType), SessionStatePtr>;
type BlockUpdateCache = FifoCache<u32, SessionStatePtr>;

pub(crate) struct SessionProcessorImpl {
    completion_task_queue: TaskQueuePtr, //task queue for session callbacks
    callbacks_task_queue: CallbackTaskQueuePtr, //task queue for session callbacks
    session_id: SessionId,               //catchain session ID (incarnation)
    session_listener: SessionListenerPtr, //session listener
    catchain: CatchainPtr,               //catchain session
    next_completion_handler_available_index: CompletionHandlerId, //index of next available complete handler
    completion_handlers: HashMap<CompletionHandlerId, Box<dyn CompletionHandler>>, //complete handlers
    completion_handlers_check_last_time: SystemTime, //time of last completion handlers check
    catchain_started: bool,                          //flag indicates that catchain has been started
    local_key: PrivateKey,                           //private key for signing
    description: SessionDescriptionImpl,             //session description
    block_to_state_map: Vec<Option<SessionStatePtr>>, //session states
    state_merge_cache: StateMergeCache,              //cache of merged states
    block_update_cache: BlockUpdateCache,            //cache of states after block updates
    real_state: SessionStatePtr,                     //real state
    virtual_state: SessionStatePtr,                  //virtual state
    current_round: u32,                              //current round sequence number
    requested_new_block: bool,                       //new block has been requested in catchain
    requested_new_block_now: bool, //new block has been requested in catchain to be generated immediately
    session_creation_time: SystemTime, //session creation time
    session_processor_creation_time: SystemTime, //session processor creation time
    next_awake_time: SystemTime,   //next awake timestamp
    round_started_at: SystemTime,  //round start time
    round_debug_at: SystemTime,    //round debug checkpoint time
    pending_generate: bool,        //block generation request has been sent to collator
    generated: bool,               //block has been generated
    sent_generated: bool,          //generated block has been sent to a catchain
    generated_block: BlockId,      //generated block ID
    blocks: RoundBlockMap,         //map of blocks for rounds
    pending_approve: BlockSet,     //set of blocks which are pending for approval
    pending_reject: BlockMap,      //map of blocks to be rejected
    rejected: BlockSet,            //set of blocks which has been rejected
    approved: BlockApproveMap,     //map of approved blocks
    active_requests: BlockSet,     //set of requested block candidates
    pending_sign: bool,            //block candidate is pending for signature
    signed: bool,                  //block candidate has been signed
    signed_block: BlockId,         //signated block ID
    signature: BlockSignature,     //block candidate signature
    log_replay_report_current_time: SystemTime, //log replay current time (for reporting)
    validates_counter: ResultStatusCounter, //result status counter for approval requests
    collates_counter: ResultStatusCounter, //result status counter for collation requests
    collates_expire_counter: ResultStatusCounter, //result status counter for expired collation requests
    commits_counter: ResultStatusCounter,         //result status counter for commits requests
    rldp_queries_counter: ResultStatusCounter,    //result status counter for RLDP queries
    preprocess_block_counter: metrics_runtime::data::Counter, //counter for preprocess calls
    preprocess_block_latency_histogram: metrics_runtime::data::Histogram, //histogram for preprocess block latency
    process_blocks_counter: metrics_runtime::data::Counter, //counter for process calls
    process_blocks_latency_histogram: metrics_runtime::data::Histogram, //histogram for process blocks latency
    request_new_block_counter: metrics_runtime::data::Counter, //counter for new blocks requesting
    check_all_counter: metrics_runtime::data::Counter,         //counter for check_all calls
    last_preprocess_block_warn_dump_time: SystemTime, //last time preprocess block latency warning has been printed
    last_process_blocks_warn_dump_time: SystemTime, //last time process blocks latency warning has been printed
    slashing_stat: SlashingValidatorStat,           //slashing validator statistics
    last_preprocess_block_time: Vec<SystemTime>, //time of last preprocess block request from a partial validator
    round_duration_histogram: metrics_runtime::data::Histogram, //histogram for round duration
    first_candidate_received: bool, //first candidate for validation has been received in current round
    first_candidate_received_latency_histogram: metrics_runtime::data::Histogram, //histogram for first candidate approved in a round
    first_candidate_approved: bool, //first candidate for validation has been received in current round
    first_candidate_approved_latency_histogram: metrics_runtime::data::Histogram, //histogram for first candidate approved in a round
    first_candidate_voted: bool, //first candidate for validation has been voted in current round
    first_candidate_voted_latency_histogram: metrics_runtime::data::Histogram, //histogram for first candidate voted in a round
    first_candidate_precommitted: bool, //first candidate for validation has been precommitted in current round
    first_candidate_precommitted_latency_histogram: metrics_runtime::data::Histogram, //histogram for first candidate precommitted in a round
    block_candidate_broadcast_validation_latency_histogram: metrics_runtime::data::Histogram, //histogram for block candidate broadcast processing during validation
    validation_latency_histogram: metrics_runtime::data::Histogram, //histogram for block candidate validation
    collation_latency_histogram: metrics_runtime::data::Histogram, //histogram for block candidate collation
    active_weight_gauge: metrics_runtime::data::Gauge,             //gauge for active weight
}

/*
    Implementation for public SessionProcessor trait
*/

impl SessionProcessor for SessionProcessorImpl {
    /*
        Accessors
    */

    fn get_description(&self) -> &dyn SessionDescription {
        &self.description
    }

    fn get_impl(&self) -> &dyn Any {
        self
    }

    fn get_mut_impl(&mut self) -> &mut dyn Any {
        self
    }

    /*
        Stop processing
    */

    fn stop(&mut self) {
        debug!("Stopping ValidatorSession processor...");

        self.catchain.stop(false);

        debug!("ValidatorSession processor has been stopped");
    }

    /*
        Awake time management
    */

    fn set_next_awake_time(&mut self, mut timestamp: std::time::SystemTime) {
        let now = std::time::SystemTime::now();

        //ignore set new awake point if it is in the past

        if timestamp < now {
            timestamp = now;
        }

        //do not set next awake point if we will awake earlier in the future

        if self.next_awake_time <= timestamp {
            return;
        }

        self.next_awake_time = timestamp;
    }

    fn reset_next_awake_time(&mut self) {
        self.next_awake_time =
            std::time::SystemTime::now() + self.description.opts().catchain_idle_timeout;
    }

    fn get_next_awake_time(&self) -> std::time::SystemTime {
        self.next_awake_time.clone()
    }

    /*
        Consensus iteration checkers
    */

    fn check_all(&mut self) {
        instrument!();

        self.check_all_counter.increment();

        //flush caches

        self.state_merge_cache.flush();
        self.block_update_cache.flush();

        //check completion handlers

        if let Ok(completion_handlers_check_elapsed) =
            self.completion_handlers_check_last_time.elapsed()
        {
            if completion_handlers_check_elapsed > COMPLETION_HANDLERS_CHECK_PERIOD {
                instrument!();
                check_execution_time!(20_000);

                self.check_completion_handlers();
                self.completion_handlers_check_last_time = std::time::SystemTime::now();
            }
        }

        //no actions are needed before start of Catchain

        if !self.catchain_started {
            return;
        }

        //don't check anything if received consensus block is not the same as current round

        if self.virtual_state.get_current_round_sequence_number() != self.current_round {
            self.request_new_block(false);
            return;
        }

        //round debug dump

        if self.description.is_in_past(self.round_debug_at) {
            self.debug_dump();

            self.round_debug_at = self.description.get_time() + ROUND_DEBUG_PERIOD;
        }

        //check session state

        let attempt_seqno = self
            .description
            .get_attempt_sequence_number(self.description.get_ts());

        self.check_sign_slot();
        self.check_approve();
        self.check_generate_slot();
        self.check_action(attempt_seqno);
        self.check_vote_for_slot(attempt_seqno);

        //update metrics

        let total_active_weight = self.get_total_active_weight();

        self.active_weight_gauge.record(total_active_weight as i64);

        if self.virtual_state.has_approved_block(&self.description)
            && !self.first_candidate_approved
        {
            trace!(
                "first block candidate has been approved in round #{}",
                self.current_round
            );

            self.first_candidate_approved = true;

            if let Ok(latency) = self.get_latency_from_round_start() {
                self.first_candidate_approved_latency_histogram
                    .record_value(latency.as_millis() as u64);
            }
        }

        if self.virtual_state.has_voted_block(&self.description) && !self.first_candidate_voted {
            trace!(
                "first block candidate has been voted in round #{}",
                self.current_round
            );

            self.first_candidate_voted = true;

            if let Ok(latency) = self.get_latency_from_round_start() {
                self.first_candidate_voted_latency_histogram
                    .record_value(latency.as_millis() as u64);
            }
        }

        if self.virtual_state.has_precommitted_block() && !self.first_candidate_precommitted {
            trace!(
                "first block candidate has been precommitted in round #{}",
                self.current_round
            );

            self.first_candidate_precommitted = true;

            if let Ok(latency) = self.get_latency_from_round_start() {
                self.first_candidate_precommitted_latency_histogram
                    .record_value(latency.as_millis() as u64);
            }
        }

        //update next check_all() call timestamp

        self.set_next_awake_time(self.round_debug_at);
        self.set_next_awake_time(self.description.get_attempt_start_at(attempt_seqno + 1));
    }

    /*
        Catchain blocks processing management
    */

    fn preprocess_block(&mut self, block: BlockPtr) {
        check_execution_time!(100_000);
        instrument!();

        let start_time = SystemTime::now();
        let source_id = block.get_source_id();

        trace!("Preprocessing block {}", block);

        self.preprocess_block_counter.increment();

        self.last_preprocess_block_time[source_id as usize] = start_time;

        let block_payload_processing_latency =
            block.get_payload().get_creation_time().elapsed().unwrap();

        self.preprocess_block_latency_histogram
            .record_value(block_payload_processing_latency.as_millis() as u64);

        if block_payload_processing_latency > BLOCK_PREPROCESSING_WARN_LATENCY {
            let block_processing_latency = block.get_creation_time().elapsed().unwrap();
            let delivery_issue = block_processing_latency < BLOCK_PREPROCESSING_WARN_LATENCY;

            if let Ok(warn_elapsed) = self.last_preprocess_block_warn_dump_time.elapsed() {
                if warn_elapsed > WARN_DUMP_PERIOD {
                    let source_public_key_hash =
                        self.description.get_source_public_key_hash(source_id);

                    warn!("{}: ValidatorSession block payload latency is {:.3}s, block latency is {:.3}s (expected_latency={:.3}s, source=v{:03} ({})): {}",
                        if delivery_issue { "Delivery time issue" } else { "Preprocessing time issue" }, block_payload_processing_latency.as_secs_f64(), block_processing_latency.as_secs_f64(), BLOCK_PREPROCESSING_WARN_LATENCY.as_secs_f64(),
                        source_id, source_public_key_hash, &block);
                    self.last_preprocess_block_warn_dump_time = SystemTime::now();
                }
            }
        }

        let payload_len = block.get_payload().data().len();
        let deps = block.get_deps();
        let deps_len = deps.len();

        trace!(
            "...received block with payload: {} bytes, and {} deps",
            payload_len,
            deps_len
        );

        //parse payload

        let (block_update, need_actualize_state) =
            if block.get_payload().data().len() != 0 || deps.len() != 0 {
                instrument!();

                trace!("...parsing incoming block update");

                //try to parse block update

                let block_update: Result<ton::BlockUpdate> =
                    catchain::utils::deserialize_tl_boxed_object(&block.get_payload().data());

                match block_update.as_ref() {
                    Ok(block_update) => {
                        let block_update = block_update.clone().only();
                        (Some(block_update), true)
                    }
                    Err(err) => {
                        let node_public_key_hash = self
                            .description
                            .get_source_public_key_hash(block.get_source_id() as u32)
                            .clone();

                        warn!(
                            "Node {} sent a block {:?} which can't be parsed: {:?}",
                            node_public_key_hash,
                            block.get_hash(),
                            err
                        );

                        (None, true)
                    }
                }
            } else {
                (None, false)
            };

        //search for state in block update cache

        let state = if let Some(state) = self.get_state_for_block_update(&block_update) {
            state
        } else {
            //merge state

            trace!("...prev block is {:?}", block.get_prev());

            let mut state = if let Some(prev) = block.get_prev() {
                self.get_state(&prev).clone()
            } else {
                trace!("...create initial state");

                SessionFactory::create_state(&mut self.description)
            };

            trace!("...merge state {:08x?} with dependencies", state.get_hash());

            for dep_block in deps {
                let dep_state = self.get_state(dep_block).clone();
                let state_hash = state.get_hash();

                state = self.merge_states(&state, &dep_state, false);

                trace!(
                    "...state merged: ({:08x?}, {:08x?}) -> {:08x?}",
                    state_hash,
                    dep_state.get_hash(),
                    state.get_hash()
                );
            }

            trace!("...merged virtual state is: {:?}", state);

            //dump block before actions applying (for debugging only)

            if DEBUG_DUMP_BLOCKS {
                trace!(
                    "...dump block before actions applying: {:?}",
                    block.get_hash()
                );

                self.dump_block(&block);
            }

            //apply actions from incoming block & check payload

            if need_actualize_state {
                instrument!();

                let node_public_key_hash = self
                    .description
                    .get_source_public_key_hash(block.get_source_id() as u32)
                    .clone();
                let node_source_id = block.get_source_id() as u32;

                if let Some(block_update) = block_update {
                    trace!("...BlockUpdate has been received: {:?}", block_update);

                    //apply actions to state

                    let attempt_id = self
                        .description
                        .get_attempt_sequence_number(block_update.ts as u64);

                    trace!("...attempt ID is {}", attempt_id);
                    trace!("...applying actions");

                    for msg in block_update.actions.iter() {
                        trace!(
                            "Node {} applying action on block {:?}: {:?}",
                            node_public_key_hash,
                            block.get_hash(),
                            msg
                        );

                        state = state.apply_action(
                            &mut self.description,
                            node_source_id,
                            attempt_id,
                            msg,
                            block.get_creation_time(),
                            block.get_payload().get_creation_time(),
                        );
                    }

                    //actualize state

                    state = state.make_all(&mut self.description, node_source_id, attempt_id);

                    //check hashes

                    trace!("...check hashes");

                    if state.get_hash() != block_update.state as u32 {
                        warn!("Node {} sent a block {:?} with hash mismatch: computed={:08x?}, received={:08x?}",
              node_public_key_hash, block.get_hash(), state.get_hash(), block_update.state as u32);

                        for msg in block_update.actions.iter() {
                            warn!("Node {} sent a block {:?} with hash mismatch: applied action: {:?}", node_public_key_hash, block.get_hash(), msg);
                        }
                    }
                } else {
                    warn!(
                        "Node {} sent a block {:?} which can't be parsed: actualize the state",
                        node_public_key_hash,
                        block.get_hash(),
                    );

                    state = state.make_all(
                        &mut self.description,
                        node_source_id,
                        state.get_ts(node_source_id),
                    );
                }
            }

            //update session states

            trace!(
                "...move state {:08x?} to persistent memory",
                state.get_hash()
            );

            state = state.move_to_persistent(&mut self.description);

            if !TELEGRAM_NODE_COMPATIBILITY_HASHES_BUG {
                //update cache

                self.block_update_cache
                    .insert(state.get_hash(), state.clone());
            }

            state
        };

        //set state

        self.set_state(&block, state.clone());

        //dump block before actions applying (for debugging only)

        if DEBUG_DUMP_BLOCKS {
            trace!(
                "...dump block after actions applying: {:?}",
                block.get_hash()
            );

            self.dump_block(&block);
        }

        //update real state for self updated block

        if (block.get_source_id() as u32) == self.get_local_idx() && !self.catchain_started {
            trace!(
                "...use preprocessed block state {:08x?} as a real state",
                self.real_state.get_hash()
            );
            self.real_state = state.clone();
        }

        let virtual_state_hash = self.virtual_state.get_hash();

        self.virtual_state = self.merge_states(&self.virtual_state.clone(), &state.clone(), true);

        trace!(
            "...state merged to virtual state: ({:08x?},{:08x?}) -> {:08x?}",
            virtual_state_hash,
            state.get_hash(),
            self.virtual_state.get_hash()
        );

        trace!("...new virtual_state: {:?}", &self.virtual_state);

        //clear temp memory after moving states to persistent memory

        trace!("...clear temporary memory after merging");

        self.description.get_cache().clear_temp_memory();

        //debug dump

        if DEBUG_DUMP_AFTER_BLOCK_APPLYING {
            self.debug_dump();
        }

        trace!("...do consensus iteration (after preprocess block)");

        //notify about starting of a new round if state is changed after merging

        let state_round = self.real_state.get_current_round_sequence_number();

        if state_round != self.current_round {
            self.new_round(state_round);
        }

        //check state in current round

        self.check_all();

        //debug output

        let processing_delay = match start_time.elapsed() {
            Ok(elapsed) => elapsed,
            Err(_err) => Duration::default(),
        };

        trace!(
            "...finish preprocessing block {} in {}ms; state={}",
            block,
            processing_delay.as_millis(),
            state.get_hash()
        );
    }

    fn process_blocks(&mut self, blocks: Vec<BlockPtr>) {
        check_execution_time!(100_000);
        instrument!();

        let start_time = SystemTime::now();

        trace!("Processing blocks {:?}", blocks);

        self.process_blocks_counter.increment();

        //reset flags

        self.requested_new_block = false;
        self.requested_new_block_now = false;

        let mut force_batching_mode = false;
        let prev_real_state_hash = self.real_state.get_hash();

        //merge real state

        trace!(
            "...merge block states to real state with hash {:08x?}",
            self.real_state.get_hash()
        );

        for block in &blocks {
            let block_payload_processing_latency =
                block.get_payload().get_creation_time().elapsed().unwrap();

            self.process_blocks_latency_histogram
                .record_value(block_payload_processing_latency.as_millis() as u64);

            if block_payload_processing_latency > BLOCK_PROCESSING_WARN_LATENCY {
                let block_processing_latency = block.get_creation_time().elapsed().unwrap();
                let delivery_issue = block_processing_latency < BLOCK_PROCESSING_WARN_LATENCY;

                if !delivery_issue {
                    force_batching_mode = true; //ask catchain to batch blocks in case of overloaded session incoming queues
                }

                let warn_elapsed = self.last_process_blocks_warn_dump_time.elapsed().unwrap();

                if warn_elapsed > WARN_DUMP_PERIOD {
                    let source_id = block.get_source_id();
                    let source_public_key_hash =
                        self.description.get_source_public_key_hash(source_id);

                    warn!("{}: ValidatorSession block payload processing latency is {:.3}s, block processing latency is {:.3}s (expected_latency={:.3}s, source=v{:03} ({})): {}",
                        if delivery_issue { "Delivery time issue" } else { "Processing time issue" }, block_payload_processing_latency.as_secs_f64(), block_processing_latency.as_secs_f64(), BLOCK_PROCESSING_WARN_LATENCY.as_secs_f64(),
                        source_id, source_public_key_hash, &block);

                    self.last_process_blocks_warn_dump_time = SystemTime::now();
                }
            }

            let real_state_hash = self.real_state.get_hash();
            let block_state = self.get_state(&block).clone();

            self.real_state = self.merge_states(&self.real_state.clone(), &block_state, false);

            trace!(
                "...real state merged: ({:08x?}, {:08x?}) -> {:08x?}",
                real_state_hash,
                block_state.get_hash(),
                self.real_state.get_hash()
            );
        }

        //start new round if it has been changed according to delivered blocks

        trace!("...do consensus iteration (after process blocks)");

        if self.real_state.get_current_round_sequence_number() != self.current_round {
            self.new_round(self.real_state.get_current_round_sequence_number());
        }

        let local_idx = self.get_local_idx();
        let ts = self.description.get_ts();
        let attempt = self.description.get_attempt_sequence_number(ts);
        let now = std::time::SystemTime::now();

        trace!(
            "...local_idx={}, round={}, attempt={}, ts_unix_time={}",
            local_idx,
            self.current_round,
            attempt,
            self.description.get_unixtime(ts)
        );

        //store all state updates in a 'message' array which will be applied to real_state when all incremental updates will ge gathered

        let mut messages: Vec<ton::Message> = Vec::new();

        //process blocks generation flow

        if self.generated && !self.sent_generated {
            //generate SubmittedBlock message to notify other validators about block candidate from this validator

            let (block_candidate, _candidate_creation_time) = self
                .get_signed_block_for_round(self.current_round, &self.generated_block)
                .unwrap();
            let file_hash = catchain::utils::get_hash(&block_candidate.data());
            let collated_data_file_hash =
                catchain::utils::get_hash(&block_candidate.collated_data());
            let message = ton::message::SubmittedBlock {
                round: self.current_round as i32,
                root_hash: block_candidate.root_hash().clone(),
                file_hash: file_hash.into(),
                collated_data_file_hash: collated_data_file_hash.into(),
            };

            trace!("...generated SubmittedBlock: {:?}", message);

            messages.push(message.into_boxed());

            self.sent_generated = true;
        }

        //process blocks to approve

        trace!("...check approvals");

        let to_approve = self
            .real_state
            .choose_blocks_to_approve(&self.description, local_idx);

        for block in to_approve {
            let block_id = block.get_id();

            if let Some(block_pair) = self.approved.get(block_id) {
                if block_pair.0 <= self.description.get_time() {
                    //if block has been approved, add corresponding ApprovedBlock message to incremental updates

                    let message = ton::message::ApprovedBlock {
                        round: self.current_round as i32,
                        candidate: block_id.clone().into(),
                        signature: block_pair.1.data().clone(),
                    };

                    trace!("...generated ApprovedBlock: {:?}", message);

                    messages.push(message.into_boxed());
                }
            }
        }

        //process blocks to reject

        for (block_id, rejection_reason) in self.pending_reject.iter() {
            let message = ton::message::RejectedBlock {
                round: self.current_round as i32,
                candidate: block_id.clone().into(),
                reason: rejection_reason.data().clone(),
            };

            trace!("...generated RejectedBlock: {:?}", message);

            messages.push(message.into_boxed());
        }

        self.pending_reject.clear();

        //process commit

        if self.signed {
            trace!("...check commit");

            if let Some(block) = self
                .real_state
                .choose_block_to_sign(&self.description, local_idx)
            {
                assert!(*block.get_id() == self.signed_block);

                let message = ton::message::Commit {
                    round: self.current_round as i32,
                    candidate: self.signed_block.clone().into(),
                    signature: self.signature.clone().into(),
                };

                trace!("...generated Commit: {:?}", message);

                messages.push(message.into_boxed());
            }
        }

        //apply incremental updates to a state

        trace!("...incremental updates applying");

        for msg in &messages {
            trace!(
                "...applying action for node #{} and attempt {}: {:?}",
                local_idx,
                attempt,
                msg
            );

            self.real_state = self.real_state.apply_action(
                &mut self.description,
                local_idx,
                attempt,
                msg,
                now.clone(),
                now.clone(),
            );
        }

        //votes processing

        trace!("...check voting");

        if self
            .real_state
            .check_need_generate_vote_for(&self.description, local_idx, attempt)
        {
            trace!("...generating VOTEFOR");

            let msg = self
                .real_state
                .generate_vote_for(&mut self.description, local_idx, attempt);

            trace!(
                "...applying VOTEFOR action for node #{} and attempt {}: {:?}",
                local_idx,
                attempt,
                msg
            );

            self.real_state = self.real_state.apply_action(
                &mut self.description,
                local_idx,
                attempt,
                &msg,
                now.clone(),
                now.clone(),
            );

            messages.push(msg);
        }

        //generating incremental updates according to a new state

        trace!("...generate incremental updates and apply them to a real state");

        loop {
            let msg = self
                .real_state
                .create_action(&self.description, local_idx, attempt);
            let stop = msg.is_none()
                || match &msg.as_ref().unwrap() {
                    ton::Message::ValidatorSession_Message_Empty(_) => true,
                    _ => false,
                };

            trace!("...generated action: {:?}", msg.as_ref().unwrap());

            self.real_state = self.real_state.apply_action(
                &mut self.description,
                local_idx,
                attempt,
                &msg.as_ref().unwrap(),
                now.clone(),
                now.clone(),
            );

            messages.push(msg.unwrap());

            const MESSAGES_COUNT_WARN: usize = 100;

            if messages.len() > MESSAGES_COUNT_WARN && messages.len() % MESSAGES_COUNT_WARN == 0 {
                warn!(
                    "Too many messages {} during processing blocks for session {}",
                    messages.len(),
                    self.session_id.to_hex_string()
                );
            }

            if stop {
                break;
            }
        }

        //move real state to persistent memory

        trace!(
            "...move real state {:08x?} to persistent memory",
            self.real_state.get_hash()
        );

        self.real_state = self.real_state.move_to_persistent(&mut self.description);

        trace!("...new real_state: {:?}", &self.real_state);

        //prepare new block to be sent to catchain

        let real_state_hash = self.real_state.get_hash();

        trace!("...created block with root_hash={:08x?}", real_state_hash);

        let payload = ton::blockupdate::BlockUpdate {
            ts: ts as i64,
            actions: messages.into(),
            state: real_state_hash as i32,
        }
        .into_boxed();
        let serialized_payload = catchain::utils::serialize_tl_boxed_object!(&payload);

        //send new block back to a catchain

        let block_may_be_skipped = prev_real_state_hash == real_state_hash;

        trace!(
            "...notify catchain about new block {}{:?}",
            if block_may_be_skipped {
                "which may be skipped "
            } else {
                ""
            },
            serialized_payload
        );

        let round = self.real_state.get_current_round_sequence_number();
        let long_round_started_at = self.round_started_at + LONG_ROUND_PERIOD;

        if round == self.current_round && self.description.is_in_past(long_round_started_at) {
            //force enable batching mode for long rounds

            force_batching_mode = true;
        }

        self.catchain.processed_block(
            catchain::CatchainFactory::create_block_payload(serialized_payload),
            block_may_be_skipped,
            force_batching_mode,
        );

        //check if new round is appeared

        trace!(
            "...round after changes applying is {} (current is {})",
            round,
            self.current_round
        );

        if round > self.current_round {
            self.new_round(round);
        }

        //merge changes from a real state to a virtual state (so they should be equal after such merging)

        let virtual_state_hash = self.virtual_state.get_hash();

        trace!(
            "...merge changes from a real state {:08x?} to a virtual state {:08x?}",
            self.real_state.get_hash(),
            virtual_state_hash
        );

        self.virtual_state =
            self.merge_states(&self.virtual_state.clone(), &self.real_state.clone(), true);

        trace!("...new virtual_state: {:?}", &self.virtual_state);

        if virtual_state_hash != self.virtual_state.get_hash() && block_may_be_skipped {
            //this is assert-like warning without halting the processing thread; only for debugging

            warn!("Block processing was skipped due to absence of real state changes but virtual state was updated");
        }

        //clear temporary memory after merging

        trace!("...clear temporary memory");

        self.description.get_cache().clear_temp_memory();

        //debug output

        let processing_delay = match start_time.elapsed() {
            Ok(elapsed) => elapsed,
            Err(_err) => Duration::default(),
        };

        trace!(
            "...finish processing blocks in {}ms; real_state={:08x?}, virtual_state={:08x?}",
            processing_delay.as_millis(),
            self.real_state.get_hash(),
            self.virtual_state.get_hash()
        );
    }

    fn finished_catchain_processing(&mut self) {
        check_execution_time!(100_000);
        instrument!();

        trace!("Finished catchain blocks processing");

        let virtual_state_hash = &self.virtual_state.get_hash();
        let real_state_hash = &self.real_state.get_hash();

        if virtual_state_hash != real_state_hash {
            warn!("SessionProcessor: virtual state and real state hashes mismatch; virtual_state={:08x?} real_state={:08x?}",
        virtual_state_hash, real_state_hash);
        }

        self.virtual_state = self.real_state.clone();

        self.check_all();
    }

    fn catchain_started(&mut self) {
        instrument!();

        info!("Catchain startup notification has been received");

        self.catchain_started = true;

        let (self_approved_blocks, round) = {
            let self_approved_blocks = self
                .virtual_state
                .get_blocks_approved_by(&self.description, self.get_local_idx());
            let round = self.virtual_state.get_current_round_sequence_number();

            (self_approved_blocks, round)
        };

        for block in self_approved_blocks {
            if block.is_none() {
                continue;
            }

            let block = block.unwrap();
            let block_source_public_key = self
                .description
                .get_source_public_key(block.get_source_index())
                .clone();
            let block_source_id = self
                .description
                .get_source_public_key_hash(block.get_source_index())
                .clone();
            let block_root_hash = block.get_root_hash().clone();
            let completion_task_queue = self.completion_task_queue.clone();

            self.notify_get_approved_candidate(
        &block_source_public_key,
        block.get_root_hash(),
        block.get_file_hash(),
        block.get_collated_data_file_hash(),
        Box::new(move |candidate : Result<ValidatorBlockCandidatePtr>|
      {
        match candidate
        {
          Err(err) => error!("SessionProcessor::started: failed to get candidate from a validator: {:?}", err),
          Ok(candidate) => {
            use ::ton_api::ton::validator_session::*;

            let broadcast = candidate::Candidate {
              src : UInt256::with_array(*block_source_id.clone().data()),
              round : round as i32,
              root_hash : block_root_hash.clone().into(),
              data : candidate.data.data().0.clone().into(),
              collated_data : candidate.collated_data.data().0.clone().into(),
            }.into_boxed();
            let data = catchain::utils::serialize_tl_boxed_object!(&broadcast);
            let data = catchain::CatchainFactory::create_block_payload(data);

            post_closure(&completion_task_queue, move |processor : &mut dyn SessionProcessor|
            {
              processor.process_broadcast(block_source_id, data);
            });
          },
        }
      }));
        }

        self.check_all();
    }

    /*
        Time synchronization for Catchain log replay
    */

    fn set_time(&mut self, time: std::time::SystemTime) {
        if log_enabled!(log::Level::Trace) {
            if let Ok(duration) = time.duration_since(self.log_replay_report_current_time) {
                const REPORT_TIMEOUT: Duration = Duration::from_millis(1000);

                if duration > REPORT_TIMEOUT {
                    trace!(
                        "Set log replay time {}",
                        catchain::utils::time_to_string(&time)
                    );
                    self.log_replay_report_current_time = time;
                }
            }
        }

        self.description.set_time(time);
    }

    /*
        Network messages processing
    */

    fn process_broadcast(&mut self, source_id: PublicKeyHash, data: BlockPayloadPtr) {
        instrument!();

        let src_idx = self.description.get_source_index(&source_id);
        let candidate =
            catchain::utils::deserialize_tl_boxed_object::<ton::Candidate>(&data.data());
        let data_hash = catchain::utils::get_hash_from_block_payload(&data);

        if let Err(err) = candidate {
            warn!(
                "Can't parse broadcast {:?} from node {}: {:?}",
                data_hash, source_id, err
            );
            return;
        }

        trace!(
            "Processing broadcast {:?} from node {} (src_idx={})",
            data_hash,
            source_id,
            src_idx
        );

        let candidate_creation_time = data.get_creation_time();
        let candidate_wrapper = candidate.ok().unwrap();
        let candidate = candidate_wrapper.clone().only();

        //check if the candidate was sent from the node which generated block

        if candidate.src.as_slice() != source_id.data() {
            warn!(
                "Broadcast's {:?} source {:?} mismatches node ID {:?}",
                data_hash, candidate.src, source_id
            );
            return;
        }

        //check block size limit

        if candidate.data.len() > self.description.opts().max_block_size as usize
            || candidate.collated_data.len()
                > self.description.opts().max_collated_data_size as usize
        {
            warn!(
                "Broadcast {:?} from source {:?} has too big size={} / collated_size={}",
                data_hash,
                source_id,
                candidate.data.len(),
                candidate.collated_data.len()
            );
            return;
        }

        //extract data

        let file_hash = catchain::utils::get_hash(&candidate.data);
        let collated_data_file_hash = catchain::utils::get_hash(&candidate.collated_data);
        let block_round = candidate.round as u32;
        let block_id = self.description.candidate_id(
            src_idx,
            &candidate.root_hash.into(),
            &file_hash.into(),
            &collated_data_file_hash.into(),
        );

        //check the block

        if block_round < self.current_round
        //block_round >= self.current_round + self.blocks.len() //this check does not needed because this node implementation stores all furture rounds instead of 100 rounds in a reference implementation
        {
            trace!(
                "Broadcast {:?} from source {:?} has invalid round {} (current round is {})",
                data_hash,
                source_id,
                block_round,
                self.current_round
            );
            return;
        }

        if self
            .get_signed_block_for_round(block_round, &block_id)
            .is_some()
        {
            trace!(
                "Duplicate broadcast {:?} from source {:?}",
                data_hash,
                source_id
            );
            return;
        }

        let priority = self.description.get_node_priority(src_idx, block_round);

        if priority < 0 {
            warn!("Broadcast {:?} from source {:?} skipped: source is not allowed to generate blocks in the round {}", data_hash,
        source_id, block_round);
            return;
        }

        //register the block

        self.insert_signed_block_for_round(
            block_round,
            &block_id,
            (Rc::new(candidate_wrapper), candidate_creation_time),
        );

        trace!(
            "...broadcast received for round {}, current round is {}",
            block_round,
            self.current_round
        );

        if block_round != self.current_round {
            return;
        }

        assert!(!self.pending_approve.contains(&block_id));
        assert!(!self.approved.contains_key(&block_id));
        assert!(!self.pending_reject.contains_key(&block_id));
        assert!(!self.rejected.contains(&block_id));

        //trying to approve this block

        let blocks = self
            .virtual_state
            .choose_blocks_to_approve(&self.description, self.get_local_idx());

        for block in blocks {
            if block.get_id() != &block_id {
                continue;
            }

            self.try_approve_block(block);

            break;
        }
    }

    fn process_query(
        &mut self,
        _source_id: PublicKeyHash,
        data: BlockPayloadPtr,
        callback: ExternalQueryResponseCallback,
    ) {
        instrument!();

        //read query data

        let reader: &mut dyn std::io::Read = &mut data.data().as_ref();
        let mut deserializer = ton_api::Deserializer::new(reader);
        let message = match deserializer.read_boxed::<ton_api::ton::TLObject>() {
            Ok(message) => {
                let message =
                    message.downcast::<::ton_api::ton::rpc::validator_session::DownloadCandidate>();
                if let Err(err) = message {
                    let err = format_err!("validator session: cannot parse query: {:?}", err);
                    callback(Err(err));
                    return;
                }

                message.unwrap()
            }
            Err(err) => {
                let err = format_err!("validator session: cannot parse query: {:?}", err);
                callback(Err(err));
                return;
            }
        };

        //check correctness

        let round_id = message.round as u32;

        if round_id > self.real_state.get_current_round_sequence_number() {
            let err = format_err!("too big round id {}", round_id);
            callback(Err(err));
            return;
        }

        let id = self.description.candidate_id(
            self.description
                .get_source_index(&KeyId::from_data(*message.id.src.as_slice())),
            &message.id.root_hash,
            &message.id.file_hash,
            &message.id.collated_data_file_hash,
        );

        let block = if round_id < self.real_state.get_current_round_sequence_number() {
            let block = self
                .real_state
                .get_committed_block(&self.description, round_id);

            if block.is_none() || block.as_ref().unwrap().get_id() != &id {
                let err = format_err!("wrong block in old round {}", round_id);
                callback(Err(err));
                return;
            }

            block.unwrap().unwrap()
        } else {
            assert!(round_id == self.real_state.get_current_round_sequence_number());

            let block = self.real_state.get_block(&self.description, &id);

            if block.is_none() || block.as_ref().unwrap().is_none() {
                let err = format_err!("wrong block in current round {}", round_id);
                callback(Err(err));
                return;
            }

            if !self
                .real_state
                .check_block_is_approved_by(self.get_local_idx(), &id)
            {
                let err = format_err!("not approved in current round {}", round_id);
                callback(Err(err));
                return;
            }

            block.unwrap().unwrap()
        };

        //request approved block from validator

        let source_idx = message.id.src;
        let candidate_response = Box::new(move |candidate: Result<ValidatorBlockCandidatePtr>| {
            if candidate.is_err() {
                let err = format_err!("failed to get candidate for round {}", round_id);
                callback(Err(err));
                return;
            }

            let candidate = candidate.unwrap();
            let candidate = ton::candidate::Candidate {
                src: source_idx,
                round: round_id as ton::int,
                root_hash: candidate.id.root_hash.clone(),
                data: candidate.data.data().clone(),
                collated_data: candidate.collated_data.data().clone(),
            }
            .into_boxed();
            let serialized_candidate = catchain::utils::serialize_tl_boxed_object!(&candidate);
            let serialized_candidate =
                catchain::CatchainFactory::create_block_payload(serialized_candidate);

            callback(Ok(serialized_candidate));
        });

        let source_public_key_hash = self
            .description
            .get_source_public_key(block.get_source_index())
            .clone();
        self.notify_get_approved_candidate(
            &source_public_key_hash,
            &message.id.root_hash,
            &message.id.file_hash,
            &message.id.collated_data_file_hash,
            candidate_response,
        );
    }
}

/*
    Implementation for crate CompletionHandlerProcessor trait
*/

impl CompletionHandlerProcessor for SessionProcessorImpl {
    fn get_completion_task_queue(&self) -> &TaskQueuePtr {
        &self.completion_task_queue
    }

    fn add_completion_handler(&mut self, handler: CompletionHandlerPtr) -> CompletionHandlerId {
        let handler_index = self.next_completion_handler_available_index;

        self.next_completion_handler_available_index += 1;

        const MAX_COMPLETION_HANDLER_INDEX: CompletionHandlerId = std::u64::MAX;

        assert!(self.next_completion_handler_available_index < MAX_COMPLETION_HANDLER_INDEX);

        self.completion_handlers.insert(handler_index, handler);

        handler_index
    }

    fn remove_completion_handler(
        &mut self,
        handler_id: CompletionHandlerId,
    ) -> Option<CompletionHandlerPtr> {
        self.completion_handlers.remove(&handler_id)
    }
}

/*
    Implementation for public Display
*/

impl fmt::Display for SessionProcessorImpl {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unimplemented!();
    }
}

/*
    Implementation internals of SessionProcessorImpl
*/

#[allow(dead_code)]
fn get_impl(value: &dyn SessionProcessor) -> &SessionProcessorImpl {
    value
        .get_impl()
        .downcast_ref::<SessionProcessorImpl>()
        .unwrap()
}

fn get_mut_impl(value: &mut dyn SessionProcessor) -> &mut SessionProcessorImpl {
    value
        .get_mut_impl()
        .downcast_mut::<SessionProcessorImpl>()
        .unwrap()
}

impl SessionProcessorImpl {
    /*
        Debug utilities
    */

    fn check_completion_handlers(&mut self) {
        instrument!();

        let mut expired_handlers = Vec::new();

        for (handler_id, handler) in self.completion_handlers.iter() {
            if let Ok(latency) = handler.get_creation_time().elapsed() {
                if latency > COMPLETION_HANDLERS_MAX_WAIT_PERIOD {
                    expired_handlers.push((handler_id.clone(), latency));
                }
            }
        }

        for (handler_id, latency) in expired_handlers.iter_mut() {
            let handler = self.completion_handlers.remove(&handler_id);

            if let Some(mut handler) = handler {
                let warning = format!("Remove ValidatorSession completion handler #{} with latency {:.3}s (expected_latency={:.3}s): created at {}", handler_id, latency.as_secs_f64(), COMPLETION_HANDLERS_MAX_WAIT_PERIOD.as_secs_f64(), catchain::utils::time_to_string(&handler.get_creation_time()));

                warn!("{}", warning);

                handler.reset_with_error(failure::err_msg(warning), self);
            }
        }
    }

    fn get_total_active_weight(&self) -> u64 {
        let mut total_active_weight = 0;

        for i in 0..self.description.get_total_nodes() as usize {
            let last_preprocess_block_time = self.last_preprocess_block_time[i];
            let weight = self.description.get_node_weight(i as u32);
            let last_received_block_delay = last_preprocess_block_time.elapsed().unwrap();

            if last_received_block_delay <= VALIDATOR_IDLE_TIMEOUT {
                total_active_weight += weight;
            }
        }

        total_active_weight
    }

    fn debug_dump(&self) {
        instrument!();

        let mut result = "".to_string();
        let round_duration = self.round_started_at.elapsed();

        if let Ok(round_duration) = round_duration {
            if round_duration > ROUND_DEBUG_PERIOD {
                warn!("Session {} round #{} is too long (duration is {:.3}s, max expected duration is {:.3}s)", self.session_id.to_hex_string(), self.real_state.get_current_round_sequence_number(), round_duration.as_secs_f64(),
                    ROUND_DEBUG_PERIOD.as_secs_f64());
            }
        }

        //all code below will work only for debug logging mode

        if !log_enabled!(log::Level::Debug) {
            return;
        }

        let total_active_weight = self.get_total_active_weight();

        result = format!(
            "{}Session {} dump:\n",
            result,
            self.session_id.to_hex_string()
        );
        if let Ok(round_duration) = round_duration {
            result = format!(
                "{}  - round_duration: {:.3}s\n",
                result,
                round_duration.as_secs_f64(),
            );
        }
        result = format!(
            "{}  - validators_count: {}\n",
            result,
            self.description.get_total_nodes()
        );
        result = format!("{}  - local_idx: v{:03}\n", result, self.get_local_idx());
        result = format!(
            "{}  - total_weight: {}\n",
            result,
            self.description.get_total_weight()
        );
        result = format!(
            "{}  - cutoff_weight: {}\n",
            result,
            self.description.get_cutoff_weight()
        );
        result = format!(
            "{}  - active_weight: {} ({:.2}%)\n",
            result,
            total_active_weight,
            100.0 * total_active_weight as f64 / self.description.get_total_weight() as f64,
        );

        let mut non_active_dump = "".to_string();

        for i in 0..self.description.get_total_nodes() as usize {
            let last_preprocess_block_time = self.last_preprocess_block_time[i];
            let last_received_block_delay = last_preprocess_block_time.elapsed().unwrap();

            if last_received_block_delay <= VALIDATOR_IDLE_TIMEOUT {
                continue;
            }

            if non_active_dump != "" {
                non_active_dump = format!("{}, ", non_active_dump);
            }

            non_active_dump = if last_preprocess_block_time != SystemTime::UNIX_EPOCH {
                format!(
                    "{}v{:03}/{:.0}s",
                    non_active_dump,
                    i,
                    last_received_block_delay.as_secs_f64()
                )
            } else {
                format!("{}v{:03}/?", non_active_dump, i)
            };
        }

        result = format!("{}  - inactive: [{}]\n", result, non_active_dump);
        result = format!("{}  - nodes:\n", result);

        for i in 0..self.description.get_total_nodes() as u32 {
            let public_key_hash = self.description.get_source_public_key_hash(i);
            let adnl_id = self.description.get_source_adnl_id(i);
            let weight = self.description.get_node_weight(i);
            let last_preprocess_block_time = self.last_preprocess_block_time[i as usize];
            let last_received_block_delay = last_preprocess_block_time.elapsed().unwrap();
            let is_active = last_received_block_delay <= VALIDATOR_IDLE_TIMEOUT;
            let last_received_block_delay = if last_preprocess_block_time != SystemTime::UNIX_EPOCH
            {
                format!("{:6.2}s", last_received_block_delay.as_secs_f64())
            } else {
                "    N/A".to_string()
            };

            result = format!(
                "{}    - v{:03}: {} last_block={}, weight={}, adnl_id={}, public_key_hash={}\n",
                result,
                i,
                if is_active { "        " } else { "inactive" },
                last_received_block_delay,
                weight,
                adnl_id,
                public_key_hash,
            );
        }

        result = format!(
            "{}  - real_state:\n    - hash: {:08x}\n{}",
            result,
            self.real_state.get_hash(),
            self.real_state.dump(&self.description)
        );
        result = format!(
            "{}  - virtual_state:\n    - hash: {:08x}\n{}",
            result,
            self.virtual_state.get_hash(),
            self.virtual_state.dump(&self.description)
        );

        debug!("{}", result);
    }

    fn dump_block(&self, block: &BlockPtr) {
        self.dump_block_impl(block, 1);
    }

    fn dump_block_impl(&self, block: &BlockPtr, indent: usize) {
        let indent_str = (0..indent).map(|_| "  ").collect::<String>();
        let state = self.find_state(block);

        trace!("{}block {:?}", indent_str, block.get_hash());
        trace!("{}  prev for {:?}:", indent_str, block.get_hash());

        let mut parents = "".to_string();

        if let Some(ref prev) = block.get_prev() {
            self.dump_block_impl(prev, indent + 1);

            parents = format!("{:?}", prev.get_hash());
        }

        trace!("{}  deps for {:?}:", indent_str, block.get_hash());

        let deps = block.get_deps();

        for dep_block in deps {
            self.dump_block_impl(dep_block, indent + 1);

            if parents != "" {
                parents = format!("{}, ", parents);
            }

            parents = format!("{}{:?}", parents, dep_block.get_hash());
        }

        trace!(
            "{}  state for {:?} (parents={}): {:?}",
            indent_str,
            block.get_hash(),
            parents,
            state
        );

        let block_update: Result<ton::BlockUpdate> =
            catchain::utils::deserialize_tl_boxed_object(&block.get_payload().data());
        let node_public_key_hash = self
            .description
            .get_source_public_key_hash(block.get_source_id() as u32)
            .clone();
        let node_source_id = block.get_source_id() as u32;

        match block_update.as_ref() {
            Ok(block_update) => {
                let block_update = block_update.clone().only();
                let attempt_id = self
                    .description
                    .get_attempt_sequence_number(block_update.ts as u64);

                trace!(
                    "{}  actions for {:?}, attempt={}, source={} ({}):",
                    indent_str,
                    block.get_hash(),
                    attempt_id,
                    node_source_id,
                    node_public_key_hash
                );

                for msg in block_update.actions.iter() {
                    trace!("{}    {:?}", indent_str, msg);
                }
            }
            _ => {}
        }
    }

    /*
        Accessors
    */

    fn get_local_idx(&self) -> u32 {
        self.description.get_self_idx()
    }

    fn get_local_id(&self) -> &PublicKeyHash {
        self.description
            .get_source_public_key_hash(self.description.get_self_idx())
    }

    fn get_local_key(&self) -> &PrivateKey {
        &self.local_key
    }

    /*
        Caches
    */

    fn get_state_for_block_update(
        &mut self,
        block_update: &Option<::ton_api::ton::validator_session::blockupdate::BlockUpdate>,
    ) -> Option<SessionStatePtr> {
        if block_update.is_none() || TELEGRAM_NODE_COMPATIBILITY_HASHES_BUG {
            return None;
        }

        let block_update = block_update.as_ref().unwrap();
        let block_update_hash = block_update.state as u32;

        if let Some(state) = self.block_update_cache.get(&block_update_hash) {
            return Some(state.clone());
        }

        None
    }

    fn merge_states(
        &mut self,
        left: &SessionStatePtr,
        right: &SessionStatePtr,
        move_to_persistent: bool,
    ) -> SessionStatePtr {
        instrument!();

        let left_hash = left.get_hash();
        let right_hash = right.get_hash();

        if left_hash == right_hash && !TELEGRAM_NODE_COMPATIBILITY_HASHES_BUG {
            return left.clone();
        }

        let merge_key = (left_hash, right_hash);

        if !TELEGRAM_NODE_COMPATIBILITY_HASHES_BUG {
            if let Some(state) = self.state_merge_cache.get(&merge_key) {
                return state.clone();
            }
        }

        let result = {
            instrument!();

            let mut result = left.merge(&right, &mut self.description);

            if move_to_persistent {
                result = result.move_to_persistent(&mut self.description);
            }

            result
        };

        if !TELEGRAM_NODE_COMPATIBILITY_HASHES_BUG {
            self.state_merge_cache.insert(merge_key, result.clone());
        }

        result
    }

    /*
        Block to state mapping
    */

    fn find_state(&self, block: &BlockPtr) -> Option<&SessionStatePtr> {
        let extra_id = block.get_extra_id() as usize;

        if extra_id < self.block_to_state_map.len() {
            if let Some(state) = &self.block_to_state_map[extra_id] {
                return Some(state);
            }
        }

        None
    }

    fn get_state(&self, block: &BlockPtr) -> &SessionStatePtr {
        let state = self.find_state(block);

        if let Some(ref state) = state {
            return state;
        }

        let extra_id = block.get_extra_id() as usize;

        error!(
            "...can't find state for block {:?} with extra ID {}",
            block, extra_id
        );

        unreachable!();
    }

    fn set_state(&mut self, block: &BlockPtr, state: SessionStatePtr) {
        let extra_id = block.get_extra_id() as usize;

        if extra_id >= self.block_to_state_map.len() {
            self.block_to_state_map.resize(extra_id + 1, None);
        }

        trace!(
            "...set state {:08x?} for block {:?} with extra ID {}",
            state.get_hash(),
            block,
            extra_id
        );

        self.block_to_state_map[extra_id] = Some(state.clone());
    }

    /*
        Round management
    */

    fn get_latency_from_round_start(
        &self,
    ) -> std::result::Result<Duration, std::time::SystemTimeError> {
        self.description
            .get_time()
            .duration_since(self.round_started_at)
    }

    fn new_round(&mut self, round: u32) {
        instrument!();

        //debug dump for states

        trace!(
            "...new round request for current round {} and round {}, {}",
            self.current_round,
            round,
            self.session_id.to_hex_string()
        );

        if DEBUG_DUMP_ON_NEW_ROUND {
            self.debug_dump();
        }

        if round != 0 {
            trace!(
                "...reset current round {}, because round {} is started",
                self.current_round,
                round
            );

            assert!(self.current_round < round);

            if let Ok(latency) = self.get_latency_from_round_start() {
                self.round_duration_histogram
                    .record_value(latency.as_millis() as u64);
            }

            self.pending_generate = false;
            self.generated = false;
            self.sent_generated = false;

            self.pending_approve.clear();
            self.rejected.clear();
            self.pending_reject.clear();
            self.approved.clear();

            self.pending_sign = false;
            self.signed = false;
            self.signature = BlockSignature::default();
            self.signed_block = BlockId::default();

            self.first_candidate_received = false;
            self.first_candidate_approved = false;
            self.first_candidate_voted = false;
            self.first_candidate_precommitted = false;

            self.active_requests.clear();
        }

        //apply finished rounds to current state

        while self.current_round < round {
            trace!(
                "...apply current round {}, target round is {}",
                self.current_round,
                round
            );

            if DEBUG_CHECK_ALL_BEFORE_ROUND_SWITCH {
                trace!(
                    "...check session state before switching of current round {}, target round is {}",
                    self.current_round, round
                );

                self.check_all();
            }

            let signed_block = self
                .real_state
                .get_committed_block(&self.description, self.current_round);
            let src_signatures = self
                .real_state
                .get_committed_block_signatures(self.current_round);
            let src_approve_signatures = self
                .real_state
                .get_committed_block_approve_signatures(self.current_round);

            assert!(
                src_signatures.is_some(),
                "Signatures are expected at the round commit phase"
            );
            assert!(
                src_approve_signatures.is_some(),
                "Signatures are expected at the round commit phase"
            );

            let signatures_exporter = |desc: &dyn SessionDescription,
                                       signatures: &BlockCandidateSignatureVectorPtr|
             -> Vec<(PublicKeyHash, BlockPayloadPtr)> {
                let mut result: Vec<(PublicKeyHash, BlockPayloadPtr)> =
                    Vec::with_capacity(desc.get_total_nodes() as usize);

                for i in 0..desc.get_total_nodes() as usize {
                    if let Some(signature) = signatures.at(i) {
                        result.push((
                            self.description
                                .get_source_public_key_hash(i as u32)
                                .clone(),
                            catchain::CatchainFactory::create_block_payload(
                                signature.get_signature().clone(),
                            ),
                        ));
                    }
                }

                result
            };

            let signatures = signatures_exporter(
                &self.description,
                &src_signatures
                    .as_ref()
                    .expect("Signatures are expected at the round commit phase"),
            );
            let approve_signatures = signatures_exporter(
                &self.description,
                &src_approve_signatures
                    .as_ref()
                    .expect("Signatures are expected at the round commit phase"),
            );

            assert!(signed_block.is_some()); //because round was finished we expect it has commit at the end, even with empty block

            let signed_block = signed_block.expect("Signed block is expected");

            if let Some(ref signed_block) = signed_block {
                //signed block was committed

                trace!(
                    "...block is signed for round {}; signatures={:?}, approve_signatures={:?}",
                    self.current_round,
                    signatures,
                    approve_signatures
                );

                if DEBUG_EVENTS_LOG {
                    info!(
                        "EVENTS LOG: Commit for round {}: root_hash={:?}",
                        self.current_round,
                        signed_block.get_root_hash()
                    );
                }

                let signed_tl_block = self
                    .get_signed_block_for_round(self.current_round, signed_block.get_id())
                    .clone();
                let validator_public_key = self
                    .description
                    .get_source_public_key(signed_block.get_source_index())
                    .clone();

                if let Some((signed_tl_block, _signed_block_creation_time)) = signed_tl_block {
                    //normal signed block

                    self.notify_block_committed(
                        self.current_round,
                        &validator_public_key,
                        &signed_block.get_root_hash(),
                        &signed_block.get_file_hash(),
                        &catchain::CatchainFactory::create_block_payload(
                            signed_tl_block.data().clone(),
                        ),
                        signatures,
                        approve_signatures,
                    );
                } else {
                    //empty signed block

                    self.notify_block_committed(
                        self.current_round,
                        &validator_public_key,
                        &signed_block.get_root_hash(),
                        &signed_block.get_file_hash(),
                        &catchain::CatchainFactory::create_empty_block_payload(),
                        signatures,
                        approve_signatures,
                    );
                }
            } else {
                //no block was committed

                trace!("...block is skipped for round {}", self.current_round);

                self.notify_block_skipped(self.current_round);
            }

            //update slashing statistics

            self.slashing_stat.reset();

            for i in 0..self.description.get_total_nodes() {
                let public_key_hash = self.description.get_source_public_key_hash(i);
                let slashing_node = self
                    .slashing_stat
                    .validators_stat
                    .get_mut(public_key_hash)
                    .unwrap();

                use slashing::Metric::*;

                slashing_node.increment(TotalRoundsCount, 1);

                if self.description.get_node_priority(i, self.current_round) >= 0 {
                    slashing_node.increment(TotalCollationRoundsCount, 1);
                }
                if let Some(ref signed_block) = signed_block {
                    if signed_block.get_source_index() == i {
                        slashing_node.increment(CollationsCount, 1);
                    }
                }
                if src_signatures.at(i as usize).is_some() {
                    slashing_node.increment(CommitsCount, 1);
                }
                if src_approve_signatures.at(i as usize).is_some() {
                    slashing_node.increment(ApprovalsCount, 1);
                }
            }

            self.notify_slashing_statistics(self.current_round, self.slashing_stat.clone());

            //remove current round block payloads because we have already processed it

            self.blocks.remove(&self.current_round);

            //increment round

            self.current_round += 1;

            if DEBUG_EVENTS_LOG {
                info!("EVENTS LOG: New round {}", self.current_round);
            }
        }

        //update debug checking time points

        self.round_started_at = self.description.get_time();
        self.round_debug_at = self.round_started_at + ROUND_DEBUG_PERIOD;

        //check state

        self.check_all();
    }

    fn request_new_block(&mut self, now: bool) {
        instrument!();

        if self.requested_new_block_now {
            //ignore double attempts to generate new block immediately

            return;
        }

        if !now && self.requested_new_block {
            //ignore double attemts to generate new block

            return;
        }

        trace!("...request new block from a catchain");

        //generate new block request to a catchain

        self.requested_new_block = true;

        let mut block_generation_time = SystemTime::now();

        if now {
            self.requested_new_block_now = true;
        } else {
            if !DEBUG_REQUEST_NEW_BLOCKS_IMMEDIATELY {
                //calculate timeout when new block should be generated

                let lambda = 10.0 / (self.description.get_total_nodes() as f64);
                let delta_secs = -1.0 / lambda
                    * f64::ln((self.description.generate_random_usize() % 999 + 1) as f64 * 0.001);
                let mut delta_secs = Duration::from_secs_f64(delta_secs);

                if delta_secs > MAX_NEXT_BLOCK_WAIT_DELAY {
                    delta_secs = MAX_NEXT_BLOCK_WAIT_DELAY;
                }

                let round_duration = self.round_started_at.elapsed();

                if let Ok(round_duration) = round_duration {
                    if round_duration > ROUND_DEBUG_PERIOD {
                        trace!("Session {} round #{} is too long (duration is {:.3}s, max expected duration is {:.3}s). Calming down", self.session_id.to_hex_string(), self.real_state.get_current_round_sequence_number(), round_duration.as_secs_f64(),
                            ROUND_DEBUG_PERIOD.as_secs_f64());

                        delta_secs = HANGED_CONSENSUS_UPDATE_TIME;
                    }
                }

                block_generation_time += delta_secs;
            }
        }

        self.request_new_block_counter.increment();

        self.catchain.request_new_block(block_generation_time);
    }

    /*
        Attempts management
    */

    fn check_action(&mut self, attempt: u32) {
        instrument!();

        if !self.catchain_started {
            return;
        }

        if self.requested_new_block {
            return;
        }

        use ton_api::ton::validator_session::round::*;

        let action =
            self.virtual_state
                .create_action(&self.description, self.get_local_idx(), attempt);

        if let Some(action) = action {
            match action {
                Message::ValidatorSession_Message_Empty(_) => {}
                _ => {
                    self.request_new_block(false);
                }
            }
        }
    }

    /*
        Blocks generation management
    */

    fn insert_signed_block_for_round(
        &mut self,
        round: u32,
        block_id: &BlockId,
        data: (BlockCandidateTlPtr, std::time::SystemTime),
    ) {
        let round_block_map = if let Some(round_block_map) = self.blocks.get(&round) {
            round_block_map.clone()
        } else {
            let round_block_map = Rc::new(RefCell::new(HashMap::new()));

            self.blocks.insert(round, round_block_map.clone());

            round_block_map
        };

        round_block_map.borrow_mut().insert(block_id.clone(), data);
    }

    fn get_signed_block_for_round(
        &self,
        round: u32,
        block_id: &BlockId,
    ) -> Option<(BlockCandidateTlPtr, std::time::SystemTime)> {
        if let Some(round_block_map) = self.blocks.get(&round) {
            if let Some(block) = round_block_map.borrow().get(&block_id) {
                return Some(block.clone());
            }
        }

        None
    }

    fn check_generate_slot(&mut self) {
        instrument!();

        //don't do anything until catchain is started

        if !self.catchain_started {
            return;
        }

        //don't generate block if it has been already generated in this round

        if self.generated || self.pending_generate {
            return;
        }

        //don't generate block if it has been sent already according to a state of this validator

        if self.real_state.check_block_is_sent_by(self.get_local_idx()) {
            self.generated = true;
            self.sent_generated = true;
            return;
        }

        //check if we have a priority to generate block in current round

        let priority = self
            .description
            .get_node_priority(self.get_local_idx(), self.current_round);

        if priority < 0 && !DEBUG_IGNORE_PROPOSALS_PRIORITY {
            return;
        }

        trace!("...block generation priority is {}", priority);

        if DEBUG_IGNORE_PROPOSALS_PRIORITY {
            warn!("...DEBUG_IGNORE_PROPOSALS_PRIORITY is enabled");
        }

        //don't generate block until the generation time slot

        let block_generation_time = if DEBUG_IGNORE_PROPOSALS_PRIORITY {
            self.description.get_time()
        } else {
            self.round_started_at + self.description.get_delay(priority as u32)
        };

        let block_generation_time_end =
            block_generation_time + self.description.opts().next_candidate_delay;

        if self.description.is_in_future(block_generation_time) {
            self.set_next_awake_time(block_generation_time);
            return;
        }

        trace!(
            "...generating new block with priority {} at {}",
            priority,
            catchain::utils::time_to_string(&block_generation_time)
        );

        //send block generation request to a collator

        self.pending_generate = true;

        let round = self.current_round;

        const MAX_GENERATION_TIME: std::time::Duration = std::time::Duration::from_millis(1000);
        let start_generation_time = std::time::SystemTime::now();
        let collation_latency_histogram = self.collation_latency_histogram.clone();

        let completion_handler = task_queue::create_completion_handler(
            self,
            move |result: Result<ValidatorBlockCandidatePtr>, processor| {
                let generation_duration = start_generation_time.elapsed().unwrap();

                collation_latency_histogram.record_value(generation_duration.as_millis() as u64);

                if generation_duration > MAX_GENERATION_TIME {
                    warn!(
                        "Execution time {:.3}ms for block generation is greater than expected time {:.3}ms at {}({})",
                        generation_duration.as_secs_f64() * 1000.0,
                        MAX_GENERATION_TIME.as_secs_f64() * 1000.0,
                        file!(),
                        line!()
                    );
                }

                let processor = get_mut_impl(processor);

                if let Ok(offset) = block_generation_time_end.elapsed() {
                    warn!(
                        "Block generation time slot has been expired by {:.3}ms at {}({})",
                        offset.as_secs_f64() * 1000.0,
                        file!(),
                        line!()
                    );

                    processor.collates_expire_counter.success();
                } else {
                    processor.collates_expire_counter.failure();
                }

                match result {
                    Ok(candidate) => {
                        trace!("SessionProcessor::check_generate_slot: new block candidate has been generated {:?}", candidate);

                        processor.collates_counter.success();

                        processor.generated_block(
                            round,
                            candidate.id.root_hash.clone().into(),
                            candidate.data.clone(),
                            candidate.collated_data.clone(),
                        );
                    }
                    Err(err) => {
                        processor.collates_counter.failure();

                        warn!("SessionProcessor::check_generate_slot: failed to generate block candidate: {:?}", err);
                    }
                }
            },
        );

        self.notify_generate_slot(self.current_round, completion_handler);
    }

    fn generated_block(
        &mut self,
        round: u32,
        root_hash: BlockId,
        data: BlockPayloadPtr,
        collated_data: BlockPayloadPtr,
    ) {
        instrument!();

        if round != self.current_round {
            //accept blocks only for current round

            return;
        }

        trace!("SessionProcessor::generated_block: candidate has been received for round={}, root_hash={:?}", round, root_hash);

        if DEBUG_EVENTS_LOG {
            info!("EVENTS LOG: New block candidate has been generated for round {}: root_hash={:?}, data_size={}, collated_data_size={}", round, root_hash,
                data.data().0.len(), collated_data.data().0.len());
        }

        if data.data().0.len() > self.description.opts().max_block_size as usize
            || collated_data.data().0.len()
                > self.description.opts().max_collated_data_size as usize
        {
            error!("SessionProcessor::generated_block: generated candidate is too big. Dropping. size={}/{}", data.data().0.len(), collated_data.data().0.len());
            return;
        }

        //prepare data

        use ton_api::ton::validator_session::*;

        let candidate_creation_time =
            if data.get_creation_time() < collated_data.get_creation_time() {
                data.get_creation_time()
            } else {
                collated_data.get_creation_time()
            };
        let file_hash = catchain::utils::get_hash_from_block_payload(&data);
        let collated_data_file_hash = catchain::utils::get_hash_from_block_payload(&collated_data);
        let candidate = Rc::new(
            candidate::Candidate {
                src: UInt256::with_array(self.get_local_id().data().clone()),
                round: round as i32,
                root_hash: root_hash.clone().into(),
                data: data.data().clone().0.into(),
                collated_data: collated_data.data().clone().0.into(),
            }
            .into_boxed(),
        );
        let serialized_block = catchain::utils::serialize_tl_boxed_object!(&*candidate);
        let serialized_block = catchain::CatchainFactory::create_block_payload(serialized_block);
        let block_id = self.description.candidate_id(
            self.get_local_idx(),
            &root_hash,
            &file_hash,
            &collated_data_file_hash,
        );

        //send broadcast to catchain about new block candidate

        self.catchain.send_broadcast(serialized_block);

        //save block and update state

        self.insert_signed_block_for_round(
            self.current_round,
            &block_id,
            (candidate, candidate_creation_time),
        );

        self.pending_generate = false;
        self.generated = true;
        self.generated_block = block_id;

        //request new block from the catchain

        self.request_new_block(true);
    }

    /*
        Approval management
    */

    fn check_approve(&mut self) {
        instrument!();

        //don't do anything until catchain is started

        if !self.catchain_started {
            return;
        }

        //choose blocks to approve from proposed candidates

        let to_approve = self
            .virtual_state
            .choose_blocks_to_approve(&self.description, self.get_local_idx());

        trace!("block to approve {:?}", &to_approve);

        if !self.first_candidate_received && to_approve.len() > 0 {
            trace!(
                "first candidate has been received in round #{}",
                self.current_round
            );

            self.first_candidate_received = true;

            if let Ok(latency) = self.get_latency_from_round_start() {
                self.first_candidate_received_latency_histogram
                    .record_value(latency.as_millis() as u64);
            }
        }

        for block in to_approve {
            self.try_approve_block(block);
        }
    }

    fn try_approve_block(&mut self, block: SentBlockPtr) {
        instrument!();

        let block_id = block.get_id();

        //check if this block has been already approved

        if let Some((approve_time, _block)) = self.approved.get(&block_id) {
            if approve_time <= &self.description.get_time() {
                self.request_new_block(false);
            } else {
                //awake when block will be approved (approved block may be valid from some specified by validator time)

                let approve_time = *approve_time; //make Rust happy about immutable / mutable borrowing

                self.set_next_awake_time(approve_time);
            }

            return;
        }

        trace!(
            "...try to approve block {:?} in round {}",
            block_id,
            self.current_round
        );

        //check if block has been waiting for approval or been rejected

        if self.pending_approve.contains(&block_id) || self.rejected.contains(&block_id) {
            trace!("...block {:?} is waiting for approval", block_id);
            return;
        }

        //compute block proposal delay according to block's source validator priority in this round

        let block_round_proposal_delay = match &block {
            Some(block) => self.description.get_delay(
                self.description
                    .get_node_priority(block.get_source_index(), self.current_round)
                    as u32,
            ),
            _ => self.description.get_empty_block_delay(),
        };
        let block_proposal_time = self.round_started_at + block_round_proposal_delay;

        if self.description.is_in_future(block_proposal_time) {
            //wait till block will be valid or approval

            trace!(
                "...block should be proposed later in round {} at {}",
                self.current_round,
                catchain::utils::time_to_string(&block_proposal_time)
            );

            self.set_next_awake_time(block_proposal_time);
            return;
        }

        //skip approval of empty block

        if block.is_none() {
            trace!(
                "...empty block will be automatically approved in round {}",
                self.current_round
            );

            self.approved.insert(
                block_id.clone(),
                (
                    SystemTime::UNIX_EPOCH,
                    catchain::CatchainFactory::create_empty_block_payload(),
                ),
            );
            self.request_new_block(false);
            return;
        }

        let block = block.as_ref().unwrap();

        const BLOCK_VALIDATION_TIMEOUT: Duration = Duration::from_secs(2);

        let block_proposal_time = self.round_started_at
            + self.description.get_delay(block.get_source_index())
            + BLOCK_VALIDATION_TIMEOUT;

        trace!(
            "...searching for block {:?} payload for round {}",
            block_id,
            self.current_round
        );

        let tl_block_opt: Option<(BlockCandidateTlPtr, std::time::SystemTime)> =
            match self.get_signed_block_for_round(self.current_round, &block_id) {
                Some(tl_block) => Some(tl_block.clone()),
                None => None,
            };

        //if block was proposed in current round - validate it

        if let Some((tl_block, broadcast_creation_time)) = tl_block_opt {
            trace!(
                "...validating block {:?} for round {}",
                tl_block,
                self.current_round
            );

            self.pending_approve.insert(block_id.clone());

            assert!(self.current_round == *tl_block.round() as u32);

            let round = self.current_round;
            let hash = block_id.clone();
            let root_hash = block.get_root_hash().clone();
            let file_hash = block.get_file_hash().clone();

            const MAX_VALIDATION_TIME: std::time::Duration = std::time::Duration::from_millis(750);
            let start_validation_time = std::time::SystemTime::now();
            let session_processor_creation_time = self.session_processor_creation_time.clone();
            let session_creation_time = self.session_creation_time.clone();
            let block_creation_time = block.get_source_block_creation_time();
            let block_payload_creation_time = block.get_source_block_payload_creation_time();
            let sent_block_creation_time = block.get_creation_time();
            let tl_block_clone = tl_block.clone();
            let validation_latency_histogram = self.validation_latency_histogram.clone();
            let block_candidate_broadcast_validation_latency_histogram = self
                .block_candidate_broadcast_validation_latency_histogram
                .clone();

            let backtrace = if DEBUG_DUMP_BACKTRACE_FOR_LATE_VALIDATIONS {
                Some(Backtrace::new())
            } else {
                None
            };

            let completion_handler = task_queue::create_completion_handler(
                self,
                move |result, processor| {
                    let validation_duration = start_validation_time.elapsed().unwrap();
                    let broadcast_processing_duration = broadcast_creation_time.elapsed().unwrap();

                    if let Err(ref err) = &result {
                        let source_id: PublicKeyHash =
                            catchain::utils::int256_to_public_key_hash(tl_block_clone.src());
                        let source_idx = processor.get_description().get_source_index(&source_id);

                        if DEBUG_EVENTS_LOG {
                            info!("EVENTS LOG: Validation failed for round {}: root_hash={}, data_size={}, collated_data_size={}", round, tl_block_clone.root_hash(),
                                tl_block_clone.data().0.len(), tl_block_clone.collated_data().0.len());
                        }

                        warn!(
                            "Validation failed for block {:?} with verdict {:?} (round={}, source=v{:03} ({}), full_processing_time={:.3}ms, expected_processing_time={:.3}ms, validation_time={:.3}ms, sent_block_creation_time={:.3}ms, block_creation_time={:.3}ms, block_payload_creation_time={:.3}ms, session_duration={:.3}s/{:.3}s) at {}({}); {}",
                            &tl_block_clone.root_hash(),
                            err,
                            round,
                            source_idx,
                            source_id,
                            broadcast_processing_duration.as_secs_f64() * 1000.0,
                            MAX_VALIDATION_TIME.as_secs_f64() * 1000.0,
                            validation_duration.as_secs_f64() * 1000.0,
                            sent_block_creation_time.elapsed().unwrap().as_secs_f64() * 1000.0,
                            block_creation_time.elapsed().unwrap().as_secs_f64() * 1000.0,
                            block_payload_creation_time.elapsed().unwrap().as_secs_f64() * 1000.0,
                            session_creation_time.elapsed().unwrap().as_secs_f64(),
                            session_processor_creation_time.elapsed().unwrap().as_secs_f64(),
                            file!(),
                            line!(),
                            if DEBUG_DUMP_BACKTRACE_FOR_LATE_VALIDATIONS { format!("{:?}", backtrace) } else { "".to_string() },
                        );

                        if validation_duration > MAX_VALIDATION_TIME {
                            warn!(
                                "Execution time {:.3}ms for validation is greater than expected time {:.3}ms at {}({})",
                                validation_duration.as_secs_f64() * 1000.0,
                                MAX_VALIDATION_TIME.as_secs_f64() * 1000.0,
                                file!(),
                                line!()
                            );
                        }

                        if broadcast_processing_duration > MAX_VALIDATION_TIME {
                            warn!(
                                "Execution time {:.3}ms for full block processing during validation is greater than expected time {:.3}ms (round={}, validation_time={:.3}ms, sent_block_creation_time={:.3}ms, block_creation_time={:.3}ms, block_payload_creation_time={:.3}ms, session_duration={:.3}s/{:.3}s) at {}({})",
                                broadcast_processing_duration.as_secs_f64() * 1000.0,
                                MAX_VALIDATION_TIME.as_secs_f64() * 1000.0,
                                round,
                                validation_duration.as_secs_f64() * 1000.0,
                                sent_block_creation_time.elapsed().unwrap().as_secs_f64() * 1000.0,
                                block_creation_time.elapsed().unwrap().as_secs_f64() * 1000.0,
                                block_payload_creation_time.elapsed().unwrap().as_secs_f64() * 1000.0,
                                session_creation_time.elapsed().unwrap().as_secs_f64(),
                                session_processor_creation_time.elapsed().unwrap().as_secs_f64(),
                                file!(),
                                line!(),
                            );
                        }
                    } else {
                        if DEBUG_EVENTS_LOG {
                            info!("EVENTS LOG: Validation succeed for round {}: root_hash={}, data_size={}, collated_data_size={}", round, tl_block_clone.root_hash(),
                                tl_block_clone.data().0.len(), tl_block_clone.collated_data().0.len());
                        }
                    }

                    validation_latency_histogram
                        .record_value(validation_duration.as_millis() as u64);
                    block_candidate_broadcast_validation_latency_histogram
                        .record_value(broadcast_processing_duration.as_millis() as u64);

                    let processor = get_mut_impl(processor);

                    match result {
                        Ok(validity_start_time) => processor.candidate_decision_ok(
                            round,
                            hash,
                            root_hash,
                            file_hash,
                            validity_start_time,
                        ),
                        Err(err) => processor.candidate_decision_fail(round, hash, err),
                    }
                },
            );

            let source_public_key = self
                .description
                .get_source_public_key(block.get_source_index())
                .clone();

            if DEBUG_EVENTS_LOG {
                info!("EVENTS LOG: Validating block candidate for round {}: root_hash={}, data_size={}, collated_data_size={}", round, tl_block.root_hash(),
                    tl_block.data().0.len(), tl_block.collated_data().0.len());
            }

            self.notify_candidate(
                round,
                &source_public_key,
                &tl_block.root_hash().clone().into(),
                &catchain::CatchainFactory::create_block_payload(tl_block.data().clone()),
                &catchain::CatchainFactory::create_block_payload(tl_block.collated_data().clone()),
                completion_handler,
            );

            return;
        }

        //if block was not proposed in current round but it's proposal time is in past - request block

        if self.description.is_in_past(block_proposal_time) {
            if self.active_requests.contains(block_id) {
                return;
            }

            trace!(
                "...request absent block {:?} for round {}",
                block_id,
                self.current_round
            );

            let approvers = self
                .virtual_state
                .get_block_approvers(&self.description, block_id);

            if approvers.len() == 0 {
                trace!(
                    "...block {:?} has not been aproved by any node yet in round {}",
                    block_id,
                    self.current_round
                );
                return;
            }

            let node_index = self.description.generate_random_usize() % approvers.len();
            let node_adnl_id = self
                .description
                .get_source_adnl_id(approvers[node_index] as u32)
                .clone();
            let source_id = self
                .description
                .get_source_public_key_hash(block.get_source_index())
                .clone();

            self.active_requests.insert(block_id.clone());

            const DOWNLOAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

            let block_id_clone = block_id.clone();
            let node_adnl_id_clone = node_adnl_id.clone();
            let source_id_clone = source_id.clone();
            let round = self.current_round;

            self.get_broadcast_p2p(
                &node_adnl_id,
                block.get_file_hash(),
                block.get_collated_data_file_hash(),
                &source_id,
                self.current_round,
                block.get_root_hash(),
                self.description.get_time() + DOWNLOAD_TIMEOUT,
                move |result: Result<BlockPayloadPtr>, processor: &mut dyn SessionProcessor| {
                    let processor = get_mut_impl(processor);

                    if processor.current_round == round {
                        processor.active_requests.remove(&block_id_clone);
                    }

                    if let Err(err) = result {
                        processor.rldp_queries_counter.failure();

                        warn!(
                            "Failed to get block candidate {:?} from node {}: {:?}",
                            block_id_clone, node_adnl_id_clone, err
                        );
                        return;
                    }

                    processor.rldp_queries_counter.success();

                    processor.process_broadcast(source_id_clone, result.ok().unwrap());
                },
            );

            return;
        }

        //wait until block proposal time will come

        trace!(
            "...wait until the next block proposal at {} (current time is {})",
            catchain::utils::time_to_string(&block_proposal_time),
            catchain::utils::time_to_string(&self.description.get_time())
        );

        self.set_next_awake_time(block_proposal_time);
    }

    fn get_broadcast_p2p<F>(
        &mut self,
        node_adnl_id: &PublicKeyHash,
        file_hash: &BlockHash,
        collated_data_file_hash: &BlockHash,
        source: &PublicKeyHash,
        round: u32,
        root_hash: &BlockHash,
        timeout: std::time::SystemTime,
        complete_handler: F,
    ) where
        F: FnOnce(Result<BlockPayloadPtr>, &mut dyn SessionProcessor) + 'static,
    {
        instrument!();

        if self.description.is_in_past(timeout) {
            complete_handler(Err(failure::format_err!("get_broadcast_p2p timeout")), self);
            return;
        }

        let download_candidate = ton::DownloadCandidate {
            round: round as ton::int,
            id: ton::candidateid::CandidateId {
                src: catchain::utils::public_key_hash_to_int256(source),
                root_hash: root_hash.clone().into(),
                file_hash: file_hash.clone().into(),
                collated_data_file_hash: collated_data_file_hash.clone().into(),
            },
        };
        let serialized_download_candidate =
            catchain::utils::serialize_tl_boxed_object!(&download_candidate);
        let serialized_download_candidate =
            catchain::CatchainFactory::create_block_payload(serialized_download_candidate);
        let max_answer_size = self.description.opts().max_block_size
            + self.description.opts().max_collated_data_size
            + 1024;
        let response_callback =
            task_queue::create_completion_handler(self, move |result, processor| {
                complete_handler(result, processor);
            });

        self.rldp_queries_counter.total_increment();

        self.catchain.send_query_via_rldp(
            node_adnl_id.clone(),
            "download candidate".to_string(),
            response_callback,
            timeout,
            serialized_download_candidate,
            max_answer_size as u64,
        );
    }

    fn candidate_decision_ok(
        &mut self,
        round: u32,
        hash: BlockId,
        root_hash: BlockHash,
        file_hash: BlockHash,
        validity_start_time: SystemTime,
    ) {
        instrument!();

        self.validates_counter.success();

        if round != self.current_round {
            return;
        }

        trace!(
            "SessionProcessor::candidate_decision_ok: approved candidate {:?}",
            hash
        );

        use ton_api::ton::*;

        let data = ::catchain::utils::serialize_tl_boxed_object!(&ton::blockid::BlockIdApprove {
            root_cell_hash: root_hash.into(),
            file_hash: file_hash.into(),
        }
        .into_boxed());

        match self.get_local_key().sign(&data.0) {
            Err(err) => error!(
                "SessionProcessor::candidate_decision_ok: failed to sign blockId {:?}: {:?}",
                data, err
            ),
            Ok(signature) => self.candidate_approved_signed(
                round,
                hash,
                validity_start_time,
                ::ton_api::ton::bytes(signature.to_vec()),
            ),
        }
    }

    fn candidate_decision_fail(&mut self, round: u32, hash: BlockId, err: failure::Error) {
        instrument!();

        self.validates_counter.failure();

        if round != self.current_round {
            return;
        }

        let reason = format!("{}", err);

        error!(
            "SessionProcessor::candidate_decision_fail: failed candidate {:?}, reason={:?}",
            hash, reason
        );

        self.pending_approve.remove(&hash);
        self.pending_reject.insert(
            hash.clone(),
            catchain::CatchainFactory::create_block_payload(reason.as_bytes().to_vec().into()),
        );
        self.rejected.insert(hash);
    }

    fn candidate_approved_signed(
        &mut self,
        _round: u32,
        hash: BlockId,
        validity_start_time: SystemTime,
        signature: BlockSignature,
    ) {
        instrument!();

        self.pending_approve.remove(&hash);
        self.approved.insert(
            hash.clone(),
            (
                validity_start_time,
                catchain::CatchainFactory::create_block_payload(signature),
            ),
        );

        if validity_start_time <= self.description.get_time() {
            self.request_new_block(false);
        } else {
            warn!("SessionProcessor::candidate_approved_signed: too new block {:?} with validity_start_time={:?}", hash, validity_start_time);
            self.set_next_awake_time(validity_start_time);
        }
    }

    /*
        Voting management
    */

    fn check_vote_for_slot(&mut self, attempt: u32) {
        instrument!();

        if !self.catchain_started {
            return;
        }

        if self.virtual_state.check_need_generate_vote_for(
            &self.description,
            self.get_local_idx(),
            attempt,
        ) {
            self.request_new_block(false);
        }
    }

    /*
        Commit management
    */

    fn check_sign_slot(&mut self) {
        instrument!();

        //if catchain is not started, there is nothing to do

        if !self.catchain_started {
            return;
        }

        //prevent second signing if we are already pending for signature

        if self.pending_sign {
            return;
        }

        //check if we have signed block

        if self
            .real_state
            .check_block_is_signed_by(self.get_local_idx())
        {
            self.signed = true;
            return;
        }

        //if we block has been signed, request catchain for a new one

        if self.signed {
            self.request_new_block(false);
            return;
        }

        //choose block for signing

        let commit_candidate = self
            .virtual_state
            .choose_block_to_sign(&self.description, self.get_local_idx());

        if commit_candidate.is_none() {
            return;
        }

        let commit_candidate = commit_candidate.unwrap();

        //check if we are trying to sign empty block

        if commit_candidate.is_none() {
            trace!("...signing empty block");

            self.signed = true;
            self.signed_block = SKIP_ROUND_CANDIDATE_BLOCKID.clone();

            self.request_new_block(false);

            return;
        }

        //block signing

        let commit_candidate = commit_candidate.unwrap();

        trace!("...signing block {:?}", commit_candidate);

        self.pending_sign = true;

        //serialize block ID

        let block_id = ton::blockid::BlockId {
            root_cell_hash: commit_candidate.get_root_hash().clone().into(),
            file_hash: commit_candidate.get_file_hash().clone().into(),
        }
        .into_boxed();
        let block_id_serialized = catchain::utils::serialize_tl_boxed_object!(&block_id);

        //sign serialized block ID

        let sign_result = self.get_local_key().sign(&block_id_serialized);

        if let Err(err) = sign_result {
            error!("...block signing error: {:?}", err);
            return;
        }

        //further process of signed block

        let block_signature = sign_result.ok().unwrap().to_vec().into();

        self.signed_block(
            self.current_round,
            commit_candidate.get_id().clone(),
            block_signature,
        );
    }

    fn signed_block(&mut self, round: u32, hash: BlockId, signature: BlockSignature) {
        instrument!();

        if round != self.current_round {
            return;
        }

        //update state with signed block

        self.pending_sign = false;
        self.signed = true;
        self.signed_block = hash;
        self.signature = signature;

        //request new block from catchain

        self.request_new_block(false);
    }

    /*
        Listener management
    */

    fn notify_slashing_statistics(&self, round: u32, stat: SlashingValidatorStat) {
        check_execution_time!(20000);
        instrument!();

        trace!(
            "SessionProcessor::notify_slashing_statistics: post on_slashing_statistics event for further processing"
        );

        let listener = self.session_listener.clone();

        post_callback_closure(&self.callbacks_task_queue, move || {
            check_execution_time!(20000);

            if let Some(listener) = listener.upgrade() {
                trace!("SessionProcessor::notify_session_statistics: on_slashing_statistics start");

                listener.on_slashing_statistics(round, stat);

                trace!(
                    "SessionProcessor::notify_session_statistics: on_slashing_statistics finish"
                );
            }
        });
    }

    fn notify_candidate(
        &mut self,
        round: u32,
        source: &PublicKey,
        root_hash: &BlockHash,
        data: &BlockPayloadPtr,
        collated_data: &BlockPayloadPtr,
        callback: ValidatorBlockCandidateDecisionCallback,
    ) {
        check_execution_time!(20000);
        instrument!();

        trace!(
            "SessionProcessor::notify_candidate: post on_candidate event for further processing"
        );

        let listener = self.session_listener.clone();
        let source_clone = source.clone();
        let root_hash_clone = root_hash.clone();
        let data_clone = data.clone();
        let collated_data_clone = collated_data.clone();

        self.validates_counter.total_increment();

        post_callback_closure(&self.callbacks_task_queue, move || {
            check_execution_time!(20000);

            if let Some(listener) = listener.upgrade() {
                trace!("SessionProcessor::notify_candidate: on_candidate start");

                listener.on_candidate(
                    round,
                    source_clone,
                    root_hash_clone,
                    data_clone,
                    collated_data_clone,
                    callback,
                );

                trace!("SessionProcessor::notify_candidate: on_candidate finish");
            }
        });
    }

    fn notify_generate_slot(&mut self, round: u32, callback: ValidatorBlockCandidateCallback) {
        check_execution_time!(20000);
        instrument!();

        trace!(
            "...post on_generate_slot event for further processing, {}",
            self.session_id
        );

        let listener = self.session_listener.clone();

        self.collates_counter.total_increment();
        self.collates_expire_counter.total_increment();

        post_callback_closure(&self.callbacks_task_queue, move || {
            check_execution_time!(20000);

            if let Some(listener) = listener.upgrade() {
                trace!("SessionProcessor::notify_generate_slot: on_generate_slot start");

                listener.on_generate_slot(round, callback);

                trace!("SessionProcessor::notify_generate_slot: on_generate_slot finish");
            }
        });
    }

    fn notify_block_committed(
        &mut self,
        round: u32,
        source: &PublicKey,
        root_hash: &BlockHash,
        file_hash: &BlockHash,
        data: &BlockPayloadPtr,
        signatures: Vec<(PublicKeyHash, BlockPayloadPtr)>,
        approve_signatures: Vec<(PublicKeyHash, BlockPayloadPtr)>,
    ) {
        check_execution_time!(20000);
        instrument!();

        trace!("...post on_block_committed event for further processing");

        let listener = self.session_listener.clone();
        let source_clone = source.clone();
        let root_hash_clone = root_hash.clone();
        let file_hash_clone = file_hash.clone();
        let data_clone = data.clone();

        self.commits_counter.total_increment();
        self.commits_counter.success();

        post_callback_closure(&self.callbacks_task_queue, move || {
            check_execution_time!(20000);

            if let Some(listener) = listener.upgrade() {
                trace!("SessionProcessor::notify_block_committed: on_block_committed start");

                listener.on_block_committed(
                    round,
                    source_clone,
                    root_hash_clone,
                    file_hash_clone,
                    data_clone,
                    signatures,
                    approve_signatures,
                );

                trace!("SessionProcessor::notify_block_committed: on_block_committed finish");
            }
        });
    }

    fn notify_block_skipped(&mut self, round: u32) {
        check_execution_time!(20000);
        instrument!();

        trace!("...post on_block_skipped event for further processing");

        let listener = self.session_listener.clone();

        self.commits_counter.total_increment();
        self.commits_counter.failure();

        post_callback_closure(&self.callbacks_task_queue, move || {
            check_execution_time!(20000);

            if let Some(listener) = listener.upgrade() {
                trace!("SessionProcessor::notify_block_skipped: on_block_skipped start");

                listener.on_block_skipped(round);

                trace!("SessionProcessor::notify_block_skipped: on_block_skipped finish");
            }
        });
    }

    fn notify_get_approved_candidate(
        &mut self,
        source: &PublicKey,
        root_hash: &BlockHash,
        file_hash: &BlockHash,
        collated_data_hash: &BlockHash,
        callback: ValidatorBlockCandidateCallback,
    ) {
        check_execution_time!(20000);
        instrument!();

        trace!("...post get_approved_candidate event for further processing");

        let listener = self.session_listener.clone();
        let source_clone = source.clone();
        let root_hash_clone = root_hash.clone();
        let file_hash_clone = file_hash.clone();
        let collated_data_hash_clone = collated_data_hash.clone();

        post_callback_closure(&self.callbacks_task_queue, move || {
            check_execution_time!(20000);

            if let Some(listener) = listener.upgrade() {
                trace!(
                    "SessionProcessor::notify_get_approved_candidate: get_approved_candidate start"
                );

                listener.get_approved_candidate(
                    source_clone,
                    root_hash_clone,
                    file_hash_clone,
                    collated_data_hash_clone,
                    callback,
                );

                trace!("SessionProcessor::notify_get_approved_candidate: get_approved_candidate finish");
            }
        });
    }

    /*
        Creation
    */

    pub(crate) fn create(
        options: SessionOptions,
        session_id: SessionId,
        ids: Vec<SessionNode>,
        local_key: PrivateKey,
        listener: SessionListenerPtr,
        catchain: CatchainPtr,
        completion_task_queue: TaskQueuePtr,
        callbacks_task_queue: CallbackTaskQueuePtr,
        session_creation_time: std::time::SystemTime,
        metrics: Option<Arc<metrics_runtime::Receiver>>,
    ) -> SessionProcessorPtr {
        //dump session params for further log replaying

        if log_enabled!(log::Level::Debug) {
            #[cfg(feature = "export_key")] 
            let exp_pvt_key_dump = hex::encode(local_key.export_key().unwrap());
            #[cfg(not(feature = "export_key"))] 
            let exp_pvt_key_dump = "<SECRET>".to_string();

            debug!(
                "Create validator session {} for local ID {} and key {} (timestamp={})",
                session_id.to_hex_string(),
                &hex::encode(local_key.id().data()),
                exp_pvt_key_dump,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis()
            );

            for node in &ids {
                debug!("Validator session {} node: weight={}, public_key={}, adnl_id={} (timestamp={})", session_id.to_hex_string(), node.weight,
                &hex::encode(&catchain::serialize_tl_boxed_object!(&node.public_key.into_public_key_tl().unwrap()).as_ref()),
                &hex::encode(node.adnl_id.data()),
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("Time went backwards").as_millis());
            }
        }

        //initialize slashing statistics

        let mut slashing_stat = SlashingValidatorStat::default();

        for node in &ids {
            slashing_stat.validators_stat.insert(
                node.public_key.id().clone(),
                slashing::Node::new(&node.public_key),
            );
        }

        //create child objects

        let local_id = local_key.id().clone();
        let mut description = SessionDescriptionImpl::new(&options, &ids, &local_id, metrics);

        //initialize metrics

        let metrics_receiver = description.get_metrics_receiver();

        let collates_counter =
            ResultStatusCounter::new(&metrics_receiver, &"collate_requests".to_owned());
        let collates_expire_counter =
            ResultStatusCounter::new(&metrics_receiver, &"collate_requests_expire".to_owned());
        let validates_counter =
            ResultStatusCounter::new(&metrics_receiver, &"validate_requests".to_owned());
        let commits_counter =
            ResultStatusCounter::new(&metrics_receiver, &"commit_requests".to_owned());
        let rldp_queries_counter =
            ResultStatusCounter::new(&metrics_receiver, &"rldp_queries".to_owned());
        let preprocess_block_counter = metrics_receiver.sink().counter("preprocess_block_calls");
        let process_blocks_counter = metrics_receiver.sink().counter("process_blocks_calls");
        let request_new_block_counter = metrics_receiver.sink().counter("request_new_block_calls");
        let check_all_counter = metrics_receiver.sink().counter("check_all_calls");
        let preprocess_block_latency_histogram = metrics_receiver
            .sink()
            .histogram("time:preprocess_block_latency");
        let process_blocks_latency_histogram = metrics_receiver
            .sink()
            .histogram("time:process_blocks_latency");
        let first_candidate_received_latency_histogram = metrics_receiver
            .sink()
            .histogram("time:round_stage1_received_latency");
        let first_candidate_approved_latency_histogram = metrics_receiver
            .sink()
            .histogram("time:round_stage2_approved_latency");
        let first_candidate_voted_latency_histogram = metrics_receiver
            .sink()
            .histogram("time:round_stage3_voted_latency");
        let first_candidate_precommitted_latency_histogram = metrics_receiver
            .sink()
            .histogram("time:round_stage4_precommitted_latency");
        let round_duration_histogram = metrics_receiver
            .sink()
            .histogram("time:round_stage5_committed_latency");
        let block_candidate_broadcast_validation_latency_histogram = metrics_receiver
            .sink()
            .histogram("time:block_candidate_broadcast_validation_latency");
        let validation_latency_histogram =
            metrics_receiver.sink().histogram("time:validation_latency");
        let collation_latency_histogram =
            metrics_receiver.sink().histogram("time:collation_latency");
        let active_weight_gauge = metrics_receiver.sink().gauge("active_weight");
        let total_weight_gauge = metrics_receiver.sink().gauge("total_weight");
        let cutoff_weight_gauge = metrics_receiver.sink().gauge("cutoff_weight");

        total_weight_gauge.record(description.get_total_weight() as i64);
        cutoff_weight_gauge.record(description.get_cutoff_weight() as i64);

        //initialize state

        let now = SystemTime::now();
        let state_merge_cache = FifoCache::new("state_merge".to_owned(), metrics_receiver);
        let block_update_cache = FifoCache::new("block_update".to_owned(), metrics_receiver);
        let initial_state = SessionFactory::create_state(&mut description);
        let initial_state = initial_state.move_to_persistent(&mut description);

        let body = Self {
            session_id: session_id,
            local_key: local_key,
            completion_task_queue: completion_task_queue,
            callbacks_task_queue: callbacks_task_queue,
            session_listener: listener,
            catchain: catchain,
            next_completion_handler_available_index: 1,
            completion_handlers: HashMap::new(),
            completion_handlers_check_last_time: SystemTime::now(),
            block_to_state_map: Vec::with_capacity(STATES_RESERVED_COUNT),
            state_merge_cache: state_merge_cache,
            block_update_cache: block_update_cache,
            catchain_started: false,
            description: description,
            real_state: initial_state.clone(),
            virtual_state: initial_state.clone(),
            current_round: 0,
            next_awake_time: now,
            round_started_at: now,
            round_debug_at: now,
            session_processor_creation_time: now,
            session_creation_time: session_creation_time,
            requested_new_block_now: false,
            requested_new_block: false,
            pending_generate: false,
            generated: false,
            sent_generated: false,
            generated_block: BlockId::default(),
            blocks: HashMap::new(),
            pending_approve: HashSet::new(),
            pending_reject: HashMap::new(),
            rejected: HashSet::new(),
            approved: HashMap::new(),
            active_requests: HashSet::new(),
            pending_sign: false,
            signed: false,
            signed_block: BlockId::default(),
            signature: BlockSignature::default(),
            log_replay_report_current_time: std::time::UNIX_EPOCH,
            collates_counter: collates_counter,
            collates_expire_counter: collates_expire_counter,
            validates_counter: validates_counter,
            commits_counter: commits_counter,
            rldp_queries_counter: rldp_queries_counter,
            preprocess_block_counter: preprocess_block_counter,
            process_blocks_counter: process_blocks_counter,
            request_new_block_counter: request_new_block_counter,
            preprocess_block_latency_histogram,
            process_blocks_latency_histogram,
            round_duration_histogram,
            first_candidate_received_latency_histogram,
            first_candidate_received: false,
            first_candidate_approved_latency_histogram,
            first_candidate_approved: false,
            first_candidate_voted_latency_histogram,
            first_candidate_voted: false,
            first_candidate_precommitted_latency_histogram,
            first_candidate_precommitted: false,
            block_candidate_broadcast_validation_latency_histogram,
            validation_latency_histogram,
            collation_latency_histogram,
            check_all_counter: check_all_counter,
            last_preprocess_block_warn_dump_time: now,
            last_process_blocks_warn_dump_time: now,
            slashing_stat: slashing_stat,
            last_preprocess_block_time: vec![SystemTime::UNIX_EPOCH; ids.len()],
            active_weight_gauge,
        };

        if DEBUG_EVENTS_LOG {
            info!("EVENTS LOG: New round {}", body.current_round);
        }

        //check state

        let result = Rc::new(RefCell::new(body));

        result.borrow_mut().check_all();

        result
    }
}
