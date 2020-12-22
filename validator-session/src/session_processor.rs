pub use super::*;

use super::session_description::SessionDescriptionImpl;
use super::task_queue::*;
use crate::ton_api::IntoBoxed;
use catchain::BlockPtr;
use catchain::CatchainListener;
use catchain::CatchainPtr;
use catchain::ExternalQueryResponseCallback;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use std::time::SystemTime;

/*
    Constants
*/

const DEBUG_IGNORE_PROPOSALS_PRIORITY: bool = false; //ignore proposals priority and generate block each round
const DEBUG_DUMP_BLOCKS: bool = false; //dump blocks with dependencies and actions for debugging
const DEBUG_REQUEST_NEW_BLOCKS_IMMEDIATELY: bool = true; //request new blocks immediately without waiting
const DEBUG_CHECK_ALL_BEFORE_ROUND_SWITCH: bool = false; //check updates before round switching

const STATES_RESERVED_COUNT: usize = 100000; //reserved states count for blocks
const ROUND_DEBUG_PERIOD: std::time::Duration = Duration::from_secs(15); //round debug time

/*
    Implementation details for SessionProcessor
*/

type CatchainOptions = catchain::Options;
type BlockCandidateTlPtr = Rc<::ton_api::ton::validator_session::Candidate>;
type BlockCandidateMap = Rc<RefCell<HashMap<BlockId, BlockCandidateTlPtr>>>;
type RoundBlockMap = HashMap<u32, BlockCandidateMap>;
type BlockApproveMap = HashMap<BlockId, (SystemTime, BlockPayload)>;
type BlockMap = HashMap<BlockId, BlockPayload>;
type BlockSet = HashSet<BlockId>;
type ArcCatchainListenerPtr = Arc<Mutex<dyn CatchainListener + Send>>;

pub(crate) struct SessionProcessorImpl {
    task_queue: TaskQueuePtr, //task queue for session callbacks
    callbacks_task_queue: CallbackTaskQueuePtr, //task queue for session callbacks
    session_id: SessionId,    //catchain session ID (incarnation)
    session_listener: SessionListenerPtr, //session listener
    catchain: CatchainPtr,    //catchain session
    _catchain_listener: ArcCatchainListenerPtr, //catchain session listener
    next_completion_handler_available_index: CompletionHandlerId, //index of next available complete handler
    completion_handlers: HashMap<CompletionHandlerId, Box<dyn Any>>, //complete handlers
    catchain_started: bool, //flag indicates that catchain has been started
    description: SessionDescriptionImpl, //session description
    block_to_state_map: Vec<Option<SessionStatePtr>>, //session states
    real_state: SessionStatePtr, //real state (TODO: comment)
    virtual_state: SessionStatePtr, //virtual state (TODO: comment)
    current_round: u32,     //current round sequence number
    requested_new_block: bool, //new block has been requested in catchain
    requested_new_block_now: bool, //new block has been requested in catchain to be generated immediately
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
}

/*
    Implementation for public SessionProcessor trait
*/

impl SessionProcessor for SessionProcessorImpl {
    /*
        Accessors
    */

    fn get_impl(&self) -> &dyn Any {
        self
    }

    fn get_mut_impl(&mut self) -> &mut dyn Any {
        self
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

        if self.next_awake_time > now && self.next_awake_time <= timestamp {
            return;
        }

        self.next_awake_time = timestamp;
    }

    fn get_next_awake_time(&self) -> std::time::SystemTime {
        self.next_awake_time.clone()
    }

    /*
        Consensus iteration checkers
    */

    fn check_all(&mut self) {
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

        //update next check_all() call timestamp

        self.set_next_awake_time(self.round_debug_at);
        self.set_next_awake_time(self.description.get_attempt_start_at(attempt_seqno + 1));
    }
}

/*
    Implementation of CatchainListener
*/

struct CatchainListenerImpl {
    task_queue: TaskQueuePtr, //task queue
}

impl CatchainListener for CatchainListenerImpl {
    fn preprocess_block(&mut self, block: BlockPtr) {
        self.post_closure(move |processor: &mut dyn SessionProcessor| {
            processor.preprocess_block(block);
        });
    }

    fn process_blocks(&mut self, blocks: Vec<BlockPtr>) {
        self.post_closure(move |processor: &mut dyn SessionProcessor| {
            processor.process_blocks(blocks);
        });
    }

    fn finished_processing(&mut self) {
        self.post_closure(move |processor: &mut dyn SessionProcessor| {
            processor.finished_processing();
        });
    }

    fn started(&mut self) {
        self.post_closure(move |processor: &mut dyn SessionProcessor| {
            processor.started();
        });
    }

    fn process_broadcast(&mut self, source_id: PublicKeyHash, data: BlockPayload) {
        self.post_closure(move |processor: &mut dyn SessionProcessor| {
            processor.process_broadcast(source_id, data);
        });
    }

    fn process_message(&mut self, source_id: PublicKeyHash, data: BlockPayload) {
        self.post_closure(move |processor: &mut dyn SessionProcessor| {
            processor.process_message(source_id, data);
        });
    }

    fn process_query(
        &mut self,
        source_id: PublicKeyHash,
        data: BlockPayload,
        callback: ExternalQueryResponseCallback,
    ) {
        self.post_closure(move |processor: &mut dyn SessionProcessor| {
            processor.process_query(source_id, data, callback);
        });
    }

    fn set_time(&mut self, time: std::time::SystemTime) {
        self.post_closure(move |processor: &mut dyn SessionProcessor| {
            processor.set_time(time);
        });
    }
}

impl CatchainListenerImpl {
    fn post_closure<F>(&mut self, task_fn: F)
    where
        F: FnOnce(&mut dyn SessionProcessor),
        F: Send + 'static,
    {
        self.task_queue.post_closure(Box::new(task_fn));
    }

    fn create(task_queue: TaskQueuePtr) -> ArcCatchainListenerPtr {
        Arc::new(Mutex::new(Self {
            task_queue: task_queue,
        }))
    }
}

impl CatchainListener for SessionProcessorImpl {
    /*
        Blocks processing management
    */

    fn preprocess_block(&mut self, block: BlockPtr) {
        let start_time = SystemTime::now();

        debug!("Preprocessing block {}", block);

        let payload_len = block.get_payload().len();
        let deps_len = block.get_deps().len();

        trace!(
            "...received block with payload: {} bytes, and {} deps",
            payload_len,
            deps_len
        );

        //merge state

        trace!("...prev block is {:?}", block.get_prev());

        let mut state = if let Some(prev) = block.get_prev() {
            self.get_state(&prev).clone()
        } else {
            trace!("...create initial state");

            SessionFactory::create_state(&mut self.description)
        };

        trace!("...merge state {:08x?} with dependencies", state.get_hash());

        let deps = block.get_deps();

        for dep_block in deps {
            let dep_state = self.get_state(dep_block).clone();
            let state_hash = state.get_hash();

            state = state.merge(&dep_state, &mut self.description);

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

        if block.get_payload().len() != 0 || deps.len() != 0 {
            trace!("...parsing incoming block update");

            //try to parse block update

            let block_update: Result<ton::BlockUpdate> =
                catchain::utils::deserialize_tl_boxed_object(&block.get_payload());
            let node_public_key_hash = self
                .description
                .get_source_public_key_hash(block.get_source_id() as u32)
                .clone();
            let node_source_id = block.get_source_id() as u32;

            match block_update.as_ref() {
                Ok(block_update) => {
                    let block_update = block_update.clone().only();

                    trace!("...BlockUpdate has been received: {:?}", block_update);

                    //apply actions to state

                    let attempt_id = self
                        .description
                        .get_attempt_sequence_number(block_update.ts as u64);

                    trace!("...attempt ID is {}", attempt_id);
                    trace!("...applying actions");

                    for msg in block_update.actions.iter() {
                        debug!(
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
                        );
                    }

                    //actualize state

                    state = state.make_all(&mut self.description, node_source_id, attempt_id);

                    //check hashes

                    trace!("...check hashes");

                    if state.get_hash() != block_update.state as u32 {
                        //TODO: convert to warning after debugging; enable apply_action below after fixes of hash computation
                        debug!("Node {} sent a block {:?} with hash mismatch: computed={:08x?}, received={:08x?}",
              node_public_key_hash, block.get_hash(), state.get_hash(), block_update.state as u32);

                        /*for msg in block_update.actions.iter() {
                            //TODO: convert to warning after debugging
                            debug!("...applied action: {:?}", msg);

                            state = state.apply_action(
                                &mut self.description,
                                node_source_id,
                                attempt_id,
                                msg,
                            );
                        }*/
                    }
                }
                Err(err) => {
                    warn!(
                        "Node {} sent a block {:?} which can't be parsed: {:?}",
                        node_public_key_hash,
                        block.get_hash(),
                        err
                    );

                    state = state.make_all(
                        &mut self.description,
                        node_source_id,
                        state.get_ts(node_source_id),
                    );
                }
            }
        }

        //update session states

        trace!(
            "...move state {:08x?} to persistent memory",
            state.get_hash()
        );

        state = state.move_to_persistent(&mut self.description);

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

        let virtual_state_hash = state.get_hash();

        self.virtual_state = self
            .virtual_state
            .merge(&state.clone(), &mut self.description);
        self.virtual_state = self.virtual_state.move_to_persistent(&mut self.description);

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

        self.debug_dump();

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
        let start_time = SystemTime::now();

        debug!("Processing blocks {:?}", blocks);

        //reset flags

        self.requested_new_block = false;
        self.requested_new_block_now = false;

        //merge real state

        trace!(
            "...merge block states to real state with hash {:08x?}",
            self.real_state.get_hash()
        );

        for block in &blocks {
            let real_state_hash = self.real_state.get_hash();
            let block_state = self.get_state(&block).clone();

            self.real_state = self.real_state.merge(&block_state, &mut self.description);

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

            let block_candidate = self
                .get_signed_block_for_round(self.current_round, &self.generated_block)
                .unwrap();
            let file_hash = catchain::utils::get_hash(&block_candidate.data());
            let collated_data_file_hash =
                catchain::utils::get_hash(&block_candidate.collated_data());
            let message = ton::message::SubmittedBlock {
                round: self.current_round as i32,
                root_hash: *block_candidate.root_hash(),
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
                        signature: block_pair.1.clone(),
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
                reason: rejection_reason.clone(),
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

            self.real_state =
                self.real_state
                    .apply_action(&mut self.description, local_idx, attempt, msg);
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

            self.real_state =
                self.real_state
                    .apply_action(&mut self.description, local_idx, attempt, &msg);

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
            );

            messages.push(msg.unwrap());

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

        trace!(
            "...notify catchain about new block {:?}",
            serialized_payload
        );

        self.catchain
            .lock()
            .unwrap()
            .processed_block(serialized_payload);

        //check if new round is appeared

        let round = self.real_state.get_current_round_sequence_number();

        trace!(
            "...round after changes applying is {} (current is {})",
            round,
            self.current_round
        );

        if round > self.current_round {
            self.new_round(round);
        }

        //merge changes from a real state to a virtual state (so they should be equal after such merging)

        trace!(
            "...merge changes from a real state {:08x?} to a virtual state {:08x?}",
            self.real_state.get_hash(),
            self.virtual_state.get_hash()
        );

        self.virtual_state = self
            .virtual_state
            .merge(&self.real_state, &mut self.description);
        self.virtual_state = self.virtual_state.move_to_persistent(&mut self.description);

        trace!("...new virtual_state: {:?}", &self.virtual_state);

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

    fn finished_processing(&mut self) {
        debug!("Finished catchain blocks processing");

        let virtual_state_hash = &self.virtual_state.get_hash();
        let real_state_hash = &self.real_state.get_hash();

        if virtual_state_hash != real_state_hash {
            warn!("SessionProcessor: virtual state and real state hashes mismatch; virtual_state={:08x?} real_state={:08x?}",
        virtual_state_hash, real_state_hash);
        }

        self.virtual_state = self.real_state.clone();

        self.check_all();
    }

    fn started(&mut self) {
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
            let task_queue = self.task_queue.clone();

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
              src : ::ton_api::ton::int256(*block_source_id.clone().data()),
              round : round as i32,
              root_hash : block_root_hash.clone().into(),
              data : candidate.data.0.clone().into(),
              collated_data : candidate.collated_data.0.clone().into(),
            }.into_boxed();
            let data = catchain::utils::serialize_tl_boxed_object!(&broadcast);

            post_closure(&task_queue, move |processor : &mut dyn SessionProcessor|
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
        Time synchronization for log replay
    */

    fn set_time(&mut self, time: std::time::SystemTime) {
        if let Ok(duration) = time.duration_since(self.log_replay_report_current_time) {
            const REPORT_TIMEOUT: Duration = Duration::from_millis(1000);

            if duration > REPORT_TIMEOUT {
                debug!(
                    "Set log replay time {}",
                    catchain::utils::time_to_string(&time)
                );
                self.log_replay_report_current_time = time;
            }
        }

        self.description.set_time(time);
    }

    /*
        Network messages processing
    */

    fn process_broadcast(&mut self, source_id: PublicKeyHash, data: BlockPayload) {
        let src_idx = self.description.get_source_index(&source_id);
        let candidate = catchain::utils::deserialize_tl_boxed_object::<ton::Candidate>(&data);
        let data_hash = catchain::utils::get_hash(&data.into());

        if let Err(err) = candidate {
            warn!(
                "Can't parse broadcast {:?} from node {}: {:?}",
                data_hash, source_id, err
            );
            return;
        }

        debug!(
            "Processing broadcast {:?} from node {} (src_idx={})",
            data_hash, source_id, src_idx
        );

        let candidate_wrapper = candidate.ok().unwrap();
        let candidate = candidate_wrapper.clone().only();

        //check if the candidate was sent from the node which generated block

        if &candidate.src.0 != source_id.data() {
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
            debug!(
                "Broadcast {:?} from source {:?} has invalid round {} (current round is {})",
                data_hash, source_id, block_round, self.current_round
            );
            return;
        }

        if self
            .get_signed_block_for_round(block_round, &block_id)
            .is_some()
        {
            debug!(
                "Duplicate broadcast {:?} from source {:?}",
                data_hash, source_id
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

        self.insert_signed_block_for_round(block_round, &block_id, Rc::new(candidate_wrapper));

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

    fn process_message(&mut self, source_id: PublicKeyHash, data: BlockPayload) {
        debug!(
            "SessionProcessor::process_message: received message from source {}: {:?}",
            source_id, data
        );
    }

    fn process_query(
        &mut self,
        _source_id: PublicKeyHash,
        _data: BlockPayload,
        _callback: ExternalQueryResponseCallback,
    ) {
        unimplemented!();
    }
}

/*
    Implementation for crate CompletionHandlerProcessor trait
*/

impl CompletionHandlerProcessor for SessionProcessorImpl {
    fn get_task_queue(&self) -> &TaskQueuePtr {
        &self.task_queue
    }

    fn add_completion_handler(&mut self, handler: CompletionHandler) -> CompletionHandlerId {
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
    ) -> Option<CompletionHandler> {
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

    fn debug_dump(&self) {
        let mut result = "".to_string();

        result = format!(
            "{}Session {} dump:\n",
            result,
            self.session_id.to_hex_string()
        );
        result = format!(
            "{}  - validators_count: {}\n",
            result,
            self.description.get_total_nodes()
        );
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

        info!("{}", result);

        debug!(
            "ValidatorSession {} metrics:",
            &self.session_id.to_hex_string()
        );

        catchain::utils::dump_metrics(
            &self.description.get_metrics_receiver(),
            &utils::dump_metric,
        );
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
            catchain::utils::deserialize_tl_boxed_object(&block.get_payload());
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

    fn get_local_key(&self) -> &PublicKey {
        self.description
            .get_source_public_key(self.description.get_self_idx())
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

    fn new_round(&mut self, round: u32) {
        //debug dump for states

        trace!(
            "...new round request for current round {} and round {}",
            self.current_round,
            round
        );

        self.debug_dump();

        if round != 0 {
            debug!(
                "...reset current round {}, because round {} is started",
                self.current_round, round
            );

            assert!(self.current_round < round);

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

            self.active_requests.clear();
        }

        //apply finished rounds to current state

        while self.current_round < round {
            debug!(
                "...apply current round {}, target round is {}",
                self.current_round, round
            );

            if DEBUG_CHECK_ALL_BEFORE_ROUND_SWITCH {
                debug!(
                    "...check session state before switching of current round {}, target round is {}",
                    self.current_round, round
                );

                self.check_all();
            }

            let signed_block = self
                .real_state
                .get_committed_block(&self.description, self.current_round);
            let signatures = self
                .real_state
                .get_committed_block_signatures(self.current_round);
            let approve_signatures = self
                .real_state
                .get_committed_block_approve_signatures(self.current_round);

            assert!(signatures.is_some());
            assert!(approve_signatures.is_some());

            let signatures_exporter = |desc: &dyn SessionDescription,
                                       signatures: &BlockCandidateSignatureVectorPtr|
             -> Vec<(PublicKeyHash, BlockPayload)> {
                let mut result: Vec<(PublicKeyHash, BlockPayload)> =
                    Vec::with_capacity(desc.get_total_nodes() as usize);

                for i in 0..desc.get_total_nodes() as usize {
                    if let Some(signature) = signatures.at(i) {
                        result.push((
                            self.description
                                .get_source_public_key_hash(i as u32)
                                .clone(),
                            signature.get_signature().clone(),
                        ));
                    }
                }

                result
            };

            let signatures = signatures_exporter(&self.description, &signatures.as_ref().unwrap());
            let approve_signatures =
                signatures_exporter(&self.description, &approve_signatures.as_ref().unwrap());

            assert!(signed_block.is_some()); //because round was finished we expect it has commit at the end, even with empty block

            let signed_block = signed_block.unwrap();

            if let Some(signed_block) = signed_block {
                //signed block was committed

                debug!(
                    "...block is signed for round {}; signatures={:?}, approve_signatures={:?}",
                    self.current_round, signatures, approve_signatures
                );

                let signed_tl_block = self
                    .get_signed_block_for_round(self.current_round, signed_block.get_id())
                    .clone();
                let validator_public_key = self
                    .description
                    .get_source_public_key(signed_block.get_source_index())
                    .clone();

                if let Some(signed_tl_block) = signed_tl_block {
                    //normal signed block

                    self.notify_block_committed(
                        self.current_round,
                        &validator_public_key,
                        &signed_block.get_root_hash(),
                        &signed_block.get_file_hash(),
                        signed_tl_block.data(),
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
                        &BlockPayload::default(),
                        signatures,
                        approve_signatures,
                    );
                }
            } else {
                //no block was committed

                debug!("...block is skipped for round {}", self.current_round);

                self.notify_block_skipped(self.current_round);
            }

            //remove current round block payloads because we have already processed it

            self.blocks.remove(&self.current_round);

            //increment round

            self.current_round += 1;
        }

        //update debug checking time points

        self.round_started_at = self.description.get_time();
        self.round_debug_at = self.round_started_at + ROUND_DEBUG_PERIOD;

        //check state

        self.check_all();
    }

    fn request_new_block(&mut self, now: bool) {
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
                let mut delta_secs = -1.0 / lambda
                    * f64::ln((self.description.generate_random_usize() % 999 + 1) as f64 * 0.001);

                if delta_secs > 0.5 {
                    delta_secs = 0.5;
                }

                block_generation_time += Duration::from_secs_f64(delta_secs);
            }
        }

        self.catchain
            .lock()
            .unwrap()
            .request_new_block(block_generation_time);
    }

    /*
        Attempts management
    */

    fn check_action(&mut self, attempt: u32) {
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
        data: BlockCandidateTlPtr,
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
    ) -> Option<BlockCandidateTlPtr> {
        if let Some(round_block_map) = self.blocks.get(&round) {
            if let Some(block) = round_block_map.borrow().get(&block_id) {
                return Some(block.clone());
            }
        }

        None
    }

    fn check_generate_slot(&mut self) {
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
        let task_queue = self.task_queue.clone();

        self.notify_generate_slot(self.current_round, Box::new(move |candidate| {
      match candidate
      {
        Ok(candidate) => {
          debug!("SessionProcessor::check_generate_slot: new block candidate has been generated {:?}", candidate);

          post_closure(&task_queue, move |processor : &mut dyn SessionProcessor| {
            get_mut_impl(processor).generated_block(round, candidate.id.root_hash.clone().into(), candidate.data.clone(), candidate.collated_data.clone());
          });
        },
        Err(_err) => {
          warn!("SessionProcessor::check_generate_slot: failed to generate block candidate");
        },
      }
    }));
    }

    fn generated_block(
        &mut self,
        round: u32,
        root_hash: BlockId,
        data: BlockPayload,
        collated_data: BlockPayload,
    ) {
        if round != self.current_round {
            //accept blocks only for current round

            return;
        }

        debug!("SessionProcessor::generated_block: candidate has been received for round={}, root_hash={:?}", round, root_hash);

        if data.0.len() > self.description.opts().max_block_size as usize
            || collated_data.0.len() > self.description.opts().max_collated_data_size as usize
        {
            error!("SessionProcessor::generated_block: generated candidate is too big. Dropping. size={}/{}", data.0.len(), collated_data.0.len());
            return;
        }

        //prepare data

        use ton_api::ton::validator_session::*;

        let file_hash = catchain::utils::get_hash(&data);
        let collated_data_file_hash = catchain::utils::get_hash(&collated_data);
        let candidate = Rc::new(Candidate::ValidatorSession_Candidate(Box::new(
            candidate::Candidate {
                src: ::ton_api::ton::int256(self.get_local_id().data().clone()),
                round: round as i32,
                root_hash: root_hash.clone().into(),
                data: data.0.into(),
                collated_data: collated_data.0.into(),
            },
        )));
        let serialized_block = catchain::utils::serialize_tl_boxed_object!(&*candidate);
        let block_id = self.description.candidate_id(
            self.get_local_idx(),
            &root_hash,
            &file_hash,
            &collated_data_file_hash,
        );

        //send broadcast to catchain about new block candidate

        self.catchain
            .lock()
            .unwrap()
            .send_broadcast(serialized_block);

        //save block and update state

        self.insert_signed_block_for_round(self.current_round, &block_id, candidate);

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
        //don't do anything until catchain is started

        if !self.catchain_started {
            return;
        }

        //choose blocks to approve from proposed candidates

        let to_approve = self
            .real_state
            .choose_blocks_to_approve(&self.description, self.get_local_idx());

        debug!("block to approve {:?}", &to_approve);

        for block in to_approve {
            self.try_approve_block(block);
        }
    }

    fn try_approve_block(&mut self, block: SentBlockPtr) {
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
                (SystemTime::UNIX_EPOCH, BlockPayload::default()),
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

        let tl_block_opt: Option<BlockCandidateTlPtr> =
            match self.get_signed_block_for_round(self.current_round, &block_id) {
                Some(tl_block) => Some(tl_block.clone()),
                None => None,
            };

        //if block was proposed in current round - validate it

        if let Some(tl_block) = tl_block_opt {
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
            let completion_handler =
                task_queue::create_completion_handler(self, move |result, processor| {
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
                });

            let source_public_key = self
                .description
                .get_source_public_key(block.get_source_index())
                .clone();

            self.notify_candidate(
                round,
                &source_public_key,
                &tl_block.root_hash().clone().into(),
                tl_block.data(),
                tl_block.collated_data(),
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
            let node_id = self
                .description
                .get_source_public_key_hash(approvers[node_index] as u32)
                .clone();
            let source_id = self
                .description
                .get_source_public_key_hash(block.get_source_index())
                .clone();

            self.active_requests.insert(block_id.clone());

            const DOWNLOAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

            let block_id_clone = block_id.clone();
            let node_id_clone = node_id.clone();
            let source_id_clone = source_id.clone();
            let round = self.current_round;

            self.get_broadcast_p2p(
                &node_id,
                block.get_file_hash(),
                block.get_collated_data_file_hash(),
                &source_id,
                self.current_round,
                block.get_root_hash(),
                SystemTime::now() + DOWNLOAD_TIMEOUT,
                move |result: Result<BlockPayload>, processor: &mut dyn SessionProcessor| {
                    let processor = get_mut_impl(processor);

                    if processor.current_round == round {
                        processor.active_requests.remove(&block_id_clone);
                    }

                    if let Err(err) = result {
                        warn!(
                            "Failed to get block candidate {:?} from node {}: {:?}",
                            block_id_clone, node_id_clone, err
                        );
                        return;
                    }

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
        _node_id: &PublicKeyHash,
        _file_hash: &BlockHash,
        _collated_data_file_hash: &BlockHash,
        _source: &PublicKeyHash,
        _round: u32,
        _root_hash: &BlockHash,
        _timeout: std::time::SystemTime,
        _complete_handler: F,
    ) where
        F: FnOnce(Result<BlockPayload>, &mut dyn SessionProcessor) + 'static,
    {
        warn!(
            "get_broadcast_p2p is not implemented yet at {}({})",
            file!(),
            line!()
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
        if round != self.current_round {
            return;
        }

        debug!(
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
        if round != self.current_round {
            return;
        }

        let reason = format!("{}", err);

        error!(
            "SessionProcessor::candidate_decision_fail: failed candidate {:?}, reason={:?}",
            hash, reason
        );

        self.pending_approve.remove(&hash);
        self.pending_reject
            .insert(hash.clone(), reason.as_bytes().to_vec().into());
        self.rejected.insert(hash);
    }

    fn candidate_approved_signed(
        &mut self,
        _round: u32,
        hash: BlockId,
        validity_start_time: SystemTime,
        signature: BlockSignature,
    ) {
        self.pending_approve.remove(&hash);
        self.approved
            .insert(hash.clone(), (validity_start_time, signature));

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

    fn notify_candidate(
        &mut self,
        round: u32,
        source: &PublicKey,
        root_hash: &BlockHash,
        data: &BlockPayload,
        collated_data: &BlockPayload,
        callback: ValidatorBlockCandidateDecisionCallback,
    ) {
        debug!(
            "SessionProcessor::notify_candidate: post on_candidate event for further processing"
        );

        let listener = self.session_listener.clone();
        let source_clone = source.clone();
        let root_hash_clone = root_hash.clone();
        let data_clone = data.clone();
        let collated_data_clone = collated_data.clone();

        post_callback_closure(&self.callbacks_task_queue, move || {
            if let Some(listener) = listener.upgrade() {
                debug!("SessionProcessor::notify_candidate: on_candidate start");

                if let Ok(mut listener) = listener.lock() {
                    listener.on_candidate(
                        round,
                        source_clone,
                        root_hash_clone,
                        data_clone,
                        collated_data_clone,
                        callback,
                    );
                }

                debug!("SessionProcessor::notify_candidate: on_candidate finish");
            }
        });
    }

    fn notify_generate_slot(&mut self, round: u32, callback: ValidatorBlockCandidateCallback) {
        trace!("...post on_generate_slot event for further processing");

        let listener = self.session_listener.clone();

        post_callback_closure(&self.callbacks_task_queue, move || {
            if let Some(listener) = listener.upgrade() {
                debug!("SessionProcessor::notify_generate_slot: on_generate_slot start");

                if let Ok(mut listener) = listener.lock() {
                    listener.on_generate_slot(round, callback);
                }

                debug!("SessionProcessor::notify_generate_slot: on_generate_slot finish");
            }
        });
    }

    fn notify_block_committed(
        &mut self,
        round: u32,
        source: &PublicKey,
        root_hash: &BlockHash,
        file_hash: &BlockHash,
        data: &BlockPayload,
        signatures: Vec<(PublicKeyHash, BlockPayload)>,
        approve_signatures: Vec<(PublicKeyHash, BlockPayload)>,
    ) {
        trace!("...post on_block_committed event for further processing");

        let listener = self.session_listener.clone();
        let source_clone = source.clone();
        let root_hash_clone = root_hash.clone();
        let file_hash_clone = file_hash.clone();
        let data_clone = data.clone();

        post_callback_closure(&self.callbacks_task_queue, move || {
            if let Some(listener) = listener.upgrade() {
                debug!("SessionProcessor::notify_block_committed: on_block_committed start");

                if let Ok(mut listener) = listener.lock() {
                    listener.on_block_committed(
                        round,
                        source_clone,
                        root_hash_clone,
                        file_hash_clone,
                        data_clone,
                        signatures,
                        approve_signatures,
                    );
                }

                debug!("SessionProcessor::notify_block_committed: on_block_committed finish");
            }
        });
    }

    fn notify_block_skipped(&mut self, round: u32) {
        trace!("...post on_block_skipped event for further processing");

        let listener = self.session_listener.clone();

        post_callback_closure(&self.callbacks_task_queue, move || {
            if let Some(listener) = listener.upgrade() {
                debug!("SessionProcessor::notify_block_skipped: on_block_skipped start");

                if let Ok(mut listener) = listener.lock() {
                    listener.on_block_skipped(round);
                }

                debug!("SessionProcessor::notify_block_skipped: on_block_skipped finish");
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
        trace!("...post get_approved_candidate event for further processing");

        let listener = self.session_listener.clone();
        let source_clone = source.clone();
        let root_hash_clone = root_hash.clone();
        let file_hash_clone = file_hash.clone();
        let collated_data_hash_clone = collated_data_hash.clone();

        post_callback_closure(&self.callbacks_task_queue, move || {
            if let Some(listener) = listener.upgrade() {
                debug!(
                    "SessionProcessor::notify_get_approved_candidate: get_approved_candidate start"
                );

                if let Ok(mut listener) = listener.lock() {
                    listener.get_approved_candidate(
                        source_clone,
                        root_hash_clone,
                        file_hash_clone,
                        collated_data_hash_clone,
                        callback,
                    );
                }

                debug!("SessionProcessor::notify_get_approved_candidate: get_approved_candidate finish");
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
        local_id: PublicKeyHash,
        db_root: &String,
        db_suffix: &String,
        allow_unsafe_self_blocks_resync: bool,
        overlay_creator: catchain::OverlayCreator,
        listener: SessionListenerPtr,
        task_queue: TaskQueuePtr,
        callbacks_task_queue: CallbackTaskQueuePtr,
    ) -> SessionProcessorPtr {
        //remove fraction part from all timeouts

        let mut options = options;

        options.catchain_idle_timeout =
            Duration::from_secs(options.catchain_idle_timeout.as_secs());
        options.round_attempt_duration =
            Duration::from_secs(options.round_attempt_duration.as_secs());
        options.next_candidate_delay = Duration::from_secs(options.next_candidate_delay.as_secs());

        //create child objects

        let catchain_options = CatchainOptions {
            idle_timeout: options.catchain_idle_timeout,
            max_deps: options.catchain_max_deps,
            skip_processed_blocks: options.catchain_skip_processed_blocks,
            debug_disable_db: false,
        };
        let mut description = SessionDescriptionImpl::new(&options, &ids, &local_id);
        let catchain_listener = CatchainListenerImpl::create(task_queue.clone());
        let catchain = catchain::CatchainFactory::create_catchain(
            &catchain_options,
            &session_id.clone(),
            &description.export_catchain_nodes(),
            &local_id,
            db_root,
            db_suffix,
            allow_unsafe_self_blocks_resync,
            overlay_creator,
            Arc::downgrade(&catchain_listener.clone()),
        );

        //initialize state

        let now = SystemTime::now();
        let initial_state = SessionFactory::create_state(&mut description);
        let initial_state = initial_state.move_to_persistent(&mut description);

        let body = Self {
            session_id: session_id,
            task_queue: task_queue,
            callbacks_task_queue: callbacks_task_queue,
            session_listener: listener,
            catchain: catchain,
            _catchain_listener: catchain_listener,
            next_completion_handler_available_index: 1,
            completion_handlers: HashMap::new(),
            block_to_state_map: Vec::with_capacity(STATES_RESERVED_COUNT),
            catchain_started: false,
            description: description,
            real_state: initial_state.clone(),
            virtual_state: initial_state.clone(),
            current_round: 0,
            next_awake_time: now,
            round_started_at: now,
            round_debug_at: now,
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
        };

        //check state

        let result = Rc::new(RefCell::new(body));

        result.borrow_mut().check_all();

        //add ID to RLDP

        warn!("SessionProcessor::create: RLDP initialization is skipped!");

        result
    }

    pub(crate) fn create_dummy(
        session_id: SessionId,
        ids: Vec<SessionNode>,
        local_id: PublicKeyHash,
        listener: SessionListenerPtr,
    ) -> SessionProcessorPtr {
        let task_queue = SessionFactory::create_task_queue();
        let callbacks_task_queue = SessionFactory::create_callback_task_queue();
        let options = SessionOptions::default();
        let db_root = "".to_string();
        let db_suffix = "".to_string();
        let allow_unsafe_self_blocks_resync = false;

        Self::create(
            options,
            session_id,
            ids,
            local_id,
            &db_root,
            &db_suffix,
            allow_unsafe_self_blocks_resync,
            Box::new(catchain::CatchainFactory::create_dummy_overlay),
            listener,
            task_queue,
            callbacks_task_queue,
        )
    }
}
