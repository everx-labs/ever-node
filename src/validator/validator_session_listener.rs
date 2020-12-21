use std::{fmt, time::{*, SystemTime}, sync::*};
use tokio::{time::timeout, runtime::Runtime};

use validator_session::*;
use crate::validator::validator_group::{ValidatorGroup, ValidatorGroupStatus};
use crossbeam_channel::{Sender, Receiver, unbounded};

pub struct OnBlockCommitted {
    round: u32,
    source: PublicKey,
    root_hash: BlockHash,
    file_hash: BlockHash,
    data: BlockPayloadPtr,
    signatures: Vec<(PublicKeyHash, BlockPayloadPtr)>,
    approve_signatures: Vec<(PublicKeyHash, BlockPayloadPtr)>
}

pub enum ValidationAction {
    OnGenerateSlot {
        round: u32, 
        callback: ValidatorBlockCandidateCallback
    },
    OnCandidate { 
        round: u32, 
        source: PublicKey,
        root_hash: BlockHash,
        data: BlockPayloadPtr,
        collated_data: BlockPayloadPtr,
        callback: ValidatorBlockCandidateDecisionCallback,
    },
    OnBlockCommitted (OnBlockCommitted),
    OnBlockSkipped {
        round: u32
    },
    OnGetApprovedCandidate {
        source: PublicKey,
        root_hash: BlockHash,
        file_hash: BlockHash,
        collated_data_hash: BlockHash,
        callback: ValidatorBlockCandidateCallback
    }
}

impl fmt::Display for OnBlockCommitted {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OnBlockCommitted round: {}", self.round)
    }
}

impl fmt::Display for ValidationAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ValidationAction::OnGenerateSlot {round, ..} => write!(f, "OnGenerateSlot round: {}", round),

            ValidationAction::OnCandidate {round, ..} =>
                write!(f, "OnCandidate round: {}", round),

            ValidationAction::OnBlockCommitted(OnBlockCommitted {round, ..}) =>
                write!(f, "OnBlockCommitted round: {}", round),

            ValidationAction::OnBlockSkipped {round} => write!(f, "OnBlockSkipped round: {}", round),

            ValidationAction::OnGetApprovedCandidate {..} => write!(f, "OnGetApprovedCandidate")
        }
    }
}

impl ValidationAction {
    fn get_round(&self) -> Option<u32> {
        match *self {
            ValidationAction::OnGenerateSlot {round, ..} => Some(round),
            ValidationAction::OnCandidate {round, ..} => Some(round),
            ValidationAction::OnBlockCommitted(OnBlockCommitted {round, ..}) => Some(round),
            ValidationAction::OnBlockSkipped {round} => Some(round),
            ValidationAction::OnGetApprovedCandidate {..} => None
        }
    }
}

pub struct ValidatorSessionListener {
    queue: Sender<ValidationAction>
}

impl ValidatorSessionListener {
    pub fn info_round(&self, round: Option<u32>) -> String {
        return format!("ValidatorSessionListener; round = {:?}", round);
    }

    fn do_send_general(&self, round: Option<u32>, action: ValidationAction) {
        if let Err(error) = self.queue.send(action) {
            log::error!(target: "validator", "Cannot send validator action: `{}`, {}",
                error,
                self.info_round(round));
        }
    }

    pub fn create() -> (Self, Receiver<ValidationAction>) {
        let (sender, receiver) = unbounded();
        return (ValidatorSessionListener { queue: sender }, receiver);
    }
}

impl validator_session::SessionListener for ValidatorSessionListener {
    /// New block candidate appears -- validate it
    fn on_candidate(
        &self,
        round: u32,
        source: PublicKey,
        root_hash: BlockHash,
        data: BlockPayloadPtr,
        collated_data: BlockPayloadPtr,
        callback: ValidatorBlockCandidateDecisionCallback,
    ) {
        log::info!(target: "validator", "SessionListener::on_candidate: new candidate from source {} with hash {} appeared, {}",
            source.id(), root_hash.to_hex_string(), self.info_round(Some(round)));
        self.do_send_general (
            Some(round),
            ValidationAction::OnCandidate {round, source, root_hash, data, collated_data, callback});
    }

    /// New block should be collated -- generate_block_candidate
    fn on_generate_slot(&self, round: u32, callback: ValidatorBlockCandidateCallback) {
        log::info!(target: "validator", 
            "SessionListener::on_generate_slot: collator request, {}",
            self.info_round(Some(round))
        );
        self.do_send_general (Some(round), ValidationAction::OnGenerateSlot {round, callback});
    }

    /// New block is committed - apply it and write to the database
    fn on_block_committed(
        &self,
        round: u32,
        source: PublicKey,
        root_hash: BlockHash,
        file_hash: BlockHash,
        data: BlockPayloadPtr,
        signatures: Vec<(PublicKeyHash, BlockPayloadPtr)>,
        approve_signatures: Vec<(PublicKeyHash, BlockPayloadPtr)>,
    ) {
        log::info!(target: "validator", "SessionListener::on_block_committed: new block from source {} with hash {:?} has been committed, {}",
            source.id(), root_hash, self.info_round(Some(round)));
        self.do_send_general (
            Some(round),
            ValidationAction::OnBlockCommitted(OnBlockCommitted {
                round, source, root_hash, file_hash, data, signatures, approve_signatures
            })
        );
    }

    /// Block generation is skipped for the current round
    fn on_block_skipped(&self, round: u32) {
        log::info!(target: "validator", "SessionListener::on_block_skipped, {}", self.info_round(Some(round)));
        self.do_send_general (Some(round), ValidationAction::OnBlockSkipped {round});
    }

    /// Ask validator to read block candidate from the database
    fn get_approved_candidate(
        &self,
        source: PublicKey,
        root_hash: BlockHash,
        file_hash: BlockHash,
        collated_data_hash: BlockHash,
        callback: ValidatorBlockCandidateCallback,
    ) {
        log::info!(target: "validator", "SessionListener::on_get_approved_candidate, {}", self.info_round(None));
        self.do_send_general (
            None,
            ValidationAction::OnGetApprovedCandidate {
                source, root_hash, file_hash, collated_data_hash, callback
            });
    }
}

impl validator_session::CatchainReplayListener for ValidatorSessionListener {
    fn replay_started(&self) {
        log::info!(target: "validator", "CatchainReplayListener: started");
    }

    fn replay_finished(&self) {
        log::info!(target: "validator", "CatchainReplayListener: finished");

        //self.data.lock().unwrap().replay_finished = true;
        unimplemented!("Replay not available");
    }
}

async fn process_validation_action (action: ValidationAction, g: Arc<ValidatorGroup>) {
    let action_str = format!("{}", action);
    log::info!(target: "validator", "Processing action: {}, {}", action_str, g.info().await);
    match action {
        ValidationAction::OnGenerateSlot {round, callback} => g.on_generate_slot (round, callback).await,

        ValidationAction::OnCandidate {round, source, root_hash, data, collated_data, callback} =>
            g.on_candidate (round, source, root_hash, data, collated_data, callback).await,

        ValidationAction::OnBlockCommitted(OnBlockCommitted{ round, source, root_hash, file_hash, data, signatures, approve_signatures }) =>
            //panic!("ValidatorAction::OnBlockCommitted must be processed in a separate thread!");
            g.on_block_committed (round, source, root_hash, file_hash, data, signatures, approve_signatures).await,

        ValidationAction::OnBlockSkipped { round } => g.on_block_skipped(round).await,

        ValidationAction::OnGetApprovedCandidate { source, root_hash, file_hash, collated_data_hash, callback} =>
            g.on_get_approved_candidate(source, root_hash, file_hash, collated_data_hash, callback).await
    }
}

pub async fn process_validation_queue(
        queue: Arc<Receiver<ValidationAction>>,
        g: Arc<ValidatorGroup>,
        rt: Arc<Runtime>
) {
    let mut cur_round = 0;

    while g.clone().get_status().await != ValidatorGroupStatus::Stopped {
        let g_clone = g.clone();
        let g_info = g_clone.info().await;
        match queue.recv_timeout(Duration::from_secs(10)) {
            Err(_r) => log::info!(target: "validator", "Session {}: queue empty", g_info),
            Ok(action) => {
                let action_str = format!("{}", action);

                if let Some(new_round) = action.get_round() {
                    if new_round < cur_round {
                        log::warn!(target: "validator", "Round {} is over, current round is {}; skipping action", new_round, cur_round);
                        continue
                    }
                    cur_round = new_round;
                }

                log::info!(target: "validator", "Starting validation action: {}, {}", action_str, g_info);

                let start_time = SystemTime::now();
                let mut join_handle = rt.spawn(async move {
                    process_validation_action (action, g_clone).await;
                });

                loop {
                    match timeout(Duration::from_secs(3), &mut join_handle).await {
                        Ok(res) => {
                            let res_txt = match res {
                                Ok(_) => "Ok".to_string(),
                                Err(r) => format!("Error: {}", r)
                            };
                            log::info!(target: "validator", "Validation action {}, {} stopped: `{}`", action_str, g_info, res_txt);
                            break
                        },
                        Err(msg) => log::warn!(target: "validator", "Validation action {}, {} takes {:#?}, too long: `{}`",
                            action_str, g_info, start_time.elapsed().unwrap(), msg
                        )
                    }

                    if g.clone().get_status().await == ValidatorGroupStatus::Stopped {
                        log::error!(target: "validator",
                            "Session processing cancelled, but validation action took {:#?} and not finished {}, {}",
                            start_time.elapsed().unwrap(), action_str, g_info
                        );
                        break;
                    }
                }
            }
        }
    }
    log::info!(target: "validator", "Exiting from validation queue processing: {}", g.info().await);
}
