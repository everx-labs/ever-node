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

use catchain::CatchainReplayListener;
use colored::Colorize;
use rand::Rng;
use std::{
    fs::File, io::{LineWriter, Write}, path::Path,
    sync::{Arc, Mutex, atomic::{AtomicBool, AtomicI32, Ordering}}, time::Duration
};
use ton_types::{Ed25519KeyOption, UInt256};
use validator_session::*;
use ton_types::KeyOption;

const DYNAMIC_COLLATION_TIMEOUT_MODE: DynamicCollationTimeoutMode = DynamicCollationTimeoutMode::DoubleTimeoutAfterSkippedRound;
const NODES_COUNT: usize = 7;
const MAX_WAIT_ROUND: u32 = 12; //max round for waiting in the test
const MIN_COMMITTED_BLOCKS: u32 = MAX_WAIT_ROUND / 3; //min number of committed blocks for the test
const COLLATION_TIME: std::time::Duration = std::time::Duration::from_millis(11000); //collation time for the test

struct DummySessionListener {
  public_key: PublicKey,
  batch_processed: Arc<AtomicBool>,
  committed_blocks_count: Arc<AtomicI32>,
}

impl SessionListener for DummySessionListener {
    fn on_candidate(
        &self,
        round: u32,
        source: PublicKey,
        root_hash: BlockHash,
        _data: BlockPayloadPtr,
        _collated_data: BlockPayloadPtr,
        callback: ValidatorBlockCandidateDecisionCallback,
    ) {
        log::info!(
           "SessionListener::on_candidate: new candidate for \
           round {} from source {} with hash {:?} appeared", 
           round, source.id(), root_hash
        );

        callback(Ok(std::time::SystemTime::now()))
    }

    fn on_generate_slot(&self, round: u32, callback: ValidatorBlockCandidateCallback) {
        log::info!(
            "SessionListener::on_generate_slot: collator request for round {}",
            round
        );

        let mut rng = rand::thread_rng();
        let mut hash_bytes = [0; 32];

        for byte in &mut hash_bytes {
            *byte = rng.gen_range(0..255);
        }

        let hash = UInt256::with_array(hash_bytes);

        let candidate = ValidatorBlockCandidate {
            public_key: self.public_key.clone(),
            id: ValidatorBlockId {
                root_hash: hash.clone(),
                file_hash: hash.clone(),
            },
            collated_file_hash: hash.clone(),
            data: catchain::CatchainFactory::create_empty_block_payload(),
            collated_data: catchain::CatchainFactory::create_empty_block_payload(),
        };

        std::thread::sleep(COLLATION_TIME);

        callback(Ok(Arc::new(candidate)));
    }

    fn on_block_committed(
        &self,
        round: u32,
        source: PublicKey,
        root_hash: BlockHash,
        _file_hash: BlockHash,
        _data: BlockPayloadPtr,
        _signatures: Vec<(PublicKeyHash, BlockPayloadPtr)>,
        _approve_signatures: Vec<(PublicKeyHash, BlockPayloadPtr)>,
    ) {
        log::info!(
            "SessionListener::on_block_committed: 
            new block from source {} with hash {:?} has been committed in round {}",
            source.id(), root_hash, round
        );
        if round >= MAX_WAIT_ROUND {
            self.batch_processed.store(true, Ordering::Release);
        }
        self.committed_blocks_count.fetch_add(1, Ordering::Relaxed);
    }

    fn on_block_skipped(&self, round: u32) {
        log::info!(
            "SessionListener::on_block_skipped: round {} has no committed block",
            round
        );
        if round >= MAX_WAIT_ROUND {
            self.batch_processed.store(true, Ordering::Release);
        }        
    }

    fn get_approved_candidate(
        &self,
        source: PublicKey,
        root_hash: BlockHash,
        _file_hash: BlockHash,
        _collated_data_hash: BlockHash,
        _callback: ValidatorBlockCandidateCallback,
    ) {
        log::info!(
            "SessionListener::get_approved_candidate: \
            approved candidate from source {:?} request for block hash {:?}",
            source, root_hash
        );
    }

    #[cfg(feature = "slashing")]
    fn on_slashing_statistics(&self, round: u32, stat: SlashingValidatorStat) {
        log::info!(
            "SessionListener::on_slashing_statistics: round #{:?}: {:?}",
            round, stat
        );
    }
}

impl CatchainReplayListener for DummySessionListener {
    fn replay_started(&self) {
        log::info!("CatchainReplayListener: started");
    }

    fn replay_finished(&self) {
        log::info!("CatchainReplayListener: finished");
    }
}

struct Session {
    _overlay_manager: CatchainOverlayManagerPtr,
    _local_key: Arc<dyn KeyOption>,
    _local_key_id: PublicKeyHash,
    local_idx: usize,
    _session_id: SessionId,
    _session: SessionPtr,
    listener: Arc<DummySessionListener>,
}

impl Session {
    fn new(local_idx: usize, ids: Vec<Arc<dyn KeyOption>>, db_path: String) -> Arc<Session> {
        assert!(local_idx < ids.len());

        let overlay_manager = catchain::CatchainFactory::create_inmemory_overlay_manager().expect("InMemoryCatchainFactory::create_inmemory_overlay_manager failed");

        let local_key = ids[local_idx as usize].clone();
        let local_key_id = local_key.id();
        let session_id: SessionId = UInt256::default();
        let listener = Arc::new(DummySessionListener {
            public_key: local_key.clone(),
            batch_processed: Arc::new(AtomicBool::new(false)),
            committed_blocks_count: Arc::new(AtomicI32::new(0)),
        });
        let ids = ids
            .iter()
            .map(|key| SessionNode {
                public_key: key.clone(),
                adnl_id: key.id().clone(),
                weight: 1,
            })
            .collect::<Vec<_>>();

        let mut session_opts = SessionOptions::default();
        session_opts.dynamic_collation_timeout_mode = DYNAMIC_COLLATION_TIMEOUT_MODE;

        let session = SessionFactory::create_session(
            &session_opts,
            &session_id,
            &ids,
            &local_key.clone(),
            db_path,
            format!("_idx{:02}", local_idx),
            false,
            overlay_manager.clone(),
            Arc::downgrade(&listener) as SessionListenerPtr,
        );

        Arc::new(
            Session {
                _overlay_manager: overlay_manager,
                local_idx,
                _local_key: local_key.clone(),
                _local_key_id: local_key_id.clone(),
                _session_id: session_id.clone(),
                _session: session.clone(),
                listener,
            }
        )
    }
}

//#[ignore]
#[test]
fn test_dynamic_collation_timeout() {
    //init logger

    let datetime: chrono::DateTime<chrono::offset::Utc> =
        std::time::SystemTime::now().clone().into();
    let out_log_file_name = format!(
        "debug-{}.log",
        datetime.format("%Y-%m-%d-%H.%M.%S").to_string()
    );
    let logs_path = Path::new("..").join("target").join("logs");
    std::fs::create_dir_all(logs_path.as_path())
        .expect("unable to create output log path");
    let file = File::create(logs_path.join(out_log_file_name))
        .expect("unable to create output log file");
    let file = Arc::new(Mutex::new(LineWriter::new(file)));

    let main_thread_id = std::thread::current().id();

    env_logger::Builder::new()
        .format(move |buf, record| {
            let message = format!("{}", record.args());
            let level = format!("{}", record.level());
            let line = match record.line() {
                Some(line) => format!("({})", line),
                None => "".to_string(),
            };
            let source = format!("{}{}", record.target(), line);
            let thread_name = {
                let current_thread = std::thread::current();

                if current_thread.id() == main_thread_id {
                    "main".to_string()
                } else if let Some(name) = current_thread.name() {
                    name.to_string()
                } else {
                    let id = current_thread.id();
                    format!("#{:?}", id)
                        .replace("ThreadId(", "")
                        .replace(")", "")
                }
            };

            let mut file = file.lock().unwrap();
            let log_line = format!(
                "{} [{: <5}] - {: <5} - {: <45}| {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S.%f"),
                level,
                thread_name,
                source,
                message
            );

            file.write_all(&log_line.as_bytes())?;
            file.write_all(b"\n")?;

            let (message, level) = match record.level() {
                log::Level::Error => (message.red(), level.red()),
                log::Level::Warn => (message.yellow(), level.yellow()),
                log::Level::Trace => (message.dimmed(), level.dimmed()),
                log::Level::Info => {
                    if record.target() == module_path!() {
                        (message.bright_green().bold(), level.bright_green().bold())
                    } else {
                        (message.bright_white().bold(), level.bright_white().bold())
                    }
                }
                _ => (message.normal(), level.normal()),
            };

            let (message, level) = if thread_name == "VS2" {
                (message.bright_green().bold(), level.bright_green().bold())
            } else {
                (message, level)
            };

            match record.level() {
                log::Level::Trace | log::Level::Debug => Ok(()),
                _ => {
                    writeln!(
                        buf,
                        "{} [{: <5}] - {: <5} - {: <45}| {}",
                        chrono::Local::now().format("%Y-%m-%dT%H:%M:%S.%f"),
                        level,
                        thread_name,
                        source,
                        message
                    )?;

                    std::io::stdout().flush()
                }
            }
        })
        .filter_level(log::LevelFilter::Info)
        .init();

    //initialize sessions

    let rand_name: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(7)
        .map(char::from)
        .collect();
    let db_path = format!("../target/catchains/log_replay {}", rand_name);
    let session_keys = (0..NODES_COUNT)
        .map(|_| Ed25519KeyOption::generate().expect("private key has not been generated"))
        .collect::<Vec<_>>();

    let sessions = (0..NODES_COUNT).map(|i| Session::new(i, session_keys.clone(), db_path.clone())).collect::<Vec<_>>();

    loop {
        let mut batch_processed = true;

        for session in &sessions {
            if !session.listener.batch_processed.load(Ordering::Relaxed) {
                batch_processed = false;
                break;
            }
        }
        
        if batch_processed {
            break;
        }

        std::thread::sleep(Duration::from_millis(10));
    }

    for session in &sessions {
        let committed_blocks_count = session.listener.committed_blocks_count.load(Ordering::Relaxed);
        assert!(committed_blocks_count >= MIN_COMMITTED_BLOCKS as i32, "session {} has only {} committed blocks", session.local_idx, committed_blocks_count);
    }

    drop(sessions);
    std::thread::sleep(Duration::from_secs(3)); //only to check final log messages about InMemoryOverlayManager destruction
}
