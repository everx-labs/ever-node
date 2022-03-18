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

use overlay::PrivateOverlayShortId;
use regex::Captures;
use regex::Regex;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use utils::*;

/*
    Constants
*/

const MAIN_LOOP_NAME: &str = "CCLR"; //catchain log replay short thread name

/*
    LogReplayOptions
*/

impl LogReplayOptions {
    pub fn with_db(db_path: String) -> Self {
        Self {
            log_file_name: "".to_string(),
            session_id: None,
            replay_without_delays: false,
            db_path,
            db_suffix: "".to_string(),
            allow_unsafe_self_blocks_resync: false,
        }
    }
}

impl fmt::Debug for LogReplayOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogReplayOptions")
            .field("log_file_name", &self.log_file_name)
            .finish()
    }
}

/*
    Utilities
*/

#[derive(Clone)]
struct SessionDesc {
    session_id: SessionId,           //catchain session ID
    node_ids: Vec<CatchainNode>,     //list of validators
    weights: Vec<ValidatorWeight>,   //list of validator weights
    local_id: Option<PublicKeyHash>, //local validator ID
    local_key: Option<PrivateKey>,   //private key
    initial_timestamp: SystemTime,   //initial timestamp for replaying
}

#[derive(Clone, Default)]
struct LogHeader {
    session_descs: Vec<SessionDesc>,          //catchain session in a log
    session_index: HashMap<SessionId, usize>, //session index in session descs array
    local_keys: HashMap<PublicKeyHash, PrivateKey>, //private keys for validator
}

impl LogHeader {
    fn get_session(&mut self, session_id: &SessionId) -> Option<&mut SessionDesc> {
        if let Some(index) = self.session_index.get(&session_id) {
            return Some(&mut self.session_descs[*index]);
        }

        None
    }

    fn add_session_id(&mut self, session_id: SessionId, timestamp: SystemTime) {
        if self.session_index.contains_key(&session_id) {
            return;
        }

        trace!("...use session ID {:?}", session_id);

        let session_desc = SessionDesc {
            session_id: session_id.clone(),
            node_ids: Vec::new(),
            weights: Vec::new(),
            local_id: None,
            local_key: None,
            initial_timestamp: timestamp,
        };

        self.session_descs.push(session_desc);
        self.session_index
            .insert(session_id, self.session_descs.len() - 1);
    }

    fn add_local_id(&mut self, session_id: SessionId, local_id: PublicKeyHash) {
        let session_desc = self.get_session(&session_id);

        if session_desc.is_none() {
            warn!("...unknown session ID {:?}", session_id);
            return;
        }

        let session_desc = session_desc.unwrap();
        let new_local_id = Some(local_id.clone());

        if session_desc.local_id == new_local_id {
            return;
        }

        trace!("...use local ID {} for session {:?}", local_id, session_id);

        session_desc.local_id = new_local_id;
    }

    fn add_private_key(&mut self, public_key_hash: PublicKeyHash, private_key: PrivateKey) {
        let local_keys = &mut self.local_keys;

        if local_keys.contains_key(&public_key_hash) {
            return;
        }

        local_keys.insert(public_key_hash, private_key.clone());

        trace!("...use local ID private key {}", private_key.id());
    }

    fn add_node(
        &mut self,
        session_id: SessionId,
        catchain_node: CatchainNode,
        weight: ValidatorWeight,
    ) {
        let session_desc = self.get_session(&session_id);

        if session_desc.is_none() {
            warn!("...unknown session ID {:?}", session_id);
            return;
        }

        let session_desc = session_desc.unwrap();

        for node in &session_desc.node_ids {
            if node.adnl_id == catchain_node.adnl_id {
                //debug!("LogReplay: skip adding with adnl_id={:?}", catchain_node.adnl_id);
                return;
            }
        }

        trace!(
            "...add node with ADNL ID {}, PublicKeyHash {} and weight {} for session {:?}",
            catchain_node.adnl_id,
            catchain_node.public_key.id(),
            weight,
            session_id
        );

        session_desc.node_ids.push(catchain_node);
        session_desc.weights.push(weight);
    }

    fn resolve_local_keys(&mut self) {
        for session_desc in &mut self.session_descs {
            let local_id = &session_desc.local_id;

            if local_id.is_none() {
                continue;
            }

            let local_id = local_id.as_ref().unwrap();

            if let Some(local_key) = self.local_keys.get(local_id) {
                session_desc.local_key = Some(local_key.clone());

                for node_desc in session_desc.node_ids.iter_mut() {
                    if &node_desc.public_key.id() == &local_id {
                        trace!("...resolve private key for {}", node_desc.public_key.id());
                        node_desc.public_key = local_key.clone();
                    }
                }
            }
        }
    }
}

type FormatParser = Box<dyn Fn(&Captures) -> Result<bool>>;

struct Format {
    regexp: Regex,        //regular expression for a log line
    parser: FormatParser, //parser of log line
}

impl Format {
    fn new<F>(regexp: &str, parser_fn: F) -> Result<Self>
    where
        F: Fn(&Captures) -> Result<bool>,
        F: 'static,
    {
        Ok(Self {
            regexp: Regex::new(regexp)?,
            parser: Box::new(parser_fn),
        })
    }
}

/*
    Log player overlay manager
*/

struct LogPlayerOverlayManager {
    node_ids: Vec<CatchainNode>,                //list of validators
    session_id: SessionId,                      //catchain session ID
    log_replay_options: LogReplayOptions,       //log replay options
    initial_timestamp: SystemTime,              //initial timestamp for replaying
    replay_listener: CatchainReplayListenerPtr, //replay listener
}

impl CatchainOverlayManager for LogPlayerOverlayManager {
    /// Create new overlay
    fn start_overlay(
        &self,
        local_id: &PublicKeyHash,
        overlay_short_id: &Arc<PrivateOverlayShortId>,
        nodes: &Vec<CatchainNode>,
        listener: CatchainOverlayListenerPtr,
        overlay_log_replay_listener: CatchainOverlayLogReplayListenerPtr,
    ) -> Result<CatchainOverlayPtr> {
        let should_stop_flag = Arc::new(AtomicBool::new(false));
        let is_stopped_flag = Arc::new(AtomicBool::new(false));
        let overlay = OverlayImpl::create_dummy_overlay(
            should_stop_flag.clone(),
            is_stopped_flag.clone(),
            local_id,
            overlay_short_id,
            nodes,
            listener,
        );
        let overlay_clone = overlay.clone();

        if let Some(replay_listener) = overlay_log_replay_listener.upgrade() {
            debug!(
                "LogPlayer: set initial replay time to {}",
                catchain::utils::time_to_string(&self.initial_timestamp)
            );
            replay_listener.on_time_changed(self.initial_timestamp);
        }

        //start processing thread

        debug!("LogPlayer: create processing thread");

        let log_replay_options = self.log_replay_options.clone();
        let session_id = self.session_id.clone();
        let node_ids = self.node_ids.clone();
        let replay_listener = self.replay_listener.clone();

        let _processing_thread = std::thread::Builder::new()
            .name(MAIN_LOOP_NAME.to_string())
            .spawn(move || {
                LogPlayerImpl::main_loop(
                    log_replay_options,
                    session_id,
                    node_ids,
                    should_stop_flag,
                    is_stopped_flag,
                    overlay_clone,
                    replay_listener,
                    overlay_log_replay_listener,
                );
            })?;

        let result: CatchainOverlayPtr = overlay;

        Ok(result)
    }

    /// Stop existing overlay
    fn stop_overlay(
        &self,
        _overlay_short_id: &Arc<PrivateOverlayShortId>,
        overlay: &CatchainOverlayPtr,
    ) {
        if let Some(overlay) = overlay.get_impl().downcast_ref::<OverlayImpl>() {
            overlay.stop();
        }
    }
}

impl LogPlayerOverlayManager {
    fn create(
        session_id: &SessionId,
        node_ids: &Vec<CatchainNode>,
        log_replay_options: &LogReplayOptions,
        initial_timestamp: &SystemTime,
        replay_listener: CatchainReplayListenerPtr,
    ) -> Self {
        Self {
            session_id: session_id.clone(),
            node_ids: node_ids.clone(),
            log_replay_options: log_replay_options.clone(),
            initial_timestamp: initial_timestamp.clone(),
            replay_listener: replay_listener,
        }
    }
}

/*
    Log player for replaying previously written logs
*/

pub(crate) struct LogPlayerImpl {
    session_id: SessionId,         //catchain session ID
    node_ids: Vec<CatchainNode>,   //list of validators
    weights: Vec<ValidatorWeight>, //list of validator weights
    local_id: PublicKeyHash,       //local validator ID
    local_key: PrivateKey,         //private key for local node
    initial_timestamp: SystemTime, //initial timestamp for replaying
    options: LogReplayOptions,     //log replay options
}

impl LogPlayer for LogPlayerImpl {
    fn get_session_id(&self) -> &SessionId {
        &self.session_id
    }

    fn get_local_id(&self) -> &PublicKeyHash {
        &self.local_id
    }

    fn get_local_key(&self) -> &PrivateKey {
        &self.local_key
    }

    fn get_nodes(&self) -> &Vec<CatchainNode> {
        &self.node_ids
    }

    fn get_weights(&self) -> &Vec<ValidatorWeight> {
        &self.weights
    }

    fn get_overlay_manager(
        &self,
        replay_listener: CatchainReplayListenerPtr,
    ) -> CatchainOverlayManagerPtr {
        Arc::new(LogPlayerOverlayManager::create(
            &self.session_id,
            &self.node_ids,
            &self.options,
            &self.initial_timestamp,
            replay_listener,
        ))
    }
}

impl LogPlayerImpl {
    /*
        Main Loop
    */

    fn main_loop(
        log_replay_options: LogReplayOptions,
        session_id: SessionId,
        node_ids: Vec<CatchainNode>,
        should_stop_flag: Arc<AtomicBool>,
        is_stopped_flag: Arc<AtomicBool>,
        overlay: Arc<OverlayImpl>,
        replay_listener: CatchainReplayListenerPtr,
        overlay_log_replay_listener: CatchainOverlayLogReplayListenerPtr,
    ) {
        let start_time = SystemTime::now();

        debug!(
            "LogReplay main loop is started with replay_options={:?}",
            log_replay_options
        );

        let listener = overlay.listener.clone();

        trace!("...replay log from '{}'", log_replay_options.log_file_name);

        if let Some(listener) = replay_listener.upgrade() {
            listener.replay_started();
        }

        if let Err(err) = Self::parse_body(
            &log_replay_options,
            &session_id,
            &node_ids,
            &should_stop_flag,
            listener,
            overlay_log_replay_listener,
        ) {
            error!("LogReplay: error: {:?}", err);
        }

        if let Some(listener) = replay_listener.upgrade() {
            listener.replay_finished();
        }

        debug!(
            "LogReplay main loop is finished in {:?}",
            start_time.elapsed()
        );

        is_stopped_flag.store(true, Ordering::Release);
    }

    /*
        Parser
    */

    fn parse(path: &String, formats: &[Format], stop_flag: &Arc<AtomicBool>) -> Result<()> {
        //open file

        let file = File::open(path)?;
        let reader = BufReader::new(file);

        for (index, line) in reader.lines().enumerate() {
            if stop_flag.load(Ordering::Relaxed) {
                trace!("...replay is stopped by request during log replaying");
                return Ok(());
            }

            let line_number = index + 1;

            if let Err(err) = line {
                warn!(
                    "LogReplay: parsing error (line #{}): {:?}",
                    line_number, err
                );
                continue;
            }

            let line = line.unwrap();

            for format in formats {
                if let Some(captures) = format.regexp.captures(&line) {
                    match (format.parser)(&captures) {
                        Err(err) => warn!(
                            "LogReplay: parsing error (line #{}): {:?}",
                            line_number, err
                        ),
                        Ok(need_continue) => {
                            if !need_continue {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn parse_body(
        replay_options: &LogReplayOptions,
        session_id: &SessionId,
        _node_ids: &Vec<CatchainNode>,
        stop_flag: &Arc<AtomicBool>,
        listener: CatchainOverlayListenerPtr,
        replay_listener: CatchainOverlayLogReplayListenerPtr,
    ) -> Result<()> {
        //enumerate lines for header

        struct Timestamp {
            log_start_timestamp: u64,
            replay_start_timestamp: std::time::SystemTime,
        }

        let start_timestamp = Rc::new(RefCell::new(Timestamp {
            log_start_timestamp: 0,
            replay_start_timestamp: std::time::SystemTime::now(),
        }));

        let stop_flag_clone = stop_flag.clone();
        let session_id = session_id.clone();
        let replay_without_delays = replay_options.replay_without_delays;

        let parse_data = Rc::new(move |message_type, captures: &Captures| {
            let data_size: u32 = captures.get(1).unwrap().as_str().parse().unwrap();
            let data = &captures.get(2).unwrap().as_str();
            let bytes = parse_hex(&data);
            let source_id = &captures.get(3).unwrap().as_str();
            let source_id = utils::parse_hex_as_public_key_hash(&source_id);
            let block_session_id = &captures.get(4).unwrap().as_str();
            let block_session_id = utils::parse_hex_as_session_id(&block_session_id);
            let timestamp: u64 = captures.get(5).unwrap().as_str().parse().unwrap();
            let mut start_timestamp = start_timestamp.borrow_mut();

            if block_session_id != session_id {
                //ignore unknown sessions
                return Ok(true);
            }

            if start_timestamp.log_start_timestamp == 0 {
                start_timestamp.log_start_timestamp = timestamp;
                start_timestamp.replay_start_timestamp = std::time::SystemTime::now();
            }

            let time_offset = timestamp - start_timestamp.log_start_timestamp;
            let event_time = start_timestamp.replay_start_timestamp
                + std::time::Duration::from_millis(time_offset);

            if replay_without_delays {
                if let Ok(timeout) = event_time.duration_since(SystemTime::now()) {
                    const DEBUG_PRINT_MIN_DELAY: std::time::Duration =
                        std::time::Duration::from_millis(1000);

                    if timeout > DEBUG_PRINT_MIN_DELAY {
                        debug!("Waiting for {:?} due to log timestamp", timeout);
                    }
                }

                loop {
                    if let Ok(_elapsed) = event_time.elapsed() {
                        break;
                    }

                    const SLEEP_PERIOD: std::time::Duration = std::time::Duration::from_millis(10);

                    let mut timeout = event_time.duration_since(SystemTime::now()).unwrap();

                    if timeout > SLEEP_PERIOD {
                        timeout = SLEEP_PERIOD
                    }

                    if stop_flag_clone.load(Ordering::Relaxed) {
                        trace!("...replay is stopped by request during log replaying");
                        return Ok(false);
                    }

                    std::thread::sleep(timeout);
                }
            }

            if stop_flag_clone.load(Ordering::Relaxed) {
                trace!("...replay is stopped by request during log replaying");
                return Ok(false);
            }

            let replay_time = std::time::UNIX_EPOCH + std::time::Duration::from_millis(timestamp);

            if let Some(replay_listener) = replay_listener.upgrade() {
                replay_listener.on_time_changed(replay_time);
            }

            assert!(bytes.len() >= data_size as usize);

            //debug!("LogReplay: received {} from source {} for overlay {}: {:?}", message_type, &source_id, block_session_id.to_hex_string(), data);

            if let Some(listener) = listener.upgrade() {
                match message_type {
                    "block" => listener.on_message(
                        source_id.clone(),
                        &CatchainFactory::create_block_payload(::ton_api::ton::bytes(bytes)),
                    ),
                    "broadcast" => listener.on_broadcast(
                        source_id.clone(),
                        &CatchainFactory::create_block_payload(::ton_api::ton::bytes(bytes)),
                    ),
                    _ => warn!("...unknown message type {}", message_type),
                }
            }

            Ok(true)
        });

        let parse_message_rust = parse_data.clone();
        let parse_broadcast_rust = parse_data.clone();
        let parse_message_cpp = parse_data.clone();
        let parse_broadcast_cpp = parse_data.clone();
        let formats = [
            Format::new(
                concat!(
                    r".*Receive message from overlay for source: size=(\d+), payload=([0-9a-fA-F]+), source=([0-9a-fA-F]+), session_id=([0-9a-fA-F]+), timestamp=(\d+).*"
                ),
                move |captures: &Captures| parse_message_rust("block", captures),
            )?,
            Format::new(
                concat!(
                    r".*Receive broadcast from overlay for source: size=(\d+), payload=([0-9a-fA-F]+), source=([0-9a-fA-F]+), session_id=([0-9a-fA-F]+), timestamp=(\d+).*"
                ),
                move |captures: &Captures| parse_broadcast_rust("broadcast", captures),
            )?,
            Format::new(
                concat!(
                    r"^CatChainReceivedBlockImpl::receive_block payload.size = (\d+) payload = ([0-9a-fA-F]+) source.size = \d+ source = ([0-9a-fA-F]+) session_id.size = \d+ session_id = ([0-9a-fA-F]+) block_source_id = \d+ height = \d+ timestamp = (\d+) .*$"
                ),
                move |captures: &Captures| parse_message_cpp("block", captures),
            )?,
            Format::new(
                concat!(
                    r"^CatChainReceivedBlockImpl::receive_broadcast payload.size = (\d+) payload = ([0-9a-fA-F]+) source.size = \d+ source = ([0-9a-fA-F]+) session_id.size = \d+ session_id = ([0-9a-fA-F]+) timestamp = (\d+) .*$"
                ),
                move |captures: &Captures| parse_broadcast_cpp("broadcast", captures),
            )?,
        ];

        Self::parse(&replay_options.log_file_name, &formats, stop_flag)?;

        Ok(())
    }

    fn parse_header(
        path: &String,
        stop_flag: &Arc<AtomicBool>,
        terminate_on_body: bool,
    ) -> Result<LogHeader> {
        let log_header: Rc<RefCell<LogHeader>> = Rc::new(RefCell::new(LogHeader::default()));

        //make Rust happy and clone environment for closures below

        let log_header_clone = log_header.clone();
        let add_session_id = move |session_id: SessionId, timestamp: SystemTime| {
            log_header_clone
                .borrow_mut()
                .add_session_id(session_id, timestamp);
        };
        let log_header_clone = log_header.clone();
        let add_local_id = move |session_id: SessionId, local_id: PublicKeyHash| {
            log_header_clone
                .borrow_mut()
                .add_local_id(session_id, local_id);
        };
        let log_header_clone = log_header.clone();
        let add_private_key = move |public_key_hash: PublicKeyHash, private_key: PrivateKey| {
            log_header_clone
                .borrow_mut()
                .add_private_key(public_key_hash, private_key);
        };
        let log_header_clone = log_header.clone();
        let add_node = move |session_id: SessionId, catchain_node: CatchainNode, weight| {
            log_header_clone
                .borrow_mut()
                .add_node(session_id, catchain_node, weight);
        };
        let add_session_id_clone = add_session_id.clone();
        let add_local_id_clone = add_local_id.clone();
        let add_private_key_clone = add_private_key.clone();
        let add_node_clone = add_node.clone();

        //enumerate lines for header

        let formats = [
            Format::new(
                r".* Create validator session ([0-9a-fA-F]+) for local ID ([0-9a-fA-F]+) and key ([0-9a-fA-F]+) .*timestamp=([0-9]+)",
                move |captures: &Captures| {
                    let session_id = parse_hex_as_session_id(&captures.get(1).unwrap().as_str());
                    let local_id = parse_hex_as_public_key_hash(&captures.get(2).unwrap().as_str());
                    let local_key = parse_hex_as_expanded_private_key(
                        &captures.get(3).unwrap().as_str().trim(),
                    );
                    let timestamp: u64 = captures.get(4).unwrap().as_str().parse().unwrap();
                    let replay_time =
                        std::time::UNIX_EPOCH + std::time::Duration::from_millis(timestamp);

                    add_session_id_clone(session_id.clone(), replay_time);
                    add_local_id_clone(session_id, local_id.clone());
                    add_private_key_clone(local_id, local_key);

                    Ok(true)
                },
            )?,
            Format::new(
                r".* Validator session ([0-9a-fA-F]+) node: weight=([0-9]+), public_key=([0-9a-fA-F]+), adnl_id=([0-9a-fA-F]+) .*timestamp=([0-9]+)",
                move |captures: &Captures| {
                    let session_id = parse_hex_as_session_id(&captures.get(1).unwrap().as_str());
                    let weight = captures
                        .get(2)
                        .unwrap()
                        .as_str()
                        .parse::<ValidatorWeight>()?;
                    let public_key = parse_hex_as_public_key(&captures.get(3).unwrap().as_str());
                    let adnl_id = parse_hex_as_public_key_hash(&captures.get(4).unwrap().as_str());
                    let catchain_node = CatchainNode {
                        public_key: public_key,
                        adnl_id: adnl_id,
                    };

                    add_node_clone(session_id, catchain_node, weight);

                    Ok(true)
                },
            )?,
            Format::new(
                r"^SessionId\.size.*SessionId = ([0-9a-fA-F]+) timestamp = ([0-9]+) *$",
                move |captures: &Captures| {
                    let hex = &captures.get(1).unwrap().as_str();
                    let session_id = parse_hex_as_session_id(hex);
                    let timestamp: u64 = captures.get(2).unwrap().as_str().parse().unwrap();
                    let replay_time =
                        std::time::UNIX_EPOCH + std::time::Duration::from_millis(timestamp);

                    add_session_id(session_id, replay_time);

                    Ok(true)
                },
            )?,
            Format::new(
                r"^SessionLocalId\.size.*SessionLocalId = ([0-9a-fA-F]+) SessionId\.size.*SessionId = ([0-9a-fA-F]+) timestamp = ([0-9]+) *$",
                move |captures: &Captures| {
                    let hex = &captures.get(1).unwrap().as_str();
                    let local_id = parse_hex_as_public_key_hash(hex);
                    let hex = &captures.get(2).unwrap().as_str();
                    let session_id = parse_hex_as_session_id(hex);

                    add_local_id(session_id, local_id);

                    Ok(true)
                },
            )?,
            Format::new(
                r"^SessionNode.*SessionId = ([0-9a-fA-F]+) .* PubKey = ([0-9a-fA-F]+).*Id = ([0-9a-fA-F]+).*AdnlId = ([0-9a-fA-F]+).*Weight = ([0-9]+) timestamp = ([0-9]+) *$",
                move |captures: &Captures| {
                    let hex = &captures.get(1).unwrap().as_str();
                    let session_id = parse_hex_as_session_id(hex.trim());
                    let hex = &captures.get(2).unwrap().as_str();
                    let public_key = parse_hex_as_public_key(hex.trim());
                    let hex = &captures.get(4).unwrap().as_str();
                    let adnl_id = parse_hex_as_public_key_hash(hex.trim());
                    let catchain_node = CatchainNode {
                        public_key: public_key,
                        adnl_id: adnl_id,
                    };
                    let weight = captures
                        .get(5)
                        .unwrap()
                        .as_str()
                        .parse::<ValidatorWeight>()?;

                    add_node(session_id, catchain_node, weight);

                    Ok(true)
                },
            )?,
            Format::new(
                r"^CatchainImpl::PrivateKey PrivateKey.size = ([0-9]+) PrivateKey = ([0-9a-fA-F]+) PublicKeyHash.size = ([0-9]+) PublicKeyHash = ([0-9a-fA-F]+) timestamp = ([0-9]+) *$",
                move |captures: &Captures| {
                    let hex = &captures.get(2).unwrap().as_str();
                    let private_key = parse_hex_as_private_key(&hex.trim()[8..72]);
                    let hex = &captures.get(4).unwrap().as_str();
                    let public_key_hash = parse_hex_as_public_key_hash(hex.trim());

                    add_private_key(public_key_hash, private_key);

                    Ok(true)
                },
            )?,
            Format::new(
                r"^CatChainReceivedBlockImpl::initialize.*$",
                move |_captures: &Captures| {
                    Ok(!terminate_on_body) //terminator
                },
            )?,
        ];

        Self::parse(path, &formats, stop_flag)?;

        let mut log_header = log_header.borrow_mut();

        //resolve private keys for all sessions

        log_header.resolve_local_keys();

        //find preferred session

        Ok(log_header.clone())
    }

    /*
        Creation
    */

    pub fn create_log_player(log_replay_options: &LogReplayOptions) -> Result<LogPlayerPtr> {
        debug!(
            "LogPlayer: created with replay_options={:?}",
            log_replay_options
        );

        //parse header

        let stop_flag = Arc::new(AtomicBool::new(false));
        let mut log_header =
            Self::parse_header(&log_replay_options.log_file_name, &stop_flag, true)?;

        if log_header.session_descs.len() == 0 {
            bail!(
                "No sessions have been found in the log '{}'",
                &log_replay_options.log_file_name
            );
        }

        let preferred_session_id =
            if let Some(preferred_session_id) = &log_replay_options.session_id {
                parse_hex_as_session_id(&preferred_session_id)
            } else {
                log_header.session_descs[log_header.session_descs.len() - 1]
                    .session_id
                    .clone()
            };
        let session_desc = log_header.get_session(&preferred_session_id);

        if session_desc.is_none() {
            bail!(
                "Session ID {} has not been found in the log '{}'",
                preferred_session_id,
                &log_replay_options.log_file_name
            );
        }

        let session_desc = session_desc.unwrap().clone();

        assert!(session_desc.node_ids.len() == session_desc.weights.len());
        assert!(session_desc.local_key.is_some());
        assert!(session_desc.local_key.as_ref().unwrap().pub_key().is_err());

        //create player

        let local_id = session_desc.local_id.unwrap();

        Ok(Rc::new(Self {
            session_id: session_desc.session_id,
            node_ids: session_desc.node_ids,
            weights: session_desc.weights,
            local_key: session_desc.local_key.unwrap().clone(),
            local_id: local_id,
            options: log_replay_options.clone(),
            initial_timestamp: session_desc.initial_timestamp,
        }))
    }

    pub fn create_log_players(log_replay_options: &LogReplayOptions) -> Vec<LogPlayerPtr> {
        debug!(
            "LogPlayer: created with replay_options={:?}",
            log_replay_options
        );

        //parse header

        let stop_flag = Arc::new(AtomicBool::new(false));
        let log_header = Self::parse_header(&log_replay_options.log_file_name, &stop_flag, false);

        let mut log_players = Vec::new();

        if let Err(_) = log_header {
            return log_players;
        }

        let log_header = log_header.unwrap();

        if log_header.session_descs.len() == 0 {
            return log_players;
        }

        for session_desc in log_header.session_descs {
            if session_desc.node_ids.len() != session_desc.weights.len()
                || session_desc.local_key.is_none()
                || !session_desc.local_key.as_ref().unwrap().pub_key().is_err()
            {
                continue;
            }

            let local_id = session_desc.local_id.unwrap();
            let log_player = Rc::new(Self {
                session_id: session_desc.session_id,
                node_ids: session_desc.node_ids,
                weights: session_desc.weights,
                local_key: session_desc.local_key.unwrap().clone(),
                local_id: local_id,
                options: log_replay_options.clone(),
                initial_timestamp: session_desc.initial_timestamp,
            });

            log_players.push(log_player);
        }

        log_players
    }

    pub fn create_catchain(
        options: &Options,
        log_replay_options: &LogReplayOptions,
        catchain_listener: CatchainListenerPtr,
        replay_listener: CatchainReplayListenerPtr,
    ) -> Result<CatchainPtr> {
        let player = Self::create_log_player(log_replay_options)?;

        Ok(CatchainFactory::create_catchain(
            options,
            player.get_session_id(),
            player.get_nodes(),
            player.get_local_key(),
            log_replay_options.db_path.clone(),
            log_replay_options.db_suffix.clone(),
            log_replay_options.allow_unsafe_self_blocks_resync,
            player.get_overlay_manager(replay_listener),
            catchain_listener,
        ))
    }
}

/*
    Overlay wrapper for logs replaying
*/

struct OverlayImpl {
    should_stop_flag: Arc<AtomicBool>, //atomic flag to indicate that LogPlayer thread should be stopped
    is_stopped_flag: Arc<AtomicBool>, //atomic flag to indicate that LogPlayer thread has been stopped
    listener: CatchainOverlayListenerPtr, //overlay listener
}

impl CatchainOverlay for OverlayImpl {
    fn get_impl(&self) -> &dyn Any {
        self
    }

    fn send_message(
        &self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        message: &BlockPayloadPtr,
    ) {
        debug!(
            "LogReplay: send message {} -> {}: {:?}",
            sender_id, receiver_id, message
        );
    }

    fn send_message_multicast(
        &self,
        receiver_ids: &[PublicKeyHash],
        sender_id: &PublicKeyHash,
        message: &BlockPayloadPtr,
    ) {
        debug!(
            "LogReplay: send multicast message {} -> {}: {:?}",
            sender_id,
            public_key_hashes_to_string(receiver_ids),
            message
        );
    }

    fn send_query(
        &self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        name: &str,
        _timeout: std::time::Duration,
        message: &BlockPayloadPtr,
        _response_callback: ExternalQueryResponseCallback,
    ) {
        debug!(
            "LogReplay: send query '{}' {} -> {}: {:?}",
            name, sender_id, receiver_id, message
        );
    }

    fn send_query_via_rldp(
        &self,
        dst: PublicKeyHash,
        name: String,
        _response_callback: ExternalQueryResponseCallback,
        _timeout: std::time::SystemTime,
        query: BlockPayloadPtr,
        _max_answer_size: u64,
    ) {
        debug!(
            "LogReplay: send query '{}' via RLDP -> {}: {:?}",
            name, dst, query
        );
    }

    fn send_broadcast_fec_ex(
        &self,
        sender_id: &PublicKeyHash,
        send_as: &PublicKeyHash,
        payload: BlockPayloadPtr,
    ) {
        debug!(
            "LogReplay: send broadcast_fec_ex {}/{}: {:?}",
            sender_id, send_as, payload
        );
    }
}

impl Drop for OverlayImpl {
    fn drop(&mut self) {
        debug!("Dropping LogPlayer overlay...");

        self.stop();
    }
}

impl OverlayImpl {
    fn stop(&self) {
        self.should_stop_flag.store(true, Ordering::Release);

        loop {
            if self.is_stopped_flag.load(Ordering::Relaxed) {
                break;
            }

            info!("...waiting for LogPlayer overlay thread");

            const CHECKING_INTERVAL: std::time::Duration = std::time::Duration::from_millis(300);

            std::thread::sleep(CHECKING_INTERVAL);
        }

        info!("LogPlayer has been stopped");
    }

    fn create_dummy_overlay(
        should_stop_flag: Arc<AtomicBool>,
        is_stopped_flag: Arc<AtomicBool>,
        _local_id: &PublicKeyHash,
        _overlay_short_id: &Arc<PrivateOverlayShortId>,
        _nodes: &Vec<CatchainNode>,
        listener: CatchainOverlayListenerPtr,
    ) -> Arc<OverlayImpl> {
        Arc::new(Self::new(should_stop_flag, is_stopped_flag, listener))
    }

    fn new(
        should_stop_flag: Arc<AtomicBool>,
        is_stopped_flag: Arc<AtomicBool>,
        listener: CatchainOverlayListenerPtr,
    ) -> Self {
        Self {
            listener: listener,
            should_stop_flag: should_stop_flag,
            is_stopped_flag: is_stopped_flag,
        }
    }
}
