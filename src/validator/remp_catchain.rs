use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use ever_crypto::KeyId;
use sha2::{Digest, Sha256};
use arc_swap::ArcSwapOption;
use crossbeam_channel::{Sender, Receiver, unbounded, TryRecvError};
use ton_api::IntoBoxed;
use ton_block::{ShardIdent, ValidatorDescr};
use ton_types::{Result, UInt256, error, fail};
use catchain::{BlockPayloadPtr, BlockPtr, CatchainFactory, CatchainListener, CatchainNode, CatchainOverlayManagerPtr, CatchainPtr, ExternalQueryResponseCallback, PrivateKey, PublicKey, PublicKeyHash};
use crate::engine_traits::EngineOperations;
use crate::validator::catchain_overlay::CatchainOverlayManagerImpl;
use crate::validator::message_cache::RmqMessage;
use crate::validator::mutex_wrapper::MutexWrapper;
use crate::validator::remp_manager::RempManager;
use crate::validator::validator_utils::{compute_validator_list_id, get_validator_key_idx, validatordescr_to_catchain_node};

const REMP_CATCHAIN_START_POLLING_INTERVAL: Duration = Duration::from_millis(1);

pub struct RempCatchainInstanceImpl {
    pub catchain_ptr: CatchainPtr,

    pending_messages_queue_receiver: Receiver<ton_api::ton::ton_node::RmqRecord>,
    pub pending_messages_queue_sender: Sender<ton_api::ton::ton_node::RmqRecord>,

    pub rmq_catchain_receiver: Receiver<ton_api::ton::ton_node::RmqRecord>,
    rmq_catchain_sender: Sender<ton_api::ton::ton_node::RmqRecord>
}

impl RempCatchainInstanceImpl {
    fn new(catchain_ptr: CatchainPtr) -> Self {
        let (pending_messages_queue_sender, pending_messages_queue_receiver) = unbounded();
        let (rmq_catchain_sender, rmq_catchain_receiver) = unbounded();
        Self {
            catchain_ptr,
            pending_messages_queue_sender, pending_messages_queue_receiver,
            rmq_catchain_sender, rmq_catchain_receiver
        }
    }
}

pub struct RempCatchainInstance {
    id: SystemTime,
    info: Arc<RempCatchainInfo>,
    instance_impl: ArcSwapOption<RempCatchainInstanceImpl>
}

impl RempCatchainInstance {
    pub fn new(info: Arc<RempCatchainInfo>) -> Self {
        Self { id: SystemTime::now(), info, instance_impl: ArcSwapOption::new(None) }
    }

    pub fn init_instance(&self, instance: Arc<RempCatchainInstanceImpl>) -> Result<()> {
        log::trace!(target: "remp", "Initializing RMQ instance {}", self);
        match self.instance_impl.swap(Some(instance)) {
            None => { log::trace!(target: "remp", "RMQ {} instance initialized", self); Ok(()) },
            Some(_) => fail!("RMQ {} store_instance: already initialized", self)
        }
    }

    fn get_instance_impl(&self) -> Result<Arc<RempCatchainInstanceImpl>> {
        self.instance_impl.load().ok_or_else(|| error!("RMQ {} REMP Catchain instance is absent: not started?", self))
    }

    pub fn is_session_active(&self) -> bool {
        self.instance_impl.load().is_some()
    }

    pub fn get_session(&self) -> Option<CatchainPtr> {
        self.instance_impl.load().map(|inst| inst.catchain_ptr.clone())
    }

    pub fn pending_messages_queue_send(&self, msg: ton_api::ton::ton_node::RmqRecord) -> Result<()> {
        let instance = self.get_instance_impl()?;
        match instance.pending_messages_queue_sender.send(msg) {
            Ok(()) => Ok(()),
            Err(e) => fail!("pending_messages_queue_sender: send error {}", e)
        }
    }

    pub fn pending_messages_queue_len(&self) -> Result<usize> {
        let instance = self.get_instance_impl()?;
        Ok(instance.pending_messages_queue_sender.len())
    }

    pub fn pending_messages_queue_try_recv(&self) -> Result<Option<ton_api::ton::ton_node::RmqRecord>> {
        let instance = self.get_instance_impl()?;
        match instance.pending_messages_queue_receiver.try_recv() {
            Ok(x) => Ok(Some(x)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => fail!("channel disconnected")
        }
    }

    pub fn rmq_catchain_receiver_len(&self) -> Result<usize> {
        let instance = self.get_instance_impl()?;
        Ok(instance.rmq_catchain_receiver.len())
    }

    pub fn rmq_catchain_try_recv(&self) -> Result<Option<ton_api::ton::ton_node::RmqRecord>> {
        let instance = self.get_instance_impl()?;
        match instance.rmq_catchain_receiver.try_recv() {
            Ok(x) => Ok(Some(x)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => fail!("channel disconnected")
        }
    }

    pub fn rmq_catchain_send(&self, msg: ton_api::ton::ton_node::RmqRecord) -> Result<()> {
        let instance = self.get_instance_impl()?;
        match instance.rmq_catchain_sender.send(msg) {
            Ok(()) => Ok(()),
            Err(e) => fail!("rmq_cathcain_sender: send error {}", e)
        }
    }

    pub fn get_id(&self) -> u128 {
        self.id.duration_since(UNIX_EPOCH).unwrap().as_micros()
    }
}

impl fmt::Display for RempCatchainInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.info, self.get_id())
    }
}

pub struct RempCatchainInfo {
    nodes: Vec<CatchainNode>,
    node_list_id: UInt256,
    pub queue_id: UInt256,
    pub local_idx: u32,
    pub local_key_id: UInt256,
    pub master_cc_seqno: u32,
    pub shard: ShardIdent,
}

impl RempCatchainInfo {
    pub fn compute_id(catchain_seqno: u32, current: &Vec<ValidatorDescr>, next: &Vec<ValidatorDescr>) -> UInt256 {
        let mut hasher = Sha256::new();
        hasher.update(catchain_seqno.to_be_bytes());
        for x in current {
            hasher.update(x.compute_node_id_short().as_slice());
        }
        for x in next {
            hasher.update(x.compute_node_id_short().as_slice());
        }
        From::<[u8; 32]>::from(hasher.finalize().into())
    }

    pub fn create(
        catchain_seqno: u32,
        master_cc_seqno: u32,
        curr: &Vec<ValidatorDescr>,
        next: &Vec<ValidatorDescr>,
        local: &PublicKey,
        shard: ShardIdent
    ) -> Result<Self> {
        let mut nodes: Vec<CatchainNode> = curr.iter().map(validatordescr_to_catchain_node).collect();
        let mut nodes_vdescr = curr.clone();

        let mut adnl_hash: HashSet<Arc<KeyId>> = HashSet::new();
        for nn in nodes.iter() {
            adnl_hash.insert(nn.adnl_id.clone());
        }

        for next_nn in next.iter() {
            let next_cn = validatordescr_to_catchain_node(next_nn);
            if !adnl_hash.contains(&next_cn.adnl_id) {
                nodes.push(next_cn);
                nodes_vdescr.push(next_nn.clone());
            }
        }

        let local_key_id = local.id().data().into();
        let local_idx = get_validator_key_idx(local, &nodes)?;
        let queue_id = Self::compute_id(catchain_seqno, curr, next);

        let node_list_id = compute_validator_list_id(&nodes_vdescr, Some((&shard, catchain_seqno))).unwrap();

        Ok(RempCatchainInfo { 
            queue_id,
            nodes,
            local_idx,
            local_key_id,
            node_list_id,
            master_cc_seqno,
            shard,
        })
    }

    pub fn is_same_catchain(&self, other: Arc<RempCatchainInfo>) -> bool {
        let general_eq = self.queue_id == other.queue_id &&
            self.nodes.len() == other.nodes.len() &&
            self.local_idx == other.local_idx &&
            self.local_key_id == other.local_key_id &&
            self.node_list_id == other.node_list_id &&
            self.master_cc_seqno == other.master_cc_seqno &&
            self.shard == other.shard;

        general_eq
    }
}

impl fmt::Display for RempCatchainInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}*{:x}*{}", self.local_idx, self.queue_id, self.shard)
    }
}

pub struct RempCatchain {
    //rt: tokio::runtime::Handle,
    engine: Arc<dyn EngineOperations>,
    remp_manager: Arc<RempManager>,

    info: Arc<RempCatchainInfo>,

    pub instance: RempCatchainInstance
}

impl RempCatchain {
    pub fn create(
        engine: Arc<dyn EngineOperations>,
        remp_manager: Arc<RempManager>,
        info: Arc<RempCatchainInfo>
    ) -> Result<Self> {
        let node_list_string: String = info.nodes.iter().map(
            |x| format!("{:x}/{:x} ",
                        UInt256::from(x.adnl_id.data()),
                        UInt256::from(x.public_key.id().data()))
        ).collect();

        log::trace!(target: "remp", "New message queue: id {:x}, nodes: {}, local_idx: {}, master_cc: {}",
            info.queue_id,
            node_list_string,
            info.local_idx,
            info.master_cc_seqno
        );

        return Ok(Self {
            engine,
            info: info.clone(),
            instance: RempCatchainInstance::new(info.clone()),
            remp_manager
        });
    }
/*
    pub fn compute_id(catchain_seqno: u32, current: &Vec<ValidatorDescr>, next: &Vec<ValidatorDescr>) -> UInt256 {
        let mut hasher = Sha256::new();
        hasher.update(catchain_seqno.to_be_bytes());
        for x in current {
            hasher.update(x.compute_node_id_short().as_slice());
        }
        for x in next {
            hasher.update(x.compute_node_id_short().as_slice());
        }
        From::<[u8; 32]>::from(hasher.finalize().into())
    }

    pub fn is_same_catchain(&self, other: Arc<RempCatchain>) -> bool {
        return self.info.is_same_catchain(other.info.clone());
    }
*/
    pub async fn start(self: Arc<RempCatchain>, local_key: PrivateKey) -> Result<CatchainPtr> {
        let overlay_manager: CatchainOverlayManagerPtr =
            Arc::new(CatchainOverlayManagerImpl::new(self.engine.validator_network(), self.info.node_list_id.clone()));
        let db_root = format!("{}/rmq", self.engine.db_root_dir()?);
        let db_suffix = "".to_string();
        let allow_unsafe_self_blocks_resync = false;

        let message_listener = Arc::downgrade(&self);

        log::info!(target: "remp", "Do starting RMQ Catchain session {} list_id={} with nodes {:?}",
            self,
            self.info.node_list_id.to_hex_string(),
            self.info.nodes.iter().map(|x| x.adnl_id.to_string()).collect::<Vec<String>>()
        );

        let rmq_local_key = if let Some(rmq_local_key) = self.engine.set_validator_list(self.info.node_list_id.clone(), &self.info.nodes).await? {
            rmq_local_key.clone()
        } else {
            return Err(failure::err_msg(format!("Cannot add RMQ validator list {}", self.info.node_list_id.to_hex_string())));
        };

        let rmq_catchain_options = self.remp_manager.options.get_catchain_options().ok_or_else(
            || error!("RMQ {}: cannot get REMP catchain options, start is impossible", self)
        )?;

        let catchain_ptr = CatchainFactory::create_catchain(
            &rmq_catchain_options,
            &self.info.queue_id,
            &self.info.nodes,
            &local_key, //&rmq_local_key,
            db_root,
            db_suffix,
            allow_unsafe_self_blocks_resync,
            overlay_manager,
            message_listener
        );

        log::info!(target: "remp", "RMQ session {} started: list_id: {:x}, rmq_local_key = {}, local_key = {}",
            self, self.info.node_list_id, rmq_local_key.id(), local_key.id());

        Ok(catchain_ptr)
    }

    pub async fn stop(&self, session_opt: Option<CatchainPtr>) -> Result<()> {
        log::info!(target: "remp", "Do stopping RMQ Catchain session {} list_id={:x}", self, self.info.node_list_id);
        match session_opt {
            Some(session) => session.stop(true),
            _ => log::trace!(target: "remp", "Queue {} is destroyed, but not started", self)
        };
        // TODO: check whether this removal is not ahead-of-time
        log::trace!(target: "remp", "RMQ session {}, removing validator list, list_id {:x}", self, self.info.node_list_id);
        self.engine.remove_validator_list(self.info.node_list_id.clone()).await?;
        log::trace!(target: "remp", "RMQ session {} stopped", self);
        Ok(())
    }

    fn unpack_payload(&self, payload: &BlockPayloadPtr, source_idx: u32) {
        log::trace!(target: "remp", "RMQ {} unpacking message {:?} from {}", self, payload.data().0, source_idx);

        let pld: Result<::ton_api::ton::validator_session::BlockUpdate> =
            catchain::utils::deserialize_tl_boxed_object(payload.data());

        match pld {
            Ok(::ton_api::ton::validator_session::BlockUpdate::ValidatorSession_BlockUpdate(pld)) => {
                let mut total = 0;
                for msgbx in pld.actions.0.iter() {
                    match msgbx {
                        ::ton_api::ton::validator_session::round::Message::ValidatorSession_Message_Commit(msg) => {
                            match RmqMessage::deserialize(&msg.signature) {
                                Ok(unpacked_message) => {
                                    total += 1;
                                    log::trace!(target: "remp",
                                        "Point 4. Message received from RMQ {}: {:?}, decoded {:?}, put to rmq_catchain queue",
                                        self, msg.signature.0, unpacked_message //catchain.received_messages.len()
                                    );
                                    if let Err(e) = self.instance.rmq_catchain_send(unpacked_message.clone()) {
                                        log::error!(
                                            target: "remp", "Point 4. Cannot put message {:?} from RMQ {} to queue: {}",
                                            unpacked_message, self, e
                                        )
                                    }
                                },
                                Err(e) => log::error!(target: "remp", "Cannot deserialize message from RMQ {} {:?}: {}",
                                    self, msg.signature.0, e
                                )
                            }
                        },
                        _ => log::error!(target: "remp", "Point 4. RMQ {}: only Commit messages are expected", self)
                    }
                };
                #[cfg(feature = "telemetry")] {
                    self.engine.remp_core_telemetry().got_from_catchain(&self.info.shard, total, 0);
                    match self.instance.rmq_catchain_receiver_len() {
                        Ok(len) => self.engine.remp_core_telemetry().in_channel_to_rmq(&self.info.shard, len),
                        Err(e) => log::error!(target: "remp", "Point 4. RMQ {}: cannot receive rmq_catchain queue len, `{}`", self, e)
                    };
                }
            }
            Err(e) => log::error!(target: "remp", "Cannot deserialize RMQ {} message: {}", self, e)
        }
    }
}

impl fmt::Display for RempCatchain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.info)
    }
}

impl CatchainListener for RempCatchain {
    fn preprocess_block(&self, block: BlockPtr) {
        let data = block.get_payload();
        log::trace!(target: "remp", "Preprocessing RMQ {} Message {:?} from {}",
            self, data.data().0, block.get_source_id()
        );
        self.unpack_payload(data, block.get_source_id());
    }

    fn process_blocks(&self, blocks: Vec<BlockPtr>) {
        log::trace!(target: "remp", "Processing RMQ {}: new external messages, len = {}", self, blocks.len());
        //if blocks.len() > 0 {
        //    for x in blocks {
        //        self.unpack_payload(x.get_payload(), x.get_source_id());
        //    }
        //}

        let mut msg_vect: Vec<::ton_api::ton::validator_session::round::Message> = Vec::new();
        while let Ok(Some(msg)) = self.instance.pending_messages_queue_try_recv() {
            let msg_body = ::ton_api::ton::validator_session::round::validator_session::message::message::Commit {
                round: 0,
                candidate: Default::default(),
                signature: RmqMessage::serialize(&msg).unwrap()
            }.into_boxed();
            log::trace!(target: "remp", "Point 3. RMQ {} sending message: {:?}, decoded {:?}",
                self, msg_body, msg
            );

            msg_vect.push(msg_body);
        };

        #[cfg(feature = "telemetry")]
            let sent_to_catchain = msg_vect.len();

        let payload = ::ton_api::ton::validator_session::blockupdate::BlockUpdate {
            ts: 0, //ts as i64,
            actions: msg_vect.into(),
            state: 0 //real_state_hash as i32,
        }.into_boxed();
        let serialized_payload = catchain::utils::serialize_tl_boxed_object!(&payload);

        match &self.instance.get_session() {
            Some(ctchn) => {
                ctchn.processed_block(
                    catchain::CatchainFactory::create_block_payload(
                        serialized_payload.clone(),
                    ), false, false);
                log::trace!(target: "remp", "Point 3. RMQ {} sent message: {:?}",
                    self, serialized_payload
                );
                #[cfg(feature = "telemetry")]
                self.engine.remp_core_telemetry().sent_to_catchain(&self.info.shard, sent_to_catchain);
            },
            None => log::error!("RMQ: Catchain session is not initialized!")
        }
    }

    fn finished_processing(&self) {
        log::trace!(target: "remp", "MessageQueue {} finished processing", self)
    }

    fn started(&self) {
        log::trace!(target: "remp", "MessageQueue {} started", self)
    }

    fn process_broadcast(&self, _source_id: PublicKeyHash, _data: BlockPayloadPtr) {
        log::trace!(target: "remp", "MessageQueue {} process broadcast", self)
    }

    fn process_query(&self, source_id: PublicKeyHash, data: BlockPayloadPtr, _callback: ExternalQueryResponseCallback) {
        let data = data.data();
        log::trace!(target: "remp", "Processing RMQ {} Query {:?} from {}", self, data.0.as_slice(), source_id);
    }

    fn set_time(&self, _timestamp: SystemTime) {
        log::trace!(target: "remp", "MessageQueue {} set time", self)
    }
}

#[derive(Debug,PartialEq,Eq,Clone)]
enum RempCatchainStatus {
    Created, Starting, Active, Stopping
}

struct RempCatchainWrapper {
    info: Arc<RempCatchain>,
    status: RempCatchainStatus,
    count: i32
}

impl RempCatchainWrapper {
    pub fn create(info: Arc<RempCatchain>) -> Self {
        RempCatchainWrapper {
            info,
            status: RempCatchainStatus::Created,
            count: 1
        }
    }

    pub fn attach(&mut self) {
        self.count += 1;
    }

    pub fn detach(&mut self) -> bool {
        self.count -= 1;
        self.count <= 0
    }

    pub fn set_active(&mut self) -> Result<()> {
        if self.status == RempCatchainStatus::Starting {
            self.status = RempCatchainStatus::Active;
            Ok(())
        }
        else {
            fail!("RempCatchainWrapper {}: cannot set active, incompatible current status", self)
        }
    }
}

impl Display for RempCatchainWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({:?})", self.info, self.status)
    }
}

pub struct RempCatchainStore {
    catchains: MutexWrapper<HashMap<UInt256, RempCatchainWrapper>>
}

impl RempCatchainStore {
    pub fn new() -> Self {
        RempCatchainStore { catchains: MutexWrapper::new(HashMap::new(), "CatchainStore".to_string()) }
    }

    pub async fn create_catchain(&self, 
        engine: Arc<dyn EngineOperations>,
        remp_manager: Arc<RempManager>,
        remp_catchain_info: Arc<RempCatchainInfo>
    ) -> Result<()> {
        self.catchains.execute_sync(|x| {
            if let Some(rcw) = x.get_mut(&remp_catchain_info.queue_id) {
                if rcw.info.info.is_same_catchain(remp_catchain_info.clone()) {
                    rcw.attach();
                    Ok(())
                } else {
                    fail!("REMP Catchain Store: adding different catchain {} (to {}) for same id {:x}",
                        remp_catchain_info, rcw.info, remp_catchain_info.queue_id
                    )
                }
            }
            else {
                let remp_catchain = Arc::new(RempCatchain::create(engine, remp_manager, remp_catchain_info.clone())?);
                x.insert(remp_catchain_info.queue_id.clone(), RempCatchainWrapper::create(remp_catchain));
                Ok(())
            }
        }).await?;

        Ok(())
    }

    pub async fn activate_catchain(&self, session_id: &UInt256) -> Result<()> {
        self.catchains.execute_sync(|x| {
            match x.get_mut(&session_id) {
                Some(cc) => cc.set_active(),
                None => fail!("REMP Catchain session {:x} start impossible -- session disappeared", session_id)
            }
        }).await
    }

    pub async fn start_catchain(&self, session_id: UInt256, local_key: PrivateKey) -> Result<Arc<RempCatchainInstanceImpl>> {
        log::trace!(target: "remp", "Starting REMP catchain {:x}", session_id);

        let (catchain_info, do_start) = loop {
            let (cc_status, catchain_info) = self.catchains.execute_sync(|x| {
                match x.get_mut(&session_id) {
                    Some(cc @ RempCatchainWrapper {status: RempCatchainStatus::Created, ..}) => {
                        cc.status = RempCatchainStatus::Starting;
                        Ok((RempCatchainStatus::Created, cc.info.clone()))
                    },
                    Some(cc) => Ok((cc.status.clone(), cc.info.clone())),
                    None => fail!("REMP Catchain session {:x} start impossible -- not found", session_id)
                }
            }).await?;

            log::trace!(target: "remp", "REMP catchain {:x} start status: {:?}", session_id, cc_status);

            match cc_status {
                RempCatchainStatus::Created => break (catchain_info, true),
                RempCatchainStatus::Starting => {
                    tokio::time::sleep(REMP_CATCHAIN_START_POLLING_INTERVAL).await;
                    continue
                },
                RempCatchainStatus::Active => {
                    log::trace!(target: "remp", "REMP catchain {:x} is already started -- copying instance", session_id);
                    break (catchain_info, false)
                },
                RempCatchainStatus::Stopping =>
                    fail!("REMP Catchain session {:x} is being stopped --- start_catchain call cannot be performed", session_id)
            }
        };

        /*
        match cc.status {
            RempCatchainStatus::Created => {
                cc.status = RempCatchainStatus::Starting;
                Ok((cc.info.clone(), true))
            },
            RempCatchainStatus::Stopping =>
                fail!("REMP Catchain session {} is being stopped --- impossible combination", cc),
            RempCatchainStatus::Active => {
                log::trace!(target: "remp", "REMP catchain {:x} is already started", session_id);
                return Ok((cc.info.clone(), false))
            }
        }
    }
*/
        if do_start {
            log::trace!(target: "remp", "Actually starting REMP catchain {:x}/{}",
                catchain_info.info.queue_id, catchain_info.info.shard
            );
            let catchain_ptr = catchain_info.clone().start(local_key).await?;
            let instance_impl = Arc::new(RempCatchainInstanceImpl::new(catchain_ptr));
            catchain_info.instance.init_instance(instance_impl.clone())?;
            self.activate_catchain(&session_id).await?;
            Ok(instance_impl)
        }
        else {
            catchain_info.instance.get_instance_impl()
        }
    }

    pub async fn stop_catchain(&self, session_id: &UInt256) -> Result<()> {
        log::trace!(target: "remp", "Stopping REMP catchain {:x}", session_id);
        let (to_remove, remaining_sessions) = self.catchains.execute_sync(|x| {
            match x.get_mut(session_id) {
                Some(catchain) => {
                    if catchain.status != RempCatchainStatus::Active {
                        fail!("REMP Catchain session {} stopping impossible -- session should be in 'Active' state", catchain)
                    }
                    else if catchain.detach() {
                        catchain.status = RempCatchainStatus::Stopping;
                        let catchain_ptr: Option<CatchainPtr> = catchain.info.instance.get_session();
                        Ok((Some((catchain.info.clone(), catchain_ptr)), catchain.count))
                    }
                    else { Ok((None, catchain.count)) }
                },
                None => fail!("REMP Catchain session {:x} not found!", session_id)
            }
        }).await?;

        if let Some((info,session)) = to_remove {
            log::trace!(target: "remp", 
                "RMQ session removed and being stopped: {:x}, {} sessions after detaching", 
                session_id, remaining_sessions
            );
            info.stop(session).await?;
            self.catchains.execute_sync(|x| x.remove(session_id)).await;
        }
        else {
            log::trace!(target: "remp", 
                "RMQ session detached {:x}, {} sessions still active", 
                session_id, remaining_sessions
            );
        }
        Ok(())
    }

    pub async fn get_catchain_session(&self, session_id: &UInt256) -> Option<CatchainPtr> {
        self.catchains.execute_sync(|x| {
            x.get(session_id).map(|rcw| rcw.info.instance.get_session()).flatten()
        }).await
    }
}
