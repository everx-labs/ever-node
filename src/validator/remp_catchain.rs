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
    engine_traits::EngineOperations,
    validator::{
        catchain_overlay::CatchainOverlayManagerImpl, message_cache::RmqMessage, 
        mutex_wrapper::MutexWrapper, remp_manager::RempManager,
        validator_utils::{
            get_group_members_by_validator_descrs, get_validator_key_idx, GeneralSessionInfo,
            validatordescr_to_catchain_node, ValidatorListHash
        }
    }
};

use catchain::{
    serialize_tl_boxed_object,
    BlockPayloadPtr, BlockPtr, CatchainFactory, CatchainListener, CatchainNode, 
    CatchainOverlayManagerPtr, CatchainPtr, ExternalQueryResponseCallback, PrivateKey,
    PublicKey, PublicKeyHash
};
use std::{
    collections::{HashMap, HashSet}, fmt, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}
};
use ton_api::{
    IntoBoxed, ton::ton_node::RempCatchainRecord
};
use ton_block::ValidatorDescr;
use ton_types::{error, fail, KeyId, Result, UInt256};

const REMP_CATCHAIN_START_POLLING_INTERVAL: Duration = Duration::from_millis(1);

pub struct RempCatchainInstanceImpl {
    pub catchain_ptr: CatchainPtr,

    pending_messages_queue_receiver: crossbeam_channel::Receiver<RempCatchainRecord>,
    pub pending_messages_queue_sender: crossbeam_channel::Sender<RempCatchainRecord>,

    pub rmq_catchain_receiver: crossbeam_channel::Receiver<RempCatchainRecord>,
    rmq_catchain_sender: crossbeam_channel::Sender<RempCatchainRecord>
}

impl RempCatchainInstanceImpl {
    fn new(catchain_ptr: CatchainPtr) -> Self {
        let (pending_messages_queue_sender, pending_messages_queue_receiver) = 
            crossbeam_channel::unbounded();
        let (rmq_catchain_sender, rmq_catchain_receiver) = 
            crossbeam_channel::unbounded();
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
    instance_impl: arc_swap::ArcSwapOption<RempCatchainInstanceImpl>
}

impl RempCatchainInstance {
    pub fn new(info: Arc<RempCatchainInfo>) -> Self {
        Self { id: SystemTime::now(), info, instance_impl: arc_swap::ArcSwapOption::new(None) }
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

    pub fn pending_messages_queue_send(&self, msg: RempCatchainRecord) -> Result<()> {
        let instance = self.get_instance_impl()?;
        match instance.pending_messages_queue_sender.send(msg) {
            Ok(()) => Ok(()),
            Err(e) => fail!("pending_messages_queue_sender: send error {}", e)
        }
    }

    #[cfg(feature = "telemetry")]
    pub fn pending_messages_queue_len(&self) -> Result<usize> {
        let instance = self.get_instance_impl()?;
        Ok(instance.pending_messages_queue_sender.len())
    }

    pub fn pending_messages_queue_try_recv(&self) -> Result<Option<RempCatchainRecord>> {
        let instance = self.get_instance_impl()?;
        match instance.pending_messages_queue_receiver.try_recv() {
            Ok(x) => Ok(Some(x)),
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => fail!("channel disconnected")
        }
    }

    pub fn rmq_catchain_receiver_len(&self) -> Result<usize> {
        let instance = self.get_instance_impl()?;
        Ok(instance.rmq_catchain_receiver.len())
    }

    pub fn rmq_catchain_try_recv(&self) -> Result<Option<RempCatchainRecord>> {
        let instance = self.get_instance_impl()?;
        match instance.rmq_catchain_receiver.try_recv() {
            Ok(x) => Ok(Some(x)),
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => fail!("channel disconnected")
        }
    }

    pub fn rmq_catchain_send(&self, msg: RempCatchainRecord) -> Result<()> {
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
    pub general_session_info: Arc<GeneralSessionInfo>,
    pub master_cc_seqno: u32,
    nodes: Vec<CatchainNode>,
    node_list_id: UInt256,
    pub queue_id: UInt256,
    pub local_idx: usize,
    pub local_key_id: UInt256
}

impl RempCatchainInfo {
    pub fn compute_id(current: &Vec<ValidatorDescr>, next: &Vec<ValidatorDescr>, general_session_info: Arc<GeneralSessionInfo>) -> UInt256 {
        let mut members = Vec::new();
        get_group_members_by_validator_descrs(current, &mut members);
        get_group_members_by_validator_descrs(next, &mut members);

        let serialized = serialize_tl_boxed_object!(&ton_api::ton::ton_node::rempsessioninfo::RempSessionInfo {
            workchain: general_session_info.shard.workchain_id(),
            shard: general_session_info.shard.shard_prefix_with_tag() as i64,
            vertical_seqno: general_session_info.max_vertical_seqno as i32,
            last_key_block_seqno: general_session_info.key_seqno as i32,
            catchain_seqno: general_session_info.catchain_seqno as i32,
            config_hash: general_session_info.opts_hash.clone(),
            members: members.into()
        }.into_boxed());

        UInt256::calc_file_hash(&serialized.0)
    }

    pub fn create(
        general_session_info: Arc<GeneralSessionInfo>,
        master_cc_seqno: u32,
        curr: &Vec<ValidatorDescr>,
        next: &Vec<ValidatorDescr>,
        local: &PublicKey,
        node_list_id: ValidatorListHash
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
        let queue_id = Self::compute_id(curr, next, general_session_info.clone());

        Ok(RempCatchainInfo {
            general_session_info,
            queue_id,
            nodes,
            local_idx,
            local_key_id,
            node_list_id,
            master_cc_seqno
        })
    }

    pub fn is_same_catchain(&self, other: Arc<RempCatchainInfo>) -> bool {
        let is_same_nodelist = 
            self.nodes.len() == other.nodes.len() &&
            self.nodes.iter().zip (other.nodes.iter()).all(|(a,b)| a.adnl_id == b.adnl_id);

        let general_eq = self.queue_id == other.queue_id &&
            self.general_session_info == other.general_session_info &&
            self.local_idx == other.local_idx &&
            self.local_key_id == other.local_key_id &&
            is_same_nodelist &&
            self.node_list_id == other.node_list_id &&
            self.master_cc_seqno == other.master_cc_seqno;

        general_eq
    }
}

impl fmt::Display for RempCatchainInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}*{:x}*{}", self.local_idx, self.queue_id, self.general_session_info.shard)
    }
}

pub struct RempCatchain {
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

        let rmq_catchain_options = self.remp_manager.options.get_catchain_options().ok_or_else(
            || error!("RMQ {}: cannot get REMP catchain options, start is impossible", self)
        )?;

        let catchain_ptr = CatchainFactory::create_catchain(
            &rmq_catchain_options,
            &self.info.queue_id,
            &self.info.nodes,
            &local_key,
            db_root,
            db_suffix,
            allow_unsafe_self_blocks_resync,
            overlay_manager,
            message_listener
        );

        log::info!(target: "remp", "RMQ session {} started: list_id: {:x}, local_key = {}",
            self, self.info.node_list_id, local_key.id());

        Ok(catchain_ptr)
    }

    pub async fn stop(&self, session_opt: Option<CatchainPtr>) -> Result<()> {
        log::info!(target: "remp", "Do stopping RMQ Catchain session {} list_id={:x}", self, self.info.node_list_id);
        match session_opt {
            Some(session) => session.stop(true),
            _ => log::trace!(target: "remp", "Queue {} is destroyed, but not started", self)
        };
        log::trace!(target: "remp", "RMQ session {} stopped", self);
        Ok(())
    }

    fn unpack_payload(&self, payload: &BlockPayloadPtr, source_idx: u32) {
        log::trace!(target: "remp", "RMQ {} unpacking message {:?} from {}", self, payload.data().0, source_idx);

        let pld: Result<::ton_api::ton::validator_session::BlockUpdate> =
            catchain::utils::deserialize_tl_boxed_object(payload.data());

        match pld {
            Ok(::ton_api::ton::validator_session::BlockUpdate::ValidatorSession_BlockUpdate(pld)) => {
                #[cfg(feature = "telemetry")]
                let mut total = 0;
                for msgbx in pld.actions.0.iter() {
                    match msgbx {
                        ::ton_api::ton::validator_session::round::Message::ValidatorSession_Message_Commit(msg) => {
                            match RmqMessage::deserialize(&msg.signature) {
                                Ok(unpacked_message) => {
                                    #[cfg(feature = "telemetry")] {
                                        total += 1;
                                    }
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
                    self.engine.remp_core_telemetry().got_from_catchain(&self.info.general_session_info.shard, total, 0);
                    match self.instance.rmq_catchain_receiver_len() {
                        Ok(len) => self.engine.remp_core_telemetry().in_channel_to_rmq(&self.info.general_session_info.shard, len),
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
        let serialized_payload = serialize_tl_boxed_object!(&payload);

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
                self.engine.remp_core_telemetry().sent_to_catchain(
                    &self.info.general_session_info.shard, 
                    sent_to_catchain
                );
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

impl fmt::Display for RempCatchainWrapper {
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

    pub async fn activate_catchain(&self, session_id: &UInt256) -> Result<()> {
        self.catchains.execute_sync(|x| {
            match x.get_mut(&session_id) {
                Some(cc) => cc.set_active(),
                None => fail!("REMP Catchain session {:x} start impossible -- session disappeared", session_id)
            }
        }).await
    }

    pub async fn start_catchain(&self,
                                engine: Arc<dyn EngineOperations>,
                                remp_manager: Arc<RempManager>,
                                to_start: Arc<RempCatchainInfo>,
                                local_key: PrivateKey
    ) -> Result<Arc<RempCatchainInstanceImpl>> {
        let session_id = &to_start.queue_id;
        log::trace!(target: "remp", "Starting REMP catchain {:x}", session_id);

        let (catchain_info, do_start) = loop {
            let (cc_status, catchain_info) = self.catchains.execute_sync(|x| {
                match x.get_mut(&session_id) {
                    Some(existing @ RempCatchainWrapper{status: RempCatchainStatus::Created, ..}) =>
                        fail!("REMP Catchain Store: impossible status {:?} for session id {:x}", existing.status, session_id),
                    Some(existing @ RempCatchainWrapper{status: RempCatchainStatus::Starting, ..}) |
                    Some(existing @ RempCatchainWrapper{status: RempCatchainStatus::Stopping, ..}) =>
                        Ok((existing.status.clone(), existing.info.clone())),
                    Some(existing @ RempCatchainWrapper{status: RempCatchainStatus::Active, ..}) => {
                        if existing.info.info.is_same_catchain(to_start.clone()) {
                            existing.attach();
                            Ok((existing.status.clone(), existing.info.clone()))
                        } else {
                            fail!("REMP Catchain Store: adding different catchain {} (to {}) for same session id {:x}",
                                to_start, existing.info, to_start.queue_id
                            )
                        }
                    }
                    None => {
                        let remp_catchain = Arc::new(RempCatchain::create(engine.clone(), remp_manager.clone(), to_start.clone())?);
                        let mut remp_catchain_wrapper = RempCatchainWrapper::create(remp_catchain.clone());
                        remp_catchain_wrapper.status = RempCatchainStatus::Starting;
                        x.insert(to_start.queue_id.clone(), remp_catchain_wrapper);
                        Ok((RempCatchainStatus::Created, remp_catchain))
                    }
                }
            }).await?;

            log::trace!(target: "remp", "REMP catchain {:x} start status: {:?}", session_id, cc_status);

            match cc_status {
                RempCatchainStatus::Created => break (catchain_info, true),
                RempCatchainStatus::Active => {
                    log::trace!(target: "remp", "REMP catchain {:x} is already started -- copying instance", session_id);
                    break (catchain_info, false)
                },
                RempCatchainStatus::Starting => {
                    log::warn!(target: "remp", "REMP Catchain session {:x} is being started --- waiting until it's done", session_id);
                },
                RempCatchainStatus::Stopping => {
                    log::warn!(target: "remp", "REMP Catchain session {:x} is being stopped --- waiting until it's done", session_id);
                }
            }

            tokio::time::sleep(REMP_CATCHAIN_START_POLLING_INTERVAL).await;
        };

        if do_start {
            log::trace!(target: "remp", "Actually starting REMP catchain {:x}/{}",
                session_id, catchain_info.info.general_session_info.shard
            );
            let catchain_ptr = catchain_info.clone().start(local_key).await?;
            let instance_impl = Arc::new(RempCatchainInstanceImpl::new(catchain_ptr));
            catchain_info.instance.init_instance(instance_impl.clone())?;
            self.activate_catchain(session_id).await?;
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
