use adnl::{ node::AdnlNode,
    common::{AdnlPeers, KeyId, KeyOption, QueryResult, serialize, serialize_append, Wait, Answer}
};
use overlay::{OverlayNode, PrivateOverlayShortId, QueriesConsumer};
use catchain::{BlockPayloadPtr, CatchainNode, CatchainOverlay, CatchainOverlayListenerPtr,
    ExternalQueryResponseCallback, PublicKeyHash
};

use rldp::RldpNode;
use std::{
    collections::HashMap, io::Cursor, sync::{Arc, atomic::{self, AtomicBool}}, time::Instant
};
use ton_api::{ Deserializer,
    ton::{ catchain::Update::Catchain_BlockUpdate, ton_node::Broadcast,
    validator_session::BlockUpdate::ValidatorSession_BlockUpdate, TLObject}
};
use ton_types::{error, fail, Result};


pub struct CatchainClient {
    runtime_handle : tokio::runtime::Handle,
    overlay_id: Arc<PrivateOverlayShortId>,
    overlay: Arc<OverlayNode>,
    rldp: Arc<RldpNode>,
    local_validator_key: Arc<KeyOption>,
    validator_keys: HashMap<Arc<KeyId>, Arc<KeyId>>,
    consumer: Arc<CatchainClientConsumer>,
    is_stop: Arc<AtomicBool>,
}

impl CatchainClient {
    const TARGET:  &'static str = "catchain_network";

    pub fn new(
        runtime_handle: &tokio::runtime::Handle,
        overlay_id: &Arc<PrivateOverlayShortId>,
        overlay: &Arc<OverlayNode>,
        rldp: Arc<RldpNode>,
        nodes: &Vec<CatchainNode>,
        local_adnl_key: &Arc<KeyOption>,
        local_validator_key: Arc<KeyOption>,
        catchain_listener: CatchainOverlayListenerPtr
    ) -> Result<Self> {

        let mut keys = HashMap::new();
        let mut peers = Vec::new();

        for node in nodes {
            if node.public_key.id() == local_adnl_key.id() {
                continue;
            }
            keys.insert(node.adnl_id.clone(), node.public_key.id().clone());
            peers.push(node.adnl_id.clone());
        }

        let id_local_key = local_adnl_key.id();
        log::debug!("new catchain_client: overlay_id {:x?}, id_local_key {:x?}",
            &overlay_id, &id_local_key
        );

        runtime_handle.block_on(
            overlay.add_private_overlay(overlay_id, &local_adnl_key, &peers))?;

        let consumer = Arc::new(CatchainClientConsumer::new(overlay_id.clone(), catchain_listener));
        overlay.add_consumer(&overlay_id, consumer.clone())?;

        Ok(CatchainClient {
            runtime_handle : runtime_handle.clone(),
            overlay_id: overlay_id.clone(),
            overlay: overlay.clone(),
            rldp: rldp,
            local_validator_key: local_validator_key,
            validator_keys: keys,
            consumer: consumer,
            is_stop: Arc::new(AtomicBool::new(false))
        })
    }

    pub async fn stop(&self) {
        if let Err(e) = self.overlay.delete_private_overlay(&self.overlay_id) {
            log::warn!("{:?}", e);
        }
        self.is_stop.store(true, atomic::Ordering::Relaxed);
        self.consumer.is_stop.store(true, atomic::Ordering::Relaxed);
        log::debug!("Overlay {} stopped.", &self.overlay_id);
    }

    pub fn catchain_listener(&self) -> &CatchainOverlayListenerPtr {
        &self.consumer.catchain_listener
    }

    pub fn validator_keys(&self) -> &HashMap<Arc<KeyId>, Arc<KeyId>> {
        &self.validator_keys
    }

    pub fn message(
        &self,
        receiver_id: &PublicKeyHash,
        message: &BlockPayloadPtr
    ) -> Result<()> {
        let overlay = self.overlay.clone();
        let overlay_id = self.overlay_id.clone();
        let msg = message.clone();
        let receiver = receiver_id.clone();
        let is_stop_state = self.is_stop.clone();

        self.runtime_handle.spawn(async move {
            let is_stop = is_stop_state.load(atomic::Ordering::Relaxed);
            if is_stop {
                log::warn!("Overlay {} was stopped!", &overlay_id);
                return;
            }
            let answer = overlay.message(&receiver, &msg.data().0, &overlay_id).await;
            log::trace!(target: Self::TARGET, "<send_message> (overlay: {}, data: {:x}{:x}{:x}{:x}, key_id: {}): {:?}",
                &overlay_id, 
                &msg.data().0[3],
                &msg.data().0[2],
                &msg.data().0[1],
                &msg.data().0[0], 
                &receiver, &answer);
        });
        Ok(())
    }

    pub async fn query(
        overlay_id: &Arc<PrivateOverlayShortId>,
        overlay: &Arc<OverlayNode>,
        receiver_id: &PublicKeyHash,
        timeout: std::time::Duration,
        message: &BlockPayloadPtr
    ) -> Result<BlockPayloadPtr> {
        let query = Deserializer::new(&mut Cursor::new(&message.data().0)).read_boxed()?;
        /*log::debug!("<send_query> send (overlay: {}, data: {:x}{:x}{:x}{:x}, key_id: {})", 
            &overlay_id, 
            &message.data().0[3],
            &message.data().0[2],
            &message.data().0[1],
            &message.data().0[0],
            &receiver_id);*/
        let now = Instant::now();
        let result = overlay.query(&receiver_id, &query, overlay_id,
            Some(AdnlNode::calc_timeout(Some(timeout.as_millis() as u64)))
        ).await?;
        let elapsed = now.elapsed();
        log::trace!(target: Self::TARGET, "<send_query> result (overlay: {}, data: {:x}{:x}{:x}{:x}, key_id: {}): {:?}({}ms)",
            &overlay_id, 
            &message.data().0[3],
            &message.data().0[2],
            &message.data().0[1],
            &message.data().0[0],
            &receiver_id, result, elapsed.as_millis());
        let result = result.ok_or_else(|| error!("answer is None!"))?;
        let mut data: catchain::RawBuffer = catchain::RawBuffer::default();
        let mut serializer = ton_api::Serializer::new(&mut data.0);
        serializer.write_boxed(&result)?;
        let data = catchain::CatchainFactory::create_block_payload(data);

        Ok(data)
    }


    async fn query_via_rldp(
        overlay_id: &Arc<PrivateOverlayShortId>,
        overlay: &Arc<OverlayNode>,
        rldp: &Arc<RldpNode>,
        _timeout: std::time::SystemTime,
        receiver_id: &PublicKeyHash,
        message: &BlockPayloadPtr,
        max_answer_size: u64
    )-> Result<BlockPayloadPtr> {
        let request: TLObject = Deserializer::new(&mut Cursor::new(&message.data().0)).read_boxed()?;
        let mut query = overlay.get_query_prefix(overlay_id)?;
        //Serializer::new(&mut query).write_bare(&message.0)?;
        serialize_append(&mut query, &request)?;

       // let timeout = timeout.duration_since(SystemTime::now())?.as_millis();
        let result = overlay.query_via_rldp(
            rldp,
            receiver_id,
            &query,
            Some(max_answer_size as i64),
            Some(5_000),
            Some(10_000),
            &overlay_id
        ).await;

        
        log::trace!(target: Self::TARGET, "result status: {}", &result.is_ok());
        let data = result?.ok_or_else(|| error!("asnwer is None!"))?;

        let data = catchain::CatchainFactory::create_block_payload(
            ton_api::ton::bytes(data)
        );
        Ok(data)
    }

    pub fn run_wait_broadcast(
        self: Arc<Self>,
        runtime_handle: &tokio::runtime::Handle,
        private_overlay_id: &Arc<PrivateOverlayShortId>,
        overlay_node: &Arc<OverlayNode>,
        validator_keys: &HashMap<Arc<KeyId>, Arc<KeyId>>,
        catchain_listener: &CatchainOverlayListenerPtr)
    {
        let overlay_id = private_overlay_id.clone();
        let self1 = self.clone();
        let self2 = self.clone();
        let overlay = overlay_node.clone();
        let keys = validator_keys.clone();
        let listener = catchain_listener.clone();
        runtime_handle.spawn(async move { 
            if let Err(e) = CatchainClient::wait_broadcasts(self1, &overlay_id, overlay, &keys, &listener).await {
                log::warn!(target: Self::TARGET, "ERROR: {}", e)
            }
        });
        let overlay_id = private_overlay_id.clone();
        let overlay = overlay_node.clone();
        let keys = validator_keys.clone();
        let listener = catchain_listener.clone();
        runtime_handle.spawn(async move { 
            if let Err(e) = CatchainClient::wait_catchain_broadcast(self2, &overlay_id, overlay, &keys, &listener).await {
                log::warn!(target: Self::TARGET, "ERROR: {}", e)
            }
        });
    }

    async fn wait_broadcasts(
        self: Arc<Self>,
        overlay_id: &Arc<PrivateOverlayShortId>,
        overlay: Arc<OverlayNode>,
        _validator_keys: &HashMap<Arc<KeyId>, Arc<KeyId>>,
        catchain_listener: &CatchainOverlayListenerPtr) -> Result<()> {
        let receiver = overlay.clone();
        let result: Option<Box<Broadcast>> = None;
        let catchain_listener = catchain_listener.clone();

        while let None = result {
            if self.is_stop.load(atomic::Ordering::Relaxed) {
                break;
            };
            let message = receiver.wait_for_broadcast(overlay_id).await;
            match message {
                Ok(message) => {
                    log::trace!(target: Self::TARGET, "private overlay broadcast (successed)");
                   // let src_id = validator_keys.get(&message.1).ok_or_else(|| error!("unknown key!"))?;
                    if let Some(listener) = catchain_listener.upgrade() {
                        listener
                            .on_broadcast(message.1/*src_id.clone()*/, &catchain::CatchainFactory::create_block_payload(::ton_api::ton::bytes(message.0)));    // Test id!
                    }
                },
                Err(e) => {
                    log::error!(target: Self::TARGET, "private overlay broadcast err: {}", e);
                },
            };
        }
        let _result = result.ok_or_else(|| error!("Failed to receive a private overlay broadcast!"))?;
        Ok(())
    }

    async fn wait_catchain_broadcast(
        self: Arc<Self>,
        overlay_id: &Arc<PrivateOverlayShortId>,
        overlay: Arc<OverlayNode>, 
        _validator_keys: &HashMap<Arc<KeyId>, Arc<KeyId>>,
        catchain_listener: &CatchainOverlayListenerPtr) -> Result<()> {
        let receiver = overlay.clone();
        let result: Option<Box<Broadcast>> = None;
        let catchain_listener = catchain_listener.clone();

        while let None = result {
            if self.is_stop.load(atomic::Ordering::Relaxed) {
                break;
            };
            let message = receiver.wait_for_catchain(overlay_id).await;
            match message {
                Ok((catchain_block_update, validator_session_block_update, source_id))  => {
                    log::trace!(target: Self::TARGET, "private overlay broadcast ValidatorSession_BlockUpdate (successed)");
                    let vs_block_update = ValidatorSession_BlockUpdate(Box::new(validator_session_block_update));
                    let block_update = Catchain_BlockUpdate(Box::new(catchain_block_update));
                   if let Some(listener) = catchain_listener.upgrade() {
                                let mut data: catchain::RawBuffer = catchain::RawBuffer::default();
                                let mut serializer = ton_api::Serializer::new(&mut data.0);
                                serializer.write_boxed(&block_update)?;
                                serializer.write_boxed(&vs_block_update)?;
                                let data = catchain::CatchainFactory::create_block_payload(data);
                        listener
                            .on_message(
                                source_id,
                                &data);
                    }
                },
                Err(e) => {
                    log::error!(target: Self::TARGET, "private overlay broadcast err: {}", e);
                },
            };
        }
        let _result = result.ok_or_else(|| error!("Failed to receive a private overlay broadcast!"))?;
        Ok(())
    }
}

impl CatchainOverlay for CatchainClient {
    fn get_impl(&self) -> &dyn std::any::Any {
        self
    }

    /// Send message
    fn send_message(&self,
        receiver_id: &PublicKeyHash,
        _sender_id: &PublicKeyHash,
        message: &BlockPayloadPtr)
    {
        let now = Instant::now();
        match self.message(receiver_id, message) {
            Ok(_) => { /*log::trace!("send_message success!");*/ },
            Err(e) => { log::warn!(target: Self::TARGET, "send_message err: {:?}", e); }
        }

        let elapsed = now.elapsed();
        if elapsed.as_micros() > 500 {
            log::trace!(target: Self::TARGET, "message elapsed: {}", elapsed.as_millis());
        };
    }

    /// Send message to multiple sources
    fn send_message_multicast(
        &self,
        receiver_ids: &[PublicKeyHash],
        _sender_id: &PublicKeyHash,
        message: &BlockPayloadPtr)
    {
        for receiver_id in receiver_ids.iter() {
            if let Err(e) = self.message(receiver_id, message) {
                log::error!(target: Self::TARGET, "send_message err: {:?}", e);
            }
        }
    }

    /// Send query
    fn send_query(
        &self,
        receiver_id: &PublicKeyHash,
        _sender_id: &PublicKeyHash,
        _name: &str,
        timeout: std::time::Duration,
        message: &BlockPayloadPtr,
        response_callback: ExternalQueryResponseCallback) {
            let receiver = receiver_id.clone();
            let timeout = timeout.clone();
            let msg = message.clone();
            let overlay_id = self.overlay_id.clone();
            let overlay = self.overlay.clone();
            let is_stop_state = self.is_stop.clone();
            self.runtime_handle.spawn(async move { 
                let is_stop = is_stop_state.load(atomic::Ordering::Relaxed);
                if is_stop {
                    log::warn!(target: Self::TARGET, "Overlay {} was stopped!", &overlay_id);
                    return;
                }
                let result = CatchainClient::query(&overlay_id, &overlay, &receiver, timeout, &msg).await;
                response_callback(result);
            });
    }

    /// Send query via RLDP (ADNL ID of the current node should be registered for the query)
    fn send_query_via_rldp(
        &self,
        dst: PublicKeyHash,
        _name: String,
        response_callback: ExternalQueryResponseCallback,
        timeout: std::time::SystemTime,
        query: BlockPayloadPtr,
        max_answer_size: u64) {
            let receiver = dst.clone();
            let timeout = timeout.clone();
            let msg = query.clone();
            let overlay_id = self.overlay_id.clone();
            let rldp = self.rldp.clone();
            let overlay = self.overlay.clone();
            self.runtime_handle.spawn(async move { 
                let result = CatchainClient::query_via_rldp(
                    &overlay_id,
                    &overlay,
                    &rldp,
                    timeout,
                    &receiver,
                    &msg,
                    max_answer_size).await;
                log::info!(target: Self::TARGET, "send_query_via_rldp: {:?}", result);
                response_callback(result);
            });
    }

    /// Send broadcast
    fn send_broadcast_fec_ex(&self, _sender_id : &PublicKeyHash, _send_as : &PublicKeyHash, payload : BlockPayloadPtr) {
        let msg = payload.clone();
        let overlay_id = self.overlay_id.clone();
        let overlay = self.overlay.clone();
        let local_validator_key = self.local_validator_key.clone();

        self.runtime_handle.spawn(async move {
            let result = overlay.broadcast(&overlay_id, &msg.data().0, Some(&local_validator_key)).await;

            log::trace!(target: Self::TARGET, "send_broadcast_fec_ex status: {:?}", result);
        });
    }
}

struct CatchainClientConsumer {
    catchain_listener: CatchainOverlayListenerPtr,
    is_stop: AtomicBool,
    overlay_id: Arc<PrivateOverlayShortId>,
}

impl CatchainClientConsumer {
    fn new(overlay_id: Arc<PrivateOverlayShortId>, catchain_listener: CatchainOverlayListenerPtr) -> Self {
        Self {
            catchain_listener: catchain_listener,
            is_stop: AtomicBool::new(false),
            overlay_id: overlay_id
        }
    }
}

#[async_trait::async_trait]
impl QueriesConsumer for CatchainClientConsumer {
    async fn try_consume_query(&self, query: TLObject, peers: &AdnlPeers) -> Result<QueryResult> {
        
        let is_stop = self.is_stop.load(atomic::Ordering::Relaxed);
        if is_stop {
            log::warn!(target: CatchainClient::TARGET, "Overlay {} was stopped!", &self.overlay_id);
            fail!("Overlay {} was stopped!", &self.overlay_id);
        }
        let now = Instant::now();
        let (wait, mut queue_reader) = Wait::new();
        if let Some(listener) = self.catchain_listener.upgrade() {
            let wait = wait.clone();
            wait.request();
            listener
                .on_query(
                    peers.other().clone(),
                    &catchain::CatchainFactory::create_block_payload(::ton_api::ton::bytes(serialize(&query)?)),
                    Box::new(move |result: Result<BlockPayloadPtr> | {
                        let result = match result {
                            Ok(answer) => {
                                 match Deserializer::new(&mut Cursor::new(answer.data().clone().0)).read_boxed::<TLObject>() {
                                    Ok(query_result) => { Ok(QueryResult::Consumed(Some(Answer::Object(query_result)))) },
                                    Err(e) => Err(e)
                                }
                            },
                            Err(e) => Err(e)
                        };
                        wait.respond(Some(result));
                    })
                );
        }
        let res = match wait.wait(&mut queue_reader, true).await {
            Some(None) => fail!("Answer was not set!"),
            Some(Some(answer)) => answer,
            None => {
                log::warn!(target: CatchainClient::TARGET, "Waiting returned an internal error (query: {:?})", query);
                fail!("Waiting returned an internal error!");
            }
        };
        if log::log_enabled!(log::Level::Trace) {
            let elapsed = now.elapsed();
            log::trace!(target: CatchainClient::TARGET, "query elapsed: {}", elapsed.as_millis());
        };
        res
    }
}
