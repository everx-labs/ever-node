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

use crate::engine_traits::EngineAlloc;
#[cfg(feature = "telemetry")]
use crate::engine_traits::EngineTelemetry;

use adnl::{
    declare_counted, 
    common::{
        AdnlPeers, Answer, CountedObject, Counter, QueryResult, 
        TaggedByteSlice, TaggedTlObject, Wait
    },
    node::AdnlNode,
};
use ever_crypto::{KeyId, KeyOption};
use overlay::{OverlayNode, PrivateOverlayShortId, QueriesConsumer};
use catchain::{
    BlockPayloadPtr, CatchainNode, CatchainOverlay, CatchainOverlayListenerPtr,
    ExternalQueryResponseCallback, PublicKeyHash
};
use rldp::RldpNode;
use std::{
    collections::HashMap, io::Cursor, sync::{Arc, atomic::{self, AtomicBool}}, time::Instant
};
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;
use ton_api::{serialize_boxed, serialize_boxed_append, tag_from_boxed_object,
    Deserializer, IntoBoxed, ton::{ ton_node::Broadcast, TLObject }
};
use ton_types::{error, fail, Result};

declare_counted!(
    pub struct CatchainClient {
        runtime_handle : tokio::runtime::Handle,
        overlay_id: Arc<PrivateOverlayShortId>,
        overlay: Arc<OverlayNode>,
        rldp: Arc<RldpNode>,
        local_validator_key: Arc<dyn KeyOption>,
        validator_keys: HashMap<Arc<KeyId>, Arc<KeyId>>,
        consumer: Arc<CatchainClientConsumer>,
        is_stop: Arc<AtomicBool>
    }
);

impl CatchainClient {
    const TARGET:  &'static str = "catchain_network";

    pub fn new(
        runtime_handle: &tokio::runtime::Handle,
        overlay_id: &Arc<PrivateOverlayShortId>,
        overlay: &Arc<OverlayNode>,
        rldp: Arc<RldpNode>,
        nodes: &Vec<CatchainNode>,
        local_adnl_key: &Arc<dyn KeyOption>,
        local_validator_key: Arc<dyn KeyOption>,
        catchain_listener: CatchainOverlayListenerPtr,
        #[cfg(feature = "telemetry")]
        telemetry: &Arc<EngineTelemetry>,
        allocated: &Arc<EngineAlloc>
    ) -> Result<Self> {

        let mut keys = HashMap::new();
        let mut peers = Vec::new();
        let runtime_handle = runtime_handle.clone();

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

        overlay.add_private_overlay(
            Some(runtime_handle.clone()), 
            overlay_id, 
            &local_adnl_key, 
            &peers
        )?;
        let consumer = Arc::new(
            CatchainClientConsumer::new(overlay_id.clone(), catchain_listener)
        );
        overlay.add_consumer(&overlay_id, consumer.clone())?;

        let ret = CatchainClient {
            runtime_handle,
            overlay_id: overlay_id.clone(),
            overlay: overlay.clone(),
            rldp: rldp,
            local_validator_key: local_validator_key,
            validator_keys: keys,
            consumer: consumer,
            is_stop: Arc::new(AtomicBool::new(false)),
            counter: allocated.catchain_clients.clone().into()
        };
        #[cfg(feature = "telemetry")]
        telemetry.catchain_clients.update(allocated.catchain_clients.load(Ordering::Relaxed));
        Ok(ret)

    }

    pub async fn stop(&self) {
        if let Err(e) = self.overlay.delete_private_overlay(&self.overlay_id) {
            log::warn!("{:?}", e);
        }
        self.is_stop.store(true, atomic::Ordering::Relaxed);
        self.consumer.is_stop.store(true, atomic::Ordering::Relaxed);

        for worker in self.consumer.worker_waiters.iter() {
            worker.val().respond(None);
        }
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
            let buf = &msg.data().0;
            let tag = if buf.len() < 4 {
                0 
            } else {
                ((buf[3] as u32) << 24) | ((buf[2] as u32) << 16) | 
                ((buf[1] as u32) <<  8) | ((buf[0] as u32) <<  0)
            };
            let msg = TaggedByteSlice {
                object: buf,
                #[cfg(feature = "telemetry")]
                tag: 0x80000001 // Catchain one-way messages 
            };
            let answer = overlay.message(&receiver, &msg, &overlay_id).await;
            log::trace!(
                target: Self::TARGET, 
                "<send_message> (overlay: {}, data: {:08x}, key_id: {}): {:?}",
                &overlay_id, tag, &receiver, &answer
            );
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
        let query: TLObject = Deserializer::new(
            &mut Cursor::new(&message.data().0)
        ).read_boxed()?;
        #[cfg(feature = "telemetry")]
        let tag = tag_from_boxed_object(&query);
        let query = TaggedTlObject {
            object: query,
            #[cfg(feature = "telemetry")]
            tag
        };
        let now = Instant::now();
        let result = overlay.query(
            &receiver_id, 
            &query, 
            overlay_id,
            Some(AdnlNode::calc_timeout(Some(timeout.as_millis() as u64)))
        ).await?;
        let elapsed = now.elapsed();
        log::trace!(
            target: Self::TARGET, 
            "<send_query> result (overlay: {}, data: {:08x}, key_id: {}): {:?}({}ms)",
            &overlay_id, tag, &receiver_id, result, elapsed.as_millis()
        );
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
        let query_body: TLObject = Deserializer::new(
            &mut Cursor::new(&message.data().0)
        ).read_boxed()?;
        let mut query = overlay.get_query_prefix(overlay_id)?;
        //Serializer::new(&mut query).write_bare(&message.0)?;
        serialize_boxed_append(&mut query, &query_body)?;
        // let timeout = timeout.duration_since(SystemTime::now())?.as_millis();
        let result = overlay.query_via_rldp(
            rldp,
            receiver_id,
            &TaggedByteSlice {
                object: &query[..],
                #[cfg(feature = "telemetry")]
                tag: tag_from_boxed_object(&query_body)
            },
            Some(max_answer_size as i64),
            None,
            &overlay_id
        ).await;
        log::trace!(target: Self::TARGET, "result status: {}", &result.is_ok());
        let (data, _) = result?;
        let data = data.ok_or_else(|| error!("asnwer is None!"))?;
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
        catchain_listener: &CatchainOverlayListenerPtr
    ) -> Result<()> {
        let receiver = overlay.clone();
        let result: Option<Box<Broadcast>> = None;
        let catchain_listener = catchain_listener.clone();

        while let None = result {
            if self.is_stop.load(atomic::Ordering::Relaxed) {
                break;
            };
            let message = receiver.wait_for_broadcast(overlay_id).await;
            match message {
                Ok(Some(message)) => {
                    log::trace!(target: Self::TARGET, "private overlay broadcast (successed)");
                   // let src_id = validator_keys.get(&message.1).ok_or_else(|| error!("unknown key!"))?;
                    if let Some(listener) = catchain_listener.upgrade() {
                        listener.on_broadcast(
                            message.recv_from, 
                            &catchain::CatchainFactory::create_block_payload(
                                ::ton_api::ton::bytes(message.data)
                            )
                        );    // Test id!
                    }
                },
                Ok(None) => { return Ok(()) },
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
                Ok(Some((catchain_block_update, validator_session_block_update, source_id)))  => {
                    log::trace!(target: Self::TARGET, "private overlay broadcast ValidatorSession_BlockUpdate (successed)");
                    let vs_block_update = validator_session_block_update.into_boxed();
                    let block_update = catchain_block_update.into_boxed();
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
                Ok(None) => { return Ok(())},
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
        self.runtime_handle.spawn(
            async move {
                let msg = TaggedByteSlice {
                    object: &msg.data().0,
                    #[cfg(feature = "telemetry")]
                    tag: 0x80000002 // Catchain broadcast
                }; 
                let result = overlay.broadcast(&overlay_id, &msg, Some(&local_validator_key)).await;
                log::trace!(target: Self::TARGET, "send_broadcast_fec_ex status: {:?}", result);
            }
        );
    }
}

struct CatchainClientConsumer {
    catchain_listener: CatchainOverlayListenerPtr,
    is_stop: AtomicBool,
    overlay_id: Arc<PrivateOverlayShortId>,
    worker_waiters: lockfree::map::Map<u128, Arc<Wait<Result<QueryResult>>>>
}

impl CatchainClientConsumer {
    fn new(
        overlay_id: Arc<PrivateOverlayShortId>,
        catchain_listener: CatchainOverlayListenerPtr
    ) -> Self {
        Self {
            catchain_listener: catchain_listener,
            is_stop: AtomicBool::new(false),
            overlay_id: overlay_id, 
            worker_waiters: lockfree::map::Map::new()
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
        let id = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;

        let data = match serialize_boxed(&query) {
            Ok(query) => query,
            Err(e) => { 
                log::warn!("query is bad: {:?}", e);
                fail!(e)
            }
        };
        let (wait, mut queue_reader) = Wait::new();
        self.worker_waiters.insert(id.as_nanos(), wait.clone());
        
        if let Some(listener) = self.catchain_listener.upgrade() {
            let wait = wait.clone();
            wait.request();
            listener
                .on_query(
                    peers.other().clone(),
                    &catchain::CatchainFactory::create_block_payload(::ton_api::ton::bytes(data)),
                    Box::new(move |result: Result<BlockPayloadPtr> | {
                        let result = match result {
                            Ok(answer) => {
                                let answer: Result<TLObject> = Deserializer::new(
                                    &mut Cursor::new(&answer.data().0)
                                ).read_boxed();
                                match answer {
                                    Ok(answer) => { 
                                        #[cfg(feature = "telemetry")]
                                        let tag = tag_from_boxed_object(&answer);
                                        let answer = TaggedTlObject {
                                            object: answer,
                                            #[cfg(feature = "telemetry")]
                                            tag
                                        };
                                        Ok(QueryResult::Consumed(Some(Answer::Object(answer)))) 
                                    },
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
        self.worker_waiters.remove(&id.as_nanos());
        res
    }
}
