/*
* Copyright (C) 2019-2023 TON Labs. All Rights Reserved.
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

use std::sync::Arc;
use std::sync::Weak;
use std::collections::HashMap;
use std::sync::Mutex;
use std::any::Any;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use weak_self::WeakSelf;
use crate::instrument;
use crate::check_execution_time;
use crate::CatchainOverlayManagerPtr;
use crate::PublicKeyHash;
use crate::CatchainNode;
use crate::CatchainOverlayLogReplayListenerPtr;
use crate::PrivateOverlayShortId;
use crate::CatchainOverlayListenerPtr;
use crate::CatchainOverlayPtr;
use crate::CatchainOverlayManager;
use crate::Result;
use crate::CatchainOverlay;
use crate::BlockPayloadPtr;
use crate::ExternalQueryResponseCallback;
use crate::CatchainFactory;

/*
    Implementation details for InMemoryOverlay
*/

struct InMemoryOverlay {
    overlay_short_id: Arc<PrivateOverlayShortId>,
    overlay_listener: CatchainOverlayListenerPtr,
    overlay_manager: Weak<InMemoryOverlayManager>,
    local_id: PublicKeyHash,
}

impl CatchainOverlay for InMemoryOverlay {
    /// Send message to multiple sources
    fn send_message_multicast(
        &self,
        receiver_ids: &[PublicKeyHash],
        sender_id: &PublicKeyHash,
        message: &BlockPayloadPtr,
    ) {
        for receiver_id in receiver_ids {
            self.send_message(receiver_id, sender_id, message);
        }
    }

    /// Send message
    fn send_message(
        &self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        message: &BlockPayloadPtr,
    ) {
        let overlay_manager_weak = self.overlay_manager.clone();
        let overlay_short_id = self.overlay_short_id.clone();
        let receiver_id = receiver_id.clone();
        let sender_id = sender_id.clone();
        let message = message.clone();
        let closure = move || {
            if let Some(overlay_manager) = overlay_manager_weak.upgrade() {
                overlay_manager.send_message(
                    &overlay_short_id,
                    &receiver_id,
                    &sender_id,
                    &message,
                );
            }
        };
        if let Some(overlay_manager) = self.overlay_manager.upgrade() {
            overlay_manager.post_closure(closure);
        }
    }

    /// Send query
    fn send_query(
        &self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        _name: &str,
        timeout: std::time::Duration,
        message: &BlockPayloadPtr,
        response_callback: ExternalQueryResponseCallback,
    ) {
        let overlay_manager_weak = self.overlay_manager.clone();
        let overlay_short_id = self.overlay_short_id.clone();
        let receiver_id = receiver_id.clone();
        let sender_id = sender_id.clone();
        let timeout = std::time::SystemTime::now() + timeout;
        let message = message.clone();
        let closure = move || {
            if let Some(overlay_manager) = overlay_manager_weak.upgrade() {
                overlay_manager.send_query(
                    &overlay_short_id,
                    &receiver_id,
                    &sender_id,
                    timeout,
                    &message,
                    response_callback,
                );
            }
        };
        if let Some(overlay_manager) = self.overlay_manager.upgrade() {
            overlay_manager.post_closure(closure);
        }
    }

    /// Send query via RLDP (ADNL ID of the current node should be registered for the query)
    fn send_query_via_rldp(
        &self,
        dst_adnl_id: PublicKeyHash,
        name: String,
        response_callback: ExternalQueryResponseCallback,
        timeout: std::time::SystemTime,
        query: BlockPayloadPtr,
        _max_answer_size: u64,
    ) {
        let timeout = timeout.duration_since(std::time::SystemTime::now()).unwrap_or_else(|_| std::time::Duration::from_secs(0));

        self.send_query(
            &dst_adnl_id,
            &self.local_id,
            name.as_str(),
            timeout,
            &query,
            response_callback,
        );
    }

    /// Send broadcast
    fn send_broadcast_fec_ex(
        &self,
        _sender_id: &PublicKeyHash,
        send_as: &PublicKeyHash,
        payload: BlockPayloadPtr,
    ) {
        let overlay_manager_weak = self.overlay_manager.clone();
        let overlay_short_id = self.overlay_short_id.clone();
        let send_as = send_as.clone();
        let payload = payload.clone();
        let closure = move || {
            if let Some(overlay_manager) = overlay_manager_weak.upgrade() {
                overlay_manager.send_broadcast(
                    &overlay_short_id,
                    &send_as,
                    payload,
                );
            }
        };
        if let Some(overlay_manager) = self.overlay_manager.upgrade() {
            overlay_manager.post_closure(closure);
        }
    }

    /// Implementation specific
    fn get_impl(&self) -> &dyn Any {
        self
    }
}

impl Drop for InMemoryOverlay {
    fn drop(&mut self) {
        log::debug!(
            "InMemoryOverlay::drop: overlay has been dropped local_id={}, overlay_short_id={}",
            self.local_id,
            self.overlay_short_id
        );
    }
}

impl InMemoryOverlay {
    /// Create new overlay
    pub fn create(
        overlay_manager: Weak<InMemoryOverlayManager>,
        local_id: &PublicKeyHash,
        overlay_short_id: &Arc<PrivateOverlayShortId>,
        overlay_listener: CatchainOverlayListenerPtr,
    ) -> Arc<InMemoryOverlay> {
        log::debug!(
            "InMemoryOverlay::create: overlay has been created local_id={}, overlay_short_id={}",
            local_id,
            overlay_short_id
        );

        Arc::new(InMemoryOverlay {
            overlay_short_id: overlay_short_id.clone(),
            overlay_listener,
            overlay_manager,
            local_id: local_id.clone(),
        })
    }

    /// Incoming message processing
    fn on_message(&self, adnl_id: PublicKeyHash, data: &BlockPayloadPtr) {
        if let Some(listener) = self.overlay_listener.upgrade() {
            listener.on_message(adnl_id, data);
        }
    }

    /// Incoming broadcast processing
    fn on_broadcast(&self, source_key_hash: PublicKeyHash, data: &BlockPayloadPtr) {
        if let Some(listener) = self.overlay_listener.upgrade() {
            listener.on_broadcast(source_key_hash, data);
        }
    }

    /// Incoming query processing
    fn on_query(
        &self,
        adnl_id: PublicKeyHash,
        data: &BlockPayloadPtr,
        response_callback: ExternalQueryResponseCallback,
    ) {
        if let Some(listener) = self.overlay_listener.upgrade() {
            listener.on_query(adnl_id, data, response_callback);
        }
    }
}

/*
    Implementation details for InMemoryOverlayGroup
*/

struct InMemoryOverlayGroup {
    overlay_short_id: Arc<PrivateOverlayShortId>, //overlay short id
    overlays: Mutex<HashMap<PublicKeyHash, Arc<InMemoryOverlay>>>, //overlays
}

impl Drop for InMemoryOverlayGroup {
    fn drop(&mut self) {
        log::debug!(
            "InMemoryOverlayGroup::drop: overlay group has been dropped overlay_short_id={}",
            self.overlay_short_id
        );
    }
}

impl InMemoryOverlayGroup {
    /// Create new overlay group
    fn create(overlay_short_id: Arc<PrivateOverlayShortId>) -> Arc<InMemoryOverlayGroup> {
        log::debug!("InMemoryOverlayGroup::create: overlay group has been created overlay_short_id={}", overlay_short_id);

        Arc::new(InMemoryOverlayGroup {
            overlay_short_id,
            overlays: Mutex::new(HashMap::new()),
        })
    }

    /// Get overlay
    fn get_overlay(&self, local_id: &PublicKeyHash) -> Option<Arc<InMemoryOverlay>> {
        if let Some(overlays) = self.overlays.lock().ok() {
            if let Some(overlay) = overlays.get(local_id) {
                return Some(overlay.clone());
            }
        }

        None
    }

    /// Get overlays
    fn get_overlays(&self) -> Vec<Arc<InMemoryOverlay>> {
        if let Some(overlays) = self.overlays.lock().ok() {
            return overlays.values().cloned().collect();
        }

        Vec::new()
    }
}

/*
    Implementation details for InMemoryOverlayManager
*/

pub(crate) struct InMemoryOverlayManager {
    weak_self: WeakSelf<InMemoryOverlayManager>, //weak pointer to itself
    queue_sender: crossbeam::channel::Sender<Box<dyn FnOnce() + Send>>, //queue from outer world to the InMemoryOverlayManager
    should_stop_flag: Arc<AtomicBool>, //flag which signals that the InMemoryOverlayManager should be stopped
    is_stopped_flag: Arc<AtomicBool>,  //flag which signals that the InMemoryOverlayManager has been stopped
    overlay_groups: Mutex<HashMap<Arc<PrivateOverlayShortId>, Arc<InMemoryOverlayGroup>>>, //overlay groups
}

impl CatchainOverlayManager for InMemoryOverlayManager {
    /// Create new overlay
    fn start_overlay(
        &self,
        local_id: &PublicKeyHash,
        overlay_short_id: &Arc<PrivateOverlayShortId>,
        _nodes: &Vec<CatchainNode>,
        overlay_listener: CatchainOverlayListenerPtr,
        _log_replay_listener: CatchainOverlayLogReplayListenerPtr,
    ) -> Result<CatchainOverlayPtr> {
        log::debug!(
            "InMemoryOverlayManager::start_overlay: local_id={}, overlay_short_id={}",
            local_id,
            overlay_short_id
        );

        let overlay_group = {
            if let Some(mut overlay_groups) = self.overlay_groups.lock().ok() {
                if let Some(overlay_group) = overlay_groups.get(overlay_short_id) {
                    overlay_group.clone()
                } else {
                    let overlay_group = InMemoryOverlayGroup::create(overlay_short_id.clone());
                    overlay_groups.insert(overlay_short_id.clone(), overlay_group.clone());
                    overlay_group
                }
            } else {
                return Err(failure::format_err!("Can't lock overlay groups").into());
            }
        };

        let overlay = InMemoryOverlay::create(
            self.get_weak(),
            local_id,
            overlay_short_id,
            overlay_listener,
        );

        if let Some(mut overlays) = overlay_group.overlays.lock().ok() {
            overlays.insert(local_id.clone(), overlay.clone());
        } else {
            return Err(failure::format_err!("Can't lock overlay group").into());
        }

        Ok(overlay as CatchainOverlayPtr)
    }

    /// Stop existing overlay
    fn stop_overlay(
        &self,
        overlay_short_id: &Arc<PrivateOverlayShortId>,
        overlay: &CatchainOverlayPtr,
    ) {
        log::debug!(
            "InMemoryOverlayManager::stop_overlay: overlay_short_id={}",
            overlay_short_id
        );

        if let Some(overlay_group) = self.get_overlay_group(overlay_short_id) {
            if let Some(overlay) = overlay.get_impl().downcast_ref::<InMemoryOverlay>() {
                if let Some(mut overlays) = overlay_group.overlays.lock().ok() {
                    overlays.remove(&overlay.local_id);

                    if overlays.is_empty() {
                        if let Some(mut overlay_groups) = self.overlay_groups.lock().ok() {
                            overlay_groups.remove(overlay_short_id);
                        }
                    }
                }
            }
        }
    }
}

impl Drop for InMemoryOverlayManager {
    fn drop(&mut self) {
        log::debug!("Dropping InMemoryOverlayManager...");
        self.stop();
    }
}

lazy_static::lazy_static! {
    static ref IN_MEMORY_OVERLAY_MANAGER: Mutex<Weak<InMemoryOverlayManager>> = {
        Mutex::new(Weak::new())
    };
}

impl InMemoryOverlayManager {
    /// Get reference to itself
    fn get_weak(&self) -> Weak<InMemoryOverlayManager> {
        self.weak_self.get()
    }

    /// Get instance
    pub(crate) fn get_instance() -> Result<CatchainOverlayManagerPtr> {
        let instance = IN_MEMORY_OVERLAY_MANAGER.lock();

        let mut instance = instance.map_err(|err| {
            failure::format_err!("Can't lock InMemoryOverlayManager instance: {}", err)
        })?;
        
        if let Some(instance) = instance.upgrade() {
            return Ok(instance);
        }

        let new_instance = InMemoryOverlayManager::create();
        *instance = Arc::downgrade(&new_instance);

        Ok(new_instance as CatchainOverlayManagerPtr)
    }

    /// Create new overlay manager
    fn create() -> Arc<InMemoryOverlayManager> {
        log::info!("Creating InMemoryOverlayManager...");

        let (queue_sender, queue_receiver) = crossbeam::channel::unbounded();
        let should_stop_flag = Arc::new(AtomicBool::new(false));
        let is_stopped_flag = Arc::new(AtomicBool::new(false));
        
        let _main_loop = {
            let should_stop_flag = should_stop_flag.clone();
            let is_stopped_flag = is_stopped_flag.clone();
            std::thread::Builder::new()
                .name("InMemoryOverlayManager".to_string())
                .spawn(move || {
                    Self::main_loop(queue_receiver, should_stop_flag, is_stopped_flag);
                })
        };

        let overlay_manager = Arc::new(InMemoryOverlayManager {
            queue_sender,
            should_stop_flag,
            is_stopped_flag,
            overlay_groups: Mutex::new(HashMap::new()),
            weak_self: WeakSelf::new(),
        });
        overlay_manager.weak_self.init(&overlay_manager);

        log::info!("InMemoryOverlayManager has been created");

        overlay_manager
    }

    /// Stop overlay manager
    fn stop(&self) {
        log::info!("Stopping InMemoryOverlayManager...");

        self.should_stop_flag.store(true, Ordering::SeqCst);

        loop {
            if self.is_stopped_flag.load(Ordering::SeqCst) {
                break;
            }

            log::debug!("Waiting for InMemoryOverlayManager to stop...");

            std::thread::sleep(Duration::from_millis(100));
        }

        log::info!("InMemoryOverlayManager has been stopped");
    }

    /// Get overlay group
    fn get_overlay_group(&self, overlay_short_id: &Arc<PrivateOverlayShortId>) -> Option<Arc<InMemoryOverlayGroup>> {
        if let Some(overlay_groups) = self.overlay_groups.lock().ok() {
            if let Some(overlay_group) = overlay_groups.get(overlay_short_id) {
                return Some(overlay_group.clone());
            }
        }

        None
    }

    /// Get overlay
    fn get_overlay(&self, overlay_short_id: &Arc<PrivateOverlayShortId>, local_id: &PublicKeyHash) -> Option<Arc<InMemoryOverlay>> {
        if let Some(overlay_group) = self.get_overlay_group(overlay_short_id) {
            return overlay_group.get_overlay(local_id);
        }

        None
    }

    /// Send message
    fn send_message(
        &self,
        overlay_short_id: &Arc<PrivateOverlayShortId>,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        message: &BlockPayloadPtr,
    ) {
        if let Some(overlay) = self.get_overlay(overlay_short_id, receiver_id) {
            overlay.on_message(sender_id.clone(), message);
        }
    }

    /// Send query
    fn send_query(
        &self,
        overlay_short_id: &Arc<PrivateOverlayShortId>,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        timeout: std::time::SystemTime,
        message: &BlockPayloadPtr,
        response_callback: ExternalQueryResponseCallback,
    ) {
        if let Some(overlay) = self.get_overlay(overlay_short_id, receiver_id) {
            if let Ok(_timeout) = timeout.duration_since(std::time::SystemTime::now()) {
                overlay.on_query(sender_id.clone(), message, response_callback);
            }
        }
    }

    /// Send broadcast
    fn send_broadcast(
        &self,
        overlay_short_id: &Arc<PrivateOverlayShortId>,
        send_as: &PublicKeyHash,
        payload: BlockPayloadPtr,
    ) {
        if let Some(overlay_group) = self.get_overlay_group(overlay_short_id) {
            for overlay in overlay_group.get_overlays() {
                overlay.on_broadcast(send_as.clone(), &payload);
            }
        }
    }

    /// Post closure to processing thread
    fn post_closure<F>(&self, task_fn: F)
    where
        F: FnOnce(),
        F: Send + 'static,
    {
        let task = Box::new(task_fn);
        if let Err(send_error) = self.queue_sender.send(task) {
            log::error!("InMemoryOverlayManager method call error: {}", send_error);
        }
    }

    /// Main loop for in-memory overlays
    fn main_loop(
        queue_receiver: crossbeam::channel::Receiver<Box<dyn FnOnce() + Send>>,
        should_stop_flag: Arc<AtomicBool>,
        is_stopped_flag: Arc<AtomicBool>,
    ) {
        log::info!("InMemoryOverlayManager processing loop is started");

        let activity_node = CatchainFactory::create_activity_node("InMemoryOverlayManager".to_string());

        //in-memory overlay processing loop

        loop {
            activity_node.tick();

            //check if the loop should be stopped

            if should_stop_flag.load(Ordering::SeqCst) {
                break;
            }

            //handle session outgoing event with timeout

            const MAX_TIMEOUT: Duration = Duration::from_secs(1);

            let task = {
                instrument!();

                queue_receiver.recv_timeout(MAX_TIMEOUT)
            };

            if let Ok(task) = task {
                instrument!();
                check_execution_time!(100_000);

                task();
            }
        }

        //finishing routines

        log::info!("InMemoryOverlayManager loop is finished");

        drop(queue_receiver);

        is_stopped_flag.store(true, Ordering::SeqCst);
    }
}
