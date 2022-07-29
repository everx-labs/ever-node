/*
* Copyright (C) 2019-2022 TON Labs. All Rights Reserved.
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

use super::block::BlockPtr;
use super::*;
use crate::network::full_node_client::FullNodeOverlayClient;
use crate::validator::validator_utils::get_adnl_id;
use adnl::common::AdnlPeers;
use adnl::common::Answer;
use adnl::common::QueryResult;
use adnl::common::TaggedTlObject;
use catchain::PublicKeyHash;
use futures::future::join_all;
use log::*;
use overlay::OverlayId;
use overlay::OverlayShortId;
use overlay::QueriesConsumer;
use spin::mutex::SpinMutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use ton_api::tag_from_boxed_object;
use ton_api::ton::ton_node::blockcandidatestatus::BlockCandidateStatus;
use ton_api::ton::TLObject;
use ton_api::IntoBoxed;
use ton_types::Result;
use tokio::time::timeout;

//TODO: remove dependency from FullNodeClient? (use public overlay directly)
//TODO: derived trait from FullNodeClient with extra methods for new consensus

/*
===============================================================================
    Constants
===============================================================================
*/

const MAX_MC_SENDING_ATTEMPTS_COUNT: usize = 5; //max attempts to send message to MC node

/*
===============================================================================
    Listener for MC overlay events
===============================================================================
*/

pub trait McOverlayListener: Send + Sync {
    /// Block status has been updated
    fn on_mc_block_status_updated(&self, block_status: BlockCandidateStatus) -> Result<BlockPtr>;
}

/*
===============================================================================
    MC overlay
===============================================================================
*/

pub struct McOverlay {
    runtime: tokio::runtime::Handle,       //runtime for spawns
    listener: Weak<dyn McOverlayListener>, //listener of overlay's events
    workchain_id: i32,                     //workchain identifier
    mc_validators: Vec<ValidatorDescr>,    //MC validators
    overlay_short_id: Arc<OverlayShortId>, //private overlay short ID
    client: Arc<dyn FullNodeOverlayClient>, //overlay for MC interactions
    stop_flag: Arc<AtomicBool>, //atomic flag to indicate that all processing threads should be stopped
    listen_broadcasts_stopped: Arc<AtomicBool>, //atomic flag to indicate that listen_broadcasts worker has been stopped
    query_listener: SpinMutex<Option<Arc<McOverlayQueryListener>>>, //listener for overlay queries
}

impl McOverlay {
    /// Create new MC overlay
    pub async fn create(
        workchain_id: i32,
        listener: Weak<dyn McOverlayListener>,
        mc_validators: Vec<ValidatorDescr>,
        engine: &EnginePtr,
        runtime: tokio::runtime::Handle,
    ) -> Result<Arc<Self>> {
        const MC_WORKCHAIN_ID : i32 = -1;
        let (overlay_short_id, overlay_id) =
            engine.calc_overlay_id(MC_WORKCHAIN_ID, ton_block::SHARD_FULL)?;

        log::debug!(target: "verificator", "Creating verification workchain's #{} MC overlay {}", workchain_id, overlay_short_id);
            
        let overlay_id = Self::decorate_overlay_id(overlay_short_id, overlay_id);
        let client = engine.get_custom_overlay(overlay_id.clone()).await?;
        let stop_flag = Arc::new(AtomicBool::new(false));
        let listen_broadcasts_stopped = Arc::new(AtomicBool::new(true));

        let overlay = Self {
            listener,
            workchain_id,
            overlay_short_id: overlay_id.0,
            client,
            stop_flag,
            listen_broadcasts_stopped,
            mc_validators,
            query_listener: SpinMutex::new(None),
            runtime,
        };
        let overlay = Arc::new(overlay);

        if workchain_id == -1 {
            //attach listener

            *overlay.query_listener.lock() = Some(McOverlayQueryListener::create(overlay.clone())?);
        }

        log::debug!(target: "verificator", "Verification workchain's #{} MC overlay {} has been created", overlay.workchain_id, overlay.overlay_short_id);

        //resolve masterchain node addresses

        overlay.client.add_new_peers(
            overlay
                .mc_validators
                .iter()
                .map(|desc| get_adnl_id(&desc))
                .collect(),
        );

        if workchain_id == -1 {
            //subscibe to broadcasts

            overlay.listen_broadcasts();
        }

        Ok(overlay)
    }

    /// Send block status to MC node
    async fn send_block_status_to_one_node(
        client: Arc<dyn FullNodeOverlayClient>,
        receiver_id: PublicKeyHash,
        listener: Weak<dyn McOverlayListener>,
        status: ton_api::ton::ton_node::BlockCandidateStatus,
    ) {
        //todo: optimize multiple serializations of status

        let mut attempt = 0;

        loop {
            if attempt >= MAX_MC_SENDING_ATTEMPTS_COUNT {
                warn!(target: "verificator", "Sending block to MC node {} is unsuccessfull after {} attempts!", receiver_id, MAX_MC_SENDING_ATTEMPTS_COUNT);
                break;
            }

            attempt += 1;

            let response = client.send_block_status(&receiver_id, status.clone()).await;

            match response {
                Ok(response) => {
                    if let Some(response) = response {
                        if let Some(listener) = listener.upgrade() {
                            if let Err(err) = listener.on_mc_block_status_updated(response.only()) {
                                warn!(target: "verificator", "Error during response processing from MC node {}: {:?}", receiver_id, err);
                            } else {
                                trace!(target: "verificator", "BlockCandidateStatus has been delivered to MC node {}", receiver_id);
                            }
                        }
                        break;
                    }
                }
                Err(err) => {
                    warn!(target: "verificator", "Sending block to MC node {} is unsuccessfull: {:?}", receiver_id, err);
                }
            }
        }
    }

    /// Send block status to all MC nodes
    pub async fn send_block_status(&self, status: BlockCandidateStatus) {
        debug!(target: "verificator", "Start sending block status to workchain's #{} MC overlay {}: {:?}", self.workchain_id, self.overlay_short_id, status);

        let status = status.into_boxed();
        let mut futures = Vec::with_capacity(self.mc_validators.len());

        for desc in &self.mc_validators {
            let adnl_id = get_adnl_id(desc);
            //todo: optimize status cloning
            let future = Self::send_block_status_to_one_node(
                self.client.clone(),
                adnl_id,
                self.listener.clone(),
                status.clone(),
            );

            futures.push(future);
        }

        join_all(futures).await;

        debug!(target: "verificator", "Finish sending block status to workchain's #{} MC overlay {}: {:?}", self.workchain_id, self.overlay_short_id, status);
    }

    /// Listening for new MC broadcasts
    fn listen_broadcasts(&self) {
        debug!(target: "verificator", "Starting listening verification workchain's #{} MC overlay {}", self.workchain_id, self.overlay_short_id);

        let stop_flag = self.stop_flag.clone();
        let workchain_id = self.workchain_id;
        let overlay_short_id = self.overlay_short_id.clone();
        let client = self.client.clone();
        let listen_broadcasts_stopped = self.listen_broadcasts_stopped.clone();
        let listener = self.listener.clone();
        let runtime_handle = self.runtime.clone();

        self.runtime.spawn(async move {
            listen_broadcasts_stopped.store(false, Ordering::Release);

            while !stop_flag.load(Ordering::Relaxed) {
                const MAX_WAIT : std::time::Duration = std::time::Duration::from_millis(500);

                let result = match timeout(MAX_WAIT, client.wait_broadcast()).await {
                    Err(_) => continue,
                    Ok(result) => result,
                };

                match result {
                    Err(e) => {
                        error!(target: "verificator", "Error while wait_broadcast for verification workchain's #{} MC overlay {}: {}", workchain_id, &overlay_short_id, e)
                    }
                    Ok(None) => {
                        warn!(target: "verificator", "wait_broadcast finished.");
                        break;
                    }
                    Ok(Some((broadcast, src))) => {
                        if let Some(_listener) = listener.upgrade() {
                            let overlay_short_id = overlay_short_id.clone();
                            runtime_handle.spawn(async move {
                                trace!(target: "verificator", "New broadcast from {}: {:?}", src, broadcast);

                                match broadcast {
                                    broadcast => {
                                        warn!(target: "verificator", "Unexpected broadcast received in verification workchain's #{} MC overlay {}: {:?}", workchain_id, overlay_short_id, broadcast);
                                    }
                                }
                            });
                        }
                    }
                }
            }

            listen_broadcasts_stopped.store(true, Ordering::Release);

            debug!(target: "verificator", "Stopped listening verification workchain's #{} MC overlay {}", workchain_id, overlay_short_id);
        });
    }

    /// Decorate overlay ID
    fn decorate_overlay_id(
        overlay_short_id: Arc<overlay::OverlayShortId>,
        overlay_id: overlay::OverlayId,
    ) -> (Arc<OverlayShortId>, OverlayId) {
        (overlay_short_id, overlay_id)
        /*let magic_suffix = [0xff, 0xbe, 0x45, 0x23]; //magic suffix to create unique hash different from public overlay hashes
        let mut overlay_id = overlay_id.to_vec();
        let mut overlay_short_id = overlay_short_id.data().to_vec();

        overlay_id.extend_from_slice(&magic_suffix);
        overlay_short_id.extend_from_slice(&magic_suffix);

        (
            OverlayShortId::from_data(*UInt256::calc_file_hash(&overlay_short_id).as_slice()),
            *UInt256::calc_file_hash(&overlay_id).as_slice(),
        )*/
    }
}

impl Drop for McOverlay {
    fn drop(&mut self) {
        debug!(target: "verificator", "Dropping verification workchain's #{} MC overlay {}...", self.workchain_id, self.overlay_short_id);

        self.stop_flag.store(true, Ordering::Release);

        loop {
            if self.listen_broadcasts_stopped.load(Ordering::Relaxed) {
                break;
            }

            debug!(target: "verificator", "...waiting for verification workers (workchain_id={}, overlay={})", self.workchain_id, self.overlay_short_id);

            const CHECKING_INTERVAL: std::time::Duration = std::time::Duration::from_millis(300);

            std::thread::sleep(CHECKING_INTERVAL);
        }

        debug!(target: "verificator", "Verification workchain's #{} MC overlay {} has been dropped", self.workchain_id, self.overlay_short_id);
    }
}

/// Listener for overlay events
struct McOverlayQueryListener {
    overlay_short_id: Arc<OverlayShortId>, //private overlay short ID
    workchain_id: i32,                     //workchain ID
    listener: Weak<dyn McOverlayListener>, //listener of overlay's events
}

#[async_trait::async_trait]
impl QueriesConsumer for McOverlayQueryListener {
    /// MC overlay incoming queries processing
    async fn try_consume_query(&self, query: TLObject, peers: &AdnlPeers) -> Result<QueryResult> {
        let adnl_id = peers.other();
        debug!(target: "verificator", "Received query from {}", adnl_id);

        let listener = self.listener.upgrade();
        if listener.is_none() {
            warn!(target: "verificator", "Overlay {} was stopped!", &self.overlay_short_id);
            failure::bail!("Overlay {} was stopped!", &self.overlay_short_id);
        }
        let listener = listener.unwrap();

        trace!(target: "verificator", "Process query from {} for verification workchain's #{} MC overlay {}: {:?}", adnl_id, self.workchain_id, self.overlay_short_id, query);

        use ton_api::ton::ton_node::BlockCandidateStatus;

        match query.downcast::<BlockCandidateStatus>() {
            Ok(block_status) => {
                let block_status = block_status.only();
                let block = listener.on_mc_block_status_updated(block_status);

                if let Err(err) = block {
                    warn!(target: "verificator", "Error during query processing in verification workchain's #{} MC overlay: {:?}", self.workchain_id, err);
                    failure::bail!("MC overlay query processing error");
                }

                let block = block.unwrap();
                let response = TLObject::new(block.lock().status().into_boxed());
                #[cfg(feature = "telemetry")]
                let tag = tag_from_boxed_object(&response);
                let response = TaggedTlObject {
                    object: response,
                    #[cfg(feature = "telemetry")]
                    tag,
                };

                return Ok(QueryResult::Consumed(Some(Answer::Object(response))));
            }
            Err(err) => {
                warn!(target: "verificator", "Unexpected query received in verification workchain's #{} MC overlay: {:?}", self.workchain_id, err);

                failure::bail!("Unexpected query for MC overlay");
            }
        }
    }
}

impl McOverlayQueryListener {
    /// Constructor
    fn create(overlay: Arc<McOverlay>) -> Result<Arc<Self>> {
        let listener = Arc::new(Self {
            overlay_short_id: overlay.overlay_short_id.clone(),
            workchain_id: overlay.workchain_id,
            listener: overlay.listener.clone(),
        });

        overlay.client.add_query_consumer(listener.clone())?;

        Ok(listener)
    }
}
