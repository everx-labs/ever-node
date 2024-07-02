/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific EVERX DEV software governing permissions and
* limitations under the License.
*/

use crate::{
    config::{ 
        ConfigEvent, ConnectivityCheckBroadcastConfig, NodeConfigHandler, NodeConfigSubscriber, TonNodeConfig
    },
    engine_traits::{EngineAlloc, OverlayOperations, PrivateOverlayOperations},
    network::{
        catchain_client::CatchainClient,
        full_node_client::{FullNodeOverlayClient, NodeClientOverlay},
        neighbours::{self, Neighbours}, remp::RempNode,
    },
    types::{awaiters_pool::AwaitersPool, spawn_cancelable},
};
#[cfg(feature = "telemetry")]
use crate::{
    engine_traits::EngineTelemetry, 
    network::telemetry::{FullNodeNetworkTelemetry, FullNodeNetworkTelemetryKind}
};

use adnl::{
    declare_counted, BroadcastSendInfo, DhtIterator, DhtNode, OverlayId, OverlayShortId, 
    OverlayNode, OverlayUtils, PrivateOverlayShortId, RldpNode,
    common::{add_counted_object_to_map, CountedObject, Counter, Subscriber, TaggedByteSlice},
    node::AdnlNode
};
use catchain::{
    CatchainNode, CatchainOverlay, CatchainOverlayListenerPtr, CatchainOverlayLogReplayListenerPtr
};
use std::{
    convert::TryInto, hash::Hash, 
    sync::{Arc, atomic::{AtomicI32, AtomicU64, AtomicBool, Ordering}}, 
    time::{Duration, SystemTime}
};
use ever_block::{error, fail, KeyId, KeyOption, Result, UInt256, BlockIdExt};
use ton_api::{
    IntoBoxed, serialize_boxed, ton::ton_node::broadcast::ConnectivityCheckBroadcast,
};
#[cfg(feature = "telemetry")]
use ton_api::tag_from_bare_type;

pub (crate) struct NetworkContext {
    pub adnl: Arc<AdnlNode>,
    pub dht: Arc<DhtNode>,
    pub overlay: Arc<OverlayNode>,
    pub rldp: Arc<RldpNode>,
    pub remp: Arc<RempNode>,
    pub broadcast_hops: Option<u8>,
    #[cfg(feature = "telemetry")]
    pub telemetry: Arc<FullNodeNetworkTelemetry>,
    #[cfg(feature = "telemetry")]
    pub engine_telemetry: Arc<EngineTelemetry>,
    pub engine_allocated: Arc<EngineAlloc>
}

pub struct NodeNetwork {
    masterchain_overlay_short_id: Arc<OverlayShortId>,
    overlays: Arc<lockfree::map::Map<Arc<OverlayShortId>, Arc<NodeClientOverlay>>>,
    network_context: Arc<NetworkContext>,
    validator_context: ValidatorContext,
    overlay_awaiters: AwaitersPool<Arc<OverlayShortId>, Arc<dyn FullNodeOverlayClient>>,
    runtime_handle: tokio::runtime::Handle,
    config_handler: Arc<NodeConfigHandler>,
    connectivity_check_config: ConnectivityCheckBroadcastConfig,
    default_rldp_roundtrip: Option<u32>,
    cancellation_token: tokio_util::sync::CancellationToken,
    #[cfg(feature = "telemetry")]
    tag_connectivity_check_broadcast: u32,
    mesh_global_configs_dir: String,
    zerostate_ids: lockfree::map::Map<i32, BlockIdExt>
}

declare_counted!(
    struct PeerContext {
        count: AtomicI32
    }
);

struct ValidatorContext {
    private_overlays: Arc<lockfree::map::Map<Arc<OverlayShortId>, Arc<CatchainClient>>>,
    actual_local_adnl_keys: Arc<lockfree::set::Set<Arc<KeyId>>>,
    all_validator_peers: Arc<lockfree::map::Map<Arc<KeyId>, Arc<PeerContext>>>,
    sets_contexts: Arc<lockfree::map::Map<UInt256, Arc<ValidatorSetContext>>>,
    current_set: Arc<lockfree::map::Map<u8, UInt256>>, // zero or one element [0]
}

//#[derive(Default)]
declare_counted!(
    struct ConnectivityStat {
        last_short_got: AtomicU64,
        last_short_latency: AtomicU64,
        last_long_got: AtomicU64,
        last_long_latency: AtomicU64
    }
);

//    #[derive(Clone)]
declare_counted!(
    struct ValidatorSetContext {
        validator_peers: Vec<Arc<KeyId>>,
        validator_key: Arc<dyn KeyOption>,
        validator_adnl_key: Arc<dyn KeyOption>,
        election_id: usize,
        connectivity_stat: Arc<lockfree::map::Map<Arc<KeyId>, ConnectivityStat>> // (last short broadcast got, last long -//-)
    }
);

impl NodeNetwork {

    pub const TAG_DHT_KEY: usize = 1;
    pub const TAG_OVERLAY_KEY: usize = 2;

    const TIMEOUT_FIND_DHT_NODES: Duration = Duration::from_secs(60);
    const TIMEOUT_FIND_OVERLAY_PEERS: Duration = Duration::from_secs(1);
    const TIMEOUT_SEARCH_VALIDATOR_KEYS: Duration = Duration::from_secs(1);
    const TIMEOUT_STORE_IP_ADDRESS: Duration = Duration::from_secs(500);
    const TIMEOUT_STORE_OVERLAY_NODE: Duration = Duration::from_secs(500);
    const TIMEOUT_UPDATE_PEERS: Duration = Duration::from_secs(5);

    pub async fn new(
        config: TonNodeConfig,
        cancellation_token: tokio_util::sync::CancellationToken,
        #[cfg(feature = "telemetry")]
        engine_telemetry: Arc<EngineTelemetry>,
        engine_allocated: Arc<EngineAlloc> 
    ) -> Result<Arc<Self>> {

        let global_config = config.load_global_config()?;
        let global_configs_dir = config.mesh_global_configs_dir();
        let masterchain_zero_state_id = global_config.zero_state()?;
        let mut connectivity_check_config = config.connectivity_check_config().clone();
        connectivity_check_config.enabled = false;
        let connectivity_check_enabled = connectivity_check_config.enabled;
        let broadcast_hops = config.extensions().broadcast_hops;

        let adnl = AdnlNode::with_config(config.adnl_node()?).await?;
        if !config.extensions().disable_compression {
            adnl.set_options(AdnlNode::OPTION_FORCE_COMPRESSION)
        }
        let dht = DhtNode::with_params(adnl.clone(), Self::TAG_DHT_KEY, None)?;
        let overlay = OverlayNode::with_adnl_node_and_zero_state(
            adnl.clone(), 
            masterchain_zero_state_id.file_hash.as_slice(),
            Self::TAG_OVERLAY_KEY
        )?;
        if config.extensions().disable_broadcast_retransmit {
            overlay.set_broadcast_retransmit(false)
        }
        let rldp = RldpNode::with_adnl_node(adnl.clone(), vec![overlay.clone()])?;
        let remp = Arc::new(RempNode::new(adnl.clone(), Self::TAG_OVERLAY_KEY)?);

        let nodes = global_config.dht_nodes()?;
        for peer in nodes.iter() {
            dht.add_peer_to_network(peer, None)?;
        }

        let masterchain_overlay_short_id = overlay.calc_overlay_short_id(
            masterchain_zero_state_id.shard().workchain_id(),
            masterchain_zero_state_id.shard().shard_prefix_with_tag() as i64,
        )?;

        let dht_key = adnl.key_by_tag(Self::TAG_DHT_KEY)?;
        NodeNetwork::periodic_store_ip_addr(dht.clone(), dht_key, None, cancellation_token.clone());

        let overlay_key = adnl.key_by_tag(Self::TAG_OVERLAY_KEY)?;
        NodeNetwork::periodic_store_ip_addr(dht.clone(), overlay_key, None, cancellation_token.clone());

        let default_rldp_roundtrip = config.default_rldp_roundtrip();

        NodeNetwork::find_dht_nodes(dht.clone(), None, cancellation_token.clone());
        let (config_handler, config_handler_context) = NodeConfigHandler::create(
            config, tokio::runtime::Handle::current()
        )?;

     //   let validator_adnl_key = adnl.key_by_tag(Self::TAG_VALIDATOR_ADNL_KEY)?;
     //   NodeNetwork::periodic_store_ip_addr(dht.clone(), validator_adnl_key);

        let zerostate_ids = lockfree::map::Map::new();
        zerostate_ids.insert(0, masterchain_zero_state_id);

        let validator_context = ValidatorContext {
            private_overlays: Arc::new(lockfree::map::Map::new()),
            actual_local_adnl_keys: Arc::new(lockfree::set::Set::new()),
            all_validator_peers: Arc::new(lockfree::map::Map::new()),
            sets_contexts: Arc::new(lockfree::map::Map::new()),
            current_set: Arc::new(lockfree::map::Map::new()),
        };

        let network_context = NetworkContext {
            adnl,
            dht,
            overlay,
            rldp,
            remp,
            broadcast_hops,
            #[cfg(feature = "telemetry")]
            telemetry: Arc::new(
                FullNodeNetworkTelemetry::new(FullNodeNetworkTelemetryKind::Client)
            ),
            #[cfg(feature = "telemetry")]
            engine_telemetry,
            engine_allocated
        };
        let network_context = Arc::new(network_context);

        let node_network = NodeNetwork {
            masterchain_overlay_short_id,
            overlays: Arc::new(lockfree::map::Map::new()),
            network_context: network_context.clone(),
            validator_context,
            overlay_awaiters: AwaitersPool::new(
                "overlay_awaiters",
                #[cfg(feature = "telemetry")]
                network_context.engine_telemetry.clone(),
                network_context.engine_allocated.clone()
            ),
            runtime_handle: tokio::runtime::Handle::current(),
            config_handler,
            default_rldp_roundtrip,
            connectivity_check_config,
            cancellation_token,
            #[cfg(feature = "telemetry")]
            tag_connectivity_check_broadcast: 
                tag_from_bare_type::<ConnectivityCheckBroadcast>(),
            mesh_global_configs_dir: global_configs_dir,
            zerostate_ids,
        };
        let node_network = Arc::new(node_network);

        NodeConfigHandler::start_sheduler(
            node_network.config_handler.clone(), 
            config_handler_context, 
            vec![node_network.clone()]
        )?;
        if connectivity_check_enabled {
           Self::connectivity_broadcasts_sender(node_network.clone());
           Self::connectivity_stat_logger(node_network.clone());
        }
        Ok(node_network)

    }

    pub fn get_key_id_by_tag(&self, tag: usize) -> Result<Arc<KeyId>> {
        let key_id = self.network_context.adnl.key_by_tag(tag)?;
        Ok(key_id.id().clone())
    }

    pub fn config_handler(&self) -> Arc<NodeConfigHandler> {
        self.config_handler.clone()
    }

    pub async fn delete_overlays(&self) {
        for guard in self.overlays.iter() {
            log::info!("Deleting overlay {}", guard.key());
            guard.val().peers().stop().await;
            match guard.val().overlay().delete_public_overlay(guard.key()) {
                Ok(result) => log::info!("Deleted overlay {} ({})", guard.key(), result),
                Err(e) => log::warn!("Deleting overlay {}: {}", guard.key(), e)
            }
        }
    }

    pub fn log_neighbors_stat(&self) {
        for guard in self.overlays.iter() {
            guard.val().peers().log_neighbors_stat();
        }
    }

    pub async fn stop_adnl(&self) {
        log::info!("Stopping node network loops...");
        self.cancellation_token.cancel();
        log::info!("Node network loops stopped. Stopping adnl...");
        self.network_context.adnl.stop().await;
        log::info!("Stopped adnl");
    }

    fn try_add_new_elem<K: Hash + Ord + Clone, T: CountedObject>(
        &self,
        map: &Arc<lockfree::map::Map<K, Arc<T>>>,
        id: &K,
        factory: impl FnMut() -> Result<Arc<T>>
    ) -> Result<Arc<T>> {
        if let Some(found) = map.get(id) { 
            Ok(found.val().clone())
        } else { 
            add_counted_object_to_map(map, id.clone(), factory)?;
            if let Some(found) = map.get(id) { 
                Ok(found.val().clone())
            } else {
                fail!("Cannot add element to map") 
            } 
        }
    }

    fn periodic_store_ip_addr(
        dht: Arc<DhtNode>,
        node_key: Arc<dyn KeyOption>,
        validator_keys: Option<Arc<lockfree::set::Set<Arc<KeyId>>>>,
        cancellation_token: tokio_util::sync::CancellationToken
    ) {
        spawn_cancelable(
            cancellation_token,
            async move {
                loop {
                    if let Err(e) = DhtNode::store_ip_address(&dht, &node_key).await {
                        log::warn!("store ip address ERROR: {}", e)
                    }
                    tokio::time::sleep(Self::TIMEOUT_STORE_IP_ADDRESS).await;
                    if let Some(actual_validator_adnl_keys) = validator_keys.as_ref() {
                        if actual_validator_adnl_keys.get(node_key.id()).is_none() {
                            log::info!("store ip address finished (for key {}).", node_key.id());
                            break
                        }
                    }
                }
            }
        )
    }

    fn periodic_store_overlay_node(
        dht: Arc<DhtNode>, 
        overlay_id: OverlayId,
        overlay_node: ton_api::ton::overlay::node::Node,
        cancellation_token: tokio_util::sync::CancellationToken
    ) {
        spawn_cancelable(
            cancellation_token,
            async move {
                loop {
                    let res = DhtNode::store_overlay_node(&dht, &overlay_id, &overlay_node).await;
                    log::info!("overlay_store status: {:?}", res);
                    tokio::time::sleep(Self::TIMEOUT_STORE_OVERLAY_NODE).await;
                }
            }
        )
    }

    fn process_overlay_peers(
        neighbours: Arc<Neighbours>,
        dht: Arc<DhtNode>,
        network_id: Option<i32>,
        overlay: Arc<OverlayNode>,
        overlay_id: Arc<OverlayShortId>,
        cancellation_token: tokio_util::sync::CancellationToken
    ) {
        spawn_cancelable(
            cancellation_token,
            async move {
                loop {
                    let got_peers =
                      Self::add_overlay_peers(&neighbours, &dht, network_id, &overlay, &overlay_id).await.
                        unwrap_or_else(|e| { log::warn!("add_overlay_peers: {}", e); false } );
                    if got_peers {
                        // Do not sleep, get next one from queue
                        tokio::task::yield_now().await;
                    } else {
                        // No peers in queue, sleep to wait
                        tokio::time::sleep(Self::TIMEOUT_FIND_OVERLAY_PEERS).await;
                    }
                }
            }
        )
    }

    async fn add_overlay_peers(
        neighbours: &Arc<Neighbours>,
        dht: &Arc<DhtNode>,
        network_id: Option<i32>,
        overlay: &Arc<OverlayNode>,
        overlay_id: &Arc<OverlayShortId>
    ) -> Result<bool> {
        match overlay.wait_for_peers(&overlay_id).await? {
            None => Ok(false),
            Some(peers) => {
                for peer in peers.iter() {
                    let peer_key: Arc<dyn KeyOption> = (&peer.id).try_into()?;
                    if neighbours.contains_overlay_peer(peer_key.id()) {
                        continue;
                    }
                    if let Some((ip, _)) = DhtNode::find_address_in_network(dht, peer_key.id(), network_id).await? {
                        overlay.add_public_peer(&ip, peer, overlay_id)?;
                        if neighbours.add_overlay_peer(peer_key.id().clone()) {
                            log::trace!("add_overlay_peers: add overlay peer {:?}, address: {}", peer, ip);
                        }
                    }
                }
                Ok(true)
            },
        }
    }

/*  
    fn save_peers(
        &self,
        overlay_id: &Arc<OverlayShortId>,
        peers: &Vec<AdnlNodeConfig>
    ) -> Result<()> {
        let id = base64_encode(overlay_id.data());
        let id = self.string_to_static_str(id);

        let mut json : Vec<String> = vec![];
        for peer in peers.iter() {

            let keys = adnl_node_compatibility_key!(
                Self::TAG_OVERLAY_KEY,
                base64_encode(
                    peer.key_by_tag(
                        Self::TAG_OVERLAY_KEY)?
                    .pub_key()?
                    )
            ).to_string();

            let config = adnl_node_test_config!(peer.ip_address(), keys).to_string();
            json.push(
                config
            );
        }
        let raw_data_peers = serde_json::to_vec(&json)?;
        self.db.store_node_state(id, raw_data_peers)?;
        Ok(())
    }

*/

    async fn update_overlay_peers(
        &self,
        network_id: Option<i32>,
        overlay_id: &Arc<OverlayShortId>,
        iter: &mut Option<DhtIterator>
    ) -> Result<Vec<Arc<KeyId>>> {
        let token = self.cancellation_token.clone();
        log::info!("Overlay {} node search in progress...", overlay_id);
        let nodes = tokio::select!{
            nodes = DhtNode::find_overlay_nodes_in_network(
                &self.network_context.dht, 
                overlay_id, 
                iter,
                network_id,
            ) => nodes,
            _ = token.cancelled() => fail!("Overlay {} node search cancelled", overlay_id)
        }?;
        log::trace!("Found overlay {} nodes ({}):", overlay_id, nodes.len());
        let mut ret = Vec::new();
        for (ip, node) in nodes.iter() {
            log::trace!("Node: {:?}, address: {}", node, ip);
            let peer = self.network_context.overlay.add_public_peer(ip, node, overlay_id)?;
            if let Some(peer) = peer {
                ret.push(peer);
            }
        }
        Ok(ret)
    }

    async fn update_peers(
        &self,
        client_overlay: &Arc<NodeClientOverlay>,
        network_id: Option<i32>,
        iter: &mut Option<DhtIterator>
    ) -> Result<()> {
        let mut peers = self.update_overlay_peers(network_id, client_overlay.overlay_id(), iter).await?;
        while let Some(peer) = peers.pop() {
            client_overlay.peers().add(peer)?;
        }
/*           
        // let nodes = self.find_overlay_nodes(client_overlay.overlay_id()).await?;
        let nodes = DhtNode::find_overlay_nodes(&self.dht, client_overlay.overlay_id(), iter).await?;

        log::info!("Found overlay ({:?}) nodes count: {}", client_overlay.overlay_id(), nodes.len());
        for (ip, key) in nodes.iter() {
            //let key = peer.key_by_tag(NodeNetwork::TAG_OVERLAY_KEY)?;
            if client_overlay.peers().contains(key.id()) {
                continue;
            }

            let mut peers = self.get_peers_from_storage(client_overlay.overlay_id())?
                .ok_or_else(|| error!("Load peers from storage is fail!")
            )?;

            log::info!("new node: {:?}", &key.id());
            let overlay_peer = client_overlay
                .overlay()
                .add_peer(
                    ip,
                    &key,
                    client_overlay.overlay_id()
                )?;

            peers.push(AdnlNodeConfig::from_ip_address_and_key(
                &ip.to_string(),
                KeyOption::from_type_and_public_key(
                    key.type_id(),
                    key.pub_key()?
                ),
                NodeNetwork::TAG_OVERLAY_KEY
            )?);

            client_overlay.peers().add(overlay_peer)?;
            self.save_peers(&client_overlay.overlay_id(), &peers)?;
        }
*/
        Ok(())
    }

    fn find_dht_nodes(
        dht: Arc<DhtNode>, 
        network_id: Option<i32>,
        cancellation_token: tokio_util::sync::CancellationToken
    ) {
        spawn_cancelable(
            cancellation_token,
            async move {
                loop {
                    let mut iter = None;
                    loop {
                        match dht.get_known_peer_of_network(&mut iter, network_id) {
                            Err(e) => {
                                log::warn!(
                                    "get_known_peer_of_network {} result: {:?}",
                                    network_id.unwrap_or_default(), e
                                );
                                break;
                            }
                            Ok(None) => break,
                            Ok(Some(id)) => {
                                if let Err(e) = dht.find_dht_nodes_in_network(&id, network_id).await {
                                    log::warn!(
                                        "find_dht_nodes_in_network {} result: {:?}",
                                        network_id.unwrap_or_default(), e
                                    )
                                }
                            }
                        }
                    }
                    tokio::time::sleep(Self::TIMEOUT_FIND_DHT_NODES).await;
                }
            }
        )
    }

    fn start_update_peers(
        self: Arc<Self>,
        client_overlay: &Arc<NodeClientOverlay>,
        network_id: Option<i32>,
    ) {
        let client_overlay = client_overlay.clone();
        spawn_cancelable(
            self.cancellation_token.clone(),
            async move {
                let mut iter = None;
                loop {
                    log::trace!("find overlay nodes by dht...");
                    if let Err(e) = self.update_peers(&client_overlay, network_id, &mut iter).await {
                        log::warn!("Error find overlay nodes by dht: {}", e);
                    }
                    if client_overlay.peers().count() >= neighbours::MAX_NEIGHBOURS {
                        log::trace!("finish find overlay nodes.");
                        break;
                    }
                    tokio::time::sleep(Self::TIMEOUT_UPDATE_PEERS).await;
                }
            }
        )
    }

    async fn get_overlay_worker(
        self: Arc<Self>,
        network_id: Option<i32>,
        overlay_id: (Arc<OverlayShortId>, OverlayId),
        local: bool,
    ) -> Result<Arc<dyn FullNodeOverlayClient>> {

        let (overlay_id_short, overlay_id_full) = overlay_id;
        if local {
            self.network_context.overlay.add_local_workchain_overlay(
                Some(self.runtime_handle.clone()), 
                &overlay_id_short,
                self.network_context.broadcast_hops
            )?;
        } else {
            self.network_context.overlay.add_other_workchain_overlay(
                Some(self.runtime_handle.clone()), 
                &overlay_id_short,
                self.network_context.broadcast_hops
            )?;
        }

        let node = self.network_context.overlay.get_signed_node(&overlay_id_short)?;
        NodeNetwork::periodic_store_overlay_node(
            self.network_context.dht.clone(),
            overlay_id_full, 
            node,
            self.cancellation_token.clone(),
        );

        let peers = self.update_overlay_peers(network_id, &overlay_id_short, &mut None).await?;
        if peers.first().is_none() {
            log::warn!("No nodes were found in overlay {}", &overlay_id_short);
        }

        let neighbours = Neighbours::new(
            &peers,
            &self.network_context.dht,
            network_id,
            &self.network_context.overlay,
            overlay_id_short.clone(),
            &self.default_rldp_roundtrip,
            self.cancellation_token.clone()
        )?;

        let peers = Arc::new(neighbours);
        let client_overlay = self.try_add_new_elem(
            &self.overlays,
            &overlay_id_short, 
            || {
                let ret = NodeClientOverlay::new(
                    overlay_id_short.clone(),
                    network_id.is_none(),
                    peers.clone(),
                    &self.network_context
                );
                Ok(Arc::new(ret))
            }
        )?;

        Neighbours::start_ping(Arc::clone(&peers));
        Neighbours::start_reload(Arc::clone(&peers));
        Neighbours::start_rnd_peers_process(Arc::clone(&peers));
        NodeNetwork::start_update_peers(self.clone(), &client_overlay, network_id);
        NodeNetwork::process_overlay_peers(
            peers.clone(), 
            self.network_context.dht.clone(), 
            network_id,
            self.network_context.overlay.clone(), 
            overlay_id_short.clone(),
            self.cancellation_token.clone(),
        );
        log::info!("Started Overlay {}", &overlay_id_short);
        Ok(client_overlay as Arc<dyn FullNodeOverlayClient>)

    }

    pub fn delete_validator_keys_for_full_node(&self, validators: Vec<CatchainNode>)  -> Result<()> {
        let local_adnl = self.network_context.adnl.key_by_tag(Self::TAG_OVERLAY_KEY)?;

        for validator in validators.iter() {
            self.network_context.adnl.delete_peer(local_adnl.id(), &validator.adnl_id)?;
        }
        Ok(())
    }

    pub fn search_validator_keys_for_full_node(
        &self,
        validators: Vec<CatchainNode>,
        callback: Arc<dyn Fn(Arc<KeyId>) + Sync + Send>
    ) -> Result<Arc<AtomicBool>> {

        let dht = self.network_context.dht.clone();
        let adnl = self.network_context.adnl.clone();
        let overlay = self.network_context.overlay.clone();
        let local_adnl = self.network_context.adnl.key_by_tag(Self::TAG_OVERLAY_KEY)?;
        let local_adnl_id = local_adnl.id().clone();
        let breaker = Arc::new(AtomicBool::new(false));
        let breaker_ = breaker.clone();
        let callback = callback.clone();

        spawn_cancelable(
            self.cancellation_token.clone(),
            async move {
                let mut current_validators = validators;
                loop {
                    match Self::search_validator_keys_round(
                        local_adnl_id.clone(),
                        &adnl,
                        &dht,
                        &overlay,
                        current_validators,
                        Some(callback.clone()),
                    ).await {
                        Ok(missing_validators) => {
                            current_validators = missing_validators;
                        },
                        Err(e) => {
                            log::warn!("{:?}", e);
                            break;
                        }
                    }
                    if current_validators.is_empty() {
                        log::info!("search_validator_keys: finished.");
                        break;
                    } else {
                        log::info!(
                            "search_validator_keys: {} missing validator keys", 
                            current_validators.len()
                        );
                    }                                    
                    if breaker.load(Ordering::Relaxed) {
                        break;
                    }
                    tokio::time::sleep(Self::TIMEOUT_SEARCH_VALIDATOR_KEYS).await;
                }
            }
        );
        Ok(breaker_)

    }

    fn search_validator_keys_for_validator(
        &self,
        local_adnl_id: Arc<KeyId>,
        validators_contexts: Arc<lockfree::map::Map<UInt256, Arc<ValidatorSetContext>>>,
        validator_list_id: UInt256,
        validators: Vec<CatchainNode>,
    ) {

        let dht = self.network_context.dht.clone();
        let adnl = self.network_context.adnl.clone();
        let overlay = self.network_context.overlay.clone();

        spawn_cancelable(
            self.cancellation_token.clone(),
            async move {
                let mut current_validators = validators;
                loop {
                    match Self::search_validator_keys_round(
                        local_adnl_id.clone(),
                        &adnl,
                        &dht,
                        &overlay,
                        current_validators,
                        None,
                    ).await {
                        Ok(missing_validators) => {
                            current_validators = missing_validators;
                        },
                        Err(e) => {
                            log::warn!("{:?}", e);
                            break;
                        }
                    }
                    if current_validators.is_empty() {
                        log::info!("search_validator_keys: finished.");
                        break;
                    } else {
                        log::info!(
                            "search_validator_keys: {} missing validator keys", 
                            current_validators.len()
                        );
                    }
                    tokio::time::sleep(Self::TIMEOUT_SEARCH_VALIDATOR_KEYS).await;
                    if validators_contexts.get(&validator_list_id).is_none() {
                        break;
                    }
                }
            }
        )

    }

    async fn search_validator_keys_round<'a>(
        local_adnl_id: Arc<KeyId>,
        adnl: &'a AdnlNode,
        dht: &'a Arc<DhtNode>,
        overlay: &'a OverlayNode,
        validators: Vec<CatchainNode>,
        full_node_callback: Option<Arc<dyn Fn(Arc<KeyId>) + Sync + Send>>,
    ) -> Result<Vec<CatchainNode>> {
        let mut lost_validators = Vec::new();
        for val in validators {
            match DhtNode::find_address_in_network(dht, &val.adnl_id, None).await {
                Ok(Some((addr, key))) => {
                    log::info!("addr found: {:?}, key: {:x?}", &addr, &key);
                    match full_node_callback {
                        Some(ref callback) => {
                            adnl.add_peer(&local_adnl_id, &addr, &Arc::new(key))?;
                            callback(val.adnl_id.clone());
                        },
                        None => { overlay.add_private_peers(&local_adnl_id, vec![(addr, key)])?; }
                    }
                }
                Ok(None) => {
                    lost_validators.push(val);
                    log::warn!("find address failed");
                }
                Err(e) => { 
                    lost_validators.push(val.clone());
                    log::error!("find address failed: {:?}", e);
                }
            }
        }
        Ok(lost_validators)
    }

    fn connectivity_stat_logger(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(1000)).await;
                if let Some(validator_set_context) = self.current_validator_set_context() {

                    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs();
                    let connectivity_stat = &validator_set_context.val().connectivity_stat;
                    let mut sb = string_builder::Builder::default();
                    sb.append(format!(
                        "\n{:<54} {:>10}  {:>10}",
                        "adnl id",
                        "short",
                        "long"
                    ));
                    for guard in connectivity_stat.iter() {
                        let short = guard.val().last_short_got.load(Ordering::Relaxed);
                        let short_latency = guard.val().last_short_latency.load(Ordering::Relaxed);
                        let long = guard.val().last_long_got.load(Ordering::Relaxed);
                        let long_latency = guard.val().last_long_latency.load(Ordering::Relaxed);

                        let short_diff = if now > short { now - short } else { 0 };
                        let long_diff = if now > long { now - long } else { 0 };
                        
                        sb.append(format!(
                            "\n{:<54} {:>6},{:>3}  {:>6},{:>3}",
                            guard.key(),
                            if short == 0 { "newer".to_string() } else { short_diff.to_string() },
                            short_latency,
                            if long == 0 { "newer".to_string() } else { long_diff.to_string() },
                            long_latency
                        ));
                    }
                    log::info!(
                        "Public overlay connectivity (last connectivity broadcast got, seconds ago, latency)\n{}",
                        sb.string().unwrap_or_default()
                    );
                }
            }
        });
    }

    pub fn process_connectivity_broadcast(self: Arc<Self>, broadcast: ConnectivityCheckBroadcast) {
        if !self.connectivity_check_config.enabled {
            return;
        }
        tokio::spawn(async move {
            if let Some(validator_set_context) = self.current_validator_set_context() {
                let pub_key = KeyId::from_data(broadcast.pub_key.inner());
                let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs();

                let u64_size = std::mem::size_of::<u64>();
                let mut latency = u64::MAX;
                if broadcast.padding.len() >= u64_size {
                    if let Ok(b) = broadcast.padding[(broadcast.padding.len() - u64_size)..].try_into() {
                        let sent = u64::from_le_bytes(b);
                        latency = if sent > now {
                            sent - now
                        } else {
                            0
                        };
                    }
                }
                
                match validator_set_context.val().connectivity_stat.get(&pub_key) {
                    Some(stat) => {
                        if broadcast.padding.len() < ConnectivityCheckBroadcastConfig::LONG_BCAST_MIN_LEN {
                            stat.val().last_short_got.store(now, Ordering::Relaxed);
                            stat.val().last_short_latency.store(latency, Ordering::Relaxed);
                        } else {
                            stat.val().last_long_got.store(now, Ordering::Relaxed);
                            stat.val().last_long_latency.store(latency, Ordering::Relaxed);
                        }
                    }
                    None => {
                        log::warn!("process_connectivity_broadcast: unknown key {}", pub_key);
                    }
                }
            }
        });
    }

    pub async fn get_validator_key(
        &self,
        validators: &[CatchainNode]
    ) -> Result<Option<(Arc<dyn KeyOption>, Arc<KeyId>)>> {
        let validator_adnl_ids = self.config_handler.get_actual_validator_adnl_ids()?;
        let local_validator = validators.iter().find_map(|val| {
            if !validator_adnl_ids.contains(&val.adnl_id) {
                return None;
            }
            Some(val.clone())
        });

        if let Some(validator) = local_validator {
            let validator_key = self.config_handler
                .get_validator_key(validator.public_key.id()).await
                .ok_or_else(|| error!("validator key not found!"))?.0;

            return Ok(Some((validator_key, validator.adnl_id.clone())))
        }

        Ok(None)
    }

    #[cfg(feature = "telemetry")]
    pub fn telemetry(&self) -> &FullNodeNetworkTelemetry {
        &self.network_context.telemetry
    }

    fn current_validator_set_context<'a>(
        &'a self
    ) -> Option<lockfree::map::ReadGuard<'a, UInt256, Arc<ValidatorSetContext>>>  {
        let id = self.validator_context.current_set.get(&0)?;
        self.validator_context.sets_contexts.get(id.val())
    }

    fn connectivity_broadcasts_sender(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut big_bc_counter = 0_u8;
            loop {
                tokio::time::sleep(
                    Duration::from_millis(self.connectivity_check_config.short_period_ms)
                ).await;
                big_bc_counter += 1;

                if let Some(validator_set_context) = self.current_validator_set_context() {
                    let key_id = validator_set_context.val().validator_key.id().data();
                    match self.send_connectivity_broadcast(key_id, vec!()).await {
                        Ok(info) => log::trace!("Sent short connectivity broadcast ({})", info.send_to),
                        Err(e) => log::warn!("Error while sending short connectivity broadcast: {}", e)
                    }
                    if big_bc_counter == self.connectivity_check_config.long_mult {
                        big_bc_counter = 0;
                        match self.send_connectivity_broadcast(
                            key_id, vec!(0xfe; self.connectivity_check_config.long_len)).await
                        {
                            Ok(info) => log::trace!("Sent long connectivity broadcast ({})", info.send_to),
                            Err(e) => log::warn!("Error while sending long connectivity broadcast: {}", e)
                        }
                    }
                }
            }
        });
    }

    async fn send_connectivity_broadcast(
        &self, 
        key_id: &[u8; 32], 
        mut padding: Vec<u8>
    ) -> Result<BroadcastSendInfo> {
        if let Some(overlay) = self.overlays.get(&self.masterchain_overlay_short_id) {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            padding.extend_from_slice(&now.to_le_bytes());
            let broadcast = ConnectivityCheckBroadcast {
                pub_key: UInt256::with_array(key_id.clone()),
                padding
            };
            overlay.val().overlay().broadcast(
                &self.masterchain_overlay_short_id, 
                &TaggedByteSlice {
                    object: &serialize_boxed(&broadcast.into_boxed())?, 
                    #[cfg(feature = "telemetry")]
                    tag: self.tag_connectivity_check_broadcast
                },
                None,
                false
            ).await
        } else {
            fail!("There is not masterchain overlay")
        }
    }

    async fn load_and_store_adnl_key(&self, validator_adnl_key_id: Arc<KeyId>, election_id: i32) -> Result<bool> {
        log::info!("load_and_store_adnl_key (AddValidatorAdnlKey) id: {}.", &validator_adnl_key_id);
        if self.validator_context.actual_local_adnl_keys.get(&validator_adnl_key_id).is_none() {
            if let Err(e) = self.validator_context.actual_local_adnl_keys.insert(validator_adnl_key_id.clone()) {
                log::warn!("load_and_store_adnl_key (AddValidatorAdnlKey) error: {}", e);
            }
            match self.config_handler.get_validator_key(&validator_adnl_key_id).await {
                Some((adnl_key, _)) => {
                    let id = match self.network_context.adnl.add_key(adnl_key.clone(), election_id as usize) {
                        Ok(id) => id,
                        Err(e) => {
                            if let Ok(old_key) = self.network_context.adnl.key_by_tag(election_id as usize) {
                                if *old_key.id() != validator_adnl_key_id {
                                    log::warn!("load_and_store_adnl_key: remove old key {}, election_id: {}", 
                                        old_key.id(), election_id);
                                } else {
                                    log::warn!("load_and_store_adnl_key: remove key {}, election_id: {}", 
                                        old_key.id(), election_id);
                                }

                                let _ = self.network_context.adnl.delete_key(old_key.id(), election_id as usize);
                                self.network_context.adnl.add_key(adnl_key, election_id as usize)?
                            } else {
                                fail!("failed initialization new validator key (id: {}, election_id: {}): {:?}",
                                    validator_adnl_key_id, election_id, e);
                            }
                        }
                    };
                    NodeNetwork::periodic_store_ip_addr(
                        self.network_context.dht.clone(),
                        self.network_context.adnl.key_by_id(&id)?,
                        Some(self.validator_context.actual_local_adnl_keys.clone()),
                        self.cancellation_token.clone(),
                    );
                    log::info!(
                        "load_and_store_adnl_key (AddValidatorAdnlKey) id: {} finished.", 
                        &validator_adnl_key_id
                    );
                    return Ok(true);
                },
                None => {
                    fail!(
                        "load_and_store_adnl_key (AddValidatorAdnlKey): validator key not found (id: {})!",
                        &validator_adnl_key_id
                    );
                }
            }
        }
        Ok(false)
    }

    pub fn remp(&self) -> &RempNode {
        &self.network_context.remp
    }

    pub fn public_overlay_key(&self) -> Result<Arc<dyn KeyOption>> {
        self.network_context.adnl.key_by_tag(Self::TAG_OVERLAY_KEY)
    }
}

#[async_trait::async_trait]
impl OverlayOperations for NodeNetwork {

    fn calc_overlay_id(
        &self, 
        network_id: Option<i32>,
        workchain: i32, 
        shard: u64
    ) -> Result<(Arc<OverlayShortId>, OverlayId)> {
        let shard = shard as i64;
        let zerostate_id = self.zerostate_ids.get(&network_id.unwrap_or_default())
            .ok_or_else(|| error!("Attempt to calc overlay id for unknown network"))?;
        let zerostate_file_hash = zerostate_id.val().file_hash().as_slice();
        
        let id = OverlayUtils::calc_overlay_id(workchain, shard, zerostate_file_hash)?;
        let short_id = OverlayUtils::calc_overlay_short_id(workchain, shard, zerostate_file_hash)?;
        Ok((short_id, id))
    }

    async fn start(&self) -> Result<()> {
        log::info!(
            "start network: ip: {}, adnl_id: {}", 
            self.network_context.adnl.ip_address(), 
            self.network_context.adnl.key_by_tag(Self::TAG_OVERLAY_KEY)?.id()
        );
        AdnlNode::start(
            &self.network_context.adnl, 
            vec![
                self.network_context.dht.clone(), 
                self.network_context.overlay.clone(),
                self.network_context.remp.clone(),
                self.network_context.rldp.clone()
            ]
        ).await
    }

    async fn get_overlay(
        &self,
        overlay_id: &OverlayShortId
    ) -> Option<Arc<dyn FullNodeOverlayClient>> {
        self.overlays.get(overlay_id).map(|v| v.val().clone() as Arc<dyn FullNodeOverlayClient>)
    }

    async fn add_overlay(
        self: Arc<Self>,
        network_id: Option<i32>,
        overlay_id: (Arc<OverlayShortId>, OverlayId),
        local: bool,
    ) -> Result<()> {
        loop {
            if self.overlays.get(&overlay_id.0).is_some() {
                break;
            } else {
                let overlay_opt = self.overlay_awaiters.do_or_wait(
                    &overlay_id.0.clone(),
                    None,
                    Arc::clone(&self).get_overlay_worker(network_id, overlay_id.clone(), local)
                ).await?;
                if overlay_opt.is_some() {
                    break;
                }
            }
        }
        Ok(())
    }

    fn add_consumer(
        &self, 
        overlay_id: &Arc<OverlayShortId>, 
        consumer: Arc<dyn Subscriber>
    ) -> Result<()> {
        self.network_context.overlay.add_consumer(overlay_id, consumer)?;
        Ok(())
    }

    async fn init_mesh_network(&self, network_id: i32, zerostate: &BlockIdExt) -> Result<()> {

        let dht = &self.network_context.dht;

        if !dht.add_network(network_id)? {
            return Ok(())
        }

        self.zerostate_ids.insert(network_id, zerostate.clone());

        let global_config = loop {
            match TonNodeConfig::load_global_config_of_network(
                &self.mesh_global_configs_dir,
                network_id,
                zerostate
            ) {
                Ok(config) => break config,
                Err(e) => {
                    log::warn!("{}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        };

        let network_id = Some(network_id);
        let nodes = global_config.dht_nodes()?;
        for peer in nodes.iter() {
            dht.add_peer_to_network(peer, network_id)?;
        }
        Self::find_dht_nodes(dht.clone(), network_id, self.cancellation_token.clone());

        Ok(())
    }
}

#[async_trait::async_trait]
impl PrivateOverlayOperations for NodeNetwork {
    async fn get_validator_bls_key(&self, key_id: &Arc<KeyId>) -> Option<Arc<dyn KeyOption>> {
        self.config_handler.get_validator_bls_key(key_id).await
    }

    async fn set_validator_list(
        &self, 
        validator_list_id: UInt256,
        validators: &Vec<CatchainNode>
    ) -> Result<Option<Arc<dyn KeyOption>>> {
        log::trace!("start set_validator_list validator_list_id: {}", &validator_list_id);

        let validator_adnl_ids = self.config_handler.get_actual_validator_adnl_ids()?;
        let local_validator = validators.iter().find_map(|val| {
            if !validator_adnl_ids.contains(&val.adnl_id) {
                return None;
            }
            Some(val.clone())
        });

        let (local_validator_key, local_validator_adnl_key, election_id) = match local_validator {
            Some(validator) => {
                let validator_key_raw = self.config_handler.get_validator_key(
                    validator.public_key.id()
                ).await;
                let (validator_key, election_id) = validator_key_raw.ok_or_else(
                    || error!("validator key not found!") 
                )?;
                let validator_adnl_key = match self.network_context.adnl.key_by_id(
                    &validator.adnl_id
                ) {
                    Ok(adnl_key) => adnl_key,
                    Err(e) => {
                        // adnl key isn`t stored in two cases: 
                        // 1. First elections. Then make storing adnl key and repeat its load.
                        // 2. Internal error. In this case the error will be returned
                        log::warn!("error load adnl validator key (first attempt): {}", e);
                        if !self.load_and_store_adnl_key(
                            validator.adnl_id.clone(), 
                            election_id
                        ).await? {
                            fail!("can't load and store adnl key (id: {})", &validator.adnl_id);
                        }
                        self.network_context.adnl.key_by_id(&validator.adnl_id)?
                    }
                };
                (validator_key, validator_adnl_key, election_id as usize)
            },
            None => { return Ok(None); }
        };

        let mut peers = Vec::new();
        let mut lost_validators = Vec::new();
        let mut peers_ids = Vec::new();

        let connectivity_stat = Arc::new(lockfree::map::Map::new());

        for val in validators {
            if val.public_key.id() == local_validator_key.id() {
                continue;
            }
            peers_ids.push(val.adnl_id.clone());
            lost_validators.push(val.clone());
            add_counted_object_to_map(
                &connectivity_stat,
                val.adnl_id.clone(),
                || {
                    let ret = ConnectivityStat {
                        last_short_got: AtomicU64::new(0),
                        last_short_latency: AtomicU64::new(0),
                        last_long_got: AtomicU64::new(0),
                        last_long_latency: AtomicU64::new(0),
                        counter: self.network_context.engine_allocated.peer_stats.clone().into()
                    };
                    #[cfg(feature = "telemetry")]
                    self.network_context.engine_telemetry.peer_stats.update(
                        self.network_context.engine_allocated.peer_stats.load(Ordering::Relaxed)
                    );
                    Ok(ret)
                }
            )?;
            match self.network_context.dht.fetch_address_of_network(&val.adnl_id, None).await {
                Ok(Some((addr, key))) => {
                    log::info!("addr: {:?}, key: {:x?}", &addr, &key);
                    peers.push((addr, key));
                },
                Ok(None) => {
                    log::info!("addr: {:?} skipped.", &val.adnl_id);
                    lost_validators.push(val.clone());
                },
                Err(e) => {
                    log::error!("find address failed: {:?}", e);
                    lost_validators.push(val.clone());
                }
            }
        }

        self.network_context.overlay.add_private_peers(local_validator_adnl_key.id(), peers)?;

        let context = self.try_add_new_elem(
            &self.validator_context.sets_contexts,
            &validator_list_id.clone(),
            || {
                let ret = ValidatorSetContext {
                    validator_peers: peers_ids.clone(),
                    validator_key: local_validator_key.clone(),
                    validator_adnl_key: local_validator_adnl_key.clone(),
                    election_id: election_id.clone(),
                    connectivity_stat: connectivity_stat.clone(),
                    counter: self.network_context.engine_allocated.validator_sets.clone().into()
                };
                #[cfg(feature = "telemetry")]
                self.network_context.engine_telemetry.validator_sets.update(
                    self.network_context.engine_allocated.validator_sets.load(Ordering::Relaxed)
                );
                Ok(Arc::new(ret))
            }
        )?;

        if !lost_validators.is_empty() {
            self.search_validator_keys_for_validator(
                local_validator_adnl_key.id().clone(),
//                self.network_context.dht.clone(), 
//                self.network_context.overlay.clone(),
                self.validator_context.sets_contexts.clone(),
                validator_list_id.clone(),
                lost_validators,
            );
        }

        for peer in context.validator_peers.iter() {
            match self.validator_context.all_validator_peers.get(peer) {
                None => { 
                    self.try_add_new_elem(
                        &self.validator_context.all_validator_peers,
                        peer,
                        || {
                            let ret = PeerContext {
                                count: AtomicI32::new(0),
                                counter: self.network_context.engine_allocated.validator_peers
                                    .clone()
                                    .into()
                            };
                            #[cfg(feature = "telemetry")]
                            self.network_context.engine_telemetry.validator_peers.update(
                                self.network_context.engine_allocated.validator_peers.load(
                                    Ordering::Relaxed
                                )
                            );
                            Ok(Arc::new(ret))
                        }
                    )?;
                },
                Some(peer_context) => {
                    peer_context.val().count.fetch_add(1, Ordering::Relaxed);
                }
            }   
        }
        log::trace!("finish set_validator_list validator_list_id: {}", &validator_list_id);
        Ok(Some(context.validator_key.clone()))
    }

    fn activate_validator_list(&self, validator_list_id: UInt256) -> Result<()> {
        log::trace!("activate_validator_list {:x}", validator_list_id);
        self.validator_context.current_set.insert(0, validator_list_id);
        Ok(())
    }

    fn remove_validator_list(&self, validator_list_id: UInt256) -> Result<bool> {
        let context = self.validator_context.sets_contexts.get(&validator_list_id);
        let mut status = false;
        if let Some(context) = context {
            let adnl_key = &context.val().validator_adnl_key;
            let mut removed_peers = Vec::new();
            let mut removed_peers_from_context = Vec::new();

            for peer in context.val().validator_peers.iter() {
                match self.validator_context.all_validator_peers.get(peer) {
                    None => { 
                        removed_peers.push(peer.clone());
                    },
                    Some(peer_context) => {
                        let val = peer_context.val().count.fetch_sub(1, Ordering::Relaxed);
                        if val <= 0 {
                            removed_peers_from_context.push(peer.clone());
                            removed_peers.push(peer.clone());
                        }
                    }
                }
            }
            for peer in removed_peers_from_context.iter() {
                self.validator_context.all_validator_peers.remove(peer);
            }

            self.network_context.overlay.delete_private_peers(adnl_key.id(), &removed_peers)?;
            self.validator_context.sets_contexts.remove(&validator_list_id);
            log::trace!("remove validator list (validator key id: {})", &validator_list_id);
            status = true;
        }

        Ok(status)
    }

    fn create_catchain_client(
        &self,
        validator_list_id: UInt256,
        overlay_short_id : &Arc<PrivateOverlayShortId>,
        nodes_public_keys : &Vec<CatchainNode>,
        listener : CatchainOverlayListenerPtr,
        _log_replay_listener: CatchainOverlayLogReplayListenerPtr
    ) -> Result<Arc<dyn CatchainOverlay + Send>> {
    
        let validator_set_context = self.validator_context.sets_contexts.get(&validator_list_id)
            .ok_or_else(
                || error!("bad validator_list_id ({})!", validator_list_id.to_hex_string())
            )?;
        let adnl_key = self.network_context.adnl.key_by_tag(
            validator_set_context.val().election_id
        )?;

        let client = self.try_add_new_elem(
            &self.validator_context.private_overlays,
            overlay_short_id,
            || {
                let ret = CatchainClient::new(
                    &self.runtime_handle,
                    overlay_short_id,
                    &self.network_context,
                    nodes_public_keys,
                    &adnl_key,
                    validator_set_context.val().validator_key.clone(),
                    listener.clone()
                )?;
                Ok(Arc::new(ret))
            }
        )?;

        CatchainClient::run_wait_broadcast(
            client.clone(),
            &self.runtime_handle,
            overlay_short_id,
            &self.network_context.overlay,
            &client.validator_keys(),
            &client.catchain_listener());
        Ok(client as Arc<dyn CatchainOverlay + Send>)

    }

    fn stop_catchain_client(&self, overlay_short_id: &Arc<PrivateOverlayShortId>) {
        if let Some(catchain_client) = self.validator_context.private_overlays.remove(overlay_short_id) {
            let client = catchain_client.val().clone();
            
            self.runtime_handle.spawn(async move {
                client.stop().await;
            });
        }
    }
}

#[async_trait::async_trait]
impl NodeConfigSubscriber for NodeNetwork {
    async fn event(&self, sender: ConfigEvent) -> Result<bool> {
        match sender {
            ConfigEvent::AddValidatorAdnlKey(validator_adnl_key_id, election_id) => {
                self.load_and_store_adnl_key(validator_adnl_key_id, election_id).await
            },
// Unused         
//            ConfigEvent::RemoveValidatorAdnlKey(validator_adnl_key_id, election_id) => {
//                log::info!("config event (RemoveValidatorAdnlKey) id: {}.", &validator_adnl_key_id);
//                self.network_context.adnl.delete_key(&validator_adnl_key_id, election_id as usize)?;
//                let status = self.validator_context.actual_local_adnl_keys.remove(&validator_adnl_key_id).is_some();
//                log::info!("config event (RemoveValidatorAdnlKey) id: {} finished({}).", &validator_adnl_key_id, &status);
//                return Ok(status);
//            }
        }
    }
}
