use crate::{
    config::{NodeConfigHandler, TonNodeConfig, ConnectivityCheckBroadcastConfig},
    engine_traits::{OverlayOperations, PrivateOverlayOperations},
    network::{
        catchain_client::CatchainClient,
        full_node_client::{NodeClientOverlay, FullNodeOverlayClient},
        neighbours::{self, Neighbours}
    },
    types::awaiters_pool::AwaitersPool,
};
use adnl::{
    common::{KeyId, KeyOption, serialize}, node::{AddressCacheIterator, AdnlNode}
};
use catchain::{CatchainNode, CatchainOverlay, CatchainOverlayListenerPtr, CatchainOverlayLogReplayListenerPtr};
use dht::DhtNode;
use overlay::{OverlayId, OverlayShortId, OverlayNode, QueriesConsumer, PrivateOverlayShortId};
use rldp::RldpNode;
use std::{
    hash::Hash, 
    sync::{Arc, atomic::{AtomicU64, Ordering}}, 
    time::{Duration, SystemTime},
    convert::TryInto,
};
use ton_types::{Result, fail, error, UInt256};
use ton_block::BlockIdExt;
use ton_api::ton::{
    int256, bytes,
    ton_node::{
        broadcast::ConnectivityCheckBroadcast,
        Broadcast::{TonNode_ConnectivityCheckBroadcast}
    }
};

type Cache<K, T> = lockfree::map::Map<K, T>;

pub struct NodeNetwork {
    adnl: Arc<AdnlNode>,
    dht: Arc<DhtNode>,
    overlay: Arc<OverlayNode>,
    rldp: Arc<RldpNode>,
    masterchain_overlay_short_id: Arc<OverlayShortId>,
    masterchain_overlay_id: OverlayId,
    overlays: Arc<Cache<Arc<OverlayShortId>, Arc<NodeClientOverlay>>>,
    validator_context: ValidatorContext,
    overlay_awaiters: AwaitersPool<Arc<OverlayShortId>, Arc<dyn FullNodeOverlayClient>>,
    runtime_handle: tokio::runtime::Handle,
    config_handler: Arc<NodeConfigHandler>,
    connectivity_check_config: ConnectivityCheckBroadcastConfig,
}

struct ValidatorContext {
    private_overlays: Arc<Cache<Arc<OverlayShortId>, Arc<CatchainClient>>>,
    validator_adnl_keys: Arc<Cache<Arc<KeyId>, usize>>,   //KeyId, tag
    sets_contexts: Arc<Cache<UInt256, ValidatorSetContext>>,
    current_set: Arc<Cache<u8, UInt256>>, // zeto or one element [0]
}

#[derive(Default)]

struct ConnectivityStat {
    last_short_got: AtomicU64,
    last_short_latency: AtomicU64,
    last_long_got: AtomicU64,
    last_long_latency: AtomicU64,
}

#[derive(Clone)]
struct ValidatorSetContext {
    validator_peers: Vec<Arc<KeyId>>,
    validator_key: Arc<KeyOption>,
    validator_adnl_key: Arc<KeyOption>,
    election_id: usize,
    connectivity_stat: Arc<Cache<Arc<KeyId>, ConnectivityStat>>, // (last short broadcast got, last long -//-)
}

impl NodeNetwork {

    pub const TAG_DHT_KEY: usize = 1;
    pub const TAG_OVERLAY_KEY: usize = 2;

    const PERIOD_CHECK_OVERLAY_NODES: u64 = 1;  // seconds
    const PERIOD_STORE_IP_ADDRESS: u64 = 500;   // seconds
    const PERIOD_START_FIND_DHT_NODE: u64 = 60; // seconds
    const PERIOD_UPDATE_PEERS: u64 = 5;         // seconds

    pub async fn new(config: TonNodeConfig) -> Result<Arc<Self>> {
        let global_config = config.load_global_config()?;
        let masterchain_zero_state_id = global_config.zero_state()?;
        let mut connectivity_check_config = config.connectivity_check_config().clone();
        connectivity_check_config.enabled = false;
        let connectivity_check_enabled = connectivity_check_config.enabled;

        let adnl = AdnlNode::with_config(config.adnl_node()?).await?;
        let dht = DhtNode::with_adnl_node(adnl.clone(), Self::TAG_DHT_KEY)?;
        let overlay = OverlayNode::with_adnl_node_and_zero_state(
            adnl.clone(), 
            masterchain_zero_state_id.file_hash.as_slice(),
            Self::TAG_OVERLAY_KEY
        )?;
        let rldp = RldpNode::with_adnl_node(adnl.clone(), vec![overlay.clone()])?;

        let nodes = global_config.dht_nodes()?;
        for peer in nodes.iter() {
            dht.add_peer(peer)?;
        }

        let masterchain_overlay_id = overlay.calc_overlay_id(
            masterchain_zero_state_id.shard().workchain_id(),
            masterchain_zero_state_id.shard().shard_prefix_with_tag() as i64,
        )?;
        let masterchain_overlay_short_id = overlay.calc_overlay_short_id(
            masterchain_zero_state_id.shard().workchain_id(),
            masterchain_zero_state_id.shard().shard_prefix_with_tag() as i64,
        )?;

        let dht_key = adnl.key_by_tag(Self::TAG_DHT_KEY)?;
        NodeNetwork::periodic_store_ip_addr(dht.clone(), dht_key, None);

        let overlay_key = adnl.key_by_tag(Self::TAG_OVERLAY_KEY)?;
        NodeNetwork::periodic_store_ip_addr(dht.clone(), overlay_key, None);

        NodeNetwork::find_dht_nodes(dht.clone());
        let config_handler = Arc::new(NodeConfigHandler::new(config)?);

     //   let validator_adnl_key = adnl.key_by_tag(Self::TAG_VALIDATOR_ADNL_KEY)?;
     //   NodeNetwork::periodic_store_ip_addr(dht.clone(), validator_adnl_key);

        let validator_context = ValidatorContext {
            private_overlays: Arc::new(Cache::new()),
            validator_adnl_keys: Arc::new(Cache::new()),
            sets_contexts: Arc::new(Cache::new()),
            current_set: Arc::new(Cache::new()),
        };

        let nn = Arc::new(NodeNetwork {
            adnl,
            dht,
            overlay,
            rldp,
            masterchain_overlay_short_id,
            masterchain_overlay_id,
            overlays: Arc::new(Cache::new()),
            validator_context: validator_context,
            overlay_awaiters: AwaitersPool::new("overlay_awaiters"),
            runtime_handle: tokio::runtime::Handle::current(),
            config_handler,
            connectivity_check_config,
        });

        if connectivity_check_enabled {
           Self::connectivity_broadcasts_sender(nn.clone());
           Self::connectivity_stat_logger(nn.clone());
        }

        Ok(nn)
    }

    pub async fn stop(&self) {
        self.adnl.stop().await
    }

    pub fn config_handler(&self) -> Arc<NodeConfigHandler> {
        self.config_handler.clone()
    }

    fn try_add_new_elem<K: Hash + Ord + Clone, T: Clone>(
        &self,
        id: &K,
        value: T,
        cache: &Arc<Cache<K, T>>
    ) -> T {
        let insertion = cache.insert_with(
            id.clone(),
            |_, prev_gen_val, updated_pair | if updated_pair.is_some() {
                // other thread already added the value into map
                // so discard this insertion attempt
                lockfree::map::Preview::Discard
            } else if prev_gen_val.is_some() {
                // we added the value just now - keep this value
                lockfree::map::Preview::Keep
            } else {
                // there is not the value in the map - try to add.
                // If other thread adding value the same time - the closure will be recalled
                lockfree::map::Preview::New(value.clone())
            }
        );
        match insertion {
            lockfree::map::Insertion::Created => {
                // overlay info we loaded now was added - use it
                value
            },
            lockfree::map::Insertion::Updated(_) => {
                // unreachable situation - all updates must be discarded
                unreachable!("overlay: unreachable Insertion::Updated")
            },
            lockfree::map::Insertion::Failed(_) => {
                // othre thread's overlay info was added - get it and use
                cache.get(id).unwrap().val().clone()
            }
        }
    }

    fn periodic_store_ip_addr(
        dht: Arc<DhtNode>,
        node_key: Arc<KeyOption>,
        validator_keys: Option<Arc<Cache<Arc<KeyId>, usize>>>)
    {
        tokio::spawn(async move {
            let node_key = node_key.clone();
            loop {
                if let Err(e) = DhtNode::store_ip_address(&dht, &node_key).await {
                    log::warn!("store ip address is ERROR: {}", e)
                }
                tokio::time::sleep(Duration::from_secs(Self::PERIOD_STORE_IP_ADDRESS)).await;
                if let Some(actual_validator_adnl_keys) = validator_keys.clone() {
                    if actual_validator_adnl_keys.get(node_key.id()).is_none() {
                        break;
                    }
                }
            }
        });
    }

    fn periodic_store_overlay_node(
        dht: Arc<DhtNode>, 
        overlay_id: OverlayId,
        overlay_node: ton_api::ton::overlay::node::Node)
    {
        tokio::spawn(async move {
            let overlay_node = overlay_node;
            let overay_id = overlay_id.clone();
            loop {
                let res = DhtNode::store_overlay_node(&dht, &overay_id, &overlay_node).await;
                log::info!("overlay_store status: {:?}", res);
                tokio::time::sleep(Duration::from_secs(Self::PERIOD_STORE_IP_ADDRESS)).await;
            }
        });
    }

    fn process_overlay_peers(
        neighbours: Arc<Neighbours>,
        dht: Arc<DhtNode>,
        overlay: Arc<OverlayNode>,
        overlay_id: Arc<OverlayShortId>)
    {
        tokio::spawn(async move {
            loop {
                if let Err(e) = Self::add_overlay_peers(&neighbours, &dht, &overlay, &overlay_id).await {
                    log::warn!("add_overlay_peers: {}", e);
                };
                tokio::time::sleep(Duration::from_secs(Self::PERIOD_CHECK_OVERLAY_NODES)).await;
            }
        });
    }

    async fn add_overlay_peers(
        neighbours: &Arc<Neighbours>,
        dht: &Arc<DhtNode>,
        overlay: &Arc<OverlayNode>,
        overlay_id: &Arc<OverlayShortId>
    ) -> Result<()> {
        let peers = overlay.wait_for_peers(&overlay_id).await?;
        for peer in peers.iter() {
            let peer_key = KeyOption::from_tl_public_key(&peer.id)?;
            if neighbours.contains_overlay_peer(peer_key.id()) {
                continue;
            }
            let (ip, _) = DhtNode::find_address(dht, peer_key.id()).await?;
            overlay.add_public_peer(&ip, peer, overlay_id)?;
            neighbours.add_overlay_peer(peer_key.id().clone())?;
            log::trace!("add_overlay_peers: add overlay peer {:?}, address: {}", peer, ip);
        }
        Ok(())
    }

/*  
    fn save_peers(
        &self,
        overlay_id: &Arc<OverlayShortId>,
        peers: &Vec<AdnlNodeConfig>
    ) -> Result<()> {
        let id = base64::encode(overlay_id.data());
        let id = self.string_to_static_str(id);

        let mut json : Vec<String> = vec![];
        for peer in peers.iter() {

            let keys = adnl_node_compatibility_key!(
                Self::TAG_OVERLAY_KEY,
                base64::encode(
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

    async fn find_overlay_nodes(
        &self,
        overlay_id: &Arc<OverlayShortId>
    ) -> Result<Vec<AdnlNodeConfig>> {
        let mut addresses : Vec<AdnlNodeConfig> = vec![];
        log::trace!("Overlay node finding...");
        let mut iter = None;
        let mut actual_nodes = Vec::new();
        while actual_nodes.is_empty() {
            actual_nodes = DhtNode::find_overlay_nodes(&self.dht, overlay_id, &mut iter).await?;
        }
        if actual_nodes.len() == 0 {
            fail!("No one node were found");
        }

        log::trace!("Found overlay nodes:");
        for (ip, key) in actual_nodes.iter(){
            log::trace!("Node: {}, address: {}", key.id(), ip);
            addresses.push(
                AdnlNodeConfig::from_ip_address_and_key(
                    &ip.to_string(),
                    KeyOption::from_type_and_public_key(
                        key.type_id(),
                        key.pub_key()?
                    ),
                    NodeNetwork::TAG_OVERLAY_KEY
                )?
            );
        }
        Ok(addresses)
    }
*/

/*
    pub fn masterchain_overlay_id(&self) -> &KeyId {
        &self.masterchain_overlay_short_id
    }
*/

    pub fn get_validator_status(&self) -> bool {
        self.config_handler.get_validator_status()
    }

    async fn update_overlay_peers(
        &self, 
        overlay_id: &Arc<OverlayShortId>,
        iter: &mut Option<AddressCacheIterator>
    ) -> Result<Vec<Arc<KeyId>>> {
        log::info!("Overlay {} node search in progress...", overlay_id);
        let nodes = DhtNode::find_overlay_nodes(&self.dht, overlay_id, iter).await?;
        log::trace!("Found overlay nodes ({}):", nodes.len());
        let mut ret = Vec::new();
        for (ip, node) in nodes.iter() {
            log::trace!("Node: {:?}, address: {}", node, ip);
            if let Some(peer) = self.overlay.add_public_peer(ip, node, overlay_id)? {
                ret.push(peer);
            }
        }
        Ok(ret)
    }

    async fn update_peers(
        &self, 
        client_overlay: &Arc<NodeClientOverlay>,
        iter: &mut Option<AddressCacheIterator>
    ) -> Result<()> {
        let mut peers = self.update_overlay_peers(client_overlay.overlay_id(), iter).await?;
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
                .ok_or_else(|| failure::err_msg("Load peers from storage is fail!")
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

    fn find_dht_nodes(dht: Arc<DhtNode>) {
        tokio::spawn(async move {
            loop {
                let mut iter = None;
                while let Some(id) = dht.get_known_peer(&mut iter) {
                    if let Err(e) = dht.find_dht_nodes(&id).await {
                        log::warn!("find_dht_nodes result: {:?}", e)
                    }
                }
                tokio::time::sleep(Duration::from_secs(Self::PERIOD_START_FIND_DHT_NODE)).await;
            }
        });
    }

    fn start_update_peers(self: Arc<Self>, client_overlay: &Arc<NodeClientOverlay>) {
        let client_overlay = client_overlay.clone();
        tokio::spawn(async move {
            let mut iter = None;
            loop {
                log::trace!("find overlay nodes by dht...");
                if let Err(e) = self.update_peers(&client_overlay, &mut iter).await {
                    log::warn!("Error find overlay nodes by dht: {}", e);
                }
                if client_overlay.peers().count() >= neighbours::MAX_NEIGHBOURS {
                    log::trace!("finish find overlay nodes.");
                    return;
                }
                tokio::time::sleep(Duration::from_secs(Self::PERIOD_UPDATE_PEERS)).await;
            }
        });
    }

    async fn get_overlay_worker(
        self: Arc<Self>,
        overlay_id: (Arc<OverlayShortId>, OverlayId)
    ) -> Result<Arc<dyn FullNodeOverlayClient>> {

        self.overlay.add_shard(Some(self.runtime_handle.clone()), &overlay_id.0)?;

        let node = self.overlay.get_signed_node(&overlay_id.0)?;
        NodeNetwork::periodic_store_overlay_node(
            self.dht.clone(),
            overlay_id.1, node);

        let peers = self.update_overlay_peers(&overlay_id.0, &mut None).await?; 
        if peers.first().is_none() {
            log::warn!("No nodes were found in overlay {}", &overlay_id.0);
        }

        let neighbours = Neighbours::new(
            &peers,
            &self.dht,
            &self.overlay,
            overlay_id.0.clone()
        )?;

        let peers = Arc::new(neighbours);

        let client_overlay = NodeClientOverlay::new(
            overlay_id.0.clone(),
            self.overlay.clone(),
            self.rldp.clone(),
            Arc::clone(&peers)
        );

        let client_overlay = Arc::new(client_overlay);

        Neighbours::start_ping(Arc::clone(&peers));
        Neighbours::start_reload(Arc::clone(&peers));
        Neighbours::start_rnd_peers_process(Arc::clone(&peers));
        NodeNetwork::start_update_peers(self.clone(), &client_overlay);
        NodeNetwork::process_overlay_peers(peers.clone(), self.dht.clone(), self.overlay.clone(), overlay_id.0.clone());
        let result = self.try_add_new_elem(&overlay_id.0, client_overlay, &self.overlays);

        Ok(result as Arc<dyn FullNodeOverlayClient>)
    }

    fn search_validator_keys(
        local_adnl_id: Arc<KeyId>,
        dht: Arc<DhtNode>,
        overlay: Arc<OverlayNode>,
        validators_contexts: Arc<Cache<UInt256, ValidatorSetContext>>,
        validator_list_id: UInt256,
        validators: Vec<CatchainNode>
    ) {
        let mut is_first_search = true;
        const SLEEP_TIME: u64 = 1;  //secs
        tokio::spawn(async move {
            let mut current_validators = validators;
            loop {
                match Self::search_validator_keys_round(
                    is_first_search,
                    local_adnl_id.clone(),
                    dht.clone(),
                    overlay.clone(),
                    current_validators).await
                {
                    Ok(lost_validators) => {
                        current_validators = lost_validators;
                    },
                    Err(e) => {
                        log::warn!("{:?}", e);
                        break;
                    }
                }
                if current_validators.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(SLEEP_TIME)).await;
                if validators_contexts.get(&validator_list_id).is_none() {
                    break;
                }
                is_first_search = false;
            }
        });
    }

    async fn search_validator_keys_round(
        is_first_search: bool,
        local_adnl_id: Arc<KeyId>,
        dht: Arc<DhtNode>,
        overlay: Arc<OverlayNode>,
        validators: Vec<CatchainNode>
    ) -> Result<Vec<CatchainNode>> {
        const SEARCH_TIMEOUT: u64 = 5;  // seconds
        let mut lost_validators = Vec::new();
        for val in validators {
            let res = if is_first_search {
                match tokio::time::timeout(
                    Duration::from_secs(SEARCH_TIMEOUT), 
                    DhtNode::find_address(&dht, &val.adnl_id)
                ).await {
                    Ok(result) => { result },
                    Err(_) => {
                        lost_validators.push(val.clone());
                        log::info!("addr: {:?} skip",  &val.adnl_id);
                        continue
                    },
                }
            } else {
                DhtNode::find_address(&dht, &val.adnl_id).await
            };
            match res {
                Ok((addr, key)) => {
                    log::info!("addr: {:?}, key: {:x?}", &addr, &key);
                    overlay.add_private_peers(&local_adnl_id, vec![(addr, key)])?;
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

    pub fn process_connectivity_broadcast(self: Arc<Self>, broadcast: Box<ConnectivityCheckBroadcast>) {
        if !self.connectivity_check_config.enabled {
            return;
        }
        tokio::spawn(async move {
            if let Some(validator_set_context) = self.current_validator_set_context() {
                let pub_key = KeyId::from_data(broadcast.pub_key.0);
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

    fn current_validator_set_context<'a>(&'a self) -> Option<lockfree::map::ReadGuard<'a, UInt256, ValidatorSetContext>>  {
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
                        Ok(n) => log::trace!("Sent short connectivity broadcast ({})", n),
                        Err(e) => log::warn!("Error while sending short connectivity broadcast: {}", e)
                    }
                    if big_bc_counter == self.connectivity_check_config.long_mult {
                        big_bc_counter = 0;
                        match self.send_connectivity_broadcast(
                            key_id, vec!(0xfe; self.connectivity_check_config.long_len)).await
                        {
                            Ok(n) => log::trace!("Sent long connectivity broadcast ({})", n),
                            Err(e) => log::warn!("Error while sending long connectivity broadcast: {}", e)
                        }
                    }
                }
            }
        });
    }

    async fn send_connectivity_broadcast(&self, key_id: &[u8; 32], mut padding: Vec<u8>) -> Result<u32> {
        if let Some(overlay) = self.overlays.get(&self.masterchain_overlay_short_id) {
            let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs();
            padding.extend_from_slice(&now.to_le_bytes());
            let broadcast = TonNode_ConnectivityCheckBroadcast(Box::new(ConnectivityCheckBroadcast {
                pub_key: int256(key_id.clone()),
                padding: bytes(padding),
            }));
            overlay.val().overlay()
                .broadcast(&self.masterchain_overlay_short_id, &serialize(&broadcast)?, None).await
        } else {
            fail!("There is not masterchain overlay")
        }
    }
}

#[async_trait::async_trait]
impl OverlayOperations for NodeNetwork {

    fn calc_overlay_id(&self, workchain: i32, shard: u64) -> Result<(Arc<OverlayShortId>, OverlayId)> {
        let id = self.overlay.calc_overlay_id(workchain, shard as i64)?;
        let short_id = self.overlay.calc_overlay_short_id(workchain, shard as i64)?;

        Ok((short_id, id))
    }

    async fn get_peers_count(&self, masterchain_zero_state_id: &BlockIdExt) -> Result<usize> {
        let masterchain_overlay_short_id = self.overlay.calc_overlay_short_id(
            masterchain_zero_state_id.shard().workchain_id(),
            masterchain_zero_state_id.shard().shard_prefix_with_tag() as i64,
        )?;
        Ok(self.update_overlay_peers(&masterchain_overlay_short_id, &mut None).await?.len())
    }

    async fn start(self: Arc<Self>) -> Result<Arc<dyn FullNodeOverlayClient>> {
        AdnlNode::start(
            &self.adnl, 
            vec![self.dht.clone(), 
                self.overlay.clone(),
                self.rldp.clone()]
        ).await?;
        Ok(self.clone().get_overlay(
            (self.masterchain_overlay_short_id.clone(), self.masterchain_overlay_id.clone())
        ).await?)
    }

    async fn get_overlay(
        self: Arc<Self>,
        overlay_id: (Arc<OverlayShortId>, OverlayId)
    ) -> Result<Arc<dyn FullNodeOverlayClient>> {
        loop {
            if let Some(overlay) = self.overlays.get(&overlay_id.0) {
                return Ok(overlay.val().clone() as Arc<dyn FullNodeOverlayClient>);
            }
            let overlay_opt = self.overlay_awaiters.do_or_wait(
                &overlay_id.0.clone(),
                None,
                Arc::clone(&self).get_overlay_worker(overlay_id.clone())
            ).await?;
            if let Some(overlay) = overlay_opt {
                return Ok(overlay)
            }
        }
    }

    fn add_consumer(&self, overlay_id: &Arc<OverlayShortId>, consumer: Arc<dyn QueriesConsumer>) -> Result<()> {
        self.overlay.add_consumer(overlay_id, consumer)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl PrivateOverlayOperations for NodeNetwork {
    async fn set_validator_list(
        &self, 
        validator_list_id: UInt256,
        validators: &Vec<CatchainNode>
    ) -> Result<Option<Arc<KeyOption>>> {
        log::trace!("set_validator_list validator_list_id: {}", validator_list_id);

        let mut local_validator_key_raw = None;
        let mut local_validator_adnl_key_raw = None;
        let mut election_id_raw = None;
        for validator in validators {
            if let Some((validator_key, _)) = self.config_handler.get_validator_key(validator.public_key.id()).await {
                local_validator_key_raw = Some(validator_key);
                if let Some ((adnl_key, election_id)) = self.config_handler.get_validator_key(&validator.adnl_id).await {
                    local_validator_adnl_key_raw = Some(adnl_key);
                    election_id_raw = Some(election_id);
                }
                break;
            }
        }

        let local_validator_key: KeyOption = match local_validator_key_raw {
            Some(key) => { key },
            None => { return Ok(None) }
        };

        let local_validator_adnl_key: KeyOption = match local_validator_adnl_key_raw {
            Some(key) => { key },
            None => { return Ok(None) }
        };

        let election_id: usize = match election_id_raw {
            Some(id) => { id as usize },
            None => { return Ok(None) }
        };

        let mut store = false;

        let adnl_key = if self.validator_context.validator_adnl_keys.get(local_validator_adnl_key.id()).is_none() {
            self.validator_context.validator_adnl_keys.insert(local_validator_adnl_key.id().clone(), election_id);
            let id = self.adnl.add_key(local_validator_adnl_key, election_id)?;
            store = true;
            self.adnl.key_by_id(&id)?
        } else {
            self.adnl.key_by_id(&local_validator_adnl_key.id().clone())?
        };

        if store {
            NodeNetwork::periodic_store_ip_addr(
                self.dht.clone(),
                adnl_key.clone(),
                Some(self.validator_context.validator_adnl_keys.clone())
            );
        }
        let mut lost_validators = Vec::new();
        let mut peers_ids = Vec::new();
/*
        let mut peers_ids: Vec<Arc<KeyId>> = validators.iter().filter_map(|val| {
            if val.public_key.id() == local_validator_key.id() {
                None
            } else {
                Some(val.adnl_id.clone())
        }}).collect();*/

        let connectivity_stat = Arc::new(Cache::new());

        for val in validators {
            if val.public_key.id() == local_validator_key.id() {
                continue;
            }
            peers_ids.push(val.adnl_id.clone());
            lost_validators.push(val.clone());
            connectivity_stat.insert(val.adnl_id.clone(), Default::default());
        }

        let validator_set_context = ValidatorSetContext {
            validator_peers: peers_ids,
            validator_key: Arc::new(local_validator_key),
            validator_adnl_key: adnl_key.clone(),
            election_id: election_id.clone(),
            connectivity_stat,
        };

        let context = self.try_add_new_elem(
            &validator_list_id.clone(),
            validator_set_context,
            &self.validator_context.sets_contexts
        );

        if !lost_validators.is_empty() {
            Self::search_validator_keys(
                adnl_key.id().clone(),
                self.dht.clone(), 
                self.overlay.clone(),
                self.validator_context.sets_contexts.clone(),
                validator_list_id,
                lost_validators
            );
        }
        Ok(Some(context.validator_key.clone()))
    }

    fn activate_validator_list(&self, validator_list_id: UInt256) -> Result<()> {
        log::trace!("activate_validator_list {}", validator_list_id);
        self.validator_context.current_set.insert(0, validator_list_id);
        Ok(())
    }

    async fn remove_validator_list(&self, validator_list_id: UInt256) -> Result<bool> {
        let context = self.validator_context.sets_contexts.get(&validator_list_id);
        let mut status = false;
        if let Some(context) = context {
            let adnl_key = &context.val().validator_adnl_key;
            self.overlay.delete_private_peers(
                adnl_key.id(),
                &context.val().validator_peers
            )?;
            if self.config_handler.get_validator_key(adnl_key.id()).await.is_none() {
                // delete adnl key 
                match self.validator_context.validator_adnl_keys.get(adnl_key.id()) {
                    Some(adnl_key_info) => {
                        self.adnl.delete_key(adnl_key_info.key(), *adnl_key_info.val())?;
                        self.validator_context.validator_adnl_keys.remove(adnl_key.id());
                    },
                    None => { log::warn!("validator adnl key don`t deleted!"); }
                }
            }
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
            .ok_or_else(|| error!("bad validator_list_id ({})!", validator_list_id.to_hex_string()))?;
        let adnl_key = self.adnl.key_by_tag(validator_set_context.val().election_id)?;

        let client = CatchainClient::new(
            &self.runtime_handle,
            overlay_short_id,
            &self.overlay,
            self.rldp.clone(),
            nodes_public_keys,
            &adnl_key,
            validator_set_context.val().validator_key.clone(),
            listener
        )?;

        let client = Arc::new(client);
        CatchainClient::run_wait_broadcast(
            client.clone(),
            &self.runtime_handle,
            overlay_short_id,
            &self.overlay,
            &client.validator_keys(),
            &client.catchain_listener());

        let result = self.try_add_new_elem(
            overlay_short_id,
            client.clone(),
            &self.validator_context.private_overlays
        );

        Ok(result  as Arc<dyn CatchainOverlay + Send>)
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
