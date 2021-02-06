use crate::{
    config::{NodeConfigHandler, TonNodeConfig},
    engine_traits::{OverlayOperations, PrivateOverlayOperations}, internal_db::InternalDb,
    network::{
        catchain_client::CatchainClient, control::ControlServer,
        full_node_client::{NodeClientOverlay, FullNodeOverlayClient},
        neighbours::{self, Neighbours}
    },
    types::awaiters_pool::AwaitersPool,
};
use adnl::{
    common::{KeyId, KeyOption}, node::{AddressCacheIterator, AdnlNode}
};
use catchain::{CatchainNode, CatchainOverlay, CatchainOverlayListenerPtr, CatchainOverlayLogReplayListenerPtr};
use dht::DhtNode;
use overlay::{OverlayId, OverlayShortId, OverlayNode, QueriesConsumer, PrivateOverlayShortId};
use rldp::RldpNode;
use std::{hash::Hash, sync::Arc, time::Duration};
use tokio::time::delay_for;
use ton_types::{Result, error, UInt256};
use ton_block::BlockIdExt;

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
    _control: Option<ControlServer>
}

struct ValidatorContext {
    private_overlays: Arc<Cache<Arc<OverlayShortId>, Arc<CatchainClient>>>,
    validator_adnl_keys: Arc<Cache<Arc<KeyId>, usize>>,   //KeyId, tag
    sets_contexts: Arc<Cache<UInt256, ValidatorSetContext>>,
}

#[derive(Clone)]
struct ValidatorSetContext {
    validator_peers: Vec<Arc<KeyId>>,
    validator_key: Arc<KeyOption>,
    validator_adnl_key: Arc<KeyOption>,
    election_id: usize
}

impl NodeNetwork {

    pub const TAG_DHT_KEY: usize = 1;
    pub const TAG_OVERLAY_KEY: usize = 2;

    const PERIOD_STORE_IP_ADDRESS: u64 = 500;   // second
    const PERIOD_START_FIND_DHT_NODE: u64 = 60; // second

    pub async fn new(config: TonNodeConfig, db: Arc<dyn InternalDb>) -> Result<Self> {
        let global_config = config.load_global_config()?;
        let masterchain_zero_state_id = global_config.zero_state()?;

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
        let control_server_config = config.control_server();
        let config_handler = Arc::new(NodeConfigHandler::new(config)?);

        let _control = match control_server_config {
            Ok(config) => Some(
                ControlServer::with_config(
                    config,
                    Some(Arc::new(super::control::DbEngine::new(db.clone()))),
                    config_handler.clone(),
                    config_handler.clone()
                ).await?
            ),
            Err(e) => {
                log::warn!("{}", e);
                None
            }
        };

     //   let validator_adnl_key = adnl.key_by_tag(Self::TAG_VALIDATOR_ADNL_KEY)?;
     //   NodeNetwork::periodic_store_ip_addr(dht.clone(), validator_adnl_key);

        let validator_context = ValidatorContext {
            private_overlays: Arc::new(Cache::new()),
            validator_adnl_keys: Arc::new(Cache::new()),
            sets_contexts: Arc::new(Cache::new()),
        };

        Ok(NodeNetwork {
            adnl,
            dht,
            overlay,
            rldp,
            masterchain_overlay_short_id,
            masterchain_overlay_id,
            overlays: Arc::new(Cache::new()),
            validator_context: validator_context,
            overlay_awaiters: AwaitersPool::new(),
            runtime_handle: tokio::runtime::Handle::current(),
            config_handler: config_handler,
            _control
        })
    }

    pub async fn stop(&self) {
        self.adnl.stop().await
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
                delay_for(Duration::from_secs(Self::PERIOD_STORE_IP_ADDRESS)).await;

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

                delay_for(Duration::from_secs(Self::PERIOD_STORE_IP_ADDRESS)).await;
            }
        });
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
                delay_for(Duration::from_secs(Self::PERIOD_START_FIND_DHT_NODE)).await;
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
                delay_for(Duration::from_secs(5)).await;
            }
        });
    }

    async fn get_overlay_worker(
        self: Arc<Self>,
        overlay_id: (Arc<OverlayShortId>, OverlayId)
    ) -> Result<Arc<dyn FullNodeOverlayClient>> {

        self.overlay.add_shard(&overlay_id.0).await?;

        let node = self.overlay.get_signed_node(&overlay_id.0)?;
        NodeNetwork::periodic_store_overlay_node(
            self.dht.clone(),
            overlay_id.1, node);

        let peers = self.update_overlay_peers(&overlay_id.0, &mut None).await?; 
        if peers.first().is_none() {
            log::warn!("No nodes were found in overlay {}", &overlay_id.0);
        }

        let neigbours = Neighbours::new(
            &peers,
            &self.dht,
            &self.overlay,
            overlay_id.0.clone()
        )?;

        let peers = Arc::new(neigbours);

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
        const SLEEP_TIME: u64 = 1;  //secs
        tokio::spawn(async move {
            let mut current_validators = validators;
            loop {
                match Self::search_validator_keys_round(
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
                delay_for(Duration::from_secs(SLEEP_TIME)).await;

                if validators_contexts.get(&validator_list_id).is_none() {
                    break;
                }
            }
        });
    }

    async fn search_validator_keys_round(
        local_adnl_id: Arc<KeyId>,
        dht: Arc<DhtNode>,
        overlay: Arc<OverlayNode>,
        validators: Vec<CatchainNode>
    ) -> Result<Vec<CatchainNode>> {
        let mut lost_validators = Vec::new();
        for val in validators {
            let res = DhtNode::find_address(&dht, &val.adnl_id).await;

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
}

impl Drop for NodeNetwork {
    fn drop(&mut self) {
        if let Some(control) = self._control.take() {
            control.shutdown()
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
                return Ok(overlay.val().clone());
            }
            let overlay_opt = self.overlay_awaiters.do_or_wait(
                &overlay_id.0.clone(),
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
        let mut peers = Vec::new();
        let mut lost_validators = Vec::new();
        let mut peers_ids = Vec::new();

        for val in validators {
            if val.public_key.id() == local_validator_key.id() {
                continue;
            }
            peers_ids.push(val.adnl_id.clone());

            match DhtNode::find_address(&self.dht, &val.adnl_id).await {
                Ok((addr, key)) => {
                    log::info!("addr: {:?}, key: {:x?}", &addr, &key);
                    peers.push((addr, key));
                }
                Err(e) => {
                    log::error!("find address failed: {:?}", e);
                    lost_validators.push(val.clone());
                }
            }
        }

        self.overlay.add_private_peers(adnl_key.id(), peers)?;
        let validator_set_context = ValidatorSetContext {
            validator_peers: peers_ids,
            validator_key: Arc::new(local_validator_key),
            validator_adnl_key: adnl_key.clone(),
            election_id: election_id.clone()
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
