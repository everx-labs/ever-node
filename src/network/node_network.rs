use crate::{
    config::{TonNodeGlobalConfig, TonNodeConfig}, db::InternalDb,  engine_traits::OverlayOperations, 
    network::{neighbours::{self, Neighbours}, full_node_client::{NodeClientOverlay, FullNodeOverlayClient}},
    types::awaiters_pool::AwaitersPool,
};
use adnl::{
    common::KeyId, node::{AddressCacheIterator, AdnlNode, AdnlNodeConfig}
};
use dht::DhtNode;
use overlay::{OverlayShortId, OverlayNode, QueriesConsumer};
use rldp::RldpNode;
use std::{sync::Arc, time::Duration};
use tokio::time::delay_for;
use ton_types::{Result, fail};                                       

type OverlayCache = lockfree::map::Map<Arc<OverlayShortId>, Arc<NodeClientOverlay>>;
pub struct NodeNetwork {
    adnl: Arc<AdnlNode>,
    dht: Arc<DhtNode>,
    overlay: Arc<OverlayNode>,
    rldp: Arc<RldpNode>,
//    use_global_cfg: bool,
    global_cfg: TonNodeGlobalConfig,
//    overlay_peers: Option<Vec<AdnlNodeConfig>>,
    db: Arc<dyn InternalDb>,
    masterchain_overlay_id: Arc<OverlayShortId>,
    overlays: Arc<OverlayCache>,
    overlay_awaiters: AwaitersPool<Arc<OverlayShortId>, Arc<dyn FullNodeOverlayClient>>,
}

impl NodeNetwork {

    pub(crate) const TAG_DHT_KEY: usize = 1;
    pub(crate) const TAG_OVERLAY_KEY: usize = 2;

    pub async fn new(config: &mut TonNodeConfig, db: Arc<dyn InternalDb>) -> Result<Self> {

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

        let overlays = Arc::new(OverlayCache::new());
        let masterchain_overlay_id = overlay.calc_overlay_short_id(
            masterchain_zero_state_id.shard().workchain_id(),
            masterchain_zero_state_id.shard().shard_prefix_with_tag() as i64,
        )?;

        Ok(NodeNetwork {
            adnl,
            dht,
            overlay,
            rldp,
            db,
            masterchain_overlay_id,
  //          use_global_cfg: config.use_global_config().clone(),
            global_cfg: global_config,
  //          overlay_peers: config.overlay_peers(),
            overlays: overlays,
            overlay_awaiters: AwaitersPool::new(),
        })
    }

    pub fn global_cfg(&self) -> &TonNodeGlobalConfig {
        &self.global_cfg
    }

    fn try_add_new_overlay(
        &self,
        overlay_id: &Arc<OverlayShortId>,
        overlay: Arc<NodeClientOverlay>
    ) -> Arc<NodeClientOverlay> {
        let insertion = self.overlays.insert_with(
            overlay_id.clone(),
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
                lockfree::map::Preview::New(overlay.clone())
            }
        );
        match insertion {
            lockfree::map::Insertion::Created => {
                // overlay info we loaded now was added - use it
                overlay
            },
            lockfree::map::Insertion::Updated(_) => {
                // unreachable situation - all updates must be discarded
                unreachable!("overlay: unreachable Insertion::Updated")
            },
            lockfree::map::Insertion::Failed(_) => {
                // othre thread's overlay info was added - get it and use
                self.overlays.get(overlay_id).unwrap().val().clone()
            }
        }
    }

    pub fn get_peers_from_storage(
        &self,
        overlay_id: &Arc<OverlayShortId>
    ) -> Result<Option<Vec<AdnlNodeConfig>>> {
        let id = base64::encode(overlay_id.data());
        let id = self.string_to_static_str(id);
        let raw_data_peers = match self.db.load_node_state(&id) {
            Ok(configs) => configs,
            Err(_) => return Ok(None)
        };
        let res_json: Vec<String> = serde_json::from_slice(&raw_data_peers)?;
        let mut result = vec![];

        for item in res_json.iter() {
            result.push(
                AdnlNodeConfig::from_json(item, false)?
            );
        }
        Ok(serde::export::Some(result))
    }

    fn string_to_static_str(&self, s: String) -> &'static str {
        Box::leak(s.into_boxed_str())
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

    pub fn masterchain_overlay_id(&self) -> &KeyId {
        &self.masterchain_overlay_id
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

    fn start_update_peers(self: Arc<Self>, client_overlay: &Arc<NodeClientOverlay>) {
        let client_overlay = client_overlay.clone();
        let _handler = tokio::spawn(async move {
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
        overlay_id: &Arc<OverlayShortId>
    ) -> Result<Arc<dyn FullNodeOverlayClient>> {

        self.overlay.add_shard(overlay_id)?;

        let peers = self.update_overlay_peers(overlay_id, &mut None).await?; 
        if peers.first().is_none() {
            fail!("No nodes were found in overlay {}", overlay_id);
        }

        let neigbours = Neighbours::new(
            &peers,
            &self.dht,
            self.rldp.clone(),
            &self.overlay,
            overlay_id.clone()
        )?;

        let peers = Arc::new(neigbours);

        let client_overlay = NodeClientOverlay::new(
            overlay_id.clone(),
            self.overlay.clone(),
            self.rldp.clone(),
            Arc::clone(&peers)
        );

        let client_overlay = Arc::new(client_overlay);

        Neighbours::start_ping(Arc::clone(&peers));
        Neighbours::start_reload(Arc::clone(&peers));
        Neighbours::start_rnd_peers_process(Arc::clone(&peers));
        NodeNetwork::start_update_peers(self.clone(), &client_overlay);
        Ok(self.try_add_new_overlay(overlay_id, client_overlay))
    }
}

#[async_trait::async_trait]
impl OverlayOperations for NodeNetwork {

    fn calc_overlay_short_id(&self, workchain: i32, shard: u64) -> Result<Arc<OverlayShortId>> {
        self.overlay.calc_overlay_short_id(workchain, shard as i64)
    }

    async fn start(self: Arc<Self>) -> Result<Arc<dyn FullNodeOverlayClient>> {
        AdnlNode::start(
            &self.adnl, 
            vec![self.dht.clone(), self.overlay.clone(), self.rldp.clone()]
        ).await?;
        Ok(self.clone().get_overlay(&self.masterchain_overlay_id).await?)
    }

    async fn get_overlay(
        self: Arc<Self>,
        overlay_id: &Arc<OverlayShortId>
    ) -> Result<Arc<dyn FullNodeOverlayClient>> {

        loop {
            if let Some(overlay) = self.overlays.get(overlay_id) {
                return Ok(overlay.val().clone());
            }
            let overlay_opt = self.overlay_awaiters.do_or_wait(
                &overlay_id,
                Arc::clone(&self).get_overlay_worker(overlay_id)
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
