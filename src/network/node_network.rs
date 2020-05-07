use crate::{
    block::{compare_block_ids, convert_block_id_ext_api2blk, convert_block_id_ext_blk2api, convert_block_id_ext_blk_vec, BlockStuff},
    block_proof::BlockProofStuff, config::{TonNodeGlobalConfig, TonNodeConfig},
    db::InternalDb, shard_state::ShardStateStuff, engine_traits::GetOverlay,
        network::neighbours::{Neighbours, Neighbour}
};

use adnl::{
    adnl_node_compatibility_key, adnl_node_test_config,
    common::{KeyId, KeyOption, serialize, serialize_append}, 
    node::{AdnlNode, AdnlNodeConfig}
};
use dht::DhtNode;
use overlay::{OverlayShortId, OverlayNode};
use rldp::RldpNode;
use std::{io::Cursor, sync::Arc};
use ton_api::{BoxedSerialize, BoxedDeserialize, Deserializer, IntoBoxed};
use ton_api::ton::{
    self,
    rpc::{
        ton_node::{
            DownloadNextBlockFull, DownloadPersistentStateSlice, DownloadZeroState,
            PreparePersistentState, GetNextBlockDescription, GetNextBlocksDescription,
            GetPrevBlocksDescription, PrepareBlockProof, DownloadBlockProof, 
            DownloadBlockProofLink, PrepareBlockProofs, DownloadBlockProofs, 
            DownloadBlockProofLinks, PrepareBlock, DownloadBlock, DownloadBlockFull, 
            PrepareBlocks, PrepareZeroState, GetNextKeyBlockIds, GetArchiveInfo, GetArchiveSlice
        }
    },
    ton_node::{ 
        ArchiveInfo, BlockDescription, BlocksDescription, Broadcast, 
        DataFull, DataList, KeyBlocks, Prepared, PreparedProof, PreparedState, 
        externalmessage::ExternalMessage, 
        broadcast::{BlockBroadcast, ExternalMessageBroadcast}, 
    }
};
use ton_block::BlockIdExt;
use ton_types::{fail, error, Result};

#[async_trait::async_trait]
pub trait FullNodeOverlayClient : Sync + Send {
    async fn broadcast_external_message(&self, msg: &[u8]) -> Result<u32>;
    async fn get_next_block_description(&self, prev_block_id: &BlockIdExt, attempts: u32) -> Result<BlockDescription>;
    async fn get_next_blocks_description(&self, prev_block_id: &BlockIdExt, limit: i32, attempts: u32) -> Result<BlocksDescription>;
    async fn get_prev_blocks_description(&self, next_block_id: &BlockIdExt, limit: i32, cutoff_seqno: i32, attempts: u32) -> Result<BlocksDescription>;
    async fn download_block_proof(&self, block_id: &BlockIdExt, allow_partial: bool, attempts: u32) -> Result<Option<BlockProofStuff>>;
    async fn download_block_proofs(&self, block_ids: Vec<BlockIdExt>, allow_partial: bool, attempts: u32) -> Result<Option<Vec<BlockProofStuff>>>;
    async fn download_block_full(&self, id: &BlockIdExt, attempts: u32) -> Result<Option<(BlockStuff, BlockProofStuff)>>;
    async fn download_block(&self, id: &BlockIdExt, attempts: u32) -> Result<Option<BlockStuff>>;
    async fn download_blocks(&self, ids: Vec<BlockIdExt>, attempts: u32) -> Result<Option<Vec<BlockStuff>>>;
    async fn check_persistent_state(&self, block_id: &BlockIdExt, masterchain_block_id: &BlockIdExt, attempts: u32) -> Result<bool>;
    async fn download_persistent_state_part(
        &self,
        block_id: &BlockIdExt,
        masterchain_block_id: &BlockIdExt,
        offset: usize,
        max_size: usize,
        attempts: u32
    ) -> Result<Vec<u8>>;
    async fn download_zero_state(&self, id: &BlockIdExt, attempts: u32) -> Result<Option<ShardStateStuff>>;
    async fn get_next_key_blocks_ids(&self, block_id: &BlockIdExt, max_size: i32, attempts: u32) -> Result<Vec<BlockIdExt>>;
    async fn download_next_block_full(&self, prev_id: &BlockIdExt, attempts: u32) -> Result<(BlockStuff, BlockProofStuff)>;
    async fn get_archive_info(&self, masterchain_seqno: u32, attempts: u32) -> Result<ArchiveInfo>;
    async fn get_archive_slice(&self, archive_id: u64, offset: u64, max_size: u32, attempts: u32) -> Result<Vec<u8>>;

    async fn wait_block_broadcast(&self) -> Result<Box<BlockBroadcast>>;
}

type OverlayCache = lockfree::map::Map<Arc<OverlayShortId>, Arc<NodeClientOverlay>>;
pub struct NodeNetwork {
    adnl: Arc<AdnlNode>,
    dht: Arc<DhtNode>,
    overlay: Arc<OverlayNode>,
    rldp: Arc<RldpNode>,
    use_global_cfg: bool,
    global_cfg: TonNodeGlobalConfig,
    overlay_peers: Option<Vec<AdnlNodeConfig>>,
    db: Arc<dyn InternalDb>,
    masterchain_overlay_id: Arc<OverlayShortId>,
    overlays: Arc<OverlayCache>,
}

#[derive(Clone)]
pub struct NodeClientOverlay {
    overlay_id: Arc<OverlayShortId>,
    overlay: Arc<OverlayNode>,
    rldp: Arc<RldpNode>,
    peers: Arc<Neighbours>
}

impl NodeNetwork {

    pub(crate) const TAG_DHT_KEY: usize = 1;
    pub(crate) const TAG_OVERLAY_KEY: usize = 2;

    pub async fn new(config: &mut TonNodeConfig, db: Arc<dyn InternalDb>) -> Result<Self> {
        let adnl_node = if let Some(adnl_node) = config.adnl_node() {
            adnl_node
        } else {
            fail!("Local ADNL node is not configured")
        };

        let global_config = config.load_global_config()?;
        let masterchain_zero_state_id = global_config.zero_state()?;

        let adnl = AdnlNode::with_config(adnl_node).await?;
        let dht = DhtNode::with_adnl_node(adnl.clone(), Self::TAG_DHT_KEY)?;
        let overlay = OverlayNode::with_adnl_node_and_zero_state(
            adnl.clone(), 
            masterchain_zero_state_id.file_hash.as_slice(),
            Self::TAG_OVERLAY_KEY
        )?;
        let rldp = RldpNode::with_adnl_node(adnl.clone())?;

        let nodes = global_config.dht_nodes()?;
        for peer in nodes.iter() {
            dht.add_peer(peer.ip_address(), &peer.key_by_tag(Self::TAG_DHT_KEY)?)?;
        }

        let overlays = Arc::new(OverlayCache::new());
        let masterchain_overlay_id = overlay.calc_overlay_short_id(
            masterchain_zero_state_id.shard().workchain_id(),
            masterchain_zero_state_id.shard().shard_prefix_with_tag() as i64,
        )?;
//        overlay.add_shard(&masterchain_overlay_id)?;

        Ok(NodeNetwork {
            adnl,
            dht,
            overlay,
            rldp,
            db,
            masterchain_overlay_id,
            use_global_cfg: config.use_global_config().clone(),
            global_cfg: global_config,
            overlay_peers: config.overlay_peers(),
            overlays: overlays,
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
        let actual_nodes = self.dht.find_overlay_nodes(&overlay_id.clone()).await?;
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

    pub fn masterchain_overlay_id(&self) -> &KeyId {
        &self.masterchain_overlay_id
    }
}

#[async_trait::async_trait]
impl GetOverlay for NodeNetwork {

    fn calc_overlay_short_id(&self, workchain: i32, shard: u64) -> Result<Arc<OverlayShortId>> {
        self.overlay.calc_overlay_short_id(workchain, shard as i64)
    }

    async fn start(&self) -> Result<Arc<dyn FullNodeOverlayClient>> {
        AdnlNode::start(
            &self.adnl, 
            vec![self.dht.clone(), self.overlay.clone(), self.rldp.clone()]
        ).await?;
        Ok(self.get_overlay(&self.masterchain_overlay_id).await?)
    }

    async fn get_overlay(
        &self, 
        overlay_id: &Arc<OverlayShortId>
    ) -> Result<Arc<dyn FullNodeOverlayClient>> {

        if let Some(overlay) = self.overlays.get(overlay_id) {
            return Ok(overlay.val().clone());
        }

        self.overlay.add_shard(overlay_id)?;

        let mut peers: Vec<Arc<KeyId>> = vec![];
        if let Some(config_peers) = &self.overlay_peers {
            log::info!("Preconfigured overlay ({:?}) nodes:", &overlay_id);
            for peer in config_peers.iter() {
                log::info!("Node: {}, address: {}",
                    peer.key_by_tag(NodeNetwork::TAG_OVERLAY_KEY)?.id(),
                    peer.ip_address()
                );
                let overlay_peer = self.overlay.add_peer(
                    peer.ip_address(),
                    &peer.key_by_tag(NodeNetwork::TAG_OVERLAY_KEY)?
                )?;
                peers.push(overlay_peer);
            }
        }

        if self.use_global_cfg {
            let addresses = if let Some(addresses) = self.get_peers_from_storage(overlay_id)? {
                log::info!("get peers (overlay: {:?}) from storage: Some()", &overlay_id);
                addresses
            } else {
                log::info!("Overlay {:?} node finding...", &overlay_id);
                let nodes = self.find_overlay_nodes(overlay_id).await?;
                self.save_peers(overlay_id, &nodes)?;
                nodes
            };

            log::info!("Found overlay ({:?}) nodes:", &overlay_id);
            for peer in addresses.iter() {
                let key = peer.key_by_tag(NodeNetwork::TAG_OVERLAY_KEY)?;
                log::info!("Node: {}, address: {}, key: {}",
                    key.id(),
                    peer.ip_address(),
                    base64::encode(key.pub_key()?)
                ); 
                let overlay_peer = self.overlay.add_peer(peer.ip_address(), &key)?;
                peers.push(overlay_peer);
            }
        }

        if peers.first().is_none() {
            fail!("No one overlay ({:?}) node found!", &overlay_id);
        }

        self.overlay.add_shard(overlay_id)?;
        let neigbours = Neighbours::new(
            &peers.clone(),
            &self.dht,
            self.rldp.clone(),
            &self.overlay,
            overlay_id.clone()
        )?;

        let client_overlay = NodeClientOverlay {
            overlay_id : overlay_id.clone(),
            overlay: self.overlay.clone(),
            rldp : self.rldp.clone(),
            peers : Arc::new(neigbours)
        };

        let client_overlay = Arc::new(client_overlay);

        Neighbours::start_ping(client_overlay.peers.clone());
        Neighbours::start_reload(client_overlay.peers.clone());
        Neighbours::start_rnd_peers_process(client_overlay.peers.clone());
        Ok(self.try_add_new_overlay(overlay_id,client_overlay))
    }
}

impl NodeClientOverlay {

    async fn send_query<T: BoxedSerialize, D: BoxedDeserialize>(&self, request: T, attempts: u32) -> Result<D> {
        let now = std::time::Instant::now();
        let answer = self.send_data_query_ex(request, attempts, None).await;
        let t = now.elapsed();
        match answer {
            Ok(res) => {
                let result: Result<D> = Deserializer::new(&mut Cursor::new(res.0)).read_boxed();

                match result {
                    Ok(data) => {
                        self.peers.update_neighbour_stats(res.1.id(), &t, true)?;
                        return Ok(data);
                    },
                    Err(e) => {
                        self.peers.update_neighbour_stats(res.1.id(), &t, false)?;
                        fail!(e)
                    },
                }
            },
            Err(e) => {
              //  self.peers.update_neighbour_stats(res.1.id(), &t, false)?;
                fail!(e)
            }
        }
    }

    async fn send_data_query<T: BoxedSerialize>(&self, request: T, attempts: u32) -> Result<Vec<u8>> {
        let now = std::time::Instant::now();
        let answer = self.send_data_query_ex(request, attempts, None).await;//.map(|(r, _)| r)
        let t = now.elapsed();

        match answer {
            Ok(res) => {
                    self.peers.update_neighbour_stats(res.1.id(), &t, true)?;
                    return Ok(res.0);
            },
            Err(e) => {
              //  self.peers.update_neighbour_stats(res.1.id(), &t, false)?;
                fail!(e)
            }
        }
    }

    async fn send_query_ex<T: BoxedSerialize, D: BoxedDeserialize>(&self, request: T, attempts: u32, good_peer: Option<Arc<Neighbour>>) -> Result<(D, Arc<Neighbour>)> {
        let now = std::time::Instant::now();
        let answer = self.send_data_query_ex(request, attempts, good_peer).await;
        let t = now.elapsed();
        match answer {
            Ok(res) => {
                let result: Result<D> = Deserializer::new(&mut Cursor::new(res.0)).read_boxed();

                match result {
                    Ok(data) => {
                        self.peers.update_neighbour_stats(res.1.id(), &t, true)?;
                        return Ok((data, res.1));
                    },
                    Err(e) => {
                        self.peers.update_neighbour_stats(res.1.id(), &t, false)?;
                        fail!(e)
                    },
                }
            },
            Err(e) => {
              //  self.peers.update_neighbour_stats(res.1.id(), &t, false)?;
                fail!(e)
            }
        }
    }

    async fn send_data_query_ex<T: BoxedSerialize>(&self, request: T, attempts: u32, good_peer: Option<Arc<Neighbour>>) -> Result<(Vec<u8>, Arc<Neighbour>)> {

        let mut query = self.overlay.get_query_prefix(&self.overlay_id)?;
        serialize_append(&mut query, &request)?;
        let data = Arc::new(query);

        let peer = if let Some(p) = good_peer {
            p
        } else {
            self.peers.choose_neighbour()?.ok_or_else(||error!("neighbour is not found!"))?
        };

        for _ in 0..attempts {
           let res = self.overlay.query_via_rldp(
                &self.rldp,
                peer.id(),
                &data, 
                Some(10 * 1024 * 1024)
            ).await?;
            if let Some(res) = res {
                return Ok((res, peer))
            } else {
                peer.query_failed();
                continue
            }
        }
        fail!("Data was not downloaded after {} attempts!", attempts);
    }

}

#[async_trait::async_trait]
impl FullNodeOverlayClient for NodeClientOverlay {

    // Returns number of nodes to broadcast to
    async fn broadcast_external_message(&self, msg: &[u8]) -> Result<u32> {
        let msg = ExternalMessageBroadcast {
            message: ExternalMessage {
                data: ton::bytes(msg.to_vec())
            }
        }.into_boxed();
        self.overlay.broadcast(&self.overlay_id, &serialize(&msg)?).await
    }

    // tonNode.getNextBlockDescription prev_block:tonNode.blockIdExt = tonNode.BlockDescription;
    // tonNode.blockDescriptionEmpty = tonNode.BlockDescription;
    // tonNode.blockDescription id:tonNode.blockIdExt = tonNode.BlockDescription;
    async fn get_next_block_description(&self, prev_block_id: &BlockIdExt, attempts: u32) -> Result<BlockDescription> {
        Ok(
            self.send_query(
                GetNextBlockDescription {
                    prev_block: convert_block_id_ext_blk2api(prev_block_id),
                },
                attempts
            ).await?
        )
    }

    // tonNode.getNextBlocksDescription prev_block:tonNode.blockIdExt limit:int = tonNode.BlocksDescription;
    // tonNode.getPrevBlocksDescription next_block:tonNode.blockIdExt limit:int cutoff_seqno:int = tonNode.BlocksDescription;
    // tonNode.blocksDescription ids:(vector tonNode.blockIdExt) incomplete:Bool = tonNode.BlocksDescription;
    async fn get_next_blocks_description(&self, prev_block_id: &BlockIdExt, limit: i32, attempts: u32) -> Result<BlocksDescription> {
        Ok(
            self.send_query(
                GetNextBlocksDescription {
                    prev_block: convert_block_id_ext_blk2api(prev_block_id),
                    limit
                },
                attempts
            ).await?
        )
    }

    async fn get_prev_blocks_description(&self, next_block_id: &BlockIdExt, limit: i32, cutoff_seqno: i32, attempts: u32) -> Result<BlocksDescription> {
        Ok(
            self.send_query(
                GetPrevBlocksDescription {
                    next_block: convert_block_id_ext_blk2api(next_block_id),
                    limit,
                    cutoff_seqno
                },
                attempts
            ).await?
        )
    }

    // tonNode.prepareBlockProof block:tonNode.blockIdExt allow_partial:Bool = tonNode.PreparedProof;
    // tonNode.preparedProofEmpty = tonNode.PreparedProof;
    // tonNode.preparedProof = tonNode.PreparedProof;
    // tonNode.preparedProofLink = tonNode.PreparedProof;
    //
    // tonNode.downloadBlockProof block:tonNode.blockIdExt = tonNode.Data;
    // tonNode.downloadBlockProofLink block:tonNode.blockIdExt = tonNode.Data;
    async fn download_block_proof(&self, block_id: &BlockIdExt, allow_partial: bool, attempts: u32) -> Result<Option<BlockProofStuff>> {
        // Prepare
        let prepare: PreparedProof = self.send_query(
            PrepareBlockProof {
                block: convert_block_id_ext_blk2api(block_id),
                allow_partial: allow_partial.into(),
            },
            attempts
        ).await?;

        // Download
        match prepare {
            PreparedProof::TonNode_PreparedProofEmpty => Ok(None),
            prepared_proof => {
                let (bytes, is_link) = match prepared_proof { 
                    PreparedProof::TonNode_PreparedProof => {
                        let proof_bytes = self.send_data_query(
                            DownloadBlockProof {
                                block: convert_block_id_ext_blk2api(block_id),
                            },
                            attempts
                        ).await?;

                        (proof_bytes, false)
                    },
                    PreparedProof::TonNode_PreparedProofLink => {
                        let proof_bytes = self.send_data_query(
                            DownloadBlockProofLink {
                                block: convert_block_id_ext_blk2api(block_id),
                            },
                            attempts
                        ).await?;

                        (proof_bytes, true)
                    },
                    _ => unreachable!(),
                };
                
                Ok(Some(
                    BlockProofStuff::deserialize(block_id, &bytes, is_link)?
                ))
            }
        }
    }

    // tonNode.prepareBlockProofs blocks:(vector tonNode.blockIdExt) allow_partial:Bool = tonNode.PreparedProof;
    // tonNode.downloadBlockProofs blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    // tonNode.downloadBlockProofLinks blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    async fn download_block_proofs(&self, block_ids: Vec<BlockIdExt>, allow_partial: bool, attempts: u32) -> Result<Option<Vec<BlockProofStuff>>> {
        // Prepare
        let prepare: PreparedProof = self.send_query(
            PrepareBlockProofs {
                blocks: convert_block_id_ext_blk_vec(&block_ids).into(),
                allow_partial: allow_partial.into()
            },
            attempts
        ).await?;

        // Download
        match prepare {
            PreparedProof::TonNode_PreparedProofEmpty => Ok(None),
            PreparedProof::TonNode_PreparedProof => {
                let _: DataList = self.send_query(
                    DownloadBlockProofs {
                        blocks: convert_block_id_ext_blk_vec(&block_ids).into(),
                    },
                    attempts
                ).await?;

                // TODO construct proof
                Ok(Some(vec!()))
            },
            PreparedProof::TonNode_PreparedProofLink => {
                let _: DataList = self.send_query(
                    DownloadBlockProofLinks {
                        blocks: convert_block_id_ext_blk_vec(&block_ids).into(),
                    },
                    attempts
                ).await?;

                // TODO construct proof
                Ok(Some(vec!()))
            },
        }
    }

    // tonNode.prepareBlock block:tonNode.blockIdExt = tonNode.Prepared;
    // tonNode.downloadBlockFull block:tonNode.blockIdExt = tonNode.DataFull;
    // tonNode.dataFull id:tonNode.blockIdExt proof:bytes block:bytes is_link:Bool = tonNode.DataFull;
    // tonNode.dataFullEmpty = tonNode.DataFull;
    //
    // tonNode.downloadBlock block:tonNode.blockIdExt = tonNode.Data; DEPRECATED?
    async fn download_block_full(&self, id: &BlockIdExt, attempts: u32) -> Result<Option<(BlockStuff, BlockProofStuff)>> {
        // Prepare
        let (prepare, peer): (Prepared, _) = self.send_query_ex(
            PrepareBlock {
                block: convert_block_id_ext_blk2api(id),
            },
            attempts,
            None
        ).await?;

        // Download
        match prepare {
            Prepared::TonNode_NotFound => Ok(None),
            Prepared::TonNode_Prepared => {
                let (data_full, _): (DataFull, _) = self.send_query_ex(
                    DownloadBlockFull {
                        block: convert_block_id_ext_blk2api(id),
                    },
                    attempts,
                    Some(peer)
                ).await?;

                match data_full {
                    DataFull::TonNode_DataFullEmpty => {
                        log::warn!("prepareBlock receives Prepared, but DownloadBlockFull receives DataFullEmpty");
                        Ok(None)
                    },
                    DataFull::TonNode_DataFull(data_full) => {
                        if !compare_block_ids(&id, &data_full.id) {
                            fail!("Block with another id was received");
                        }

                        let block = BlockStuff::deserialize(&data_full.block.0)?;
                        let proof = BlockProofStuff::deserialize(
                            block.id(),
                            &data_full.proof.0,
                            data_full.is_link.into())?;

                        Ok(Some((block, proof)))
                    }
                }
            }
        }
    }

    async fn download_block(&self, id: &BlockIdExt, attempts: u32) -> Result<Option<BlockStuff>> {
        // Prepare
        let prepare: Prepared = self.send_query(
            PrepareBlock {
                block: convert_block_id_ext_blk2api(id),
            },
            attempts
        ).await?;

        // Download
        match prepare {
            Prepared::TonNode_NotFound => Ok(None),
            Prepared::TonNode_Prepared => {
                let block_bytes = self.send_data_query(
                    DownloadBlock {
                        block: convert_block_id_ext_blk2api(id),
                    },
                    attempts
                ).await?;

                Ok(
                    Some(
                        BlockStuff::deserialize(&block_bytes)?
                    )
                )
            },
        }
    }

    // tonNode.prepareBlocks blocks:(vector tonNode.blockIdExt) = tonNode.Prepared; DEPRECATED?
    // tonNode.downloadBlocks blocks:(vector tonNode.blockIdExt) = tonNode.DataList; DEPRECATED?
    async fn download_blocks(&self, ids: Vec<BlockIdExt>, attempts: u32) -> Result<Option<Vec<BlockStuff>>> {
        // Prepare
        let prepare: Prepared = self.send_query(
            PrepareBlocks {
                blocks: convert_block_id_ext_blk_vec(&ids).into(),
            },
            attempts
        ).await?;

        // Download
        match prepare {
            Prepared::TonNode_NotFound => Ok(None),
            Prepared::TonNode_Prepared => {
                let blocks_bytes: DataList = self.send_query(
                    DownloadBlockProofs {
                        blocks: convert_block_id_ext_blk_vec(&ids).into(),
                    },
                    attempts
                ).await?;

                let mut blocks = vec!();
                for block_bytes in blocks_bytes.data().0.iter() {
                    blocks.push(
                        BlockStuff::deserialize(&block_bytes.0)?
                    )
                }
                Ok(Some(blocks))
            }
        }
    }

    async fn check_persistent_state(&self, block_id: &BlockIdExt, masterchain_block_id: &BlockIdExt, attempts: u32) -> Result<bool> {
        let prepare: PreparedState = self.send_query(
            PreparePersistentState {
                block: convert_block_id_ext_blk2api(block_id),
                masterchain_block: convert_block_id_ext_blk2api(masterchain_block_id)
            },
            attempts
        ).await?;

        Ok(
            match prepare {
                PreparedState::TonNode_NotFoundState => false,
                PreparedState::TonNode_PreparedState => true
            }
        )
    }

    // tonNode.preparePersistentState block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt = tonNode.PreparedState;
    // tonNode.downloadPersistentState block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt = tonNode.Data; DEPRECATED?
    // tonNode.downloadPersistentStateSlice block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt offset:long max_size:long = tonNode.Data;
    async fn download_persistent_state_part(
        &self,
        block_id: &BlockIdExt,
        masterchain_block_id: &BlockIdExt,
        offset: usize,
        max_size: usize,
        attempts: u32
    ) -> Result<Vec<u8>> {
        let data = self.send_data_query(
            DownloadPersistentStateSlice {
                block: convert_block_id_ext_blk2api(block_id),
                masterchain_block: convert_block_id_ext_blk2api(masterchain_block_id),
                offset: offset as i64,
                max_size: max_size as i64,
            },
            attempts
        ).await?;

        Ok(data)
    }

    // tonNode.prepareZeroState block:tonNode.blockIdExt = tonNode.PreparedState;
    // tonNode.downloadZeroState block:tonNode.blockIdExt = tonNode.Data;
    async fn download_zero_state(&self, id: &BlockIdExt, attempts: u32) -> Result<Option<ShardStateStuff>> {
        // Prepare
        let prepare: PreparedState = self.send_query(
            PrepareZeroState {
                block: convert_block_id_ext_blk2api(id),
            },
            attempts
        ).await?;

        // Download
        match prepare {
            PreparedState::TonNode_NotFoundState => Ok(None),
            PreparedState::TonNode_PreparedState => {
                let state_bytes = self.send_data_query(
                    DownloadZeroState {
                        block: convert_block_id_ext_blk2api(id),
                    },
                    attempts
                ).await?;

                Ok(
                    Some(
                        ShardStateStuff::deserialize(id.clone(), &state_bytes)?
                    )
                )
            }
        }
    }

    // tonNode.keyBlocks blocks:(vector tonNode.blockIdExt) incomplete:Bool error:Bool = tonNode.KeyBlocks;
    // tonNode.getNextKeyBlockIds block:tonNode.blockIdExt max_size:int = tonNode.KeyBlocks;
    async fn get_next_key_blocks_ids(&self, block_id: &BlockIdExt, max_size: i32, attempts: u32) -> Result<Vec<BlockIdExt>> {
        let query = GetNextKeyBlockIds {
            block: convert_block_id_ext_blk2api(block_id),
            max_size
        };
        self.send_query(query, attempts).await
            .and_then(|ids: KeyBlocks| ids.blocks().iter().try_fold(Vec::new(), |mut vec, id| {
                vec.push(convert_block_id_ext_api2blk(id)?);
                Ok(vec)
            }))
    }
    
    // tonNode.downloadNextBlockFull prev_block:tonNode.blockIdExt = tonNode.DataFull;
    async fn download_next_block_full(&self, prev_id: &BlockIdExt, attempts: u32) -> Result<(BlockStuff, BlockProofStuff)> {
        // Download
        let data_full: DataFull = self.send_query(
            DownloadNextBlockFull {
                prev_block: convert_block_id_ext_blk2api(prev_id)
            },
            attempts
        ).await?;

        // Parse
        let block_bytes = data_full
            .block()
            .ok_or_else(|| failure::err_msg("get_next_block_attempts: got empty data"))?;
        let block = BlockStuff::deserialize(block_bytes)?;

        let proof_bytes = data_full
            .proof()
            .ok_or_else(|| failure::err_msg("get_next_block_attempts: got empty data"))?;
        let is_link = data_full
            .is_link()
            .ok_or_else(|| failure::err_msg("get_next_block_attempts: got empty data"))?;
        let proof = BlockProofStuff::deserialize(block.id(), proof_bytes, (is_link.clone()).into())?;

        Ok((block, proof))
    }

    // tonNode.getArchiveInfo masterchain_seqno:int = tonNode.ArchiveInfo;
    async fn get_archive_info(&self, masterchain_seqno: u32, attempts: u32) -> Result<ArchiveInfo> {
        Ok(
            self.send_query(
                GetArchiveInfo {
                    masterchain_seqno: masterchain_seqno as i32
                },
                attempts
            ).await?
        )
    }

    // tonNode.getArchiveSlice archive_id:long offset:long max_size:int = tonNode.Data;
    async fn get_archive_slice(&self, archive_id: u64, offset: u64, max_size: u32, attempts: u32) -> Result<Vec<u8>> {
        self.send_data_query(
            GetArchiveSlice {
                archive_id: archive_id as i64,
                offset: offset as i64,
                max_size: max_size as i32,
            },
            attempts
        ).await
    }

    async fn wait_block_broadcast(&self) -> Result<Box<BlockBroadcast>> {
        let receiver = self.overlay.clone();
        let id = self.overlay_id.clone();
        let mut result: Option<Box<BlockBroadcast>> = None;

        while let None = result {
            let message = receiver.wait_for_broadcast(&id).await;
            match message {
                Ok(data) => {
                    let answer: Broadcast = Deserializer::new(&mut Cursor::new(data)).read_boxed()?;
                    match answer {
                        Broadcast::TonNode_BlockBroadcast(broadcast) => {
                            log::trace!("broadcast block:{:?}", &broadcast.id);
                            result = Some(broadcast);
                        },
                        Broadcast::TonNode_ExternalMessageBroadcast(broadcast) => {
                            log::trace!("TonNode_ExternalMessageBroadcast: {:?}", broadcast);
                        },
                        Broadcast::TonNode_IhrMessageBroadcast(broadcast) => {
                            log::trace!("TonNode_IhrMessageBroadcast: {:?}", broadcast);
                        },
                        Broadcast::TonNode_NewShardBlockBroadcast(broadcast) => {
                            log::trace!("TonNode_NewShardBlockBroadcast: {:?}", broadcast);
                        },
                    }
                },
                Err(e) => {
                    log::error!("broadcast err: {}", e);
                },
            };
        }

        let result = result.ok_or_else(|| error!("Failed to receive a broadcast!"))?;
        Ok(result)
    }

}
