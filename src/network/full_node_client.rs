use crate::{
    block::{
        compare_block_ids, convert_block_id_ext_api2blk, convert_block_id_ext_blk2api, BlockStuff
    },
    block_proof::BlockProofStuff, shard_state::ShardStateStuff,
    network::neighbours::{Neighbours, Neighbour},
};

use adnl::{common::{serialize, serialize_append}, node::AdnlNode};
use overlay::{OverlayShortId, OverlayNode};
use rldp::RldpNode;
use std::{io::Cursor, time::{Duration, Instant}, sync::Arc};
use ton_api::{BoxedSerialize, BoxedDeserialize, Deserializer, IntoBoxed};
use ton_api::ton::{
    self, TLObject,
    rpc::{
        ton_node::{
            DownloadNextBlockFull, DownloadPersistentStateSlice, DownloadZeroState,
            PreparePersistentState, GetNextBlockDescription, GetNextBlocksDescription,
            GetPrevBlocksDescription,DownloadBlockProof, DownloadBlockProofLink,
            DownloadKeyBlockProof, DownloadKeyBlockProofLink, PrepareBlock, DownloadBlock, 
            DownloadBlockFull, PrepareZeroState, GetNextKeyBlockIds, GetArchiveInfo, GetArchiveSlice
        }
    },
    ton_node::{ 
        ArchiveInfo, BlockDescription, BlocksDescription, Broadcast, DataFull,
        KeyBlocks, Prepared, PreparedProof, PreparedState,
        broadcast::{BlockBroadcast, ExternalMessageBroadcast}, externalmessage::ExternalMessage
    }
};
use ton_block::BlockIdExt;
use ton_types::{fail, error, Result};

pub struct Attempts {
    pub limit: u32,
    pub count: u32
}

#[async_trait::async_trait]
pub trait FullNodeOverlayClient : Sync + Send {
    async fn broadcast_external_message(&self, msg: &[u8]) -> Result<u32>;
    async fn get_next_block_description(&self, prev_block_id: &BlockIdExt, attempts: &Attempts) -> Result<BlockDescription>;
    async fn get_next_blocks_description(&self, prev_block_id: &BlockIdExt, limit: i32, attempts: &Attempts) -> Result<BlocksDescription>;
    async fn get_prev_blocks_description(&self, next_block_id: &BlockIdExt, limit: i32, cutoff_seqno: i32, attempts: &Attempts) -> Result<BlocksDescription>;
    async fn download_block_proof(&self, block_id: &BlockIdExt, is_link: bool, key_block: bool, attempts: &Attempts) -> Result<Option<BlockProofStuff>>;
    async fn download_block_full(&self, id: &BlockIdExt, attempts: &Attempts) -> Result<Option<(BlockStuff, BlockProofStuff)>>;
    async fn download_block(&self, id: &BlockIdExt, attempts: &Attempts) -> Result<Option<BlockStuff>>;
    async fn check_persistent_state(
        &self,
        block_id: &BlockIdExt,
        masterchain_block_id: &BlockIdExt,
        attempts: &Attempts
    ) -> Result<(bool, Arc<Neighbour>)>;
    async fn download_persistent_state_part(
        &self,
        block_id: &BlockIdExt,
        masterchain_block_id: &BlockIdExt,
        offset: usize,
        max_size: usize,
        peer: Option<Arc<Neighbour>>,
        attempts: &Attempts
    ) -> Result<Vec<u8>>;
    async fn download_zero_state(&self, id: &BlockIdExt, attempts: &Attempts) -> Result<Option<ShardStateStuff>>;
    async fn download_next_key_blocks_ids(&self, block_id: &BlockIdExt, max_size: i32, attempts: &Attempts) -> Result<Vec<BlockIdExt>>;
    async fn download_next_block_full(&self, prev_id: &BlockIdExt, attempts: &Attempts) -> Result<(BlockStuff, BlockProofStuff)>;
    async fn get_archive_info(&self, masterchain_seqno: u32, attempts: &Attempts) -> Result<ArchiveInfo>;
    async fn get_archive_slice(&self, archive_id: u64, offset: u64, max_size: u64, attempts: &Attempts) -> Result<Vec<u8>>;

    async fn wait_block_broadcast(&self) -> Result<Box<BlockBroadcast>>;
}

#[derive(Clone)]
pub struct NodeClientOverlay {
    overlay_id: Arc<OverlayShortId>,
    overlay: Arc<OverlayNode>,
    rldp: Arc<RldpNode>,
    peers: Arc<Neighbours>
}

impl NodeClientOverlay {

    const ADNL_ATTEMPTS: u32 = 50;
    const TIMEOUT_PREPARE: u64 = 6000; // Milliseconds
    const TIMEOUT_DELTA: u64 = 50;     // Milliseconds

    pub fn new(
        overlay_id: Arc<OverlayShortId>,
        overlay: Arc<OverlayNode>,
        rldp: Arc<RldpNode>,
        peers: Arc<Neighbours>
    ) -> Self {
        Self{overlay_id, overlay, rldp, peers}
    }

    pub fn overlay_id(&self) -> &Arc<OverlayShortId> {
        &self.overlay_id
    }

    pub fn overlay(&self) -> &Arc<OverlayNode> {
        &self.overlay
    }

    pub fn peers(&self) -> &Arc<Neighbours> {
        &self.peers
    }

    // use this function if request size and answer size < 768 bytes (send query via ADNL)
    async fn send_adnl_query<T, D>(
        &self, 
        request: T, 
        attempts: Option<u32>,
        timeout: Option<u64>
    ) -> Result<(D, Arc<Neighbour>)>
    where
        T: ton_api::AnyBoxedSerialize,
        D: ton_api::AnyBoxedSerialize
    {

        let data = TLObject::new(request);
        let attempts = attempts.unwrap_or(Self::ADNL_ATTEMPTS);

        for _ in 0..attempts {

            let peer = self.peers.choose_neighbour()?.ok_or_else(||error!("neighbour is not found!"))?;
            log::trace!("USE PEER {}, REQUEST {:?}", peer.id(), data);

            let now = std::time::Instant::now();
            let timeout = timeout.or(Some(AdnlNode::calc_timeout(peer.roundtrip_adnl())));
            let answer = self.overlay.query(peer.id(), &data, &self.overlay_id, timeout).await?;
            let t = now.elapsed();
         
            if let Some(answer) = answer {
                match answer.downcast::<D>() {
                    Ok(answer) => {
                        self.peers.update_neighbour_stats(peer.id(), &t, true, false)?;
                        return Ok((answer, peer))
                    },
                    Err(obj) => {
                        log::warn!("Wrong answer {:?} to {:?} from {}", obj, data, peer.id())
                    }
                }
            } else {
                log::warn!("No reply to {:?} from {}", data, peer.id())
            }
            self.peers.update_neighbour_stats(peer.id(), &t, false, false)?;

        }

        fail!("Cannot send query {:?} in {} attempts", data, attempts)
 
    }

    async fn send_rldp_query_to_peer_raw<T>(
        &self, 
        request: &T,
        good_peer: Option<Arc<Neighbour>>,
        attempt: u32
    ) -> Result<Vec<u8>>
    where
        T: BoxedSerialize + std::fmt::Debug
    {
        let (answer, peer, elapsed) = self.send_rldp_query(request, good_peer, attempt).await?;
        self.peers.update_neighbour_stats(peer.id(), &elapsed, true, true)?;
        Ok(answer)
    }

    async fn send_rldp_query_no_peer<T, D>(&self, request: &T, attempts: u32) -> Result<D> 
    where
        T: BoxedSerialize + std::fmt::Debug,
        D: BoxedDeserialize
    {
        for attempt in 0..attempts {
            if let Ok((answer, _)) = self.send_rldp_query_to_peer(request, None, attempt).await {
                return Ok(answer)
            }
        }
        fail!("Cannot send query {:?} in {} attempts", request, attempts)
    }

    async fn send_rldp_query_to_peer<T, D>(
        &self,
        request: &T,
        good_peer: Option<Arc<Neighbour>>,
        attempt: u32
    ) -> Result<(D, Arc<Neighbour>)>
    where
        T: BoxedSerialize + std::fmt::Debug,
        D: BoxedDeserialize
    {
        let (answer, peer, elapsed) = self.send_rldp_query(request, good_peer, attempt).await?;
        match Deserializer::new(&mut Cursor::new(answer)).read_boxed() {
            Ok(data) => {
                self.peers.update_neighbour_stats(peer.id(), &elapsed, true, true)?;
                Ok((data, peer))
            },
            Err(e) => {
                self.peers.update_neighbour_stats(peer.id(), &elapsed, false, true)?;
                fail!(e)
            }
        }
    }

    async fn send_rldp_query<T>(
        &self, 
        request: &T, 
        good_peer: Option<Arc<Neighbour>>,
        attempt: u32
    ) -> Result<(Vec<u8>, Arc<Neighbour>, Duration)> 
    where 
        T: BoxedSerialize + std::fmt::Debug 
    {

        let mut query = self.overlay.get_query_prefix(&self.overlay_id)?;
        serialize_append(&mut query, request)?;
        let data = Arc::new(query);

        let peer = if let Some(peer) = good_peer {
            peer
        } else {
            self.peers.choose_neighbour()?.ok_or_else(||error!("neighbour is not found!"))?
        };

        log::trace!("USE PEER {}, REQUEST {:?}", peer.id(), request);
        let now = Instant::now();
        let answer = self.overlay.query_via_rldp(
            &self.rldp,
            peer.id(),
            &data,
            Some(10 * 1024 * 1024),
            peer.roundtrip_adnl(),
            peer.roundtrip_rldp().map(|t| t + attempt as u64 * Self::TIMEOUT_DELTA)
        ).await?;

        let t = now.elapsed();
        if let Some(answer) = answer {  
            Ok((answer, peer, t))
        } else {
            self.peers.update_neighbour_stats(peer.id(), &t, false, true)?;
            fail!("No RLDP answer to {:?} from {}", request, peer.id())
        }

    }

}

#[async_trait::async_trait]
impl FullNodeOverlayClient for NodeClientOverlay {

    // Returns number of nodes to broadcast to
    async fn broadcast_external_message(&self, msg: &[u8]) -> Result<u32>{
        let broadcast = ExternalMessageBroadcast {
            message: ExternalMessage {
                data: ton::bytes(msg.to_vec())
            }
        }.into_boxed();
        self.overlay.broadcast(&self.overlay_id, &serialize(&broadcast)?).await
    }

    // tonNode.getNextBlockDescription prev_block:tonNode.blockIdExt = tonNode.BlockDescription;
    // tonNode.blockDescriptionEmpty = tonNode.BlockDescription;
    // tonNode.blockDescription id:tonNode.blockIdExt = tonNode.BlockDescription;
    async fn get_next_block_description(
        &self, 
        prev_block_id: &BlockIdExt, 
        attempts: &Attempts
    ) -> Result<BlockDescription> {
        self.send_rldp_query_no_peer(
            &GetNextBlockDescription {
                prev_block: convert_block_id_ext_blk2api(prev_block_id),
            },
            attempts.limit
        ).await
    }

    // tonNode.getNextBlocksDescription prev_block:tonNode.blockIdExt limit:int = tonNode.BlocksDescription;
    // tonNode.getPrevBlocksDescription next_block:tonNode.blockIdExt limit:int cutoff_seqno:int = tonNode.BlocksDescription;
    // tonNode.blocksDescription ids:(vector tonNode.blockIdExt) incomplete:Bool = tonNode.BlocksDescription;
    async fn get_next_blocks_description(
        &self, 
        prev_block_id: &BlockIdExt, 
        limit: i32, 
        attempts: &Attempts
    ) -> Result<BlocksDescription> {
        self.send_rldp_query_no_peer(
            &GetNextBlocksDescription {
                prev_block: convert_block_id_ext_blk2api(prev_block_id),
                limit
            },
            attempts.limit
        ).await
    }

    async fn get_prev_blocks_description(
        &self, 
        next_block_id: &BlockIdExt, 
        limit: i32, 
        cutoff_seqno: i32, 
        attempts: &Attempts
    ) -> Result<BlocksDescription> {
        self.send_rldp_query_no_peer(
            &GetPrevBlocksDescription {
                next_block: convert_block_id_ext_blk2api(next_block_id),
                limit,
                cutoff_seqno
            },
            attempts.limit
        ).await
    }

    // tonNode.prepareBlockProof block:tonNode.blockIdExt allow_partial:Bool = tonNode.PreparedProof;
    // tonNode.preparedProofEmpty = tonNode.PreparedProof;
    // tonNode.preparedProof = tonNode.PreparedProof;
    // tonNode.preparedProofLink = tonNode.PreparedProof;
    //
    // tonNode.downloadBlockProof block:tonNode.blockIdExt = tonNode.Data;
    // tonNode.downloadBlockProofLink block:tonNode.blockIdExt = tonNode.Data;
    async fn download_block_proof(
        &self, 
        block_id: &BlockIdExt, 
        is_link: bool, 
        key_block: bool, 
        attempts: &Attempts
    ) -> Result<Option<BlockProofStuff>> {
        // Prepare
        let (prepare, good_peer): (PreparedProof, _) = if key_block {
            self.send_adnl_query(
                ton_api::ton::rpc::ton_node::PrepareKeyBlockProof {
                    block: convert_block_id_ext_blk2api(block_id),
                    allow_partial: is_link.into()
                },
                None,
                Some(Self::TIMEOUT_PREPARE)
            ).await?
        } else {
            self.send_adnl_query(
                ton_api::ton::rpc::ton_node::PrepareBlockProof {
                    block: convert_block_id_ext_blk2api(block_id),
                    allow_partial: is_link.into(),
                },
                None,
                Some(Self::TIMEOUT_PREPARE)
            ).await?
        };

        // Download
        match prepare {
            PreparedProof::TonNode_PreparedProofEmpty => Ok(None),
            PreparedProof::TonNode_PreparedProof => Ok(Some(if key_block {
                BlockProofStuff::deserialize(block_id, self.send_rldp_query_to_peer_raw(
                    &DownloadKeyBlockProof { block: convert_block_id_ext_blk2api(block_id) },
                    Some(good_peer),
                    attempts.count
                ).await?, false)?
            } else {
                BlockProofStuff::deserialize(block_id, self.send_rldp_query_to_peer_raw(
                    &DownloadBlockProof { block: convert_block_id_ext_blk2api(block_id), },
                    Some(good_peer),
                    attempts.count
                ).await?, false)?
            })),
            PreparedProof::TonNode_PreparedProofLink => Ok(Some(if key_block {
                BlockProofStuff::deserialize(block_id, self.send_rldp_query_to_peer_raw(
                    &DownloadKeyBlockProofLink { block: convert_block_id_ext_blk2api(block_id), },
                    Some(good_peer),
                    attempts.count
                ).await?, true)?
            } else {
                BlockProofStuff::deserialize(block_id, self.send_rldp_query_to_peer_raw(
                    &DownloadBlockProofLink { block: convert_block_id_ext_blk2api(block_id), },
                    Some(good_peer),
                    attempts.count
                ).await?, true)?
            }))
        }
    }

    // tonNode.prepareBlock block:tonNode.blockIdExt = tonNode.Prepared;
    // tonNode.downloadBlockFull block:tonNode.blockIdExt = tonNode.DataFull;
    // tonNode.dataFull id:tonNode.blockIdExt proof:bytes block:bytes is_link:Bool = tonNode.DataFull;
    // tonNode.dataFullEmpty = tonNode.DataFull;
    //
    // tonNode.downloadBlock block:tonNode.blockIdExt = tonNode.Data; DEPRECATED?
    async fn download_block_full(
        &self, 
        id: &BlockIdExt, 
        attempts: &Attempts
    ) -> Result<Option<(BlockStuff, BlockProofStuff)>> {
        // Prepare
        let (prepare, peer): (Prepared, _) = self.send_adnl_query(
            PrepareBlock {block: convert_block_id_ext_blk2api(id)},
            None,
            Some(Self::TIMEOUT_PREPARE)
        ).await?;
        log::trace!("USE PEER {}, PREPARE {} FINISHED", peer.id(), id);

        // Download
        match prepare {
            Prepared::TonNode_NotFound => Ok(None),
            Prepared::TonNode_Prepared => {
                let (data_full, _): (DataFull, _) = self.send_rldp_query_to_peer(
                    &DownloadBlockFull {
                        block: convert_block_id_ext_blk2api(id),
                    },
                    Some(peer),
                    attempts.count
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

                        let block = BlockStuff::deserialize_checked(id.clone(), data_full.block.0)?;
                        let proof = BlockProofStuff::deserialize(
                            block.id(),
                            data_full.proof.0,
                            data_full.is_link.into())?;

                        Ok(Some((block, proof)))
                    }
                }
            }
        }
    }

    // DEPRECATED
    async fn download_block(
        &self, 
        id: &BlockIdExt, 
        attempts: &Attempts
    ) -> Result<Option<BlockStuff>> {
        // Prepare
        let (prepare, peer): (Prepared, _) = self.send_adnl_query(
            PrepareBlock {
                block: convert_block_id_ext_blk2api(id),
            },
            None,
            Some(Self::TIMEOUT_PREPARE)
        ).await?;

        // Download
        match prepare {
            Prepared::TonNode_NotFound => Ok(None),
            Prepared::TonNode_Prepared => {
                let block_bytes = self.send_rldp_query_to_peer_raw(
                    &DownloadBlock {
                        block: convert_block_id_ext_blk2api(id),
                    },
                    Some(peer),
                    attempts.count 	
                ).await?;
                Ok(
                    Some(
                        BlockStuff::deserialize_checked(id.clone(), block_bytes)?
                    )
                )
            },
        }
    }

    async fn check_persistent_state(
        &self,
        block_id: &BlockIdExt,
        masterchain_block_id: &BlockIdExt,
        attempts: &Attempts
    ) -> Result<(bool, Arc<Neighbour>)> {
        let (prepare, peer): (PreparedState, _) = self.send_adnl_query(
            TLObject::new(PreparePersistentState {
                block: convert_block_id_ext_blk2api(block_id),
                masterchain_block: convert_block_id_ext_blk2api(masterchain_block_id)
            }),
            Some(attempts.limit),
            Some(Self::TIMEOUT_PREPARE)
        ).await?;

        Ok((
            match prepare {
                PreparedState::TonNode_NotFoundState => false,
                PreparedState::TonNode_PreparedState => true
            },
            peer
        ))
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
        good_peer: Option<Arc<Neighbour>>,
        attempts: &Attempts
    ) -> Result<Vec<u8>> {
        self.send_rldp_query_to_peer_raw(
            &DownloadPersistentStateSlice {
                block: convert_block_id_ext_blk2api(block_id),
                masterchain_block: convert_block_id_ext_blk2api(masterchain_block_id),
                offset: offset as i64,
                max_size: max_size as i64,
            },
            good_peer,
            attempts.count
        ).await
    }

    // tonNode.prepareZeroState block:tonNode.blockIdExt = tonNode.PreparedState;
    // tonNode.downloadZeroState block:tonNode.blockIdExt = tonNode.Data;
    async fn download_zero_state(
        &self, 
        id: &BlockIdExt, 
        attempts: &Attempts
    ) -> Result<Option<ShardStateStuff>> {
        // Prepare
        let (prepare, good_peer): (PreparedState, _) = self.send_adnl_query(
            TLObject::new(PrepareZeroState {
                block: convert_block_id_ext_blk2api(id),
            }),
            None,
            Some(Self::TIMEOUT_PREPARE)
        ).await?;

        // Download
        match prepare {
            PreparedState::TonNode_NotFoundState => Ok(None),
            PreparedState::TonNode_PreparedState => {
                let state_bytes = self.send_rldp_query_to_peer_raw(
                    &DownloadZeroState {
                        block: convert_block_id_ext_blk2api(id),
                    },
                    Some(good_peer),
                    attempts.count
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
    async fn download_next_key_blocks_ids(
        &self, 
        block_id: &BlockIdExt, 
        max_size: i32, 
        attempts: &Attempts
    ) -> Result<Vec<BlockIdExt>> {
        let query = GetNextKeyBlockIds {
            block: convert_block_id_ext_blk2api(block_id),
            max_size
        };
        self.send_adnl_query(query, Some(attempts.limit), None)
            .await
            .and_then(|(ids, _): (KeyBlocks, _)| ids.blocks().iter().try_fold(Vec::new(), |mut vec, id| {
                vec.push(convert_block_id_ext_api2blk(id)?);
                Ok(vec)
            }))
    }
    
    // tonNode.downloadNextBlockFull prev_block:tonNode.blockIdExt = tonNode.DataFull;
    async fn download_next_block_full(
        &self, 
        prev_id: &BlockIdExt, 
        attempts: &Attempts
    ) -> Result<(BlockStuff, BlockProofStuff)> {
        // Download
        let data_full: DataFull = self.send_rldp_query_no_peer(
            &DownloadNextBlockFull {
                prev_block: convert_block_id_ext_blk2api(prev_id)
            },
            attempts.limit
        ).await?;

        // Parse
        let id = convert_block_id_ext_api2blk(
            data_full.id().ok_or_else(|| failure::err_msg("get_next_block_attempts: got empty id"))?
        )?;
        let block_bytes = data_full
            .block()
            .ok_or_else(|| failure::err_msg("get_next_block_attempts: got empty data"))?;
        let block = BlockStuff::deserialize_checked(id, block_bytes.to_vec())?;

        let proof_bytes = data_full
            .proof()
            .ok_or_else(|| failure::err_msg("get_next_block_attempts: got empty data"))?;
        let is_link = data_full
            .is_link()
            .ok_or_else(|| failure::err_msg("get_next_block_attempts: got empty data"))?;
        let proof = BlockProofStuff::deserialize(block.id(), proof_bytes.to_vec(), (is_link.clone()).into())?;

        Ok((block, proof))
    }

    // tonNode.getArchiveInfo masterchain_seqno:int = tonNode.ArchiveInfo;
    async fn get_archive_info(
        &self, 
        masterchain_seqno: u32, 
        attempts: &Attempts
    ) -> Result<ArchiveInfo> {
        self.send_rldp_query_no_peer(
            &GetArchiveInfo {
                masterchain_seqno: masterchain_seqno as i32
            },
            attempts.limit
        ).await
    }

    // tonNode.getArchiveSlice archive_id:long offset:long max_size:int = tonNode.Data;
    async fn get_archive_slice(
        &self, 
        archive_id: u64, 
        offset: u64, 
        max_size: u64, 
        attempts: &Attempts
    ) -> Result<Vec<u8>> {
        self.send_rldp_query_to_peer_raw(
            &GetArchiveSlice {
                archive_id: archive_id as i64,
                offset: offset as i64,
                max_size: max_size as i32,
            },
            None,
            attempts.count
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
                    let answer: Broadcast = Deserializer::new(&mut Cursor::new(data.0)).read_boxed()?;
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
