/*
* Copyright (C) 2019-2023 EverX. All Rights Reserved.
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

use crate::{
    block::BlockStuff, block_proof::BlockProofStuff,
    network::{neighbours::{Neighbours, Neighbour}, node_network::NetworkContext}, 
    shard_state::ShardStateStuff, types::top_block_descr::TopBlockDescrStuff
};

use adnl::{
    declare_counted,
    common::{CountedObject, Counter,TaggedByteSlice, TaggedObject, TaggedTlObject}, 
    node::AdnlNode
};
use overlay::{BroadcastSendInfo, OverlayShortId, OverlayNode};
use std::{io::Cursor, time::Instant, sync::Arc, time::Duration};
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;
use ton_api::{
    serialize_boxed, serialize_boxed_append, 
    BoxedSerialize, BoxedDeserialize, Deserializer, IntoBoxed,
    ton::{
        self, TLObject, Bool,
        rpc::ton_node::{
            DownloadNextBlockFull, DownloadPersistentStateSlice, DownloadZeroState,
            PreparePersistentState, DownloadBlockProof, DownloadBlockProofLink,
            DownloadKeyBlockProof, DownloadKeyBlockProofLink, PrepareBlock, DownloadBlockFull,
            PrepareZeroState, GetNextKeyBlockIds, GetArchiveInfo, GetArchiveSlice,
            PrepareBlockProof, PrepareKeyBlockProof, PrepareQueueUpdate, DownloadQueueUpdate,
            PreparePersistentMsgQueue, DownloadPersistentMsgQueueSlice,
        },
        ton_node::{ 
            ArchiveInfo, Broadcast, 
            DataFull, KeyBlocks, Prepared, PreparedProof, PreparedState, 
            broadcast::{
                BlockBroadcast, ExternalMessageBroadcast, NewShardBlockBroadcast,
                QueueUpdateBroadcast,
            }, 
            externalmessage::ExternalMessage, 
        }
    }
};
#[cfg(feature = "telemetry")]
use ton_api::{tag_from_boxed_type, tag_from_bare_type};
use ton_block::BlockIdExt;
use ton_types::{error, fail, KeyId, Result};

#[async_trait::async_trait]
pub trait FullNodeOverlayClient : Sync + Send {
    async fn broadcast_external_message(&self, msg: &[u8]) -> Result<BroadcastSendInfo>;
    async fn send_block_broadcast(&self, broadcast: BlockBroadcast) -> Result<()>;
    async fn send_queue_update_broadcast(&self, broadcast: QueueUpdateBroadcast) -> Result<()>;
    async fn send_top_shard_block_description(&self, tbd: &TopBlockDescrStuff) -> Result<()>;
    async fn download_block_proof(&self, block_id: &BlockIdExt, is_link: bool, key_block: bool) -> Result<BlockProofStuff>;
    async fn download_block_full(&self, id: &BlockIdExt) -> Result<(BlockStuff, BlockProofStuff)>;
    async fn download_out_msg_queue_update(
        &self,
        id: &BlockIdExt,
        target_wc: i32,
    ) -> Result<BlockStuff>;
    async fn check_persistent_state(
        &self,
        block_id: &BlockIdExt,
        msg_queue_for: Option<i32>,
        masterchain_block_id: &BlockIdExt,
        active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>,
    ) -> Result<Option<Arc<Neighbour>>>;
    async fn download_persistent_state_part(
        &self,
        block_id: &BlockIdExt,
        msg_queue_for: Option<i32>,
        masterchain_block_id: &BlockIdExt,
        offset: usize,
        max_size: usize,
        peer: Arc<Neighbour>,
        attempt: u32,
    ) -> Result<Vec<u8>>;
    async fn download_zero_state(
        &self, 
        id: &BlockIdExt
    ) -> Result<(Arc<ShardStateStuff>, Vec<u8>)>;
    async fn download_next_key_blocks_ids(&self, block_id: &BlockIdExt, max_size: i32) -> Result<(Vec<BlockIdExt>, bool)>;
    async fn download_next_block_full(&self, prev_id: &BlockIdExt) -> Result<(BlockStuff, BlockProofStuff)>;
    async fn download_archive(
        &self, 
        mc_seq_no: u32,
        active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>
    ) -> Result<Option<Vec<u8>>>;
    async fn wait_broadcast(&self) -> Result<Option<(Broadcast, Arc<KeyId>)>>;
}

//    #[derive(Clone)]
declare_counted!(
    pub struct NodeClientOverlay {
        overlay_id: Arc<OverlayShortId>,
        peers: Arc<Neighbours>,
        network_context: Arc<NetworkContext>,
        #[cfg(feature = "telemetry")]
        tag_block_broadcast: u32,
        #[cfg(feature = "telemetry")]
        tag_queue_update_broadcast: u32,
        #[cfg(feature = "telemetry")]
        tag_download_block_full: u32,
        #[cfg(feature = "telemetry")]
        tag_download_queue_update: u32,
        #[cfg(feature = "telemetry")]
        tag_download_block_proof: u32,
        #[cfg(feature = "telemetry")]
        tag_download_block_proof_link: u32,
        #[cfg(feature = "telemetry")]
        tag_download_key_block_proof: u32,
        #[cfg(feature = "telemetry")]
        tag_download_key_block_proof_link: u32,
        #[cfg(feature = "telemetry")]
        tag_download_next_block_full: u32,
        #[cfg(feature = "telemetry")]
        tag_download_persistent_state_slice: u32,
        #[cfg(feature = "telemetry")]
        tag_download_persistent_msg_queue_slice: u32,
        #[cfg(feature = "telemetry")]
        tag_download_zero_state: u32,
        #[cfg(feature = "telemetry")]
        tag_external_message_broadcast: u32,
        #[cfg(feature = "telemetry")]
        tag_get_archive_info: u32,
        #[cfg(feature = "telemetry")]
        tag_get_archive_slice: u32,
        #[cfg(feature = "telemetry")]
        tag_get_next_key_block_ids: u32,
        #[cfg(feature = "telemetry")]
        tag_new_shard_block_broadcast: u32,
        #[cfg(feature = "telemetry")]
        tag_prepare_block: u32,
        #[cfg(feature = "telemetry")]
        tag_prepare_queue_update: u32,
        #[cfg(feature = "telemetry")]
        tag_prepare_block_proof: u32,
        #[cfg(feature = "telemetry")]
        tag_prepare_key_block_proof: u32,
        #[cfg(feature = "telemetry")]
        tag_prepare_persistent_state: u32,
        #[cfg(feature = "telemetry")]
        tag_prepare_persistent_msg_queue: u32,
        #[cfg(feature = "telemetry")]
        tag_prepare_zero_state: u32
    }
);

impl NodeClientOverlay {   

    const ADNL_ATTEMPTS: u32 = 50;
    const TIMEOUT_PREPARE: u64 = 6000; // Milliseconds
    const TIMEOUT_DELTA: u64 = 50; // Milliseconds
    const TIMEOUT_NO_NEIGHBOURS: u64 = 1000; // Milliseconds

    pub (crate) fn new(
        overlay_id: Arc<OverlayShortId>,
        peers: Arc<Neighbours>,
        network_context: &Arc<NetworkContext>
    ) -> Self { 
        let ret = Self {
            overlay_id,
            peers,
            network_context: network_context.clone(),
            #[cfg(feature = "telemetry")]
            tag_block_broadcast: tag_from_bare_type::<BlockBroadcast>(),
            #[cfg(feature = "telemetry")]
            tag_queue_update_broadcast: tag_from_bare_type::<QueueUpdateBroadcast>(),
            #[cfg(feature = "telemetry")]
            tag_download_block_full: tag_from_boxed_type::<DownloadBlockFull>(),
            #[cfg(feature = "telemetry")]
            tag_download_queue_update: tag_from_boxed_type::<DownloadQueueUpdate>(),
            #[cfg(feature = "telemetry")]
            tag_download_block_proof: tag_from_boxed_type::<DownloadBlockProof>(),
            #[cfg(feature = "telemetry")]
            tag_download_block_proof_link: tag_from_boxed_type::<DownloadBlockProofLink>(),
            #[cfg(feature = "telemetry")]
            tag_download_key_block_proof: tag_from_boxed_type::<DownloadKeyBlockProof>(),
            #[cfg(feature = "telemetry")]
            tag_download_key_block_proof_link: tag_from_boxed_type::<DownloadKeyBlockProofLink>(),
            #[cfg(feature = "telemetry")]
            tag_download_next_block_full: tag_from_boxed_type::<DownloadNextBlockFull>(),
            #[cfg(feature = "telemetry")]
            tag_download_persistent_state_slice: 
                tag_from_boxed_type::<DownloadPersistentStateSlice>(),
            #[cfg(feature = "telemetry")]
            tag_download_persistent_msg_queue_slice:
                tag_from_boxed_type::<DownloadPersistentMsgQueueSlice>(),
            #[cfg(feature = "telemetry")]
            tag_download_zero_state: tag_from_boxed_type::<DownloadZeroState>(),
            #[cfg(feature = "telemetry")]
            tag_external_message_broadcast: tag_from_bare_type::<ExternalMessageBroadcast>(),
            #[cfg(feature = "telemetry")]
            tag_get_archive_info: tag_from_boxed_type::<GetArchiveInfo>(),
            #[cfg(feature = "telemetry")]
            tag_get_archive_slice: tag_from_boxed_type::<GetArchiveSlice>(),
            #[cfg(feature = "telemetry")]
            tag_get_next_key_block_ids: tag_from_boxed_type::<GetNextKeyBlockIds>(),
            #[cfg(feature = "telemetry")]
            tag_new_shard_block_broadcast: tag_from_bare_type::<NewShardBlockBroadcast>(),
            #[cfg(feature = "telemetry")]
            tag_prepare_block: tag_from_boxed_type::<PrepareBlock>(),
            #[cfg(feature = "telemetry")]
            tag_prepare_queue_update: tag_from_boxed_type::<PrepareQueueUpdate>(),
            #[cfg(feature = "telemetry")]
            tag_prepare_block_proof: tag_from_boxed_type::<PrepareBlockProof>(),
            #[cfg(feature = "telemetry")]
            tag_prepare_key_block_proof: tag_from_boxed_type::<PrepareKeyBlockProof>(),
            #[cfg(feature = "telemetry")]
            tag_prepare_persistent_state: tag_from_boxed_type::<PreparePersistentState>(),
            #[cfg(feature = "telemetry")]
            tag_prepare_persistent_msg_queue: tag_from_boxed_type::<PreparePersistentMsgQueue>(),
            #[cfg(feature = "telemetry")]
            tag_prepare_zero_state: tag_from_boxed_type::<PrepareZeroState>(),
            counter: network_context.engine_allocated.overlay_clients.clone().into()
        };
        #[cfg(feature = "telemetry")]
        network_context.engine_telemetry.overlay_clients.update(
            network_context.engine_allocated.overlay_clients.load(Ordering::Relaxed)
        );
        ret
    }

    pub fn overlay_id(&self) -> &Arc<OverlayShortId> {
        &self.overlay_id
    }

    pub fn overlay(&self) -> &Arc<OverlayNode> {
        &self.network_context.overlay
    }

    pub fn peers(&self) -> &Arc<Neighbours> {
        &self.peers
    }

    async fn send_adnl_query_to_peer<R, D>(
        &self, 
        peer: &Arc<Neighbour>,
        data: &TaggedTlObject,
        timeout: Option<u64>
    ) -> Result<Option<D>>
    where
        R: ton_api::AnyBoxedSerialize,
        D: ton_api::AnyBoxedSerialize
    {

        let request_str = if log::log_enabled!(log::Level::Trace) || cfg!(feature = "telemetry") {
            format!("ADNL {:?}", data.object)
        } else {
            String::default()
        };
        log::trace!("USE PEER {}, {}", peer.id(), request_str);

        let now = Instant::now();
        let timeout = timeout.or(Some(AdnlNode::calc_timeout(peer.roundtrip_adnl())));
        let answer = self.network_context.overlay.query(
            peer.id(), 
            &data, 
            &self.overlay_id, 
            timeout
        ).await?;
        let roundtrip = now.elapsed().as_millis() as u64;

        if let Some(answer) = answer {
            match answer.downcast::<D>() {
                Ok(answer) => {
                    peer.query_success(roundtrip, false);
                    #[cfg(feature = "telemetry")]
                    self.network_context.telemetry.consumed_query(
                        request_str, 
                        true, 
                        now.elapsed(), 
                        0
                    ); // TODO data size (need to patch overlay)
                    return Ok(Some(answer))
                },
                Err(obj) => {
                    #[cfg(feature = "telemetry")]
                    self.network_context.telemetry.consumed_query(
                        request_str, 
                        false, 
                        now.elapsed(), 
                        0
                    );
                    log::warn!("Wrong answer {:?} to {:?} from {}", obj, data.object, peer.id())
                }
            }
        } else {
            #[cfg(feature = "telemetry")]
            self.network_context.telemetry.consumed_query(request_str, false, now.elapsed(), 0);
            log::warn!("No reply to {:?} from {}", data.object, peer.id())
        }

        self.peers.update_neighbour_stats(peer.id(), roundtrip, false, false, true)?;
        Ok(None)

    }

    // use this function if request size and answer size < 768 bytes (send query via ADNL)
    async fn send_adnl_query<R, D>(
        &self, 
        request: TaggedObject<R>, 
        attempts: Option<u32>,
        timeout: Option<u64>,
        active_peers: Option<&Arc<lockfree::set::Set<Arc<KeyId>>>>
    ) -> Result<(D, Arc<Neighbour>)>
    where
        R: ton_api::AnyBoxedSerialize,
        D: ton_api::AnyBoxedSerialize
    {

        let data = TaggedTlObject {
            object: TLObject::new(request.object),
            #[cfg(feature = "telemetry")]
            tag: request.tag
        };
        let attempts = attempts.unwrap_or(Self::ADNL_ATTEMPTS);

        for _ in 0..attempts {
            let peer = if let Some(p) = self.peers.choose_neighbour()? {
                p
            } else {
                tokio::time::sleep(Duration::from_millis(Self::TIMEOUT_NO_NEIGHBOURS)).await;
                fail!("neighbour is not found!")
            };
            if let Some(active_peers) = &active_peers {
                if active_peers.insert(peer.id().clone()).is_ok() {
                    continue;
                }
            }
            match self.send_adnl_query_to_peer::<R, D>(&peer, &data, timeout).await {
                Err(e) => {
                    if let Some(active_peers) = &active_peers {
                        active_peers.remove(peer.id());
                    }
                    return Err(e) 
                },
                Ok(Some(answer)) => return Ok((answer, peer)),
                Ok(None) => if let Some(active_peers) = &active_peers {
                    active_peers.remove(peer.id());
                }
            }
        }

        fail!("Cannot send query {:?} in {} attempts", data.object, attempts)
 
    }

    async fn send_rldp_query_raw<T>(
        &self, 
        request: &TaggedObject<T>,
        peer: Arc<Neighbour>,
        attempt: u32,
    ) -> Result<Vec<u8>>
    where
        T: BoxedSerialize + std::fmt::Debug
    {
        let (answer, peer, roundtrip) = self.send_rldp_query(request, peer, attempt).await?;
        peer.query_success(roundtrip, true);
        Ok(answer)
    }

    async fn send_rldp_query_typed<T, D>(
        &self,
        request: &TaggedObject<T>,
        peer: Arc<Neighbour>,
        attempt: u32,
    ) -> Result<(D, Arc<Neighbour>)>
    where
        T: BoxedSerialize + std::fmt::Debug,
        D: BoxedDeserialize
    {
        let (answer, peer, roundtrip) = self.send_rldp_query(request, peer, attempt).await?;
        match Deserializer::new(&mut Cursor::new(answer)).read_boxed() {
            Ok(data) => {
                peer.query_success(roundtrip, true);
                Ok((data, peer))
            },
            Err(e) => {
                self.peers.update_neighbour_stats(peer.id(), roundtrip, false, true, true)?;
                fail!(e)
            }
        }
    }

    async fn send_rldp_query<T>(
        &self, 
        request: &TaggedObject<T>, 
        peer: Arc<Neighbour>,
        attempt: u32
    ) -> Result<(Vec<u8>, Arc<Neighbour>, u64)> 
    where 
        T: BoxedSerialize + std::fmt::Debug 
    {

        let mut query = self.network_context.overlay.get_query_prefix(&self.overlay_id)?;
        serialize_boxed_append(&mut query, &request.object)?;
        let request_str = if log::log_enabled!(log::Level::Trace) || cfg!(feature = "telemetry") {
            format!("{}", std::any::type_name::<T>())
        } else {
            String::default()
        };

        log::trace!("USE PEER {}, {}", peer.id(), request_str);
        #[cfg(feature = "telemetry")]
        let now = Instant::now();
        let (answer, roundtrip) = self.network_context.overlay.query_via_rldp(
            &self.network_context.rldp,
            peer.id(),
            &TaggedByteSlice {
                object: &query[..],
                #[cfg(feature = "telemetry")]
                tag: request.tag
            },
            Some(10 * 1024 * 1024),
            peer.roundtrip_rldp().map(|t| t + attempt as u64 * Self::TIMEOUT_DELTA),
            &self.overlay_id
        ).await?;

        if let Some(answer) = answer {
            #[cfg(feature = "telemetry")]
            self.network_context.telemetry.consumed_query(
                request_str, 
                true, 
                now.elapsed(), 
                answer.len()
            );
            Ok((answer, peer, roundtrip))
        } else {
            #[cfg(feature = "telemetry")]
            self.network_context.telemetry.consumed_query(request_str, false, now.elapsed(), 0);
            self.peers.update_neighbour_stats(peer.id(), roundtrip, false, true, true)?;
            fail!("No RLDP answer to {:?} from {}", request.object, peer.id())
        }

    }

}

#[async_trait::async_trait]
impl FullNodeOverlayClient for NodeClientOverlay {

    async fn broadcast_external_message(&self, msg: &[u8]) -> Result<BroadcastSendInfo> {
        let broadcast = ExternalMessageBroadcast {
            message: ExternalMessage {
                data: ton::bytes(msg.to_vec())
            }
        }.into_boxed();
        let broadcast = TaggedByteSlice {
            object: &serialize_boxed(&broadcast)?,
            #[cfg(feature = "telemetry")]
            tag: self.tag_external_message_broadcast
        };
        self.network_context.overlay.broadcast(
            &self.overlay_id, 
            &broadcast, 
            None,
            self.network_context.broadcast_hops
        ).await
    }
    
    async fn send_block_broadcast(&self, broadcast: BlockBroadcast) -> Result<()> {
        let id = broadcast.id.clone();
        let broadcast = TaggedByteSlice {
            object: &serialize_boxed(&broadcast.into_boxed())?,
            #[cfg(feature = "telemetry")]
            tag: self.tag_block_broadcast
        };        
        let info = self.network_context.overlay.broadcast(
            &self.overlay_id, 
            &broadcast, 
            None,
            self.network_context.broadcast_hops
        ).await?;
        log::trace!(
            "send_block_broadcast {} (overlay {}) sent to {} nodes", 
            self.overlay_id, id, info.send_to
        );
        Ok(())
    }

    async fn send_queue_update_broadcast(&self, broadcast: QueueUpdateBroadcast) -> Result<()> {
        let id = broadcast.id.clone();
        let wc = broadcast.target_wc;
        let broadcast = TaggedByteSlice {
            object: &serialize_boxed(&broadcast.into_boxed())?,
            #[cfg(feature = "telemetry")]
            tag: self.tag_queue_update_broadcast
        };
        let info = self.network_context.overlay.broadcast(
            &self.overlay_id,
            &broadcast,
            None,
            self.network_context.broadcast_hops
        ).await?;
        log::trace!(
            "send_queue_update_broadcast {} for wc {} (overlay {}) sent to {} nodes", 
            self.overlay_id, wc, id, info.send_to
        );
        Ok(())
    }
    
    async fn send_top_shard_block_description(&self, tbd: &TopBlockDescrStuff) -> Result<()> {
        let broadcast = NewShardBlockBroadcast { 
            block: tbd.new_shard_block()? 
        }.into_boxed();
        let broadcast = TaggedByteSlice {
            object: &serialize_boxed(&broadcast)?,
            #[cfg(feature = "telemetry")]
            tag: self.tag_new_shard_block_broadcast
        };        
        let info = self.network_context.overlay.broadcast(
            &self.overlay_id, 
            &broadcast, 
            None,
            self.network_context.broadcast_hops
        ).await?;
        log::trace!(
            "send_top_shard_block_description {} (overlay {}) sent to {} nodes", 
            tbd.proof_for(), self.overlay_id, info.send_to
        );
        Ok(())
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
    ) -> Result<BlockProofStuff> {

        // Prepare
        let (prepare, good_peer): (PreparedProof, _) = if key_block {
            self.send_adnl_query(
                TaggedObject {
                    object: PrepareKeyBlockProof {
                        block: block_id.clone(),
                        allow_partial: is_link.into()
                    },
                    #[cfg(feature = "telemetry")]
                    tag: self.tag_prepare_key_block_proof
                },
                None, 
                Some(Self::TIMEOUT_PREPARE), 
                None
            ).await?
        } else {
            self.send_adnl_query(
                TaggedObject {
                    object: PrepareBlockProof {
                        block: block_id.clone(),
                        allow_partial: is_link.into()
                    },
                    #[cfg(feature = "telemetry")]
                    tag: self.tag_prepare_block_proof
                },
                None, 
                Some(Self::TIMEOUT_PREPARE), 
                None
            ).await?
        };

        // Download
        let (proof, is_link) = match prepare {
            PreparedProof::TonNode_PreparedProofEmpty => {
                fail!("Got `TonNode_PreparedProofEmpty` from {}", good_peer.id())
            },
            PreparedProof::TonNode_PreparedProof => {
                let proof = if key_block {
                    self.send_rldp_query_raw(
                        &TaggedObject {
                            object: DownloadKeyBlockProof { 
                                block: block_id.clone() 
                            },
                            #[cfg(feature = "telemetry")]
                            tag: self.tag_download_key_block_proof
                        },
                        good_peer,
                        0
                    ).await?
                } else {
                    self.send_rldp_query_raw(
                        &TaggedObject {
                            object: DownloadBlockProof { 
                                block: block_id.clone() 
                            },
                            #[cfg(feature = "telemetry")]
                            tag: self.tag_download_block_proof
                        },
                        good_peer,
                        0
                    ).await?
                };
                (proof, false)
            }, 
            PreparedProof::TonNode_PreparedProofLink => {
                let proof = if key_block {
                    self.send_rldp_query_raw(
                        &TaggedObject {
                            object: DownloadKeyBlockProofLink { 
                                block: block_id.clone()
                            },
                            #[cfg(feature = "telemetry")]
                            tag: self.tag_download_key_block_proof_link
                        },
                        good_peer,
                        0
                    ).await?
                } else {
                    self.send_rldp_query_raw(
                        &TaggedObject {
                            object: DownloadBlockProofLink { 
                                block: block_id.clone()
                            },
                            #[cfg(feature = "telemetry")]
                            tag: self.tag_download_block_proof_link
                        },
                        good_peer,
                        0
                    ).await?
                };
                (proof, true)
            }
        };
         
        BlockProofStuff::deserialize(block_id, proof, is_link)

/*
Ok(if key_block {
                BlockProofStuff::deserialize(block_id, self.send_rldp_query_raw(
                    &DownloadKeyBlockProof { block: convert_block_id_ext_blk2api(block_id) },
                    good_peer,
                    0
                ).await?, false)?
            } else {
                BlockProofStuff::deserialize(block_id, self.send_rldp_query_raw(
                    &DownloadBlockProof { block: convert_block_id_ext_blk2api(block_id), },
                    good_peer,
                    0
                ).await?, false)?
            }),
            PreparedProof::TonNode_PreparedProofLink => Ok(if key_block {
                BlockProofStuff::deserialize(block_id, self.send_rldp_query_raw(
                    &DownloadKeyBlockProofLink { block: convert_block_id_ext_blk2api(block_id), },
                    good_peer,
                    0
                ).await?, true)?
            } else {
                BlockProofStuff::deserialize(block_id, self.send_rldp_query_raw(
                    &DownloadBlockProofLink { block: convert_block_id_ext_blk2api(block_id), },
                    good_peer,
                    0
                ).await?, true)?
            })
            }
        }
*/
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
    ) -> Result<(BlockStuff, BlockProofStuff)> {

        // Prepare
        let (prepare, peer): (Prepared, _) = self.send_adnl_query(
            TaggedObject {
                object: PrepareBlock {
                    block: id.clone()
                },
                #[cfg(feature = "telemetry")]
                tag: self.tag_prepare_block
            },
            Some(1),
            None,
            None
        ).await?;
        log::trace!("USE PEER {}, PREPARE {} FINISHED", peer.id(), id);

        // Download
        match prepare {
            Prepared::TonNode_NotFound => fail!("Got `TonNode_NotFound` from {}", peer.id()),
            Prepared::TonNode_Prepared => {
                let (data_full, _): (DataFull, _) = self.send_rldp_query_typed(
                    &TaggedObject {
                        object: DownloadBlockFull {
                            block: id.clone(),
                        },
                        #[cfg(feature = "telemetry")]
                        tag: self.tag_download_block_full
                    },
                    peer,
                    0,
                ).await?;
                match data_full {
                    DataFull::TonNode_DataFullEmpty => {
                        fail!("prepareBlock receives Prepared, but DownloadBlockFull receives DataFullEmpty");
                    },
                    DataFull::TonNode_DataFull(data_full) => {
                        if id != &data_full.id {
                            fail!("Block with another id was received");
                        }
                        let block = BlockStuff::deserialize_block_checked(id.clone(), data_full.block.0)?;
                        let proof = BlockProofStuff::deserialize(
                            block.id(),
                            data_full.proof.0,
                            data_full.is_link.into())?;
                        Ok((block, proof))
                    }
                }
            }
        }

    }

    async fn download_out_msg_queue_update(
        &self,
        id: &BlockIdExt,
        target_wc: i32,
    ) -> Result<BlockStuff> {

        // Prepare
        let (prepare, good_peer): (Prepared, _) = self.send_adnl_query(
            TaggedObject {
                object: PrepareQueueUpdate {
                    block: id.clone(),
                    target_wc
                },
                #[cfg(feature = "telemetry")]
                tag: self.tag_prepare_queue_update
            },
            Some(1),
            None,
            None
        ).await?;
        log::trace!("download_out_msg_queue_update {}: {:?}, peer {}", id, prepare, good_peer.id());

        // Download
        match prepare {
            Prepared::TonNode_NotFound => fail!("Got `TonNode_NotFound` from {}", good_peer.id()),
            Prepared::TonNode_Prepared => {
                let update_bytes = self.send_rldp_query_raw(
                    &TaggedObject {
                        object: DownloadQueueUpdate {
                            block: id.clone(),
                            target_wc,
                        },
                        #[cfg(feature = "telemetry")]
                        tag: self.tag_download_queue_update
                    },
                    good_peer,
                    0
                ).await?;
                BlockStuff::deserialize_queue_update(id.clone(), target_wc, update_bytes)
            }
        }
    }

    // tonNode.preparePersistentState block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt = tonNode.PreparedState;
    // tonNode.preparePersistentMsgQueue block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt target_wc:int = tonNode.PreparedState;
    async fn check_persistent_state(
        &self,
        block_id: &BlockIdExt,
        msg_queue_for: Option<i32>,
        masterchain_block_id: &BlockIdExt,
        active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>,
    ) -> Result<Option<Arc<Neighbour>>> {
        let request = if let Some(target_wc) = msg_queue_for {
            TLObject::new(
                PreparePersistentMsgQueue {
                    block: block_id.clone(),
                    masterchain_block: masterchain_block_id.clone(),
                    target_wc
                }
            )
        } else {
            TLObject::new(
                PreparePersistentState {
                    block: block_id.clone(),
                    masterchain_block: masterchain_block_id.clone()
                }
            )
        };
        let (prepare, peer): (PreparedState, _) = self.send_adnl_query(
            TaggedTlObject {
                object: request,
                #[cfg(feature = "telemetry")]
                tag: self.tag_prepare_persistent_state
            },
            None,
            Some(Self::TIMEOUT_PREPARE), 
            Some(active_peers)
        ).await?;
        match prepare {
            PreparedState::TonNode_NotFoundState => {
                active_peers.remove(peer.id());
                Ok(None)
            },
            PreparedState::TonNode_PreparedState => Ok(Some(peer))
        }
    }

    // tonNode.downloadPersistentStateSlice block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt offset:long max_size:long = tonNode.Data;
    // tonNode.downloadPersistentMsgQueueSlice block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt target_wc:int offset:long max_size:long = tonNode.Data;
    async fn download_persistent_state_part(
        &self,
        block_id: &BlockIdExt,
        msg_queue_for: Option<i32>,
        masterchain_block_id: &BlockIdExt,
        offset: usize,
        max_size: usize,
        peer: Arc<Neighbour>,
        attempt: u32,
    ) -> Result<Vec<u8>> {
        if let Some(target_wc) = msg_queue_for {
            let request = TaggedObject {
                object: DownloadPersistentMsgQueueSlice {
                    block: block_id.clone(),
                    masterchain_block: masterchain_block_id.clone(),
                    offset: offset as i64,
                    max_size: max_size as i64,
                    target_wc
                },
                #[cfg(feature = "telemetry")]
                tag: self.tag_download_persistent_msg_queue_slice
            };
            self.send_rldp_query_raw(&request, peer, attempt).await
        } else {
            let request = TaggedObject {
                object: DownloadPersistentStateSlice {
                    block: block_id.clone(),
                    masterchain_block: masterchain_block_id.clone(),
                    offset: offset as i64,
                    max_size: max_size as i64,
                },
                #[cfg(feature = "telemetry")]
                tag: self.tag_download_persistent_state_slice
            };
            self.send_rldp_query_raw(&request, peer, attempt).await
        }
    }

    // tonNode.prepareZeroState block:tonNode.blockIdExt = tonNode.PreparedState;
    // tonNode.downloadZeroState block:tonNode.blockIdExt = tonNode.Data;
    async fn download_zero_state(
        &self, 
        id: &BlockIdExt, 
    ) -> Result<(Arc<ShardStateStuff>, Vec<u8>)> {
        // Prepare
        let (prepare, good_peer): (PreparedState, _) = self.send_adnl_query(
            TaggedTlObject {
                object: TLObject::new(
                    PrepareZeroState {
                        block: id.clone()
                    }
                ),
                #[cfg(feature = "telemetry")]
                tag: self.tag_prepare_zero_state
            },
            None,
            Some(Self::TIMEOUT_PREPARE),
            None
        ).await?;

        // Download
        match prepare {
            PreparedState::TonNode_NotFoundState => {
                fail!("Got `TonNode_NotFoundState` from {}", good_peer.id())
            },
            PreparedState::TonNode_PreparedState => {
                let state_bytes = self.send_rldp_query_raw(
                    &TaggedObject {
                        object: DownloadZeroState {
                            block: id.clone(),
                        },
                        #[cfg(feature = "telemetry")]
                        tag: self.tag_download_zero_state
                    },
                    good_peer,
                    0
                ).await?;
                let state = ShardStateStuff::deserialize_zerostate(
                    id.clone(), 
                    &state_bytes,
                    #[cfg(feature = "telemetry")]
                    &self.network_context.engine_telemetry,
                    &self.network_context.engine_allocated
                )?;
                Ok((state, state_bytes))
            }
        }
    }

    // tonNode.keyBlocks blocks:(vector tonNode.blockIdExt) incomplete:Bool error:Bool = tonNode.KeyBlocks;
    // tonNode.getNextKeyBlockIds block:tonNode.blockIdExt max_size:int = tonNode.KeyBlocks;
    async fn download_next_key_blocks_ids(
        &self, 
        block_id: &BlockIdExt, 
        max_size: i32, 
    ) -> Result<(Vec<BlockIdExt>, bool)> {
        let query = TaggedObject {
            object: GetNextKeyBlockIds {
                block: block_id.clone(),
                max_size
            },
            #[cfg(feature = "telemetry")]
            tag: self.tag_get_next_key_block_ids
        };
        let (ids, _): (KeyBlocks, _) = self.send_adnl_query(query, None, None, None).await?;
        let mut vec = Vec::new();
        for id in ids.blocks().iter() {
            vec.push(id.clone());
        }
        Ok((vec, ids.incomplete() == &Bool::BoolTrue))
    }

    // tonNode.downloadNextBlockFull prev_block:tonNode.blockIdExt = tonNode.DataFull;
    async fn download_next_block_full(
        &self, 
        prev_id: &BlockIdExt, 
    ) -> Result<(BlockStuff, BlockProofStuff)> {

        let request = TaggedObject { 
            object: DownloadNextBlockFull {
                prev_block: prev_id.clone()
            },
            #[cfg(feature = "telemetry")]
            tag: self.tag_download_next_block_full
        };

        // Set neighbor
        let peer = if let Some(p) = self.peers.choose_neighbour()? {
            p
        } else {
            tokio::time::sleep(Duration::from_millis(Self::TIMEOUT_NO_NEIGHBOURS)).await;
            fail!("neighbour is not found!")
        };
        log::trace!("USE PEER {}, REQUEST {:?}", peer.id(), request.object);
        
        // Download
        let (data_full, peer): (DataFull, _) = self.send_rldp_query_typed(
            &request,
            peer,
            0
        ).await?;

        // Parse
        match data_full {
            DataFull::TonNode_DataFullEmpty =>  {
                fail!("Got `TonNode_DataFullEmpty` from {}", peer.id())
            },
            DataFull::TonNode_DataFull(data_full) => {
                let block = BlockStuff::deserialize_block_checked(
                    data_full.id.clone(), 
                    data_full.block.to_vec()
                )?;
                let proof = BlockProofStuff::deserialize(
                    block.id(), 
                    data_full.proof.to_vec(), 
                    data_full.is_link.clone().into()
                )?;
                Ok((block, proof))
            }
        }

    }

    async fn download_archive(
        &self, 
        mc_seq_no: u32,
        active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>
    ) -> Result<Option<Vec<u8>>> {

        const CHUNK_SIZE: i32 = 1 << 21;
        // tonNode.getArchiveInfo masterchain_seqno:int = tonNode.ArchiveInfo;
        let (archive_info, peer) = self.send_adnl_query(
            TaggedObject {
                object: GetArchiveInfo {
                    masterchain_seqno: mc_seq_no as i32
                },
                #[cfg(feature = "telemetry")]
                tag: self.tag_get_archive_info
            },
            None,
            Some(Self::TIMEOUT_PREPARE),
            Some(active_peers)
        ).await?;

        match archive_info {
            ArchiveInfo::TonNode_ArchiveNotFound => {
                active_peers.remove(peer.id());
                Ok(None)
            },
            ArchiveInfo::TonNode_ArchiveInfo(info) => {
                let mut result = Vec::new();
                let mut offset = 0;
                let mut part_attempt = 0;
                let mut peer_attempt = 0;
                loop {
                    let slice = TaggedObject {
                        object: GetArchiveSlice {
                            archive_id: info.id, offset, max_size: CHUNK_SIZE
                        },
                        #[cfg(feature = "telemetry")]
                        tag: self.tag_get_archive_slice
                    };
                    match self.send_rldp_query_raw(&slice, peer.clone(), peer_attempt).await {
                        Ok(mut block_bytes) => {
                            let actual_size = block_bytes.len() as i32;
                            result.append(&mut block_bytes);
                            if actual_size < CHUNK_SIZE {
                                active_peers.remove(peer.id());
                                return Ok(Some(result))
                            }
                            offset += actual_size as i64;
                            part_attempt = 0;
                        },
                        Err(e) => {
                            peer_attempt += 1;
                            part_attempt += 1;
                            log::error!(
                                "download_archive {}: {}, offset: {}, attempt: {}",
                                info.id, e, offset, part_attempt
                            );
                            if part_attempt > 10 {
                                active_peers.remove(peer.id());
                                fail!(
                                    "Error download_archive after {} attempts : {}", 
                                    part_attempt, e
                                )
                            }
                        }
                    }
                }
            }
        }
    }

    async fn wait_broadcast(&self) -> Result<Option<(Broadcast, Arc<KeyId>)>> {
        loop {
            match self.network_context.overlay.wait_for_broadcast(&self.overlay_id).await? {
                Some(info) => {
                    let answer: Broadcast = Deserializer::new(
                        &mut Cursor::new(info.data)
                    ).read_boxed()?;
                    break Ok(Some((answer, info.recv_from)))
                },
                None => break Ok(None),
            }
        }
    }

}
