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

use crate::{
    engine_traits::EngineOperations, 
    network::neighbours::{PROTOCOL_CAPABILITIES, PROTOCOL_VERSION}
};

use adnl::common::{AdnlPeers, Answer, QueryResult, TaggedByteVec, TaggedObject};
use overlay::QueriesConsumer;
use std::{cmp::min, fmt::Debug, sync::Arc};
use ton_api::{serialize_boxed, tag_from_boxed_type, tag_from_boxed_object, 
    AnyBoxedSerialize, IntoBoxed,
    ton::{
        self, TLObject, Vector,
        rpc::ton_node::{
            DownloadNextBlockFull, DownloadPersistentStateSlice, DownloadZeroState,
            PreparePersistentState, GetNextBlockDescription,
            DownloadBlockProof, DownloadBlockProofLink, DownloadKeyBlockProof, DownloadKeyBlockProofLink,
            PrepareBlock, DownloadBlock, DownloadBlockFull, GetArchiveInfo,
            PrepareZeroState, GetNextKeyBlockIds, GetArchiveSlice, 
            PrepareBlockProof, PrepareKeyBlockProof, DownloadPersistentState, GetCapabilities

        },
        ton_node::{
            self,
            ArchiveInfo as ArchiveInfoBoxed, BlockDescription, Capabilities as CapabilitiesBoxed, 
            DataFull as DataFullBoxed, KeyBlocks, Prepared, PreparedProof, PreparedState,
            archiveinfo::ArchiveInfo, capabilities::Capabilities, datafull::DataFull
        }
    }
};
use ton_block::BlockIdExt;
use ton_types::{fail, Result};

// max part size for partially transmitted data like archives and states
const PART_MAX_SIZE: usize = 1 << 21; 

pub struct FullNodeOverlayService {
    engine: Arc<dyn EngineOperations>,
    #[cfg(feature = "telemetry")]
    tag_capabilities: u32,
    #[cfg(feature = "telemetry")]
    tag_key_blocks: u32
}

impl FullNodeOverlayService {

    pub fn new(engine: Arc<dyn EngineOperations>) -> Self {
        Self{
            engine,
            #[cfg(feature = "telemetry")]
            tag_capabilities: tag_from_boxed_type::<CapabilitiesBoxed>(),
            #[cfg(feature = "telemetry")]
            tag_key_blocks: tag_from_boxed_type::<KeyBlocks>(),
        }
    }

    // tonNode.getNextBlockDescription prev_block:tonNode.blockIdExt = tonNode.BlockDescription;
    async fn get_next_block_description(
        &self, 
        query: GetNextBlockDescription
    ) -> Result<TaggedObject<BlockDescription>> {
        let answer = match self.engine.load_block_next1(&query.prev_block).await {
            Ok(id) => {
                ton_node::blockdescription::BlockDescription{
                    id: id.into()
                }.into_boxed()
            }
            Err(_) => BlockDescription::TonNode_BlockDescriptionEmpty
        };
        #[cfg(feature = "telemetry")]
        let tag = tag_from_boxed_object(&answer);
        let answer = TaggedObject {
            object: answer,
            #[cfg(feature = "telemetry")]
            tag
        };
        Ok(answer)
    }

    // tonNode.getNextBlocksDescription prev_block:tonNode.blockIdExt limit:int = tonNode.BlocksDescription;
    // Not supported in t-node

    // tonNode.getPrevBlocksDescription next_block:tonNode.blockIdExt limit:int cutoff_seqno:int = tonNode.BlocksDescription;
    // Not supported in t-node

    fn prepare_block_proof_internal(
        &self, 
        block_id: BlockIdExt,
        allow_partial: bool,
        key_block: bool
    ) -> Result<TaggedObject<PreparedProof>> {
        let answer = if let Some(handle) = self.engine.load_block_handle(&block_id)? {
            if key_block && !handle.is_key_block()? {
                fail!("prepare_key_block_proof: given block is not key");
            }
            if !handle.has_proof() && (!allow_partial || !handle.has_proof_link()) {
                PreparedProof::TonNode_PreparedProofEmpty
            }
            else if handle.has_proof() && handle.id().shard().is_masterchain() {
                PreparedProof::TonNode_PreparedProof
            } else {
                PreparedProof::TonNode_PreparedProofLink
            }
        } else {
            PreparedProof::TonNode_PreparedProofEmpty
        };
        #[cfg(feature = "telemetry")]
        let tag = tag_from_boxed_object(&answer);
        let answer = TaggedObject {
            object: answer,
            #[cfg(feature = "telemetry")]
            tag
        };
        Ok(answer)
    }

    // tonNode.prepareBlockProof block:tonNode.blockIdExt allow_partial:Bool = tonNode.PreparedProof;
    async fn prepare_block_proof(
        &self, 
        query: PrepareBlockProof
    ) -> Result<TaggedObject<PreparedProof>> {
        self.prepare_block_proof_internal(query.block, query.allow_partial.into(), false)
    }

    // tonNode.prepareKeyBlockProof block:tonNode.blockIdExt allow_partial:Bool = tonNode.PreparedProof;
    async fn prepare_key_block_proof(
        &self, 
        query: PrepareKeyBlockProof
    ) -> Result<TaggedObject<PreparedProof>> {
        self.prepare_block_proof_internal(query.block, query.allow_partial.into(), true)
    }

    // tonNode.prepareBlockProofs blocks:(vector tonNode.blockIdExt) allow_partial:Bool = tonNode.PreparedProof;
    // Not supported in t-node

    // tonNode.prepareKeyBlockProofs blocks:(vector tonNode.blockIdExt) allow_partial:Bool = tonNode.PreparedProof;
    // Not supported in t-node

    // tonNode.prepareBlock block:tonNode.blockIdExt = tonNode.Prepared;
    async fn prepare_block(&self, query: PrepareBlock) -> Result<TaggedObject<Prepared>> {
        let answer = if let Some(handle) = self.engine.load_block_handle(&query.block)? {
            if handle.has_data() {
                Prepared::TonNode_Prepared
            } else {
                Prepared::TonNode_NotFound
            }
        } else {
            Prepared::TonNode_NotFound
        };
        #[cfg(feature = "telemetry")]
        let tag = tag_from_boxed_object(&answer);
        let answer = TaggedObject {
            object: answer,
            #[cfg(feature = "telemetry")]
            tag
        };
        Ok(answer)
    }

    // tonNode.prepareBlocks blocks:(vector tonNode.blockIdExt) = tonNode.Prepared;
    // Not supported in t-node

    fn prepare_state_internal(
        &self, 
        block_id: BlockIdExt
    ) -> Result<TaggedObject<PreparedState>> {
        let answer = if let Some(handle) = self.engine.load_block_handle(&block_id)? {
            if handle.has_persistent_state() {
                PreparedState::TonNode_PreparedState
            } else {
                PreparedState::TonNode_NotFoundState
            }
        } else {
            PreparedState::TonNode_NotFoundState
        };
        #[cfg(feature = "telemetry")]
        let tag = tag_from_boxed_object(&answer);
        let answer = TaggedObject {
            object: answer,
            #[cfg(feature = "telemetry")]
            tag
        };
        Ok(answer)
    }

    // tonNode.preparePersistentState block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt = tonNode.PreparedState;
    async fn prepare_persistent_state(
        &self, 
        query: PreparePersistentState
    ) -> Result<TaggedObject<PreparedState>> {
        self.prepare_state_internal(query.block)
    }

    // tonNode.prepareZeroState block:tonNode.blockIdExt = tonNode.PreparedState;
    async fn prepare_zero_state(
        &self, 
        query: PrepareZeroState
    ) -> Result<TaggedObject<PreparedState>> {
        self.prepare_state_internal(query.block)
    }

    const NEXT_KEY_BLOCKS_LIMIT: usize = 8;

    fn build_next_key_blocks_answer(blocks: Vec<BlockIdExt>, incomplete: bool, error: bool) -> KeyBlocks {
        ton_node::keyblocks::KeyBlocks {
            blocks: Vector::from(blocks),
            incomplete: incomplete.into(),
            error: error.into(),
        }.into_boxed()
    }

    async fn get_next_key_block_ids_(
        &self,
        start_block_id: &BlockIdExt,
        limit: usize
    ) -> Result<KeyBlocks> {
        if !start_block_id.shard().is_masterchain() {
            fail!("Given block {} doesn't belong master chain", start_block_id);
        }

        let last_mc_state = match self.engine.load_last_applied_mc_block_id()? {
            Some(block_id) if start_block_id.seq_no() < block_id.seq_no() => {
                self.engine.load_state(&block_id).await?
            }
            _ => {
                return Ok(ton_node::keyblocks::KeyBlocks {
                    blocks: Vec::new().into(),
                    incomplete: false.into(),
                    error: true.into(),
                }.into_boxed())
            }
        };
        let prev_blocks = &last_mc_state.shard_state_extra()?.prev_blocks;

        if start_block_id.seq_no != 0 {
            // check if start block is key-block
            prev_blocks.check_key_block(start_block_id, Some(true))?;
        }

        let mut ids = vec!();
        let mut seq_no = start_block_id.seq_no();
        while let Some(id) = prev_blocks.get_next_key_block(seq_no + 1)? {
            seq_no = id.seq_no;
            let ext_id = id.master_block_id().1;
            ids.push(ext_id);
            if ids.len() == limit {
                break;
            }
        }
        let incomplete = ids.len() < limit;
        Ok(Self::build_next_key_blocks_answer(ids, incomplete, false))
    }

    // tonNode.getNextKeyBlockIds block:tonNode.blockIdExt max_size:int = tonNode.KeyBlocks;
    async fn get_next_key_block_ids(
        &self, 
        query: GetNextKeyBlockIds
    ) -> Result<TaggedObject<KeyBlocks>> {
        let limit = min(Self::NEXT_KEY_BLOCKS_LIMIT, query.max_size as usize);
        let answer = match self.get_next_key_block_ids_(&query.block, limit).await {
            Err(e) => {
                log::warn!("tonNode.getNextKeyBlockIds: {:?}", e);
                Self::build_next_key_blocks_answer(vec!(), false, true)
            },
            Ok(r) => r
        };
        let answer = TaggedObject {
            object: answer,
            #[cfg(feature = "telemetry")]
            tag: self.tag_key_blocks
        };
        Ok(answer)
    }

    // tonNode.downloadNextBlockFull prev_block:tonNode.blockIdExt = tonNode.DataFull;
    async fn download_next_block_full(
        &self, 
        query: DownloadNextBlockFull
    ) -> Result<TaggedObject<DataFullBoxed>> {
        let mut answer = DataFullBoxed::TonNode_DataFullEmpty;
        if let Some(prev_handle) = self.engine.load_block_handle(&query.prev_block)? {
            if prev_handle.has_next1() {
                let next_id = self.engine.load_block_next1(&query.prev_block).await?;
                if let Some(next_handle) = self.engine.load_block_handle(&next_id)? {
                    let mut is_link = false;
                    if next_handle.has_data() && next_handle.has_proof_or_link(&mut is_link) {
                        let block = self.engine.load_block_raw(&next_handle).await?;
                        let proof = self.engine.load_block_proof_raw(&next_handle, is_link).await?;
                        answer = DataFull {
                            id: next_id.into(),
                            proof: ton::bytes(proof),
                            block: ton::bytes(block),
                            is_link: if is_link { 
                                ton::Bool::BoolTrue 
                            } else { 
                                ton::Bool::BoolFalse 
                            }
                        }.into_boxed();
                    }
                }
            }
        }
        #[cfg(feature = "telemetry")]
        let tag = tag_from_boxed_object(&answer);
        let answer = TaggedObject {
            object: answer,
            #[cfg(feature = "telemetry")]
            tag
        };
        Ok(answer)
    }

    // tonNode.downloadBlockFull block:tonNode.blockIdExt = tonNode.DataFull;
    async fn download_block_full(
        &self, 
        query: DownloadBlockFull
    ) -> Result<TaggedObject<DataFullBoxed>> {
        let mut answer = DataFullBoxed::TonNode_DataFullEmpty;
        if let Some(handle) = self.engine.load_block_handle(&query.block)? {
            let mut is_link = false;
            if handle.has_data() && handle.has_proof_or_link(&mut is_link) {
                let block = self.engine.load_block_raw(&handle).await?;
                let proof = self.engine.load_block_proof_raw(&handle, is_link).await?;
                answer = DataFull {
                    id: query.block,
                    proof: ton_api::ton::bytes(proof),
                    block: ton_api::ton::bytes(block),
                    is_link: if is_link { ton::Bool::BoolTrue } else { ton::Bool::BoolFalse }
                }.into_boxed();
            }
        }
        #[cfg(feature = "telemetry")]
        let tag = tag_from_boxed_object(&answer);
        let answer = TaggedObject {
            object: answer,
            #[cfg(feature = "telemetry")]
            tag
        };
        Ok(answer)
    }

    // tonNode.downloadBlock block:tonNode.blockIdExt = tonNode.Data;
    async fn download_block(&self, query: DownloadBlock) -> Result<TaggedByteVec> {
        if let Some(handle) = self.engine.load_block_handle(&query.block)? {
            if handle.has_data() {
                let answer = TaggedByteVec {
                    object: self.engine.load_block_raw(&handle).await?,
                    #[cfg(feature = "telemetry")]
                    tag: 0x8000000A // Raw reply do download block
                };
                return Ok(answer)
            }
        }
        fail!("Block's data isn't initialized");
    }

    // tonNode.downloadBlocks blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    // Not supported in t-node

    // tonNode.downloadPersistentState block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt = tonNode.Data;
    async fn download_persistent_state(
        &self, 
        query: DownloadPersistentState
    ) -> Result<TaggedByteVec> {
        // This request is never called in t-node, because new downloadPersistentStateSlice exists.
        // Because of state is pretty big it is bad idea to send it by one request.
        fail!(
            "`tonNode.downloadPersistentState` request is not supported (block: {}, mc block: {})",
            query.block,
            query.masterchain_block
        );
    }

    // tonNode.downloadPersistentStateSlice block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt offset:long max_size:long = tonNode.Data;
    async fn download_persistent_state_slice(
        &self, 
        query: DownloadPersistentStateSlice
    ) -> Result<TaggedByteVec> {
        if query.max_size as usize > PART_MAX_SIZE {
            fail!("Part size {} is too big, max is {}", query.max_size, PART_MAX_SIZE);
        }
        if let Some(handle) = self.engine.load_block_handle(&query.block)? {
            if handle.has_persistent_state() {
                let data = self.engine.load_persistent_state_slice(
                    &handle,
                    query.offset as u64,
                    query.max_size as u64
                ).await?;
                let answer = TaggedByteVec {
                    object: data,
                    #[cfg(feature = "telemetry")]
                    tag: 0x8000000B // Raw reply to download state slice
                };
                return Ok(answer)
            }             
        }
        fail!("Shard state {} doesn't have a persistent state", query.block)
    }

    // tonNode.downloadZeroState block:tonNode.blockIdExt = tonNode.Data;
    async fn download_zero_state(&self, query: DownloadZeroState) -> Result<TaggedByteVec> {
        if let Some(handle) = self.engine.load_block_handle(&query.block)? {
            if handle.has_persistent_state() {
                let size = self.engine.load_persistent_state_size(&query.block).await?;
                let data = self.engine.load_persistent_state_slice(&handle, 0, size).await?;
                let answer = TaggedByteVec {
                    object: data,
                    #[cfg(feature = "telemetry")]
                    tag: 0x8000000C // Raw reply to download zero state
                };
                return Ok(answer)
            }
        }
        fail!("Zero state {} is not inited", query.block)
    }

    async fn download_block_proof_internal(
        &self, 
        block_id: BlockIdExt, 
        is_link: bool, 
        _key_block: bool
    ) -> Result<TaggedByteVec> {
        if let Some(handle) = self.engine.load_block_handle(&block_id)? {
            if (is_link && handle.has_proof_link()) || (!is_link && handle.has_proof()) {
                let answer = TaggedByteVec {
                    object: self.engine.load_block_proof_raw(&handle, is_link).await?,
                    #[cfg(feature = "telemetry")]
                    tag: 0x8000000D // Raw reply to download proof
                };
                return Ok(answer)
            }
        }
        if is_link {
            fail!("Block's proof link isn't initialized")
        } else {
            fail!("Block's proof isn't initialized")
        }
    }

    // tonNode.downloadBlockProof block:tonNode.blockIdExt = tonNode.Data;
    async fn download_block_proof(&self, query: DownloadBlockProof) -> Result<TaggedByteVec> {
        self.download_block_proof_internal(query.block, false, false).await
    }

    // tonNode.downloadKeyBlockProof block:tonNode.blockIdExt = tonNode.Data;
    async fn download_key_block_proof(
        &self, 
        query: DownloadKeyBlockProof
    ) -> Result<TaggedByteVec> {
        self.download_block_proof_internal(query.block, false, true).await
    }

    // tonNode.downloadBlockProofs blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    // Not supported in t-node
    
    // tonNode.downloadKeyBlockProofs blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    // Not supported in t-node

    // tonNode.downloadBlockProofLink block:tonNode.blockIdExt = tonNode.Data;
    async fn download_block_proof_link(
        &self, 
        query: DownloadBlockProofLink
    ) -> Result<TaggedByteVec> {
        self.download_block_proof_internal(query.block, true, false).await
    }

    // tonNode.downloadKeyBlockProofLink block:tonNode.blockIdExt = tonNode.Data;
    async fn download_key_block_proof_link(
        &self, 
        query: DownloadKeyBlockProofLink
    ) -> Result<TaggedByteVec> {
        self.download_block_proof_internal(query.block, true, true).await
    }

    // tonNode.downloadBlockProofLinks blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    // Not supported in t-node

    // tonNode.downloadKeyBlockProofLinks blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    // Not supported in t-node

    // tonNode.getArchiveInfo masterchain_seqno:int = tonNode.ArchiveInfo;
    async fn get_archive_info(
        &self, 
        query: GetArchiveInfo
    ) -> Result<TaggedObject<ArchiveInfoBoxed>> {
        let mut answer = ArchiveInfoBoxed::TonNode_ArchiveNotFound;
        if let Some(id) = self.engine.load_last_applied_mc_block_id()? {
            if query.masterchain_seqno as u32 <= id.seq_no() {
                if let Some(id) = self.engine.load_shard_client_mc_block_id()? {
                    if query.masterchain_seqno as u32 <= id.seq_no() {
                        if let Some(id) = self.engine.get_archive_id(
                            query.masterchain_seqno as u32
                        ).await {
                            answer = ArchiveInfo {
                                id: id as ton::long
                            }.into_boxed()
                        }
                    }
                }
            }
        }                
        #[cfg(feature = "telemetry")]
        let tag = tag_from_boxed_object(&answer);
        let answer = TaggedObject {
            object: answer,
            #[cfg(feature = "telemetry")]
            tag
        };
        Ok(answer)
    }

    // tonNode.getArchiveSlice archive_id:long offset:long max_size:int = tonNode.Data;
    async fn get_archive_slice(&self, query: GetArchiveSlice) -> Result<TaggedByteVec> {
        if query.max_size as usize > PART_MAX_SIZE {
            fail!("Part size {} is too big, max is {}", query.max_size, PART_MAX_SIZE);
        }
        let answer = TaggedByteVec {
            object: self.engine.get_archive_slice(
                query.archive_id as u64, 
                query.offset as u64, 
                query.max_size as u32
            ).await?,
            #[cfg(feature = "telemetry")]
            tag: 0x8000000E // Raw reply to download archive slice
        };
        Ok(answer)
    }

    // tonNode.getCapabilities = tonNode.Capabilities;
    async fn get_capabilities(
        &self, 
        _query: GetCapabilities
    ) -> Result<TaggedObject<CapabilitiesBoxed>> {
        let answer = TaggedObject {
            object: Capabilities {
                version: PROTOCOL_VERSION,
                capabilities: PROTOCOL_CAPABILITIES,
            }.into_boxed(),
            #[cfg(feature = "telemetry")]
            tag: self.tag_capabilities
        };
        Ok(answer)
    }

    async fn consume_query<'a, Q, A, F>(
        &'a self,
        query: TLObject,
        consumer: &'a (dyn Fn(&'a Self, Q) -> F + Send + Sync)
    ) -> Result<std::result::Result<QueryResult, TLObject>>
    where
        Q: AnyBoxedSerialize + Debug,
        A: AnyBoxedSerialize,
        F: futures::Future<Output = Result<TaggedObject<A>>>,
    {
        Ok(
            match query.downcast::<Q>() {
                Ok(query) => {
                    let query_str = if log::log_enabled!(log::Level::Trace) || cfg!(feature = "telemetry") {
                        format!("{}", std::any::type_name::<Q>())
                    } else {
                        String::default()
                    };
                    log::trace!("consume_query: before consume query {}", query_str);
                    #[cfg(feature = "telemetry")]
                    let now = std::time::Instant::now();
                    let answer = match consumer(self, query).await {
                        Ok(answer) => {
                            let answer = TaggedByteVec {
                                object: serialize_boxed(&answer.object)?,
                                #[cfg(feature = "telemetry")]
                                tag: answer.tag
                            };    
                            log::trace!("consume_query: consumed {}", query_str);
                            #[cfg(feature = "telemetry")]
                            self.engine.full_node_service_telemetry().consumed_query(
                                query_str, true, now.elapsed(), answer.object.len()
                            );
                            answer
                        }
                        Err(e) => {
                            log::trace!("consume_query: consumed {}, error {:?}", query_str, e);
                            #[cfg(feature = "telemetry")]
                            self.engine.full_node_service_telemetry().consumed_query(
                                query_str, false, now.elapsed(), 0
                            );
                            return Err(e)
                        }
                    };
                    Ok(QueryResult::Consumed(Some(Answer::Raw(answer))))
                },
                Err(query) => Err(query)
            }
        )
    }

    async fn consume_query_raw<'a, Q, F>(
        &'a self,
        query: TLObject,
        consumer: &'a (dyn Fn(&'a Self, Q) -> F + Send + Sync)
    ) -> Result<std::result::Result<QueryResult, TLObject>>
    where
        Q: AnyBoxedSerialize + Debug,
        F: futures::Future<Output = Result<TaggedByteVec>>,
    {
        Ok(
            match query.downcast::<Q>() {
                Ok(query) => {
                    let query_str = if log::log_enabled!(log::Level::Trace) || cfg!(feature = "telemetry") {
                        format!("{}", std::any::type_name::<Q>())
                    } else {
                        String::default()
                    };
                    log::trace!("consume_query_raw: before consume query {}", query_str);
                    #[cfg(feature = "telemetry")]
                    let now = std::time::Instant::now();
                    let answer = match consumer(self, query).await {
                        Ok(answer) => {
                            #[cfg(feature = "telemetry")]
                            log::trace!("consume_query_raw: consumed {}", query_str);
                            #[cfg(feature = "telemetry")]
                            self.engine.full_node_service_telemetry().consumed_query(
                                query_str, true, now.elapsed(), answer.object.len()
                            );
                            answer
                        }
                        Err(e) => {
                            #[cfg(feature = "telemetry")]
                            log::trace!("consume_query_raw: consumed {}, error {:?}", query_str, e);
                            #[cfg(feature = "telemetry")]
                            self.engine.full_node_service_telemetry().consumed_query(
                                query_str, false, now.elapsed(), 0
                            );
                            return Err(e)
                        }
                    };
                    Ok(QueryResult::Consumed(Some(Answer::Raw(answer))))
                },
                Err(query) => Err(query)
            }
        )
    }

}

#[async_trait::async_trait]
impl QueriesConsumer for FullNodeOverlayService {
    #[allow(dead_code)]
    async fn try_consume_query(&self, query: TLObject, _adnl_peers: &AdnlPeers) -> Result<QueryResult> {

        log::debug!("try_consume_query {:?}", query);

        let query = match self.consume_query::<GetNextBlockDescription, _, _>(
            query,
            &Self::get_next_block_description
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<PrepareBlockProof, _, _>(
            query,
            &Self::prepare_block_proof
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<PrepareKeyBlockProof, _, _>(
            query,
            &Self::prepare_key_block_proof
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<PrepareBlock, _, _>(
            query,
            &Self::prepare_block
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<PreparePersistentState, _, _>(
            query,
            &Self::prepare_persistent_state
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<PrepareZeroState, _, _>(
            query,
            &Self::prepare_zero_state
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<GetNextKeyBlockIds, _, _>(
            query,
            &Self::get_next_key_block_ids
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<DownloadNextBlockFull, _, _>(
            query,
            &Self::download_next_block_full
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<DownloadBlockFull, _, _>(
            query,
            &Self::download_block_full
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query_raw::<DownloadBlock, _>(
            query,
            &Self::download_block
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query_raw::<DownloadPersistentState, _>(
            query,
            &Self::download_persistent_state
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query_raw::<DownloadPersistentStateSlice, _>(
            query,
            &Self::download_persistent_state_slice
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query_raw::<DownloadZeroState, _>(
            query,
            &Self::download_zero_state
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query_raw::<DownloadBlockProof, _>(
            query,
            &Self::download_block_proof
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query_raw::<DownloadKeyBlockProof, _>(
            query,
            &Self::download_key_block_proof
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query_raw::<DownloadBlockProofLink, _>(
            query,
            &Self::download_block_proof_link
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query_raw::<DownloadKeyBlockProofLink, _>(
            query,
            &Self::download_key_block_proof_link
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<GetArchiveInfo, _, _>(
            query,
            &Self::get_archive_info
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query_raw::<GetArchiveSlice, _>(
            query,
            &Self::get_archive_slice
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<GetCapabilities, _, _>(
            query,
            &Self::get_capabilities
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        log::warn!("Unsupported full node query {:?}", query);
        failure::bail!("Unsupported full node query {:?}", query);
    }
}
