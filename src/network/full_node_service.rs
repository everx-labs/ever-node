use crate::{
    engine_traits::EngineOperations, 
    network::neighbours::{PROTOCOL_CAPABILITIES, PROTOCOL_VERSION}
};

use adnl::common::{AdnlPeers, QueryResult, Answer};
use overlay::QueriesConsumer;
use std::{sync::Arc, convert::TryInto, cmp::min, fmt::Debug};
use ton_api::{BoxedSerialize};
use ton_api::{
    IntoBoxed, AnyBoxedSerialize,
    ton::{
        self, TLObject, Vector,
        rpc::{
            ton_node::{
                DownloadNextBlockFull, DownloadPersistentStateSlice, DownloadZeroState,
                PreparePersistentState, GetNextBlockDescription,
                DownloadBlockProof, DownloadBlockProofLink, DownloadKeyBlockProof, DownloadKeyBlockProofLink,
                PrepareBlock, DownloadBlock, DownloadBlockFull, GetArchiveInfo,
                PrepareZeroState, GetNextKeyBlockIds, GetArchiveSlice, 
                PrepareBlockProof, PrepareKeyBlockProof, DownloadPersistentState, GetCapabilities

            }
        },
        ton_node::{
            self,
            ArchiveInfo, BlockDescription, DataFull, KeyBlocks,
            Prepared, PreparedProof, PreparedState, Capabilities,

        }
    }
};
use ton_block::BlockIdExt;
use ton_types::{fail, Result};

pub struct FullNodeOverlayService {
    engine: Arc<dyn EngineOperations>
}

impl FullNodeOverlayService {
    pub fn new(engine: Arc<dyn EngineOperations>) -> Self {
        Self{engine}
    }

    // tonNode.getNextBlockDescription prev_block:tonNode.blockIdExt = tonNode.BlockDescription;
    async fn get_next_block_description(&self, query: GetNextBlockDescription) -> Result<BlockDescription> {
        let prev_id = (&query.prev_block).try_into()?;
        let answer = match self.engine.load_block_next1(&prev_id).await {
            Ok(id) => { 
                BlockDescription::TonNode_BlockDescription(
                    Box::new(
                        ton_node::blockdescription::BlockDescription{
                            id: id.into()
                        }
                    )
                )
            },
            Err(_) => BlockDescription::TonNode_BlockDescriptionEmpty
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
    ) -> Result<PreparedProof> {
        if let Some(handle) = self.engine.load_block_handle(&block_id)? {
            if key_block && !handle.is_key_block()? {
                fail!("prepare_key_block_proof: given block is not key");
            }
            if !handle.has_proof() && (!allow_partial || !handle.has_proof_link()) {
                Ok(PreparedProof::TonNode_PreparedProofEmpty)
            }
            else if handle.has_proof() && handle.id().shard().is_masterchain() {
                Ok(PreparedProof::TonNode_PreparedProof)
            } else {
                Ok(PreparedProof::TonNode_PreparedProofLink)
            }
        } else {
            Ok(PreparedProof::TonNode_PreparedProofEmpty)
        }
    }

    // tonNode.prepareBlockProof block:tonNode.blockIdExt allow_partial:Bool = tonNode.PreparedProof;
    async fn prepare_block_proof(&self, query: PrepareBlockProof) -> Result<PreparedProof> {
        self.prepare_block_proof_internal(
            (&query.block).try_into()?, 
            query.allow_partial.into(),
            false
        )
    }

    // tonNode.prepareKeyBlockProof block:tonNode.blockIdExt allow_partial:Bool = tonNode.PreparedProof;
    async fn prepare_key_block_proof(&self, query: PrepareKeyBlockProof) -> Result<PreparedProof> {
        self.prepare_block_proof_internal(
            (&query.block).try_into()?, 
            query.allow_partial.into(),
            true
        )
    }

    // tonNode.prepareBlockProofs blocks:(vector tonNode.blockIdExt) allow_partial:Bool = tonNode.PreparedProof;
    // Not supported in t-node

    // tonNode.prepareKeyBlockProofs blocks:(vector tonNode.blockIdExt) allow_partial:Bool = tonNode.PreparedProof;
    // Not supported in t-node

    // tonNode.prepareBlock block:tonNode.blockIdExt = tonNode.Prepared;
    async fn prepare_block(&self, query: PrepareBlock) -> Result<Prepared> {
        let block_id = (&query.block).try_into()?;
        if let Some(handle) = self.engine.load_block_handle(&block_id)? {
            if handle.has_data() {
                Ok(Prepared::TonNode_Prepared)
            } else {
                Ok(Prepared::TonNode_NotFound)
            }
        } else {
            Ok(Prepared::TonNode_NotFound)
        }
    }

    // tonNode.prepareBlocks blocks:(vector tonNode.blockIdExt) = tonNode.Prepared;
    // Not supported in t-node

    fn prepare_state_internal(&self, block_id: BlockIdExt) -> Result<PreparedState> {
        if let Some(handle) = self.engine.load_block_handle(&block_id)? {
            Ok(
                if handle.has_persistent_state() {
                    PreparedState::TonNode_PreparedState
                } else {
                    PreparedState::TonNode_NotFoundState
                }
            )
        } else {
            Ok(PreparedState::TonNode_NotFoundState)
        }
    }

    // tonNode.preparePersistentState block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt = tonNode.PreparedState;
    async fn prepare_persistent_state(&self, query: PreparePersistentState) -> Result<PreparedState> {
        self.prepare_state_internal((&query.block).try_into()?)
    }

    // tonNode.prepareZeroState block:tonNode.blockIdExt = tonNode.PreparedState;
    async fn prepare_zero_state(&self, query: PrepareZeroState) -> Result<PreparedState> {
        self.prepare_state_internal((&query.block).try_into()?)
    }

    const NEXT_KEY_BLOCKS_LIMIT: usize = 8;

    fn build_next_key_blocks_answer(blocks: Vec<BlockIdExt>, incomplete: bool, error: bool) -> KeyBlocks {
        let mut blocks_vec = Vector::default();
        blocks_vec.0 = blocks
            .into_iter()
            .map(|id| ton_node::blockidext::BlockIdExt::from(id))
            .collect();
        KeyBlocks::TonNode_KeyBlocks(
            Box::new(
                ton_node::keyblocks::KeyBlocks{
                    blocks: blocks_vec,
                    incomplete: incomplete.into(),
                    error: error.into(),
                }
            )
        )
    }

    async fn get_next_key_block_ids_(
        &self,
        start_block_id: &BlockIdExt,
        limit: usize
    ) -> Result<KeyBlocks> {
        if !start_block_id.shard().is_masterchain() {
            fail!("Given block {} doesn't belong master chain", start_block_id);
        }

        let last_mc_state = self.engine.load_last_applied_mc_state().await?;
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
    async fn get_next_key_block_ids(&self, query: GetNextKeyBlockIds) -> Result<KeyBlocks> {
        let start_block_id = (&query.block).try_into()?;
        let limit = min(Self::NEXT_KEY_BLOCKS_LIMIT, query.max_size as usize);

        Ok(
            match self.get_next_key_block_ids_(&start_block_id, limit).await {
                Err(e) => {
                    log::warn!("tonNode.getNextKeyBlockIds: {:?}", e);
                    Self::build_next_key_blocks_answer(vec!(), false, true)
                },
                Ok(r) => r
            }
        )
    }

    // tonNode.downloadNextBlockFull prev_block:tonNode.blockIdExt = tonNode.DataFull;
    async fn download_next_block_full(&self, query: DownloadNextBlockFull) -> Result<DataFull> {
        let block_id = (&query.prev_block).try_into()?;
        if let Some(prev_handle) = self.engine.load_block_handle(&block_id)? {
            if prev_handle.has_next1() {
                let next_id = self.engine.load_block_next1(&block_id).await?;
                if let Some(next_handle) = self.engine.load_block_handle(&next_id)? {
                    let mut is_link = false;
                    if next_handle.has_data() && next_handle.has_proof_or_link(&mut is_link) {
                        let block = self.engine.load_block_raw(&next_handle).await?;
                        let proof = self.engine.load_block_proof_raw(&next_handle, is_link).await?;
                        return Ok(DataFull::TonNode_DataFull(Box::new(
                            ton_node::datafull::DataFull{
                                id: next_id.into(),
                                proof: ton_api::ton::bytes(proof),
                                block: ton_api::ton::bytes(block),
                                is_link: if is_link { 
                                    ton::Bool::BoolTrue 
                                } else { 
                                    ton::Bool::BoolFalse 
                                }
                            }
                        )));
                    }
                }
            }
        }
        Ok(DataFull::TonNode_DataFullEmpty)
    }

    // tonNode.downloadBlockFull block:tonNode.blockIdExt = tonNode.DataFull;
    async fn download_block_full(&self, query: DownloadBlockFull) -> Result<DataFull> {
        let block_id = (&query.block).try_into()?;
        if let Some(handle) = self.engine.load_block_handle(&block_id)? {
            let mut is_link = false;
            if handle.has_data() && handle.has_proof_or_link(&mut is_link) {
                let block = self.engine.load_block_raw(&handle).await?;
                let proof = self.engine.load_block_proof_raw(&handle, is_link).await?;
                return Ok(DataFull::TonNode_DataFull(Box::new(
                    ton_node::datafull::DataFull{
                        id: query.block,
                        proof: ton_api::ton::bytes(proof),
                        block: ton_api::ton::bytes(block),
                        is_link: if is_link { ton::Bool::BoolTrue } else { ton::Bool::BoolFalse }
                    }
                )))
            }
        }
        Ok(DataFull::TonNode_DataFullEmpty)
    }

    // tonNode.downloadBlock block:tonNode.blockIdExt = tonNode.Data;
    async fn download_block(&self, query: DownloadBlock) -> Result<Vec<u8>> {
        let block_id = (&query.block).try_into()?;
        if let Some(handle) = self.engine.load_block_handle(&block_id)? {
            if handle.has_data() {
                return Ok(self.engine.load_block_raw(&handle).await?)
            }
        }
        fail!("Block's data isn't initialized");
    }

    // tonNode.downloadBlocks blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    // Not supported in t-node

    // tonNode.downloadPersistentState block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt = tonNode.Data;
    async fn download_persistent_state(&self, query: DownloadPersistentState) -> Result<Vec<u8>> {
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
    ) -> Result<Vec<u8>> {
        let block_id = (&query.block).try_into()?;
        if let Some(handle) = self.engine.load_block_handle(&block_id)? {
            if handle.has_persistent_state() {
                let data = self.engine.load_persistent_state_slice(
                    &handle,
                    query.offset as u64,
                    query.max_size as u64
                ).await?;
                return Ok(data)
            }             
        }
        fail!("Shard state {} doesn't have a persistent state", block_id);
    }

    // tonNode.downloadZeroState block:tonNode.blockIdExt = tonNode.Data;
    async fn download_zero_state(&self, query: DownloadZeroState) -> Result<Vec<u8>> {
        let block_id = (&query.block).try_into()?;
        if let Some(handle) = self.engine.load_block_handle(&block_id)? {
            if handle.has_persistent_state() {
                let size = self.engine.load_persistent_state_size(&block_id).await?;
                let data = self.engine.load_persistent_state_slice(&handle, 0, size).await?;
                return Ok(data)
            }
        }
        fail!("Zero state {} is not inited", block_id);
    }

    async fn download_block_proof_internal(
        &self, 
        block_id: BlockIdExt, 
        is_link: bool, 
        _key_block: bool
    ) -> Result<Vec<u8>> {
        if let Some(handle) = self.engine.load_block_handle(&block_id)? {
            if (is_link && handle.has_proof_link()) || (!is_link && handle.has_proof()) {
                return Ok(self.engine.load_block_proof_raw(&handle, is_link).await?)
            }
        }
        if is_link {
            fail!("Block's proof link isn't initialized");
        } else {
            fail!("Block's proof isn't initialized");
        }
    }

    // tonNode.downloadBlockProof block:tonNode.blockIdExt = tonNode.Data;
    async fn download_block_proof(&self, query: DownloadBlockProof) -> Result<Vec<u8>> {
        self.download_block_proof_internal((&query.block).try_into()?, false, false).await
    }

    // tonNode.downloadKeyBlockProof block:tonNode.blockIdExt = tonNode.Data;
    async fn download_key_block_proof(&self, query: DownloadKeyBlockProof) -> Result<Vec<u8>> {
        self.download_block_proof_internal((&query.block).try_into()?, false, true).await
    }

    // tonNode.downloadBlockProofs blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    // Not supported in t-node
    
    // tonNode.downloadKeyBlockProofs blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    // Not supported in t-node

    // tonNode.downloadBlockProofLink block:tonNode.blockIdExt = tonNode.Data;
    async fn download_block_proof_link(&self, query: DownloadBlockProofLink) -> Result<Vec<u8>> {
        self.download_block_proof_internal((&query.block).try_into()?, true, false).await
    }

    // tonNode.downloadKeyBlockProofLink block:tonNode.blockIdExt = tonNode.Data;
    async fn download_key_block_proof_link(
        &self, 
        query: DownloadKeyBlockProofLink
    ) -> Result<Vec<u8>> {
        self.download_block_proof_internal((&query.block).try_into()?, true, true).await
    }

    // tonNode.downloadBlockProofLinks blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    // Not supported in t-node

    // tonNode.downloadKeyBlockProofLinks blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    // Not supported in t-node

    // tonNode.getArchiveInfo masterchain_seqno:int = tonNode.ArchiveInfo;
    async fn get_archive_info(&self, query: GetArchiveInfo) -> Result<ArchiveInfo> {
        if query.masterchain_seqno as u32 > self.engine.load_last_applied_mc_block_id().await?.seq_no()
            || query.masterchain_seqno as u32 > self.engine.load_shards_client_mc_block_id().await?.seq_no()
        {
            return Ok(ArchiveInfo::TonNode_ArchiveNotFound);
        }

        Ok(if let Some(id) = self.engine.get_archive_id(query.masterchain_seqno as u32).await {
            ArchiveInfo::TonNode_ArchiveInfo(
                Box::new(
                    ton_api::ton::ton_node::archiveinfo::ArchiveInfo {
                        id: id as ton_api::ton::long
                    }
                )
            )
        } else {
            ArchiveInfo::TonNode_ArchiveNotFound
        })
    }

    // tonNode.getArchiveSlice archive_id:long offset:long max_size:int = tonNode.Data;
    async fn get_archive_slice(&self, query: GetArchiveSlice) -> Result<Vec<u8>> {
        self.engine.get_archive_slice(
            query.archive_id as u64, query.offset as u64, query.max_size as u32).await
    }

    // tonNode.getCapabilities = tonNode.Capabilities;
    async fn get_capabilities(&self, _query: GetCapabilities) -> Result<Capabilities> {
        Ok(
            ton_node::capabilities::Capabilities {
                version: PROTOCOL_VERSION,
                capabilities: PROTOCOL_CAPABILITIES,
            }.into_boxed()
        )
    }

    async fn consume_query<'a, Q, A, F>(
        &'a self,
        query: TLObject,
        consumer: &'a (dyn Fn(&'a Self, Q) -> F + Send + Sync)
    ) -> Result<std::result::Result<QueryResult, TLObject>>
    where
        Q: AnyBoxedSerialize + Debug,
        A: BoxedSerialize + Send + Sync + serde::Serialize + Debug + 'static,
        F: futures::Future<Output = Result<A>>,
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
                            let answer = adnl::common::serialize(&answer)?;
                            log::trace!("consume_query: consumed {}", query_str);
                            #[cfg(feature = "telemetry")]
                            self.engine.full_node_service_telemetry()
                                .consumed_query(query_str, true, now.elapsed(), answer.len());
                            answer
                        }
                        Err(e) => {
                            log::trace!("consume_query: consumed {}, error {:?}", query_str, e);
                            #[cfg(feature = "telemetry")]
                            self.engine.full_node_service_telemetry()
                                .consumed_query(query_str, false, now.elapsed(), 0);
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
        F: futures::Future<Output = Result<Vec<u8>>>,
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
                            self.engine.full_node_service_telemetry()
                                .consumed_query(query_str, true, now.elapsed(), answer.len());
                            answer
                        }
                        Err(e) => {
                            #[cfg(feature = "telemetry")]
                            log::trace!("consume_query_raw: consumed {}, error {:?}", query_str, e);
                            #[cfg(feature = "telemetry")]
                            self.engine.full_node_service_telemetry()
                                .consumed_query(query_str, false, now.elapsed(), 0);
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
