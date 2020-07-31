use crate::{
    engine_traits::EngineOperations, network::neighbours::{PROTO_CAPABILITIES, PROTO_VERSION}
};

use adnl::common::{QueryResult};
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
            ArchiveInfo, BlockDescription, Data, DataFull, KeyBlocks,
            Prepared, PreparedProof, PreparedState, Capabilities,

        }
    }
};
use ton_block::BlockIdExt;
use ton_types::{fail, error, Result};

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

    // tonNode.prepareBlockProof block:tonNode.blockIdExt allow_partial:Bool = tonNode.PreparedProof;
    async fn prepare_block_proof(&self, query: PrepareBlockProof) -> Result<PreparedProof> {
        let block_id = (&query.block).try_into()?;
        let handle = self.engine.load_block_handle(&block_id)?;
        let allow_partial: bool = query.allow_partial.into();
        if !handle.proof_inited() && (!allow_partial || !handle.proof_link_inited()) {
            Ok(PreparedProof::TonNode_PreparedProofEmpty)
        }
        else if handle.proof_inited() && handle.id().shard().is_masterchain() {
            Ok(PreparedProof::TonNode_PreparedProof)
        } else {
            Ok(PreparedProof::TonNode_PreparedProofLink)
        } 
    }

    // tonNode.prepareKeyBlockProof block:tonNode.blockIdExt allow_partial:Bool = tonNode.PreparedProof;
    async fn prepare_key_block_proof(&self, query: PrepareKeyBlockProof) -> Result<PreparedProof> {
        let block_id = (&query.block).try_into()?;
        let handle = self.engine.load_block_handle(&block_id)?;
        if !handle.is_key_block()? {
            fail!("prepare_key_block_proof: given block is not key");
        }
        let allow_partial: bool = query.allow_partial.into();
        if !handle.proof_inited() && (!allow_partial || !handle.proof_link_inited()) {
            Ok(PreparedProof::TonNode_PreparedProofEmpty)
        }
        else if handle.proof_inited() && handle.id().shard().is_masterchain() {
            Ok(PreparedProof::TonNode_PreparedProof)
        } else {
            Ok(PreparedProof::TonNode_PreparedProofLink)
        }
    }

    // tonNode.prepareBlockProofs blocks:(vector tonNode.blockIdExt) allow_partial:Bool = tonNode.PreparedProof;
    // Not supported in t-node

    // tonNode.prepareKeyBlockProofs blocks:(vector tonNode.blockIdExt) allow_partial:Bool = tonNode.PreparedProof;
    // Not supported in t-node

    // tonNode.prepareBlock block:tonNode.blockIdExt = tonNode.Prepared;
    async fn prepare_block(&self, query: PrepareBlock) -> Result<Prepared> {
        let block_id = (&query.block).try_into()?;
        let handle = self.engine.load_block_handle(&block_id)?;
        if handle.data_inited() {
            Ok(Prepared::TonNode_Prepared)
        } else {
            Ok(Prepared::TonNode_NotFound)
        }
    }

    // tonNode.prepareBlocks blocks:(vector tonNode.blockIdExt) = tonNode.Prepared;
    // Not supported in t-node

    // tonNode.preparePersistentState block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt = tonNode.PreparedState;
    async fn prepare_persistent_state(&self, query: PreparePersistentState) -> Result<PreparedState> {
        let block_id = (&query.block).try_into()?;
        let handle = self.engine.load_block_handle(&block_id)?;
        Ok(
            if handle.persistent_state_inited() {
                PreparedState::TonNode_PreparedState
            } else {
                PreparedState::TonNode_NotFoundState
            }
        )
    }

    // tonNode.prepareZeroState block:tonNode.blockIdExt = tonNode.PreparedState;
    async fn prepare_zero_state(&self, query: PrepareZeroState) -> Result<PreparedState> {
        let block_id = (&query.block).try_into()?;
        let handle = self.engine.load_block_handle(&block_id)?;
        Ok(
            if handle.persistent_state_inited() {
                PreparedState::TonNode_PreparedState
            } else {
                PreparedState::TonNode_NotFoundState
            }
        )
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
        let prev_blocks = &last_mc_state
            .shard_state()
            .read_custom()?
            .ok_or_else(|| error!("Given master state doesn't contain `custom` field"))?
            .prev_blocks;

        if start_block_id.seq_no != 0 {
            // check if start block is key-block
            let id = prev_blocks.get_prev_key_block(start_block_id.seq_no())?
                .ok_or_else(|| error!("Given master state doesn't contain previous key block for {}", start_block_id))?;
            if id.seq_no != start_block_id.seq_no ||
                id.root_hash != *start_block_id.root_hash() ||
                id.file_hash != *start_block_id.file_hash() {
                fail!("Given block {} is not a key-block", start_block_id);
            }
        }

        let mut ids = vec!();
        let mut seq_no = start_block_id.seq_no();
        while let Some(id) = prev_blocks.get_next_key_block(seq_no + 1)? {
            seq_no = id.seq_no;
            let ext_id = BlockIdExt {
                shard_id: start_block_id.shard().clone(),
                seq_no: seq_no,
                root_hash: id.root_hash,
                file_hash: id.file_hash,
            };
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
        let prev_handle = self.engine.load_block_handle(&block_id)?;
        if prev_handle.next1_inited() {
            let next_id = self.engine.load_block_next1(&block_id).await?;
            let next_handle = self.engine.load_block_handle(&next_id)?;
            let mut is_link = false;
            if next_handle.data_inited() && next_handle.proof_or_link_inited(&mut is_link) {
                let block = self.engine.load_block_raw(&next_handle).await?;
                let proof = self.engine.load_block_proof_raw(&prev_handle, is_link).await?;
                return Ok(DataFull::TonNode_DataFull(Box::new(
                    ton_node::datafull::DataFull{
                        id: next_id.into(),
                        proof: ton_api::ton::bytes(proof),
                        block: ton_api::ton::bytes(block),
                        is_link: if is_link { ton::Bool::BoolTrue } else { ton::Bool::BoolFalse }, 
                    }
                )));
            }
        }
        Ok(DataFull::TonNode_DataFullEmpty)
    }

    // tonNode.downloadBlockFull block:tonNode.blockIdExt = tonNode.DataFull;
    async fn download_block_full(&self, query: DownloadBlockFull) -> Result<DataFull> {
        let block_id = (&query.block).try_into()?;
        let handle = self.engine.load_block_handle(&block_id)?;
        let mut is_link = false;
        if handle.data_inited() && handle.proof_or_link_inited(&mut is_link) {
            let block = self.engine.load_block_raw(&handle).await?;
            let proof = self.engine.load_block_proof_raw(&handle, is_link).await?;
            return Ok(DataFull::TonNode_DataFull(Box::new(
                ton_node::datafull::DataFull{
                    id: query.block,
                    proof: ton_api::ton::bytes(proof),
                    block: ton_api::ton::bytes(block),
                    is_link: if is_link { ton::Bool::BoolTrue } else { ton::Bool::BoolFalse }, 
                }
            )));
        }
        Ok(DataFull::TonNode_DataFullEmpty)
    }

    // tonNode.downloadBlock block:tonNode.blockIdExt = tonNode.Data;
    async fn download_block(&self, query: DownloadBlock) -> Result<Data> {
        let block_id = (&query.block).try_into()?;
        let handle = self.engine.load_block_handle(&block_id)?;
        if !handle.data_inited() {
            fail!("Block's data isn't initialized");
        }
        let data = self.engine.load_block_raw(&handle).await?;
        Ok(
            Data::TonNode_Data(
                Box::new(
                    ton_node::data::Data{
                        data: ton_api::ton::bytes(data)
                    }
                )
            )
        )
    }

    // tonNode.downloadBlocks blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    // Not supported in t-node

    // tonNode.downloadPersistentState block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt = tonNode.Data;
    async fn download_persistent_state(&self, query: DownloadPersistentState) -> Result<Data> {
        // This request is never called in t-node, because new downloadPersistentStateSlice exists.
        // Because of state is pretty big it is bad idea to send it by one request.
        fail!(
            "`tonNode.downloadPersistentState` request is not supported (block: {}, mc block: {})",
            query.block,
            query.masterchain_block
        );
    }

    // tonNode.downloadPersistentStateSlice block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt offset:long max_size:long = tonNode.Data;
    async fn download_persistent_state_slice(&self, query: DownloadPersistentStateSlice) -> Result<Data> {
        let block_id = (&query.block).try_into()?;
        let handle = self.engine.load_block_handle(&block_id)?;
        if !handle.persistent_state_inited() {
            fail!("Shard state {} doesn't have a persistent state", block_id);
        }
        let data = self.engine.load_persistent_state_slice(
            &handle,
            query.offset as u64,
            query.max_size as u64
        ).await?;
        Ok(
            ton_node::data::Data{
                data: ton_api::ton::bytes(data)
            }.into_boxed()
        )
    }

    // tonNode.downloadZeroState block:tonNode.blockIdExt = tonNode.Data;
    async fn download_zero_state(&self, query: DownloadZeroState) -> Result<Data> {
        let block_id = (&query.block).try_into()?;
        let handle = self.engine.load_block_handle(&block_id)?;
        if !handle.persistent_state_inited() {
            fail!("Zero state {} is not inited", block_id);
        }

        let size = self.engine.load_persistent_state_size(&block_id).await?;
        let data = self.engine.load_persistent_state_slice(&handle, 0, size).await?;
        Ok(
            ton_node::data::Data{
                data: ton_api::ton::bytes(data)
            }.into_boxed()
        )
    }

    // tonNode.downloadBlockProof block:tonNode.blockIdExt = tonNode.Data;
    async fn download_block_proof(&self, query: DownloadBlockProof) -> Result<Data> {
        self.download_block_proof_((&query.block).try_into()?, false, false).await
    }

    // tonNode.downloadKeyBlockProof block:tonNode.blockIdExt = tonNode.Data;
    async fn download_key_block_proof(&self, query: DownloadKeyBlockProof) -> Result<Data> {
        self.download_block_proof_((&query.block).try_into()?, false, true).await
    }

    // tonNode.downloadBlockProofs blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    // Not supported in t-node
    
    // tonNode.downloadKeyBlockProofs blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    // Not supported in t-node

    // tonNode.downloadBlockProofLink block:tonNode.blockIdExt = tonNode.Data;
    async fn download_block_proof_link(&self, query: DownloadBlockProofLink) -> Result<Data> {
        self.download_block_proof_((&query.block).try_into()?, true, false).await
    }

    // tonNode.downloadKeyBlockProofLink block:tonNode.blockIdExt = tonNode.Data;
    async fn download_key_block_proof_link(&self, query: DownloadKeyBlockProofLink) -> Result<Data> {
        self.download_block_proof_((&query.block).try_into()?, true, true).await
    }

    async fn download_block_proof_(&self, block_id: BlockIdExt, is_link: bool, _key_block: bool) -> Result<Data> {
        let handle = self.engine.load_block_handle(&block_id)?;
        if is_link && !handle.proof_link_inited() {
            fail!("Block's proof link isn't initialized");
        } else if !is_link && !handle.proof_inited() {
            fail!("Block's proof isn't initialized");
        }
        let proof = self.engine.load_block_proof_raw(&handle, is_link).await?;
        Ok(
            Data::TonNode_Data(
                Box::new(
                    ton_node::data::Data{
                        data: ton_api::ton::bytes(proof)
                    }
                )
            )
        )
    }

    // tonNode.downloadBlockProofLinks blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    // Not supported in t-node

    // tonNode.downloadKeyBlockProofLinks blocks:(vector tonNode.blockIdExt) = tonNode.DataList;
    // Not supported in t-node

    // tonNode.getArchiveInfo masterchain_seqno:int = tonNode.ArchiveInfo;
    async fn get_archive_info(&self, _query: GetArchiveInfo) -> Result<ArchiveInfo> {
        fail!("`tonNode.getArchiveInfo` request is not supported");
    }

    // tonNode.getArchiveSlice archive_id:long offset:long max_size:int = tonNode.Data;
    async fn get_archive_slice(&self, _query: GetArchiveSlice) -> Result<Data> {
        fail!("`tonNode.getArchiveSlice` request is not supported");
    }

    // tonNode.getCapabilities = tonNode.Capabilities;
    async fn get_capabilities(&self, _query: GetCapabilities) -> Result<Capabilities> {
        Ok(
            ton_node::capabilities::Capabilities {
                version: PROTO_VERSION,
                capabilities: PROTO_CAPABILITIES,
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

                    log::trace!("consume_query: before consume query {:?}", query);

                    let answer = match consumer(self, query).await {
                        Ok(answer) => {
                            log::trace!("consume_query: consumed, answer {:?}", answer);
                            answer
                        }
                        Err(e) => {
                            log::trace!("consume_query: consumed, error {:?}", e);
                            return Err(e)
                        }
                    };

                    Ok(
                        QueryResult::Consumed(Some(TLObject::new(answer)))
                    )
                },
                Err(query) => Err(query)
            }
        )
    }
}

#[async_trait::async_trait]
impl QueriesConsumer for FullNodeOverlayService {
    #[allow(dead_code)]
    async fn try_consume_query(&self, query: TLObject) -> Result<QueryResult> {

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

        let query = match self.consume_query::<DownloadBlock, _, _>(
            query,
            &Self::download_block
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<DownloadPersistentState, _, _>(
            query,
            &Self::download_persistent_state
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<DownloadPersistentStateSlice, _, _>(
            query,
            &Self::download_persistent_state_slice
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<DownloadZeroState, _, _>(
            query,
            &Self::download_zero_state
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<DownloadBlockProof, _, _>(
            query,
            &Self::download_block_proof
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<DownloadKeyBlockProof, _, _>(
            query,
            &Self::download_key_block_proof
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<DownloadBlockProofLink, _, _>(
            query,
            &Self::download_block_proof_link
        ).await? {
            Ok(answer) => return Ok(answer),
            Err(query) => query
        };

        let query = match self.consume_query::<DownloadKeyBlockProofLink, _, _>(
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

        let query = match self.consume_query::<GetArchiveSlice, _, _>(
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