use crate::{
    engine_traits::EngineOperations,
    block::BlockStuff,
    shard_state::ShardHashesStuff,
};
use ton_types::{Result, error};
use ton_block::{BlockIdExt, SHARD_FULL, ShardIdent, BASE_WORKCHAIN_ID, MASTERCHAIN_ID};
use std::{collections::HashSet, sync::Arc};
use storage::block_handle_db::BlockHandle;

pub struct ShardBlocksObserver {
    top_processed_blocks: HashSet<BlockIdExt>,
    engine: Arc<dyn EngineOperations>,
}

impl ShardBlocksObserver {
    pub async fn new(
        start_mc_block: &BlockHandle,
        engine: Arc<dyn EngineOperations>,
    ) -> Result<Self> {
        let mut top_processed_blocks = HashSet::new();
        let processed_wc = engine.processed_workchain().unwrap_or(BASE_WORKCHAIN_ID);
        if processed_wc != MASTERCHAIN_ID {
            if start_mc_block.id().seq_no() > 0 {
                let start_mc_block = engine.load_block(start_mc_block).await?;
                let shard_hashes: ShardHashesStuff = start_mc_block.shards()?.into();
                for id in shard_hashes.top_blocks(&vec!(processed_wc))? {
                    top_processed_blocks.insert(id);
                }
            } else {
                let zerostate = engine.load_state(start_mc_block.id()).await?;
                let workchains = zerostate.config_params()?.workchains()?;
                let wc = workchains.get(&processed_wc)?
                    .ok_or_else(|| error!("workchains doesn't have description for workchain {}", processed_wc))?;
                top_processed_blocks.insert(BlockIdExt {
                    shard_id: ShardIdent::with_tagged_prefix(processed_wc, SHARD_FULL)?,
                    seq_no: 0,
                    root_hash: wc.zerostate_root_hash,
                    file_hash: wc.zerostate_file_hash,
                });
            }
        }
        Ok(Self{top_processed_blocks, engine})
    }

    pub async fn process_next_mc_block(&mut self, mc_block: &BlockStuff) -> Result<Vec<(BlockStuff, BlockIdExt)>> {
        log::trace!("process_next_mc_block: {}", mc_block.id());
        let mut blocks_for_external_processing = Vec::new();
        loop {
            let mut new_top_processed_blocks = HashSet::new();
            let mut has_new_blocks = false;

            log::trace!("process_next_mc_block: new iteration");

            for id in self.top_processed_blocks.iter() {
                let handle = self.engine.load_block_handle(id)?
                    .ok_or_else(|| error!("Can't load top processed block's handle with id {}", id))?;
                if handle.has_next1() {
                    let next_id = self.engine.load_block_next1(handle.id()).await?;
                    let next_handle = self.engine.load_block_handle(&next_id)?
                        .ok_or_else(|| error!("Can't load next #1 block's handle with id {}", next_id))?;
                    let block = self.engine.load_block(&next_handle).await?;
                    
                    blocks_for_external_processing.push((block, mc_block.id().clone()));

                    log::trace!("process_next_mc_block: {} has next1 {}", handle.id(), next_id);
                    new_top_processed_blocks.insert(next_id);
                    has_new_blocks = true;
                } else {
                    log::trace!("process_next_mc_block: {} doesn't have next1", handle.id());
                    new_top_processed_blocks.insert(handle.id().clone());
                }
                if handle.has_next2() {
                    let next_id = self.engine.load_block_next2(handle.id()).await?
                        .ok_or_else(|| error!(
                            "INTERNAL ERROR! Block {}: 'handle.has_next2' is true but 'engine.load_block_next2' is None", 
                            handle.id()
                        ))?;
                    let next_handle = self.engine.load_block_handle(&next_id)?
                        .ok_or_else(|| error!("Can't load next #2 block's handle with id {}", next_id))?;
                    let block = self.engine.load_block(&next_handle).await?;

                    blocks_for_external_processing.push((block, mc_block.id().clone()));

                    log::trace!("process_next_mc_block: {} has next2 {}", handle.id(), next_id);
                    new_top_processed_blocks.insert(next_id);
                    has_new_blocks = true;
                }
            }

            self.top_processed_blocks = new_top_processed_blocks;
            if !has_new_blocks {
                log::trace!("process_next_mc_block: end");
                break Ok(blocks_for_external_processing);
            }
        }
    }
}