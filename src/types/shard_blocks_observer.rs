use crate::{
    engine_traits::EngineOperations,
    block::BlockStuff,
    shard_state::ShardHashesStuff,
};
use ton_types::{Result, error};
use ton_block::{BlockIdExt, SHARD_FULL, ShardIdent};
use std::{collections::HashSet, sync::Arc};
use storage::block_handle_db::BlockHandle;

pub struct ShardBlocksObserver<F: Fn(&BlockStuff, &BlockIdExt) -> Result<()>> {
    top_processed_blocks: HashSet<BlockIdExt>,
    engine: Arc<dyn EngineOperations>,
    on_new_block: F,
}

impl<F: Fn(&BlockStuff, &BlockIdExt) -> Result<()>> ShardBlocksObserver<F> {
    pub async fn new(
        start_mc_block: &BlockHandle,
        engine: Arc<dyn EngineOperations>,
        on_new_block: F
    ) -> Result<Self> {
        let mut top_processed_blocks = HashSet::new();
        let (_, processed_wc) = engine.processed_workchain().await?;
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
        Ok(Self{top_processed_blocks, engine, on_new_block})
    }

    pub async fn process_next_mc_block(&mut self, mc_block: &BlockStuff) -> Result<()> {
        loop {
            let mut new_top_processed_blocks = HashSet::new();
            let mut has_new_blocks = false;

            for id in self.top_processed_blocks.iter() {
                let handle = self.engine.load_block_handle(id)?
                    .ok_or_else(|| error!("Can't load top processed block's handle with id {}", id))?;
                if handle.has_next1() {
                    let next_id = self.engine.load_block_next1(handle.id()).await?;
                    let next_handle = self.engine.load_block_handle(&next_id)?
                        .ok_or_else(|| error!("Can't load next #1 block's handle with id {}", next_id))?;
                    let block = self.engine.load_block(&next_handle).await?;
                    
                    (self.on_new_block)(&block, mc_block.id())?;
                    
                    new_top_processed_blocks.insert(next_id);
                    has_new_blocks = true;
                } else {
                    new_top_processed_blocks.insert(handle.id().clone());
                }
                if handle.has_next2() {
                    let next_id = self.engine.load_block_next2(handle.id()).await?;
                    let next_handle = self.engine.load_block_handle(&next_id)?
                        .ok_or_else(|| error!("Can't load next #2 block's handle with id {}", next_id))?;
                    let block = self.engine.load_block(&next_handle).await?;
                    
                    (self.on_new_block)(&block, mc_block.id())?;
                    
                    new_top_processed_blocks.insert(next_id);
                    has_new_blocks = true;
                }
            }

            self.top_processed_blocks = new_top_processed_blocks;
            if !has_new_blocks {
                break Ok(());
            }
        }
    }
}
