use crate::{
    block::{BlockStuff, convert_block_id_ext_api2blk},
    shard_state::ShardStateStuff,
    block_proof::BlockProofStuff,
    engine::{Engine, LastMcBlockId},
    engine_traits::EngineOperations,
    db::{BlockHandle, NodeState},
};

use std::{sync::Arc, ops::Deref};
use ton_types::{fail, Result};
use ton_block::{BlockIdExt, AccountIdPrefixFull};

#[async_trait::async_trait]
impl EngineOperations for Engine {

    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Arc<BlockHandle>> {
        self.db().load_block_handle(id)
    }

    async fn load_applied_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        // TODO make cache?
        if handle.applied() {
            self.db().load_block_data(handle.id())
        } else if handle.data_inited() {
            fail!("Block is not applied yet")
        } else {
            fail!("No block")
        }
    }

    async fn wait_applied_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        if handle.applied() {
            self.db().load_block_data(handle.id())
        } else {
            self.block_applying_awaiters().wait(handle.id()).await?;
            self.db().load_block_data(handle.id())
        }
    }

    async fn find_block_by_seq_no(&self, acc_pfx: &AccountIdPrefixFull, seqno: u32) -> Result<Arc<BlockHandle>> {
        self.db().find_block_by_seq_no(acc_pfx, seqno)
    }

    async fn find_block_by_unix_time(&self, acc_pfx: &AccountIdPrefixFull, utime: u32) -> Result<Arc<BlockHandle>> {
        self.db().find_block_by_unix_time(acc_pfx, utime)
    }

    async fn find_block_by_lt(&self, acc_pfx: &AccountIdPrefixFull, lt: u64) -> Result<Arc<BlockHandle>> {
        self.db().find_block_by_lt(acc_pfx, lt)
    }

    async fn load_last_applied_mc_block(&self) -> Result<BlockStuff> {
        let handle = self.load_block_handle(&self.load_last_applied_mc_block_id().await?)?;
        self.load_applied_block(&handle).await
    }

    async fn load_last_applied_mc_block_id(&self) -> Result<BlockIdExt> {
        LastMcBlockId::load_from_db(self.db())
            .and_then(|id| convert_block_id_ext_api2blk(&id.0))
    }

    async fn apply_block(self: Arc<Self>, handle: &BlockHandle, block: Option<&BlockStuff>) -> Result<()> {
        if handle.applied() {
            Ok(())
        } else if let Some(block) = block {
            self.apply_block_do_or_wait(handle, block).await
        } else {
            let block = if handle.data_inited() {
                self.db().load_block_data(handle.id())?
            } else {
                let (block, proof) = self.download_block(handle.id()).await?;
                if !handle.proof_inited() {
                    proof.check_proof(self.deref()).await?;
                    self.store_block_proof(handle, &proof).await?;
                }
                if !handle.data_inited() {
                    self.store_block(handle, &block).await?;
                }
                block
            };
            self.apply_block_do_or_wait(handle, &block).await
        }
    }

    async fn download_block(&self, id: &BlockIdExt) -> Result<(BlockStuff, BlockProofStuff)> {
        self.download_block_awaiters().do_or_wait(
            id,
            self.download_block_worker(id)
        ).await
    }

    async fn download_block_proof_link(&self, id: &BlockIdExt) -> Result<BlockProofStuff> {
        self.download_block_proof_link_awaiters().do_or_wait(
            id,
            self.download_block_proof_link_worker(id)
        ).await
    }

    async fn download_next_block(&self, prev_id: &BlockIdExt) -> Result<(BlockStuff, BlockProofStuff)> {
        self.download_block_awaiters().do_or_wait(
            prev_id,
            self.download_next_block_worker(prev_id)
        ).await
    }

    async fn download_state(&self, block_id: &BlockIdExt, master_id: &BlockIdExt) -> Result<ShardStateStuff> {
        let overlay = self.get_full_node_overlay(block_id.shard().workchain_id(), block_id.shard().shard_prefix_with_tag()).await?;
        crate::full_node::state_helper::download_persistent_state(block_id, master_id, overlay.deref()).await
    }

    async fn store_block(&self, handle: &BlockHandle, block: &BlockStuff) -> Result<()> {
        self.db().store_block_data(handle, block)
    }

    async fn store_block_proof(&self, handle: &BlockHandle, proof: &BlockProofStuff) -> Result<()> {
        self.db().store_block_proof(handle, proof)
    }

    async fn load_block_proof(&self, handle: &BlockHandle, is_link: bool) -> Result<BlockProofStuff> {
        // TODO make cache?
        self.db().load_block_proof(handle.id(), is_link)
    }

    async fn load_mc_zero_state(&self) -> Result<ShardStateStuff> {
        self.db().load_shard_state_dynamic(&self.zero_state_id())
    }

    async fn load_state(&self, handle: &BlockHandle) -> Result<ShardStateStuff> {
        self.db().load_shard_state_dynamic(handle.id())
    }

    async fn wait_state(&self, handle: &BlockHandle) -> Result<ShardStateStuff> {
        if handle.state_inited() {
            self.load_state(handle).await
        } else {
            self.shard_states_awaiters().wait(handle.id()).await
        }
    }

    async fn store_state(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<()> {
        self.shard_states_awaiters().do_or_wait(
            state.block_id(),
            async { Ok(state.clone()) }
        ).await?;
        self.db().store_shard_state_dynamic(handle, state)
    }

    async fn store_block_prev(&self, handle: &BlockHandle, prev: &BlockIdExt) -> Result<()> {
        self.db().store_block_prev(handle, prev)
    }

    async fn load_block_prev(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db().load_block_prev(id)
    }

    async fn store_block_prev2(&self, handle: &BlockHandle, prev2: &BlockIdExt) -> Result<()> {
        self.db().store_block_prev2(handle, prev2)
    }

    async fn load_block_prev2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db().load_block_prev2(id)
    }

    async fn store_block_next1(&self, handle: &BlockHandle, next: &BlockIdExt) -> Result<()> {
        self.db().store_block_next1(handle, next)
    }

    async fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db().load_block_next1(id)
    }

    async fn store_block_next2(&self, handle: &BlockHandle, next2: &BlockIdExt) -> Result<()> {
        self.db().store_block_next2(handle, next2)
    }

    async fn load_block_next2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db().load_block_next2(id)
    }

    async fn process_block_in_ext_db(
        &self,
        handle: &BlockHandle,
        block: &BlockStuff,
        proof: Option<&BlockProofStuff>,
        state: &ShardStateStuff)
    -> Result<()> {
        if self.ext_db().len() > 0 {
            if proof.is_some() && !handle.id().shard().is_masterchain() {
                fail!("Non master blocks should be processed without proof")
            }
            if proof.is_none() && handle.id().shard().is_masterchain() {
                let proof = self.load_block_proof(handle, false).await?;
                for db in self.ext_db() {
                    db.process_block(block, Some(&proof), state).await?;
                }
            } else {
                for db in self.ext_db() {
                    db.process_block(block, proof, state).await?;
                }
            }
        }
        self.db().store_block_processed_in_ext_db(handle)?;
        Ok(())
    }

    async fn process_full_state_in_ext_db(&self, state: &ShardStateStuff)-> Result<()> {
        for db in self.ext_db() {
            db.process_full_state(state).await?;
        }
        Ok(())
    }

    async fn get_next_key_blocks_ids(&self, block_id: &BlockIdExt, _priority: u32) -> Result<Vec<BlockIdExt>> {
        let mc_overlay = self.get_masterchain_overlay().await?;
        mc_overlay.get_next_key_blocks_ids(block_id, 5, 10).await
    }

}
