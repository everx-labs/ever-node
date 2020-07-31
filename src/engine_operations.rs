use crate::{
    block::BlockStuff,
    shard_state::ShardStateStuff,
    block_proof::BlockProofStuff,
    engine::{Engine, LastMcBlockId, ShardsClientMcBlockId, STATSD, SavingState1, SavingState2},
    engine_traits::EngineOperations,
    db::{BlockHandle, NodeState},
    error::NodeError,
    network::full_node_client::Attempts
};

use std::{sync::Arc, ops::Deref, time::Duration, convert::TryInto};
use ton_types::{fail, error, Result};
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

    async fn load_block_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        self.db().load_block_data_raw(handle.id())
    }

    async fn wait_applied_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        if handle.applied() {
            self.db().load_block_data(handle.id())
        } else {
            self.block_applying_awaiters().wait(handle.id()).await?;
            self.db().load_block_data(handle.id())
        }
    }

    async fn wait_next_applied_mc_block(&self, prev_handle: &BlockHandle) -> Result<(Arc<BlockHandle>, BlockStuff)> {
        if !prev_handle.id().shard().is_masterchain() {
            fail!(NodeError::InvalidArg("`prev_handle` doesn't belong masterchain".to_string()))
        }
        let next_id = loop {
            if prev_handle.next1_inited() {
                break self.load_block_next1(prev_handle.id()).await?;
            } else {
                if let Some(id) = self.next_block_applying_awaiters().wait(prev_handle.id()).await? {
                    break id;
                }
            }
        };
        Ok((
            self.load_block_handle(&next_id)?,
            self.db().load_block_data(&next_id)?
        ))
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

    async fn load_last_applied_mc_state(&self) -> Result<ShardStateStuff> {
        self.load_state(&self.load_last_applied_mc_block_id().await?).await
    }

    async fn load_last_applied_mc_block_id(&self) -> Result<BlockIdExt> {
        (&LastMcBlockId::load_from_db(self.db())?.0).try_into()
    }

    async fn load_shards_client_mc_block_id(&self) -> Result<BlockIdExt> {
        (&ShardsClientMcBlockId::load_from_db(self.db())?.0).try_into()
    }

    async fn store_shards_client_mc_block_id(&self, id: &BlockIdExt) -> Result<()> {
        STATSD.gauge("shards_client_mc_block", id.seq_no() as f64);
        ShardsClientMcBlockId(id.into()).store_to_db(self.db())
    }

    async fn apply_block(self: Arc<Self>, handle: &BlockHandle, block: Option<&BlockStuff>) -> Result<()> {
        while !handle.applied() {
            if let Some(block) = block {
                if self.block_applying_awaiters().do_or_wait(
                    handle.id(),
                    self.clone().apply_block_worker(handle, block)
                ).await?.is_some() {
                    break;
                }
            } else {
                if self.block_applying_awaiters().do_or_wait(
                    handle.id(),
                    self.clone().download_and_apply_block_worker(handle)
                ).await?.is_some() {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn download_block(&self, id: &BlockIdExt) -> Result<(BlockStuff, BlockProofStuff)> {
        self.download_block_worker(id).await
    }

    async fn download_block_proof(&self, id: &BlockIdExt, is_link: bool, key_block: bool) -> Result<BlockProofStuff> {
        self.download_block_proof_worker(id, is_link, key_block).await
    }

    async fn download_next_block(&self, prev_id: &BlockIdExt) -> Result<(BlockStuff, BlockProofStuff)> {
        self.download_next_block_worker(prev_id).await
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

    async fn load_block_proof_raw(&self, handle: &BlockHandle, is_link: bool) -> Result<Vec<u8>> {
        self.db().load_block_proof_raw(handle.id(), is_link)
    }

    async fn load_mc_zero_state(&self) -> Result<ShardStateStuff> {
        let id = self.zero_state_id();
        let len = self.db().load_shard_state_persistent_size(id).await?;
        let data = self.db().load_shard_state_persistent_slice(id, 0, len).await?;

        ShardStateStuff::deserialize(self.zero_state_id().clone(), &data)
    }

    async fn load_state(&self, block_id: &BlockIdExt) -> Result<ShardStateStuff> {
        self.db().load_shard_state_dynamic(block_id)
    }

    async fn load_persistent_state_size(&self, block_id: &BlockIdExt) -> Result<u64> {
        self.db().load_shard_state_persistent_size(block_id).await
    }

    async fn load_persistent_state_slice(
        &self,
        handle: &BlockHandle,
        offset: u64,
        length: u64
    ) -> Result<Vec<u8>> {
        self.db().load_shard_state_persistent_slice(handle.id(), offset, length).await
    }

    async fn wait_state(&self, handle: &BlockHandle) -> Result<ShardStateStuff> {
        loop {
            if handle.state_inited() {
                break self.load_state(handle.id()).await
            } else {
                if let Some(ss) = self.shard_states_awaiters().wait(handle.id()).await? {
                    break Ok(ss)
                }
            }
        }
    }

    async fn store_state(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<()> {
        self.shard_states_awaiters().do_or_wait(
            state.block_id(),
            async { Ok(state.clone()) }
        ).await?;
        self.db().store_shard_state_dynamic(handle, state)
    }

    async fn store_persistent_state(self: Arc<Self>, state: ShardStateStuff) -> Result<()> {
        // this method is normally called only for master key blocks (from apply procedure)
        // so it is impossible this method would be called in parallel

        let handle = self.load_block_handle(state.block_id())?;
        let empty_id = ton_api::ton::ton_node::blockidext::BlockIdExt::default();
        let ss1 = SavingState1::load_from_db(self.db()).unwrap_or_default();
        if ss1.0 != empty_id {
            let ss2 = SavingState2::load_from_db(self.db()).unwrap_or_default();
            if ss2.0 != empty_id {
                fail!("Can't save persistent state: both `SavingState` properties are set!")
            } else {
                SavingState2(state.block_id().into()).store_to_db(self.db().deref())?;
                tokio::spawn(async move {
                    self.store_persistent_state_attempts(&handle, &state).await;
                    while let Err(e) = SavingState2(empty_id.clone()).store_to_db(self.db().deref()) {
                        log::error!("CRITICAL Error setting `SavingState2` param: {:?}", e);
                        futures_timer::Delay::new(Duration::from_millis(1000)).await;
                    }
                    log::info!("Persistent shard state is saved {}", state.block_id());
                });
            }
        } else {
            SavingState1(state.block_id().into()).store_to_db(self.db().deref())?;
            tokio::spawn(async move {
                self.store_persistent_state_attempts(&handle, &state).await;
                while let Err(e) = SavingState1(empty_id.clone()).store_to_db(self.db().deref()) {
                    log::error!("CRITICAL Error setting `SavingState1` param: {:?}", e);
                    futures_timer::Delay::new(Duration::from_millis(1000)).await;
                }
                log::info!("Persistent shard state is saved {}", state.block_id());
            });
        }
        Ok(())
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

    async fn download_next_key_blocks_ids(&self, block_id: &BlockIdExt, _priority: u32) -> Result<Vec<BlockIdExt>> {
        let mc_overlay = self.get_masterchain_overlay().await?;
        mc_overlay.download_next_key_blocks_ids(
            block_id, 
            5, 
            &Attempts {
                limit: 10,
                count: 0
            }
        ).await
    }

    fn set_applied(&self, block_id: &BlockIdExt) -> Result<()> {
        self.db().store_block_applied(block_id)
    }

    fn initial_sync_disabled(&self) -> bool { (self as &Engine).initial_sync_disabled() }

    fn init_mc_block_id(&self) -> &BlockIdExt { (self as &Engine).init_mc_block_id() }

    fn set_init_mc_block_id(&self, init_mc_block_id: &BlockIdExt) {
        (self as &Engine).set_init_mc_block_id(init_mc_block_id)
    }

    async fn broadcast_to_public_overlay(&self, to: &AccountIdPrefixFull, data: &[u8]) -> Result<u32> {
        let overlay = self.get_full_node_overlay(to.workchain_id, to.prefix).await?;
        let res = overlay.broadcast_external_message(data).await?;

        Ok(res)
    }
}
