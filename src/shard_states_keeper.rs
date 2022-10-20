use crate::{
    internal_db::{
        InternalDb, state_gc_resolver::AllowStateGcSmartResolver, LAST_APPLIED_MC_BLOCK,
    },
    shard_state::ShardStateStuff,
    engine_traits::{EngineOperations, EngineAlloc},
    engine::{Engine, Stopper},
    boot::self,
};
#[cfg(feature = "async_ss_storage")]
use crate::internal_db::SsNotificationCallback;
#[cfg(feature = "telemetry")]
use crate::engine_traits::EngineTelemetry;
use storage::{
    block_handle_db::BlockHandle,
};
#[cfg(not(feature = "async_ss_storage"))]
use storage::shardstate_db::AllowStateGcResolver;
#[cfg(feature = "async_ss_storage")]
use storage::shardstate_db_async::AllowStateGcResolver;
use ton_block::{BlockIdExt, ShardIdent};
use ton_types::{fail, error, Result, UInt256};
use adnl::common::add_unbound_object_to_map;
use std::{
    time::Duration, ops::Deref,
};
use ton_types::BocDeserializer;

/// This structs works beetween engine and db.
/// ValidatorManager  Collator  ValidatorQuery  etc.   <- high level node commponents
///       ↓              ↓             ↓
///                 Engine
///                  ↓ ↑
///           **ShardStatesKeeper**
///                    ↓
///          InternalDb  NodeNetwork                   <- low level node components
///              ↓            ↓
///       databases  network protocols
pub struct ShardStatesKeeper {
    db: Arc<InternalDb>,
    gc_resolver: Arc<AllowStateGcSmartResolver>,
    states: lockfree::map::Map<BlockIdExt, Arc<ShardStateStuff>>,
    enable_persistent_gc: bool,
    stopper: Arc<Stopper>,
    #[cfg(feature = "telemetry")]
    telemetry: Arc<EngineTelemetry>,
    allocated: Arc<EngineAlloc>,
}

use std::sync::Arc;

impl ShardStatesKeeper {

    pub fn new(
        db: Arc<InternalDb>,
        enable_shard_state_persistent_gc: bool,
        cells_lifetime_sec: u64,
        stopper: Arc<Stopper>,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<EngineTelemetry>,
        allocated: Arc<EngineAlloc>
    ) -> Result<Arc<Self>> {

        log::trace!("start_states_gc");
        let gc_resolver = Arc::new(AllowStateGcSmartResolver::new(cells_lifetime_sec));
        db.start_states_gc(gc_resolver.clone());

        Ok(Arc::new(ShardStatesKeeper {
            db,
            gc_resolver,
            enable_persistent_gc: enable_shard_state_persistent_gc,
            states: lockfree::map::Map::new(),
            stopper,
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated,
        }))
    }

    pub async fn start(
        self: Arc<Self>,
        engine: Arc<Engine>,
        last_applied_mc_block: BlockIdExt,
        shard_client_mc_block: BlockIdExt,
        ss_keeper_block: BlockIdExt,
    ) -> Result<tokio::task::JoinHandle<()>> {

        log::trace!("start");

        self.restore_states(&engine, last_applied_mc_block, shard_client_mc_block).await?;

        let join_handle = tokio::spawn(async move {
            engine.acquire_stop(Engine::MASK_SERVICE_PSS_KEEPER);
            while let Err(e) = self.worker(&engine, &ss_keeper_block).await {
                log::error!("FATAL!!! Unexpected error in persistent states keeper: {:?}", e);
            }
            engine.release_stop(Engine::MASK_SERVICE_PSS_KEEPER);
        });
        Ok(join_handle)
    }

    #[async_recursion::async_recursion]
    pub async fn load_state(&self, block_id: &BlockIdExt) -> Result<Arc<ShardStateStuff>> {
        log::trace!("load_state {}", block_id);
        if let Some(state) = self.states.get(block_id) {
            log::trace!("load_state {} FROM CACHE", block_id);
            return Ok(state.val().clone())
        } else {
            let state = match self.db.load_shard_state_dynamic(block_id) {
                Ok(s) => {
                    log::trace!("load_state {} FROM DB", block_id);
                    s
                }
                Err(_e) => {
                    let s = self.catch_up_state(block_id).await?;
                    log::trace!("load_state {} RESTORED", block_id);
                    s
                }
            };
            self.states.insert(block_id.clone(), state.clone());
            Ok(state)
        }
    }

    pub async fn store_state(
        &self,
        handle: &Arc<BlockHandle>, 
        mut state: Arc<ShardStateStuff>,
        persistent_state: Option<&[u8]>,
        force: bool,
    ) -> Result<(Arc<ShardStateStuff>, bool)> {

        self.check_stop()?;

        #[cfg(feature = "async_ss_storage")] {
            let (cb1, cb2) = if let Some(state_data) = persistent_state {
                // while boot - zerostate and init persistent state are saved using this parameter 
                self.db.store_shard_state_persistent_raw(&handle, state_data, None).await?;
                let cb = SsNotificationCallback::new();
                (
                    Some(cb.clone() as Arc<dyn storage::shardstate_db_async::Callback>), 
                    Some(cb)
                )
            } else {
                (None, None)
            };
            self.db.store_shard_state_dynamic(
                handle,
                &state,
                None,
                #[cfg(feature = "async_ss_storage")]
                cb1,
                force
            ).await?;

            if let Some(cb) = cb2 {
                cb.wait().await;
                // reload state after saving just to free fully loaded tree 
                // and use lazy loaded cells futher
                state = self.db.load_shard_state_dynamic(handle.id())?;
            }
        }
        #[cfg(not(feature = "async_ss_storage"))] {
            if let Some(state_data) = persistent_state {
                // while boot zerostate and init persistent state are saved using this parameter 
                self.db.store_shard_state_persistent_raw(&handle, state_data, None).await?;
            }
            (state, _) = self.db.store_shard_state_dynamic(
                handle,
                &state,
                None,
                force
            ).await?;
        }

        let saved = add_unbound_object_to_map(
            &self.states, 
            handle.id().clone(), 
            || Ok(state.clone())
        )?;

        Ok((state, saved))
    }

    pub async fn check_and_store_state(
        &self, 
        handle: &Arc<BlockHandle>,
        root_hash: &UInt256,
        data: Arc<Vec<u8>>,
        low_memory_mode: bool,
    ) -> Result<Arc<ShardStateStuff>> {

        // deserialise cells (and save it by the way - in case of low memory mode)

        let now = std::time::Instant::now();
        log::info!(
            "check_and_store_state: deserialize (low_memory_mode: {}) {}...",
            low_memory_mode, handle.id()
        );

        #[cfg(feature = "async_ss_storage")]
        let state_root = {
            let mut deserialiser = BocDeserializer::new();
            if low_memory_mode {
                let done_cells_storage = self.db.create_done_cells_storage(root_hash)?;
                deserialiser = deserialiser.set_done_cells_storage(done_cells_storage);
            }
            deserialiser
                .set_abort(&|| self.stopper.check_stop())
                .deserialize_inmem(data.clone())?
                .withdraw_one_root()?
        };
        #[cfg(not(feature = "async_ss_storage"))]
        let state_root = BocDeserializer::new()
            .set_abort(&|| self.stopper.check_stop())
            .deserialize_inmem(data.clone())?
            .withdraw_one_root()?;

        log::info!(
            "check_and_store_state: deserialized (low_memory_mode: {}) {} TIME {}",
            handle.id(), low_memory_mode, now.elapsed().as_millis()
        );

        let state = ShardStateStuff::from_root_cell(
            handle.id().clone(), 
            state_root,
            #[cfg(feature = "telemetry")]
            &self.telemetry,
            &self.allocated
        )?;

        let (state, _) = self.store_state(handle, state, Some(&data), false).await?;

        Ok(state)
    }

    fn check_stop(&self) -> Result<()> {
        if self.stopper.check_stop() {
            fail!("Stopped")
        } else {
            Ok(())
        }
    }

    async fn restore_states(
        &self,
        engine: &Engine,
        last_applied_mc_block: BlockIdExt,
        shard_client_mc_block: BlockIdExt,
    ) -> Result<()> {

        log::trace!("restore_states  last_applied_mc_block: {}  shard_client_mc_block: {}...", 
            last_applied_mc_block, shard_client_mc_block);

        let _ = self.load_state(&last_applied_mc_block).await?;

        let mc_state = self.load_state(&shard_client_mc_block).await?;
        let (_, processed_wc) = engine.processed_workchain().await?;
        let shard_blocks = mc_state.shard_hashes()?.top_blocks(&[processed_wc])?; 
        for block_id in &shard_blocks {
            self.load_state(block_id).await?;
        }

        Ok(())
    }

    pub const MAX_CATCH_UP_DEPTH: u32 = 2000;
    const MAX_NEW_STATE_OFFSET: u32 = 50;
    async fn catch_up_state(
        &self, 
        id: &BlockIdExt,
    ) -> Result<Arc<ShardStateStuff>> {

        log::trace!("catch_up_state {}...", id);
        let now = std::time::Instant::now();

        // load latest block we know
        let latest_mc_id = self.db.load_full_node_state(LAST_APPLIED_MC_BLOCK)?
            .ok_or_else(|| error!("Can't load LAST_APPLIED_MC_BLOCK in catch_up_state"))?;
        let latest_id = if id.shard().is_masterchain() {
            (*latest_mc_id).clone()
        } else {
            // load latest known block in needed shard
            let latest_mc_state = self.load_state(&latest_mc_id).await?;
            let mut latest_id = None;
            latest_mc_state.shards()?.iterate_shards(|ident, descr| {
                if ident.intersect_with(id.shard()) {
                    latest_id = Some(BlockIdExt {
                        shard_id: ident,
                        seq_no: descr.seq_no,
                        root_hash: descr.root_hash,
                        file_hash: descr.file_hash
                    });
                }
                Ok(true)
            })?;
            match latest_id {
                None => fail!("Can't find latest known shard for {}", id),
                Some(id) => id
            }
        };

        // check the state is not too new
        if id.seq_no() > latest_id.seq_no() + Self::MAX_NEW_STATE_OFFSET {
            fail!("Attempt to load too new state {}, latest known block {}",
                id, latest_id);
        }

        let state = self.restore_state_recursive(id, 0).await?;

        log::trace!("catch_up_state {} CATCHED UP - TIME {}ms", id, now.elapsed().as_millis());
        Ok(state)
    }

    #[async_recursion::async_recursion]
    async fn restore_state_recursive(
        &self, 
        id: &BlockIdExt, 
        depth: u32,
    ) -> Result<Arc<ShardStateStuff>> {

        self.check_stop()?;

        if depth > Self::MAX_CATCH_UP_DEPTH {
            fail!("restore_state_recursive: max depth achived on id {}", id);
        }
        if let Some(state) = self.states.get(id) {
            log::trace!("load_state {} FROM CACHE", id);
            Ok(state.val().clone())
        } else if let Ok(state) = self.db.load_shard_state_dynamic(id) {
            self.states.insert(id.clone(), state.clone());
            Ok(state)
        } else {
            let handle = self.db.load_block_handle(id)?.ok_or_else(
                || error!("Cannot load handle for {}", id)
            )?;
            let block = self.db.load_block_data(&handle).await?;
            let prev_root = match block.construct_prev_id()? {
                (prev, None) => {
                    self.restore_state_recursive(&prev, depth + 1).await?
                        .root_cell().clone()
                },
                (prev1, Some(prev2)) => {
                    let root1 = self.restore_state_recursive(&prev1, depth + 1).await?
                        .root_cell().clone();
                    let root2 = self.restore_state_recursive(&prev2, depth + 1).await?
                        .root_cell().clone();
                    ShardStateStuff::construct_split_root(root1, root2)?
                }
            };

            let merkle_update = block.block().read_state_update()?;
            let id = id.clone();
            #[cfg(feature = "telemetry")]
            let telemetry = self.telemetry.clone();
            let allocated = self.allocated.clone();

            let state = tokio::task::spawn_blocking(
                move || -> Result<Arc<ShardStateStuff>> {
                    let now = std::time::Instant::now();
                    let root = merkle_update.apply_for(&prev_root)?;
                    log::trace!("TIME: restore_state_recursive: applied Merkle update {}ms   {}",
                        now.elapsed().as_millis(), id);
                    ShardStateStuff::from_root_cell(
                        id, 
                        root,
                        #[cfg(feature = "telemetry")]
                        &telemetry,
                        &allocated
                    )
                }
            ).await??;

            self.store_state(&handle, state.clone(), None, true).await?;

            Ok(state)
        }
    }

    async fn worker(
        &self,
        engine: &Arc<Engine>,
        ss_keeper_block: &BlockIdExt
    ) -> Result<()> {

        if !ss_keeper_block.shard().is_masterchain() {
            fail!("'ss_keeper_block' mast belong master chain");
        }
        let mut handle = engine.load_block_handle(ss_keeper_block)?.ok_or_else(
            || error!("Cannot load handle for ss keeper block {}", ss_keeper_block)
        )?;
        loop {
            let mc_state = engine.load_state(handle.id()).await?;
            let mut is_persistent_state = false;
            if handle.id().seq_no() != 0 && handle.is_key_block()? {
                if let Some(prev_key_block_id) = mc_state
                    .shard_state_extra()?
                    .prev_blocks
                    .get_prev_key_block(handle.id().seq_no() - 1)? 
                {
                    let block_id = BlockIdExt {
                        shard_id: ShardIdent::masterchain(),
                        seq_no: prev_key_block_id.seq_no,
                        root_hash: prev_key_block_id.root_hash,
                        file_hash: prev_key_block_id.file_hash
                    };
                    let prev_handle = engine.load_block_handle(&block_id)?.ok_or_else(
                        || error!("Cannot load handle for ss keeper prev key block {}", block_id)
                    )?;
                    is_persistent_state = engine.is_persistent_state(
                        handle.gen_utime()?, prev_handle.gen_utime()?, boot::PSS_PERIOD_BITS);
                }
            }

            if is_persistent_state {
                // store states

                let e = engine.clone();
                let abort = Arc::new(move || e.check_stop());

                log::trace!("states_keeper: saving {}", handle.id());
                let now = std::time::Instant::now();
                self.wait_and_store_persistent_state(
                    engine.deref(), &handle, abort.clone()).await;
                if engine.check_stop() {
                    return Ok(());
                }
                log::trace!("saved {} TIME {}ms", handle.id(), now.elapsed().as_millis());

                let (_, processed_wc) = engine.processed_workchain().await?;
                let shard_blocks = mc_state.shard_hashes()?.top_blocks(&[processed_wc])?; 
                for block_id in &shard_blocks {
                    log::trace!("saving {}", block_id);
                    let now = std::time::Instant::now();
                    let handle = engine.load_block_handle(block_id)?.ok_or_else(
                        || error!("Cannot load handle for SS keeper shard block {}", block_id)
                    )?;
                    self.wait_and_store_persistent_state(
                        engine.deref(), &handle, Arc::new(|| false)).await;
                    if engine.check_stop() {
                        return Ok(());
                    }
                    log::trace!("saved {} TIME {}ms",
                        handle.id(), now.elapsed().as_millis());
                };
                log::info!("saved mc state {} and all related shards", handle.id().seq_no());

                // gc iteration for persistent/stored states

                if self.enable_persistent_gc {
                    let calc_ttl = |t| {
                        let ttl = engine.persistent_state_ttl(t, boot::PSS_PERIOD_BITS);
                        let expired = ttl <= engine.now();
                        (ttl, expired)
                    };
                    let mut last_stored = shard_blocks;
                    last_stored.push(handle.id().clone());
                    let zerostate_id = engine.zero_state_id();
                    if let Err(e) = self.db.shard_state_persistent_gc(calc_ttl, zerostate_id).await {
                        log::warn!("persistent states gc: {}", e);
                    }
                }

                if engine.check_stop() {
                    return Ok(());
                }
            }

            // update gc resolver

            let shard_client = engine.load_shard_client_mc_block_id()?.ok_or_else(
                || error!("INTERNAL ERROR: No shard client MC block id in ss keeper worker")
            )?;
            let mut min_id: &BlockIdExt = if shard_client.seq_no() < handle.id().seq_no() { 
                &shard_client
            } else { 
                handle.id()
            };
            let mut last_rotation_block_id_str = "none".to_string();
            let last_rotation_block_id = engine.load_last_rotation_block_id()?;
            if let Some(id) = &last_rotation_block_id {
                if min_id.seq_no() > id.seq_no() { 
                    min_id = id 
                }
                last_rotation_block_id_str = format!("{}", id.seq_no())
            }
            log::trace!(
                "Before state_gc_resolver.advance  shard_client {}  ss_keeper {}  last_rotation_block_id {}  min {}", 
                shard_client.seq_no(),
                handle.id(),
                last_rotation_block_id_str,
                min_id.seq_no()
            );
            let advanced = self.gc_resolver.advance(min_id, engine.deref()).await?;
    
            // clear cache

            if advanced {
                // check states in self.states and delete unneeded (using gc resolver)
                let mut total = 0;
                let mut cleaned = 0;
                for guard in &self.states {
                    total += 1;
                    if self.gc_resolver.allow_state_gc(&guard.0, 0, 0)? {
                        self.states.remove(&guard.0);
                        cleaned +=1;
                    }
                }
                log::debug!("Cleaned: {}, total: {}", cleaned, total);
            }

            // wait next mc block

            handle = loop {
                if let Ok(h) = engine.wait_next_applied_mc_block(&handle, Some(500)).await {
                    break h.0;
                } else if engine.check_stop() {
                    return Ok(());
                }
            };
            engine.save_pss_keeper_mc_block_id(handle.id())?;
        }
    }

    pub async fn wait_fully_stored_state(
        &self, 
        engine: &Engine, 
        id: &BlockIdExt
    ) -> Result<Arc<ShardStateStuff>> {

        // We need to load state from storage (not from cache) with special flag - not to use cache.
        // It allows not to store cells in cache for a long time. So it improves memory consumption.
        //
        // Polling is not a bad scenario here, because it is much easier all other variants 
        // and we do not need super speed.
        loop {
            match self.db.load_shard_state_dynamic_ex(
                id, 
                #[cfg(feature = "async_ss_storage")]
                false
            ) {
                Ok(ss) => break Ok(ss),
                Err(_) => {
                    if engine.check_stop() {
                        fail!("Stopped");
                    }
                    futures_timer::Delay::new(Duration::from_millis(500)).await;
                }
            }
        }
    }

    async fn wait_and_store_persistent_state_attempt(
        &self, 
        engine: &Engine, 
        handle: &Arc<BlockHandle>, 
        abort: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<()> {
        let ss = self.wait_fully_stored_state(engine, handle.id()).await?;
        self.db.store_shard_state_persistent(handle, ss, None, abort.clone()).await?;
        Ok(())
    }

    async fn wait_and_store_persistent_state(
        &self, 
        engine: &Engine, 
        handle: &Arc<BlockHandle>, 
        abort: Arc<dyn Fn() -> bool + Send + Sync>,
    ) {
        let mut attempts = 1;
        while let Err(e) = self.wait_and_store_persistent_state_attempt(engine, handle, abort.clone()).await {
            if engine.check_stop() {
                break
            }
            log::error!("CRITICAL Error saving persistent state (attempt: {}): {:?}", attempts, e);
            attempts += 1;
            futures_timer::Delay::new(Duration::from_millis(5000)).await;
        }
    }
}
