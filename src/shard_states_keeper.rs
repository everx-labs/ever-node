use crate::{
    internal_db::{
        InternalDb, state_gc_resolver::AllowStateGcSmartResolver, LAST_APPLIED_MC_BLOCK,
    },
    shard_state::ShardStateStuff,
    engine_traits::{EngineOperations, EngineAlloc},
    engine::{Engine, Stopper},
    boot,
};
#[cfg(feature = "telemetry")]
use crate::engine_traits::EngineTelemetry;
use storage::{
    block_handle_db::BlockHandle, shardstate_db_async::AllowStateGcResolver, 
    shardstate_db_async::SsNotificationCallback,
};
use ton_block::{BlockIdExt, ShardIdent};
use ton_types::{fail, error, Result, UInt256, BocReader};
use adnl::common::add_unbound_object_to_map;
use std::{
    time::Duration, ops::Deref,
};

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
    max_catch_up_depth: u32,
    skip_saving_pss: bool,
    #[cfg(feature = "telemetry")]
    telemetry: Arc<EngineTelemetry>,
    allocated: Arc<EngineAlloc>,
}

use std::sync::Arc;

impl ShardStatesKeeper {

    pub fn new(
        db: Arc<InternalDb>,
        enable_shard_state_persistent_gc: bool,
        skip_saving_pss: bool,
        cells_lifetime_sec: u64,
        stopper: Arc<Stopper>,
        max_catch_up_depth: u32,
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
            max_catch_up_depth,
            skip_saving_pss,
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
        mut ss_keeper_block: BlockIdExt,
    ) -> Result<tokio::task::JoinHandle<()>> {

        log::trace!("start");

        self.restore_states(last_applied_mc_block, shard_client_mc_block).await?;

        let join_handle = tokio::spawn(async move {
            engine.acquire_stop(Engine::MASK_SERVICE_PSS_KEEPER);
            while let Err(e) = self.worker(&engine, &ss_keeper_block).await {
                log::error!("CRITICAL!!! Unexpected error in persistent states keeper: {:?}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
                ss_keeper_block = 'a: loop {
                    match engine.load_pss_keeper_mc_block_id() {
                        Err(e) => {
                            log::error!("CRITICAL!!! load_pss_keeper_mc_block_id: {:?}", e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        Ok(None) => {
                            log::error!("CRITICAL!!! load_pss_keeper_mc_block_id returned None");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        Ok(Some(id)) => break 'a (*id).clone()
                    }
                };
            }
            engine.release_stop(Engine::MASK_SERVICE_PSS_KEEPER);
        });
        Ok(join_handle)
    }

    pub fn allow_state_gc(&self, block_id: &BlockIdExt) -> Result<bool> {
        self.gc_resolver.allow_state_gc(block_id, 0, u64::MAX)
    }

    #[async_recursion::async_recursion]
    pub async fn load_state(&self, block_id: &BlockIdExt) -> Result<Arc<ShardStateStuff>> {
        log::trace!("load_state {}", block_id);
        if self.gc_resolver.allow_state_gc(block_id, 0, u64::MAX)? {
            fail!("Attempt to load state {} which is already allowed to GC", block_id);
        }
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
            cb1,
            force
        ).await?;

        if let Some(cb) = cb2 {
            cb.wait().await;
            // reload state after saving just to free fully loaded tree 
            // and use lazy loaded cells futher
            state = self.db.load_shard_state_dynamic(handle.id())?;
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

        let state_root = {
            let mut boc_reader = BocReader::new();
            if low_memory_mode {
                let done_cells_storage = self.db.create_done_cells_storage(root_hash)?;
                boc_reader = boc_reader.set_done_cells_storage(done_cells_storage);
            }
            boc_reader
                .set_abort(&|| self.stopper.check_stop())
                .read_inmem(data.clone())?
                .withdraw_single_root()?
        };

        if state_root.repr_hash() != *root_hash {
            fail!("Invalid state hash {:x} != {:x}", state_root.repr_hash(), root_hash);
        }

        log::info!(
            "check_and_store_state: deserialized (low_memory_mode: {}) {} TIME {}",
            handle.id(), low_memory_mode, now.elapsed().as_millis()
        );

        let state = if let Some(wc) = handle.is_queue_update_for() {
            ShardStateStuff::from_out_msg_queue_root_cell(
                handle.id().clone(), 
                state_root,
                wc,
                #[cfg(feature = "telemetry")]
                &self.telemetry,
                &self.allocated
            )?
        } else {
            ShardStateStuff::from_state_root_cell(
                handle.id().clone(), 
                state_root,
                #[cfg(feature = "telemetry")]
                &self.telemetry,
                &self.allocated
            )?
        };

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
        last_applied_mc_block: BlockIdExt,
        shard_client_mc_block: BlockIdExt,
    ) -> Result<()> {

        log::trace!("restore_states  last_applied_mc_block: {}  shard_client_mc_block: {}...", 
            last_applied_mc_block, shard_client_mc_block);

        let _ = self.load_state(&last_applied_mc_block).await?;

        let mc_state = self.load_state(&shard_client_mc_block).await?;
        let shard_blocks = mc_state.shard_hashes()?.top_blocks_all()?; 
        for block_id in &shard_blocks {
            self.load_state(block_id).await?;
        }

        Ok(())
    }

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

        let state = self.restore_state_recursive(id).await?;

        log::trace!("catch_up_state {} CATCHED UP - TIME {}ms", id, now.elapsed().as_millis());
        Ok(state)
    }

    async fn restore_state_recursive(
        &self, 
        id: &BlockIdExt, 
    ) -> Result<Arc<ShardStateStuff>> {

        let try_get_state = |id: &BlockIdExt| {
            if let Some(state) = self.states.get(id) {
                log::trace!("load_state {} FROM CACHE", id);
                Some(state.val().clone())
            } else if let Ok(state) = self.db.load_shard_state_dynamic(id) {
                self.states.insert(id.clone(), state.clone());
                Some(state)
            } else {
                None
            }
        };

        if let Some(state) = try_get_state(id) {
            return Ok(state)
        }

        let top_id = id.clone();
        let mut stack = vec!(id.clone());
        loop {

            self.check_stop()?;

            if stack.len() as u32 > self.max_catch_up_depth {
                fail!("restore_state_recursive: max depth achived on id {}", id);
            }

            let id = stack.last().ok_or_else(|| error!("INTERNAL ERROR: restore_state_recursive: stask is empty"))?;
        
            let handle = self.db.load_block_handle(id)?.ok_or_else(
                || error!("Cannot load handle for {}", id)
            )?;
            let block = self.db.load_block_data(&handle).await?;
            let prev_root = match block.construct_prev_id()? {
                (prev, None) => {
                    if let Some(pr) = try_get_state(&prev) {
                        pr.root_cell().clone()
                    } else {
                        stack.push(prev.clone());
                        continue;
                    }
                },
                (prev1, Some(prev2)) => {
                    let root1 = if let Some(pr) = try_get_state(&prev1) {
                        pr.root_cell().clone()
                    } else {
                        stack.push(prev1.clone());
                        continue;
                    };
                    let root2 = if let Some(pr) = try_get_state(&prev2) {
                        pr.root_cell().clone()
                    } else {
                        stack.push(prev2.clone());
                        continue;
                    };
                    ShardStateStuff::construct_split_root(root1, root2)?
                }
            };

            let is_queue_update_for = block.is_queue_update_for();
            let merkle_update = if let Some (target_wc) = &is_queue_update_for {
                block.get_queue_update_for(*target_wc)?.update
            } else {
                block.block()?.read_state_update()?
            };

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

                    if let Some (target_wc) = &is_queue_update_for {
                        ShardStateStuff::from_out_msg_queue_root_cell(
                            id, 
                            root,
                            *target_wc,
                            #[cfg(feature = "telemetry")]
                            &telemetry,
                            &allocated
                        )
                    } else {
                        ShardStateStuff::from_state_root_cell(
                            id, 
                            root,
                            #[cfg(feature = "telemetry")]
                            &telemetry,
                            &allocated
                        )
                    }
                }
            ).await??;

            self.store_state(&handle, state.clone(), None, true).await?;

            stack.pop().ok_or_else(|| error!("INTERNAL ERROR: restore_state_recursive: stask is empty when pop()"))?;

            if stack.is_empty() {
                if *state.block_id() != top_id {
                    fail!(
                        "INTERNAL ERROR: restore_state_recursive: found state is wrong {} != {}",
                        state.block_id(), top_id
                    );
                }
                return Ok(state);
            }
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
                if self.skip_saving_pss {
                    log::trace!("Persistent state is skipped due to config {}", handle.id());
                } else {
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

                    let shard_blocks = mc_state.shard_hashes()?.top_blocks_all()?; 
                    for block_id in &shard_blocks {
                        log::trace!("saving {}", block_id);
                        let now = std::time::Instant::now();
                        let handle = 'a: loop {
                            match engine.wait_applied_block(block_id, Some(1000)).await {
                                Ok(h) => break 'a h,
                                Err(e) => {
                                    if engine.check_stop() {
                                        return Ok(());
                                    }
                                    log::debug!(
                                        "states_keeper: haven't got shard block handle {} yet: {:?}", 
                                        block_id, e
                                    );
                                }
                            }
                        };
                        self.wait_and_store_persistent_state(
                            engine.deref(), &handle, abort.clone()).await;
                        if engine.check_stop() {
                            return Ok(());
                        }
                        log::trace!("saved {} TIME {}ms", handle.id(), now.elapsed().as_millis());
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
            let archives_gc = engine.load_archives_gc_mc_block_id()?.ok_or_else(
                || error!("INTERNAL ERROR: No archives GC block id in ss keeper worker")
            )?;
            if archives_gc.seq_no() < min_id.seq_no() { 
                min_id = &archives_gc;
            }
            log::info!(
                "Before state_gc_resolver.advance  shard_client {}  ss_keeper {}  last_rotation_block_id {}  archives_gc {}  min {}", 
                shard_client,
                handle.id(),
                last_rotation_block_id_str,
                archives_gc,
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
