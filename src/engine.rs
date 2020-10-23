use crate::{
    define_db_prop,
    block::{convert_block_id_ext_api2blk, BlockStuff, BlockIdExtExtention},
    block_proof::BlockProofStuff,
    config::{TonNodeConfig, KafkaConsumerConfig},
    db::{BlockHandle, InternalDb, InternalDbConfig, InternalDbImpl, NodeState},
    engine_traits::{ExternalDb, OverlayOperations, EngineOperations},
    error::NodeError,
    full_node::{
        apply_block::apply_block,
        shard_client::{
            process_block_broadcast, start_masterchain_client, start_shards_client
        },
    },
    network::{
        full_node_client::{Attempts, FullNodeOverlayClient},
        full_node_service::FullNodeOverlayService
    },
    shard_state::ShardStateStuff,
    types::awaiters_pool::AwaitersPool,
};
#[cfg(feature = "local_test")]
use crate::network::node_network_stub::NodeNetworkStub;
#[cfg(not(feature = "local_test"))]
use crate::network::node_network::NodeNetwork;

#[cfg(feature = "external_db")]
use crate::external_db::kafka_consumer::KafkaConsumer;

#[cfg(feature = "metrics")]
use statsd::client;
#[cfg(feature = "metrics")]
use std::env;

use overlay::QueriesConsumer;
use std::{sync::Arc, ops::Deref, time::Duration, convert::TryInto};

use ton_block::{
    self, ShardIdent, BlockIdExt, MASTERCHAIN_ID, SHARD_FULL, BASE_WORKCHAIN_ID, ShardDescr
};
use ton_types::{error, Result, fail};
use tokio::task::JoinHandle;
use lazy_static;

define_db_prop!(LastMcBlockId,   "LastMcBlockId",   ton_api::ton::ton_node::blockidext::BlockIdExt);
define_db_prop!(InitMcBlockId,   "InitMcBlockId",   ton_api::ton::ton_node::blockidext::BlockIdExt);
define_db_prop!(GcMcBlockId,     "GcMcBlockId",     ton_api::ton::ton_node::blockidext::BlockIdExt);
define_db_prop!(ClientMcBlockId, "ClientMcBlockId", ton_api::ton::ton_node::blockidext::BlockIdExt);
// Persistent shard states (master and all shardes) actual for previous master block were saved
define_db_prop!(PssKeeperBlockId, "PssKeeperBlockId", ton_api::ton::ton_node::blockidext::BlockIdExt);
// Last master block all shard blocks listed in was applied
define_db_prop!(ShardsClientMcBlockId, "ShardsClientMcBlockId", ton_api::ton::ton_node::blockidext::BlockIdExt);

pub struct Engine {
    db: Arc<dyn InternalDb>,
    ext_db: Vec<Arc<dyn ExternalDb>>,
    overlay_operations: Arc<dyn OverlayOperations>,

    shard_states_awaiters: AwaitersPool<BlockIdExt, ShardStateStuff>,
    block_applying_awaiters: AwaitersPool<BlockIdExt, ()>,
    next_block_applying_awaiters: AwaitersPool<BlockIdExt, BlockIdExt>,

    zero_state_id: BlockIdExt,
    init_mc_block_id: BlockIdExt,
    initial_sync_disabled: bool,
}

impl Engine {

    pub(crate) const MAX_EXTERNAL_MESSAGE_DEPTH: u16 = 512;
    pub(crate) const MAX_EXTERNAL_MESSAGE_SIZE: usize = 65535;

    #[allow(unused_mut)]
    pub async fn new(mut general_config: TonNodeConfig, ext_db: Vec<Arc<dyn ExternalDb>>) -> Result<Arc<Self>> {
        log::info!("Creating engine...");

        let db_config = InternalDbConfig { db_directory: "node_db".to_owned() };
        let db = Arc::new(InternalDbImpl::new(db_config).await?);

        let global_config = general_config.load_global_config()?;
        let zero_state_id = global_config.zero_state().expect("check zero state settings");
        let mut init_mc_block_id = global_config.init_block()?.unwrap_or_else(|| zero_state_id.clone());
        if let Ok(block_id) = InitMcBlockId::load_from_db(db.deref()) {
            if block_id.0.seqno > init_mc_block_id.seq_no as i32 {
                init_mc_block_id = convert_block_id_ext_api2blk(&block_id.0)?;
            }
        }
        let initial_sync_disabled = false;

        #[cfg(feature = "local_test")]
        let network = {
            let path = dirs::home_dir().unwrap().into_os_string().into_string().unwrap();
            let path = format!("{}/full-node-test", path);
            Arc::new(NodeNetworkStub::new(&general_config, db.clone(), path)?)
        };
        #[cfg(not(feature = "local_test"))]
        let network = Arc::new(NodeNetwork::new(&mut general_config, db.clone()).await?);
        network.clone().start().await?;

        log::info!("Engine is created.");

        let engine = Arc::new(Engine {
            db,
            ext_db,
            overlay_operations: network.clone(),
            shard_states_awaiters: AwaitersPool::new(),
            block_applying_awaiters: AwaitersPool::new(),
            next_block_applying_awaiters: AwaitersPool::new(),
            zero_state_id,
            init_mc_block_id,
            initial_sync_disabled,
        });

        let full_node_service: Arc<dyn QueriesConsumer> = Arc::new(
            FullNodeOverlayService::new(Arc::clone(&engine) as Arc<dyn EngineOperations>)
        );
        network.add_consumer(
            &network.calc_overlay_short_id(MASTERCHAIN_ID, SHARD_FULL)?,
            Arc::clone(&full_node_service)
        )?;
        network.add_consumer(
            &network.calc_overlay_short_id(BASE_WORKCHAIN_ID, SHARD_FULL)?,
            Arc::clone(&full_node_service)
        )?;

        Ok(engine)
    }

    pub fn db(&self) -> &Arc<dyn InternalDb> { &self.db }

    pub fn ext_db(&self) -> &Vec<Arc<dyn ExternalDb>> { &self.ext_db }

    pub fn zero_state_id(&self) -> &BlockIdExt { &self.zero_state_id }

    pub fn init_mc_block_id(&self) -> &BlockIdExt {&self.init_mc_block_id}

    pub fn initial_sync_disabled(&self) -> bool {self.initial_sync_disabled}

    pub fn set_init_mc_block_id(&self, init_mc_block_id: &BlockIdExt) {
        InitMcBlockId(
            ton_api::ton::ton_node::blockidext::BlockIdExt::from(init_mc_block_id)
        ).store_to_db(self.db().deref()).ok();
    }

    pub async fn get_masterchain_overlay(&self) -> Result<Arc<dyn FullNodeOverlayClient>> {
        self.get_full_node_overlay(ton_block::MASTERCHAIN_ID, ton_block::SHARD_FULL).await
    }

    pub async fn get_full_node_overlay(&self, workchain: i32, shard: u64) -> Result<Arc<dyn FullNodeOverlayClient>> {
        let id = self.overlay_operations.calc_overlay_short_id(workchain, shard)?;
        self.overlay_operations.clone().get_overlay(&id).await
    }

    pub fn shard_states_awaiters(&self) -> &AwaitersPool<BlockIdExt, ShardStateStuff> {
        &self.shard_states_awaiters
    }

    pub fn block_applying_awaiters(&self) -> &AwaitersPool<BlockIdExt, ()> {
        &self.block_applying_awaiters
    }

    pub fn next_block_applying_awaiters(&self) -> &AwaitersPool<BlockIdExt, BlockIdExt> {
        &self.next_block_applying_awaiters
    }

    pub async fn download_and_apply_block_worker(self: Arc<Self>, handle: &BlockHandle, mc_seq_no: u32) -> Result<()> {

        if handle.applied() {
            log::trace!("download_and_apply_block_worker: block is already applied {}", handle.id());
            return Ok(());
        }

        let block = if handle.data_inited() {
            self.load_block(handle).await?
        } else {
            let now = std::time::Instant::now();
            log::trace!("Start downloading block for apply... {}", handle.id());

            let (block, proof) = self.download_block(handle.id()).await?;
            let downloading_time = now.elapsed().as_millis();
            let now = std::time::Instant::now();
            proof.check_proof(self.deref()).await?;
            self.store_block_proof(handle, &proof).await?;
            self.store_block(handle, &block).await?;

            log::trace!("Downloaded block for apply {} TIME download: {}ms, check & save: {}",
                block.id(), downloading_time, now.elapsed().as_millis());
            block
        };

        self.apply_block_worker(handle, &block, mc_seq_no).await?;

        Ok(())
    }

    pub async fn apply_block_worker(
        self: Arc<Self>,
        handle: &BlockHandle,
        block: &BlockStuff,
        mc_seq_no: u32
    ) -> Result<()> {
        if handle.applied() {
            log::trace!("apply_block_worker: block is already applied {}", handle.id());
            return Ok(());
        }

        log::trace!("Start applying block... {}", block.id());

        // TODO fix trash with Arc and clone
        apply_block(handle, block, mc_seq_no, &(self.clone() as Arc<dyn EngineOperations>)).await?;
        self.set_applied(handle, mc_seq_no).await?;

        let ago = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?.as_secs() as i32 - block.gen_utime()? as i32;
        if block.id().shard().is_masterchain() {
            self.store_last_applied_mc_block_id(block.id()).await?;
            STATSD.gauge("last_applied_mc_block", block.id().seq_no() as f64);
            log::info!("Applied block {}, {} seconds old", block.id(), ago);

            let (prev_id, prev2_id_opt) = block.construct_prev_id()?;
            if prev2_id_opt.is_some() {
                fail!("UNEXPECTED error: master block refers two previous blocks");
            }
            let id = handle.id().clone();
            self.next_block_applying_awaiters.do_or_wait(&prev_id, async move { Ok(id) }).await?;
        } else {
            log::info!("Applied block {} ref_mc_block: {}, {} seconds old", block.id(), mc_seq_no, ago);
        }
        Ok(())
    }

    async fn listen_shard_blocks_broadcasts(self: Arc<Self>, shard_ident: ShardIdent) -> Result<()> {
        log::debug!("Started listening overlay for shard {}", shard_ident);
        let client = self.get_full_node_overlay(
            shard_ident.workchain_id(),
            shard_ident.shard_prefix_with_tag()
        ).await?;
        tokio::spawn(async move {
            loop {
                match client.wait_block_broadcast().await {
                    Err(e) => log::error!("Error while wait_block_broadcast for shard {}: {}", shard_ident, e),
                    Ok(brodcast) => {
                        // because of ALL blocks-broadcasts received in one task - spawn for each block
                        let engine = self.clone() as Arc<dyn EngineOperations>;
                        tokio::spawn(async move {
                            if let Err(e) = process_block_broadcast(&engine, &brodcast).await {
                                log::error!("Error while processing block broadcast {}: {}", brodcast.id, e);
                            }
                        });
                    }
                }
            }
        });
        Ok(())
    }

    pub async fn download_next_block_worker(&self, prev_id: &BlockIdExt) -> Result<(BlockStuff, BlockProofStuff)> {
        if !prev_id.is_masterchain() {
            fail!(NodeError::InvalidArg("`download_next_block` is allowed only for masterchain".to_string()))
        }
        let client = self.get_full_node_overlay(
            prev_id.shard().workchain_id(),
            prev_id.shard().shard_prefix_with_tag()
        ).await?;
        let mut attempts = Attempts {
            limit: 3,
            count: 0
        };
        loop {
            match client.download_next_block_full(&prev_id, &attempts).await {
                Err(e) => {
                    log::log!(
                        if attempts.count > 10 { log::Level::Warn } else { log::Level::Debug },
                        "download_next_block: download_next_block_full (attempt {}): prev_id: {}, {:?}",
                        attempts.count, prev_id, e
                    );
                    attempts.count += 1;
                    futures_timer::Delay::new(Duration::from_millis(100)).await;
                },
                Ok(block_and_proof) => break Ok(block_and_proof)
            }
        }
    }

    pub async fn download_block_worker(&self, id: &BlockIdExt) -> Result<(BlockStuff, BlockProofStuff)> {
        let client = self.get_full_node_overlay(
            id.shard().workchain_id(),
            ton_block::SHARD_FULL, // FIXME: which overlay id use?   id.shard().shard_prefix_with_tag()
        ).await?;
        let mut attempts = Attempts {
            limit: 0,
            count: 0
        };
        loop {
            match client.download_block_full(&id, &attempts).await {
                Err(e) => {
                    log::log!(
                        if attempts.count > 10 { log::Level::Warn } else { log::Level::Debug },
                        "download_block: download_block_full (attempt {}): id: {}, {:?}",
                        attempts.count, id, e
                    );
                    attempts.count += 1;
//                    futures_timer::Delay::new(Duration::from_millis(1000)).await;
                },
                Ok(None) => {
                    log::log!(
                        if attempts.count > 10 { log::Level::Warn } else { log::Level::Debug },
                        "download_block: download_block_full (attempt {}): id: {}, got no data",
                        attempts.count, id
                    );
                    attempts.count	 += 1;
//                    futures_timer::Delay::new(Duration::from_millis(1000)).await;
                }
                Ok(Some((block, proof))) => break Ok((block, proof))
            }
        }
    }

    pub async fn download_block_proof_worker(&self, id: &BlockIdExt, is_link: bool, key_block: bool) -> Result<BlockProofStuff> {
        let client = self.get_full_node_overlay(
            id.shard().workchain_id(),
            ton_block::SHARD_FULL, // FIXME: which overlay id use?   id.shard().shard_prefix_with_tag()
        ).await?;
        let mut attempts = Attempts {
            limit: 0,
            count: 0
        };
        loop {
            match client.download_block_proof(&id, is_link, key_block, &attempts).await {
                Err(e) => {
                    log::log!(
                        if attempts.count > 10 { log::Level::Warn } else { log::Level::Debug },
                        "download_block: download_block_proof (attempt {}): id: {}, {:?}",
                        attempts.count, id, e
                    );
                    attempts.count += 1;
//                    futures_timer::Delay::new(Duration::from_millis(1000)).await;
                },
                Ok(None) => {
                    log::log!(
                        if attempts.count > 10 { log::Level::Warn } else { log::Level::Debug },
                        "download_block: download_block_proof (attempt {}): id: {}, got no data",
                        attempts.count, id
                    );
                    attempts.count += 1;
//                    futures_timer::Delay::new(Duration::from_millis(1000)).await;
                }
                Ok(Some(proof)) => break Ok(proof)
            }
        }
    }

    pub(crate) async fn do_check_initial_sync_complete(&self) -> Result<bool> {
        let last_mc_id = self.load_last_applied_mc_block_id().await?;
        let last_mc_handle = self.load_block_handle(&last_mc_id)?;
        if last_mc_handle.gen_utime()? + 600 <= self.now() {
            return Ok(false);
        }
        let shards_client = self.load_shards_client_mc_block_id().await?;
        if shards_client.seq_no() + 16 < last_mc_id.seq_no() {
            return Ok(false);
        }

        // TODO see ValidatorManagerImpl::out_of_sync() in t-node for additional conditions (may be)

        return Ok(true);
    }

    pub fn start_gc_scheduler(self: Arc<Self>) -> JoinHandle<()>{
        log::info!("Starting GC scheduler...");

        let handle = tokio::spawn(async move {
            loop {
                log::trace!("Waiting GC...");
                // 24 hour delay:
                futures_timer::Delay::new(Duration::from_secs(3600 * 24)).await;
                let self_cloned = Arc::clone(&self);
                if let Err(error) = tokio::task::spawn_blocking(move || {
                    log::info!("Starting GC...");
                    match self_cloned.db.gc_shard_state_dynamic_db() {
                        Ok(count) => log::info!("Finished GC. Deleted {} cells.", count),
                        Err(error) => log::error!("GC: Error occurred during garbage collecting of dynamic BOC: {:?}", error),
                    }
                }).await {
                    log::error!("GC: Failed to start. {:?}", error)
                }
            }
        });

        log::info!("GC scheduler started.");

        handle
    }

    async fn store_pss_keeper_block_id(&self, id: &BlockIdExt) -> Result<()> {
        PssKeeperBlockId(id.into()).store_to_db(self.db().deref())
    }

    pub fn start_persistent_states_keeper(
        engine: Arc<Engine>,
        pss_keeper_block: BlockIdExt
    ) -> Result<JoinHandle<()>> {
        let join_handle = tokio::spawn(async move {
            if let Err(e) = Self::persistent_states_keeper(engine, pss_keeper_block).await {
                log::error!("FATAL!!! Unexpected error in persistent states keeper: {:?}", e);
            }
        });
        Ok(join_handle)
    }

    pub async fn persistent_states_keeper(
        engine: Arc<Engine>,
        pss_keeper_block: BlockIdExt
    ) -> Result<()> {
        if !pss_keeper_block.shard().is_masterchain() {
            fail!("'pss_keeper_block' mast belong master chain");
        }
        let mut handle = engine.load_block_handle(&pss_keeper_block)?;
        loop {
            if handle.is_key_block()? {
                let mc_state = engine.load_state(handle.id()).await?;
                let is_persistent_state = if handle.id().seq_no() == 0 {
                    true
                } else {
                    if let Some(prev_key_block_id) =
                        mc_state.shard_state_extra()?.prev_blocks.get_prev_key_block(handle.id().seq_no() - 1)? {
                        let prev_handle = engine.load_block_handle(&BlockIdExt {
                            shard_id: ShardIdent::masterchain(),
                            seq_no: prev_key_block_id.seq_no,
                            root_hash: prev_key_block_id.root_hash,
                            file_hash: prev_key_block_id.file_hash
                        })?;
                        engine.is_persistent_state(handle.gen_utime()?, prev_handle.gen_utime()?)
                    } else {
                        false
                    }
                };
                if !is_persistent_state {
                    log::trace!("persistent_states_keeper: skip keyblock (is not persistent) {}", handle.id());
                } else {
                    log::trace!("persistent_states_keeper: saving {}", handle.id());
                    let now = std::time::Instant::now();
                    engine.store_persistent_state_attempts(&handle, &mc_state).await;
                    log::trace!("persistent_states_keeper: saved {} TIME {}ms",
                        handle.id(), now.elapsed().as_millis());

                    let mut shard_blocks = vec!();
                    mc_state.shard_state_extra()?.shards.iterate_shards(&mut |ident: ShardIdent, descr: ShardDescr| {
                        shard_blocks.push(BlockIdExt {
                            shard_id: ident,
                            seq_no: descr.seq_no,
                            root_hash: descr.root_hash,
                            file_hash: descr.file_hash
                        });
                        Ok(true)
                    })?;
                    for block_id in shard_blocks {
                        log::trace!("persistent_states_keeper: saving {}", block_id);
                        let now = std::time::Instant::now();
                        
                        let handle = engine.load_block_handle(&block_id)?;
                        let ss = engine.wait_state(&handle).await?;
                        engine.store_persistent_state_attempts(&handle, &ss).await;

                        log::trace!("persistent_states_keeper: saved {} TIME {}ms",
                            handle.id(), now.elapsed().as_millis());
                    };
                }
            }
            handle = engine.wait_next_applied_mc_block(&handle).await?.0;
            engine.store_pss_keeper_block_id(handle.id(), ).await?;
        }
    }

    pub async fn store_persistent_state_attempts(&self, handle: &Arc<BlockHandle>, ss: &ShardStateStuff) {
        let mut attempts = 1;
        while let Err(e) = self.db.store_shard_state_persistent(handle, ss).await {
            log::error!("CRITICAL Error saving persistent state (attempt: {}): {:?}", attempts, e);
            attempts += 1;
            futures_timer::Delay::new(Duration::from_millis(5000)).await;
        }
    }
}

async fn boot(engine: &Arc<Engine>) -> Result<(BlockIdExt, BlockIdExt, BlockIdExt)> {
    log::info!("Booting...");

    let mut result = LastMcBlockId::load_from_db(engine.db().deref())
        .and_then(|id| (&id.0).try_into());
    if let Ok(id) = result {
        result = crate::boot::warm_boot(engine.clone(), id).await;
    }
    let last_mc_block = match result {
        Ok(id) => id,
        Err(err) => {
            log::warn!("error before cold boot: {}", err);
            let id = crate::boot::cold_boot(engine.clone()).await?;
            engine.store_last_applied_mc_block_id(&id).await?;
            id
        }
    };

    let shards_client_block = match ShardsClientMcBlockId::load_from_db(engine.db().deref()) {
        Ok(id) => (&id.0).try_into()?,
        Err(_) => {
            engine.store_shards_client_mc_block_id(&last_mc_block).await?;
            log::info!("`ShardsClientMcBlockId` wasn't set - it is inited by `LastMcBlockId`.");
            last_mc_block.clone()
        }
    };

    let pss_keeper_block = match PssKeeperBlockId::load_from_db(engine.db().deref()) {
        Ok(id) => (&id.0).try_into()?,
        Err(_) => {
            engine.store_pss_keeper_block_id(&last_mc_block).await?;
            log::info!("`PssKeeperBlockId` wasn't set - it is inited by `LastMcBlockId`.");
            last_mc_block.clone()
        }
    };

    log::info!("Boot complete.");
    log::info!("LastMcBlockId: {}", last_mc_block);
    log::info!("ShardsClientMcBlockId: {}", shards_client_block);
    Ok((last_mc_block, shards_client_block, pss_keeper_block))
}

pub async fn run(general_config: TonNodeConfig, ext_db: Vec<Arc<dyn ExternalDb>>) -> Result<()> {
    log::info!("Engine::run");

    let consumer_config = general_config.kafka_consumer_config();

    //// Create engine
    let engine = Engine::new(general_config, ext_db).await?;

    //// Boot
    let (mut last_mc_block, mut shards_client_mc_block, pss_keeper_block) = boot(&engine).await?;

    if !engine.check_initial_sync_complete().await? {
        crate::sync::start_sync(Arc::clone(&engine) as Arc<dyn EngineOperations>).await?;
        last_mc_block = LastMcBlockId::load_from_db(engine.db().deref())
            .and_then(|id| (&id.0).try_into())?;
        shards_client_mc_block = ShardsClientMcBlockId::load_from_db(engine.db().deref())
            .and_then(|id| (&id.0).try_into())?;
    }

    //// Start services
    start_external_broadcast_process(engine.clone(), &consumer_config)?;

    // TODO: Temporary disabled GC because of DB corruption. Enable after fix.
    // Arc::clone(&engine).start_gc_scheduler();

    // mc broadcasts (shard blocks are received by this listener too)
    Arc::clone(&engine).listen_shard_blocks_broadcasts(ShardIdent::masterchain()).await?;

    // shard blocks download client
    let _ = start_shards_client(engine.clone(), shards_client_mc_block)?;

    let _ = Engine::start_persistent_states_keeper(engine.clone(), pss_keeper_block)?;

    // mc blocks download client
    let _ = start_masterchain_client(engine, last_mc_block)?.await;

    Ok(())
}

#[cfg(not(feature = "external_db"))]
pub fn start_external_broadcast_process(
    _engine: Arc<dyn EngineOperations>, 
    _consumer_config: &Option<KafkaConsumerConfig>
) -> Result<()> { 
    Ok(())
}

#[cfg(feature = "external_db")]
pub fn start_external_broadcast_process(
    engine: Arc<dyn EngineOperations>, 
    consumer_config: &Option<KafkaConsumerConfig>
) -> Result<()> {
    if let Some(consumer_config) = consumer_config {
        let config = consumer_config.clone();
        tokio::spawn(async move {
            match KafkaConsumer::new(config, engine) {
                Ok(consumer) => {
                    match consumer.run().await {
                        Ok(_) => {},
                        Err(e) => { log::error!("Kafka listening is failed: {}", e)}
                    }
                }
                Err(e) => { log::error!("Start listening kafka is failed: {}", e)}
            }
        });
    } else {
        fail!("consumer config is not set!")
    }
    Ok(())
}

#[cfg(feature = "metrics")]
lazy_static::lazy_static! {
    pub static ref STATSD: client::Client = {
        let mut statsd_endp = env::var("STATSD_DOMAIN").expect("STATSD_DOMAIN env variable not found");
        let statsd_port = env::var("STATSD_PORT").expect("STATSD_PORT env variable not found");
        statsd_endp.push_str(&statsd_port);
        let statsd_client = match client::Client::new(statsd_endp, "rnode"){
            Ok(client) => client,
            Err(e) => {
                panic!("Can't init statsd client: {:?}", e);
            }
        };
        statsd_client
    };
}

#[cfg(not(feature = "metrics"))]
pub struct StatsdClient {}

#[cfg(not(feature = "metrics"))]
impl StatsdClient {
    pub fn new() -> StatsdClient { StatsdClient{} }
    pub fn gauge(&self, _metric_name: &str, _value: f64) {}
    pub fn incr(&self, _metric_name: &str) {}
    pub fn histogram(&self, _metric_name: &str, _value: f64) {}
    pub fn timer(&self, _metric_name: &str, _value: f64) {}
}

#[cfg(not(feature = "metrics"))]
lazy_static::lazy_static! {
    pub static ref STATSD: StatsdClient = StatsdClient::new();
}
