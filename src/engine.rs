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
        full_node_client::{Attempts, FullNodeOverlayClient}, node_network::NodeNetwork,
        full_node_service::FullNodeOverlayService
    },
    shard_state::ShardStateStuff,
    types::awaiters_pool::AwaitersPool,
};
#[cfg(features = "local_test")]
use crate::network::node_network_stub::NodeNetworkStub;

#[cfg(feature = "external_db")]
use crate::external_db::kafka_consumer::KafkaConsumer;

#[cfg(feature = "metrics")]
use statsd::client;
#[cfg(feature = "metrics")]
use std::env;

use overlay::QueriesConsumer;
use std::{sync::Arc, ops::Deref, time::Duration, convert::TryInto};

use ton_block::{
    self, ShardIdent, BlockIdExt, MASTERCHAIN_ID, SHARD_FULL, BASE_WORKCHAIN_ID
};
use ton_types::{error, Result, fail};
use tokio::task::JoinHandle;
use lazy_static;

define_db_prop!(LastMcBlockId,   "LastMcBlockId",   ton_api::ton::ton_node::blockidext::BlockIdExt);
define_db_prop!(InitMcBlockId,   "InitMcBlockId",   ton_api::ton::ton_node::blockidext::BlockIdExt);
define_db_prop!(GcMcBlockId,     "GcMcBlockId",     ton_api::ton::ton_node::blockidext::BlockIdExt);
define_db_prop!(ClientMcBlockId, "ClientMcBlockId", ton_api::ton::ton_node::blockidext::BlockIdExt);
define_db_prop!(SavingState1,    "SavingState1",    ton_api::ton::ton_node::blockidext::BlockIdExt);
define_db_prop!(SavingState2,    "SavingState2",    ton_api::ton::ton_node::blockidext::BlockIdExt);
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

    pub async fn new(mut general_config: TonNodeConfig, ext_db: Vec<Arc<dyn ExternalDb>>) -> Result<Arc<Self>> {
        log::info!(target: "node", "Creating engine...");

        let db_config = InternalDbConfig { db_directory: "node_db".to_owned() };
        let db = Arc::new(InternalDbImpl::new(db_config)?);

        let global_config = general_config.load_global_config()?;
        let zero_state_id = global_config.zero_state().expect("check zero state settings");
        let mut init_mc_block_id = global_config.init_block()?.unwrap_or_else(|| zero_state_id.clone());
        if let Ok(block_id) = InitMcBlockId::load_from_db(db.deref()) {
            if block_id.0.seqno > init_mc_block_id.seq_no as i32 {
                init_mc_block_id = convert_block_id_ext_api2blk(&block_id.0)?;
            }
        }
        let initial_sync_disabled = false;

        #[cfg(features = "local_test")]
        let network = {
            let path = dirs::home_dir().unwrap().into_os_string().into_string().unwrap();
            let path = format!("{}/full-node-test", path);
            Arc::new(NodeNetworkStub::new(&general_config, db.clone(), path)?)
        };
        #[cfg(not(features = "local_test"))]
        let network = Arc::new(NodeNetwork::new(&mut general_config, db.clone()).await?);
        network.clone().start().await?;

        log::info!(target: "node", "Engine is created.");

        let engine = Arc::new(Engine {
            db: db.clone(),
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

    pub fn db(&self) -> &dyn InternalDb { self.db.deref() }

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

    pub async fn download_and_apply_block_worker(self: Arc<Self>, handle: &BlockHandle) -> Result<()> {

        if handle.applied() {
            log::trace!("download_and_apply_block_worker: block is already applied {}", handle.id());
            return Ok(());
        }

        let block = if handle.data_inited() {
            self.db().load_block_data(handle.id())?
        } else {
            log::trace!("Start downloading block for apply... {}", handle.id());

            let (block, proof) = self.download_block(handle.id()).await?;
            proof.check_proof(self.deref()).await?;
            self.store_block_proof(handle, &proof).await?;
            self.store_block(handle, &block).await?;

            log::trace!("Downloaded block for apply {}", block.id());
            block
        };

        self.apply_block_worker(handle, &block).await?;

        Ok(())
    }

    pub async fn apply_block_worker(self: Arc<Self>, handle: &BlockHandle, block: &BlockStuff) -> Result<()> {

        if handle.applied() {
            log::trace!("apply_block_worker: block is already applied {}", handle.id());
            return Ok(());
        }

        log::trace!("Start applying block... {}", block.id());

        // TODO fix trash with Arc and clone
        apply_block(handle.deref(), block, &(self.clone() as Arc<dyn EngineOperations>)).await?;
        self.deref().db.store_block_applied(block.id())?;

        let ago = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?.as_secs() as i32 - block.gen_utime()? as i32;
        if block.id().shard().is_masterchain() {
            LastMcBlockId(block.id_api().clone()).store_to_db(self.db.deref())?;
            STATSD.gauge("last_applied_mc_block", block.id().seq_no() as f64);
            log::info!("Applied block {}, {} seconds old", block.id(), ago);

            let (prev_id, prev2_id_opt) = block.construct_prev_id()?;
            if prev2_id_opt.is_some() {
                fail!("UNEXPECTED error: master block refers two previous blocks");
            }
            let id = handle.id().clone();
            self.next_block_applying_awaiters.do_or_wait(&prev_id, async move { Ok(id) }).await?;
        } else {
            let master_ref = block
                .block()
                .read_info()?
                .read_master_ref()?
                .ok_or_else(|| NodeError::InvalidData(format!(
                    "Block {} doesn't contain masterchain block extra", block.id(),
                )))?;
            log::info!("Applied block {} ref_mc_block: {}, {} seconds old", block.id(), master_ref.master.seq_no, ago);
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

    pub fn start_gc_scheduler(self: Arc<Self>) -> JoinHandle<()>{
        log::info!(target: "node", "Starting GC scheduler...");

        let handle = tokio::spawn(async move {
            loop {
                log::trace!(target: "node", "Waiting GC...");
                // 24 hour delay:
                futures_timer::Delay::new(Duration::from_secs(3600 * 24)).await;
                let self_cloned = Arc::clone(&self);
                if let Err(error) = tokio::task::spawn_blocking(move || {
                    log::info!(target: "node", "Starting GC...");
                    match self_cloned.db.gc_shard_state_dynamic_db() {
                        Ok(count) => log::info!(target: "node", "Finished GC. Deleted {} cells.", count),
                        Err(error) => log::error!(target: "node", "GC: Error occurred during garbage collecting of dynamic BOC: {:?}", error),
                    }
                }).await {
                    log::error!(target: "node", "GC: Failed to start. {:?}", error)
                }
            }
        });

        log::info!(target: "node", "GC scheduler started.");

        handle
    }

    /// Checks if some persistent state's saving is interrupted and resave it if need
    async fn check_persistent_states(&self) -> Result<()> {
        let empty_id = ton_api::ton::ton_node::blockidext::BlockIdExt::default();
        let ss1 = SavingState1::load_from_db(self.db()).unwrap_or_default();
        if ss1.0 != empty_id {
            let handle = self.load_block_handle(&(&ss1.0).try_into()?)?;
            let state = self.load_state(handle.id()).await?;
            self.store_persistent_state_attempts(&handle, &state).await;
            SavingState1(empty_id.clone()).store_to_db(self.db().deref())?;
            log::info!("Persistent shard state saved {}", state.block_id());
        }
        let ss2 = SavingState2::load_from_db(self.db()).unwrap_or_default();
        if ss2.0 != empty_id {
            let handle = self.load_block_handle(&(&ss2.0).try_into()?)?;
            let state = self.load_state(handle.id()).await?;
            self.store_persistent_state_attempts(&handle, &state).await;
            SavingState2(empty_id.clone()).store_to_db(self.db().deref())?;
            log::info!("Persistent shard state saved {}", state.block_id());
        }
        Ok(())
    }

    pub async fn store_persistent_state_attempts(&self, handle: &BlockHandle, ss: &ShardStateStuff) {
        let mut attempts = 1;
        while let Err(e) = self.db.store_shard_state_persistent(handle, ss).await {
            log::error!("CRITICAL Error saving persistent state (attempt: {}): {:?}", attempts, e);
            attempts += 1;
            futures_timer::Delay::new(Duration::from_millis(5000)).await;
        }
    }
}

async fn boot(engine: &Arc<Engine>) -> Result<(BlockIdExt, BlockIdExt)> {
    log::info!(target: "node", "Booting...");

    let mut result = LastMcBlockId::load_from_db(engine.db().deref()).and_then(|id| (&id.0).try_into());
    if let Ok(id) = result {
        result = crate::boot::warm_boot(engine.clone(), id).await;
    }
    let last_mc_block = match result {
        Ok(id) => id,
        Err(err) => {
            log::warn!("error before cold boot: {}", err);
            crate::boot::cold_boot(engine.clone()).await?
        }
    };

    let shards_client_block = match ShardsClientMcBlockId::load_from_db(engine.db().deref()) {
        Ok(id) => (&id.0).try_into()?,
        Err(_) => {
            engine.store_shards_client_mc_block_id(&last_mc_block).await?;
            log::info!(target: "node", "`ShardsClientMcBlockId` wasn't set - it is inited by `LastMcBlockId`.");
            last_mc_block.clone()
        }
    };

    log::info!(target: "node", "Boot complete.");
    log::info!(target: "node", "LastMcBlockId: {}", last_mc_block);
    log::info!(target: "node", "ShardsClientMcBlockId: {}", shards_client_block);
    Ok((last_mc_block, shards_client_block))
}

pub async fn run(general_config: TonNodeConfig, ext_db: Vec<Arc<dyn ExternalDb>>) -> Result<()> {
    log::info!(target: "node", "Engine::run");

    let consumer_config = general_config.kafka_consumer_config();

    //// Create engine

    let engine = Engine::new(general_config, ext_db).await?;

    engine.check_persistent_states().await?;

    //// Boot

    let (last_mc_block, shards_client_mc_block) = boot(&engine).await?;

    //// Start services
    start_external_broadcast_process(engine.clone(), &consumer_config)?;

    Arc::clone(&engine).start_gc_scheduler();

    // mc broadcasts (shard blocks are received by this listener too)
    Arc::clone(&engine).listen_shard_blocks_broadcasts(ShardIdent::masterchain()).await?;

    // shard blocks download client
    let _ = start_shards_client(engine.clone(), shards_client_mc_block)?;

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
        let statsd_client = match client::Client::new(statsd_endp, "rnode."){
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
}

#[cfg(not(feature = "metrics"))]
lazy_static::lazy_static! {
    pub static ref STATSD: StatsdClient = StatsdClient::new();
}
