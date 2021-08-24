use crate::{
    block::{BlockStuff, BlockIdExtExtention},
    block_proof::BlockProofStuff,
    config::{TonNodeConfig, KafkaConsumerConfig, CollatorTestBundlesGeneralConfig},
    engine_traits::{
        ExternalDb, EngineOperations,
        OverlayOperations, PrivateOverlayOperations, Server, ValidatedBlockStat,
    },
    full_node::{
        apply_block::{self, apply_block},
        shard_client::{
            process_block_broadcast, start_masterchain_client, start_shards_client,
            SHARD_BROADCAST_WINDOW
        },
    },
    internal_db::{
        InternalDb, InternalDbConfig, InternalDbImpl, 
        INITIAL_MC_BLOCK, LAST_APPLIED_MC_BLOCK, PSS_KEEPER_MC_BLOCK,
        state_gc_resolver::AllowStateGcSmartResolver
    },
    network::{
        full_node_client::FullNodeOverlayClient, control::ControlServer,
        full_node_service::FullNodeOverlayService
    },
    shard_state::ShardStateStuff,
    types::{awaiters_pool::AwaitersPool, lockfree_cache::TimeBasedCache},
    ext_messages::{MessagesPool, EXT_MESSAGES_TRACE_TARGET},
    validator::{validator_manager::start_validator_manager, candidate_db::LastRotationBlockDb},
    shard_blocks::{
        ShardBlocksPool, resend_top_shard_blocks_worker, save_top_shard_blocks_worker, 
        ShardBlockProcessingResult
    },
    boot,
};
#[cfg(feature = "slashing")]
use crate::{
    block::convert_block_id_ext_api2blk,
    engine_traits::ValidatedBlockStatNode,
    validator::validator_utils::calc_subset_for_workchain,
};
#[cfg(feature = "local_test")]
use crate::network::node_network_stub::NodeNetworkStub;
#[cfg(not(feature = "local_test"))]
use crate::network::node_network::NodeNetwork;
#[cfg(feature = "external_db")]
use crate::external_db::kafka_consumer::KafkaConsumer;
#[cfg(feature = "telemetry")]
use crate::{
    full_node::telemetry::FullNodeTelemetry,
    validator::telemetry::CollatorValidatorTelemetry,
    network::telemetry::{FullNodeNetworkTelemetry, FullNodeNetworkTelemetryKind},
};
use overlay::QueriesConsumer;
#[cfg(feature = "metrics")]
use statsd::client;
use std::{
    convert::TryInto, ops::Deref, sync::{Arc, atomic::{AtomicBool, AtomicU32, Ordering, AtomicI32, AtomicU64}},
    time::Duration, collections::HashMap,
};
#[cfg(feature = "slashing")]
use std::collections::HashSet;
#[cfg(feature = "metrics")]
use std::env;
use storage::types::BlockHandle;
use ton_block::{
    self, ShardIdent, BlockIdExt, MASTERCHAIN_ID, SHARD_FULL,
};
use ton_types::{error, fail, Result};
#[cfg(feature = "slashing")]
use ton_types::UInt256;
use ton_api::ton::ton_node::{
    Broadcast, broadcast::{BlockBroadcast, ExternalMessageBroadcast, NewShardBlockBroadcast}
};
use adnl::{common::KeyId, server::AdnlServerConfig};
use crossbeam_channel::{Sender, Receiver};

const MAX_VALIDATED_BLOCK_STATS_ENTRIES_COUNT: usize = 10000; //maximum number of validated block stats entries in engine's queue

pub struct Engine {
    db: Arc<dyn InternalDb>,
    ext_db: Vec<Arc<dyn ExternalDb>>,
    overlay_operations: Arc<dyn OverlayOperations>,
    shard_states_awaiters: AwaitersPool<BlockIdExt, ShardStateStuff>,
    block_applying_awaiters: AwaitersPool<BlockIdExt, ()>,
    next_block_applying_awaiters: AwaitersPool<BlockIdExt, BlockIdExt>,
    download_block_awaiters: AwaitersPool<BlockIdExt, (BlockStuff, BlockProofStuff)>,
    external_messages: MessagesPool,
    servers: lockfree::queue::Queue<Server>,
    stop: Arc<AtomicU32>,

    zero_state_id: BlockIdExt,
    init_mc_block_id: BlockIdExt,
    initial_sync_disabled: bool,
    pub network: Arc<NodeNetwork>,
    archives_life_time: Option<u32>,
    shard_blocks: ShardBlocksPool,
    last_known_mc_block_seqno: AtomicU32,
    last_known_keyblock_seqno: AtomicU32,
    will_validate: AtomicBool,

    test_bundles_config: CollatorTestBundlesGeneralConfig,
 
    shard_states_cache: TimeBasedCache<BlockIdExt, ShardStateStuff>,
    loaded_from_ss_cache: AtomicU64,
    loaded_ss_total: AtomicU64,
    pub workchain_id: AtomicI32,

    state_gc_resolver: Arc<AllowStateGcSmartResolver>,
    validation_status: lockfree::map::Map<ShardIdent, u64>,
    collation_status: lockfree::map::Map<ShardIdent, u64>,
    validated_block_stats_sender: Sender<ValidatedBlockStat>,
    validated_block_stats_receiver: Receiver<ValidatedBlockStat>,

    #[cfg(feature = "telemetry")]
    full_node_telemetry: FullNodeTelemetry,
    #[cfg(feature = "telemetry")]
    collator_telemetry: CollatorValidatorTelemetry,
    #[cfg(feature = "telemetry")]
    validator_telemetry: CollatorValidatorTelemetry,
    #[cfg(feature = "telemetry")]
    full_node_service_telemetry: FullNodeNetworkTelemetry,

    last_rotation_block_db: LastRotationBlockDb, // TODO use node-state DB instead
}

struct DownloadContext<'a, T> {
    client: Arc<dyn FullNodeOverlayClient>,
    db: &'a dyn InternalDb,
    downloader: Arc<dyn Downloader<Item = T>>,
    id: &'a BlockIdExt,
    limit: Option<u32>,
    log_error_limit: u32,
    name: &'a str,
    timeout: Option<(u64, u64, u64)>, // (current, multiplier*10, max)
    #[cfg(feature = "telemetry")]
    full_node_telemetry: &'a FullNodeTelemetry,
}

impl <T> DownloadContext<'_, T> {

    async fn download(&mut self) -> Result<T> {
        let mut attempt = 1;
        loop {
            match self.downloader.try_download(self).await {
                Err(e) => self.log(format!("{}", e).as_str(), attempt),
                Ok(ret) => break Ok(ret)
            }
            attempt += 1;
            if let Some(limit) = &self.limit {
                if &attempt > limit {
                    fail!("Downloader: out of attempts");
                }
            }
            if let Some((current, mult, max)) = &mut self.timeout {
                *current = (*max).min(*current * *mult / 10);
                futures_timer::Delay::new(Duration::from_millis(*current)).await;
            } else {
                tokio::task::yield_now().await;
            }
        }
    }

    fn log(&self, msg: &str, attempt: u32) {
       log::log!(
           if attempt > self.log_error_limit {
               log::Level::Warn
           } else {
               log::Level::Debug
           },
           "{} (attempt {}): id: {}, {}",
           self.name, attempt, self.id, msg
       )
    }

}

#[async_trait::async_trait]
trait Downloader: Send + Sync {
    type Item;
    async fn try_download(
        &self, 
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Self::Item>;
}

struct BlockDownloader;

#[async_trait::async_trait]
impl Downloader for BlockDownloader {
    type Item = (BlockStuff, BlockProofStuff);
    async fn try_download(
        &self, 
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Self::Item> {
        if let Some(handle) = context.db.load_block_handle(context.id)? {
            let mut is_link = false;
            if handle.has_data() && handle.has_proof_or_link(&mut is_link) {
                return Ok((
                    context.db.load_block_data(&handle).await?,
                    context.db.load_block_proof(&handle, is_link).await?
                ));
            }
        }
        #[cfg(feature = "telemetry")]
        context.full_node_telemetry.new_downloading_block_attempt(context.id);
        let ret = context.client.download_block_full(context.id).await;
        #[cfg(feature = "telemetry")]
        if ret.is_ok() { 
            context.full_node_telemetry.new_downloaded_block(context.id);
        }
        ret
    }
}

struct BlockProofDownloader {
    is_link: bool,
    key_block: bool  
}

#[async_trait::async_trait]
impl Downloader for BlockProofDownloader {
    type Item = BlockProofStuff;
    async fn try_download(
        &self, 
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Self::Item> {
        if let Some(handle) = context.db.load_block_handle(context.id)? {
            let mut is_link = false;
            if handle.has_proof_or_link(&mut is_link) {
                return Ok(context.db.load_block_proof(&handle, is_link).await?);
            }
        }
        context.client.download_block_proof(
            context.id, 
            self.is_link, 
            self.key_block, 
        ).await        
    }
}              

struct NextBlockDownloader;

#[async_trait::async_trait]
impl Downloader for NextBlockDownloader {
    type Item = (BlockStuff, BlockProofStuff);
    async fn try_download(
        &self, 
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Self::Item> {
        if let Some(prev_handle) = context.db.load_block_handle(context.id)? {
            if prev_handle.has_next1() {
                let next_id = context.db.load_block_next1(context.id)?;
                if let Some(next_handle) = context.db.load_block_handle(&next_id)? {
                    let mut is_link = false;
                    if next_handle.has_data() && next_handle.has_proof_or_link(&mut is_link) {
                        return Ok((
                            context.db.load_block_data(&next_handle).await?,
                            context.db.load_block_proof(&next_handle, is_link).await?
                        ));
                    }
                }
            }
        }
        context.client.download_next_block_full(context.id).await
    }    
}  

struct ZeroStateDownloader;

#[async_trait::async_trait]
impl Downloader for ZeroStateDownloader {
    type Item = (ShardStateStuff, Vec<u8>);
    async fn try_download(
        &self, 
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Self::Item> {
        if let Some(handle) = context.db.load_block_handle(context.id)? {
            if handle.has_state() {
                let zs = context.db.load_shard_state_dynamic(context.id)?;
                let mut data = vec!();
                zs.write_to(&mut data)?;
                return Ok((zs, data));
            }
        }
        context.client.download_zero_state(context.id).await
    }
}

impl Engine {

    // Masks for services
    pub const MASK_SERVICE_KAFKA_CONSUMER: u32                 = 0x0001;
    pub const MASK_SERVICE_MASTERCHAIN_BROADCAST_LISTENER: u32 = 0x0002;
    pub const MASK_SERVICE_MASTERCHAIN_CLIENT: u32             = 0x0004;
    pub const MASK_SERVICE_PSS_KEEPER: u32                     = 0x0008;
    pub const MASK_SERVICE_SHARDCHAIN_BROADCAST_LISTENER: u32  = 0x0010;
    pub const MASK_SERVICE_SHARDCHAIN_CLIENT: u32              = 0x0020;
    pub const MASK_SERVICE_TOP_SHARDBLOCKS_SENDER: u32         = 0x0040;
    pub const MASK_SERVICE_VALIDATOR_MANAGER: u32              = 0x0080;

    const MASK_STOP: u32 = 0x80000000; 
    const TIMEOUT_STOP: u64 = 100; // Milliseconds

    pub async fn new(
        general_config: TonNodeConfig, 
        ext_db: Vec<Arc<dyn ExternalDb>>, 
        initial_sync_disabled : bool
    ) -> Result<Arc<Self>> {

        log::info!("Creating engine...");

        let archives_life_time = general_config.gc_archives_life_time_hours();
        let db_directory = general_config.internal_db_path().unwrap_or_else(|| {"node_db"}).to_string();
        let cells_gc_interval_ms = general_config.cells_gc_interval_ms();
        let last_rotation_block_db = LastRotationBlockDb::new(db_directory.clone());
        let db_config = InternalDbConfig { db_directory, cells_gc_interval_ms };
        let db = Arc::new(InternalDbImpl::new(db_config).await?);
        let global_config = general_config.load_global_config()?;
        let test_bundles_config = general_config.test_bundles_config().clone();
        let zero_state_id = global_config.zero_state().expect("check zero state settings");
        let mut init_mc_block_id = global_config.init_block()?.unwrap_or_else(|| zero_state_id.clone());
        if let Ok(Some(block_id)) = db.load_node_state(INITIAL_MC_BLOCK) {
            if block_id.seq_no > init_mc_block_id.seq_no {
                init_mc_block_id = block_id.deref().clone()
            }
        }
        let workchain_id = general_config.workchain_id().unwrap_or(ton_block::INVALID_WORKCHAIN_ID);
        log::info!("workchain_id from config {}", workchain_id);
        let workchain_id = AtomicI32::new(workchain_id);

        #[cfg(feature = "local_test")]
        let network = {
            let path = dirs::home_dir().unwrap().into_os_string().into_string().unwrap();
            let path = format!("{}/full-node-test", path);
            Arc::new(NodeNetworkStub::new(&general_config, db.clone(), path)?)
        };
        #[cfg(not(feature = "local_test"))]
        let network = NodeNetwork::new(general_config).await?;
        network.clone().start().await?;

        let shard_blocks = match db.load_all_top_shard_blocks() {
            Ok(tsbs) => tsbs,
            Err(e) => {
                log::error!("Can't load top shard blocks from db (continue without ones): {:?}", e);
                HashMap::default()
            }
        };
        let last_mc_seqno = db
            .load_node_state(LAST_APPLIED_MC_BLOCK)?
            .map(|id| id.seq_no as u32)
            .unwrap_or_default();
        let (shard_blocks_pool, shard_blocks_receiver) = 
            ShardBlocksPool::new(shard_blocks, last_mc_seqno, false);

        let state_gc_resolver = Arc::new(AllowStateGcSmartResolver::new());
        db.start_states_gc(state_gc_resolver.clone());

        log::info!("Engine is created.");

        let (validated_block_stats_sender, validated_block_stats_receiver) = crossbeam_channel::bounded(MAX_VALIDATED_BLOCK_STATS_ENTRIES_COUNT);
        let engine = Arc::new(Engine {
            db,
            ext_db,
            overlay_operations: network.clone(),
            shard_states_awaiters: AwaitersPool::new("shard_states_awaiters"),
            block_applying_awaiters: AwaitersPool::new("block_applying_awaiters"),
            next_block_applying_awaiters: AwaitersPool::new("next_block_applying_awaiters"),
            download_block_awaiters: AwaitersPool::new("download_block_awaiters"),
            external_messages: MessagesPool::new(),
            servers: lockfree::queue::Queue::new(),
            stop: Arc::new(AtomicU32::new(0)),
            zero_state_id,
            init_mc_block_id,
            initial_sync_disabled,
            archives_life_time,
            network: network.clone(),
            shard_blocks: shard_blocks_pool,
            last_known_mc_block_seqno: AtomicU32::new(0),
            last_known_keyblock_seqno: AtomicU32::new(0),
            will_validate: AtomicBool::new(false),
            test_bundles_config,
            shard_states_cache: TimeBasedCache::new(120, "shard_states_cache".to_string()),
            loaded_from_ss_cache: AtomicU64::new(0),
            loaded_ss_total: AtomicU64::new(0),
            workchain_id,
            state_gc_resolver,
            validation_status: lockfree::map::Map::new(),
            collation_status: lockfree::map::Map::new(),
            validated_block_stats_sender,
            validated_block_stats_receiver,
            #[cfg(feature = "telemetry")]
            full_node_telemetry: FullNodeTelemetry::new(),
            #[cfg(feature = "telemetry")]
            collator_telemetry: CollatorValidatorTelemetry::default(),
            #[cfg(feature = "telemetry")]
            validator_telemetry: CollatorValidatorTelemetry::default(),
            #[cfg(feature = "telemetry")]
            full_node_service_telemetry: FullNodeNetworkTelemetry::new(FullNodeNetworkTelemetryKind::Service),
            last_rotation_block_db,
        });

        save_top_shard_blocks_worker(engine.clone(), shard_blocks_receiver);

        Ok(engine)
    }

    pub async fn stop(&self) {
        let stopp = self.stop.fetch_or(Self::MASK_STOP, Ordering::Relaxed);
        log::info!(target: "sync", "Stop count {:04x}", stopp);
        while let Some(server) = self.servers.pop() {
            match server {
                Server::ControlServer(server) => {
                    log::info!(target: "sync", "Stop control server");
                    server.shutdown().await
                },
                Server::KafkaConsumer(trigger) => {
                    log::info!(target: "sync", "Stop kafka consumer");
                    drop(trigger)
                }
            }
        }
        loop {
            tokio::time::sleep(Duration::from_millis(Self::TIMEOUT_STOP)).await;
            let stopp = self.stop.load(Ordering::Relaxed) & !Self::MASK_STOP;
            log::info!(target: "sync", "Stop count update {:04x}", stopp);
            if (self.stop.load(Ordering::Relaxed) & !Self::MASK_STOP) == 0 {
                break
            }
        }    
        self.network.stop().await
    }

    pub fn acquire_stop(&self, mask: u32) {
        self.stop.fetch_or(mask, Ordering::Relaxed);
    }

    pub fn check_stop(&self) -> bool {
        (self.stop.load(Ordering::Relaxed) & Self::MASK_STOP) != 0 
    }

    pub fn release_stop(&self, mask: u32) {
        self.stop.fetch_and(!mask, Ordering::Relaxed);
    }

    pub fn register_server(&self, server: Server) {
        self.servers.push(server)
    }

    pub fn db(&self) -> &Arc<dyn InternalDb> { &self.db }

    pub fn validator_network(&self) -> Arc<dyn PrivateOverlayOperations> { self.network.clone() }

    pub fn network(&self) -> &NodeNetwork { &self.network }

    pub fn ext_db(&self) -> &Vec<Arc<dyn ExternalDb>> { &self.ext_db }

    pub fn zero_state_id(&self) -> &BlockIdExt { &self.zero_state_id }

    pub fn init_mc_block_id(&self) -> &BlockIdExt {&self.init_mc_block_id}

    pub fn initial_sync_disabled(&self) -> bool {self.initial_sync_disabled}

    pub fn shard_states_cache(&self) -> &TimeBasedCache<BlockIdExt, ShardStateStuff> {
        &self.shard_states_cache
    }

    pub fn update_shard_states_cache_stat(&self, loaded_from_cache: bool) {
        let loaded_ss_total = self.loaded_ss_total.fetch_add(1, Ordering::Relaxed) + 1;
        let loaded_from_ss_cache = if loaded_from_cache {
            self.loaded_from_ss_cache.fetch_add(1, Ordering::Relaxed) + 1
        } else {
            self.loaded_from_ss_cache.load(Ordering::Relaxed)
        };
        log::trace!("shard_states_cache  total loaded: {}  from cache: {}  use cache: {}%",
            loaded_ss_total, loaded_from_ss_cache, (100 * loaded_from_ss_cache) / loaded_ss_total);
    }

/*
    pub fn save_init_mc_block_id(&self, id: &BlockIdExt) -> Result<()> {
        self.db.save_node_state(INITIAL_MC_BLOCK, id)
    }
*/

    pub async fn get_masterchain_overlay(&self) -> Result<Arc<dyn FullNodeOverlayClient>> {
        self.get_full_node_overlay(ton_block::MASTERCHAIN_ID, ton_block::SHARD_FULL).await
    }

    pub async fn get_full_node_overlay(&self, workchain: i32, shard: u64) -> Result<Arc<dyn FullNodeOverlayClient>> {
        let id = self.overlay_operations.calc_overlay_id(workchain, shard)?;
        self.overlay_operations.clone().get_overlay(id).await
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

    pub fn download_block_awaiters(&self) -> &AwaitersPool<BlockIdExt, (BlockStuff, BlockProofStuff)> {
        &self.download_block_awaiters
    }

    pub fn external_messages(&self) -> &MessagesPool {
        &self.external_messages
    }

    pub fn shard_blocks(&self) -> &ShardBlocksPool {
        &self.shard_blocks
    }

    pub fn set_will_validate(&self, will_validate: bool) {
        self.will_validate.store(will_validate, Ordering::SeqCst);
    }

    pub fn will_validate(&self) -> bool {
        self.will_validate.load(Ordering::SeqCst)
    }

    pub fn update_last_known_mc_block_seqno(&self, seqno: u32) -> bool {
        self.last_known_mc_block_seqno.fetch_max(seqno, Ordering::SeqCst) < seqno
    }

    pub fn update_last_known_keyblock_seqno(&self, seqno: u32) -> bool {
        self.last_known_keyblock_seqno.fetch_max(seqno, Ordering::SeqCst) < seqno
    }

    pub fn test_bundles_config(&self) -> &CollatorTestBundlesGeneralConfig {
        &self.test_bundles_config
    }

    #[cfg(feature = "telemetry")]
    pub fn full_node_telemetry(&self) -> &FullNodeTelemetry {
        &self.full_node_telemetry
    }

    #[cfg(feature = "telemetry")]
    pub fn collator_telemetry(&self) -> &CollatorValidatorTelemetry {
        &self.collator_telemetry
    }

    #[cfg(feature = "telemetry")]
    pub fn validator_telemetry(&self) -> &CollatorValidatorTelemetry {
        &self.validator_telemetry
    }

    #[cfg(feature = "telemetry")]
    pub fn full_node_service_telemetry(&self) -> &FullNodeNetworkTelemetry {
        &self.full_node_service_telemetry
    }

    pub fn validation_status(&self) -> &lockfree::map::Map<ShardIdent, u64> {
        &self.validation_status
    }

    pub fn collation_status(&self) -> &lockfree::map::Map<ShardIdent, u64> {
        &self.collation_status
    }

    pub fn last_rotation_block_db(&self) -> &LastRotationBlockDb {
        &self.last_rotation_block_db
    }

    pub async fn download_and_apply_block_worker(
        self: Arc<Self>, 
        id: &BlockIdExt, 
        mc_seq_no: u32, 
        pre_apply: bool,
        recursion_depth: u32
    ) -> Result<()> {

        if recursion_depth > apply_block::MAX_RECURSION_DEPTH {
            fail!("Download and apply block {} - too deep recursion ({} >= {})",
                id, recursion_depth, apply_block::MAX_RECURSION_DEPTH);
        }

        loop {
            if let Some(handle) = self.load_block_handle(id)? {
                if handle.is_applied() || pre_apply && handle.has_state() {
                    log::trace!(
                        "download_and_apply_block_worker(pre_apply: {}): block is already applied {}",
                        pre_apply,
                        handle.id()
                    );
                    return Ok(());
                }
                if handle.has_data() {
                    while !((pre_apply && handle.has_state()) || handle.is_applied()) {
                        let s = self.clone();
                        self.block_applying_awaiters().do_or_wait(
                            handle.id(),
                            None,
                            async {
                                let block = s.load_block(&handle).await?;
                                s.apply_block_worker(&handle, &block, mc_seq_no, pre_apply, recursion_depth).await?;
                                Ok(())
                            }
                        ).await?;
                    }
                    return Ok(());
                }
            } 

            let now = std::time::Instant::now();
            log::trace!(
                "Start downloading block for {}apply... {}",
                if pre_apply { "pre-" } else { "" },
                id
            );
            // for pre-apply only 10 attempts, for apply - infinity
            let (attempts, timeout) = if pre_apply { 
                (Some(10), Some((50, 15, 500)))
            } else { 
                (None, None)
            };

            if let Some((block, proof)) = self.download_block_awaiters().do_or_wait(
                id,
                None,
                self.download_block_worker(id, attempts, timeout)
            ).await? {

                if self.load_block_handle(id)?.is_some() {
                    continue;
                }

                let downloading_time = now.elapsed().as_millis();

                let now = std::time::Instant::now();
                proof.check_proof(self.deref()).await?;
                let handle = self.store_block(&block).await?;
                let handle = if let Some(handle) = handle.as_non_created() {
                    handle
                } else {
                    continue
                };
                let handle = self.store_block_proof(id, Some(handle), &proof).await?;
                let handle = handle.as_non_created().ok_or_else(
                    || error!("INTERNAL ERROR: bad result for store block {} proof", id)
                )?;                    
                log::trace!(
                    "Downloaded block for {}apply {} TIME download: {}ms, check & save: {}", 
                    if pre_apply { "pre-" } else { "" }, 
                    block.id(),
                    downloading_time, 
                    now.elapsed().as_millis(),
                );
                self.apply_block(&handle, &block, mc_seq_no, pre_apply).await?;
                return Ok(())

            }
        }
    }

    pub async fn apply_block_worker(
        self: Arc<Self>,
        handle: &Arc<BlockHandle>,
        block: &BlockStuff,
        mc_seq_no: u32,
        pre_apply: bool,
        recursion_depth: u32
    ) -> Result<()> {
        if handle.is_applied() || pre_apply && handle.has_state() {
            log::trace!(
                "apply_block_worker(pre_apply: {}): block is already applied {}",
                pre_apply,
                handle.id()
            );
            return Ok(());
        }

        if recursion_depth > apply_block::MAX_RECURSION_DEPTH {
            fail!("Apply block {} - too deep recursion ({} >= {})",
                handle.id(), recursion_depth, apply_block::MAX_RECURSION_DEPTH);
        }

        log::trace!("Start {}applying block... {}", if pre_apply { "pre-" } else { "" }, block.id());

        // TODO fix trash with Arc and clone
        apply_block(handle, block, mc_seq_no, &(self.clone() as Arc<dyn EngineOperations>), pre_apply, recursion_depth).await?;

        let gen_utime = block.gen_utime()?;
        let ago = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?.as_secs() as i32 - gen_utime as i32;
        if block.id().shard().is_masterchain() {
            if !pre_apply {
                self.save_last_applied_mc_block_id(block.id())?;
                STATSD.gauge("last_applied_mc_block", block.id().seq_no() as f64);
                STATSD.gauge("timediff", ago as f64);
                self.shard_blocks().update_shard_blocks(&self.load_state(block.id()).await?)?;

                if self.set_applied(handle, mc_seq_no).await? {
                    #[cfg(feature = "telemetry")]
                    self.full_node_telemetry().submit_transactions(gen_utime as u64, block.calculate_tr_count()?);
                }

                let (prev_id, prev2_id_opt) = block.construct_prev_id()?;
                if prev2_id_opt.is_some() {
                    fail!("UNEXPECTED error: master block refers two previous blocks");
                }
                let id = handle.id().clone();
                self.next_block_applying_awaiters.do_or_wait(&prev_id, None, async move { Ok(id) }).await?;

                // Advance states GC
                let shard_client = self.load_shard_client_mc_block_id()?.ok_or_else(
                    || error!("INTERNAL ERROR: No shard client MC block id when apply block")
                )?;
                let pss_keeper = self.load_pss_keeper_mc_block_id()?.ok_or_else(
                    || error!("INTERNAL ERROR: No PSS keeper MC block id when apply block")
                )?;
                let mut min_id: &BlockIdExt = if shard_client.seq_no() < pss_keeper.seq_no() { 
                    &shard_client
                } else { 
                    &pss_keeper
                };
                let last_rotation_block_id = self.get_last_rotation_block_id()?;
                let mut last_rotation_block_id_str = "none".to_string();
                if let Some(id) = &last_rotation_block_id {
                    if min_id.seq_no() > id.seq_no() { 
                        min_id = &id 
                    }
                    last_rotation_block_id_str = format!("{}", id.seq_no())
                }
                log::trace!(
                    "Before state_gc_resolver.advance  shard_client {}  pss_keeper {}  last_rotation_block_id {}  min {}", 
                    shard_client.seq_no(),
                    pss_keeper.seq_no(),
                    last_rotation_block_id_str,
                    min_id.seq_no()
                );
                self.state_gc_resolver.advance(min_id, self.deref()).await?;
            }

            log::info!(
                "{} block {}, {} seconds old",
                if pre_apply { "Pre-applied" } else { "Applied" },
                block.id(),
                ago
            );
        } else {
            if !pre_apply {
                if self.set_applied(handle, mc_seq_no).await? {
                    #[cfg(feature = "telemetry")]
                    self.full_node_telemetry().submit_transactions(gen_utime as u64, block.calculate_tr_count()?);
                }
            }
            log::info!(
                "{} block {} ref_mc_block: {}, {} seconds old",
                if pre_apply { "Pre-applied" } else { "Applied" },
                block.id(),
                mc_seq_no,
                ago
            );
        }
        Ok(())
    }

    async fn listen_broadcasts(self: Arc<Self>, shard_ident: ShardIdent, mask: u32) -> Result<()> {
        log::debug!("Started listening overlay for shard {}", shard_ident);
        let client = self.get_full_node_overlay(
            shard_ident.workchain_id(),
            shard_ident.shard_prefix_with_tag()
        ).await?;
        tokio::spawn(async move {
            self.acquire_stop(mask);
            loop {
                if self.check_stop() {
                    break
                }
                match client.wait_broadcast().await {
                    Err(e) => log::error!("Error while wait_broadcast for shard {}: {}", shard_ident, e),
                    Ok((brodcast, src)) => {
                        match brodcast {
                            Broadcast::TonNode_BlockBroadcast(broadcast) => {
                                self.clone().process_block_broadcast(broadcast, src);
                            }
                            Broadcast::TonNode_ExternalMessageBroadcast(broadcast) => {
                                self.process_ext_msg_broadcast(broadcast, src);
                            }
                            Broadcast::TonNode_IhrMessageBroadcast(broadcast) => {
                                log::trace!("TonNode_IhrMessageBroadcast from {}: {:?}", src, broadcast);
                            }
                            Broadcast::TonNode_NewShardBlockBroadcast(broadcast) => {
                                self.clone().process_new_shard_block_broadcast(broadcast, src);
                            }
                            Broadcast::TonNode_ConnectivityCheckBroadcast(broadcast) => {
                                self.network.clone().process_connectivity_broadcast(broadcast);
                            }
                        }
                    }
                }
            }
            self.release_stop(mask);
        });
        Ok(())
    }

    pub fn validated_block_stats_sender(&self) -> &Sender<ValidatedBlockStat> { &self.validated_block_stats_sender }
    pub fn validated_block_stats_receiver(&self) -> &Receiver<ValidatedBlockStat> { &self.validated_block_stats_receiver }

    #[cfg(feature = "slashing")]
    async fn process_validated_block_stats_for_mc(&self, block_id: &BlockIdExt, signing_nodes: &[UInt256]) -> Result<()> {
        let block_handle = self.load_block_handle(&block_id)?.ok_or_else(|| error!("Cannot load handle for block {}", block_id))?;
        let mut is_link = false;
        if block_handle.has_proof_or_link(&mut is_link) {
            let (virt_block, _) = self.load_block_proof(&block_handle, is_link).await?.virtualize_block()?;
            let extra = virt_block.read_extra()?;
            let created_by = extra.created_by();
            self.process_validated_block_stats(block_id, signing_nodes, created_by).await
        } else {
            Ok(())
        }
    }

    #[cfg(feature = "slashing")]
    async fn process_validated_block_stats(&self, block_id: &BlockIdExt, signing_nodes: &[UInt256], created_by: &UInt256) -> Result<()> {
        let last_mc_state = self.load_last_applied_mc_state().await?;
        let (cur_validator_set, cc_config) = last_mc_state.read_cur_validator_set_and_cc_conf()?;
        let shard = block_id.shard();
        let cc_seqno = if shard.is_masterchain() {
            last_mc_state.shard_state_extra()?.validator_info.catchain_seqno
        } else {
            last_mc_state.shards()?.calc_shard_cc_seqno(shard)?
        };
        let (validators, _hash_short) = calc_subset_for_workchain(
            &cur_validator_set,
            last_mc_state.config_params()?,
            &cc_config,
            shard.shard_prefix_with_tag(),
            shard.workchain_id(),
            cc_seqno,
            Default::default())?;

        let commit_validators = signing_nodes.iter().collect::<HashSet<_>>();

        let mut validated_block_stat_nodes = Vec::new();

        for validator in &validators {
            let signed = commit_validators.contains(&validator.compute_node_id_short());
            let collated = &validator.public_key == created_by;
            let validated_block_stat_node = ValidatedBlockStatNode {
                public_key: validator.public_key.clone(),
                signed,
                collated,
            };

            validated_block_stat_nodes.push(validated_block_stat_node);
        }

        let validated_block_stat = ValidatedBlockStat {
            nodes : validated_block_stat_nodes,
        };

        self.push_validated_block_stat(validated_block_stat)?;

        Ok(())
    }

    fn process_block_broadcast(self: Arc<Self>, broadcast: Box<BlockBroadcast>, src: Arc<KeyId>) {
        // because of ALL blocks-broadcasts received in one task - spawn for each block
        log::trace!("Processing block broadcast {}", broadcast.id);
        let engine = self.clone() as Arc<dyn EngineOperations>;
        tokio::spawn(async move {
            if let Err(e) = process_block_broadcast(&engine, &broadcast).await {
                log::error!("Error while processing block broadcast {} from {}: {}", broadcast.id, src, e);
            } else {
                log::trace!("Processed block broadcast {} from {}", broadcast.id, src);

                #[cfg(feature = "slashing")]
                if let Ok(block_id) = convert_block_id_ext_api2blk(&broadcast.id) {
                    if block_id.shard().is_masterchain() {
                        let mut signing_nodes = Vec::new();
                        for api_sig in broadcast.signatures.iter() {
                            signing_nodes.push(UInt256::from(&api_sig.who.0));
                        }

                        if let Err(e) = self.process_validated_block_stats_for_mc(&block_id, &signing_nodes).await {
                            log::error!("Error while processing block broadcast stats {} from {}: {}", broadcast.id, src, e);        
                        } else {
                            log::trace!("Processed block broadcast stats {} from {}", broadcast.id, src);                            
                        }
                    }
                }
            }
        });
    }

    fn process_ext_msg_broadcast(&self, broadcast: Box<ExternalMessageBroadcast>, src: Arc<KeyId>) {
        // just add to list
        if !self.is_validator() {
            log::trace!(
                target: EXT_MESSAGES_TRACE_TARGET,
                "Skipped ext message broadcast {}bytes from {}: NOT A VALIDATOR",
                broadcast.message.data.0.len(), src
            );
        } else {
            log::trace!("Processing ext message broadcast {}bytes from {}", broadcast.message.data.0.len(), src);
            match self.external_messages().new_message_raw(&broadcast.message.data.0, self.now()) {
                Err(e) => {
                    log::error!(
                        target: EXT_MESSAGES_TRACE_TARGET,
                        "Error while processing ext message broadcast {}bytes from {}: {}",
                        broadcast.message.data.0.len(), src, e
                    );
                }
                Ok(id) => {
                    log::debug!(
                        target: EXT_MESSAGES_TRACE_TARGET,
                        "Processed ext message broadcast {:x} {}bytes from {} (added into collator's queue)",
                        id, broadcast.message.data.0.len(), src
                    );
                }
            }
        }
    }

    fn process_new_shard_block_broadcast(self: Arc<Self>, broadcast: Box<NewShardBlockBroadcast>, src: Arc<KeyId>) {
        let id = broadcast.block.block.clone();
        if self.is_validator() {
            log::trace!("Processing new shard block broadcast {} from {}", id, src);
            tokio::spawn(async move {
                if self.check_sync().await.unwrap_or(false) {
                    match self.clone().process_new_shard_block(broadcast).await {
                        Err(e) => {
                            log::error!("Error while processing new shard block broadcast {} from {}: {}", id, src, e);
                            #[cfg(feature = "telemetry")]
                            self.full_node_telemetry().bad_top_block_broadcast();
                        }
                        Ok(id) => {
                            log::trace!("Processed new shard block broadcast {} from {}", id, src);
                            #[cfg(feature = "telemetry")]
                            self.full_node_telemetry().good_top_block_broadcast(&id);
                        }
                    }
                } else {
                    log::trace!("Processing new shard block broadcast {} from {} NO SYNC", id, src);
                }
            });
        } else {
            log::trace!("Processing new shard block broadcast {} from {} NOT A VALIDATOR", id, src);
        }
    }

    async fn process_new_shard_block(
        self: Arc<Self>, 
        broadcast: Box<NewShardBlockBroadcast>
    ) -> Result<BlockIdExt> {
        let id = (&broadcast.block.block).try_into()?;
        let cc_seqno = broadcast.block.cc_seqno as u32;
        let data = broadcast.block.data.0;

        // check only
        let result = self.shard_blocks.process_shard_block_raw(&id, cc_seqno, data, false, true, self.deref()).await?;
        if let ShardBlockProcessingResult::MightBeAdded(tbd) = result {

            let (mc_seqno, _created_by) = tbd.top_block_mc_seqno_and_creator()?;
            let shard_client_mc_block_id = self.load_shard_client_mc_block_id()?.ok_or_else(
                || error!("INTERNAL ERROR: No shard client MC block set after boot")
            )?;
            if shard_client_mc_block_id.seq_no() + SHARD_BROADCAST_WINDOW < mc_seqno {
                log::debug!(
                    "Skipped new shard block broadcast {} because it refers to master block {}, but shard client is on {}",
                    id, mc_seqno, shard_client_mc_block_id.seq_no()
                );
                return Ok(id);
            }

            // fill stats for slashing
            #[cfg(feature = "slashing")]
            {
                let mut signing_nodes = Vec::new();
                if let Some(commit_signatures) = tbd.top_block_descr().signatures() {
                    use ton_types::HashmapType;
                    commit_signatures.pure_signatures.signatures().iterate_slices(|ref mut _key, ref mut slice| {
                        use ton_block::Deserializable;
                        let sign = ton_block::CryptoSignaturePair::construct_from(slice)?;
                        signing_nodes.push(sign.node_id_short.clone());
                        Ok(true)
                    })?;
                }

                if let Err(e) = self.process_validated_block_stats(&id, &signing_nodes, &_created_by).await {
                    log::error!("Error while processing shard block broadcast stats {}: {}", id, e);
                } else {
                    log::trace!("Processed shard block broadcast stats {}", id);
                }
            }
            // force download and apply afrer timeout
            // let id1 = id.clone();
            // let engine = self.clone();
            // tokio::spawn(async move {
            //     futures_timer::Delay::new(Duration::from_millis(10_000)).await;
            //     if let Err(e) = engine.download_and_apply_block(&id1, 0, true).await {
            //         log::error!("Error in download_and_apply_block after top-block-broadcast {}: {}", id1, e);
            //     }
            // });

            // add to list (for collator) only if shard state is avaliable
            let (_master, workchain_id) = self.processed_workchain().await?;
            let id = id.clone();
            tokio::spawn(async move {
                let mut result = true;
                // wait for state only for processed workchains
                if workchain_id == id.shard().workchain_id() {
                    if let Err(e) = self.clone().wait_state(&id, Some(10_000), false).await {
                        log::error!("Error in wait_state after top-block-broadcast false {}: {}", id, e);
                        if let Err(e) = self.clone().wait_state(&id, Some(10_000), true).await {
                            log::error!("Error in wait_state after top-block-broadcast true {}: {}", id, e);
                            result = false;
                        }
                    }
                }
                if result {
                    if let Err(e) = self.shard_blocks.process_shard_block(
                        &id, cc_seqno, || Ok(tbd.clone()), false, false, self.deref()).await {
                        log::error!("Error in process_shard_block after wait_state {}: {}", id, e);
                    }
                }
            });
        }
        Ok(id)              

    }                  

    async fn create_download_context<'a, T>(
         &'a self,
         downloader: Arc<dyn Downloader<Item = T>>,
         id: &'a BlockIdExt, 
         limit: Option<u32>,
         log_error_limit: u32,
         name: &'a str,
         timeout: Option<(u64, u64, u64)>
    ) -> Result<DownloadContext<'a, T>> {
        let ret = DownloadContext {
            client: self.get_full_node_overlay(
                id.shard().workchain_id(),
                id.shard().shard_prefix_with_tag()
            ).await?,
            db: self.db.deref(),
            downloader,
            id,
            limit,
            log_error_limit,
            name,
            timeout,
            #[cfg(feature = "telemetry")]
            full_node_telemetry: self.full_node_telemetry(),
        };
        Ok(ret)
    }   

    pub async fn download_next_block_worker(
        &self,
        prev_id: &BlockIdExt,
        limit: Option<u32>
    ) -> Result<(BlockStuff, BlockProofStuff)> {
        if !prev_id.is_masterchain() {
            fail!("download_next_block is allowed only for masterchain")
        }
        self.create_download_context(
            Arc::new(NextBlockDownloader),
            prev_id, 
            limit,
            10,
            "download_next_block_worker", 
            Some((50, 11, 1000))
        ).await?.download().await
    }

    pub async fn download_block_worker(
        &self,
        id: &BlockIdExt,
        limit: Option<u32>,
        timeout: Option<(u64, u64, u64)>
    ) -> Result<(BlockStuff, BlockProofStuff)> {
        self.create_download_context(
            Arc::new(BlockDownloader),
            id, 
            limit,
            0,
            "download_block_worker", 
            timeout
        ).await?.download().await
    }

    pub async fn download_block_proof_worker(
        &self,
        id: &BlockIdExt,
        is_link: bool,
        key_block: bool,
        limit: Option<u32>
    ) -> Result<BlockProofStuff> {
        self.create_download_context(
            Arc::new(
                BlockProofDownloader {
                    is_link, 
                    key_block
                }
            ),
            id, 
            limit,
            0,
            "download_block_proof_worker", 
            None
        ).await?.download().await
    }

    pub async fn download_zerostate_worker(
        &self,
        id: &BlockIdExt,
        limit: Option<u32>
    ) -> Result<(ShardStateStuff, Vec<u8>)> {
        self.create_download_context(
            Arc::new(ZeroStateDownloader),
            id, 
            limit,
            0,
            "download_zerostate_worker", 
            Some((10, 12, 3000))
        ).await?.download().await
    }

    pub(crate) async fn check_sync(&self) -> Result<bool> {

        let last_applied_mc_id = if let Some(id) = self.load_last_applied_mc_block_id()? {
            id
        } else {
            fail!("INTERNAL ERROR: No last applied MC block set after boot")
        };
        let shard_client_mc_id = if let Some(id) = self.load_shard_client_mc_block_id()? {
            id
        } else {
            fail!("INTERNAL ERROR: No shard client MC block set after boot")
        };
        if shard_client_mc_id.seq_no() + 16 < last_applied_mc_id.seq_no() {
            return Ok(false)
        }

        let last_mc_handle = self.load_block_handle(&last_applied_mc_id)?.ok_or_else(
            || error!("Cannot load handle for last masterchain block {}", last_applied_mc_id)
        )?;
        if last_mc_handle.gen_utime()? + 600 > self.now() {
            return Ok(true)
        }

        if self.last_known_keyblock_seqno.load(Ordering::Relaxed) > last_applied_mc_id.seq_no() {
            return Ok(false)
        }

        // experimental check. t-node doesn't have one
        //if self.last_known_mc_block_seqno.load(Ordering::Relaxed) > last_mc_id.seq_no() + 16 {
        //    return Ok(false);
        //}

        Ok(self.is_validator())
    }

// Unused temporarily
/* 
    pub fn start_gc_scheduler(self: Arc<Self>) -> tokio::task::JoinHandle<()>{
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
*/

    fn load_pss_keeper_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
        self.db().load_node_state(PSS_KEEPER_MC_BLOCK)
    }

    pub fn save_pss_keeper_mc_block_id(&self, id: &BlockIdExt) -> Result<()> {
        self.db().save_node_state(PSS_KEEPER_MC_BLOCK, id)
    }

    pub fn start_persistent_states_keeper(
        engine: Arc<Engine>,
        pss_keeper_block: BlockIdExt
    ) -> Result<tokio::task::JoinHandle<()>> {
        log::info!("start_persistent_states_keeper");
        let join_handle = tokio::spawn(async move {
            engine.acquire_stop(Engine::MASK_SERVICE_PSS_KEEPER);
            if let Err(e) = Self::persistent_states_keeper(&engine, pss_keeper_block).await {
                log::error!("FATAL!!! Unexpected error in persistent states keeper: {:?}", e);
            }
            engine.release_stop(Engine::MASK_SERVICE_PSS_KEEPER);
        });
        Ok(join_handle)
    }

    pub async fn persistent_states_keeper(
        engine: &Arc<Engine>,
        pss_keeper_block: BlockIdExt
    ) -> Result<()> {
        if !pss_keeper_block.shard().is_masterchain() {
            fail!("'pss_keeper_block' mast belong master chain");
        }
        let mut handle = engine.load_block_handle(&pss_keeper_block)?.ok_or_else(
            || error!("Cannot load handle for PSS keeper block {}", pss_keeper_block)
        )?;
        loop {
            if engine.check_stop() {
                break
            }
            let mc_state = engine.load_state(handle.id()).await?;
            if handle.is_key_block()? {
                let is_persistent_state = if handle.id().seq_no() == 0 {
                    false // zerostate are saved another way (see boot)
                } else {
                    if let Some(prev_key_block_id) =
                        mc_state.shard_state_extra()?.prev_blocks.get_prev_key_block(handle.id().seq_no() - 1)? {
                        let block_id = BlockIdExt {
                            shard_id: ShardIdent::masterchain(),
                            seq_no: prev_key_block_id.seq_no,
                            root_hash: prev_key_block_id.root_hash,
                            file_hash: prev_key_block_id.file_hash
                        };
                        let prev_handle = engine.load_block_handle(&block_id)?.ok_or_else(
                            || error!("Cannot load handle for PSS keeper prev key block {}", block_id)
                        )?;
                        engine.is_persistent_state(handle.gen_utime()?, prev_handle.gen_utime()?,
                            boot::PSS_PERIOD_BITS)
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
                    let (_master, workchain_id) = engine.processed_workchain().await?;
                    mc_state.shards()?.iterate_shards(|ident, descr| {
                        if ident.is_masterchain() || ident.workchain_id() == workchain_id {
                            shard_blocks.push(BlockIdExt {
                                shard_id: ident,
                                seq_no: descr.seq_no,
                                root_hash: descr.root_hash,
                                file_hash: descr.file_hash
                            });
                        }
                        Ok(true)
                    })?;
                    for block_id in shard_blocks {
                        if engine.check_stop() {
                            break
                        }
                        log::trace!("persistent_states_keeper: saving {}", block_id);
                        let now = std::time::Instant::now();
                        
                        let ss = engine.clone().wait_state(&block_id, None, false).await?;
                        let handle = engine.load_block_handle(&block_id)?.ok_or_else(
                            || error!("Cannot load handle for PSS keeper shard block {}", block_id)
                        )?;
                        engine.store_persistent_state_attempts(&handle, &ss).await;

                        log::trace!("persistent_states_keeper: saved {} TIME {}ms",
                            handle.id(), now.elapsed().as_millis());
                    };
                }
                if engine.check_stop() {
                    break
                }
                if let Err(e) = Self::check_gc_for_archives(&engine, &handle, &mc_state).await {
                    log::warn!("Error (archive_manager gc): {:?}", e);
                }
            }
            handle = engine.wait_next_applied_mc_block(&handle, None).await?.0;
            engine.save_pss_keeper_mc_block_id(handle.id())?;
        }
        Ok(())
    }

    async fn check_gc_for_archives(
        engine: &Arc<Engine>,
        curr_block_handle: &Arc<BlockHandle>,
        mc_state: &ShardStateStuff
    ) -> Result<()> {
        let mut prev_pss_block = None;
        let mut prev_prev_pss_block = None;
        let mut handle = curr_block_handle.clone();
        let mut check_date = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;

        match &engine.archives_life_time {
            None => return Ok(()),
            Some(life_time) => {
                match check_date.checked_sub(Duration::from_secs((life_time * 3600) as u64)) {
                    Some(date) => {
                        log::info!("archive gc: checked date {}.", &date.as_secs());
                        check_date = date
                    },
                    None => {
                        log::info!("archive gc: life_time in config is bad, actual checked date: {}",
                            &check_date.as_secs()
                        );
                    }
                }
            }
        }

        loop {
            if handle.id().seq_no() == 0 {
                return Ok(());
            }

            if let Some(prev_key_block_id) =
                mc_state.shard_state_extra()?.prev_blocks.get_prev_key_block(handle.id().seq_no() - 1)? {

                let block_id = BlockIdExt {
                    shard_id: ShardIdent::masterchain(),
                    seq_no: prev_key_block_id.seq_no,
                    root_hash: prev_key_block_id.root_hash,
                    file_hash: prev_key_block_id.file_hash
                };
                let prev_handle = engine.load_block_handle(&block_id)?.ok_or_else(
                    || error!("Cannot load handle for PSS keeper prev key block {}", block_id)
                )?;
                if engine.is_persistent_state(curr_block_handle.gen_utime()?, 
                   prev_handle.gen_utime()?, boot::PSS_PERIOD_BITS) {
                    prev_prev_pss_block = prev_pss_block;
                    prev_pss_block = Some(prev_handle.clone());
                }
                handle = prev_handle;

                if let Some(pss_block) = &prev_prev_pss_block {
                    let gen_time = pss_block.gen_utime()? as u64;
                    let check_date = check_date.as_secs();
                    if gen_time < check_date {
                        log::info!(
                            "gc for archives: found block (gen time: {}, seq_no: {}), check date: {}",
                            &gen_time, pss_block.id().seq_no(), &check_date
                        );
                        break;
                    }
                }
            } else {
                return Ok(());
            }

            if let Some(gc_marked_block) = &prev_prev_pss_block {
                log::info!("start gc for archives..");
                engine.db.archive_manager().gc(&gc_marked_block.id()).await;
                log::info!("finish gc for archives.");
                break;
            }
        }
        Ok(())
    }

    pub async fn store_persistent_state_attempts(&self, handle: &Arc<BlockHandle>, ss: &ShardStateStuff) {
        let mut attempts = 1;
        while let Err(e) = self.db.store_shard_state_persistent(handle, ss, None).await {
            log::error!("CRITICAL Error saving persistent state (attempt: {}): {:?}", attempts, e);
            if self.check_stop() {
                break
            }
            attempts += 1;
            futures_timer::Delay::new(Duration::from_millis(5000)).await;
        }
    }

}

pub(crate) async fn load_zero_state(engine: &Arc<Engine>, path: &str) -> Result<bool> {
    let zero_id = engine.zero_state_id();
    log::trace!("loading mc static zero state {} from path {}", zero_id, path);

    if let Some(handle) = engine.load_block_handle(zero_id)? {
        if handle.is_applied() {
            log::trace!("zero state already applied");
            return Ok(false)
        }
    }

    let (mc_zero_state, mc_zs_bytes) = {
        let path = format!("{}/{:x}.boc", path, zero_id.file_hash());
        let bytes = tokio::fs::read(&path).await
            .map_err(|err| error!("Cannot read mc zerostate {}: {}", path, err))?;
        (ShardStateStuff::deserialize_zerostate(zero_id.clone(), &bytes)?, bytes)
    };

    let workchains = mc_zero_state.workchains()?;
    for (wc_id, wc_info) in workchains {
        let id = BlockIdExt {
            shard_id: ShardIdent::with_tagged_prefix(wc_id, SHARD_FULL)?,
            seq_no: 0,
            root_hash: wc_info.zerostate_root_hash,
            file_hash: wc_info.zerostate_file_hash,
        };
        if let Some(handle) = engine.load_block_handle(&id)? {
            if handle.is_applied() {
                continue
            }
        }
        log::trace!("loading wc static zero state {}", id);
        let path = format!("{}/{:x}.boc", path, id.file_hash());
        let bytes = tokio::fs::read(&path).await
            .map_err(|err| error!("Cannot read zerostate {}: {}", path, err))?;
        let zs = ShardStateStuff::deserialize_zerostate(id.clone(), &bytes)?;
        let (_, handle) = engine.store_zerostate(zs, &bytes).await?;
        engine.set_applied(&handle, id.seq_no()).await?;
    }

    let (_, handle) = engine.store_zerostate(mc_zero_state, &mc_zs_bytes).await?;
    engine.set_applied(&handle, zero_id.seq_no()).await?;
    log::trace!("All static zero states had been load");
    return Ok(true)

}

async fn boot(engine: &Arc<Engine>, zerostate_path: Option<&str>) -> Result<(BlockIdExt, BlockIdExt, BlockIdExt)> {
    log::info!("Booting...");

    if let Some(zerostate_path) = zerostate_path {
        load_zero_state(&engine, zerostate_path).await?;
    }

    let result = match engine.load_last_applied_mc_block_id() {
        Ok(Some(id)) => crate::boot::warm_boot(engine.clone(), id).await,
        Ok(None) => Err(error!("No last applied MC block, warm boot is not possible")),
        Err(x) => Err(x)
    };
//    }/* else {
//        let res = engine.overlay_operations.get_peers_count(engine.zero_state_id()).await?;
//        if res == 0 { 
//            fail!("No nodes were found and no warm_boot");
//        }
//    }*/

    let (last_applied_mc_block, cold) = match result {
        Ok(id) => (id, false),
        Err(err) => {
            log::debug!("before cold boot: {}", err);
            let id = boot::cold_boot(engine.clone()).await?;
            engine.save_last_applied_mc_block_id(&id)?;
            (id, true)
        }
    };

    let shard_client_mc_block = match engine.load_shard_client_mc_block_id() {
        Ok(Some(id)) => id.deref().clone(),
        _ => {
            if !cold {
                fail!("INTERNAL ERROR: No shard client MC block in warm boot")
            }
            engine.save_shard_client_mc_block_id(&last_applied_mc_block)?;
            log::info!("Shard client MC block reset to last applied MC block");
            last_applied_mc_block.clone()
        }
    };

    let pss_keeper_mc_block = match engine.db().load_node_state(PSS_KEEPER_MC_BLOCK) {
        Ok(Some(id)) => id.deref().clone(),
        _ => {
            if !cold {
                fail!("INTERNAL ERROR: No PSS keeper MC block in warm boot")
            }
            engine.save_pss_keeper_mc_block_id(&last_applied_mc_block)?;
            log::info!("PSS keeper MC block reset to last applied MC block");
            last_applied_mc_block.clone()
        }
    };

    log::info!("Boot complete.");
    log::info!("LastMcBlockId: {}", last_applied_mc_block);
    log::info!("ShardsClientMcBlockId: {}", shard_client_mc_block);
    Ok((last_applied_mc_block, shard_client_mc_block, pss_keeper_mc_block))

}                              

async fn run_control_server(engine: Arc<Engine>, config: AdnlServerConfig) -> Result<ControlServer> {
    ControlServer::with_config(
        config,
        Some(Arc::clone(&engine) as Arc<dyn EngineOperations>),
        engine.network().config_handler(),
        engine.network().config_handler()
    ).await
}

pub async fn run(
    node_config: TonNodeConfig, 
    zerostate_path: Option<&str>, 
    ext_db: Vec<Arc<dyn ExternalDb>>, 
    initial_sync_disabled : bool
) -> Result<(Arc<Engine>, tokio::task::JoinHandle<()>)> {

    log::info!("Engine::run");

    let consumer_config = node_config.kafka_consumer_config();
    let control_server_config = node_config.control_server()?;

    // Create engine
    let engine = Engine::new(node_config, ext_db, initial_sync_disabled).await?;

    #[cfg(feature = "telemetry")]
    telemetry_logger(engine.clone());

    // Console service - run first to allow console to connect to generate new keys 
    // while node is looking for net
    if let Some(config) = control_server_config {
        engine.register_server(
            Server::ControlServer(run_control_server(engine.clone(), config).await?)
        )
    };

    // Messages from external DB (usually kafka)
    start_external_broadcast_process(engine.clone(), &consumer_config)?;

    let full_node_service = FullNodeOverlayService::new(Arc::clone(&engine) as Arc<dyn EngineOperations>);
    let full_node_service: Arc<dyn QueriesConsumer> = Arc::new(full_node_service);

    engine.get_full_node_overlay(MASTERCHAIN_ID, SHARD_FULL).await?;
    let network = engine.network();
    let overlay_id = network.calc_overlay_id(MASTERCHAIN_ID, SHARD_FULL)?.0;
    network.add_consumer(&overlay_id, full_node_service.clone())?;

    // Boot
    let (mut last_applied_mc_block, mut shard_client_mc_block, pss_keeper_block) = boot(&engine, zerostate_path).await?;

    let (master, workchain_id) = engine.processed_workchain().await?;
    log::info!("processed masterchain: {} workchain: {}", master, workchain_id);

    // Broadcasts (blocks, external messages etc.)
    Arc::clone(&engine).listen_broadcasts(
        ShardIdent::masterchain(),
        Engine::MASK_SERVICE_MASTERCHAIN_BROADCAST_LISTENER
    ).await?;

    Arc::clone(&engine).listen_broadcasts(
        ShardIdent::with_tagged_prefix(workchain_id, SHARD_FULL)?,
        Engine::MASK_SERVICE_SHARDCHAIN_BROADCAST_LISTENER
    ).await?;
    let overlay_id = network.calc_overlay_id(workchain_id, SHARD_FULL)?.0;
    network.add_consumer(&overlay_id, full_node_service.clone())?;
    engine.get_full_node_overlay(workchain_id, SHARD_FULL).await?;

    // Saving of persistent states (for sync)
    let _ = Engine::start_persistent_states_keeper(engine.clone(), pss_keeper_block)?;

    // Start validator manager, which will start validator sessions when necessary
    start_validator_manager(Arc::clone(&engine) as Arc<dyn EngineOperations>);

    // Sync by archives
    if !engine.check_sync().await? {
        // temporary remove sync with archives
        struct FakeSync;
        #[async_trait::async_trait]
        impl crate::sync::StopSyncChecker for FakeSync {
            async fn check(&self, _engine: &Arc<dyn EngineOperations>) -> bool { true }
        }
        crate::sync::start_sync(Arc::clone(&engine) as Arc<dyn EngineOperations>, Some(&FakeSync)).await?;
        last_applied_mc_block = engine.load_last_applied_mc_block_id()?.ok_or_else(
            || error!("INTERNAL ERROR: No last applied MC block after boot")
        )?.deref().clone();
        shard_client_mc_block = engine.load_shard_client_mc_block_id()?.ok_or_else(
            || error!("INTERNAL ERROR: No shard client MC block after boot")
        )?.deref().clone();
    }

    // top shard blocks
    resend_top_shard_blocks_worker(engine.clone());

    // blocks download clients
    let join_shards = start_shards_client(engine.clone(), shard_client_mc_block)?;
    let join_master = start_masterchain_client(engine.clone(), last_applied_mc_block)?;
    let join_engine = tokio::spawn(
        async move {
            let (_, _) = tokio::join!(join_master, join_shards);
        }
    );   
    Ok((engine, join_engine))

}

#[cfg(feature = "telemetry")]
fn telemetry_logger(engine: Arc<Engine>) {
    const TELEMETRY_TIMEOUT: u64 = 30;
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(TELEMETRY_TIMEOUT)).await;
            log::debug!(
                target: "telemetry",
                "Full node's telemetry:\n{}",
                engine.full_node_telemetry().report()
            );
            log::debug!(
                target: "telemetry",
                "Collator's telemetry:\n{}",
                engine.collator_telemetry().report()
            );
            log::debug!(
                target: "telemetry",
                "Validator's telemetry:\n{}",
                engine.validator_telemetry().report()
            );
            log::debug!(
                target: "telemetry",
                "Full node service's telemetry:\n{}",
                engine.full_node_service_telemetry().report(TELEMETRY_TIMEOUT)
            );
            log::debug!(
                target: "telemetry",
                "Full node client's telemetry:\n{}",
                engine.network.telemetry().report(TELEMETRY_TIMEOUT)
            );
        }
    });
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
            match KafkaConsumer::new(config, engine.clone()) {
                Ok(consumer) => {
                    engine.acquire_stop(Engine::MASK_SERVICE_KAFKA_CONSUMER);
                    match consumer.run().await {
                        Ok(_) => {},
                        Err(e) => { log::error!("Kafka listening is failed: {}", e)}
                    }
                    engine.release_stop(Engine::MASK_SERVICE_KAFKA_CONSUMER);
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
#[allow(dead_code)]
impl StatsdClient {
    pub fn new() -> StatsdClient { StatsdClient{} }
    pub fn gauge(&self, _metric_name: &str, _value: f64) {}
    pub fn incr(&self, _metric_name: &str) {}
    #[allow(dead_code)]
    pub fn histogram(&self, _metric_name: &str, _value: f64) {}
    #[cfg(feature = "external_db")]
    pub fn timer(&self, _metric_name: &str, _value: f64) {}
}

#[cfg(not(feature = "metrics"))]
lazy_static::lazy_static! {
    pub static ref STATSD: StatsdClient = StatsdClient::new();
}
