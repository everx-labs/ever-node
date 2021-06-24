use crate::{
    define_db_prop, 
    block::{convert_block_id_ext_api2blk, BlockStuff, BlockIdExtExtention},
    block_proof::BlockProofStuff,
    config::{TonNodeConfig, KafkaConsumerConfig, CollatorTestBundlesGeneralConfig},
    engine_traits::{ExternalDb, OverlayOperations, EngineOperations, PrivateOverlayOperations},
    full_node::{
        apply_block::{self, apply_block},
        shard_client::{
            process_block_broadcast, start_masterchain_client, start_shards_client,
            SHARD_BROADCAST_WINDOW
        },
    },
    internal_db::{InternalDb, InternalDbConfig, InternalDbImpl, NodeState},
    network::{
        full_node_client::FullNodeOverlayClient, control::ControlServer,
        full_node_service::FullNodeOverlayService
    },
    shard_state::ShardStateStuff,
    types::{awaiters_pool::AwaitersPool, lockfree_cache::TimeBasedCache},
    ext_messages::MessagesPool,
    validator::validator_manager,
    shard_blocks::{
        ShardBlocksPool, resend_top_shard_blocks_worker, save_top_shard_blocks_worker, ShardBlockProcessingResult
    },
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
    network::telemetry::FullNodeNetworkTelemetry,
};
use overlay::QueriesConsumer;
#[cfg(feature = "metrics")]
use statsd::client;
use std::{
    convert::TryInto, ops::Deref, sync::{Arc, atomic::{AtomicBool, AtomicU32, Ordering, AtomicU64}},
    time::Duration, mem::ManuallyDrop, collections::HashMap,
};
#[cfg(feature = "metrics")]
use std::env;
use storage::types::BlockHandle;
use ton_block::{
    self, ShardIdent, BlockIdExt, MASTERCHAIN_ID, SHARD_FULL, BASE_WORKCHAIN_ID, ShardDescr
};
use ton_types::{error, Result, fail};
use ton_api::ton::ton_node::{
    Broadcast, broadcast::{BlockBroadcast, ExternalMessageBroadcast, NewShardBlockBroadcast}
};
use adnl::{common::KeyId, server::AdnlServerConfig};

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
    download_block_awaiters: AwaitersPool<BlockIdExt, (BlockStuff, BlockProofStuff)>,
    external_messages: MessagesPool,

    zero_state_id: BlockIdExt,
    init_mc_block_id: BlockIdExt,
    initial_sync_disabled: bool,
    network: Arc<NodeNetwork>,
    shard_blocks: ShardBlocksPool,
    last_known_mc_block_seqno: AtomicU32,
    last_known_keyblock_seqno: AtomicU32,
    will_validate: AtomicBool,

    test_bundles_config: CollatorTestBundlesGeneralConfig,
 
    shard_states_cache: TimeBasedCache<BlockIdExt, ShardStateStuff>,
    loaded_from_ss_cache: AtomicU64,
    loaded_ss_total: AtomicU64,

    #[cfg(feature = "telemetry")]
    full_node_telemetry: FullNodeTelemetry,
    #[cfg(feature = "telemetry")]
    collator_telemetry: CollatorValidatorTelemetry,
    #[cfg(feature = "telemetry")]
    validator_telemetry: CollatorValidatorTelemetry,
    #[cfg(feature = "telemetry")]
    full_node_service_telemetry: FullNodeNetworkTelemetry,
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
                Ok(None) => self.log("got no_data", attempt),
                Ok(Some(ret)) => break Ok(ret)
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
    ) -> Result<Option<Self::Item>>;
}

struct BlockDownloader;

#[async_trait::async_trait]
impl Downloader for BlockDownloader {
    type Item = (BlockStuff, BlockProofStuff);
    async fn try_download(
        &self, 
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Option<Self::Item>> {
        if let Some(handle) = context.db.load_block_handle(context.id)? {
            let mut is_link = false;
            if handle.has_data() && handle.has_proof_or_link(&mut is_link) {
                return Ok(Some((
                    context.db.load_block_data(&handle).await?,
                    context.db.load_block_proof(&handle, is_link).await?
                )));
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
    ) -> Result<Option<Self::Item>> {
        if let Some(handle) = context.db.load_block_handle(context.id)? {
            let mut is_link = false;
            if handle.has_proof_or_link(&mut is_link) {
                return Ok(Some(context.db.load_block_proof(&handle, is_link).await?));
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
    ) -> Result<Option<Self::Item>> {
        if let Some(prev_handle) = context.db.load_block_handle(context.id)? {
            if prev_handle.has_next1() {
                let next_id = context.db.load_block_next1(context.id)?;
                if let Some(next_handle) = context.db.load_block_handle(&next_id)? {
                    let mut is_link = false;
                    if next_handle.has_data() && next_handle.has_proof_or_link(&mut is_link) {
                        return Ok(Some((
                            context.db.load_block_data(&next_handle).await?,
                            context.db.load_block_proof(&next_handle, is_link).await?
                        )));
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
    ) -> Result<Option<Self::Item>> {
        if let Some(handle) = context.db.load_block_handle(context.id)? {
            if handle.has_state() {
                let zs = context.db.load_shard_state_dynamic(context.id)?;
                let mut data = vec!();
                zs.write_to(&mut data)?;
                return Ok(Some((zs, data)));
            }
        }
        context.client.download_zero_state(context.id).await
    }
}

impl Engine {

    pub async fn new(general_config: TonNodeConfig, ext_db: Vec<Arc<dyn ExternalDb>>, initial_sync_disabled : bool) -> Result<Arc<Self>> {

         log::info!("Creating engine...");

        let db_directory = general_config.internal_db_path().unwrap_or_else(|| {"node_db"});
        let db_config = InternalDbConfig { db_directory: db_directory.to_string() };
        let db = Arc::new(InternalDbImpl::new(db_config).await?);
        let global_config = general_config.load_global_config()?;
        let test_bundles_config = general_config.test_bundles_config().clone();
        let zero_state_id = global_config.zero_state().expect("check zero state settings");
        let mut init_mc_block_id = global_config.init_block()?.unwrap_or_else(|| zero_state_id.clone());
        if let Ok(block_id) = InitMcBlockId::load_from_db(db.deref()) {
            if block_id.0.seqno > init_mc_block_id.seq_no as i32 {
                init_mc_block_id = convert_block_id_ext_api2blk(&block_id.0)?;
            }
        }

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
        let last_mc_seqno = LastMcBlockId::load_from_db(db.deref())
            .map(|id| id.0.seqno as u32).unwrap_or_default();
        let (shard_blocks_pool, shard_blocks_receiver) = 
            ShardBlocksPool::new(shard_blocks, last_mc_seqno, false);

        log::info!("Engine is created.");

        let engine = Arc::new(Engine {
            db,
            ext_db,
            overlay_operations: network.clone(),
            shard_states_awaiters: AwaitersPool::new("shard_states_awaiters"),
            block_applying_awaiters: AwaitersPool::new("block_applying_awaiters"),
            next_block_applying_awaiters: AwaitersPool::new("next_block_applying_awaiters"),
            download_block_awaiters: AwaitersPool::new("download_block_awaiters"),
            external_messages: MessagesPool::new(),
            zero_state_id,
            init_mc_block_id,
            initial_sync_disabled,
            network: network.clone(),
            shard_blocks: shard_blocks_pool,
            last_known_mc_block_seqno: AtomicU32::new(0),
            last_known_keyblock_seqno: AtomicU32::new(0),
            will_validate: AtomicBool::new(false),
            test_bundles_config,
            shard_states_cache: TimeBasedCache::new(120, "shard_states_cache".to_string()),
            loaded_from_ss_cache: AtomicU64::new(0),
            loaded_ss_total: AtomicU64::new(0),
            #[cfg(feature = "telemetry")]
            full_node_telemetry: FullNodeTelemetry::new(),
            #[cfg(feature = "telemetry")]
            collator_telemetry: CollatorValidatorTelemetry::default(),
            #[cfg(feature = "telemetry")]
            validator_telemetry: CollatorValidatorTelemetry::default(),
            #[cfg(feature = "telemetry")]
            full_node_service_telemetry: FullNodeNetworkTelemetry::default(),
        });

        save_top_shard_blocks_worker(engine.clone(), shard_blocks_receiver);

        engine.get_full_node_overlay(BASE_WORKCHAIN_ID, SHARD_FULL).await?;

        Ok(engine)
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

    pub fn set_init_mc_block_id(&self, init_mc_block_id: &BlockIdExt) {
        InitMcBlockId(
            ton_api::ton::ton_node::blockidext::BlockIdExt::from(init_mc_block_id)
        ).store_to_db(self.db().deref()).ok();
    }

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
                let handle = self.store_block(&block).await?.handle;
                let handle = self.store_block_proof(id, Some(handle), &proof).await?;

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
                self.store_last_applied_mc_block_id(block.id()).await?;
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

    async fn listen_broadcasts(self: Arc<Self>, shard_ident: ShardIdent) -> Result<()> {
        log::debug!("Started listening overlay for shard {}", shard_ident);
        let client = self.get_full_node_overlay(
            shard_ident.workchain_id(),
            shard_ident.shard_prefix_with_tag()
        ).await?;
        tokio::spawn(async move {
            loop {
                match client.wait_broadcast().await {
                    Err(e) => log::error!("Error while wait_broadcast for shard {}: {}", shard_ident, e),
                    Ok((brodcast, src)) => {
                        match brodcast {
                            Broadcast::TonNode_BlockBroadcast(broadcast) => {
                                self.clone().process_block_broadcast(broadcast, src);
                            },
                            Broadcast::TonNode_ExternalMessageBroadcast(broadcast) => {
                                self.process_ext_msg_broadcast(broadcast, src);
                            },
                            Broadcast::TonNode_IhrMessageBroadcast(broadcast) => {
                                log::trace!("TonNode_IhrMessageBroadcast from {}: {:?}", src, broadcast);
                            },
                            Broadcast::TonNode_NewShardBlockBroadcast(broadcast) => {
                                self.clone().process_new_shard_block_broadcast(broadcast, src);
                            },
                            Broadcast::TonNode_ConnectivityCheckBroadcast(broadcast) => {
                                self.network.clone().process_connectivity_broadcast(broadcast);
                            },
                        }
                    }
                }
            }
        });
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
            }
        });
    }

    fn process_ext_msg_broadcast(&self, broadcast: Box<ExternalMessageBroadcast>, src: Arc<KeyId>) {
        // just add to list
        if !self.is_validator() {
            log::trace!("Skipped ext message broadcast {}bytes from {}: NOT A VALIDATOR",
                broadcast.message.data.0.len(), src);
        } else {
            log::trace!("Processing ext message broadcast {}bytes from {}", broadcast.message.data.0.len(), src);
            match self.external_messages().new_message_raw(&broadcast.message.data.0, self.now()) {
                Err(e) => log::debug!("Error while processing ext message broadcast {}bytes from {}: {}",
                    broadcast.message.data.0.len(), src, e),
                Ok(id) => log::trace!("Processed ext message broadcast {:x} {}bytes from {}",
                    id, broadcast.message.data.0.len(), src),
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
        if let ShardBlockProcessingResult::MightBeAdded(tbd) = 
            self.shard_blocks.process_shard_block_raw(&id, cc_seqno, data, false, true, self.deref()).await? {

            let mc_seqno = tbd.top_block_mc_seqno()?;
            let shards_client_mc_block_id = self.load_shards_client_mc_block_id().await?;
            if shards_client_mc_block_id.seq_no() + SHARD_BROADCAST_WINDOW < mc_seqno {
                log::debug!(
                    "Skipped new shard block broadcast {} because it refers to master block {}, but shard client is on {}",
                    id, mc_seqno, shards_client_mc_block_id.seq_no()
                );
                return Ok(id);
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

            // add to list (for collator) only if shard state is awaliable
            let id = id.clone();
            tokio::spawn(async move {
                if let Err(e) = self.clone().wait_state(&id, Some(10_000), false).await.or(
                    self.clone().wait_state(&id, Some(10_000), true).await
                ) {
                    log::error!("Error in wait_state after top-block-broadcast {}: {}", id, e);
                } else {
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

        let shards_client = self.load_shards_client_mc_block_id().await?;
        let last_mc_id = self.load_last_applied_mc_block_id().await?;
        if shards_client.seq_no() + 16 < last_mc_id.seq_no() {
            return Ok(false);
        }

        let last_mc_handle = self.load_block_handle(&last_mc_id)?.ok_or_else(
            || error!("Cannot load handle for last masterchain block {}", last_mc_id)
        )?;
        if last_mc_handle.gen_utime()? + 600 > self.now() {
            return Ok(true);
        }

        if self.last_known_keyblock_seqno.load(Ordering::Relaxed) > last_mc_id.seq_no() {
            return Ok(false);
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

    async fn store_pss_keeper_block_id(&self, id: &BlockIdExt) -> Result<()> {
        PssKeeperBlockId(id.into()).store_to_db(self.db().deref())
    }

    pub fn start_persistent_states_keeper(
        engine: Arc<Engine>,
        pss_keeper_block: BlockIdExt
    ) -> Result<tokio::task::JoinHandle<()>> {
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
        let mut handle = engine.load_block_handle(&pss_keeper_block)?.ok_or_else(
            || error!("Cannot load handle for PSS keeper block {}", pss_keeper_block)
        )?;
        loop {
            if handle.is_key_block()? {
                let mc_state = engine.load_state(handle.id()).await?;
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
                        
                        let ss = engine.clone().wait_state(&block_id, None, false).await?;
                        let handle = engine.load_block_handle(&block_id)?.ok_or_else(
                            || error!("Cannot load handle for PSS keeper shard block {}", block_id)
                        )?;
                        engine.store_persistent_state_attempts(&handle, &ss).await;

                        log::trace!("persistent_states_keeper: saved {} TIME {}ms",
                            handle.id(), now.elapsed().as_millis());
                    };
                }
            }
            handle = engine.wait_next_applied_mc_block(&handle, None).await?.0;
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

pub(crate) async fn load_zero_state(engine: &Arc<Engine>, path: &str) -> Result<bool> {
 
    let zero_id = engine.zero_state_id();
    if let Some(handle) = engine.load_block_handle(zero_id)? {
        if handle.is_applied() {
            return Ok(false);
        }
    }

    let (mc_zero_state, mc_zs_bytes) = {
        log::trace!("loading mc static zero state {}", zero_id);
        let path = format!("{}/{:x}.boc", path, zero_id.file_hash());
        let bytes = tokio::fs::read(&path).await
            .map_err(|err| error!("Cannot read mc zerostate {}: {}", path, err))?;
        (ShardStateStuff::deserialize_zerostate(zero_id.clone(), &bytes)?, bytes)
    };

    let mut zerostates_ids = vec!();
    mc_zero_state.config_params()?.workchains()?.iterate_with_keys(|wc_id: i32, wc_info| {
        zerostates_ids.push(BlockIdExt {
            shard_id: ShardIdent::with_tagged_prefix(wc_id, SHARD_FULL)?,
            seq_no: 0,
            root_hash: wc_info.zerostate_root_hash,
            file_hash: wc_info.zerostate_file_hash,
        });
        Ok(true)
    })?;

    for id in &zerostates_ids {
        if let Some(handle) = engine.load_block_handle(id)? {
            if handle.is_applied() {
                continue
            }
        }
        log::trace!("loading wc static zero state {}", id);
        let path = format!("{}/{:x}.boc", path, id.file_hash());
        let bytes = tokio::fs::read(&path).await
            .map_err(|err| error!("Cannot read zerostate {}: {}", path, err))?;
        let zs = ShardStateStuff::deserialize_zerostate(id.clone(), &bytes)?;
        let handle = engine.store_zerostate(id, &zs, &bytes).await?;
        engine.set_applied(&handle, id.seq_no()).await?;
    }

    let handle = engine.store_zerostate(&zero_id, &mc_zero_state, &mc_zs_bytes).await?;
    engine.set_applied(&handle, zero_id.seq_no()).await?;
    log::trace!("All static zero states had been load");
    return Ok(true)

}

async fn boot(engine: &Arc<Engine>, zerostate_path: Option<&str>) -> Result<(BlockIdExt, BlockIdExt, BlockIdExt)> {
    log::info!("Booting...");

    if let Some(zerostate_path) = zerostate_path {
        load_zero_state(engine, zerostate_path).await?;
    }

    let mut result = LastMcBlockId::load_from_db(engine.db().deref())
        .and_then(|id| (&id.0).try_into());
    if let Ok(id) = result {
        result = crate::boot::warm_boot(engine.clone(), id).await;
    }/* else {
        let res = engine.overlay_operations.get_peers_count(engine.zero_state_id()).await?;
        if res == 0 { 
            fail!("No nodes were found and no warm_boot");
        }
    }*/

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

fn run_full_node_service(engine: Arc<Engine>) -> Result<Arc<FullNodeOverlayService>> {
    let full_node_service = Arc::new(
        FullNodeOverlayService::new(Arc::clone(&engine) as Arc<dyn EngineOperations>)
    );
    let network = engine.network();
    network.add_consumer(
        &network.calc_overlay_id(MASTERCHAIN_ID, SHARD_FULL)?.0,
        full_node_service.clone() as Arc<dyn QueriesConsumer>
    )?;
    network.add_consumer(
        &network.calc_overlay_id(BASE_WORKCHAIN_ID, SHARD_FULL)?.0,
        full_node_service.clone() as Arc<dyn QueriesConsumer>
    )?;

    Ok(full_node_service)
}

async fn run_control_server(engine: Arc<Engine>, config: AdnlServerConfig) -> Result<ControlServer> {
    ControlServer::with_config(
        config,
        Some(Arc::clone(&engine) as Arc<dyn EngineOperations>),
        engine.network().config_handler(),
        engine.network().config_handler()
    ).await
}

pub async fn run(node_config: TonNodeConfig, zerostate_path: Option<&str>, ext_db: Vec<Arc<dyn ExternalDb>>, initial_sync_disabled : bool) -> Result<()> {
    log::info!("Engine::run");

    let consumer_config = node_config.kafka_consumer_config();
    let control_server_config = node_config.control_server()?;

    // Create engine
    let engine = Engine::new(node_config, ext_db, initial_sync_disabled).await?;

    #[cfg(feature = "telemetry")]
    telemetry_logger(engine.clone());

    // Full node's service
    let _ = run_full_node_service(engine.clone())?;

    // Console service
    if let Some(config) = control_server_config {
        let control_server = run_control_server(engine.clone(), config).await?;
        // Asking the compiler not to drop `control_server`, despite we don't have any link to it.
        let _ = ManuallyDrop::new(control_server);
    };

    // Boot
    let (mut last_mc_block, mut shards_client_mc_block, pss_keeper_block) = boot(&engine, zerostate_path).await?;

    // Messages from external DB (usually kafka)
    start_external_broadcast_process(engine.clone(), &consumer_config)?;

    // Broadcasts (blocks, external messages etc.)
    Arc::clone(&engine).listen_broadcasts(ShardIdent::masterchain()).await?;
    Arc::clone(&engine).listen_broadcasts(ShardIdent::with_tagged_prefix(BASE_WORKCHAIN_ID, SHARD_FULL)?).await?;

    // Saving of persistenr states (for sync)
    let _ = Engine::start_persistent_states_keeper(engine.clone(), pss_keeper_block)?;

    // Start validator manager, which will start validator sessions when necessary
    start_validator(Arc::clone(&engine));

    // Sync by archives
    if !engine.check_sync().await? {
        crate::sync::start_sync(Arc::clone(&engine) as Arc<dyn EngineOperations>).await?;
        last_mc_block = LastMcBlockId::load_from_db(engine.db().deref())
            .and_then(|id| (&id.0).try_into())?;
        shards_client_mc_block = ShardsClientMcBlockId::load_from_db(engine.db().deref())
            .and_then(|id| (&id.0).try_into())?;
    }

    // top shard blocks
    resend_top_shard_blocks_worker(engine.clone());

    // blocks download clients
    let _ = start_shards_client(engine.clone(), shards_client_mc_block)?;
    let _ = start_masterchain_client(engine, last_mc_block)?.await;

    Ok(())
}

fn start_validator(engine: Arc<Engine>) {
    const CHECK_VALIDATOR_TIMEOUT: u64 = 60;    //secs
    tokio::spawn(async move {
        loop {
            if engine.network.get_validator_status() {
                log::info!("starting validator...");
                if let Err(e) = validator_manager::start_validator_manager(engine.clone()) {
                    log::error!("{:?}", e);
                }
                return;
            }
            tokio::time::sleep(Duration::from_secs(CHECK_VALIDATOR_TIMEOUT)).await;
        }
    });
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
