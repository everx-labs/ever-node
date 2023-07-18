/*
* Copyright (C) 2019-2023 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific TON DEV software governing permissions and
* limitations under the License.
*/

use crate::{
    block::{BlockStuff, BlockIdExtExtention}, block_proof::BlockProofStuff, boot,
    config::{
        TonNodeConfig, KafkaConsumerConfig, CollatorTestBundlesGeneralConfig, 
        ValidatorManagerConfig, CollatorConfig
    },
    engine_traits::{
        ExternalDb, EngineAlloc, EngineOperations,
        OverlayOperations, PrivateOverlayOperations, Server,
    },
    ext_messages::{MessagesPool, EXT_MESSAGES_TRACE_TARGET, RempMessagesPool},
    full_node::{
        apply_block::{self, apply_block},
        shard_client::{
            process_block_broadcast, start_masterchain_client, start_shards_client,
            SHARD_BROADCAST_WINDOW, apply_proof_chain,
        },
        counters::TpsCounter,
        remp_client::RempClient,
    },
    internal_db::{
        InternalDb, InternalDbConfig, 
        INITIAL_MC_BLOCK, LAST_APPLIED_MC_BLOCK, PSS_KEEPER_MC_BLOCK, ARCHIVES_GC_BLOCK,
    },
    network::{
        control::{ControlServer, DataSource, StatusReporter},
        full_node_client::FullNodeOverlayClient, full_node_service::FullNodeOverlayService,
        node_network::NodeNetwork
    },
    shard_blocks::{
        ShardBlocksPool, resend_top_shard_blocks_worker, save_top_shard_blocks_worker, 
        ShardBlockProcessingResult
    },
    shard_state::ShardStateStuff,
    shard_states_keeper::ShardStatesKeeper,
    types::awaiters_pool::AwaitersPool,
    validator::{
        remp_service::RempService,
        validator_manager::{start_validator_manager, ValidationStatus},
    }
};
#[cfg(feature = "external_db")]
use crate::external_db::kafka_consumer::KafkaConsumer;
#[cfg(feature = "slashing")]
use crate::validator::{
    slashing::{ValidatedBlockStat, ValidatedBlockStatNode},
    validator_utils::calc_subset_for_workchain,
};
#[cfg(feature = "telemetry")]
use crate::{
    engine_traits::EngineTelemetry, full_node::telemetry::{FullNodeTelemetry, RempClientTelemetry},
    network::telemetry::{FullNodeNetworkTelemetry, FullNodeNetworkTelemetryKind},
    validator::telemetry::{CollatorValidatorTelemetry, RempCoreTelemetry},
};

#[cfg(feature = "telemetry")]
use adnl::telemetry::{Metric, MetricBuilder, TelemetryItem, TelemetryPrinter};
use overlay::QueriesConsumer;
use std::{
    ops::Deref, sync::{Arc, atomic::{AtomicBool, AtomicU8, AtomicU32, Ordering, AtomicU64}},
    time::Duration, collections::{HashMap, HashSet},
};
#[cfg(feature = "metrics")]
use std::env;
#[cfg(feature = "slashing")]
use std::collections::HashSet;
use storage::{StorageAlloc, block_handle_db::BlockHandle};
#[cfg(feature = "telemetry")]
use storage::types::StorageCell;
#[cfg(feature = "telemetry")]
use storage::StorageTelemetry;
use ton_api::ton::ton_node::{
    Broadcast, 
    broadcast::{
        BlockBroadcast, QueueUpdateBroadcast, ExternalMessageBroadcast, NewShardBlockBroadcast
    }
};
use ton_block::{
    self, ShardIdent, BlockIdExt, MASTERCHAIN_ID, SHARD_FULL, BASE_WORKCHAIN_ID, GlobalCapabilities,
    OutMsgQueue
};
use ton_types::{error, fail, KeyId, Result, UInt256};
#[cfg(feature = "slashing")]
use ton_types::HashmapType;
#[cfg(feature = "telemetry")]
use ton_types::Cell;

#[cfg(feature = "slashing")]
//maximum number of validated block stats entries in engine's queue
const MAX_VALIDATED_BLOCK_STATS_ENTRIES_COUNT: usize = 10000; 

pub struct Engine {
    db: Arc<InternalDb>,
    ext_db: Vec<Arc<dyn ExternalDb>>,
    overlay_operations: Arc<dyn OverlayOperations>,
    shard_states_awaiters: AwaitersPool<BlockIdExt, Arc<ShardStateStuff>>,
    block_applying_awaiters: AwaitersPool<BlockIdExt, ()>,
    next_block_applying_awaiters: AwaitersPool<BlockIdExt, BlockIdExt>,
    download_block_awaiters: AwaitersPool<BlockIdExt, (BlockStuff, BlockProofStuff)>,
    download_queue_update_awaiters: AwaitersPool<BlockIdExt, BlockStuff>,
    external_messages: MessagesPool,
    servers: lockfree::queue::Queue<Server>,
    stopper: Arc<Stopper>,

    remp_client: Option<Arc<RempClient>>,
    remp_service: Option<Arc<RempService>>,
    remp_messages: Option<Arc<RempMessagesPool>>,

    zero_state_id: BlockIdExt,
    init_mc_block_id: BlockIdExt,
    initial_sync_disabled: bool,
    pub network: Arc<NodeNetwork>,
    archives_life_time: Option<u32>,
    // enable_shard_state_persistent_gc: bool,
    shard_blocks: ShardBlocksPool,
    last_known_mc_block_seqno: AtomicU32,
    last_known_keyblock_seqno: AtomicU32,
    will_validate: AtomicBool,
    sync_status: AtomicU32,
    low_memory_mode: bool,
    remp_capability: AtomicBool,

    test_bundles_config: CollatorTestBundlesGeneralConfig,
    collator_config: CollatorConfig,
 
    shard_states_keeper: Arc<ShardStatesKeeper>,
    processed_workchain: Option<i32>,

    // None - queue calculating is in progress
    split_queues_cache: lockfree::map::Map<BlockIdExt, Option<(OutMsgQueue, OutMsgQueue, HashSet<UInt256>)>>,
    validation_status: Arc<AtomicU8>,
    last_validation_time: lockfree::map::Map<ShardIdent, u64>,
    last_collation_time: lockfree::map::Map<ShardIdent, u64>,
    #[cfg(feature = "slashing")]
    validated_block_stats_sender: crossbeam_channel::Sender<ValidatedBlockStat>,
    #[cfg(feature = "slashing")]
    validated_block_stats_receiver: crossbeam_channel::Receiver<ValidatedBlockStat>,

    #[cfg(feature = "telemetry")]
    full_node_telemetry: FullNodeTelemetry,
    #[cfg(feature = "telemetry")]
    remp_core_telemetry: Arc<RempCoreTelemetry>,
    #[cfg(feature = "telemetry")]
    collator_telemetry: CollatorValidatorTelemetry,
    #[cfg(feature = "telemetry")]
    validator_telemetry: CollatorValidatorTelemetry,
    #[cfg(feature = "telemetry")]
    full_node_service_telemetry: FullNodeNetworkTelemetry,
    #[cfg(feature = "telemetry")]
    engine_telemetry: Arc<EngineTelemetry>,
    engine_allocated: Arc<EngineAlloc>,
    #[cfg(feature = "telemetry")]
    telemetry_printer: TelemetryPrinter,
    #[cfg(feature = "telemetry")]
    remp_client_telemetry: RempClientTelemetry,

    tps_counter: TpsCounter,

    pub out_queues_cache: std::sync::Mutex<std::collections::HashMap<ShardIdent, ton_block::OutMsgQueue>>,
}

struct DownloadContext<'a, T> {
    engine: &'a Engine,
    client: Arc<dyn FullNodeOverlayClient>,
    downloader: Arc<dyn Downloader<Item = T>>,
    id: &'a BlockIdExt,
    limit: Option<u32>,
    log_error_limit: u32,
    name: &'a str,
    timeout: Option<(u64, u64, u64)>, // (current, multiplier*10, max)
}

impl <T> DownloadContext<'_, T> {

    async fn download(&mut self) -> Result<T> {
        let mut attempt = 1;
        loop {
            if self.engine.check_stop() {
                fail!("{} id: {}, stop flag was set", self.name, self.id);
            }
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
        if let Some(handle) = context.engine.db.load_block_handle(context.id)? {
            let mut is_link = false;
            if handle.has_data() && handle.has_proof_or_link(&mut is_link) {
                let block = match context.engine.db.load_block_data(&handle).await {
                    Err(e) => if !handle.has_data() {
                        None
                    } else {
                        return Err(e)
                    },
                    Ok(block) => Some(block)
                };
                let proof = if block.is_none() {
                    None
                } else {
                    match context.engine.db.load_block_proof(&handle, is_link).await {
                        Err(e) => if is_link && !handle.has_proof_link() {
                            None
                        } else if !is_link && !handle.has_proof() {
                            None
                        } else {
                            return Err(e)
                        },
                        Ok(proof) => Some(proof)
                    }
                };
                if let Some(block) = block {
                    if let Some(proof) = proof {
                        return Ok((block, proof))
                    }
                }
            }
        }
        #[cfg(feature = "telemetry")]
        context.engine.full_node_telemetry.new_downloading_block_attempt(context.id);
        let ret = context.client.download_block_full(context.id).await;
        #[cfg(feature = "telemetry")]
        if ret.is_ok() { 
            context.engine.full_node_telemetry.new_downloaded_block(context.id);
        }
        ret
    }
}

struct QueueUpdateDownloader {
    update_for_wc: i32,
}

#[async_trait::async_trait]
impl Downloader for QueueUpdateDownloader {
    type Item = BlockStuff;
    async fn try_download(
        &self, 
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Self::Item> {
        if let Some(handle) = context.engine.db.load_block_handle(context.id)? {
            if handle.has_data() && handle.is_queue_update() {
                match context.engine.db.load_block_data(&handle).await {
                    Ok(block) => return Ok(block),
                    Err(e) if handle.has_data() => return Err(e),
                    _ => ()
                }
            }
        }
        #[cfg(feature = "telemetry")]
        context.engine.full_node_telemetry.new_downloading_block_attempt(context.id);
        let ret = context.client.download_out_msg_queue_update(context.id, self.update_for_wc).await;
        #[cfg(feature = "telemetry")]
        if ret.is_ok() {
            context.engine.full_node_telemetry.new_downloaded_block(context.id);
        }
        let update = ret?;

        Ok(update)
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
        if let Some(handle) = context.engine.db.load_block_handle(context.id)? {
            let mut is_link = false;
            if handle.has_proof_or_link(&mut is_link) {
                return Ok(context.engine.db.load_block_proof(&handle, is_link).await?);
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
        if let Some(prev_handle) = context.engine.db.load_block_handle(context.id)? {
            if prev_handle.has_next1() {
                let next_id = context.engine.db.load_block_next1(context.id)?;
                if let Some(next_handle) = context.engine.db.load_block_handle(&next_id)? {
                    let mut is_link = false;
                    if next_handle.has_data() && next_handle.has_proof_or_link(&mut is_link) {
                        return Ok((
                            context.engine.db.load_block_data(&next_handle).await?,
                            context.engine.db.load_block_proof(&next_handle, is_link).await?
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
    type Item = (Arc<ShardStateStuff>, Vec<u8>);
    async fn try_download(
        &self, 
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Self::Item> {
        if let Some(handle) = context.engine.db.load_block_handle(context.id)? {
            if handle.has_state() {
                let zs = context.engine.db.load_shard_state_dynamic(context.id)?;
                let mut data = vec!();
                zs.write_to(&mut data)?;
                return Ok((zs, data));
            }
        }
        context.client.download_zero_state(context.id).await
    }
}

pub struct Stopper {
    stop: Arc<AtomicU32>,
}

impl Stopper {
    pub fn new() -> Self {
        Stopper {
            stop: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn set_stop(&self) {
        let stop = self.stop.fetch_or(Engine::MASK_STOP, Ordering::Relaxed);
        Self::log_stop_status(stop);
    }

    pub async fn wait_stop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_millis(Engine::TIMEOUT_STOP_MS)).await;
            let stop = self.stop.load(Ordering::Relaxed) & !Engine::MASK_STOP;
            Self::log_stop_status(stop);
            if (self.stop.load(Ordering::Relaxed) & !Engine::MASK_STOP) == 0 {
                break
            }
        }
    }

    pub fn log_stop_status(bitmap: u32) {
        let mut ss = String::new();
        if bitmap & Engine::MASK_SERVICE_BOOT != 0 {
            ss.push_str("boot, ");
        }
        if bitmap & Engine::MASK_SERVICE_DB_RESTORE != 0 {
            ss.push_str("DB restore, ");
        }
        #[cfg(feature = "external_db")]
        if bitmap & Engine::MASK_SERVICE_KAFKA_CONSUMER != 0 {
            ss.push_str("kafka consumer, ");
        }
        if bitmap & Engine::MASK_SERVICE_MASTERCHAIN_BROADCAST_LISTENER != 0 {
            ss.push_str("masterchain broadcasts listener, ");
        }
        if bitmap & Engine::MASK_SERVICE_MASTERCHAIN_CLIENT != 0 {
            ss.push_str("masterchain client, ");
        }
        if bitmap & Engine::MASK_SERVICE_PSS_KEEPER != 0 {
            ss.push_str("persistent states storer, ");
        }
        if bitmap & Engine::MASK_SERVICE_SHARDCHAIN_BROADCAST_LISTENER != 0 {
            ss.push_str("shardchains broadcasts listener, ");
        }
        if bitmap & Engine::MASK_SERVICE_SHARDCHAIN_CLIENT != 0 {
            ss.push_str("shardchains client, ");
        }
        if bitmap & Engine::MASK_SERVICE_SHARDSTATE_GC != 0 {
            ss.push_str("shard states GC, ");
        }
        if bitmap & Engine::MASK_SERVICE_TOP_SHARDBLOCKS_SENDER != 0 {
            ss.push_str("top shard blocks sender, ");
        }
        if bitmap & Engine::MASK_SERVICE_VALIDATOR_MANAGER != 0 {
            ss.push_str("validator manager, ");
        }
        if bitmap & Engine::MASK_SERVICE_ARCHIVES_GC != 0 {
            ss.push_str("archives gc, ");
        }
        log::warn!("These services are still stopping ({:04x}): {}", bitmap, ss);
    }

    pub fn acquire_stop(&self, mask: u32) {
        self.stop.fetch_or(mask, Ordering::Relaxed);
    }

    pub fn check_stop(&self) -> bool {
        (self.stop.load(Ordering::Relaxed) & Engine::MASK_STOP) != 0 
    }

    pub fn release_stop(&self, mask: u32) {
        self.stop.fetch_and(!mask, Ordering::Relaxed);
    }
}

impl Engine {

    // Masks for services
    #[cfg(feature = "external_db")]
    pub const MASK_SERVICE_KAFKA_CONSUMER: u32                 = 0x0001;
    pub const MASK_SERVICE_MASTERCHAIN_BROADCAST_LISTENER: u32 = 0x0002;
    pub const MASK_SERVICE_MASTERCHAIN_CLIENT: u32             = 0x0004;
    pub const MASK_SERVICE_PSS_KEEPER: u32                     = 0x0008;
    pub const MASK_SERVICE_SHARDCHAIN_BROADCAST_LISTENER: u32  = 0x0010;
    pub const MASK_SERVICE_SHARDCHAIN_CLIENT: u32              = 0x0020;
    pub const MASK_SERVICE_SHARDSTATE_GC: u32                  = 0x0040;
    pub const MASK_SERVICE_TOP_SHARDBLOCKS_SENDER: u32         = 0x0080;
    pub const MASK_SERVICE_VALIDATOR_MANAGER: u32              = 0x0100;
    pub const MASK_SERVICE_BOOT: u32                           = 0x0200;
    pub const MASK_SERVICE_DB_RESTORE: u32                     = 0x0400;
    pub const MASK_SERVICE_ARCHIVES_GC: u32                    = 0x0800;

    // Sync status
    pub const SYNC_STATUS_START_BOOT: u32           = 0x0001;
    pub const SYNC_STATUS_LOAD_MASTER_STATE: u32    = 0x0002;
    pub const SYNC_STATUS_LOAD_SHARD_STATES: u32    = 0x0003;
    pub const SYNC_STATUS_FINISH_BOOT: u32          = 0x0004;
    pub const SYNC_STATUS_SYNC_BLOCKS: u32          = 0x0005;
    pub const SYNC_STATUS_FINISH_SYNC: u32          = 0x0006;
    pub const SYNC_STATUS_CHECKING_DB: u32          = 0x0007;
    pub const SYNC_STATUS_DB_BROKEN: u32            = 0x0008;
    
    const MASK_STOP: u32 = 0x80000000; 
    const TIMEOUT_STOP_MS: u64 = 1000; 
    #[cfg(feature = "telemetry")]
    const TIMEOUT_TELEMETRY_SEC: u64 = 30;

    pub async fn new(
        general_config: TonNodeConfig, 
        ext_db: Vec<Arc<dyn ExternalDb>>, 
        initial_sync_disabled : bool,
        force_check_db: bool,
        stopper: Arc<Stopper>
    ) -> Result<Arc<Self>> {

        struct DbStatusReporter {
            is_broken: AtomicBool
        } 

        impl StatusReporter for DbStatusReporter {
            fn get_report(&self) -> u32 {
                if self.is_broken.load(Ordering::Relaxed) {
                    Engine::SYNC_STATUS_DB_BROKEN
                } else {
                    Engine::SYNC_STATUS_CHECKING_DB
                }
            } 
        }

        async fn open_db(
            db_config: InternalDbConfig,
            restore_db_enabled: bool,
            force_check_db: bool,
            is_broken: Option<&AtomicBool>,
            stopper: &Arc<Stopper>,
            #[cfg(feature = "telemetry")]
            telemetry: Arc<EngineTelemetry>,
            allocated: Arc<EngineAlloc>
        ) -> Result<Arc<InternalDb>> {
            let check_stop = || {
                if stopper.check_stop() {
                    fail!("DB restore was stopped")
                }
                Ok(())
            };                                 
            let db = Arc::new(InternalDb::with_update(
                db_config,
                restore_db_enabled,
                force_check_db,
                true,
                &check_stop,
                is_broken,
                #[cfg(feature = "telemetry")]
                telemetry,
                allocated,
            ).await?);
            Ok(db)
        }

        log::info!("Creating engine...");

        #[cfg(feature = "telemetry")] 
        let (metrics, engine_telemetry) = Self::create_telemetry();
        let storage_allocated = Arc::new(StorageAlloc::default());
        let engine_allocated = Arc::new(
            EngineAlloc {
                storage: storage_allocated,
                awaiters: Arc::new(AtomicU64::new(0)),
                catchain_clients: Arc::new(AtomicU64::new(0)),
                overlay_clients: Arc::new(AtomicU64::new(0)),
                peer_stats: Arc::new(AtomicU64::new(0)),
                shard_states: Arc::new(AtomicU64::new(0)),
                top_blocks: Arc::new(AtomicU64::new(0)),
                validator_peers: Arc::new(AtomicU64::new(0)),
                validator_sets: Arc::new(AtomicU64::new(0))
            }
        );

        let archives_life_time = general_config.gc_archives_life_time_hours();
        let remp_config = general_config.remp_config().clone();
        let cells_lifetime_sec = general_config.cells_gc_config().cells_lifetime_sec;
        let enable_shard_state_persistent_gc = general_config.enable_shard_state_persistent_gc();
        let skip_saving_persistent_states = general_config.skip_saving_persistent_states();
        let restore_db = general_config.restore_db();
        let processed_workchain = general_config.workchain();

        let cells_db_config = general_config.cells_db_config().clone();
        let db_config = InternalDbConfig { 
            db_directory: general_config.internal_db_path().to_string(), 
            cells_gc_interval_sec: general_config.cells_gc_config().gc_interval_sec,
            cells_db_config: cells_db_config.clone(),
        };
        let control_config = general_config.control_server()?;
        let global_config = general_config.load_global_config()?;
        let test_bundles_config = general_config.test_bundles_config().clone();
        let collator_config = general_config.collator_config().clone();
        let low_memory_mode = general_config.low_memory_mode();

        let network = NodeNetwork::new(
            general_config,
            #[cfg(feature = "telemetry")]
            engine_telemetry.clone(),
            engine_allocated.clone()
        ).await?;
        network.start().await?;

        let (status_reporter, status_server) = if let Some(control_config) = control_config {
            log::info!("Invoking DB status control server");
            let status_reporter = Arc::new(
                DbStatusReporter {
                    is_broken: AtomicBool::new(false)
                }
            );
            let status_server = ControlServer::with_params(
                control_config,
                DataSource::Status(status_reporter.clone()),
                network.config_handler(),
                network.config_handler(),
                Some(&network)
            ).await?;
            (Some(status_reporter), Some(status_server))
        } else {
            (None, None)
        };

        stopper.acquire_stop(Self::MASK_SERVICE_DB_RESTORE);
        let db = open_db(
            db_config, 
            restore_db,
            force_check_db,
            if let Some(status_reporter) = status_reporter.as_ref() {
                Some(&status_reporter.is_broken)
            } else {
                None
            },
            &stopper,
            #[cfg(feature = "telemetry")]
            engine_telemetry.clone(),
            engine_allocated.clone()
        ).await;
        if let Some(status_server) = status_server {
            log::info!("Stopping DB status control server...");
            status_server.shutdown().await;
            log::info!("Stopped DB status control server");
        }
        stopper.release_stop(Self::MASK_SERVICE_DB_RESTORE);
        let db = db?;

        let zero_state_id = global_config.zero_state().expect("check zero state settings");
        let mut init_mc_block_id = global_config.init_block()?.unwrap_or_else(|| zero_state_id.clone());
        if let Ok(Some(block_id)) = db.load_full_node_state(INITIAL_MC_BLOCK) {
            if block_id.seq_no > init_mc_block_id.seq_no {
                init_mc_block_id = block_id.deref().clone()
            }
        }

        log::info!("load_all_top_shard_blocks");
        let shard_blocks = match db.load_all_top_shard_blocks() {
            Ok(tsbs) => tsbs,
            Err(e) => {
                log::error!("Can't load top shard blocks from db (continue without ones): {:?}", e);
                HashMap::default()
            }
        };
        log::info!("load_node_state");
        let last_mc_seqno = db
            .load_full_node_state(LAST_APPLIED_MC_BLOCK)?
            .map(|id| id.seq_no as u32)
            .unwrap_or_default();
        let (shard_blocks_pool, shard_blocks_receiver) = ShardBlocksPool::new(
            shard_blocks, 
            last_mc_seqno, 
            false,
            #[cfg(feature = "telemetry")]
            &engine_telemetry,
            &engine_allocated
        )?;

        let shard_states_keeper = ShardStatesKeeper::new(
            db.clone(),
            enable_shard_state_persistent_gc,
            skip_saving_persistent_states,
            cells_lifetime_sec,
            stopper.clone(),
            cells_db_config.states_db_queue_len + 10,
            #[cfg(feature = "telemetry")]
            engine_telemetry.clone(),
            engine_allocated.clone()
        )?;

        let remp_client = if remp_config.is_client_enabled() {
            let remp_client = Arc::new(RempClient::new(network.public_overlay_key()?.id().data().into()));
            network.remp().set_receipts_subscriber(remp_client.clone())?;
            Some(remp_client)
        } else {
            None
        };
        #[cfg(feature = "telemetry")]
        let remp_core_telemetry = Arc::new(RempCoreTelemetry::new(Self::TIMEOUT_TELEMETRY_SEC));
        let (remp_service, remp_messages) = if remp_config.is_service_enabled() {
            let remp_service = Arc::new(RempService::new());
            network.remp().set_messages_subscriber(remp_service.clone())?;
            #[cfg(feature = "telemetry")]
            network.remp().set_telemetry(remp_core_telemetry.clone())?;
            network.remp().start()?;
            (Some(remp_service), Some(Arc::new(RempMessagesPool::new())))
        } else {
            (None, None)
        };

        log::info!("Engine is created.");

        #[cfg(feature = "slashing")]
        let (validated_block_stats_sender, validated_block_stats_receiver) = 
            crossbeam_channel::bounded(MAX_VALIDATED_BLOCK_STATS_ENTRIES_COUNT);
        let engine = Arc::new(Engine {
            db,
            ext_db,
            overlay_operations: network.clone() as Arc<dyn OverlayOperations>,
            shard_states_awaiters: AwaitersPool::new(
                "shard_states_awaiters",
                #[cfg(feature = "telemetry")]
                engine_telemetry.clone(),
                engine_allocated.clone()
            ),
            block_applying_awaiters: AwaitersPool::new(
                "block_applying_awaiters",
                #[cfg(feature = "telemetry")]
                engine_telemetry.clone(),
                engine_allocated.clone()
            ),
            next_block_applying_awaiters: AwaitersPool::new(
                "next_block_applying_awaiters",
                #[cfg(feature = "telemetry")]
                engine_telemetry.clone(),
                engine_allocated.clone()
            ),
            download_block_awaiters: AwaitersPool::new(
                "download_block_awaiters",
                #[cfg(feature = "telemetry")]
                engine_telemetry.clone(),
                engine_allocated.clone()
            ),
            download_queue_update_awaiters: AwaitersPool::new(
                "download_queue_update_awaiters",
                #[cfg(feature = "telemetry")]
                engine_telemetry.clone(),
                engine_allocated.clone()
            ),
            external_messages: MessagesPool::new(),
            servers: lockfree::queue::Queue::new(),
            remp_client,
            remp_service,
            remp_messages,
            stopper,
            zero_state_id,
            init_mc_block_id,
            initial_sync_disabled,
            archives_life_time,
            network,
            shard_blocks: shard_blocks_pool,
            last_known_mc_block_seqno: AtomicU32::new(0),
            last_known_keyblock_seqno: AtomicU32::new(0),
            will_validate: AtomicBool::new(false),
            sync_status: AtomicU32::new(0),
            low_memory_mode,
            remp_capability: AtomicBool::new(false),
            test_bundles_config,
            collator_config,
            shard_states_keeper: shard_states_keeper.clone(),
            processed_workchain,
            split_queues_cache: lockfree::map::Map::new(),
            validation_status: Arc::new(AtomicU8::new(0)),
            last_validation_time: lockfree::map::Map::new(),
            last_collation_time: lockfree::map::Map::new(),
            #[cfg(feature = "slashing")]
            validated_block_stats_sender,
            #[cfg(feature = "slashing")]
            validated_block_stats_receiver,
            #[cfg(feature = "telemetry")]
            full_node_telemetry: FullNodeTelemetry::new(),
            #[cfg(feature = "telemetry")]
            remp_core_telemetry,
            #[cfg(feature = "telemetry")]
            collator_telemetry: CollatorValidatorTelemetry::default(),
            #[cfg(feature = "telemetry")]
            validator_telemetry: CollatorValidatorTelemetry::default(),
            #[cfg(feature = "telemetry")]
            full_node_service_telemetry: 
                FullNodeNetworkTelemetry::new(FullNodeNetworkTelemetryKind::Service),
            #[cfg(feature = "telemetry")]
            engine_telemetry,
            #[cfg(feature = "telemetry")]
            remp_client_telemetry: RempClientTelemetry::default(),
            engine_allocated,
            #[cfg(feature = "telemetry")]
            telemetry_printer: TelemetryPrinter::with_params(
                Self::TIMEOUT_TELEMETRY_SEC,
                metrics
            ),
            tps_counter: TpsCounter::new(),

            out_queues_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
        });

        if let Some(rs) = engine.remp_service() {
            rs.set_engine(engine.clone())?;
        }

        engine.acquire_stop(Self::MASK_SERVICE_SHARDSTATE_GC);
        save_top_shard_blocks_worker(engine.clone(), shard_blocks_receiver);
        Ok(engine)
    }

    pub fn set_sync_status(&self, status: u32) {
        log::info!("sync status now is: {}", status);
        self.sync_status.store(status, Ordering::Relaxed);
    }

    pub fn get_sync_status(&self) -> u32 {
        self.sync_status.load(Ordering::Relaxed)
    }

    pub async fn wait_stop(self: Arc<Self>) {
        // set stop flag
        self.stopper.set_stop();

        self.network.delete_overlays().await;

        // stop servers
        while let Some(server) = self.servers.pop() {
            match server {
                Server::ControlServer(server) => {
                    log::info!("Stopping control server...");
                    server.shutdown().await;
                    log::info!("Stopped control server");
                },
                #[cfg(feature = "external_db")]
                Server::KafkaConsumer(trigger) => {
                    log::info!("Stopping kafka consumer...");
                    drop(trigger);
                    log::info!("Stopped kafka consumer...");
                }
            }
        }

        // stop states GC
        let engine = self.clone();
        tokio::spawn(
            async move {
                 engine.db.stop_states_db().await;
                 engine.stopper.release_stop(Self::MASK_SERVICE_SHARDSTATE_GC);
            }
        );

        // wait while all node's services will stop
        self.stopper.clone().wait_stop().await;

        self.network.stop_adnl().await;
    }

    pub fn stopper(&self) -> &Stopper {
        &self.stopper
    }

    pub fn register_server(&self, server: Server) {
        self.servers.push(server)
    }

    pub fn db(&self) -> &Arc<InternalDb> { &self.db }

    pub fn validator_network(&self) -> Arc<dyn PrivateOverlayOperations> { self.network.clone() }

    pub fn network(&self) -> &NodeNetwork { &self.network }

    pub fn ext_db(&self) -> &Vec<Arc<dyn ExternalDb>> { &self.ext_db }

    pub fn zero_state_id(&self) -> &BlockIdExt { &self.zero_state_id }

    pub fn init_mc_block_id(&self) -> &BlockIdExt {&self.init_mc_block_id}

    pub fn initial_sync_disabled(&self) -> bool {self.initial_sync_disabled}

    pub fn shard_states_keeper(&self) -> &ShardStatesKeeper { &self.shard_states_keeper }

    pub async fn get_masterchain_overlay(&self) -> Result<Arc<dyn FullNodeOverlayClient>> {
        self.get_full_node_overlay(ton_block::MASTERCHAIN_ID, ton_block::SHARD_FULL).await
    }

    pub async fn get_full_node_overlay(&self, workchain: i32, shard: u64) -> Result<Arc<dyn FullNodeOverlayClient>> {
        let id = self.overlay_operations.calc_overlay_id(workchain, shard)?;
        if let Some(overlay) = self.overlay_operations.get_overlay(&id.0).await {
            Ok(overlay)
        } else {
            log::debug!(
                "Overlay for workchain {workchain} was not found. Attempt to add foreign overlay."
            );
            self.overlay_operations.clone().add_overlay(id.clone(), false).await?;
            self.overlay_operations.get_overlay(&id.0).await
                .ok_or_else(|| error!(
                    "INTERNAL ERROR: overlay for workchain {workchain} was not found after calling add_overlay"
                ))
        }
    }

    pub async fn add_full_node_overlay(
        &self,
        workchain: i32,
        shard: u64,
        foreign: bool
    ) -> Result<()> {
        let id = self.overlay_operations.calc_overlay_id(workchain, shard)?;
        self.overlay_operations.clone().add_overlay(id, !foreign).await
    }

    pub fn shard_states_awaiters(&self) -> &AwaitersPool<BlockIdExt, Arc<ShardStateStuff>> {
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

    pub fn download_queue_update_awaiters(&self) -> &AwaitersPool<BlockIdExt, BlockStuff> {
        &self.download_queue_update_awaiters
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

    pub fn collator_config(&self) -> &CollatorConfig {
        &self.collator_config
    }

    #[cfg(feature = "telemetry")]
    pub fn full_node_telemetry(&self) -> &FullNodeTelemetry {
        &self.full_node_telemetry
    }

    #[cfg(feature = "telemetry")]
    pub fn remp_core_telemetry(&self) -> &RempCoreTelemetry {
        &self.remp_core_telemetry
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

    #[cfg(feature = "telemetry")]
    pub fn engine_telemetry(&self) -> &Arc<EngineTelemetry> {
        &self.engine_telemetry
    }

    #[cfg(feature = "telemetry")]
    pub fn remp_client_telemetry(&self) -> &RempClientTelemetry {
        &self.remp_client_telemetry
    }

    pub fn engine_allocated(&self) -> &Arc<EngineAlloc> {
        &self.engine_allocated
    }

    pub fn validation_status(&self) -> ValidationStatus {
        ValidationStatus::from_u8(self.validation_status.load(Ordering::Relaxed))
    }

    pub fn set_validation_status(&self, status: ValidationStatus) {
        self.validation_status.store(status as u8, Ordering::Relaxed)
    }

    pub fn last_validation_time(&self) -> &lockfree::map::Map<ShardIdent, u64> {
        &self.last_validation_time
    }

    pub fn set_last_validation_time(&self, shard: ShardIdent, time: u64) {
        self.last_validation_time.insert(shard, time);
    }

    pub fn remove_last_validation_time(&self, shard: &ShardIdent) {
        self.last_validation_time.remove(shard);
    }

    pub fn last_collation_time(&self) -> &lockfree::map::Map<ShardIdent, u64> {
        &self.last_collation_time
    }

    pub fn set_last_collation_time(&self, shard: ShardIdent, time: u64) {
        self.last_collation_time.insert(shard, time);
    }

    pub fn remove_last_collation_time(&self, shard: &ShardIdent) {
        self.last_collation_time.remove(shard);
    }

    pub fn tps_counter(&self) -> &TpsCounter {
        &self.tps_counter
    }

    pub fn low_memory_mode(&self) -> bool {
        self.low_memory_mode
    }

    pub fn remp_service(&self) -> Option<&RempService> {
        self.remp_service.as_ref().map(|arc| arc.deref())
    }

    pub fn remp_client(&self) -> Option<&Arc<RempClient>> {
        self.remp_client.as_ref()
    }

    pub fn remp_messages(&self) -> Result<&RempMessagesPool> {
        let rm = self.remp_messages.as_ref().ok_or_else(|| error!("Remp messages pool was not inited."))?.deref();
        Ok(rm)
    }

    pub fn remp_capability(&self) -> bool {
        self.remp_capability.load(Ordering::Relaxed)
    }

    pub fn set_remp_capability(&self, value: bool) {
        self.remp_capability.store(value, Ordering::Relaxed);
    }

    pub fn split_queues_cache(&self) -> 
        &lockfree::map::Map<BlockIdExt, Option<(OutMsgQueue, OutMsgQueue, HashSet<UInt256>)>> 
    {
        &self.split_queues_cache
    }

    pub fn processed_workchain(&self) -> Option<i32> {
        self.processed_workchain
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
                let mut is_link = false;
                if handle.has_data() && (handle.is_queue_update() || handle.has_proof_or_link(&mut is_link)) {
                    while !((pre_apply && handle.has_state()) || handle.is_applied()) {
                        let s = self.clone();
                        let res = self.block_applying_awaiters().do_or_wait(
                            handle.id(),
                            None,
                            async {
                                let block = s.load_block(&handle).await?;
                                s.apply_block_worker(&handle, &block, mc_seq_no, pre_apply, recursion_depth).await?;
                                Ok(())
                            }
                        ).await;
                        if res.is_err() {
                            if !handle.has_data() {
                                break
                            }
                            res?;
                        }
                    }
                    if handle.has_data() {
                        return Ok(());
                    }
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

            let (foreign_block, own_wc) = self.is_foreign_wc(id.shard().workchain_id()).await?;

            if foreign_block {
                if let Some(queue_update) = self.download_queue_update_awaiters().do_or_wait(
                    id,
                    None,
                    self.download_queue_update_worker(id, own_wc, attempts, timeout)
                ).await? {

                    let downloading_time = now.elapsed().as_millis();

                    let now = std::time::Instant::now();
                    
                    // Check queue update like block proof
                    BlockProofStuff::check_queue_update(&queue_update)?;

                    let handle = self.store_block(&queue_update).await?;
                    let handle = if let Some(handle) = handle.to_non_created() {
                        handle
                    } else {
                        continue
                    };
                    log::trace!(
                        "Downloaded queue update for {}apply {} TIME download: {}ms, check & save: {}", 
                        if pre_apply { "pre-" } else { "" }, 
                        queue_update.id(),
                        downloading_time, 
                        now.elapsed().as_millis(),
                    );
                    self.apply_block(&handle, &queue_update, mc_seq_no, pre_apply).await?;
                    return Ok(())
                }
            } else {

                if let Some((block, proof)) = self.download_block_awaiters().do_or_wait(
                    id,
                    None,
                    self.download_block_worker(id, attempts, timeout)
                ).await? {

                    let downloading_time = now.elapsed().as_millis();

                    let now = std::time::Instant::now();
                    proof.check_proof(self.deref()).await?;
                    let handle = self.store_block(&block).await?;
                    let handle = if let Some(handle) = handle.to_non_created() {
                        handle
                    } else {
                        continue
                    };
                    let handle = self.store_block_proof(id, Some(handle), &proof).await?;
                    let handle = handle.to_non_created().ok_or_else(
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

        let is_empty_queue_update = handle.is_empty_queue_update();
        let op_name = if pre_apply { "pre-applying" } else { "applying" };
        let block_name = if let Some(wc) = handle.is_queue_update_for() {
            if is_empty_queue_update {
                format!("empty queue update for {}", wc)
            } else {
                format!("queue update for {}", wc)
            }
        } else {
            "block".to_owned()
        };
        let id = block.id();
        log::debug!("Start {op_name} {block_name} {id}");

        let mut is_link = false;
        let has_proof = handle.has_proof_or_link(&mut is_link);
        let has_data = handle.has_data();
        let is_queue_update = handle.is_queue_update();

        if !((has_proof || is_queue_update) && (has_data || is_empty_queue_update)) {
            fail!(
                "Block must have proof ({}) or be queue update ({}) and data ({}) \
                or be empty queue update ({}) saved before applying",
                has_proof, is_queue_update, has_data, is_empty_queue_update
            );
        }

        apply_block(handle, block, mc_seq_no, &(self.clone() as Arc<dyn EngineOperations>),
            pre_apply, recursion_depth).await?;

        let gen_utime = block.gen_utime()?;
        let ago = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?.as_secs() as i32 - gen_utime as i32;
        if block.id().shard().is_masterchain() {
            if !pre_apply {
                self.shard_blocks()
                    .update_shard_blocks(&self.load_state(block.id()).await?)
                    .await?;

                let first_time_applied = self.set_applied(handle, block.id().seq_no()).await?;

                if first_time_applied {
                    if let Err(e) = self.save_last_applied_mc_block_id(block.id()) {
                        log::error!("Can't save last applied mc block {}: {}", block.id(), e);
                    }
                }
                STATSD.gauge("last_applied_mc_block", block.id().seq_no() as f64);
                STATSD.gauge("timediff", ago as f64);

                if let Err(e) = self.mc_block_post_apply(block, gen_utime, first_time_applied).await {
                    log::error!("Error after apply block {}: {}", block.id(), e);
                }
            }

            let op_name = if pre_apply { "Pre-applied" } else { "Applied" };
            log::info!("{op_name} {block_name} {id}, {ago} seconds old");
        } else {
            if !pre_apply {
                if self.set_applied(handle, mc_seq_no).await? && !block.is_queue_update(){
                    self.tps_counter.submit_transactions(gen_utime as u64, block.calculate_tr_count()?);
                }
            }

            let op_name = if pre_apply { "Pre-applied" } else { "Applied" };
            log::info!("{op_name} {block_name} {id} ref_mc_block: {mc_seq_no}, {ago} seconds old");
        }
        Ok(())
    }

    async fn mc_block_post_apply(
        &self,
        block: &BlockStuff,
        gen_utime: u32,
        first_time_applied: bool
    ) -> Result<()> {

        if first_time_applied {
            self.tps_counter.submit_transactions(gen_utime as u64, block.calculate_tr_count()?);
        }

        if block.is_key_block()? {
            self.remp_capability.store(
                block.get_config_params()?.has_capability(GlobalCapabilities::CapRemp),
                Ordering::Relaxed
            );
            // While the node boots start key block is not processed by this function.
            // So see process_full_state_in_ext_db for the same code
        }

        let (prev_id, prev2_id_opt) = block.construct_prev_id()?;
        if prev2_id_opt.is_some() {
            fail!("UNEXPECTED error: master block refers two previous blocks");
        }
        let id = block.id().clone();
        self.next_block_applying_awaiters.do_or_wait(&prev_id, None, async move { Ok(id) }).await?;

        let mut total = 0;
        let mut removed = 0;
        for guard in &self.split_queues_cache {
            total += 1;
            if self.shard_states_keeper.allow_state_gc(guard.key())? {
                self.split_queues_cache.remove(guard.key());
                removed += 1;
            }
        }
        log::debug!("Split queues cache GC: total {total}, removed {removed}");

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
                    Ok(None) => {
                        log::warn!("wait_broadcast finished.");
                        break;
                    },
                    Ok(Some((broadcast, src))) => {
                        match broadcast {
                            Broadcast::TonNode_BlockBroadcast(broadcast) => {
                                self.clone().process_block_broadcast(broadcast, src);
                            }
                            Broadcast::TonNode_QueueUpdateBroadcast(broadcast) => {
                                self.clone().process_queue_update_broadcast(broadcast, src);
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

    #[cfg(feature = "slashing")]
    pub fn validated_block_stats_sender(
        &self
    ) -> &crossbeam_channel::Sender<ValidatedBlockStat> { 
        &self.validated_block_stats_sender 
    }

    #[cfg(feature = "slashing")]
    pub fn validated_block_stats_receiver(
        &self
    ) -> &crossbeam_channel::Receiver<ValidatedBlockStat> { 
        &self.validated_block_stats_receiver 
    }

    #[cfg(feature = "telemetry")] 
    fn create_telemetry() -> (Vec<TelemetryItem>, Arc<EngineTelemetry>) {

        fn create_metric(name: &str) -> Arc<Metric> {
            Metric::without_totals(name, Engine::TIMEOUT_TELEMETRY_SEC)
        }
        fn create_metric_ex(name: &str) -> Arc<MetricBuilder> {
            MetricBuilder::with_metric_and_period(
                Metric::with_total_amount(name, Engine::TIMEOUT_TELEMETRY_SEC),
                1_000_000_000 // 1 sec in nanos
            )
        }
 
        let storage_telemetry = Arc::new(
            StorageTelemetry {
                file_entries: create_metric("Alloc NODE file entries"),
                handles: create_metric("Alloc NODE block handles"),
                packages: create_metric("Alloc NODE packages"),
                storage_cells: create_metric("Alloc NODE storage cells"),
                shardstates_queue: create_metric("Alloc NODE shardstates queue"),
                cells_counters: create_metric("Alloc NODE cells counters"),
                cell_counter_from_cache: create_metric_ex("NODE read cache cell_counters/sec"),
                cell_counter_from_db: create_metric_ex("NODE read db cell_counters/sec"),
                updated_old_cells: create_metric_ex("NODE old format update cells/sec"),
                updated_cells: create_metric_ex("NODE update cell_counters/sec"),
                new_cells: create_metric_ex("NODE create new cells/sec"),
                deleted_cells: create_metric_ex("NODE delete cells/sec"),
            }
        );
        let engine_telemetry = Arc::new(
            EngineTelemetry {
                storage: storage_telemetry,
                awaiters: create_metric("Alloc NODE awaiters"),
                catchain_clients: create_metric("Alloc NODE catchains"),
                cells: create_metric("Alloc NODE cells"),
                overlay_clients: create_metric("Alloc NODE overlays"),
                peer_stats: create_metric("Alloc NODE peer stats"),
                shard_states: create_metric("Alloc NODE shard states"),
                top_blocks: create_metric("Alloc NODE top blocks"),
                validator_peers: create_metric("Alloc NODE validator peers"),
                validator_sets: create_metric("Alloc NODE validator sets")
            }
        );
        let metrics = vec![
            TelemetryItem::Metric(engine_telemetry.storage.file_entries.clone()),
            TelemetryItem::Metric(engine_telemetry.storage.handles.clone()),
            TelemetryItem::Metric(engine_telemetry.storage.packages.clone()),
            TelemetryItem::Metric(engine_telemetry.storage.storage_cells.clone()),
            TelemetryItem::Metric(engine_telemetry.storage.shardstates_queue.clone()),
            TelemetryItem::Metric(engine_telemetry.storage.cells_counters.clone()),
            TelemetryItem::MetricBuilder(engine_telemetry.storage.cell_counter_from_cache.clone()),
            TelemetryItem::MetricBuilder(engine_telemetry.storage.cell_counter_from_db.clone()),
            TelemetryItem::MetricBuilder(engine_telemetry.storage.updated_old_cells.clone()),
            TelemetryItem::MetricBuilder(engine_telemetry.storage.updated_cells.clone()),
            TelemetryItem::MetricBuilder(engine_telemetry.storage.new_cells.clone()),
            TelemetryItem::MetricBuilder(engine_telemetry.storage.deleted_cells.clone()),
            TelemetryItem::Metric(engine_telemetry.awaiters.clone()),
            TelemetryItem::Metric(engine_telemetry.catchain_clients.clone()),
            TelemetryItem::Metric(engine_telemetry.cells.clone()),
            TelemetryItem::Metric(engine_telemetry.overlay_clients.clone()),
            TelemetryItem::Metric(engine_telemetry.peer_stats.clone()),
            TelemetryItem::Metric(engine_telemetry.shard_states.clone()),
            TelemetryItem::Metric(engine_telemetry.top_blocks.clone()),
            TelemetryItem::Metric(engine_telemetry.validator_peers.clone()),
            TelemetryItem::Metric(engine_telemetry.validator_sets.clone())
        ];
        (metrics, engine_telemetry)

    }

    #[cfg(feature = "slashing")]
    async fn process_validated_block_stats_for_mc(&self, block_id: &BlockIdExt, signing_nodes: HashSet<UInt256>) -> Result<()> {
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
    async fn process_validated_block_stats(&self, block_id: &BlockIdExt, signing_nodes: HashSet<UInt256>, created_by: &UInt256) -> Result<()> {
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

        let mut nodes = Vec::new();

        for validator in &validators {
            let signed = signing_nodes.contains(&validator.compute_node_id_short());
            let collated = &validator.public_key == created_by;
            let validated_block_stat_node = ValidatedBlockStatNode {
                public_key: validator.public_key.clone(),
                signed,
                collated,
            };

            nodes.push(validated_block_stat_node);
        }

        self.push_validated_block_stat(ValidatedBlockStat { nodes })?;

        Ok(())
    }

    fn process_block_broadcast(self: Arc<Self>, broadcast: BlockBroadcast, src: Arc<KeyId>) {
        // because of ALL blocks-broadcasts received in one task - spawn for each block
        log::trace!("Processing block broadcast {}", broadcast.id);
        let engine = self.clone() as Arc<dyn EngineOperations>;
        tokio::spawn(async move {
            let (is_foreign_block, _own_wc) = 
                engine.is_foreign_wc(broadcast.id.shard().workchain_id()).await.unwrap_or((true, 0));
            if is_foreign_block {
                log::debug!("Skipped block broadcast {} (foreign wc)", broadcast.id);
                return;
            }
            match process_block_broadcast(&engine, &broadcast).await {
                Err(e) => {
                    log::error!("Error while processing block broadcast {} from {}: {:?}", broadcast.id, src, e)
                }
                Ok(_block_opt) => {
                    log::trace!("Processed block broadcast {} from {}", broadcast.id, src);

                    #[cfg(feature = "slashing")]
                    if broadcast.id.shard().is_masterchain() {
                        let mut signing_nodes = HashSet::new();
                        for api_sig in broadcast.signatures.iter() {
                            signing_nodes.insert(api_sig.who.clone());
                        }

                        if let Err(e) = self.process_validated_block_stats_for_mc(&broadcast.id, signing_nodes).await {
                            log::error!("Error while processing block broadcast stats {} from {}: {}", broadcast.id, src, e);        
                        } else {
                            log::trace!("Processed block broadcast stats {} from {}", broadcast.id, src);                            
                        }

                        if let (Some(block), Some(remp_client)) = (_block_opt, &self.remp_client) {
                            remp_client.clone().process_new_block(block);
                        }
                    }
                }
            }
        });
    }

    fn process_queue_update_broadcast(self: Arc<Self>, broadcast: QueueUpdateBroadcast, src: Arc<KeyId>) {
        // because of ALL blocks-broadcasts received in one task - spawn for each block
        log::trace!("Processing queue update broadcast {} for wc {} from {}",
            broadcast.id, broadcast.target_wc, src);
        let engine = self.clone() as Arc<dyn EngineOperations>;
        tokio::spawn(async move {
            let (is_foreign_block, own_wc) = 
                engine.is_foreign_wc(broadcast.id.shard().workchain_id()).await.unwrap_or((false, 0));
            if !is_foreign_block {
                log::debug!("Skipped queue update broadcast {} for wc {} (own wc)", 
                    broadcast.id, broadcast.target_wc);
                return;
            }
            if broadcast.target_wc != own_wc {
                log::debug!("Skipped queue update broadcast {} for wc {} (update for another wc)", 
                    broadcast.id, broadcast.target_wc);
                return;
            }
            if let Err(e) = process_block_broadcast(&engine, &broadcast).await {
                log::error!("Error while processing queue update broadcast {} for wc {} from {}: {:?}",
                    broadcast.id, broadcast.target_wc, src, e);
            } else {
                log::trace!("Processed queue update broadcast {} for wc {} from {}", 
                    broadcast.id, broadcast.target_wc, src);
            }
        });
    }

    fn process_ext_msg_broadcast(&self, broadcast: ExternalMessageBroadcast, src: Arc<KeyId>) {
        let remp = self.remp_capability();
        // just add to list
        if !self.is_validator() {
            log::trace!(
                target: EXT_MESSAGES_TRACE_TARGET,
                "Skipped ext message broadcast {}bytes from {}: NOT A VALIDATOR",
                broadcast.message.data.0.len(), src
            );
        } else if remp {
            log::warn!(
                "Skipped ext message broadcast {}bytes from {}: REMP CAPABILITY IS ENABLED",
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

    fn process_new_shard_block_broadcast(self: Arc<Self>, broadcast: NewShardBlockBroadcast, src: Arc<KeyId>) {
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

    async fn process_new_shard_block(self: Arc<Self>, broadcast: NewShardBlockBroadcast) -> Result<BlockIdExt> {
        let id = broadcast.block.block;
        let cc_seqno = broadcast.block.cc_seqno as u32;
        let data = broadcast.block.data.0;
        let processed_wc = self.processed_workchain().unwrap_or(BASE_WORKCHAIN_ID);

        if processed_wc != MASTERCHAIN_ID && processed_wc != id.shard().workchain_id() {
            log::debug!("Skipped new shard block broadcast {} from foreign workchain", id);
            return Ok(id);
        }

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
                let mut signing_nodes = HashSet::new();
                if let Some(commit_signatures) = tbd.top_block_descr().signatures() {
                    commit_signatures.pure_signatures.signatures().iterate_slices(|ref mut _key, ref mut slice| {
                        use ton_block::Deserializable;
                        let sign = ton_block::CryptoSignaturePair::construct_from(slice)?;
                        signing_nodes.insert(sign.node_id_short.clone());
                        Ok(true)
                    })?;
                }

                if let Err(e) = self.process_validated_block_stats(&id, signing_nodes, &_created_by).await {
                    log::error!("Error while processing shard block broadcast stats {}: {}", id, e);
                } else {
                    log::trace!("Processed shard block broadcast stats {}", id);
                }
            }

            // add to list (for collator) only if shard state is avaliable
            let id = id.clone();
            let engine = self.clone() as Arc<dyn EngineOperations>;
            let process_proof_chain = processed_wc == MASTERCHAIN_ID &&
                engine
                    .load_actual_config_params().await?
                    .has_capability(GlobalCapabilities::CapWorkchains);
            tokio::spawn(async move {

                if process_proof_chain {
                    // we will not get empty queue updates, because it is already contained 
                    // in this broadcast. So we need to pre-apply it here
                    if let Err(e) = apply_proof_chain(
                        tbd.top_block_descr().chain(),
                        MASTERCHAIN_ID,
                        &engine,
                        &id,
                        0,
                        true,
                        true,
                    ).await {
                        log::error!("Error in apply_proof_chain after top-block-broadcast {}: {}", id, e);
                        return;
                    }
                }

                // just passively waiting for 10s...
                if let Err(e) = self.clone().wait_state(&id, Some(10_000), false).await {
                    log::error!("Error in wait_state after top-block-broadcast false {}: {}", id, e);
                    // ...and then allow to download needed blocks forced
                    if let Err(e) = self.clone().wait_state(&id, Some(10_000), true).await {
                        log::error!("Error in wait_state after top-block-broadcast true {}: {}", id, e);
                        return;
                    }
                }

                if let Err(e) = self.shard_blocks.process_shard_block(
                    &id, cc_seqno, || Ok(tbd.clone()), false, self.deref()).await {
                    log::error!("Error in process_shard_block after wait_state {}: {}", id, e);
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
            engine: self,
            downloader,
            id,
            limit,
            log_error_limit,
            name,
            timeout,
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

    pub async fn download_queue_update_worker(
        &self,
        id: &BlockIdExt,
        update_for_wc: i32,
        limit: Option<u32>,
        timeout: Option<(u64, u64, u64)>
    ) -> Result<BlockStuff> {
        self.create_download_context(
            Arc::new(QueueUpdateDownloader{ update_for_wc }),
            id, 
            limit,
            0,
            "download_queue_update_worker", 
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
        if id.seq_no() == 0 {
            fail!("cannot download block proof for zero state")
        }
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
    ) -> Result<(Arc<ShardStateStuff>, Vec<u8>)> {
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

    pub fn load_pss_keeper_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
        self.db().load_full_node_state(PSS_KEEPER_MC_BLOCK)
    }

    pub fn save_pss_keeper_mc_block_id(&self, id: &BlockIdExt) -> Result<()> {
        self.db().save_full_node_state(PSS_KEEPER_MC_BLOCK, id)
    }

    pub fn load_archives_gc_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
        self.db().load_full_node_state(ARCHIVES_GC_BLOCK)
    }

    pub fn save_archives_gc_mc_block_id(&self, id: &BlockIdExt) -> Result<()> {
        self.db().save_full_node_state(ARCHIVES_GC_BLOCK, id)
    }

    pub fn start_archives_gc(
        engine: Arc<Engine>,
        archives_gc_block: BlockIdExt
    ) -> Result<tokio::task::JoinHandle<()>> {
        log::info!("start_archives_gc");
        let join_handle = tokio::spawn(async move {
            engine.acquire_stop(Engine::MASK_SERVICE_ARCHIVES_GC);
            if let Err(e) = Self::archives_gc_worker(&engine, archives_gc_block).await {
                log::error!("CRITICAL!!! Unexpected error in archives gc: {:?}", e);
            }
            engine.release_stop(Engine::MASK_SERVICE_ARCHIVES_GC);
        });
        Ok(join_handle)
    }

    pub async fn archives_gc_worker(
        engine: &Arc<Engine>,
        archives_gc_block: BlockIdExt
    ) -> Result<()> {

        fn check_unapplied_block_id(
            mut id: BlockIdExt, 
            engine: &Arc<Engine>
        ) -> Result<Option<BlockIdExt>> {
            loop {
                let handle = engine.load_block_handle(&id)?.ok_or_else(
                    || error!("No handle for block {} in DB", id)
                )?;
                if handle.is_archived() {
                    return Ok(Some(id))
                }
                if engine.load_block_prev2(&id).is_ok() {
                    // Skip drop unapplied block right after merge
                    return Ok(None)
                }
                id = engine.load_block_prev1(&id)?
            }
        }

        if !archives_gc_block.shard().is_masterchain() {
            fail!("'archives_gc_block' must belong master chain");
        }
        let mut handle = engine.load_block_handle(&archives_gc_block)?.ok_or_else(
            || error!("Cannot load handle for archives_gc_block {}", archives_gc_block)
        )?;
        let mut last_clean_unapplied_time = std::time::Instant::now();
        'm: loop {
            let mc_state = engine.load_state(handle.id()).await?;
            if engine.check_stop() {
                break 'm;
            }
            if handle.is_key_block()? {
                let mc_state = engine.load_state(handle.id()).await?;
                if let Err(e) = Self::check_gc_for_archives(&engine, &handle, &mc_state).await {
                    log::error!("Archives GC: {}", e);
                }
            }
            // clean unapplied blocks every 15 seconds
            if last_clean_unapplied_time.elapsed().as_secs() > 15 {
                let mut ids = Vec::new();
                for id in mc_state.top_blocks_all()? {
                    match check_unapplied_block_id(id, engine) {
                        Ok(Some(id)) => ids.push(id),
                        Err(e) => log::warn!("unapplied files gc: {}", e),
                        _ => ()
                    }    
                }
                engine.db().clean_unapplied_files(&ids).await;
                last_clean_unapplied_time = std::time::Instant::now();
            }
            handle = loop {
                match engine.wait_next_applied_mc_block(&handle, Some(500)).await {
                    Ok(r) => break r.0,
                    Err(_) => {
                        if engine.check_stop() {
                            break 'm;
                        }
                    }
                }
            };
            engine.save_archives_gc_mc_block_id(handle.id())?;
        }
        Ok(())

    }

    async fn check_gc_for_archives(
        engine: &Arc<Engine>,
        last_keyblock: &Arc<BlockHandle>,
        mc_state: &ShardStateStuff
    ) -> Result<()> {
        let mut gc_max_date = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
        match &engine.archives_life_time {
            None => return Ok(()),
            Some(life_time) => {
                match gc_max_date.checked_sub(Duration::from_secs((life_time * 3600) as u64)) {
                    Some(date) => {
                        log::info!("archive gc: checked date {}.", &date.as_secs());
                        gc_max_date = date
                    },
                    None => {
                        log::info!("archive gc: life_time in config is bad, actual checked date: {}",
                            &gc_max_date.as_secs()
                        );
                    }
                }
            }
        }

        let mut visited_pss_blocks = 0;
        let mut keyblock = last_keyblock.clone();
        let prev_blocks = &mc_state.shard_state_extra()?.prev_blocks;
        loop {
            match prev_blocks.get_prev_key_block(keyblock.id().seq_no() - 1)? {
                None => return Ok(()),
                Some(prev_keyblock) => {
                    let prev_keyblock = BlockIdExt::from_ext_blk(prev_keyblock);
                    let prev_keyblock = engine.load_block_handle(&prev_keyblock)?.ok_or_else(
                        || error!("Cannot load handle for PSS keeper prev key block {}", prev_keyblock)
                    )?;
                    if engine.is_persistent_state(
                        keyblock.gen_utime()?, prev_keyblock.gen_utime()?, boot::PSS_PERIOD_BITS
                    ) {
                        visited_pss_blocks += 1;

                        // Due to boot process specific (pss period and key_block_utime_step combinations)
                        // we shouldn't delete last 4 pss blocks
                        // ....................pss_block....pss_block....pss_block....pss_block...
                        // visited_pss_blocks:         4            3            2            1
                        //                    ↑ we may delete blocks starting at least here (before 4th pss)
                        
                        if engine.shard_states_keeper.allow_state_gc(keyblock.id())? {
                            if visited_pss_blocks >= 4 {
                                let gen_time = keyblock.gen_utime()? as u64;
                                let gc_max_date = gc_max_date.as_secs();
                                if gen_time < gc_max_date {
                                    log::info!(
                                        "gc for archives: found block (gen time: {}, seq_no: {}), gc max date: {}",
                                        &gen_time, keyblock.id().seq_no(), &gc_max_date
                                    );
                                    log::info!("start gc for archives..");
                                    engine.db.archive_gc(keyblock.id()).await?;
                                    log::info!("finish gc for archives.");
                                    return Ok(());
                                }
                            }
                        }
                    }
                    if prev_keyblock.id().seq_no() == 0 {
                        return Ok(());
                    }
                    keyblock = prev_keyblock;
                }
            }
        }
    }

    fn check_finish_sync(self: Arc<Self>) {
        tokio::spawn(async move {
            const SLEEP_TIME: u64 = 30;
            loop {
                if let Ok(true) = self.check_sync().await {
                    self.set_sync_status(Engine::SYNC_STATUS_FINISH_SYNC);
                    return;
                };
                tokio::time::sleep(Duration::from_secs(SLEEP_TIME)).await;
            }
            
        });
    }

    #[allow(dead_code)]
    pub async fn truncate_database(&self, mc_seq_no: u32) -> Result<()> {
        let mc_state = self.load_last_applied_mc_state().await?;
        let block_id = mc_state.find_block_id(mc_seq_no)?;
        let prev_block_id = self.load_block_prev1(&block_id)?;
        // check if previous state is present
        self.load_state(&prev_block_id).await
            .map_err(|err| error!("no previous block state present for {}", err))?;

        self.db().truncate_database(&block_id).await?;
        self.save_last_applied_mc_block_id(&prev_block_id)?;

        if let Some(block_id) = self.load_pss_keeper_mc_block_id()? {
            if block_id.seq_no > prev_block_id.seq_no {
                self.save_pss_keeper_mc_block_id(&prev_block_id)?;
            }
        }
        if let Some(block_id) = self.load_shard_client_mc_block_id()? {
            if block_id.seq_no > prev_block_id.seq_no {
                self.save_shard_client_mc_block_id(&prev_block_id)?;
            }
        }
        if let Some(block_id) = self.load_last_rotation_block_id()? {
            if block_id.seq_no > prev_block_id.seq_no {
                self.save_last_rotation_block_id(&prev_block_id)?;
            }
        }
        if let Some(block_id) = self.load_archives_gc_mc_block_id()? {
            if block_id.seq_no > prev_block_id.seq_no {
                self.save_archives_gc_mc_block_id(&prev_block_id)?;
            }
        }
        Ok(())
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
        (
            ShardStateStuff::deserialize_zerostate(
                zero_id.clone(), 
                &bytes,
                #[cfg(feature = "telemetry")]
                engine.engine_telemetry(),
                engine.engine_allocated()
            )?,
            bytes
        )
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
        let zs = ShardStateStuff::deserialize_zerostate(
            id.clone(), 
            &bytes,
            #[cfg(feature = "telemetry")]
            engine.engine_telemetry(),
            engine.engine_allocated()
        )?;
        let (zs, handle) = engine.store_zerostate(zs, &bytes).await?;
        engine.set_applied(&handle, id.seq_no()).await?;
        engine.process_full_state_in_ext_db(&zs).await?;
    }

    let (mc_zero_state, handle) = engine.store_zerostate(mc_zero_state, &mc_zs_bytes).await?;
    engine.set_applied(&handle, zero_id.seq_no()).await?;
    engine.process_full_state_in_ext_db(&mc_zero_state).await?;

    log::trace!("All static zero states had been load");
    return Ok(true)

}

async fn boot(engine: &Arc<Engine>, zerostate_path: Option<&str>) 
-> Result<(BlockIdExt, BlockIdExt, BlockIdExt, BlockIdExt)> {
    log::info!("Booting...");
    engine.set_sync_status(Engine::SYNC_STATUS_START_BOOT);

    if let Some(zerostate_path) = zerostate_path {
        load_zero_state(&engine, zerostate_path).await?;
    }

    let (last_applied_mc_block, cold) = match engine.load_last_applied_mc_state().await {
        Ok(state) => {
            engine.remp_capability.store(
                state.config_params()?.has_capability(GlobalCapabilities::CapRemp),
                Ordering::Relaxed
            );
            (state.block_id().clone(), false)
        }
        Err(err) => {
            log::debug!("before cold boot: {}", err);

            engine.acquire_stop(Engine::MASK_SERVICE_BOOT);
            let id = boot::cold_boot(engine.clone()).await;
            engine.release_stop(Engine::MASK_SERVICE_BOOT);
            let id = id?;
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

    let ss_keeper_mc_block = match engine.db().load_full_node_state(PSS_KEEPER_MC_BLOCK) {
        Ok(Some(id)) => id.deref().clone(),
        _ => {
            if !cold {
                fail!("INTERNAL ERROR: No shard states keeper MC block in warm boot")
            }
            engine.save_pss_keeper_mc_block_id(&last_applied_mc_block)?;
            log::info!("SS keeper MC block reset to last applied MC block");
            last_applied_mc_block.clone()
        }
    };

    let archives_gc_block = match engine.db().load_full_node_state(ARCHIVES_GC_BLOCK) {
        Ok(Some(id)) => id.deref().clone(),
        _ => {
            // for compatibility don't catch error if there isn't state
            engine.save_archives_gc_mc_block_id(&ss_keeper_mc_block)?;
            log::info!("Archives gc MC block reset to ss_keeper_mc_block");
            last_applied_mc_block.clone()
        }
    };

    engine.set_sync_status(Engine::SYNC_STATUS_FINISH_BOOT);
    log::info!("Boot complete.");
    log::info!("last_applied_mc_block: {}", last_applied_mc_block);
    log::info!("shard_client_mc_block: {}", shard_client_mc_block);
    log::info!("ss_keeper_mc_block: {}", ss_keeper_mc_block);
    log::info!("archives_gc_block: {}", archives_gc_block);
    Ok((last_applied_mc_block, shard_client_mc_block, ss_keeper_mc_block, archives_gc_block))
}

pub async fn run(
    node_config: TonNodeConfig,
    zerostate_path: Option<&str>, 
    ext_db: Vec<Arc<dyn ExternalDb>>,
    validator_runtime: tokio::runtime::Handle, 
    initial_sync_disabled : bool,
    force_check_db: bool,
    stopper: Arc<Stopper>,
) -> Result<(Arc<Engine>, tokio::task::JoinHandle<()>)> {
    log::info!("Engine::run");

    let consumer_config = node_config.kafka_consumer_config();
    let control_server_config = node_config.control_server()?;
    let remp_config = node_config.remp_config().clone();
    let vm_config = ValidatorManagerConfig::read_configs(
        node_config.unsafe_catchain_patches_files(),
        node_config.validation_countdown_mode()
    );
    let wc_from_config = node_config.workchain();
    let remp_client_pool = node_config.remp_config().remp_client_pool();

    // Create engine
    let engine = Engine::new(node_config, ext_db, initial_sync_disabled, force_check_db, stopper).await?;

    #[cfg(feature = "telemetry")]
    telemetry_logger(engine.clone());
    // Console service - run first to allow console to connect to generate new keys 
    // while node is looking for net
    if let Some(config) = control_server_config {
        let server = Server::ControlServer(
            ControlServer::with_params(
                config,
                DataSource::Engine(engine.clone()),
                engine.network().config_handler(),
                engine.network().config_handler(),
                Some(engine.network().clone())
            ).await?
        );
        engine.register_server(server)
    };

    // Messages from external DB (usually kafka)
    start_external_broadcast_process(engine.clone(), &consumer_config)?;

    let full_node_service = FullNodeOverlayService::new(Arc::clone(&engine) as Arc<dyn EngineOperations>);
    let full_node_service: Arc<dyn QueriesConsumer> = Arc::new(full_node_service);

    // Overlays, depends on 'workchain' option is set or not
    let network = engine.network();
    if let Some(wc) = &wc_from_config {
        log::info!("Processed workchain from config: {wc}");

        engine.add_full_node_overlay(*wc, SHARD_FULL, false).await?;
        network.add_consumer(
            &network.calc_overlay_id(*wc, SHARD_FULL)?.0, full_node_service.clone())?;

        if *wc != MASTERCHAIN_ID {
            engine.add_full_node_overlay(MASTERCHAIN_ID, SHARD_FULL, true).await?;
            network.add_consumer(
                &network.calc_overlay_id(MASTERCHAIN_ID, SHARD_FULL)?.0, full_node_service.clone())?;
        }
    } else {
        // Legacy mode.
        log::info!("Processed workchain was not specifed. LEGACY MODE");

        engine.add_full_node_overlay(BASE_WORKCHAIN_ID, SHARD_FULL, false).await?;
        network.add_consumer(
            &network.calc_overlay_id(BASE_WORKCHAIN_ID, SHARD_FULL)?.0, full_node_service.clone())?;

        engine.add_full_node_overlay(MASTERCHAIN_ID, SHARD_FULL, false).await?;
        network.add_consumer(
            &network.calc_overlay_id(MASTERCHAIN_ID, SHARD_FULL)?.0, full_node_service.clone())?;
    }

    // Boot
    let (mut last_applied_mc_block, mut shard_client_mc_block, ss_keeper_block, archives_gc_block) =
        boot(&engine, zerostate_path).await?;

    // Broadcasts (blocks, external messages etc.)
    if let Some(wc) = &wc_from_config {
        Arc::clone(&engine).listen_broadcasts(
            ShardIdent::with_tagged_prefix(*wc, SHARD_FULL)?,
            if *wc == MASTERCHAIN_ID {
                Engine::MASK_SERVICE_MASTERCHAIN_BROADCAST_LISTENER
            } else {
                Engine::MASK_SERVICE_SHARDCHAIN_BROADCAST_LISTENER
            }
        ).await?;
    } else {
        Arc::clone(&engine).listen_broadcasts(
            ShardIdent::masterchain(),
            Engine::MASK_SERVICE_MASTERCHAIN_BROADCAST_LISTENER
        ).await?;
        Arc::clone(&engine).listen_broadcasts(
            ShardIdent::with_tagged_prefix(BASE_WORKCHAIN_ID, SHARD_FULL)?,
            Engine::MASK_SERVICE_SHARDCHAIN_BROADCAST_LISTENER
        ).await?;
    }

    let _ = Engine::start_archives_gc(engine.clone(), archives_gc_block)?;

    engine.shard_states_keeper.clone().start(
        engine.clone(),
        last_applied_mc_block.clone(),
        shard_client_mc_block.clone(),
        ss_keeper_block
    ).await?;

    if remp_config.is_client_enabled() {
        engine.remp_client.as_ref()
            .ok_or_else(|| error!("Remp client is None, while beeing enabled in config"))?
            .clone().start(engine.clone(), remp_client_pool).await?;
    }
    // Start validator manager, which will start validator sessions when necessary
    start_validator_manager(
        Arc::clone(&engine) as Arc<dyn EngineOperations>,
        validator_runtime,
        vm_config,
        remp_config,
    );

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
    engine.set_sync_status(Engine::SYNC_STATUS_SYNC_BLOCKS);
    Engine::check_finish_sync(Arc::clone(&engine));
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
    tokio::spawn(async move {
        let mut elapsed = 0;
        let millis = 500;
        loop {
            tokio::time::sleep(Duration::from_millis(millis)).await;
            engine.engine_telemetry.storage.file_entries.update(
                engine.engine_allocated.storage.file_entries.load(Ordering::Relaxed)
            );    
            engine.engine_telemetry.storage.handles.update(
                engine.engine_allocated.storage.handles.load(Ordering::Relaxed)
            );    
            engine.engine_telemetry.storage.packages.update(
                engine.engine_allocated.storage.packages.load(Ordering::Relaxed)
            );    
            engine.engine_telemetry.storage.storage_cells.update(
                engine.engine_allocated.storage.storage_cells.load(Ordering::Relaxed)
            );  
            engine.engine_telemetry.awaiters.update(
                engine.engine_allocated.awaiters.load(Ordering::Relaxed)
            );        
            engine.engine_telemetry.catchain_clients.update(
                engine.engine_allocated.catchain_clients.load(Ordering::Relaxed)
            );        
            engine.engine_telemetry.storage.storage_cells.update(StorageCell::cell_count());
            engine.engine_telemetry.cells.update(Cell::cell_count()); 
            engine.engine_telemetry.overlay_clients.update(
                engine.engine_allocated.overlay_clients.load(Ordering::Relaxed)
            );        
            engine.engine_telemetry.peer_stats.update(
                engine.engine_allocated.peer_stats.load(Ordering::Relaxed)
            );        
            engine.engine_telemetry.shard_states.update(
                engine.engine_allocated.shard_states.load(Ordering::Relaxed)
            );        
            engine.engine_telemetry.top_blocks.update(
                engine.engine_allocated.top_blocks.load(Ordering::Relaxed)
            );        
            engine.engine_telemetry.validator_peers.update(
                engine.engine_allocated.validator_peers.load(Ordering::Relaxed)
            );        
            engine.engine_telemetry.validator_sets.update(
                engine.engine_allocated.validator_sets.load(Ordering::Relaxed)
            );        
            engine.telemetry_printer.try_print();
            elapsed += millis;
            if elapsed < Engine::TIMEOUT_TELEMETRY_SEC * 1000 {
                continue
            } else {
                elapsed = 0
            }
            let period = crate::full_node::telemetry::TPS_PERIOD_1;
            let tps_1 = engine.tps_counter.calc_tps(period)
                .unwrap_or_else(|e| { 
                    log::error!("Can't calc tps for {}sec period: {}", period, e);
                    0
                });
            let period = crate::full_node::telemetry::TPS_PERIOD_2;
            let tps_2 = engine.tps_counter.calc_tps(period)
                .unwrap_or_else(|e| { 
                    log::error!("Can't calc tps for {}sec period: {}", period, e);
                    0
                });
            log::debug!(
                target: "telemetry",
                "Full node's telemetry:\n{}",
                engine.full_node_telemetry().report(tps_1, tps_2)
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
                engine.full_node_service_telemetry().report(Engine::TIMEOUT_TELEMETRY_SEC)
            );
            log::debug!(
                target: "telemetry",
                "Full node client's telemetry:\n{}",
                engine.network.telemetry().report(Engine::TIMEOUT_TELEMETRY_SEC)
            );
            if engine.remp_client.is_some() {
                log::debug!(
                    target: "telemetry",
                    "Remp client's telemetry:\n{}",
                    engine.remp_client_telemetry().report()
                );
            }
            if engine.remp_service().is_some() {
                log::debug!(
                    target: "telemetry",
                    "Remp core's telemetry:\n{}",
                    engine.remp_core_telemetry().report()
                );
            }
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
    pub static ref STATSD: statsd::client::Client = {
        let mut statsd_endp = env::var("STATSD_DOMAIN").expect("STATSD_DOMAIN env variable not found");
        let statsd_port = env::var("STATSD_PORT").expect("STATSD_PORT env variable not found");
        statsd_endp.push_str(&statsd_port);
        let statsd_client = match statsd::client::Client::new(statsd_endp, "rnode"){
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
