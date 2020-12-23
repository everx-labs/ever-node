use crate::{
    define_db_prop,
    block::{convert_block_id_ext_api2blk, BlockStuff, BlockIdExtExtention},
    block_proof::BlockProofStuff,
    config::{TonNodeConfig, KafkaConsumerConfig},
    db::{BlockHandle, InternalDb, InternalDbConfig, InternalDbImpl, NodeState},
    engine_traits::{ExternalDb, OverlayOperations, EngineOperations, PrivateOverlayOperations},
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
    ext_messages::MessagesPool,
    validator::validator_manager,
    shard_blocks::ShardBlocksPool,
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
use std::{
    sync::{Arc, atomic::{AtomicBool, AtomicU32, Ordering}},
    ops::Deref,
    time::Duration,
    convert::TryInto
};

use ton_block::{
    self, ShardIdent, BlockIdExt, MASTERCHAIN_ID, SHARD_FULL, BASE_WORKCHAIN_ID, ShardDescr
};
use ton_types::{error, Result, fail};
use tokio::{time::delay_for, task::JoinHandle};
use lazy_static;
use ton_api::ton::ton_node::{
    Broadcast, broadcast::{BlockBroadcast, ExternalMessageBroadcast, NewShardBlockBroadcast}
};

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
    download_block_awaiters: AwaitersPool<BlockIdExt, BlockStuff>,
    external_messages: MessagesPool,

    zero_state_id: BlockIdExt,
    init_mc_block_id: BlockIdExt,
    initial_sync_disabled: bool,
    network: Arc<NodeNetwork>,
    aux_mc_shard_states: lockfree::map::Map<u32, ShardStateStuff>,
    shard_states: lockfree::map::Map<ShardIdent, ShardStateStuff>,
    shard_blocks: ShardBlocksPool,
    last_known_mc_block_seqno: AtomicU32,
    last_known_keyblock_seqno: AtomicU32,
    will_validate: AtomicBool,
}

struct DownloadContext<'a, T> {
    attempts: Attempts,
    client: Arc<dyn FullNodeOverlayClient>,
    downloader: Arc<dyn Downloader<Item = T>>,
    id: &'a BlockIdExt,
    limit: Option<u32>, 
    name: &'a str,
    timeout: Option<(u64, u64)> // (current, max)
}

impl <T> DownloadContext<'_, T> {

    async fn download(&mut self) -> Result<T> {
        loop {
            match self.downloader.try_download(self).await {
                Err(e) => self.log(format!("{}", e).as_str()),
                Ok(None) => self.log("got no_data"),
                Ok(Some(ret)) => break Ok(ret)
            }
            self.attempts.count += 1;
            if let Some(limit) = &self.limit {
                if &self.attempts.count > limit {
                    fail!("Downloader: out of attempts");
                }
            }
            if let Some((current, max)) = &mut self.timeout {
                *current = (*max).max(*current * 11) / 10;
                futures_timer::Delay::new(Duration::from_millis(*current)).await;
            }
        }
    }

    fn log(&self, msg: &str) {
       log::log!(
           if self.attempts.count > 10 {
               log::Level::Warn
           } else {
               log::Level::Debug
           },
           "{} (attempt {}): id: {}, {}",
           self.name, self.attempts.count, self.id, msg
       )
    }

}

#[async_trait::async_trait]
trait Downloader: Send + Sync {
    type Item;
    async fn try_download(
        &self, 
        context: &DownloadContext<'_, Self::Item>
    ) -> Result<Option<Self::Item>>;
}

struct BlockDownloader;

#[async_trait::async_trait]
impl Downloader for BlockDownloader {
    type Item = (BlockStuff, BlockProofStuff);
    async fn try_download(
        &self, 
        context: &DownloadContext<'_, Self::Item>
    ) -> Result<Option<Self::Item>> {
        context.client.download_block_full(context.id, &context.attempts).await        
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
        context: &DownloadContext<'_, Self::Item>
    ) -> Result<Option<Self::Item>> {
        context.client.download_block_proof(
            context.id, 
            self.is_link, 
            self.key_block, 
            &context.attempts
        ).await        
    }
}              

struct NextBlockDownloader;

#[async_trait::async_trait]
impl Downloader for NextBlockDownloader {
    type Item = (BlockStuff, BlockProofStuff);
    async fn try_download(
        &self, 
        context: &DownloadContext<'_, Self::Item>
    ) -> Result<Option<Self::Item>> {
        context.client.download_next_block_full(context.id, &context.attempts).await        
    }    
}  

struct ZeroStateDownloader;

#[async_trait::async_trait]
impl Downloader for ZeroStateDownloader {
    type Item = (ShardStateStuff, Vec<u8>);
    async fn try_download(
        &self, 
        context: &DownloadContext<'_, Self::Item>
    ) -> Result<Option<Self::Item>> {
        context.client.download_zero_state(context.id, &context.attempts).await
    }
}

impl Engine {
    pub async fn new(general_config: TonNodeConfig, ext_db: Vec<Arc<dyn ExternalDb>>) -> Result<Arc<Self>> {
         log::info!("Creating engine...");

        let db_directory = general_config.internal_db_path().unwrap_or_else(|| {"node_db"});
        let db_config = InternalDbConfig { db_directory: db_directory.to_string() };
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
        let network = Arc::new(NodeNetwork::new(general_config, db.clone()).await?);
        network.clone().start().await?;

        log::info!("Engine is created.");

        let engine = Arc::new(Engine {
            db,
            ext_db,
            overlay_operations: network.clone(),
            shard_states_awaiters: AwaitersPool::new(),
            block_applying_awaiters: AwaitersPool::new(),
            next_block_applying_awaiters: AwaitersPool::new(),
            download_block_awaiters: AwaitersPool::new(),
            external_messages: MessagesPool::new(),
            zero_state_id,
            init_mc_block_id,
            initial_sync_disabled,
            network: network.clone(),
            aux_mc_shard_states: Default::default(),
            shard_states: Default::default(),
            shard_blocks: ShardBlocksPool::new(),
            last_known_mc_block_seqno: AtomicU32::new(0),
            last_known_keyblock_seqno: AtomicU32::new(0),
            will_validate: AtomicBool::new(false),
        });

        let full_node_service: Arc<dyn QueriesConsumer> = Arc::new(
            FullNodeOverlayService::new(Arc::clone(&engine) as Arc<dyn EngineOperations>)
        );
        network.add_consumer(
            &network.calc_overlay_id(MASTERCHAIN_ID, SHARD_FULL)?.0,
            Arc::clone(&full_node_service)
        )?;
        network.add_consumer(
            &network.calc_overlay_id(BASE_WORKCHAIN_ID, SHARD_FULL)?.0,
            Arc::clone(&full_node_service)
        )?;

        engine.get_full_node_overlay(BASE_WORKCHAIN_ID, SHARD_FULL).await?;

        Ok(engine)
    }

    pub fn db(&self) -> &Arc<dyn InternalDb> { &self.db }

    pub fn validator_network(&self) -> Arc<dyn PrivateOverlayOperations> { self.network.clone() }

    pub fn ext_db(&self) -> &Vec<Arc<dyn ExternalDb>> { &self.ext_db }

    pub fn zero_state_id(&self) -> &BlockIdExt { &self.zero_state_id }

    pub fn init_mc_block_id(&self) -> &BlockIdExt {&self.init_mc_block_id}

    pub fn initial_sync_disabled(&self) -> bool {self.initial_sync_disabled}

    pub fn aux_mc_shard_states(&self) -> &lockfree::map::Map<u32, ShardStateStuff> {
        &self.aux_mc_shard_states
    }

    pub fn shard_states(&self) -> &lockfree::map::Map<ShardIdent, ShardStateStuff> {
        &self.shard_states
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

    pub fn download_block_awaiters(&self) -> &AwaitersPool<BlockIdExt, BlockStuff> {
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

    pub async fn download_and_apply_block_worker(self: Arc<Self>, handle: &BlockHandle, mc_seq_no: u32, pre_apply: bool) -> Result<()> {

        if handle.applied() || pre_apply && handle.state_inited() {
            log::trace!(
                "download_and_apply_block_worker(pre_apply: {}): block is already applied {}",
                pre_apply,
                handle.id()
            );
            return Ok(());
        }

        let block = if handle.data_inited() {
            self.load_block(handle).await?
        } else {
            let now = std::time::Instant::now();
            log::trace!(
                "Start downloading block for {}apply... {}",
                if pre_apply { "pre-" } else { "" },
                handle.id()
            );

            // for pre-apply only 10 attempts, for apply - infinity
            let attempts = if pre_apply { 
                Some(10) 
            } else { 
                None
            };
            let (block, proof) = self.download_block_worker(handle.id(), attempts).await?;
            let downloading_time = now.elapsed().as_millis();
            let now = std::time::Instant::now();
            proof.check_proof(self.deref()).await?;
            self.store_block_proof(handle, &proof).await?;
            self.store_block(handle, &block).await?;

            log::trace!("Downloaded block for {}apply {} TIME download: {}ms, check & save: {}", 
                if pre_apply { "pre-" } else { "" }, 
                block.id(),
                downloading_time, 
                now.elapsed().as_millis(),
            );
            block
        };

        self.apply_block_worker(handle, &block, mc_seq_no, pre_apply).await?;

        Ok(())
    }

    pub async fn apply_block_worker(
        self: Arc<Self>,
        handle: &BlockHandle,
        block: &BlockStuff,
        mc_seq_no: u32,
        pre_apply: bool,
    ) -> Result<()> {
        if handle.applied() || pre_apply && handle.state_inited() {
            log::trace!(
                "apply_block_worker(pre_apply: {}): block is already applied {}",
                pre_apply,
                handle.id()
            );
            return Ok(());
        }

        log::trace!("Start {}applying block... {}", if pre_apply { "pre-" } else { "" }, block.id());

        // TODO fix trash with Arc and clone
        apply_block(handle, block, mc_seq_no, &(self.clone() as Arc<dyn EngineOperations>), pre_apply).await?;

        if !pre_apply {
            self.set_applied(handle, mc_seq_no).await?;
        }

        let ago = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?.as_secs() as i32 - block.gen_utime()? as i32;
        if block.id().shard().is_masterchain() {
            if !pre_apply {
                self.store_last_applied_mc_block_id(block.id()).await?;
                STATSD.gauge("last_applied_mc_block", block.id().seq_no() as f64);

                self.shard_blocks().update_shard_blocks(&self.load_state(block.id()).await?)?;

                let (prev_id, prev2_id_opt) = block.construct_prev_id()?;
                if prev2_id_opt.is_some() {
                    fail!("UNEXPECTED error: master block refers two previous blocks");
                }
                let id = handle.id().clone();
                self.next_block_applying_awaiters.do_or_wait(&prev_id, async move { Ok(id) }).await?;
            }

            log::info!(
                "{} block {}, {} seconds old",
                if pre_apply { "Pre-applied" } else { "Applied" },
                block.id(),
                ago
            );
        } else {
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
                    Ok(brodcast) => {
                        match brodcast {
                            Broadcast::TonNode_BlockBroadcast(broadcast) => {
                                self.clone().process_block_broadcast(broadcast);
                            },
                            Broadcast::TonNode_ExternalMessageBroadcast(broadcast) => {
                                self.process_ext_msg_broadcast(broadcast);
                            },
                            Broadcast::TonNode_IhrMessageBroadcast(broadcast) => {
                                log::trace!("TonNode_IhrMessageBroadcast: {:?}", broadcast);
                            },
                            Broadcast::TonNode_NewShardBlockBroadcast(broadcast) => {
                                self.clone().process_new_shard_block_broadcast(broadcast);
                            },
                        }
                    }
                }
            }
        });
        Ok(())
    }

    fn process_block_broadcast(self: Arc<Self>, broadcast: Box<BlockBroadcast>) {
        // because of ALL blocks-broadcasts received in one task - spawn for each block
        log::trace!("Processing block broadcast {}", broadcast.id);
        let engine = self.clone() as Arc<dyn EngineOperations>;
        tokio::spawn(async move {
            if let Err(e) = process_block_broadcast(&engine, &broadcast).await {
                log::error!("Error while processing block broadcast {}: {}", broadcast.id, e);
            } else {
                log::trace!("Processed block broadcast {}", broadcast.id);
            }
        });
    }

    fn process_ext_msg_broadcast(&self, broadcast: Box<ExternalMessageBroadcast>) {
        // just add to list
        log::trace!("Processing ext message broadcast {}bytes", broadcast.message.data.0.len());
        if let Err(e) = self.new_external_message_raw(&broadcast.message.data.0) {
            log::debug!("Error while processing ext message broadcast {}bytes: {}",
                broadcast.message.data.0.len(), e);
        } else {
            log::trace!("Processed ext message broadcast {}bytes", broadcast.message.data.0.len());
        }
    }

    fn process_new_shard_block_broadcast(self: Arc<Self>, broadcast: Box<NewShardBlockBroadcast>) {
        let id = broadcast.block.block.clone();
        if self.is_validator() {
            log::trace!("Processing new shard block broadcast {}", id);
            tokio::spawn(async move {
                if self.check_sync().await.unwrap_or(false) {
                    if let Err(e) = self.process_new_shard_block(broadcast).await {
                        log::error!("Error while processing new shard block broadcast {}: {}", id, e);
                    } else {
                        log::trace!("Processed new shard block broadcast {}", id);
                    }
                } else {
                    log::trace!("Processing new shard block broadcast {} NO SYNC", id);
                }
            });
        } else {
            log::trace!("Processing new shard block broadcast {} NOT A VALIDATOR", id);
        }
    }

    async fn process_new_shard_block(self: Arc<Self>, broadcast: Box<NewShardBlockBroadcast>) -> Result<()> {
        let id = (&broadcast.block.block).try_into()?;
        let cc_seqno = broadcast.block.cc_seqno as u32;
        let data = broadcast.block.data.0;
        if self.shard_blocks.add_shard_block(&id, cc_seqno, data, self.deref()).await? {
            let handle = self.load_block_handle(&id)?;
            self.apply_block(handle.deref(), None, 0, true).await?;
        }
        Ok(())              
    }                  

    async fn create_download_context<'a, T>(
         &'a self,
         downloader: Arc<dyn Downloader<Item = T>>,
         id: &'a BlockIdExt, 
         limit: Option<u32>,
         name: &'a str,
         timeout: Option<(u64, u64)>
    ) -> Result<DownloadContext<'a, T>> {
        let ret = DownloadContext {
            attempts: Attempts {
                limit: 3,
                count: 0
            },
            client: self.get_full_node_overlay(
                id.shard().workchain_id(),
                id.shard().shard_prefix_with_tag()
            ).await?,
            downloader,
            id,
            limit,
            name,
            timeout
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
            "download_next_block_worker", 
            Some((50, 1000))
        ).await?.download().await
/*
        let client = self.get_full_node_overlay(
            prev_id.shard().workchain_id(),
            prev_id.shard().shard_prefix_with_tag()
        ).await?;
        let mut attempts = Attempts {
            total_limit: limit,
            limit: 3,
            count: 0
        };
        let mut timeout = 50.0;
        loop {
            match client.download_next_block_full(&prev_id, &attempts).await {
                Err(e) => {
                    log::log!(
                        if attempts.count > 30 { log::Level::Warn } else { log::Level::Debug },
                        "download_next_block_worker (attempt {}): prev_id: {}, {}",
                        attempts.count, prev_id, e
                    );
                },
                Ok(block_and_proof) => break Ok(block_and_proof)
            }
            attempts.count += 1;
            if (limit > 0) && (attempts.count > limit) {
                fail!("download_next_block_worker: out of attempts");
            }
            timeout = 1000.0_f32.max(timeout * 1.1);
            futures_timer::Delay::new(Duration::from_millis(timeout as u64)).await;
        }
*/
    }

    pub async fn download_block_worker(
        &self,
        id: &BlockIdExt,
        limit: Option<u32>
    ) -> Result<(BlockStuff, BlockProofStuff)> {
        self.create_download_context(
            Arc::new(BlockDownloader),
            id, 
            limit, 
            "download_block_worker", 
            None
        ).await?.download().await
/*        
        let client = self.get_full_node_overlay(
            id.shard().workchain_id(),
            ton_block::SHARD_FULL, // FIXME: which overlay id use?   id.shard().shard_prefix_with_tag()
        ).await?;
        let mut attempts = Attempts {
            total_limit: limit,
            limit: 0,
            count: 0
        };
        loop {
            match client.download_block_full(&id, &attempts).await {
                Err(e) => {
                    log::log!(
                        if attempts.count > 10 { log::Level::Warn } else { log::Level::Debug },
                        "download_block_worker (attempt {}): id: {}, {:?}",
                        attempts.count, id, e
                    );
                },
                Ok(None) => {
                    log::log!(
                        if attempts.count > 10 { log::Level::Warn } else { log::Level::Debug },
                        "download_block_worker (attempt {}): id: {}, got no data",
                        attempts.count, id
                    );
                },
                Ok(Some((block, proof))) => break Ok((block, proof))
            }
            attempts.count += 1;
            if (limit > 0) && (attempts.count > limit) {
                fail!("download_block_worker: out of attempts");
            }
            futures_timer::Delay::new(Duration::from_millis(10)).await;
        }
*/
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
            "download_block_proof_worker", 
            None
        ).await?.download().await
/*
        let client = self.get_full_node_overlay(
            id.shard().workchain_id(),
            ton_block::SHARD_FULL, // FIXME: which overlay id use?   id.shard().shard_prefix_with_tag()
        ).await?;
        let mut attempts = Attempts {
            total_limit: limit,
            limit: 0,
            count: 0
        };
        loop {
            match client.download_block_proof(&id, is_link, key_block, &attempts).await {
                Err(e) => {
                    log::log!(
                        if attempts.count > 10 { log::Level::Warn } else { log::Level::Debug },
                        "download_block_proof_worker (attempt {}): id: {}, {:?}",
                        attempts.count, id, e
                    );
                },
                Ok(None) => {
                    log::log!(
                        if attempts.count > 10 { log::Level::Warn } else { log::Level::Debug },
                        "download_block_proof_worker (attempt {}): id: {}, got no data",
                        attempts.count, id
                    );
                }
                Ok(Some(proof)) => break Ok(proof)
            }
            attempts.count += 1;
            if attempts.total_limit > 0 && attempts.count > attempts.total_limit {
                fail!("download_block_proof_worker: out of attempts");
            }
            futures_timer::Delay::new(Duration::from_millis(10)).await;
        }
*/
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
            "download_zerostate_worker", 
            Some((10, 3000))
        ).await?.download().await
/*
        let mut timeout = 1.0;
        loop {
            let overlay = self.get_full_node_overlay(id.shard().workchain_id(), id.shard().shard_prefix_with_tag()).await?;
            let attempts = Attempts {
                total_limit: 0,
                limit: 3,
                count: 0
            };
            match overlay.download_zero_state(id, &attempts).await {
                Err(e) => log::warn!("Can't load zerostate {}: {}", id, e),
                Ok(None) => log::warn!("Can't load zerostate {}: none returned", id),
                Ok(Some(zs)) => return Ok(zs)
            }
            timeout = 3000.0_f32.max(timeout * 1.2);
            tokio::time::delay_for(Duration::from_millis(timeout as u64)).await;
        }
*/
    }

    pub(crate) async fn check_sync(&self) -> Result<bool> {

        let shards_client = self.load_shards_client_mc_block_id().await?;
        let last_mc_id = self.load_last_applied_mc_block_id().await?;
        if shards_client.seq_no() + 16 < last_mc_id.seq_no() {
            return Ok(false);
        }

        let last_mc_handle = self.load_block_handle(&last_mc_id)?;
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
                    false // zerostate are saved another way (see boot)
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

pub(crate) async fn load_zero_state(engine: &Arc<Engine>, path: &str) -> Result<bool> {
    let handle = engine.load_block_handle(engine.zero_state_id())?;

    if handle.applied() {
        return Ok(false);
    }

    let (mc_zero_state, mc_zs_bytes) = {
        log::trace!("loading mc static zero state {}", handle.id());
        let path = format!("{}/{:x}.boc", path, handle.id().file_hash());
        let bytes = tokio::fs::read(&path).await
            .map_err(|err| error!("Cannot read mc zerostate {}: {}", path, err))?;
        (ShardStateStuff::deserialize_zerostate(handle.id().clone(), &bytes)?, bytes)
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
        let handle = engine.load_block_handle(id)?;
        if !handle.applied() {
            log::trace!("loading wc static zero state {}", id);
            let path = format!("{}/{:x}.boc", path, id.file_hash());
            let bytes = tokio::fs::read(&path).await
                .map_err(|err| error!("Cannot read zerostate {}: {}", path, err))?;
            let zs = ShardStateStuff::deserialize_zerostate(handle.id().clone(), &bytes)?;
            engine.store_zerostate(&handle, &zs, &bytes).await?;
            engine.set_applied(&handle, handle.id().seq_no()).await?;
        }
    }

    engine.store_zerostate(&handle, &mc_zero_state, &mc_zs_bytes).await?;
    engine.set_applied(&handle, handle.id().seq_no()).await?;

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

pub async fn run(node_config: TonNodeConfig, zerostate_path: Option<&str>, ext_db: Vec<Arc<dyn ExternalDb>>) -> Result<()> {
    log::info!("Engine::run");

    let consumer_config = node_config.kafka_consumer_config();

    //// Create engine
    let engine = Engine::new(node_config, ext_db).await?;

    //// Boot
    let (mut last_mc_block, mut shards_client_mc_block, pss_keeper_block) = boot(&engine, zerostate_path).await?;

    //// Start services
    start_external_broadcast_process(engine.clone(), &consumer_config)?;

    // TODO: Temporary disabled GC because of DB corruption. Enable after fix.
    // Arc::clone(&engine).start_gc_scheduler();

    // broadcasts
    Arc::clone(&engine).listen_broadcasts(ShardIdent::masterchain()).await?;
    Arc::clone(&engine).listen_broadcasts(ShardIdent::with_tagged_prefix(BASE_WORKCHAIN_ID, SHARD_FULL)?).await?;

    let _ = Engine::start_persistent_states_keeper(engine.clone(), pss_keeper_block)?;

    // start validator manager, which will start validator sessions when necessary
    start_validator(Arc::clone(&engine));

    // sync by archives
    if !engine.check_sync().await? {
        crate::sync::start_sync(Arc::clone(&engine) as Arc<dyn EngineOperations>).await?;
        last_mc_block = LastMcBlockId::load_from_db(engine.db().deref())
            .and_then(|id| (&id.0).try_into())?;
        shards_client_mc_block = ShardsClientMcBlockId::load_from_db(engine.db().deref())
            .and_then(|id| (&id.0).try_into())?;
    }

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
                log::info!("started validator...");
                if let Err(e) = validator_manager::start_validator_manager(engine.clone()) {
                    log::error!("{:?}", e);
                }
                return;
            }
            delay_for(Duration::from_secs(CHECK_VALIDATOR_TIMEOUT)).await;
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
