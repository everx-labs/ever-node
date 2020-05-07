use crate::{
    define_db_prop,
    block::{
        convert_block_id_ext_api2blk,
        BlockStuff, BlockIdExtExtention
    },
    block_proof::BlockProofStuff,
    boot::cold_sync_stub,
    config::{TonNodeConfig, KafkaConsumerConfig},
    db::{BlockHandle, InternalDb, InternalDbConfig, InternalDbImpl, NodeState},
    engine_traits::{ExternalDb, GetOverlay, EngineOperations},
    error::NodeError,
    full_node::{
        apply_block::apply_block, shard_client::{process_block_broadcast, ShardClient},
    },
    network::node_network::{FullNodeOverlayClient, NodeNetwork},
    shard_state::ShardStateStuff,
    types::awaiters_pool::AwaitersPool,
};

#[cfg(feature = "external_db")]
use crate::external_db::kafka_consumer::KafkaConsumer;


use std::{sync::Arc, ops::Deref, time::Duration, collections::HashMap};
use ton_block::{self, ShardIdent, BlockIdExt};
use ton_types::{error, Result, fail};

define_db_prop!(LastMcBlockId,   "LastMcBlockId",   ton_api::ton::ton_node::blockidext::BlockIdExt);
define_db_prop!(GcMcBlockId,     "GcMcBlockId",     ton_api::ton::ton_node::blockidext::BlockIdExt);
define_db_prop!(ClientMcBlockId, "ClientMcBlockId", ton_api::ton::ton_node::blockidext::BlockIdExt);

pub struct Engine {
    db: Arc<dyn InternalDb>,
    ext_db: Vec<Arc<dyn ExternalDb>>,
    get_overlay: Arc<dyn GetOverlay>,

    shard_states_awaiters: AwaitersPool<BlockIdExt, ShardStateStuff>,
    block_applying_awaiters: AwaitersPool<BlockIdExt, ()>,
    download_block_awaiters: AwaitersPool<BlockIdExt, (BlockStuff, BlockProofStuff)>,
    download_block_proof_link_awaiters: AwaitersPool<BlockIdExt, BlockProofStuff>,
    download_next_block_awaiters: AwaitersPool<BlockIdExt, (BlockStuff, BlockProofStuff)>,

    //running_shards: Arc<lockfree::set::Set<u64>>,
    init_mc_block_id: BlockIdExt,
    zero_state_id: BlockIdExt,
}

impl Engine {

    pub(crate) const MAX_EXTERNAL_MESSAGE_DEPTH: u16 = 512;
    pub(crate) const MAX_EXTERNAL_MESSAGE_SIZE: usize = 65535;

    pub async fn new(mut general_config: TonNodeConfig, ext_db: Vec<Arc<dyn ExternalDb>>) -> Result<Arc<Self>> {
        let db_config = InternalDbConfig { db_directory: "node_db".to_owned() };
        let db = Arc::new(InternalDbImpl::new(db_config)?);

        let global_config = general_config.load_global_config()?;
        let zero_state_id = global_config.zero_state().expect("check zero state settings");
        let init_mc_block_id = global_config.init_block()?.unwrap_or_else(|| zero_state_id.clone());

        let network: Arc<dyn GetOverlay> = 
            Arc::new(NodeNetwork::new(&mut general_config, db.clone()).await?);
        network.clone().start().await?;

        Ok(Arc::new(Engine {
            db: db.clone(),
            ext_db,
            get_overlay: network.clone(),
            shard_states_awaiters: Default::default(),
            block_applying_awaiters: Default::default(),
            download_block_awaiters: Default::default(),
            download_next_block_awaiters: Default::default(),
            download_block_proof_link_awaiters: Default::default(),
            //running_shards: Arc::new(lockfree::set::Set::new()),
            init_mc_block_id,
            zero_state_id,
        }))
    }

    pub fn db(&self) -> &dyn InternalDb {
        self.db.deref()
    }

    pub fn ext_db(&self) -> &Vec<Arc<dyn ExternalDb>> {
        &self.ext_db
    }

    pub fn init_mc_block_id(&self) -> &BlockIdExt {
        &self.init_mc_block_id
    }

    pub fn zero_state_id(&self) -> &BlockIdExt { &self.zero_state_id }

    pub async fn get_masterchain_overlay(&self) -> Result<Arc<dyn FullNodeOverlayClient>> {
        self.get_full_node_overlay(ton_block::MASTERCHAIN_ID, ton_block::SHARD_FULL).await
    }

    pub async fn get_full_node_overlay(&self, workchain: i32, shard: u64) -> Result<Arc<dyn FullNodeOverlayClient>> {
        let id = self.get_overlay.calc_overlay_short_id(workchain, shard)?;
        self.get_overlay.get_overlay(&id).await
    }

    pub fn shard_states_awaiters(&self) -> &AwaitersPool<BlockIdExt, ShardStateStuff> {
        &self.shard_states_awaiters
    }

    pub fn block_applying_awaiters(&self) -> &AwaitersPool<BlockIdExt, ()> {
        &self.block_applying_awaiters
    }

    pub fn download_block_awaiters(&self) -> &AwaitersPool<BlockIdExt, (BlockStuff, BlockProofStuff)> {
        &self.download_block_awaiters
    }

    pub fn download_block_proof_link_awaiters(&self) -> &AwaitersPool<BlockIdExt, BlockProofStuff> {
        &self.download_block_proof_link_awaiters
    }

    pub fn download_next_block_awaiters(&self) -> &AwaitersPool<BlockIdExt, (BlockStuff, BlockProofStuff)> {
        &self.download_next_block_awaiters
    }

    async fn apply_block_worker(self: Arc<Self>, handle: &BlockHandle, block: &BlockStuff) -> Result<()> {

        log::trace!("Start applying block... {}", block.id());

        // TODO fix trash with Arc and clone
        apply_block(handle.deref(), block, &(self.clone() as Arc<dyn EngineOperations>)).await?;
        self.deref().db.store_block_applied(block.id())?;

        if block.id().shard().is_masterchain() {
            log::info!("Applied block {}", block.id());
        } else {
            let master_ref = block
                .block()
                .read_info()?
                .read_master_ref()?
                .ok_or_else(|| NodeError::InvalidData(format!(
                    "Block {} doesn't contain masterchain block extra", block.id(),
                )))?;
            log::info!("Applied block {} ref_mc_block: {}", block.id(), master_ref.master.seq_no);
        }

        if block.id().is_masterchain() {
            LastMcBlockId(block.id_api().clone()).store_to_db(self.db.deref())?;

            let shards_blocks = block.shards_blocks()?;
            tokio::spawn(async move {
                if let Err(e) = self.process_shards_info_from_mc(shards_blocks).await {
                    log::error!("Unexpected error in process_shards_info_from_mc: {:?}", e);
                }
            });
        }

        Ok(())
    }

    async fn process_shards_info_from_mc(self: Arc<Self>, shards_blocks: HashMap<ShardIdent, BlockIdExt>) -> Result<()> {
        for (_shard_ident, block_id) in shards_blocks.iter() {

            // Need to run all new overlays from new shard's id to full id
            // for example for shard b01011 (with tag) -> b01011 b0101 b011 b01 b1
            /* 
               it is strange but ALL blocks broadcasts are received with master chain overlay prefix,
               so let's listen only masterchain

            let mut shard_ident = shard_ident.clone();
            while self.running_shards.get(&shard_ident.shard_prefix_with_tag()).is_none() {
                // is it ok somebody else inserts the same value same moment - this case Err is returned
                let shard_prefix = shard_ident.shard_prefix_with_tag();
                if self.running_shards.insert(shard_prefix).is_ok() {
                    let engine = self.clone();
                    let shard_ident = shard_ident.clone();
                    if let Err(e) = engine.clone().listen_shard_blocks_broadcasts(shard_ident).await {
                        log::error!("Error while listen_shard_blocks_broadcasts {}: {}",
                            shard_ident, e);
                        engine.running_shards.remove(&shard_prefix);
                    }
                }
                if shard_ident.is_full() {
                    break;
                }
                shard_ident = shard_ident.merge()?;
            }*/
            let shard_block_handle = self.db.load_block_handle(block_id)?;
            if !shard_block_handle.applied() {
                let engine = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = engine.apply_block(shard_block_handle.deref(), None).await {
                        log::error!("Error while apply shard block {}: {}",
                            shard_block_handle.id(), e);
                    }
                });
            }
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

    pub async fn apply_block_do_or_wait(self: Arc<Self>, handle: &BlockHandle, block: &BlockStuff) -> Result<()> {
        if !handle.applied() {
            self.block_applying_awaiters().do_or_wait(
                block.id(),
                self.clone().apply_block_worker(handle.deref(), block)
            ).await?;
        }
        Ok(())
    }

    pub async fn download_next_block_worker(&self, prev_id: &BlockIdExt) -> Result<(BlockStuff, BlockProofStuff)> {
        if !prev_id.is_masterchain() {
            fail!(NodeError::InvalidArg("`download_next_block` is allowed only for master chain".to_string()))
        }
        let client = self.get_full_node_overlay(
            prev_id.shard().workchain_id(),
            prev_id.shard().shard_prefix_with_tag()
        ).await?;
        let mut attempt = 0_u32;
        loop {
            match client.download_next_block_full(&prev_id, 3).await {
                Err(e) => {
                    log::log!(
                        if attempt > 10 { log::Level::Warn } else { log::Level::Debug },
                        "download_next_block: download_next_block_full (attempt {}): prev_id: {}, {:?}",
                        attempt, prev_id, e
                    );
                    attempt += 1;
                    futures_timer::Delay::new(Duration::from_millis(1000)).await;
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
        let mut attempt = 0_u32;
        loop {
            match client.download_block_full(&id, 3).await {
                Err(e) => {
                    log::log!(
                        if attempt > 10 { log::Level::Warn } else { log::Level::Debug },
                        "download_block: download_block_full (attempt {}): id: {}, {:?}",
                        attempt, id, e
                    );
                    attempt += 1;
                    futures_timer::Delay::new(Duration::from_millis(1000)).await;
                },
                Ok(None) => {
                    log::log!(
                        if attempt > 10 { log::Level::Warn } else { log::Level::Debug },
                        "download_block: download_block_full (attempt {}): id: {}, got no data",
                        attempt, id
                    );
                    attempt += 1;
                    futures_timer::Delay::new(Duration::from_millis(1000)).await;
                }
                Ok(Some((block, proof))) => break Ok((block, proof))
            }
        }
    }
    pub async fn download_block_proof_link_worker(&self, id: &BlockIdExt) -> Result<BlockProofStuff> {
        let client = self.get_full_node_overlay(
            id.shard().workchain_id(),
            ton_block::SHARD_FULL, // FIXME: which overlay id use?   id.shard().shard_prefix_with_tag()
        ).await?;
        let mut attempt = 0_u32;
        loop {
            match client.download_block_proof(&id, true, 3).await {
                Err(e) => {
                    log::log!(
                        if attempt > 10 { log::Level::Warn } else { log::Level::Debug },
                        "download_block: download_block_proof (attempt {}): id: {}, {:?}",
                        attempt, id, e
                    );
                    attempt += 1;
                    futures_timer::Delay::new(Duration::from_millis(1000)).await;
                },
                Ok(None) => {
                    log::log!(
                        if attempt > 10 { log::Level::Warn } else { log::Level::Debug },
                        "download_block: download_block_proof (attempt {}): id: {}, got no data",
                        attempt, id
                    );
                    attempt += 1;
                    futures_timer::Delay::new(Duration::from_millis(1000)).await;
                }
                Ok(Some(proof)) => break Ok(proof)
            }
        }
    }
}

pub async fn run(general_config: TonNodeConfig, ext_db: Vec<Arc<dyn ExternalDb>>) -> Result<()> {

    let consumer_config = general_config.kafka_consumer_config();
    //// Create engine

    let engine = Engine::new(general_config, ext_db).await?;

    //// Sync

    let last_mc_block = match LastMcBlockId::load_from_db(engine.db().deref()) {
        Ok(id) => convert_block_id_ext_api2blk(&id.0)?,
        Err(_) => cold_sync_stub(engine.clone()).await?
    };


    //// Start services
    start_external_broadcast_process(engine.clone(), &consumer_config)?;

    // mc broadcasts
    engine.clone().listen_shard_blocks_broadcasts(ShardIdent::masterchain()).await?;

    // mc blocks download client
    let mc_client = Arc::new(ShardClient::new(engine)?);
    let _ = mc_client.start_master_chain(last_mc_block).await?.await;

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
