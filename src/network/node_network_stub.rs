use crate::{
    block::{convert_block_id_ext_api2blk, BlockStuff}, config::{TonNodeGlobalConfig, TonNodeConfig},
    shard_state::ShardStateStuff, network::{full_node_client::FullNodeOverlayClient, neighbours::Neighbour},
    engine_traits::OverlayOperations,
    block_proof::BlockProofStuff,
    db::{InternalDb}
};

use overlay::OverlayShortId;
use std::{
    io::{Seek, Read},
    sync::{Arc, atomic::{AtomicU32, AtomicU64, Ordering}},
    time::{SystemTime, Duration},
    collections::HashMap
};
use tokio::io::AsyncReadExt;
use ton_api::ton::{
    ton_node::{ 
        ArchiveInfo, BlockDescription, BlocksDescription, 
    }
};
use adnl::common::KeyId;
use overlay::QueriesConsumer;
use ton_block::{BlockIdExt, ShardIdent};
use ton_types::{fail, Result, error};

const MASTER_BLOCKS_TO_SYNC: u32 = 15;
const MASTER_BLOCKS_INTERVAL_SEC: u64 = 5;
const DOWNLOAD_BLOCK_DELAY: u64 = 100;

struct OverlayClientStub {
    db: Arc<dyn InternalDb>,
    storage_root: String,
    _global_cfg: TonNodeGlobalConfig,
    start_mc_block: AtomicU32,
    cur_mc_block: AtomicU32,
    last_mc_block_time: AtomicU64,

    last_shard_blocks: Arc<tokio::sync::Mutex<HashMap<ShardIdent, BlockIdExt>>>,
}

pub struct NodeNetworkStub {
    client: Arc<OverlayClientStub>,
}

impl NodeNetworkStub {

    pub fn new(config: &TonNodeConfig, db: Arc<dyn InternalDb>, storage_root: String) -> Result<Self> {
        let client = OverlayClientStub {
            storage_root,
            _global_cfg: config.load_global_config()?,
            db,
            start_mc_block: AtomicU32::new(0),
            cur_mc_block: AtomicU32::new(0),
            last_mc_block_time: AtomicU64::new(0),
            last_shard_blocks: Arc::new(tokio::sync::Mutex::new(HashMap::new()))
        };
        let network = Self {
            client: Arc::new(client)
        };
        Ok(network)
    }

}

#[async_trait::async_trait]
impl OverlayOperations for NodeNetworkStub {
    fn calc_overlay_short_id(&self, _workchain: i32, _shard: u64) -> Result<Arc<OverlayShortId>> {
        let zero = [0_u8; 32];
        Ok(adnl::common::KeyId::from_data(zero))
    }
    async fn start(self: Arc<Self>) -> Result<Arc<dyn FullNodeOverlayClient>> {
        Ok(self.client.clone())
    }
    async fn get_overlay(
        self: Arc<Self>,
        _overlay_id: &Arc<OverlayShortId>
    ) -> Result<Arc<dyn FullNodeOverlayClient>> {
        Ok(self.client.clone())
    }
    fn add_consumer(&self, _overlay_id: &Arc<OverlayShortId>, _consumer: Arc<dyn QueriesConsumer>) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl FullNodeOverlayClient for OverlayClientStub {

    async fn broadcast_external_message(&self, _msg: &[u8]) -> Result<u32> {
        unimplemented!();
    }

    async fn get_next_block_description(&self, _prev_block_id: &BlockIdExt, _attempts: u32) -> Result<BlockDescription> {
        unimplemented!();
    }

    async fn get_next_blocks_description(&self, _prev_block_id: &BlockIdExt, _limit: i32, _attempts: u32) -> Result<BlocksDescription> {
        unimplemented!();
    }

    async fn get_prev_blocks_description(&self, _next_block_id: &BlockIdExt, _limit: i32, _cutoff_seqno: i32, _attempts: u32) -> Result<BlocksDescription> {
        unimplemented!();
    }

    async fn download_block_proof(&self, _block_id: &BlockIdExt, _is_link: bool, _key_block: bool, _attempts: u32) -> Result<Option<BlockProofStuff>> {
        unimplemented!();
    }

    async fn download_block_full(&self, id: &BlockIdExt, _attempts: u32) -> Result<Option<(BlockStuff, BlockProofStuff)>> {
        // first try to check if it in DB
        let block = match self.db.load_block_data(id) {
            Err(_) => {
                let filename = format!("{}/blocks/{:016x}/{}", self.storage_root, id.shard().shard_prefix_with_tag(), id.seq_no);
                BlockStuff::read_from_file(&filename)?
            }
            Ok(block) => block
        };
        // always get?
        if block.block().read_info()?.key_block() {
            let mut last_shard_blocks = self.last_shard_blocks.lock().await;
            *last_shard_blocks = block.shards_blocks()?;
        }
        futures_timer::Delay::new(Duration::from_millis(DOWNLOAD_BLOCK_DELAY)).await;
        Ok(Some((block, BlockProofStuff::default())))
    }

    async fn download_block(&self, _id: &BlockIdExt, _attempts: u32) -> Result<Option<BlockStuff>> {
        unimplemented!();
    }

    async fn check_persistent_state(
        &self,
        block_id: &BlockIdExt,
        _masterchain_block_id: &BlockIdExt,
        _attempts: u32
    ) -> Result<(bool, Arc<Neighbour>)> {
        let filename = format!("{}/states/{:016x}/{}", self.storage_root, block_id.shard().shard_prefix_with_tag(), block_id.seq_no);
        let peer = Arc::new(Neighbour::new(KeyId::from_data([0; 32]))?);
        if let Ok(_file) = tokio::fs::File::open(filename).await {
            Ok((true, peer))
        } else {
            Ok((false, peer))
        }
    }

    async fn download_persistent_state_part(
        &self,
        block_id: &BlockIdExt,
        _masterchain_block_id: &BlockIdExt,
        offset: usize,
        max_size: usize,
        _attempts: u32,
        _peer: Option<Arc<Neighbour>>
    ) -> Result<(Vec<u8>, Arc<Neighbour>)> {
        // first try to check if it in DB
        if offset | max_size == 0 {
            let handle = self.db.load_block_handle(block_id)?;
            if handle.state_inited() {
                fail!("already downloaded")
            } else {
                let filename = format!("{}/states/{:016x}/{}", self.storage_root, block_id.shard().shard_prefix_with_tag(), block_id.seq_no);
                let peer = Arc::new(Neighbour::new(KeyId::from_data([0; 32]))?);
                return Ok((std::fs::read(filename)?, peer))
            }
        }

        // let filename = format!("{}/states/{:016x}/{}", self.storage_root, block_id.shard().shard_prefix_with_tag(), block_id.seq_no);
        // let mut file = tokio::fs::File::open(filename).await?;

        // let total_size = file.seek(std::io::SeekFrom::End(0)).await? as usize;
        // file.seek(std::io::SeekFrom::Start(offset as u64)).await?;

        // if offset > total_size {
        //     fail!("offset > total_size");
        // }

        // let mut data = vec![0; std::cmp::min(max_size, total_size - offset)];
        // file.read(&mut data).await?;
        // Ok(data)

        let filename = format!("{}/states/{:016x}/{}", self.storage_root, block_id.shard().shard_prefix_with_tag(), block_id.seq_no);
        let mut file = std::fs::File::open(filename)?;

        let total_size = file.seek(std::io::SeekFrom::End(0))? as usize;
        file.seek(std::io::SeekFrom::Start(offset as u64))?;

        if offset > total_size {
            fail!("offset > total_size");
        }

        tokio::task::yield_now().await;

        let mut data = vec![0; std::cmp::min(max_size, total_size - offset)];

        tokio::task::yield_now().await;

        file.read(&mut data)?;

        let peer = Arc::new(Neighbour::new(KeyId::from_data([0; 32]))?);
        Ok((data, peer))
    }

    async fn download_zero_state(&self, _id: &BlockIdExt, _attempts: u32) -> Result<Option<ShardStateStuff>> {
        unimplemented!();
    }

    #[cfg(test)]
    async fn download_next_key_blocks_ids(&self, block_id: &BlockIdExt, _max_size: i32, _attempts: u32) -> Result<Vec<BlockIdExt>> {
        crate::test_helper::test_download_next_key_blocks_ids(block_id)
    }

    #[cfg(not(test))]
    async fn download_next_key_blocks_ids(&self, _block_id: &BlockIdExt, _max_size: i32, _attempts: u32) -> Result<Vec<BlockIdExt>> {
        unimplemented!();
    }

    async fn download_next_block_full(&self, prev_id: &BlockIdExt, attempts: u32) -> Result<(BlockStuff, BlockProofStuff)> {

        let next: BlockIdExt = if prev_id.shard().is_masterchain() {

            if self.start_mc_block.load(Ordering::Relaxed) == 0 {
                self.start_mc_block.store(prev_id.seq_no, Ordering::Relaxed);
            }
            if (prev_id.seq_no + 1) - self.start_mc_block.load(Ordering::Relaxed) > MASTER_BLOCKS_TO_SYNC {
                let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs();
                if now - self.last_mc_block_time.load(Ordering::Relaxed) < MASTER_BLOCKS_INTERVAL_SEC {
                    fail!("No block yet")
                }
                self.last_mc_block_time.store(now, Ordering::Relaxed);
            }

            self.cur_mc_block.store(prev_id.seq_no + 1, Ordering::Relaxed);

            let mut next = prev_id.clone();
            next.seq_no += 1;
            next
        } else {
            let filename = format!("{}/next_blocks/{:016x}/{}", self.storage_root, prev_id.shard().shard_prefix_with_tag(), prev_id.seq_no);
            if let Ok(mut file) = tokio::fs::File::open(filename).await {
                let mut data = vec!();
                file.read_to_end(&mut data).await?;
                let mut data = std::io::Cursor::new(data);
                let mut d = ton_api::Deserializer::new(&mut data);
                convert_block_id_ext_api2blk(&d.read_bare()?)?
            } else {
                fail!("No next block record")
            }
        };

        Ok(
            self.download_block_full(&next, attempts).await?
                .ok_or_else(|| error!("No block file"))?
        )
    }

    async fn get_archive_info(&self, _masterchain_seqno: u32, _attempts: u32) -> Result<ArchiveInfo> {
        unimplemented!();
    }

    async fn get_archive_slice(&self, _archive_id: u64, _offset: u64, _max_size: u32, _attempts: u32) -> Result<Vec<u8>> {
        unimplemented!();
    }

    async fn wait_block_broadcast(&self) -> Result<Box<ton_api::ton::ton_node::broadcast::BlockBroadcast>> {
        futures::future::pending().await
        /*loop {
            let (duration, lost, rnd, lag_incr): (Duration, bool, usize, u32) = {
                let mut rng = rand::thread_rng();
                let rnd1: u64 = rand::Rng::gen(&mut rng);
                let rnd2: u32 = rand::Rng::gen(&mut rng);
                let rnd3: usize = rand::Rng::gen(&mut rng);
                let rnd4: u32 = rand::Rng::gen(&mut rng);
                (
                    Duration::from_millis(100 + (rnd1 % 50)),
                    rnd2 % 5 == 1,
                    rnd3,
                    rnd4 % 5,
                )
            };

            futures_timer::Delay::new(duration).await;

            // "Lost packet"
            if lost {
                continue;
            }

            let start_mc_block = self.start_mc_block.load(Ordering::Relaxed);
            let cur_mc_block = self.cur_mc_block.load(Ordering::Relaxed);
            let mut block_id = {
                let last_shard_blocks = self.last_shard_blocks.lock().await;
                last_shard_blocks.iter().nth(rnd % last_shard_blocks.len()).unwrap().1.clone()
            };

            if cur_mc_block < start_mc_block + MASTER_BLOCKS_TO_SYNC {
                // if we just catch up blockchain - receive some new blocks
                let lag = cur_mc_block - start_mc_block;
                block_id.seq_no += lag + lag_incr;
            } else {
                // othervice try to receive next shardblocks
                block_id.seq_no += 1;
            }

            // if we calculated wrong block id - it is lost packet =)

            if let Ok(Some(r)) = self.download_block_full(&block_id, 1).await {
                return Ok(r)
            }
        }*/
    }
}
