use crate::{
    block_proof::BlockProofStuff, engine_traits::EngineOperations, 
    shard_state::ShardStateStuff, engine::Engine
};

use std::{ops::Deref, sync::Arc, time::Duration};
use std::path::Path;
use storage::block_handle_db::BlockHandle;
use ton_api::ton::Ok;
use ton_block::{BlockIdExt, ShardIdent, SHARD_FULL, ConnectedNwConfig};
use ton_types::{error, fail, KeyId, Result};
use crate::block::BlockStuff;
use crate::validator::accept_block::create_new_proof_link;

pub const PSS_PERIOD_BITS: u32 = 17;
const RETRY_MASTER_STATE_DOWNLOAD: usize = 10;

struct Boot {
    nw_id: u32,
    nw_config: ConnectedNwConfig,
    engine: Arc<dyn EngineOperations>,
    descr: String,
}

impl Boot {
    pub async fn boot(engine: Arc<dyn EngineOperations>, nw_id: u32, nw_config: ConnectedNwConfig) 
    -> Result<Arc<BlockHandle>> {
        let descr = format!("boot into {}", nw_id);
        let boot = Self { nw_id, nw_config, engine, descr };

        let (handle, zero_state, init_block_proof) = boot.get_init_point().await?;

        let mut key_blocks = 
            boot.get_key_blocks(handle, zero_state.as_ref(), init_block_proof).await?;

        let last = key_blocks.pop().ok_or_else(|| error!("INTERNAL ERROR: `key_blocks is empty"))?;

        log::info!("{}: last key block {}", boot.descr, last.id());

        Ok(last)
    }

    async fn get_init_point(&self)
    -> Result<(Arc<BlockHandle>, Option<Arc<ShardStateStuff>>, Option<BlockProofStuff>)> {
/*
        let block_id = &self.nw_config.init_block;

        log::info!("{}: get init point: {}", self.descr, block_id);

        // TODO process hardforks. Hardfork block has only proof link, not full proof!

        if block_id.seq_no() == 0 {
            let (handle, zero_state) = self.download_zerostate().await?;
            return Ok((handle, Some(zero_state), None));
        }
    
        let (handle, proof) = loop {

            if self.engine.check_stop() {
                fail!("Boot was stopped");
            }
    
            log::info!("{}: download init block proof {}", self.descr, block_id);
    
            

            // Ok((handle, None, Some(proof)))
        };
*/
        unimplemented!()

    }

    async fn get_key_blocks(
        &self,
        mut handle: Arc<BlockHandle>,
        zero_state: Option<&Arc<ShardStateStuff>>,
        mut prev_block_proof: Option<BlockProofStuff>,
    ) -> Result<Vec<Arc<BlockHandle>>> {
        
        // get next blocks

        // download and check proof

        // stop if
        //    last key block + pss interval > now
        // or timeout

        unimplemented!()
    }

    async fn download_zerostate(&self) -> Result<(Arc<BlockHandle>, Arc<ShardStateStuff>)> {
        unimplemented!()
    }

    async fn download_and_check_block_proof(
        &self, 
        block_id: &BlockIdExt, 
        is_link: bool, 
    ) -> Result<(Arc<BlockHandle>, BlockProofStuff)> {
        log::debug!("{}: download_and_check_block_proof {}, is_link: {}",
            self.descr, block_id, is_link);

        // try to load from DB

        // try to download

        // check proof

        // save to DB

        unimplemented!()
    }
}

pub struct ConnectedNwClient {
    
}

impl ConnectedNwClient {
    pub fn start(
        engine: Arc<dyn EngineOperations>,
        new: bool,
        nw_id: u32,
        nw_config: ConnectedNwConfig
    ) {
        unimplemented!()

        // if nw is not commited into mc - it is completely new - perform boot

        // if nw is commited into mc - it is already known - take commited block

        // download full out message queues 

        // start worker

            // download next mc block and queues updates

            // apply block and update queues

    }

    fn process_broadcast(&self) -> Result<()> {

        // process broadcast with mc block and queues updates - check proof and save block and updates

        unimplemented!()
    }

    fn process_commited(&self) -> Result<()> {

        // new block of connected nw was commited into masterchain

        // if we don't know about this block - download it

        unimplemented!()
    }

}


pub struct MeshClient {
    
}

impl MeshClient {
    pub fn start(engine: Arc<dyn EngineOperations>) -> Self {
        unimplemented!()

        // on the start
        // - get last master block
        // - create needed ConnectedNwClient

        // Later - observe next master blocks
        // - if key block with new connected nw info appered - create ConnectedNwClient
        // - call process_commited in ConnectedNwClient
    }

    fn process_broadcast(&self) -> Result<()> {
    
        // call process_broadcast in ConnectedNwClient

        unimplemented!()
    }
}