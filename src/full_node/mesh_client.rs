use std::{ops::Deref, sync::Arc, time::Duration};

use ton_block::{BlockIdExt, ConnectedNwConfig};
use ton_types::{error, fail,  Result};

use storage::block_handle_db::BlockHandle;
use crate::{
    block_proof::BlockProofStuff, engine_traits::EngineOperations, 
    shard_state::ShardStateStuff, block::BlockStuff, boot::PSS_PERIOD_BITS,
};

#[derive(Clone)]
enum BlockProofOrZerostate {
    KeyBlock(BlockProofStuff),
    Zerostate(Arc<ShardStateStuff>),
    None,
}

impl BlockProofOrZerostate {
    pub fn with_zerostate(zerostate: Arc<ShardStateStuff>) -> Self {
        Self::Zerostate(zerostate)
    }
    pub fn with_key_block(proof: BlockProofStuff) -> Self {
        Self::KeyBlock(proof)
    }
    pub fn block_proof(&self) -> Option<&BlockProofStuff> {
        match self {
            Self::KeyBlock(proof) => Some(proof),
            Self::Zerostate(_) => None,
            Self::None => None,
        }
    }
    pub fn zerostate(&self) -> Option<&ShardStateStuff> {
        match self {
            Self::KeyBlock(_) => None,
            Self::Zerostate(zerostate) => Some(zerostate.deref()),
            Self::None => None,
        }
    }
    pub fn id(&self) -> Option<&BlockIdExt> {
        match self {
            Self::KeyBlock(proof) => Some(proof.id()),
            Self::Zerostate(zerostate) => Some(zerostate.block_id()),
            Self::None => None,
        }
    }
}

struct Boot<'a> {
    nw_id: u32,
    nw_config: &'a ConnectedNwConfig,
    engine: Arc<dyn EngineOperations>,
    descr: String,
}

impl<'a> Boot<'a> {
    pub async fn boot(
        engine: Arc<dyn EngineOperations>,
        nw_id: u32,
        nw_config: &'a ConnectedNwConfig,
        last_commited_block: Option<BlockIdExt>,
    ) -> Result<(Arc<BlockHandle>, BlockProofOrZerostate)> {
        let descr = format!("boot into {}", nw_id);
        let boot = Self { nw_id, nw_config, engine, descr };

        boot.check_nw_config()?;

        let last_commited_block = boot.process_hardforks(last_commited_block)?;

        let (handle, init_block_proof_or_zs) = boot.get_init_point().await?;

        let last_block_proof_or_zs = boot.get_key_blocks(handle, init_block_proof_or_zs).await?;

        log::info!("{}: last key block {}", boot.descr, last_block_proof_or_zs.id().ok_or_else(
            || error!("INTERNAL ERROR: last key block is none")
        )?);

        let latest_block_handle =
            boot.get_latest_block(last_commited_block.as_ref(), &last_block_proof_or_zs).await?;

        Ok((latest_block_handle, last_block_proof_or_zs))
    }

    pub fn check_nw_config(&self) -> Result<()> {

        if let Some(last_hf) = self.nw_config.hardforks.last() {
            if self.nw_config.init_block.seq_no() < last_hf.seq_no() {
                fail!("init block {} is older then last hardfork {}", 
                    self.nw_config.init_block, last_hf);
            }
            if last_hf.seq_no() == self.nw_config.init_block.seq_no() {
                if last_hf != &self.nw_config.init_block {
                    fail!("init block {} is not equal to last hardfork {} with same seqno", 
                        self.nw_config.init_block, last_hf);
                }
            }
        }
        Ok(())
    }

    pub fn process_hardforks(&self, mut last_commited_block: Option<BlockIdExt>) -> Result<Option<BlockIdExt>> {

        if let Some(last_hf) = self.nw_config.hardforks.last() {
            let last_processed_hf = self.engine.load_last_mesh_processed_hardfork(self.nw_id)?;
            if let Some(last_processed_hf) = last_processed_hf {
                if last_processed_hf.seq_no() == last_hf.seq_no() {
                    if last_processed_hf.deref() != last_hf {
                        fail!(
                            "FATAL: last processed by the node hardfork {} is not equal \
                            to last hardfork from config {}",
                            last_processed_hf, last_hf
                        );
                    }
                    log::info!("{}: hardforks are already processed up to {}", 
                        self.descr, last_processed_hf);
                    return Ok(last_commited_block);
                }
                if last_processed_hf.seq_no() > last_hf.seq_no() {
                    fail!(
                        "FATAL: last processed by the node hardfork {} is unknown \
                        - it is newer then last hardfork from config {}",
                        last_processed_hf, last_hf
                    );
                }
            }

            if let Some(last_commited_block) = last_commited_block {
                if last_commited_block.seq_no() < last_hf.seq_no() {
                    log::info!(
                        "{}: last hardfork is {}, last commited block is {}. Continue with hardfork",
                        self.descr, last_hf, last_commited_block
                    );
                } else if last_commited_block.seq_no() == last_hf.seq_no() {
                    if last_commited_block != *last_hf {
                        fail!(
                            "FATAL: last commited block {} is not equal to last hardfork {}",
                            last_commited_block, last_hf
                        );

                        // TODO: clean up DB and continue?
                    }
                } else if last_commited_block.seq_no() > last_hf.seq_no() {
                    fail!(
                        "FATAL: last commited block {} is newer then last hardfork {}",
                        last_commited_block, last_hf
                    );

                    // TODO: clean up DB and continue?
                }
            }

            self.engine.save_last_mesh_processed_hardfork(self.nw_id, last_hf)?;

            last_commited_block = None;
        }
        Ok(last_commited_block)
    }

    async fn get_init_point(&self)
    -> Result<(Arc<BlockHandle>, BlockProofOrZerostate)> {

        log::debug!("{}: get_init_point", self.descr);

        let mut block_id = &self.nw_config.init_block;
        let is_hardfork = self.nw_config.hardforks.last()
            .map(|hf| hf.seq_no() == self.nw_config.init_block.seq_no()).unwrap_or(false);

        let last_known_key_block_id = self.engine.load_last_mesh_key_block_id(self.nw_id)?;
        match &last_known_key_block_id {
            Some(id) if id.seq_no() > block_id.seq_no() => {
                block_id = id.deref();
                log::info!("{}: start from last key block from DB: {}", self.descr, block_id);
            }
            _ => {
                log::info!("{}: start from config->init_block {}: {}", 
                    self.descr, if is_hardfork { "(hardfork)" } else { "" }, block_id);
            }
        };

        loop {
            if self.engine.check_stop() {
                fail!("{}: stopped", self.descr);
            }

            if block_id.seq_no() == 0 {
                log::info!("{}: downloading zerostate {}", self.descr, block_id);
                match self.engine.download_zerostate(self.nw_id, block_id).await {
                    Ok((zerostate, bytes)) => {
                        log::info!("{}: saving zerostate {}", self.descr, block_id);
                        let (zerostate, handle) = 
                            self.engine.store_zerostate(zerostate, &bytes).await?;
                        self.engine.set_applied(&handle, 0).await?;
                        return Ok((handle, BlockProofOrZerostate::with_zerostate(zerostate)));
                    }
                    Err(e) => {
                        log::warn!("{}: Can't download zerostate {}: {}", self.descr, block_id, e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            } else { 
                log::info!("{}: downloading & checking init block proof {}", self.descr, block_id);
                match self.download_and_check_block_proof(
                    block_id, true, is_hardfork, &BlockProofOrZerostate::None
                ).await {
                    Ok((handle, proof)) => {
                        return Ok((handle, BlockProofOrZerostate::with_key_block(proof)))
                    }
                    Err(e) => {
                        log::warn!("{}: {}", self.descr, e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            }
        }
    }

    async fn get_key_blocks(
        &self,
        handle: Arc<BlockHandle>,
        mut prev_block_proof: BlockProofOrZerostate,
    ) -> Result<BlockProofOrZerostate> {

        let mut got_last_block_at = 0;
        let mut key_blocks = vec!(handle.clone());
        let mut prev_handle = handle;

        'main_loop: loop {
            if self.engine.check_stop() {
                fail!("{}: stopped", self.descr);
            }

            // Get next block ids
            log::info!("{}: downloading next key blocks ids {}", self.descr, prev_handle.id());
            let (next_key_blocks_ids, got_at) = match 
                self.engine.download_next_key_blocks_ids(self.nw_id, prev_handle.id()).await
            {
                Err(e) => {
                    log::warn!("{}: download_next_key_blocks_ids {}: {}", self.descr, prev_handle.id(), e);
                    futures_timer::Delay::new(Duration::from_secs(1)).await;
                    continue;
                }
                Ok(nkb) => {
                    log::info!("{}: got {} next key blocks", self.descr, nkb.len());
                    (nkb, self.engine.now())
                }
            };

            // Get and check proofs
            for next_id in &next_key_blocks_ids {
                if next_id.seq_no() == 0 {
                    log::warn!("{}: somebody sent next key block with zero state {}",
                        self.descr, next_id);
                    continue 'main_loop;
                }

                if next_id.seq_no() <= prev_handle.id().seq_no() {
                    log::warn!(
                        "{}: somebody sent next key block id {} with seq_no less or equal to already got {}",
                        self.descr, next_id, prev_handle.id());
                    continue 'main_loop;
                }

                match self.download_and_check_block_proof(next_id, false, false, &prev_block_proof).await {
                    Ok((next_handle, proof)) => {
                        prev_handle = next_handle;
                        key_blocks.push(prev_handle.clone());
                        prev_block_proof = BlockProofOrZerostate::with_key_block(proof);
                        got_last_block_at = got_at;
                        self.engine.save_last_mesh_key_block_id(self.nw_id, next_id)?;
                        log::info!("{}: {} is OK", self.descr, next_id);
                    }
                    Err(err) => {
                        log::warn!("{}: {}", self.descr, err);
                        futures_timer::Delay::new(Duration::from_secs(1)).await;
                        continue 'main_loop;
                    }
                }
            }

            // Check finish conditions

            let now = self.engine.now();

            // if we got last key block more then 15 min ago - finish
            if now - got_last_block_at > 15 * 60 {
                log::warn!("{}: finish because of timeout", self.descr);
                return Ok(prev_block_proof);
            }

            // if we got last key block more then 30 sec ago
            // and last key block + pss interval > now - finish
            if now - got_last_block_at > 30 {
                let pss_interval = 1 << PSS_PERIOD_BITS;
                let last_block_utime = prev_handle.gen_utime()?;
                if last_block_utime + pss_interval > now {
                    log::info!("{}: finish because of last block + pss interval", self.descr);
                    return Ok(prev_block_proof);
                }
            }
        }
    }

    async fn download_and_check_block_proof(
        &self, 
        block_id: &BlockIdExt,
        is_init_block: bool,
        is_hardfork: bool,
        prev_key_block_proof_or_zs: &BlockProofOrZerostate,
    ) -> Result<(Arc<BlockHandle>, BlockProofStuff)> {
        log::debug!("{}: download_and_check_block_proof {}", self.descr, block_id);

        // Try to load from DB
        if let Some(handle) = self.engine.load_block_handle(block_id)? {
            if let Ok(proof) = self.engine.load_block_proof(&handle, is_hardfork).await {
                return Ok((handle, proof));
            }
        }

        // Download
        let proof = self.engine.download_block_proof(self.nw_id, block_id, is_hardfork, true).await
            .map_err(|e| error!("Can't download block proof {}: {}", block_id, e))?;

        if is_init_block {
            proof.check_proof_as_link()
                .map_err(|e| error!("Block proof {} check (as link) failed: {}", block_id, e))?;
        } else if is_hardfork {
            proof.check_proof_link()
                .map_err(|e| error!("Block proof link {} check failed: {}", block_id, e))?;
        } else {
            if let Some(zerostate) = prev_key_block_proof_or_zs.zerostate() {
                proof.check_with_master_state(zerostate)
                    .map_err(|e| error!("Block proof {} check failed: {}", block_id, e))?;
            } else if let Some(prev_key_block_proof) = prev_key_block_proof_or_zs.block_proof() {
                proof.check_with_prev_key_block_proof(prev_key_block_proof)
                    .map_err(|e| error!("Block proof {} check failed: {}", block_id, e))?;
            } else {
                fail!("INTERNAL ERROR: there is no both zerostate and prev key block proof");
            }
        }

        // Save to DB
        let handle = self.engine
            .store_block_proof(self.nw_id, block_id, None, &proof).await?
            .to_non_created().ok_or_else(
                || error!("INTERNAL ERROR: bad result for store block {} proof", block_id)
            )?;

        if !handle.is_key_block()? {
            fail!("Block {} is not a key block", block_id);
        }

        Ok((handle, proof))
    }

    async fn get_latest_block(
        &self,
        mut id: Option<&BlockIdExt>,
        last_key_block_proof: &BlockProofOrZerostate
    ) -> Result<Arc<BlockHandle>> {

        if let Some(id) = id {
            log::info!("{}: downloading mesh kit {}", self.descr, id);
        } else {
            log::info!("{}: downloading latest mesh kit", self.descr);
        }

        'top: loop {
            if self.engine.check_stop() {
                fail!("{}: stopped", self.descr);
            }

            // Download given mesh kit (mc block + queues) or latest one
            let (block, proof) = if let Some(id) = id {
                log::info!("{}: downloading mesh kit {}...", self.descr, id);
                match self.engine.download_mesh_kit(self.nw_id, id).await {
                    Ok(r) => r,
                    Err(e) => {
                        log::warn!("{}: Can't download mesh kit {}: {}", self.descr, id, e);
                        futures_timer::Delay::new(Duration::from_secs(1)).await;
                        continue 'top;
                    }
                }
            } else {
                log::info!("{}: downloading latest mesh kit...", self.descr);
                match self.engine.download_latest_mesh_kit(self.nw_id).await {
                    Ok(r) => r,
                    Err(e) => {
                        log::warn!("{}: Can't download latest mesh kit: {}", self.descr, e);
                        futures_timer::Delay::new(Duration::from_secs(1)).await;
                        continue 'top;
                    }
                }
            };
            log::info!("{}: downloaded mesh kit {}", self.descr, block.id());

            // Check proof
            let key_block_seqno = block.virt_block()?.read_info()?.prev_key_block_seqno();
            if let Some(zerostate) = last_key_block_proof.zerostate() {
                if key_block_seqno != 0 {
                    if id.is_some() {
                        log::warn!(
                            "{}: mesh kit {} is signed by block #{}, not by zerostate. \
                            Will try to download latest mesh kit.",
                            self.descr, block.id(), key_block_seqno
                        );
                        id = None;
                        continue 'top;
                    } else {
                        log::warn!("{}: mesh kit {} is signed by block #{}, not by zerostate",
                            self.descr, block.id(), key_block_seqno);
                        futures_timer::Delay::new(Duration::from_secs(1)).await;
                        continue 'top;
                    }
                }

                if let Err(e) = proof.check_with_master_state(zerostate) {
                    log::warn!("{}: mesh kit {} check failed: {}", self.descr, block.id(), e);
                    futures_timer::Delay::new(Duration::from_secs(1)).await;
                    continue 'top;
                }

            } else if let Some(last_key_block_proof) = last_key_block_proof.block_proof() {

                if key_block_seqno != last_key_block_proof.id().seq_no() {
                    if id.is_some() {
                        log::warn!(
                            "{}: mesh kit {} is signed by block #{}, not by {}. \
                            Will try to download latest mesh kit.",
                            self.descr, block.id(), key_block_seqno, last_key_block_proof.id()
                        );
                        id = None;
                        futures_timer::Delay::new(Duration::from_secs(1)).await;
                        continue 'top;
                    } else {
                        log::warn!("{}: mesh kit {} is signed by block #{}, not by {}",
                            self.descr, block.id(), key_block_seqno, last_key_block_proof.id());
                        futures_timer::Delay::new(Duration::from_secs(1)).await;
                        continue 'top;
                    }
                }

                if let Err(e) = proof.check_with_prev_key_block_proof(last_key_block_proof) {
                    log::warn!("{}: mesh kit {} check failed: {}", self.descr, block.id(), e);
                    futures_timer::Delay::new(Duration::from_secs(1)).await;
                    continue 'top;
                }
            } else {
                fail!("INTERNAL ERROR: there is no both zerostate and prev key block proof");
            }

            // TODO: Save


            // TODO: Apply


            unimplemented!()
            
            // return Ok(handle)
        }
    }

}

pub struct ConnectedNwClient {
    engine: Arc<dyn EngineOperations>,
    nw_id: u32,
    descr: String,
}

impl ConnectedNwClient {
    pub async fn start(
        engine: Arc<dyn EngineOperations>,
        last_block_id: Option<BlockIdExt>,
        nw_id: u32,
        nw_config: ConnectedNwConfig
    ) {
        let descr = format!("mesh nw client {}", nw_id);
        let client = Arc::new(Self { engine, nw_id, descr});

        tokio::spawn({
            let client = client.clone();
            async move {

                // Boot
                let (last_mesh_kit, last_key_block) = 'l: loop {
                    match Boot::boot(
                        client.engine.clone(), client.nw_id, &nw_config, last_block_id.clone()
                    ).await {
                        Ok(id) => {
                            log::info!("{}: boot finished", client.descr);
                            break 'l id;
                        }
                        Err(e) => {
                            log::warn!("{}: Can't boot: {}", client.descr, e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                };

                // Start worker
                // client.worker(last_mesh_kit).await;
                unimplemented!()
            }
        });
    }

    pub fn process_broadcast(&self) -> Result<()> {

        // process broadcast with mc block and queues updates - check proof and save block and updates

        unimplemented!()
    }

    pub fn process_commited(&self, block_id: BlockIdExt) -> Result<()> {

        // new block of connected nw was commited into masterchain

        // if we don't know about this block - download it

        unimplemented!()
    }

    async fn worker(
        self: Arc<Self>,
        mut last_mc_block: Arc<BlockHandle>,
        mut last_key_block_proof: BlockProofOrZerostate,
    ) -> Result<()> {

        loop {
            if self.engine.check_stop() {
                log::info!("{}: stopped", self.descr);
                return Ok(());
            }

            if last_mc_block.gen_utime()? + 5 > self.engine.now() {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            if last_mc_block.has_next1() {
                let next = self.engine.load_block_next1(last_mc_block.id()).await?;
                if let Some(next_mc_block) = self.engine.load_block_handle(&next)? {
                    if next_mc_block.is_applied() {
                        last_mc_block = next_mc_block;
                        continue;
                    }
                }
            }

            log::trace!("{}: downloading next block... prev: {}", self.descr, last_mc_block.id());

            // TODO: after receiving a single data structure, FullNodeOverlayClient creates from it
            //       BlockStuff with MeshUpdate inside and BlockProofStuff with signatures inside.
            //       Same tree of cells with a fragment of the block stores inside both structs.

            let (mesh_update, proof) =
                self.engine.download_next_block(self.nw_id, last_mc_block.id()).await?;
            log::trace!("{}: downloading next block: got {}", self.descr, mesh_update.id());

            if mesh_update.id().seq_no != last_mc_block.id().seq_no + 1 ||
               mesh_update.id().shard_id != last_mc_block.id().shard_id 
            {
                log::warn!("{}: invalid next master block, got: {}, prev: {}",
                    self.descr, mesh_update.id(), last_mc_block.id());
            }

            // Check proof
            if let Some(zerostate) = last_key_block_proof.zerostate() {
                log::trace!("{}: checking with zerostate, mesh update: {}", self.descr, mesh_update.id());
                if let Err(e) = proof.check_with_master_state(zerostate) {
                    log::warn!(
                        "{}: mesh kit {} check with zerostate failed: {}",
                        self.descr, mesh_update.id(), e
                    );
                    futures_timer::Delay::new(Duration::from_secs(1)).await;
                    continue;
                } 
            } else if let Some(key_block_proof) = last_key_block_proof.block_proof() {
                log::trace!(
                    "{}: checking with prev key block proof {}, mesh update: {}",
                    self.descr, key_block_proof.id(), mesh_update.id()
                );
                if let Err(e) = proof.check_with_prev_key_block_proof(key_block_proof) {
                    log::warn!(
                        "{}: mesh kit {} check with prev key block proof {} failed: {}",
                        self.descr, mesh_update.id(), key_block_proof.id(), e
                    );
                    futures_timer::Delay::new(Duration::from_secs(1)).await;
                    continue;
                }
            } else {
                fail!("INTERNAL ERROR: there is no both zerostate and prev key block proof");
            }

            // save
            log::trace!("{}: saving mesh update {}...", self.descr, mesh_update.id());
            let handle = self.engine.store_block(&mesh_update).await?.to_any();

            // apply
            log::trace!("{}: applying mesh update {}...", self.descr, mesh_update.id());
            self.engine.clone().apply_block(&handle, &mesh_update, 0, false).await?;

            if mesh_update.is_key_block()? {
                log::trace!("{}: update last key block to: {}", self.descr, mesh_update.id());
                last_key_block_proof = BlockProofOrZerostate::with_key_block(proof);
            }

            last_mc_block = handle;
            log::trace!("{}: applied {}", self.descr, mesh_update.id());
        }

    }

}


pub struct MeshClient {
    engine: Arc<dyn EngineOperations>,
    clients: lockfree::map::Map<u32, Arc<ConnectedNwClient>>,
}

impl MeshClient {
    pub async fn start(engine: Arc<dyn EngineOperations>) -> Result<()> {

        // on the start
        // - get last master block
        // - create needed ConnectedNwClient

        // Later - observe next master blocks

        let client = Self { engine, clients: lockfree::map::Map::new() };

        let last_mc_block_id = client.engine.load_last_applied_mc_block_id()?
            .ok_or_else(|| error!("No last mc block"))?;

        



        client.start_worker(last_mc_block_id);

        Ok(())
    }

    fn start_worker(self: Self, mut last_mc_block_id: Arc<BlockIdExt>) {
        tokio::spawn(async move {
            loop {
                match self.worker(&last_mc_block_id).await {
                    Ok(_) => {
                        log::info!("MeshClient worker finished");
                        break;
                    }
                    Err(e) => {
                        log::error!("CRITICAL!!! Unexpecetd fail in MeshClient worker: {}", e);
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                last_mc_block_id = loop {
                    match self.engine.load_last_applied_mc_block_id() {
                        Ok(Some(id)) => break id,
                        Ok(None) => {
                            log::warn!("CRITICAL!!! No last mc block");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                        Err(e) => {
                            log::error!("CRITICAL!!! Can't get last mc block: {}", e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    };
                };
            }
        });
    }

    async fn worker(&self, mc_block_id: &BlockIdExt) -> Result<()> {

        let mut mc_block: BlockStuff;
        let mut mc_block_handle = self.engine.load_block_handle(mc_block_id)?
            .ok_or_else(|| error!("Can't load handle for mc block {}", mc_block_id))?;
        std::mem::drop(mc_block_id);

        loop {

            log::debug!("MeshClient: waiting next mc block {}", mc_block_handle.id());

            (mc_block_handle, mc_block) = loop {
                if let Ok(r) = self.engine.wait_next_applied_mc_block(&mc_block_handle, Some(500)).await {
                    break r;
                } else if self.engine.check_stop() {
                    return Ok(());
                } else {
                    let diff = self.engine.now() - mc_block_handle.gen_utime()?;
                    if diff > 15 {
                        log::warn!("MeshClient: no next mc block more then {diff} sec");
                    }
                }
            };

            log::debug!("MeshClient: got mc block {}", mc_block.id());

            for (nw_id, block_id) in mc_block.mesh_top_blocks()? {
                if let Some(client) = self.clients.get(&nw_id) {
                    client.val().process_commited(block_id)?;
                } else {
                    log::warn!("Found commit block {block_id} of unknown network {nw_id}");
                }
            }

            if mc_block.block()?.read_info()?.key_block() {

                log::debug!("MeshClient: process key block {}", mc_block.id());

                // If key block with new connected nw info appered - create ConnectedNwClient.
                // *Remark*. If connected nw is added to config but not activatad yet - 
                //           collator will not commit its blocks, but the client have to download them 
                //           to be ready to activation.

                // If connected nw is removed from config - stop client.

                // If hardfork appered - recreate client with the hf block.

                unimplemented!()
            }
        }
    }

    fn process_broadcast(&self) -> Result<()> {
    
        // call process_broadcast in ConnectedNwClient

        unimplemented!()
    }
}