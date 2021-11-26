/*
* Copyright (C) 2019-2021 TON Labs. All Rights Reserved.
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
    types::top_block_descr::{TopBlockDescrStuff, TopBlockDescrId}, engine::Engine, 
    engine_traits::{EngineAlloc, EngineOperations},shard_state::ShardStateStuff,
};
#[cfg(feature = "telemetry")]
use crate::engine_traits::EngineTelemetry;
use adnl::{
    declare_counted, 
    common::{
        add_counted_object_to_map, add_counted_object_to_map_with_update, CountedObject, 
        Counter
    }
};
use ton_block::{BlockIdExt, TopBlockDescr, Deserializable, BlockSignatures};
use ton_types::{fail, Result};
use std::{
    sync::{Arc, atomic::{AtomicU32, Ordering}},
    time::Duration,
    ops::Deref,
    collections::HashMap,
};
use rand::Rng;


pub enum StoreAction {
    Save(TopBlockDescrId, Arc<TopBlockDescrStuff>),
    Remove(TopBlockDescrId)
}

pub enum ShardBlockProcessingResult {
    Duplication,
    MightBeAdded(Arc<TopBlockDescrStuff>)
}

declare_counted!(
    struct ShardBlocksPoolItem {
        top_block: Arc<TopBlockDescrStuff>,
        own: bool
    }
);

pub struct ShardBlocksPool {
    last_mc_seq_no: AtomicU32,
    shard_blocks: lockfree::map::Map<TopBlockDescrId, ShardBlocksPoolItem>,
    storage_sender: Option<tokio::sync::mpsc::UnboundedSender<StoreAction>>,
    is_fake: bool,
}

impl ShardBlocksPool {

    pub fn new(
        shard_blocks: HashMap<TopBlockDescrId, TopBlockDescrStuff>,
        last_mc_seqno: u32,
        is_fake: bool,
        #[cfg(feature = "telemetry")]
        telemetry: &Arc<EngineTelemetry>,
        allocated: &Arc<EngineAlloc>
    ) -> Result<(Self, tokio::sync::mpsc::UnboundedReceiver<StoreAction>)> {
        let tsbs = lockfree::map::Map::new();
        for (key, val) in shard_blocks {
            let val = Arc::new(val);
            add_counted_object_to_map(  
                &tsbs,
                key,
                || {
                    let ret = ShardBlocksPoolItem { 
                        top_block: val.clone(), 
                        own: false,
                        counter: allocated.top_blocks.clone().into() 
                    };
                    #[cfg(feature = "telemetry")]
                    telemetry.top_blocks.update(allocated.top_blocks.load(Ordering::Relaxed));
                    Ok(ret)
                }
            )?;
        }
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let ret = ShardBlocksPool {
            last_mc_seq_no: AtomicU32::new(last_mc_seqno),
            shard_blocks: tsbs,
            storage_sender: Some(sender.clone()),
            is_fake,
        };
        Ok((ret, receiver))
    }

    pub async fn process_shard_block_raw(
        &self,
        id: &BlockIdExt,
        cc_seqno: u32,
        data: Vec<u8>,
        own: bool,
        check_only: bool,
        engine: &dyn EngineOperations,
    ) -> Result<ShardBlockProcessingResult> {
        let factory = || {
            let tbd = if self.is_fake {
                TopBlockDescr::with_id_and_signatures(id.clone(), BlockSignatures::default())
            } else {
                TopBlockDescr::construct_from_bytes(&data)?
            };
            Ok(Arc::new(TopBlockDescrStuff::new(tbd, &id, self.is_fake)?))
        };
        self.process_shard_block(id, cc_seqno, factory, own, check_only, engine).await
    }

    pub async fn process_shard_block(
        &self,
        id: &BlockIdExt,
        cc_seqno: u32,
        mut factory: impl FnMut() -> Result<Arc<TopBlockDescrStuff>>,
        own: bool,
        check_only: bool,
        engine: &dyn EngineOperations,
    ) -> Result<ShardBlockProcessingResult> {

        let tbds_id = TopBlockDescrId::new(id.shard().clone(), cc_seqno);
        let mut tbds = None;

        let tbds = loop {

            log::trace!("process_shard_block iteration cc_seqno: {} id: {}", cc_seqno, id);
            
            // check for duplication
            if let Some(prev) = self.shard_blocks.get(&tbds_id) {
                if id.seq_no() <= prev.val().top_block.proof_for().seq_no() {
                    log::trace!("process_shard_block duplication cc_seqno: {} id: {} prev: {}", 
                        cc_seqno, id, prev.val().top_block.proof_for());
                    return Ok(ShardBlockProcessingResult::Duplication);
                }
            }

            // validate top block descr
            let tbds = if let Some(tbds) = &tbds {
                tbds
            } else {
                let ret = factory()?;
                tbds.get_or_insert_with(|| ret) 
            };

            if !self.is_fake {
                tbds.validate(&engine.load_last_applied_mc_state().await?)?;
            }

            if check_only {
                log::trace!("process_shard_block check only  id: {}", id);
                break tbds.clone();
            }

            // add
            // This is so-called "interactive insertion"
            let mut old = None;
            let added = add_counted_object_to_map_with_update(
                &self.shard_blocks,
                tbds_id.clone(),
                |found| {
                    if let Some(found) = found {
                        // someone already added the value into map
                        if id.seq_no() <= found.top_block.proof_for().seq_no() {
                            return Ok(None)
                        }
                        old.replace(found.top_block.clone());
                    } 
                    let top_blocks = &engine.engine_allocated().top_blocks;
                    let ret = ShardBlocksPoolItem { 
                        top_block: tbds.clone(),
                        own,
                        counter: top_blocks.clone().into() 
                    };
                    #[cfg(feature = "telemetry")]
                    engine.engine_telemetry().top_blocks.update(
                        top_blocks.load(Ordering::Relaxed)
                    );
                    Ok(Some(ret))
                }
            )?;
            if !added {
                continue
            }

            if let Some(old) = old {
                log::trace!(
                    "process_shard_block updated cc_seqno: {} id: {} prev: {}",
                    cc_seqno, id, old.proof_for()
                )
            } else {
                log::trace!("process_shard_block added cc_seqno: {} id: {}", cc_seqno, id)
            }
            break tbds.clone()

            /*
            let result = self.shard_blocks.insert_with(tbds_id.clone(), |_key, prev, updated | {
                if let Some((_, val)) = updated {
                    // someone already added the value into map
                    if id.seq_no() <= val.top_block.proof_for().seq_no() {
                        lockfree::map::Preview::Discard
                    } else {
                        lockfree::map::Preview::New(ShardBlocksPoolItem { 
                            top_block: tbds.as_ref().unwrap().clone(),
                            own 
                        })
                    }
                } else if prev.is_some() {
                    // it is value we inserted just now
                    lockfree::map::Preview::Keep
                } else {
                    // there is not the value in the map - try to add.
                    // If other thread adding value the same time - the closure will be recalled
                    lockfree::map::Preview::New(ShardBlocksPoolItem { 
                        top_block: tbds.as_ref().unwrap().clone(),
                        own 
                    })
                }
            });
            match result {
                lockfree::map::Insertion::Created => {
                    log::trace!("process_shard_block added  cc_seqno: {}  id: {}", cc_seqno, id);
                    break 'a;
                },
                lockfree::map::Insertion::Updated(old) => {
                    log::trace!("process_shard_block updated  cc_seqno: {}  id: {}  old: {} {}",
                        cc_seqno, id, old.key().cc_seqno, old.key().id);
                    break 'a;
                },
                lockfree::map::Insertion::Failed(_) => {
                    continue;
                }
            }
            */

        };

        self.send_to_storage(StoreAction::Save(tbds_id.clone(), tbds.clone()));
        Ok(ShardBlockProcessingResult::MightBeAdded(tbds))

    }

    pub fn get_shard_blocks(&self, last_mc_seq_no: u32, only_own: bool) -> Result<Vec<Arc<TopBlockDescrStuff>>> {
        if last_mc_seq_no != self.last_mc_seq_no.load(Ordering::Relaxed) {
            log::error!("get_shard_blocks: Given last_mc_seq_no {} is not actual", last_mc_seq_no);
            fail!("Given last_mc_seq_no {} is not actual {}", last_mc_seq_no, self.last_mc_seq_no.load(Ordering::Relaxed));
        } else {
            let mut returned_list = string_builder::Builder::default();
            let mut blocks = Vec::new();
            for guard in self.shard_blocks.iter() {
                if !only_own || guard.val().own {
                    blocks.push(guard.val().top_block.clone());
                    returned_list.append(format!("\n{} {}", guard.key().cc_seqno, guard.key().id));
                }
            }        
            log::trace!("get_shard_blocks last_mc_seq_no {} returned: {}", 
                last_mc_seq_no, returned_list.string().unwrap_or_default());
            Ok(blocks)
        }
    }

    pub fn update_shard_blocks(&self, last_mc_state: &Arc<ShardStateStuff>) -> Result<()> {
        self.last_mc_seq_no.store(last_mc_state.block_id().seq_no(), Ordering::Relaxed);
        let mut removed_list = string_builder::Builder::default();
        for block in self.shard_blocks.iter() {
            if block.val().top_block.validate(last_mc_state).is_err() {
                self.shard_blocks.remove(block.key());
                self.send_to_storage(StoreAction::Remove(block.key().clone()));
                removed_list.append(format!("\n{} {}", block.key().cc_seqno, block.key().id));
            }
        }
        log::trace!("update_shard_blocks last_mc_state {} removed: {}", 
            last_mc_state.block_id(), removed_list.string().unwrap_or_default());
        Ok(())
    }

    fn send_to_storage(&self, action: StoreAction) {
        if let Some(storage_sender) = self.storage_sender.as_ref() {
            match storage_sender.send(action) {
                Ok(_) => log::trace!("ShardBlocksPool::send_to_storage: sent"),
                Err(err) => log::error!("ShardBlocksPool::send_to_storage: can't send {}", err),
            }
        }
    }
}

pub fn resend_top_shard_blocks_worker(engine: Arc<dyn EngineOperations>) {
    tokio::spawn(async move {
        engine.acquire_stop(Engine::MASK_SERVICE_TOP_SHARDBLOCKS_SENDER);
        loop {
            if engine.check_stop() {
                break
            }
            // 2..3 seconds
            let delay = rand::thread_rng().gen_range(2000, 3000);
            futures_timer::Delay::new(Duration::from_millis(delay)).await;
            match resend_top_shard_blocks(engine.deref()).await {
                Ok(_) => log::trace!("resend_top_shard_blocks: ok"),
                Err(e) => log::error!("resend_top_shard_blocks: {:?}", e)
            }
        }
        engine.release_stop(Engine::MASK_SERVICE_TOP_SHARDBLOCKS_SENDER);
    });
}

async fn resend_top_shard_blocks(engine: &dyn EngineOperations) -> Result<()> {
    let id = if let Some(id) = engine.load_last_applied_mc_block_id()? {
        id
    } else {
        fail!("INTERNAL ERROR: No last applied MC block after sync")
    };
    let tsbs = engine.get_own_shard_blocks(id.seq_no)?;
    for tsb in tsbs {
        engine.send_top_shard_block_description(tsb, 0, true).await?;
    }
    Ok(())
}

pub fn save_top_shard_blocks_worker(
    engine: Arc<dyn EngineOperations>,
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<StoreAction>
) {
    tokio::spawn(async move {
        while let Some(action) = receiver.recv().await {
            match action {
                StoreAction::Save(id, tsb) => {
                    match engine.save_top_shard_block(&id, &tsb) {
                        Ok(_) => log::trace!("save_top_shard_block {}: OK", id),
                        Err(e) => log::error!("save_top_shard_block {}: {:?}", id, e),
                    }
                }
                StoreAction::Remove(id) => {
                    match engine.remove_top_shard_block(&id) {
                        Ok(_) => log::trace!("remove_top_shard_block {}: OK", id),
                        Err(e) => log::error!("remove_top_shard_block {}: {:?}", id, e),
                    }
                }
            }
        }
    });
}
