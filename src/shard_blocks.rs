use crate::{
    types::top_block_descr::{TopBlockDescrStuff, TopBlockDescrId},
    engine_traits::EngineOperations,
    shard_state::ShardStateStuff,
};
use ton_block::{BlockIdExt, TopBlockDescr, Deserializable, BlockSignatures};
use ton_types::{fail, error, Result, deserialize_tree_of_cells};
use std::{
    io::Cursor,
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

pub struct ShardBlocksPool {
    last_mc_seq_no: AtomicU32,
    shard_blocks: lockfree::map::Map<TopBlockDescrId, Arc<TopBlockDescrStuff>>,
    storage_sender: Option<tokio::sync::mpsc::UnboundedSender<StoreAction>>,
    is_fake: bool,
}

impl ShardBlocksPool {

    pub fn new(
        shard_blocks: HashMap<TopBlockDescrId, TopBlockDescrStuff>,
        last_mc_seqno: u32,
        is_fake: bool
    ) -> (Self, tokio::sync::mpsc::UnboundedReceiver<StoreAction>) {
        let tsbs = lockfree::map::Map::new();
        for (k, v) in shard_blocks {
            tsbs.insert(k, Arc::new(v));
        }
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        (
            ShardBlocksPool {
                last_mc_seq_no: AtomicU32::new(last_mc_seqno),
                shard_blocks: tsbs,
                storage_sender: Some(sender.clone()),
                is_fake,
            },
            receiver
        )
    }

    pub fn fake_without_store()
    -> Self {
        ShardBlocksPool {
            last_mc_seq_no: AtomicU32::new(0),
            shard_blocks: lockfree::map::Map::new(),
            storage_sender: None,
            is_fake: true,
        }
    }

    pub async fn add_shard_block(
        &self,
        id: &BlockIdExt,
        cc_seqno: u32,
        data: Vec<u8>,
        engine: &dyn EngineOperations,
    ) -> Result<bool> {

        let tbds_id = TopBlockDescrId::new(id.shard().clone(), cc_seqno);
        let mut tbds = None;

        'a: loop {

            log::trace!("add_shard_block iteration  cc_seqno: {}  id: {}", cc_seqno, id);

            // check for duplication
            if let Some(prev) = self.shard_blocks.get(&tbds_id) {
                if id.seq_no() <= prev.val().proof_for().seq_no() {
                    log::trace!("add_shard_block duplication  cc_seqno: {}  id: {}  prev: {}", 
                        cc_seqno, id, prev.val().proof_for());
                    return Ok(false);
                }
            }

            // validate top block descr
            if tbds.is_none() {
                let tbd = if self.is_fake {
                    TopBlockDescr::with_id_and_signatures(id.clone(), BlockSignatures::default())
                } else {
                    let root = deserialize_tree_of_cells(&mut Cursor::new(&data))?;
                    TopBlockDescr::construct_from(&mut root.into())?
                };
                tbds = Some(Arc::new(TopBlockDescrStuff::new(tbd, &id, self.is_fake)?));
            }

            if !self.is_fake {
                tbds.as_ref().unwrap().validate(&engine.load_last_applied_mc_state().await?)?;
            }

            // add

            // TODO use adapters from adnl::common
            // This is so-called "interactive insertion"
            let result = self.shard_blocks.insert_with(tbds_id.clone(), |_key, prev_gen_val, updated_pair | {
                if updated_pair.is_some() {
                    // someone already added the value into map
                    if id.seq_no() <= updated_pair.unwrap().1.proof_for().seq_no() {
                        lockfree::map::Preview::Discard
                    } else {
                        lockfree::map::Preview::New(tbds.as_ref().unwrap().clone())
                    }
                } else if prev_gen_val.is_some() {
                    // it is value we inserted just now
                    lockfree::map::Preview::Keep
                } else {
                    // there is not the value in the map - try to add.
                    // If other thread adding value the same time - the closure will be recalled
                    lockfree::map::Preview::New(tbds.as_ref().unwrap().clone())
                }
            });
            match result {
                lockfree::map::Insertion::Created => {
                    log::trace!("add_shard_block added  cc_seqno: {}  id: {}", cc_seqno, id);
                    break 'a;
                },
                lockfree::map::Insertion::Updated(old) => {
                    log::trace!("add_shard_block updated  cc_seqno: {}  id: {}  old: {} {}",
                        cc_seqno, id, old.key().cc_seqno, old.key().id);
                    break 'a;
                },
                lockfree::map::Insertion::Failed(_) => {
                    continue;
                }
            }
        }

        let tbds = tbds.unwrap();

        self.send_to_storage(StoreAction::Save(tbds_id.clone(), tbds.clone()));

        let last_id = engine.load_last_applied_mc_block_id().await?;
        match engine.load_block_handle(&last_id)?.ok_or_else(
            || error!("Cannot load handle for last master block {}", last_id)
        ) {
            Ok(mc_block) => Ok(
                self.is_fake || tbds.gen_utime() < mc_block.gen_utime()? + 60
            ),
            Err(e) => {
                log::trace!("add_shard_block failed for block {}, {} (too new?)", id, e);
                Ok(false)
            }
        }
    }

    pub fn get_shard_blocks(&self, last_mc_seq_no: u32) -> Result<Vec<Arc<TopBlockDescrStuff>>> {
        if last_mc_seq_no != self.last_mc_seq_no.load(Ordering::Relaxed) {
            log::error!("get_shard_blocks: Given last_mc_seq_no {} is not actual", last_mc_seq_no);
            fail!("Given last_mc_seq_no {} is not actual {}", last_mc_seq_no, self.last_mc_seq_no.load(Ordering::Relaxed));
        } else {
            let mut returned_list = string_builder::Builder::default();
            let mut blocks = Vec::new();
            for block in self.shard_blocks.iter() {
                blocks.push(block.1.clone());
                returned_list.append(format!("\n{} {}", block.key().cc_seqno, block.key().id));
            }        
            log::trace!("get_shard_blocks last_mc_seq_no {} returned: {}", 
                last_mc_seq_no, returned_list.string().unwrap_or_default());
            Ok(blocks)
        }
    }

    pub fn update_shard_blocks(&self, last_mc_state: &ShardStateStuff) -> Result<()> {
        self.last_mc_seq_no.store(last_mc_state.block_id().seq_no(), Ordering::Relaxed);
        let mut removed_list = string_builder::Builder::default();
        for block in self.shard_blocks.iter() {
            if block.val().validate(last_mc_state).is_err() {
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
                Err(_) => log::error!("ShardBlocksPool::send_to_storage: can't send"),
            }
        }
    }
}

pub fn resend_top_shard_blocks_worker(engine: Arc<dyn EngineOperations>) {
    tokio::spawn(async move {
        loop {
             // 2..3 seconds
            let delay = rand::thread_rng().gen_range(2000, 3000);
            futures_timer::Delay::new(Duration::from_millis(delay)).await;
            match resend_top_shard_blocks(engine.deref()).await {
                Ok(_) => log::trace!("resend_top_shard_blocks: ok"),
                Err(e) => log::error!("resend_top_shard_blocks: {:?}", e)
            }
        }
    });
}

async fn resend_top_shard_blocks(engine: &dyn EngineOperations) -> Result<()> {
    let id = engine.load_last_applied_mc_block_id().await?;
    let tsbs = engine.get_shard_blocks(id.seq_no)?;
    for tsb in tsbs {
        engine.send_top_shard_block_description(&tsb).await?;
    }
    Ok(())
}

pub fn save_top_shard_blocks_worker(
    engine: Arc<dyn EngineOperations>,
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<StoreAction>,
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
