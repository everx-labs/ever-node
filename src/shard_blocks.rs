use crate::{
    types::top_block_descr::{TopBlockDescrStuff, TopBlockDescrId},
    engine_traits::EngineOperations,
    shard_state::ShardStateStuff,
};
use std::{io::Cursor, sync::{Arc, atomic::{AtomicU32, Ordering}}};
use ton_block::{BlockIdExt, TopBlockDescr, Deserializable, BlockSignatures};
use ton_types::{error, fail, Result, deserialize_tree_of_cells};


pub struct ShardBlocksPool {
    last_mc_seq_no: AtomicU32,
    shard_blocks: lockfree::map::Map<TopBlockDescrId, Arc<TopBlockDescrStuff>>,
    is_fake: bool,
}

impl ShardBlocksPool {

    pub fn new() -> Self {
        ShardBlocksPool {
            last_mc_seq_no: AtomicU32::new(0),
            shard_blocks: lockfree::map::Map::new(),
            is_fake: false,
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

        let last_id = engine.load_last_applied_mc_block_id().await?;
        match engine.load_block_handle(&last_id)?.ok_or_else(
            || error!("Cannot load handle for last master block {}", last_id)
        ) {
            Ok(mc_block) => Ok(
                self.is_fake || tbds.unwrap().gen_utime() < mc_block.gen_utime()? + 60
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
                removed_list.append(format!("\n{} {}", block.key().cc_seqno, block.key().id));
            }
        }
        log::trace!("update_shard_blocks last_mc_state {} removed: {}", 
            last_mc_state.block_id(), removed_list.string().unwrap_or_default());
        Ok(())
    }
}
