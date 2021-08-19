use storage::shardstate_db::AllowStateGcResolver;
use ton_block::{BlockIdExt, UnixTime32, ShardIdent, AccountIdPrefixFull};
use ton_types::Result;
use adnl::common::add_object_to_map_with_update;
use crate::engine_traits::EngineOperations;
use std::{
    sync::atomic::{AtomicU32, Ordering},
    collections::HashSet,
};

pub struct AllowStateGcSmartResolver {
    last_processed_block: AtomicU32,
    min_ref_mc_block: AtomicU32,
    min_actual_ss: lockfree::map::Map<ShardIdent, AtomicU32>,
}

impl AllowStateGcSmartResolver {
    pub fn new() -> Self {
        Self {
            last_processed_block: AtomicU32::new(0),
            min_ref_mc_block: AtomicU32::new(0),
            min_actual_ss: lockfree::map::Map::new(),
        }
    }

    pub async fn advance(&self, mc_block_id: &BlockIdExt, engine: &dyn EngineOperations) -> Result<()> {
        let seqno = mc_block_id.seq_no();
        if seqno <= self.last_processed_block.fetch_max(seqno, Ordering::Relaxed) {
            return Ok(())
        }

        let mc_state = engine.load_state(mc_block_id).await?;
        let new_min_ref_mc_seqno = mc_state.state().min_ref_mc_seqno();
        let old_min_ref_mc_seqno = self.min_ref_mc_block.fetch_max(new_min_ref_mc_seqno, Ordering::Relaxed);

        log::trace!(
            "AllowStateGcSmartResolver::advance:  new_min_ref_mc_seqno {}  old_min_ref_mc_seqno {}",
            new_min_ref_mc_seqno, old_min_ref_mc_seqno,
        );

        if new_min_ref_mc_seqno > old_min_ref_mc_seqno {
            log::info!(
                "AllowStateGcSmartResolver::advance: updated min_ref_mc_block {} -> {}, mc_block_id: {}",
                old_min_ref_mc_seqno, new_min_ref_mc_seqno, mc_block_id,
            );

            let mc_pfx = AccountIdPrefixFull::any_masterchain();
            let handle = engine.find_block_by_seq_no(&mc_pfx, new_min_ref_mc_seqno).await?;
            let min_mc_state = engine.load_state(handle.id()).await?;

            let (_master, workchain_id) = engine.processed_workchain().await?;
            let top_blocks = min_mc_state.shard_hashes()?.top_blocks(&[workchain_id])?;
            let mut actual_shardes = HashSet::new();
            for id in top_blocks {
                add_object_to_map_with_update(
                    &self.min_actual_ss,
                    id.shard().clone(),
                    |found| if let Some(a) = found {
                        let old_val = a.fetch_max(id.seq_no(), Ordering::Relaxed);
                        if old_val != id.seq_no() {
                            log::info!(
                                "AllowStateGcSmartResolver::advance: updated min actual state for shard {}: {} -> {}",
                                id.shard(), old_val, id.seq_no()
                            );
                        }
                        Ok(None)
                    } else {
                        log::info!(
                            "AllowStateGcSmartResolver::advance: added min actual state for shard {}: {}",
                            id.shard(), id.seq_no()
                        );
                        Ok(Some(AtomicU32::new(id.seq_no())))
                    }
                )?;
                actual_shardes.insert(id.shard().clone());
            }

            for kv in self.min_actual_ss.iter() {
                if !actual_shardes.contains(kv.key()) {
                    self.min_actual_ss.remove(kv.key());
                    log::info!(
                        "AllowStateGcSmartResolver::advance: removed shard {}", kv.key()
                    );
                }
            }
        }

        Ok(())
    }
}

impl AllowStateGcResolver for AllowStateGcSmartResolver {
    fn allow_state_gc(&self, block_id: &BlockIdExt, _gc_utime: UnixTime32) -> Result<bool> {
        if block_id.shard().is_masterchain() {
            if block_id.seq_no() != 0 { // we need zerostate
                let min_ref_mc_block = self.min_ref_mc_block.load(Ordering::Relaxed);
                return Ok(block_id.seq_no() < min_ref_mc_block);
            }
        } else {
            if let Some(kv) = self.min_actual_ss.get(block_id.shard()) {
                let min_actual = kv.val().load(Ordering::Relaxed);
                return Ok(block_id.seq_no() < min_actual);
            } else {
                for kv in self.min_actual_ss.iter() {
                    if block_id.shard().intersect_with(kv.key()) {
                        let min_actual = kv.val().load(Ordering::Relaxed); 
                        return Ok(block_id.seq_no() < min_actual);
                    }
                }
            }
        }
        Ok(false)
    }
}
