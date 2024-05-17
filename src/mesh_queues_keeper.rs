use std::sync::Arc;
use storage::shardstate_db_async::AllowStateGcResolver;
use ever_block::{BlockIdExt, ShardIdent, OutMsgQueueInfo, Result, fail};

pub struct MeshQueuesKeeper {
    queues: lockfree::map::Map<(i32, BlockIdExt, ShardIdent), Arc<OutMsgQueueInfo>>,
}

impl MeshQueuesKeeper {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            queues: lockfree::map::Map::new(),
        })
    }

    // ShardStatesKeeper calls this method from clean_cache_worker if cache_resolver advanced
    pub fn gc(
        &self, 
        cache_resolver: &dyn AllowStateGcResolver,
    ) -> Result<()> {
        log::debug!("MeshQueuesKeeper::gc: started");
        let now = std::time::Instant::now();
        let mut total = 0;
        let mut cleaned = 0;


        // TODO while mesh nw is not active it is not commited to our masterchain, so we don't know 
        // what to delete. It is not a big deal, because queues are empty, but we should fix it somehow.


        for guard in &self.queues {
            total += 1;
            let (nw_id, mc_id, shard) = guard.key();
            if cache_resolver.allow_state_gc(*nw_id, mc_id, 0, 0)? {
                log::trace!("MeshQueuesKeeper::gc: removing queue for {nw_id} {mc_id} {shard}");
                self.queues.remove(guard.key());
                cleaned +=1;
            }
        }
        log::debug!(
            "MeshQueuesKeeper::gc: finished TIME {time}ms, cleaned: {cleaned}, total: {total}",
            time = now.elapsed().as_millis()
        );
        Ok(())
    }

    pub fn store_mesh_queue(
        &self,
        nw_id: i32,
        mc_block_id: &BlockIdExt,
        shard: &ShardIdent,
        queue: Arc<OutMsgQueueInfo>
    ) -> Result<()> {
        let key = (nw_id, mc_block_id.clone(), shard.clone());
        let _ = self.queues.insert(key, queue);
        Ok(())
    }

    pub fn load_mesh_queue(
        &self,
        nw_id: i32,
        mc_block_id: &BlockIdExt,
        shard: &ShardIdent
    ) -> Result<Arc<OutMsgQueueInfo>> {
        let key = (nw_id, mc_block_id.clone(), shard.clone());
        if let Some(queue_root) = self.queues.get(&key) {
            Ok(queue_root.val().clone())
        } else {
            fail!("Mesh queue {nw_id} {mc_block_id} {shard} not found", )
        }
    }
}