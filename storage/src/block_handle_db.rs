use crate::{
    TARGET, db_impl_serializable, db::traits::KvcWriteable, node_state_db::NodeStateDb, 
    traits::Serializable, types::{BlockHandle, BlockMeta}
};
use adnl::common::{add_unbound_object_to_map, add_unbound_object_to_map_with_update};
use std::{io::Cursor, sync::{Arc, Weak}};
use ton_block::BlockIdExt;
use ton_types::{error, fail, Result};


db_impl_serializable!(BlockHandleDb, KvcWriteable, BlockIdExt, BlockMeta);

pub(crate) type BlockHandleCache = lockfree::map::Map<BlockIdExt, Weak<BlockHandle>>;

#[derive(Debug)]
pub enum StoreJob {
    Handle(Arc<BlockHandle>),
    State((&'static str, Arc<BlockIdExt>))
}

#[async_trait::async_trait]
pub trait Callback: Sync + Send {
    async fn invoke(&self, job: StoreJob, ok: bool);
}
 
pub struct BlockHandleStorage {
    handle_db: Arc<BlockHandleDb>,
    handle_cache: Arc<BlockHandleCache>,
    state_db: Arc<NodeStateDb>,
    state_cache: lockfree::map::Map<&'static str, Arc<BlockIdExt>>,
    storer: tokio::sync::mpsc::UnboundedSender<(StoreJob, Option<Arc<dyn Callback>>)>
}

impl BlockHandleStorage {

    pub fn with_dbs(handle_db: Arc<BlockHandleDb>, state_db: Arc<NodeStateDb>) -> Self {
        let (sender, mut reader) = tokio::sync::mpsc::unbounded_channel();
        let ret = Self {
            handle_db: handle_db.clone(),
            handle_cache: Arc::new(lockfree::map::Map::new()),
            state_db: state_db.clone(),
            state_cache: lockfree::map::Map::new(),
            storer: sender
        };
        tokio::spawn( 
            async move {
                while let Some((job, callback)) = reader.recv().await {
                    let ok = match &job {
                        StoreJob::Handle(handle) => {
                            if let Err(e) = handle_db.put_value(handle.id(), handle.meta()) {
                                log::error!(
                                    target: TARGET, 
                                    "ERROR: {} while storing handle {}", 
                                    e, handle.id()
                                );
                                false
                            } else {
                                true
                            }
                        },
                        StoreJob::State((key, id)) => {
                            let mut buf = Vec::new();
                            let result = id
                                .serialize(&mut buf)            
                                .and_then(|_| state_db.put(key, &buf[..]));
                            if let Err(e) = result {
                                log::error!(
                                    target: TARGET, 
                                    "ERROR: {} while storing state {}", 
                                    e, id
                                );
                                false
                            } else {
                                true
                            }
                        }
                    };
                    if let Some(callback) = callback {
                        callback.invoke(job, ok).await;
                    }
                }
                // Graceful close
                reader.close();
                while let Some(_) = reader.recv().await {
                }
            }
        );
        ret
    }
  	
    pub fn create_handle(
        &self, 
        id: BlockIdExt, 
        meta: BlockMeta,
        callback: Option<Arc<dyn Callback>>
    ) -> Result<Option<Arc<BlockHandle>>> {
        self.create_handle_and_store(id, meta, callback, true)
    }

    pub fn create_state(
        &self,
        key: &'static str,
        id: &BlockIdExt
    ) -> Result<Arc<BlockIdExt>> {
        let id = Arc::new(id.clone());
        if !add_unbound_object_to_map_with_update(
            &self.state_cache, 
            key,
            |_| Ok(Some(id.clone()))
        )? {
            fail!("INTERNAL ERROR: cannot create {} state")
        }
        Ok(id)
    }

    pub fn load_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>> {
        log::trace!(target: TARGET, "load block handle {}", id);
        let ret = loop {
            let weak = self.handle_cache.get(id);
            if let Some(Some(handle)) = weak.map(|weak| weak.val().upgrade()) {
                break Some(handle)
            }
            if let Some(meta) = self.handle_db.try_get_value(id)? {
                let handle = self.create_handle_and_store(id.clone(), meta, None, false)?;
                if let Some(handle) = handle {
                    break Some(handle)
                }
            } else {
                break None
            }
        };
        Ok(ret)
    }

    pub fn load_state(&self, key: &'static str) -> Result<Option<Arc<BlockIdExt>>> {
        log::trace!(target: TARGET, "load state {}", key);
        let ret = loop {
            if let Some(id) = self.state_cache.get(key) {
                break Some(id.val().clone())
            }
            if let Some(db_slice) = self.state_db.try_get(&key)? {
                let mut cursor = Cursor::new(db_slice.as_ref());
                let id = BlockIdExt::deserialize(&mut cursor)?;
                break Some(self.create_state(key, &id)?)
            } else {
                break None
            }
        };
        Ok(ret)
    }
    
    pub fn store_handle(
        &self, 
        handle: &Arc<BlockHandle>, 
        callback: Option<Arc<dyn Callback>>
    ) -> Result<()> {
        self.storer.send((StoreJob::Handle(handle.clone()), callback)).map_err(
            |_| error!("Cannot store handle {}: storer thread dropped", handle.id())
        )?;
        Ok(())
    }

    pub fn store_state(
        &self,
        key: &'static str,  
        id: &BlockIdExt
    ) -> Result<()> {
        let refid = self.create_state(key, id)?;
        self.storer.send((StoreJob::State((key, refid)), None)).map_err(
            |_| error!("Cannot store state {}: storer thread dropped", id)
        )?;
        Ok(())
    }

    fn create_handle_and_store(
        &self, 
        id: BlockIdExt, 
        meta: BlockMeta,
        callback: Option<Arc<dyn Callback>>,
        store: bool
    ) -> Result<Option<Arc<BlockHandle>>> {
        let ret = Arc::new(BlockHandle::with_values(id.clone(), meta, self.handle_cache.clone()));
        let added = add_unbound_object_to_map(
            &self.handle_cache, 
            id, 
            || Ok(Arc::downgrade(&ret))
        )?;
        if added {
            if store {
                self.store_handle(&ret, callback)?
            }
            Ok(Some(ret))
        } else {
            Ok(None)
        }
    }


}


