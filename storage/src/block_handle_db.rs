use crate::{
    db_impl_serializable, db::traits::KvcWriteable, traits::Serializable, 
    types::{BlockHandle, BlockMeta}, TARGET,
};
use adnl::common::add_object_to_map;
use std::{fmt::Debug, sync::{Arc, Weak}};
use ton_block::BlockIdExt;
use ton_types::Result;


db_impl_serializable!(BlockHandleDb, KvcWriteable, BlockIdExt, BlockMeta);

pub(crate) type BlockHandleCache = lockfree::map::Map<BlockIdExt, Weak<BlockHandle>>;
pub struct BlockHandleStorage {
    cache: Arc<BlockHandleCache>,
    db: Arc<BlockHandleDb>,
    storer: tokio::sync::mpsc::UnboundedSender<Arc<BlockHandle>>
}

impl BlockHandleStorage {

    pub fn with_db(db: Arc<BlockHandleDb>) -> Self {
        let (sender, mut reader) = tokio::sync::mpsc::unbounded_channel();
        let ret = Self {
            cache: Arc::new(lockfree::map::Map::new()),
            db: db.clone(),
            storer: sender
        };
        tokio::spawn( 
            async move {
                while let Some(handle) = reader.recv().await {
                    if let Err(e) = db.put_value(handle.id(), handle.meta()) {
                        log::error!(target: TARGET, "ERROR: {} while storing", e);
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
  	
    pub fn load_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>> {
        log::trace!(target: TARGET, "load_block_handle {}", id);
        let ret = loop {
            let weak = self.cache.get(id);
            if let Some(Some(handle)) = weak.map(|weak| weak.val().upgrade()) {
//println!("FETCHED");
                break Some(handle)
            }
            if let Some(meta) = self.db.try_get_value(id)? {
                if let Some(handle) = self.create_handle(id.clone(), meta)? {
//println!("CREATED");
                    break Some(handle)
                }
            } else {
                break None
            }
//println!("SECOND ROUND");
        };
        Ok(ret)
    }

    pub fn store_handle(&self, handle: &Arc<BlockHandle>) -> Result<()> {
        self.storer.send(handle.clone())?;
        Ok(())
    }

    pub fn create_handle(
        &self, 
        id: BlockIdExt, 
        meta: BlockMeta
    ) -> Result<Option<Arc<BlockHandle>>> {
        let ret = Arc::new(BlockHandle::with_values(id.clone(), meta, self.cache.clone()));
        let added = add_object_to_map(
            &self.cache, 
            id, 
            || Ok(Arc::downgrade(&ret))
        )?;
        if added {
            self.store_handle(&ret)?;
            Ok(Some(ret))
        } else {
            Ok(None)
        }
    }

}

