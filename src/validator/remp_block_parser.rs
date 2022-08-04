use crate::{block::BlockStuff, engine_traits::EngineOperations};
use ton_types::{Result, UInt256, error};
use ton_block::{BlockIdExt, HashmapAugType};
use std::sync::Arc;
use std::ops::Deref;

#[async_trait::async_trait]
pub trait BlockProcessor : Sync + Send {
    async fn process_message (&self, message_id: &UInt256);
}

pub async fn process_block_messages (block: BlockStuff, msg_processor: Arc<dyn BlockProcessor>) -> Result<()> {
    log::trace!(target: "remp", "REMP started processing block {}", block.id());

    let mut messages_in_block: Vec<UInt256> = Vec::new();
    block.block().read_extra()?.read_in_msg_descr()?.iterate_slices_with_keys(|key, _msg_slice| {
        messages_in_block.push(key);
        Ok(true)
    })?;

    for key in messages_in_block {
        msg_processor.process_message(&key).await;
    }

    log::trace!(target: "remp", "REMP finished processing block {}", block.id());
    Ok(())
}

pub async fn process_block_messages_by_blockid (
    engine: Arc<dyn EngineOperations>, id: BlockIdExt, msg_processor: Arc<dyn BlockProcessor>
) -> Result<()> {
    let block = engine.load_block(
        engine.load_block_handle(&id)?.ok_or_else(
            || error!("Cannot load handle {}", id)
        )?.deref()
    ).await?;

    process_block_messages(block, msg_processor).await
}
