use crate::{block::BlockStuff, engine_traits::EngineOperations};
use ton_types::{Result, UInt256, error, fail};
use ton_block::{BlockIdExt, HashmapAugType};
use std::sync::Arc;
use std::ops::Deref;
use std::time::Duration;
use storage::block_handle_db::BlockHandle;
use crate::types::shard_blocks_observer::ShardBlocksObserver;
use ton_api::ton::ton_node::{RempMessageLevel, RempMessageStatus};
use crossbeam_channel::{Sender, Receiver, unbounded, TryRecvError};
use ton_api::ton::ton_node::rempmessagestatus::RempAccepted;
use crate::validator::message_cache::MessageCache;

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

struct RempMasterBlockIndexingProcessor {
    block_id: BlockIdExt,
    master_id: BlockIdExt,
    message_cache: Arc<MessageCache>
}

impl RempMasterBlockIndexingProcessor {
    pub fn new (block_id: BlockIdExt, master_id: BlockIdExt, message_cache: Arc<MessageCache>) -> Self {
        Self { block_id, master_id, message_cache }
    }
}

#[async_trait::async_trait]
impl BlockProcessor for RempMasterBlockIndexingProcessor {
    async fn process_message (&self, message_id: &UInt256) {
        let accepted = RempAccepted {
            level: RempMessageLevel::TonNode_RempMasterchain,
            block_id: self.block_id.clone(),
            master_id: self.master_id.clone()
        };

        if let Err(e) = self.message_cache.update_message_status (message_id, RempMessageStatus::TonNode_RempAccepted(accepted)).await {
            log::warn!(target: "remp", "Update message {:x} status failed: {}", message_id, e);
        }
    }
}

pub struct RempBlockObserverToplevel {
    engine: Arc<dyn EngineOperations>,
    rt: tokio::runtime::Handle,
    message_cache: Arc<MessageCache>,
    queue_sender: Sender<BlockStuff>,
    queue_receiver: Receiver<BlockStuff>,
    response_sender: Sender<(BlockStuff, BlockIdExt)>,
    response_receiver: Receiver<(BlockStuff, BlockIdExt)>,
}

impl RempBlockObserverToplevel {
    pub fn new (
        engine: Arc<dyn EngineOperations>,
        message_cache: Arc<MessageCache>,
        rt: tokio::runtime::Handle
    ) -> Self {
        let (queue_sender, queue_receiver) = unbounded();
        let (response_sender, response_receiver) = unbounded();
        Self {
            engine, message_cache,
            queue_sender, queue_receiver,
            response_sender, response_receiver,
            rt
        }
    }

    async fn top_level_cycle(&self, init_mc_block: Arc<BlockHandle>) -> Result<()> {
        let mut observer = ShardBlocksObserver::new(
            &init_mc_block,
            self.engine.clone(),
            |block, mc_id| {
                match self.response_sender.send((block.clone(), mc_id.clone())) {
                    Err(e) => fail!("{}", e),
                    Ok(()) => Ok(())
                }
            }
        ).await?;

        loop {
            match self.queue_receiver.try_recv() {
                Ok(x) => {
                    match observer.process_next_mc_block(&x).await {
                        Err(e) => log::error!(target: "remp", "Cannot process new blocks for REMP: `{}`", e),
                        Ok(()) =>
                            while let Ok((blk_stuff, blk_id)) = self.response_receiver.try_recv() {
                                let msg_processor = RempMasterBlockIndexingProcessor::new(blk_id.clone(), x.id().clone(), self.message_cache.clone());
                                match process_block_messages(blk_stuff, Arc::new(msg_processor)).await {
                                    Ok(()) => (),
                                    Err(e) => log::error!(target: "remp", "Cannot process messages from block {}: `{}`", blk_id, e)
                                }
                            }
                    }
                },
                Err(TryRecvError::Disconnected) => {
                    log::warn!(target: "remp", "RempBlockObserverToplevel: end of masterblock channel, exiting");
                    return Ok(());
                },
                Err(TryRecvError::Empty) => (),
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub fn toplevel(engine: Arc<dyn EngineOperations>,
                    message_cache: Arc<MessageCache>,
                    rt: tokio::runtime::Handle,
                    init_mc_block: Arc<BlockHandle>
    ) -> Result<Sender<BlockStuff>> {
        let mut top_level_self = Self::new(engine, message_cache, rt.clone());
        let sender = top_level_self.queue_sender.clone();

        rt.clone().spawn(async move {
            if let Err(e) = top_level_self.top_level_cycle(init_mc_block).await {
                log::error!(target: "remp", "RempBlockObserver failure: `{}`", e)
            }
        });

        Ok(sender)
    }
}
