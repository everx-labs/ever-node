/*
* Copyright (C) 2019-2023 EverX. All Rights Reserved.
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

use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Sender, Receiver, unbounded, TryRecvError};

use ton_types::{Result, UInt256, error};
use ton_block::{BlockIdExt, Deserializable, HashmapAugType, InMsg};
use ton_api::ton::ton_node::{RempMessageLevel, RempMessageStatus, rempmessagestatus::RempAccepted};
use storage::block_handle_db::BlockHandle;

use crate::{block::BlockStuff, engine_traits::EngineOperations};
use crate::types::shard_blocks_observer::ShardBlocksObserver;
use crate::validator::message_cache::MessageCache;
use crate::validator::validator_utils::get_message_uid;

#[async_trait::async_trait]
pub trait BlockProcessor : Sync + Send {
    async fn process_message (&self, message_id: &UInt256, message_uid: &UInt256);
}

pub async fn process_block_messages (block: BlockStuff, msg_processor: Arc<dyn BlockProcessor>) -> Result<()> {
    log::trace!(target: "remp", "REMP started processing block {}", block.id());

    let mut messages_in_block: Vec<(UInt256,UInt256)> = Vec::new();
    let block_result = block.block()?.read_extra()?.read_in_msg_descr()?.iterate_slices_with_keys(|key, mut msg_slice| {
        let in_msg = InMsg::construct_from(&mut msg_slice)?;
        let message = in_msg.read_message()?;
        messages_in_block.push((key, get_message_uid(&message)));
        Ok(true)
    });

    if let Err(err) = block_result {
        log::error!(target: "remp", "Error processing block {}: {}", block.id(), err);
    }

    for (key, uid) in messages_in_block {
        msg_processor.process_message(&key, &uid).await;
    }

    log::trace!(target: "remp", "REMP finished processing block {}", block.id());
    Ok(())
}

pub async fn process_block_messages_by_blockid (
    engine: Arc<dyn EngineOperations>, id: BlockIdExt, msg_processor: Arc<dyn BlockProcessor>
) -> Result<()> {
    let handle = engine.load_block_handle(&id)?
        .ok_or_else(|| error!("Cannot load handle {}", id))?;
    let block = engine.load_block(&handle).await?;

    process_block_messages(block, msg_processor).await
}

pub struct RempMasterBlockIndexingProcessor {
    block_id: BlockIdExt,
    master_id: BlockIdExt,
    message_cache: Arc<MessageCache>,
    masterchain_seqno: u32
}

impl RempMasterBlockIndexingProcessor {
    pub fn new (block_id: BlockIdExt, master_id: BlockIdExt, message_cache: Arc<MessageCache>, masterchain_seqno: u32) -> Self {
        Self { block_id, master_id, message_cache, masterchain_seqno }
    }
}

#[async_trait::async_trait]
impl BlockProcessor for RempMasterBlockIndexingProcessor {
    async fn process_message (&self, message_id: &UInt256, message_uid: &UInt256) {
        let accepted = RempAccepted {
            level: RempMessageLevel::TonNode_RempMasterchain,
            block_id: self.block_id.clone(),
            master_id: self.master_id.clone()
        };

        if let Err(e) = self.message_cache.add_external_message_status(
            message_id, message_uid, None, RempMessageStatus::TonNode_RempAccepted(accepted),
            |_o,n| n.clone(), self.masterchain_seqno
        ).await {
            log::warn!(target: "remp", "Update message {:x}, uid {:x} status failed: {}", message_id, message_uid, e);
        }
    }
}

pub struct RempBlockObserverToplevel {
    engine: Arc<dyn EngineOperations>,
    message_cache: Arc<MessageCache>,
    queue_sender: Sender<(BlockStuff, u32)>,
    queue_receiver: Receiver<(BlockStuff, u32)>,
}

impl RempBlockObserverToplevel {
    pub fn new (
        engine: Arc<dyn EngineOperations>,
        message_cache: Arc<MessageCache>,
    ) -> Self {
        let (queue_sender, queue_receiver) = unbounded();
        //let (response_sender, response_receiver) = unbounded();
        Self {
            engine, message_cache,
            queue_sender, queue_receiver,
            //response_sender, response_receiver
        }
    }

    async fn top_level_cycle(&self, init_mc_block: Arc<BlockHandle>) -> Result<()> {
        let mut observer = ShardBlocksObserver::new(
            &init_mc_block,
            self.engine.clone()
        ).await?;

        loop {
            match self.queue_receiver.try_recv() {
                Ok((master_blk, masterchain_seqno)) => {
                    log::trace!(target: "remp", "Next master block {} is available, processing messages from it...", master_blk.id());
                    match observer.process_next_mc_block(&master_blk).await {
                        Err(e) => log::error!(target: "remp", "Cannot process new blocks for REMP: `{}`", e),
                        Ok(new_shard_blocks) => {
                            for (blk_stuff, _mc_id) in new_shard_blocks {
                                let blk_id = blk_stuff.id().clone();
                                let msg_processor = RempMasterBlockIndexingProcessor::new(
                                    blk_id.clone(),
                                    master_blk.id().clone(),
                                    self.message_cache.clone(),
                                    masterchain_seqno
                                );
                                match process_block_messages(blk_stuff, Arc::new(msg_processor)).await {
                                    Ok(()) => (),
                                    Err(e) => log::error!(target: "remp", "Cannot process messages from block {}: `{}`", blk_id, e)
                                }
                            }
                        }
                    }
                },
                Err(TryRecvError::Disconnected) => {
                    log::warn!(target: "remp", "RempBlockObserverToplevel: end of masterblock channel, exiting");
                    return Ok(());
                },
                Err(TryRecvError::Empty) => ()
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub fn toplevel(engine: Arc<dyn EngineOperations>,
                    message_cache: Arc<MessageCache>,
                    rt: tokio::runtime::Handle,
                    init_mc_block: Arc<BlockHandle>
    ) -> Result<Sender<(BlockStuff,u32)>> {
        let top_level_self = Self::new(engine, message_cache);
        let sender = top_level_self.queue_sender.clone();

        rt.clone().spawn(async move {
            if let Err(e) = top_level_self.top_level_cycle(init_mc_block).await {
                log::error!(target: "remp", "RempBlockObserver failure: `{}`", e)
            }
        });

        Ok(sender)
    }
}
