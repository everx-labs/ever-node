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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Sender, Receiver, unbounded, TryRecvError};

use ton_types::{Result, UInt256, error, fail};
use ton_block::{BlockIdExt, Deserializable, HashmapAugType, InMsg, ShardIdent};
use ton_api::ton::ton_node::{RempMessageLevel, RempMessageStatus, rempmessagestatus::RempAccepted};
use storage::block_handle_db::BlockHandle;

use crate::{block::BlockStuff, engine_traits::EngineOperations};
use crate::types::shard_blocks_observer::ShardBlocksObserver;
use crate::validator::message_cache::MessageCache;
use crate::validator::sessions_computing::{SessionValidatorsCache, SessionValidatorsList};
use crate::validator::validator_utils::get_message_uid;

#[async_trait::async_trait]
pub trait BlockProcessor : Sync + Send {
    async fn process_message (&self, message_id: &UInt256, message_uid: &UInt256);
}

pub async fn process_block_messages (message_cache: &MessageCache, block: BlockStuff, msg_processor: Arc<dyn BlockProcessor>) -> Result<()> {
    let mut messages_in_block: Vec<(UInt256,UInt256)> = Vec::new();
    let cc_seqno = block.block()?.read_info()?.gen_catchain_seqno();

    log::trace!(target: "remp", "REMP started processing block {} cc_seqno {}", block.id(), cc_seqno);

    block.block()?.read_extra()?.read_in_msg_descr()?.iterate_slices_with_keys(|key, mut msg_slice| {
        if let InMsg::External(ext) = InMsg::construct_from(&mut msg_slice)? {
            let message = ext.read_message()?;
            messages_in_block.push((key, get_message_uid(&message)));
        }
        Ok(true)
    })?;

    for (key, uid) in messages_in_block {
        msg_processor.process_message(&key, &uid).await;
    }

    let earlier_processed = !message_cache.mark_block_processed(block.id())?;
    log::trace!(target: "remp", "REMP finished processing block {} cc_seqno {}; block was earlier processed: {}", block.id(), cc_seqno, earlier_processed);
    Ok(())
}

pub async fn process_block_messages_by_blockid (
    engine: Arc<dyn EngineOperations>, message_cache: &MessageCache, id: BlockIdExt, msg_processor: Arc<dyn BlockProcessor>
) -> Result<()> {
    let handle = engine.load_block_handle(&id)?
        .ok_or_else(|| error!("Cannot load handle {}", id))?;
    let block = engine.load_block(&handle).await?;

    process_block_messages(message_cache, block, msg_processor).await
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

struct RempMasterblockObserverImpl {
    engine: Arc<dyn EngineOperations>,
    message_cache: Arc<MessageCache>,
    queue_sender: Sender<(BlockStuff, u32)>,
    queue_receiver: Receiver<(BlockStuff, u32)>,
}

impl RempMasterblockObserverImpl {
    pub fn new (
        engine: Arc<dyn EngineOperations>,
        message_cache: Arc<MessageCache>,
    ) -> Self {
        let (queue_sender, queue_receiver) = unbounded();
        Self {
            engine, message_cache,
            queue_sender, queue_receiver,
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
                    let master_blk_id = master_blk.id().clone();
                    log::trace!(target: "remp", "Next master block {} is available, processing messages from it...", master_blk_id);
                    match observer.process_next_mc_block(&master_blk).await {
                        Err(e) => log::error!(target: "remp", "Cannot process new blocks for REMP: `{}`", e),
                        Ok(shard_blocks) => {
                            let mut new_blocks = shard_blocks;
                            new_blocks.push((master_blk, master_blk_id.clone()));

                            for (blk_stuff, _mc_id) in new_blocks {
                                let blk_id = blk_stuff.id().clone();
                                let msg_processor = RempMasterBlockIndexingProcessor::new(
                                    blk_id.clone(),
                                    master_blk_id.clone(),
                                    self.message_cache.clone(),
                                    masterchain_seqno
                                );
                                if let Err(e) = process_block_messages(&self.message_cache, blk_stuff, Arc::new(msg_processor)).await {
                                    log::error!(target: "remp", "Cannot process messages from block {}: `{}`", blk_id, e)
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

            tokio::time::sleep(Duration::from_millis(1)).await;
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
            else {
                log::info!(target: "remp", "RempBlockObserver stopped")
            }
        });

        Ok(sender)
    }
}

pub struct RempMasterblockObserver {
    engine: Arc<dyn EngineOperations>,
    pub queue_sender: Sender<(BlockStuff, u32)>
}

impl RempMasterblockObserver {
    pub fn create_and_start(engine: Arc<dyn EngineOperations>,
                            message_cache: Arc<MessageCache>,
                            rt: tokio::runtime::Handle,
                            init_mc_block: Arc<BlockHandle>) -> Result<Self> {
        let queue_sender = RempMasterblockObserverImpl::toplevel(engine.clone(), message_cache, rt, init_mc_block)?;
        Ok(Self { engine, queue_sender })
    }

    pub async fn process_master_block_handle(&self, mc_handle: &BlockHandle) -> Result<()> {
        if mc_handle.id().seq_no() != 0 {
            let mc_blockstuff = self.engine.load_block(&mc_handle).await?;
            let master_cc_seqno = mc_blockstuff.block()?.read_info()?.gen_catchain_seqno();
            self.queue_sender.send((mc_blockstuff, master_cc_seqno))?;
        }
        Ok(())
    }

    pub async fn process_master_block_range(&self, lwb: &BlockIdExt, upb: &BlockIdExt) -> Result<()> {
        if lwb.seq_no() > upb.seq_no() {
            return Ok(())
        }

        let mut blk = lwb.clone();
        loop {
            let handle = self.engine
                .load_block_handle(&blk)?
                .ok_or_else(|| error!("Error in process_master_block_range {}..={}: cannot load block handle for {}",
                    lwb, upb, blk
                ))?;

            self.process_master_block_handle(&handle).await?;
            if &blk == upb {
                return Ok(());
            }
            blk = self.engine.load_block_next1(&blk).await?
        }
    }
}

/// Returns first non-processed block before `start`, but after `target_mc`
pub async fn check_history_up_to_cc(engine: Arc<dyn EngineOperations>, message_cache: Arc<MessageCache>, start: &Vec<BlockIdExt>, target_cc: u32)
    -> Result<Option<BlockIdExt>>
{
    let mut blocks_to_process = start.clone();
    let target_blocks = if target_cc > 0 {
        message_cache.get_inf_shards(target_cc)?
    }
    else {
        HashSet::default()
    };

    while let Some(top) = blocks_to_process.pop() {
        if top.seq_no == 0 || target_blocks.contains(&top) {
            log::trace!(target: "remp", "Traced {:?} to root/blockchain start, block {}; ref master cc {}", start, top, target_cc);
            continue;
        }

        if !message_cache.is_block_processed(&top)? {
            log::trace!(target: "remp", "Traced {:?} to one of its not processed predecessors: {}; waiting is needed", start, top);
            return Ok(Some(top.clone()))
        }
        else {
            blocks_to_process.push(engine.load_block_prev1(&top)?);
            if let Some(prev2) = engine.load_block_prev2(&top)? {
                blocks_to_process.push(prev2);
            }
        }
    }

    return Ok(None);
}

async fn get_block_cc_seqno(engine: Arc<dyn EngineOperations>, blk: &BlockIdExt) -> Result<u32> {
    let handle = engine.load_block_handle(blk)?.ok_or_else(|| error!("Cannot load info for block {}", blk))?;
    let block_stuff = engine.load_block(&handle).await?;
    let block_info = block_stuff.block()?.read_info()?;
    Ok(block_info.gen_catchain_seqno())
}

pub async fn find_previous_sessions(engine: Arc<dyn EngineOperations>, session_cache: &SessionValidatorsCache, prev: &Vec<BlockIdExt>, curr_cc_seqno: u32, curr_shard: &ShardIdent)
    -> Result<Arc<SessionValidatorsList>>
{
    if let Some(prev_list) = session_cache.get_prev_list (curr_shard, curr_cc_seqno) {
        return Ok(prev_list);
    }

    let mut prevs: HashSet<(ShardIdent, u32)> = HashSet::new();
    let mut to_process = prev.clone();
    while let Some(blk) = to_process.pop() {
        let cc_seqno = get_block_cc_seqno(engine.clone(), &blk).await?;
        log::trace!(target: "remp", "Computing prev blocks for [{:?}] cc_seqno {}: block {}, cc_seqno {}", prev, curr_cc_seqno, blk, cc_seqno);
        if blk.seq_no() > 0 && blk.shard() == curr_shard && cc_seqno == curr_cc_seqno {
            to_process.push(engine.load_block_prev1(&blk)?);
            if let Some(prev2) = engine.load_block_prev2(&blk)? {
                to_process.push(prev2);
            }
        }
        else {
            if let Some((old_shard, old_cc_seqno)) = prevs.replace((blk.shard().clone(), cc_seqno)) {
                fail!("Computing prev blocks for [{:?}], cc_seqno {}, shard {}; history is inconsistent: references {}/{} and {}/{}",
                    prev, curr_cc_seqno, curr_shard, old_shard, old_cc_seqno, blk.shard(), cc_seqno
                )
            }
        }
    };

    let mut res: SessionValidatorsList = SessionValidatorsList::new();

    for (shard,cc_seqno) in prevs.iter() {
        let session = session_cache.get_info(shard,*cc_seqno)
            .ok_or_else(|| error!("Computing prev blocks for [{:?}], cc_seqno {}, shard {}; history is inconsistent: info for session {} cc_seqno {} is not available",
                prev, curr_cc_seqno, curr_shard, shard, cc_seqno
            ))?;

        log::trace!(target: "remp", "Computing prev blocks for [{:?}] cc_seqno {}: got session {}", prev, curr_cc_seqno, session);
        res.add_session(session)?;
    }

    let prev_list = Arc::new(res);

    if let Some(max_cc_seqno) = prev_list.get_maximal_cc_seqno() {
        if max_cc_seqno != curr_cc_seqno - 1 {
            session_cache.add_prev_list (curr_shard, curr_cc_seqno, Arc::new(SessionValidatorsList::new()))?;
            fail!("Computing prev session for shard {} cc_seqno {} by prev blocks [{:?}]: unexpected diff {} != {} - 1",
                curr_shard, curr_cc_seqno, prev, max_cc_seqno, curr_cc_seqno
            )
        }
    }

    session_cache.add_prev_list (curr_shard, curr_cc_seqno, prev_list.clone())?;
    Ok(prev_list)
}
