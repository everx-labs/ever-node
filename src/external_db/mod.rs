/*
* Copyright (C) 2019-2021 TON Labs. All Rights Reserved.
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

use crate::{
    engine_traits::{ExternalDb, EngineOperations, ChainRange}, config::ExternalDbConfig,
    engine::Engine, block::BlockStuff,
};
use processor::Processor;
use std::sync::Arc;
use ton_block::BlockIdExt;
use ton_types::{Result, error, fail};

mod processor;
#[cfg(feature = "external_db")]
mod kafka_producer;
#[cfg(feature = "external_db")]
pub mod kafka_consumer;
#[cfg(not(feature = "external_db"))]
mod stub_producer;

#[async_trait::async_trait]
pub trait WriteData : Sync + Send {
    fn enabled(&self) -> bool;
    fn sharding_depth(&self) -> u32;
    async fn write_data(&self, key: String, data: String, attributes: Option<&[(&str, &[u8])]>, partition_key: Option<u32>) -> Result<()>;
    async fn write_raw_data(&self, key: Vec<u8>, data: Vec<u8>, attributes: Option<&[(&str, &[u8])]>, partition_key: Option<u32>) -> Result<()>;
}

#[allow(dead_code)]
#[cfg(not(feature = "external_db"))]
pub fn create_external_db(config: ExternalDbConfig, front_workchain_ids: Vec<i32>) -> Result<Arc<dyn ExternalDb>> {
    Ok(
        Arc::new(
            Processor::new(
                stub_producer::StubProducer{enabled: true},
                stub_producer::StubProducer{enabled: true},
                stub_producer::StubProducer{enabled: true},
                stub_producer::StubProducer{enabled: true},
                stub_producer::StubProducer{enabled: true},
                stub_producer::StubProducer{enabled: true},
                stub_producer::StubProducer{enabled: true},
                stub_producer::StubProducer{enabled: true},
                config.bad_blocks_storage,
                front_workchain_ids,
            )
        )
    )
}

#[allow(dead_code)]
#[cfg(feature = "external_db")]
pub fn create_external_db(
    config: ExternalDbConfig, front_workchain_ids: Vec<i32>, control_id: Option<[u8; 32]>
) -> Result<Arc<dyn ExternalDb>> {

    let max_account_bytes_size = match config.account_producer.big_messages_storage {
        Some(_) => config.account_producer.big_message_max_size,
        None => Some(config.account_producer.message_max_size),
    };
    let writers = processor::Writers {
        write_block: kafka_producer::KafkaProducer::new(config.block_producer)?,
        write_raw_block: kafka_producer::KafkaProducer::new(config.raw_block_producer)?,
        write_message: kafka_producer::KafkaProducer::new(config.message_producer)?,
        write_transaction: kafka_producer::KafkaProducer::new(config.transaction_producer)?,
        write_account: kafka_producer::KafkaProducer::new(config.account_producer)?,
        write_block_proof: kafka_producer::KafkaProducer::new(config.block_proof_producer)?,
        write_raw_block_proof: kafka_producer::KafkaProducer::new(config.raw_block_proof_producer)?,
        write_chain_range: kafka_producer::KafkaProducer::new(config.chain_range_producer)?,
        write_remp_statuses: kafka_producer::KafkaProducer::new(config.remp_statuses_producer)?,
        write_shard_hashes: kafka_producer::KafkaProducer::new(config.shard_hashes_producer)?,
    };
    if writers.write_shard_hashes.enabled() && control_id.is_none() {
        fail!("Control server config should be specified is shard hashes writer is enabled")
    }
    Ok(
        Arc::new(
            Processor::new(
                writers,
                config.bad_blocks_storage,
                front_workchain_ids,
                max_account_bytes_size,
                control_id.unwrap_or_default(),
            )
        )
    )
}

pub fn start_external_db_worker(
    engine: Arc<dyn EngineOperations>,
    mut external_db_block: BlockIdExt
) -> Result<tokio::task::JoinHandle<()>> {

    log::info!("start_external_db_worker");
    let join_handle = tokio::spawn(async move {
        engine.acquire_stop(Engine::MASK_SERVICE_EXTERNAL_DB);
        loop {
            if let Err(e) = external_db_worker(&engine, external_db_block).await {
                log::error!("CRITICAL!!! Unexpected error in external db worker: {:?}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                external_db_block = loop {
                    match engine.load_external_db_mc_block_id() {
                        Ok(Some(id)) => break (*id).clone(),
                        _ => {
                            log::error!("CRITICAL!!! Can not load external_db_mc_block_id");
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        }
                    }
                };
            } else {
                break;
            }
        }
        engine.release_stop(Engine::MASK_SERVICE_EXTERNAL_DB);
    });
    Ok(join_handle)
}

pub async fn produce_shard_hashes(
    engine: &Arc<dyn EngineOperations>,
    mc_block: &BlockStuff,
) -> Result<()> {
    let mut shards = mc_block.top_blocks_all()?;
    shards.push(mc_block.id().clone());
    engine.process_shard_hashes_in_ext_db(&shards).await?;
    Ok(())
}

pub async fn external_db_worker(
    engine: &Arc<dyn EngineOperations>,
    external_db_block: BlockIdExt
) -> Result<()> {

    log::info!("start external db worker {}", external_db_block);

    if !external_db_block.shard().is_masterchain() {
        fail!("'external_db_block' must belong master chain");
    }
    let mut handle = engine.load_block_handle(&external_db_block)?.ok_or_else(
        || error!("Cannot load handle for external_db_block {}", external_db_block)
    )?;
    let mut block = if handle.id().seq_no() > 0 {
        Some(engine.load_block(&handle).await?)
    } else {
        None
    };
    'm: loop {
        if engine.check_stop() {
            break 'm;
        }

        if let Some(block) = &block {
            let mc_seq_no = handle.id().seq_no();
            let mut tasks: Vec<tokio::task::JoinHandle<Result<Vec<BlockIdExt>>>> = vec!();
            let mut range = ChainRange {
                master_block: block.id().clone(),
                shard_blocks: Vec::new(),
            };

            // Master block
            let state = engine.load_state(handle.id()).await?;
            let prev_ids = block.construct_prev_id()?;
            let prev_state = engine.load_state(&prev_ids.0).await?;
            tasks.push(tokio::spawn({
                let engine = engine.clone();
                let handle = handle.clone();
                let block = block.clone();
                let prev_state = prev_state.clone();
                async move {
                    engine.process_block_in_ext_db(
                        &handle, &block, None, &state, (&prev_state, None), mc_seq_no).await?;
                    Ok(vec!())
                }
            }));

            // Shard blocks
            let prev_top_blocks = prev_state.top_blocks_all()?;
            for id in block.top_blocks_all()? {
                if id.seq_no() != 0 {
                    let prev_top_block = prev_top_blocks.iter()
                        .find(|p_id| p_id.shard().intersect_with(&id.shard()))
                        .ok_or_else(|| error!("Cannot find prev top block for {}", id))?.clone();

                    let engine = engine.clone();
                    let mut id = id.clone();
                    tasks.push(tokio::spawn(async move {
                        let mut chain = vec!();
                        while id != prev_top_block && id.seq_no() != 0 {
                            let handle = loop {
                                match engine.clone().wait_applied_block(&id, Some(10_000)).await {
                                    Ok(h) => break h,
                                    Err(_) => {
                                        if engine.check_stop() {
                                            return Ok(vec!());
                                        }
                                        log::warn!(
                                            "External DB worker: wait_applied_block({}): timeout", id);
                                    }
                                }
                            };
                            let block = engine.load_block(&handle).await?;
                            let prev_ids = block.construct_prev_id()?;
                            chain.push((handle, block));
                            if prev_ids.1.is_some() {
                                // after merge. "Before marge" block is always commited in MC.
                                break;
                            }
                            id = prev_ids.0;
                            if engine.check_stop() {
                                return Ok(vec!())
                            }
                        }

                        let mut range = vec!();
                        while let Some((handle, block)) = chain.pop() {
                            let state = engine.load_state(handle.id()).await?;
                            let prev_ids = block.construct_prev_id()?;
                            let prev_state = engine.load_state(&prev_ids.0).await?;
                            let prev_state_2 = if let Some(id) = prev_ids.1 {
                                Some(engine.load_state(&id).await?)
                            } else {
                                None
                            };
                            engine.process_block_in_ext_db(
                                &handle, &block, None, &state,
                                (&prev_state, prev_state_2.as_ref()), mc_seq_no
                            ).await?;
                            range.push(block.id().clone());
                            if engine.check_stop() {
                                return Ok(vec!())
                            }
                        }
                        Ok(range)
                    }));
                }
            }

            for r in futures::future::join_all(tasks)
                .await
                .into_iter()
            {
                for id in r?? {
                    range.shard_blocks.push(id)
                }
            }

            if engine.check_stop() {
                return Ok(())
            }

            // Shard hashes must be processed after all shard blocks
            if engine.produce_shard_hashes_enabled() {
                produce_shard_hashes(engine, &block).await?;
            }
            engine.process_chain_range_in_ext_db(&range).await?;
        }
        engine.save_external_db_mc_block_id(handle.id())?;

        // Wait for next master block
        loop {
            match engine.wait_next_applied_mc_block(&handle, Some(15000)).await {
                Ok(r) => {
                    handle = r.0;
                    block = Some(r.1);
                    break;
                }
                Err(_) => {
                    if engine.check_stop() {
                        break 'm;
                    }
                    log::warn!("External DB worker: wait_next_applied_mc_block({}): timeout",
                        handle.id());
                }
            }
        };
    }
    Ok(())

}
