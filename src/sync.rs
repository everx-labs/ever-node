use crate::{
    block::{BlockIdExtExtention, BlockStuff}, block_proof::BlockProofStuff, boot,
    engine_traits::EngineOperations
};
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};
use storage::{
    archives::{
        archive_manager::SLICE_SIZE, package::read_package_from, 
        package_entry_id::PackageEntryId
    },
    types::BlockHandle
};
use tokio::task::JoinHandle;
use ton_block::BlockIdExt;
use ton_types::{error, fail, Result};

type PreDownloadTask = (u32, JoinHandle<Result<Vec<u8>>>);

pub(crate) async fn start_sync(engine: Arc<dyn EngineOperations>) -> Result<()> {
    log::info!(target: "sync", "Started sync");
    let mut predownload_task = None;
    while !engine.check_sync().await? {
        let mc_block_id = Arc::new(engine.load_last_applied_mc_block_id().await?);
        let sc_block_id = Arc::new(engine.load_shards_client_mc_block_id().await?);
        let sync_mc_block_id = if mc_block_id.seq_no() > sc_block_id.seq_no() {
            Arc::clone(&sc_block_id)
        } else {
            Arc::clone(&mc_block_id)
        };

        log::info!(
            target: "sync",
            "Last MC block_id for sync = {} (MC = {}, SC = {})",
            sync_mc_block_id,
            mc_block_id,
            sc_block_id,
        );
        predownload_task = match download_and_import_package(
            &engine,
            &sync_mc_block_id,
            predownload_task
        ).await {
            Ok(predownload_task) => Some(predownload_task),
            Err(err) => {
                log::error!(target: "sync", "Error while downloading and applying package: {}", err);
                None
            }
        };
    }

    // TODO: Uncomment, when will tokio support JoinHandle.abort() (probably, in the next version):
    // if let Some((_seq_no, predownload_task)) = predownload_task {
    //     predownload_task.abort();
    // }

    log::info!(target: "sync", "Sync complete");

    Ok(())
}

async fn download_archive(engine: Arc<dyn EngineOperations>, mc_seq_no: u32) -> Result<Vec<u8>> {
    log::info!(target: "sync", "Requesting archive for MC seq_no = {}", mc_seq_no);
    match engine.download_archive(mc_seq_no).await {
        Ok(Some(data)) => {
            log::info!(
                target: "sync",
                "Downloaded archive for MC seq_no = {}, package size = {} bytes",
                mc_seq_no,
                data.len()
            );
        
            return Ok(data)
        
        },
        Err(e) => fail!("Download archive failed for MC seq_no = {}, err: {}", mc_seq_no, e),
        _ => fail!("Did`t downloaded archive for MC seq_no = {}", mc_seq_no)
    }
}

async fn download_and_import_package(
    engine: &Arc<dyn EngineOperations>,
    last_mc_block_id: &Arc<BlockIdExt>,
    predownload_task: Option<PreDownloadTask>,
) -> Result<PreDownloadTask> {
    let mc_seq_no = last_mc_block_id.seq_no() + 1;

    let download_current_task = if let Some((predownload_seq_no, predownload_task)) = predownload_task {
        if predownload_seq_no <= mc_seq_no && predownload_seq_no + SLICE_SIZE > mc_seq_no {
            Some(predownload_task)
        } else {
            None
        }
    } else {
        None
    }.unwrap_or_else(|| tokio::spawn(download_archive(Arc::clone(engine), mc_seq_no)));

    let data = download_current_task.await??;
    log::info!(target: "sync", "Reading package for mc_seq_no = {}", mc_seq_no);
    let maps = Arc::new(read_package(data).await?);

    log::info!(
        target: "sync",
        "Package contains {} masterchain blocks, {} blocks overall.",
        maps.mc_blocks_ids.len(),
        maps.blocks.len(),
    );

    assert!(maps.mc_blocks_ids.len() <= u32::max_value() as usize);
    let predownload_seq_no = mc_seq_no + maps.mc_blocks_ids.len() as u32;
    let download_next_task = tokio::spawn(download_archive(Arc::clone(engine), predownload_seq_no));

    import_package(maps, engine, last_mc_block_id).await?;

    Ok((predownload_seq_no, download_next_task))
}

async fn import_package(
    maps: Arc<BlockMaps>,
    engine: &Arc<dyn EngineOperations>,
    last_mc_block_id: &Arc<BlockIdExt>,
) -> Result<()> {
    if maps.mc_blocks_ids.keys().next().is_none() {
        fail!("Archive doesn't contain any masterchain blocks!");
    }

    import_mc_blocks(engine, &maps, last_mc_block_id).await?;
    import_shard_blocks(engine, maps).await?;

    Ok(())
}

async fn read_package(data: Vec<u8>) -> Result<BlockMaps> {
    let mut maps = BlockMaps::default();

    let mut reader = read_package_from(&data[..]).await?;
    while let Some(entry) = reader.next().await? {
        log::trace!(
            target: "sync",
            "Processing archive entry: {}, size = {}",
            entry.filename(),
            entry.data().len()
        );
        let entry_id = PackageEntryId::from_filename(entry.filename())?;

        match entry_id {
            PackageEntryId::Block(id) => {
                let id = Arc::new(id);
                maps.blocks.entry(Arc::clone(&id))
                    .or_insert_with(|| BlocksEntry::default())
                    .block = Some(Arc::new(
                    BlockStuff::deserialize_checked((*id).clone(), entry.take_data())?
                ));
                if id.is_masterchain() {
                    maps.mc_blocks_ids.insert(id.seq_no(), id);
                }
            },

            PackageEntryId::Proof(id) => {
                if !id.is_masterchain() {
                    log::warn!(
                        target: "sync",
                        "Proof for shard chain must be skipped: {}, entry filename: {}",
                        id,
                        entry.filename()
                    );
                    continue;
                }
                let id = Arc::new(id);
                maps.blocks.entry(Arc::clone(&id))
                    .or_insert_with(|| BlocksEntry::default())
                    .proof = Some(Arc::new(
                    BlockProofStuff::deserialize(&id, entry.take_data(), false)?
                ));
                maps.mc_blocks_ids.insert(id.seq_no(), id);
            },

            PackageEntryId::ProofLink(id) => {
                if id.is_masterchain() {
                    log::warn!(
                        target: "sync",
                        "Proof-link for masterchain must be skipped: {}, entry filename: {}",
                        id,
                        entry.filename()
                    );
                    continue;
                }
                maps.blocks.entry(Arc::new(id))
                    .or_insert_with(|| BlocksEntry::default())
                    .proof = Some(Arc::new(
                    BlockProofStuff::deserialize(&id, entry.take_data(), true)?
                ));
            },

            _ => fail!("Unsupported entry: {:?}", entry_id),
        }
    }

    Ok(maps)
}

async fn save_block(
    engine: &Arc<dyn EngineOperations>,
    block_id: &BlockIdExt,
    entry: &BlocksEntry
) -> Result<(Arc<BlockHandle>, Arc<BlockStuff>, Arc<BlockProofStuff>)> {
    log::trace!(target: "sync", "save_block: id = {}", block_id);
    let block = if let Some(ref block) = entry.block {
        Arc::clone(block)
    } else {
        fail!("Block not found in archive: {}", block_id);
    };
    let proof = if let Some(ref proof) = entry.proof {
        Arc::clone(proof)
    } else {
        let link_str = if block_id.shard().is_masterchain() {
            ""
        } else {
            "link"
        };
        fail!("Proof{} not found in archive: {}", link_str, block_id);
    };
    proof.check_proof(engine.as_ref()).await?;
    let handle = engine.store_block(&block).await?;
    let handle = engine.store_block_proof(block_id, Some(handle), &proof).await?;
    Ok((handle, block, proof))
}

#[derive(Default, Debug)]
struct BlocksEntry {
    block: Option<Arc<BlockStuff>>,
    proof: Option<Arc<BlockProofStuff>>,
}

#[derive(Default, Debug)]
struct BlockMaps {
    mc_blocks_ids: BTreeMap<u32, Arc<BlockIdExt>>,
    blocks: BTreeMap<Arc<BlockIdExt>, BlocksEntry>,
}

async fn wait_for(tasks: Vec<JoinHandle<Result<()>>>) -> Result<()> {
    futures::future::try_join_all(tasks).await?
        .into_iter()
        .find(|arch_result| arch_result.is_err())
        .unwrap_or(Ok(()))
}

async fn import_mc_blocks(
    engine: &Arc<dyn EngineOperations>,
    maps: &BlockMaps,
    mut last_mc_block_id: &Arc<BlockIdExt>,
) -> Result<()> {

    for id in maps.mc_blocks_ids.values() {

        if id.seq_no() <= last_mc_block_id.seq_no() {
            if id.seq_no() == last_mc_block_id.seq_no() {
                if **last_mc_block_id != **id {
                    fail!("Bad old masterchain block ID");
                }
            }
            log::debug!(target: "sync", "Skipped already applied MC block: {}", id);
            continue;
        }
        if id.seq_no() != last_mc_block_id.seq_no() + 1 {
            fail!(
                "There is a hole in the masterchain seq_no! Last applied seq_no = {}, current seq_no = {}",
                last_mc_block_id.seq_no(),
                id.seq_no()
            );
        }

        log::debug!(target: "sync", "Importing MC block: {}", id);
        last_mc_block_id = id;
        if let Some(handle) = engine.load_block_handle(&last_mc_block_id)? {
            if handle.is_applied() {
                log::debug!(
                    target: "sync", 
                    "Skipped already applied MC block: {}", 
                    last_mc_block_id
                );
                continue
            }
        } 

        let entry = maps.blocks.get(last_mc_block_id).expect("Inconsistent BlocksMap");
        let (handle, block, _proof) = save_block(engine, &last_mc_block_id, entry).await?;
        log::debug!(target: "sync", "Applying masterchain block: {}...", last_mc_block_id);
        Arc::clone(engine).apply_block(
            &handle, &block, last_mc_block_id.seq_no(), false
        ).await?;

    }
 
    Ok(())

}

async fn import_shard_blocks(
    engine: &Arc<dyn EngineOperations>,
    maps: Arc<BlockMaps>,
) -> Result<()> {

    for (id, entry) in maps.blocks.iter() {
        if !id.is_masterchain() {
            save_block(engine, id, entry).await?;
        }
    }

    let mut last_applied_mc_block_id =
        Arc::new(engine.load_shards_client_mc_block_id().await?);
    for mc_block_id in maps.mc_blocks_ids.values() {
        let mc_seq_no = mc_block_id.seq_no();
        if mc_seq_no <= last_applied_mc_block_id.seq_no() {
            log::debug!(
                target: "sync",
                "Skipped shardchain blocks for already appplied MC block: {}",
                mc_block_id
            );
            continue;
        }

        log::debug!(target: "sync", "Importing shardchain blocks for MC block: {}...", mc_block_id);

        let mc_handle = engine.load_block_handle(&mc_block_id)?.ok_or_else(
            || error!("Cannot load handle for master block {}", mc_block_id)
        )?;
        let mc_block = engine.load_block(&mc_handle).await?;

        let shard_blocks = mc_block.shards_blocks()?;
        let mut tasks = Vec::with_capacity(shard_blocks.len());
        for (_shard, id) in shard_blocks {
            let engine = Arc::clone(engine);
            let mc_handle = Arc::clone(&mc_handle);
            let maps = Arc::clone(&maps);
            tasks.push(tokio::spawn(async move {
                log::debug!(
                    target: "sync",
                    "Importing shardchain block: {} for MC block: {}...",
                    id,
                    mc_handle.id()
                );

                let handle = engine.load_block_handle(&id)?.ok_or_else(
                    || error!("Cannot load handle for shard block {}", id)
                )?;
                if handle.is_applied() {
                    log::debug!(target: "sync", "Skipped already applied block: {}", id);
                    return Ok(());
                }

                if id.seq_no() == 0 {
                    log::info!(target: "sync", "Downloading zerostate: {}...", id);
                    boot::download_zero_state(engine.as_ref(), &id).await?;
                    return Ok(());
                }

                log::debug!(target: "sync", "Applying shardchain block: {}...", id);
                let block = match maps.blocks.get(&id) {
                    Some(entry) => {
                        match entry.block {
                            Some(ref block) => Some(block.as_ref().clone()),
                            None => engine.load_block(&handle).await.ok(),
                        }
                    },
                    None => {
                        log::warn!(target: "sync", "Shard block is not found in the package: {}", id);
                        engine.load_block(&handle).await.ok()
                    },
                };
                if let Some(block) = block {
                    Arc::clone(&engine).apply_block(&handle, &block, mc_seq_no, false).await
                } else {
                    log::warn!(
                        target: "sync",
                        "Shard block is not found either in the package or in the un-applied blocks. \
                        We will try to download it individually: {}",
                        id
                    );
                    Arc::clone(&engine).download_and_apply_block(
                        handle.id(), mc_seq_no, false
                    ).await
                } 
            }));
        }

        wait_for(tasks).await?;

        last_applied_mc_block_id = Arc::clone(mc_block_id);
        engine.store_shards_client_mc_block_id(&mc_block_id).await?;
    }

    Ok(())
}

