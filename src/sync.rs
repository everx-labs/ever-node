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
    block::{BlockIdExtExtention, BlockStuff}, block_proof::BlockProofStuff, boot,
    engine_traits::EngineOperations
};
use adnl::common::Wait;
use ever_crypto::KeyId;
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};
use storage::{
    archives::{
        ARCHIVE_PACKAGE_SIZE, package::read_package_from, 
        package_entry_id::PackageEntryId
    },
    block_handle_db::BlockHandle
};
use tokio::task::JoinHandle;
use ton_block::BlockIdExt;
use ton_types::{error, fail, Result};

//type PreDownloadTask = (u32, JoinHandle<Result<Vec<u8>>>);

const TARGET: &str = "sync";

#[async_trait::async_trait]
pub trait StopSyncChecker {
    async fn check(&self, engine: &Arc<dyn EngineOperations>) -> bool;
}

pub(crate) async fn start_sync(
    engine: Arc<dyn EngineOperations>,
    check_stop_sync: Option<&dyn StopSyncChecker>,
) -> Result<()> {

    async fn apply(
        engine: &Arc<dyn EngineOperations>, 
        seq_no: u32, 
        last_mc_block_id: &Arc<BlockIdExt>, 
        data: &Vec<u8>
    ) -> Result<bool> {
        log::info!(target: TARGET, "Reading package for MC seq_no = {}", seq_no);
        let maps = Arc::new(read_package(data).await?);
        log::info!(
            target: TARGET,
            "Package contains {} masterchain blocks, {} blocks overall.",
            maps.mc_blocks_ids.len(),
            maps.blocks.len(),
        );
        import_package(maps, engine, last_mc_block_id).await?;
        log::info!(target: TARGET, "Package imported for MC seq_no = {}", seq_no);
        Ok(true)
    }

    fn download(
        engine: &Arc<dyn EngineOperations>, 
        wait: &Arc<Wait<(u32, Result<Option<Vec<u8>>>)>>, 
        seq_no: u32,
        active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>
    ) {
        wait.request();
        let engine = engine.clone();
        let wait = wait.clone();
        let active_peers = active_peers.clone();
        tokio::spawn(
            async move {
                let res = download_archive(engine, seq_no, &active_peers).await;
                wait.respond(Some((seq_no, res)));
            }
        );
        log::info!(target: TARGET, "Download scheduled for MC seq_no = {}", seq_no);
    }

    async fn force_redownload(
        engine: &Arc<dyn EngineOperations>, 
        wait: &Arc<Wait<(u32, Result<Option<Vec<u8>>>)>>, 
        queue: &mut Vec<(u32, ArchiveStatus)>,
        active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>
    ) -> Result<()> {
        // Find latest downloaded archive
        let mut latest = None;
        for (seq_no, status) in queue.iter_mut() {
            if let ArchiveStatus::Downloaded(_) = status {
                if let Some(latest) = latest {
                    if latest >= *seq_no {
                        continue
                    }
                }
                latest = Some(*seq_no)
            }
        }
        // Redownload previous not found ones 
        if let Some(latest) = latest {
            for (seq_no, status) in queue.iter_mut() {
                if let ArchiveStatus::NotFound = status {
                    if latest <= *seq_no {
                        continue
                    }
                    *status = ArchiveStatus::Downloading;
                    download(engine, wait, *seq_no, active_peers)
                }
            }
        }
        // Redownload earliest not found if only not found remained
        if !engine.check_sync().await? {
            let mut earliest = None;
            for (seq_no, status) in queue.iter_mut() {
                if let ArchiveStatus::NotFound = status {
                    if let Some(earliest) = earliest {
                        if earliest <= *seq_no {
                            continue
                        }
                    }
                    earliest = Some(*seq_no)
                } else {
                    // There is another status
                    return Ok(())
                }
            }
            if let Some(earliest) = earliest {
                for (seq_no, status) in queue.iter_mut() {
                    if earliest == *seq_no {
                        *status = ArchiveStatus::Downloading;
                        download(engine, wait, *seq_no, active_peers);
                        break
                    }
                }
            }
        }
        Ok(())
    }

    async fn new_downloads(
        engine: &Arc<dyn EngineOperations>, 
        wait: &Arc<Wait<(u32, Result<Option<Vec<u8>>>)>>, 
        queue: &mut Vec<(u32, ArchiveStatus)>,
        active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>,
        mut sync_mc_seq_no: u32,
        concurrency: usize
    ) -> Result<()> {
        force_redownload(engine, wait, queue, active_peers).await?;
        while wait.count() < concurrency {
            if queue.iter().count() > concurrency {
                // Do not download too much in advance due to possible OOM
                break
            } 
            if queue.iter().position(|(seq_no, _)| seq_no == &sync_mc_seq_no).is_none() {
                queue.push((sync_mc_seq_no, ArchiveStatus::Downloading));
                download(engine, wait, sync_mc_seq_no, active_peers);
            }
            sync_mc_seq_no += ARCHIVE_PACKAGE_SIZE;
        }
        Ok(())
    }

    enum ArchiveStatus {
        Downloading,
        NotFound,
        Downloaded(Vec<u8>)
    }

    const MAX_CONCURRENCY: usize = 8;

    log::info!(target: TARGET, "Started sync");
    let active_peers = Arc::new(lockfree::set::Set::new());
    let mut queue: Vec<(u32, ArchiveStatus)> = Vec::new();
    let (wait, mut reader) = Wait::new();
    let mut concurrency = 1;

    'check: while !engine.check_sync().await? {

        if let Some(check_stop_sync) = check_stop_sync.as_ref() {
            if check_stop_sync.check(&engine).await {
                log::info!(target: TARGET, "Sync is managed to stop");
                return Ok(())
            }
        }

        // Select sync block ID
        let mc_block_id = if let Some(id) = engine.load_last_applied_mc_block_id()? {
            id
        } else {
            fail!("INTERNAL ERROR: No last applied MC block in sync")
        };
        let sc_block_id = if let Some(id) = engine.load_shard_client_mc_block_id()? {
            id
        } else {
            fail!("INTERNAL ERROR: No shard client MC block in sync")
        };
        let last_mc_block_id = if mc_block_id.seq_no() > sc_block_id.seq_no() {
            Arc::clone(&sc_block_id)
        } else {
            Arc::clone(&mc_block_id)
        };

        log::info!(
            target: TARGET,
            "Last MC seq_no for sync = {} (MC = {}, SC = {})",
            last_mc_block_id.seq_no(), mc_block_id, sc_block_id,
        );

        // Try to find proper # in queue
        let sync_mc_seq_no = last_mc_block_id.seq_no() + 1;
        loop {
            new_downloads(
                &engine, &wait, &mut queue, &active_peers, sync_mc_seq_no, concurrency
            ).await?;
            if let Some(index) = queue.iter().position(
                |(seq_no, status)| match status {
                    ArchiveStatus::Downloaded(_) => seq_no <= &sync_mc_seq_no,
                    _ => false
                }
            ) {
                if let (seq_no, ArchiveStatus::Downloaded(data)) = queue.remove(index) {
                    match apply(&engine, seq_no, &last_mc_block_id, &data).await {
                        Ok(true) => continue 'check,
                        Ok(false) => (),
                        Err(e) => log::error!(
                            target: TARGET,
                            "Cannot apply queued package for MC seq_no = {}: {}",
                            seq_no, e
                        )
                    }
                } else {
                    fail!("INTERNAL ERROR: sync queue broken")
                }
            } else {
                break
            }
        }

        log::info!(target: TARGET, "Continue sync with MC seq_no {}", sync_mc_seq_no);

        // Otherwise download
        while !engine.check_sync().await? {
            new_downloads(
                &engine, &wait, &mut queue, &active_peers, sync_mc_seq_no, concurrency
            ).await?;
/*
            while wait.count() < concurrency {
                if queue.iter().count() > concurrency {
                    // Do not download too much in advance due to possible OOM
                    break
                } 
                if queue.iter().position(|(seq_no, _)| seq_no == &sync_mc_seq_no).is_none() {
                    queue.push((sync_mc_seq_no, ArchiveStatus::Downloading));
                    download(&engine, &wait, sync_mc_seq_no, &active_peers);
                }
                sync_mc_seq_no += ARCHIVE_PACKAGE_SIZE;
            }
*/
            match wait.wait(&mut reader, false).await {
                Some(Some((seq_no, Err(e)))) => {
                    log::error!(
                        target: TARGET,
                        "Error while downloading package seq_no {}: {}",
                        seq_no, e
                    );
                    download(&engine, &wait, seq_no, &active_peers)
                },
                Some(Some((seq_no_recv, Ok(data)))) => {
                    if let Some(index) = queue.iter().position(
                        |(seq_no_send, status)| match status {
                            ArchiveStatus::Downloading => seq_no_send == &seq_no_recv,
                            _ => false
                        }
                    ) {
                        if let Some(data) = data {
/*
                            // Redownload all previously not found archives
                            for (seq_no, status) in queue.iter_mut() {
                                if let ArchiveStatus::NotFound = status {
                                    if *seq_no < seq_no_recv {
                                        *status = ArchiveStatus::Downloading;
                                        download(&engine, &wait, *seq_no, &active_peers)
                                    }
                                }
                            }
*/
                            if seq_no_recv <= last_mc_block_id.seq_no() + 1 {
                                match apply(&engine, seq_no_recv, &last_mc_block_id, &data).await {
                                    Ok(ok) => {
                                        queue.remove(index);
                                        if ok {
                                            concurrency = MAX_CONCURRENCY;
                                            break
                                        }
                                    },
                                    Err(e) => {
                                        log::error!(
                                            target: TARGET,
                                            "Cannot apply downloaded package for MC seq_no = {}: {}",
                                            seq_no_recv, e
                                        );
                                        download(&engine, &wait, seq_no_recv, &active_peers)
                                    }
                                }
                            } else {
                                let (_, status) = &mut queue[index];
                                *status = ArchiveStatus::Downloaded(data);
                                force_redownload(&engine, &wait, &mut queue, &active_peers).await?;
                            }
                        } else {
                            let (_, status) = &mut queue[index];
                            *status = ArchiveStatus::NotFound;
                            force_redownload(&engine, &wait, &mut queue, &active_peers).await?;
/*                            if queue.iter().position(
                                |(seq_no, status)| match status {
                                    ArchiveStatus::Downloaded(_) => seq_no > &seq_no_recv,
                                    _ => false
                                }
                            ).is_some() {
                                // Redownload because there is a later archive
                                download(&engine, &wait, seq_no_recv, &active_peers);
                            } else {
                                // Mark not found
                                let (_, status) = &mut queue[index];
                                *status = ArchiveStatus::NotFound;
                            }
*/
                        }
                    } else {
                        fail!("INTERNAL ERROR: sync queue broken")
                    }
                },
                _ => fail!("INTERNAL ERROR: sync broken")
            }
        }

    }

    log::info!(target: TARGET, "Sync complete");
    Ok(())

}

async fn download_archive(
    engine: Arc<dyn EngineOperations>, 
    mc_seq_no: u32,
    active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>
) -> Result<Option<Vec<u8>>> {
    log::info!(target: "sync", "Requesting archive for MC seq_no = {}", mc_seq_no);
    match engine.download_archive(mc_seq_no, active_peers).await {
        Ok(Some(data)) => {
            log::info!(
                target: "sync",
                "Downloaded archive for MC seq_no = {}, package size = {} bytes",
                mc_seq_no,
                data.len()
            );
            Ok(Some(data))
        },
        Ok(None) => {
            log::info!(target: "sync", "No archive found for MC seq_no = {}", mc_seq_no);
            Ok(None)
        },
        Err(e) => fail!(
            "Download archive failed for MC seq_no = {}, err: {}, active peers {}", 
            mc_seq_no, e, active_peers.iter().count()
        )
    }
}

/*
async fn download_and_import_package(
    engine: &Arc<dyn EngineOperations>,
    last_mc_block_id: &Arc<BlockIdExt>
) -> Result<()> {
    let mc_seq_no = last_mc_block_id.seq_no() + 1;

    let download_current_task = if let Some((predownload_seq_no, predownload_task)) = predownload_task {
        if predownload_seq_no <= mc_seq_no && predownload_seq_no + ARCHIVE_PACKAGE_SIZE > mc_seq_no {
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
*/

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

async fn read_package(data: &Vec<u8>) -> Result<BlockMaps> {
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
    let handle = engine.store_block(&block).await?.as_non_created().ok_or_else(
        || error!("INTERNAL ERROR: mismatch in block {} store result during sync", block_id)
    )?;
    let handle = engine.store_block_proof(block_id, Some(handle), &proof).await?
        .as_non_created()
        .ok_or_else(
            || error!(
                "INTERNAL ERROR: mismatch in block {} proof store result during sync", 
                block_id
            )
        )?;
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
    futures::future::join_all(tasks).await
        .into_iter().find(|r| match r {
            Err(_) => true,
            Ok(Err(_)) => true,
            Ok(Ok(_)) => false,
        })
        .unwrap_or(Ok(Ok(())))??;
    Ok(())
}

async fn import_mc_blocks(
    engine: &Arc<dyn EngineOperations>,
    maps: &BlockMaps,
    mut last_mc_block_id: &Arc<BlockIdExt>
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
 
    log::debug!(target: TARGET, "Last applied MC seq_no = {}", last_mc_block_id.seq_no());
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

    let mut shard_client_mc_block_id = match engine.load_shard_client_mc_block_id()? {
        Some(id) => id,
        None => fail!("INTERNAL ERROR: No shard client MC block set in sync")
    };
    let (_master, workchain_id) = engine.processed_workchain().await?;
    for mc_block_id in maps.mc_blocks_ids.values() {
        let mc_seq_no = mc_block_id.seq_no();
        if mc_seq_no <= shard_client_mc_block_id.seq_no() {
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

        let shard_blocks = mc_block.shards_blocks(workchain_id)?;
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
                    boot::download_zerostate(engine.as_ref(), &id).await?;
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

        shard_client_mc_block_id = Arc::clone(mc_block_id);
        engine.save_shard_client_mc_block_id(&mc_block_id)?;
    }

    Ok(())
}

