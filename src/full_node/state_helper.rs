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
    engine_traits::EngineOperations, network::full_node_client::FullNodeOverlayClient,
    shard_state::ShardStateStuff
};

use ever_crypto::KeyId;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use ton_block::BlockIdExt;
use ton_types::{error, fail, Result};

pub async fn download_persistent_state(
    id: &BlockIdExt,
    master_id: &BlockIdExt,
    overlay: &dyn FullNodeOverlayClient,
    engine: &dyn EngineOperations, 
    active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>,
    attempts: Option<usize>,
    check_stop: &(dyn Fn() -> Result<()> + Sync + Send),
) -> Result<(Arc<ShardStateStuff>, Arc<Vec<u8>>)> {
    let mut result = None;
    for _ in 0..10 {
        match download_persistent_state_iter(
            id, master_id, overlay, engine, active_peers, attempts.clone(), check_stop,
        ).await {
            Err(e) => {
                log::warn!("download_persistent_state_iter err: {}", e);
                result = Some(Err(e));
                futures_timer::Delay::new(std::time::Duration::from_millis(1000)).await;
                continue;
            },
            Ok(res) => { 
                return Ok(res); 
            }
        }
    }
    result.ok_or_else(|| error!("internal error!"))?
}

async fn download_persistent_state_iter(
    id: &BlockIdExt,
    master_id: &BlockIdExt,
    overlay: &dyn FullNodeOverlayClient,
    engine: &dyn EngineOperations,
    active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>,
    mut attempts: Option<usize>,
    check_stop: &(dyn Fn() -> Result<()> + Sync + Send),
) -> Result<(Arc<ShardStateStuff>, Arc<Vec<u8>>)> {

    if id.seq_no == 0 {
        fail!("zerostate is not supported");
    }

    // Check
    let peer = loop {
        check_stop()?;
        if let Some(remained) = attempts.as_mut() {
            if *remained == 0 {
                fail!("Can't find peer to load persistent state")
            }
            *remained -= 1;
        }
        match overlay.check_persistent_state(id, master_id, active_peers).await {
            Err(e) => 
                log::trace!("check_persistent_state {}: {}", id.shard(), e),
            Ok(None) => 
                log::trace!("download_persistent_state {}: state not found!", id.shard()),
            Ok(Some(p)) => 
                break p
        }
        futures_timer::Delay::new(std::time::Duration::from_millis(100)).await;
    };

    // Download
    log::info!("download_persistent_state: start: id: {}, master_id: {}", id, master_id);
    let now = std::time::Instant::now();

    let mut offset = 0;
    let parts = Arc::new(lockfree::map::Map::new());
    let max_size = 1 << 20; // part max size
    let mut download_futures = vec!();
    let total_size = Arc::new(AtomicUsize::new(usize::max_value()));
    let errors = Arc::new(AtomicUsize::new(0));
    let threads = 3;
    let peer_drop = peer.clone();

    for thread in 0..threads {
        let parts = parts.clone();
        let total_size = total_size.clone();
        let errors = errors.clone();
        let peer = peer.clone();
        download_futures.push(async move {
            let mut peer_attempt = 0;
            let mut part_attempt = 0;
            while offset < total_size.load(Ordering::Relaxed) {
                check_stop()?;
                let result = overlay.download_persistent_state_part(
                    id, master_id, offset, max_size, peer.clone(), peer_attempt
                ).await;
                match result {
                    Ok(next_bytes) => {
                        part_attempt = 0;
                        let len = next_bytes.len();
                        parts.insert(offset, next_bytes);
                        //if (offset / max_size) % 10 == 0 {
                            log::info!("download_persistent_state {}: got part offset: {}", id.shard(), offset);
                        //}
                        if len < max_size {
                            log::info!("the total length of persistent state {} might be {}", id.shard(), offset + len);
                            total_size.fetch_min(offset + len, Ordering::Relaxed);
                            break
                        }
                        offset += max_size * threads;
                    }
                    Err(e) => {
                        errors.fetch_add(1, Ordering::SeqCst);
                        part_attempt += 1;
                        peer_attempt += 1;
                        log::error!(
                            "download_persistent_state_part {}: {}, attempt: {}, total errors: {}",
                            id.shard(), e, part_attempt, errors.load(Ordering::Relaxed)
                        );
                        if part_attempt > 10 {
                            fail!(
                                "Error download_persistent_state_part after {} attempts: {}", 
                                part_attempt, e
                            )
                        }
                        futures_timer::Delay::new(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
            log::trace!("download_persistent_state {} thread {} finished", id.shard(), thread);
            Ok(())
        });
        offset += max_size;
    }

    let res = futures::future::join_all(download_futures)
        .await
        .into_iter()
        .find(|r| r.is_err());
    active_peers.remove(peer_drop.id());
    res.unwrap_or(Ok(()))?;

    log::info!(
        "download_persistent_state: DOWNLOADED {}ms, id: {}, master_id: {} ", 
        now.elapsed().as_millis(), id, master_id
    );

    // TODO try to walk map by iterator and fill preallocated vector to speed up this process
    let now = std::time::Instant::now();
    let total_size = total_size.load(Ordering::Relaxed);
    let mut state_bytes = Vec::with_capacity(total_size);
    let mut i = 0;
    while let Some(part) = parts.get(&i) {
        state_bytes.extend_from_slice(&part.1);
        i += max_size;
    }
    log::trace!(
        "download_persistent_state: CONCAT {}ms, id: {}, master_id: {} ", 
        now.elapsed().as_millis(), id, master_id
    );
    assert_eq!(total_size, state_bytes.len());

    let state_bytes = Arc::new(state_bytes);
    Ok((ShardStateStuff::deserialize_inmem(
        id.clone(), 
        state_bytes.clone(),
        #[cfg(feature = "telemetry")]
        engine.engine_telemetry(),
        engine.engine_allocated()
    )?,
    state_bytes))
}
