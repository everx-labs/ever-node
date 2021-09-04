use crate::{
    network::full_node_client::FullNodeOverlayClient,
    shard_state::ShardStateStuff
};

use adnl::common::KeyId;
#[cfg(not(feature = "local_test"))]
use std::{sync::{Arc, atomic::{AtomicUsize, Ordering}}};
use ton_block::{BlockIdExt};
#[cfg(not(feature = "local_test"))]
use ton_types::{error, fail};
use ton_types::Result;

#[cfg(feature = "local_test")]
pub async fn download_persistent_state(
    id: &BlockIdExt,
    master_id: &BlockIdExt,
    overlay: &dyn FullNodeOverlayClient
) -> Result<ShardStateStuff> {
    let bytes = overlay.download_persistent_state_part(id, master_id, 0, 0, None).await?;
    ShardStateStuff::deserialize(id.clone(), &bytes)
}

#[cfg(not(feature = "local_test"))]
pub async fn download_persistent_state(
    id: &BlockIdExt,
    master_id: &BlockIdExt,
    overlay: &dyn FullNodeOverlayClient,
    active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>
) -> Result<ShardStateStuff> {
    let mut result = None;
    for _ in 0..10 {
        match download_persistent_state_iter(id, master_id, overlay, active_peers).await {
            Err(e) => {
                log::warn!("download_persistent_state_iter err: {}", e);
                result = Some(Err(e));
                continue;
            },
            Ok(res) => { 
                return Ok(res); 
            }
        }
    }
    result.ok_or_else(|| error!("internal error!"))?
}

#[cfg(not(feature = "local_test"))]
async fn download_persistent_state_iter(
    id: &BlockIdExt,
    master_id: &BlockIdExt,
    overlay: &dyn FullNodeOverlayClient,
    active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>
) -> Result<ShardStateStuff> {

    if id.seq_no == 0 {
        fail!("zerostate is not supported");
    }

    // Check
    let peer = loop {
        match overlay.check_persistent_state(id, master_id, active_peers).await {
            Err(e) => 
                log::trace!("check_persistent_state {}: {}", id.shard(), e),
            Ok(None) => 
                log::trace!("download_persistent_state {}: state not found!", id.shard()),
            Ok(Some(peer)) => 
                break peer,
        }
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

    for _ in 0..threads {
        let parts = parts.clone();
        let total_size = total_size.clone();
        let errors = errors.clone();
        let peer = peer.clone();
        download_futures.push(async move {
            let mut peer_attempt = 0;
            let mut part_attempt = 0;
            loop {

                if offset >= total_size.load(Ordering::Relaxed) {
                    return Ok(());
                }

                match overlay.download_persistent_state_part(
                    id, master_id, offset, max_size, peer.clone(), peer_attempt
                ).await {
                    Ok(next_bytes) => {
                        part_attempt = 0;
                        let len = next_bytes.len();
                        parts.insert(offset, next_bytes);
                        //if (offset / max_size) % 10 == 0 {
                            log::info!("download_persistent_state {}: got part offset: {}", id.shard(), offset);
                        //}
                        if len < max_size {
                            total_size.store(offset + len, Ordering::Relaxed);
                            return Ok(());
                        }
                        offset += max_size * threads;
                    },
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
                        futures_timer::Delay::new(
                            std::time::Duration::from_millis(
                                100_u64
                            )
                        ).await;
                    },
                }

            }
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
    let total_size = total_size.load(Ordering::Relaxed);
    let mut state_bytes = Vec::with_capacity(total_size);
    let mut i = 0;
    while let Some(part) = parts.get(&i) {
        state_bytes.extend_from_slice(&part.1);
        i += max_size;
    }
    assert_eq!(total_size, state_bytes.len());

    ShardStateStuff::deserialize(id.clone(), &state_bytes)
}
