use crate::{
    network::full_node_client::{Attempts, FullNodeOverlayClient},
    shard_state::ShardStateStuff
};

#[cfg(not(feature = "local_test"))]
use std::{ sync::{Arc, atomic::{AtomicUsize, Ordering}} };

use ton_block::{BlockIdExt};
use ton_types::{fail, Result};

#[cfg(feature = "local_test")]
pub async fn download_persistent_state(
    id: &BlockIdExt,
    master_id: &BlockIdExt,
    overlay: &dyn FullNodeOverlayClient
) -> Result<ShardStateStuff> {
    let bytes = overlay.download_persistent_state_part(id, master_id, 0, 0, 0, None).await?.0;
    ShardStateStuff::deserialize(id.clone(), &bytes)
}

#[cfg(not(feature = "local_test"))]
pub async fn download_persistent_state(
    id: &BlockIdExt,
    master_id: &BlockIdExt,
    overlay: &dyn FullNodeOverlayClient
) -> Result<ShardStateStuff> {

    if id.seq_no == 0 {
        loop {
            let attempts = Attempts {
                limit: 3,
                count: 0
            };
            match overlay.download_zero_state(id, &attempts).await {
                Err(e) => log::warn!("Can't load zerostate {}: {:?}", id, e),
                Ok(None) => log::warn!("Can't load zerostate {}: none returned", id),
                Ok(Some(zs)) => return Ok(zs)
            }
        }
    }

    // Check
    let peer = loop {
        let attempts = Attempts {
            limit: 100,
            count: 0
        };
        match overlay.check_persistent_state(id, master_id, &attempts).await {
            Err(e) => log::trace!("check_persistent_state {}: {}", id.shard(), e),
            Ok((false, _)) => log::trace!("download_persistent_state {}: state not found!", id.shard()),
            Ok((true, peer)) => break peer,
        }
    };

    // Download
    log::trace!("download_persistent_state: start: id: {}, master_id: {}", id, master_id);
    let now = std::time::Instant::now();

    let mut offset = 0;
    let parts = Arc::new(lockfree::map::Map::new());
    let max_size = 65536; // part max size
    let mut download_futures = vec!();
    let total_size = Arc::new(AtomicUsize::new(usize::max_value()));
    let errors = Arc::new(AtomicUsize::new(0));
    let threads = 3;

    for _ in 0..threads {
        let parts = parts.clone();
        let total_size = total_size.clone();
        let errors = errors.clone();
        let peer = peer.clone();
        download_futures.push(async move {
            let mut attempts = Attempts {
                limit: 0,
                count: 0
            };
            loop {

                if offset >= total_size.load(Ordering::Relaxed) {
                    return Ok(());
                }

                match overlay.download_persistent_state_part(
                    id, master_id, offset, max_size, Some(peer.clone()), &attempts
                ).await {
                    Ok(next_bytes) => {
                        attempts.count = 0;
                        let len = next_bytes.len();
                        parts.insert(offset, next_bytes);
                        //if (offset / max_size) % 10 == 0 {
                            log::trace!("download_persistent_state {}: got part offset: {}", id.shard(), offset);
                        //}
                        if len < max_size {
                            total_size.store(offset + len, Ordering::Relaxed);
                            return Ok(());
                        }
                        offset += max_size * threads;
                    },
                    Err(e) => {
                        errors.fetch_add(1, Ordering::SeqCst);
                        attempts.count += 1;
                        log::error!("download_persistent_state_part {}: {}, attempt: {}, total errors: {}",
                            id.shard(), e, attempts.count, errors.load(Ordering::Relaxed));
                        if attempts.count > 100 {
                            fail!(
                                "Error download_persistent_state_part after {} attempts : {}", 
                                attempts.count, e
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

    futures::future::join_all(download_futures)
        .await
        .into_iter()
        .find(|r| r.is_err())
        .unwrap_or(Ok(()))?;

    log::trace!("download_persistent_state: DOWNLOADED {}ms, id: {}, master_id: {} ", now.elapsed().as_millis(), id, master_id);

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