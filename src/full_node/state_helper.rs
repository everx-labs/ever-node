use crate::{
    network::node_network::FullNodeOverlayClient,
    shard_state::ShardStateStuff
};

use std::{ sync::{Arc, atomic::{AtomicUsize, Ordering}} };
use ton_block::{BlockIdExt};
use ton_types::{Result};
use ton_types::fail;

pub async fn download_persistent_state(
    id: &BlockIdExt,
    master_id: &BlockIdExt,
    overlay: &dyn FullNodeOverlayClient
) -> Result<ShardStateStuff> {

    if id.seq_no == 0 {
        loop {
            match overlay.download_zero_state(id, 3).await {
                Err(e) => log::warn!("Can't load zerostate {}: {:?}", id, e),
                Ok(None) => log::warn!("Can't load zerostate {}: none returned", id),
                Ok(Some(zs)) => return Ok(zs)
            }
        }
    }

    // Check
    if !overlay.check_persistent_state(id, master_id, 100).await? { // TODO master block id???
        fail!("download_persistent_state: state not found!")
    }

    // Download
    log::trace!("download_persistent_state: start: id: {}, master_id: {}", id, master_id);
    let now = std::time::Instant::now();

    let mut offset = 0;
    let parts = Arc::new(lockfree::map::Map::new());
    let max_size = 65536; // part max size
    let mut download_futures = vec!();
    let total_size = Arc::new(AtomicUsize::new(usize::max_value()));
    let errors = Arc::new(AtomicUsize::new(0));
    let threads = 40;

    for _ in 0..threads {
        let parts = parts.clone();
        let total_size = total_size.clone();
        let errors = errors.clone();
        download_futures.push(async move {
            let mut attempts = 0_u32;
            loop {

                if offset >= total_size.load(Ordering::Relaxed) {
                    return Ok(());
                }

                match overlay.download_persistent_state_part(id, master_id, offset, max_size, 1).await {
                    Ok(next_bytes) => {
                        attempts = 0;
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
                        attempts += 1;
                        log::error!("download_persistent_state_part {}: {}, attempt: {}, total errors: {}",
                            id.shard(), e, attempts, errors.load(Ordering::Relaxed));
                        if attempts > 65536 {
                            failure::bail!("Error download_persistent_state_part after {} attempts : {}", attempts, e);
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