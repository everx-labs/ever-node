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

use crate::network::full_node_client::FullNodeOverlayClient;

use std::sync::Arc;
use ton_block::BlockIdExt;
use ton_types::{error, fail, KeyId, Result};

pub async fn download_persistent_state(
    id: &BlockIdExt,
    msg_queue_for: Option<i32>,
    master_id: &BlockIdExt,
    overlay: &dyn FullNodeOverlayClient,
    active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>,
    attempts: Option<usize>,
    check_stop: &(dyn Fn() -> Result<()> + Sync + Send),
) -> Result<Arc<Vec<u8>>> {
    let mut result = None;
    for _ in 0..10 {
        match download_persistent_state_iter(
            id, msg_queue_for, master_id, overlay, active_peers, attempts.clone(), check_stop,
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
    msg_queue_for: Option<i32>,
    master_id: &BlockIdExt,
    overlay: &dyn FullNodeOverlayClient,
    active_peers: &Arc<lockfree::set::Set<Arc<KeyId>>>,
    mut attempts: Option<usize>,
    check_stop: &(dyn Fn() -> Result<()> + Sync + Send),
) -> Result<Arc<Vec<u8>>> {

    if id.seq_no == 0 {
        fail!("zerostate is not supported");
    }

    let descr = if let Some(wc) = msg_queue_for {
        format!("out messages queue for wc {}", wc)
    } else {
        "persistent state".to_owned()
    };

    // Check
    let peer = loop {
        check_stop()?;
        if let Some(remained) = attempts.as_mut() {
            if *remained == 0 {
                fail!("Can't find peer to load {} {}", descr, id.shard())
            }
            *remained -= 1;
        }
        match overlay.check_persistent_state(id, msg_queue_for, master_id, active_peers).await {
            Err(e) => 
                log::trace!("check_persistent_state descr {} {}: {}", descr, id.shard(), e),
            Ok(None) => 
                log::trace!("download_persistent_state {}: {} not found!", descr, id.shard()),
            Ok(Some(p)) => 
                break p
        }
        futures_timer::Delay::new(std::time::Duration::from_millis(100)).await;
    };

    // Download
    log::info!("download_persistent_state: start: id: {}, master_id: {}", id, master_id);
    let now = std::time::Instant::now();

    let max_size = 1 << 20; // part max size
    let mut offset = 0;
    let mut peer_attempt = 0;
    let mut part_attempt = 0;
    let mut errors = 0;
    let mut state_bytes = vec!();
    loop {
        check_stop()?;
        let result = overlay.download_persistent_state_part(
            id, msg_queue_for, master_id, offset, max_size, peer.clone(), peer_attempt
        ).await;
        match result {
            Ok(next_bytes) => {
                part_attempt = 0;
                let len = next_bytes.len();
                state_bytes.extend_from_slice(&next_bytes);
                //if (offset / max_size) % 10 == 0 {
                    log::info!("download_persistent_state {}: got part offset: {}", id.shard(), offset);
                //}
                if len < max_size {
                    log::info!("the total length of {} {} might be {}", descr, id.shard(), offset + len);
                    break
                }
                offset += max_size;
            }
            Err(e) => {
                errors += 1;
                part_attempt += 1;
                peer_attempt += 1;
                log::error!(
                    "download_persistent_state_part {}: {}, attempt: {}, total errors: {}",
                    id.shard(), e, part_attempt, errors
                );
                if part_attempt > 10 {
                    fail!("Error download_persistent_state_part after {} attempts: {}", 
                        part_attempt, e)
                }
                futures_timer::Delay::new(std::time::Duration::from_millis(100)).await;
            }
        }
    }

    log::info!("download_persistent_state: DOWNLOADED {} {}sec, id: {}, master_id: {} ", 
        descr, now.elapsed().as_secs(), id, master_id);

    Ok(Arc::new(state_bytes))
}