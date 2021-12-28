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

pub mod archives;
pub mod block_db;
pub mod block_handle_db;
pub mod block_info_db;
pub mod catchain_persistent_db;
mod cell_db;
pub mod db;
mod dynamic_boc_db;
mod error;
mod macros; 
pub mod node_state_db;
pub mod shardstate_db;
pub mod shardstate_persistent_db;
pub mod traits;
pub mod types;
pub mod shard_top_blocks_db;

#[cfg(feature = "telemetry")]
use adnl::telemetry::Metric;
use std::{sync::{Arc, atomic::AtomicU64}, time::{Duration, Instant}};

pub struct TimeChecker {
    operation: String,
    threshold: Duration,
    start: Instant,
}

impl TimeChecker {
    pub fn new(operation: String, threshold_ms: u64) -> Self {
        let start = std::time::Instant::now();
        log::trace!("{} - started", operation);
        Self {
            operation,
            threshold: Duration::from_millis(threshold_ms),
            start,
        }
    }
}

impl Drop for TimeChecker {
    fn drop(&mut self) {
        let time = self.start.elapsed();
        if time < self.threshold {
            log::trace!("{} - finished, TIME: {}", self.operation, time.as_millis());
        } else {
            log::warn!("{} - finished too slow, TIME: {}ms, expected: {}ms", 
                self.operation, time.as_millis(), self.threshold.as_millis());
        }
    }
}

#[cfg(feature = "telemetry")]
pub struct StorageTelemetry {
    pub file_entries: Arc<Metric>,
    pub handles: Arc<Metric>,
    pub packages: Arc<Metric>,
    pub storage_cells: Arc<Metric>
}

pub struct StorageAlloc {
    pub file_entries: Arc<AtomicU64>,
    pub handles: Arc<AtomicU64>,
    pub packages: Arc<AtomicU64>,
    pub storage_cells: Arc<AtomicU64>
}

pub(crate) const TARGET: &str = "storage";
