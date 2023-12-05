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
pub mod dynamic_boc_rc_db;
pub mod error;
mod macros; 
pub mod node_state_db;
pub mod shardstate_db_async;
pub mod traits;
pub mod types;
pub mod shard_top_blocks_db;
#[cfg(test)]
mod tests;

#[cfg(feature = "telemetry")]
use adnl::telemetry::{Metric, MetricBuilder};
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
    pub storage_cells: Arc<Metric>,
    pub shardstates_queue: Arc<Metric>,
    pub cells_counters: Arc<Metric>,
    pub cell_counter_from_cache: Arc<MetricBuilder>,
    pub cell_counter_from_db: Arc<MetricBuilder>,
    pub updated_old_cells: Arc<MetricBuilder>,
    pub updated_cells: Arc<MetricBuilder>,
    pub new_cells: Arc<MetricBuilder>,
    pub deleted_cells: Arc<MetricBuilder>,
}
#[cfg(feature = "telemetry")]
impl Default for StorageTelemetry {
    fn default() -> Self {
        Self {
            file_entries: Metric::without_totals("", 1),
            handles: Metric::without_totals("", 1),
            packages: Metric::without_totals("", 1),
            storage_cells: Metric::without_totals("", 1),
            shardstates_queue: Metric::without_totals("", 1),
            cells_counters: Metric::without_totals("", 1),
            cell_counter_from_cache: MetricBuilder::with_metric_and_period(Metric::with_total_amount("", 1), 1000000000),
            cell_counter_from_db: MetricBuilder::with_metric_and_period(Metric::with_total_amount("", 1), 1000000000),
            updated_old_cells: MetricBuilder::with_metric_and_period(Metric::with_total_amount("", 1), 1000000000),
            updated_cells: MetricBuilder::with_metric_and_period(Metric::with_total_amount("", 1), 1000000000),
            new_cells: MetricBuilder::with_metric_and_period(Metric::with_total_amount("", 1), 1000000000),
            deleted_cells: MetricBuilder::with_metric_and_period(Metric::with_total_amount("", 1), 1000000000),
        }
    }
}

#[derive(Default)]
pub struct StorageAlloc {
    pub file_entries: Arc<AtomicU64>,
    pub handles: Arc<AtomicU64>,
    pub packages: Arc<AtomicU64>,
    pub storage_cells: Arc<AtomicU64>,
}

pub(crate) const TARGET: &str = "storage";
