pub mod archives;
pub mod block_db;
pub mod block_handle_db;
pub mod block_index_db;
pub mod block_info_db;
pub mod catchain_persistent_db;
mod cell_db;
pub mod db;
mod dynamic_boc_db;
mod error;
mod lt_db;
mod lt_desc_db;
mod macros; 
pub mod node_state_db;
pub mod shardstate_db;
pub mod shardstate_persistent_db;
pub mod traits;
pub mod types;
pub mod shard_top_blocks_db;

use std::time::{Duration, Instant};

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

pub(crate) const TARGET: &str = "storage";
