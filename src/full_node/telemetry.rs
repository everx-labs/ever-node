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

use adnl::common::{add_unbound_object_to_map, add_unbound_object_to_map_with_update};
use std::{
    time::Instant,
    sync::atomic::{AtomicU64, AtomicU32, Ordering},
    cmp::{max, min},
};
use ton_block::BlockIdExt;

const MAX_DOWNLOAD_BLOCK_ATTEMPTS: usize = 10;
const TOP_BLOCK_BCAST_TTL_SEC: usize = 30;
const DOWNLOADING_BLOCK_TTL_SEC: usize = 300;

pub const TPS_PERIOD_1: u64 = 5 * 60; // 5 min
pub const TPS_PERIOD_2: u64 = 60; // 1 min


pub struct FullNodeTelemetry {
    // Almost all metrics are zeroed while report creation

    applied_blocks: AtomicU64,
    pre_applied_blocks: AtomicU64,
    pre_applied_blocks_bcast: AtomicU64,
    pre_applied_blocks_dloaded: AtomicU64,
    // Map's items are deleted after block bcast got or while report creation 
    // (if top block bcast was got more 15sec ago)
    last_top_block_broadcasts: lockfree::map::Map<BlockIdExt, Instant>,
    top_block_broadcasts: AtomicU64,
    top_block_broadcasts_unic: AtomicU64,
    top_block_broadcasts_bad: AtomicU64,
    block_broadcasts: AtomicU64,
    block_broadcasts_duplicates: AtomicU64,
    block_broadcasts_unneeded: AtomicU64,
    block_downloaded_attempts: [AtomicU64; MAX_DOWNLOAD_BLOCK_ATTEMPTS + 1],
    downloading_blocks: lockfree::map::Map<BlockIdExt, Instant>,
    downloading_blocks_attempts: lockfree::map::Map<BlockIdExt, AtomicU32>,
    block_download_times: AtomicU64,
    block_download_time_sum: AtomicU64,
    block_download_time_min: AtomicU64,
    block_download_time_max: AtomicU64,
    block_broadcast_delay_sum: AtomicU64,
    block_broadcast_delay_min: AtomicU64,
    block_broadcast_delay_max: AtomicU64,
    sent_top_block_broadcasts: AtomicU64,
    sent_block_broadcasts: AtomicU64,
    sent_ext_msg_broadcasts: AtomicU64,
}

impl FullNodeTelemetry {

    pub fn new() -> Self {
        Self {
            applied_blocks: AtomicU64::new(0),
            pre_applied_blocks: AtomicU64::new(0),
            pre_applied_blocks_bcast: AtomicU64::new(0),
            pre_applied_blocks_dloaded: AtomicU64::new(0),
            last_top_block_broadcasts: Default::default(),
            top_block_broadcasts: AtomicU64::new(0),
            top_block_broadcasts_unic: AtomicU64::new(0),
            top_block_broadcasts_bad: AtomicU64::new(0),
            block_broadcasts: AtomicU64::new(0),
            block_broadcasts_duplicates: AtomicU64::new(0),
            block_broadcasts_unneeded: AtomicU64::new(0),
            block_downloaded_attempts: Default::default(),
            downloading_blocks: Default::default(),
            downloading_blocks_attempts: Default::default(),
            block_download_times: AtomicU64::new(0),
            block_download_time_sum: AtomicU64::new(0),
            block_download_time_min: AtomicU64::new(u64::MAX),
            block_download_time_max: AtomicU64::new(0),
            block_broadcast_delay_sum: AtomicU64::new(0),
            block_broadcast_delay_min: AtomicU64::new(u64::MAX),
            block_broadcast_delay_max: AtomicU64::new(0),
            sent_top_block_broadcasts: AtomicU64::new(0),
            sent_block_broadcasts: AtomicU64::new(0),
            sent_ext_msg_broadcasts: AtomicU64::new(0),
        }
    }

    pub fn new_applied_block(&self) {
        self.applied_blocks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn new_pre_applied_block(&self, got_by_broadcast: bool) {
        self.pre_applied_blocks.fetch_add(1, Ordering::Relaxed);
        if got_by_broadcast {
            self.pre_applied_blocks_bcast.fetch_add(1, Ordering::Relaxed);
        } else {
            self.pre_applied_blocks_dloaded.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn good_top_block_broadcast(&self, block_id: &BlockIdExt) {
        self.top_block_broadcasts.fetch_add(1, Ordering::Relaxed);
        if add_unbound_object_to_map(
            &self.last_top_block_broadcasts, block_id.clone(), || Ok(Instant::now())
        ).expect("Can't return error") {
            self.top_block_broadcasts_unic.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn bad_top_block_broadcast(&self) {
        self.top_block_broadcasts_bad.fetch_add(1, Ordering::Relaxed);
    }

    pub fn new_block_broadcast(&self, block_id: &BlockIdExt, duplicate: bool, unneeded: bool) {
        self.block_broadcasts.fetch_add(1, Ordering::Relaxed);
        if duplicate {
            self.block_broadcasts_duplicates.fetch_add(1, Ordering::Relaxed);
        }
        if unneeded {
            self.block_broadcasts_unneeded.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(top_block_time) = self.last_top_block_broadcasts.get(block_id) {
            let block_broadcast_delay = max(1, top_block_time.1.elapsed().as_millis()) as u64;
            self.block_broadcast_delay_min.fetch_min(block_broadcast_delay, Ordering::Relaxed);
            self.block_broadcast_delay_max.fetch_max(block_broadcast_delay, Ordering::Relaxed);
            self.block_broadcast_delay_sum.fetch_add(block_broadcast_delay, Ordering::Relaxed);
            self.last_top_block_broadcasts.remove(block_id);
        }
    }

    pub fn new_downloading_block_attempt(&self, block_id: &BlockIdExt) {
        add_unbound_object_to_map_with_update(
            &self.downloading_blocks_attempts,
            block_id.clone(),
            |found| if let Some(a) = found {
                a.fetch_add(1, Ordering::Relaxed);
                Ok(None)
            } else {
                Ok(Some(AtomicU32::new(1)))
            }
        ).expect("Can't return error");
        add_unbound_object_to_map(
            &self.downloading_blocks, block_id.clone(), || Ok(Instant::now())
        ).expect("Can't return error");
    }

    pub fn new_downloaded_block(&self, block_id: &BlockIdExt) {
        self.block_download_times.fetch_add(1, Ordering::Relaxed);

        if let Some(attempt) = self.downloading_blocks_attempts.get(block_id) {
            let index = (max(attempt.1.load(Ordering::Relaxed), 1) - 1) as usize;
            self.block_downloaded_attempts[min(MAX_DOWNLOAD_BLOCK_ATTEMPTS, index)]
                .fetch_add(1, Ordering::Relaxed);
            self.downloading_blocks_attempts.remove(block_id);
        } else {
            log::trace!("Can't find downloaded block attempts {}", block_id);
        }
        
        if let Some(start) = self.downloading_blocks.get(block_id) {
            let time = start.1.elapsed().as_millis() as u64;
            self.block_download_time_sum.fetch_add(time, Ordering::Relaxed);
            self.block_download_time_min.fetch_min(time, Ordering::Relaxed);
            self.block_download_time_max.fetch_max(time, Ordering::Relaxed);
            self.downloading_blocks.remove(block_id);
        } else {
            log::trace!("Can't find downloaded block start time {}", block_id);
        }
    }

    pub fn sent_top_block_broadcast(&self) {
        self.sent_top_block_broadcasts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn sent_block_broadcast(&self) {
        self.sent_block_broadcasts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn sent_ext_msg_broadcast(&self) {
        self.sent_ext_msg_broadcasts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn report(&self, tps_1: u32, tps_2: u32) -> String {

        // Get and reset statistic
        let applied_blocks = self.applied_blocks.swap(0, Ordering::Relaxed);
        let pre_applied_blocks = self.pre_applied_blocks.swap(0, Ordering::Relaxed);
        let pre_applied_blocks_bcast = self.pre_applied_blocks_bcast.swap(0, Ordering::Relaxed);
        let pre_applied_blocks_dloaded = self.pre_applied_blocks_dloaded.swap(0, Ordering::Relaxed);
        let top_block_broadcasts = self.top_block_broadcasts.swap(0, Ordering::Relaxed);
        let top_block_broadcasts_unic = self.top_block_broadcasts_unic.swap(0, Ordering::Relaxed);
        let top_block_broadcasts_bad = self.top_block_broadcasts_bad.swap(0, Ordering::Relaxed);
        let top_block_broadcasts_dupl = top_block_broadcasts - top_block_broadcasts_unic;
        let block_broadcasts = self.block_broadcasts.swap(0, Ordering::Relaxed);
        let block_broadcasts_duplicates = self.block_broadcasts_duplicates.swap(0, Ordering::Relaxed);
        let block_broadcasts_unneeded = self.block_broadcasts_unneeded.swap(0, Ordering::Relaxed);
        let mut block_downloaded_attempts = [0_u64; MAX_DOWNLOAD_BLOCK_ATTEMPTS + 1];
        let mut block_downloaded_attempts_sum = 0;
        for i in 0..MAX_DOWNLOAD_BLOCK_ATTEMPTS + 1 {
            block_downloaded_attempts[i] = self.block_downloaded_attempts[i].swap(0, Ordering::Relaxed);
            block_downloaded_attempts_sum += block_downloaded_attempts[i];
        }
        let block_download_times = self.block_download_times.swap(0, Ordering::Relaxed);
        let block_download_time_sum = self.block_download_time_sum.swap(0, Ordering::Relaxed);
        let block_download_time_min = self.block_download_time_min.swap(u64::MAX, Ordering::Relaxed);
        let block_download_time_max = self.block_download_time_max.swap(0, Ordering::Relaxed);
        let block_broadcast_delay_sum = self.block_broadcast_delay_sum.swap(0, Ordering::Relaxed);
        let block_broadcast_delay_min = self.block_broadcast_delay_min.swap(u64::MAX, Ordering::Relaxed);
        let block_broadcast_delay_max = self.block_broadcast_delay_max.swap(0, Ordering::Relaxed);
        let sent_top_block_broadcasts = self.sent_top_block_broadcasts.swap(0, Ordering::Relaxed);
        let sent_block_broadcasts = self.sent_block_broadcasts.swap(0, Ordering::Relaxed);
        let sent_ext_msg_broadcasts = self.sent_ext_msg_broadcasts.swap(0, Ordering::Relaxed);
        for guard in self.downloading_blocks_attempts.iter() {
            self.downloading_blocks_attempts.remove(guard.key());
        }

        let mut lost_block_broadcasts = 0;
        for guard in self.last_top_block_broadcasts.iter() {
            if guard.val().elapsed().as_secs() > TOP_BLOCK_BCAST_TTL_SEC as u64 {
                self.last_top_block_broadcasts.remove(guard.key());
                lost_block_broadcasts += 1;
            }
        }
        for guard in self.downloading_blocks.iter() {
            if guard.val().elapsed().as_secs() > DOWNLOADING_BLOCK_TTL_SEC as u64 {
                self.downloading_blocks.remove(guard.key());
            }
        }

        // Report to statsd
        // TODO

        // Build string report
        let mut report = string_builder::Builder::default();

        report.append(format!("applied blocks              {:>10} {:>3.0}%\n", 
            applied_blocks, 
            if pre_applied_blocks > 0 { applied_blocks as f64 / pre_applied_blocks as f64 * 100_f64 } else { 0_f64 }
        ));
        report.append(format!("pre-applied blocks          {:>10} 100%\n", pre_applied_blocks));
        report.append(format!("    got by broadcast        {:>10} {:>3.0}%\n", 
            pre_applied_blocks_bcast,
            if pre_applied_blocks > 0 { pre_applied_blocks_bcast as f64 / pre_applied_blocks as f64 * 100_f64 } else { 0_f64 }
        ));
        report.append(format!("    downloaded              {:>10} {:>3.0}%\n", 
            pre_applied_blocks_dloaded,
            if pre_applied_blocks > 0 { pre_applied_blocks_dloaded as f64  / pre_applied_blocks as f64 * 100_f64 } else { 0_f64 }
        ));
        report.append(        "transactions per second\n");
        report.append(format!("    for {:>3.0}sec window   {:>10}\n", 
            TPS_PERIOD_1,
            tps_1
        ));
        report.append(format!("    for {:>3.0}sec window   {:>10}\n", 
            TPS_PERIOD_2,
            tps_2
        ));

        report.append(        "***\n");
        report.append(        "sent broadcasts:\n");
        report.append(format!("    top block descriptions  {:>10}\n", sent_top_block_broadcasts));
        report.append(format!("    blocks                  {:>10}\n", sent_block_broadcasts));
        report.append(format!("    external messages       {:>10}\n", sent_ext_msg_broadcasts));
        report.append(        "***\n");
        report.append(format!("bad top block broadcast     {:>10}\n", top_block_broadcasts_bad));
        report.append(format!("good top block broadcast    {:>10} 100%\n", top_block_broadcasts_unic));
        report.append(format!(
            "top block broadcast dupl    {:>10} {:>3.0}%\n",
            top_block_broadcasts_dupl,
            if top_block_broadcasts_dupl != 0 { top_block_broadcasts_dupl as f64 / top_block_broadcasts_unic as f64 * 100_f64 } else { 0_f64 }));
        report.append(        "*\n");
        report.append(format!("lost block broadcasts       {:>10}\n", lost_block_broadcasts));
        report.append(format!("block broadcast             {:>10} 100%\n", block_broadcasts));
        report.append(format!("block broadcast duplicates  {:>10} {:>3.0}%\n",
            block_broadcasts_duplicates,
            if block_broadcasts > 0 { block_broadcasts_duplicates as f64  / block_broadcasts as f64  * 100_f64 } else { 0_f64 }
        ));
        report.append(format!("unneeded block-broadcast    {:>10} {:>3.0}%\n",
            block_broadcasts_unneeded,
            if block_broadcasts > 0 { block_broadcasts_unneeded as f64  / block_broadcasts as f64  * 100_f64 } else { 0_f64 }
        ));
        report.append(        "(block was already downloaded)\n");
        report.append(        "***\n");
        report.append(        "block download attempts:\n");
        for i in 0..MAX_DOWNLOAD_BLOCK_ATTEMPTS + 1 {
            let i_str = format!("{:>4}", i + 1);
            report.append(format!(
                "                      {}  {:>10} {:>3.0}%\n",
                if i != MAX_DOWNLOAD_BLOCK_ATTEMPTS { &i_str } else { "more" },
                block_downloaded_attempts[i],
                if block_downloaded_attempts_sum > 0 { block_downloaded_attempts[i] as f64  / block_downloaded_attempts_sum as f64  * 100_f64 } else { 0_f64 }
            ));
        }
        report.append(        "***\n");
        if block_download_times > 0 {
            report.append(format!(
                "block download time (min avg max)  {}  {:.0}  {}\n", 
                block_download_time_min,
                block_download_time_sum / block_download_times,
                block_download_time_max
            ));
        }
        if block_broadcasts > 0 {
            report.append(format!(
                "delay top-block-bcast ↔︎ block broadcast (min avg max)  {}  {:.0}  {}", 
                block_broadcast_delay_min,
                block_broadcast_delay_sum as f64 / block_broadcasts as f64,
                block_broadcast_delay_max
            ));
        }    

        report.string().expect("unexpected error while building full node's telemetry report")
    }
}

