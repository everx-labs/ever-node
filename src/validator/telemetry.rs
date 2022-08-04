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

use adnl::{
    common::{add_unbound_object_to_map_with_update, add_unbound_object_to_map},
    telemetry::Metric
};
use std::{
    time::Duration,
    sync::{Arc, atomic::{AtomicU32, AtomicUsize, Ordering}},
    cmp::{max, min},
    collections::HashMap,
};
use ton_block::ShardIdent;

const TR_PER_BLOCK_STEPS: usize = 10;
const TR_PER_BLOCK_STEP: u32 = 100;
const GAS_PER_BLOCK_STEPS: usize = 10;
const GAS_PER_BLOCK_STEP: u32 = 500000;
const LONG_ATTEMPT_CUTOFF_MS: u32 = 1000;

#[derive(Default)]
pub struct CollatorValidatorTelemetry {
    master: ShardTelemetry,
    shardes: lockfree::map::Map<ShardIdent, ShardTelemetry>
}

struct ShardTelemetry {
    total_attempts: AtomicU32,
    total_time: AtomicU32,
    min_time: AtomicU32,
    max_time: AtomicU32,
    too_long: AtomicU32, // Success but longer LONG_ATTEMPT_CUTOFF_MS ms

    total_fail: AtomicU32,
    wait_state_fail: AtomicU32,
    get_block_fail: AtomicU32,
    wrong_validator_set: AtomicU32,
    more_than_8_blocks: AtomicU32,
    not_actual_mc: AtomicU32,

    // for success attempts
    total_transactions: AtomicU32,
    transactions_per_block: [AtomicU32; TR_PER_BLOCK_STEPS],
    total_gas: AtomicU32,
    gas_per_block: [AtomicU32; GAS_PER_BLOCK_STEPS],
}

struct ShardTelemetrySample {
    total_attempts: u32,
    total_time: u32,
    min_time: u32,
    max_time: u32,
    too_long: u32,
    total_fail: u32,
    wait_state_fail: u32,
    get_block_fail: u32,
    wrong_validator_set: u32,
    more_than_8_blocks: u32,
    not_actual_mc: u32,
    total_transactions: u32,
    transactions_per_block: [u32; TR_PER_BLOCK_STEPS],
    total_gas: u32,
    gas_per_block: [u32; GAS_PER_BLOCK_STEPS],
}

impl CollatorValidatorTelemetry {
    pub fn succeeded_attempt(
        &self,
        shard: &ShardIdent,
        time: Duration,
        transactions: u32,
        gas: u32
    ) {
        if shard.is_masterchain() {
            self.master.succeeded_attempt(time, transactions, gas);
        } else {
            add_unbound_object_to_map_with_update(
                &self.shardes,
                shard.clone(),
                |found| if let Some(found) = found {
                    found.succeeded_attempt(time, transactions, gas);
                    Ok(None)
                } else {
                    let s = ShardTelemetry::default();
                    s.succeeded_attempt(time, transactions, gas);
                    Ok(Some(s))
                }
            ).expect("Can't return error");
        }
    }

    pub fn failed_attempt(&self, shard: &ShardIdent, error: &str) {
        if shard.is_masterchain() {
            self.master.failed_attempt(error);
        } else {
            add_unbound_object_to_map_with_update(
                &self.shardes,
                shard.clone(),
                |found| if let Some(found) = found {
                    found.failed_attempt(error);
                    Ok(None)
                } else {
                    let s = ShardTelemetry::default();
                    s.failed_attempt(error);
                    Ok(Some(s))
                }
            ).expect("Can't return error");
        }
    }

    pub fn report(&self) -> String {
        let master = self.master.reset();
        let mut shardes = HashMap::new();
        
        let mut report = string_builder::Builder::default();
        report.append("***\nMaster chain:\n");
        report.append(master.report());

        if self.shardes.iter().count() > 0 {
            let mut shardes_total = ShardTelemetry::default().reset();
            for shard in self.shardes.iter() {
                let sample = shard.val().reset();
                shardes_total.add(&sample);
                shardes.insert(shard.key().clone(), sample);
                self.shardes.remove(shard.key());
            }
            report.append("***\nShard chains total:\n");
            report.append(shardes_total.report());
        }

        for (shard_id, sample) in shardes {
            report.append(format!("***\nShard chain {}:\n", shard_id));
            report.append(sample.report());
        }

        report.string().expect("unexpected error while building collator/validator telemetry report")
    }
}

impl Default for ShardTelemetry {
    fn default() -> Self { 
        Self {
            total_attempts: AtomicU32::new(0),
            total_time: AtomicU32::new(0),
            min_time: AtomicU32::new(u32::MAX),
            max_time: AtomicU32::new(0),
            too_long: AtomicU32::new(0),
            total_fail: AtomicU32::new(0),
            wait_state_fail: AtomicU32::new(0),
            get_block_fail: AtomicU32::new(0),
            wrong_validator_set: AtomicU32::new(0),
            more_than_8_blocks: AtomicU32::new(0),
            not_actual_mc: AtomicU32::new(0),
            total_transactions: AtomicU32::new(0),
            transactions_per_block: Default::default(),
            total_gas: AtomicU32::new(0),
            gas_per_block: Default::default(),
        }
    }
}

impl ShardTelemetry {

    pub fn succeeded_attempt(
        &self,
        time: Duration,
        transactions: u32,
        gas: u32
    ) {
        self.total_attempts.fetch_add(1, Ordering::Relaxed);
        let time = time.as_millis() as u32;
        self.total_time.fetch_add(time, Ordering::Relaxed);
        self.min_time.fetch_min(time, Ordering::Relaxed);
        self.max_time.fetch_max(time, Ordering::Relaxed);
        if time > LONG_ATTEMPT_CUTOFF_MS {
            self.too_long.fetch_add(1, Ordering::Relaxed);
        }
        self.total_transactions.fetch_add(transactions, Ordering::Relaxed);
        self.transactions_per_block[min((transactions / TR_PER_BLOCK_STEP) as usize, TR_PER_BLOCK_STEPS - 1)]
            .fetch_add(1, Ordering::Relaxed);
        self.total_gas.fetch_add(transactions, Ordering::Relaxed);
        self.gas_per_block[min((gas / GAS_PER_BLOCK_STEP) as usize, GAS_PER_BLOCK_STEPS - 1)]
           .fetch_add(1, Ordering::Relaxed);
    }

    pub fn failed_attempt(&self, error: &str) {
        self.total_attempts.fetch_add(1, Ordering::Relaxed);
        self.total_fail.fetch_add(1, Ordering::Relaxed);
        if error.contains("shard_states_awaiters: timeout") {
            self.wait_state_fail.fetch_add(1, Ordering::Relaxed);
        }
        if error.contains("Key not found") || error.contains("KeyNotFound") {
            self.get_block_fail.fetch_add(1, Ordering::Relaxed);
        }
        if error.contains("only validator set with cc_seqno") {
            self.wrong_validator_set.fetch_add(1, Ordering::Relaxed);
        }
        if error.contains("an unregistered chain of length > 8") {
            self.more_than_8_blocks.fetch_add(1, Ordering::Relaxed);
        }
        if error.contains("Given last_mc_seq_no ") && error.contains(" is not actual") {
            self.not_actual_mc.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn reset(&self) -> ShardTelemetrySample {
        let mut transactions_per_block = [0; TR_PER_BLOCK_STEPS];
        let mut gas_per_block = [0; TR_PER_BLOCK_STEPS];
        for i in 0..TR_PER_BLOCK_STEPS {
            transactions_per_block[i] = self.transactions_per_block[i].swap(0, Ordering::Relaxed);
            gas_per_block[i] = self.gas_per_block[i].swap(0, Ordering::Relaxed);
        }
        ShardTelemetrySample {
            total_attempts: self.total_attempts.swap(0, Ordering::Relaxed),
            total_time: self.total_time.swap(0, Ordering::Relaxed),
            min_time: self.min_time.swap(u32::MAX, Ordering::Relaxed),
            max_time: self.max_time.swap(0, Ordering::Relaxed),
            too_long: self.too_long.swap(0, Ordering::Relaxed),
            total_fail: self.total_fail.swap(0, Ordering::Relaxed),
            wait_state_fail: self.wait_state_fail.swap(0, Ordering::Relaxed),
            get_block_fail: self.get_block_fail.swap(0, Ordering::Relaxed),
            wrong_validator_set: self.wrong_validator_set.swap(0, Ordering::Relaxed),
            more_than_8_blocks: self.more_than_8_blocks.swap(0, Ordering::Relaxed),
            not_actual_mc: self.not_actual_mc.swap(0, Ordering::Relaxed),
            total_transactions: self.total_transactions.swap(0, Ordering::Relaxed),
            transactions_per_block,
            total_gas: self.total_gas.swap(0, Ordering::Relaxed),
            gas_per_block,
        }
    }
}

impl ShardTelemetrySample {

    fn add(&mut self, other: &Self) {
        for i in 0..TR_PER_BLOCK_STEPS {
            self.transactions_per_block[i] += other.transactions_per_block[i];
            self.gas_per_block[i] += other.gas_per_block[i];
        }
        self.total_attempts += other.total_attempts;
        self.total_time += other.total_time;
        self.min_time = min(self.min_time, other.min_time);
        self.max_time = max(self.max_time, other.max_time);
        self.too_long += other.too_long;
        self.total_fail += other.total_fail;
        self.wait_state_fail += other.wait_state_fail;
        self.get_block_fail += other.get_block_fail;
        self.wrong_validator_set += other. wrong_validator_set;
        self.more_than_8_blocks += other. more_than_8_blocks;
        self.not_actual_mc += other. not_actual_mc;
        self.total_transactions += other.total_transactions;
        self.total_gas += other.total_gas;
    }

    pub fn report(&self) -> String {
        if self.total_attempts == 0 {
            return "No one attempt".to_owned();
        }

        let mut report = string_builder::Builder::default();
        let total_success = self.total_attempts - self.total_fail;

        report.append(format!("attempts                  {:>10}  100%\n", self.total_attempts));
        report.append(format!("total succeeded           {:>10}  {:>3.0}%\n", 
            total_success, total_success as f64 / self.total_attempts as f64 * 100_f64));
        report.append(format!("longer than {:>4}ms        {:>10}  {:>3.0}%\n",
            LONG_ATTEMPT_CUTOFF_MS,
            self.too_long,
            self.too_long as f64 / self.total_attempts as f64 * 100_f64
        ));
        report.append(format!("total failed              {:>10}  {:>3.0}%\n", 
            self.total_fail, self.total_fail as f64 / self.total_attempts as f64 * 100_f64));
        
        if self.total_fail > 0 {
            report.append(        "reasons of fail:\n");
            report.append(format!("    no wait state         {:>10}  {:>3.0}%\n", 
                self.wait_state_fail, self.wait_state_fail as f64 / self.total_fail as f64 * 100_f64));
            report.append(format!("    can't get_block       {:>10}  {:>3.0}%\n", 
                self.get_block_fail, self.get_block_fail as f64 / self.total_fail as f64 * 100_f64));
            report.append(format!("    wrong validator set   {:>10}  {:>3.0}%\n", 
                self.wrong_validator_set, self.wrong_validator_set as f64 / self.total_fail as f64 * 100_f64));
            report.append(format!("    8 blocks w/a mc commit{:>10}  {:>3.0}%\n", 
                self.more_than_8_blocks, self.more_than_8_blocks as f64 / self.total_fail as f64 * 100_f64));
            report.append(format!("    given mc isn't actual {:>10}  {:>3.0}%\n", 
                self.not_actual_mc, self.not_actual_mc as f64 / self.total_fail as f64 * 100_f64));
            let other_fail = self.total_fail - self.wait_state_fail - self.get_block_fail -
                self.wrong_validator_set - self.more_than_8_blocks - self.not_actual_mc;
            report.append(format!("    other                 {:>10}  {:>3.0}%\n", 
                other_fail, other_fail as f64 / self.total_fail as f64 * 100_f64));
        }
        
        if total_success > 0 {
            report.append(        "transactions per block:\n");
            for i in 0..TR_PER_BLOCK_STEPS {
                report.append(format!(
                                "    {:>4}..{  }            {:>10}  {:>3.0}%\n",
                    i as u32 * TR_PER_BLOCK_STEP, 
                    if i == TR_PER_BLOCK_STEPS - 1 {
                        "    ".to_owned() 
                    } else { 
                        format!("{:<4}", (i + 1) as u32 * TR_PER_BLOCK_STEP) 
                    },
                    self.transactions_per_block[i],
                    self.transactions_per_block[i] as f64 / total_success as f64 * 100_f64
                ));
            }
            report.append(format!("    avg                   {:>10}\n", self.total_transactions / total_success));

            report.append(        "gas per block:\n");
            for i in 0..GAS_PER_BLOCK_STEPS {
                report.append(format!(
                                "    {:>8}..{}    {:>10}  {:>3.0}%\n",
                    i as u32 * GAS_PER_BLOCK_STEP, 
                    if i == GAS_PER_BLOCK_STEPS - 1 {
                        "        ".to_owned()
                    } else { 
                        format!("{:<8}", (i + 1) as u32 * GAS_PER_BLOCK_STEP) 
                    },
                    self.gas_per_block[i],
                    self.gas_per_block[i] as f64 / total_success as f64 * 100_f64
                ));
            }
            report.append(format!("    avg                   {:>10}\n", self.total_gas / total_success));

            report.append(format!("time, ms (min avg max)           {} {} {}\n", 
                self.min_time,
                self.total_time / total_success,
                self.max_time
            ));
        }
        report.string().expect("unexpected error while building collator/validator telemetry report")
    }
}

struct RempQueueTelemetry {
    pub got_from_fullnode: AtomicUsize,
    pub in_channel_to_catchain: Arc<Metric>,
    pub sent_to_catchain:  AtomicUsize,
    pub got_from_catchain: AtomicUsize,
    pub ignored_from_catchain: AtomicUsize,
    pub in_channel_to_rmq: Arc<Metric>,
    pub pending_collation: Arc<Metric>,
    pub rmq_catchain_mutex_awaiting: Arc<Metric>,
}

#[derive(Default)]
struct RempQueueTelemetrySample {
    pub got_from_fullnode: usize,
    pub in_channel_to_catchain: (u64, u64, u64), // cur, avg, max
    pub sent_to_catchain:  usize,
    pub got_from_catchain: usize,
    pub ignored_from_catchain: usize,
    pub in_channel_to_rmq: (u64, u64, u64),
    pub pending_collation: (u64, u64, u64),
    pub rmq_catchain_mutex_awaiting: (u64, u64, u64),
}

impl RempQueueTelemetry {
    pub fn new(average_period_secs: u64) -> Self {
        RempQueueTelemetry {
            got_from_fullnode: AtomicUsize::default(),
            in_channel_to_catchain: Metric::without_totals("in channel to catchain", average_period_secs),
            sent_to_catchain:  AtomicUsize::default(),
            got_from_catchain: AtomicUsize::default(),
            ignored_from_catchain: AtomicUsize::default(),
            in_channel_to_rmq: Metric::without_totals("in channel to rmq", average_period_secs),
            pending_collation: Metric::without_totals("pending collation", average_period_secs),
            rmq_catchain_mutex_awaiting: Metric::without_totals("rmq_catchain_mutex_awaiting", average_period_secs),
        }
    }

    pub fn reset(&self) -> RempQueueTelemetrySample {
        RempQueueTelemetrySample {
            got_from_fullnode: self.got_from_fullnode.swap(0, Ordering::Relaxed),
            in_channel_to_catchain: get_curr_avg_max(&self.in_channel_to_catchain),
            sent_to_catchain: self.sent_to_catchain.swap(0, Ordering::Relaxed),
            got_from_catchain: self.got_from_catchain.swap(0, Ordering::Relaxed),
            ignored_from_catchain: self.ignored_from_catchain.swap(0, Ordering::Relaxed),
            in_channel_to_rmq: get_curr_avg_max(&self.in_channel_to_rmq),
            pending_collation: get_curr_avg_max(&self.pending_collation),
            rmq_catchain_mutex_awaiting: get_curr_avg_max(&self.rmq_catchain_mutex_awaiting),
        }
    }
}

pub struct RempCoreTelemetry {
    period_sec: u64,
    got_from_fullnode: AtomicUsize,
    in_channel_from_fullnode: Arc<Metric>,
    pending_from_fullnode: Arc<Metric>,
    queues: lockfree::map::Map<ShardIdent, RempQueueTelemetry>,
    add_to_cache_attempts: AtomicUsize,
    added_to_cache: AtomicUsize,
    deleted_from_cache: AtomicUsize,
    cache_size: Arc<Metric>,
    cache_mutex_awaiting: Arc<Metric>,
    incoming_mutex_awaiting: Arc<Metric>,
    collator_receipt_mutex_awaiting: Arc<Metric>,
}

struct RempCoreTelemetrySample {
    pub got_from_fullnode: usize,
    pub in_channel_from_fullnode: (u64, u64, u64),
    pub pending_from_fullnode: (u64, u64, u64),
    pub queues: HashMap<ShardIdent, RempQueueTelemetrySample>,
    pub queues_total: RempQueueTelemetrySample,
    pub add_to_cache_attempts: usize,
    pub added_to_cache: usize,
    pub deleted_from_cache: usize,
    pub cache_size: (u64, u64, u64),
    pub cache_mutex_awaiting: (u64, u64, u64),
    pub incoming_mutex_awaiting: (u64, u64, u64),
    pub collator_receipt_mutex_awaiting: (u64, u64, u64),
}

impl RempCoreTelemetry {

    pub fn new(period_sec: u64) -> Self {
        RempCoreTelemetry {
            period_sec,
            got_from_fullnode: AtomicUsize::default(),
            in_channel_from_fullnode: Metric::without_totals("in channel from fullnode", period_sec),
            pending_from_fullnode: Metric::without_totals("pending from fullnode", period_sec),
            queues: lockfree::map::Map::new(),
            add_to_cache_attempts: AtomicUsize::default(),
            added_to_cache: AtomicUsize::default(),
            deleted_from_cache: AtomicUsize::default(),
            cache_size: Metric::without_totals("messages cache size", period_sec),
            cache_mutex_awaiting: Metric::without_totals("cache_mutex_awaiting", period_sec),
            incoming_mutex_awaiting: Metric::without_totals("incoming_mutex_awaiting", period_sec),
            collator_receipt_mutex_awaiting: Metric::without_totals("collator_receipt_mutex_awaiting", period_sec),
        }
    }

    pub fn message_from_fullnode(&self) {
        self.got_from_fullnode.fetch_add(1, Ordering::Relaxed);
    }

    pub fn in_channel_from_fullnode(&self, length: usize) {
        self.in_channel_from_fullnode.update(length as u64);
    }

    pub fn pending_from_fullnode(&self, length: usize) {
        self.pending_from_fullnode.update(length as u64);
    }

    pub fn messages_from_fullnode_for_shard(&self, shard: &ShardIdent, new_messages: usize) {
        self.update_shard_telemetry(
            shard,
            |t| { t.got_from_fullnode.fetch_add(new_messages, Ordering::Relaxed); }
        );
    }

    pub fn in_channel_to_catchain(&self, shard: &ShardIdent, count: usize) {
        self.update_shard_telemetry(
            shard,
            |t| { t.in_channel_to_catchain.update(count as u64); }
        );
    }

    pub fn sent_to_catchain(&self, shard: &ShardIdent, new_messages: usize) {
        self.update_shard_telemetry(
            shard,
            |t| { t.sent_to_catchain.fetch_add(new_messages, Ordering::Relaxed); }
        );
    }

    pub fn got_from_catchain(&self, shard: &ShardIdent, total: usize, ignored: usize) {
        self.update_shard_telemetry(
            shard,
            |t| {
                t.got_from_catchain.fetch_add(total, Ordering::Relaxed);
                t.ignored_from_catchain.fetch_add(ignored, Ordering::Relaxed);
             }
        );
    }

    pub fn in_channel_to_rmq(&self, shard: &ShardIdent, count: usize) {
        self.update_shard_telemetry(
            shard,
            |t| { t.in_channel_to_rmq.update(count as u64); }
        );
    }

    pub fn pending_collation(&self, shard: &ShardIdent, count: usize) {
        self.update_shard_telemetry(
            shard,
            |t| { t.pending_collation.update(count as u64); }
        );
    }

    pub fn add_to_cache_attempt(&self, added: bool) {
        self.add_to_cache_attempts.fetch_add(1, Ordering::Relaxed);
        if added {
            self.added_to_cache.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn deleted_from_cache(&self, deleted_messages: usize) {
        self.deleted_from_cache.fetch_add(deleted_messages, Ordering::Relaxed);
    }

    pub fn cache_size(&self, size: usize) {
        self.cache_size.update(size as u64);
    }

    pub fn cache_mutex_metric(&self) -> Arc<Metric> {
        self.cache_mutex_awaiting.clone()
    }

    pub fn incoming_mutex_metric(&self) -> Arc<Metric> {
        self.incoming_mutex_awaiting.clone()
    }

    pub fn collator_receipt_mutex_metric(&self) -> Arc<Metric> {
        self.collator_receipt_mutex_awaiting.clone()
    }

    pub fn cache_size_metric(&self) -> Arc<Metric> {
        self.cache_size.clone()
    }

    pub fn rmq_catchain_mutex_metric(&self, shard: &ShardIdent) -> Arc<Metric> {
        loop {
            if let Some(q) = self.queues.get(shard) {
                return q.val().rmq_catchain_mutex_awaiting.clone();
            } else {
                let _ = add_unbound_object_to_map(&self.queues, shard.clone(),
                    || Ok(RempQueueTelemetry::new(self.period_sec))).expect("Can't return error");
            }
        }
    }

    fn update_shard_telemetry(
        &self,
        shard: &ShardIdent,
        mut updater: impl FnMut(&RempQueueTelemetry)
    ) {
        // We undarstand that teoretically closure might be called more than one time,
        // and `new_messages` might added twice and more, but in practise we usually don't have 
        // concurrent access to one shard.
        add_unbound_object_to_map_with_update(
            &self.queues,
            shard.clone(),
            |found| if let Some(found) = found {
                updater(found);
                Ok(None)
            } else {
                let t = RempQueueTelemetry::new(self.period_sec);
                updater(&t);
                Ok(Some(t))
            }
        ).expect("Can't return error");
    }

    fn reset(&self) -> RempCoreTelemetrySample {
        let mut queues = HashMap::new();
        let mut queues_total = RempQueueTelemetrySample::default();
        for guard in &self.queues {
            let s = guard.val().reset();
            queues_total.got_from_fullnode += s.got_from_fullnode;
            queues_total.sent_to_catchain += s.sent_to_catchain;
            queues_total.got_from_catchain += s.got_from_catchain;
            queues_total.ignored_from_catchain += s.ignored_from_catchain;
            if s.got_from_fullnode > 0 || s.sent_to_catchain > 0 || 
               s.got_from_catchain > 0 || s.ignored_from_catchain > 0 
            {
                queues.insert(guard.key().clone(), s);
            }
        }
        RempCoreTelemetrySample {
            got_from_fullnode: self.got_from_fullnode.swap(0, Ordering::Relaxed),
            in_channel_from_fullnode: get_curr_avg_max(&self.in_channel_from_fullnode),
            pending_from_fullnode: get_curr_avg_max(&self.pending_from_fullnode),
            queues,
            queues_total,
            add_to_cache_attempts: self.add_to_cache_attempts.swap(0, Ordering::Relaxed),
            added_to_cache: self.added_to_cache.swap(0, Ordering::Relaxed),
            deleted_from_cache: self.deleted_from_cache.swap(0, Ordering::Relaxed),
            cache_size: get_curr_avg_max(&self.cache_size),
            cache_mutex_awaiting: get_curr_avg_max(&self.cache_mutex_awaiting),
            incoming_mutex_awaiting: get_curr_avg_max(&self.incoming_mutex_awaiting),
            collator_receipt_mutex_awaiting: get_curr_avg_max(&self.collator_receipt_mutex_awaiting),
        }
    }

    pub fn report(&self) -> String {
        let mut report = string_builder::Builder::default();
        let sample = self.reset();

        report.append(format!("*for {}sec period. cur|avg|max\n", self.period_sec));
        report.append("                                 all shards");
        for s in sample.queues.keys() {
            report.append(format!("   {}", s.shard_prefix_as_str_with_tag()));
        }
        report.append("\n");

        report.append(format!(
            "got from fullnode                     {:>5}\n",
            sample.got_from_fullnode));
        report.append(format!(
            "in channel from fullnode  {:>5}|{:>5}|{:>5}\n",
            sample.in_channel_from_fullnode.0,
            sample.in_channel_from_fullnode.1,
            sample.in_channel_from_fullnode.2
        ));
        report.append(format!("pending from fullnode     {:>5}|{:>5}|{:>5}\n",
            sample.pending_from_fullnode.0, sample.pending_from_fullnode.1, sample.pending_from_fullnode.2
        ));

        report.append(format!(
            "got from fullnode                     {:>5}", sample.queues_total.got_from_fullnode
        ));
        for s in sample.queues.values() {
            report.append(format!("              {:>5}", s.got_from_fullnode));
        }
        report.append("\n");

        report.append("in channel to catchain                     ");
        for s in sample.queues.values() {
            report.append(format!("  {:>5}|{:>5}|{:>5}",
                s.in_channel_to_catchain.0, s.in_channel_to_catchain.1, s.in_channel_to_catchain.2
            ));
        }
        report.append("\n");

        report.append(format!(
            "sent to catchain                      {:>5}", sample.queues_total.sent_to_catchain
        ));
        for s in sample.queues.values() {
            report.append(format!("              {:>5}", s.sent_to_catchain));
        }
        report.append("\n");

        report.append(format!(
            "got from catchain (total)             {:>5}", sample.queues_total.got_from_catchain
        ));
        for s in sample.queues.values() {
            report.append(format!("              {:>5}", s.got_from_catchain));
        }
        report.append("\n");

        report.append(format!(
            "  new                                 {:>5}", 
            sample.queues_total.got_from_catchain - sample.queues_total.ignored_from_catchain
        ));
        for s in sample.queues.values() {
            report.append(format!("              {:>5}", s.got_from_catchain - s.ignored_from_catchain));
        }
        report.append("\n");

        report.append(format!(
            "  duplicates                          {:>5}", sample.queues_total.ignored_from_catchain
        ));
        for s in sample.queues.values() {
            report.append(format!("              {:>5}", s.ignored_from_catchain));
        }
        report.append("\n");
        
        report.append("in channel to rmq                          ");
        for s in sample.queues.values() {
            report.append(format!("  {:>5}|{:>5}|{:>5}",
                s.in_channel_to_rmq.0, s.in_channel_to_rmq.1, s.in_channel_to_rmq.2
            ));
        }
        report.append("\n");

        report.append("pending collation                          ");
        for s in sample.queues.values() {
            report.append(format!("  {:>5}|{:>5}|{:>5}",
                s.pending_collation.0, s.pending_collation.1, s.pending_collation.2
            ));
        }
        report.append("\n");

        report.append("rmq catchain mutex awaiting (µs)           ");
        for s in sample.queues.values() {
            report.append(format!("  {:>5}|{:>5}|{:>5}",
                s.rmq_catchain_mutex_awaiting.0, s.rmq_catchain_mutex_awaiting.1, s.rmq_catchain_mutex_awaiting.2
            ));
        }
        report.append("\n");

        report.append(format!(
            "add to cache (total)                  {:>5}\n", sample.add_to_cache_attempts
        ));
        report.append(format!(
            "  new                                 {:>5}\n",
            sample.added_to_cache
        ));
        report.append(format!(
            "  duplicates                          {:>5}\n",
            sample.add_to_cache_attempts - sample.added_to_cache
        ));
        report.append(format!(
            "deleted from cache                    {:>5}\n", sample.deleted_from_cache
        ));
        report.append(format!(
            "cache size                {:>5}|{:>5}|{:>5}\n",
            sample.cache_size.0, sample.cache_size.1, sample.cache_size.2
        ));
        report.append(format!(
            "cache mutex awaiting (µs) {:>5}|{:>5}|{:>5}\n",
            sample.cache_mutex_awaiting.0, sample.cache_mutex_awaiting.1, sample.cache_mutex_awaiting.2
        ));
        report.append(format!(
            "incoming mutex awaiting (µs) {:>5}|{:>5}|{:>5}\n",
            sample.incoming_mutex_awaiting.0, sample.incoming_mutex_awaiting.1, sample.incoming_mutex_awaiting.2
        ));
        report.append(format!(
            "collator receipt mutex... {:>5}|{:>5}|{:>5}\n",
            sample.collator_receipt_mutex_awaiting.0, sample.collator_receipt_mutex_awaiting.1, 
            sample.collator_receipt_mutex_awaiting.2
        ));

        report.string().expect("unexpected error while building remp core telemetry report")
    }
}

fn get_curr_avg_max(metric: &Metric) -> (u64, u64, u64) {
    (metric.current(), metric.get_average(), metric.maximum())
}

