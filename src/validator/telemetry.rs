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

use adnl::common::add_unbound_object_to_map_with_update;
use std::{
    time::Duration,
    sync::atomic::{AtomicU32, Ordering},
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

