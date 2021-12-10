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
    time::{SystemTime, UNIX_EPOCH},
    sync::atomic::{AtomicU32, Ordering},
};
use ton_types::{fail, Result};

const CLEAR_STAT_INTERVAL_BLOCKS: u32 = 10_000;
const MAX_TPS_PERIOD_SEC: u64 = 3600;
const TPS_PERIOD_LAG: u64 = 30;

pub struct TpsCounter {
    transactions: lockfree::map::Map<u64, AtomicU32>, // unix time - transactions
    gc_counter: AtomicU32,
}

impl TpsCounter {
    pub fn new() -> Self {
        TpsCounter {
            transactions: Default::default(),
            gc_counter: AtomicU32::new(0)
        }
    }

    pub fn submit_transactions(&self, block_time: u64, tr_count: usize) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        if now >= block_time && now - block_time <= MAX_TPS_PERIOD_SEC + TPS_PERIOD_LAG {
            add_unbound_object_to_map_with_update(
                &self.transactions,
                block_time,
                |found| if let Some(a) = found {
                    a.fetch_add(tr_count as u32, Ordering::Relaxed);
                    Ok(None)
                } else {
                    Ok(Some(AtomicU32::new(tr_count as u32)))
                }
            ).expect("Can't return error");
        }

        if self.gc_counter.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |c| if c >= CLEAR_STAT_INTERVAL_BLOCKS { Some(0) } else { Some(c + 1) } 
        ).unwrap_or(0) >= CLEAR_STAT_INTERVAL_BLOCKS {
            self.gc();
        }
    }

    pub fn calc_tps(&self, period: u64) -> Result<u32> {
        let mut tr_count = 0;
        if period == 0 {
            fail!("period can't be zero");
        }
        if period > MAX_TPS_PERIOD_SEC {
            fail!("period is too big (max is {})", MAX_TPS_PERIOD_SEC);
        }
        for guard in self.transactions.iter() {
            let time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() - TPS_PERIOD_LAG;
            if time > *guard.key() {
                if time - guard.key() <= period {
                    tr_count += guard.val().load(Ordering::Relaxed);
                }
            }
        }
        Ok(tr_count / period as u32)
    }

    fn gc(&self) {
        let time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        for guard in self.transactions.iter() {
            if time > *guard.key() {
                if time - guard.key() > MAX_TPS_PERIOD_SEC + TPS_PERIOD_LAG {
                    self.transactions.remove(guard.key());
                }
            }
        }
    }
}
