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

use std::{collections::HashSet, cmp::max, sync::Arc};
use ton_block::{BlockLimits, ParamLimitIndex};
use ton_types::{Cell, Result, UInt256, UsageTree};

pub struct BlockLimitStatus {
    accounts: u32,
    gas_used: u32,
    limits: Arc<BlockLimits>,
    lt_current: u64,
    lt_start: u64,
    in_msgs: u32,
    out_msgs: u32,
    out_msg_queue_ops: u32,
    stats: CellStorageStats,
    transactions: u32
}

impl BlockLimitStatus {

    /// Constructor
    pub fn with_limits(limits: Arc<BlockLimits>) -> Self { 
        Self {
            accounts: 0,
            gas_used: 0,
            limits,
            lt_current: 0,
            lt_start: std::u64::MAX,
            in_msgs: 0,
            out_msgs: 0,
            out_msg_queue_ops: 0,
            stats: CellStorageStats::default(),
            transactions: 0,
        }
    }

    /// Add gas usage
    pub fn add_gas_used(&mut self, gas: u32) {
        self.gas_used += gas;
    }

    pub fn gas_used(&self) -> u32 { self.gas_used }

    /// Add transactions
    pub fn add_transaction(&mut self, account: bool) {
        self.transactions += 1;
        if account {
            self.accounts += 1;
        }
    }

    /// Classify the status
    pub fn classify(&self) -> ParamLimitIndex {
        max(
            max(
                self.limits.bytes().classify(self.estimate_block_size(None)),
                self.limits.gas().classify(self.gas_used)
            ),
            self.limits.lt_delta().classify(self.lt_delta())
        )
    }

    /// Register operation with output message queue in the block
    pub fn register_out_msg_queue_op(
        &mut self, 
        root: Option<&Cell>,
        usage_tree: &UsageTree,
        force: bool
    ) -> Result<()> {
        self.out_msg_queue_ops += 1;
        if force || ((self.out_msg_queue_ops & 63) == 0) {
            if let Some(root) = root {
                self.stats.add_proof(root, usage_tree)?;
            }
        }
        Ok(())
    }

    pub fn register_in_msg_op(&mut self, msg_cell: &Cell, msg_dict: &Cell) -> Result<()> {
        self.in_msgs += 1;
        self.stats.add_cell(msg_cell)?;
        if (self.in_msgs & 63) == 0 {
            self.stats.add_cell(msg_dict)?;
        }
        Ok(())
    }

    pub fn register_out_msg_op(&mut self, msg_cell: &Cell, msg_dict: &Cell) -> Result<()> {
        self.out_msgs += 1;
        self.stats.add_cell(msg_cell)?;
        if (self.out_msgs & 63) == 0 {
            self.stats.add_cell(msg_dict)?;
        }
        Ok(())
    }

    /// Update logical time
    pub fn update_lt(&mut self, lt: u64) {
        self.lt_current = max(self.lt_current, lt);
        if self.lt_start > self.lt_current {
            self.lt_start = lt;
        }
    }

    pub fn lt(&self) -> u64 {
        self.lt_current
    }

    fn estimate_block_size(&self, extra: Option<&CellStorageStats>) -> u32 {
        let mut bits = 
            self.stats.cells_stats.bits + self.stats.proof_stats.bits;
        let mut cels = 
            self.stats.cells_stats.cells + self.stats.proof_stats.cells;
        let mut ints = 
            self.stats.cells_stats.internal_refs + self.stats.proof_stats.internal_refs;
        let mut exts = 
            self.stats.cells_stats.external_refs + self.stats.proof_stats.external_refs;
        if let Some(extra) = extra {
            bits += extra.cells_stats.bits;
            cels += extra.cells_stats.cells;
            ints += extra.cells_stats.internal_refs;
            exts += extra.cells_stats.external_refs;
        }
        let ret = 2000 + if extra.is_some() {
            200
        } else {
            0
        };
        ret + (bits >> 3) + cels * 12 + ints * 3 + exts * 4 + 
            self.accounts * 200 + 
            self.transactions * 200
    }

    pub fn fits(&self, level: ParamLimitIndex) -> bool {
        self.limits.fits(
            level, 
            self.estimate_block_size(None),
            self.gas_used,
            self.lt_delta()
        )
    }

    fn lt_delta(&self) -> u32 {
        self.lt_current.checked_sub(self.lt_start).unwrap_or(0) as u32
    }
/*
    pub fn dump_block_size(&self) {
        dbg!(self.stats.cells_stats.bits, self.stats.proof_stats.bits);
        dbg!(self.stats.cells_stats.cells, self.stats.proof_stats.cells);
        dbg!(self.stats.cells_stats.internal_refs, self.stats.proof_stats.internal_refs);
        dbg!(self.stats.cells_stats.external_refs, self.stats.proof_stats.external_refs);
        dbg!(self.accounts, self.transactions);
        dbg!(self.estimate_block_size(None));
    }
*/
}

#[derive(Default)]
struct Stats {
    bits: u32,
    cells: u32,
    internal_refs: u32,
    external_refs: u32
}

#[derive(Default)]
pub struct CellStorageStats {
    cells_seen: HashSet<UInt256>,
    cells_stats: Stats,
    proof_seen: HashSet<UInt256>,
    proof_stats: Stats
}

impl CellStorageStats {

    pub fn add_cell(&mut self, cell: &Cell) -> Result<()> {
        self.traverse(cell, true, false, None)
    }
    
    pub fn add_proof(&mut self, cell: &Cell, usage_tree: &UsageTree) -> Result<()> {
        self.traverse(cell, false, true, Some(usage_tree))
    }

    fn traverse(
        &mut self, 
        cell: &Cell, 
        mut cells_stat: bool, 
        mut proof_stat: bool,
        usage_tree: Option<&UsageTree>
    ) -> Result<()> {
/*
  if (cell.is_null()) {
    // FIXME: save error flag?
    return;
  }
*/
        if cells_stat {
            self.cells_stats.internal_refs += 1;          
            /* if (parent_ && parent_->seen_.count(cell->get_hash()) != 0) || */
            if !self.cells_seen.insert(cell.repr_hash()) {
                // This cell and its children had been already seen 
                cells_stat = false
            } else {
                self.cells_stats.cells += 1
            }
        }
/*
  if (need_stat) {
    stat_.internal_refs++;
    if ((parent_ && parent_->seen_.count(cell->get_hash()) != 0) || !seen_.insert(cell->get_hash()).second) {
      need_stat = false;
    } else {
      stat_.cells++;
    }
  }
*/

        if proof_stat {
            if Some(true) == usage_tree.map(|usage_tree| usage_tree.contains(&cell.repr_hash())) {
                self.proof_stats.external_refs += 1;          
                proof_stat = false;
            }
            /* auto tree_node = cell->get_tree_node();
            if (!tree_node.empty() && tree_node.is_from_tree(usage_tree_)) {
                proof_stat_.external_refs++;
                need_proof_stat = false;
            } else { */
            else {
                self.proof_stats.internal_refs += 1;          
                // if (parent_ && parent_->proof_seen_.count(cell->get_hash()) != 0) ||
                if !self.proof_seen.insert(cell.repr_hash()) {
                    // This cell and its children had been already seen 
                    proof_stat = false
                } else {
                    self.proof_stats.cells += 1
                }
            }
        }
/*
  if (need_proof_stat) {
    auto tree_node = cell->get_tree_node();
    if (!tree_node.empty() && tree_node.is_from_tree(usage_tree_)) {
      proof_stat_.external_refs++;
      need_proof_stat = false;
    } else {
      proof_stat_.internal_refs++;
      if ((parent_ && parent_->proof_seen_.count(cell->get_hash()) != 0) ||
          !proof_seen_.insert(cell->get_hash()).second) {
        need_proof_stat = false;
      } else {
        proof_stat_.cells++;
      }
    }
  }
*/

        if !cells_stat && !proof_stat {
            return Ok(())
        }
/*
  if (!need_proof_stat && !need_stat) {
    return;
  }
*/
   
        let bits = cell.bit_length() as u32;     
        if cells_stat {
            self.cells_stats.bits += bits
        }
        if proof_stat {
            self.proof_stats.bits += bits
        }
/*
  vm::CellSlice cs{vm::NoVm{}, std::move(cell)};
  if (need_stat) {
    stat_.bits += cs.size();
  }
  if (need_proof_stat) {
    proof_stat_.bits += cs.size();
  }
*/
        for i in 0..cell.references_count() {
            self.traverse(&cell.reference(i)?, cells_stat, proof_stat, usage_tree)?
        }
/*
  while (cs.size_refs()) {
    dfs(cs.fetch_ref(), need_stat, need_proof_stat);
  }
*/
        Ok(())

    } 

}
