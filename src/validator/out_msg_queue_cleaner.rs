/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific EVERX DEV software governing permissions and
* limitations under the License.
*/

use std::{
    cell::{RefCell, RefMut},
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
    time::Instant,
};
use ever_block::OutMsgQueue;
use ever_block::{
    error, BuilderData, Cell, HashmapFilterResult, HashmapRemover, HashmapType, IBitstring,
    LabelReader, Result, SliceData,
};

/// * Cell = {label:remainder} = {label:lt:data_and_refs}
/// * key = {branch_key:label}
#[derive(Clone)]
pub struct HashmapNodeObj {
    key: SliceData,
    remainder: SliceData,
    lt: u64,
}

pub struct HashmapNodeParseResult {
    pub node_obj: HashmapNodeObj,
    bottom_bit_len: usize,
}

impl HashmapNodeObj {
    pub fn lt(&self) -> u64 {
        self.lt
    }
    pub fn key(&self) -> &SliceData {
        &self.key
    }
    pub fn key_hex(&self) -> String {
        hex::encode(self.key.storage())
    }
    /// Moves remainder on 64 bits and returns the rest
    pub fn data_and_refs(&self) -> Result<SliceData> {
        let mut remainder = self.remainder.clone();
        remainder.move_by(64)?;
        Ok(remainder)
    }
    pub fn parse_node_from_cell(
        node_cell: Cell,
        top_bit_len: usize,
        branch_key: BuilderData,
    ) -> Result<HashmapNodeParseResult> {
        let mut current_bit_len = top_bit_len;
        let mut cursor = SliceData::load_cell(node_cell)?;
        let key = LabelReader::read_label_raw(&mut cursor, &mut current_bit_len, branch_key)?;
        let key = SliceData::load_bitstring(key)?;
        let lt = read_u64_from(&cursor)?;

        let node_obj = HashmapNodeObj {
            key,
            remainder: cursor,
            lt,
        };

        Ok(HashmapNodeParseResult {
            node_obj,
            bottom_bit_len: current_bit_len,
        })
    }
}

impl PartialEq for HashmapNodeObj {
    fn eq(&self, other: &Self) -> bool {
        self.lt == other.lt && self.key == other.key
    }
}
impl Eq for HashmapNodeObj {}

impl Ord for HashmapNodeObj {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // first compare lt
        let mut cmp = self.lt.cmp(&other.lt);
        if cmp == std::cmp::Ordering::Equal {
            // check if we have full key and leaf
            cmp = self.key.remaining_bits().cmp(&other.key.remaining_bits());
            if cmp == std::cmp::Ordering::Equal {
                // compare hashes for leafs
                if self.key.remaining_bits() == 352 {
                    cmp = self.key.storage()[12..44].cmp(&other.key.storage()[12..44]);
                } else {
                    // compare full keys for other nodes
                    cmp = self.key.cmp(&other.key)
                }
            }
        }
        cmp.reverse()
    }
}
impl PartialOrd for HashmapNodeObj {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

type HashmapNodeRef = Rc<RefCell<HashmapNode>>;

#[derive(Clone)]
struct HashmapNode {
    /// Node is a root of hashmap
    is_root: bool,

    /// The length of a key before node. Is used to restore `branch_key` from the child node key.
    /// * branch_key = {parent_key:branch_direction_bit}
    branch_key_len: usize,

    /// The depth of the node key top
    ///             +-top_bit_len
    ///             |
    /// parent_key:0:label:remainder
    ///                   |
    ///                   +-bottom_bit_len
    top_bit_len: usize,

    /// The depth of the current node key bottom
    ///             +-top_bit_len
    ///             |
    /// parent_key:0:label:remainder
    ///                   |
    ///                   +-bottom_bit_len
    bottom_bit_len: usize,

    /// Node value Cell
    node_cell: Option<Cell>,

    /// Parsed node value HashmapNodeObj
    node_obj: HashmapNodeObj,

    /// * true - node alredy contains new value after processing
    node_updated: bool,

    /// true - node is a leaf and it was removed,
    /// or node is a fork and both child nodes were removed
    node_removed: bool,

    /// true - node is a leaf and it was processed,
    /// or node is a fork and it was processed after children
    is_processed: bool,

    parent: Option<HashmapNodeRef>,

    /// true - node is a right child in a fork
    is_right_child: bool,

    /// Child node with direction bit 0
    child_left: Option<HashmapNodeRef>,
    child_left_processed: bool,
    child_left_updated: bool,
    child_left_removed: bool,

    /// Child node with direction bit 1
    child_right: Option<HashmapNodeRef>,
    child_right_processed: bool,
    child_right_updated: bool,
    child_right_removed: bool,
}

impl PartialEq for HashmapNode {
    fn eq(&self, other: &Self) -> bool {
        let self_node_obj = self.node_obj_ref();
        let other_node_obj = other.node_obj_ref();
        self_node_obj == other_node_obj
    }
}
impl Eq for HashmapNode {}

impl Ord for HashmapNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_node_obj = self.node_obj_ref();
        let other_node_obj = other.node_obj_ref();
        self_node_obj.cmp(other_node_obj)
    }
}
impl PartialOrd for HashmapNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl HashmapNode {
    fn new(
        node_cell: Cell,
        top_bit_len: usize,
        branch_key: BuilderData,
        is_right_child: bool,
    ) -> Result<Self> {
        let is_root = branch_key.is_empty();
        let branch_key_len = branch_key.length_in_bits();
        let parse_res =
            HashmapNodeObj::parse_node_from_cell(node_cell.clone(), top_bit_len, branch_key)?;
        let res = Self {
            is_root,
            branch_key_len,
            top_bit_len,
            bottom_bit_len: parse_res.bottom_bit_len,
            node_cell: Some(node_cell),
            node_obj: parse_res.node_obj,
            node_updated: false,
            node_removed: false,
            is_processed: false,
            parent: None,
            is_right_child,
            child_left: None,
            child_left_processed: false,
            child_left_updated: false,
            child_left_removed: false,
            child_right: None,
            child_right_processed: false,
            child_right_updated: false,
            child_right_removed: false,
        };
        Ok(res)
    }

    fn bottom_bit_len(&self) -> usize {
        self.bottom_bit_len
    }

    fn node_obj_ref(&self) -> &HashmapNodeObj {
        &self.node_obj
    }

    fn node_cell_ref(&self) -> Result<&Cell> {
        match &self.node_cell {
            Some(cell) => Ok(cell),
            None => self
                .node_obj_ref()
                .remainder
                .cell_opt()
                .ok_or_else(|| error!("`node_obj.remainder` should contain Cell")),
        }
    }

    fn extract_node_cell(&mut self) -> Cell {
        match self.node_cell.take() {
            Some(cell) => cell,
            None => self.node_obj_ref().remainder.cell(),
        }
    }

    fn is_leaf(&self) -> bool {
        self.bottom_bit_len() == 0
    }

    /// Reads, parses and returns child nodes
    fn read_children(&self) -> Result<Option<Vec<HashmapNode>>> {
        // do not try to read children if node is a leaf
        if self.is_leaf() {
            return Ok(None);
        }

        // read and parse children
        let bit_len = self.bottom_bit_len() - 1;
        let node_obj = self.node_obj_ref();

        let mut children = Vec::with_capacity(2);
        for idx in 0..2 {
            let child_node_cell = node_obj.remainder.reference(idx)?;

            // put branch direction bit into key
            let is_right = idx == 1;
            let mut branch_key = node_obj.key().as_builder();
            branch_key.append_bit_bool(is_right)?;

            let child = HashmapNode::new(child_node_cell, bit_len, branch_key, is_right)?;

            children.push(child);
        }

        Ok(Some(children))
    }
}

#[derive(PartialEq, Eq)]
struct CursorPendingResultKey {
    bottom_bit_len: usize,
    key: SliceData,
}
impl Ord for CursorPendingResultKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // first compare bottom_bit_len
        let mut cmp = self.bottom_bit_len.cmp(&other.bottom_bit_len);
        if cmp == std::cmp::Ordering::Equal {
            // check if we have full key and leaf
            cmp = self.key.remaining_bits().cmp(&other.key.remaining_bits());
            if cmp == std::cmp::Ordering::Equal {
                // compare hashes for leafs
                if self.key.remaining_bits() == 352 {
                    cmp = self.key.storage()[12..44].cmp(&other.key.storage()[12..44]);
                } else {
                    // compare full keys for other nodes
                    cmp = self.key.cmp(&other.key)
                }
            }
            cmp = cmp.reverse();
        }
        cmp
    }
}
impl PartialOrd for CursorPendingResultKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct HashmapOrderedFilterCursor {
    root: HashmapNode,
    ordered_queue: BTreeSet<HashmapNode>,
    pending_results: BTreeMap<CursorPendingResultKey, HashmapNodeRef>,
    max_lt: u64,
    process_executed: bool,
    stop_processing: bool,
    stopped_by_max_lt: bool,
    stopped_by_time_limit: bool,
    cancel_processing: bool,
    processing_finished: bool,
    hops_counter: usize,

    processed_count: usize,
    removed_count: usize,

    time_limit_nanos: i128,

    timer: Instant,

    avg_time_handle_child_result: i128,
    handle_child_result_op_count: i128,
    avg_time_aggregate_children_results: i128,
    aggregate_children_results_op_count: i128,

    finalize_operations_count_forcast: i128,
}

#[derive(Debug, Hash, PartialEq, Eq)]
enum CursorTimerType {
    HandleChildResult,
    AgregateChildrenResults,
}

impl HashmapOrderedFilterCursor {
    pub fn with_root_cell(
        root_cell: Cell,
        root_bit_len: usize,
        max_lt: u64,
        time_limit_nanos: i128,
    ) -> Result<Self> {
        let root = HashmapNode::new(root_cell, root_bit_len, BuilderData::default(), false)?;
        Ok(Self {
            root: root.clone(),
            ordered_queue: BTreeSet::from([root]),
            pending_results: BTreeMap::new(),
            max_lt,
            process_executed: false,
            stop_processing: false,
            stopped_by_max_lt: false,
            stopped_by_time_limit: false,
            cancel_processing: false,
            processing_finished: false,
            hops_counter: 0,

            processed_count: 0,
            removed_count: 0,

            time_limit_nanos,

            timer: Instant::now(),

            avg_time_handle_child_result: 0,
            handle_child_result_op_count: 0,
            avg_time_aggregate_children_results: 0,
            aggregate_children_results_op_count: 0,

            finalize_operations_count_forcast: 0,
        })
    }

    fn sum_avg_time(&mut self, timer_type: CursorTimerType, elapsed_nanos: i128, log: bool) {
        match timer_type {
            CursorTimerType::HandleChildResult => {
                self.avg_time_handle_child_result = (self.avg_time_handle_child_result
                    * self.handle_child_result_op_count
                    + elapsed_nanos)
                    / (self.handle_child_result_op_count + 1);
                self.handle_child_result_op_count += 1;
                if log {
                    log::trace!(
                        "clean_out_msg_queue: hop {}: timer_type: {:?}, elapsed_nanos: {}, on ops count = {} avg = {}",
                        self.hops_counter, timer_type, elapsed_nanos,
                        self.handle_child_result_op_count, self.avg_time_handle_child_result,
                    );
                }
            }
            CursorTimerType::AgregateChildrenResults => {
                self.avg_time_aggregate_children_results = (self
                    .avg_time_aggregate_children_results
                    * self.aggregate_children_results_op_count
                    + elapsed_nanos)
                    / (self.aggregate_children_results_op_count + 1);
                self.aggregate_children_results_op_count += 1;
                if log {
                    log::trace!(
                        "clean_out_msg_queue: hop {}: timer_type: {:?}, elapsed_nanos: {}, on ops count = {} avg = {}",
                        self.hops_counter, timer_type, elapsed_nanos,
                        self.aggregate_children_results_op_count, self.avg_time_aggregate_children_results,
                    );
                }
            }
        }
    }
    fn get_avg_time(&self, timer_type: CursorTimerType) -> i128 {
        match timer_type {
            CursorTimerType::HandleChildResult => self.avg_time_handle_child_result,
            CursorTimerType::AgregateChildrenResults => self.avg_time_aggregate_children_results,
        }
    }

    fn check_time_left_for_finalization(&self) -> bool {
        let elapsed = self.timer.elapsed().as_nanos() as i128;
        let time_limit = self.time_limit_nanos;
        let avg_one_finalize_op_time = self.get_avg_time(CursorTimerType::HandleChildResult) * 3
            / 2
            + self.get_avg_time(CursorTimerType::AgregateChildrenResults) * 3 / 2;
        let time_to_finalize_forcast =
            avg_one_finalize_op_time * (self.finalize_operations_count_forcast + 4);
        let time_left = time_limit - elapsed;
        if time_left < time_to_finalize_forcast {
            log::debug!(
                "clean_out_msg_queue: hop {}: avg_time_handle_child_result = {}, avg_time_aggregate_children_results = {}",
                self.hops_counter,
                self.avg_time_handle_child_result,
                self.avg_time_aggregate_children_results,
            );

            log::debug!(
                "clean_out_msg_queue: hop {}: time_limit = {}, elapsed = {}, time_left = {}; avg_one_finalize_op_time = {}, finalize_operations_count_forcast = {}",
                self.hops_counter,
                time_limit, elapsed, time_left, avg_one_finalize_op_time,
                self.finalize_operations_count_forcast,
            );

            log::debug!(
                "clean_out_msg_queue: hop {}: will stop processing by timer: time left {} < {} time to finalize forcast",
                self.hops_counter,
                time_left, time_to_finalize_forcast,
            );

            true
        } else {
            false
        }
    }

    pub fn process_hashmap<F>(
        &mut self,
        timer: Instant,
        filter_func: F,
        log_prefix_opt: Option<String>,
    ) -> Result<(bool, Option<Cell>)>
    where
        F: FnMut(&HashmapNodeObj) -> Result<HashmapFilterResult>,
    {
        let log_prefix = log_prefix_opt.clone().unwrap_or_default();

        if self.process_executed {
            Err(error!(
                "unable to process hashmap again with current cursor"
            ))
        } else {
            self.process_executed = true;

            self.timer = timer;

            // start processing from root position
            let res = self.process(filter_func);

            log::debug!(
                "{}clean_out_msg_queue: hop {}: processed = {}, deleted = {}",
                log_prefix,
                self.hops_counter,
                self.processed_count,
                self.removed_count,
            );

            log::debug!(
                "{}clean_out_msg_queue: hop {}: stop_processing = {}, stopped_by_max_lt = {}, stopped_by_time_limit = {}, cancel_processing = {}",
                log_prefix,
                self.hops_counter,
                self.stop_processing,
                self.stopped_by_max_lt,
                self.stopped_by_time_limit,
                self.cancel_processing,
            );

            let elapsed = self.timer.elapsed().as_nanos();
            log::debug!(
                "{}clean_out_msg_queue: hop {}: queue cleaning finished in {} nanos",
                log_prefix,
                self.hops_counter,
                elapsed,
            );

            res
        }
    }

    fn process<F>(&mut self, mut filter_func: F) -> Result<(bool, Option<Cell>)>
    where
        F: FnMut(&HashmapNodeObj) -> Result<HashmapFilterResult>,
    {
        if self.ordered_queue.is_empty() {
            log::debug!(
                "clean_out_msg_queue: hop {}: queue is empty",
                self.hops_counter
            );
            self.processing_finished = true;
            return Ok((false, Some(self.root.extract_node_cell())));
        }

        // the number of attemts to calculate the avg children results aggregation time
        let mut aggregate_children_results_avg_time_calc_attempts = 0;

        // move to next position
        while let Some(mut next) = self.ordered_queue.pop_last() {
            self.hops_counter += 1;

            log::trace!(
                "clean_out_msg_queue: hop {}: processing position lt = {}, bottom_bit_len = {}, key = {}",
                self.hops_counter,
                next.node_obj_ref().lt(),
                next.bottom_bit_len(),
                next.node_obj_ref().key_hex(),
            );

            // filter leaf when cursor reached it
            if self.try_filter_current_leaf(&mut next, &mut filter_func)? {
                // exit processing when Cancel flag rised
                if self.cancel_processing {
                    log::trace!(
                        "clean_out_msg_queue: hop {}: Cancel flag rised, exit processing",
                        self.hops_counter,
                    );
                    return Ok((false, None));
                }

                // aggregate processing result into parent
                let parent = next.parent.take();
                let is_right_child = next.is_right_child;
                let child_updated = next.node_updated;
                let child_removed = next.node_removed;
                let next_ref_cell = Rc::new(RefCell::new(next));
                if let Some(root_ref_cell) = self.handle_child_result(
                    parent,
                    next_ref_cell,
                    child_updated,
                    child_removed,
                    is_right_child,
                )? {
                    let root_ref_cell = root_ref_cell.borrow();
                    if root_ref_cell.node_removed {
                        log::debug!(
                            "clean_out_msg_queue: hop {}: root node removed, will return empty cell",
                            self.hops_counter,
                        );
                        return Ok((true, None));
                    } else {
                        log::trace!(
                            "clean_out_msg_queue: hop {}: will return updated cell of root node",
                            self.hops_counter,
                        );
                        let cell = root_ref_cell.node_cell_ref()?.clone();
                        return Ok((root_ref_cell.node_updated, Some(cell)));
                    }
                }
            }
            // read and enqueue children for processing if Stop flag is not rised
            else if !self.stop_processing {
                // TODO: do not enqueue child with lower lt, process it immediately
                if let Some(children) = next.read_children()? {
                    // clone current node and children to emulate children results aggregation
                    // to calc avg time for this operation
                    if aggregate_children_results_avg_time_calc_attempts < 6 {
                        let mut cloned_child_left = children.get(0).cloned().unwrap();
                        cloned_child_left.is_processed = true;
                        cloned_child_left.node_updated = true;

                        let cloned_child_left_ref_cell =
                            Some(Rc::new(RefCell::new(cloned_child_left)));

                        let mut cloned_next = next.clone();
                        cloned_next.child_left_processed = true;
                        cloned_next.child_left_updated = true;

                        let aggregate_timer = Instant::now();

                        let cloned_next_ref_cell = Rc::new(RefCell::new(cloned_next));
                        let cloned_next_ref_mut = cloned_next_ref_cell.borrow_mut();

                        Self::aggregate_children_results(
                            cloned_next_ref_mut,
                            cloned_child_left_ref_cell,
                            None,
                            self.hops_counter,
                        )?;

                        // skip first operation time when calc avg because it is extremely big
                        if aggregate_children_results_avg_time_calc_attempts > 0 {
                            self.sum_avg_time(
                                CursorTimerType::AgregateChildrenResults,
                                aggregate_timer.elapsed().as_nanos() as i128,
                                true,
                            );
                        }

                        aggregate_children_results_avg_time_calc_attempts += 1;
                    }

                    let next_ref_cell = Rc::new(RefCell::new(next));
                    for mut child in children {
                        #[cfg(not(feature = "only_sorted_clean"))]
                        if child.node_obj_ref().lt() <= self.max_lt {
                            child.parent = Some(next_ref_cell.clone());
                            self.enqueue_next(child)?;
                        }
                        #[cfg(feature = "only_sorted_clean")]
                        {
                            child.parent = Some(next_ref_cell.clone());
                            self.enqueue_next(child)?;
                        }
                    }
                    // when we add children we will need to handle their results
                    // so we increase finalize operations counter
                    // to forecast the finalization time futher
                    self.finalize_operations_count_forcast += 1;
                }
            }

            // check the finalization time forecast
            // and rise the Stop flag when we can exceed the cleaning time limit
            if self.check_time_left_for_finalization() {
                self.stop_processing = true;
                self.stopped_by_time_limit = true;
            }

            // finalize processing when stop flag rised
            if self.stop_processing {
                log::debug!(
                    "clean_out_msg_queue: hop {}: Stop flag rised, finalizing processing",
                    self.hops_counter,
                );
                return self.finalize_processing();
            }
        }
        // sorted queue drained because we skipped nodes with lt > max_lt
        // so now we should finalize processing
        {
            self.stop_processing = true;
            self.stopped_by_max_lt = true;
            log::debug!(
                "clean_out_msg_queue: hop {}: Processing stopped because next nodes have lt > max_lt, finalizing processing",
                self.hops_counter,
            );
            return self.finalize_processing();
        }
    }

    fn enqueue_next(&mut self, next: HashmapNode) -> Result<()> {
        log::trace!(
            "clean_out_msg_queue: hop {}: current queue len {}, enqueueing child lt = {}, bottom_bit_len = {}, is_right = {}, key = {}",
            self.hops_counter,
            self.ordered_queue.len(),
            next.node_obj_ref().lt(),
            next.bottom_bit_len(),
            next.is_right_child,
            next.node_obj_ref().key_hex(),
        );
        let new_child_enqueued = self.ordered_queue.insert(next);
        if !new_child_enqueued {
            return Err(error!("node already exists in BTree!"));
        }
        Ok(())
    }

    fn try_filter_current_leaf<F>(
        &mut self,
        current: &mut HashmapNode,
        filter_func: &mut F,
    ) -> Result<bool>
    where
        F: FnMut(&HashmapNodeObj) -> Result<HashmapFilterResult>,
    {
        // stop pocessing when max_lt reached
        #[cfg(not(feature = "only_sorted_clean"))]
        if current.node_obj_ref().lt() > self.max_lt {
            log::debug!(
                "clean_out_msg_queue: hop {}: stop processing when current node (bottom_bit_len = {}, key = {}) lt {} > max_lt {}, elapsed = {} nanos",
                self.hops_counter,
                current.node_obj_ref().key_hex(),
                current.bottom_bit_len(),
                current.node_obj_ref().lt(),
                self.max_lt,
                self.timer.elapsed().as_nanos(),
            );
            self.stop_processing = true;
            self.stopped_by_max_lt = true;
            return Ok(false);
        }

        if !current.is_leaf() {
            return Ok(false);
        }

        log::trace!(
            "clean_out_msg_queue: hop {}: filtering current leaf lt = {}, bottom_bit_len = {}, processed = {}, key = {}",
            self.hops_counter,
            current.node_obj_ref().lt(),
            current.bottom_bit_len(),
            current.is_processed,
            current.node_obj_ref().key_hex(),
        );

        // skip if already processed
        if current.is_processed {
            log::trace!("clean_out_msg_queue: hop {}: skipped", self.hops_counter);
            return Ok(true);
        }

        // execute filtering
        match filter_func(current.node_obj_ref())? {
            HashmapFilterResult::Accept => {
                log::trace!(
                    "clean_out_msg_queue: hop {}: filter result = Accepted",
                    self.hops_counter
                );
                // do nothing
            }
            HashmapFilterResult::Remove => {
                log::trace!(
                    "clean_out_msg_queue: hop {}: filter result = Removed",
                    self.hops_counter
                );
                current.node_removed = true;

                self.removed_count += 1;
            }
            HashmapFilterResult::Stop => {
                log::trace!(
                    "clean_out_msg_queue: hop {}: filter result = Stop",
                    self.hops_counter
                );
                self.stop_processing = true;
            }
            HashmapFilterResult::Cancel => {
                log::trace!(
                    "clean_out_msg_queue: hop {}: filter result = Cancel",
                    self.hops_counter
                );
                self.cancel_processing = true;
            }
        }

        // stop pocessing when max_lt reached
        #[cfg(not(feature = "only_sorted_clean"))]
        if current.node_obj_ref().lt() == self.max_lt {
            log::debug!(
                "clean_out_msg_queue: hop {}: stop processing when current node (bottom_bit_len = {}, key = {}) lt {} == max_lt {}, elapsed = {} nanos",
                self.hops_counter,
                current.bottom_bit_len(),
                current.node_obj_ref().key_hex(),
                current.node_obj_ref().lt(),
                self.max_lt,
                self.timer.elapsed().as_nanos(),
            );

            self.stop_processing = true;
            self.stopped_by_max_lt = true;
        }

        current.is_processed = true;

        self.processed_count += 1;

        Ok(true)
    }

    fn finalize_processing(&mut self) -> Result<(bool, Option<Cell>)> {
        while let Some((pending_result_key, node_ref_cell)) = self.pending_results.pop_first() {
            self.hops_counter += 1;

            log::trace!(
                "clean_out_msg_queue: hop {}: processing pending result on the node bottom_bit_len = {}, key = {}",
                self.hops_counter,
                pending_result_key.bottom_bit_len,
                hex::encode(pending_result_key.key.storage()),
            );

            // handle children result for current node
            if let Some((parent, current_updated, current_removed, current_is_right_child)) = {
                let mut current_ref_mut = node_ref_cell.borrow_mut();

                if !current_ref_mut.is_processed {
                    let child_left_opt = current_ref_mut.child_left.take();
                    let child_right_opt = current_ref_mut.child_right.take();

                    current_ref_mut = Self::aggregate_children_results(
                        current_ref_mut,
                        child_left_opt,
                        child_right_opt,
                        self.hops_counter,
                    )?;

                    if current_ref_mut.is_root {
                        self.processing_finished = true;
                    }

                    Some((
                        current_ref_mut.parent.take(),
                        current_ref_mut.node_updated,
                        current_ref_mut.node_removed,
                        current_ref_mut.is_right_child,
                    ))
                } else {
                    log::trace!(
                        "clean_out_msg_queue: hop {}: already processed node bottom_bit_len = {}, key = {}",
                        self.hops_counter,
                        pending_result_key.bottom_bit_len,
                        hex::encode(pending_result_key.key.storage()),
                    );
                    None
                }
            } {
                if let Some(root_ref_cell) = self.handle_child_result(
                    parent,
                    node_ref_cell,
                    current_updated,
                    current_removed,
                    current_is_right_child,
                )? {
                    let root_ref_cell = root_ref_cell.borrow();
                    if root_ref_cell.node_removed {
                        log::trace!(
                            "clean_out_msg_queue: hop {}: root node removed, will return empty cell",
                            self.hops_counter,
                        );
                        return Ok((true, None));
                    } else {
                        log::trace!(
                            "clean_out_msg_queue: hop {}: will return updated cell of root node",
                            self.hops_counter,
                        );
                        let cell = root_ref_cell.node_cell_ref()?.clone();
                        return Ok((root_ref_cell.node_updated, Some(cell)));
                    }
                }
            }
        }

        // we can get here:
        // - if queue is empty
        // - if min lt > max processed lt and we didn't clean and all
        // so queue unchanged, return the original root node cell
        Ok((false, Some(self.root.extract_node_cell())))
    }

    fn handle_child_result(
        &mut self,
        current: Option<HashmapNodeRef>,
        child: HashmapNodeRef,
        child_updated: bool,
        child_removed: bool,
        is_right_child: bool,
    ) -> Result<Option<HashmapNodeRef>> {
        // start an exclusive timer for handle_child method
        let mut handle_child_elapsed = 0;
        let handle_child_timer = Instant::now();

        self.hops_counter += 1;

        if let Some(current) = current {
            let mut processed = false;
            let current_is_right_child;
            let mut current_updated = false;
            let mut current_removed = false;
            let (parent, pending_result_key) = {
                let mut current_ref_mut = current.borrow_mut();
                current_is_right_child = current_ref_mut.is_right_child;

                log::trace!(
                    "clean_out_msg_queue: hop {}: processing child result for node lt = {}, bottom_bit_len = {}, key = {}",
                    self.hops_counter,
                    current_ref_mut.node_obj_ref().lt(),
                    current_ref_mut.bottom_bit_len(),
                    current_ref_mut.node_obj_ref().key_hex(),
                );

                // process current node if both children processed, otherwise just save child result
                let children = if is_right_child {
                    current_ref_mut.child_right_processed = true;
                    current_ref_mut.child_right_updated = child_updated;
                    current_ref_mut.child_right_removed = child_removed;
                    let child_right = if !child_removed { Some(child) } else { None };
                    if current_ref_mut.child_left_processed {
                        Some((current_ref_mut.child_left.take(), child_right))
                    } else {
                        current_ref_mut.child_right = child_right;
                        None
                    }
                } else {
                    current_ref_mut.child_left_processed = true;
                    current_ref_mut.child_left_updated = child_updated;
                    current_ref_mut.child_left_removed = child_removed;
                    let child_left = if !child_removed { Some(child) } else { None };
                    if current_ref_mut.child_right_processed {
                        Some((child_left, current_ref_mut.child_right.take()))
                    } else {
                        current_ref_mut.child_left = child_left;
                        None
                    }
                };

                if let Some(children) = children {
                    // save current elapsed time for handle_child method
                    // handle_child_elapsed += handle_child_timer.elapsed().as_nanos() as i128;

                    // start new timer for the children results aggregation method
                    // let handle_children_timer = Instant::now();

                    self.hops_counter += 1;
                    current_ref_mut = Self::aggregate_children_results(
                        current_ref_mut,
                        children.0,
                        children.1,
                        self.hops_counter,
                    )?;

                    // calculate new avg for the children results aggregation method
                    // self.sum_avg_time_b(
                    //     CursorTimerType::AgregateChildrenResults,
                    //     handle_children_timer.elapsed().as_nanos() as i128,
                    //     false,
                    // );

                    // resume timer for handle_child method
                    // handle_child_timer = Instant::now();

                    current_updated = current_ref_mut.node_updated;
                    current_removed = current_ref_mut.node_removed;
                    if current_ref_mut.is_root {
                        self.processing_finished = true;
                    }
                    processed = true;

                    (
                        current_ref_mut.parent.take(),
                        CursorPendingResultKey {
                            bottom_bit_len: current_ref_mut.bottom_bit_len,
                            key: current_ref_mut.node_obj_ref().key().clone(),
                        },
                    )
                } else {
                    (
                        None,
                        CursorPendingResultKey {
                            bottom_bit_len: current_ref_mut.bottom_bit_len,
                            key: current_ref_mut.node_obj_ref().key().clone(),
                        },
                    )
                }
            };

            if processed {
                log::trace!(
                    "clean_out_msg_queue: hop {}: aggregating result to the parent node",
                    self.hops_counter,
                );

                {
                    log::trace!(
                        "clean_out_msg_queue: hop {}: remove current processed node from pending results cache bottom_bit_len = {}, key = {}",
                        self.hops_counter,
                        pending_result_key.bottom_bit_len,
                        hex::encode(pending_result_key.key.storage()),
                    );
                    // remove current node from results cache to not to handle on Stop futher
                    self.pending_results.remove(&pending_result_key);

                    // when we handled children results
                    // we can decrease finalize operations counter
                    self.finalize_operations_count_forcast -= 1;
                }

                // calculate new avg for handle_child method
                handle_child_elapsed += handle_child_timer.elapsed().as_nanos() as i128;
                self.sum_avg_time(
                    CursorTimerType::HandleChildResult,
                    handle_child_elapsed,
                    false,
                );

                self.handle_child_result(
                    parent,
                    current,
                    current_updated,
                    current_removed,
                    current_is_right_child,
                )
            } else {
                log::trace!(
                    "clean_out_msg_queue: hop {}: not both children processed, go to next node",
                    self.hops_counter,
                );

                {
                    log::trace!(
                        "clean_out_msg_queue: hop {}: saving to pending results cache current node bottom_bit_len = {}, key = {}",
                        self.hops_counter,
                        pending_result_key.bottom_bit_len,
                        hex::encode(pending_result_key.key.storage()),
                    );
                    // save current node to results cache to handle Stop futher
                    if let Some(existing) = self.pending_results.insert(pending_result_key, current)
                    {
                        let existing = existing.borrow();
                        return Err(error!(
                            "Pending results already contains node bottom_bit_len = {}, key = {}",
                            existing.bottom_bit_len,
                            existing.node_obj_ref().key_hex(),
                        ));
                    }
                }

                // calculate new avg for handle_child method
                handle_child_elapsed += handle_child_timer.elapsed().as_nanos() as i128;
                self.sum_avg_time(
                    CursorTimerType::HandleChildResult,
                    handle_child_elapsed,
                    false,
                );

                Ok(None)
            }
        } else {
            log::trace!(
                "clean_out_msg_queue: hop {}: child does not have parent, it is a root, return it",
                self.hops_counter,
            );
            Ok(Some(child))
        }
    }

    fn aggregate_children_results(
        mut current: RefMut<HashmapNode>,
        child_left: Option<HashmapNodeRef>,
        child_right: Option<HashmapNodeRef>,
        hops_counter: usize,
    ) -> Result<RefMut<HashmapNode>> {
        // remove current node if both children removed
        if current.child_left_removed && current.child_right_removed {
            log::trace!(
                "clean_out_msg_queue: hop {}: will remove current node lt = {}, bottom_bit_len = {}, key = {}",
                hops_counter,
                current.node_obj_ref().lt(),
                current.bottom_bit_len(),
                current.node_obj_ref().key_hex(),
            );
            current.node_removed = true;
        }
        // convert fork into edge if only one child removed
        else if current.child_left_removed || current.child_right_removed {
            log::trace!(
                "clean_out_msg_queue: hop {}: will transform into edge current node lt = {}, bottom_bit_len = {}, key = {}",
                hops_counter,
                current.node_obj_ref().lt(),
                current.bottom_bit_len(),
                current.node_obj_ref().key_hex(),
            );
            //TODO: use dictionary::make_var_edge(), needs to make ForkComponent public

            let branch_key_len = current.branch_key_len;
            let top_bit_len = current.top_bit_len;

            let child_opt = if current.child_left_removed {
                child_right
            } else {
                child_left
            };

            let children;
            let child_ref;

            let (child_node_key, child_node_reminder) = if let Some(child) = child_opt.as_ref() {
                child_ref = Some(child.borrow());
                let child_node_obj = child_ref.as_ref().unwrap().node_obj_ref();
                (child_node_obj.key(), &child_node_obj.remainder)
            } else {
                children = current.read_children()?;
                let children_ref = children.as_ref().unwrap();
                let child = if current.child_left_removed {
                    children_ref.get(1).unwrap()
                } else {
                    children_ref.get(0).unwrap()
                };
                let child_node_obj = child.node_obj_ref();
                (child_node_obj.key(), &child_node_obj.remainder)
            };

            let mut label = child_node_key.clone();
            label.move_by(branch_key_len)?;

            let builder = OutMsgQueue::make_cell_with_remainder(
                label,
                top_bit_len,
                child_node_reminder
            )?;

            let new_node_cell = builder.into_cell()?;

            let restored_branch_key = current.node_obj_ref().key().get_slice(0, branch_key_len)?;

            let parse_res = HashmapNodeObj::parse_node_from_cell(
                new_node_cell.clone(),
                top_bit_len,
                restored_branch_key.into_builder(),
            )?;

            current.node_cell = Some(new_node_cell);
            current.node_obj = parse_res.node_obj;
            current.bottom_bit_len = parse_res.bottom_bit_len;

            log::trace!(
                "clean_out_msg_queue: hop {}: current node updated lt = {}, bottom_bit_len = {}, key = {}",
                hops_counter,
                current.node_obj_ref().lt(),
                current.bottom_bit_len(),
                current.node_obj_ref().key_hex(),
            );

            current.node_updated = true;
        }
        // update current fork if both children not removed but at least one updated
        else if current.child_left_updated || current.child_right_updated {
            log::trace!(
                "clean_out_msg_queue: hop {}: will update fork for current node lt = {}, bottom_bit_len = {}, key = {}",
                hops_counter,
                current.node_obj_ref().lt(),
                current.bottom_bit_len(),
                current.node_obj_ref().key_hex(),
            );
            //TODO: use dictionary::make_var_edge(), needs to make ForkComponent public

            let branch_key_len = current.branch_key_len;
            let top_bit_len = current.top_bit_len;

            let curr_node_obj = current.node_obj_ref();

            let mut label = curr_node_obj.key().clone();
            label.move_by(branch_key_len)?;

            let (child_left_cell, child_right_cell) = {
                let child_left_cell = if let Some(child_left) = child_left {
                    let mut child_left_ref_mut = child_left.borrow_mut();
                    child_left_ref_mut.extract_node_cell()
                } else {
                    curr_node_obj.remainder.reference(0)?
                };
                let child_right_cell = if let Some(child_right) = child_right {
                    let mut child_right_ref_mut = child_right.borrow_mut();
                    child_right_ref_mut.extract_node_cell()
                } else {
                    curr_node_obj.remainder.reference(1)?
                };
                (child_left_cell, child_right_cell)
            };

            let (builder, _cursor) = OutMsgQueue::make_fork(
                &label,
                top_bit_len,
                child_left_cell,
                child_right_cell,
                false,
            )?;

            let new_node_cell = builder.into_cell()?;

            let restored_branch_key = curr_node_obj.key().get_slice(0, branch_key_len)?;

            let parse_res = HashmapNodeObj::parse_node_from_cell(
                new_node_cell.clone(),
                top_bit_len,
                restored_branch_key.into_builder(),
            )?;

            current.node_cell = Some(new_node_cell);
            current.node_obj = parse_res.node_obj;
            current.bottom_bit_len = parse_res.bottom_bit_len;

            log::trace!(
                "clean_out_msg_queue: hop {}: current node updated lt = {}, bottom_bit_len = {}, key = {}",
                hops_counter,
                current.node_obj_ref().lt(),
                current.bottom_bit_len(),
                current.node_obj_ref().key_hex(),
            );

            current.node_updated = true;
        } else {
            log::trace!(
                "clean_out_msg_queue: hop {}: nothing changed for current node lt = {}, bottom_bit_len = {}, key = {}",
                hops_counter,
                current.node_obj_ref().lt(),
                current.bottom_bit_len(),
                current.node_obj_ref().key_hex(),
            );
        }
        current.is_processed = true;

        Ok(current)
    }
}

fn read_u64_from(cursor: &SliceData) -> Result<u64> {
    let mut value: u64 = 0;
    for i in 0..8 {
        value |= (cursor.get_byte(8 * i)? as u64) << (8 * (7 - i));
    }
    Ok(value)
}

/// returns true if partial clean performed
pub fn hashmap_filter_ordered_by_lt_hash<F>(
    queue: &mut OutMsgQueue,
    max_lt: u64,
    time_limit_nanos: i128,
    filter_func: F,
    log_prefix_opt: Option<String>,
) -> Result<bool>
where
    F: FnMut(&HashmapNodeObj) -> Result<HashmapFilterResult>,
{
    let log_prefix = log_prefix_opt.clone().unwrap_or_default();
    let cleaning_timer = std::time::Instant::now();

    let mut filter_cursor = HashmapOrderedFilterCursor::with_root_cell(
        queue.data().unwrap().clone(),
        queue.bit_len(),
        max_lt,
        time_limit_nanos,
    )?;

    let cursor_creation_elapsed = cleaning_timer.elapsed().as_nanos();

    let (updated, cell) =
        filter_cursor.process_hashmap(cleaning_timer, filter_func, log_prefix_opt)?;
    if updated {
        log::debug!("{}updating queue root cell", log_prefix);
        *queue.data_mut() = cell;
        queue.after_remove()?;
    }

    log::debug!(
        "{}...including cursor creation time = {} nanos",
        log_prefix,
        cursor_creation_elapsed,
    );

    let partial = filter_cursor.stop_processing | filter_cursor.cancel_processing;

    Ok(partial)
}
