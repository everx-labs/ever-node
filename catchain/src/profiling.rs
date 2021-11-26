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

pub use super::*;

use metrics_runtime;

/*
    Instances counter
*/

pub struct InstanceCounter {
    create_counter: metrics_runtime::data::Counter,
    drop_counter: metrics_runtime::data::Counter,
    need_drop: bool,
}

impl InstanceCounter {
    pub fn new(receiver: &metrics_runtime::Receiver, key: &String) -> Self {
        let body = Self {
            create_counter: receiver.sink().counter(format!("{}.create", &key)),
            drop_counter: receiver.sink().counter(format!("{}.drop", &key)),
            need_drop: false,
        };

        body
    }

    pub fn clone_refs_only(&self) -> Self {
        let body = Self {
            create_counter: self.create_counter.clone(),
            drop_counter: self.drop_counter.clone(),
            need_drop: false,
        };

        body
    }

    pub fn force_drop(&mut self) {
        if self.need_drop {
            self.drop_counter.increment();
            self.need_drop = false;
        }
    }
}

impl Clone for InstanceCounter {
    fn clone(&self) -> Self {
        let mut body = self.clone_refs_only();

        body.need_drop = true;
        body.create_counter.increment();

        body
    }
}

impl Drop for InstanceCounter {
    fn drop(&mut self) {
        self.force_drop();
    }
}

/*
    Result status counter
*/

#[derive(Clone)]
pub struct ResultStatusCounter {
    total_counter: metrics_runtime::data::Counter,
    success_counter: metrics_runtime::data::Counter,
    failure_counter: metrics_runtime::data::Counter,
}

impl ResultStatusCounter {
    pub fn new(receiver: &metrics_runtime::Receiver, key: &String) -> Self {
        let body = Self {
            total_counter: receiver.sink().counter(format!("{}.total", &key)),
            success_counter: receiver.sink().counter(format!("{}.success", &key)),
            failure_counter: receiver.sink().counter(format!("{}.failure", &key)),
        };

        body
    }

    pub fn total_increment(&self) {
        self.total_counter.increment();
    }

    pub fn success(&self) {
        self.success_counter.increment();
    }

    pub fn failure(&self) {
        self.failure_counter.increment();
    }
}

/*
    Builtin profiler
*/

/// Code line description
pub struct ProfileCodeLine {
    pub file_name: &'static str,     //file name
    pub line: u32,                   //line number
    pub function_name: &'static str, //name of function
}

/// Span for profiling metrics gathering (like function call in a callstack)
#[derive(Clone, Copy)]
struct ProfilerSpan {
    parent: i64,                         //parent span index in the array of spans
    first: i64,                          //first child of this span
    next: i64,                           //next child on the same sibling level
    code_line: &'static ProfileCodeLine, //corresponding code line
    duration: u128,                      //duration of execution in nanoseconds
}

/// Profiler
pub struct Profiler {
    spans: Vec<ProfilerSpan>, //array of spans for the profiler
    current_span: usize,      //index of current span in a callstack
}

impl Profiler {
    /// Get local current thread instance
    pub fn local_instance() -> &'static std::thread::LocalKey<RefCell<Profiler>> {
        thread_local!(static INSTANCE: RefCell<Profiler> = RefCell::new(Profiler::new()));
        &INSTANCE
    }

    /// Get global instance of the profiler (for merging from local instance to global)
    pub fn global_instance() -> std::sync::Arc<std::sync::Mutex<Profiler>> {
        lazy_static! {
            pub static ref INSTANCE: std::sync::Arc<std::sync::Mutex<Profiler>> =
                std::sync::Arc::new(std::sync::Mutex::<Profiler>::new(Profiler::new()));
        }
        INSTANCE.clone()
    }

    /// New profiler creation
    fn new() -> Self {
        lazy_static! {
            pub static ref ROOT_CODE_LINE: ProfileCodeLine = ProfileCodeLine {
                file_name: "",
                function_name: "<root>",
                line: 0,
            };
        }

        const SPANS_POOL_SIZE: usize = 1000;

        let mut profiler = Profiler {
            spans: Vec::with_capacity(SPANS_POOL_SIZE),
            current_span: 0,
        };

        profiler.spans.push(ProfilerSpan {
            parent: -1,
            next: -1,
            first: -1,
            code_line: &ROOT_CODE_LINE,
            duration: 0,
        });

        profiler
    }

    /// Start span execution
    fn enter_span(code_line: &'static ProfileCodeLine) {
        Self::local_instance().with(|profiler| profiler.borrow_mut().enter_span_impl(code_line));
    }

    /// Finish span execution with metrics reporting
    fn exit_span(duration: u128) {
        Self::local_instance().with(|profiler| profiler.borrow_mut().exit_span_impl(duration));
    }

    /// Span accessing / creation (returns index of span)
    fn get_span(&mut self, code_line: &'static ProfileCodeLine) -> usize {
        let parent = &self.spans[self.current_span as usize];
        let mut span_index = parent.first;
        let mut last_span_index = -1;

        while span_index != -1 {
            if (self.spans[span_index as usize].code_line as *const ProfileCodeLine)
                == (code_line as *const ProfileCodeLine)
            {
                return span_index as usize;
            }

            last_span_index = span_index;
            span_index = self.spans[span_index as usize].next;
        }

        self.spans.push(ProfilerSpan {
            parent: self.current_span as i64,
            next: -1,
            first: -1,
            code_line: code_line,
            duration: 0,
        });

        let new_span_index = (self.spans.len() - 1) as i64;

        if last_span_index != -1 {
            assert!(self.spans[last_span_index as usize].next == -1);
            self.spans[last_span_index as usize].next = new_span_index;
        } else {
            let parent = &mut self.spans[self.current_span as usize];

            assert!(parent.first == -1);

            parent.first = new_span_index;
        }

        new_span_index as usize
    }

    fn enter_span_impl(&mut self, code_line: &'static ProfileCodeLine) {
        let span_index = self.get_span(code_line);

        self.current_span = span_index;
    }

    fn exit_span_impl(&mut self, duration: u128) {
        assert!(self.current_span != 0);

        let span = &mut self.spans[self.current_span];

        span.duration += duration;
        self.current_span = span.parent as usize;
    }

    /// Profiler spans accumulation from this instance to global instance
    pub fn accumulate_to_global_instance(&mut self) {
        if let Ok(mut instance) = Self::global_instance().lock() {
            instance.accumulate(self);
        }
    }

    /// Accumulate metrics between profilers
    fn accumulate(&mut self, src_profiler: &mut Profiler) {
        self.accumulate_span(src_profiler, 0, 0);
    }

    /// Recursive implementation of spans accumulation in a profiler
    fn accumulate_span(
        &mut self,
        src_profiler: &mut Profiler,
        src_span_index: usize,
        dst_span_index: usize,
    ) {
        let src_span = &mut src_profiler.spans[src_span_index];
        let dst_span = &mut self.spans[dst_span_index];

        assert!(
            (src_span.code_line as *const ProfileCodeLine)
                == (dst_span.code_line as *const ProfileCodeLine)
        );

        dst_span.duration += src_span.duration;
        src_span.duration = 0;

        let mut src_child_span_index = src_span.first;

        while src_child_span_index != -1 {
            let dst_child_span_index =
                self.get_span(src_profiler.spans[src_child_span_index as usize].code_line);

            self.accumulate_span(
                src_profiler,
                src_child_span_index as usize,
                dst_child_span_index,
            );

            src_child_span_index = src_profiler.spans[src_child_span_index as usize].next;
        }
    }

    /// Clone profiler data
    pub fn clone(&self) -> Profiler {
        Profiler {
            spans: self.spans.clone(),
            current_span: self.current_span,
        }
    }

    /// Dump profiler metrics
    pub fn dump(&self) -> String {
        let mut self_time_spans: Vec<(usize, u128)> = Vec::new();

        let result = self.dump_span(0, 0, 0, &mut self_time_spans);

        self_time_spans.sort_by(|a, b| a.1.cmp(&b.1));

        let result = format!(
            "{}\n{}",
            result,
            self.dump_spans_self_time(&self_time_spans)
        );

        result
    }

    /// Dump spans self time
    fn dump_spans_self_time(&self, self_time_spans: &Vec<(usize, u128)>) -> String {
        let mut total_duration = 0;

        for (_, self_duration) in self_time_spans.iter() {
            total_duration += self_duration;
        }

        let mut result = format!("Self time:");

        for (span_index, self_duration) in self_time_spans.iter().rev() {
            let percentage = (*self_duration as f64) / (total_duration as f64) * 100.0;

            const MIN_DUMP_PERCENTAGE: f64 = 1.0;
            const MIN_DURATION: u128 = 1000;

            if *self_duration < MIN_DURATION || percentage < MIN_DUMP_PERCENTAGE {
                result = format!("{}\n  <{:.1}% - ...", result, MIN_DUMP_PERCENTAGE);
                break;
            }

            let span = &self.spans[*span_index];

            result = format!(
                "{}\n  {:.1}% - {}ns - {} ({}({}))",
                result,
                percentage,
                self_duration,
                span.code_line.function_name,
                span.code_line.file_name,
                span.code_line.line
            );
        }

        result
    }

    /// Dump span metrics
    fn dump_span(
        &self,
        parent_span_index: usize,
        depth: usize,
        mut root_total_duration: u128,
        self_time_spans: &mut Vec<(usize, u128)>,
    ) -> String {
        let indent = String::from_utf8(vec![b' '; (depth + 1) * 2])
            .unwrap()
            .to_string();
        let parent_span = &self.spans[parent_span_index];
        let mut child_span_index = parent_span.first;
        let mut sorted_spans = Vec::new();
        let mut total_children_duration = 0;

        while child_span_index != -1 {
            let child_span = &self.spans[child_span_index as usize];
            let child_duration = child_span.duration;

            sorted_spans.push((child_span_index, child_duration));

            total_children_duration += child_duration;
            child_span_index = child_span.next;
        }

        let total_duration = if parent_span_index == 0 {
            total_children_duration
        } else {
            parent_span.duration
        };
        let self_duration = total_duration - total_children_duration;

        self_time_spans.push((parent_span_index, self_duration));

        if sorted_spans.len() > 0 {
            sorted_spans.push((parent_span_index as i64, self_duration));
        }

        sorted_spans.sort_by(|a, b| a.1.cmp(&b.1));

        if parent_span_index == 0 {
            root_total_duration = total_duration;
        }

        let mut result = format!(
            "{}ns - {} ({}({}))",
            total_duration,
            parent_span.code_line.function_name,
            parent_span.code_line.file_name,
            parent_span.code_line.line
        );

        for (child_span_index, child_total_duration) in sorted_spans.iter().rev() {
            let percentage = (*child_total_duration as f64) / (total_duration as f64) * 100.0;
            let total_percentage =
                (*child_total_duration as f64) / (root_total_duration as f64) * 100.0;

            const MIN_DUMP_PERCENTAGE: f64 = 1.0;
            const MIN_DURATION: u128 = 1000;

            if *child_total_duration < MIN_DURATION || percentage < MIN_DUMP_PERCENTAGE {
                result = format!("{}\n{}<{:.1}% - ...", result, indent, MIN_DUMP_PERCENTAGE);
                break;
            }

            if *child_span_index as usize != parent_span_index {
                let child_result = self.dump_span(
                    *child_span_index as usize,
                    depth + 1,
                    root_total_duration,
                    self_time_spans,
                );

                result = format!(
                    "{}\n{}{:.1}% / {:.1}% - {}",
                    result, indent, percentage, total_percentage, child_result
                );
            } else {
                result = format!(
                    "{}\n{}{:.1}% / {:.1}% - {}ns - SELF/OTHER",
                    result, indent, percentage, total_percentage, parent_span.duration
                );
            }
        }

        result
    }
}

/// Guard for profiler call
pub struct ProfileGuard {
    start_time: std::time::Instant, //timestamp of guard creation
}

impl ProfileGuard {
    pub fn new(code_line: &'static ProfileCodeLine) -> Self {
        let body = Self {
            start_time: std::time::Instant::now(),
        };

        Profiler::enter_span(code_line);

        body
    }
}

impl Drop for ProfileGuard {
    fn drop(&mut self) {
        let duration = self.start_time.elapsed().as_nanos();

        Profiler::exit_span(duration);
    }
}

/// Guard for execution time warnings
pub struct ExecutionTimeGuard {
    start_time: std::time::Instant,      //timestamp of guard creation
    code_line: &'static ProfileCodeLine, //code line
    max_duration: &'static std::time::Duration, //max execution duration
}

impl ExecutionTimeGuard {
    pub fn new(
        code_line: &'static ProfileCodeLine,
        max_duration: &'static std::time::Duration,
    ) -> Self {
        let body = Self {
            start_time: std::time::Instant::now(),
            code_line: code_line,
            max_duration: max_duration,
        };

        body
    }
}

impl Drop for ExecutionTimeGuard {
    fn drop(&mut self) {
        let duration = self.start_time.elapsed();

        if duration < *self.max_duration {
            return;
        }

        warn!(
            "Execution time {:.3}ms is greater than expected time {:.3}ms for {} at {}({})",
            duration.as_secs_f64() * 1000.0,
            self.max_duration.as_secs_f64() * 1000.0,
            self.code_line.function_name,
            self.code_line.file_name,
            self.code_line.line
        );
    }
}

/// Macro for current function name extraction
#[macro_export]
macro_rules! function {
    ($prefix_len:expr) => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        &name[1..name.len() - $prefix_len]
    }};
}

/// Macro for profiler guard creation
#[macro_export]
macro_rules! instrument {
    () => {
        lazy_static! {
            static ref INSTRUMENT_CODE_LINE: super::catchain::profiling::ProfileCodeLine =
                super::catchain::profiling::ProfileCodeLine {
                    file_name: file!(),
                    function_name: super::catchain::profiling::function!(85),
                    line: line!(),
                };
        }

        let _instrument_guard =
            super::catchain::profiling::ProfileGuard::new(&INSTRUMENT_CODE_LINE);
    };
}

/// Macro for execution time warning checks
#[macro_export]
macro_rules! check_execution_time {
    ($max_duration_microseconds:expr) => {
        lazy_static! {
            static ref CHECK_EXECUTION_TIME_CODE_LINE: super::catchain::profiling::ProfileCodeLine =
                super::catchain::profiling::ProfileCodeLine {
                    file_name: file!(),
                    function_name: super::catchain::profiling::function!(95),
                    line: line!(),
                };
            static ref CHECK_EXECUTION_TIME_MAX_DURATION: std::time::Duration =
                std::time::Duration::from_micros($max_duration_microseconds);
        }

        let _check_execution_time_guard = super::catchain::profiling::ExecutionTimeGuard::new(
            &CHECK_EXECUTION_TIME_CODE_LINE,
            &CHECK_EXECUTION_TIME_MAX_DURATION,
        );
    };
}
