use std::{
    time::Duration,
    sync::atomic::{AtomicU64, Ordering},
};

struct Query {
    total_queries: AtomicU64,
    total_fail: AtomicU64,
    total_time: AtomicU64,
    fail_time: AtomicU64,
    min_sucess_time: AtomicU64,
    max_sucess_time: AtomicU64,
    min_fail_time: AtomicU64,
    max_fail_time: AtomicU64,
    data: AtomicU64,
}

#[derive(Default)]
pub struct FullNodeNetworkTelemetry {
    queries: lockfree::map::Map<String, Query>
}

impl FullNodeNetworkTelemetry {
    pub fn consumed_query(
        &self,
        query: String,
        success: bool,
        time: Duration,
        data: usize,
    ) {
        let time = time.as_millis() as u64;
        adnl::common::add_object_to_map_with_update(
            &self.queries,
            query,
            |found| if let Some(found) = found {
                found.total_queries.fetch_add(1, Ordering::Relaxed);
                if !success {
                    found.total_fail.fetch_add(1, Ordering::Relaxed);
                    found.fail_time.fetch_add(time, Ordering::Relaxed);
                    found.min_fail_time.fetch_min(time, Ordering::Relaxed);
                    found.max_fail_time.fetch_max(time, Ordering::Relaxed);
                }
                found.total_time.fetch_add(time, Ordering::Relaxed);
                found.min_sucess_time.fetch_min(time, Ordering::Relaxed);
                found.max_sucess_time.fetch_max(time, Ordering::Relaxed);
                found.data.fetch_add(data as u64, Ordering::Relaxed);
                Ok(None)
            } else {
                let q = Query {
                    total_queries: AtomicU64::new(1),
                    total_fail: AtomicU64::new(if !success {1} else {0}),
                    total_time: AtomicU64::new(time),
                    fail_time: AtomicU64::new(if !success {time} else {0}),
                    min_sucess_time: AtomicU64::new(time),
                    max_sucess_time: AtomicU64::new(time),
                    min_fail_time: AtomicU64::new(if !success {time} else {u64::MAX}),
                    max_fail_time: AtomicU64::new(if !success {time} else {0}),
                    data: AtomicU64::new(data as u64),
                };
                Ok(Some(q))
            }
        ).expect("Can't return error");
    }

    pub fn report(&self, update_period_sec: u64) -> String {
        let mut report = string_builder::Builder::default();
        let mut total_data = 0;
        report.append(
            "                                   query       total       bytes  succeded   time: min   avg   max    failed    time: min   avg   max");
        for query in self.queries.iter() {

            let total_queries = query.val().total_queries.load(Ordering::Relaxed);
            if total_queries == 0 {
                report.append(format!("{} no one query", query.key())); // unexpected
                continue;
            }

            let total_fail = query.val().total_fail.load(Ordering::Relaxed);
            let total_time = query.val().total_time.load(Ordering::Relaxed);
            let fail_time = query.val().fail_time.load(Ordering::Relaxed);
            let min_sucess_time = query.val().min_sucess_time.load(Ordering::Relaxed);
            let max_sucess_time = query.val().max_sucess_time.load(Ordering::Relaxed);
            let min_fail_time = query.val().min_fail_time.load(Ordering::Relaxed);
            let max_fail_time = query.val().max_fail_time.load(Ordering::Relaxed);
            let data = query.val().data.load(Ordering::Relaxed);
            let total_succedded = total_queries - total_fail;
            let sucess_time = total_time - fail_time;
            total_data += data;

            if query.key().len() <= 40 {
                report.append(format!("\n{:>40}", query.key()));
            } else {
                report.append(format!(
                    "\n{}..{}", 
                    query.key().get(..8).unwrap_or_default(),
                    query.key().get(query.key().len() - 30..).unwrap_or_default(),
                ));
            }

            report.append(format!("  {:>10}  {:>10}", total_queries, data));
            if total_succedded > 0 {
                report.append(format!("{:>10}({:>3.0}%){:>6}{:>6}{:>6}",
                    total_succedded,
                    total_succedded as f64 / total_queries as f64 * 100_f64,
                    min_sucess_time,
                    if total_succedded > 0 { sucess_time / total_succedded } else { 0 },
                    max_sucess_time,
                ));
            } else {
                report.append("         0                        ");
            }
            if total_fail > 0 {
                report.append(format!("{:>10}({:>3.0}%) {:>6}{:>6}{:>6}", 
                    total_fail,
                    total_fail as f64 / total_queries as f64 * 100_f64,
                    min_fail_time,
                    if total_fail > 0 { fail_time / total_fail } else { 0 },
                    max_fail_time,
                ));
            } else {
                report.append("         0");
            }
            self.queries.remove(query.key());
        }
        report.append(format!("\nTotal: {} bytes", total_data));
        if update_period_sec != 0 {
            report.append(format!(", {} bytes/secons", total_data / update_period_sec));
        }

        report.string().expect("unexpected error while building full node service's telemetry report")
    }
}

