// code borrowed from https://github.com/metrics-rs/metrics/blob/f7434bd58527fdeda1a75344cdaed6ea0f7441cc/metrics-exporter-prometheus/src/recorder.rs
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Recorder, SharedString, Unit};
use metrics_util::MetricKindMask;
use metrics_util::registry::{AtomicStorage, GenerationalAtomicStorage, GenerationalStorage, Recency, Registry};

struct Inner {
    pub registry: Registry<Key, GenerationalAtomicStorage>,
    pub recency: Recency<Key>,
}

impl Inner {
    fn get_recent_metrics(&self) -> Snapshot {
        let mut counters = HashMap::new();
        let counter_handles = self.registry.get_counter_handles();
        for (key, counter) in counter_handles {
            let gen = counter.get_generation();
            if !self.recency.should_store_counter(&key, gen, &self.registry) {
                continue;
            }

            let name = key.name().to_string();
            let value = counter.get_inner().load(Ordering::Acquire);
            let entry =
                counters.entry(name).or_default();
            *entry = value;
        }

        let mut gauges = HashMap::new();
        let gauge_handles = self.registry.get_gauge_handles();
        for (key, gauge) in gauge_handles {
            let gen = gauge.get_generation();
            if !self.recency.should_store_gauge(&key, gen, &self.registry) {
                continue;
            }

            let name = key.name().to_string();
            let value = f64::from_bits(gauge.get_inner().load(Ordering::Acquire));
            let entry =
                gauges.entry(name).or_default();
            *entry = value;
        }

        let mut histograms = HashMap::new();
        let histogram_handles = self.registry.get_histogram_handles();
        for (key, histogram) in histogram_handles {
            let gen = histogram.get_generation();
            if !self.recency.should_store_histogram(&key, gen, &self.registry) {
                continue;
            }

            let name = key.name().to_string();
            let mut data = Vec::with_capacity(128);
            histogram.get_inner().clear_with(|hist| data.extend_from_slice(hist));
            let entry =
                histograms.entry(name).or_default();
            *entry = data.iter().map(|&x| x as u64).collect();
        }

        Snapshot { counters, gauges, histograms }
    }
}

pub struct MetricsRecorder {
    inner: Arc<Inner>,
}

impl MetricsRecorder {
    /// Gets a [`MetricsHandle`] to this recorder.
    pub fn handle(&self) -> MetricsHandle {
        MetricsHandle { inner: self.inner.clone() }
    }
}

impl From<Inner> for MetricsRecorder {
    fn from(inner: Inner) -> Self {
        MetricsRecorder { inner: Arc::new(inner) }
    }
}

impl Recorder for MetricsRecorder {
    fn describe_counter(&self, _key_name: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key_name: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(
        &self,
        _key_name: KeyName,
        _unit: Option<Unit>,
        _description: SharedString,
    ) {}

    fn register_counter(&self, key: &Key) -> Counter {
        self.inner.registry.get_or_create_counter(key, |c| c.clone().into())
    }

    fn register_gauge(&self, key: &Key) -> Gauge {
        self.inner.registry.get_or_create_gauge(key, |c| c.clone().into())
    }

    fn register_histogram(&self, key: &Key) -> Histogram {
        self.inner.registry.get_or_create_histogram(key, |c| c.clone().into())
    }
}

/// Handle for accessing metrics stored via [`MetricsRecorder`].
///
/// In certain scenarios, it may be necessary to directly handle requests that would otherwise be
/// handled directly by the HTTP listener, or push gateway background task.  [`MetricsHandle`]
/// allows rendering a snapshot of the current metrics stored by an installed [`MetricsRecorder`]
/// as a payload conforming to the Prometheus exposition format.
#[derive(Clone)]
pub struct MetricsHandle {
    inner: Arc<Inner>,
}

impl MetricsHandle {
    pub fn new(idle_timeout: Option<std::time::Duration>) -> Self {
        let inner = Inner {
            registry: Registry::new(GenerationalStorage::new(AtomicStorage)),
            recency: Recency::new(quanta::Clock::new(), MetricKindMask::ALL, idle_timeout)
        };
        let inner = Arc::new(inner);

        Self {
            inner
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        self.inner.get_recent_metrics()
    }

    pub fn sink(&self) -> MetricsRecorder {
        MetricsRecorder { inner: self.inner.clone() }
    }
}

pub struct Snapshot {
    pub counters: HashMap<String, u64>,
    pub gauges: HashMap<String, f64>,
    pub histograms: HashMap<String, Vec<u64>>,
}
