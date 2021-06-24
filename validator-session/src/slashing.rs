use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;

/// Public key hash
pub type PublicKeyHash = ::catchain::PublicKeyHash;

/// Public key
pub type PublicKey = ::catchain::PublicKey;

/// Session statistics metric
#[derive(Clone, Debug)]
#[repr(usize)]
pub enum Metric {
    /// Total rounds count
    TotalRoundsCount,

    /// Total collation rounds count
    TotalCollationRoundsCount,

    /// Total approved collations count
    CollationsCount,

    /// Total approvals count
    ApprovalsCount,

    /// Commits count
    CommitsCount,

    /// Number of metrics
    MetricsCount,
}

/// Session statistics entry
#[derive(Clone)]
pub struct Node {
    /// Node public key
    pub public_key: PublicKey,

    /// Statistics metrics
    pub metrics: [usize; Metric::MetricsCount as usize],
}

impl Node {
    /// New statistics entry
    pub fn new(public_key: &PublicKey) -> Self {
        Self {
            public_key: public_key.clone(),
            metrics: [0; Metric::MetricsCount as usize],
        }
    }

    /// Reset statistics
    pub fn reset(&mut self) {
        for metric in &mut self.metrics {
            *metric = 0;
        }
    }

    /// Merge metrics
    pub fn merge(&mut self, entry: &Node) {
        for it in entry.metrics.iter().zip(self.metrics.iter_mut()) {
            let (src_it, dst_it) = it;

            *dst_it += *src_it;
        }
    }

    /// Increment metric
    pub fn increment(&mut self, metric_id: Metric, value: usize) {
        self.metrics[metric_id as usize] += value;
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node(public_key_hash={}, metrics={:?})", self.public_key.id(), self.metrics)
    }
}

/// Session statistics
#[derive(Clone, Debug, Default)]
pub struct ValidatorStat {
    /// Statistics entries for each validator
    pub validators_stat: HashMap<PublicKeyHash, Node>,
}

impl ValidatorStat {
    /// Create new statistics
    pub fn new() -> Self {
        Self {
            validators_stat: HashMap::new(),
        }
    }

    /// Clear all statistics and reset
    pub fn clear(&mut self) {
        self.validators_stat.clear();
    }

    /// Reset statistics
    pub fn reset(&mut self) {
        for (_, entry) in self.validators_stat.iter_mut() {
            entry.reset();
        }
    }

    /// Merge statistics
    pub fn merge(&mut self, stat: &ValidatorStat) {
        for (pub_key_hash, stat) in stat.validators_stat.iter() {
            self.merge_entry(pub_key_hash, stat);
        }
    }

    /// Merge entry
    pub fn merge_entry(&mut self, pub_key_hash: &PublicKeyHash, stat: &Node) {
        if let Some(ref mut self_stat) = self.validators_stat.get_mut(pub_key_hash) {
            self_stat.merge(stat);
        } else {
            self.validators_stat
                .insert(pub_key_hash.clone(), stat.clone());
        }
    }

    /// Aggregate metrics
    pub fn aggregate(&self, confidence_z: f64) -> AggregatedValidatorStat {
        AggregatedValidatorStat::new(self, confidence_z)
    }
}

/// Session statistics aggregated metric
#[derive(Clone, Debug)]
#[repr(usize)]
pub enum AggregatedMetric {
    /// Collations participation
    CollationsParticipation,

    /// Approvals participation
    ApprovalsParticipation,

    /// Commits participation
    CommitsParticipation,

    /// Number of metrics
    AggregatedMetricsCount,
}

/// Aggregated validator session metric
#[derive(Clone)]
pub struct AggregatedNode {
    /// Node public key
    pub public_key: PublicKey,

    /// Aggregated statistics metrics
    pub metrics: [f64; AggregatedMetric::AggregatedMetricsCount as usize],
}

impl AggregatedNode {
    /// Create new aggregated entry
    pub fn new(entry: &Node) -> Self {
        let mut result = Self {
            public_key: entry.public_key.clone(),
            metrics: [0.0; AggregatedMetric::AggregatedMetricsCount as usize],
        };

        use AggregatedMetric::*;
        use Metric::*;

        let rounds_count = entry.metrics[TotalRoundsCount as usize] as f64;
        let collation_rounds_count = entry.metrics[TotalCollationRoundsCount as usize] as f64;

        if collation_rounds_count > 0.0 {
            result.metrics[CollationsParticipation as usize] =
                (entry.metrics[CollationsCount as usize] as f64) / collation_rounds_count;
        }

        if rounds_count > 0.0 {
            result.metrics[ApprovalsParticipation as usize] =
                (entry.metrics[ApprovalsCount as usize] as f64) / rounds_count;
            result.metrics[CommitsParticipation as usize] =
                (entry.metrics[CommitsCount as usize] as f64) / rounds_count;
        }

        result
    }
}

impl fmt::Debug for AggregatedNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AggregatedNode(public_key_hash={}, metrics={:?})", self.public_key.id(), self.metrics)
    }
}

/// Aggeraged metric parameters
#[derive(Clone, Copy, Debug, Default)]
pub struct AggregatedMetricParams {
    /// Expected value
    pub expected_value: f64,

    /// Standard deviation
    pub standard_deviation: f64,

    /// Min confidence value
    pub min_confidence_value: f64,

    /// Max confidence value
    pub max_confidence_value: f64,
}

/// Slashed validator
#[derive(Clone)]
pub struct SlashedNode {
    /// Public key of validator
    pub public_key: PublicKey,

    /// Slashed metric
    pub metric_id: AggregatedMetric,
}

impl fmt::Debug for SlashedNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SlashedNode(public_key_hash={}, metric_id={:?})", self.public_key.id(), self.metric_id)
    }
}

/// Session aggregated statistics
#[derive(Clone, Debug)]
pub struct AggregatedValidatorStat {
    /// Aggregated statistics entries for each validator
    validators_stat: HashMap<PublicKeyHash, AggregatedNode>,

    /// Aggregated metric params (like expected value, standard deviation)
    metrics_params: [AggregatedMetricParams; AggregatedMetric::AggregatedMetricsCount as usize],

    /// Slashed validators list
    slashed_validators: Vec<SlashedNode>,
}

impl AggregatedValidatorStat {
    /// Create new aggregated statistics
    pub fn new(stat: &ValidatorStat, confidence_z: f64) -> Self {
        let validators_stat: HashMap<PublicKeyHash, AggregatedNode> = stat
            .validators_stat
            .clone()
            .into_iter()
            .map(|(pub_key_hash, entry)| (pub_key_hash, AggregatedNode::new(&entry)))
            .collect();
        let metrics_params: Vec<AggregatedMetricParams> = (0
            ..AggregatedMetric::AggregatedMetricsCount as usize)
            .map(|metric| Self::compute_params_for_metric(&validators_stat, metric, confidence_z))
            .collect();
        let mut slashed_validators = Vec::new();

        for (_pub_key_hash, entry) in &validators_stat {
            for (metric_id, metric_norm) in metrics_params.iter().enumerate() {
                if entry.metrics[metric_id] >= metric_norm.min_confidence_value {
                    continue;
                }

                slashed_validators.push(SlashedNode {
                    public_key: entry.public_key.clone(),
                    metric_id: unsafe { ::std::mem::transmute(metric_id) },
                });
            }
        }

        Self {
            metrics_params: metrics_params.as_slice().try_into().unwrap(),
            validators_stat: validators_stat,
            slashed_validators: slashed_validators,
        }
    }

    /// Compute stat params for metric
    fn compute_params_for_metric(
        stat: &HashMap<PublicKeyHash, AggregatedNode>,
        metric_id: usize,
        z: f64,
    ) -> AggregatedMetricParams {
        let mut result = AggregatedMetricParams::default();

        if stat.len() == 0 {
            return result;
        }

        result.expected_value = stat
            .iter()
            .map(|(_, entry)| entry.metrics[metric_id])
            .sum::<f64>()
            / (stat.len() as f64);
        result.standard_deviation = f64::sqrt(
            stat.iter()
                .map(|(_, entry)| {
                    let x = entry.metrics[metric_id] - result.expected_value;
                    x * x
                })
                .sum::<f64>()
                / (stat.len() as f64),
        );

        //confidence interval computation
        //see https://en.wikipedia.org/wiki/Confidence_interval#Basic_steps for details

        let delta = z * result.standard_deviation / f64::sqrt(stat.len() as f64);

        result.min_confidence_value = result.expected_value - delta;
        result.max_confidence_value = result.expected_value + delta;

        result
    }

    /// Slashed validators
    pub fn get_slashed_validators(&self) -> &Vec<SlashedNode> {
        &self.slashed_validators
    }

    /// Get aggregated node params
    pub fn get_aggregated_node(
        &self,
        node_public_key_hash: &PublicKeyHash,
    ) -> Option<&AggregatedNode> {
        for (key, node) in &self.validators_stat {
            if key == node_public_key_hash {
                return Some(node);
            }
        }

        None
    }
}
