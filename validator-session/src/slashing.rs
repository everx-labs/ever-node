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

use std::collections::HashMap;
// use std::convert::TryInto;
use std::fmt;

/// Public key hash
pub type PublicKeyHash = ::catchain::PublicKeyHash;

/// Public key
pub type PublicKey = ::catchain::PublicKey;

/// Slashing params
pub type SlashingConfig = ::ton_block::SlashingConfig;

/// Session statistics metric
#[derive(Clone, Copy, Debug)]
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

    /// Total rounds count (computed on apply level)
    ApplyLevelTotalBlocksCount,

    /// Total approved collations count (computed on apply level)
    ApplyLevelCollationsCount,

    /// Total commits count (computed on apply level)
    ApplyLevelCommitsCount,

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
        write!(
            f,
            "Node(public_key_hash={}, metrics={:?})",
            self.public_key.id(),
            self.metrics
        )
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
    pub fn aggregate(&self, params: &SlashingConfig) -> AggregatedValidatorStat {
        AggregatedValidatorStat::new(self, params)
    }
}

/// Session statistics aggregated metric
#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub enum AggregatedMetric {
    /// Collations participation
    CollationsParticipation,

    /// Approvals participation
    ApprovalsParticipation,

    /// Commits participation
    CommitsParticipation,

    /// Collations participation (computed on apply level)
    ApplyLevelCollationsParticipation,

    /// Commits participation (computed on apply level)
    ApplyLevelCommitsParticipation,

    /// Aggregated validator score
    ValidationScore,

    /// Aggregated slashing score
    SlashingScore,

    /// Number of metrics
    AggregatedMetricsCount,
}

/// Aggregated validator session metric
#[derive(Clone)]
pub struct AggregatedNode {
    /// Node public key
    pub public_key: PublicKey,

    /// Source node
    pub node: Node,

    /// Aggregated statistics metrics
    pub metrics: [f64; AggregatedMetric::AggregatedMetricsCount as usize],
}

impl AggregatedNode {
    /// Create new aggregated entry
    pub fn new(entry: &Node, config: &SlashingConfig) -> Self {
        let mut result = Self {
            public_key: entry.public_key.clone(),
            node: entry.clone(),
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

        let apply_blocks_count = entry.metrics[ApplyLevelTotalBlocksCount as usize] as f64;

        if apply_blocks_count > 0.0 {
            let apply_collations_count = entry.metrics[ApplyLevelCollationsCount as usize] as f64;
            let apply_commits_count = entry.metrics[ApplyLevelCommitsCount as usize] as f64;

            result.metrics[ApplyLevelCollationsParticipation as usize] =
                apply_collations_count / apply_blocks_count;
            result.metrics[ApplyLevelCommitsParticipation as usize] =
                apply_commits_count / apply_blocks_count;
        }

        let total_weight = (config.signing_score_weight + config.collations_score_weight) as f64;
        let signing_weight = config.signing_score_weight as f64 / total_weight;
        let collations_weight = config.collations_score_weight as f64 / total_weight;

        let validation_score = collations_weight
            * result.metrics[ApplyLevelCollationsParticipation as usize]
            + signing_weight * result.metrics[ApplyLevelCommitsParticipation as usize];
        let slashing_score = 1.0 - validation_score;

        result.metrics[ValidationScore as usize] = validation_score;
        result.metrics[SlashingScore as usize] = slashing_score;

        result
    }
}

impl fmt::Debug for AggregatedNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AggregatedNode(public_key_hash={}, metrics={:?})",
            self.public_key.id(),
            self.metrics
        )
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
        write!(
            f,
            "SlashedNode(public_key_hash={}, metric_id={:?})",
            self.public_key.id(),
            self.metric_id
        )
    }
}

/// Session aggregated statistics
#[derive(Clone, Debug)]
pub struct AggregatedValidatorStat {
    /// Aggregated statistics entries for each validator
    validators_stat: HashMap<PublicKeyHash, AggregatedNode>,

    /// Aggregated metric params (like expected value, standard deviation)
    // metrics_params: [AggregatedMetricParams; AggregatedMetric::AggregatedMetricsCount as usize],

    /// Slashed validators list
    slashed_validators: Vec<SlashedNode>,
}

impl AggregatedValidatorStat {
    /// Create new aggregated statistics
    pub fn new(stat: &ValidatorStat, config: &SlashingConfig) -> Self {
        let confidence_z: f64 =
            (config.z_param_numerator as f64) / (config.z_param_denominator as f64);
        let min_slashing_protection_score: f64 =
            config.min_slashing_protection_score as f64 / 100.0;
        let min_samples_count = config.min_samples_count;
        let validators_stat: HashMap<PublicKeyHash, AggregatedNode> = stat
            .validators_stat
            .clone()
            .into_iter()
            .map(|(pub_key_hash, entry)| (pub_key_hash, AggregatedNode::new(&entry, config)))
            .collect();
        let metrics_params: Vec<AggregatedMetricParams> = (0
            ..AggregatedMetric::AggregatedMetricsCount as usize)
            .map(|metric| Self::compute_params_for_metric(&validators_stat, metric, confidence_z))
            .collect();
        let mut slashed_validators = Vec::new();

        for (_pub_key_hash, entry) in &validators_stat {
            for (metric_id, metric_norm) in metrics_params.iter().enumerate() {
                use AggregatedMetric::*;
                use Metric::*;

                if metric_id != ValidationScore as usize {
                    continue;
                }

                let apply_blocks_count =
                    entry.node.metrics[ApplyLevelTotalBlocksCount as usize] as u32;

                if apply_blocks_count < min_samples_count {
                    continue;
                }

                let score = entry.metrics[metric_id];

                if score >= metric_norm.min_confidence_value {
                    continue;
                }

                if score >= min_slashing_protection_score {
                    continue;
                }

                debug!("Add validator {} to slashed list because of score {:.4} with protection level {:.2} and min confidence level {:.4}",
                    &hex::encode(entry.public_key.pub_key().expect("PublicKey is assigned")), score, min_slashing_protection_score, metric_norm.min_confidence_value);

                slashed_validators.push(SlashedNode {
                    public_key: entry.public_key.clone(),
                    metric_id: unsafe { ::std::mem::transmute(metric_id) },
                });
            }
        }

        Self {
            // metrics_params: metrics_params.as_slice().try_into().unwrap(),
            validators_stat,
            slashed_validators,
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
