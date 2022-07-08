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

extern crate hex;

/// Imports
pub use super::*;
use ever_crypto::{Ed25519KeyOption, KeyId};
use crate::ton_api::IntoBoxed;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryInto;

/*
    to string conversions
*/

pub fn bytes_to_string(v: &::ton_api::ton::bytes) -> String {
    hex::encode(&v.0)
}

pub fn public_key_hashes_to_string(v: &[PublicKeyHash]) -> String {
    let mut result: String = "[".to_string();
    let mut first = true;

    for key in v {
        if !first {
            result += ", ";
        } else {
            first = false;
        }

        result = format!("{}{}", result, key);
    }

    result + "]"
}

pub fn time_to_string(time: &std::time::SystemTime) -> String {
    let datetime: chrono::DateTime<chrono::offset::Utc> = time.clone().into();

    datetime.format("%Y-%m-%d %T.%f").to_string()
}

pub fn time_to_timestamp_string(time: &std::time::SystemTime) -> String {
    match time.duration_since(std::time::UNIX_EPOCH) {
        Ok(timestamp) => format!("{:.3}", timestamp.as_millis() as f64 / 1000.0),
        Err(err) => format!("{}", err),
    }
}

/*
    type conversions
*/

pub fn parse_hex(hex_asm: &str) -> Vec<u8> {
    let mut hex_bytes = hex_asm
        .as_bytes()
        .iter()
        .filter_map(|b| match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        })
        .fuse();

    let mut bytes = Vec::new();
    while let (Some(h), Some(l)) = (hex_bytes.next(), hex_bytes.next()) {
        bytes.push(h << 4 | l)
    }

    bytes
}

pub fn parse_hex_to_array(hex_asm: &str, dst: &mut [u8]) {
    assert!(dst.len() * 2 >= hex_asm.len());
    dst.iter_mut().for_each(|x| *x = 0);

    for (i, c) in hex_asm.chars().enumerate() {
        dst[i / 2] = dst[i / 2] * 16 + u8::from_str_radix(&c.to_string(), 16).unwrap();
    }
}

pub fn parse_hex_as_int256(hex_asm: &str) -> UInt256 {
    let hex_bytes = parse_hex(&hex_asm);
    UInt256::from_slice(hex_bytes.as_slice())
}

pub fn parse_hex_as_bytes(hex_asm: &str) -> ::ton_api::ton::bytes {
    ::ton_api::ton::bytes(parse_hex(&hex_asm))
}

pub fn parse_hex_as_block_payload(hex_asm: &str) -> BlockPayloadPtr {
    CatchainFactory::create_block_payload(parse_hex_as_bytes(hex_asm))
}

pub fn parse_hex_as_public_key(hex_asm: &str) -> PublicKey {
    assert!(hex_asm.len() % 2 == 0);
    let mut key_slice = vec![0; hex_asm.len() / 2];
    parse_hex_to_array(hex_asm, &mut key_slice[..]);
    //TODO: errors processing for key creation
    Ed25519KeyOption::from_public_key_tl_serialized(&key_slice).unwrap()
}

pub fn parse_hex_as_public_key_raw(hex_asm: &str) -> PublicKey {
    assert!(hex_asm.len() == 64);
    let mut key_slice = [0u8; 32];
    parse_hex_to_array(hex_asm, &mut key_slice[..]);
    //TODO: errors processing for key creation
    Ed25519KeyOption::from_public_key(&key_slice)
}

pub fn parse_hex_as_public_key_hash(hex_asm: &str) -> PublicKeyHash {
    let mut key_slice: [u8; 32] = [0; 32];
    parse_hex_to_array(hex_asm, &mut key_slice);
    KeyId::from_data(key_slice)
}

pub fn parse_hex_as_session_id(hex_asm: &str) -> SessionId {
    parse_hex_as_int256(hex_asm)
}

pub fn parse_hex_as_private_key(hex_asm: &str) -> PrivateKey {
    assert!(hex_asm.len() % 2 == 0);
    let mut key_slice = vec![0; hex_asm.len() / 2];
    parse_hex_to_array(hex_asm, &mut key_slice[..]);
    //TODO: errors processing for key creation
    assert!(key_slice.len() == 32);
    Ed25519KeyOption::from_private_key(key_slice.as_slice().try_into().unwrap()).unwrap()
}

pub fn parse_hex_as_expanded_private_key(hex_asm: &str) -> PrivateKey {
    assert!(hex_asm.len() % 2 == 0);
    let mut key_slice = vec![0; hex_asm.len() / 2];
    parse_hex_to_array(hex_asm, &mut key_slice[..]);
    //TODO: errors processing for key creation
    assert!(key_slice.len() == 64);
    Ed25519KeyOption::from_expanded_key(key_slice.as_slice().try_into().unwrap()).unwrap()
}

pub fn get_hash(data: &::ton_api::ton::bytes) -> BlockHash {
    UInt256::calc_file_hash(&data.0)
}

pub fn get_hash_from_block_payload(data: &BlockPayloadPtr) -> BlockHash {
    UInt256::calc_file_hash(&data.data().0)
}

pub fn int256_to_public_key_hash(public_key: &UInt256) -> PublicKeyHash {
    KeyId::from_data(*public_key.as_slice())
}

pub fn get_public_key_hash(public_key: &PublicKey) -> PublicKeyHash {
    public_key.id().clone()
}

pub fn public_key_hash_to_int256(v: &PublicKeyHash) -> UInt256 {
    UInt256::with_array(*v.data())
}

pub fn get_overlay_id(first_block: &ton_api::ton::catchain::FirstBlock) -> Result<SessionId> {
    let serialized_first_block = serialize_tl_boxed_object!(first_block);
    let overlay_id = ::ton_api::ton::pub_::publickey::Overlay {
        name: serialized_first_block.into(),
    };
    let serialized_overlay_id = serialize_tl_boxed_object!(&overlay_id.into_boxed());
    Ok(UInt256::calc_file_hash(&serialized_overlay_id.0))
}

pub fn get_block_id(
    incarnation: &SessionId,
    source_hash: &PublicKeyHash,
    height: ::ton_api::ton::int,
    payload: &RawBuffer,
) -> ton::BlockId {
    ::ton_api::ton::catchain::block::id::Id {
        incarnation: incarnation.clone().into(),
        src: public_key_hash_to_int256(source_hash),
        height: height,
        data_hash: get_hash(payload).into(),
    }
    .into_boxed()
}

pub fn get_block_dependency_id(block: &ton::BlockDep, receiver: &dyn Receiver) -> ton::BlockId {
    ::ton_api::ton::catchain::block::id::Id {
        incarnation: receiver.get_incarnation().clone().into(),
        src: public_key_hash_to_int256(receiver.get_source_public_key_hash(block.src as usize)),
        height: block.height,
        data_hash: block.data_hash.clone(),
    }
    .into_boxed()
}

pub fn get_root_block_id(incarnation: &SessionId) -> ton::BlockId {
    ::ton_api::ton::catchain::block::id::Id {
        incarnation: incarnation.clone().into(),
        src: incarnation.clone().into(),
        height: 0,
        data_hash: incarnation.clone().into(),
    }
    .into_boxed()
}

pub fn get_block_id_hash(id: &ton::BlockId) -> BlockHash {
    let mut serial = Vec::<u8>::new();
    let mut serializer = ton_api::Serializer::new(&mut serial);
    serializer.write_boxed(id).unwrap();
    get_hash(&ton_api::ton::bytes(serial))
}

pub fn get_block_dependency_hash(block: &ton::BlockDep, receiver: &dyn Receiver) -> BlockHash {
    get_block_id_hash(&get_block_dependency_id(block, receiver))
}

/*
    serialization utils
*/

#[macro_export]
macro_rules! serialize_tl_bare_object
{
  ($($args:expr),*) => {{
    let mut ret : ton_api::ton::bytes = ton_api::ton::bytes::default();
    let mut serializer = ton_api::Serializer::new(&mut ret.0);

    $(serializer.write_bare($args).unwrap();)*

    ret
  }}
}

#[macro_export]
macro_rules! serialize_tl_boxed_object
{
  ($($args:expr),*) => {{
    let mut ret : ton_api::ton::bytes = ton_api::ton::bytes::default();
    let mut serializer = ton_api::Serializer::new(&mut ret.0);

    $(serializer.write_boxed($args).unwrap();)*

    ret
  }}
}

pub fn serialize_block_with_payload(
    block: &ton::Block,
    payload: &BlockPayloadPtr,
) -> Result<RawBuffer> {
    let mut raw_data: RawBuffer = RawBuffer::default();
    let mut serializer = ton_api::Serializer::new(&mut raw_data.0);

    serializer.write_boxed(&block.clone().into_boxed())?;
    raw_data.0.extend(payload.data().iter());

    Ok(raw_data)
}

pub fn serialize_query_boxed_response<T>(response: Result<T>) -> Result<BlockPayloadPtr>
where
    T: ::ton_api::BoxedSerialize,
{
    match response {
        Ok(response) => {
            let mut ret: RawBuffer = RawBuffer::default();
            let mut serializer = ton_api::Serializer::new(&mut ret.0);

            serializer.write_boxed(&response).unwrap();

            Ok(CatchainFactory::create_block_payload(ret))
        }
        Err(err) => Err(err),
    }
}

pub fn deserialize_tl_bare_object<T: ::ton_api::BareDeserialize>(bytes: &RawBuffer) -> Result<T> {
    let cloned_bytes = bytes.clone();
    let data: &mut &[u8] = &mut cloned_bytes.0.as_ref();
    ton_api::Deserializer::new(data).read_bare()
}

pub fn deserialize_tl_boxed_object<T: ::ton_api::BoxedDeserialize>(bytes: &RawBuffer) -> Result<T> {
    let cloned_bytes = bytes.clone();
    let data: &mut &[u8] = &mut cloned_bytes.0.as_ref();
    ton_api::Deserializer::new(data).read_boxed()
}

/*
   metrics
*/

#[derive(Copy, Clone)]
enum MetricUsage {
    Counter,
    Derivative,
    Percents,
    Float,
    Latency,
}

pub struct Metric {
    value: u64,
    usage: MetricUsage,
}

pub struct MetricsDumper {
    prev_metrics: BTreeMap<String, Metric>,
    compute_handlers:
        HashMap<String, Box<dyn Fn(&String, &BTreeMap<String, Metric>) -> Option<Metric>>>,
    derivative_metrics: HashSet<String>,
    last_dump_time: std::time::SystemTime,
}

impl MetricsDumper {
    pub const METRIC_DERIVATIVE_MULTIPLIER: f64 = 1000000.0;
    pub const METRIC_FLOAT_MULTIPLIER: f64 = 10000.0;
    pub const METRIC_TIME_MULTIPLIER: f64 = 1000.0;

    pub fn add_compute_handler<F>(&mut self, key: String, handler: F)
    where
        F: Fn(&String, &BTreeMap<String, Metric>) -> Option<Metric>,
        F: 'static,
    {
        self.compute_handlers.insert(key, Box::new(handler));
    }

    pub fn add_derivative_metric(&mut self, key: String) {
        self.derivative_metrics.insert(key);
    }

    fn update_histogram(
        &mut self,
        metrics: &mut BTreeMap<String, Metric>,
        mut basic_key: String,
        mut values: Vec<u64>,
    ) {
        let (mut last, mut avg, mut med, mut min, mut max) = if values.len() > 0 {
            let last = values[values.len() - 1] as f64;
            let avg = values.iter().sum::<u64>() as f64 / values.len() as f64;

            values.sort();

            let med = values[values.len() / 2] as f64;
            let min = values[0] as f64;
            let max = values[values.len() - 1] as f64;

            (last, avg, med, min, max)
        } else {
            (0.0, 0.0, 0.0, 0.0, 0.0)
        };

        let mut usage = MetricUsage::Float;

        if let Some(stripped_basic_key) = basic_key.strip_prefix("time:") {
            basic_key = stripped_basic_key.to_string();
            usage = MetricUsage::Latency;
            last /= Self::METRIC_TIME_MULTIPLIER;
            avg /= Self::METRIC_TIME_MULTIPLIER;
            med /= Self::METRIC_TIME_MULTIPLIER;
            min /= Self::METRIC_TIME_MULTIPLIER;
            max /= Self::METRIC_TIME_MULTIPLIER;
        }

        metrics.insert(
            format!("{}.last", basic_key),
            Metric {
                value: (last * Self::METRIC_FLOAT_MULTIPLIER) as u64,
                usage: usage,
            },
        );
        metrics.insert(
            format!("{}.avg", basic_key),
            Metric {
                value: (avg * Self::METRIC_FLOAT_MULTIPLIER) as u64,
                usage: usage,
            },
        );
        metrics.insert(
            format!("{}.med", basic_key),
            Metric {
                value: (med * Self::METRIC_FLOAT_MULTIPLIER) as u64,
                usage: usage,
            },
        );
        metrics.insert(
            format!("{}.min", basic_key),
            Metric {
                value: (min * Self::METRIC_FLOAT_MULTIPLIER) as u64,
                usage: usage,
            },
        );
        metrics.insert(
            format!("{}.max", basic_key),
            Metric {
                value: (max * Self::METRIC_FLOAT_MULTIPLIER) as u64,
                usage: usage,
            },
        );
        metrics.insert(
            format!("{}.cnt", basic_key),
            Metric {
                value: values.len() as u64,
                usage: MetricUsage::Counter,
            },
        );
    }

    pub fn update(&mut self, metrics_receiver: &metrics_runtime::Receiver) {
        //convert metrics

        let mut metrics: BTreeMap<String, Metric> = BTreeMap::new();

        for (key, value) in &metrics_receiver.controller().snapshot().into_measurements() {
            let key = key.name().to_string();

            match value {
                metrics_runtime::Measurement::Counter(value) => {
                    metrics.insert(
                        key,
                        Metric {
                            value: *value,
                            usage: MetricUsage::Counter,
                        },
                    );
                }
                metrics_runtime::Measurement::Histogram(value) => {
                    self.update_histogram(&mut metrics, key, value.decompress());
                }
                metrics_runtime::Measurement::Gauge(value) => {
                    let mut usage = MetricUsage::Counter;
                    let mut key = key.to_string();

                    if let Some(stripped_basic_key) = key.strip_prefix("percents:") {
                        usage = MetricUsage::Percents;
                        key = stripped_basic_key.to_string();
                    } else {
                        if let Some(stripped_basic_key) = key.strip_prefix("float:") {
                            key = stripped_basic_key.to_string();
                            usage = MetricUsage::Float;
                        }
                    }

                    metrics.insert(
                        key,
                        Metric {
                            value: *value as u64,
                            usage: usage,
                        },
                    );
                }
            }
        }

        //snapshot time

        let duration = self.last_dump_time.elapsed().unwrap().as_secs_f64();
        self.last_dump_time = std::time::SystemTime::now();

        //compute metrics

        for (key, handler) in &self.compute_handlers {
            if let Some(value) = handler(key, &metrics) {
                metrics.insert(key.to_string(), value);
            }
        }

        //compute derivative metrics

        for key in &self.derivative_metrics {
            if let Some(value) = metrics.get(key) {
                if let Some(prev_value) = self.prev_metrics.get(key) {
                    let delta = (value.value as isize - prev_value.value as isize) as f64;
                    let derivative = (delta / duration * Self::METRIC_DERIVATIVE_MULTIPLIER) as u64;

                    metrics.insert(
                        format!("{}.speed", key),
                        Metric {
                            value: derivative,
                            usage: MetricUsage::Derivative,
                        },
                    );
                }
            }
        }

        //update state

        self.prev_metrics = metrics;
    }

    pub fn dump<F>(&self, handler: F)
    where
        F: Fn(String),
    {
        for (key, metric) in &self.prev_metrics {
            use MetricUsage::*;

            let metric_dump = match metric.usage {
                Counter => format!("{}", metric.value),
                Derivative => {
                    let value = metric.value as f64 / Self::METRIC_DERIVATIVE_MULTIPLIER;

                    let (multiplier, suffix) = if value > 1000000.0 {
                        (1000000.0, "M")
                    } else if value > 1000.0 {
                        (1000.0, "K")
                    } else {
                        (1.0, "")
                    };

                    format!("{:.2}{}/s", value / multiplier, suffix)
                }
                Percents => format!(
                    "{:.1}%",
                    (metric.value as f64) / Self::METRIC_FLOAT_MULTIPLIER * 100.0
                ),
                Float => format!(
                    "{:.2}",
                    (metric.value as f64) / Self::METRIC_FLOAT_MULTIPLIER
                ),
                Latency => format!(
                    "{:.3}s",
                    (metric.value as f64) / Self::METRIC_FLOAT_MULTIPLIER
                ),
            };

            handler(format!("    {:12} - {}", metric_dump, key));
        }
    }

    pub fn new() -> MetricsDumper {
        MetricsDumper {
            last_dump_time: std::time::SystemTime::now(),
            prev_metrics: BTreeMap::new(),
            compute_handlers: HashMap::new(),
            derivative_metrics: HashSet::new(),
        }
    }
}

fn get_metrics_counters_pair(
    metrics: &BTreeMap<String, Metric>,
    key1: &String,
    key2: &String,
) -> Option<(u64, u64)> {
    let value1 = metrics.get(key1);
    let value1 = match value1 {
        Some(value) => value,
        _ => return None,
    };

    let value2 = metrics.get(key2);
    let value2 = match value2 {
        Some(value) => value,
        _ => return None,
    };

    if !match value1.usage {
        MetricUsage::Counter => true,
        _ => false,
    } || !match value2.usage {
        MetricUsage::Counter => true,
        _ => false,
    } {
        return None;
    }

    Some((value1.value, value2.value))
}

pub fn compute_diff_counter(
    basic_key: &String,
    metrics: &BTreeMap<String, Metric>,
    add_suffix: &str,
    sub_suffix: &str,
) -> Option<Metric> {
    let create_key = basic_key.clone() + add_suffix;
    let drop_key = basic_key.clone() + sub_suffix;

    if let Some((create_value, drop_value)) =
        get_metrics_counters_pair(metrics, &create_key, &drop_key)
    {
        let instance_count = if create_value > drop_value {
            create_value - drop_value
        } else {
            0
        };

        return Some(Metric {
            value: instance_count,
            usage: MetricUsage::Counter,
        });
    }

    Some(Metric {
        value: 0,
        usage: MetricUsage::Counter,
    })
}

pub fn compute_instance_counter(
    basic_key: &String,
    metrics: &BTreeMap<String, Metric>,
) -> Option<Metric> {
    compute_diff_counter(basic_key, metrics, ".create", ".drop")
}

pub fn compute_queue_size_counter(
    basic_key: &String,
    metrics: &BTreeMap<String, Metric>,
) -> Option<Metric> {
    compute_diff_counter(basic_key, metrics, ".posts", ".pulls")
}

pub fn add_compute_percentage_metric(
    metrics_dumper: &mut MetricsDumper,
    key: &String,
    value_key: &String,
    total_key: &String,
    bias: f64,
) {
    add_compute_relative_metric_impl(
        metrics_dumper,
        key,
        value_key,
        total_key,
        bias,
        MetricUsage::Percents,
    );
}

pub fn add_compute_relative_metric(
    metrics_dumper: &mut MetricsDumper,
    key: &String,
    value_key: &String,
    total_key: &String,
    bias: f64,
) {
    add_compute_relative_metric_impl(
        metrics_dumper,
        key,
        value_key,
        total_key,
        bias,
        MetricUsage::Float,
    );
}

fn add_compute_relative_metric_impl(
    metrics_dumper: &mut MetricsDumper,
    key: &String,
    value_key: &String,
    total_key: &String,
    bias: f64,
    usage: MetricUsage,
) {
    let value_key = value_key.clone();
    let total_key = total_key.clone();
    metrics_dumper.add_compute_handler(key.to_string(), move |_key, metrics| -> Option<Metric> {
        if let Some((value, total_value)) =
            get_metrics_counters_pair(metrics, &value_key, &total_key)
        {
            if total_value != 0 {
                let percentage = (value as f64) / (total_value as f64) + bias;

                return Some(Metric {
                    value: (percentage * MetricsDumper::METRIC_FLOAT_MULTIPLIER) as u64,
                    usage: usage,
                });
            }
        }

        None
    });
}

pub fn add_compute_result_metric(metrics_dumper: &mut MetricsDumper, basic_key: &String) {
    metrics_dumper.add_compute_handler(
        format!("{}.success.frequency", basic_key),
        &utils::compute_result_success_metric,
    );
    metrics_dumper.add_compute_handler(
        format!("{}.failure.frequency", basic_key),
        &utils::compute_result_failure_metric,
    );
    metrics_dumper.add_compute_handler(
        format!("{}.ignore.frequency", basic_key),
        &utils::compute_result_ignore_metric,
    );
}

pub fn compute_result_status_metric(
    basic_key: &String,
    success: bool,
    metrics: &BTreeMap<String, Metric>,
) -> Option<Metric> {
    let suffix = if success {
        ".success.frequency"
    } else {
        ".failure.frequency"
    };
    let basic_key = basic_key.trim_end_matches(suffix).to_string();
    let key1 = basic_key.clone() + if success { ".success" } else { ".failure" };
    let key2 = basic_key.clone() + ".total";

    if let Some((value, total_value)) = get_metrics_counters_pair(metrics, &key1, &key2) {
        let percentage = (value as f64) / (total_value as f64);

        return Some(Metric {
            value: (percentage * MetricsDumper::METRIC_FLOAT_MULTIPLIER) as u64,
            usage: MetricUsage::Percents,
        });
    }

    None
}

pub fn compute_result_success_metric(
    basic_key: &String,
    metrics: &BTreeMap<String, Metric>,
) -> Option<Metric> {
    compute_result_status_metric(basic_key, true, metrics)
}

pub fn compute_result_failure_metric(
    basic_key: &String,
    metrics: &BTreeMap<String, Metric>,
) -> Option<Metric> {
    compute_result_status_metric(basic_key, false, metrics)
}

pub fn compute_result_ignore_metric(
    basic_key: &String,
    metrics: &BTreeMap<String, Metric>,
) -> Option<Metric> {
    let basic_key = basic_key.trim_end_matches(".ignore.frequency").to_string();
    let key1 = basic_key.clone() + ".success";
    let key2 = basic_key.clone() + ".failure";
    let key3 = basic_key.clone() + ".total";

    let success = get_metrics_counters_pair(metrics, &key1, &key3);
    let failure = get_metrics_counters_pair(metrics, &key2, &key3);

    if success.is_none() || failure.is_none() {
        return None;
    }

    let total_value = success.unwrap().1 as f64;
    let success = success.unwrap().0 as f64;
    let failure = failure.unwrap().0 as f64;
    let reports_count = success + failure;

    let percentage = (total_value - reports_count as f64) / (total_value as f64);

    Some(Metric {
        value: (percentage * MetricsDumper::METRIC_FLOAT_MULTIPLIER) as u64,
        usage: MetricUsage::Percents,
    })
}
