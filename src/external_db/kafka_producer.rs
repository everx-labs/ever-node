use std::{iter::FromIterator, collections::{HashMap, HashSet, hash_map::RandomState}, time::{self, Duration}};
use crate::{
    config::KafkaProducerConfig,
    error::NodeError,
    external_db::WriteData,
};
use rdkafka::{message::OwnedHeaders, producer::Producer};
use ton_types::{Result, fail};

const EXTERNAL_MESSAGE_DATA_HEADER_KEY: &str = "external-message-ref";
const PATTERN_TO_REPLACE: &str = "{message_filename}";

enum TopicConfig {
    Single(String),
    Sharded(HashMap<u32, String>),
}

pub(super) struct KafkaProducer {
    config: KafkaProducerConfig,
    producer: Option<rdkafka::producer::FutureProducer>,
    topic: TopicConfig,
}

impl KafkaProducer {
    pub fn new(config: KafkaProducerConfig) -> Result<Self> {
        let topic_log_name = config.topic.as_deref()
            .or_else(|| config.sharded_topics.as_ref().map(|t| t[0].name.as_str()))
            .unwrap_or("unknown")
            .to_owned();
        if !config.enabled {
            log::trace!("Kafka producer (topic: {}) is DISABLED", topic_log_name);
            Ok(Self { config, producer: None, topic: TopicConfig::Single(topic_log_name) })
        } else {
            log::trace!("Creating kafka producer (topic: {})...", topic_log_name);
            let producer = rdkafka::config::ClientConfig::new()
                .set("bootstrap.servers", &config.brokers)
                .set("message.timeout.ms", &config.message_timeout_ms.to_string())
                .set("message.max.bytes", &config.message_max_size.to_string())
                .set("retry.backoff.ms", &config.attempt_timeout_ms.to_string())
                .create()?;

            let topic = Self::init_topic_config(&producer, &config)?;

            if let Some(pattern) = &config.external_message_ref_address_pattern {
                if pattern.find(PATTERN_TO_REPLACE).is_none() {
                    return Err(crate::error::NodeError::InvalidArg("Pattern has no matches to replace".to_owned()).into());
                }
            }

            Ok(Self { config, producer: Some(producer), topic } )
        }
    }

    fn init_topic_config(
        producer: &rdkafka::producer::FutureProducer,
        config: &KafkaProducerConfig,
    ) -> Result<TopicConfig> {
        if let Some(topic_masks) = &config.sharded_topics {
            let mut topics = HashMap::new();

            if config.sharding_depth == 0 && topic_masks.len() == 1 {
                return Ok(TopicConfig::Single(topic_masks[0].name.clone()));
            }

            if 1 << config.sharding_depth != topic_masks.len() {
                return Err(NodeError::InvalidArg(
                    format!(
                        "Topics count {} doesn't match the sharding depth {}",
                        topic_masks.len(),
                        config.sharding_depth)
                ).into());
            }

            let metadata = producer.client()
                .fetch_metadata(
                    None,
                    Duration::from_millis(config.message_timeout_ms as u64)
                )
                .map_err(|err| NodeError::Other(
                    format!("Can not read kafka configuration: {}", err)
                ))?;

            let topics_set: HashSet<&str, RandomState> = HashSet::from_iter(metadata.topics().iter().map(|meta| meta.name()));

            for topic_mask in topic_masks {
                if topic_mask.mask.len() != config.sharding_depth as usize {
                    return Err(NodeError::InvalidArg(
                        format!(
                            "Mask length '{}' doesn't match the sharding depth {}",
                            topic_mask.mask,
                            config.sharding_depth)
                    ).into());
                }

                let mask = u32::from_str_radix(&topic_mask.mask, 2)
                    .map_err(|err| NodeError::InvalidArg(
                        format!("Can not parse mask for topic {}: {}", topic_mask.mask, err)
                    ))?;

                if !topics_set.contains(&topic_mask.name.as_str()) {
                    return Err(NodeError::InvalidData(
                        format!(
                            "Topic '{}' doesn't exist",
                            topic_mask.name,)
                    ).into());
                }

                topics.insert(mask, topic_mask.name.clone());
            }

            Ok(TopicConfig::Sharded(topics))
        } else if let Some(topic) = &config.topic {
            if config.sharding_depth > 0 {
                let metadata = producer.client()
                    .fetch_metadata(
                        Some(topic),
                        Duration::from_millis(config.message_timeout_ms as u64)
                    )
                    .map_err(|err| NodeError::Other(
                        format!("Can not read topic {} configuration: {}", topic, err)
                    ))?;

                let topic_metadata = metadata.topics().get(0)
                    .ok_or_else(|| NodeError::InvalidData("No topic config in metadata".to_owned()))?;

                if topic_metadata.name() != topic {
                    return Err(NodeError::InvalidData(
                        format!(
                            "Invalid topic metadata: topic name ({}) doesn't match requested name ({})",
                            topic_metadata.name(),
                            topic)
                    ).into());
                }

                if 1 << config.sharding_depth != topic_metadata.partitions().len() {
                    return Err(NodeError::InvalidArg(
                        format!(
                            "Partitions count {} in topic {} doesn't match the sharding depth {}",
                            topic_metadata.partitions().len(),
                            topic,
                            config.sharding_depth)
                    ).into());
                }
            }

            Ok(TopicConfig::Single(topic.to_owned()))
        } else {
            Err(NodeError::InvalidArg(
                "Neither topic name nor sharded topic list provided".to_owned()
            ).into())
        }
    }


    fn store_oversized(&self, key: &str, data: &[u8], topic: &str) -> Result<()> {
        let root = std::path::Path::new(&self.config.big_messages_storage);
        let dir = root.join(std::path::Path::new(topic));
        let _ = std::fs::create_dir_all(dir.clone());
        let result = std::fs::write(dir.join(std::path::Path::new(&key)), &data);
        match &result {
            Ok(_) => log::error!(
                "Too big message ({} bytes, limit is {}), saved into {}",
                data.len(),
                self.config.message_max_size,
                dir.join(std::path::Path::new(&key)).to_str().unwrap_or_default()),
            Err(e) => log::error!(
                "Too big message ({} bytes, limit is {}), error while saving into {}: {}",
                data.len(),
                self.config.message_max_size,
                dir.join(std::path::Path::new(&key)).to_str().unwrap_or_default(),
                e)
        };
        result.map_err(|err| err.into())
    }

    async fn process_oversized(&self, key: &str, data: &[u8], topic: &str) -> Result<()> {
        self.store_oversized(&key, &data, topic)?;
        if let Some(pattern) = &self.config.external_message_ref_address_pattern {
            loop {
                let path = pattern.replace(PATTERN_TO_REPLACE, &key);
                let headers = rdkafka::message::OwnedHeaders::new_with_capacity(1)
                    .add(EXTERNAL_MESSAGE_DATA_HEADER_KEY, &path);
                let result = self.producer.as_ref().unwrap().send(
                    rdkafka::producer::FutureRecord::to(topic)
                        .key(&format!("\"{}\"", key))
                        .headers(headers)
                        .payload(""),
                    None,
                ).await;
                match result {
                    Ok(_) => {
                        log::trace!("Produced oversized path record, topic: {}, key: {}", topic, key);
                        break;
                    },
                    Err((e, _)) => log::warn!(
                        "Error while producing oversized path record into kafka, topic: {}, key: {}, error: {}", topic, key, e
                    ),
                }
                futures_timer::Delay::new(
                    time::Duration::from_millis(
                        self.config.attempt_timeout_ms as u64
                    )
                ).await;
            }
        }
        Ok(())
    }

    async fn write_internal(
        &self,
        key: Vec<u8>,
        key_str: String,
        data: Vec<u8>,
        attributes: Option<&[(&str, &[u8])]>,
        partition_key: Option<u32>
    ) -> Result<()> {
        if !self.enabled() {
            fail!("Producer is disabled");
        }
        else if self.producer.is_none() {
            fail!("Internal error: producer is enabled but kafka producer instance is None");
        }

        loop {
            let topic = match &self.topic {
                TopicConfig::Single(name) => name,
                TopicConfig::Sharded(map) => {
                    let topic_number = partition_key
                        .ok_or_else(|| NodeError::InvalidData(
                            format!("Topic number is not specified for sharded topics config ({})", key_str))
                        )?;
                    map.get(&topic_number)
                        .ok_or_else(|| NodeError::InvalidData(
                            format!("No topic for partition key {} ({})", topic_number, key_str))
                        )?
                }
            };

            log::trace!("Producing record, topic: {}, key: {}, size: {}", topic, key_str, data.len());
            let now = std::time::Instant::now();
            let mut record = rdkafka::producer::FutureRecord::to(topic)
                .key(&key)
                .payload(&data);
            if let Some(attributes) = attributes {
                let mut headers = OwnedHeaders::new();
                for (name, value) in attributes {
                    headers = headers.add(*name, *value);
                }
                record = record.headers(headers);
            }
            if let Some(partition_key) = partition_key {
                if let TopicConfig::Single(_) = self.topic {
                    log::trace!("Partition key {} ({})", partition_key, key_str);
                    record = record.partition(partition_key as i32);
                }
            }
            let produce_future = self.producer.as_ref().unwrap().send(record, None);
            match produce_future.await {
                Ok(_) => {
                    log::trace!("Produced record, topic: {}, key: {}, time: {} mcs", topic, key_str, now.elapsed().as_micros());
                    break;
                },
                Err((e, _)) => {
                    match e.rdkafka_error_code() {
                        Some(rdkafka::types::RDKafkaErrorCode::MessageSizeTooLarge) => {
                            self.process_oversized(&key_str, &data, &topic).await?;
                            break;
                        }
                        _ => log::warn!("Error while producing into kafka, topic: {}, key: {}, error: {}", topic, key_str, e),
                    }
                },
            }
            futures_timer::Delay::new(
                time::Duration::from_millis(
                    self.config.attempt_timeout_ms as u64
                )
            ).await;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl WriteData for KafkaProducer {

    fn enabled(&self) -> bool { self.config.enabled }

    fn sharding_depth(&self) -> u32 { self.config.sharding_depth }

    async fn write_raw_data(&self, key: Vec<u8>, data: Vec<u8>, attributes: Option<&[(&str, &[u8])]>, partition_key: Option<u32>) -> Result<()> {
        let key_str = format!("{}", hex::encode(&key));
        self.write_internal(key, key_str, data, attributes, partition_key).await
    }

    async fn write_data(&self, key: String, data: String, attributes: Option<&[(&str, &[u8])]>, partition_key: Option<u32>) -> Result<()> {
        self.write_internal(key.clone().into_bytes(), key, data.into_bytes(), attributes, partition_key).await
    }
}
