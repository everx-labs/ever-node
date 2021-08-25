use std::time;
use crate::{external_db::WriteData, config::KafkaProducerConfig};
use rdkafka::message::OwnedHeaders;
use ton_types::{Result, fail};

const EXTERNAL_MESSAGE_DATA_HEADER_KEY: &str = "external-message-ref";
const PATTERN_TO_REPLACE: &str = "{message_filename}";

pub(super) struct KafkaProducer {
    config: KafkaProducerConfig,
    producer: Option<rdkafka::producer::FutureProducer>,
}

impl KafkaProducer {
    pub fn new(config: KafkaProducerConfig) -> Result<Self> {
        if !config.enabled {
            log::trace!("Kafka producer (topic: {}) is DISABLED", config.topic);
            Ok(Self { config, producer: None } )
        } else {
            log::trace!("Creating kafka producer (topic: {})...", config.topic);
            let producer = rdkafka::config::ClientConfig::new()
                .set("bootstrap.servers", &config.brokers)
                .set("message.timeout.ms", &config.message_timeout_ms.to_string())
                .set("message.max.bytes", &config.message_max_size.to_string())
                .create()?;

            if let Some(pattern) = &config.external_message_ref_address_pattern {
                if pattern.find(PATTERN_TO_REPLACE).is_none() {
                    return Err(crate::error::NodeError::InvalidArg("Pattern has no matches to replace".to_owned()).into());
                }
            }

            Ok(Self { config, producer: Some(producer) } )
        }
    }

    fn store_oversized(&self, key: &str, data: &[u8]) -> Result<()> {
        let root = std::path::Path::new(&self.config.big_messages_storage);
        let dir = root.join(std::path::Path::new(&self.config.topic));
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

    async fn process_oversized(&self, key: &str, data: &[u8]) -> Result<()> {
        self.store_oversized(&key, &data)?;
        if let Some(pattern) = &self.config.external_message_ref_address_pattern {
            loop {
                let path = pattern.replace(PATTERN_TO_REPLACE, &key);
                let headers = rdkafka::message::OwnedHeaders::new_with_capacity(1)
                    .add(EXTERNAL_MESSAGE_DATA_HEADER_KEY, &path);
                let result = self.producer.as_ref().unwrap().send(
                    rdkafka::producer::FutureRecord::to(&self.config.topic)
                        .key(&format!("\"{}\"", key))
                        .headers(headers)
                        .payload(""),
                    0,
                ).await;
                match result {
                    Ok(Ok(_)) => {
                        log::trace!("Produced oversized path record, topic: {}, key: {}", self.config.topic, key);
                        break;
                    },
                    Ok(Err((e, _))) => log::warn!(
                        "Error while producing oversized path record into kafka, topic: {}, key: {}, error: {}", self.config.topic, key, e
                    ),
                    Err(e) => log::warn!(
                        "Internal error while producing oversized path record into kafka, topic: {}, key: {}, error: {}", self.config.topic, key, e
                    )
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

    async fn write_internal(&self, key: Vec<u8>, key_str: String, data: Vec<u8>, attributes: Option<&[(&str, &[u8])]>) -> Result<()> {
        if !self.enabled() {
            fail!("Producer is disabled");
        }
        else if self.producer.is_none() {
            fail!("Internal error: producer is enabled but kafka producer instance is None");
        }

        loop {
            log::trace!("Producing record, topic: {}, key: {}", self.config.topic, key_str);
            let now = std::time::Instant::now();
            let mut record = rdkafka::producer::FutureRecord::to(&self.config.topic)
                .key(&key)
                .payload(&data);
            if let Some(attributes) = attributes {
                let mut headers = OwnedHeaders::new();
                for (name, value) in attributes {
                    headers = headers.add(*name, *value);
                }
                record = record.headers(headers);
            }
            let produce_future = self.producer.as_ref().unwrap().send(record, 0);
            match produce_future.await {
                Ok(Ok(_)) => {
                    log::trace!("Produced record, topic: {}, key: {}, time: {} mcs", self.config.topic, key_str, now.elapsed().as_micros());
                    break;
                },
                Ok(Err((e, _))) => {
                    match e {
                        rdkafka::error::KafkaError::MessageProduction(rdkafka::error::RDKafkaError::MessageSizeTooLarge) => {
                            self.process_oversized(&key_str, &data).await?;
                            break;
                        }
                        _ => log::warn!("Error while producing into kafka, topic: {}, key: {}, error: {}", self.config.topic, key_str, e),
                    }
                },
                Err(e) => log::warn!("Internal error while producing into kafka, topic: {}, key: {}, error: {}", self.config.topic, key_str, e),
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

    async fn write_raw_data(&self, key: Vec<u8>, data: Vec<u8>, attributes: Option<&[(&str, &[u8])]>) -> Result<()> {
        let key_str = format!("{}", hex::encode(&key));
        self.write_internal(key, key_str, data, attributes).await
    }

    async fn write_data(&self, key: String, data: String, attributes: Option<&[(&str, &[u8])]>) -> Result<()> {
        self.write_internal(key.clone().into_bytes(), key, data.into_bytes(), attributes).await
    }
}
