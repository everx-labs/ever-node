use std::time;
use crate::{external_db::WriteData, config::KafkaProducerConfig};
use ton_types::{Result, fail};

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

            Ok(Self { config, producer: Some(producer) } )
        }
    }

    async fn write_internal(&self, key: Vec<u8>, key_str: String, data: Vec<u8>) -> Result<()> {
        if !self.enabled() {
            fail!("Producer is disabled");
        }
        else if self.producer.is_none() {
            fail!("Internal error: producer is enabled but kafka producer instance is None");
        }

        if data.len() > self.config.message_max_size {
            let root = std::path::Path::new(&self.config.big_messages_storage);
            let dir = root.join(std::path::Path::new(&self.config.topic));
            let _ = std::fs::create_dir_all(dir.clone());
            match std::fs::write(dir.join(std::path::Path::new(&key_str)), &data) {
                Ok(_) => log::error!(
                    "Too big message ({} bytes, limit is {}), saved into {}",
                    data.len(),
                    self.config.message_max_size,
                    dir.join(std::path::Path::new(&key_str)).to_str().unwrap_or_default()),
                Err(e) => log::error!(
                    "Too big message ({} bytes, limit is {}), error while saving into {}: {}",
                    data.len(),
                    self.config.message_max_size,
                    dir.join(std::path::Path::new(&key_str)).to_str().unwrap_or_default(),
                    e),
            };
            return Ok(());
        }
        loop {
            log::trace!("Producing record, topic: {}, key: {}", self.config.topic, key_str);
            let now = std::time::Instant::now();
            let produce_future = self.producer.as_ref().unwrap().send(
                rdkafka::producer::FutureRecord::to(&self.config.topic)
                    .key(&key)
                    .payload(&data),
                0,
            );
            match produce_future.await {
                Ok(Ok(_)) => {
                    log::trace!("Produced record, topic: {}, key: {}, time: {} mcs", self.config.topic, key_str, now.elapsed().as_micros());
                    break;
                },
                Ok(Err((e, _))) => log::warn!("Error while producing into kafka, topic: {}, key: {}, error: {}", self.config.topic, key_str, e),
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

    async fn write_raw_data(&self, key: Vec<u8>, data: Vec<u8>) -> Result<()> {
        let key_str = format!("{}", hex::encode(&key));
        self.write_internal(key, key_str, data).await
    }

    async fn write_data(&self, key: String, data: String) -> Result<()> {
        self.write_internal(key.clone().into_bytes(), key, data.into_bytes()).await
    }
}
