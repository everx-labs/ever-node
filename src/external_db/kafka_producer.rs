use std::time;
use crate::external_db::WriteData;
use ton_types::Result;

#[derive(Debug, Default, serde::Deserialize)]
pub struct KafkaProducerConfig {
    brokers: String,
    message_timeout_ms: u32,
    topic: String,
    attempt_timeout_ms: u32,
    message_max_size: usize,
    big_messages_storage: String,
}

pub(super) struct KafkaProducer {
    config: KafkaProducerConfig,
    producer: rdkafka::producer::FutureProducer,
}

impl KafkaProducer {
    pub fn new(config: KafkaProducerConfig) -> Result<Self> {
        log::trace!("Creating kafka producer (topic: {})...", config.topic);
        let producer = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", &config.message_timeout_ms.to_string())
            .set("message.max.bytes", &config.message_max_size.to_string())
            .create()?;

        Ok(Self { config, producer } )
    }
}

#[async_trait::async_trait]
impl WriteData for KafkaProducer {
    async fn write_data(&self, key: String, data: String) -> Result<()> {
        if data.len() > self.config.message_max_size {
            let root = std::path::Path::new(&self.config.big_messages_storage);
            let dir = root.join(std::path::Path::new(&self.config.topic));
            let _ = std::fs::create_dir_all(dir.clone());
            match std::fs::write(dir.join(std::path::Path::new(&key)), &data) {
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
                    e),
            };
            return Ok(());
        }
        loop {
            log::trace!("Producing record, topic: {}, key: {}", self.config.topic, key);
            let now = std::time::Instant::now();
            let produce_future = self.producer.send(
                rdkafka::producer::FutureRecord::to(&self.config.topic)
                    .key(&key)
                    .payload(&data),
                0,
            );
            match produce_future.await {
                Ok(Ok(_)) => {
                    log::trace!("Produced record, topic: {}, key: {}, time: {} mcs", self.config.topic, key, now.elapsed().as_micros());
                    break;
                },
                Ok(Err((e, _))) => log::warn!("Error while producing into kafka, topic: {}, key: {}, error: {}", self.config.topic, key, e),
                Err(e) => log::warn!("Internal error while producing into kafka, topic: {}, key: {}, error: {}", self.config.topic, key, e),
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
