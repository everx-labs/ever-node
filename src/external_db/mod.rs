use crate::engine_traits::ExternalDb;
use kafka_producer::{ KafkaProducerConfig, KafkaProducer };
use stub_producer::StubProducer;
use processor::Processor;

use std::sync::Arc;
use ton_types::Result;

mod processor;
mod kafka_producer;
pub mod kafka_consumer;
mod stub_producer;

#[cfg(test)]
#[path = "tests/test.rs"]
mod tests;

#[derive(Debug, Default, serde::Deserialize)]
pub struct ExternalDbConfig {
    block_producer: KafkaProducerConfig,
    message_producer: KafkaProducerConfig,
    transaction_producer: KafkaProducerConfig,
    account_producer: KafkaProducerConfig,
    block_proof_producer: KafkaProducerConfig,
    bad_blocks_storage: String,
}

#[async_trait::async_trait]
pub trait WriteData : Sync + Send {
    async fn write_data(&self, key: String, data: String) -> Result<()>;
}

#[allow(dead_code)]
pub fn create_external_db(config: ExternalDbConfig) -> Result<Arc<dyn ExternalDb>> {
    Ok(
        Arc::new(
            Processor::new(
                KafkaProducer::new(config.block_producer)?,
                KafkaProducer::new(config.message_producer)?,
                KafkaProducer::new(config.transaction_producer)?,
                KafkaProducer::new(config.account_producer)?,
                KafkaProducer::new(config.block_proof_producer)?,
                config.bad_blocks_storage,
            )
        )
    )
}