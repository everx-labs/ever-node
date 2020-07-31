use crate::{engine_traits::ExternalDb, config::ExternalDbConfig};
use kafka_producer::{ KafkaProducer };
use stub_producer::StubProducer;
use processor::Processor;

use std::sync::Arc;
use ton_types::Result;

mod processor;
mod kafka_producer;
pub mod kafka_consumer;
mod stub_producer;


#[async_trait::async_trait]
pub trait WriteData : Sync + Send {
    fn enabled(&self) -> bool;
    async fn write_data(&self, key: String, data: String) -> Result<()>;
    async fn write_raw_data(&self, key: Vec<u8>, data: Vec<u8>) -> Result<()>;
}

#[allow(dead_code)]
pub fn create_external_db(config: ExternalDbConfig) -> Result<Arc<dyn ExternalDb>> {
    if cfg!(feature = "local_test") {
        Ok(
            Arc::new(
                Processor::new(
                    StubProducer{enabled: true},
                    StubProducer{enabled: true},
                    StubProducer{enabled: true},
                    StubProducer{enabled: true},
                    StubProducer{enabled: true},
                    StubProducer{enabled: true},
                    config.bad_blocks_storage,
                )
            )
        )
    } else {
        Ok(
            Arc::new(
                Processor::new(
                    KafkaProducer::new(config.block_producer)?,
                    KafkaProducer::new(config.raw_block_producer)?,
                    KafkaProducer::new(config.message_producer)?,
                    KafkaProducer::new(config.transaction_producer)?,
                    KafkaProducer::new(config.account_producer)?,
                    KafkaProducer::new(config.block_proof_producer)?,
                    config.bad_blocks_storage,
                )
            )
        )
    }
}