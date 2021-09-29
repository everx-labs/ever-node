use crate::{engine_traits::ExternalDb, config::ExternalDbConfig};
use processor::Processor;

use std::sync::Arc;
use ton_types::Result;

mod processor;
#[cfg(feature = "external_db")]
mod kafka_producer;
#[cfg(feature = "external_db")]
pub mod kafka_consumer;
#[cfg(not(feature = "external_db"))]
mod stub_producer;


#[async_trait::async_trait]
pub trait WriteData : Sync + Send {
    fn enabled(&self) -> bool;
    fn sharding_depth(&self) -> u32;
    async fn write_data(&self, key: String, data: String, attributes: Option<&[(&str, &[u8])]>, partition_key: Option<u32>) -> Result<()>;
    async fn write_raw_data(&self, key: Vec<u8>, data: Vec<u8>, attributes: Option<&[(&str, &[u8])]>, partition_key: Option<u32>) -> Result<()>;
}

#[allow(dead_code)]
#[cfg(not(feature = "external_db"))]
pub fn create_external_db(config: ExternalDbConfig, front_workchain_ids: Vec<i32>) -> Result<Arc<dyn ExternalDb>> {
    Ok(
        Arc::new(
            Processor::new(
                stub_producer::StubProducer{enabled: true},
                stub_producer::StubProducer{enabled: true},
                stub_producer::StubProducer{enabled: true},
                stub_producer::StubProducer{enabled: true},
                stub_producer::StubProducer{enabled: true},
                stub_producer::StubProducer{enabled: true},
                stub_producer::StubProducer{enabled: true},
                config.bad_blocks_storage,
                front_workchain_ids,
            )
        )
    )
}

#[allow(dead_code)]
#[cfg(feature = "external_db")]
pub fn create_external_db(config: ExternalDbConfig, front_workchain_ids: Vec<i32>) -> Result<Arc<dyn ExternalDb>> {
    Ok(
        Arc::new(
            Processor::new(
                kafka_producer::KafkaProducer::new(config.block_producer)?,
                kafka_producer::KafkaProducer::new(config.raw_block_producer)?,
                kafka_producer::KafkaProducer::new(config.message_producer)?,
                kafka_producer::KafkaProducer::new(config.transaction_producer)?,
                kafka_producer::KafkaProducer::new(config.account_producer)?,
                kafka_producer::KafkaProducer::new(config.block_proof_producer)?,
                kafka_producer::KafkaProducer::new(config.chain_range_producer)?,
                config.bad_blocks_storage,
                front_workchain_ids,
            )
        )
    )
}
