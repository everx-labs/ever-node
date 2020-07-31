use crate::{
    engine_traits::EngineOperations, config::KafkaConsumerConfig, jaeger
};
use futures::StreamExt;
use rdkafka::consumer::Consumer;
use rdkafka::Message;
use std::sync::Arc;
use std::time;
use ton_types::Result;

pub struct KafkaConsumer {
    config: KafkaConsumerConfig,
    engine: Arc<dyn EngineOperations>
}

impl KafkaConsumer {
    pub fn new(config: KafkaConsumerConfig, engine: Arc<dyn EngineOperations>) -> Result<Self> {
        Ok(Self { config, engine })
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            if let Err(err) = self.run_attempt().await {
                log::trace!("Error while \"{}\" topic processing: {}",  self.config.topic, err);
                futures_timer::Delay::new(
                    time::Duration::from_millis(
                        self.config.run_attempt_timeout_ms as u64
                    )
                ).await;
            }
        }
    }

    pub async fn run_attempt(&self) -> Result<()> {

        log::trace!("Creating consumer...");

        let consumer: rdkafka::consumer::stream_consumer::StreamConsumer 
            = rdkafka::config::ClientConfig::new()
                .set("group.id", &self.config.group_id)
                .set("bootstrap.servers", &self.config.brokers)
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", &self.config.session_timeout_ms.to_string())
                .set("enable.auto.commit", "false")
                .create()?;

        log::trace!("Subscribing...");
        consumer.subscribe(&[&self.config.topic])?;

        log::trace!("Starting consumer...");
        let mut message_stream = consumer.start();
        while let Some(borrowed_message) = message_stream.next().await {
            let borrowed_message = borrowed_message?;
            let message_descr = format!("topic: {}, partition: {}, offset: {}", 
                borrowed_message.topic(), borrowed_message.partition(), borrowed_message.offset());
            let now = std::time::Instant::now();
            if let Some(payload) = rdkafka::Message::payload(&borrowed_message) {
                log::trace!("Processing record, {:?}", payload);
                
                let count = self.engine.redirect_external_message(&payload).await?;
                log::trace!("count number of nodes to broadcast to: {}", count);
            } else {
                log::warn!("Record with empty payload, {}", message_descr);
            }

            consumer.commit_message(&borrowed_message, rdkafka::consumer::CommitMode::Async)?;
            log::trace!("Processed record, {}, time: {} mcs", message_descr, now.elapsed().as_micros());
            if let Some(msg_key) = rdkafka::Message::key(&borrowed_message){
                jaeger::message_from_kafka_received(msg_key);
            } else {
                log::error!(target: "jaeger", "Can't read key from record");
            }
        }

        Ok(())
    }
}
