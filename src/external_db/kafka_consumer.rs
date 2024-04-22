/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific EVERX DEV software governing permissions and
* limitations under the License.
*/

use crate::{
    engine_traits::{EngineOperations, Server}, config::KafkaConsumerConfig, jaeger
};
use futures::StreamExt;
use rdkafka::{consumer::Consumer, Message, message::BorrowedMessage};
use std::sync::Arc;
use std::time;
use stream_cancel::StreamExt as StreamCancelExt;
use ever_block::{Result, UInt256, fail, error};

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
            if self.engine.check_stop() {
                break Ok(())
            }
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
        let (trigger, tripwire) = stream_cancel::Tripwire::new();
        let mut message_stream = consumer.stream().take_until_if(tripwire);
        self.engine.register_server(Server::KafkaConsumer(trigger));

        while let Some(borrowed_message) = message_stream.next().await {
            if self.engine.check_stop() {
                break
            }
            let borrowed_message = borrowed_message?;
            let message_descr = format!(
                "topic: {}, key: {}, partition: {}, offset: {}", 
                borrowed_message.topic(),
                borrowed_message.key().map(|k| hex::encode(k)).unwrap_or_else(|| "<no key>".to_owned()),
                borrowed_message.partition(),
                borrowed_message.offset()
            );
            let now = std::time::Instant::now();
            log::trace!("Processing record, {}", message_descr);
            match self.process_message(&borrowed_message).await {
                Ok(_) => log::trace!("redirected external message {}", message_descr),
                Err(e) => log::error!("error while processing external message {}: {:?}", message_descr, e),
            };
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

    async fn process_message(&self, message: &BorrowedMessage<'_>) -> Result<()> {
        let key = message.key().ok_or_else(|| error!("Message must have key"))?;
        let key_len = key.len(); 
        if key_len < 32 {
            fail!("Message key length must be at least 32 (actual is {})", key_len);
        }
        let key = UInt256::from(key);

        if let Some(payload) = rdkafka::Message::payload(message) {
            self.engine.redirect_external_message(&payload, key).await?;
        } else {
            fail!("Message with empty payload {}", key);
        }

        Ok(())
    } 
}
