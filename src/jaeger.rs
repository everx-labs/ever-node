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

use rustracing::sampler::AllSampler;
use rustracing_jaeger::{
    reporter::JaegerCompactReporter, span::SpanContext, span::SpanReceiver, Tracer,
};
use std::net::ToSocketAddrs;
use std::{collections::HashMap, env, sync::Mutex};

use ever_block::fail;
use ever_block::types::*;

#[allow(dead_code)]
enum LogKind {
    Normal,
    Error,
}

struct JaegerHelper {
    tracer: Tracer,
    reporter: JaegerCompactReporter,
    span_rx: SpanReceiver,
}

lazy_static::lazy_static! {
    static ref JAEGER: Option<Mutex<JaegerHelper>> = JaegerHelper::new("r-node");
}

pub fn init_jaeger() {
    lazy_static::initialize(&JAEGER);
    log::trace!("Jaeger lazy init");
}

#[cfg(feature = "external_db")]
pub fn message_from_kafka_received(kf_key: &[u8]) {
    if let Some(jaeger) = JAEGER.as_ref() {
        let msg_id_bytes = kf_key[0..32].to_vec();
        tokio::task::spawn_blocking(move || match jaeger.lock() {
            Ok(mut helper) => {
                if msg_id_bytes.len() == 32 {
                    let msg_id = hex::encode(&msg_id_bytes);
                    helper.send_span(msg_id, "kafka msg received".to_string());
                } else {
                    log::error!(target: "jaeger", "Corrupted key field in message from q-server");
                }
            }
            Err(e) => {
                log::error!(target: "jaeger", "Mutex locking error: {}", e);
            }
        });
    }
}

pub fn broadcast_sended(msg_id: String) {
    if let Some(jaeger) = JAEGER.as_ref() {
        tokio::task::spawn_blocking(move || match jaeger.lock() {
            Ok(mut helper) => {
                helper.send_span(msg_id, "broadcast sended".to_string())
            },
            Err(e) => {
                log::error!(target: "jaeger", "Mutex locking error: {}", e);
            }
        });
    }
}

impl JaegerHelper {

    pub fn new(service_name: &str) -> Option<Mutex<JaegerHelper>> {
        let (span_tx, span_rx) = crossbeam_channel::bounded(1000);
        let tracer = Tracer::with_sender(AllSampler, span_tx);
        let mut reporter = match JaegerCompactReporter::new(service_name) {
            Ok(reporter) => reporter,
            Err(e) => {
                log::error!(target: "jaeger", "Can't create jaeger reporter: {}", e);
                panic!("Can't create jaeger reporter: {}", e)
            }
        };
        let agent_host = match env::var("JAEGER_AGENT_HOST") {
            Ok(val) => val,
            Err(_) => {
                log::info!(target: "jaeger", "JAEGER_AGENT_HOST not set. Use default value");
                "localhost".to_string()
            }
        };
        let agent_port = match env::var("JAEGER_AGENT_PORT") {
            Ok(val) => val,
            Err(_) => {
                log::info!(target: "jaeger", "JAEGER_AGENT_PORT not set. Use default value");
                "6831".to_string()
            }
        };
        let agent_url = match format!("{}:{}", agent_host, agent_port)
            .to_socket_addrs()
            .map(|mut iter| iter.next())
        {
            Ok(Some(url)) => url,
            _ => {
                log::error!(
                    target: "jaeger", 
                    "Invalid JAEGER_* env. Can't parse string to valid address"
                );
                return None
            }
        };
        match reporter.set_agent_addr(agent_url) {
            Ok(_) => log::info!(
                target: "jaeger", 
                "Init done with addr {}:{}", agent_host, agent_port
            ),
            Err(e) => log::error!(
                target: "jaeger", 
                "Can't set agent address to jaeger library. Internal rust_jaegertracing error: {}",
                e
            )
        }
        let ret = JaegerHelper {
            tracer,
            reporter,
            span_rx,
        };
        Some(Mutex::new(ret))
    }

    pub fn send_span(&mut self, msg_id: String, span_name: String) {
        match self.create_root_span(msg_id) {
            Ok(span_root) => {
                self.start_span(span_root, span_name);
                self.report_span();
            }
            Err(e) => {
                log::error!(target: "jaeger", "Error: {}", e);
            }
        }
    }

    fn create_root_span(&mut self, msg_id: String) -> Result<SpanContext> {
        let mut carrier = HashMap::new();
        let span_ctx = format!("{}:{}:0:1", &msg_id[0..16], &msg_id[16..32]);
        carrier.insert("uber-trace-id".to_string(), span_ctx);
        if let Ok(Some(ctx)) = SpanContext::extract_from_text_map(&carrier) {
            Ok(ctx)
        } else {
            fail!("Can't extract root span context from textmap")
        }
    }

    fn start_span(&mut self, ctx: SpanContext, name: String) {
        let _span = self.tracer.span(name).child_of(&ctx).start();
        log::trace!(target: "jaeger", "Span started");
    }

    fn report_span(&mut self) {
        if let Err(e) = self
            .reporter
            .report(&(self.span_rx).try_iter().collect::<Vec<_>>())
        {
            log::error!(target: "jaeger", "Can't report span. Internal rustracing_jaeger crate error in reporter: {}", e);
        }
    }
}
