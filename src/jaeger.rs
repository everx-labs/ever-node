use rustracing::sampler::AllSampler;
use rustracing_jaeger::{
    reporter::JaegerCompactReporter,
    span::SpanContext,
    span::SpanReceiver,
    Tracer,
};
use std::{
    collections::HashMap,
    env, 
    net::ToSocketAddrs,
    sync::Mutex,
};
use base64::decode;

use ton_types::types::*;
use ton_types::{Result, fail};

#[allow(dead_code)]
enum LogKind {
    Normal,
    Error
}

struct JaegerHelper {
    tracer: Tracer,
    reporter: JaegerCompactReporter,
    span_rx: SpanReceiver
}

lazy_static::lazy_static! {
    static ref JAEGER: Mutex<JaegerHelper> = Mutex::new(JaegerHelper::new("r-node"));
}

pub fn init_jaeger(){
    lazy_static::initialize(&JAEGER);
    log::trace!("Jaeger lazy init");
}

pub fn message_from_kafka_received(kf_key: &[u8]) {
    let msg_id_b64 = kf_key.to_owned();
    tokio::task::spawn_blocking(move || {
        match JAEGER.lock() {
            Ok(mut helper) => {
                if let Ok(msg_id_bytes) = decode(&msg_id_b64) {
                    if msg_id_bytes.len() == 32 {
                        let msg_id = UInt256::from(msg_id_bytes).to_hex_string();
                        helper.send_span(msg_id, "kafka msg received".to_string());
                    }
                } 
                log::error!(target: "jaeger", "Corrupted key field in message from q-server");
            }
            Err(e) => { log::error!(target: "jaeger", "Mutex locking error: {}", e); }
        }
    });
}

pub fn broadcast_sended(msg_id: String) {
    tokio::task::spawn_blocking(move || {
        match JAEGER.lock() {
            Ok(mut helper) => {
                helper.send_span(msg_id, "broadcast sended".to_string());
            }
            Err(e) => { log::error!(target: "jaeger", "Mutex locking error: {}", e); }
        }
    });
}

impl JaegerHelper {
    pub fn new(service_name: &str) -> JaegerHelper {
        let (span_tx, span_rx) = crossbeam_channel::bounded(1000);
        let tracer = Tracer::with_sender(AllSampler, span_tx);
        let mut reporter = match JaegerCompactReporter::new(service_name){
            Ok(reporter) => reporter,
            Err(_) => panic!("Can't create socket to localhost")
        };
        let agent_host = match env::var("JAEGER_AGENT_HOST") {
            Ok(val) => val,
            Err(_) => "localhost".to_string()
        };
        let agent_port = match env::var("JAEGER_AGENT_PORT") {
            Ok(val) => val,
            Err(_) => "6831".to_string()
        };
        let mut addr = format!("{}:{}", agent_host, agent_port).to_socket_addrs().
            expect("Invalid JAEGER_* env");
        match reporter.set_agent_addr(addr.next().expect("Invalid JAEGER_* env")) {
            Ok(_) => { log::error!(target: "jaeger", "Init done with addr: "); }
            Err(e) => { log::error!(target: "jaeger", "Internal rust_jaegertracing error: {}", e); }
        }
        JaegerHelper {
            tracer, 
            reporter,
            span_rx
        }
    }
    
    pub fn send_span(&mut self, msg_id: String, span_name: String){
        match self.create_root_span(msg_id) {
            Ok(span_root) => {
                self.start_span(span_root, span_name);
                self.report_span();
            }
            Err(e) => { log::error!(target: "jaeger", "Error: {}", e); }
        }
    }
    
    fn create_root_span(&mut self, msg_id: String) -> Result<SpanContext> {
        let mut carrier = HashMap::new();
        let span_ctx = format!("{}:{}:0:1", &msg_id[0..16], &msg_id[16..32]);
        carrier.insert(
            "uber-trace-id".to_string(),
            span_ctx.to_string());
        if let Ok(Some(ctx)) = SpanContext::extract_from_text_map(&carrier) {
            Ok(ctx)
        } else {
            fail!("Can't extract root span context from textmap")
        }
    }
    
    fn start_span(&mut self, ctx: SpanContext, name: String) {
        let _span = self.tracer
            .span(name)
            .child_of(&ctx)
            .start();
        log::trace!(target: "jaeger", "Span started");
    }
   
    fn report_span(&mut self){
        if let Err(e) = self.reporter.report(&(self.span_rx).try_iter().collect::<Vec<_>>()){
            log::error!(target: "jaeger", "Internal rustracing_jaeger crate error in reporter. Desc: {}", e); 
        }
    }
}

