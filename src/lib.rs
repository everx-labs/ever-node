pub mod block;
pub mod block_proof;
pub mod boot;
pub mod config;
pub mod db;
pub mod error;
pub mod engine;
pub mod engine_traits;
pub mod engine_operations;
pub mod full_node;
pub mod macros;
pub mod network;
pub mod shard_state;
pub mod sync;
pub mod types;

#[cfg(feature = "tracing")]
pub mod jaeger;

#[cfg(not(feature = "tracing"))]
pub mod jaeger {
    pub fn init_jaeger(){}
    pub fn message_from_kafka_received(_kf_key: &[u8]) {}
    pub fn broadcast_sended(_msg_id: String) {}
}

#[cfg(feature = "external_db")]
mod external_db;


use std::sync::Arc;
use config::{NodeConfigHandler, TonNodeConfig};
use network::control::ControlServer;
use ton_types::Result;

pub struct LightEngine {
    _config_handler: Arc<NodeConfigHandler>,
    control: Option<ControlServer>
}

impl LightEngine {
    pub async fn with_config(node_config_path: &str) -> Result<Self> {
        let node_config = TonNodeConfig::from_file("target", node_config_path, None, "")?;
        let control_server_config = node_config.control_server();
        let config_handler = Arc::new(NodeConfigHandler::new(node_config)?);
        let control = match control_server_config {
            Ok(config) => Some(
                ControlServer::with_config(config, config_handler.clone(), config_handler.clone()).await?
            ),
            Err(e) => {
                log::warn!("{}", e);
                None
            }
        };

        Ok(Self {
            _config_handler: config_handler,
            control
        })
    }
    pub fn shutdown(mut self) {
        if let Some(control) = self.control.take() {
            control.shutdown()
        }
    }
}
