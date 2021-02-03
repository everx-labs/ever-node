pub mod block;
pub mod block_proof;
pub mod boot;
pub mod collator_test_bundle;
pub mod config;
pub mod error;
pub mod engine;
pub mod engine_traits;
pub mod engine_operations;
pub mod ext_messages;
pub mod full_node;
pub mod internal_db;
pub mod macros;
pub mod network;
pub mod out_msg_queue;
pub mod rng;
pub mod shard_blocks;
pub mod shard_state;
pub mod sync;
pub mod types;
pub mod validating_utils;
pub mod validator;

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


