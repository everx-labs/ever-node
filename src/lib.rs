/*
* Copyright (C) 2019-2021 TON Labs. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific TON DEV software governing permissions and
* limitations under the License.
*/

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
    #[cfg(feature = "external_db")]
    pub fn message_from_kafka_received(_kf_key: &[u8]) {}
    pub fn broadcast_sended(_msg_id: String) {}
}

#[cfg(feature = "external_db")]
mod external_db;

