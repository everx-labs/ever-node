/*
* Copyright (C) 2019-2022 TON Labs. All Rights Reserved.
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

extern crate catchain;

use crate::engine_traits::EngineOperations;
use crate::validator::BlockCandidate;
use std::collections::HashMap;
/// API dependencies
use std::sync::Arc;
use std::sync::Weak;
use ever_block::{BlockIdExt, KeyOption, Result, UInt256, ValidatorDescr};
use validator_session::PrivateKey;
use validator_session::PublicKeyHash;
use catchain::profiling::InstanceCounter;

mod block;
mod multi_signature_bls;
mod multi_signature_unsafe;
mod verification_manager;
mod workchain;
mod workchain_overlay;
mod utils;

pub const DEFAULT_USE_DEBUG_BLS_KEYS: bool = true; //generate missing BLS key from ED25519 public key (only for testing)
pub const USE_VALIDATORS_WEIGHTS: bool = false; //use weights from ValidatorDescr for BLS signature weight aggregation
pub const DEBUG_NACK_APPEARENCE: bool = false; //emulate appearance of NACK messages

/// Engine ptr
type EnginePtr = Arc<dyn EngineOperations>;

/// Verification manager pointer
pub type VerificationManagerPtr = Arc<dyn VerificationManager>;

/// Pointer to verification listener
pub type VerificationListenerPtr = Weak<dyn VerificationListener>;

/// Trait for verification events
#[async_trait::async_trait]
pub trait VerificationListener: Sync + Send {
    /// Verify block candidate
    async fn verify(&self, block_candidate: &BlockCandidate) -> bool;
}

/// Verficiation manager config
#[derive(Clone, Debug)]
pub struct VerificationManagerConfig {
    /// Max wait time for delivery of shardchain block to MC validator
    pub max_mc_delivery_wait_timeout: Option<std::time::Duration>,
}

/// Verification manager
#[async_trait::async_trait]
pub trait VerificationManager: Sync + Send {
    /// New block broadcast has been generated
    async fn send_new_block_candidate(&self, candidate: &BlockCandidate);

    /// Wait for block verification
    fn wait_for_block_verification(
        &self,
        block_id: &BlockIdExt,
        timeout: Option<std::time::Duration>,
    ) -> bool;

    /// Update workchains
    async fn update_workchains<'a>(
        &'a self,
        local_key_id: PublicKeyHash,
        local_bls_key: PrivateKey,
        workchain_id: i32,
        utime_since: u32,
        workchain_validators: &'a Vec<ValidatorDescr>,
        mc_validators: &'a Vec<ValidatorDescr>,
        listener: &'a VerificationListenerPtr,
        use_debug_bls_keys: bool,
    );
}

/// Factory for verification objects
pub struct VerificationFactory {}

impl VerificationFactory {
    /// Create new verification manager
    pub fn create_manager(engine: EnginePtr, runtime: tokio::runtime::Handle, config: VerificationManagerConfig) -> VerificationManagerPtr {
        verification_manager::VerificationManagerImpl::create(engine, runtime, config)
    }

    /// Generate test BLS key based on public key
    pub fn generate_test_bls_key(public_key: &Arc<dyn KeyOption>) -> Result<Arc<dyn KeyOption>> {
        utils::generate_test_bls_key(public_key)
    }
}
