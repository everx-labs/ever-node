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
use ton_block::BlockIdExt;
use ton_block::ValidatorDescr;
use ton_types::UInt256;
use validator_session::PrivateKey;
use validator_session::InstanceCounter;
use lazy_static::*;

mod block;
mod multi_signature_bls;
mod multi_signature_unsafe;
mod verification_manager;
mod workchain;
mod workchain_overlay;
mod utils;

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

/// Verification manager
#[async_trait::async_trait]
pub trait VerificationManager: Sync + Send {
    /// New block broadcast has been generated
    async fn send_new_block_candidate(&self, candidate: &BlockCandidate);

    /// Get block status (delivered, rejected)
    fn get_block_status(
        &self,
        block_id: &BlockIdExt,
        collated_data_file_hash: &UInt256,
        created_by: &UInt256,
    ) -> (bool, bool);

    /// Update workchains
    async fn update_workchains<'a>(
        &'a self,
        local_key: PrivateKey,
        local_bls_key: PrivateKey,
        workchain_id: i32,
        workchain_validators: &'a Vec<ValidatorDescr>,
        mc_validators: &'a Vec<ValidatorDescr>,
        listener: &'a VerificationListenerPtr,
    );
}

/// Factory for verification objects
pub struct VerificationFactory {}

impl VerificationFactory {
    /// Create new verification manager
    pub fn create_manager(engine: EnginePtr, runtime: tokio::runtime::Handle) -> VerificationManagerPtr {
        verification_manager::VerificationManagerImpl::create(engine, runtime)
    }
}
