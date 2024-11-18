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

use super::*;
use ever_block::{
    crc32_digest, fail, Result,
    bls::{
        aggregate_two_bls_signatures, aggregate_public_keys_based_on_nodes_info, 
        BLS_PUBLIC_KEY_LEN, BLS_SECRET_KEY_LEN, get_nodes_info_from_sig, NodesInfo, 
        sign_and_add_node_info, truncate_nodes_info_and_verify
    }
};
use std::time::SystemTime;
use std::time::Duration;
use validator_session::ValidatorWeight;
use catchain::check_execution_time;

/*
    Constants
*/

const BLS_DESERIALIZE_WARN_DELAY: Duration = Duration::from_millis(50); //delay for BLS signatures warnings dump

/*
===============================================================================
    MultiSignature
===============================================================================
*/

#[derive(Clone)]
pub struct MultiSignature {
    hash: u32,          //hash of signature
    msg: Vec<u8>,       //serialized block candidate for signing
    nodes: Vec<u16>,    //signers
    signature: Vec<u8>, //signature
}

impl std::fmt::Debug for MultiSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(nodes_info) = get_nodes_info_from_sig(&self.signature) {
            if let Ok(nodes_info) = NodesInfo::deserialize(&nodes_info) {
                write!(f, "[")?;

                let mut first = true;

                for (index, number_of_occurrence) in &nodes_info.map {
                    if !first {
                        write!(f, ", ")?;
                    } else {
                        first = false;
                    }

                    write!(f, "{}x{}", index, number_of_occurrence)?;
                }

                return write!(f, "]");
            }
        }

        write!(f, "[]")
    }
}

impl MultiSignature {
    /// Add node to signature
    pub fn sign(&mut self, local_key: &PrivateKey, idx: u16, nodes_count: u16) -> Result<()> {
        let sk_bytes = local_key.export_key()?;
        let sk_bytes: [u8; BLS_SECRET_KEY_LEN] = sk_bytes.to_vec().try_into().unwrap_or_else(|v: Vec<u8>| panic!("Expected a Vec of length {} but it was {}", BLS_SECRET_KEY_LEN, v.len()));

        self.merge_impl(&sign_and_add_node_info(&sk_bytes, &self.msg, idx, nodes_count)?)
    }

    /// Merge signatures
    pub fn merge(&mut self, other: &MultiSignature) -> Result<()> {
        assert!(self.msg == other.msg);
        self.merge_impl(&other.signature)
    }

    /// Merge signatures (internal)
    fn merge_impl(&mut self, other_signature: &[u8]) -> Result<()> {
        check_execution_time!(5_000);
        if self.signature.is_empty() {
            self.signature = other_signature.to_vec();

            self.update_hash();

            Ok(())
        } else if other_signature.is_empty() {
            return Ok(());
        } else {
            let new_signature = aggregate_two_bls_signatures(&self.signature, other_signature)?;
            let (new_nodes, new_hash) = Self::compute_nodes_and_hash(&new_signature);

            if new_hash != self.hash {
                self.signature = new_signature;
                self.nodes = new_nodes;
                self.hash = new_hash;
            }

            return Ok(());
        }
    }

    /// Get hash
    pub fn get_hash(&self) -> u32 {
        self.hash
    }

    /// Is the signature empty
    pub fn empty(&self) -> bool {
        self.signature.is_empty()
    }

    /// Check if node signature is present
    pub fn is_signed_by_node(&self, idx: usize) -> bool {
        self.nodes.contains(&(idx as u16))
    }

    /// Total weight
    pub fn get_total_weight(&self, validators: &[ValidatorDescr]) -> ValidatorWeight {
        let mut total_weight = 0;

        for validator_idx in &self.nodes {
            let validator_idx = *validator_idx as usize;
            if validator_idx >= validators.len() {
                continue;
            }

            total_weight += validators[validator_idx].weight;
        }

        total_weight
    }

    /// Compute nodes and hash
    fn compute_nodes_and_hash(signature: &[u8]) -> (Vec<u16>, u32) {
        check_execution_time!(1_000);
        let mut new_nodes = Vec::new();

        if !signature.is_empty() {
            if let Ok(nodes_info) = get_nodes_info_from_sig(signature) {
                if let Ok(nodes_info) = NodesInfo::deserialize(&nodes_info) {
                    for (validator_idx, number_of_occurrence) in &nodes_info.map {
                        if *number_of_occurrence < 1 {
                            continue;
                        }

                        new_nodes.push(*validator_idx);
                    }

                    new_nodes.sort();
                }
            }
        }

        let hash = crc32_digest(Self::raw_byte_access(&new_nodes));

        (new_nodes, hash)
    }

    /// Update hash
    fn update_hash(&mut self) {
        (self.nodes, self.hash) = Self::compute_nodes_and_hash(&self.signature);
    }

    /// Get bytes for hash computation
    fn raw_byte_access(s16: &[u16]) -> &[u8] {
        unsafe { std::slice::from_raw_parts(s16.as_ptr() as *const u8, s16.len() * 2) }
    }

    /// Serialize signature
    pub fn serialize(&self) -> Vec<u8> {
        //deflate::deflate_bytes(&self.signature)
        self.signature.clone()
    }

    /// Deserialize signature
    pub fn deserialize(type_id: u8, candidate_id: &UInt256, wc_pub_key_refs: &[&[u8; BLS_PUBLIC_KEY_LEN]], serialized_signature: &[u8]) -> Result<Self> {
        check_execution_time!(20_000);

        // let signature = inflate::inflate_bytes(serialized_signature)
        //     .map_err(|err| error!("inflate error: {}", err))?;
        let signature = serialized_signature.to_vec();

        let mut body = Self::new(type_id, candidate_id.clone());

        body.signature = signature.clone();

        body.update_hash();

        if !signature.is_empty() {
            let start_time = SystemTime::now();
            let nodes_info = get_nodes_info_from_sig(&signature)?;
            let aggregated_pub_key = aggregate_public_keys_based_on_nodes_info(wc_pub_key_refs, &nodes_info)?;

            if !truncate_nodes_info_and_verify(&signature, &aggregated_pub_key, &body.msg)? {
                fail!("Can't verify block candidate {:?} signature {:?} (type {})", candidate_id, body, type_id);
            }

            let processing_delay = start_time.elapsed().unwrap_or_default();

            if processing_delay > BLS_DESERIALIZE_WARN_DELAY {
                log::warn!(target: "verificator", "Long BLS deserialization latency={:.3}s for signature={:?}", processing_delay.as_secs_f64(), body);
            }
        }

        Ok(body)
    }

    /// Create new signature
    pub fn new(type_id: u8, candidate_id: UInt256) -> Self {
        let msg: [u8; 32] = *candidate_id.as_array();
        let mut msg = msg.to_vec();

        msg.push(type_id);        

        let mut body = Self {
            msg,
            signature: Vec::new(),
            nodes: Vec::new(),
            hash: 0,
        };

        body.update_hash();

        body
    }
}
