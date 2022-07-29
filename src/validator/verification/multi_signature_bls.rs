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
use ton_types::Result;
use validator_session::ValidatorWeight;
use ever_bls_lib::bls::get_nodes_info_from_sig;
use ever_bls_lib::bls::sign_and_add_node_info;
use ever_bls_lib::bls::aggregate_two_bls_signatures;
use ever_bls_lib::bls::aggregate_public_keys_based_on_nodes_info;
use ever_bls_lib::bls::verify;
use ever_bls_lib::bls::NodesInfo;
use ever_bls_lib::bls::BLS_PUBLIC_KEY_LEN;
use ever_bls_lib::bls::BLS_SECRET_KEY_LEN;
use ever_bls_lib::bls::truncate_nodes_info_from_sig;

/*
===============================================================================
    MultiSignature
===============================================================================
*/

#[derive(Clone)]
pub struct MultiSignature {
    hash: u32,          //hash of signature
    msg: Vec<u8>,       //serialized block candidate for signing
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

        write!(f, "[???]")
    }
}

impl MultiSignature {
    /// Add node to signature
    pub fn sign(&mut self, local_key: &PrivateKey, idx: u16, nodes_count: u16) -> Result<()> {
        let sk_bytes = local_key.export_key()?;
        let sk_bytes: [u8; BLS_SECRET_KEY_LEN] = sk_bytes.to_vec().try_into().unwrap_or_else(|v: Vec<u8>| panic!("Expected a Vec of length {} but it was {}", BLS_SECRET_KEY_LEN, v.len()));

        self.signature = sign_and_add_node_info(&sk_bytes, &self.msg, idx, nodes_count)?;

        self.update_hash();

        Ok(())
    }

    /// Merge signatures
    pub fn merge(&mut self, other: &MultiSignature) -> Result<()> {
        if self.signature.len() == 0 {
            self.signature = other.signature.clone()
        } else if other.signature.len() == 0 {
            return Ok(());
        } else {
            self.signature = aggregate_two_bls_signatures(&self.signature, &other.signature)?;
        }

        self.update_hash();

        Ok(())
    }

    /// Get hash
    pub fn get_hash(&self) -> u32 {
        self.hash
    }

    /// Is the signature empty
    pub fn empty(&self) -> bool {
        self.signature.len() == 0
    }

    /// Total weight
    pub fn get_total_weight(&self, validators: &Vec<ValidatorDescr>) -> ValidatorWeight {
        let mut total_weight = 0;

        if let Ok(nodes_info) = get_nodes_info_from_sig(&self.signature) {
            if let Ok(nodes_info) = NodesInfo::deserialize(&nodes_info) {
                for (validator_idx, _number_of_occurrence) in &nodes_info.map {
                    if *validator_idx >= validators.len() as u16 {
                        continue;
                    }

                    total_weight += validators[*validator_idx as usize].weight;
                }
            }
        }

        total_weight
    }

    /// Update hash
    fn update_hash(&mut self) {
        self.hash = crc32c::crc32c(&self.signature)
    }

    /// Serialize signature
    pub fn serialize(&self) -> Vec<u8> {
        deflate::deflate_bytes(&self.signature)
    }

    /// Deserialize signature
    pub fn deserialize(type_id: u8, candidate_id: &UInt256, wc_pub_key_refs: &Vec<&[u8; BLS_PUBLIC_KEY_LEN]>, serialized_signature: &[u8]) -> Result<Self> {
        let signature = inflate::inflate_bytes(serialized_signature);

        if let Err(err) = signature {
            failure::bail!("inflate error: {}", err);
        }

        let signature = signature.unwrap();

        let msg: [u8; 32] = candidate_id.clone().into();
        let mut msg = msg.to_vec();

        msg.push(type_id);

        let mut body = Self {
            msg,
            signature: signature.clone(),
            hash: 0,
        };

        body.update_hash();

        let nodes_info = get_nodes_info_from_sig(&signature)?;
        let aggregated_pub_key = aggregate_public_keys_based_on_nodes_info(wc_pub_key_refs, &nodes_info)?;
        let signature = truncate_nodes_info_from_sig(&signature)?;

        if !verify(&signature, &body.msg, &aggregated_pub_key)? {
            failure::bail!("Can't verify block candidate {} signature {:?} (type {})", candidate_id, signature, type_id);
        }

        Ok(body)
    }

    /// Create new signature
    pub fn new(type_id: u8, candidate_id: UInt256) -> Self {
        let msg: [u8; 32] = candidate_id.clone().into();
        let mut msg = msg.to_vec();

        msg.push(type_id);        

        let mut body = Self {
            msg,
            signature: Vec::new(),
            hash: 0,
        };

        body.update_hash();

        body
    }
}
