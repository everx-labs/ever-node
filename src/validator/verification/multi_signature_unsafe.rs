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
use ever_block::{crc32_digest, error, Result, bls::BLS_PUBLIC_KEY_LEN};
use validator_session::ValidatorWeight;

/*
===============================================================================
    MultiSignature
===============================================================================
*/

#[derive(Clone)]
pub struct MultiSignature {
    hash: u32,       //hash of signature
    nodes: Vec<u16>, //vector of node indexes which signed (fake, for testing only, should be replace with BLS)
}

impl std::fmt::Debug for MultiSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{:?}]", self.nodes)
    }
}

//TODO: enable dead-code warnings after merging usafe with BLS implementations
#[allow(dead_code)]
impl MultiSignature {
    /// Add node to signature
    pub fn sign(&mut self, _local_key: &PrivateKey, idx: u16, _nodes_count: u16) -> Result<()> {
        match self.nodes.binary_search(&idx) {
            Ok(_pos) => {} // element already in vector
            Err(pos) => {
                self.nodes.insert(pos, idx);
                self.update_hash();
            }
        }

        Ok(())
    }

    /// Merge signatures
    pub fn merge(&mut self, other: &MultiSignature) -> Result<()> {
        let a = &self.nodes;
        let b = &other.nodes;
        let (mut i, mut j) = (0, 0);
        let mut sorted = vec![];
        let remaining;
        let remaining_idx;
        loop {
            if i == a.len() {
                remaining = b;
                remaining_idx = j;
                break;
            }

            if j == b.len() {
                remaining = a;
                remaining_idx = i;
                break;
            }

            if a[i] <= b[j] {
                sorted.push(a[i]);

                if a[i] == b[j] {
                    j += 1;
                }

                i += 1;
            } else {
                sorted.push(b[j]);
                j += 1;
            }
        }

        sorted.extend_from_slice(&remaining[remaining_idx..]);

        if self.nodes.len() != sorted.len() {
            //new elements appeared

            if sorted.len() == other.nodes.len() {
                self.nodes = other.nodes.clone();
                self.hash = other.hash;
            } else {
                self.nodes = sorted;
                self.update_hash();
            }
        }

        Ok(())
    }

    /// Get hash
    pub fn get_hash(&self) -> u32 {
        self.hash
    }

    /// Is the signature empty
    pub fn empty(&self) -> bool {
        self.nodes.is_empty()
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

    /// Get bytes for hash computation
    fn raw_byte_access(s16: &[u16]) -> &[u8] {
        unsafe { std::slice::from_raw_parts(s16.as_ptr() as *const u8, s16.len() * 2) }
    }

    /// Get words from bytes
    fn raw_words_access(s8: &[u8]) -> &[u16] {
        unsafe { std::slice::from_raw_parts(s8.as_ptr() as *const u16, s8.len() / 2) }
    }

    /// Update hash
    fn update_hash(&mut self) {
        self.hash = crc32_digest(Self::raw_byte_access(&self.nodes))
    }

    /// Serialize signature
    pub fn serialize(&self) -> Vec<u8> {
        deflate::deflate_bytes(Self::raw_byte_access(&self.nodes))
    }

    /// Deserialize signature
    pub fn deserialize(_type: u8, _candidate_id: &UInt256, _wc_pub_key_refs: &[&[u8; BLS_PUBLIC_KEY_LEN]], serialized_signature: &[u8]) -> Result<Self> {
        let decoded = inflate::inflate_bytes(serialized_signature)
            .map_err(|err| error!("inflate error: {}", err))?;

        let decoded = Self::raw_words_access(&decoded);
        let mut body = Self {
            nodes: decoded.into(),
            hash: 0,
        };

        body.update_hash();

        Ok(body)
    }

    /// Create new signature
    pub fn new(_type: u8, _candidate_id: UInt256) -> Self {
        let mut body = Self {
            nodes: Vec::new(),
            hash: 0,
        };

        body.update_hash();

        body
    }
}
