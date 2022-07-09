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
use ever_crypto::{Ed25519KeyOption, KeyId};
use catchain::{BlockPayloadPtr, PublicKey, PublicKeyHash, CatchainNode};
use std::sync::Arc;
use std::collections::HashMap;
use sha2::{Digest, Sha256};
use ton_block::{
    Deserializable,
    BlockSignatures, BlockSignaturesPure, CatchainConfig, ConfigParams,
    CryptoSignature, CryptoSignaturePair,
    ShardIdent, SigPubKey,
    UnixTime32, ValidatorBaseInfo, ValidatorDescr, ValidatorSet,
    Workchains, WorkchainDescr,
};
use ton_types::{Result, UInt256, HashmapType, fail};
use validator_session::SessionNode;


pub fn sigpubkey_to_publickey(k: &SigPubKey) -> PublicKey {
    Ed25519KeyOption::from_public_key(k.key_bytes())
}

pub fn make_cryptosig(s: BlockPayloadPtr) -> Result<CryptoSignature> {
    return CryptoSignature::from_bytes(s.data().0.as_slice());
}

pub fn make_cryptosig_pair(
    pair: (PublicKeyHash, BlockPayloadPtr),
) -> Result<CryptoSignaturePair> {
    let csig = make_cryptosig(pair.1)?;
    return Ok(CryptoSignaturePair::with_params(pair.0.data().into(), csig));
}

pub fn pairvec_to_cryptopair_vec(
    vec: Vec<(PublicKeyHash, BlockPayloadPtr)>,
) -> Result<Vec<CryptoSignaturePair>> {
    return vec.into_iter().
        map(|p| make_cryptosig_pair(p)).
        collect();
}

#[allow(dead_code)]
pub fn pairvec_to_puresigs(
    pvec: Vec<(PublicKeyHash, BlockPayloadPtr)>,
) -> Result<BlockSignaturesPure> {
    let mut pure_sigs = BlockSignaturesPure::new();
    for p in pvec {
        let pair = make_cryptosig_pair(p)?;
        pure_sigs.add_sigpair(pair);
    }
    return Ok(pure_sigs);
}

#[allow(dead_code)]
pub fn pairvec_val_to_sigs(
    pvec: Vec<(PublicKeyHash, BlockPayloadPtr)>,
    vset: &ValidatorSet,
) -> Result<BlockSignatures> {
    let pure_sigs = pairvec_to_puresigs(pvec)?;
    let vset_catchain_seqno = vset.catchain_seqno();
    let vset_hash = ValidatorSet::calc_subset_hash_short(vset.list(), vset_catchain_seqno)?;
    let vset_info = ValidatorBaseInfo::with_params(vset_hash, vset_catchain_seqno);
    return Ok(BlockSignatures::with_params(vset_info, pure_sigs));
}

pub fn check_crypto_signatures(signatures: &BlockSignaturesPure, validators_list: &[ValidatorDescr], data: &[u8]) -> Result<u64> {
    // Calc validators short ids
    let validators_map = validators_list.iter().map(|desc| {
        let key = Ed25519KeyOption::from_public_key(desc.public_key.as_slice()).id().clone();
        (key, desc)
    }).collect::<HashMap<_, _>>();
    // Check signatures
    let mut weight = 0;
    signatures.signatures().iterate_slices(|_key, ref mut slice| {
        let sign = CryptoSignaturePair::construct_from(slice)?;
        let key = KeyId::from_data(sign.node_id_short.inner());
        if let Some(vd) = validators_map.get(&key) {
            if !vd.public_key.verify_signature(data, &sign.sign) {
                fail!("bad signature from validator with pub_key {}", key)
            }
            weight += vd.weight;
        }
        Ok(true)
    })?;
    Ok(weight)
}

pub fn validatordescr_to_catchain_node(descr: &ValidatorDescr) -> Result<CatchainNode> {
    Ok(catchain::CatchainNode {
        adnl_id: get_adnl_id(descr),
        public_key: sigpubkey_to_publickey(&descr.public_key)
    })
}

pub fn validatordescr_to_session_node(descr: &ValidatorDescr) -> ton_types::Result<SessionNode> {
    Ok(validator_session::SessionNode {
        adnl_id: get_adnl_id(descr),
        public_key: sigpubkey_to_publickey(&descr.public_key),
        weight: descr.weight
    })
}

pub fn validator_query_candidate_to_validator_block_candidate(
        source: PublicKey, candidate: super::BlockCandidate
    ) -> validator_session::ValidatorBlockCandidate
{
    validator_session::ValidatorBlockCandidate {
        public_key: source,
        id: validator_session::ValidatorBlockId {
            root_hash: candidate.block_id.root_hash,
            file_hash: candidate.block_id.file_hash,
        },
        collated_file_hash: candidate.collated_file_hash,
        data: catchain::CatchainFactory::create_block_payload(candidate.data.into()),
        collated_data: catchain::CatchainFactory::create_block_payload(candidate.collated_data.into()),
    }
}

pub fn validatorset_to_string(vs: &ValidatorSet) -> String {
    let mut res = string_builder::Builder::default();
    let vs_list = vs.list();
    for i in 0..vs_list.len() {
        if let Some(x) = vs_list.get(i) {
            let adnl = x.adnl_addr.clone().map_or("** no-addr **".to_string(), |x| x.to_hex_string());
            res.append(format!("val_set.{}.pk = {} val_set.{}.weigth = {} val_set.{}.addr = {} ",
                               i, hex::encode(x.public_key.key_bytes()), i, x.weight, i, adnl
            ));
        }
    }
    res.string().unwrap_or_default()
}

// returns adnl_id of validator or calc it by the 
pub fn get_adnl_id(validator: &ValidatorDescr) -> Arc<KeyId> {
    if let Some(addr) = &validator.adnl_addr {
        KeyId::from_data(*addr.as_slice())
    } else {
        KeyId::from_data(validator.compute_node_id_short().inner())
    }
}

pub type ValidatorListHash = UInt256;

/// compute sha256 for hashes of public keys of all validators
pub fn compute_validator_list_id(list: &[ValidatorDescr]) -> Option<ValidatorListHash> {
    if !list.is_empty() {
        let mut hasher = Sha256::new();
        for x in list {
            hasher.update(x.compute_node_id_short().as_slice());
        }
        let hash: [u8; 32] = hasher.finalize().into();
        Some(hash.into())
    } else {
        None
    }
}

pub fn compute_validator_set_cc(
    config: &ConfigParams,
    shard: &ShardIdent,
    at: u32,
    cc_seqno: u32,
    cc_seqno_delta: &mut u32
) -> Result<Vec<ValidatorDescr>> {
    let vset = config.validator_set()?;
    let ccc = config.catchain_config()?;
    if (*cc_seqno_delta & 0xfffffffe) != 0 {
        fail!("seqno_delta>1 is not implemented yet");
    }
    *cc_seqno_delta += cc_seqno;
    let (set, _hash) = calc_subset_for_workchain(
        &vset,
        config,
        &ccc,
        shard.shard_prefix_with_tag(),
        shard.workchain_id(),
        *cc_seqno_delta,
        at.into()
    )?;
    Ok(set)
}

#[cfg(feature="workchains")]
fn calc_workchain_id(descr: &ValidatorDescr) -> i32 {
    calc_workchain_id_by_adnl_id(descr.compute_node_id_short().as_slice())
}

#[cfg(feature="workchains")]
fn calc_workchain_id_by_adnl_id(adnl_id: &[u8]) -> i32 {
    (adnl_id[0] % 32) as i32 - 1
}

lazy_static::lazy_static! {
    static ref SINGLE_WORKCHAIN: Workchains = {
        let mut workchains = Workchains::default();
        workchains.set(&0, &WorkchainDescr::default()).unwrap();
        workchains
    };
}

pub fn try_calc_subset_for_workchain(
    vset: &ValidatorSet,
    config: &ConfigParams,
    cc_config: &CatchainConfig, 
    shard_pfx: u64, 
    workchain_id: i32, 
    cc_seqno: u32,
    _time: UnixTime32,
) -> Result<Option<(Vec<ValidatorDescr>, u32)>> {
    // in case on old block proof it doesn't contain workchains in config so 1 by default
    let workchains = config.workchains().unwrap_or_else(|_| SINGLE_WORKCHAIN.clone());
    match workchains.len()? as i32 {
        0 => fail!("workchain description is empty"),
        1 => Ok(Some(vset.calc_subset(cc_config, shard_pfx, workchain_id, cc_seqno, _time)?)),
        #[cfg(not(feature="workchains"))]
        _ => {
            fail!("workchains not supported")
        }
        #[cfg(feature="workchains")]
        count => {
            let mut list = Vec::new();
            for descr in vset.list() {
                let id = calc_workchain_id(descr);
                if (id == workchain_id) || (id >= count) {
                    list.push(descr.clone());
                }
            }
            if list.len() >= cc_config.shard_validators_num as usize {
                let vset = ValidatorSet::new(
                    vset.utime_since(),
                    vset.utime_until(),
                    vset.main(),
                    list
                )?;
                Ok(Some(vset.calc_subset(cc_config, shard_pfx, workchain_id, cc_seqno, _time)?))
            } else {
                // not enough validators -- config is ok, but we cannot validate the shard at the moment
                Ok(None)
            }
        }
    }
}

pub fn calc_subset_for_workchain(
    vset: &ValidatorSet,
    config: &ConfigParams,
    cc_config: &CatchainConfig,
    shard_pfx: u64,
    workchain_id: i32,
    cc_seqno: u32,
    time: UnixTime32,
) -> Result<(Vec<ValidatorDescr>, u32)> {
    match try_calc_subset_for_workchain(vset, config, cc_config, shard_pfx, workchain_id, cc_seqno, time)? {
        Some(x) => Ok(x),
        None =>
            fail!(
                "Not enough validators from total {} for workchain {}:{:016X} cc_seqno: {}",
                vset.list().len(), workchain_id, shard_pfx, cc_seqno
            )
    }
}

#[cfg(feature="workchains")]
pub fn mine_key_for_workchain(id_opt: Option<i32>) -> (ever_crypto::KeyOptionJson, Arc<dyn ever_crypto::KeyOption>) {
    loop {
        if let Ok((private, public)) = Ed25519KeyOption::generate_with_json() {
            if id_opt.is_none() || Some(calc_workchain_id_by_adnl_id(public.id().data())) == id_opt {
                return (private, public)
            }
        }
    }
}
