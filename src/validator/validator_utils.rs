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
use ton_api::ton::engine::validator::validator::groupmember::GroupMember;
use ton_block::{BlockSignatures, BlockSignaturesPure, CatchainConfig, ConfigParams, CryptoSignature, CryptoSignaturePair, Deserializable, Serializable, Message, ShardIdent, SigPubKey, UnixTime32, ValidatorBaseInfo, ValidatorDescr, ValidatorSet, Workchains, WorkchainDescr, GlobalCapabilities};
use ton_types::{BuilderData, Result, UInt256, HashmapType, fail, error};
use validator_session::SessionNode;
use crate::engine_traits::EngineOperations;


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

pub fn validatordescr_to_catchain_node(descr: &ValidatorDescr) -> CatchainNode {
    catchain::CatchainNode {
        adnl_id: get_adnl_id(descr),
        public_key: sigpubkey_to_publickey(&descr.public_key)
    }
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
pub fn compute_validator_list_id(list: &[ValidatorDescr], session_data: Option<(u32, u32, &ShardIdent)>) -> Result<Option<ValidatorListHash>> {
    if !list.is_empty() {
        let mut hasher = Sha256::new();
        if let Some((cc,master_cc,shard)) = session_data {
            hasher.update(cc.to_be_bytes());
            hasher.update(master_cc.to_be_bytes());
            let mut serialized = BuilderData::new();
            shard.write_to(&mut serialized)?;
            hasher.update(serialized.data());
        }
        for x in list {
            hasher.update(x.compute_node_id_short().as_slice());
        }
        let hash: [u8; 32] = hasher.finalize().into();
        Ok(Some(hash.into()))
    } else {
        Ok(None)
    }
}

// pub fn get_validator_key_idx_in_validator_set(key: &PublicKey, set: &ValidatorSet) -> Result<u32> {
//     let mut idx = 0;
//     for validator in set.list() {
//         let validator_key = sigpubkey_to_publickey(&validator.public_key);
//         if key.id() == validator_key.id() {
//             return Ok(idx);
//         }
//         idx += 1;
//     }
//     Err(failure::err_msg(format!("Key {} not found in validator set {:?}", key.id(), set)))
// }

pub fn get_validator_key_idx(public_key: &PublicKey, nodes: &Vec<CatchainNode>) -> Result<usize> {
    let key_id = public_key.id();
    match nodes.iter().position(|validator| validator.public_key.id() == key_id) {
        Some(idx) => Ok(idx),
        None => fail!("Key {} not found in validator list", key_id)
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

pub async fn get_shard_by_message(engine: Arc<dyn EngineOperations>, message: Arc<Message>) -> Result<ShardIdent> {
    let dst_wc = message.dst_workchain_id()
        .ok_or_else(|| error!("Can't get workchain id from message"))?;
    let dst_address = message.int_dst_account_id()
        .ok_or_else(|| error!("Can't get standart destination address from message"))?;

    // find account and related shard
    let (_account, shard) = engine.load_account(dst_wc, dst_address.clone()).await?;
    Ok(shard)
}

#[derive(PartialEq)]
pub struct GeneralSessionInfo {
    pub shard: ShardIdent,
    pub opts_hash: UInt256,
    pub catchain_seqno: u32,
    pub key_seqno: u32,
    pub max_vertical_seqno: u32,
}

impl std::fmt::Display for GeneralSessionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}, cc {}", self.shard, self.catchain_seqno)
    }
}

pub fn get_group_members_by_validator_descrs(iterator: &Vec<ValidatorDescr>, dst: &mut Vec<GroupMember>)  {
    for descr in iterator.iter() {
        let node_id = descr.compute_node_id_short();
        let adnl_id = descr.adnl_addr.clone().unwrap_or(node_id.clone());
        dst.push(ton_api::ton::engine::validator::validator::groupmember::GroupMember {
            public_key_hash: node_id,
            adnl: adnl_id,
            weight: descr.weight as i64,
        });
    };
}

pub fn is_remp_enabled(engine: Arc<dyn EngineOperations>, config_params: &ConfigParams) -> bool {

    return config_params.has_capability(GlobalCapabilities::CapRemp);
}
