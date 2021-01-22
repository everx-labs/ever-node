use catchain::{BlockPayloadPtr, PublicKey, PublicKeyHash, CatchainNode};
use std::sync::*;
use sha2::{Digest, Sha256};
use ton_block::{
    signature::{
        BlockSignatures, BlockSignaturesPure, CryptoSignature, CryptoSignaturePair, SigPubKey,
    },
    ValidatorBaseInfo, ValidatorSet, ValidatorDescr, ShardIdent
};
use ton_types::types::UInt256;
use validator_session::SessionNode;

pub fn sigpubkey_to_publickey(k: &SigPubKey) -> PublicKey {
    let public_key_bytes = k.key_bytes();
    return Arc::new(adnl::common::KeyOption::from_type_and_public_key(
        adnl::common::KeyOption::KEY_ED25519,
        public_key_bytes,
    ));
}

pub fn make_cryptosig(s: BlockPayloadPtr) -> ton_types::Result<CryptoSignature> {
    return CryptoSignature::from_bytes(s.data().0.as_slice());
}

pub fn make_cryptosig_pair(
    pair: (PublicKeyHash, BlockPayloadPtr),
) -> ton_types::Result<CryptoSignaturePair> {
    let csig = make_cryptosig(pair.1)?;
    return Ok(CryptoSignaturePair::with_params(pair.0.data().into(), csig));
}

pub fn pairvec_to_cryptopair_vec(
    vec: Vec<(PublicKeyHash, BlockPayloadPtr)>,
) -> ton_types::Result<Vec<CryptoSignaturePair>> {
    return vec.into_iter().
        map(|p| make_cryptosig_pair(p)).
        collect();
}

#[allow(dead_code)]
pub fn pairvec_to_puresigs(
    pvec: Vec<(PublicKeyHash, BlockPayloadPtr)>,
) -> ton_types::Result<BlockSignaturesPure> {
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
) -> ton_types::Result<BlockSignatures> {
    let pure_sigs = pairvec_to_puresigs(pvec)?;
    let vset_catchain_seqno = vset.catchain_seqno();
    let vset_hash = ValidatorSet::calc_subset_hash_short(vset.list(), vset_catchain_seqno)?;
    let vset_info = ValidatorBaseInfo::with_params(vset_hash, vset_catchain_seqno);
    return Ok(BlockSignatures::with_params(vset_info, pure_sigs));
}

pub fn hex_to_publickey(key: &str) -> PublicKey {
    let kvec = hex::decode(key).unwrap();
    let mut array: [u8; 32] = [0; 32];
    for idx in 0..32 {
        array[idx] = kvec[idx];
    }
    return Arc::new(adnl::common::KeyOption::from_type_and_public_key(
        adnl::common::KeyOption::KEY_ED25519,
        &array,
    ));
}

pub fn validatordescr_to_catchain_node(descr: &ValidatorDescr) -> ton_types::Result<CatchainNode> {
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

pub fn get_shard_name(ident: &ShardIdent) -> String {
    return format!("{}:{}", ident.workchain_id(), ident.shard_prefix_with_tag());
}

pub fn validator_query_candidate_to_validator_block_candidate(
        source: PublicKey, candidate: super::BlockCandidate
    ) -> validator_session::ValidatorBlockCandidate
{
    return validator_session::ValidatorBlockCandidate {
        public_key: source,
        id: validator_session::ValidatorBlockId {
            root_hash: candidate.block_id.root_hash().clone(),
            file_hash: candidate.block_id.file_hash().clone(),
        },
        collated_file_hash: candidate.collated_file_hash.clone(),
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
    return res.string().unwrap();
}

pub fn get_adnl_id(validator: &ValidatorDescr) -> Arc<adnl::common::KeyId> {
    if let Some(addr) = &validator.adnl_addr {
        adnl::common::KeyId::from_data(*addr.as_slice())
    } else {
        adnl::common::KeyOption::from_type_and_public_key(
            adnl::common::KeyOption::KEY_ED25519, validator.public_key.key_bytes()
        ).id().clone()
    }
}

pub type ValidatorListHash = UInt256;

pub fn compute_validator_list_id(list: &Vec<ValidatorDescr>) -> Option<ValidatorListHash> {
    if !list.is_empty() {
        let mut hasher = Sha256::new();
        for x in list {
            hasher.input(x.compute_node_id_short().as_slice());
        }
        let hash: [u8; 32] = hasher.result().clone().into();
        Some(hash.into())
    }
    else {
        None
    }
}
