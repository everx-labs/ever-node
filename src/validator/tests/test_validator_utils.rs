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

use super::*;
#[cfg(not(feature = "fast_finality"))]
use crate::shard_state::ShardHashesStuff;
#[cfg(not(feature = "fast_finality"))]
use crate::{
    block::BlockStuff, error::NodeError,
    validator::accept_block::create_new_proof
};

#[cfg(not(feature = "fast_finality"))]
use ton_block::{
    MASTERCHAIN_ID,
    Block, BlockIdExt, ConfigParamEnum, CryptoSignature, CryptoSignaturePair, Deserializable,
    SigPubKey, 
};
#[cfg(not(feature = "fast_finality"))]
use ton_types::{error, HashmapType};
#[cfg(not(feature = "fast_finality"))]
use crate::validator::accept_block::create_new_proof_link;

#[cfg(not(feature = "fast_finality"))]
fn block_config(block_stuff: &BlockStuff) -> Result<ConfigParams> {
    block_stuff.block()?.read_extra()?.read_custom()?.and_then(
        |custom| custom.config().cloned()
    ).ok_or_else(
        || error!(NodeError::InvalidArg("State doesn't contain `custom` field".to_string()))
    )
}

#[test]
fn test_mine_key() {
    // let now = std::time::Instant::now();
    let (_, pub_key) = mine_key_for_workchain(None);
    assert_ne!(pub_key.id().data(), &[0; 32]);
    let (_, pub_key) = mine_key_for_workchain(Some(-1));
    assert_ne!(pub_key.id().data(), &[0; 32]);
    assert_eq!(pub_key.id().data()[0] % 32, 0);
    let (_, pub_key) = mine_key_for_workchain(Some(0));
    assert_ne!(pub_key.id().data(), &[0; 32]);
    assert_eq!(pub_key.id().data()[0] % 32, 1);
    let (_, pub_key) = mine_key_for_workchain(Some(1));
    assert_ne!(pub_key.id().data(), &[0; 32]);
    assert_eq!(pub_key.id().data()[0] % 32, 2);
    let (_, pub_key) = mine_key_for_workchain(Some(2));
    assert_ne!(pub_key.id().data(), &[0; 32]);
    assert_eq!(pub_key.id().data()[0] % 32, 3);
    // assert!(now.elapsed().as_micros() < 1000);
}

#[test]
fn test_calc_workchain_id_by_adnl_id() {
    assert_eq!(calc_workchain_id_by_adnl_id(&[0; 32]), -1);
    assert_eq!(calc_workchain_id_by_adnl_id(&[1; 32]), 0);
    assert_eq!(calc_workchain_id_by_adnl_id(&[2; 32]), 1);
    assert_eq!(calc_workchain_id_by_adnl_id(&[3; 32]), 2);
}

#[cfg(not(feature = "fast_finality"))]
#[tokio::test]
async fn test_validator_set() {
    let block = Block::construct_from_file("src/tests/static/key_block.boc").unwrap();
    let custom = block.read_extra().unwrap().read_custom().unwrap().unwrap();
    let mut config = custom.config().unwrap().clone();
    assert!(config.prev_validator_set_present().unwrap(), "key block must be after elections");

    let vset = config.validator_set().unwrap();
    assert_eq!(vset.list().len(), 21);

    let election_id = vset.utime_since();
    assert_eq!(election_id, 1627898896);

    if let ConfigParamEnum::ConfigParam8(mut cp) = config.config(8).unwrap().unwrap() {
        cp.global_version.capabilities |= GlobalCapabilities::CapWorkchains as u64;
        config.set_config(ConfigParamEnum::ConfigParam8(cp)).unwrap();
    }
    config.has_capability(GlobalCapabilities::CapWorkchains);

    // let elector = Account::construct_from_file("src/tests/static/elector.boc").unwrap();
    // let stakes = load_stakes_from_elector_by_id(&elector, election_id).await.unwrap();

    let cc_seqno = block.read_info().unwrap().gen_catchain_seqno();

    // stakes.iter().enumerate().for_each(|(i, (pub_key, stake))| {
    //     let stake_id = ((stake / 1_000_000_000) % 100) as i32 - 1;
    //     println!("{}: pub_key: {} stake: {} stake_id: {}", i, hex::encode(pub_key), stake, stake_id);
    // });
    vset.list().iter().enumerate().for_each(|(i,descr)| {
        let real_id = calc_workchain_id(descr);
        // let stake = stakes.get(descr.public_key.as_slice()).unwrap();
        // let stake_id = ((stake / 1_000_000_000) % 100) as i32 - 1;
        println!("{}: pub_key: {} real_id: {}", i, hex::encode(descr.public_key.as_slice()), real_id);
        // assert_eq!(real_id, workchain_id);
        // assert_eq!(real_id, stake_id);
    });

    for workchain_id in -1..=1 {
        println!("workchain_id: {}", workchain_id);
        let subset = if workchain_id != -1 {
            #[cfg(not(feature = "fast_finality"))]
            {
                let shard = ShardIdent::with_workchain_id(workchain_id).unwrap();
                calc_subset_for_workchain_standard(&vset, &config, &shard, cc_seqno).unwrap()
            }

            // TODO: update with a proper subset calculation
            #[cfg(feature = "fast_finality")]
            unimplemented!("STUB UNTIL MERGED");
        } else {
            calc_subset_for_masterchain(&vset, &config, cc_seqno).unwrap()
        };
        assert_eq!(subset.validators.len(), 7);
        subset.validators.iter().enumerate().for_each(|(i,descr)| {
            let real_id = calc_workchain_id(descr);
            // let stake = stakes.get(descr.public_key.as_slice()).unwrap();
            // let stake_id = ((stake / 1_000_000_000) % 100) as i32 - 1;
            println!("{}: pub_key: {} real_id: {}", i, hex::encode(descr.public_key.as_slice()), real_id);
            // assert_eq!(real_id, stake_id);
            assert_eq!(real_id, workchain_id);
        });
    }
}

#[cfg(not(feature = "fast_finality"))]
#[test]
fn test_any_keyblock_validator_set() {
    check_any_keyblock_validator_set("src/tests/static/key_block.boc");
    // check_any_keyblock_validator_set("c:\\work\\!boc\\block_boc_1bedabe657d691078d466cc6d381dce259024947f0ffc3a4ebd06c57bb82de4a.boc");
    // check_any_keyblock_validator_set("c:\\work\\!boc\\block_boc_7cd2e04387ad7ddd28fe9b9790557be72c5d397a75129aee5df46f745ab573ac.boc");
    // check_any_keyblock_validator_set("c:\\work\\!boc\\block_boc_863e8f3a0af67987adb26dc51cd772a750bafdc98338e9caef74da3d8a5ab81c.boc");
}

#[cfg(not(feature = "fast_finality"))]
fn check_any_keyblock_validator_set(file_name: &str) {
    let block = Block::construct_from_file(file_name).unwrap();
    let custom = block.read_extra().unwrap().read_custom().unwrap().unwrap();
    let mut config = custom.config().unwrap().clone();

    if let ConfigParamEnum::ConfigParam8(mut cp) = config.config(8).unwrap().unwrap() {
        cp.global_version.capabilities |= GlobalCapabilities::CapWorkchains as u64;
        config.set_config(ConfigParamEnum::ConfigParam8(cp)).unwrap();
    }
    config.has_capability(GlobalCapabilities::CapWorkchains);

    let vset = config.validator_set().unwrap();
    let election_id = vset.utime_since();
    println!("elections: {} total validators: {}", election_id, vset.list().len());

    let cc_seqno = block.read_info().unwrap().gen_catchain_seqno();

    vset.list().iter().enumerate().for_each(|(i,descr)| {
        let id = calc_workchain_id(descr);
        println!("{}: pub_key: {} id: {}", i, hex::encode(descr.public_key.as_slice()), id);
    });

    let count = config.workchains().unwrap().len().unwrap() as i32;
    let shards = ShardHashesStuff::from(custom.shards().clone());
    for workchain_id in -1..count {
        let shard_ids = match workchain_id {
            MASTERCHAIN_ID => vec!(BlockIdExt::with_params(ShardIdent::masterchain(), 0, Default::default(), Default::default())),
            workchain_id => shards.top_blocks(&[workchain_id]).unwrap()
        };
        for block_id in shard_ids {
            println!("{}", block_id.shard());
            let vset = config.validator_set().unwrap();
            let subset = if workchain_id != -1 {
                #[cfg(not(feature = "fast_finality"))]
                {
                    calc_subset_for_workchain_standard(&vset, &config, block_id.shard(), cc_seqno).unwrap()
                }

                // TODO: update with a proper subset calculation
                #[cfg(feature = "fast_finality")]
                unimplemented!("STUB UNTIL MERGED")
            } else {
                calc_subset_for_masterchain(&vset, &config, cc_seqno).unwrap()
            };
            assert_eq!(subset.validators.len(), 7);
            subset.validators.iter().enumerate().for_each(|(i,descr)| {
                let real_id = calc_workchain_id(descr);
                println!("{}: pub_key: {} real_id: {}", i, hex::encode(descr.public_key.as_slice()), real_id);
                assert_eq!(real_id, workchain_id);
            });
        }
    }
}

#[cfg(not(feature = "fast_finality"))]
#[test]
fn test_create_new_proof_with_workchains() {
    let block_stuff = BlockStuff::read_block_from_file("src/tests/static/key_block.boc").unwrap();
    let validator_set = block_config(&block_stuff).unwrap().validator_set().unwrap();
    let data = ton_block::Block::build_data_for_sign(
        block_stuff.id().root_hash(), 
        block_stuff.id().file_hash()
    );

    let mut list = Vec::new();
    let mut signatures = Vec::new();
    for _ in 0..7 {
        let (pvt_key, pub_key) = Ed25519KeyOption::generate_with_json().unwrap();
        let public_key = SigPubKey::from_bytes(pub_key.pub_key().unwrap()).unwrap();
        let descr = ValidatorDescr::with_params(
            public_key,
            17,
            None,
            None
        );
        let pvt_key = Ed25519KeyOption::from_private_key_json(&pvt_key).unwrap();
        let signature = pvt_key.sign(&data).unwrap();
        let signature = CryptoSignature::from_bytes(&signature).unwrap();
        assert!(descr.public_key.verify_signature(&data, &signature));
        assert_ne!(pub_key.id().data(), &[0; 32]);
        signatures.push(CryptoSignaturePair::with_params(
            pub_key.id().data().clone().into(),
            signature
        ));
        list.push(descr);
    }

    let validator_set = ValidatorSet::with_cc_seqno(
        validator_set.utime_since(),
        validator_set.utime_until(),
        validator_set.main(),
        block_stuff.block().unwrap().read_info().unwrap().gen_catchain_seqno(),
        list
    ).unwrap();

    // TODO: use proper collator range
    let (proof, signatures) = create_new_proof(
        &block_stuff, 
        &validator_set, 
        #[cfg(feature = "fast_finality")]
        &Default::default(), 
        &signatures
    ).unwrap();
    assert_eq!(signatures.pure_signatures.signatures().len().unwrap(), 7);

    let (virt_block, _root) = proof.virtualize_block().unwrap();
    let custom = virt_block.read_extra().unwrap().read_custom().unwrap().unwrap();
    let config = custom.config().unwrap();
    let workchains = config.workchains().unwrap();
    assert_eq!(workchains.export_keys::<i32>().unwrap(), vec![0, 1]);
    assert_eq!(block_config(&block_stuff).unwrap().workchains().unwrap(), workchains);

    // check proof link creation for masterchain without signatures
    let _proof = create_new_proof_link(&block_stuff).unwrap();
}

