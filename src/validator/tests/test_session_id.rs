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

use std::{
    fs,
    io::{self, BufRead},
    time::Duration,
};
use ton_block::{signature::SigPubKey, validators::ValidatorDescr};

use super::*;
use crate::validator::{log_parser::LogParser, sessions_computing::GeneralSessionInfo};

fn parse_shard_ident(parser: &LogParser, name: &str) -> ShardIdent {
    ShardIdent::with_tagged_prefix(
        parser.parse_field_fromstr::<i32>(&format!("{}.workchain", name)),
        parser.parse_field_fromstr::<u64>(&format!("{}.shard", name)),
    )
    .unwrap()
}

#[allow(dead_code)]
fn parse_duration(parser: &LogParser, field: &str) -> Duration {
    let duration = parser.parse_field_fromstr::<f64>(field);
    let secs = duration.floor();
    let nanosecs = duration - secs;
    if nanosecs != 0.0 {
        unimplemented!("Fractional duration");
    }
    Duration::from_secs(secs as u64)
}

fn parse_validator_descr(parser: &LogParser, name: &str) -> ValidatorDescr {
    ValidatorDescr::with_params(
        SigPubKey::from_bytes(&parser.parse_slice(&format!("{}.key", name))).unwrap(),
        parser.parse_field_fromstr::<u64>(&format!("{}.weight", name)),
        Some(UInt256::from_slice(
            parser.parse_slice(&format!("{}.addr", name)).0.as_slice(),
        )),
        None
    )
}

struct ValidatorParams {
    general_session_info: Arc<GeneralSessionInfo>,
    val_set: ValidatorSet
}

impl ValidatorParams {
    fn parse(parser: &LogParser) -> Self {
        let mut validators = Vec::new();
        for num in 0..parser.get_field_count("val_set") {
            validators.push(parse_validator_descr(&parser, &format!("val_set.{}", num)));
        }

        let catchain_seqno = parser.parse_field_fromstr::<u32>("catchain_seqno");
        ValidatorParams {
            general_session_info: Arc::new(GeneralSessionInfo {
                catchain_seqno,
                opts_hash: UInt256::from_slice(parser.parse_slice("opts_hash").0.as_slice()),
                shard: parse_shard_ident(parser, "shard"),
                key_seqno: parser.parse_field_fromstr::<u32>("key_seqno"),
                max_vertical_seqno: 0
            }),
            val_set: ValidatorSet::with_cc_seqno(0, 0, 0, catchain_seqno, validators).unwrap()
        }
    }
}

fn do_test_get_validator_set_id(contents: &str) {
    let parser = LogParser::new(&contents);
    /*
        let opts = validator_session::SessionOptions {
            catchain_idle_timeout: parse_duration(&parser, "catchain_idle_timeout"),
            catchain_max_deps: parser.parse_field_fromstr::<u32>("catchain_max_deps"),
            catchain_skip_processed_blocks: false,
            round_candidates: parser.parse_field_fromstr::<u32>("round_candidates"),
            next_candidate_delay: Duration::from_secs(2), //parse_duration(&parser, "next_candidate_delay"),
            round_attempt_duration: Duration::from_secs(parser.parse_field_fromstr::<u64>("round_attempt_duration")),
            max_round_attempts: parser.parse_field_fromstr::<u32>("max_round_attempts"),
            max_block_size: parser.parse_field_fromstr::<u32>("max_block_size"),
            max_collated_data_size: parser.parse_field_fromstr::<u32>("max_collated_data_size"),
            new_catchain_ids: parser.parse_field_fromstr::<u32>("new_catchain_ids") > 0
        };
    */
    let p = ValidatorParams::parse(&parser);

    let serialized = get_session_id_serialize(p.general_session_info.clone(), &p.val_set.list().to_vec(), true);
    let computed_id = get_session_id(p.general_session_info.clone(), &p.val_set.list().to_vec(), true);
    let actual_id = UInt256::from_slice(&parser.parse_slice("group_id").0);

    println!("Serialized: {}", hex::encode(&serialized.0));
    println!("Actual group-id: {}", actual_id);
    println!("Computed group-id: {}", computed_id);

    assert_eq!(actual_id, computed_id);
}

#[test]
fn test_session_id_normal() {
    let file = fs::File::open("src/validator/tests/static/test_session_id_normal.log").unwrap();
    for line in io::BufReader::new(file).lines() {
        let line_unwrapped = line.unwrap();
        println!("Contents: {}", line_unwrapped);
        do_test_get_validator_set_id(&line_unwrapped);
    }
}

fn do_test_catchain_unsafe_rotate(s: &str) {
    let parser = LogParser::new(&s);
    let p = ValidatorParams::parse(&parser);
    let prev_block = 1; //parser.parse_field_fromstr::<u32>("prev_block");
    let rotation_id = parser.parse_field_fromstr::<u32>("rotation_id");

    let mut config: ValidatorManagerConfig = ValidatorManagerConfig::default();
    config.unsafe_catchain_rotates.insert(p.general_session_info.catchain_seqno, (prev_block, rotation_id));

    //let session_id = get_session_id(&p.shard, &p.val_set, &p.opts_hash, p.key_seqno, true, 0);
    let session_id = get_session_id(p.general_session_info.clone(), &p.val_set.list().to_vec(), true);
    let unsafe_serialized = compute_session_unsafe_serialized(&session_id, rotation_id);
    let actual_serialized = parser.parse_slice("unsafe_serialized").0;

    let unsafe_id = get_session_unsafe_id(
        p.general_session_info.clone(),
        &p.val_set.list().to_vec(),
        true,
        Some(prev_block),
        &config
    );
    let real_unsafe_id = UInt256::from_slice(&parser.parse_slice("unsafe_id").0);

    println!("Actual unsafe-id: {:x}", real_unsafe_id);
    println!("Computed unsafe-id: {:x}", unsafe_id);

    println!("Actual unsafe-serialized: {}", hex::encode(actual_serialized));
    println!("Computed unsafe-serialized: {}", hex::encode(unsafe_serialized));

    assert_eq!(unsafe_id, real_unsafe_id);
}

#[test]
fn test_session_id_unsafe() {
    let file = fs::File::open("src/validator/tests/static/test_session_id_unsafe.log").unwrap();
    for line in io::BufReader::new(file).lines() {
        let line_unwrapped = line.unwrap();
        println!("Contents: {}", line_unwrapped);
        do_test_catchain_unsafe_rotate(&line_unwrapped);
    }
}
