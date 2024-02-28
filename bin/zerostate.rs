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

use clap::{Arg, App};
use serde_json::{Map, Value};
use ton_block::{
    ConfigParamEnum, ConfigParam12,
    Deserializable, Serializable, ShardIdent, ShardStateUnsplit, StateInit,
};
use ton_types::{base64_encode, HashmapType, Result, UInt256, write_boc};

fn import_zerostate(json: &str) -> Result<ShardStateUnsplit> {
    let map = serde_json::from_str::<Map<String, Value>>(&json)?;
    let mut mc_zero_state = ton_block_json::parse_state(&map)?;
    let now = mc_zero_state.gen_time();
    // let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as u32;
    // mc_zero_state.set_gen_time(now);
    let mut extra = mc_zero_state.read_custom()?.expect("must be in mc state");
    let mut workchains = extra.config.workchains()?;
    let mut wc_zero_state = vec![];
    workchains.clone().iterate_with_keys(|workchain_id, mut descr| {
        let shard = ShardIdent::with_tagged_prefix(workchain_id, ton_block::SHARD_FULL)?;
        // generate empty shard state and set desired fields
        let mut state = ShardStateUnsplit::with_ident(shard);
        state.set_gen_time(now);
        state.set_global_id(mc_zero_state.global_id());
        state.set_min_ref_mc_seqno(u32::MAX);

        let cell = state.serialize()?;
        descr.zerostate_root_hash = cell.repr_hash();
        let bytes = ton_types::write_boc(&cell)?;
        descr.zerostate_file_hash = UInt256::calc_file_hash(&bytes);
        workchains.set(&workchain_id, &descr)?;
        let name = format!("{:x}.boc", descr.zerostate_file_hash);
        std::fs::write(name, &bytes)?;
        wc_zero_state.push(state);
        Ok(true)
    })?;
    extra.config.set_config(ConfigParamEnum::ConfigParam12(ConfigParam12{workchains}))?;
    let ccvc = extra.config.catchain_config()?;
    let cur_validators = extra.config.validator_set()?;
    let (_validators, hash_short) = cur_validators.calc_subset(
        &ccvc, 
        ton_block::SHARD_FULL, 
        ton_block::MASTERCHAIN_ID, 
        0,
        now.into()
    )?;
    extra.validator_info.validator_list_hash_short = hash_short;
    extra.validator_info.nx_cc_updated = true;
    extra.validator_info.catchain_seqno = 0;
    mc_zero_state.write_custom(Some(&extra))?;
    Ok(mc_zero_state)
}

fn write_zero_state(mc_zero_state: ShardStateUnsplit) -> Result<()> {
    let cell = mc_zero_state.serialize().unwrap();
    let bytes = write_boc(&cell).unwrap();
    let file_hash = UInt256::calc_file_hash(&bytes);
    let name = format!("{:x}.boc", file_hash);
    std::fs::write(name, &bytes).unwrap();

    // CHECK mc_zero_state
    let mc_zero_state = ShardStateUnsplit::construct_from_bytes(&bytes).expect("can't deserialize state");
    let extra = mc_zero_state.read_custom().expect("extra wasn't read from state").expect("extra must be in state");
    extra.config.config_params.iterate_slices(|ref mut key, ref mut param| {
        u32::construct_from(key).expect("index wasn't deserialized incorrectly");
        param.checked_drain_reference().expect("must contain reference");
        Ok(true)
    }).expect("something wrong with config");
    let prices = extra.config.storage_prices().expect("prices weren't read from config");
    for i in 0..prices.len().expect("prices len wasn't read") as u32 {
        prices.get(i).expect(&format!("prices description {} wasn't read", i));
    }

    let json = serde_json::json!({
        "zero_state": {
            "workchain": -1,
            "shard": -9223372036854775808i64,
            "seqno": 0,
            "root_hash": base64_encode(cell.repr_hash().as_slice()),
            "file_hash": base64_encode(&file_hash.as_slice()),
        }
    });

    let json = serde_json::to_string_pretty(&json)?;
    std::fs::write("config.json", &json)?;
    println!("{}", json);

    // check correctness
    // std::fs::write("new.json", ton_block_json::debug_state(mc_zero_state)?)?;

    Ok(())
}

fn main() {
    let args = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .arg(Arg::with_name("INPUT")
            .short("i")
            .long("input")
            .help("input json filename with masterchain zerostate")
            .required(true)
            .default_value("zero_state.json")
            .takes_value(true)
            .number_of_values(1))
        .arg(Arg::with_name("CONFIG")
            .short("c")
            .long("config")
            .help("config TVC filename to get its code")
            .takes_value(true)
            .number_of_values(1))
        .arg(Arg::with_name("ELECTOR")
            .short("e")
            .long("elector")
            .help("elector TVC filename to get its code and data")
            .takes_value(true)
            .number_of_values(1))
        .get_matches();

    let file_name = args.value_of("INPUT").expect("required set for INPUT");
    let json = std::fs::read_to_string(file_name).unwrap();
    let mut mc_zero_state = import_zerostate(&json).unwrap();
    let config_code = if let Some(file_name) = args.value_of("CONFIG") {
        let state_init = StateInit::construct_from_file(file_name)
            .unwrap_or_else(|err| panic!("something wrong with config TVC file {} : {}", file_name, err));
        state_init.code().cloned()
    } else {
        None
    };
    mc_zero_state.update_config_smc_with_code(config_code).unwrap();
    if let Some(file_name) = args.value_of("ELECTOR") {
        let state_init = StateInit::construct_from_file(file_name)
            .unwrap_or_else(|err| panic!("something wrong with elector TVC file {} : {}", file_name, err));
        mc_zero_state.update_elector_smc(state_init.code().cloned(), state_init.data().cloned()).unwrap();
    }
    write_zero_state(mc_zero_state).unwrap();
}
