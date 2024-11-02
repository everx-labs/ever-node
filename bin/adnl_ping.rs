/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific EVERX DEV software governing permissions and
* limitations under the License.
*/

use adnl::{
    common::TaggedTlObject, 
    node::{AdnlNode, AdnlNodeConfig, AdnlNodeConfigJson, IpAddress}
};
use adnl::OverlayNode;
use std::{convert::TryInto, env, fs::File, io::BufReader, sync::Arc};
use ton_api::ton::{TLObject, rpc::ton_node::GetCapabilities};
#[cfg(feature = "telemetry")]
use ton_api::tag_from_boxed_type;
use ever_node::config::TonNodeGlobalConfigJson;
use ever_block::{error, fail, base64_decode, Ed25519KeyOption, Result};

include!("../common/src/test.rs");

const IP: &str = "0.0.0.0:4191";
const KEY_TAG: usize = 2;

fn read_config<T: for<'de> serde::Deserialize<'de>>(cfgfile: &str, cfgtype: &str) -> Result<T> {
    let file = File::open(cfgfile)?;
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).map_err(
        |e| error!("Cannot read {} config from file {}: {}", cfgtype, cfgfile, e) 
    )
}

fn ping(
    pub_key: &str, 
    ip_addr: &str, 
    global_cfgfile: &str, 
    local_cfgfile: Option<&str>
) -> Result<()> {

    let global_cfg: TonNodeGlobalConfigJson = read_config(global_cfgfile, "global")?;
    let zero_state_file_hash = *global_cfg.zero_state()?.file_hash.as_slice();
    let ip = IpAddress::from_versioned_string(ip_addr, None)?;

    let rt = tokio::runtime::Runtime::new()?;
    let local_cfg = if let Some(local_cfgfile) = local_cfgfile {
        let local_cfg: AdnlNodeConfigJson = read_config(local_cfgfile, "local")?;
        AdnlNodeConfig::from_json_config(&local_cfg)?
    } else {
        let (_, local_cfg) = AdnlNodeConfig::with_ip_address_and_private_key_tags(
            IP,
            vec![KEY_TAG]
        )?;
        local_cfg
    };
    let adnl = rt.block_on(AdnlNode::with_config(local_cfg))?;
    let overlay = OverlayNode::with_adnl_node_and_zero_state(
        adnl.clone(), 
        &zero_state_file_hash,
        KEY_TAG
    )?;
    let overlay_id = overlay.calc_overlay_short_id(
        -1i32, 
        0x8000000000000000u64 as i64
    )?;

    rt.block_on(AdnlNode::start(&adnl, vec![overlay.clone()]))?;
    if !rt.block_on(async { overlay.add_local_workchain_overlay(None, &overlay_id, None) })? {
        fail!("Cannot add overlay {}", overlay_id)
    }
    let local_key = adnl.key_by_tag(KEY_TAG)?;
    let other_key = Arc::new(
        Ed25519KeyOption::from_public_key(
            (&base64_decode(pub_key)?[..]).try_into()?
        )
    );
    let other_id = adnl.add_peer(local_key.id(), &ip, &other_key)?;
    let other_id = if let Some(other_id) = other_id {
        other_id
    } else {
        fail!("Cannot add peer to ADNL")
    };

    println!("Pinging {}/{} by GetRandomPeers", other_id, ip_addr);
    if let Some(reply) = rt.block_on(overlay.get_random_peers(&other_id, &overlay_id, None))? {
        println!("Got response: {:?}", reply)
    } else {
        println!("No response to ping")
    }

    println!("Pinging {}/{} by GetCapabilities", other_id, ip_addr);
    let query = TaggedTlObject {
        object: TLObject::new(GetCapabilities),
        #[cfg(feature = "telemetry")]
        tag: tag_from_boxed_type::<GetCapabilities>()
    };
    if let Some(reply) = rt.block_on(overlay.query(&other_id, &query, &overlay_id, None))? {
        println!("Got response: {:?}", reply)
    } else {
        println!("No response to ping")
    }

    Ok(())

}

fn print_usage() {
    println!(
        "Usage: adnl_ping <pub-key> <ip-addr> <path-to-global-config> \
        [--config <path-to-local-config>]"
    )
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let local_config = if args.len() == 6 {
        if args[4].as_str() != "--config" {
            print_usage();
            return
        }    
        Some(args[5].as_str())
    } else if args.len() == 4 {
        None
    } else {
        print_usage();
        return
    };
    init_log("./common/config/log_cfg.yml");
    ping(args[1].as_str(), args[2].as_str(), args[3].as_str(), local_config).unwrap_or_else(
        |e| println!("ADNL pinging error: {}", e)
    )
}
