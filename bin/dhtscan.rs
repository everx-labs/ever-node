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

use adnl::node::{AdnlNode, AdnlNodeConfig};
use adnl::DhtNode;
use adnl::OverlayNode;
use std::{collections::HashMap, env, fs::File, io::BufReader, ops::Deref, sync::Arc};
use ton_node::config::TonNodeGlobalConfigJson;
use ton_types::{error, fail, base64_encode, KeyOption, Result};

include!("../common/src/test.rs");

const IP: &str = "0.0.0.0:4191";
const KEY_TAG: usize = 1;

fn scan(cfgfile: &str, jsonl: bool, search_overlay: bool, use_workchain0: bool) -> Result<()> {

    let file = File::open(cfgfile)?;
    let reader = BufReader::new(file);
    let config: TonNodeGlobalConfigJson = serde_json::from_reader(reader).map_err(
        |e| error!("Cannot read config from file {}: {}", cfgfile, e) 
    )?;
    let zero_state = config.zero_state()?;
    let zero_state = zero_state.file_hash;
    let dht_nodes = config.get_dht_nodes_configs()?;

    let mut rt = tokio::runtime::Runtime::new()?;
    let (_, config) = AdnlNodeConfig::with_ip_address_and_private_key_tags(
        IP, 
        vec![KEY_TAG]
    )?;
    let adnl = rt.block_on(AdnlNode::with_config(config))?;
    let dht = DhtNode::with_params(adnl.clone(), KEY_TAG, None)?;
    let overlay = OverlayNode::with_adnl_node_and_zero_state(
        adnl.clone(), 
        zero_state.as_slice(), 
        KEY_TAG
    )?;

    rt.block_on(AdnlNode::start(&adnl, vec![dht.clone(), overlay.clone()]))?;

    let mut preset_nodes = Vec::new();
    for dht_node in dht_nodes.iter() {
        if let Some(key) = dht.add_peer_to_network(dht_node, None)? {
            preset_nodes.push(key)
        } else {
            fail!("Invalid DHT peer {:?}", dht_node)
        }
    }

    println!("Scanning DHT...");
    for node in preset_nodes.iter() {
        rt.block_on(dht.find_dht_nodes_in_network(node, None))?;
    }

    scan_overlay(&mut rt, &dht, preset_nodes.len(), &overlay, search_overlay, -1)?;
    if use_workchain0 {
        scan_overlay(&mut rt, &dht, preset_nodes.len(), &overlay, search_overlay, 0)?;
    }
    if search_overlay {
        return Ok(())
    }

    let mut count = 0;
    let nodes = dht.get_known_nodes_of_network(5000, None)?;
    if nodes.len() > dht_nodes.len() {
        println!("---- Found DHT nodes:");
        for node in nodes {
            let mut skip = false;
            for dht_node in dht_nodes.iter() {
                if dht_node.id == node.id {
                    skip = true;
                    break
                }
            } 
            if skip {
                continue;
            }
            let key: Arc<dyn KeyOption> = (&node.id).try_into()?;
            match rt.block_on(dht.ping_in_network(key.id(), None)) {
                Ok(true) => (),
                _ => continue
            }
            let adr = AdnlNode::parse_address_list(&node.addr_list)?.ok_or_else(
                || error!("Cannot parse address list {:?}", node.addr_list)
            )?.into_udp();
            let json = serde_json::json!(
                {
                    "@type": "dht.node",
                    "id": {
                        "@type": "pub.ed25519",
                        "key": base64_encode(key.pub_key()?)
                    },
                    "addr_list": {
                        "@type": "adnl.addressList",
                        "addrs": [
                            {
                                "@type": "adnl.address.udp",
                                "ip": adr.ip,
                                "port": adr.port
                            }
                        ],
                        "version": node.addr_list.version,
                        "reinit_date": node.addr_list.reinit_date,
                        "priority": node.addr_list.priority,
                        "expire_at": node.addr_list.expire_at
                    },
                    "version": node.version,
                    "signature": base64_encode(node.signature.deref())
                }
            ); 
            count += 1;
            println!(
                "{},", 
                if jsonl {
                    serde_json::to_string(&json)?
                } else { 
                    serde_json::to_string_pretty(&json)?
                }
            );
        }       
        println!("Total: {} DHT nodes", count);
    } else {
        println!("---- No DHT nodes found");
    }
    Ok(())

}

fn scan_overlay(
    rt: &mut tokio::runtime::Runtime,
    dht: &Arc<DhtNode>,
    dht_presets: usize,
    overlay: &Arc<OverlayNode>,
    search_overlay: bool, 
    workchain: i32
) -> Result<()> {

    let overlay_id = overlay.calc_overlay_short_id(workchain, 0x8000000000000000u64 as i64)?;

    let mut iter = None;
    let mut overlays = HashMap::new();
    loop {
        let res = rt.block_on(
            DhtNode::find_overlay_nodes_in_network(&dht, &overlay_id, &mut iter, None)
        )?;
        let count = overlays.len();
        for (ip, node) in res {
            let key: Arc<dyn KeyOption> = (&node.id).try_into()?;
            overlays.insert(key.id().clone(), (ip, node));
        }
        if search_overlay {
            println!(
                "Found {} new OVERLAY nodes in {}({}), searching more...", 
                overlays.len() - count, overlay_id, workchain
            )
        } else {
            println!(
                "Found {} new DHT nodes, searching more...", 
                dht.get_known_nodes_of_network(5000, None)?.len() - dht_presets
            )
        }
        if iter.is_none() {
            break;
        }
    }

    if search_overlay {
        println!("---- Found OVERLAY nodes in {}({}):", overlay_id, workchain);
        for (ip, node) in overlays.iter() {
            println!("IP {}: {:?}", ip, node)
        }
        println!("---- Found {} OVERLAY nodes totally", overlays.len());
    }
    Ok(())

}

fn main() {
    let mut config = None;
    let mut jsonl = false;
    let mut overlay = false;
    let mut workchain0 = false;
    for arg in env::args().skip(1) {
        if arg == "--jsonl" {
            jsonl = true
        } else if arg == "--overlay" {
            overlay = true
        } else if arg == "--workchain0" {
            workchain0 = true
        } else {
            config = Some(arg)
        }
    }
    let config = if let Some(config) = config {
        config
    } else {
        println!("Usage: dhtscan [--jsonl] [--overlay] [--workchain0] <path-to-global-config>");
        return
    };
    init_log("./common/config/log_cfg.yml");
    scan(&config, jsonl, overlay, workchain0).unwrap_or_else(
        |e| println!("DHT scanning error: {}", e)
    )
}
