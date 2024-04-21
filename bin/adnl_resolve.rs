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

use adnl::node::{AdnlNode, AdnlNodeConfig};
use adnl::{DhtNode, DhtSearchPolicy};
use std::{convert::TryInto, env, fs::File, io::BufReader};
use ton_node::config::TonNodeGlobalConfigJson;
use ton_types::{error, fail, base64_decode, KeyId, Result};

include!("../common/src/test.rs");

const IP: &str = "0.0.0.0:4191";
const KEY_TAG: usize = 1;

async fn scan(adnlid: &str, cfgfile: &str) -> Result<()> {

    let file = File::open(cfgfile)?;
    let reader = BufReader::new(file);
    let config: TonNodeGlobalConfigJson = serde_json::from_reader(reader).map_err(
        |e| error!("Cannot read config from file {}: {}", cfgfile, e) 
    )?;
    let dht_nodes = config.get_dht_nodes_configs()?;
    let (_, config) = AdnlNodeConfig::with_ip_address_and_private_key_tags(
        IP, 
        vec![KEY_TAG]
    )?;
    let adnl = AdnlNode::with_config(config).await?;
    let dht = DhtNode::with_params(adnl.clone(), KEY_TAG, None)?;
    AdnlNode::start(&adnl, vec![dht.clone()]).await?;

    let mut nodes = Vec::new();
    let mut bad_nodes = Vec::new();
    for dht_node in dht_nodes.iter() {
        if let Some(key) = dht.add_peer_to_network(dht_node, None)? {
            nodes.push(key)
        } else {
            fail!("Invalid DHT peer {:?}", dht_node)
        }
    }

    let keyid = KeyId::from_data((&base64_decode(adnlid)?[..32]).try_into()?);
    let mut context = None;
    let mut index = 0;
    println!("Searching DHT for {}...", keyid);
    loop {
        if let Ok(Some((ip, key))) = DhtNode::find_address_in_network_with_context(
            &dht, 
            &keyid,
            &mut context,
            DhtSearchPolicy::FastSearch(5),
            None
        ).await {
            println!("Found {} / {}", ip, key.id());
            return Ok(())
        }
        if index >= nodes.len() {
            nodes.clear();
            for dht_node in dht.get_known_nodes_of_network(10000, None)?.iter() {
                if let Some(key) = dht.add_peer_to_network(dht_node, None)? {
                    if !bad_nodes.contains(&key) {
                        nodes.push(key)
                    }
                }
            } 
            if nodes.is_empty() {
                fail!("No good DHT peers")
            }
            index = 0;
        }
        println!(
            "Not found yet, scanning more DHT nodes from {} ({} of {}) ...", 
            nodes[index], 
            index, 
            nodes.len()
        );
        if !dht.find_dht_nodes_in_network(&nodes[index], None).await? {
            println!("DHT node {} is non-responsive", nodes[index]); 
            bad_nodes.push(nodes.remove(index))
        } else {
            index += 1
        }
    }
    
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        println!("Usage: adnl_resolve <adnl-id> <path-to-global-config>");
        return
    };
    init_log("./common/config/log_cfg.yml");
    if let Err(e) = scan(args[1].as_str(), args[2].as_str()).await {
        println!("ADNL resolving error: {}", e)
    }
}
