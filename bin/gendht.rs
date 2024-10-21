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

use adnl::{adnl_node_test_key, adnl_node_test_config, node::{AdnlNode, AdnlNodeConfig}};
use adnl::DhtNode;
use std::{env, ops::Deref, sync::Arc};
use ever_block::{error, base64_encode, KeyOption, Result};

async fn gen(ip: &str, dht_key_enc: &str) -> Result<()> {
    let config = AdnlNodeConfig::from_json(
       adnl_node_test_config!(ip, adnl_node_test_key!(1_usize, dht_key_enc))
    ).unwrap();
    let adnl = AdnlNode::with_config(config).await.unwrap();
    let dht = DhtNode::with_params(adnl.clone(), 1_usize, None).unwrap();
    let node = dht.get_signed_node_of_network(None).unwrap();
    let key: Arc<dyn KeyOption> = (&node.id).try_into()?;
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
    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
} 

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        println!("Usage: gendht <ip:port> <private DHT key in base64>");
        return
    };
    gen(&args[1], &args[2]).await.unwrap_or_else(|e| println!("gen error: {}", e))
}
