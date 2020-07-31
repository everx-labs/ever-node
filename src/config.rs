use crate::{network::node_network::NodeNetwork};
use adnl::{adnl_node_test_config, adnl_node_test_key,
    from_slice, common::KeyOption, node::{AdnlNodeConfig, AdnlNodeConfigJson}
};
use hex::FromHex;
use std::{io::BufReader, fs::File, net::{Ipv4Addr, IpAddr, SocketAddr}, path::Path};
use ton_api::{
    IntoBoxed, 
    ton::{
        self, adnl::{address::address::Udp, addresslist::AddressList as AdnlAddressList}, 
        dht::node::Node as DhtNodeConfig, pub_::publickey::Ed25519
    }
};
use ton_block::{BlockIdExt, ShardIdent};
use ton_types::{error, fail, Result, UInt256};

pub struct TonNodeConfig {
    log_config_path: Option<String>,
    ton_global_config_path: Option<String>,
    use_global_config: bool,
    adnl_node: AdnlNodeConfigJson,
    port: Option<u16>,
    overlay_peers: Option<Vec<AdnlNodeConfig>>,
    dht_peers: Option<Vec<AdnlNodeConfig>>,
    kafka_consumer_config: Option<KafkaConsumerConfig>,
    external_db_config: Option<ExternalDbConfig>
}

pub struct TonNodeGlobalConfig(TonNodeGlobalConfigJson);

#[derive(serde::Deserialize, serde::Serialize)]
struct TonNodeConfigJson {
    log_config_name: Option<String>,
    ton_global_config_name: Option<String>,
    use_global_config: bool,
    ip_address: Option<String>,
    adnl_node: Option<AdnlNodeConfigJson>,
    overlay_peers: Option<Vec<AdnlNodeConfigJson>>,
    kafka_consumer_config: Option<KafkaConsumerConfig>,
    external_db_config: Option<ExternalDbConfig>
}

#[derive(serde::Deserialize, serde::Serialize, Default, Debug, Clone)]
pub struct KafkaConsumerConfig {
    pub group_id: String,
    pub brokers: String,
    pub topic: String,
    pub session_timeout_ms: u32,
    pub run_attempt_timeout_ms: u32
}


#[derive(Debug, Default, serde::Deserialize, serde::Serialize, Clone)]
pub struct KafkaProducerConfig {
    pub enabled: bool,
    pub brokers: String,
    pub message_timeout_ms: u32,
    pub topic: String,
    pub attempt_timeout_ms: u32,
    pub message_max_size: usize,
    pub big_messages_storage: String,
}

#[derive(Debug, Default, serde::Deserialize, serde::Serialize, Clone)]
#[serde(default)]
pub struct ExternalDbConfig {
    pub block_producer: KafkaProducerConfig,
    pub raw_block_producer: KafkaProducerConfig,
    pub message_producer: KafkaProducerConfig,
    pub transaction_producer: KafkaProducerConfig,
    pub account_producer: KafkaProducerConfig,
    pub block_proof_producer: KafkaProducerConfig,
    pub bad_blocks_storage: String,
}

impl TonNodeConfig {

    pub fn from_file(configs_dir: &str, json_file_name: &str, default_config_name: &str) -> Result<Self> { 
        let config_file_path = TonNodeConfig::build_path(configs_dir, json_file_name)?;
        let config_file = File::open(config_file_path.clone());

        let config_json = match config_file {
            Ok(file) => {
                let reader = BufReader::new(file);
                let config: TonNodeConfigJson = serde_json::from_reader(reader)?;
                config
            },
            Err(_) => {
                // generate new config from default_config
                let default_config_file = File::open(
                    TonNodeConfig::build_path(configs_dir, default_config_name)?
                )?;
                let reader = BufReader::new(default_config_file);
                let mut config: TonNodeConfigJson = serde_json::from_reader(reader)?;
                // TODO: transfer to Helper
                // generate private key
                let dht_key = KeyOption::with_type_id(KeyOption::KEY_ED25519)?;
                let dht_key_enc = base64::encode(dht_key.pvt_key()?);
                let overlay_key = KeyOption::with_type_id(KeyOption::KEY_ED25519)?;
                let overlay_key_enc = base64::encode(overlay_key.pvt_key()?);
                if let Some(ip_address) = config.ip_address {
                    config.adnl_node = Some(
                        serde_json::from_str(
                            adnl_node_test_config!(
                                ip_address,
                                adnl_node_test_key!(NodeNetwork::TAG_DHT_KEY, dht_key_enc),
                                adnl_node_test_key!(NodeNetwork::TAG_OVERLAY_KEY, overlay_key_enc)
                            )
                        )?
                    );
                    config.ip_address = None
                } else {
                    fail!("IP address is not set in default config")
                }
                std::fs::write(config_file_path, serde_json::to_string_pretty(&config)?)?;
                config
            }
        };

        let dht_peers = Vec::new();

        let peers = if let Some(peers_json) = config_json.overlay_peers {
            let mut peers = Vec::new();
            for it in peers_json.iter() {
                peers.push(
                    AdnlNodeConfig::from_json_config(it, false)?
                );
            }
            Some(peers)
        } else {
            None
        };

        let adnl_node = if let Some(adnl_node) = config_json.adnl_node {
            adnl_node
        } else {
            fail!("ADNL node is not configured")
        };

        let global_config_path = config_json.ton_global_config_name
            .map(|name| TonNodeConfig::build_path(configs_dir, &name))
            .transpose()?;
        
        let log_config_path = config_json.log_config_name
            .map(|name| TonNodeConfig::build_path(configs_dir, &name))
            .transpose()?;

        let result = TonNodeConfig {
            log_config_path: log_config_path,
            ton_global_config_path: global_config_path,
            use_global_config: config_json.use_global_config,
            adnl_node,
            port: None,
            overlay_peers: peers,
            dht_peers: Some(dht_peers),
            kafka_consumer_config: config_json.kafka_consumer_config,
            external_db_config: config_json.external_db_config
        };
        Ok(result)
    }

    pub fn adnl_node(&self) -> Result<AdnlNodeConfig> {
        let mut ret = AdnlNodeConfig::from_json_config(&self.adnl_node, true)?;
        if let Some(port) = self.port {
            ret.set_port(port)
        }
        Ok(ret)
    }

    pub fn log_config_path(&self) -> Option<&String> {
        self.log_config_path.as_ref()
    }

    pub fn dht_peers(&self) -> Option<&Vec<AdnlNodeConfig>> {
        self.dht_peers.as_ref()
    }

    pub fn ton_global_config_path(&self) -> Option<&String> {
        self.ton_global_config_path.as_ref()
    }

    pub fn overlay_peers(&mut self) -> Option<Vec<AdnlNodeConfig>> {
        self.overlay_peers.take()
    }

    pub fn use_global_config(&self) -> bool {
        self.use_global_config.clone()
    }

    pub fn kafka_consumer_config(&self) -> Option<KafkaConsumerConfig> {
        self.kafka_consumer_config.clone()
    }

    pub fn external_db_config(&self) -> Option<ExternalDbConfig> {
        self.external_db_config.clone()
    }

    pub fn set_port(&mut self, port: u16) {
        self.port.replace(port);
    }
 
    pub fn load_global_config(&self) -> Result<TonNodeGlobalConfig> {
        let path = self.ton_global_config_path.as_ref()
            .ok_or_else(|| error!("global config informations not found!"))?;
        let data = std::fs::read_to_string(path)
            .map_err(|err| error!("Global config file is not found! : {}", err))?;
        TonNodeGlobalConfig::from_json(&data)
    }

    fn build_path(directory_name: &str, file_name: &str) -> Result<String> {
        let path = Path::new(directory_name);
        let path = path.join(file_name);
        let result = path.to_str()
            .ok_or_else(|| error!("path is not valid!"))?;
        Ok(String::from(result))
    }
}

impl TonNodeGlobalConfig {
    /// Constructor from json file
    pub fn from_json(json : &str) -> Result<Self> {
        let ton_node_global_cfg_json = TonNodeGlobalConfigJson::from_json(&json)?;
        Ok(TonNodeGlobalConfig(serde::export::Some(ton_node_global_cfg_json)
            .ok_or_else(|| error!("Global cannot be parsed!"))?))
    }

    pub fn zero_state(&self) -> Result<BlockIdExt> {
        self.0.zero_state()
    }

    pub fn init_block(&self) -> Result<Option<BlockIdExt>> {
        self.0.init_block()
    }
    
    pub fn dht_nodes(&self) -> Result<Vec<DhtNodeConfig>> {
        self.0.get_dht_nodes_configs()
    }

    pub fn dht_param_a(&self) -> Result<i32> {
        self.0.dht.a.ok_or_else(|| error!("Dht param a is not set!"))
    }

    pub fn dht_param_k(&self) -> Result<i32> {
        self.0.dht.k.ok_or_else(|| error!("Dht param k is not set!"))
        // let res : Vec<AdnlNodeConfig> = if let Some(ton_node_cfg) = &self.ton_node_global_config_json {
        //     ton_node_cfg.get_adnl_nodes_configs()?
        // } else {
        //     fail!("Global config is not found!")
        // };
        // Ok(res)
    }
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
pub struct TonNodeGlobalConfigJson {
    #[serde(alias = "@type")]
    type_node : String,
    dht : DhtGlobalConfig,
    validator : Validator,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct DhtGlobalConfig {
    #[serde(alias = "@type")]
    type_dht : Option<String>,
    k : Option<i32>,
    a : Option<i32>,
    static_nodes : DhtNodes,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct DhtNodes {
    #[serde(alias = "@type")]
    type_dht : Option<String>,
    nodes : Vec<DhtNode>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct DhtNode {
    #[serde(alias = "@type")]
    type_node : Option<String>,
    id : IdDhtNode,
    addr_list : AddressList,
    version : Option <i32>,
    signature : Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct IdDhtNode {
    #[serde(alias = "@type")]
    type_node : Option<String>,
    key : Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct AddressList {
    #[serde(alias = "@type")]
    type_node : Option<String>,
    addrs : Vec<Address>,
    version : Option<i32>,
    reinit_date : Option<i32>,
    priority : Option<i32>,
    expire_at : Option<i32>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
pub struct Address {
    #[serde(alias = "@type")]
    type_node : Option<String>,
    ip : Option<i64>,
    port : Option<u16>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct Validator {
    #[serde(alias = "@type")]
    type_node : Option<String>,
    zero_state : ZeroState,
    init_block : Option<InitBlock>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct ZeroState {
    workchain : Option<i32>,
    shard : Option<i64>,
    seqno : Option<i32>,
    root_hash : Option<String>,
    file_hash : Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct InitBlock {
    workchain : Option<i32>,
    shard : Option<i64>,
    seqno : Option<i32>,
    root_hash : Option<String>,
    file_hash : Option<String>,
}

impl Address {
    pub fn convert_address(&self) -> Result<SocketAddr> {
       let ip = if let Some(addr) = &self.ip() {
             IpAddr::V4(Address::convert_ip_addr(addr)?)
        } else {
             fail!("IP address not found!");
        };

        let port = self.port.ok_or_else(|| error!("Port not found!"))?;
        let addr : SocketAddr = SocketAddr::new(ip, port);
        Ok(addr)
    }

    const IP_ADDR_COUNT_FIELDS : usize = 4;

    pub fn convert_ip_addr(intel_format_ip : &i64) -> Result<Ipv4Addr> {
        let ip_hex = format!("{:08x}", intel_format_ip);
        let mut ip_hex: Vec<u8> = Vec::from_hex(ip_hex)?;

        ip_hex.reverse();
        if ip_hex.len() < Address::IP_ADDR_COUNT_FIELDS {
            fail!("IP address is bad");
        }

        let address = Ipv4Addr::new(ip_hex[3], ip_hex[2], ip_hex[1], ip_hex[0]);
         Ok(address)
    }

    pub fn to_str(&self) -> Result<String> {
         serde::export::Ok(self.convert_address()?.to_string())
    }

    pub fn ip(&self) -> Option<&i64> {
        self.ip.as_ref()
    }
}

pub const PUB_ED25519 : &str = "pub.ed25519";

impl IdDhtNode {

    pub fn convert_key(&self) -> Result<KeyOption> {
        let type_id = self.type_node.as_ref().ok_or_else(|| error!("Type_node is not set!"))?;
       
        let type_id = if type_id.eq(PUB_ED25519) {
            KeyOption::KEY_ED25519
        } else {
            fail!("unknown type_node!")
        };

        let key = if let Some(key) = &self.key {
            base64::decode(key)?
        } else {
            fail!("No public key!");
        };

        let key = &key[..32];
        let pub_key = from_slice!(key, 32);

        let ret = KeyOption::from_type_and_public_key(type_id, &pub_key);
        Ok(ret)
    }
}

/*
impl DhtNode {
    pub fn convert_to_adnl_node_cfg(&self) -> Result<AdnlNodeConfig> {
        let key_option = self.id.convert_key()?;

        //TODO!!!!
        let address = self.addr_list.addrs[0].to_str()?;
        let ret = AdnlNodeConfig::from_ip_address_and_key(
            &address,
            key_option,
            NodeNetwork::TAG_DHT_KEY
        )?;
        Ok(ret)
    }
}
*/

impl TonNodeGlobalConfigJson {
    
    pub fn from_json_file(json_file : &str) -> Result<Self> {
        let file = File::open(json_file)?;
        let reader = BufReader::new(file);
        let json_config : TonNodeGlobalConfigJson = serde_json::from_reader(reader)?;
        Ok(json_config)
    }

    /// Constructs new configuration from JSON data
    pub fn from_json(json : &str) -> Result<Self> {
        let json_config : TonNodeGlobalConfigJson = serde_json::from_str(json)?;
        Ok(json_config)
    }

    pub fn get_dht_nodes_configs(&self) -> Result<Vec<DhtNodeConfig>> {
        let mut result = Vec::new();
        for dht_node in self.dht.static_nodes.nodes.iter() {
            let key = dht_node.id.convert_key()?;
            let mut addrs = Vec::new();
            for addr in dht_node.addr_list.addrs.iter() {
                let ip = if let Some(ip) = addr.ip {
                    ip
                } else {
                    continue;
                };
                let port = if let Some(port) = addr.port {
                    port
                } else {
                    continue
                };
                let addr = Udp {
                    ip: ip as i32,
                    port: port as i32
                }.into_boxed();
                addrs.push(addr);
            }
            let version = if let Some(version) = dht_node.addr_list.version {
                version
            } else {
                continue
            };
            let reinit_date = if let Some(reinit_date) = dht_node.addr_list.reinit_date {
                reinit_date
            } else {
                continue
            };
            let priority = if let Some(priority) = dht_node.addr_list.priority {
                priority
            } else {
                continue
            };
            let expire_at = if let Some(expire_at) = dht_node.addr_list.expire_at {
                expire_at
            } else {
                continue
            };           
            let addr_list = AdnlAddressList {
                addrs: addrs.into(),
                version,
                reinit_date,
                priority,
                expire_at
            }; 
            let version = if let Some(version) = dht_node.version {
                version
            } else {
                continue
            };
            let signature = if let Some(signature) = &dht_node.signature {
                signature
            } else {
                continue
            };
            let node = DhtNodeConfig {
                id: Ed25519 {
                    key: ton::int256(key.pub_key()?.clone())
                }.into_boxed(),
                addr_list,
                version,
                signature: ton::bytes(base64::decode(signature)?)
            };
            result.push(node)//convert_to_dht_node_cfg()?);
        }
        Ok(result)
    }

    pub fn zero_state(&self) -> Result<BlockIdExt> {
        let workchain_id = self
            .validator
            .zero_state
            .workchain
            .ok_or_else(|| error!("Unknown workchain id (of zero_state)!"))?;

        let seqno = self
            .validator
            .zero_state
            .seqno
            .ok_or_else(|| error!("Unknown workchain seqno (of zero_state)!"))?;

        let shard = self
            .validator
            .zero_state
            .shard
            .ok_or_else(|| error!("Unknown workchain shard (of zero_state)!"))?;

        let root_hash = self
            .validator
            .zero_state
            .root_hash
            .as_ref()
            .ok_or_else(|| error!("Unknown workchain root_hash (of zero_state)!"))?;
                
        let root_hash = UInt256::from(base64::decode(&root_hash)?);

        let file_hash = self
            .validator
            .zero_state
            .file_hash
            .as_ref()
            .ok_or_else(|| error!("Unknown workchain file_hash (of zero_state)!"))?;

        let file_hash = UInt256::from(base64::decode(&file_hash)?);

        Ok(BlockIdExt {
            shard_id: ShardIdent::with_tagged_prefix(workchain_id, shard as u64)?,
            seq_no: seqno as u32,
            root_hash,
            file_hash,
        })
    }

    pub fn init_block(&self) -> Result<Option<BlockIdExt>> {
        let init_block = match self.validator.init_block {
            Some(ref init_block) => init_block,
            None => return Ok(None)
        };
        
        let workchain_id = init_block.workchain
            .ok_or_else(|| error!("Unknown workchain id (of zero_state)!"))?;

        let seqno = init_block.seqno
            .ok_or_else(|| error!("Unknown workchain seqno (of zero_state)!"))?;

        let shard = init_block.shard
            .ok_or_else(|| error!("Unknown workchain shard (of zero_state)!"))?;

        let root_hash = init_block.root_hash.as_ref()
            .ok_or_else(|| error!("Unknown workchain root_hash (of zero_state)!"))?;
        let root_hash = UInt256::from(base64::decode(&root_hash)?);

        let file_hash = init_block.file_hash.as_ref()
            .ok_or_else(|| error!("Unknown workchain file_hash (of zero_state)!"))?;
        let file_hash = UInt256::from(base64::decode(&file_hash)?);

        Ok(Some(BlockIdExt {
            shard_id: ShardIdent::with_tagged_prefix(workchain_id, shard as u64)?,
            seq_no: seqno as u32,
            root_hash,
            file_hash,
        }))
    }
}
