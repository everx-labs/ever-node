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

use crate::network::node_network::NodeNetwork;
use adnl::{
    client::AdnlClientConfigJson,
    common::{add_unbound_object_to_map_with_update, Wait},
    node::{AdnlNodeConfig, AdnlNodeConfigJson}, server::{AdnlServerConfig, AdnlServerConfigJson}
};
use ever_crypto::{Ed25519KeyOption, KeyId, KeyOption, KeyOptionJson};
use std::{
    collections::HashMap, convert::TryInto, fs::File, io::BufReader, path::Path,
    sync::{Arc, atomic::{self, AtomicI32} }
};
use ton_api::{
    IntoBoxed, 
    ton::{
        self, adnl::{address::address::Udp, addresslist::AddressList as AdnlAddressList}, 
        dht::node::Node as DhtNodeConfig, pub_::publickey::Ed25519
    }
};
#[cfg(feature="external_db")]
use ton_block::{BASE_WORKCHAIN_ID, MASTERCHAIN_ID};
use ton_block::{BlockIdExt, ShardIdent};
use ton_types::{error, fail, Result, UInt256};


#[macro_export]
macro_rules! key_option_public_key {
    ($key: expr) => {
        format!(
            "{{
               \"type_id\": 1209251014,
               \"pub_key\": \"{}\"
            }}",
            $key
        ).as_str()
    }
}

#[async_trait::async_trait]
pub trait KeyRing : Sync + Send  {
    async fn generate(&self) -> Result<[u8; 32]>;
    // find private key in KeyRing by public key hash
    fn find(&self, key_hash: &[u8; 32]) -> Result<Arc<dyn KeyOption>>;
    fn sign_data(&self, key_hash: &[u8; 32], data: &[u8]) -> Result<Vec<u8>>;
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub struct CellsGcConfig {
    pub gc_interval_sec: u32,
    pub cells_lifetime_sec: u64,
}

impl Default for CellsGcConfig {
    fn default() -> Self {
        CellsGcConfig {
            gc_interval_sec: 900,
            cells_lifetime_sec: 1800,
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct TonNodeConfig {
    log_config_name: Option<String>,
    ton_global_config_name: Option<String>,
    #[cfg(feature="workchains")]
    #[serde(skip_serializing_if = "Option::is_none")]
    workchain: Option<i32>,
    internal_db_path: Option<String>,
    unsafe_catchain_patches_path: Option<String>,
    #[serde(skip_serializing)]
    ip_address: Option<String>,
    adnl_node: Option<AdnlNodeConfigJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    extensions: Option<NodeExtensions>,
    validator_keys: Option<Vec<ValidatorKeysJson>>,
    #[serde(skip_serializing)]
    control_server_port: Option<u16>,
    control_server: Option<AdnlServerConfigJson>,
    kafka_consumer_config: Option<KafkaConsumerConfig>,
    external_db_config: Option<ExternalDbConfig>,
    default_rldp_roundtrip_ms: Option<u32>,
    #[serde(default)]
    test_bundles_config: CollatorTestBundlesGeneralConfig,
    #[serde(default = "default_connectivity_check_config")]
    connectivity_check_config: ConnectivityCheckBroadcastConfig,
    gc: Option<GC>,
    validator_key_ring: Option<HashMap<String, KeyOptionJson>>,
    #[serde(skip)]
    configs_dir: String,
    #[serde(skip)]
    port: Option<u16>,
    #[serde(skip)]
    file_name: String,
    #[serde(default)]
    restore_db: bool
}

pub struct TonNodeGlobalConfig(TonNodeGlobalConfigJson);

#[derive(serde::Deserialize, serde::Serialize)]
pub struct NodeExtensions {
    #[serde(default)]
    pub disable_broadcast_retransmit: bool
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
struct ValidatorKeysJson {
    election_id: i32,
    validator_key_id: String,
    validator_adnl_key_id: Option<String>
}

#[derive(serde::Deserialize, serde::Serialize, Default, Debug, Clone)]
pub struct KafkaConsumerConfig {
    pub group_id: String,
    pub brokers: String,
    pub topic: String,
    pub session_timeout_ms: u32,
    pub run_attempt_timeout_ms: u32
}

#[derive(serde::Deserialize, serde::Serialize, Default, Debug, Clone)]
pub struct GC {
    enable_for_archives: bool,
    archives_life_time_hours: Option<u32>, // Hours
    enable_for_shard_state_persistent: bool,
    #[serde(default = "CellsGcConfig::default")]
    cells_gc_config: CellsGcConfig,
}

#[derive(Debug, Default, serde::Deserialize, serde::Serialize, Clone)]
pub struct TopicMask {
    pub mask: String,
    pub name: String,
}

#[derive(Debug, Default, serde::Deserialize, serde::Serialize, Clone)]
pub struct KafkaProducerConfig {
    pub enabled: bool,
    pub brokers: String,
    pub message_timeout_ms: u32,
    pub topic: Option<String>,
    pub sharded_topics: Option<Vec<TopicMask>>,
    #[serde(default)]
    pub sharding_depth: u32,
    pub attempt_timeout_ms: u32,
    pub message_max_size: usize,
    pub big_messages_storage: Option<String>,
    pub external_message_ref_address_prefix: Option<String>
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
    pub chain_range_producer: KafkaProducerConfig,
    pub bad_blocks_storage: String,
}

#[derive(Debug, Default, serde::Deserialize, serde::Serialize, Clone)]
#[serde(default)]
pub struct CollatorTestBundlesConfig {
    build_for_unknown_errors: bool,
    known_errors: Vec<String>,
    build_for_errors: bool,
    errors: Vec<String>,
    path: String,
}

impl CollatorTestBundlesConfig {

    pub fn is_enable(&self) -> bool {
        self.build_for_unknown_errors ||
            (self.build_for_errors && self.errors.len() > 0)
    }

    pub fn need_to_build_for(&self, error: &str) -> bool {
        self.build_for_unknown_errors &&
            self.known_errors.iter().all(|e| !error.contains(e))
        || self.build_for_errors && 
            self.errors.iter().any(|e| error.contains(e))
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

#[derive(Debug, Default, serde::Deserialize, serde::Serialize, Clone)]
#[serde(default)]
pub struct ConnectivityCheckBroadcastConfig {
    pub enabled: bool,
    pub long_len: usize,
    pub short_period_ms: u64,
    pub long_mult: u8,
}

pub fn default_connectivity_check_config() -> ConnectivityCheckBroadcastConfig {
    ConnectivityCheckBroadcastConfig {
        enabled: true,
        long_len: 2 * 1024,
        short_period_ms: 1000,
        long_mult: 5,
    }
}

impl ConnectivityCheckBroadcastConfig {
    pub const LONG_BCAST_MIN_LEN: usize = 769;

    pub fn check(&self) -> Result<()> {
        if self.long_len < Self::LONG_BCAST_MIN_LEN {
            fail!("long_len should be >= {}", Self::LONG_BCAST_MIN_LEN);
        }
        if self.short_period_ms == 0 {
            fail!("short_period_ms can't have zero value");
        }
        if self.short_period_ms > 1_000_000 {
            fail!("short_period_ms should be <= 1_000_000");
        }
        if self.short_period_ms < 100 {
            fail!("short_period_ms should be >= 100");
        }
        if self.long_mult == 0 {
            fail!("long_mult can't have zero value");
        }
        Ok(())
    }
}

#[derive(Debug, Default, serde::Deserialize, serde::Serialize, Clone)]
#[serde(default)]
pub struct CollatorTestBundlesGeneralConfig {
    pub collator: CollatorTestBundlesConfig,
    pub validator: CollatorTestBundlesConfig,
}

const LOCAL_HOST: &str = "127.0.0.1";

impl TonNodeConfig {

    pub const DEFAULT_DB_ROOT: &'static str = "node_db";    

    #[cfg(feature="external_db")]
    pub fn front_workchain_ids(&self) -> Vec<i32> {
        #[cfg(feature="workchains")]
        match self.workchain {
            None | Some(0) | Some(-1) => vec![MASTERCHAIN_ID, BASE_WORKCHAIN_ID],
            Some(workchain_id) => vec![workchain_id]
        }
        #[cfg(not(feature="workchains"))]
        {
            vec![MASTERCHAIN_ID, BASE_WORKCHAIN_ID]            
        }
    }

    #[cfg(feature="workchains")]
    pub fn workchain_id(&self) -> Option<i32> {
        self.workchain
    }

    pub fn from_file(
        configs_dir: &str,
        json_file_name: &str,
        adnl_config: Option<AdnlNodeConfigJson>,
        default_config_name: &str,
        client_console_key: Option<String>
    ) -> Result<Self> { 
        let config_file_path = TonNodeConfig::build_path(configs_dir, json_file_name)?;
        let config_file = File::open(config_file_path.clone());

        let mut config_json = match config_file {
            Ok(file) => {
                let reader = BufReader::new(file);
                let config: TonNodeConfig = serde_json::from_reader(reader)?;

                if client_console_key.is_some() {
                    println!("Can't generate console_config.json: delete config.json before");
                }
                config
            }
            Err(_) => {
                // generate new config from default_config
                let path = TonNodeConfig::build_path(configs_dir, default_config_name)?;
                let default_config_file = File::open(&path)
                    .map_err(|err| error!("Can`t open {}: {}", path, err))?;

                let reader = BufReader::new(default_config_file);
                let mut config: TonNodeConfig = serde_json::from_reader(reader)?;
                // Set ADNL config
                config.adnl_node = if let Some(adnl_config) = adnl_config {
                    Some(adnl_config)
                } else {
                    let ip_address = if let Some(ip_address) = &config.ip_address {
                        ip_address
                    } else {
                        fail!("IP address is not set in default config")
                    };
                    let (adnl_config, _) = AdnlNodeConfig::with_ip_address_and_private_key_tags(
                        ip_address, 
                        vec![NodeNetwork::TAG_DHT_KEY, NodeNetwork::TAG_OVERLAY_KEY]
                    )?;
                    Some(adnl_config)
                };
                config.create_and_save_console_configs(
                    configs_dir,
                    client_console_key
                )?;
                config.ip_address = None;
                std::fs::write(config_file_path, serde_json::to_string_pretty(&config)?)?;
                config
            }
        };

        config_json.connectivity_check_config.check()?;

        config_json.configs_dir = configs_dir.to_string();
        config_json.file_name = json_file_name.to_string();

        Ok(config_json)
    }

    pub fn adnl_node(&self) -> Result<AdnlNodeConfig> {
        let adnl_node = self.adnl_node.as_ref().ok_or_else(|| error!("ADNL node is not configured!"))?;

        let mut ret = AdnlNodeConfig::from_json_config(&adnl_node)?;
        if let Some(port) = self.port {
            ret.set_port(port)
        }
        Ok(ret)
    }

    pub fn control_server(&self) -> Result<Option<AdnlServerConfig>> {
        match &self.control_server {
            Some(cs) => Ok(Some(AdnlServerConfig::from_json_config(cs)?)),
            None => Ok(None)
        }
    }

    pub fn log_config_path(&self) -> Option<String> {
        if let Some(log_config_name) = &self.log_config_name {
            if let Ok(log_path) = TonNodeConfig::build_path(&self.configs_dir, &log_config_name) {
                return Some(log_path);
            }
        }
        None
    }

    pub fn unsafe_catchain_patches_files(&self) -> Vec<String> {
        let mut result = Vec::new();
        if let Some(catchain_patches) = &self.unsafe_catchain_patches_path {
            if let Ok(log_path) = TonNodeConfig::build_path(&self.configs_dir, &catchain_patches) {
                if let Ok(dir) = std::fs::read_dir(log_path) {
                    for filename in dir.into_iter() {
                        if let Ok(fname) = filename {
                            if let Some(path_str) = fname.path().to_str() {
                                if path_str.ends_with(".json") {
                                    result.push(path_str.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
        result
    }

    pub fn gc_archives_life_time_hours(&self) -> Option<u32> {
        match &self.gc {
            Some(gc) => {
                if !gc.enable_for_archives {
                    return None;
                }
                match gc.archives_life_time_hours {
                    Some(life_time) => return Some(life_time),
                    None => return Some(0)
                }
            },
            None => None
        }
    }

    pub fn kafka_consumer_config(&self) -> Option<KafkaConsumerConfig> {
        self.kafka_consumer_config.clone()
    }

    pub fn internal_db_path(&self) -> &str {
        self.internal_db_path.as_ref().map(|path| path.as_str()).unwrap_or(Self::DEFAULT_DB_ROOT)
    }

    pub fn cells_gc_config(&self) -> CellsGcConfig {
        match &self.gc {
            Some(conf) => conf.cells_gc_config.clone(),
            None => CellsGcConfig::default(),
        }
    }

    pub fn enable_shard_state_persistent_gc(&self) -> bool {
        self.gc.as_ref().map(|c| c.enable_for_shard_state_persistent).unwrap_or(false)
    }
    
  
    pub fn default_rldp_roundtrip(&self) -> Option<u32> {
        self.default_rldp_roundtrip_ms
    }

    #[cfg(feature = "external_db")]
    pub fn external_db_config(&self) -> Option<ExternalDbConfig> {
        self.external_db_config.clone()
    }

    pub fn test_bundles_config(&self) -> &CollatorTestBundlesGeneralConfig {
        &self.test_bundles_config
    }
    pub fn connectivity_check_config(&self) -> &ConnectivityCheckBroadcastConfig {
        &self.connectivity_check_config
    }
    pub fn extensions(&self) -> Option<&NodeExtensions> {
        self.extensions.as_ref()
    }
    pub fn restore_db(&self) -> bool {
        self.restore_db
    }

 
    pub fn load_global_config(&self) -> Result<TonNodeGlobalConfig> {
        let name = self.ton_global_config_name.as_ref().ok_or_else(
            || error!("global config information not found in config.json!")
        )?;
        let global_config_path = TonNodeConfig::build_path(&self.configs_dir, &name)?;
/*        
        let data = std::fs::read_to_string(global_config_path)
            .map_err(|err| error!("Global config file is not found! : {}", err))?;
*/
        TonNodeGlobalConfig::from_json_file(global_config_path.as_str())
    }

// Unused
//    pub fn remove_all_validator_keys(&mut self) {
//        self.validator_keys = None;
//    }

    fn create_and_save_console_configs(
        &mut self,
        configs_dir: &str,
        client_pub_key: Option<String>
    ) -> Result<()> {
        let server_address = if let Some (port) = self.control_server_port {
            format!("{}:{}", LOCAL_HOST, port)
        } else {
            println!(
                "Can`t generate console_config.json: default_config.json doesn`t contain control_server_port."
            );
            return Ok(());
        };
        let (server_private_key, server_key) = Ed25519KeyOption::generate_with_json()?;

        // generate and save client console template
        let config_file_path = TonNodeConfig::build_path(configs_dir, "console_config.json")?;
        let console_client_config = AdnlClientConfigJson::with_params(
            &server_address,
            serde_json::from_str(key_option_public_key!(
                base64::encode(&server_key.pub_key()?)
            ))?,
            None
        );
        std::fs::write(config_file_path, serde_json::to_string_pretty(&console_client_config)?)
            .map_err(|err| error!("Can`t create console_config.json: {}", err))?;

        // generate and save server config
        let client_keys = if let Some(client_key) = client_pub_key {
            vec![serde_json::from_str(&client_key)?]
        } else {
            Vec::new()
        };

        let console_server_config = AdnlServerConfigJson::with_params(
            server_address,
            server_private_key,
            client_keys,
            None
        );

        self.control_server = Some(console_server_config);
        self.control_server_port = None;
        Ok(())
    }

    fn get_validator_key_info(
        &self,
        validator_key_id: &str,
    ) -> Result<Option<ValidatorKeysJson>> {
        if let Some(validator_keys) = &self.validator_keys {
            for key_json in validator_keys {
                if key_json.validator_key_id == validator_key_id {
                    return Ok(Some(key_json.clone()));
                }
            }
        }
        Ok(None)
    }

    fn get_validator_key_info_by_election_id(
        &self,
        election_id: &i32,
    ) -> Result<Option<ValidatorKeysJson>> {
        if let Some(validator_keys) = &self.validator_keys {
            for key_json in validator_keys {
                if key_json.election_id == *election_id {
                    return Ok(Some(key_json.clone()));
                }
            }
        }
        Ok(None)
    }

    fn update_validator_key_info(&mut self, updated_info: ValidatorKeysJson) -> Result<ValidatorKeysJson> {
        if let Some(validator_keys) = &mut self.validator_keys {
            for keys_info in validator_keys.iter_mut() {
                    if keys_info.election_id == updated_info.election_id {
                        keys_info.validator_key_id = updated_info.validator_key_id;
                        keys_info.validator_adnl_key_id = updated_info.validator_adnl_key_id;
                        return Ok(keys_info.clone());
                }
            }
        } 
        fail!("Validator keys information was not found!");
    }

    fn build_path(directory_name: &str, file_name: &str) -> Result<String> {
        let path = Path::new(directory_name);
        let path = path.join(file_name);
        let result = path.to_str()
            .ok_or_else(|| error!("path is not valid!"))?;
        Ok(String::from(result))
    }

    fn save_to_file(&self, file_name: &str) -> Result<()> {
        let config_file_path = TonNodeConfig::build_path(&self.configs_dir, file_name)?;
        std::fs::write(config_file_path, serde_json::to_string_pretty(&self)?)?;
        Ok(())
    }

    fn generate_and_save_keys(&mut self) -> Result<([u8; 32], Arc<dyn KeyOption>)> {
        #[cfg(feature="workchains")]
        let (private, public) = crate::validator::validator_utils::mine_key_for_workchain(self.workchain);
        #[cfg(not(feature="workchains"))]
        let (private, public) = Ed25519KeyOption::generate_with_json()?;
        let key_id = public.id().data();
        let key_ring = self.validator_key_ring.get_or_insert_with(|| HashMap::new());
        key_ring.insert(base64::encode(key_id), private);
        Ok((key_id.clone(), public))
    }

    fn is_correct_election_id(&self, election_id: i32) -> bool {
        if let Some(validator_keys) = &self.validator_keys {
            for key_json in validator_keys {
                if key_json.election_id > election_id {
                    return false;
                }
            }
        }
        return true;
    }

    fn add_validator_key(&mut self, key_id: &[u8; 32], election_id: i32) -> Result<ValidatorKeysJson> {
        let key_info = ValidatorKeysJson {
            election_id,
            validator_key_id: base64::encode(key_id),
            validator_adnl_key_id: None
        };

        if !self.is_correct_election_id(election_id) {
            fail!("Invalid arg: bad election_id!");
        }
        let added_key_info = self.get_validator_key_info_by_election_id(&election_id)?;
        match &mut self.validator_keys {
            Some(validator_keys) => {
                match added_key_info {
                    Some(_) => {
                        self.update_validator_key_info(key_info.clone())?;
                    },
                    None => {
                        validator_keys.push(key_info.clone());
                    },
                }
            },
            None => {
                let mut keys  = Vec::new();
                keys.push(key_info.clone());
                self.validator_keys = Some(keys);
            }
        }
        Ok(key_info)
    }

    fn add_validator_adnl_key(
        &mut self,
        validator_key_id: &[u8; 32],
        adnl_key_id: &[u8; 32]
    ) -> Result<ValidatorKeysJson> {
        if let Some(mut key_info) = self.get_validator_key_info(&base64::encode(validator_key_id))? {
            key_info.validator_adnl_key_id = Some(base64::encode(adnl_key_id));
            self.update_validator_key_info(key_info)
        } else {
            fail!("Validator key was not added!")
        }
    }

    fn remove_validator_key(&mut self, validator_key_id: String, election_id: i32) -> Result<bool> {
        if let Some(validator_keys) = self.validator_keys.as_mut() {
            let pos = validator_keys.iter()
                .position(|item| item.validator_key_id == validator_key_id && item.election_id == election_id);
            if let Some(pos) = pos {
                validator_keys.swap_remove(pos);
                return Ok(true)
            }
        }
        Ok(false)
    }

    fn remove_key_from_key_ring(&mut self, validator_key_id: &String) {
        if let Some(key_ring) = self.validator_key_ring.as_mut() {
            key_ring.remove(validator_key_id);
        }
    }
}

pub enum ConfigEvent {
    AddValidatorAdnlKey(Arc<KeyId>, i32),
    //RemoveValidatorAdnlKey(Arc<KeyId>, i32)
}

#[async_trait::async_trait]
pub trait NodeConfigSubscriber: Send + Sync {
    async fn event(&self, sender: ConfigEvent) -> Result<bool>;
}

#[derive(Debug)]
enum Task {
    Generate,
    AddValidatorKey([u8; 32], i32),
    AddValidatorAdnlKey([u8; 32], [u8; 32]),
    GetKey([u8; 32]),
    StoreStatesGcInterval(u32),
    #[cfg(feature="workchains")]
    StoreWorkchainId(i32),
}

#[derive(Debug)]
enum Answer {
    Generate(Result<[u8; 32]>),
    GetKey(Option<Arc<dyn KeyOption>>),
    Result(Result<()>),
}

pub struct NodeConfigHandlerContext {
    reader: tokio::sync::mpsc::UnboundedReceiver<Arc<(Arc<Wait<Answer>>, Task)>>,
    config: TonNodeConfig,
}

pub struct NodeConfigHandler {
    runtime_handle: tokio::runtime::Handle,
    sender: tokio::sync::mpsc::UnboundedSender<Arc<(Arc<Wait<Answer>>, Task)>>,
    key_ring: Arc<lockfree::map::Map<String, Arc<dyn KeyOption>>>,
    validator_keys: Arc<ValidatorKeys>,
    #[cfg(feature="workchains,external_db")]
    workchain_id: Option<i32>,
}

impl NodeConfigHandler {
    pub fn create(
        config: TonNodeConfig,
        runtime_handle: tokio::runtime::Handle
    ) -> Result<(Arc<Self>, NodeConfigHandlerContext)> {
        let (sender, reader) = tokio::sync::mpsc::unbounded_channel();
        let config_handler = Arc::new(NodeConfigHandler {
            runtime_handle,
            sender,
            key_ring: Arc::new(lockfree::map::Map::new()),
            validator_keys: Arc::new(ValidatorKeys::new()),
            #[cfg(feature="workchains,external_db")]
            workchain_id: config.workchain,
        });

        Ok((config_handler, NodeConfigHandlerContext{reader, config}))
    }

    pub fn get_validator_status(&self) -> bool {
        self.validator_keys.is_empty()
    }

    pub async fn add_validator_key(
        &self, key_hash: &[u8; 32], elecation_date: ton::int,
    ) -> Result<()> {
        let (wait, mut queue_reader) = Wait::new();
        let pushed_task = Arc::new((wait.clone(), Task::AddValidatorKey(key_hash.clone(), elecation_date)));
        wait.request();
        if let Err(e) = self.sender.send(pushed_task) {
            fail!("Error add_validator_key: {}", e);
        }
        match wait.wait(&mut queue_reader, true).await {
            Some(None) => fail!("Answer was not set!"),
            Some(Some(Answer::Result(result))) => result,
            Some(Some(_)) => fail!("Bad answer (AddValidatorKey)!"),
            None => fail!("Waiting returned an internal error!")
        }
    }

    pub async fn add_validator_adnl_key(
        &self,
        validator_key_hash: &[u8; 32],
        validator_adnl_key_hash: &[u8; 32]
    ) -> Result<()> {
        let (wait, mut queue_reader) = Wait::new();
        let pushed_task = Arc::new((
            wait.clone(), 
            Task::AddValidatorAdnlKey(validator_key_hash.clone(), validator_adnl_key_hash.clone())
        ));

        wait.request();
        if let Err(e) = self.sender.send(pushed_task) {
            fail!("Error add_validator_adnl_key: {}", e);
        }
        match wait.wait(&mut queue_reader, true).await {
            Some(None) => fail!("Answer was not set!"),
            Some(Some(Answer::Result(result))) => result,
            Some(Some(_)) => fail!("Bad answer (AddValidatorAdnlKey)!"),
            None => fail!("Waiting returned an internal error!")
        }
    }

    #[cfg(feature="workchains")]
    pub fn store_workchain(&self, workchain_id: i32) {
        let (wait, _) = Wait::new();
        let pushed_task = Arc::new((wait.clone(), Task::StoreWorkchainId(workchain_id)));
        wait.request();
        if let Err(e) = self.sender.send(pushed_task) {
            log::warn!("Problem store workchain_id: {}", e);
        }
    }

    pub fn store_states_gc_interval(&self, interval: u32) {
        let (wait, _) = Wait::new();
        let pushed_task = Arc::new((wait.clone(), Task::StoreStatesGcInterval(interval)));
        wait.request();
        if let Err(e) = self.sender.send(pushed_task) {
            log::warn!("Problem store states gc interval: {}", e);
        }
    }

// Unused
///// returns validator's public key
//    pub fn get_current_validator_key(&self, vset: &ValidatorSet) -> Option<[u8; 32]> {
//        // search by adnl_id in validator_keys first
//        for id_key in self.validator_keys.values.iter() {
//            if let Some(adnl_id) = id_key.1.validator_adnl_key_id.as_ref() {
//                match UInt256::from_str(adnl_id) {
//                    Ok(adnl_id) => {
//                        let pub_key_opt = vset.list().iter().find_map(|descr| {
//                            if descr.adnl_addr.as_ref() == Some(&adnl_id) {
//                                Some(descr.public_key.key_bytes().clone())
//                            } else {
//                                None
//                            }
//                        });
//                        if let Some(pub_key) = pub_key_opt.as_ref() {
//                            log::info!("get_current_validator_key returns pub_key {}", hex::encode(pub_key));
//                            return pub_key_opt
//                        }
//                    }
//                    Err(err) => log::warn!("adnl_id error: {}", err)
//                }
//            }
//        }
//        // then search by key_id from vset in keyring
//        for descr in vset.list().iter() {
//            let key_id = base64::encode(descr.compute_node_id_short().as_slice());
//            let pub_key_found = self.key_ring.iter().position(|k_v| k_v.0 == key_id).is_some();
//            if pub_key_found {
//                log::info!("get_current_validator_key returns pub_key {}", hex::encode(descr.public_key.key_bytes()));
//                return Some(descr.public_key.key_bytes().clone())
//            }
//        }
//        log::warn!("get_current_validator_key key not found");
//        None
//    }

// Unused
//    pub fn workchain_id(&self) -> Option<i32> {
//        self.workchain_id
//    }

    pub fn get_actual_validator_adnl_ids(&self) -> Result<Vec<Arc<KeyId>>> {
        let adnl_ids = self.validator_keys.get_validator_adnl_ids();
        let mut result = Vec::new();

        for adnl_id in adnl_ids.iter() {
            let id = base64::decode(adnl_id)?;
            result.push(KeyId::from_data(id[..].try_into()?));
        }
        Ok(result)
    }

    pub async fn get_validator_key(&self, key_id: &Arc<KeyId>) -> Option<(Arc<dyn KeyOption>, i32)> {
        match self.validator_keys.get(&base64::encode(key_id.data())) {
            Some(key) => {
                //       let result = if let Some(key) = self.key_ring.get(&key_id) {
                //           Some(key.(val(), key_election_id))
                let result = if let Some(key_opt) = self.get_key_raw(*key_id.data()).await {
                    Some((key_opt, key.election_id))
                } else {
                    None
                };
                result
            },
            None => None,
        }
    }

    async fn get_key_raw(&self, key_hash: [u8; 32]) -> Option<Arc<dyn KeyOption>> {
        let (wait, mut queue_reader) = Wait::new();
        let pushed_task = Arc::new((wait.clone(), Task::GetKey(key_hash)));
        wait.request();
        if let Err(e) = self.sender.send(pushed_task) {
            log::warn!("Error get_key_raw {}", e);
            return None;
        }
        match wait.wait(&mut queue_reader, true).await {
            Some(Some(Answer::GetKey(key))) => key,
            _ => return None
        }
    }

    fn generate_and_save(
        key_ring: &Arc<lockfree::map::Map<String, Arc<dyn KeyOption>>>,
        config: &mut TonNodeConfig,
        config_name: &str
    ) -> Result<[u8; 32]> {
        let (key_id, public_key) = config.generate_and_save_keys()?;
        config.save_to_file(config_name)?;

        let id = base64::encode(&key_id);
        key_ring.insert(id, public_key.clone());
        Ok(key_id)
    }

    fn revision_validator_keys(
        validator_keys: Arc<ValidatorKeys>,
        config: &mut TonNodeConfig
    )-> Result<()> {
        loop {
            match &config.validator_keys {
                Some(config_validator_keys) => {
                    if config_validator_keys.len() > 2 {
                        let oldest_validator_key = NodeConfigHandler::get_oldest_validator_key(&config);
                        if let Some(oldest_key) = oldest_validator_key {
                                config.remove_validator_key(
                                    oldest_key.validator_key_id.clone(),
                                    oldest_key.election_id
                                )?;
                                validator_keys.remove(&oldest_key)?;
                                config.remove_key_from_key_ring(&oldest_key.validator_key_id.clone());
                                if let Some(adnl_key_id) = oldest_key.validator_adnl_key_id {
                                    config.remove_key_from_key_ring(&adnl_key_id);
                                }
                        }
                    } else {
                        break;
                    }
                }, 
                None => break
            }
        }
        Ok(())
    }

    fn add_validator_adnl_key_and_save(
        self: Arc<Self>,
        validator_keys: Arc<ValidatorKeys>,
        config: &mut TonNodeConfig,
        validator_key_hash: &[u8; 32],
        validator_adnl_key_hash: &[u8; 32],
        subscribers: Vec<Arc<dyn NodeConfigSubscriber>>
    )-> Result<()> {
        let key = config.add_validator_adnl_key(&validator_key_hash, validator_adnl_key_hash)?;
        let election_id = key.election_id.clone();
        validator_keys.add(key)?;

        let adnl_key_id = KeyId::from_data(*validator_adnl_key_hash);

        self.clone().runtime_handle.spawn(async move {
            for subscriber in subscribers.iter() {
                if let Err(e) = subscriber.event(
                    ConfigEvent::AddValidatorAdnlKey(adnl_key_id.clone(), election_id)
                ).await {
                    log::warn!("subscriber error: {:?}", e);
                }
            }
        });
        // check validator keys
        Self::revision_validator_keys(validator_keys, config)?;
        config.save_to_file(&config.file_name)?;
        Ok(())
    }

    fn add_validator_key_and_save(
        validator_keys: Arc<ValidatorKeys>,
        config: &mut TonNodeConfig,
        key_id: &[u8; 32],
        election_id: i32
    )-> Result<()> {
        let key = config.add_validator_key(&key_id, election_id)?;
        validator_keys.add(key)?;
        config.save_to_file(&config.file_name)?;
        Ok(())
    }

    fn get_oldest_validator_key(config: &TonNodeConfig) -> Option<ValidatorKeysJson> {
        let mut oldest_validator_key: Option<ValidatorKeysJson> = None;
        if let Some(validator_keys) = &config.validator_keys {
            for key in validator_keys.iter() {
                if let Some(oldest_val_key) = &oldest_validator_key {
                    if key.election_id < oldest_val_key.election_id {
                        oldest_validator_key = Some(key.clone());
                    }
                } else {
                    oldest_validator_key = Some(key.clone());
                }
            }
        }
        oldest_validator_key
    }

    fn get_key(config: &TonNodeConfig, key_id: [u8; 32]) -> Option<Arc<dyn KeyOption>> {
        if let Some(validator_key_ring) = &config.validator_key_ring {
            if let Some(key_data)  = validator_key_ring.get(&base64::encode(&key_id)) {
                match Ed25519KeyOption::from_private_key_json(&key_data) {
                    Ok(key) => { return Some(key)},
                    _ => return None
                }
            }
        }
        None
    }

    fn load_config(&self, config: &TonNodeConfig, subscribers: &Vec<Arc<dyn NodeConfigSubscriber>>) -> Result<()> {
        // load key ring
        if let Some(key_ring) = &config.validator_key_ring {
            for (key_id, key) in key_ring.iter() {
                if let Err(e) = self.add_key_to_dynamic_key_ring(key_id.to_string(), key) {
                    log::warn!("fail added key from key ring: {}", e);
                }
            }
        }

        // load validator keys
        if let Some(validator_keys) = &config.validator_keys {
            for key in validator_keys.iter() {
                if let Err(e) = self.validator_keys.add(key.clone()) {
                    log::warn!("fail added key to validator keys map: {}", e);
                }
                match &key.validator_adnl_key_id {
                    None => { continue; }
                    Some(validator_adnl_key_id) => {
                        let adnl_key_id = base64::decode(&validator_adnl_key_id)?;
                        let adnl_key_id = KeyId::from_data(adnl_key_id[..].try_into()?);
                        let election_id = key.election_id;
                        let subscribers = subscribers.clone();
                        self.clone().runtime_handle.spawn(async move {
                            for subscriber in subscribers.iter() {
                                if let Err(e) = subscriber.event(
                                    ConfigEvent::AddValidatorAdnlKey(adnl_key_id.clone(), election_id)
                                ).await {
                                    log::warn!("subscriber error: {:?}", e);
                                }
                            }
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn add_key_to_dynamic_key_ring(&self, key_id: String, key_json: &KeyOptionJson) -> Result<()> {
        if let Some(key) = self.key_ring.insert(key_id, Ed25519KeyOption::from_private_key_json(key_json)?) {
            log::warn!("Added key was already in key ring collection (id: {})", key.key());
        }
        
        Ok(())
    }

    pub fn start_sheduler(
        self: Arc<Self>,
        config_handler_context: NodeConfigHandlerContext,
        subscribers: Vec<Arc<dyn NodeConfigSubscriber>>
    ) -> Result<()> {
        let name = config_handler_context.config.file_name.clone();
        let mut actual_config = config_handler_context.config;
        let mut reader = config_handler_context.reader;
        let key_ring = self.key_ring.clone();
        let validator_keys = self.validator_keys.clone();
        self.load_config(&actual_config, &subscribers)?;
        
        self.clone().runtime_handle.spawn(async move {
            while let Some(task) = reader.recv().await {
                let answer = match task.1 {
                    Task::Generate => {
                        let result = NodeConfigHandler::generate_and_save(&key_ring, &mut actual_config, &name);
                        Answer::Generate(result)
                    }
                    Task::AddValidatorAdnlKey(key, adnl_key) => {
                        let result = NodeConfigHandler::add_validator_adnl_key_and_save(
                            self.clone(),
                            validator_keys.clone(),
                            &mut actual_config,
                            &key,
                            &adnl_key,
                            subscribers.clone()
                        );
                        Answer::Result(result)
                    }
                    Task::AddValidatorKey(key, election_id) => {
                        let result = NodeConfigHandler::add_validator_key_and_save(
                            validator_keys.clone(), &mut actual_config, &key, election_id
                        );
                        Answer::Result(result)
                    }
                    Task::GetKey(key_data) => {
                        let result = NodeConfigHandler::get_key(&actual_config, key_data);
                        Answer::GetKey(result)
                    }
                    #[cfg(feature="workchains")]
                    Task::StoreWorkchainId(workchain_id) => {
                        actual_config.workchain = Some(workchain_id);
                        let result = actual_config.save_to_file(&name);
                        Answer::Result(result)
                    }
                    Task::StoreStatesGcInterval(interval) => {
                        if let Some(c) = &mut actual_config.gc {
                            c.cells_gc_config.gc_interval_sec = interval;
                        } else {
                            actual_config.gc = Some(GC {
                                cells_gc_config: CellsGcConfig {
                                    gc_interval_sec: interval,
                                    ..Default::default()
                                },
                                ..Default::default()
                            });
                        }
                        let result = actual_config.save_to_file(&name);
                        Answer::Result(result)
                    }
                };
                task.0.respond(Some(answer));
            }
            reader.close();
        });
        Ok(())
    }
}

#[async_trait::async_trait]
impl KeyRing for NodeConfigHandler {
    async fn generate(&self) -> Result<[u8; 32]> {
        let (wait, mut queue_reader) = Wait::new();
        let pushed_task = Arc::new((wait.clone(), Task::Generate));
        wait.request();
        if let Err(e) = self.sender.send(pushed_task) {
            fail!("Error generate: {}", e);
        }
        match wait.wait(&mut queue_reader, true).await {
            Some(None) => fail!("Answer was not set!"),
            Some(Some(Answer::Generate(result))) => result,
            Some(Some(_)) => fail!("Bad answer (Generate)!"),
            None => fail!("Waiting returned an internal error!")
        }
    }

    fn sign_data(&self, key_hash: &[u8; 32], data: &[u8]) -> Result<Vec<u8>> {
        let private = self.find(key_hash)?;
        Ok(private.sign(data)?.to_vec())
    }

    // find private key in KeyRing by public key hash
    fn find(&self, key_id: &[u8; 32]) -> Result<Arc<dyn KeyOption>> {
       let id = base64::encode(key_id);
        match self.key_ring.get(&id) {
            Some(key) => Ok(key.val().clone()),
            None => fail!("key not found for hash: {}", &id)
        }
    }
}

impl TonNodeGlobalConfig {

    /// Constructor from json file
    pub fn from_json_file(json_file: &str) -> Result<Self> {
        let ton_node_global_cfg_json = TonNodeGlobalConfigJson::from_json_file(json_file)?;
        Ok(TonNodeGlobalConfig(ton_node_global_cfg_json))
    }
/*
    pub fn from_json(json : &str) -> Result<Self> {
        let ton_node_global_cfg_json = TonNodeGlobalConfigJson::from_json(&json)?;
        Ok(TonNodeGlobalConfig(ton_node_global_cfg_json))
    }
*/

    pub fn zero_state(&self) -> Result<BlockIdExt> {
        self.0.zero_state()
    }

    pub fn init_block(&self) -> Result<Option<BlockIdExt>> {
        self.0.init_block()
    }
    
    pub fn dht_nodes(&self) -> Result<Vec<DhtNodeConfig>> {
        self.0.get_dht_nodes_configs()
    }

// Unused
//    pub fn dht_param_a(&self) -> Result<i32> {
//        self.0.dht.a.ok_or_else(|| error!("Dht param a is not set!"))
//    }

// Unused
//    pub fn dht_param_k(&self) -> Result<i32> {
//        self.0.dht.k.ok_or_else(|| error!("Dht param k is not set!"))
//    }

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

pub const PUB_ED25519 : &str = "pub.ed25519";

impl IdDhtNode {

    pub fn convert_key(&self) -> Result<Arc<dyn KeyOption>> {
        let type_id = self.type_node.as_ref().ok_or_else(|| error!("Type_node is not set!"))?;
       
        if !type_id.eq(PUB_ED25519) {
            fail!("unknown type_node!")
        };

        let key = if let Some(key) = &self.key {
            base64::decode(key)?
        } else {
            fail!("No public key!");
        };

        let pub_key = key[..32].try_into()?;
        Ok(Ed25519KeyOption::from_public_key(pub_key))
    }
}

impl TonNodeGlobalConfigJson {
    
    /// Constructs new configuration from JSON data
    pub fn from_json_file(json_file: &str) -> Result<Self> {
        match File::open(json_file) {
            Ok(file) => {
                let reader = BufReader::new(file);
                Ok(serde_json::from_reader(reader)?)
            }
            Err(err) => fail!("cannot open file {} : {}", json_file, err)
        }
    }
/*
    pub fn from_json(json: &str) -> Result<Self> {
        let json_config: TonNodeGlobalConfigJson = serde_json::from_str(json)?;
        Ok(json_config)
    }
*/

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
                    key: UInt256::with_array(key
                        .pub_key()?
                        .try_into()?
                    )
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
                
        let root_hash = UInt256::from_str(&root_hash)?;

        let file_hash = self
            .validator
            .zero_state
            .file_hash
            .as_ref()
            .ok_or_else(|| error!("Unknown workchain file_hash (of zero_state)!"))?;

        let file_hash = UInt256::from_str(&file_hash)?;

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
        let root_hash = UInt256::from_str(&root_hash)?;

        let file_hash = init_block.file_hash.as_ref()
            .ok_or_else(|| error!("Unknown workchain file_hash (of zero_state)!"))?;
        let file_hash = UInt256::from_str(&file_hash)?;

        Ok(Some(BlockIdExt {
            shard_id: ShardIdent::with_tagged_prefix(workchain_id, shard as u64)?,
            seq_no: seqno as u32,
            root_hash,
            file_hash,
        }))
    }
}

struct ValidatorKeys {
    values: lockfree::map::Map<i32, ValidatorKeysJson>, // election_id, keys_info
    index: lockfree::map::Map<i32, i32>,                // current_election_id, next_election_id
    first: AtomicI32
}

impl ValidatorKeys {
    fn new() -> Self {
        ValidatorKeys {
            values: lockfree::map::Map::new(),
            index: lockfree::map::Map::new(),
            first: AtomicI32::new(0)
        }
    }

    fn is_empty(&self) -> bool {
        self.first.load(atomic::Ordering::Relaxed) > 0
    }

    fn add(&self, key: ValidatorKeysJson) -> Result<()> {
        // inserted in sorted order
        let mut first = false;

        add_unbound_object_to_map_with_update(
            &self.values, 
            key.election_id, 
            |_| {
                if self.first.compare_exchange(
                    0, key.election_id, atomic::Ordering::Relaxed, atomic::Ordering::Relaxed
                ).is_ok() {
                    first = true;
                }
                Ok(Some(key.clone()))
            }
        )?;

        if first {
            return Ok(());
        }

        let mut current = self.first.load(atomic::Ordering::Relaxed);
        if current > key.election_id {
            add_unbound_object_to_map_with_update(
                &self.index, 
                key.election_id, 
                |_| {
                    if let Err(prev) = self.first.fetch_update(
                        atomic::Ordering::Relaxed, 
                        atomic::Ordering::Relaxed, 
                        |x| {
                            if x > key.election_id {
                                Some(key.election_id)
                            } else {
                                None
                            }
                        }
                    ) {
                        let old = self.index.insert(prev, key.election_id).ok_or_else(
                            || error!("validator keys collections was broken!")
                        )?;
                        Ok(Some(*old.val()))
                    } else {
                        Ok(Some(current))
                    }
                }
            )?;
            return Ok(());
        } else if current == key.election_id {
            return Ok(())
        }

        loop {
            if let Some(item) = &self.index.get(&current) {
                if item.val() > &key.election_id {
                    add_unbound_object_to_map_with_update(
                        &self.index, 
                        *item.key(), 
                        |_| {
                            self.index.insert(key.election_id, *item.val());
                            Ok(Some(key.election_id))
                        }
                    )?;
                    break;
                } else if item.val() == &key.election_id {
                    break;
                } else {
                    current = *item.val();
                }
            } else {
                self.index.insert(current, key.election_id);
                break;
            };
        }

        Ok(())
    }

    fn remove(&self, key: &ValidatorKeysJson) -> Result<bool> {
        let mut current = self.first.load(atomic::Ordering::Relaxed);

        if current == key.election_id {
            if let Some(item) = &self.index.get(&current) {
                self.first.store(*item.val(), atomic::Ordering::Relaxed);
            } else {
                self.first.store(0, atomic::Ordering::Relaxed);
            }
            return Ok(true);
        }

        while let Some(item) = &self.index.get(&current) {
            if item.val() == &key.election_id {
                if let Some(removed_item) = &self.index.get(&item.val()) {
                    self.index.insert(*item.key(), *removed_item.val());
                } else {
                    // remove last element
                    self.index.remove(item.key());
                }
                return Ok(true)
            } else {
                current = *item.val();
            }
        }
        Ok(false)
    }

    fn get(&self, id_key: &str) -> Option<ValidatorKeysJson> {
        let mut current = self.first.load(atomic::Ordering::Relaxed);
        loop {
            if let Some(result) = self.get_try(id_key, current) {
                return Some(result)
            }
            match self.index.get(&current) {
                Some(next) => current = *next.val(),
                None => return None
            }
        }
    }

    fn get_try(&self, id_key: &str, index: i32) -> Option<ValidatorKeysJson> {
        let mut result = None;
        if let Some(key) = self.values.get(&index) {
            if &key.val().validator_key_id == id_key {
                result = Some(key.val().clone());
            } else if let Some(adnl_key) = &key.val().validator_adnl_key_id {
                if adnl_key == id_key {
                    result = Some(key.val().clone());
                }
            }
        }
        result
    }

    fn get_validator_adnl_ids(&self) -> Vec<String> {
        let mut adnl_ids = Vec::new();
        let mut current = self.first.load(atomic::Ordering::Relaxed);
        loop {
            if let Some(validator_info) = self.values.get(&current) {
                if let Some(adnl_key) = &validator_info.val().validator_adnl_key_id {
                    adnl_ids.push(adnl_key.clone());
                } else {
                    adnl_ids.push(validator_info.val().validator_key_id.clone());
                }
            }
            match self.index.get(&current) {
                Some(next) => current = *next.val(),
                None => return adnl_ids
            }
        }
    }
}
