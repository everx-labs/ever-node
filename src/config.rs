use crate::{network::node_network::NodeNetwork};
use adnl::{from_slice, client::AdnlClientConfigJson,
    common::{
        add_object_to_map, add_object_to_map_with_update, KeyId, KeyOption,
        KeyOptionJson, Wait
    },
    node::{AdnlNodeConfig, AdnlNodeConfigJson},
    server::{AdnlServerConfig, AdnlServerConfigJson}
};
use hex::FromHex;
use std::{
    collections::HashMap, io::{BufReader}, fs::File,
    net::{Ipv4Addr, IpAddr, SocketAddr}, path::Path,
    sync::{Arc, atomic::{self, AtomicI32} }, time::Duration
};
use ton_api::{
    IntoBoxed, 
    ton::{
        self, adnl::{address::address::Udp, addresslist::AddressList as AdnlAddressList}, 
        dht::node::Node as DhtNodeConfig, pub_::publickey::Ed25519
    }
};
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
    fn find(&self, key_hash: &[u8; 32]) -> Result<Arc<KeyOption>>;
    fn sign_data(&self, key_hash: &[u8; 32], data: &[u8]) -> Result<Vec<u8>>;
}

pub trait NodeConfigSubscriber: Send + Sync {
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct TonNodeConfig {
    log_config_name: Option<String>,
    ton_global_config_name: Option<String>,
    #[serde(skip_serializing)]
    ip_address: Option<String>,
    adnl_node: Option<AdnlNodeConfigJson>,
    validator_keys: Option<Vec<ValidatorKeysJson>>,
    #[serde(skip_serializing)]
    control_server_port: Option<u16>,
    control_server: Option<AdnlServerConfigJson>,
    kafka_consumer_config: Option<KafkaConsumerConfig>,
    external_db_config: Option<ExternalDbConfig>,
    validator_key_ring: Option<HashMap<String, KeyOptionJson>>,
    #[serde(skip)]
    configs_dir: String,
    #[serde(skip)]
    port: Option<u16>,
    #[serde(skip)]
    file_name: String
}

pub struct TonNodeGlobalConfig(TonNodeGlobalConfigJson);

#[derive(serde::Deserialize, serde::Serialize, Clone)]
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

const LOCAL_HOST: &str = "127.0.0.1";

impl TonNodeConfig {
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
            },
            Err(_) => {
                // generate new config from default_config
                let default_config_file = File::open(
                    TonNodeConfig::build_path(configs_dir, default_config_name)?
                )?;
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
                    let (adnl_config, _) = AdnlNodeConfig::with_ip_address_and_key_type(
                        ip_address, 
                        KeyOption::KEY_ED25519,
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
        config_json.configs_dir = configs_dir.to_string();
        config_json.file_name = json_file_name.to_string();

        Ok(config_json)
    }

    pub fn adnl_node(&self) -> Result<AdnlNodeConfig> {
        let adnl_node = self.adnl_node.as_ref().ok_or_else(|| error!("ADNL node is not configured!"))?;

        let mut ret = AdnlNodeConfig::from_json_config(&adnl_node, true)?;
        if let Some(port) = self.port {
            ret.set_port(port)
        }
        Ok(ret)
    }

    pub fn control_server(&self) -> Result<AdnlServerConfig> {
        if let Some(control_server) = &self.control_server {
            AdnlServerConfig::from_json_config(control_server)
        } else {
            fail!("Control server is not configured")
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
        let name = self.ton_global_config_name.as_ref()
            .ok_or_else(|| error!("global config informations not found!"))?;

        let global_config_path = TonNodeConfig::build_path(&self.configs_dir, &name)?;

        let data = std::fs::read_to_string(global_config_path)
            .map_err(|err| error!("Global config file is not found! : {}", err))?;
        TonNodeGlobalConfig::from_json(&data)
    }

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
        let (server_private_key, server_key) = KeyOption::with_type_id(KeyOption::KEY_ED25519)?;

        // generate and save client console template
        let config_file_path = TonNodeConfig::build_path(configs_dir, "console_config.json")?;
        let console_client_config = AdnlClientConfigJson::with_params(
            &server_address,
            serde_json::from_str(key_option_public_key!(
                base64::encode(&server_key.pub_key()?)
            ))?,
            None
        );
        std::fs::write(config_file_path, serde_json::to_string_pretty(&console_client_config)?)?;

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
        validator_key_id: String,
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

    fn update_validator_key_info(&mut self, updated_info: ValidatorKeysJson) -> Result<ValidatorKeysJson> {
        if let Some(validator_keys) = &mut self.validator_keys {
            for keys_info in validator_keys.iter_mut() {
                if keys_info.validator_key_id == updated_info.validator_key_id && 
                    keys_info.election_id == updated_info.election_id {
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

    fn generate_and_save_keys(&mut self) -> Result<([u8; 32], Arc<KeyOption>)> {
        let (private, public) = KeyOption::with_type_id(KeyOption::KEY_ED25519)?;
        let key_id = public.id().data();
        match &mut self.validator_key_ring {
            Some(key_ring) => {
                key_ring.insert(base64::encode(key_id), private);
            },
            None => {
                let mut key_ring = HashMap::new();
                key_ring.insert(base64::encode(key_id), private);
                self.validator_key_ring = Some(key_ring);
            }
        };
        Ok((key_id.clone(), Arc::new(public)))
    }

    fn add_validator_key(&mut self, key_id: &[u8; 32], election_date: i32) -> Result<()> {
        let key_info = ValidatorKeysJson {
            election_id: election_date,
            validator_key_id: base64::encode(key_id),
            validator_adnl_key_id: None
        };
        match &mut self.validator_keys {
            Some(validator_keys) => {
                validator_keys.push(key_info);
            },
            None => {
                let mut keys  = Vec::new();
                keys.push(key_info);
                self.validator_keys = Some(keys);
            }
        }
        Ok(())
    }

    fn add_validator_adnl_key(
        &mut self,
        validator_key_id: &[u8; 32],
        adnl_key_id: &[u8; 32]
    ) -> Result<ValidatorKeysJson> {
        let key_info = self.get_validator_key_info(base64::encode(validator_key_id));
        match key_info {
            Ok(Some(validator_key_info)) => {
                let mut key_info = validator_key_info.clone();
                key_info.validator_adnl_key_id = Some(base64::encode(adnl_key_id));
                self.update_validator_key_info(key_info)
            },
            Ok(None) => {
                fail!("Validator key was not added!");
            },
            Err(e) => Err(e)
        }
    }

    fn remove_validator_key(&mut self, validator_key_id: String, election_id: i32) -> Result<bool> {
        let mut removed = false;
        if let Some(validator_keys) = &self.validator_keys {
            let mut new_validator_keys = Vec::new();
            for key_json in validator_keys {
                if key_json.validator_key_id == validator_key_id && key_json.election_id == election_id {
                    removed = true;
                    continue;
                }
                new_validator_keys.push(key_json.clone());
            }

            if removed {
                self.validator_keys = Some(new_validator_keys);
            }
        }
        Ok(removed)
    }

}

enum Task {
    Generate,
    AddValidatorKey([u8; 32], i32),
    AddValidatorAdnlKey([u8; 32], [u8; 32]),
    GetKey([u8; 32])
}

enum Answer {
    Generate(Result<[u8; 32]>),
    AddValidatorKey(Result<()>),
    AddValidatorAdnlKey(Result<()>),
    GetKey(Option<KeyOption>)
}

pub struct NodeConfigHandler {
    //subscribers: Vec<Arc<dyn NodeConfigSubscriber>>,
    tasks: Arc<lockfree::queue::Queue<Arc<(Arc<Wait<Answer>>, Task)>>>,
    key_ring: Arc<lockfree::map::Map<String, Arc<KeyOption>>>,
    validator_keys: Arc<lockfree::map::Map<i32, (ValidatorKeysJson, Option<i32>)>>, // election_id, (keys_info, next_election_id)
    last_election_id: Arc<AtomicI32>,
    first_election_id: Arc<AtomicI32>
}

impl NodeConfigHandler {
    const TIMEOUT_SHEDULER_STOP: u64 = 1;       // Milliseconds

    pub fn new(config: TonNodeConfig) -> Result<Self> {
        let handler = NodeConfigHandler {
           // subscribers: Vec::new(),
            tasks: Arc::new(lockfree::queue::Queue::new()),
            key_ring: Arc::new(lockfree::map::Map::new()),
            validator_keys: Arc::new(lockfree::map::Map::new()),
            first_election_id: Arc::new(AtomicI32::new(0)),
            last_election_id: Arc::new(AtomicI32::new(0))
        };
        handler.start_sheduler(config.file_name.clone(), config)?;

        Ok(handler)
    }

    pub async fn add_validator_key(
        &self, key_hash: &[u8; 32], elecation_date: ton::int,
    ) -> Result<()> {
        let (wait, mut queue_reader) = Wait::new();
        let pushed_task = Arc::new((wait.clone(), Task::AddValidatorKey(key_hash.clone(), elecation_date)));
        wait.request();
        self.tasks.push(pushed_task);
        let answer = match wait.wait(&mut queue_reader, true).await {
            Some(None) => fail!("Answer was not set!"),
            Some(Some(answer)) => answer,
            None => fail!("Waiting returned an internal error!")
        };

        let result = match answer {
            Answer::AddValidatorKey(res) => res,
            _ => fail!("Bad answer (AddValidatorKey)!"),
        };

        result
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
        self.tasks.push(pushed_task);
        let answer = match wait.wait(&mut queue_reader, true).await {
            Some(None) => fail!("Answer was not set!"),
            Some(Some(answer)) => answer,
            None => fail!("Waiting returned an internal error!")
        };

       let result = match answer {
           Answer::AddValidatorAdnlKey(res) => res,
           _ => fail!("Bad answer (AddValidatorAdnlKey)!"),
       };

       result
    }

    pub async fn get_validator_key(&self, key_id: &Arc<KeyId>) -> Option<(KeyOption, i32)> {
        let first_election_id = self.first_election_id.load(atomic::Ordering::Relaxed);

        if first_election_id <= 0 {
            return None;
        }

        let mut current_election_id = Some(first_election_id);
        let key_id_str = base64::encode(key_id.data());
        let mut key_election_id = 0;
        while let Some(election_id) = current_election_id {
            let validator_key = self.validator_keys.get(&election_id);
            match validator_key {
                Some(key_info) => {
                    if key_info.val().0.validator_key_id == key_id_str {
                        key_election_id = key_info.val().0.election_id;
                    } else if let Some(validator_adnl_key_id) = &key_info.val().0.validator_adnl_key_id {
                        if validator_adnl_key_id == &key_id_str {
                            key_election_id = key_info.val().0.election_id;
                        }
                    }
                    current_election_id = key_info.val().1;
                    if key_election_id > 0 {
                        break;
                    }
                },
                None => { current_election_id = None; }
            }
        }
        
        if key_election_id <= 0 {
            return None;
        }

 //       let result = if let Some(key) = self.key_ring.get(&key_id) {
 //           Some(key.(val(), key_election_id))
        let result = if let Some(key) = self.get_key_raw(*key_id.data()).await {
            Some((key, key_election_id))
        } else {
            None
        };

        result
    }

    async fn get_key_raw(&self, key_hash: [u8; 32]) -> Option<KeyOption> {
        let (wait, mut queue_reader) = Wait::new();
        let pushed_task = Arc::new((wait.clone(), Task::GetKey(key_hash)));
        wait.request();
        self.tasks.push(pushed_task);
        let answer = match wait.wait(&mut queue_reader, true).await {
            Some(Some(answer)) => answer,
            _ => return None
        };

       let result = match answer {
           Answer::GetKey(key) => key,
           _ => None,
       };

       result
    }

    fn generate_and_save(
        key_ring: &Arc<lockfree::map::Map<String, Arc<KeyOption>>>,
        config: &mut TonNodeConfig,
        config_name: &str
    ) -> Result<[u8; 32]> {
        let (key_id, public_key) = config.generate_and_save_keys()?;
        config.save_to_file(config_name)?;

        let id = base64::encode(&key_id);
        add_object_to_map(key_ring, id, || Ok(public_key.clone()))?;
        Ok(key_id)
    }

    fn add_validator_adnl_key_and_save(
        validator_keys: Arc<lockfree::map::Map<i32, (ValidatorKeysJson, Option<i32>)>>,
        config: &mut TonNodeConfig,
        validator_key_hash: &[u8; 32],
        validator_adnl_key_hash: &[u8; 32],
        first_election_id: Arc<AtomicI32>,
        last_election_id: Arc<AtomicI32>
    )-> Result<()> {
        let key = config.add_validator_adnl_key(&validator_key_hash, validator_adnl_key_hash)?;
        let oldest_validator_key = NodeConfigHandler::get_oldest_validator_key(&config);
        if let Some(oldest_key) = oldest_validator_key {
            if oldest_key.election_id < key.election_id {
                let new_last_election_id = key.election_id;
                config.remove_validator_key(oldest_key.validator_key_id, oldest_key.election_id)?;
                add_object_to_map(&validator_keys, key.election_id,
                    || Ok((key.clone(), None)))?;
                let prev_election_id = last_election_id.load(atomic::Ordering::Relaxed);
                if prev_election_id > 0 {
                    add_object_to_map_with_update(&validator_keys, prev_election_id,
                        |old_value| if let Some(old_value) = old_value {
                            Ok(Some((old_value.0.clone(), Some(new_last_election_id))))
                        } else {
                            Ok(None)
                        }
                    )?;
                }
                last_election_id.store(new_last_election_id, atomic::Ordering::Relaxed);
                if (first_election_id.load(atomic::Ordering::Relaxed)) == 0 {
                    first_election_id.store(new_last_election_id, atomic::Ordering::Relaxed);
                } else {
                    validator_keys.remove(&oldest_key.election_id);
                }
            }
        }
        config.save_to_file(&config.file_name)?;
        Ok(())
    }

    fn add_validator_key_and_save(
        config: &mut TonNodeConfig,
        key_id: &[u8; 32],
        election_id: i32
    )-> Result<()> {
        config.add_validator_key(&key_id, election_id)?;
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

    fn get_key(config: &TonNodeConfig, key_id: [u8; 32]) -> Option<KeyOption> {
        if let Some(validator_key_ring) = &config.validator_key_ring {
            if let Some(key_data)  = validator_key_ring.get(&base64::encode(&key_id)) {
                match KeyOption::from_private_key(&key_data) {
                    Ok(key) => { return Some(key)},
                    _ => return None
                }
            }
        }
        None
    }

    fn load_config(&self, config: &TonNodeConfig) -> Result<()>{
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
               if let Err(e) = self.add_or_update_validator_key_in_map(key) {
                   log::warn!("fail added key to validator keys map: {}", e);
               }
            }
        }
        Ok(())
    }

    fn add_or_update_validator_key_in_map(&self, validator_key: &ValidatorKeysJson) -> Result<()> {
        if self.first_election_id.load(atomic::Ordering::Relaxed) <= 0 {
            add_object_to_map(
                &self.validator_keys,
                validator_key.election_id,
                || Ok((validator_key.clone(), None))
            )?;
            self.first_election_id.store(validator_key.election_id, atomic::Ordering::Relaxed);
            return Ok(());
        }

        let mut next_election_id = Some(self.first_election_id.load(atomic::Ordering::Relaxed));
        let mut prev_election_id = 0;
        // search prev_election_id
        while let Some(election_id) = next_election_id {
            let key = self.validator_keys.get(&election_id)
                .ok_or_else(|| error!("Failed to load key!"))?;

            if election_id > validator_key.election_id {
                break;
            }
            prev_election_id = election_id;
            next_election_id = key.val().1;
        }

        add_object_to_map(
            &self.validator_keys,
            validator_key.election_id,
            || Ok((validator_key.clone(), next_election_id))
        )?;

        add_object_to_map_with_update(&self.validator_keys, prev_election_id,
            |old_value| if let Some(old_value) = old_value {
                Ok(Some((old_value.0.clone(), Some(validator_key.election_id))))
            } else {
                Ok(None)
            }
        )?;

        if self.last_election_id.load(atomic::Ordering::Relaxed) < validator_key.election_id {
            self.last_election_id.store(validator_key.election_id, atomic::Ordering::Relaxed);
        }

        Ok(())
    }

    fn add_key_to_dynamic_key_ring(&self, key_id: String, key_json: &KeyOptionJson) -> Result<()> {
        add_object_to_map(&self.key_ring, key_id,
            || Ok(Arc::new(KeyOption::from_private_key(key_json)?))
        )?;
        Ok(())
    }

    fn start_sheduler(&self, config_name: String, config: TonNodeConfig) -> Result<()> {
        let mut actual_config = config;
        let tasks = self.tasks.clone();
        let name = config_name.clone();
        let key_ring = self.key_ring.clone();
        let validator_keys = self.validator_keys.clone();
        let last_election_id = self.last_election_id.clone();
        let first_election_id = self.first_election_id.clone();
        self.load_config(&actual_config)?;
        
        tokio::spawn(async move {
            loop {
                if let Some(task) = tasks.pop() {
                    match task.1 {
                        Task::Generate => {
                            let result = NodeConfigHandler::generate_and_save(&key_ring, &mut actual_config, &name);
                            task.0.respond(Some(Answer::Generate(result)));
                        },
                        Task::AddValidatorAdnlKey(key, adnl_key) => {
                            let result = NodeConfigHandler::add_validator_adnl_key_and_save(
                                validator_keys.clone(),
                                &mut actual_config,
                                &key,
                                &adnl_key,
                                first_election_id.clone(),
                                last_election_id.clone()
                            );
                            task.0.respond(Some(Answer::AddValidatorAdnlKey(result)));
                        },
                        Task::AddValidatorKey(key, election_id) => {
                            let result = NodeConfigHandler::add_validator_key_and_save(
                                &mut actual_config, &key, election_id
                            );
                            task.0.respond(Some(Answer::AddValidatorKey(result)));
                        }, 
                        Task::GetKey(key_data) => {
                            let result = NodeConfigHandler::get_key(&actual_config, key_data);
                            task.0.respond(Some(Answer::GetKey(result))); 
                        }
                    }
                } else {
                    tokio::time::delay_for(Duration::from_millis(Self::TIMEOUT_SHEDULER_STOP)).await;
                }
            }
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
        self.tasks.push(pushed_task);
        let answer = match wait.wait(&mut queue_reader, true).await {
            Some(None) => fail!("Answer was not set!"),
            Some(Some(answer)) => answer,
            None => fail!("Waiting returned an internal error!")
        };

       let result = match answer {
           Answer::Generate(res) => res,
           _ => fail!("Bad answer (Generate)!")
       };

       result
    }

    fn sign_data(&self, key_hash: &[u8; 32], data: &[u8]) -> Result<Vec<u8>> {
        let private = self.find(key_hash)?;
        Ok(private.sign(data)?.to_vec())
    }

    // find private key in KeyRing by public key hash
    fn find(&self, key_id: &[u8; 32]) -> Result<Arc<KeyOption>> {
       let id = base64::encode(key_id);
        match self.key_ring.get(&id) {
            Some(key) => Ok(key.val().clone()),
            None => fail!("key not found for hash: {}", &id)
        }
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
