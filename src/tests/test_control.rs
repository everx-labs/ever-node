/*
* Copyright (C) 2019-2023 EverX. All Rights Reserved.
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

use crate::{
    collator_test_bundle::create_engine_allocated, 
    config::TonNodeConfig, engine::Engine, engine_traits::EngineOperations, 
    internal_db::{InternalDb, InternalDbConfig, state_gc_resolver::AllowStateGcSmartResolver}, 
    network::{
        control::{ControlQuerySubscriber, ControlServer, DataSource, StatusReporter},
        node_network::NodeNetwork
    },
    shard_state::ShardStateStuff, test_helper::{gen_master_state, gen_shard_state},
    validating_utils::{supported_capabilities, supported_version},
    validator::validator_manager::ValidationStatus, shard_states_keeper::PinnedShardStateGuard,
};
#[cfg(feature = "telemetry")]
use crate::collator_test_bundle::create_engine_telemetry;

use adnl::{
    common::TaggedTlObject, client::{AdnlClient, AdnlClientConfig},
    server::AdnlServerConfig
};
use std::{
    collections::HashMap, fs, ops::Deref, sync::{Arc, atomic::{AtomicBool, Ordering}},
    time::SystemTime
};
use storage::block_handle_db::BlockHandle;
use ton_api::{ 
    serialize_boxed, tag_from_boxed_type, AnyBoxedSerialize,
    ton::{
        self, TLObject, accountaddress::AccountAddress,
        engine::validator::{ControlQueryError, KeyHash, Stats},
        lite_server::ConfigInfo, raw::{ShardAccountState, AppliedShardsInfo},
        rpc::{
            engine::validator::{ControlQuery, GenerateKeyPair, GetSelectedStats, GetStats},
            lite_server::GetConfigAll, 
            raw::{GetShardAccountState, GetAccountByBlock, GetAppliedShardsInfo},
        }
    }
};
use ton_api::ton::raw::ShardAccountMeta;
use ton_api::ton::rpc::raw::{GetAccountMetaByBlock, GetShardAccountMeta};
use ton_block::{
    Account, BlockIdExt, ConfigParamEnum, ConfigParams, Deserializable,
    generate_test_account_by_init_code_hash, Message, Serializable, ShardIdent
};
use ton_types::{
    error, fail, base64_encode, Ed25519KeyOption, KeyId, KeyOption, Result, UInt256
};

// key pair for server
// "pub_key": "cujCRU4rQbSw48yHVHxQtRPhUlbo+BuZggFTQSu04Y8="
// "pvt_key": "cJIxGZviebMQWL726DRejqVzRTSXPv/1sO/ab6XOZXk="

// key pair for client
// "pub_key": "RYokIiD5AFkzfTBgC6NhtAGFKm0+gwhN4suTzaW0Sjw="
// "pvt_key": "oEivbTDjSOSCgooUM0DAS2z2hIdnLw/PT82A/OFLDmA="

const ADNL_SERVER_CONFIG: &str = r#"{
    "address": "127.0.0.1:4925",
    "server_key": {
        "type_id": 1209251014,
        "pvt_key": "cJIxGZviebMQWL726DRejqVzRTSXPv/1sO/ab6XOZXk="
    },
    "clients": {
        "list": [
            {
                "type_id": 1209251014,
                "pub_key": "RYokIiD5AFkzfTBgC6NhtAGFKm0+gwhN4suTzaW0Sjw="
            }
        ]
    }
}"#;

const ADNL_CLIENT_CONFIG: &str = r#"{
    "server_address": "127.0.0.1:4925",
    "server_key": {
        "type_id": 1209251014,
        "pub_key": "cujCRU4rQbSw48yHVHxQtRPhUlbo+BuZggFTQSu04Y8="
    },
    "client_key": {
        "type_id": 1209251014,
        "pvt_key": "oEivbTDjSOSCgooUM0DAS2z2hIdnLw/PT82A/OFLDmA="
    }
}"#;

const IP_NODE: &str = "127.0.0.1:4191";
const DEFAULT_CONFIG: &str = "default_config.json";

async fn generate_keypair(client: &mut AdnlClient) -> Result<Arc<dyn KeyOption>> {
    let answer: KeyHash = request(client, GenerateKeyPair).await?;
    Ok(Ed25519KeyOption::from_public_key(answer.key_hash().as_slice()))
}

async fn query(client: &mut AdnlClient, query: &TLObject) -> Result<TLObject> {
    let control_query = TaggedTlObject {
        object: TLObject::new(
            ControlQuery {
                data: ton::bytes(serialize_boxed(query)?)
            },
        ),
        #[cfg(feature = "telemetry")]
        tag: tag_from_boxed_type::<ControlQuery>()
    };
    match client.query(&control_query).await?.downcast::<ControlQueryError>() {
        Ok(error) => fail!("Error response to {:?}: {:?}", query, error),
        Err(answer) => Ok(answer)
    }
}

async fn request<Q, A>(client: &mut AdnlClient, _query: Q) -> Result<A>
where
    A: AnyBoxedSerialize,
    Q: AnyBoxedSerialize
{
    let boxed = TLObject::new(_query);
    query(client, &boxed).await?.downcast::<A>()
        .map_err(
            |answer| error!("Unsupported answer to {:?}: {:?}", boxed, answer)
        )
}

async fn start_control_with_options(
    data_source: DataSource,
    config: Option<TonNodeConfig>,
    server_only: bool
) -> Result<(ControlServer, Option<AdnlClient>, Arc<KeyId>)> {
    let config = if let Some(config) = config {
        config
    } else {
        fs::copy("./configs/ton-global.config.json", "./target/ton-global.config.json")?;
        crate::test_helper::get_config(IP_NODE, Some("./target"), DEFAULT_CONFIG).await?
    };
    let network = NodeNetwork::new(
        config,
        Arc::new(tokio_util::sync::CancellationToken::new()),
        #[cfg(feature = "telemetry")]
        create_engine_telemetry(),
        create_engine_allocated()
    ).await.unwrap();
    let key_id = network.get_key_id_by_tag(NodeNetwork::TAG_OVERLAY_KEY)?;
    let config = AdnlServerConfig::from_json(ADNL_SERVER_CONFIG)?;
    let control = ControlServer::with_params(
        config,
        data_source,
        network.config_handler(),
        network.config_handler(),
        Some(&network)
    ).await?;
    let client = if server_only {
        None
    } else {
        let (_, config) = AdnlClientConfig::from_json(ADNL_CLIENT_CONFIG)?;
        Some(AdnlClient::connect(&config).await?)
    };
    Ok((control, client, key_id))
}

async fn start_control(
    data_source: DataSource
) -> Result<(ControlServer, AdnlClient, Arc<KeyId>)> {
    let (server, client, key_id) =
        start_control_with_options(data_source, None, false).await?;
    Ok((server, client.unwrap(), key_id))
}

async fn start_control_with_config(
    data_source: DataSource,
    config: TonNodeConfig,
) -> Result<(ControlServer, AdnlClient, Arc<KeyId>)> {
    let (server, client, key_id) =
        start_control_with_options(data_source, Some(config), false).await?;
    Ok((server, client.unwrap(), key_id))
}

async fn recreate_client(old_client: Option<AdnlClient>) -> AdnlClient {
    if let Some(old) = old_client {
        old.shutdown().await.unwrap();
    }
    let client_config = AdnlClientConfig::from_json(ADNL_CLIENT_CONFIG).unwrap().1;
    AdnlClient::connect(&client_config).await.unwrap()
}

#[tokio::test]
async fn test_get_all_config_params() {

    struct TestEngine {
        config_addr: UInt256,
        state_id: BlockIdExt,
        state: Arc<ShardStateStuff>
    }

    impl TestEngine {
        fn new() -> Self {
            let (state_id, state) = gen_master_state(
                None,
                None,
                None,
                &[],
                #[cfg(feature = "telemetry")]
                None,
                None
            );
            Self {
                config_addr: state.state().unwrap().read_custom().unwrap().unwrap().config.config_address().unwrap(),
                state_id,
                state
            }
        }
    }

    #[async_trait::async_trait]
    impl EngineOperations for TestEngine {
        async fn load_last_applied_mc_state(&self) -> Result<Arc<ShardStateStuff>> {
            Ok(self.state.clone())
        }
    }

    crate::test_helper::init_test_log();
    let engine = Arc::new(TestEngine::new());
    let (control, mut client, _) = start_control(
        DataSource::Engine(engine.clone())
    ).await.unwrap();
    let answer: ConfigInfo = request(
        &mut client,
        GetConfigAll {
            mode: 0,
            id: engine.state_id.clone()
        }
    ).await.unwrap();

    let config_params = ConfigParams::construct_from_bytes(&answer.config_proof()).unwrap();
    let param0 = config_params.config(0).unwrap().unwrap();
    let param0 = match param0 {
        ConfigParamEnum::ConfigParam0(param) => param,
        _ => panic!("ConfigParams id bad!")
    };
    assert_eq!(param0.config_addr, engine.config_addr);

    client.shutdown().await.unwrap();
    control.shutdown().await;

}

#[tokio::test]
async fn test_getaccount() {

    struct TestEngine {
        account: Account,
        master_state_id: BlockIdExt,
        master_state: Arc<ShardStateStuff>,
        shard_state_id: BlockIdExt,
        shard_state: Arc<ShardStateStuff>
    }

    impl TestEngine {
        fn new() -> Self {

            #[cfg(feature = "telemetry")]
            let telemetry = create_engine_telemetry();
            let allocated = create_engine_allocated();

            let account = generate_test_account_by_init_code_hash(false);
            let (shard_state_id, shard_state) = gen_shard_state(
                None,
                &[&account],
                #[cfg(feature = "telemetry")]
                Some(telemetry.clone()),
                Some(allocated.clone()),
                None,
            );
            let (master_state_id, master_state) = gen_master_state(
                None,
                Some(shard_state_id.clone()),
                None,
                &[],
                #[cfg(feature = "telemetry")]
                Some(telemetry.clone()),
                Some(allocated.clone())
            );

            Self {
                account,
                master_state_id,
                master_state,
                shard_state_id,
                shard_state,
            }

        }
    }

    #[async_trait::async_trait]
    impl EngineOperations for TestEngine {
        async fn load_last_applied_mc_state(&self) -> Result<Arc<ShardStateStuff>> {
            Ok(self.master_state.clone())
        }
        fn load_shard_client_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
            Ok(Some(Arc::new(self.master_state_id.clone())))
        }
        async fn load_state(&self, block_id: &BlockIdExt) -> Result<Arc<ShardStateStuff>> {
            if *block_id == self.master_state_id {
                Ok(self.master_state.clone())
            } else if *block_id == self.shard_state_id {
                Ok(self.shard_state.clone())
            } else {
                fail!("Wrong block ID {}", block_id)
            }
        }
        async fn load_and_pin_state(&self, block_id: &BlockIdExt) -> Result<PinnedShardStateGuard> {
            if *block_id == self.master_state_id {
                PinnedShardStateGuard::new(
                    self.master_state.clone(),
                    Arc::new(AllowStateGcSmartResolver::new(10))
                )
            } else if *block_id == self.shard_state_id {
                PinnedShardStateGuard::new(
                    self.shard_state.clone(),
                    Arc::new(AllowStateGcSmartResolver::new(10))
                )
            } else {
                fail!("Wrong block ID {}", block_id)
            }
        }

        fn find_full_block_id(&self, root_hash: &UInt256) -> Result<Option<BlockIdExt>> {
            Ok(if *root_hash == self.master_state_id.root_hash {
                Some(self.master_state_id.clone())
            } else if *root_hash == self.shard_state_id.root_hash {
                Some(self.shard_state_id.clone())
            } else {
                None
            })
        }

    }

    crate::test_helper::init_test_log();
    let engine = Arc::new(TestEngine::new());
    let control = start_control_with_options(
        DataSource::Engine(engine.clone()),
            None, true,
    ).await.unwrap().0;

    let mut client = recreate_client(None).await;

    /* let account1 = AccountAddress {
        account_address: "-1:7777777777777777777777777777777777777777777777777777777777777777".to_string()
    };
    let empty_answer: ShardAccountState = request(
        &mut client, GetShardAccountState {account_address: account1}
    ).await.unwrap();
    println!("{:?}", empty_answer);
    */
    let account2 = AccountAddress {
        account_address: format!("{}", engine.account.get_addr().unwrap())
    };
    let answer: ShardAccountState = request(
        &mut client, GetShardAccountState {account_address: account2.clone()}
    ).await.unwrap();
    assert_eq!(answer.shard_account().is_some(), true);

    let mut client = recreate_client(Some(client)).await;

    let answer: ShardAccountMeta = request(
        &mut client, GetShardAccountMeta {account_address: account2.clone()}
    ).await.unwrap();
    assert_eq!(answer.shard_account_meta().is_some(), true);

    let mut client = recreate_client(Some(client)).await;

    let account_id = UInt256::construct_from_cell(engine.account.get_addr().unwrap().address().into_cell()).unwrap();

    let answer: ShardAccountState = request(
        &mut client, GetAccountByBlock {account_id: account_id.clone(), block_root_hash: engine.shard_state_id.root_hash.clone()}
    ).await.unwrap();
    assert!(answer.shard_account().is_some());

    let mut client = recreate_client(Some(client)).await;

    let answer: ShardAccountMeta = request(
        &mut client, GetAccountMetaByBlock {account_id, block_root_hash: engine.shard_state_id.root_hash.clone()}
    ).await.unwrap();
    assert!(answer.shard_account_meta().is_some());

    client.shutdown().await.unwrap();
    control.shutdown().await;

}


#[tokio::test]
async fn test_get_applied_shards_info() {

    struct TestEngine {
        applied_master_block_id: BlockIdExt,
        applied_shard_block_id: BlockIdExt,
        shard_client_master_state_id: BlockIdExt,
        shard_client_master_state: Arc<ShardStateStuff>,
    }

    impl TestEngine {
        fn new() -> Self {

            #[cfg(feature = "telemetry")]
            let telemetry = create_engine_telemetry();
            let allocated = create_engine_allocated();

            let (applied_shard_block_id, _) = gen_shard_state(
                None,
                &[],
                #[cfg(feature = "telemetry")]
                Some(telemetry.clone()),
                Some(allocated.clone()),
                None,
            );
            let (shard_client_master_state_id, shard_client_master_state) = gen_master_state(
                None,
                Some(applied_shard_block_id.clone()),
                None,
                &[],
                #[cfg(feature = "telemetry")]
                Some(telemetry.clone()),
                Some(allocated.clone())
            );

            let mut applied_master_block_id = shard_client_master_state_id.clone();
            applied_master_block_id.seq_no += 1;

            Self {
                applied_master_block_id,
                applied_shard_block_id,
                shard_client_master_state_id,
                shard_client_master_state,
            }

        }
    }

    #[async_trait::async_trait]
    impl EngineOperations for TestEngine {
        fn load_last_applied_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
            Ok(Some(Arc::new(self.applied_master_block_id.clone())))
        }
        fn load_shard_client_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
            Ok(Some(Arc::new(self.shard_client_master_state_id.clone())))
        }
        async fn load_state(&self, block_id: &BlockIdExt) -> Result<Arc<ShardStateStuff>> {
            if *block_id == self.shard_client_master_state_id {
                Ok(self.shard_client_master_state.clone())
            } else {
                fail!("Wrong block ID {}", block_id)
            }
        }
    }

    crate::test_helper::init_test_log();
    let engine = Arc::new(TestEngine::new());
    let control = start_control_with_options(
        DataSource::Engine(engine.clone()),
            None, true,
    ).await.unwrap().0;

    let mut client = recreate_client(None).await;

    let answer: AppliedShardsInfo = request(
        &mut client, GetAppliedShardsInfo {}
    ).await.unwrap();
    assert_eq!(
        answer.shards(),
        &vec![engine.applied_shard_block_id.clone(), engine.applied_master_block_id.clone()].into()
    );

    client.shutdown().await.unwrap();
    control.shutdown().await;

}

#[tokio::test]
async fn test_connect_to_control() {

    struct TestSource;
    impl StatusReporter for TestSource {
        fn get_report(&self) -> u32 {
            0
        }
    }

    crate::test_helper::init_test_log();
    let (control, mut client, _) = start_control(
        DataSource::Status(Arc::new(TestSource))
    ).await.unwrap();

    let pub_key = generate_keypair(&mut client).await.unwrap();
    log::debug!("public key: {}", base64_encode(pub_key.pub_key().unwrap()));
    client.shutdown().await.unwrap();
    control.shutdown().await;

}

struct TestSendMsgEngine {
    expected_data: Vec<u8>
}

#[async_trait::async_trait]
impl EngineOperations for TestSendMsgEngine {
    async fn redirect_external_message(&self, message_data: &[u8], _id: UInt256) -> Result<()> {
        assert_eq!(message_data, &self.expected_data);
        Ok(())
    }
}

#[tokio::test]
async fn test_control_send_message() {

    struct TestSendMsgEngine {
        expected_data: Vec<u8>
    }

    #[async_trait::async_trait]
    impl EngineOperations for TestSendMsgEngine {
        async fn redirect_external_message(
            &self,
            message_data: &[u8],
            _id: UInt256
        ) -> Result<()> {
            assert_eq!(message_data, &self.expected_data);
            Ok(())
        }
    }

    crate::test_helper::init_test_log();

    let body = Message::default().write_to_bytes().unwrap();
    let engine = TestSendMsgEngine{expected_data: body.clone()};
    let config = TonNodeConfig::from_file(
        "./target",
        "config_test_control.json",
        None,
        "../configs/default_config.json",
        None
    ).unwrap();

    let (control, mut client, _) = start_control_with_config(
        DataSource::Engine(Arc::new(engine)),
        config
    ).await.unwrap();

    let _answer: ton_api::ton::engine::validator::Success = request(
        &mut client, ton::rpc::lite_server::SendMessage {body: body.into()}
    ).await.unwrap();
    client.shutdown().await.unwrap();
    control.shutdown().await;

}

#[tokio::test(flavor = "multi_thread")]
async fn test_control_db_restore() {

    struct TestSource {
        broken: AtomicBool
    }

    impl StatusReporter for TestSource {
        fn get_report(&self) -> u32 {
            if self.broken.load(Ordering::Relaxed) {
                Engine::SYNC_STATUS_DB_BROKEN
            } else {
                Engine::SYNC_STATUS_CHECKING_DB
            }
        }
    }

    crate::test_helper::init_test_log();
    let status = Arc::new(
        TestSource {
            broken: AtomicBool::new(false)
        }
    );
    let (control, _, _) = start_control_with_options(
        DataSource::Status(status.clone()),
        None,
        true
    ).await.unwrap();
    let (_, config) = AdnlClientConfig::from_json(ADNL_CLIENT_CONFIG).unwrap();
    let mut client = AdnlClient::connect(&config).await.unwrap();

    let answer: Stats = request(
        &mut client,
        GetSelectedStats {
            filter: "node_status".to_string()
        }
    ).await.unwrap();
    let answer = answer.only();
    let answer = &answer.stats.deref()[0];
    assert_eq!(answer.key, "node_status");
    assert_eq!(answer.value, "\"checking_db\"");

    status.broken.store(true, Ordering::Relaxed);

    let answer: Stats = request(
        &mut client,
        GetSelectedStats {
            filter: "node_status".to_string()
        }
    ).await.unwrap();
    let answer = answer.only();
    let answer = &answer.stats.deref()[0];
    assert_eq!(answer.key, "node_status");
    assert_eq!(answer.value, "\"db_broken\"");
    client.shutdown().await.unwrap();
    control.shutdown().await;

}

#[test]
fn test_convert_for_stats() {
    assert_eq!("Disabled",  &format!("{:?}", ValidationStatus::from_u8(5)));
    assert_eq!("Disabled",  &format!("{:?}", ValidationStatus::from_u8(0)));
    assert_eq!("Waiting",   &format!("{:?}", ValidationStatus::from_u8(1)));
    assert_eq!("Countdown", &format!("{:?}", ValidationStatus::from_u8(2)));
    assert_eq!("Active"   , &format!("{:?}", ValidationStatus::from_u8(3)));

    let shard_id = ShardIdent::with_tagged_prefix(15, 0xABCD_0000_0000_0000u64).unwrap();
    let root_hash = "bac24be401b3489f90018d08137c4063f24bfc6def86a61836060d6dbc32e703".parse().unwrap();
    let file_hash = "3baf367e57116fcf5df3c7333a7ea4aa5704dac36e696b4c7dfbda383babe9ae".parse().unwrap();
    let block_id = BlockIdExt::with_params(shard_id.clone(), 100500, root_hash, file_hash);
    assert_eq!(
        r#"{"shard":"15:abcd000000000000","seq_no":100500,"rh":"bac24be401b3489f90018d08137c4063f24bfc6def86a61836060d6dbc32e703","fh":"3baf367e57116fcf5df3c7333a7ea4aa5704dac36e696b4c7dfbda383babe9ae"}"#,
        ControlQuerySubscriber::block_id_to_json(&block_id).as_str()
    );

    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let map = lockfree::map::Map::new();
    map.insert(ShardIdent::masterchain(), now - 5);
    assert_eq!(
        "{\n  \"-1:8000000000000000\": 5\n}",
        ControlQuerySubscriber::statistics_to_json(&map, now as i64, true).as_str()
    );
    let map = lockfree::map::Map::new();
    map.insert(shard_id, now - 130);
    assert_eq!(
        "{\n  \"15:abcd000000000000\": 130\n}",
        ControlQuerySubscriber::statistics_to_json(&map, now as i64, true).as_str()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stats() {

    const DB_PATH: &str = "./target/node_db";

    struct TestEngine {
        db: InternalDb,
        master_state_id: BlockIdExt,
        master_state: Arc<ShardStateStuff>,
        last_validation_time: lockfree::map::Map<ShardIdent, u64>
    }

    impl TestEngine {
        async fn new() -> Self {
            #[cfg(feature = "telemetry")]
            let telemetry = create_engine_telemetry();
            let allocated = create_engine_allocated();
            let (master_state_id, master_state) = gen_master_state(
                None,
                None,
                None,
                &[],
                #[cfg(feature = "telemetry")]
                Some(telemetry.clone()),
                Some(allocated.clone())
            );
            std::fs::remove_dir_all(DB_PATH).ok();
            let db_config = InternalDbConfig {
                db_directory: String::from(DB_PATH),
                ..Default::default()
            };
            let db = InternalDb::with_update(
                db_config,
                false,
                false,
                false,
                &|| Ok(()),
                None,
                #[cfg(feature = "telemetry")]
                telemetry.clone(),
                allocated.clone()
            ).await.unwrap();
            db.create_or_load_block_handle(
                &master_state_id,
                None,
                None,
                Some(1),
                None
            ).unwrap()._to_created().unwrap();
            Self {
                db,
                master_state_id,
                master_state,
                last_validation_time: lockfree::map::Map::new()
            }
        }
    }

    #[async_trait::async_trait]
    impl EngineOperations for TestEngine {
        fn calc_tps(&self, _period: u64) -> Result<u32> {
            Ok(0)
        }
        fn get_sync_status(&self) -> u32 {
            Engine::SYNC_STATUS_SYNC_BLOCKS
        }
        fn last_collation_time(&self) -> &lockfree::map::Map<ShardIdent, u64> {
            &self.last_validation_time
        }
        fn last_validation_time(&self) -> &lockfree::map::Map<ShardIdent, u64> {
            &self.last_validation_time
        }
        fn load_block_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>> {
            self.db.load_block_handle(id)
        }
        fn load_last_applied_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
            Ok(Some(Arc::new(self.master_state_id.clone())))
        }
        async fn load_last_applied_mc_state(&self) -> Result<Arc<ShardStateStuff>> {
            Ok(self.master_state.clone())
        }
        fn load_shard_client_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
            Ok(Some(Arc::new(self.master_state_id.clone())))
        }
        fn validation_status(&self) -> ValidationStatus {
            ValidationStatus::Active
        }
    }

    struct Ethalon<'a> {
        val: &'a str,
        mask: u32
    }

    fn add_ethalon<'a>(map: &mut HashMap<&'a str, Ethalon<'a>>, key: &'a str, val: &'a str) {
        let ethalon = Ethalon {
            val,
            mask: 1u32 << map.iter().count()
        };
        map.insert(key, ethalon);
    }

    fn value_ok(val: &str, ethalon: &str) -> Option<()> {
        if val != ethalon {
            None
        } else {
            Some(())
        }
    }

    fn check_stats(
        stats: &Stats,
        engine: &Arc<TestEngine>,
        key_id: &Arc<KeyId>,
        new_format: bool
    ) {

        let master_block_id = ControlQuerySubscriber::block_id_to_json(&engine.master_state_id);
        let node_version = format!("\"{}\"", env!("CARGO_PKG_VERSION"));
        let overlay_key = format!("\"{}\"", key_id);
        let supported_capabilities = format!("{}", supported_capabilities());
        let supported_version = format!("{}", supported_version());
        let timediff = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let mut ethalon_stats = HashMap::new();
        if !new_format {
            add_ethalon(&mut ethalon_stats, "collation_stats", "{}");
        }
        if new_format {
            add_ethalon(&mut ethalon_stats, "global_id", "0");
        }
        add_ethalon(&mut ethalon_stats, "in_current_vset_p34", "false");
        add_ethalon(&mut ethalon_stats, "in_next_vset_p36", "false");
        add_ethalon(&mut ethalon_stats, "last_applied_masterchain_block_id", &master_block_id);
        if new_format {
            add_ethalon(&mut ethalon_stats, "last_collation_ago_sec", "{}");
            add_ethalon(&mut ethalon_stats, "last_validation_ago_sec", "{}");
        }
        add_ethalon(&mut ethalon_stats, "masterchainblocknumber", "0");
        add_ethalon(&mut ethalon_stats, "masterchainblocktime", "1");
        if new_format {
            add_ethalon(&mut ethalon_stats, "node_status", "\"synchronization_by_blocks\"");
        }
        add_ethalon(&mut ethalon_stats, "node_version", &node_version);
        add_ethalon(&mut ethalon_stats, "processed_workchain", "\"not specified\"");
        add_ethalon(&mut ethalon_stats, "public_overlay_key_id", &overlay_key);
        add_ethalon(&mut ethalon_stats, "shards_timediff", "timediff");
        if new_format {
            add_ethalon(&mut ethalon_stats, "supported_block", &supported_version);
            add_ethalon(&mut ethalon_stats, "supported_capabilities", &supported_capabilities);
        }
        if !new_format {
            add_ethalon(&mut ethalon_stats, "sync_status", "\"synchronization_by_blocks\"");
        }
        add_ethalon(&mut ethalon_stats, "timediff", "timediff");
        add_ethalon(&mut ethalon_stats, "tps_10", "0");
        add_ethalon(&mut ethalon_stats, "tps_300", "0");
        if !new_format {
            add_ethalon(&mut ethalon_stats, "validation_stats", "{}");
        }
        add_ethalon(&mut ethalon_stats, "validation_status", "\"Active\"");

        let mut mask = u32::MAX >> (32 - ethalon_stats.iter().count());
        for stat in stats.stats().deref() {
            let ethalon = ethalon_stats
                .get(&stat.key as &str)
                .expect(&format!("Key {} is not found in Stats ethalon", stat.key));
            if (mask & ethalon.mask) == 0 {
                panic!("Doubled stat {} in Stat reply", stat.key)
            } else {
                mask &= !ethalon.mask
            }
            match ethalon.val {
                "timediff" => {
                    let ethalon = format!("{}", timediff);
                    value_ok(&stat.value, &ethalon)
                        .or_else(
                            || {
                                let ethalon = format!("{}", timediff - 1);
                                value_ok(&stat.value, &ethalon)
                            }
                        )
                        .or_else(
                            || {
                                let ethalon = format!("{}", timediff + 1);
                                value_ok(&stat.value, &ethalon)
                            }
                        )
                },
                _ => value_ok(&stat.value, ethalon.val)
            }.expect(
                &format!(
                    "Value for key {} does not match: {}, expected {}",
                    stat.key, stat.value, ethalon.val
                )
            )
        }

        if mask != 0 {
            panic!("Some stats ({:x}) did not found in Stat reply", mask)
        }

    }

    crate::test_helper::init_test_log();
    let engine = Arc::new(TestEngine::new().await);
    let (control, mut client, key_id) = start_control(
        DataSource::Engine(engine.clone())
    ).await.unwrap();

    let answer: Stats = request(
        &mut client,
        GetStats
    ).await.unwrap();
    check_stats(&answer, &engine, &key_id, false);

    let answer: Stats = request(
        &mut client,
        GetSelectedStats {
            filter: "*".to_string()
        }
    ).await.unwrap();
    check_stats(&answer, &engine, &key_id, true);

    client.shutdown().await.unwrap();
    control.shutdown().await;

}

