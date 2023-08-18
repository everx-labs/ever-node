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
    collator_test_bundle::CollatorTestBundle, config::{KeyRing, NodeConfigHandler},
    engine_traits::EngineOperations, engine::Engine, network::node_network::NodeNetwork,
    validator::{validator_utils::validatordescr_to_catchain_node},
    validating_utils::{supported_version, supported_capabilities}
};

use adnl::{
    common::{QueryResult, Subscriber, AdnlPeers},
    server::{AdnlServer, AdnlServerConfig}
};
use std::sync::Arc;
use ton_api::{
    deserialize_boxed,
    ton::{
        self, PublicKey, TLObject, accountaddress::AccountAddress, 
        engine::validator::{
            keyhash::KeyHash, onestat::OneStat, signature::Signature, stats::Stats, Success
        },
        lite_server::configinfo::ConfigInfo, 
        raw::{ShardAccountState as ShardAccountStateBoxed, shardaccountstate::ShardAccountState},
        rpc::engine::validator::{
            AddAdnlId, AddValidatorAdnlAddress, AddValidatorPermanentKey, AddValidatorTempKey,
            ControlQuery, ExportPublicKey, GenerateKeyPair, Sign, GetBundle, GetFutureBundle
        },
    },
    IntoBoxed,
};
use ton_block::{BlockIdExt, MsgAddressInt, Serializable, ShardIdent, MASTERCHAIN_ID};
use ton_block_json::serialize_config_param;
use ton_types::{error, fail, KeyId, read_single_root_boc, Result, UInt256};

pub struct ControlServer {
    adnl: AdnlServer
}

impl ControlServer {
    pub async fn with_params(
        config: AdnlServerConfig,
        data_source: DataSource,
        key_ring: Arc<dyn KeyRing>,
        node_config: Arc<NodeConfigHandler>,
        network: Option<&NodeNetwork>
    ) -> Result<Self> {
        let ret = Self {
            adnl: AdnlServer::listen(
                config, 
                vec![
                    Arc::new(
                        ControlQuerySubscriber::new(data_source, key_ring, node_config, network)?
                    )
                ]
            ).await? 
        };
        Ok(ret)
    }
    pub async fn shutdown(self) {
        self.adnl.shutdown().await
    }
}

pub trait StatusReporter: Send + Sync {
    fn get_report(&self) -> u32;
}

pub enum DataSource {
    Engine(Arc<dyn EngineOperations>),
    Status(Arc<dyn StatusReporter>)
}

struct ControlQuerySubscriber {
    data_source: DataSource,
    key_ring: Arc<dyn KeyRing>, 
    config: Arc<NodeConfigHandler>,
    public_overlay_adnl_id: Option<Arc<KeyId>>
}

impl ControlQuerySubscriber {

    fn new(
        data_source: DataSource, 
        key_ring: Arc<dyn KeyRing>, 
        config: Arc<NodeConfigHandler>,
        network: Option<&NodeNetwork>,
    ) -> Result<Self> {
        let key_id = if let Some (network) = network {
            Some(network.get_key_id_by_tag(NodeNetwork::TAG_OVERLAY_KEY)?)
        } else {
            None
        };
        let ret = Self {
            data_source,
            key_ring,
            config,
            public_overlay_adnl_id: key_id
        };
        Ok(ret)
    }

    fn engine(&self) -> Result<&Arc<dyn EngineOperations>> {
        match self.data_source {
            DataSource::Engine(ref engine) => Ok(engine),
            _ => fail!("`engine is not set`")
        }
    }

    async fn get_all_config_params(&self) -> Result<ConfigInfo> {
        let engine = self.engine()?;
        let mc_state = engine.load_last_applied_mc_state().await?;
        let block_id = mc_state.block_id();
        let config_params = mc_state.config_params()?;        
        let config_info = ConfigInfo {
            mode: 0,
            id: block_id.clone(),
            state_proof: ton::bytes(vec!()),
            config_proof: ton::bytes(config_params.write_to_bytes()?)
        };
        Ok(config_info)
    }

    async fn get_config_params(&self, param_number: u32) -> Result<ConfigInfo> {
        let engine = self.engine()?;
        let mc_state = engine.load_last_applied_mc_state().await?;
        let config_params = mc_state.config_params()?;
        let config_param = serialize_config_param(config_params, param_number)?;
        let config_info = ConfigInfo {
            mode: 0,
            id: mc_state.block_id().clone(),
            state_proof: ton::bytes(vec!()),
            config_proof: ton::bytes(config_param.into_bytes())
        };
        Ok(config_info)
    }

    async fn get_account_state(&self, address: AccountAddress) -> Result<ShardAccountStateBoxed> {
        let engine = self.engine()?;
        let addr: MsgAddressInt = address.account_address.parse()?;
        let state = if addr.is_masterchain() {
            engine.load_last_applied_mc_state().await?
        } else {
            let mc_block_id = engine.load_shard_client_mc_block_id()?;
            let mc_block_id = mc_block_id.ok_or_else(
                || error!("Cannot load shard_client_mc_block_id!")
            )?;
            let mc_state = engine.load_state(&mc_block_id).await?;
            let mut shard_state = None;
            for id in mc_state.top_blocks(addr.workchain_id())? {
                if id.shard().contains_account(addr.address().clone())? {
                    shard_state = engine.load_state(&id).await.ok();
                    break;
                }
            }
            shard_state.ok_or_else(
                || error!("Cannot find actual shard for account {}", &address.account_address)
            )?
        };
        let shard_account_opt = state.shard_account(&addr.address())?;
        let result = match shard_account_opt {
            Some(shard_account) => {
                ShardAccountState {
                    shard_account: ton::bytes(shard_account.write_to_bytes()?),
                }.into_boxed()
            },
            None => ShardAccountStateBoxed::Raw_ShardAccountNone
        };
        Ok(result)
    }

    fn convert_sync_status(&self, sync_status: u32 ) -> String {
        match sync_status {
            Engine::SYNC_STATUS_START_BOOT => "start_boot".to_string(),
            Engine::SYNC_STATUS_LOAD_MASTER_STATE => "load_master_state".to_string(),
            Engine::SYNC_STATUS_LOAD_SHARD_STATES => "load_shard_states".to_string(),
            Engine::SYNC_STATUS_FINISH_BOOT => "finish_boot".to_string(),
            Engine::SYNC_STATUS_SYNC_BLOCKS => "synchronization_by_blocks".to_string(),
            Engine::SYNC_STATUS_FINISH_SYNC => "synchronization_finished".to_string(),
            Engine::SYNC_STATUS_CHECKING_DB => "checking_db".to_string(),
            Engine::SYNC_STATUS_DB_BROKEN => "db_broken".to_string(),
            _ => "no_set_status".to_string()
        }
    }

    fn block_id_to_json(block_id: &BlockIdExt) -> String {
        serde_json::json!({
            "shard":  block_id.shard().to_string(),
            "seq_no": block_id.seq_no(),
            "rh":     format!("{:x}", block_id.root_hash),
            "fh":     format!("{:x}", block_id.file_hash)
        }).to_string()
    }

    fn add_stats(stats: &mut Vec<OneStat>, key: impl ToString, value: impl ToString) {
        stats.push(OneStat {
            key: key.to_string(),
            value: value.to_string()
        })
    }

    fn statistics_to_json(
        map: &lockfree::map::Map<ShardIdent, u64>,
        now: i64,
        new_format: bool
    ) -> String {
        let mut json_map = serde_json::Map::new();
        for item in map.iter() {
            let value = if new_format {
                match *item.val() {
                    0 => -1,
                    value => now - value as i64
                }.into()
            } else {
                match *item.val() {
                    0 => "never".to_string(),
                    value => format!("{} sec ago", now - value as i64)
                }.into()
            };
            json_map.insert(item.key().to_string(), value);
        }
        format!("{:#}", serde_json::Value::from(json_map))
    }

    fn get_shards_time_diff(engine: &Arc<dyn EngineOperations>, now: u32) -> Result<u32> {
        let shard_client_mc_block_id = engine.load_shard_client_mc_block_id()?
            .ok_or_else(|| error!("Cannot load shard_mc_block_id"))?;
        let shard_client_mc_block_handle = engine.load_block_handle(&shard_client_mc_block_id)?
            .ok_or_else(|| error!("Cannot load handle for block {}", &shard_client_mc_block_id))?;
        Ok(now - shard_client_mc_block_handle.gen_utime()?)
    }

    async fn get_selected_stats(&self, filter: Option<&str>) -> Result<Stats> {

        let mut stats = Vec::new();
        let new_format = filter.is_some();

        // sync status
        let sync_status = match &self.data_source {
            DataSource::Engine(engine) => engine.get_sync_status(),
            DataSource::Status(status) => status.get_report()
        }; 
        let sync_status = format!("\"{}\"", self.convert_sync_status(sync_status));
        Self::add_stats(
            &mut stats, 
            if new_format {
                "node_status"
            } else {
                "sync_status"
            }, 
            sync_status
        );
        if let DataSource::Status(_) = &self.data_source {
            return Ok(Stats {stats: stats.into()})
        }

        let engine = self.engine()?;
        let now = engine.now();

        let mc_block_id = if let Some(id) = engine.load_last_applied_mc_block_id()? {
            id
        } else {
            Self::add_stats(&mut stats, "masterchainblock", "\"not set\"");
            return Ok(Stats {stats: stats.into()})
        };

        let mc_block_handle = engine.load_block_handle(&mc_block_id)?
            .ok_or_else(|| error!("Cannot load handle for block {}", &mc_block_id))?;
       
        // masterchainblocktime
        Self::add_stats(&mut stats, "masterchainblocktime", mc_block_handle.gen_utime()?);

        // masterchainblocknumber
        Self::add_stats(&mut stats, "masterchainblocknumber", mc_block_handle.id().seq_no());

        Self::add_stats(&mut stats, "node_version", format!("\"{}\"", env!("CARGO_PKG_VERSION")));
        let public_overlay_adnl_id = self.public_overlay_adnl_id.as_ref().ok_or_else(|| 
            error!("Public overlay key id didn`t set!")
        )?;
        Self::add_stats(&mut stats, "public_overlay_key_id", format!("\"{}\"", &public_overlay_adnl_id));

        if new_format {
            Self::add_stats(&mut stats, "supported_block", supported_version());
            Self::add_stats(&mut stats, "supported_capabilities", supported_capabilities());
        }

        // timediff
        let diff = now - mc_block_handle.gen_utime()?;
        Self::add_stats(&mut stats, "timediff", diff);

        // shards timediff
        match Self::get_shards_time_diff(engine, now) {
            Err(_) => Self::add_stats(&mut stats, "shards_timediff", "\"unknown\""),
            Ok(shards_timediff) => Self::add_stats(&mut stats, "shards_timediff", shards_timediff),
        };
        
        let mc_state = engine.load_last_applied_mc_state().await.ok();      

        // global network ID
        if new_format {
            if let Some(mc_state) = &mc_state {
                Self::add_stats(&mut stats, "global_id", mc_state.state()?.global_id())
            } else {
                Self::add_stats(&mut stats, "global_id", "\"unknown\"")
            }
        }

        // in_current_vset_p34
        let adnl_ids = self.config.get_actual_validator_adnl_ids()?;
        if let Some(mc_state) = &mc_state {
            let current = mc_state.config_params()?.validator_set()?.list().iter().any(|val| {
                let catchain_node = validatordescr_to_catchain_node(val);
                let is_validator = adnl_ids.contains(&catchain_node.adnl_id);
                if is_validator {
                    Self::add_stats(&mut stats,
                        "current_vset_p34_adnl_id",
                        format!("\"{}\"", &catchain_node.adnl_id)
                    );
                }
                is_validator
            });
            Self::add_stats(&mut stats, "in_current_vset_p34", current)
        } else {
            Self::add_stats(&mut stats, "in_current_vset_p34", "\"unknown\"")
        }

        // in_next_vset_p36
        if let Some(mc_state) = &mc_state {
            let next = mc_state.config_params()?.next_validator_set()?.list().iter().any(|val| {
                let catchain_node = validatordescr_to_catchain_node(val);
                let is_validator = adnl_ids.contains(&catchain_node.adnl_id);
                if is_validator {
                    Self::add_stats(&mut stats,
                        "next_vset_p36_adnl_id",
                        format!("\"{}\"", &catchain_node.adnl_id)
                    );
                }
                is_validator
            });
            Self::add_stats(&mut stats, "in_next_vset_p36", next)
        } else {
            Self::add_stats(&mut stats, "in_next_vset_p36", "\"unknown\"")
        }

        let value = match engine.load_last_applied_mc_block_id() {
            Ok(Some(block_id)) => Self::block_id_to_json(&block_id),
            Ok(None) => "\"no last applied masterchain block{}\"".to_string(),
            Err(err) => format!("\"{}\"", err)
        };
        Self::add_stats(&mut stats, "last_applied_masterchain_block_id", value);

        let value = match engine.processed_workchain() {
            Some(MASTERCHAIN_ID) => "\"masterchain\"".to_string(),
            Some(workchain_id) => format!("\"{}\"", workchain_id),
            None => "\"not specified\"".to_string(),
        };
        Self::add_stats(&mut stats, "processed_workchain", value);

        let value = Self::statistics_to_json(
            engine.last_validation_time(), 
            now as i64, 
            new_format
        );
        if new_format {
            Self::add_stats(&mut stats, "last_validation_ago_sec", value)
        } else {
            Self::add_stats(&mut stats, "validation_stats", value)
        }

        let value = Self::statistics_to_json(
            engine.last_collation_time(), 
            now as i64,
            new_format
        );
        if new_format {
            Self::add_stats(&mut stats, "last_collation_ago_sec", value)
        } else {
            Self::add_stats(&mut stats, "collation_stats", value)
        }

        // tps_10
        if let Ok(tps) = engine.calc_tps(10) {
            Self::add_stats(&mut stats, "tps_10", tps);
        }

        // tps_300
        if let Ok(tps) = engine.calc_tps(300) {
            Self::add_stats(&mut stats, "tps_300", tps);
        }

        Self::add_stats(&mut stats, "validation_status", format!("\"{:?}\"", engine.validation_status()));

        Ok(Stats { stats: stats.into() })

    }

    async fn process_generate_keypair(&self) -> Result<KeyHash> {
        let ret = KeyHash {
            key_hash: UInt256::with_array(self.key_ring.generate().await?)
        };
        Ok(ret)
    }

    fn export_public_key(&self, key_hash: &[u8; 32]) -> Result<PublicKey> {
        let private = self.key_ring.find(key_hash)?;
        (&private).try_into()
    }

    fn process_sign_data(&self, key_hash: &[u8; 32], data: &[u8]) -> Result<Signature> {
        let sign = self.key_ring.sign_data(key_hash, data)?;
        Ok(Signature {signature: ton::bytes(sign)})
    }

    async fn add_validator_permanent_key(
        &self, 
        key_hash: &[u8; 32], 
        election_date: ton::int, 
        _ttl: ton::int
    ) -> Result<Success> {
        self.config.add_validator_key(key_hash, election_date).await?;
        Ok(Success::Engine_Validator_Success)
    }

    fn add_validator_temp_key(
        &self, 
        _perm_key_hash: &[u8; 32], 
        _key_hash: &[u8; 32], 
        _ttl: ton::int
    ) -> Result<Success> {
        Ok(Success::Engine_Validator_Success)
    }

    async fn add_validator_adnl_address(
        &self, 
        perm_key_hash: &[u8; 32], 
        key_hash: &[u8; 32], 
        _ttl: ton::int
    ) -> Result<Success> {
        self.config.add_validator_adnl_key(perm_key_hash, key_hash).await?;
        Ok(Success::Engine_Validator_Success)
    }
    
    fn add_adnl_address(&self, _key_hash: &[u8; 32], _category: ton::int) -> Result<Success> {
        Ok(Success::Engine_Validator_Success)
    }

    async fn prepare_bundle(&self, block_id: BlockIdExt) -> Result<Success> {
        if let DataSource::Engine(ref engine) = self.data_source {
            let bundle = CollatorTestBundle::build_with_ethalon(&block_id, engine).await?;
            tokio::task::spawn_blocking(move || {
                bundle.save("target/bundles").ok();
            });
        }
        Ok(Success::Engine_Validator_Success)
    }

    async fn prepare_future_bundle(&self, prev_block_ids: Vec<BlockIdExt>) -> Result<Success> {
        if let DataSource::Engine(ref engine) = self.data_source {
            let bundle = CollatorTestBundle::build_for_collating_block(
                prev_block_ids, engine
            ).await?;
            tokio::task::spawn_blocking(move || {
                bundle.save("target/bundles").ok();
            });
        }
        Ok(Success::Engine_Validator_Success)
    }

    async fn redirect_external_message(&self, message_data: &[u8]) -> Result<Success> {
        let engine = self.engine()?;
        let id = read_single_root_boc(message_data)?.repr_hash();
        engine.redirect_external_message(message_data, id).await?;
        Ok(Success::Engine_Validator_Success)
    }

    fn set_states_gc_interval(&self, interval_ms: u32) -> Result<Success> {
        self.engine()?.adjust_states_gc_interval(interval_ms);
        self.config.store_states_gc_interval(interval_ms);
        Ok(Success::Engine_Validator_Success)
    }

}

#[async_trait::async_trait]
impl Subscriber for ControlQuerySubscriber {
    async fn try_consume_query(&self, object: TLObject, _peers: &AdnlPeers) -> Result<QueryResult> {
        log::info!("recieve object (control server): {:?}", object);
        let query = match object.downcast::<ControlQuery>() {
            Ok(query) => deserialize_boxed(&query.data[..])?,
            Err(object) => return Ok(QueryResult::Rejected(object))
        };
        log::info!("query (control server): {:?}", query);
        let query = match query.downcast::<GenerateKeyPair>() {
            Ok(_) => return QueryResult::consume(
                self.process_generate_keypair().await?,
                #[cfg(feature = "telemetry")]
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<ExportPublicKey>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.export_public_key(query.key_hash.as_slice())?,
                #[cfg(feature = "telemetry")]
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<Sign>() {
            Ok(query) => return QueryResult::consume(
                self.process_sign_data(query.key_hash.as_slice(), &query.data)?,
                #[cfg(feature = "telemetry")]
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<AddValidatorPermanentKey>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.add_validator_permanent_key(
                    query.key_hash.as_slice(), query.election_date, query.ttl
                ).await?,
                #[cfg(feature = "telemetry")]
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<AddValidatorTempKey>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.add_validator_temp_key(
                    query.permanent_key_hash.as_slice(), query.key_hash.as_slice(), query.ttl
                )?,
                #[cfg(feature = "telemetry")]
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<AddValidatorAdnlAddress>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.add_validator_adnl_address(
                    query.permanent_key_hash.as_slice(), query.key_hash.as_slice(), query.ttl
                ).await?,
                #[cfg(feature = "telemetry")]
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<AddAdnlId>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.add_adnl_address(query.key_hash.as_slice(), query.category)?,
                #[cfg(feature = "telemetry")]
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<GetBundle>() {
            Ok(query) => {
                return QueryResult::consume_boxed(
                    self.prepare_bundle(query.block_id.clone()).await?, 
                #[cfg(feature = "telemetry")]
                None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<GetFutureBundle>() {
            Ok(query) => {
                let prev_block_ids = query.prev_block_ids.iter().map(
                    |id| id.clone()
                ).collect();
                return QueryResult::consume_boxed(
                    self.prepare_future_bundle(prev_block_ids).await?,
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::lite_server::SendMessage>() {
            Ok(query) => {
                let message_data = query.body.0;
                return QueryResult::consume_boxed(
                    self.redirect_external_message(&message_data).await?,
                    #[cfg(feature = "telemetry")]
                    None
                )
            }
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::raw::GetShardAccountState>() {
            Ok(account) => {
                let answer = self.get_account_state(account.account_address).await?;
                return QueryResult::consume_boxed(
                    answer, 
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::lite_server::GetConfigParams>() {
            Ok(query) => {
                let param_number = query.param_list.iter().next().ok_or_else(|| error!("Invalid param_number"))?;
                let answer = self.get_config_params(*param_number as u32).await?;

                return QueryResult::consume_boxed(
                    answer.into_boxed(),
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::lite_server::GetConfigAll>() {
            Ok(_) => {
                let answer = self.get_all_config_params().await?;
                return QueryResult::consume_boxed(
                    answer.into_boxed(),
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::GetStats>() {
            Ok(_) => {
                let answer = self.get_selected_stats(None).await?;
                return QueryResult::consume_boxed(
                    answer.into_boxed(),
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::GetSelectedStats>() {
            Ok(get_stats) => {
                let answer = self.get_selected_stats(Some(&get_stats.filter)).await?;
                return QueryResult::consume_boxed(
                    answer.into_boxed(),
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::SetStatesGcInterval>() {
            Ok(query) => {
                return QueryResult::consume_boxed(
                    self.set_states_gc_interval(query.interval_ms as u32)?,
                    #[cfg(feature = "telemetry")]
                    None
                )
            }
            Err(query) => query
        };
        log::warn!("Unsupported ControlQuery (control server): {:?}", query);
        fail!("Unsupported ControlQuery {:?}", query)
    }
}

