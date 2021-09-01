use crate::{
    block::{convert_block_id_ext_api2blk, convert_block_id_ext_blk2api},
    collator_test_bundle::CollatorTestBundle,
    config::{KeyRing, NodeConfigHandler},
    engine_traits::EngineOperations,
    validator::validator_utils::validatordescr_to_catchain_node
};
use adnl::{
    common::{deserialize, serialize, QueryResult, Subscriber, AdnlPeers},
    server::{AdnlServer, AdnlServerConfig}
};
use std::{ops::Deref, sync::Arc, str::FromStr, time::{SystemTime, UNIX_EPOCH}};
use ton_api::{
    ton::{
        self, bytes, PublicKey, TLObject, accountaddress::AccountAddress, raw::fullaccountstate::FullAccountState,
        engine::validator::{
            keyhash::KeyHash, onestat::OneStat, signature::Signature, stats::Stats, Success
        },
        lite_server::configinfo::ConfigInfo,
        rpc::engine::validator::{
            AddAdnlId, AddValidatorAdnlAddress, AddValidatorPermanentKey, AddValidatorTempKey, 
            ControlQuery, ExportPublicKey, GenerateKeyPair, Sign, GetBundle, GetFutureBundle
        },
    },
    IntoBoxed,
};
use ton_types::{fail, error, Result, serialize_toc};
use ton_block::{BlockIdExt, MsgAddressInt, Serializable};
use ton_block_json::serialize_config_param;

pub struct ControlServer {
    adnl: AdnlServer
}

impl ControlServer {
    pub async fn with_config(
        config: AdnlServerConfig,
        engine: Option<Arc<dyn EngineOperations>>,
        key_ring: Arc<dyn KeyRing>,
        node_config: Arc<NodeConfigHandler>
    ) -> Result<Self> {
        let ret = Self {
            adnl: AdnlServer::listen(
                config, 
                vec![Arc::new(ControlQuerySubscriber::new(engine, key_ring, node_config))]
            ).await? 
        };
        Ok(ret)
    }
    pub async fn shutdown(self) {
        self.adnl.shutdown().await
    }
}

struct ControlQuerySubscriber {
    engine: Option<Arc<dyn EngineOperations>>,
    key_ring: Arc<dyn KeyRing>, 
    config: Arc<NodeConfigHandler>
}

impl ControlQuerySubscriber {
    fn new(
        engine: Option<Arc<dyn EngineOperations>>, 
        key_ring: Arc<dyn KeyRing>, 
        config: Arc<NodeConfigHandler>
    ) -> Self {
        let ret = Self {
            engine,
            key_ring,
            config
        };
        // To get rid of unused engine field warning
        if ret.engine.is_none() {
            log::debug!("Running control server without engine access")
        }
        ret
    }

    async fn get_config_params(&self, param_number: u32) -> Result<ton::lite_server::configinfo::ConfigInfo> {
        if let Some(engine) = self.engine.as_ref() {
            let mc_state = engine.load_last_applied_mc_state().await?;
            let config_params = mc_state.config_params()?;
            let config_param = serialize_config_param(&config_params, param_number)?;
            let config_info = ConfigInfo {
                mode: 0,
                id: convert_block_id_ext_blk2api(mc_state.block_id()),
                state_proof: bytes(vec!()),
                config_proof: bytes(config_param.into_bytes())
            };

            Ok(config_info)
        } else {
            fail!("Engine was not set!");
        }
    }

    async fn get_account_state(&self, address: AccountAddress, workchain: i32) -> Result<ton_api::ton::data::Data> {
        if let Some(engine) = self.engine.as_ref() {
            let addr = MsgAddressInt::from_str(&address.account_address)?;
            let mc_state = if addr.is_masterchain() {
                engine.load_last_applied_mc_state().await?
            } else {
                let mc_block_id = engine.load_shard_client_mc_block_id()?;
                let mc_block_id = mc_block_id.ok_or_else(|| error!("shard_client_mc_block_id can`t load!"))?;
                engine.load_state(&mc_block_id).await?
            };
            
            let state = if addr.is_masterchain() {
                    mc_state
            } else {
                let mut shard_state = None;
                for id in mc_state.shard_hashes()?.top_blocks(&[workchain])? {
                    if id.shard().contains_account(addr.address().clone())? {
                        shard_state = engine.load_state(&id).await.ok();
                        break;
                    }
                }
                shard_state.ok_or_else(|| error!("Cannot found actual state from account {}", &address.account_address))?
            };

            let shard_account = state.state()
                .read_accounts()?
                .account(&addr.address())?
                .ok_or_else(|| error!("Error load account {} from state", &address.account_address))?;

            let account = shard_account.read_account()?;

            let code = account.get_code()
                .ok_or_else(|| error!("Cannot load code from account {}", &address.account_address))?;

            //let data = account.get_data()
            //    .ok_or_else(|| error!("Cannot load data from account {}", &address.account_address))?;

            let transaction_id = ton::internal::transactionid::TransactionId {
                lt: shard_account.last_trans_lt() as i64,
                hash: bytes(shard_account.last_trans_hash().as_slice().to_vec()),
            };
            let account_state = FullAccountState {
                balance: 0,
                code: bytes(serialize_toc(&code)?),
                //data: bytes(serialize_toc(&data)?),
                data: bytes(serialize_toc(&shard_account.account_cell())?),
                last_transaction_id: transaction_id,
                block_id: convert_block_id_ext_blk2api(state.block_id()),
                frozen_hash: bytes(account.status().write_to_bytes()?),
                sync_utime: 0
            };

            let account = ton_api::ton::data::Data {
                bytes: ton_api::secure::SecureBytes::new(serialize(&account_state.into_boxed())?)
            };

            Ok(account)
        } else {
            fail!("Engine was not set!");
        }
    }

    async fn get_stats(&self) -> Result<Stats> {
        if let Some(engine) = self.engine.as_ref() {
            let mut stats: ton::vector<ton::Bare, OneStat> = ton::vector::default();

            let mc_block_id = if let Some(id) = engine.load_last_applied_mc_block_id()? {
                id
            } else {
                stats.0.push(OneStat {
                    key: "masterchainblock".to_string(),
                    value: "not set".to_string()
                });
                return Ok(Stats {stats: stats})
            };

            // masterchainblocktime
            let mc_block_handle = engine.load_block_handle(&mc_block_id)?.ok_or_else(
                || error!("Cannot load handle for block {}", &mc_block_id)
            )?;
            stats.0.push(OneStat {
                key: "masterchainblocktime".to_string(),
                value: mc_block_handle.gen_utime()?.to_string()
            });

            // masterchainblocknumber
            stats.0.push(OneStat {
                key: "masterchainblocknumber".to_string(),
                value: mc_block_id.seq_no().to_string()
            });

            // timediff
            let diff = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i32 - 
                mc_block_handle.gen_utime()? as i32;

            stats.0.push(OneStat {
                key: "timediff".to_string(),
                value: diff.to_string()
            });

            // in_current_vset_p34
            let adnl_ids = self.config.get_actual_validator_adnl_ids()?;
            let mc_state = engine.load_last_applied_mc_state().await?;
            let current = mc_state.config_params()?.validator_set()?.list().iter().any(|val| {
                match validatordescr_to_catchain_node(val) {
                    Ok(catchain_node) => adnl_ids.contains(&catchain_node.adnl_id),
                    _ => false
                }
            });
            stats.0.push(OneStat {
                key: "in_current_vset_p34".to_string(),
                value: current.to_string()
            });

            // in_next_vset_p36
            let next = mc_state.config_params()?.next_validator_set()?.list().iter().any(|val| {
                match validatordescr_to_catchain_node(val) {
                    Ok(catchain_node) => adnl_ids.contains(&catchain_node.adnl_id),
                    _ => false
                }
            });
            stats.0.push(OneStat {
                key: "in_next_vset_p36".to_string(), 
                value: next.to_string()
            });

            let value = match engine.load_last_applied_mc_state_or_zerostate().await {
                Ok(mc_state) => mc_state.block_id().to_string(),
                Err(err) => err.to_string()
            };
            let key = "last applied masterchain block id".to_string();
            let value = format!("\"{}\"", value);
            stats.0.push(OneStat { key, value });

            let value = match engine.processed_workchain().await {
                Ok((true, _workchain_id)) => "masterchain".to_string(),
                Ok((false, workchain_id)) => format!("{}", workchain_id),
                Err(err) => err.to_string()
            };
            let key = "processed workchain".to_string();
            stats.0.push(OneStat { key, value });

            // validation_stats
            let validation_stats = engine.validation_status();

            let mut stat = String::new();
            let ago = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs();

            for item in validation_stats.iter() {
                stat.push_str("shard: ");
                stat.push_str(&item.key().to_string()); 
                stat.push_str(" - ");
                if item.val() == &0 {
                    stat.push_str("never");
                } else {
                    stat.push_str(&(ago - item.val()).to_string());
                    stat.push_str(" sec ago");
                }
                stat.push_str("\t");
            }

            let value = format!("\"{}\"", stat.to_string()); 
            stats.0.push(OneStat {
                key: "validation_stats".to_string(), 
                value: value
            });

            // collation_stats
            let mut stat = String::new();
            let collation_stats = engine.collation_status();
            for item in collation_stats.iter() {
                stat.push_str("shard: ");
                stat.push_str(&item.key().to_string()); 
                stat.push_str(" - ");
                if item.val() == &0 {
                    stat.push_str("never");
                } else {
                    stat.push_str(&(ago - item.val()).to_string());
                    stat.push_str(" sec ago");
                }
                stat.push_str("\t");
            }

            let value = format!("\"{}\"", stat.to_string()); 
            stats.0.push(OneStat {
                key: "collation_stats".to_string(), 
                value: value
            });

            // tps_10
            if let Ok(tps) = engine.calc_tps(10) {
                stats.0.push(OneStat {
                    key: "tps_10".to_string(), 
                    value: tps.to_string()
                });
            }

            // tps_300
            if let Ok(tps) = engine.calc_tps(300) {
                stats.0.push(OneStat {
                    key: "tps_300".to_string(), 
                    value: tps.to_string()
                });
            }

            Ok(Stats {stats})
        } else {
            fail!("Engine was not set!");
        }
    }

    async fn process_generate_keypair(&self) -> Result<KeyHash> {
        let key_hash = self.key_ring.generate().await?;
        Ok(KeyHash {key_hash: ton::int256(key_hash)})
    }
    fn export_public_key(&self, key_hash: &[u8; 32]) -> Result<PublicKey> {
        let private = self.key_ring.find(key_hash)?;
        private.into_tl_public_key()
    }
    fn process_sign_data(&self, key_hash: &[u8; 32], data: &[u8]) -> Result<Signature> {
        let sign = self.key_ring.sign_data(key_hash, data)?;
        Ok(Signature {signature: ton::bytes(sign)})
    }
    async fn add_validator_permanent_key(&self, key_hash: &[u8; 32], elecation_date: ton::int, _ttl: ton::int) -> Result<Success> {
        self.config.add_validator_key(key_hash, elecation_date).await?;
        Ok(Success::Engine_Validator_Success)
    }
    fn add_validator_temp_key(&self, _perm_key_hash: &[u8; 32], _key_hash: &[u8; 32], _ttl: ton::int) -> Result<Success> {
        Ok(Success::Engine_Validator_Success)
    }
    async fn add_validator_adnl_address(&self, perm_key_hash: &[u8; 32], key_hash: &[u8; 32], _ttl: ton::int) -> Result<Success> {
        self.config.add_validator_adnl_key(perm_key_hash, key_hash).await?;
        Ok(Success::Engine_Validator_Success)
    }
    fn add_adnl_address(&self, _key_hash: &[u8; 32], _category: ton::int) -> Result<Success> {
        Ok(Success::Engine_Validator_Success)
    }
    async fn prepare_bundle(&self, block_id: BlockIdExt) -> Result<Success> {
        if let Some(engine) = self.engine.as_ref() {
            let bundle = CollatorTestBundle::build_with_ethalon(&block_id, engine.deref()).await?;
            tokio::task::spawn_blocking(move || {
                bundle.save("target/bundles").ok();
            });
        }
        Ok(Success::Engine_Validator_Success)
    }
    async fn prepare_future_bundle(&self, prev_block_ids: Vec<BlockIdExt>) -> Result<Success> {
        if let Some(engine) = self.engine.as_ref() {
            let bundle = CollatorTestBundle::build_for_collating_block(prev_block_ids, engine.deref()).await?;
            tokio::task::spawn_blocking(move || {
                bundle.save("target/bundles").ok();
            });
        }
        Ok(Success::Engine_Validator_Success)
    }
    async fn redirect_external_message(&self, message_data: &[u8]) -> Result<Success> {
        if let Some(engine) = self.engine.as_ref() {
            engine.redirect_external_message(&message_data).await?;
            Ok(Success::Engine_Validator_Success)
        } else {
            fail!("`engine is not set`")
        }
    }

    fn set_states_gc_interval(&self, interval_ms: u32) -> Result<Success> {
        if let Some(engine) = self.engine.as_ref() {
            engine.adjust_states_gc_interval(interval_ms);
            self.config.store_states_gc_interval(interval_ms);
            Ok(Success::Engine_Validator_Success)
        } else {
            fail!("`engine is not set`")
        }
    }
}

#[async_trait::async_trait]
impl Subscriber for ControlQuerySubscriber {
    async fn try_consume_query(&self, object: TLObject, _peers: &AdnlPeers) -> Result<QueryResult> {
        log::info!("recieve object (control server): {:?}", object);
        let query = match object.downcast::<ControlQuery>() {
            Ok(query) => deserialize(&query.data[..])?,
            Err(object) => return Ok(QueryResult::Rejected(object))
        };
        log::info!("query (control server): {:?}", query);
        let query = match query.downcast::<GenerateKeyPair>() {
            Ok(_) => return QueryResult::consume(self.process_generate_keypair().await?, None),
            Err(query) => query
        };
        let query = match query.downcast::<ExportPublicKey>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.export_public_key(&query.key_hash.0)?,
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<Sign>() {
            Ok(query) => return QueryResult::consume(
                self.process_sign_data(&query.key_hash.0, &query.data)?,
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<AddValidatorPermanentKey>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.add_validator_permanent_key(
                    &query.key_hash.0, query.election_date, query.ttl
                ).await?,
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<AddValidatorTempKey>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.add_validator_temp_key(
                    &query.permanent_key_hash.0, &query.key_hash.0, query.ttl
                )?,
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<AddValidatorAdnlAddress>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.add_validator_adnl_address(
                    &query.permanent_key_hash.0, &query.key_hash.0, query.ttl
                ).await?,
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<AddAdnlId>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.add_adnl_address(&query.key_hash.0, query.category)?,
                None
            ),
            Err(query) => query
        };
        let query = match query.downcast::<GetBundle>() {
            Ok(query) => {
                let block_id = convert_block_id_ext_api2blk(&query.block_id)?;
                return QueryResult::consume_boxed(self.prepare_bundle(block_id).await?, None)
            },
            Err(query) => query
        };
        let query = match query.downcast::<GetFutureBundle>() {
            Ok(query) => {
                let prev_block_ids = query.prev_block_ids.iter().filter_map(
                    |id| convert_block_id_ext_api2blk(&id).ok()
                ).collect();
                return QueryResult::consume_boxed(
                    self.prepare_future_bundle(prev_block_ids).await?,
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
                    None
                )
            }
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::raw::GetAccount>() {
            Ok(account) => {
                let answer = self.get_account_state(account.account_address, account.workchain).await?;
                return QueryResult::consume_boxed(answer.into_boxed(), None)
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::lite_server::GetConfigParams>() {
            Ok(query) => {
                let param_number = query.param_list.iter().next().ok_or_else(|| error!("Invalid param_number"))?;
                let answer = self.get_config_params(*param_number as u32).await?;

                return QueryResult::consume_boxed(answer.into_boxed(), None)
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::GetStats>() {
            Ok(_) => {
                let answer = self.get_stats().await?;
                return QueryResult::consume_boxed(answer.into_boxed(), None)
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::SetStatesGcInterval>() {
            Ok(query) => {
                return QueryResult::consume_boxed(
                    self.set_states_gc_interval(query.interval_ms as u32)?,
                    None
                )
            }
            Err(query) => query
        };
        log::warn!("Unsupported ControlQuery (control server): {:?}", query);
        fail!("Unsupported ControlQuery {:?}", query)
    }
}

