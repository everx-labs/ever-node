use crate::{
    block::convert_block_id_ext_api2blk,
    collator_test_bundle::CollatorTestBundle,
    config::{KeyRing, NodeConfigHandler},
    engine_traits::EngineOperations,
    validator::validator_utils::validatordescr_to_catchain_node
};
use adnl::{
    common::{deserialize, QueryResult, Subscriber, AdnlPeers},
    server::{AdnlServer, AdnlServerConfig}
};
use std::{ops::Deref, sync::Arc, time::{SystemTime, UNIX_EPOCH}};
use ton_api::ton::{
    self, PublicKey, TLObject,
    engine::validator::{
        keyhash::KeyHash, onestat::OneStat, signature::Signature, stats::Stats, Success
    },
    rpc::engine::validator::{
        AddAdnlId, AddValidatorAdnlAddress, AddValidatorPermanentKey, AddValidatorTempKey, 
        ControlQuery, ExportPublicKey, GenerateKeyPair, Sign, GetBundle, GetFutureBundle,
    }
};
use ton_types::{fail, error, Result};
use ton_block::BlockIdExt;

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

    async fn get_stats(&self) -> Result<Stats> {
        if let Some(engine) = self.engine.as_ref() {
            let mut stats: ton::vector<ton::Bare, OneStat> = ton::vector::default();

            // masterchainblocktime
            let mc_block_id = engine.load_last_applied_mc_block_id().await?;
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

            Ok(Stats {stats: stats})
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
            Ok(_) => return QueryResult::consume(
                self.process_generate_keypair().await?
            ),
            Err(query) => query
        };
        let query = match query.downcast::<ExportPublicKey>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.export_public_key(&query.key_hash.0)?
            ),
            Err(query) => query
        };
        let query = match query.downcast::<Sign>() {
            Ok(query) => return QueryResult::consume(
                self.process_sign_data(&query.key_hash.0, &query.data)?
            ),
            Err(query) => query
        };
        let query = match query.downcast::<AddValidatorPermanentKey>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.add_validator_permanent_key(
                    &query.key_hash.0, query.election_date, query.ttl
                ).await?
            ),
            Err(query) => query
        };
        let query = match query.downcast::<AddValidatorTempKey>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.add_validator_temp_key(
                    &query.permanent_key_hash.0, &query.key_hash.0, query.ttl
                )?
            ),
            Err(query) => query
        };
        let query = match query.downcast::<AddValidatorAdnlAddress>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.add_validator_adnl_address(
                    &query.permanent_key_hash.0, &query.key_hash.0, query.ttl
                ).await?
            ),
            Err(query) => query
        };
        let query = match query.downcast::<AddAdnlId>() {
            Ok(query) => return QueryResult::consume_boxed(
                self.add_adnl_address(&query.key_hash.0, query.category)?
            ),
            Err(query) => query
        };
        let query = match query.downcast::<GetBundle>() {
            Ok(query) => {
                let block_id = convert_block_id_ext_api2blk(&query.block_id)?;
                return QueryResult::consume_boxed(self.prepare_bundle(block_id).await?)
            },
            Err(query) => query
        };
        let query = match query.downcast::<GetFutureBundle>() {
            Ok(query) => {
                let prev_block_ids = query.prev_block_ids.iter().filter_map(
                    |id| convert_block_id_ext_api2blk(&id).ok()
                ).collect();
                return QueryResult::consume_boxed(
                    self.prepare_future_bundle(prev_block_ids).await?
                )
            },
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::lite_server::SendMessage>() {
            Ok(query) => {
                let message_data = query.body.0;
                return QueryResult::consume_boxed(self.redirect_external_message(&message_data).await?)
            }
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::GetStats>() {
            Ok(_) => {
                return QueryResult::consume_boxed(
                    ton_api::ton::engine::validator::Stats::Engine_Validator_Stats(
                        Box::new(self.get_stats().await?)
                ))
            },
            Err(query) => query
        };
        log::warn!("Unsupported ControlQuery (control server): {:?}", query);
        fail!("Unsupported ControlQuery {:?}", query)
    }
}

