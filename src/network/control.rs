use crate::{
    block::{convert_block_id_ext_api2blk, BlockStuff},
    block_proof::BlockProofStuff,
    db::{BlockHandle, InternalDb},
    collator_test_bundle::CollatorTestBundle,
    config::{KeyRing, NodeConfigHandler},
    engine_traits::EngineOperations, shard_state::ShardStateStuff,
};
use std::sync::Arc;
use std::ops::Deref;

use adnl::common::{deserialize, QueryResult, Subscriber, AdnlPeers};
use adnl::server::{AdnlServer, AdnlServerConfig};
use ton_api::ton::{
    self, PublicKey, TLObject,
    engine::validator::{
        keyhash::KeyHash,
        signature::Signature,
        Success,
    },
    rpc::engine::validator::ControlQuery,
};
use ton_types::{fail, Result};
use ton_block::{AccountIdPrefixFull, BlockIdExt};

pub(crate) struct DbEngine {
    db: Arc<dyn InternalDb>
}

impl DbEngine {
    pub fn new(db: Arc<dyn InternalDb>) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl crate::engine_traits::EngineOperations for DbEngine {
    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Arc<BlockHandle>> {
        self.db.load_block_handle(id)
    }
    async fn load_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        self.db.load_block_data(handle).await
    }
    async fn load_block_proof(&self, handle: &BlockHandle, is_link: bool) -> Result<BlockProofStuff> {
        self.db.load_block_proof(handle, is_link).await
    }
    async fn load_state(&self, block_id: &BlockIdExt) -> Result<ShardStateStuff> {
        self.db.load_shard_state_dynamic(block_id)
    }
    async fn find_block_by_seq_no(&self, acc_pfx: &AccountIdPrefixFull, seqno: u32) -> Result<Arc<BlockHandle>> {
        self.db.find_block_by_seq_no(acc_pfx, seqno)
    }
}

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
            adnl: AdnlServer::listen(config, vec![Arc::new(ControlQuerySubscriber::new(engine, key_ring, node_config))]).await? 
        };
        Ok(ret)
    }

    pub fn shutdown(self) {
        self.adnl.shutdown()
    }
}

struct ControlQuerySubscriber {
    engine: Option<Arc<dyn EngineOperations>>,
    key_ring: Arc<dyn KeyRing>, 
    config: Arc<NodeConfigHandler>
}

impl ControlQuerySubscriber {
    fn new(engine: Option<Arc<dyn EngineOperations>>, key_ring: Arc<dyn KeyRing>, config: Arc<NodeConfigHandler>) -> Self {
        Self {
            engine,
            key_ring,
            config
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
        //todo!()
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
            let bundle = CollatorTestBundle::build_with_ethalon(&block_id, engine.deref()).await.unwrap();
            bundle.save("target/bundles")?;
        }
        Ok(Success::Engine_Validator_Success)
    }
    async fn prepare_future_bundle(&self, prev_block_ids: Vec<BlockIdExt>) -> Result<Success> {
        if let Some(engine) = self.engine.as_ref() {
            let bundle = CollatorTestBundle::build_for_collating_block(prev_block_ids, engine.deref()).await?;
            bundle.save("target/bundles")?;
        }
        Ok(Success::Engine_Validator_Success)
    }
}

#[async_trait::async_trait]
impl Subscriber for ControlQuerySubscriber {
    async fn try_consume_query(&self, object: TLObject, _peers: &AdnlPeers) -> Result<QueryResult> {
        log::debug!("recieve object: {:?}", object);
        let query = match object.downcast::<ControlQuery>() {
            Ok(query) => deserialize(&query.data[..])?,
            Err(object) => return Ok(QueryResult::Rejected(object))
        };
        let query = match query.downcast::<ton::rpc::engine::validator::GenerateKeyPair>() {
            Ok(_) => return QueryResult::consume(self.process_generate_keypair().await?),
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::ExportPublicKey>() {
            Ok(query) => {
                return QueryResult::consume_boxed(self.export_public_key(&query.key_hash.0)?)
            }
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::Sign>() {
            Ok(query) => {
                return QueryResult::consume(self.process_sign_data(&query.key_hash.0, &query.data)?)
            }
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::AddValidatorPermanentKey>() {
            Ok(query) => {
                return QueryResult::consume_boxed(
                    self.add_validator_permanent_key(&query.key_hash.0, query.election_date, query.ttl).await?
                )
            }
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::AddValidatorTempKey>() {
            Ok(query) => {
                return QueryResult::consume_boxed(self.add_validator_temp_key(&query.permanent_key_hash.0, &query.key_hash.0, query.ttl)?)
            }
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::AddValidatorAdnlAddress>() {
            Ok(query) => {
                return QueryResult::consume_boxed(
                    self.add_validator_adnl_address(&query.permanent_key_hash.0, &query.key_hash.0, query.ttl).await?
                )
            }
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::AddAdnlId>() {
            Ok(query) => {
                return QueryResult::consume_boxed(self.add_adnl_address(&query.key_hash.0, query.category)?)
            }
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::GetBundle>() {
            Ok(query) => {
                let block_id = convert_block_id_ext_api2blk(&query.block_id)?;
                return QueryResult::consume_boxed(self.prepare_bundle(block_id).await?)
            }
            Err(query) => query
        };
        let query = match query.downcast::<ton::rpc::engine::validator::GetFutureBundle>() {
            Ok(query) => {
                let prev_block_ids = query.prev_block_ids.iter().filter_map(|id| convert_block_id_ext_api2blk(&id).ok()).collect();
                return QueryResult::consume_boxed(self.prepare_future_bundle(prev_block_ids).await?)
            }
            Err(query) => query
        };
        fail!("Unsupported ControlQuery {:?}", query)
    }
}

