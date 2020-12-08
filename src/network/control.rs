use crate::config::{KeyRing, NodeConfigHandler};
use std::sync::Arc;

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

pub(crate) struct ControlServer {
    adnl: AdnlServer
}

impl ControlServer {
    pub async fn with_config(
        config: AdnlServerConfig,
        key_ring: Arc<dyn KeyRing>,
        node_config: Arc<NodeConfigHandler>
    ) -> Result<Self> {
        let ret = Self {
            adnl: AdnlServer::listen(config, vec![Arc::new(ControlQuerySubscriber::new(key_ring, node_config))]).await? 
        };
        Ok(ret)
    }

    pub fn shutdown(self) {
        self.adnl.shutdown()
    }
}
/*
struct KeyRing {
    temp: Arc<RwLock<HashMap<[u8; 32], KeyOptionJson>>>
}

impl KeyRing {
    fn new() -> Self {
        Self {
            temp: Arc::new(RwLock::new(HashMap::new()))
        }
    }
    fn generate(&self) -> Result<[u8; 32]> {
        let (private, public) = KeyOption::with_type_id(KeyOption::KEY_ED25519)?;
        let key_hash = hash_boxed(&public.into_tl_public_key()?)?;
        self.temp.write().unwrap().insert(key_hash, private);
        Ok(key_hash)
    }
    // find private key in KeyRing by public key hash
    fn find(&self, key_hash: &[u8; 32]) -> Result<KeyOption> {
        match self.temp.read().unwrap().get(key_hash) {
            Some(pvt_key) => KeyOption::from_private_key(pvt_key),
            None => fail!("key not found for hash: {}", hex::encode(key_hash))
        }
    }
}
*/
struct ControlQuerySubscriber {
    key_ring: Arc<dyn KeyRing>, 
    config: Arc<NodeConfigHandler>
}

impl ControlQuerySubscriber {
    fn new(key_ring: Arc<dyn KeyRing>, config: Arc<NodeConfigHandler>) -> Self {
        Self {
            key_ring: key_ring, 
            config: config
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
        fail!("Unsupported ControlQuery {:?}", query)
    }
}

