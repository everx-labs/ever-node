use clap::{Arg, App};
use storage::block_handle_db::BlockHandle;
use std::{
    str::FromStr,
    sync::{atomic::{AtomicU32, Ordering}, Arc},
    time::SystemTime,
};
use ton_block::{ShardIdent, Message, BlockIdExt, ValidatorSet};
use ton_types::{fail, UInt256};
use ton_node::{
    block::BlockStuff,
    block_proof::BlockProofStuff,
    collator_test_bundle::create_engine_allocated, 
    engine_traits::{EngineAlloc, EngineTelemetry, EngineOperations},
    ext_messages::MessagesPool,
    internal_db::{LAST_APPLIED_MC_BLOCK, SHARD_CLIENT_MC_BLOCK, InternalDb, InternalDbConfig},
    shard_state::ShardStateStuff,
    types::top_block_descr::TopBlockDescrStuff,
    validator::{
        CollatorSettings,
        collator::Collator,
        telemetry::CollatorValidatorTelemetry
    },
};
#[cfg(feature = "telemetry")]
use ton_node::collator_test_bundle::create_engine_telemetry;
use ton_types::{error, Result};

/**
 * Moc Engine for crafting block
 * Opens database for readonly and produces masterchain block by seq no
 * It can also add external messages
 */
pub struct TestEngine {
    pub db: Arc<InternalDb>,
    pub now: Arc<AtomicU32>,
    pub ext_messages: MessagesPool,
    #[cfg(feature = "telemetry")]
    engine_telemetry: Arc<EngineTelemetry>,
    #[cfg(feature = "telemetry")]
    collator_telemetry: CollatorValidatorTelemetry,
    engine_allocated: Arc<EngineAlloc>,
}

impl TestEngine {
    pub async fn new(db_dir: &str) -> Result<Self> {
        let db_config = InternalDbConfig { 
            cells_gc_interval_sec: 0, 
            db_directory: db_dir.to_string() 
        };
        #[cfg(feature = "telemetry")]
        let telemetry = create_engine_telemetry();
        let allocated = create_engine_allocated();
        let db = Arc::new(
            InternalDb::with_update(
                db_config, 
                false,
                false,
                false,
                &|| Ok(()),
                None,
                #[cfg(feature = "telemetry")]
                telemetry.clone(),
                allocated.clone(),
            ).await?
        );
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as u32;
        Ok(Self {
            db,
            now: Arc::new(AtomicU32::new(now)),
            ext_messages: MessagesPool::new(),
            #[cfg(feature = "telemetry")]
            engine_telemetry: telemetry,
            #[cfg(feature = "telemetry")]
            collator_telemetry: CollatorValidatorTelemetry::default(),
            engine_allocated: allocated
        })
    }
}

#[async_trait::async_trait]
impl EngineOperations for TestEngine {

    fn now(&self) -> u32 {
        self.now.load(Ordering::Relaxed)
    }

    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>> {
        self.db.load_block_handle(id)
    }

    async fn load_state(&self, block_id: &BlockIdExt) -> Result<Arc<ShardStateStuff>> {
        self.db.load_shard_state_dynamic(block_id)
    }

    async fn load_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        self.db.load_block_data(handle).await
    }

    async fn load_block_proof(
        &self, 
        handle: &Arc<BlockHandle>, 
        is_link: bool
    ) -> Result<BlockProofStuff> {
        self.db.load_block_proof(handle, is_link).await
    }

    fn load_last_applied_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
        self.db.load_full_node_state(LAST_APPLIED_MC_BLOCK)
    }
    async fn load_last_applied_mc_state(&self) -> Result<Arc<ShardStateStuff>> {
        if let Some(id) = self.load_last_applied_mc_block_id()? {
            self.load_state(&id).await
        } else {
            fail!("No last applied MC block set")
        }
    }
    fn load_shard_client_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
        self.db.load_full_node_state(SHARD_CLIENT_MC_BLOCK)
    }

    fn load_block_prev1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db.load_block_prev1(id)
    }

    fn load_block_prev2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db.load_block_prev2(id)
    }

    async fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db.load_block_next1(id)
    }

    async fn load_block_next2(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.db.load_block_next2(id)
    }

    async fn wait_state(
        self: Arc<Self>,
        id: &BlockIdExt,
        _timeout_ms: Option<u64>,
        _allow_block_downloading: bool
    ) -> Result<Arc<ShardStateStuff>> {
        self.load_state(id).await
    }

    async fn find_mc_block_by_seq_no(
        &self,
        seqno: u32
    ) -> Result<Arc<BlockHandle>> {
        self.db.find_mc_block_by_seq_no(seqno)
    }

    async fn download_and_apply_block(
        self: Arc<Self>, 
        _id: &BlockIdExt, 
        _mc_seq_no: u32, 
        _pre_apply: bool
    ) -> Result<()> {
        Ok(())
    }

    async fn download_block(&self, id: &BlockIdExt, _limit: Option<u32>) -> Result<(BlockStuff, BlockProofStuff)> {
        let handle = self.load_block_handle(id)?.ok_or_else(
            || error!("Cannot load handle for block {}", id)
        )?;
        Ok((
            self.db.load_block_data(&handle).await?,
            self.db.load_block_proof(&handle, !id.shard().is_masterchain()).await?
        ))
    }
    fn new_external_message(&self, id: UInt256, message: Arc<Message>) -> Result<()> {
        self.ext_messages.new_message(id, message, self.now())
    }
    fn get_external_messages(&self, shard: &ShardIdent) -> Result<Vec<(Arc<Message>, UInt256)>> {
        self.ext_messages.get_messages(shard, self.now())
    }
    fn complete_external_messages(&self, to_delay: Vec<UInt256>, to_delete: Vec<UInt256>) -> Result<()> {
        self.ext_messages.complete_messages(to_delay, to_delete, self.now())
    }
    // return empty shard blocks - collator will get it from previous state
    fn get_shard_blocks(&self, _mc_seq_no: u32) -> Result<Vec<Arc<TopBlockDescrStuff>>> {
        Ok(Vec::new())
    }

    #[cfg(feature = "telemetry")]
    fn engine_telemetry(&self) -> &Arc<EngineTelemetry> {
        &self.engine_telemetry
    }

    fn engine_allocated(&self) -> &Arc<EngineAlloc> {
        &self.engine_allocated
    }

    #[cfg(feature = "telemetry")]
    fn collator_telemetry(&self) -> &CollatorValidatorTelemetry {
        &self.collator_telemetry
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .arg(Arg::with_name("PATH")
            .short("p")
            .long("path")
            .help("path to DB")
            .takes_value(true)
            .required(true)
            .default_value("node_db")
            .number_of_values(1))
        .arg(Arg::with_name("SEQ NO")
            .short("s")
            .long("seqno")
            .help("seq no of masterchain block")
            .takes_value(true)
            .number_of_values(1))
        .arg(Arg::with_name("LAST")
            .short("l")
            .long("last")
            .help("last seq no of masterchain block"))
        .get_matches();

    let db_dir = args.value_of("PATH").expect("path to database must be set");

    let engine = Arc::new(TestEngine::new(db_dir).await
        .map_err(|err| error!("cannot create engine: {}", err))?);

    let mc_state = engine.load_last_applied_mc_state().await?;
    let mc_block_id = mc_state.block_id();
    if args.is_present("LAST") {
        println!("last applied msaterchain block id is {}", mc_block_id)
    }
    if let Some(mc_seq_no) = args.value_of("SEQ NO") {
        let mc_seq_no = u32::from_str(mc_seq_no).expect("masterchain seq no must be u32");
        let block_id = mc_state.find_block_id(mc_seq_no)?;
        let prev_block_id = engine.load_block_prev1(&block_id)?;
        let collator = Collator::new(
            ShardIdent::masterchain(),
            prev_block_id.clone(),
            vec!(prev_block_id),
            ValidatorSet::default(),
            UInt256::default(),
            engine,
            None,
            CollatorSettings::fake()
        )?;
        let (block, _state) = collator.collate(10000).await.unwrap();
        // _state.read_custom().unwrap().unwrap().shards().dump("test");
        let file_name = format!("configs/{:x}.boc", block.block_id.root_hash);
        std::fs::write(&file_name, &block.data).unwrap();
        let id = serde_json::json!({
            "hardforks": [{
                "workchain": -1,
                "shard": -9223372036854775808i64,
                "seqno": block.block_id.seq_no,
                "root_hash": base64::encode(block.block_id.root_hash.as_slice()),
                "file_hash": base64::encode(block.block_id.file_hash.as_slice())
            }]
        });
        println!("{:#}", id);
    }
    Ok(())
}
