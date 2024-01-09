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

use catchain::utils::*;
use catchain::{
    BlockPtr, BlockPayloadPtr, CatchainFactory, CatchainListener, CatchainNode, 
    ExternalQueryResponseCallback, Options, PublicKeyHash, SessionId
};
use chrono::Local;
use env_logger::Builder;
use log::{info, LevelFilter};
use std::{io::Write, path::Path, sync::Arc};

include!("../../storage/src/db/tests/destroy_db.rs");

const DB_PATH: &str = "../target/test";

struct DummyCatchainListener {}

impl CatchainListener for DummyCatchainListener {
    fn preprocess_block(&self, block: BlockPtr) {
        info!("DummyCatchainListener::preprocess_block: {:?}", block);
    }

    fn process_blocks(&self, blocks: Vec<BlockPtr>) {
        info!("DummyCatchainListener::process_blocks: {:?}", blocks);
    }

    fn finished_processing(&self) {
        info!("DummyCatchainListener::finished_processing");
    }

    fn started(&self) {
        info!("DummyCatchainListener::started");
    }

    /// Notify about incoming broadcasts
    fn process_broadcast(&self, _source_id: PublicKeyHash, _data: BlockPayloadPtr) {}

    /// Notify about incoming query
    fn process_query(
        &self,
        _source_id: PublicKeyHash,
        _data: BlockPayloadPtr,
        _callback: ExternalQueryResponseCallback,
    ) {
    }

    fn set_time(&self, _timestamp: std::time::SystemTime) {}
}

impl DummyCatchainListener {
    fn new() -> Arc<dyn CatchainListener + Send + Sync> {
        Arc::new(DummyCatchainListener {})
    }
}

#[tokio::test]
async fn test_catchain_creation() {
  
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} {}:{} [{}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.file().unwrap(),
                record.line().unwrap(),
                record.level(),
                record.args()
            )
        })
        .filter(None, LevelFilter::Debug)
        .init();

    const DB_NAME: &str = "catchains_test_catchain_creation";
    let db_path = Path::new(DB_PATH).join(DB_NAME).display().to_string();
    let _session_id =
        parse_hex_as_int256("d7324489ec0a02389ee95153ea11fd57a5ead6cd81ce6ad2ca4c3a6256e0c5ca");
    let sources = "
1579261803 Node hash = 0: 9E17F4CBF539DFE58C8AC3322155A15ED67B89610F354CC10BB146103481E015
1579261803 Node hash = 1: DB4F391AFD4E27518BC9D2A4F712922E95C180AE3D83662950CFCDE410EC6936
1579261803 Node hash = 2: 316901A6EFE2D8848A09006037E4B6054BCF29B805A1C16EDF8DDD9EADE0C2B8
1579261803 Node hash = 3: 55979C4B04D123EDBFDA9592CEDD73BD99940F54F5EABDAE513615D909BB000B
1579261803 Node hash = 4: FE75D4583509FAB5A5A0D951655114C443FDB881FD1A684BC508A064F4DC299B
1579261803 Node hash = 5: B3A7FF7A415AAD986D43D8BD5FCA00CB71E8D9523EB5D34CB1BDA77090DC8483
1579261803 Node hash = 6: B6071C8C7A39C7D4980FA2260B12A34D6DDFAEB6F6EF349FA4C143EF4247EE56
1579261803 Node hash = 7: FBE516CF1B8DC51C9256A55A503CBEF5E182563AE19FAE66FDDD63DB0D69E20D
1579261803 Node hash = 8: AABC1428696BBA5DDE640EAD0D50691D8AA488C1819E3FC07F3B6985807ADBA9
1579261803 Node hash = 9: 25FACB3CEB576F2AF3B48394944A681582F34DCDFC5BBED51EB1800D7D4E0D5D
1579261803 Node hash = 10: 3893D11C7C5CCBC24501EDD28A44EB0C6E619F967DDEEB610DAA2FBE82CC33AF
";

    let mut source_ids: Vec<CatchainNode> = Vec::new();

    for (_i, src) in sources.lines().enumerate() {
        match src.split(": ").nth(1) {
            Some(hex) => {
                let public_key = parse_hex_as_public_key_raw(hex);
                let adnl_id = get_public_key_hash(&public_key);

                source_ids.push(CatchainNode {
                    public_key: public_key,
                    adnl_id: adnl_id,
                });
            }
            None => (),
        }
    }
    let local_key = &source_ids[0].public_key;

    let catchain_listener = DummyCatchainListener::new();
    let options = Options::default();
    let session_id = SessionId::default();
    let db_suffix = String::new();
    let allow_unsafe_self_blocks_resync = false;
    let catchain = CatchainFactory::create_catchain(
        &options,
        &session_id,
        &source_ids,
        &local_key,
        db_path,
        db_suffix,
        allow_unsafe_self_blocks_resync,
        CatchainFactory::create_dummy_overlay_manager(),
        Arc::downgrade(&catchain_listener),
    );

    print!("Finished! Now exiting...\n");
    catchain.stop(true);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap()
 
}
