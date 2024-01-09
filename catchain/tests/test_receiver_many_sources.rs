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
use catchain::{CatchainFactory, CatchainNode};
use chrono::Local;
use env_logger::Builder;
use log::{error, info, LevelFilter};
use std::{io::Write, path::Path, rc::Rc};

include!("../../storage/src/db/tests/destroy_db.rs");

const DB_PATH: &str = "../target/test";

#[tokio::test]
async fn test_receiver_many_sources() {

    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter(None, LevelFilter::Debug)
        .init();

    let session_id =
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

    const DB_NAME: &str = "catchains_test_receiver_many_sources";
    let db_path = Path::new(DB_PATH).join(DB_NAME).display().to_string();
    let receiver_listener = CatchainFactory::create_dummy_receiver_listener();
    let db_suffix = String::new();
    let allow_unsafe_self_blocks_resync = false;
    let receiver = CatchainFactory::create_receiver(
        Rc::downgrade(&receiver_listener),
        &session_id,
        &source_ids,
        &source_ids[0].public_key,
        db_path,
        db_suffix,
        allow_unsafe_self_blocks_resync,
        None,
    ).unwrap();
    let messages = "
1579261805 data = c4586723d7324489ec0a02389ee95153ea11fd57a5ead6cd81ce6ad2ca4c3a6256e0c5ca03000000010000000b00000000000000d7324489ec0a02389ee95153ea11fd57a5ead6cd81ce6ad2ca4c3a6256e0c5ca000000000000000040876a5f9270c1d75ef7bbc2f64f0cdf487c7d42a0b0da62cbcac937ddaf8a818a1d38348db1171a2d435269cc5d3f240be13dba246ccfc6ac0f600962f93acb0b00000072005e9cde7b7c0f72005e9cde7b7c0f
1579261805 data = c4586723d7324489ec0a02389ee95153ea11fd57a5ead6cd81ce6ad2ca4c3a6256e0c5ca02000000010000000b00000000000000d7324489ec0a02389ee95153ea11fd57a5ead6cd81ce6ad2ca4c3a6256e0c5ca000000000000000040b20e325dc3ef531c743d390c4aa64a3efb4e01346c04731b15d5eb3340525112bee963aaf983e7ea2e549ad526d5ff90b90e95c4c6c0d885831800317de06d03000000c1b19ff71c8a52b5c1b19ff71c8a52b5
1579261805 data = c4586723d7324489ec0a02389ee95153ea11fd57a5ead6cd81ce6ad2ca4c3a6256e0c5ca05000000010000000b00000000000000d7324489ec0a02389ee95153ea11fd57a5ead6cd81ce6ad2ca4c3a6256e0c5ca00000000000000004056f62a00a4af08fee9e18a46abf4135f4b430afa5edef6d09352a1a7618541a4079e5574848942b47807b66f447c0028bf82202087abb4f61875e10c1ae4310a000000b60d52334e6270d9b60d52334e6270d9
1579261805 data = c4586723d7324489ec0a02389ee95153ea11fd57a5ead6cd81ce6ad2ca4c3a6256e0c5ca01000000010000000b00000000000000d7324489ec0a02389ee95153ea11fd57a5ead6cd81ce6ad2ca4c3a6256e0c5ca0000000000000000407dfd32567a30674e6d50a6439aa061c845b01f68b44b929597b9461d40ced3be983430b6370a5c0f101e2917d1845c36ec809879982de029afe0ead80ef3d807000000b8f93ff61a220064b8f93ff61a220064
";

    let dummy_key_hash = receiver.borrow().get_source_public_key_hash(0).clone();
    for msgs in messages.lines() {
        match msgs.split(" = ").nth(1) {
            Some(hex) => {
                let message = parse_hex(hex);

                match receiver
                    .borrow_mut()
                    .receive_message_from_overlay(&dummy_key_hash, &mut message.as_ref())
                {
                    Ok(block) => info!("Block is received: {}", block.borrow()),
                    Err(err) => error!("Block receiving error: {}", err),
                };
            }
            None => (),
        }
    }

    println!("{}", receiver.borrow().to_string());

    receiver.borrow_mut().process();
    receiver.borrow_mut().destroy_db();
    drop(receiver);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap()

}
