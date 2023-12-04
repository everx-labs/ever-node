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
use catchain::{CatchainFactory, CatchainNode, ReceiverPtr};
use std::{path::Path, rc::Rc};

include!("../../storage/src/db/tests/destroy_db.rs");

const DB_PATH: &str = "../target/test";

#[tokio::test]
async fn test_receiver() {

    let session_id =
        parse_hex_as_int256("53f5a70f995c2f5ea6c0bb055153fd012a01aabb9b041ad3164adfaf19ccb554");
    let source_key_hash = parse_hex_as_public_key_hash(
        "0000000000000000000000000000000000000000000000000000000000000000",
    );

    //add 11 fake sources (based on test data)
    let source_ids = [
        "9E17F4CBF539DFE58C8AC3322155A15ED67B89610F354CC10BB146103481E015",
        "DB4F391AFD4E27518BC9D2A4F712922E95C180AE3D83662950CFCDE410EC6936",
        "316901A6EFE2D8848A09006037E4B6054BCF29B805A1C16EDF8DDD9EADE0C2B8",
        "55979C4B04D123EDBFDA9592CEDD73BD99940F54F5EABDAE513615D909BB000B",
        "FE75D4583509FAB5A5A0D951655114C443FDB881FD1A684BC508A064F4DC299B",
        "B3A7FF7A415AAD986D43D8BD5FCA00CB71E8D9523EB5D34CB1BDA77090DC8483",
        "B6071C8C7A39C7D4980FA2260B12A34D6DDFAEB6F6EF349FA4C143EF4247EE56",
        "FBE516CF1B8DC51C9256A55A503CBEF5E182563AE19FAE66FDDD63DB0D69E20D",
        "AABC1428696BBA5DDE640EAD0D50691D8AA488C1819E3FC07F3B6985807ADBA9",
        "25FACB3CEB576F2AF3B48394944A681582F34DCDFC5BBED51EB1800D7D4E0D5D",
        "3893D11C7C5CCBC24501EDD28A44EB0C6E619F967DDEEB610DAA2FBE82CC33AF",
    ];
    let mut sources: Vec<CatchainNode> = Vec::new();
    let mut receivers: Vec<ReceiverPtr> = Vec::new();

    for i in 0..11 {
        let public_key = parse_hex_as_public_key_raw(&source_ids[i]);
        let public_key_hash = get_public_key_hash(&public_key);

        print!("{:?}\n", public_key_hash);

        sources.push(CatchainNode {
            public_key: public_key.clone(),
            adnl_id: public_key_hash,
        });
    }

    const DB_NAME: &str = "catchains_test_receiver";
    let db_path = Path::new(DB_PATH).join(DB_NAME).display().to_string();
    let db_suffix = "".to_string();
    let allow_unsafe_self_blocks_resync = false;
    let receiver_listener = CatchainFactory::create_dummy_receiver_listener();
    let receiver = CatchainFactory::create_receiver(
        Rc::downgrade(&receiver_listener),
        &session_id,
        &sources,
        &sources[0].public_key,
        db_path,
        db_suffix,
        allow_unsafe_self_blocks_resync,
        None,
    ).unwrap();
    receivers.push(receiver);

    let message = parse_hex("c458672353f5a70f995c2f5ea6c0bb055153fd012a01aabb9b041ad3164adfaf19ccb55401000000010000000b0000000000000053f5a70f995c2f5ea6c0bb055153fd012a01aabb9b041ad3164adfaf19ccb554000000000000000040394071ef465593e0ee7fc80d4573d222dddcf4b58b42ab864624e94963e9dece34a56d3f7c7f8cf950f1b819aac0c0c9ff1b0b38182da49bfe6d9f4c2b253b08000000eaa010ed3710e6d6eaa010ed3710e6d6");

    match receivers[0]
        .borrow_mut()
        .receive_message_from_overlay(&source_key_hash, &mut message.as_ref())
    {
        Ok(block) => println!("Block is received: {}", block.borrow().to_string()),
        Err(err) => println!("Block receiving error: {}", err),
    };

    receivers[0].borrow_mut().destroy_db();
    drop(receivers);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap()

}
