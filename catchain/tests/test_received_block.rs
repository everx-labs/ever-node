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

use catchain::CatchainFactory;
use std::path::Path;

include!("../../storage/src/db/tests/destroy_db.rs");

const DB_PATH: &str = "../target/test";

#[tokio::test]
async fn test_received_block() {

    const DB_NAME: &str = "catchains_test_received_block";
    let db_path = Path::new(DB_PATH).join(DB_NAME).display().to_string();
    let receiver = CatchainFactory::create_dummy_receiver(db_path).unwrap();
    let x = receiver.borrow().create_block_from_string_dump (&"
        payload.size = 16
        payload = e2567d17f4ef37dce2567d17f4ef37dc
        fork_id = 0
        source_id = 7
        hash = 7070AD153095141EF18C18A49D8B7E7BCC49FC6FF87DD0185256EA23F5FA64C7
        data_hash = 483054FFC47954A12CA57D623ABD6BBB6C8F02B3754CF74219EB1D30783F82E8
        height = 1
        signature.size = 64
        signature = 1a512676c343e486bb65003b9cfa91cbadf0582bd5e8e1af5da6bf5168b463c7b5dadd447f06f90a4415a51a30e62b2a5fa46c7d613b1c95caae345f984ce70c
    ".to_string());

    assert_eq!(x.borrow().get_height(), 1);
    assert_eq!(x.borrow().get_fork_id(), 0);
    assert_eq!(x.borrow().get_source_id(), 7);
    assert_eq!(
        format!("{}", x.borrow().get_hash().to_hex_string()),
        "7070ad153095141ef18c18a49d8b7e7bcc49fc6ff87dd0185256ea23f5fa64c7"
    );

    receiver.borrow_mut().destroy_db();
    drop(receiver);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap()

}

#[tokio::test]
async fn test_received_multiple_blocks() {

    const DB_NAME: &str = "catchains_test_received_multiple_blocks";
    let db_path = Path::new(DB_PATH).join(DB_NAME).display().to_string();
    let receiver = CatchainFactory::create_dummy_receiver(db_path).unwrap();
    let x = receiver.borrow().create_block_from_string_dump (&"
        payload.size = 16
        payload = e2567d17f4ef37dce2567d17f4ef37dc
        fork_id = 0
        source_id = 7
        hash = 7070AD153095141EF18C18A49D8B7E7BCC49FC6FF87DD0185256EA23F5FA64C7
        data_hash = 483054FFC47954A12CA57D623ABD6BBB6C8F02B3754CF74219EB1D30783F82E8
        height = 1
        signature.size = 64
        signature = 1a512676c343e486bb65003b9cfa91cbadf0582bd5e8e1af5da6bf5168b463c7b5dadd447f06f90a4415a51a30e62b2a5fa46c7d613b1c95caae345f984ce70c
    ".to_string());

    let y = receiver.borrow().create_block_from_string_dump (&"
        payload.size = 16
        payload = c80e2e35477b28c0c80e2e35477b28c0
        fork_id = 0
        source_id = 1
        hash = 8AAA9211269AA1061902DB197BA9577E96A1DC50BDF7572B2A1A5765337B3CC7
        data_hash = CD5C5AB8630FF43813F8421CDFF129CDB7439A29F8FC0B0FBC69D7D62D34B2D8
        height = 1
        signature.size = 64
        signature = 42947640891298f566a633ad56b5e700d649540e371825118dee1f10879e6a9b1b364b514c5a96aef9dbc62b6b42df5b8b4875f1443017428f764a0a5dfccf02
    ".to_string());

    assert_eq!(x.borrow().get_height(), 1);
    assert_eq!(x.borrow().get_fork_id(), 0);
    assert_eq!(x.borrow().get_source_id(), 7);
    assert_eq!(
        format!("{}", x.borrow().get_hash().to_hex_string()),
        "7070ad153095141ef18c18a49d8b7e7bcc49fc6ff87dd0185256ea23f5fa64c7"
    );

    println!("y: {}", y.borrow().to_string());
    println!("{}", receiver.borrow().to_string());
    receiver.borrow_mut().destroy_db();
    drop(receiver);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap()

}
