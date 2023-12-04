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

use catchain::*;
use std::path::Path;

include!("../../storage/src/db/tests/destroy_db.rs");

const DB_PATH: &str = "../target/test";

#[tokio::test]
async fn log_replay_multiple_players() {

    const DB_NAME: &str = "catchains_log_replay_multiple_players";

    let db_path = Path::new(DB_PATH).join(DB_NAME).display().to_string();
    let mut replay_opts = LogReplayOptions::with_db(db_path);
    replay_opts.log_file_name = "tests/log_header.log".to_string();
    let log_players = CatchainFactory::create_log_players(&replay_opts);

    for log_player in log_players {
        println!(
            "Session {} with {} nodes and local_id {} has been detected",
            log_player.get_session_id().to_hex_string(),
            log_player.get_nodes().len(),
            log_player.get_local_id()
        );
    }

    drop(replay_opts);
    destroy_rocks_db(DB_PATH, DB_NAME).await.unwrap()

}
