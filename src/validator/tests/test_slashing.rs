/*
* Copyright (C) 2019-2023 TON Labs. All Rights Reserved.
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

use super::*;
use ever_crypto::KeyOption;

fn key_from_str(key: &str) -> Arc<dyn KeyOption> {
    Ed25519KeyOption::from_public_key(key.parse::<UInt256>().unwrap().as_slice())
    // Ed25519KeyOption::from_private_key(key.parse::<UInt256>().unwrap().as_slice()).unwrap()
}

fn save_slash_validator_message(reporter: &str, victim: &str, file_name: &str) {
    let reporter = key_from_str(reporter);
    let victim = key_from_str(victim);
    let metric_id = SlashingAggregatedMetric::ValidationScore;
    let time_now_ms = 16590899030001; // 1658862426000;

    let message = SlashingManager::prepare_slash_validator_message(
        ELECTOR_ADDRESS.clone(),
        &reporter,
        &victim,
        metric_id,
        time_now_ms
    ).unwrap().unwrap();

    message.write_to_file(file_name).unwrap();
}

#[test]
fn test_prepare_slash_validator_message() {
    // let reporter_privkey = key_from_str("fd9dea47d76da90db2853d7ac1d3f5807fc737740bd10ac90a302e59decc305f");
    // let validator_pubkey = key_from_str("2cae5b04e05a879a8ace1e503d65a9d6e46ef1f1a47ed06c067736f27bd208ac");

    let victim = "0x0e1b4f5827ac7c54bb7617ba54310cf12016151969b9af250abe6353bdd8f869";
    let reporters = [
        "0x4529bfebbcc05f3777443547c4e7bf8d17adaa0d5cda907c49f1bb3ec301a500",
        "0xb28c9ae6d4b6c89f3bb6c475de733172701b5ad1c99b811112cd8e8ba9ac0402",
        "0xccc7b7ff86157b4d18a513ec24e6d7d1cf4a0825adc58a70ba357b7693cd347d",
        "0xe1d5fb5a7817771d791482014ebf86e74c73389d0c468987198b4104daf5f46a",
        "0xe833f4344dd1d2ff68ff88709269f94e5c4e30c4da8ba9cf10689eb4812f49ad",
    ];

    for (i, reporter) in reporters.iter().enumerate() {
        let file_name = format!("target/elector_msg_{}.boc", i + 1);
        save_slash_validator_message(reporter, victim, &file_name);
    }
}
