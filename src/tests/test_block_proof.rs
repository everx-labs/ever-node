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

use crate::{block::BlockStuff, block_proof::BlockProofStuff};

#[test]
fn test_check_master_blocks_proof() {

    let name = "src/tests/static/test_master_block_proof/key_block__3082181";
    let key_block = BlockStuff::read_block_from_file(name).unwrap();

    let bytes = std::fs::read("src/tests/static/test_master_block_proof/key_proof__3082181").unwrap();
    let key_block_proof = BlockProofStuff::deserialize(key_block.id(), bytes, false).unwrap();

    for seqno in 3082182..=3082200 {
        let name = format!("src/tests/static/test_master_block_proof/block__{}", seqno);
        let block = BlockStuff::read_block_from_file(&name).unwrap();

        let name = format!("src/tests/static/test_master_block_proof/proof__{}", seqno);
        let bytes = std::fs::read(&name).unwrap();
        let block_proof = BlockProofStuff::deserialize(block.id(), bytes, false).unwrap();

        block_proof.check_with_prev_key_block_proof(&key_block_proof).unwrap(); 
    }
}

#[test]
fn test_check_master_blocks_proof_shuffle() {

    let name = "src/tests/static/test_master_block_proof_shuffle/key_block__3236530";
    let key_block = BlockStuff::read_block_from_file(name).unwrap();

    let bytes = std::fs::read("src/tests/static/test_master_block_proof_shuffle/key_proof__3236530").unwrap();
    let key_block_proof = BlockProofStuff::deserialize(key_block.id(), bytes, false).unwrap();

    for seqno in 3236531..=3236550 {
        let name = format!("src/tests/static/test_master_block_proof_shuffle/block__{}", seqno);
        let block = BlockStuff::read_block_from_file(&name).unwrap();

        let bytes = std::fs::read(
            &format!("src/tests/static/test_master_block_proof_shuffle/proof__{}", seqno)
        ).unwrap();
        let block_proof = BlockProofStuff::deserialize(block.id(), bytes, false).unwrap();

        block_proof.check_with_prev_key_block_proof(&key_block_proof).unwrap(); 
    }
}
