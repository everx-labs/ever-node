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

use super::*;
use ton_block::ShardIdent;
use ton_types::read_single_root_boc;

#[test]
fn test_block_stuff_deserialize() {
    // block	(-1,8000000000000000,2429446)
    // roothash	A3A94C6D84B310D35A15A8ACC731EF3E04661C871200C2488952AC892A5543E8
    // filehash	F2BC2888EAE466FC172140A90949EDBC7091D81CCB4194A225C56C0F1D095097

    let name = "src/tests/static/F2BC2888EAE466FC172140A90949EDBC7091D81CCB4194A225C56C0F1D095097.boc";
    let bs = BlockStuff::read_block_from_file(name).unwrap();
    let id = BlockIdExt {
        shard_id: ShardIdent::masterchain(),
        seq_no: 2429446,
        root_hash: "A3A94C6D84B310D35A15A8ACC731EF3E04661C871200C2488952AC892A5543E8".parse().unwrap(),
        file_hash: "F2BC2888EAE466FC172140A90949EDBC7091D81CCB4194A225C56C0F1D095097".parse().unwrap(),
    };
    assert_eq!(bs.id(), &id);
}

#[test]
fn test_construct_and_check_prev_stuff_master() {
    let proof_data = std::fs::read("src/tests/static/test_master_block_proof_shuffle/proof__3236531").unwrap();
    let proof_root = read_single_root_boc(&proof_data).unwrap();
    let proof = ton_block::BlockProof::construct_from_cell(proof_root.clone()).unwrap();
    let merkle_proof = ton_block::MerkleProof::construct_from_cell(proof.root.clone()).unwrap();
    let block_virt_root = merkle_proof.proof.clone().virtualize(1);

    let (id, stuff) = construct_and_check_prev_stuff(
        &block_virt_root,
        &proof.proof_for,
        true
    ).unwrap();

    let virt_block = ton_block::Block::construct_from_cell(block_virt_root.clone()).unwrap();
    let virt_block_info = virt_block.read_info().unwrap();
    let prev = virt_block_info.read_prev_ids().unwrap();
    let prev = &prev[0];

    let mc_block_id = BlockIdExt {
        shard_id: virt_block_info.shard().clone(),
        seq_no: prev.seq_no,
        root_hash: prev.root_hash.clone(),
        file_hash: prev.file_hash.clone()
    };

    let mut id2 = proof.proof_for.clone();
    id2.file_hash = UInt256::default();

    assert_eq!(id, id2);
    assert_eq!(stuff.mc_block_id, mc_block_id);
    assert_eq!(stuff.prev[0], mc_block_id);
    assert_eq!(stuff.prev.len(), 1);
    assert_eq!(stuff.after_split, false);
}

#[test]
fn test_construct_and_check_prev_stuff_shard() {
    let proof_data = std::fs::read("src/tests/static/test_shard_block_proof/proof_4377262").unwrap();
    let proof_root = read_single_root_boc(&proof_data).unwrap();
    let proof = ton_block::BlockProof::construct_from_cell(proof_root.clone()).unwrap();
    let merkle_proof = ton_block::MerkleProof::construct_from_cell(proof.root.clone()).unwrap();
    let block_virt_root = merkle_proof.proof.clone().virtualize(1);

    let (id, stuff) = construct_and_check_prev_stuff(
        &block_virt_root,
        &proof.proof_for,
        true
    ).unwrap();

    let virt_block = ton_block::Block::construct_from_cell(block_virt_root.clone()).unwrap();
    let virt_block_info = virt_block.read_info().unwrap();

    let prev = virt_block_info.read_prev_ids().unwrap();
    let prev = &prev[0];
    let prev_block_id = ton_block::BlockIdExt {
        shard_id: virt_block_info.shard().clone(),
        seq_no: prev.seq_no,
        root_hash: prev.root_hash.clone(),
        file_hash: prev.file_hash.clone()
    };

    let mc_block_id = virt_block_info.read_master_ref().unwrap().unwrap().master;
    let mc_block_id = BlockIdExt {
        shard_id: ton_block::ShardIdent::masterchain(),
        seq_no: mc_block_id.seq_no,
        root_hash: mc_block_id.root_hash.clone(),
        file_hash: mc_block_id.file_hash.clone()
    };

    let mut id2 = proof.proof_for.clone();
    id2.file_hash = UInt256::default();

    assert_eq!(id, id2);
    assert_eq!(stuff.mc_block_id, mc_block_id);
    assert_eq!(stuff.prev[0], prev_block_id);
    assert_eq!(stuff.prev.len(), 1);
    assert_eq!(stuff.after_split, false);
}