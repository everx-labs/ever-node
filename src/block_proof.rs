/*
* Copyright (C) 2019-2022 TON Labs. All Rights Reserved.
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

use std::sync::Arc;

use ton_block::{Block, BlockIdExt, BlockInfo, BlockProof, ConfigParams, Deserializable, MerkleProof, Serializable};
use ton_types::{Cell, Result, fail, error, HashmapType, BocReader, write_boc};

use crate::{
    block::{BlockIdExtExtention, BlockStuff},
    error::NodeError,
    shard_state::ShardStateStuff,
    engine_traits::EngineOperations,
    validator::validator_utils::{
        check_crypto_signatures, calc_subset_for_masterchain, ValidatorSubsetInfo
    },
};

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct BlockProofStuff {
    proof: BlockProof,
    root: Cell,
    is_link: bool,
    id: BlockIdExt,
    data: Arc<Vec<u8>>,
}

impl BlockProofStuff {
    pub fn deserialize(block_id: &BlockIdExt, data: Vec<u8>, is_link: bool) -> Result<Self> {
        let data = Arc::new(data);
        let root = BocReader::new().read_inmem(data.clone())?.withdraw_single_root()?;
        let proof = BlockProof::construct_from_cell(root.clone())?;
        if &proof.proof_for != block_id {
            fail!(
                NodeError::InvalidData(format!("proof for another block (found: {}, expected: {})", proof.proof_for, block_id))
            )
        }
        if !block_id.is_masterchain() && !is_link {
            fail!(
                NodeError::InvalidData(format!("proof for non-masterchain block {}", block_id))
            )
        }
        Ok(BlockProofStuff { proof, root, is_link, id: block_id.clone(), data })
    }

    pub fn new(proof: BlockProof, is_link: bool) -> Result<Self> {
        let id = proof.proof_for.clone();
        if !id.is_masterchain() && !is_link {
            fail!(
                NodeError::InvalidData(format!("proof for non-masterchain block {}", id))
            )
        }
        let cell = proof.serialize()?.into();
        let data = Arc::new(write_boc(&cell)?);
        Ok(Self {
            root: proof.serialize()?.into(),
            proof,
            is_link,
            id,
            data,
        })
    }

// Unused
//    pub fn root(&self) -> &Cell {
//        &self.root
//    }

    pub fn proof_root(&self) -> &Cell {
        &self.proof.root
    }

    pub fn virtualize_block(&self) -> Result<(Block, Cell)> {
        let merkle_proof = MerkleProof::construct_from_cell(self.proof.root.clone())?;
        let block_virt_root = merkle_proof.proof.clone().virtualize(1);
        if *self.proof.proof_for.root_hash() != block_virt_root.repr_hash() {
            fail!(NodeError::InvalidData(format!(
                "merkle proof has invalid virtual hash (found: {}, expected: {})",
                block_virt_root.repr_hash(),
                self.proof.proof_for
            )))
        }
        if block_virt_root.repr_hash() != self.id().root_hash {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with incorrect root hash: expected {:x}, found: {:x} ",
                self.id(),
                self.id().root_hash,
                block_virt_root.repr_hash()
            )))
        }
        Ok((Block::construct_from_cell(block_virt_root.clone())?, block_virt_root))
    }

    pub fn get_config_params(&self) -> Result<ConfigParams> {
        let (virt_block, _) = self.virtualize_block()?;
        self.read_config_params(&virt_block)
    }

    pub fn is_link(&self) -> bool {
        self.is_link
    }

    pub fn id(&self) -> &BlockIdExt {
        &self.id
    }

    #[cfg(feature="external_db")]
    pub fn proof(&self) -> &BlockProof {
        &self.proof
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn drain_data(self) -> Vec<u8> { 
        drop(self.proof);
        drop(self.root);
        debug_assert_eq!(Arc::strong_count(&self.data), 1);
        Arc::try_unwrap(self.data).unwrap_or_else(|s| (*s).clone())
    }

    pub fn check_with_prev_key_block_proof(&self, prev_key_block_proof: &BlockProofStuff) -> Result<()> {
        let now = std::time::Instant::now();
        log::trace!("Checking proof for block: {}", self.id());

        let (virt_block, virt_block_info) = self.pre_check_block_proof()?;

        self.check_with_prev_key_block_proof_(prev_key_block_proof, &virt_block, &virt_block_info)?;

        log::trace!("Checked proof for block: {}   TIME {}ms", self.id(), now.elapsed().as_millis());
        Ok(())
    }

    pub fn check_with_master_state(&self, master_state: &ShardStateStuff) -> Result<()> {
        let now = std::time::Instant::now();
        log::trace!("Checking proof for block: {}", self.id());

        if self.is_link {
            fail!(NodeError::InvalidOperation(format!(
                "Can't verify block {}: can't call `check_with_master_state` for proof link", self.id()
            )))
        }

        let (virt_block, virt_block_info) = self.pre_check_block_proof()?;

        self.check_with_master_state_(master_state, &virt_block, &virt_block_info)?;

        log::trace!("Checked proof for block: {}   TIME {}ms", self.id(), now.elapsed().as_millis());
        Ok(())
    }

    pub fn check_proof_as_link(&self) -> Result<()> {
        let now = std::time::Instant::now();
        log::trace!("Checking proof for block: {}", self.id());

        if self.is_link {
            fail!(NodeError::InvalidOperation(format!(
                "Can't call `check_proof_as_link` not for proof, block {}", self.id()
            )))
        }
        self.pre_check_block_proof()?;
        log::trace!("Checked proof as link for block: {}   TIME {}ms", self.id(), now.elapsed().as_millis());
        Ok(())
    }

    pub fn check_proof_link(&self) -> Result<()> {
        let now = std::time::Instant::now();
        log::trace!("Checking proof for block: {}", self.id());

        if !self.is_link {
            fail!(NodeError::InvalidOperation(format!(
                "Can't call `check_proof_link` not for proof link, block {}", self.id()
            )))
        }
        self.pre_check_block_proof()?;
        log::trace!("Checked proof link for block: {}   TIME {}ms", self.id(), now.elapsed().as_millis());
        Ok(())
    }

    pub async fn check_proof(&self, engine: &dyn EngineOperations) -> Result<()> {
        if self.is_link() {
            self.check_proof_link()?;
        } else {
            let now = std::time::Instant::now();
            log::trace!("Checking proof for block: {}", self.id());

            let (virt_block, virt_block_info) = self.pre_check_block_proof()?;
            let prev_key_block_seqno = virt_block_info.prev_key_block_seqno();

            if prev_key_block_seqno == 0 {
                let zerostate = engine.load_mc_zero_state().await?;
                self.check_with_master_state_(&zerostate, &virt_block, &virt_block_info)?;
            } else {
                let handle = engine.find_mc_block_by_seq_no(prev_key_block_seqno).await
                    .map_err(|err|
                        error!(
                            "Couldn't find previous MC key block by seq_no = {}: {}",
                            prev_key_block_seqno,
                            err
                        )
                    )?;
                let prev_key_block_proof = engine.load_block_proof(&handle, false).await?;
                self.check_with_prev_key_block_proof_(
                    &prev_key_block_proof, 
                    &virt_block, 
                    &virt_block_info
                )?;
            }

            log::trace!("Checked proof for block: {}   TIME {}ms", self.id(), now.elapsed().as_millis());
        }
        Ok(())
    }

    pub fn check_queue_update(queue_update: &BlockStuff) -> Result<()> {
        let id = queue_update.id();
        let wc = queue_update.is_queue_update_for()
            .ok_or_else(|| error!("Block {} is not a queue update", id))?;

        let merkle_proof = MerkleProof::construct_from_cell(queue_update.root_cell().clone())?;

        Self::pre_check_virtual_block(
            id,
            queue_update.block_or_queue_update()?,
            &merkle_proof.proof.virtualize(1)
        )?;

        // get root cell of queue update and check it has zero level
        let merkle_update_root = queue_update
            .block_or_queue_update()?
            .out_msg_queue_updates.as_ref()
            .ok_or_else(|| error!("Queue update {} doesn't contain out_msg_queue_updates", id))?
            .get_as_slice(&wc)?
            .ok_or_else(|| error!("Queue update {} doesn't contain out msg queue update for wc {}", id, wc))?
            .cell()
            .reference(0)?;
        if merkle_update_root.level() != 0 {
            fail!("Queue update {} for wc {} has root cell with non zero level", id, wc);
        }

        Ok(())
    }

// Unused
//    pub fn get_cur_validators_set(&self) -> Result<(ValidatorSet, CatchainConfig)> {
//        let (virt_key_block, prev_key_block_info) = self.pre_check_block_proof()?;
//        if !prev_key_block_info.key_block() {
//            fail!(NodeError::InvalidData(format!(
//                "proof for key block {} contains a Merkle proof which declares non key block",
//                self.id(),
//            )))
//        }
//        let (cur_validator_set, cc_config) = virt_key_block.read_cur_validator_set_and_cc_conf()
//            .map_err(|err| { 
//                NodeError::InvalidData(format!(
//                    "Сan't extract config params from key block's proof {}: {}",
//                    self.id, err
//                ))
//            })?;
//        Ok((cur_validator_set, cc_config))
//    }

    pub fn check_with_prev_key_block_proof_(
        &self,
        prev_key_block_proof: &BlockProofStuff,
        virt_block: &Block,
        virt_block_info: &BlockInfo
    ) -> Result<()> {

        if !self.id().is_masterchain() {
            fail!(NodeError::InvalidData(format!(
                "Can't verify non masterchain block {} using previous key masterchain block", self.id()
            )))
        }
        if !prev_key_block_proof.id().is_masterchain() {
            fail!(NodeError::InvalidData(format!(
                "Invalid previous key block: it's id {} doesn't belong to the masterchain", prev_key_block_proof.id()
            )))
        }
        let prev_key_block_seqno = virt_block.read_info()?.prev_key_block_seqno();
        if prev_key_block_proof.id().seq_no as u32 != prev_key_block_seqno {
            fail!(NodeError::InvalidData(format!(
                "Can't verify block {} using key block {} because the block declares different previous key block seqno {}",
                self.id(),
                prev_key_block_proof.id(),
                prev_key_block_seqno
            )))
        } 
        if prev_key_block_proof.id().seq_no >= self.id().seq_no {
            fail!(NodeError::InvalidData(format!(
                "Can't verify block {} using key block {} with larger or equal seqno", self.id(), prev_key_block_proof.id()
            )))
        }
        let subset = self.process_prev_key_block_proof(prev_key_block_proof)?;

        if virt_block_info.key_block() {
            self.pre_check_key_block_proof(virt_block)?;
        }

        self.check_signatures(&subset)
    }

    fn check_with_master_state_(&self, master_state: &ShardStateStuff, virt_block: &Block, virt_block_info: &BlockInfo) -> Result<()> {
        if virt_block_info.key_block() {
            self.pre_check_key_block_proof(&virt_block)?;
        }
        let subset = self.process_given_state(master_state, virt_block_info)?;
        self.check_signatures(&subset)
    }

    fn pre_check_block_proof(&self) -> Result<(Block, BlockInfo)> {
        if !self.id().is_masterchain() && self.proof.signatures.is_some() {
            fail!(NodeError::InvalidData(format!(
                "proof for non-master block {} can't contain signatures",
                self.id(),
            )))
        }
        let (virt_block, virt_block_root) = self.virtualize_block()?;
        let info = Self::pre_check_virtual_block(self.id(), &virt_block, &virt_block_root)?;
        Ok((virt_block, info))
    }

    fn pre_check_virtual_block(
        id: &BlockIdExt,
        virt_block: &Block,
        virt_block_root: &Cell
    ) -> Result<BlockInfo> {

        if virt_block_root.repr_hash() != id.root_hash {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with incorrect root hash: expected {:x}, found: {:x} ",
                id,
                id.root_hash,
                virt_block_root.repr_hash()
            )))
        }

        let info = virt_block.read_info()?;
        let value_flow = virt_block.read_value_flow()?;
        value_flow.read_in_full_depth()
            .map_err(|e| error!("Can't read value flow in full depth: {}", e))?;
        let _state_update = virt_block.read_state_update()?;

        if info.version() != 0 {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with incorrect block info's version {}",
                id,
                info.version()
            )))
        }

        if info.seq_no() != id.seq_no {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with seq_no {}, but {} is expected",
                id,
                info.seq_no(),
                id.seq_no
            )))
        }

        if info.shard() != id.shard() {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with shard id {}, but {} is expected",
                id,
                info.shard(),
                id.shard()
            )))
        }

        if info.read_master_ref()?.is_some() != (!info.shard().is_masterchain()) {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with invalid not_master flag in block info",
                id,
            )))
        }

        if id.is_masterchain() && (info.after_merge() || info.before_split() || info.after_split()) {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with a block info which declares split/merge for a masterchain block",
                id,
            )))
        }

        if info.after_merge() && info.after_split() {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with a block info which declares both after merge and after split flags",
                id,
            )))
        }

        if info.after_split() && (info.shard().is_full()) {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with a block info which declares both after_split flag and non zero shard prefix",
                id,
            )))
        }

        if info.after_merge() && !info.shard().can_split() {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with a block info which declares both after_merge flag and shard prefix which can't split anymore",
                id,
            )))
        }

        if info.key_block() && !id.is_masterchain() {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof which declares non master chain but key block",
                id,
            )))
        }

        Ok(info)
    }

    fn read_config_params(&self, virt_block: &Block) -> Result<ConfigParams> {
        let extra = virt_block.read_extra()?;
        let mut mc_extra = extra.read_custom()?
            .ok_or_else(|| NodeError::InvalidData(format!(
                "proof for key block {} contains a Merkle proof without masterchain block extra",
                self.id(),
            )))?;
        let config = mc_extra.config_mut().take()
            .ok_or_else(|| NodeError::InvalidData(format!(
                "proof for key block {} contains a Merkle proof without config params",
                self.id(),
            )))?;
        Ok(config)
    }

    fn pre_check_key_block_proof(&self, virt_block: &Block) -> Result<()> {
        let config = self.read_config_params(virt_block)?;
        let _cur_validator_set = config.config(34)?
            .ok_or_else(|| NodeError::InvalidData(format!(
                "proof for key block {} contains a Merkle proof without current validators config param (34)",
                self.id(),
            )))?;
        for i_config in 32..=38 {
            let _val_set = config.config(i_config)?;
        }
        let _catchain_config = config.config(28)?;
        if let Err(e) = config.workchains() {
            log::trace!("{}", e);
        }

        Ok(())
    }

    fn process_prev_key_block_proof(
        &self,
        prev_key_block_proof: &BlockProofStuff,
    ) -> Result<ValidatorSubsetInfo> {
        let (virt_key_block, prev_key_block_info) = prev_key_block_proof.pre_check_block_proof()?;

        if !prev_key_block_info.key_block() {
            fail!(NodeError::InvalidData(format!(
                "proof for key block {} contains a Merkle proof which declares non key block",
                prev_key_block_proof.id(),
            )))
        }

        let (validator_set, _cc_config) = virt_key_block.read_cur_validator_set_and_cc_conf()
            .map_err(|err| { 
                NodeError::InvalidData(format!(
                    "While checking proof for {}: can't extract config params from key block's proof {}: {}",
                    self.id, prev_key_block_proof.id(), err
                ))
            })?;

        let config = virt_key_block
            .read_extra()?
            .read_custom()?
            .and_then(|custom| custom.config().cloned())
            .ok_or_else(|| error!(NodeError::InvalidArg(
                "State doesn't contain `custom` field".to_string()
            )))?;

        calc_subset_for_masterchain(
            &validator_set,
            &config,
            self.proof.signatures.as_ref().map(|s| s.validator_info.catchain_seqno).unwrap_or(0),
        )
    }

    fn check_signatures(&self, subset: &ValidatorSubsetInfo) -> Result<()> {

        // Pre checks
        if self.proof.signatures.is_none() {
            fail!(NodeError::InvalidData(format!(
                "Proof for {} doesn't have signatures to check",
                self.id(),
            )));
        }
        let signatures = self.proof.signatures.as_ref().unwrap();
        if signatures.validator_info.validator_list_hash_short != subset.short_hash {
            fail!(NodeError::InvalidData(format!(
                "Bad validator set hash in proof for block {}, calculated: {}, found: {}",
                self.id(),
                subset.short_hash,
                signatures.validator_info.validator_list_hash_short
            )));
        }
        let expected_count = signatures.pure_signatures.count() as usize;
        let count = signatures.pure_signatures.signatures().count(expected_count)?;
        if expected_count != count {
            fail!(NodeError::InvalidData(format!(
                "Proof for {}: signature count mismatch: declared: {}, calculated: {}",
                self.id(),
                expected_count,
                count
            )));
        }

        // Check signatures
        let checked_data = ton_block::Block::build_data_for_sign(
            &self.id.root_hash,
            &self.id.file_hash
        );
        let total_weight: u64 = subset.validators.iter().map(|v| v.weight).sum();
        let weight = check_crypto_signatures(&signatures.pure_signatures, &subset.validators, &checked_data)
            .map_err(|err| { 
                NodeError::InvalidData(
                    format!("Proof for {}: error while check signatures: {}", self.id(), err)
                )
            })?;

        // Check weight
        if weight != signatures.pure_signatures.weight() {
            fail!(NodeError::InvalidData(format!(
                "Proof for {}: total signature weight mismatch: declared: {}, calculated: {}",
                self.id(),
                signatures.pure_signatures.weight(),
                weight
            )));
        }

        if weight * 3 <= total_weight * 2 {
            fail!(NodeError::InvalidData(format!(
                "Proof for {}: too small signatures weight",
                self.id(),
            )));
        }

        Ok(())
    }

    fn process_given_state(&self, state: &ShardStateStuff, block_info: &ton_block::BlockInfo)
    -> Result<ValidatorSubsetInfo> {

        // Checks
        if !state.block_id().is_masterchain() {
            fail!(NodeError::InvalidData(format!(
                "Can't check proof for {}: given state {} doesn't belong masterchain",
                self.id(),
                state.block_id()
            )));
        }
        if !self.id().is_masterchain() {
            fail!(NodeError::InvalidData(format!(
                "Can't check proof for non master block {} using master state",
                self.id(),
            )));
        }
        if (state.block_id().seq_no as u32) < block_info.prev_key_block_seqno() {
            fail!(NodeError::InvalidData(format!(
                "Can't check proof for block {} using master state {}, because it is older than the previous key block with seqno {}",
                self.id(),
                state.block_id(),
                block_info.prev_key_block_seqno()
            )));
        }
        if state.block_id().seq_no > self.id().seq_no {
            fail!(NodeError::InvalidData(format!(
                "Can't check proof for block {} using newer master state {}",
                self.id(),
                state.block_id(),
            )));
        }

        let (cur_validator_set, _cc_config) = state.state()?.read_cur_validator_set_and_cc_conf()?;

        calc_subset_for_masterchain(
            &cur_validator_set,
            state.config_params()?,
            self.proof.signatures.as_ref().map(|s| s.validator_info.catchain_seqno).unwrap_or(0),
        )
    }
}

