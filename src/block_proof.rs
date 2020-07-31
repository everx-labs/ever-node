use ton_block::{Block, BlockIdExt, Deserializable, MerkleProof, BlockInfo, UnixTime32,
    ValidatorDescr, ValidatorSet, CatchainConfig, AccountIdPrefixFull
};
use ton_types::{Result, fail, error, dictionary::HashmapType, deserialize_tree_of_cells};

use crate::{
    block::{BlockIdExtExtention},
    error::NodeError,
    shard_state::ShardStateStuff,
    engine_traits::EngineOperations,
};


#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct BlockProofStuff {
    proof: ton_block::BlockProof,
    is_link: bool,
    id: BlockIdExt,
    data: Vec<u8>,
}

impl BlockProofStuff {
    pub fn deserialize(block_id: &BlockIdExt, data: Vec<u8>, is_link: bool) -> Result<Self> {
        let root = deserialize_tree_of_cells(&mut std::io::Cursor::new(&data))?;
        let proof = ton_block::BlockProof::construct_from(&mut root.clone().into())?;
        if &proof.proof_for != block_id {
            fail!(
                NodeError::InvalidData(format!("proof for another block ({} != {})", proof.proof_for, block_id))
            )
        }
        if !block_id.is_masterchain() && !is_link {
            fail!(
                NodeError::InvalidData(format!("proof for non-masterchain block {}", block_id))
            )
        }
        #[cfg(feature = "store_copy")]
        {
            let path = format!("./target/replication/proofs/{}", block_id.shard().shard_prefix_as_str_with_tag());
            std::fs::create_dir_all("{}").ok();
            std::fs::write(format!("{}/{}", path, block_id.seq_no()), &data).ok();
        }
        Ok(BlockProofStuff { proof, is_link, id: block_id.clone(), data })
    }

    #[cfg(test)]
    pub fn read_from_file(block_id: &BlockIdExt, filename: &str, is_link: bool) -> Result<Self> {
        let data = std::fs::read(filename)?;
        Self::deserialize(block_id, data, is_link)
    }

    pub fn virtualize_block(&self) -> Result<(Block, ton_api::ton::int256)> {
        let merkle_proof = MerkleProof::construct_from(&mut self.proof.root.clone().into())?;
        let block_virt_root = merkle_proof.proof.clone().virtualize(1);
        let hash = ton_api::ton::int256(block_virt_root.repr_hash().as_slice().to_owned());
        Ok((Block::construct_from(&mut block_virt_root.into())?, hash))
    }

    pub fn is_link(&self) -> bool {
        self.is_link
    }

    pub fn id(&self) -> &BlockIdExt {
        &self.id
    }

    pub fn proof(&self) -> &ton_block::BlockProof {
        &self.proof
    }

    pub fn data(&self) -> &[u8] {
        &self.data
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
                let mc_pfx = AccountIdPrefixFull::any_masterchain();
                let handle = engine.find_block_by_seq_no(&mc_pfx, prev_key_block_seqno).await?;
                let prev_key_block_proof = engine.load_block_proof(&handle, false).await?;

                self.check_with_prev_key_block_proof_(&prev_key_block_proof, &virt_block, &virt_block_info)?;
            }

            log::trace!("Checked proof for block: {}   TIME {}ms", self.id(), now.elapsed().as_millis());
        }
        Ok(())
    }

    pub fn get_cur_validators_set(&self) -> Result<(ValidatorSet, CatchainConfig)> {
        let (virt_key_block, prev_key_block_info) = self.pre_check_block_proof()?;

        if !prev_key_block_info.key_block() {
            fail!(NodeError::InvalidData(format!(
                "proof for key block {} contains a Merkle proof which declares non key block",
                self.id(),
            )))
        }

        let (cur_validator_set, cc_config) = virt_key_block.read_cur_validator_set_and_cc_conf()
            .map_err(|err| { 
                NodeError::InvalidData(format!(
                    "Сan't extract config params from key block's proof {}: {}",
                    self.id, err
                ))
            })?;

        Ok((cur_validator_set, cc_config))
    }

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
        let (validators, validators_hash_short) =
            self.process_prev_key_block_proof(prev_key_block_proof, virt_block_info.gen_utime().0)?;

        if virt_block_info.key_block() {
            self.pre_check_key_block_proof(virt_block)?;
        }

        self.check_signatures(validators, validators_hash_short)
    }

    fn check_with_master_state_(&self, master_state: &ShardStateStuff, virt_block: &Block, virt_block_info: &BlockInfo) -> Result<()> {
        if virt_block_info.key_block() {
            self.pre_check_key_block_proof(&virt_block)?;
        }

        let (validators, validators_hash_short) =
            self.process_given_state(master_state, virt_block_info)?;

        self.check_signatures(validators, validators_hash_short)
    }

    fn pre_check_block_proof(&self) -> Result<(Block, BlockInfo)> {

        if !self.id().is_masterchain() && self.proof.signatures.is_some() {
            fail!(NodeError::InvalidData(format!(
                "proof for non-master block {} can't contain signatures",
                self.id(),
            )))
        }

        let (virt_block, virt_block_hash) = self.virtualize_block()?;

        if &virt_block_hash.0 != self.id().root_hash.as_slice() {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with incorrect root hash: expected {}, found: {} ",
                self.id(),
                self.id().root_hash,
                virt_block_hash
            )))
        }

        let info = virt_block.read_info()?;
        let _value_flow = virt_block.read_value_flow()?;
        let _state_update = virt_block.read_state_update()?;

        if info.version() != 0 {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with incorrect block info's version {}",
                self.id(),
                info.version()
            )))
        }

        if info.seq_no() != self.id().seq_no as u32 {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with seq_no {}, but {} is expected",
                self.id(),
                info.seq_no(),
                self.id().seq_no
            )))
        }

        if info.shard() != self.id().shard() {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with shard id {}, but {} is expected",
                self.id(),
                info.shard(),
                self.id().shard()
            )))
        }

        if info.read_master_ref()?.is_some() != (!info.shard().is_masterchain()) {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with invalid not_master flag in block info",
                self.id(),
            )))
        }

        if self.id().is_masterchain() && (info.after_merge() || info.before_split() || info.after_split()) {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with a block info which declares split/merge for a masterchain block",
                self.id(),
            )))
        }

        if info.after_merge() && info.after_split() {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with a block info which declares both after merge and after split flags",
                self.id(),
            )))
        }

        if info.after_split() && (info.shard().is_full()) {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with a block info which declares both after_split flag and non zero shard prefix",
                self.id(),
            )))
        }

        if info.after_merge() && !info.shard().can_split() {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof with a block info which declares both after_merge flag and shard prefix which can't split anymore",
                self.id(),
            )))
        }

        if info.key_block() && !self.id().is_masterchain() {
            fail!(NodeError::InvalidData(format!(
                "proof for block {} contains a Merkle proof which declares non masterchain but key block",
                self.id(),
            )))
        }

        Ok((virt_block, info))
    }

    fn pre_check_key_block_proof(&self, virt_block: &Block) -> Result<()> {
        let extra = virt_block.read_extra()?;
        let mc_extra = extra.read_custom()?
            .ok_or_else(|| NodeError::InvalidData(format!(
                "proof for key block {} contains a Merkle proof without masterchain block extra",
                self.id(),
            )))?;
        let config = mc_extra.config()
            .ok_or_else(|| NodeError::InvalidData(format!(
                "proof for key block {} contains a Merkle proof without config params",
                self.id(),
            )))?;
        let _cur_validator_set = config.config(34)?
            .ok_or_else(|| NodeError::InvalidData(format!(
                "proof for key block {} contains a Merkle proof without current validators config param (34)",
                self.id(),
            )))?;
        for i_config in 32..=38 {
            let _val_set = config.config(i_config)?;
        }
        let _catchain_config = config.config(28)?;

        Ok(())
    }

    fn process_prev_key_block_proof(&self, prev_key_block_proof: &BlockProofStuff, gen_utime: u32)
    -> Result<(Vec<ValidatorDescr>, u32)> {
        let (virt_key_block, prev_key_block_info) = prev_key_block_proof.pre_check_block_proof()?;

        if !prev_key_block_info.key_block() {
            fail!(NodeError::InvalidData(format!(
                "proof for key block {} contains a Merkle proof which declares non key block",
                prev_key_block_proof.id(),
            )))
        }

        let (validator_set, cc_config) = virt_key_block.read_cur_validator_set_and_cc_conf()
            .map_err(|err| { 
                NodeError::InvalidData(format!(
                    "While checking proof for {}: can't extract config params from key block's proof {}: {}",
                    self.id, prev_key_block_proof.id(), err
                ))
            })?;

        self.calc_validators_subset(&validator_set, &cc_config, gen_utime)
    }

    fn calc_validators_subset(
        &self,
        validator_set: &ValidatorSet,
        cc_config: &CatchainConfig,
        gen_utime: u32
    ) -> Result<(Vec<ValidatorDescr>, u32)> {
        
        validator_set.calc_subset(
            cc_config, 
            self.id().shard().shard_prefix_with_tag(), 
            self.id().shard().workchain_id(), 
            self.proof.signatures.as_ref().map(|s| s.validator_info.catchain_seqno).unwrap_or(0),
            UnixTime32(gen_utime)
        )
    }

    fn check_signatures(&self, validators_list: Vec<ValidatorDescr>, list_hash_short: u32) -> Result<()> {

        // Pre checks

        if self.proof.signatures.is_none() {
            fail!(NodeError::InvalidData(format!(
                "Proof for {} doesn't have signatures to check",
                self.id(),
            )));
        }
        let signatures = self.proof.signatures.as_ref().unwrap();
        if signatures.validator_info.validator_list_hash_short != list_hash_short {
            fail!(NodeError::InvalidData(format!(
                "Bad validator set hash in proof for block {}, calculated: {}, found: {}",
                self.id(),
                list_hash_short,
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
        let total_weight: u64 = validators_list.iter().map(|v| v.weight).sum();
        let weight = signatures.pure_signatures.check_signatures(validators_list, &checked_data)
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
    -> Result<(Vec<ValidatorDescr>, u32)> {

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

        let (cur_validator_set, cc_config) = state.shard_state().read_cur_validator_set_and_cc_conf()?;

        let (validators, hash_short) = cur_validator_set.calc_subset(
            &cc_config, 
            self.id().shard().shard_prefix_with_tag(), 
            self.id().shard().workchain_id(), 
            self.proof.signatures.as_ref().map(|s| s.validator_info.catchain_seqno).unwrap_or(0),
            block_info.gen_utime())?;

        Ok((validators, hash_short))
    }
}