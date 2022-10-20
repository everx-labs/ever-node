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

use crate::{
    block::construct_and_check_prev_stuff,
    shard_state::ShardStateStuff,
    validator::validator_utils::{calc_subset_for_workchain, check_crypto_signatures},
};

use bitflags::bitflags;
use std::{
    cmp::{max, Ordering, PartialOrd, Ord}, convert::TryInto, fmt, io::{Write, Cursor},
    sync::{Arc, atomic::{AtomicI8, self}}
};
use ton_block::{
    Block, TopBlockDescr, BlockInfo, BlockIdExt, MerkleProof, McShardRecord,
    McStateExtra, Deserializable, HashmapAugType, BlockSignatures, CurrencyCollection,
    AddSub, ShardIdent, Serializable, CopyleftRewards
};
use ton_types::{
    error, Result, fail, Cell, UInt256,
    cells_serialization::{serialize_tree_of_cells, deserialize_tree_of_cells}
};
use ton_api::ton::ton_node::newshardblock::NewShardBlock;

bitflags! {
    pub struct Mode: u8 {
        const DEFAULT = 0;
        const FAIL_NEW = 1;
        const FAIL_TOO_NEW = 2;
        const ALLOW_OLD = 4;
        const ALLOW_NEXT_VSET = 8;
        const SKIP_CHECK_SIG = 16;
    }
}

#[derive(Default, Debug)]
pub struct TopBlockDescrStuff {
    tbd: TopBlockDescr,
    vert_seq_no: u32,
    chain_mc_blk_ids: Vec<BlockIdExt>,
    chain_blk_ids: Vec<BlockIdExt>,
    gen_utime: u32,
    chain_fees: Vec<(CurrencyCollection, CurrencyCollection, CopyleftRewards)>,
    chain_head_prev: Vec<BlockIdExt>,
    creators: Vec<UInt256>,
    is_fake: bool,
    signarutes_validation_result: AtomicI8, // 0 did not check, -1 bad, 1 good
}

impl TopBlockDescrStuff {

    pub fn new(tbd: TopBlockDescr, block_id: &BlockIdExt, is_fake: bool) -> Result<Self> {

        log::trace!(
            target: "validator",
            "TopBlockDescrStuff::new block_id: {}, is_fake: {}",
            block_id,
            is_fake,
        );

        if (tbd.signatures().is_none() || 
            tbd.signatures().map(|s| s.pure_signatures.count()).unwrap_or(0) == 0) &&
           !is_fake {
            fail!(
                "invalid BlockSignatures in ShardTopBlockDescr for {}: no signatures present, \
                and fake mode is not enabled",
                tbd.proof_for()
            );
        }

        // read proof link chain

        let mut vert_seq_no = Default::default();
        let mut gen_utime = Default::default();
        let mut chain_head_prev = vec!();
        let mut chain_mc_blk_ids = vec!();
        let mut chain_blk_ids = vec!();
        let mut chain_fees = vec!();
        let mut creators = vec!();

        let mut cur_id = tbd.proof_for().clone();
        let mut next_info = None;
        for (i, proof_root) in tbd.chain().iter().enumerate() {

            //CHECK(chain->size_ext() == (i == rec.len - 1 ? 0x10000u : 0x20000u));

            let is_head = i == tbd.chain().len() - 1;

            let (prev1_id, prev2_id, cur_mc_id, cur_info, funds, creator) = Self::read_one_proof(
                block_id,
                &cur_id,
                proof_root,
                is_head,
                tbd.signatures(),
                next_info
            ).map_err(|e| error!(
                "error unpacking proof link for {} in ShardTopBlockDescr for {}: {}",
                cur_id,
                tbd.proof_for(),
                e
            ))?;

            if i == 0 {
                vert_seq_no = cur_info.vert_seq_no();
                gen_utime = cur_info.gen_utime().as_u32();
            }
            if is_head {
                chain_head_prev.push(prev1_id.clone());
                if let Some(p) = prev2_id {
                    chain_head_prev.push(p);
                }
            }

            chain_mc_blk_ids.push(cur_mc_id.clone());
            chain_blk_ids.push(cur_id.clone());
            chain_fees.push(funds);
            creators.push(creator);

            next_info = Some((cur_id, cur_mc_id, cur_info));
            cur_id = prev1_id;
        }


        Ok(
            Self {
                tbd,
                vert_seq_no,
                chain_mc_blk_ids,
                chain_blk_ids,
                gen_utime,
                chain_fees,
                chain_head_prev,
                creators,
                is_fake,
                signarutes_validation_result: AtomicI8::new(0),
            }
        )
    }

    pub fn from_bytes(bytes: &[u8], is_fake: bool)-> Result<Self> {
        let root = deserialize_tree_of_cells(&mut Cursor::new(&bytes))?;
        let tbd = TopBlockDescr::construct_from(&mut root.into())?;
        let id = tbd.proof_for().clone();
        TopBlockDescrStuff::new(tbd, &id, is_fake)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut data = Vec::<u8>::new();
        serialize_tree_of_cells(&self.tbd.serialize()?, &mut data)?;
        Ok(data)
    }

    pub fn gen_utime(&self) -> u32 {
        self.gen_utime
    }

    pub fn get_prev_at(&self, index: usize) -> Vec<BlockIdExt> {
        if index < self.size() {
            vec!(self.chain_blk_ids[index].clone())
        } else if index == self.size() {
            self.chain_head_prev.clone()
        } else {
            Default::default()
        }
    }

    pub fn get_top_descr(&self, index: usize) -> Result<McShardRecord> {
        self.get_prev_descr(0, index)
    }

// Unused
//    pub fn drain_top_block_descr(self) -> TopBlockDescr {
//        self.tbd
//    }

    pub fn top_block_descr(&self) -> &TopBlockDescr {
        &self.tbd
    }

    pub fn size(&self) -> usize {
        self.chain_blk_ids.len()
    }

// Unused
//    pub fn top_block_mc_seqno(&self) -> Result<u32> {
//        let merkle_proof = MerkleProof::construct_from(&mut (&self.tbd.chain()[0]).into())?;
//        let block_virt_root = merkle_proof.proof.clone().virtualize(1);
//        let block = Block::construct_from(&mut block_virt_root.into())?;
//        Ok(block.read_info()?.read_master_id()?.seq_no)
//    }

    pub fn top_block_mc_seqno_and_creator(&self) -> Result<(u32, UInt256)> {
        let merkle_proof = MerkleProof::construct_from(&mut (&self.tbd.chain()[0]).into())?;
        let block_virt_root = merkle_proof.proof.clone().virtualize(1);
        let block = Block::construct_from(&mut block_virt_root.into())?;
        Ok((block.read_info()?.read_master_id()?.seq_no, block.read_extra()?.created_by().clone()))
    }

    pub fn get_prev_descr(&self, pos: usize, sum_cnt: usize) -> Result<McShardRecord> {
        if pos >= self.size() || sum_cnt > self.size() || (pos + sum_cnt) > self.size() {
            fail!("Invalid arguments")
        }

        let merkle_proof = MerkleProof::construct_from(&mut (&self.tbd.chain()[pos]).into())?;
        let block_virt_root = merkle_proof.proof.clone().virtualize(1);
        let block = Block::construct_from(&mut block_virt_root.into())?;
        let mut shard_rec = McShardRecord::from_block(&block, self.chain_blk_ids[pos].clone())?;

        shard_rec.descr.fees_collected = CurrencyCollection::default();
        shard_rec.descr.funds_created = CurrencyCollection::default();
        shard_rec.descr.copyleft_rewards = CopyleftRewards::default();

        for i in 0..sum_cnt {
            shard_rec.descr.fees_collected.add(&self.chain_fees[pos + i].0)?;
            shard_rec.descr.funds_created.add(&self.chain_fees[pos + i].1)?;
            shard_rec.descr.copyleft_rewards.merge_rewards(&self.chain_fees[pos + i].2)?;
        }

        Ok(shard_rec)
    }

    pub fn get_creator_list(&self, count: usize) -> Result<Vec<UInt256>> {
        if count > self.size() {
            fail!("Invalid `count`")
        }
        let mut res = vec!();
        for i in (0..count).rev() {
            res.push(self.creators[i].clone());
        }
        Ok(res)
    }

    pub fn proof_for(&self) -> &BlockIdExt {
        &self.tbd.proof_for()
    }

    pub fn new_shard_block(&self) -> Result<NewShardBlock> {
        let signatures = self.tbd.signatures()
            .ok_or_else(|| error!("There is no signatures in top block descr"))?;

        Ok(NewShardBlock {
            block: self.proof_for().clone().into(),
            cc_seqno: signatures.validator_info.catchain_seqno as i32,
            data: self.to_bytes()?.into(),
        })
    }

    pub fn prevalidate(
        &self,
        last_mc_block_id: &BlockIdExt,
        last_mc_state_extra: &McStateExtra,
        vert_seq_no: u32,
        mode: Mode,
        res_flags: &mut u8
    ) -> Result<i32> {
        *res_flags = 0;
        self.validate_internal(last_mc_block_id, last_mc_state_extra, vert_seq_no, res_flags, mode)
    }

    pub fn validate(&self, last_mc_state: &Arc<ShardStateStuff>) -> Result<i32> {
        let mut res_flags = 0;

        let last_mc_block_id = last_mc_state.block_id();
        let last_mc_state_extra = last_mc_state.state().read_custom()?
            .ok_or_else(|| error!("State for {} doesn't have McStateExtra", last_mc_state.block_id()))?;

        self.validate_internal(
            last_mc_block_id,
            &last_mc_state_extra,
            last_mc_state.state().vert_seq_no(),
            &mut res_flags,
            Mode::ALLOW_NEXT_VSET
        )
    }

    fn read_one_proof(
        block_id: &BlockIdExt,
        cur_id: &BlockIdExt,
        proof_root: &Cell,
        is_head: bool,
        signatures: Option<&BlockSignatures>,
        next_info: Option<(BlockIdExt, BlockIdExt, BlockInfo)>
    ) -> Result<(BlockIdExt, Option<BlockIdExt>, BlockIdExt, BlockInfo, (CurrencyCollection, CurrencyCollection, CopyleftRewards), UInt256)> {
        let merkle_proof = MerkleProof::construct_from(&mut proof_root.into())?;
        let block_virt_root = merkle_proof.proof.clone().virtualize(1);

        if block_virt_root.repr_hash() != cur_id.root_hash {
            fail!(
                "link for block {} inside ShardTopBlockDescr of {} contains a Merkle proof \
                with incorrect root hash: expected {}, found {}",
                cur_id,
                block_id,
                cur_id.root_hash.to_hex_string(),
                block_virt_root.repr_hash().to_hex_string()
            )
        }

        let (_, mut prev_stuff) = construct_and_check_prev_stuff(
            &block_virt_root,
            &cur_id,
            false
        ).map_err(|e| {
            error!(
                "error in link for block {} inside ShardTopBlockDescr of {}: {:?}",
                cur_id,
                block_id,
                e
            )
        })?;
        let prev2_id = if prev_stuff.prev.len() > 1 { Some(prev_stuff.prev.pop().unwrap()) } else { None };
        let prev1_id = prev_stuff.prev.pop().unwrap();
        let mc_block_id = prev_stuff.mc_block_id;


        let block = Block::construct_from(&mut block_virt_root.into())?;
        let info = block.read_info()?;
        let value_flow = block.read_value_flow()?;
        value_flow.read_in_full_depth()
            .map_err(|e| error!("Can't read value flow in full depth: {}", e))?;

        if info.version() != 0 {
            fail!("Block -> info -> version should be zero (found {})", info.version())
        }

        // Original t-node contains logic which skip a virtualize error
        // for "backward compatibility with older Proofs / ProofLinks".
        // May be add the compatibility too?
        let extra = block.read_extra()?;

        if let Some(signatures) = signatures {

            if info.gen_catchain_seqno() != signatures.validator_info.catchain_seqno {
                fail!(
                    "link for block {} is invalid because block header \
                    has catchain_seqno = {} \
                    while ShardTopBlockDescr declares {}",
                    cur_id,
                    info.gen_catchain_seqno(),
                    signatures.validator_info.catchain_seqno
                )
            }

            if info.gen_validator_list_hash_short() != signatures.validator_info.validator_list_hash_short {
                fail!(
                    "link for block {} is invalid because block header \
                    has gen_validator_list_hash_short = {} \
                    while ShardTopBlockDescr declares {}",
                    cur_id,
                    info.gen_validator_list_hash_short(),
                    signatures.validator_info.validator_list_hash_short
                )
            }
        }

        /*
        if (chain_mc_blk_ids_.empty()) {
            after_split_ = info.after_split;
            after_merge_ = info.after_merge;
            before_split_ = info.before_split;
            gen_utime_ = first_gen_utime_ = info.gen_utime;
            vert_seqno_ = info.vert_seq_no;
        } else {

        */

        if let Some((next_id, next_mc_id, next_info)) = next_info {

            if next_mc_id.seq_no < mc_block_id.seq_no {
                fail!(
                    "link for block {} refers to masterchain block {} while the next block \
                    refers to an older masterchain block {}",
                    cur_id,
                    mc_block_id,
                    next_mc_id
                )
            }

            if (next_mc_id.seq_no == mc_block_id.seq_no) && (next_mc_id != mc_block_id) {
                fail!(
                    "link for block {} refers to masterchain block {} while the next block \
                    refers to a different same height masterchain block {}",
                    cur_id,
                    mc_block_id,
                    next_mc_id
                )
            }

            if info.before_split() {
                fail!("intermediate link for block {} is declared to be before a split", cur_id)
            }

            if info.gen_utime().as_u32() > next_info.gen_utime().as_u32() {
                fail!(
                    "block creation unixtime goes back from {} to {} in intermediate link \
                    for blocks {} and {}",
                    info.gen_utime(),
                    next_info.gen_utime(),
                    cur_id,
                    next_id
                )
            }

            if next_info.vert_seq_no() != info.vert_seq_no() {
                fail!(
                    "intermediate link for block {} has vertical seqno {} \
                    distinct from the final value in chain {}",
                    cur_id,
                    info.vert_seq_no(),
                    next_info.vert_seq_no(),
                )
            }
        }

        /*
        chain_mc_blk_ids_.push_back(mc_blkid);
        chain_blk_ids_.push_back(cur_id);
        chain_fees_.emplace_back(std::move(fees_collected), std::move(funds_created));
        creators_.push_back(extra.created_by);
        */
        

        if !is_head {
            if info.after_split() || info.after_merge() {
                fail!("intermediate link for block {} is after a split or a merge", cur_id);
            }
            if prev1_id.seq_no() + 1 != cur_id.seq_no() {
                fail!(
                    "intermediate link for block {} increases seqno by more than one from {}",
                    cur_id,
                    prev1_id.seq_no()
                )
            }
        } else {
            /*
            hd_after_split_ = info.after_split;
            hd_after_merge_ = info.after_merge;
            CHECK(link_prev_.size() == 1U + info.after_merge);
            BlockSeqno sq = link_prev_[0].id.seqno;
            */

            let max_prev = if let Some(prev2_id) = prev2_id.as_ref() {
                if prev1_id.seq_no() > prev2_id.seq_no() {
                    &prev1_id
                } else {
                    prev2_id
                }
            } else {
                &prev1_id
            };
            if max_prev.seq_no() + 1 != cur_id.seq_no() {
                fail!(
                    "initial link for block {} increases seqno by more than one from previous {}",
                    cur_id,
                    max_prev
                )
            }
        }

        Ok((
            prev1_id,
            prev2_id,
            mc_block_id,
            info,
            (value_flow.fees_collected, value_flow.created, value_flow.copyleft_rewards),
            extra.created_by
        ))
    }

    fn validate_internal(
        &self,
        last_mc_block_id: &BlockIdExt,
        last_mc_state_extra: &McStateExtra,
        vert_seq_no: u32,
        res_flags: &mut u8,
        mode: Mode
    ) -> Result<i32> {

        log::trace!(
            target: "validator",
            "TopBlockDescrStuff::validate_internal proof_for: {}, mc block: {}, mode: {:?}",
            self.proof_for(),
            last_mc_block_id,
            mode
        );

        debug_assert!(self.size() > 0);
        debug_assert!(self.size() <= 8);
        debug_assert!(self.chain_mc_blk_ids.len() == self.chain_blk_ids.len());

        if last_mc_block_id.seq_no() < self.chain_mc_blk_ids[0].seq_no() {

            let delta = self.chain_mc_blk_ids[0].seq_no() - last_mc_block_id.seq_no();
            if mode.intersects(Mode::FAIL_NEW) || (delta > 8 && mode.intersects(Mode::FAIL_TOO_NEW)) {
                fail!(
                    "ShardTopBlockDescr for {} is too new for us: it refers \
                    to masterchain block {}, but we know only {}",
                    self.proof_for(),
                    self.chain_mc_blk_ids[0],
                    last_mc_block_id
                )
            }
            return Ok(-1);  // valid, but too new
        }

        if vert_seq_no != self.vert_seq_no {
            if self.vert_seq_no < vert_seq_no {
                fail!(
                    "ShardTopBlockDescr for {} is too old: it has vertical seqno {} \
                    but we already know about {}",
                    self.proof_for(),
                    self.vert_seq_no,
                    vert_seq_no
                )
            } else if mode.intersects(Mode::FAIL_NEW) {
                fail!(
                    "ShardTopBlockDescr for {} is too new for us: it has vertical seqno {} \
                    but we know only about {}",
                    self.proof_for(),
                    self.vert_seq_no,
                    vert_seq_no
                )
            }
        }

        let mut next_mc_seqno = std::u32::MAX;
        for mc_id in self.chain_mc_blk_ids.iter() {
            if mc_id.seq_no() > next_mc_seqno {
                *res_flags |= 1;  // permanently invalid
                fail!(
                    "ShardTopBlockDescr for {} is invalid because its chain refers \
                    to masterchain blocks with non-monotonic seqno",
                    self.proof_for(),
                )
            }
            next_mc_seqno = mc_id.seq_no();
            let valid = if mc_id.seq_no() == last_mc_block_id.seq_no() {
                mc_id == last_mc_block_id
            } else {
                last_mc_state_extra.prev_blocks.get(&mc_id.seq_no())?
                    .map(|r| {
                        &r.blk_ref().root_hash == mc_id.root_hash() &&
                            &r.blk_ref().file_hash == mc_id.file_hash()
                    })
                    .unwrap_or(false)
            };
            if !valid {
                *res_flags |= 1;  // permanently invalid
                fail!(
                    "ShardTopBlockDescr for {} is invalid because it refers to masterchain block {} \
                    which is not an ancestor of our block {}",
                    self.proof_for(),
                    mc_id,
                    last_mc_block_id
                )
            }
        }

        // For shard S which is presented in the current master state 
        // find_shard_hash(l_shard_id) returns S's description,
        // if S has two childs it returns left (zero) child's description
        let l_shard_id = self.proof_for().shard().left_ancestor_mask()?;
        let l_shard_descr = last_mc_state_extra.shards().find_shard(&l_shard_id)?
            .ok_or_else(|| {
                error!(
                    "ShardTopBlockDescr for {} is invalid or too new because this workchain \
                    is absent from known masterchain configuration",
                    self.proof_for()
                )
            })?;

        if l_shard_descr.block_id().seq_no() >= self.proof_for().seq_no() {
            if !mode.intersects(Mode::ALLOW_OLD) {
                *res_flags |= 1;  // permanently invalidate unless old ShardTopBlockDescr are allowed
            }
            fail!(
                "ShardTopBlockDescr for {} is too old: we a_ancestorlready know \
                a newer shardchain block {}",
                self.proof_for(),
                l_shard_descr.block_id()
            ) 
        }
        if l_shard_descr.block_id().seq_no() < self.chain_head_prev[0].seq_no() {
            if mode.intersects(Mode::FAIL_NEW) {
                fail!(
                    "ShardTopBlockDescr for {} is too new for us: it starts from shardchain \
                    block {} but we know only {}",
                    self.proof_for(),
                    self.chain_head_prev[0],
                    l_shard_descr.block_id()
                )
            }
            return Ok(-1); // valid, but too new
        }

        let r_shard_descr = if !self.proof_for().shard().is_parent_for(&l_shard_descr.shard()) {
            // Shard S is presented in the current master state
            debug_assert!(l_shard_descr.block_id().shard().is_ancestor_for(self.proof_for().shard()));
            l_shard_descr.clone()
        } else {

            // For shard S which is not presented in the current master state 
            // find_shard_hash(r_shard_id) returns right (one) child
            let r_shard_id = self.proof_for().shard().right_ancestor_mask()?;
            let r_shard_descr = last_mc_state_extra.shards().find_shard(&r_shard_id)?
                .ok_or_else(|| {
                    error!(
                        "ShardTopBlockDescr for {} is invalid or too new because this workchain \
                        is absent from known masterchain configuration (2)",
                        self.proof_for()
                    )
                })?;

            if r_shard_descr.block_id().seq_no() >= self.proof_for().seq_no() {
                // we know a shardchain block that it is at least as new as this one
                *res_flags |= 1;  // permanently invalidate unless old ShardTopBlockDescr are allowed
                fail!(
                    "ShardTopBlockDescr for {} is invalid in a strange fashion: we already know \
                    a newer shardchain block {} but only in the right branch; \
                    corresponds to a shardchain fork?",
                    self.proof_for(),
                    r_shard_descr.block_id()
                );
            }

            r_shard_descr
        };


        let chain_head_prev2 = if self.chain_head_prev.len() > 1 {
            &self.chain_head_prev[1]
        } else {
            &self.chain_head_prev[0]
        };
        if r_shard_descr.block_id().seq_no() < chain_head_prev2.seq_no {
            if mode.intersects(Mode::FAIL_NEW) {
                fail!(
                    "ShardTopBlockDescr for {} is too new for us: it starts from shardchain block \
                    {} but we know only {}",
                    self.proof_for(),
                    chain_head_prev2.seq_no,
                    r_shard_descr.block_id().seq_no()
                )
            }
            return Ok(-1); // valid, but too new
        }

        let clen = (self.proof_for().seq_no - max(l_shard_descr.block_id().seq_no(), r_shard_descr.block_id().seq_no())) as usize;
        debug_assert!(clen > 0);
        debug_assert!(clen <= 8);
        debug_assert!(clen <= self.size());

        if clen < self.size() {
            if &self.chain_blk_ids[clen] != l_shard_descr.block_id() {
                *res_flags |= 1;
                fail!(
                    "ShardTopBlockDescr for {} is invalid: it contains a reference to its ancestor \
                    {} but the masterchain refers to another shardchain block {} of the same height",
                    self.proof_for(),
                    self.chain_blk_ids[clen],
                    if l_shard_descr.block_id().seq_no() < r_shard_descr.block_id().seq_no() { 
                        r_shard_descr.block_id() 
                    } else { 
                        l_shard_descr.block_id()
                    }
                )
            }
            debug_assert!(l_shard_descr.block_id().shard() == self.proof_for().shard());
            debug_assert!(l_shard_descr == r_shard_descr);
        } else {
            if &self.chain_head_prev[0] != l_shard_descr.block_id() {
                *res_flags |= 1;
                fail!(
                    "ShardTopBlockDescr for {} is invalid: it contains a reference to its left ancestor \
                    {} but the masterchain instead refers to another shardchain block {}",
                    self.proof_for(),
                    self.chain_head_prev[0],
                    l_shard_descr.block_id()
                )
            }
            if chain_head_prev2 != r_shard_descr.block_id() {
                *res_flags |= 1;
                fail!(
                    "ShardTopBlockDescr for {} is invalid: it contains a reference to its right ancestor \
                    {} but the masterchain instead refers to another shardchain block {}",
                    self.proof_for(),
                    chain_head_prev2,
                    r_shard_descr.block_id()
                )
            }
        }

        log::debug!(
            target: "validator",
            "ShardTopBlockDescr for {} appears to have a valid chain of {} new links out of {}",
            self.proof_for(),
            clen,
            self.size() 
        );


        // compute and check validator set

        let cur_validator_set = last_mc_state_extra.config.validator_set()?;
        let cc_config = last_mc_state_extra.config.catchain_config()?;
        let cc_seqno = last_mc_state_extra.shards().calc_shard_cc_seqno(self.proof_for().shard())?;

        let mut vset_ok = false;
        let (mut validators, mut hash_short) = calc_subset_for_workchain(
            &cur_validator_set,
            &last_mc_state_extra.config,
            &cc_config, 
            self.proof_for().shard().shard_prefix_with_tag(), 
            self.proof_for().shard().workchain_id(), 
            cc_seqno,
            0.into())?;

        // original t-node's code contains this check later.
        // But we have some checks there which needs signatures!!!
        // May be t-node uses valid `signatures` struct with fake signatures
        if mode.intersects(Mode::SKIP_CHECK_SIG) {
            return Ok(clen as i32)
        }

        let signatures = self.tbd.signatures()
            .ok_or_else(|| error!("There is no signatures in top block descr"))?;

        if (cc_seqno == signatures.validator_info.catchain_seqno) &&
           (hash_short == signatures.validator_info.validator_list_hash_short) {
            *res_flags |= 4;
            vset_ok = true;
        } else if mode.intersects(Mode::ALLOW_NEXT_VSET) {
            let next_validator_set = last_mc_state_extra.config.next_validator_set()?;
            let (next_validators, next_hash_short) = calc_subset_for_workchain(
                &next_validator_set,
                &last_mc_state_extra.config,
                &cc_config, 
                self.proof_for().shard().shard_prefix_with_tag(), 
                self.proof_for().shard().workchain_id(), 
                cc_seqno,
                0.into())?;

            if (cc_seqno == signatures.validator_info.catchain_seqno) &&
               (next_hash_short == signatures.validator_info.validator_list_hash_short) {
                *res_flags |= 8;
               vset_ok = true;
               validators = next_validators;
               hash_short = next_hash_short;
            }
        }

        if !vset_ok {
            fail!(
                "ShardTopBlockDescr for {} is invalid because it refers to shard validator set \
                with hash {} and catchain_seqno {} while the current masterchain configuration \
                expects {} and {}",
                self.proof_for(),
                signatures.validator_info.validator_list_hash_short,
                signatures.validator_info.catchain_seqno,
                hash_short,
                cc_seqno
            )
        }


        // check signatures

        // if mode.intersects(Mode::SKIP_CHECK_SIG) {
        //     return Ok(clen as i32)
        // }

        match self.signarutes_validation_result.load(atomic::Ordering::Relaxed) {
            -1 => fail!("ShardTopBlockDescr for {} has bad signatures (according to cached result)", self.proof_for()),
            1 => {
                log::debug!("ShardTopBlockDescr for {} has valid signatures (according to cached result)", self.proof_for());
                *res_flags |= 0x10;
                return Ok(clen as i32)
            }
            _ => ()
        }
        
        let checked_data = Block::build_data_for_sign(
            &self.proof_for().root_hash,
            &self.proof_for().file_hash
        );
        let total_weight: u64 = validators.iter().map(|v| v.weight).sum();
        let weight = match check_crypto_signatures(&signatures.pure_signatures, &validators, &checked_data) {
            Err(err) => {
                *res_flags |= 0x21;
                self.signarutes_validation_result.store(-1, atomic::Ordering::Relaxed);
                fail!(
                    "ShardTopBlockDescr for {} does not have valid signatures: {}",
                    self.proof_for(),
                    err
                )
            }
            Ok(w) => w
        };
        if !self.is_fake && weight * 3 <= total_weight * 2 {
            *res_flags |= 0x21;
            self.signarutes_validation_result.store(-1, atomic::Ordering::Relaxed);
            fail!(
                "ShardTopBlockDescr for {} has too small signatures weight {} of {}",
                self.proof_for(),
                weight, total_weight
            );
        }
        *res_flags |= 0x10;  // signatures checked ok

        // Check weight
        if !self.is_fake && weight != signatures.pure_signatures.weight() {
            self.signarutes_validation_result.store(-1, atomic::Ordering::Relaxed);
            fail!(
                "ShardTopBlockDescr for {} has incorrect signature weight {} (actual weight is {})",
                self.proof_for(),
                signatures.pure_signatures.weight(),
                weight
            );
        }

        self.signarutes_validation_result.store(1, atomic::Ordering::Relaxed);

        log::debug!(
            "ShardTopBlockDescr for {} has valid validator signatures of total weight {} out of {}",
            self.proof_for(),
            weight,
            total_weight
        );

        return Ok(clen as i32)
    }
}

// see cmp_shard_block_descr_ref in t-node
pub fn cmp_shard_block_descr(a: &TopBlockDescrStuff, b: &TopBlockDescrStuff) -> Ordering {
    let a = a.proof_for();
    let b = b.proof_for();
    match a.shard().workchain_id().cmp(&b.shard().workchain_id()) {
        Ordering::Equal => {
            match a.shard().shard_prefix_with_tag().cmp(&b.shard().shard_prefix_with_tag()) {
                Ordering::Equal => {
                    b.seq_no().cmp(&a.seq_no())
                }
                ord => ord
            }
        }
        ord => ord
    }
}

#[derive(Default, Debug, PartialEq, Eq, Hash, Clone)]
pub struct TopBlockDescrId {
    pub id: ShardIdent,
    pub cc_seqno: u32,
}

impl TopBlockDescrId {
    pub fn new(id: ShardIdent, cc_seqno: u32) -> Self {
        Self{id, cc_seqno}
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (wc_bytes, rest) = bytes.split_at(std::mem::size_of::<i32>());
        let (shard_bytes, rest) = rest.split_at(std::mem::size_of::<u64>());
        let (cc_bytes, _) = rest.split_at(std::mem::size_of::<u32>());

        Ok(Self{
            id: ShardIdent::with_tagged_prefix(
                i32::from_le_bytes(wc_bytes.try_into()?),
                u64::from_le_bytes(shard_bytes.try_into()?),
            )?,
            cc_seqno: u32::from_le_bytes(cc_bytes.try_into()?),
        })
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut writer = Cursor::new(vec!());
        writer.write_all(&self.id.workchain_id().to_le_bytes())?;
        writer.write_all(&self.id.shard_prefix_with_tag().to_le_bytes())?;
        writer.write_all(&self.cc_seqno.to_le_bytes())?;
        Ok(writer.into_inner())
    }

}

impl PartialOrd for TopBlockDescrId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopBlockDescrId {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_id = self.id.shard_prefix_with_tag();
        let other_id = other.id.shard_prefix_with_tag();
        if self_id < other_id || (self_id == other_id && self.cc_seqno < other.cc_seqno) {
            Ordering::Less
        } else if self == other {
            Ordering::Equal
        } else {
            Ordering::Greater
        }
    }
}

impl fmt::Display for TopBlockDescrId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.id, self.cc_seqno)
    }
}
