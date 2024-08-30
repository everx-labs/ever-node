/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific EVERX DEV software governing permissions and
* limitations under the License.
*/

use crate::{
    block::{
        construct_and_check_prev_stuff, BlockStuff, make_queue_update_from_block_raw, 
        make_mesh_update_raw
    },
    block_proof::BlockProofStuff, engine_traits::EngineOperations,
    full_node::apply_block::calc_shard_state, shard_state::ShardStateStuff,
    types::top_block_descr::TopBlockDescrStuff,
    validator::validator_utils::check_crypto_signatures,
    validating_utils::{UNREGISTERED_CHAIN_MAX_LEN, fmt_block_id_short},
};
use storage::block_handle_db::BlockHandle;
use std::{cmp::{max, min}, sync::Arc, ops::Deref, time::Duration, collections::HashSet};
use ever_block::{
    error, fail, Block, BlockIdExt, BlockProof, BlockSignatures, BlockSignaturesPure,
    CryptoSignaturePair, Deserializable, HashmapType, McShardRecord, MerkleProof, OutQueueUpdate,
    Result, Serializable, TopBlockDescr, ValidatorBaseInfo, ValidatorSet, UInt256, UsageTree,

}; 
use ton_api::ton::ton_node::{
    blocksignature::BlockSignature, 
    broadcast::{BlockBroadcast, QueueUpdateBroadcast, MeshUpdateBroadcast}
};

pub async fn accept_block(
    id: BlockIdExt,
    data: Option<Vec<u8>>,
    prev: Vec<BlockIdExt>,
    validator_set: ValidatorSet,
    signatures: Vec<CryptoSignaturePair>,
    _approve_signatures: Vec<CryptoSignaturePair>, // is not actually used by t-node
    send_block_broadcast: bool,
    engine: Arc<dyn EngineOperations>,
) -> Result<()> {
    let block_descr = fmt_block_id_short(&id);
    log::debug!(target: "validator", "({}): accept_block: {}", block_descr, id);

    let is_fake = false;
    let is_fork = false;

    if prev.len() == 0 || prev.len() > 2 {
        fail!("`prev` has invalid length");
    }

    let block_opt = data.map(|data| -> Result<BlockStuff> {
        let block = BlockStuff::deserialize_block(id.clone(), data)?;
        precheck_header(&block, &prev, is_fake, is_fork)?;
        Ok(block)
    }).transpose()?;

    let mut timeout = 50;
    let mut attempt = 0;
    const MAX_ATTEMPTS: usize = 100;
    let (handle, block, proof, signatures) = loop {
        match accept_block_routine(
            &id,
            block_opt.clone(),
            &prev,
            &validator_set,
            &signatures,
            &engine,
            is_fake,
            is_fork,
        ).await {
            Ok(Some(r)) => break r,
            Ok(None) => {
                log::debug!(
                    target: "validator",
                    "({}): accept_block: skipping - block has already pre-applied",
                    block_descr,
                );
                return Ok(())
            },
            Err(e) => {
                attempt += 1;
                log::warn!(target: "validator", 
                    "({}): accept_block routine (attempt {attempt} of {MAX_ATTEMPTS}): {e}", block_descr);
                if attempt > MAX_ATTEMPTS {
                    log::error!("({}): accept_block routine: OUT OF {MAX_ATTEMPTS} ATTEMPTS", block_descr);
                    fail!("accept block routine {id}: OUT OF {MAX_ATTEMPTS} ATTEMPTS", );
                } else {
                    tokio::time::sleep(Duration::from_millis(timeout)).await;
                    timeout = min(timeout * 2, 1000);
                }
            }
        }
    };

    if id.shard().is_masterchain() {
        log::debug!(target: "validator", "({}): acccept_block: Applying block", block_descr);
        engine.clone().apply_block(&handle, &block, id.seq_no(), false).await?;
        log::debug!(target: "validator", "({}): acccept_block: Applied block", block_descr);
    } else {
        let last_mc_state = choose_mc_state(&block, &engine).await?;

        if let Some(tbd) = create_top_shard_block_description(
            &block,
            signatures.clone(),
            &last_mc_state,
            &prev,
            engine.deref()
        ).await? {

            let tbd_stuff = Arc::new(TopBlockDescrStuff::new(tbd, block.id(), false, send_block_broadcast)?);
            tbd_stuff.validate(&last_mc_state)?;

            let engine = engine.clone();
            let cc_seqno = validator_set.catchain_seqno();

            // NOTE: `process_shard_block` is called from `send_top_shard_block_description`,
            // so in case with a single collator we need to wait until this broadcast
            // will be processed. Otherwise there are data races, which are acceptable
            // for multiple collators per session.
            macro_rules! spawn_resend(($expr:expr) => { tokio::spawn(async move { $expr }); };);

            let block_descr = block_descr.clone();
            spawn_resend!({
                log::trace!(target: "validator", "({}): accept_block: sending shard block description broadcast", block_descr);
                if let Err(e) = engine.send_top_shard_block_description(tbd_stuff, cc_seqno, false).await {
                    log::warn!(
                        target: "validator", 
                        "({}): accept_block: error while sending shard block description broadcast: {}",
                        block_descr,
                        e
                    );
                } else {
                    log::trace!(
                        target: "validator",
                        "({}): accept_block: sent shard block description broadcast",
                        block_descr,
                    );
                }
            });
        }
    }

    if send_block_broadcast {
        log::debug!(target: "validator", "({}): accept_block: build block broadcasts", block_descr);
        let block_broadcast = build_block_broadcast(&block, &validator_set, &signatures, proof)?;
        let queue_update_broadcasts = build_queue_update_broadcasts(&block, &validator_set, &signatures)?;
        let engine = engine.clone();
        let block_descr = block_descr.clone();
        let mesh_update_broadcasts = if block.id().shard().is_masterchain() {
            build_mesh_update_broadcasts(&block, &signatures, engine.deref()).await?
        } else {
            vec!()
        };

        tokio::spawn(async move {
            log::trace!(target: "validator", "({}): accept_block: sending block broadcast", block_descr);
            if let Err(e) = engine.send_block_broadcast(block_broadcast).await {
                log::warn!(
                    target: "validator", 
                    "({}): accept_block: error while sending block broadcast: {}",
                    block_descr,
                    e
                );
            } else {
                log::trace!(target: "validator", "({}): accept_block: sent block broadcast", block_descr);
            }
            for broadcast in queue_update_broadcasts {
                let wc = broadcast.target_wc;
                if let Err(e) = engine.send_queue_update_broadcast(broadcast).await {
                    log::warn!(
                        target: "validator", 
                        "({}): accept_block: error while sending queue update broadcast for wc {}: {}",
                        block_descr,
                        wc, e
                    );
                } else {
                    log::trace!(
                        target: "validator",
                        "({}): accept_block: sent queue update broadcast for {}",
                        block_descr,
                        wc
                    );
                }
            }
            for broadcast in mesh_update_broadcasts {
                let nw = broadcast.target_nw;
                if let Err(e) = engine.send_mesh_update_broadcast(broadcast).await {
                    log::warn!(
                        target: "validator", 
                        "({}): accept_block: error while sending mesh update broadcast for nw {}: {}",
                        block_descr, nw, e
                    );
                } else {
                    log::trace!(
                        target: "validator",
                        "({}): accept_block: sent mesh update broadcast for {}",
                        block_descr, nw
                    );
                }
            }
        });
    }

    log::debug!(target: "validator", "({}): accept_block: done", block_descr);
    Ok(())
}

pub async fn accept_block_routine(
    id: &BlockIdExt,
    block_opt: Option<BlockStuff>,
    prev: &[BlockIdExt],
    validator_set: &ValidatorSet,
    signatures: &[CryptoSignaturePair],
    engine: &Arc<dyn EngineOperations>,
    is_fake: bool,
    is_fork:  bool
) -> Result<Option<(Arc<BlockHandle>, BlockStuff, BlockProofStuff, BlockSignatures)>> {

    let handle_opt = if let Some(handle) = engine.load_block_handle(&id)? {
        if handle.is_applied() {
            return Ok(None)
        }
        if !handle.has_data() {
            fail!("INTERNAL ERROR: got uninitialized handle for block {}", id)
        }
        Some(handle)
    } else {
        None
    };

    #[cfg(feature = "telemetry")]
    let mut block_broadcast = block_opt.is_some();

    let block_descr = fmt_block_id_short(&id);

    let block = match block_opt {
        Some(b) => b,
        None => {
            let block = match &handle_opt {
                Some(h) if h.has_data() => {
                    log::debug!(target: "validator", "({}): accept_block: loading block from db", block_descr);
                    let block = engine.load_block(h).await?;
                    block
                }
                _ => {
                    log::debug!(target: "validator", "({}): accept_block: downloading block", block_descr);
                    let (block, _proof) = engine.download_block(&id, Some(10)).await?;
                    block
                }
            };
            precheck_header(&block, prev, is_fake, is_fork)?;
            block
        }
    };

    let mut handle = if let Some(handle) = handle_opt {
        handle
    } else {
        log::debug!(target: "validator", "({}): accept_block: storing block", block_descr);
        let result = engine.store_block(&block).await?;
        #[cfg(feature = "telemetry")]
        if block_broadcast {
            block_broadcast = result._is_updated();
        }
        let handle = result.to_non_created().ok_or_else(
            || error!("INTERNAL ERROR: accept for block {} mismatch", id)
        )?;
        #[cfg(feature = "telemetry")]
        if block_broadcast {
            handle.set_got_by_broadcast(true);
        }
        handle
    };

    log::debug!(target: "validator", "({}): accept_block: storing block prev", block_descr);
    engine.store_block_prev1(&handle, &prev[0])?;
    if prev.len() == 2 {
        engine.store_block_prev2(&handle, &prev[1])?;
    }

    if handle.has_state() {
        return Ok(None)
    }

    log::debug!(target: "validator", "({}): accept_block: calculating shard state", block_descr);
    let _ss = calc_shard_state(
        &handle,
        &block,
        &(prev[0].clone(), prev.get(1).cloned()),
        &engine
    ).await?;


    let (proof, signatures) = create_new_proof(
        &block,
        &validator_set,
        signatures)?;

    log::debug!(target: "validator", "({}): accept_block: storing block proof", block_descr);
    handle = engine.store_block_proof(0, &id, Some(handle), &proof).await?
        .to_non_created()
        .ok_or_else(
            || error!("INTERNAL ERROR: accept for block {} proof mismatch", id)
        )?;

    Ok(Some((handle, block, proof, signatures)))
}

async fn choose_mc_state(
    block: &BlockStuff,
    engine: &Arc<dyn EngineOperations>
) -> Result<Arc<ShardStateStuff>> {
    let block_descr = fmt_block_id_short(block.id());
    let mc_block_id = block.construct_master_id()?;
    let mut last_mc_state = engine.load_last_applied_mc_state().await?;

    if last_mc_state.block_id().seq_no() < mc_block_id.seq_no() {
        // shardchain block refers to newer masterchain block
        log::warn!(
            target: "validator", 
            "({}): shardchain block refers to newer masterchain block {}, trying to obtain it",
            block_descr,
            mc_block_id
        );
        let new_mc_state = engine.clone().wait_state(&mc_block_id, Some(60_000), true).await?;
        new_mc_state
            .shard_state_extra()?
            .prev_blocks
            .check_block(last_mc_state.block_id())
            .map_err(|_| error!(
                "shardchain block {} refers to masterchain block {} \
                which is not an successor of last masterchain block: {}",
                block.id(),
                mc_block_id,
                last_mc_state.block_id()
            ))?;
        last_mc_state = new_mc_state;
    } else if last_mc_state.block_id().seq_no() > mc_block_id.seq_no() {
        // shardchain block refers to older masterchain block
        last_mc_state
            .shard_state_extra()?
            .prev_blocks
            .check_block(&mc_block_id)
            .map_err(|_| error!(
                "shardchain block {} refers to masterchain block {} \
                which is not an ancestor of that referred to by the next one: {}",
                block.id(),
                mc_block_id,
                last_mc_state.block_id()
            ))?;
    } else if *last_mc_state.block_id() != mc_block_id {
        fail!(
            "shardchain block {} refers to masterchain block {} distinct from last \
            masterchain block {} of the same height",
            block.id(),
            mc_block_id,
            last_mc_state.block_id()
        );
    }

    Ok(last_mc_state)
}


fn precheck_header(
    block: &BlockStuff,
    prev: &[BlockIdExt],
    is_fake: bool,
    is_fork: bool,
) -> Result<()> {
    let block_descr = fmt_block_id_short(block.id());

    log::trace!(target: "validator", "({}): precheck_header", block_descr);

    // 1. root hash and file hash check (root hash was checked in BlockStuff constructor)

    if is_fake || is_fork {
        let file_hash = UInt256::calc_file_hash(block.data());
        if *block.id().file_hash() != file_hash {
            fail!(
                "block root hash mismatch: expected {}, , found {}",
                block.id().file_hash().to_hex_string(),
                file_hash.to_hex_string()
            )
        }
    }

    // 2. check header fields

    let (_, prev_stuff) = construct_and_check_prev_stuff(block.root_cell(), block.id(), false)?;
    /*if is_fork {
        prev_ = prev;
    } else*/ if *prev != prev_stuff.prev {
        fail!("invalid previous block reference(s) in block header");
    }

    // 3. unpack header and check vert_seqno fields

    let info = block.block()?.read_info()?;

    if info.vert_seqno_incr() != 0 && !is_fork {
        fail!("block header has vert_seqno_incr set in an ordinary AcceptBlock")
    }
    if info.vert_seqno_incr() == 0 && is_fork {
        fail!("fork block header has no vert_seqno_incr")
    }
    if is_fork && !info.key_block() {
        fail!("fork block is not a key block")
    }

    Ok(())
}

pub fn create_new_proof(
    block_stuff: &BlockStuff,
    validator_set: &ValidatorSet,
    signatures: &[CryptoSignaturePair]
) -> Result<(BlockProofStuff, BlockSignatures)> {
    create_new_proof_internal(block_stuff, validator_set, signatures, false)
}

pub fn create_new_proof_link(block_stuff: &BlockStuff) -> Result<BlockProofStuff> {
    let (proof, _) = create_new_proof_internal(block_stuff, &ValidatorSet::default(), &Vec::new(), true)?;
    Ok(proof)
}

fn create_new_proof_internal(
    block_stuff: &BlockStuff,
    validator_set: &ValidatorSet,
    signatures: &[CryptoSignaturePair],
    force_proof_link: bool,
) -> Result<(BlockProofStuff, BlockSignatures)> {
    let id = block_stuff.id();
    let block_descr = fmt_block_id_short(id);
    log::debug!(target: "validator", "({}): accept_block: create_new_proof", block_descr);

    // visit block while building a Merkle proof
    let usage_tree = UsageTree::with_root(block_stuff.root_cell().clone());
    let block = Block::construct_from_cell(usage_tree.root_cell())?;
    visit_block_for_proof(&block, id)?;

    // queues updates. Need to include all cells of empty update and only root otherwice 
    let mut roots = HashSet::new();
    if let Some(updates) = block.out_msg_queue_updates.as_ref() {
        updates.iterate(|update| {
            if update.is_empty {
                roots.insert(update.update.old.repr_hash());
                roots.insert(update.update.new.repr_hash());
            }
            Ok(true)
        })?;
    }

    // finish constructing Merkle proof from visited cells
    let merkle_proof = MerkleProof::create_with_subtrees(
        block_stuff.root_cell(),
        |h| usage_tree.contains(h),
        |h| roots.contains(h),
    )?;

    if id.shard().is_masterchain() && !block.read_info()?.key_block() {
        if block
            .read_extra()?
            .read_custom()?
            .ok_or_else(|| error!("can not extract masterchain block extra from key block {}", id))?
            .is_key_block() {
                fail!("extra header of non-key masterchain block {} declares key_block=true", id);
        }
    }

    if force_proof_link {
        let proof = BlockProof {
            proof_for: id.clone(),
            root: merkle_proof.serialize()?,
            signatures: None
        };
        return Ok((BlockProofStuff::new(proof, true)?, BlockSignatures::default()))
    }

    // build BlockSignatures struct
    let total_weight = validator_set.total_weight();
    let mut block_signatures_pure = BlockSignaturesPure::with_weight(total_weight);
    for sign in signatures {
        block_signatures_pure.add_sigpair(sign.clone())
    }

    // #[cfg(feature = "fast_finality")]
    // let cc_seqno = collator_range.start;
    // #[cfg(not(feature = "fast_finality"))]
    let cc_seqno = validator_set.catchain_seqno();

    let mut block_signatures = BlockSignatures::with_params(
        ValidatorBaseInfo::with_params(
            ValidatorSet::calc_subset_hash_short(validator_set.list(), cc_seqno)?,
            validator_set.catchain_seqno()
        ), 
        block_signatures_pure
    );

    // check signatures 
    // TODO make function somewhere, BlockProofStuff contains same code

    let checked_data = Block::build_data_for_sign(
        &id.root_hash,
        &id.file_hash
    );
    let weight = check_crypto_signatures(&block_signatures.pure_signatures, validator_set.list(), &checked_data)
        .map_err(|e| error!("Error while check signatures for block {}: {}", id, e))?;

    block_signatures.pure_signatures.set_weight(weight);

    if weight * 3 <= total_weight * 2 {
        fail!("Block {}: too small signatures weight (weight: {}, total: {})", id, weight, total_weight);
    }


    // Construct proof
    let is_link = !id.shard().is_masterchain();
    let proof = BlockProof {
        proof_for: id.clone(),
        root: merkle_proof.serialize()?,
        signatures: if !is_link { Some(block_signatures.clone()) } else { None }
    };

    Ok((BlockProofStuff::new(proof, is_link)?, block_signatures))
}

pub fn visit_block_for_proof(block: &Block, id: &BlockIdExt) -> Result<()> {

    // Visit header

    let info = block.read_info()?;
    let _prev_ref = info.read_prev_ref()?;
    let _prev_vert_ref = info.read_prev_vert_ref()?;
    let master_ref = info.read_master_ref()?;
    let extra = block.read_extra()?;
    block.read_value_flow()?.read_in_full_depth()?;

    // check some header fields, especially shard

    if master_ref.is_none() ^ info.shard().is_masterchain() {
        fail!("block {} has invalid not_master flag in its header", id);
    }
    if info.shard().is_masterchain() && (info.after_merge() || info.after_split() || info.before_split()) {
        fail!("masterchain block header of {} announces merge/split in its header", id);
    }
    if !info.shard().is_masterchain() && info.key_block() {
        fail!("non-masterchain block header of {} announces this block to be a key block", id);
    }

    // check state update
    let _state_update = block.read_state_update()?;

    // visit validator-set related fields in key blocks
    let mc_extra = extra.read_custom()?;
    if info.key_block() {

        let mc_extra = mc_extra.as_ref()
            .ok_or_else(|| error!("can not extract masterchain block extra from key block {}", id))?;

        let config = mc_extra.config()
            .ok_or_else(|| error!("can not extract config params from masterchain block extra of block {}", id))?;

        for i_config in 32..=38 {
            let _val_set = config.config(i_config)?;
        }
        let _catchain_config = config.config(28)?;
        // MVP for workchains
        let _workchains = config.workchains()?.export_vector()?;
    }

    Ok(())
}

pub async fn create_top_shard_block_description(
    block: &BlockStuff,
    signatures: BlockSignatures,
    mc_state: &Arc<ShardStateStuff>,
    prev: &Vec<BlockIdExt>,
    engine: &dyn EngineOperations,
) -> Result<Option<TopBlockDescr>> {

    if let Some((oldest_ancestor_seqno, ancestors)) = find_known_ancestors(block, mc_state)? {

        let proof_links = build_proof_chain(block, prev, oldest_ancestor_seqno,
            ancestors, engine, mc_state).await?;

        let mut tbd = TopBlockDescr::with_id_and_signatures(block.id().clone(), signatures);
        for proof in proof_links {
            // log::trace!(
            //     "create_top_shard_block_description for {} append proof for {}\n{:#.100}",
            //     block.id(),
            //     proof.id(),
            //     proof.proof_root()
            // );
            tbd.append_proof(proof.proof_root().clone());
        }

        Ok(Some(tbd))
    } else {
        Ok(None)
    }
}

fn find_known_ancestors(
    block: &BlockStuff,
    mc_state: &ShardStateStuff
) -> Result<Option<(u32, Vec<McShardRecord>)>> {
    let block_descr = fmt_block_id_short(block.id());

    let master_ref = block.block()?.read_info()?.read_master_ref()?
        .ok_or_else(|| error!("Block {} doesn't have `master_ref`", block.id()))?.master;
    let shard = block.id().shard();
    let shards = mc_state.shards()?;

    let mut ancestors = vec!();
    let oldest_ancestor_seqno;

    match shards.find_shard(shard) {
        Ok(None) => {
            let (a1, a2) = shard.split()?;
            let ancestor1 = shards.get_shard(&a1)?;
            let ancestor2 = shards.find_shard(&a2)?;

            if let (Some(ancestor1), Some(ancestor2)) = (ancestor1, ancestor2) {
                log::trace!(target: "validator", "({}): found two ancestors: {} and {}", block_descr, ancestor1.shard(), ancestor2.shard());
                oldest_ancestor_seqno = max(ancestor1.block_id().seq_no(), ancestor2.block_id().seq_no());
                ancestors.push(ancestor1);
                ancestors.push(ancestor2);
            } else {
                log::warn!(
                    target: "validator", 
                    "({}): cannot retrieve information about shard {} from masterchain block {}, \
                    skipping ShardTopBlockDescr creation",
                    block_descr,
                    shard,
                    mc_state.block_id()
                );

                if mc_state.block_id().seq_no() <= master_ref.seq_no {
                    fail!(
                        "cannot retrieve information about shard {} from masterchain block {}",
                        shard,
                        mc_state.block_id()
                    );
                }
                return Ok(None)
            }
        }
        Ok(Some(ancestor)) if ancestor.shard() == shard => {
            log::trace!(target: "validator", "({}): found one regular ancestor {}", block_descr, ancestor.shard());
            oldest_ancestor_seqno = ancestor.block_id().seq_no();
            ancestors.push(ancestor);
        }
        Ok(Some(ancestor)) if ancestor.shard().is_parent_for(shard) => {
            log::trace!(target: "validator", "({}): found one parent ancestor {}", block_descr, ancestor.shard());
            oldest_ancestor_seqno = ancestor.block_id().seq_no();
            ancestors.push(ancestor);
        }
        Ok(Some(unknown_shard)) => {
            fail!(
                "While finding shard {} in block {} found {}",
                shard,
                mc_state.block_id(),
                unknown_shard.shard()
            )
        }
        Err(e) => fail!("Error while calling `find_shard` for shard {}: {}", shard, e )
    }

    if oldest_ancestor_seqno >= block.id().seq_no() {
        log::warn!(
            target: "validator", 
            "({}): skipping ShardTopBlockDescr creation because a newer block {} \
            is already present in masterchain block {}",
            block_descr,
            ancestors[0].block_id(),
            mc_state.block_id()
        );
        return Ok(None)
    }

    if block.id().seq_no() > oldest_ancestor_seqno + UNREGISTERED_CHAIN_MAX_LEN {
        fail!(
            "cannot accept shardchain block {} because it requires including a chain \
            of more than {} new shardchain blocks",
            block.id(),
            UNREGISTERED_CHAIN_MAX_LEN
        );
    }

    Ok(Some((oldest_ancestor_seqno, ancestors)))
}

async fn build_proof_chain(
    block: &BlockStuff,
    prev: &Vec<BlockIdExt>,
    oldest_ancestor_seqno: u32,
    ancestors: Vec<McShardRecord>,
    engine: &dyn EngineOperations,
    mc_state: &ShardStateStuff
) -> Result<Vec<BlockProofStuff>> {

    let handle = engine.load_block_handle(block.id())?.ok_or_else(
        || error!("Cannot load handle for block {}", block.id())
    )?;
    let mut proof_links = vec![engine.load_block_proof(&handle, true).await?];
    let mut mc_block_id = block.construct_master_id()?;
    let mut link_prev = prev.clone();

    loop {
        let last_proof = proof_links.last().unwrap();
        if last_proof.id().seq_no() == oldest_ancestor_seqno + 1 {
            // first (oldest) link in chain
            if ancestors.len() != link_prev.len() ||
               *ancestors[0].block_id() != link_prev[0] || 
               (ancestors.len() == 2 && *ancestors[1].block_id() != link_prev[1]) {
                fail!(
                    "invalid first link at block {} for shardchain block {}",
                    last_proof.id(),
                    block.id()
                );
            }
            break;
        } else {
            // intermediate link
            if link_prev.len() != 1 ||
               link_prev[0].shard() != block.id().shard() || 
               link_prev[0].seq_no() + 1 != last_proof.id().seq_no() {
                fail!(
                    "invalid intermediate link at block {} for shardchain block {}",
                    last_proof.id(),
                    block.id()
                );
            }
        }

        let prev_handle = engine.load_block_handle(&link_prev[0])?.ok_or_else(
            || error!("Cannot load handle for prev block {}", link_prev[0])
        )?;
        let proof_link = match engine.load_block_proof(&prev_handle, true).await {
            Ok(proof_link) => proof_link,
            Err(_) => break
        };
        link_prev = validate_proof_link(&proof_link, &mut mc_block_id, mc_state)?;

        proof_links.push(proof_link);
    }

    Ok(proof_links)
}

fn validate_proof_link(
    proof_link: &BlockProofStuff,
    mc_block_id: &mut BlockIdExt,
    mc_state: &ShardStateStuff

) -> Result<Vec<BlockIdExt>> {

    let (virt_block, virt_block_root) = proof_link.virtualize_block()?;
    let value_flow = virt_block.read_value_flow()?;
    value_flow.read_in_full_depth()
        .map_err(|e| error!("Can't read value flow in full depth: {}", e))?;

    let (_, prev_stuff) = construct_and_check_prev_stuff(
        &virt_block_root,
        proof_link.id(),
        false
    ).map_err(|e| error!("error in block header in proof link for {}: {}", proof_link.id(), e))?;

    if prev_stuff.mc_block_id.seq_no() > mc_block_id.seq_no() {
        fail!(
            "previous shardchain block {} refers to a newer masterchain block {} \
            than that referred to by the next one: {}",
            proof_link.id(),
            prev_stuff.mc_block_id,
            mc_block_id
        );
    } else if prev_stuff.mc_block_id.seq_no() < mc_block_id.seq_no() {
        mc_state
            .shard_state_extra()?
            .prev_blocks
            .check_block(&prev_stuff.mc_block_id)
            .map_err(|err| error!(
                "previous shardchain block {} refers to masterchain block {} \
                which is not an ancestor of that referred to by the next one: {} : {}",
                proof_link.id(),
                prev_stuff.mc_block_id,
                mc_state.block_id(),
                err
            ))?;

        *mc_block_id = prev_stuff.mc_block_id;
    } else if prev_stuff.mc_block_id != *mc_block_id {
        fail!(
            "previous shardchain block {} refers to masterchain block {} with the same height as, \
            but distinct from that referred to by the next shardchain block: {}",
            proof_link.id(),
            mc_block_id,
            prev_stuff.mc_block_id
        );
    }

    //let _extra = virt_block.read_block_extra()?;   t-node's comment: "TEMP (uncomment later)"

    Ok(prev_stuff.prev)
}

fn pack_signatures(signatures: &BlockSignatures) -> Result<Vec<BlockSignature>> {
    let mut packed_signatures = vec!();
    signatures.pure_signatures.signatures().iterate_slices(|ref mut _key, ref mut slice| {
        let sign = CryptoSignaturePair::construct_from(slice)?;
        packed_signatures.push(
            BlockSignature {
                who: UInt256::with_array(*sign.node_id_short.as_slice()),
                signature: sign.sign.as_bytes().to_vec()
            }
        );
        Ok(true)
    })?;
    Ok(packed_signatures)
}

fn build_block_broadcast(
    block: &BlockStuff,
    validator_set: &ValidatorSet,
    signatures: &BlockSignatures,
    proof: BlockProofStuff,

) -> Result<BlockBroadcast> {
    log::trace!(target: "validator", "accept_block {}: build_block_broadcast", block.id());
    let packed_signatures = pack_signatures(&signatures)?;
    Ok(
        BlockBroadcast {
            id: block.id().clone(),
            catchain_seqno: validator_set.catchain_seqno() as i32,
            validator_set_hash: signatures.validator_info.validator_list_hash_short as i32,
            signatures: packed_signatures.into(),
            proof: proof.drain_data(),
            data: block.data().to_vec()
        }
    )
}

fn build_queue_update_broadcasts(
    block: &BlockStuff,
    validator_set: &ValidatorSet,
    signatures: &BlockSignatures,
) -> Result<Vec<QueueUpdateBroadcast>> {

    log::trace!(target: "validator", "accept_block {}: build_queue_update_broadcasts", block.id());

    let packed_signatures = pack_signatures(&signatures)?;
    let mut broadcasts = vec!();

    if let Some(updates) = block.block()?.out_msg_queue_updates.as_ref() {
        updates.iterate_with_keys(|wc_id: i32, update: OutQueueUpdate| {
            if !update.is_empty {
                let update = make_queue_update_from_block_raw(block, wc_id)?;
                broadcasts.push(QueueUpdateBroadcast {
                    id: block.id().clone(),
                    catchain_seqno: validator_set.catchain_seqno() as i32,
                    validator_set_hash: signatures.validator_info.validator_list_hash_short as i32,
                    signatures: packed_signatures.clone().into(),
                    data: update,
                    target_wc: wc_id
                });
            }
            Ok(true)
        })?;
    }

    Ok(broadcasts)
}

async fn build_mesh_update_broadcasts(
    block: &BlockStuff,
    signatures: &BlockSignatures,
    engine: &dyn EngineOperations,
) -> Result<Vec<MeshUpdateBroadcast>> {

    log::trace!(target: "validator", "accept_block {}: build_mesh_update_broadcasts", block.id());
    let last_mc_state = engine.load_state(block.id()).await?;
    let mut broadcasts = vec!();

    if let Some(mesh_config) = last_mc_state.config_params()?.mesh_config()? {
        let mut active_nws = vec!();
        mesh_config.iterate_with_keys(|nw_id: i32, nw_config| {
            if nw_config.is_active {
                active_nws.push((nw_id, nw_config));
            }
            Ok(true)
        })?;
        if !active_nws.is_empty() {
            for (nw_id, nw_config) in active_nws {
                if nw_config.is_active {
                    let mu = make_mesh_update_raw(block, nw_id, signatures.clone(), engine).await?;
                    broadcasts.push(MeshUpdateBroadcast {
                        src_nw: last_mc_state.state()?.global_id(),
                        id: block.id().clone(),
                        target_nw: nw_id,
                        data: mu,
                    });
                }
            }
        }
    }
    Ok(broadcasts)
}