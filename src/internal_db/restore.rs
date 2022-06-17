use crate::{
    block::{BlockIdExtExtention, BlockStuff},
    internal_db::{
        InternalDb, LAST_APPLIED_MC_BLOCK, SHARD_CLIENT_MC_BLOCK, LAST_ROTATION_MC_BLOCK,
        PSS_KEEPER_MC_BLOCK, BlockHandle,
    },
    shard_state::ShardStateStuff,
};
use ton_block::{BlockIdExt, MASTERCHAIN_ID, ShardIdent, SHARD_FULL};
use ton_types::{error, fail, Result, UInt256, Cell};
use storage::traits::Serializable;
use std::{
    borrow::Cow, collections::{HashSet, HashMap}, fs::{write, remove_file}, io::Cursor,
    ops::Deref, path::Path, sync::atomic::{AtomicBool, Ordering}, time::Duration
};

const UNEXPECTED_TERMINATION_BEACON_FILE: &str = "ton_node.running";
const RESTORING_BEACON_FILE: &str = "ton_node.restoring";
const LAST_MC_BLOCKS: u32 = 100;
const SHARD_CLIENT_MC_BLOCK_CANDIDATES: u32 = 300;

pub async fn check_db(
    mut db: InternalDb,
    processed_wc: i32,
    restore_db: bool,
    force: bool,
    check_stop: &(dyn Fn() -> Result<()> + Sync),
    is_broken: Option<&AtomicBool>
) -> Result<InternalDb> {                            	

    async fn force_db_reset(
        err: failure::Error,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
        is_broken: Option<&AtomicBool>
    ) -> ! {
        if let Some(is_broken) = is_broken {
            is_broken.store(true, Ordering::Relaxed)
        }
        log::error!(
            "Error while restoring database: {}. Need to clear db and re-sync node.", 
            err
        );
        loop {
            tokio::time::sleep(Duration::from_millis(300)).await; 
            if check_stop().is_err() {
                std::process::exit(0xFF)
            }
        }
    } 

    let unexpected_termination = check_unexpected_termination(&db.config.db_directory);
    let restoring = check_restoring(&db.config.db_directory);

    if unexpected_termination || restoring || force {
        if force {
            log::info!("Starting check & restore db process forcely");
        } else if restore_db {
            log::warn!("Previous node run was unexpectedly terminated, \
            starting check & restore process...");
        } else {
            if unexpected_termination {
                log::warn!("Previous node run was terminated unexpectedly, \
                    but 'restore_db' option in node config is 'false', \
                    so restore operation is skipped. Node may work incorrectly.");
            } else {
                log::warn!("Previous node run was terminated unexpectedly while DB's \
                    checking or restoring, but now 'restore_db' option in node \
                    config is 'false', so restore operation is skipped. Node may work incorrectly.");
            }
            return Ok(db);
        }

        set_restoring(&db.config.db_directory)?;
        match restore_last_applied_mc_block(&db, check_stop).await {
            Ok(Some(last_applied_mc_block)) => {
                let shard_client_mc_block = restore_shard_client_mc_block(
                    &db, &last_applied_mc_block, processed_wc, check_stop).await?;
                db = match restore(
                    db, &last_applied_mc_block, &shard_client_mc_block, processed_wc, check_stop
                ).await {
                    Ok(db) => db,
                    Err(err) => force_db_reset(err, check_stop, is_broken).await
                };
            }
            Ok(None) => {
                log::info!("End of check & restore: looks like node hasn't \
                    ever booted in blockchain.");
            }
            Err(err) => force_db_reset(err, check_stop, is_broken).await
        }
        reset_restoring(&db.config.db_directory)?;
    }
    set_unexpected_termination(&db.config.db_directory)?;
    Ok(db)
}

pub fn set_graceful_termination(db_dir: &str) {
    let path = Path::new(db_dir).join(UNEXPECTED_TERMINATION_BEACON_FILE);
    if let Err(e) = remove_file(&path) {
        log::error!(
            "set_graceful_termination: can't remove file {}, please do it manually, \
                otherwice check and restore DB operation will run next start (error: {})",
            UNEXPECTED_TERMINATION_BEACON_FILE,
            e
        );
    }
}

fn check_unexpected_termination(db_dir: &str) -> bool {
    Path::new(db_dir).join(UNEXPECTED_TERMINATION_BEACON_FILE).as_path().exists()
}

fn set_unexpected_termination(db_dir: &str) -> Result<()> {
    let path = Path::new(db_dir).join(UNEXPECTED_TERMINATION_BEACON_FILE);
    write(&path, "")?;
    Ok(())
}

fn reset_restoring(db_dir: &str) -> Result<()> {
    let path = Path::new(db_dir).join(RESTORING_BEACON_FILE);
    remove_file(&path)?;
    Ok(())
}

fn check_restoring(db_dir: &str) -> bool {
    Path::new(db_dir).join(RESTORING_BEACON_FILE).as_path().exists()
}

fn set_restoring(db_dir: &str) -> Result<()> {
    let path = Path::new(db_dir).join(RESTORING_BEACON_FILE);
    write(&path, "")?;
    Ok(())
}

async fn restore_last_applied_mc_block(
    db: &InternalDb,
    check_stop: &(dyn Fn() -> Result<()> + Sync),
) -> Result<Option<BlockStuff>> {
    log::trace!("restore_last_applied_mc_block");
    match db.load_full_node_state(LAST_APPLIED_MC_BLOCK) {
        Ok(None) => return Ok(None),
        Ok(Some(id)) => {
            match check_one_block(db, &id, false, true).await {
                Ok(block) => {
                    log::info!("restore_last_applied_mc_block: {} looks good", id);
                    return Ok(Some(block));
                }
                Err(e) => log::warn!("LAST_APPLIED_MC_BLOCK {} is broken: {}", id, e)
            }
        },
        Err(e) => {
            log::warn!("Can't load LAST_APPLIED_MC_BLOCK: {}, ", e);
        }
    }

    let block = search_and_restore_last_applied_mc_block(db, check_stop).await
        .map_err(|e| error!("search_and_restore_last_applied_mc_block: {}", e))?;
    Ok(Some(block))
}

async fn search_and_restore_last_applied_mc_block(
    db: &InternalDb,
    check_stop: &(dyn Fn() -> Result<()> + Sync),
) -> Result<BlockStuff> {
    log::trace!("search_and_restore_last_applied_mc_block");
    
    let mut last_mc_blocks = search_last_mc_blocks(db, check_stop)?;
    while let Some(id) = last_mc_blocks.pop() {
        check_stop()?;
        log::trace!("search_and_restore_last_applied_mc_block: trying {}...", id);
        match check_one_block(db, &id, false, true).await {
            Ok(block) => return Ok(block),
            Err(e) => log::warn!("{} is broken: {}", id, e)
        }
    }
    fail!("All found last mc blocks were broken")
}

async fn restore_shard_client_mc_block(
    db: &InternalDb,
    last_applied_mc_block: &BlockStuff,
    processed_wc: i32,
    check_stop: &(dyn Fn() -> Result<()> + Sync),
) -> Result<BlockStuff> {
    let mut block = match db.load_full_node_state(SHARD_CLIENT_MC_BLOCK) {
        Ok(None) => {
            log::warn!("SHARD_CLIENT_MC_BLOCK is None, use last applied mc block instead");
            last_applied_mc_block.clone()
        }
        Ok(Some(id)) => {
            log::trace!("SHARD_CLIENT_MC_BLOCK: {}", id);
            if *id == *last_applied_mc_block.id() {
                last_applied_mc_block.clone()
            } else {
                match check_one_block(db, &id, false, true).await {
                    Ok(block) => {
                        log::trace!("SHARD_CLIENT_MC_BLOCK {} looks good", id);
                        block
                    }
                    Err(e) => {
                        log::warn!(
                            "SHARD_CLIENT_MC_BLOCK {} is broken: {}; \
                                use last applied mc block instead",
                            id, e
                        );
                        last_applied_mc_block.clone()
                    }
                }
            }
        },
        Err(e) => {
            log::warn!("Can't load SHARD_CLIENT_MC_BLOCK: {}; \
                use last applied mc block instead", e);
            last_applied_mc_block.clone()
        }
    };

    let mut checked_blocks = 0_u32;
    loop {
        check_stop()?;
        match check_shard_client_mc_block(db, &block, processed_wc, false, check_stop).await {
            Ok(_) => {
                log::info!("restore_shard_client_mc_block: {} has all shard blocks", block.id());
                return Ok(block)
            }
            Err(e) => {
                log::warn!("{} doesn't have all shard blocks: {}, use prev", block.id(), e);
                let prev_id = block.construct_prev_id()?.0;
                block = check_one_block(db, &prev_id, true, true).await.map_err(
                    |e| error!("restore_shard_client_mc_block: can't check prev block: {}", e))?;
            }
        }
        checked_blocks += 1;
        if checked_blocks > SHARD_CLIENT_MC_BLOCK_CANDIDATES {
            fail!("Can't find good mc block for shard client");
        }
    }
}

async fn restore(
    mut db: InternalDb,
    last_applied_mc_block: &BlockStuff,
    shard_client_mc_block: &BlockStuff,
    processed_wc: i32,
    check_stop: &(dyn Fn() -> Result<()> + Sync),
) -> Result<InternalDb> {

    let last_mc_block =
        if last_applied_mc_block.id().seq_no() > shard_client_mc_block.id().seq_no() {
            shard_client_mc_block
        } else {
            last_applied_mc_block
        };

    log::info!("restore, use as last block: {}", last_mc_block.id());

    // truncate DB
    log::debug!("try to truncate database after {}", last_mc_block.id());
    match db.load_block_next1(last_mc_block.id()) {
        Err(_) => {
            log::info!("Can't load next mc block (prev {}), \
                db truncation will be skipped", last_mc_block.id());
        },
        Ok(next_id) => {
            match db.truncate_database(&next_id, processed_wc).await {
                Err(e) => 
                    log::warn!("Error while db truncation at block {}: {}", next_id, e),
                Ok(_) => 
                    log::info!("Database was truncated at {}", next_id),
            }
        }
    }
    db.save_full_node_state(SHARD_CLIENT_MC_BLOCK, last_mc_block.id())?;
    db.save_full_node_state(LAST_APPLIED_MC_BLOCK, last_mc_block.id())?;

    if let Some(block_id) = db.load_full_node_state(LAST_ROTATION_MC_BLOCK)? {
        if block_id.seq_no > last_mc_block.id().seq_no {
            db.save_validator_state(LAST_ROTATION_MC_BLOCK, last_mc_block.id())?;
        }
    }

    if let Some(block_id) = db.load_full_node_state(PSS_KEEPER_MC_BLOCK)? {
        if block_id.seq_no > last_mc_block.id().seq_no {
            db.save_full_node_state(PSS_KEEPER_MC_BLOCK, last_mc_block.id())?;
        }
    }

    let min_mc_state_id = calc_min_mc_state_id(&db, last_mc_block.id()).await?;
    
    log::info!("Checking shard states...");
    let mut mc_block = Cow::Borrowed(last_mc_block);
    let mut broken_cells = false;
    let mut checked_cells = HashSet::new();
    let mut persistent_state_handle = None;
    loop {
        check_stop()?;
        // check master state
        if mc_block.id().seq_no() >= min_mc_state_id.seq_no() && !broken_cells {
            if let Err(e) = check_state(&db, mc_block.id(), &mut checked_cells, check_stop) {
                log::warn!("Error while checking state {} {}", mc_block.id(), e);
                broken_cells = true;
            }
        }
        
        // check shard blocks and states

        let shard_blocks = check_shard_client_mc_block(
            &db, &mc_block, processed_wc, persistent_state_handle.is_none(), check_stop).await?;

        if mc_block.id().seq_no() >= min_mc_state_id.seq_no() && !broken_cells {
            for block_id in &shard_blocks {
                if let Err(e) = check_state(&db, block_id, &mut checked_cells, check_stop) {
                    log::warn!("Error while checking state {} {}", block_id, e);
                    broken_cells = true;
                    break;
                }
            }
        }

        if persistent_state_handle.is_some() {
            break;
        }

        // check prev master block
        let prev_id = mc_block.construct_prev_id()?.0;

        // if prev is zerostate check it and exit cycle
        if prev_id.seq_no() == 0 {
            if min_mc_state_id.seq_no() == 0 && !broken_cells {
                if let Err(e) = check_state(&db, &prev_id, &mut checked_cells, check_stop) {
                    log::warn!("Error while checking state {} {}", prev_id, e);
                    broken_cells = true;
                } else {
                    // check wc zerostate
                    let zerostate = db.load_shard_state_dynamic(&prev_id)?;
                    let workchains = zerostate.config_params()?.workchains()?;
                    let wc = workchains.get(&processed_wc)?.ok_or_else(|| {
                        error!("workchains doesn't have description for workchain {}", processed_wc)
                    })?;
                    let wc_zerostate_id = BlockIdExt {
                        shard_id: ShardIdent::with_tagged_prefix(processed_wc, SHARD_FULL)?,
                        seq_no: 0,
                        root_hash: wc.zerostate_root_hash,
                        file_hash: wc.zerostate_file_hash,
                    };
                    if let Err(e) = check_state(&db, &wc_zerostate_id, &mut checked_cells, check_stop) {
                        log::warn!("Error while checking state {} {}", wc_zerostate_id, e);
                        broken_cells = true;
                    }
                }
            }

            persistent_state_handle = Some(db.load_block_handle(&prev_id)?
                .ok_or_else(|| error!("there is no handle for zerostate {}", prev_id))?);
            break;
        }

        // if this mc block has persistent state - end cycle
        let prev_handle = db.load_block_handle(&prev_id)?
            .ok_or_else(|| error!("there is no handle for block {}", prev_id))?;
        if prev_handle.has_persistent_state() {
            persistent_state_handle = Some(prev_handle);
        }

        mc_block = Cow::Owned(check_one_block(
            &db, &prev_id, true, persistent_state_handle.is_none()).await?);
        log::debug!("restore: mc block looks good {}", prev_id);
    };

    if broken_cells {
        log::warn!("Shard states db is broken, it will be clear and restored from persistent \
            states and blocks. It will take some time...");
        db.reset_unapplied_handles()?;
        db.clean_shard_state_dynamic_db()?;
        log::debug!("Shard states db was cleaned");
        restore_states(
            &db,
            persistent_state_handle
                .ok_or_else(|| error!("internal error: persistent_state_handle is None"))?.deref(),
            &min_mc_state_id,
            processed_wc,
            check_stop
        ).await?;
    }

    log::info!("Restore successfully finished");

    Ok(db)
}

async fn check_shard_client_mc_block(
    db: &InternalDb,
    block: &BlockStuff,
    processed_wc: i32,
    check_to_prev_mc_block: bool,
    check_stop: &(dyn Fn() -> Result<()> + Sync),
) -> Result<Vec<BlockIdExt>> {
    log::trace!("check_shard_client_mc_block, mc block {}", block.id());
    
    let shard_block_from_prev_mc = if !check_to_prev_mc_block || block.id().seq_no() == 1 {
        vec!()
    } else {
        let id = block.construct_prev_id()?.0;
        let handle = db.load_block_handle(&id)?
            .ok_or_else(|| error!("there is no handle for block {}", id))?;
        let block = db.load_block_data(&handle).await?;
        block.shard_hashes()?.top_blocks(&[processed_wc])?
    };

    let top_shard_blocks_ids = block.shard_hashes()?.top_blocks(&[processed_wc])?;
    let mut shard_blocks_ids = Vec::with_capacity(top_shard_blocks_ids.len());
    for mut id in top_shard_blocks_ids {
        loop {
            check_stop()?;
            log::trace!("check_shard_client_mc_block: checking {}", id);
            if id.seq_no() == 0 {
                // zerostate doesn't have correspond block, but we return its id to check 
                // the state later
                shard_blocks_ids.push(id);
                break;
            } else {
                let block = check_one_block(db, &id, false, check_to_prev_mc_block).await
                    .map_err(|e| error!("Shard block has a problem: {}", e))?;
                log::debug!("check_shard_client_mc_block: {} looks good", id);
                // splites or merges is possible only at commited (into masterchain) blocks,
                // so it is ok to use only one prev block here
                let prev_id = block.construct_prev_id()?.0;
                shard_blocks_ids.push(id.clone());

                if shard_block_from_prev_mc.is_empty() {
                    break;
                } else {
                    if shard_block_from_prev_mc.contains(&prev_id) 
                        || shard_block_from_prev_mc.contains(&id) {
                        break;
                    }
                    id = prev_id;
                }
            }
        }
    }
    Ok(shard_blocks_ids)
}

fn search_last_mc_blocks(
    db: &InternalDb,
    check_stop: &(dyn Fn() -> Result<()> + Sync),
) -> Result<Vec<BlockIdExt>> {
    let mut last_mc_block = BlockIdExt::default();
    log::trace!("search_last_mc_blocks: search last id");
    db.next1_block_db.for_each(&mut |_key, val| {
        check_stop()?;
        let id = BlockIdExt::deserialize(&mut Cursor::new(val))?;
        if id.shard().workchain_id() == MASTERCHAIN_ID && id.seq_no() as u32 > last_mc_block.seq_no() {
            last_mc_block = id;
        }
        Ok(true)
    })?;
    if last_mc_block == BlockIdExt::default() {
        fail!("Can't find last mc block in next1_block_db");
    }

    log::trace!("search_last_mc_blocks: last id is {}; \
        search last {} ids", last_mc_block, LAST_MC_BLOCKS);
    let mut last_mc_blocks = Vec::with_capacity(LAST_MC_BLOCKS as usize);
    db.next1_block_db.for_each(&mut |_key, val| {
        check_stop()?;
        let id = BlockIdExt::deserialize(&mut Cursor::new(val))?;
        if id.shard().workchain_id() == MASTERCHAIN_ID 
            && id.seq_no() as u32 + LAST_MC_BLOCKS >= last_mc_block.seq_no() {
            last_mc_blocks.push(id);
        }
        Ok(true)
    })?;
    last_mc_blocks.sort_unstable_by_key(|id| id.seq_no());
    log::trace!("search_last_mc_blocks: found {} id", last_mc_blocks.len());
    Ok(last_mc_blocks)
}

async fn check_one_block(
    db: &InternalDb,
    id: &BlockIdExt,
    should_has_next: bool,
    should_has_prev: bool,
) -> Result<BlockStuff> {
    log::trace!("check_one_block {}", id);

    let handle = db.load_block_handle(id)?
        .ok_or_else(|| error!("there is no handle for block {}", id))?;
    let block_data = db.load_block_data_raw(&handle).await?;
    let block = BlockStuff::deserialize_checked(id.clone(), block_data)?;
    let _proof = db.load_block_proof(&handle, !id.is_masterchain()).await?;

    if !handle.has_data() {
        log::warn!("Block {} has handle.has_data() false", id);
        handle.set_data();
        db.store_block_handle(&handle, None)?;
    }
    if id.shard().is_masterchain() {
        if !handle.has_proof() {
            log::warn!("Block {} has handle.has_proof() false", id);
            handle.set_proof();
            db.store_block_handle(&handle, None)?;
        }
    } else {
        if !handle.has_proof_link() {
            log::warn!("Block {} has handle.has_proof_or_link() false", id);
            handle.set_proof_link();
            db.store_block_handle(&handle, None)?;
        }
    }

    if !handle.is_applied() {
        fail!("Block {} has not applied");
    }
    if !handle.is_archived() {
        if handle.masterchain_ref_seq_no() == 0 {
            fail!("Applied block {} is not archived and doesn't have masterchain_ref_seq_no", id);
        } else {
            db.archive_block(id, None).await?;
        }
    }

    // prev 1
    if should_has_prev {
        let (prev1, prev2) = block.construct_prev_id()?;
        match db.load_block_prev1(id) {
            Ok(prev1_db) => {
                if prev1_db != prev1 {
                    log::warn!("Block {} has real prev {}, but in db {}, restore", id, prev1, prev1_db);
                    let mut value = Vec::new();
                    prev1.serialize(&mut value)?;
                    db.prev1_block_db.put(id, &value)?;
                    if handle.set_prev1() {
                        db.store_block_handle(&handle, None)?;
                    }
                }
            },
            Err(e) => {
                log::warn!("Block {} has no prev1 in db ({}), restore", e, id);
                let mut value = Vec::new();
                prev1.serialize(&mut value)?;
                db.prev1_block_db.put(id, &value)?;
                if handle.set_prev1() {
                    db.store_block_handle(&handle, None)?;
                }
            }
        }
        if !handle.has_prev1() {
            log::warn!("Applied block {} has handle.has_prev1() false", id);
            handle.set_prev1();
            db.store_block_handle(&handle, None)?;
        }

        // prev 2 (after merge)
        if let Some(prev2) = prev2 {
            match db.load_block_prev2(id) {
                Ok(prev2_db) => {
                    if prev2_db != prev2 {
                        log::warn!("Block {} has real prev {}, but in db {}, restore", id, prev2, prev2_db);
                        let mut value = Vec::new();
                        prev1.serialize(&mut value)?;
                        db.prev1_block_db.put(id, &value)?;
                        if handle.set_prev2() {
                            db.store_block_handle(&handle, None)?;
                        }
                    }
                },
                Err(e) => {
                    log::warn!("Block {} has no prev2 in db ({}), restore", e, id);
                    let mut value = Vec::new();
                    prev1.serialize(&mut value)?;
                    db.prev1_block_db.put(id, &value)?;
                    if handle.set_prev2() {
                        db.store_block_handle(&handle, None)?;
                    }
                }
            }
            if !handle.has_prev2() {
                log::warn!("Applied block {} has handle.has_prev2() false", id);
                handle.set_prev2();
                db.store_block_handle(&handle, None)?;
            }
        }
    }

    if should_has_next {
        // next 1
        match db.load_block_next1(id) {
            Ok(_) => {
                if !handle.has_next1() {
                    log::warn!("Applied block {} has handle.has_next1() false", id);
                    handle.set_next1();
                    db.store_block_handle(&handle, None)?;
                }
            },
            Err(e) => fail!("Applied block {} doesn't have next1 ({})", id, e)
        }

        // next 2 (before split)
        if block.block().read_info()?.before_split() {
            match db.load_block_next2(id) {
                Ok(_) => {
                    if !handle.has_next2() {
                        log::warn!("Applied block is before split, but {} has handle.has_next2() false", id);
                        handle.set_next2();
                        db.store_block_handle(&handle, None)?;
                    }
                },
                Err(e) => fail!("Applied block {} is before split, but doesn't have next2 ({})", id, e)
            }
        }
    }

    Ok(block)
}

async fn calc_min_mc_state_id(
    db: &InternalDb,
    shard_client_mc_block_id: &BlockIdExt
) -> Result<BlockIdExt> {
    log::trace!("calc_min_mc_state_id");

    let pss_keeper = db.load_full_node_state(PSS_KEEPER_MC_BLOCK)?.ok_or_else(
        || error!("INTERNAL ERROR: No PSS keeper MC block id when apply block")
    )?;
    let mut min_id: &BlockIdExt = if shard_client_mc_block_id.seq_no() < pss_keeper.seq_no() { 
        &shard_client_mc_block_id
    } else { 
        &pss_keeper
    };
    let last_rotation_block_id = db.load_validator_state(LAST_ROTATION_MC_BLOCK)?;
    if let Some(id) = &last_rotation_block_id {
        if id.seq_no() < min_id.seq_no() {
            if min_id.seq_no() - id.seq_no() < 10000 {
                min_id = &id;
            } else {
                db.drop_validator_state(LAST_ROTATION_MC_BLOCK)?;
            }
        }
    }

    match db.load_shard_state_dynamic(min_id) {
        Err(e) => {
            log::warn!(
                "calc_min_mc_state_id: can't load state {} {}. Will use it as min",
                e, min_id
            );
            Ok(min_id.clone())
        }
        Ok(mc_state) => {
            let new_min_ref_mc_seqno = mc_state.state().min_ref_mc_seqno();

            match db.find_mc_block_by_seq_no(new_min_ref_mc_seqno) {
                Err(e) => {
                    log::warn!(
                        "calc_min_mc_state_id: can't find_mc_block_by_seq_no {} {}. Will use {} as min",
                        e, new_min_ref_mc_seqno, min_id
                    );
                    Ok(min_id.clone())
                }
                Ok(handle) => {

                    log::trace!("calc_min_mc_state_id: {}", handle.id());
                    Ok(handle.id().clone())
                }
            }
        }
    }
}

fn check_state(
    db: &InternalDb,
    id: &BlockIdExt,
    checked_cells: &mut HashSet<UInt256>,
    check_stop: &(dyn Fn() -> Result<()> + Sync),
) -> Result<()> {
    log::trace!("check_state {}", id);

    fn check_cell(
        cell: Cell,
        checked_cells: &mut HashSet<UInt256>,
        check_stop: &(dyn Fn() -> Result<()> + Sync),
    ) -> Result<(u64, u64)> {
        check_stop()?;
        const MAX_56_BITS: u64 = 0x00FF_FFFF_FFFF_FFFFu64;
        let mut expected_cells = 1_u64;
        let mut expected_bits = cell.bit_length() as u64;
        let new_cell = checked_cells.insert(cell.repr_hash());
        for i in 0..cell.references_count() {
            let child = cell.reference(i)?;
            if new_cell {
                let (c, b) = check_cell(child, checked_cells, check_stop)?;
                expected_cells = expected_cells.saturating_add(c);
                expected_bits = expected_bits.saturating_add(b);
            } else {
                expected_cells = expected_cells.saturating_add(child.tree_cell_count());
                expected_bits = expected_bits.saturating_add(child.tree_bits_count());
            }
            if expected_bits > MAX_56_BITS {
                expected_bits = MAX_56_BITS;
            }
            if expected_cells > MAX_56_BITS {
                expected_cells = MAX_56_BITS;
            }
        }

        if cell.tree_cell_count() != expected_cells {
            fail!("cell {:x} stored cell count {} != expected {}", 
                cell.repr_hash(), cell.tree_cell_count(), expected_cells);
        }
        if cell.tree_bits_count() != expected_bits {
            fail!("cell {} stored bit count {} != expected {}", 
                cell.repr_hash(), cell.tree_bits_count(), expected_bits);
        }
        Ok((expected_cells, expected_bits))
    }

    let root = db.shard_state_dynamic_db.get(id)?;
    check_cell(root, checked_cells, check_stop)?;

    Ok(())
}

async fn restore_states(
    db: &InternalDb,
    persistent_state_handle: &BlockHandle,
    min_mc_state_id: &BlockIdExt,
    processed_wc: i32,
    check_stop: &(dyn Fn() -> Result<()> + Sync),
) -> Result<()> {
    log::trace!("restore_states");

    if persistent_state_handle.id().seq_no() > min_mc_state_id.seq_no() {
        fail!("restore_states: min mc state can't be older persistent state");
    }

    // create list of mc and shard ids by min_mc_state_id
    let min_mc_handle = db.load_block_handle(&min_mc_state_id)?
        .ok_or_else(|| error!("there is no handle for block {}", min_mc_state_id))?;
    let min_mc_block = db.load_block_data(&min_mc_handle).await?;
    let mut min_stored_states = min_mc_block.shard_hashes()?.top_blocks(&[processed_wc])?;
    min_stored_states.push(min_mc_state_id.clone());

    // master chain
    let mc_state = db.load_shard_state_persistent(persistent_state_handle.id()).await?;
    let next_id = db.load_block_next1(mc_state.block_id())?;
    restore_chain(
        db,
        mc_state.root_cell().clone(),
        next_id,
        &min_stored_states,
        false,
        check_stop,
    ).await?;

    // shards
    let mut after_merge = HashMap::new();
    for id in mc_state.top_blocks(processed_wc)? {
        let pers_root = db.load_shard_state_persistent(&id).await?.root_cell().clone();

        if let Ok(next_id2) = db.load_block_next2(&id) {
            run_chain_restore(
                db, 
                pers_root.clone(), 
                next_id2, 
                &min_stored_states, 
                //&max_stored_states, 
                &mut after_merge,
                check_stop
            ).await?;
        }

        let next_id1 = db.load_block_next1(&id)?;
        run_chain_restore(
            db,
            pers_root,
            next_id1,
            &min_stored_states,
            //&max_stored_states,
            &mut after_merge,
            check_stop
        ).await?;
    }

    Ok(())
}

// Calls restore_chain for given states and it processes merges next way:
// - if we have other child (in after_merge map) - call restore_chain for merged root
// - if not - add first child's root into after_merge map
async fn run_chain_restore(
    db: &InternalDb,
    mut prev_state_root: Cell,
    mut block_id: BlockIdExt,
    min_stored_states: &[BlockIdExt],
    //max_stored_states: &[BlockIdExt],
    after_merge: &mut HashMap<BlockIdExt, Cell>,
    check_stop: &(dyn Fn() -> Result<()> + Sync),
) -> Result<()> {
    let mut store_states = false;
    loop {
        match restore_chain(
            db,
            prev_state_root.clone(),
            block_id,
            min_stored_states,
            //max_stored_states,
            store_states,
            check_stop,
        ).await? {
            None => break,
            Some((root1, store_states1, block_id1, next_block_id1)) => {
                if let Some(other) = after_merge.remove(&next_block_id1) {
                    prev_state_root = if block_id1.shard().is_left_child() {
                        ShardStateStuff::construct_split_root(root1, other)?
                    } else {
                        ShardStateStuff::construct_split_root(other, root1)?
                    };
                    block_id = next_block_id1;
                    store_states = store_states1;
                } else {
                    after_merge.insert(next_block_id1, root1);
                    return Ok(())
                }
            }
        }
    }
    Ok(())
}

// Restores all states from prev_state_root by merkle updates in blocks.
// Processes splits by recurdion. 
// Returns None if there is no next block, and Some in a merge.
#[async_recursion::async_recursion]
async fn restore_chain(
    db: &InternalDb,
    mut prev_state_root: Cell,
    mut block_id: BlockIdExt,
    min_stored_states: &[BlockIdExt],
    //max_stored_states: &[BlockIdExt],
    mut store_states: bool,
    check_stop: &(dyn Fn() -> Result<()> + Sync),
) -> Result<Option<(Cell, bool, BlockIdExt, BlockIdExt)>> {
    log::trace!("restore_chain: {}, store_states {}", block_id, store_states);
    loop {
        check_stop()?;
        // load block
        let block_handle = db.load_block_handle(&block_id)?.ok_or_else(
            || error!("Cannot load handle {}", block_id)
        )?;
        let block = db.load_block_data(&block_handle).await?;

        // apply merkle update
        let merkle_update = block.block().read_state_update()?;
        let block_id_ = block_id.clone();
        let state_root = tokio::task::spawn_blocking(
            move || -> Result<Cell> {
                let now = std::time::Instant::now();
                let root = merkle_update.apply_for(&prev_state_root)?;
                log::trace!("TIME: restore_chain: applied Merkle update {}ms   {}",
                    now.elapsed().as_millis(), block_id_);
                Ok(root)
            }
        ).await??;

        // store state if need
        if min_stored_states.iter().any(|id| *id == block_id) {
            log::debug!("restore_chain {}, store_states becomes true", block_id);
            store_states = true;
        }
        if store_states {
            db.store_shard_state_dynamic_raw_force(
                &block_handle,
                state_root.clone(),
                None,
                check_stop,
            ).await?;
        }

        // if max_stored_states.contains(&block_id) {
        //     log::trace!("restore_chain: max stored state achived {}, return", block_id);
        //     return Ok(None);
        // }

        // analize next block...
        match (db.load_block_next1(&block_id), db.load_block_next2(&block_id)) {
            (Ok(next_id), Err(_)) => {
                if next_id.shard() == block_id.shard() {
                    // next block is not afrer split or merge - continue this loop
                    prev_state_root = state_root;
                    block_id = next_id;
                    continue;
                } else {
                    // next block is after split, return to before split loop (see below)
                    log::debug!("restore_chain: returns - afrer merge block {}", next_id);
                    return Ok(Some((state_root, store_states, block_id, next_id)))
                }
            }
            (Ok(next_id_1), Ok(next_id_2)) => {
                // before split, create two cycles recursively
                let r1 = restore_chain(
                    db,
                    state_root.clone(),
                    next_id_1,
                    min_stored_states,
                    //max_stored_states,
                    store_states,
                    check_stop,
                ).await?;
                let r2 = restore_chain(
                    db,
                    state_root.clone(),
                    next_id_2,
                    min_stored_states,
                    //max_stored_states,
                    store_states,
                    check_stop,
                ).await?;

                match (r1, r2) {
                    (Some((root1, store_states1, _, block_id1)),
                     Some((root2, store_states2, _, block_id2))) => {
                        store_states = store_states1 | store_states2;

                        // afrer merge - check ids and continue this loop

                        if block_id1 != block_id2 {
                            fail!("restore_chain: afrer merge there are two different blocks {} and {}", 
                                block_id1, block_id2);
                        }

                        block_id = block_id1;
                        prev_state_root = ShardStateStuff::construct_split_root(root1, root2)?
                    }
                    (None, None) => {return Ok(None)}
                    (r1, r2) => {
                        fail!("restore_chain: recursive calls returned {:?} and {:?}", r1, r2)
                    }
                }
            }
            _ => {
                log::debug!("restore_chain: returns - no next block for {}", block_id);
                return Ok(None)
            }
        }
    }
}
