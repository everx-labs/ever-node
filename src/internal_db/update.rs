use crate::internal_db::{
    InternalDb, CURRENT_DB_VERSION, DB_VERSION_0, DB_VERSION_2, DB_VERSION_3, DB_VERSION_4,
    restore::check_db
};
use std::sync::atomic::AtomicBool;
use ton_types::{Result, fail};

pub async fn update(
    mut db: InternalDb, 
    version: u32,
    check_stop: &(dyn Fn() -> Result<()> + Sync),
    is_broken: Option<&AtomicBool>,
    force_check_db: bool,
    restore_db_enabled: bool,
) -> Result<InternalDb> {
    if version == CURRENT_DB_VERSION {
        return Ok(db)
    }

    if version < DB_VERSION_2 {
        if version == DB_VERSION_0 {
            log::info!(
                "Detected old database version 0. This version possibly contains wrong cells and bits \
                counters in cells DB. Need to restore database"
            );
        } else {
            log::info!(
               "Detected old database version {}. This version contains shard states DB \
                in old format. Async shard states DB has another format. Need to refill \
                DB using restore procedure.", version
            );
        }
        if let Err(e) = db.clean_shard_state_dynamic_db() {
            log::warn!("Clear shard state db: {}", e);
        }
        db = check_db(db, 0, true, true, check_stop, is_broken).await?;
        db.store_db_version(DB_VERSION_3)?;
    } else if version == DB_VERSION_2 {
        log::info!(
            "Detected old database version 2. This version contains performance issue in cells DB. \
            Database will update on the fly."
        );
        db = check_db(db, 0, restore_db_enabled, force_check_db, check_stop, is_broken).await?;
        db.store_db_version(DB_VERSION_3)?;
    } else if version == DB_VERSION_3 {
        log::info!(
            "Detected old database version 3. This version contains performance issue in cells DB. \
            Database will be updated."
        );
        db.update_cells_db_upto_4().await?;
        db.store_db_version(DB_VERSION_4)?;
    } else if version != CURRENT_DB_VERSION {
        fail!("Wrong database version {}, supported: {}", version, CURRENT_DB_VERSION);
    }

    Ok(db)
}
