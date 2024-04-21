use crate::internal_db::{
    InternalDb, CURRENT_DB_VERSION, DB_VERSION_3, DB_VERSION_4, DB_VERSION_5,
    restore::check_db
};
use std::sync::atomic::AtomicBool;
use ton_types::{Result, fail};

pub async fn update(
    mut db: InternalDb, 
    mut version: u32,
    check_stop: &(dyn Fn() -> Result<()> + Sync),
    is_broken: Option<&AtomicBool>,
    _force_check_db: bool,
    _restore_db_enabled: bool,
) -> Result<InternalDb> {
    if version == CURRENT_DB_VERSION {
        return Ok(db)
    }

    if version < DB_VERSION_3 {
        log::info!(
            "Detected old database version {version}. Need to migrate to latest version"
        );
        db = check_db(db, 0, true, true, check_stop, is_broken).await?;
        db.store_db_version(DB_VERSION_4)?;
        version = DB_VERSION_4;
    } else if version == DB_VERSION_3 {
        log::info!(
            "Detected old database version 3. This version contains performance issue in cells DB. \
            Database will be updated."
        );
        db.update_cells_db_upto_4().await?;
        db.store_db_version(DB_VERSION_4)?;
        version = DB_VERSION_4;
    }

    if version < DB_VERSION_5 {
        log::info!(
            "Detected old database version {version}. Need to migrate to version 5",
        );
        db.migrate_handles_to_v5()?;
        db.store_db_version(DB_VERSION_5)?;
        version = DB_VERSION_5;
    }

    if version != CURRENT_DB_VERSION {
        fail!("Wrong database version {}, supported: {}", version, CURRENT_DB_VERSION);
    }

    Ok(db)
}
