use crate::internal_db::{
    InternalDb, CURRENT_DB_VERSION, DB_VERSION_5, DB_VERSION_4,
};
use std::sync::atomic::AtomicBool;
use ever_block::{Result, fail};

pub async fn update(
    db: InternalDb, 
    mut version: u32,
    _check_stop: &(dyn Fn() -> Result<()> + Sync),
    _is_broken: Option<&AtomicBool>,
    _force_check_db: bool,
    _restore_db_enabled: bool,
) -> Result<InternalDb> {
    if version == CURRENT_DB_VERSION {
        return Ok(db)
    }

    if version < DB_VERSION_4 {
        fail!("Detected tool old database version {version}. Last supported version is {DB_VERSION_4}. \
            Please stop the node and clean database.", );
    }
    if version == DB_VERSION_4 {
        log::info!("Detected old database version {version}. Migration to v5 will take some time...");
        db.migrate_handles_to_v5()?;
        db.store_db_version(DB_VERSION_5)?;
        version = DB_VERSION_5;
    }
    if version == DB_VERSION_5 {
        log::info!("Detected old database version {version}. Migration to v6 will take some time...");

        db.update_cells_db_to_v6().await?;

        db.store_db_version(CURRENT_DB_VERSION)?;
        version = CURRENT_DB_VERSION;
    }

    if version != CURRENT_DB_VERSION {
        fail!("Wrong database version {}, supported: {}", version, CURRENT_DB_VERSION);
    }

    Ok(db)
}
