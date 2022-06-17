use crate::internal_db::{
    InternalDb, CURRENT_DB_VERSION, DB_VERSION_0, DB_VERSION_1, restore::check_db
};
use std::sync::atomic::AtomicBool;
use ton_types::Result;

pub async fn update(
    mut db: InternalDb, 
    mut version: u32,
    check_stop: &(dyn Fn() -> Result<()> + Sync),
    is_broken: Option<&AtomicBool>
) -> Result<InternalDb> {
    if version == CURRENT_DB_VERSION {
        return Ok(db)
    }

    if version == DB_VERSION_0 {
        // 0 -> 1
        log::info!(
            "Detected old database version 0. This version possibly contains wrong cells and bits \
             counters in cells DB. Need to restore database");
        db = check_db(db, 0, true, true, check_stop, is_broken).await?;
        version = DB_VERSION_1;
        db.store_db_version(version)?;
    }

    // future updates...
    // if version == DB_VERSION_1 {
    //
    // }

    Ok(db)
}
