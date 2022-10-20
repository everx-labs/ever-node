use crate::internal_db::{
    InternalDb, CURRENT_DB_VERSION, DB_VERSION_0, restore::check_db
};
#[cfg(feature = "async_ss_storage")]
use crate::internal_db::DB_VERSION_2;
#[cfg(not(feature = "async_ss_storage"))]
use crate::internal_db::DB_VERSION_1;
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

    #[cfg(not(feature = "async_ss_storage"))]
    if version == DB_VERSION_0 {
        // 0 -> 1
        log::info!(
            "Detected old database version 0. This version possibly contains wrong cells and bits \
             counters in cells DB. Need to restore database");
        db = check_db(db, 0, true, true, check_stop, is_broken).await?;
        version = DB_VERSION_1;
        db.store_db_version(version)?;
    }

    #[cfg(feature = "async_ss_storage")]
    if version < DB_VERSION_2 {
        if version == DB_VERSION_0 {
            log::info!(
                "Detected old database version 0. This version possibly contains wrong cells and bits \
                counters in cells DB. Need to restore database"
            );
        }
        log::info!(
            "Detected old database version {}. This version contains shard states DB \
             in old format. Async shard states DB has another format. Need to refill \
             DB using restore procedure.", version
        );
        if let Err(e) = db.clean_shard_state_dynamic_db() {
            log::warn!("Clear shard state db: {}", e);
        }
        db = check_db(db, 0, true, true, check_stop, is_broken).await?;
        version = DB_VERSION_2;
        db.store_db_version(version)?;
    }

    Ok(db)
}
