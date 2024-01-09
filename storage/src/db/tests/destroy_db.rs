
pub async fn destroy_rocks_db(path: &str, name: &str) -> ton_types::Result<()> {
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    let mut path = std::path::Path::new(path);
    let db_path = path.join(name);
    // Clean up DB
    if db_path.exists() {
        let opts = rocksdb::Options::default();
        loop {
            if let Err(e) = rocksdb::DBWithThreadMode::<rocksdb::MultiThreaded>::destroy(
                &opts, 
                db_path.as_path()
            ) {
                println!("Can't destroy DB: {}", e);
                tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            } else {
                break
            }
        }
    }
    // Clean up DB folder
    if db_path.exists() {
        std::fs::remove_dir_all(db_path).map_err(
            |e| ton_types::error!("Can't clean DB folder: {}", e)
        )?
    }
    // Clean up upper folder if empty
    while path.exists() {
        if std::fs::read_dir(path)?.count() > 0 {
            break
        }
        std::fs::remove_dir_all(path).map_err(
            |e| ton_types::error!("Can't clean DB enclosing folder: {}", e)
        )?;
        path = if let Some(path) = path.parent() {
            path
        } else { 
            break
        }
    }
    Ok(())
}
