pub use super::*;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use ton_node_storage::catchain_persistent_db::*;

/*
    Implementation details for Database
*/

pub struct DatabaseImpl {
    db_path: String,                                //path to database
    db: CatchainPersistentDb,                       //persistent storage
    put_tx_counter: metrics_runtime::data::Counter, //DB put transactions counter
    get_tx_counter: metrics_runtime::data::Counter, //DB get transactions counter
    destroy_db: Arc<AtomicBool>,                    //DB should be destroyed at drop
}

/*
    Implementation for public Database trait
*/

impl Database for DatabaseImpl {
    /*
        Database management
    */

    fn get_db_path(&self) -> &String {
        &self.db_path
    }

    fn destroy(&self) {
        self.destroy_db.store(true, Ordering::SeqCst);
    }

    /*
        Blocks management
    */

    fn is_block_in_db(&self, hash: &BlockHash) -> bool {
        instrument!();

        match self.db.contains(&hash) {
            Ok(status) => status,
            _ => false,
        }
    }

    fn get_block(&self, hash: &BlockHash) -> Result<RawBuffer> {
        check_execution_time!(10000);
        instrument!();

        self.get_tx_counter.increment();

        match self.db.get(hash) {
            Ok(ref data) => Ok(ton_api::ton::bytes(data.as_ref().to_vec())),
            Err(err) => bail!("Block {} not found: {:?}", hash, err),
        }
    }

    fn put_block(&self, hash: &BlockHash, data: RawBuffer) {
        check_execution_time!(10000);
        instrument!();

        self.put_tx_counter.increment();

        match self.db.put(&hash, &data) {
            Err(err) => error!("Block {} DB saving error: {:?}", hash, err),
            _ => (),
        }
    }

    fn erase_block(&self, hash: &BlockHash) {
        check_execution_time!(10000);
        instrument!();

        match self.db.delete(&hash) {
            Err(err) => warn!("Block {} DB erasing error: {:?}", hash, err),
            _ => (),
        }
    }
}

/*
    Drop implementation for Database
*/

impl Drop for DatabaseImpl {
    fn drop(&mut self) {
        instrument!();

        debug!("Dropping Catchain database...");

        if self.destroy_db.load(Ordering::SeqCst) {
            debug!("Destroying DB at path '{}'", self.db_path);
            self.destroy_database();
        }

        debug!("Catchain database has been successfully dropped");
    }
}

/*
    Private DatabaseImpl details
*/

impl DatabaseImpl {
    fn destroy_database(&mut self) {
        match self.db.destroy() {
            Err(err) => error!("Database {} destroying error: {:?}", self.db_path, err),
            _ => (),
        }
    }

    pub(crate) fn create(
        path: &String,
        metrics_receiver: &metrics_runtime::Receiver,
    ) -> DatabasePtr {
        debug!("Creating catchain DB at path '{}'", path);

        let put_tx_counter = metrics_receiver.sink().counter("db_put_txs");
        let get_tx_counter = metrics_receiver.sink().counter("db_get_txs");
        let db = CatchainPersistentDb::with_path(path);

        Arc::new(Self {
            db_path: path.clone(),
            db: db,
            put_tx_counter: put_tx_counter,
            get_tx_counter: get_tx_counter,
            destroy_db: Arc::new(AtomicBool::new(false)),
        })
    }
}
