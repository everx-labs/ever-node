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
    check_execution_time, instrument, BlockHash, Database, DatabasePtr, RawBuffer,
    utils::MetricsHandle
};
use metrics::Recorder; 
use std::{path::Path, sync::{Arc, atomic::{AtomicBool, Ordering}}};
use storage::catchain_persistent_db::CatchainPersistentDb;
use ton_types::{fail, Result};

/*
    Implementation details for Database
*/

pub struct DatabaseImpl {
    db: CatchainPersistentDb,                       //persistent storage
    put_tx_counter: metrics::Counter, //DB put transactions counter
    get_tx_counter: metrics::Counter, //DB get transactions counter
    destroy_db: Arc<AtomicBool>,                    //DB should be destroyed at drop
}

/*
    Implementation for public Database trait
*/

impl Database for DatabaseImpl {
    /*
        Database management
    */

    /// Return path to db
    fn get_db_path(&self) -> &Path {
        self.db.path()
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
        check_execution_time!(50000);
        instrument!();

        self.get_tx_counter.increment(1);

        match self.db.get(hash) {
            Ok(ref data) => Ok(ton_api::ton::bytes(data.as_ref().to_vec())),
            Err(err) => fail!("Block {} not found: {:?}", hash, err),
        }
    }

    fn put_block(&self, hash: &BlockHash, data: RawBuffer) {
        check_execution_time!(50000);
        instrument!();

        self.put_tx_counter.increment(1);

        match self.db.put(&hash, &data) {
            Err(err) => log::error!("Block {} DB saving error: {:?}", hash, err),
            _ => (),
        }
    }

    fn erase_block(&self, hash: &BlockHash) {
        check_execution_time!(50000);
        instrument!();

        match self.db.delete(&hash) {
            Err(err) => log::warn!("Block {} DB erasing error: {:?}", hash, err),
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

        log::debug!("Dropping Catchain database...");

        if self.destroy_db.load(Ordering::SeqCst) {
            log::debug!("Destroying DB at path '{}'", self.get_db_path().display());
            self.destroy_database();
        }

        log::debug!("Catchain database has been successfully dropped");
    }
}

/*
    Private DatabaseImpl details
*/

impl DatabaseImpl {
    fn destroy_database(&mut self) {
        if let Err(err) = self.db.destroy() {
            log::error!("cannot destroy catchain db: {}", err)
        }
    }

    pub(crate) fn create(
        path: &str,
        name: &str,
        metrics_receiver: &MetricsHandle
    ) -> Result<DatabasePtr> {
        log::debug!("Creating catchain table in DB at path '{}'", path);

        let put_tx_counter = metrics_receiver.sink().register_counter(&"db_put_txs".into());
        let get_tx_counter = metrics_receiver.sink().register_counter(&"db_get_txs".into());
        let db = CatchainPersistentDb::with_path(path, name)?;

        let ret = Self {
            db,
            put_tx_counter,
            get_tx_counter,
            destroy_db: Arc::new(AtomicBool::new(false)),
        };
        Ok(Arc::new(ret))
    }
}
