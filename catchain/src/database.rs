pub use super::*;
use ton_node_storage::catchain_persistent_db::*;

/*
    Implementation details for Database
*/

pub struct DatabaseImpl {
    pub db_path: String,          //path to database
    pub db: CatchainPersistentDb, //persistent storage
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

    /*
        Blocks management
    */

    fn is_block_in_db(&self, hash: &BlockHash) -> bool {
        match self.db.contains(&hash) {
            Ok(status) => status,
            _ => false,
        }
    }

    fn get_block(&self, hash: &BlockHash) -> Result<BlockPayload> {
        match self.db.get(hash) {
            Ok(ref data) => Ok(ton_api::ton::bytes(data.as_ref().to_vec())),
            Err(err) => bail!("Block {} not found: {:?}", hash, err),
        }
    }

    fn put_block(&self, hash: &BlockHash, data: BlockPayload) {
        match self.db.put(&hash, &data) {
            Err(err) => error!("Block {} DB saving error: {:?}", hash, err),
            _ => (),
        }
    }

    fn erase_block(&self, hash: &BlockHash) {
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
        debug!("Dropping Catchain database...");

        self.destroy_database();
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

    pub(crate) fn create(path: &String) -> DatabasePtr {
        debug!("Creating catchain DB at path '{}'", path);

        let db = CatchainPersistentDb::with_path(path);

        Rc::new(Self {
            db_path: path.clone(),
            db: db,
        })
    }
}
