use std::collections::hash_set::HashSet;
use ton_block::{
    Account, InMsg, OutMsg, Deserializable, Serializable, MessageProcessingStatus, Transaction,
    TransactionProcessingStatus, BlockProcessingStatus, Block, BlockProof, HashmapAugType,
    AccountBlock, ShardAccount
};
use ton_types::{
    cells_serialization::serialize_toc, types::UInt256, Cell, Result, SliceData, HashmapType
};

use crate::{
    engine_traits::ExternalDb, block::BlockStuff, shard_state::ShardStateStuff,
    error::NodeError, external_db::WriteData, block_proof::BlockProofStuff,
};

enum DbRecord {
    Empty,
    Message(String, String),
    Transaction(String, String),
    Account(String, String),
    BlockProof(String, String),
    Block(String, String),
    RawBlock(Vec<u8>, Vec<u8>)
}

pub(super) struct Processor<T: WriteData> {
    write_block: T,
    write_raw_block: T,
    write_message: T,
    write_transaction: T,
    write_account: T,
    write_block_proof: T,
    bad_blocks_storage: String,
}

impl<T: WriteData> Processor<T> {

    pub fn new(
        write_block: T,
        write_raw_block: T,
        write_message: T,
        write_transaction: T,
        write_account: T,
        write_block_proof: T,
        bad_blocks_storage: String) 
    -> Self {
        Processor{ write_block, write_raw_block, write_message, write_transaction, write_account, write_block_proof, bad_blocks_storage }
    }

    fn prepare_in_message_record(
        in_msg: InMsg, 
        block_root: &Cell, 
        block_id: UInt256,
        add_proof: bool,
    ) -> Result<DbRecord> {
        let transaction_id = in_msg.transaction_cell().map(|cell| cell.repr_hash());
        let transaction_now = in_msg.read_transaction()?.map(|t| t.now);
        let msg = in_msg.read_message()?;
        let cell = in_msg.message_cell()?;
        let boc = serialize_toc(&cell)?;
        let proof = if add_proof {
            Some(serialize_toc(&msg.prepare_proof(true, &block_root)?)?)
        } else {
            None
        };
        let set = ton_block_json::MessageSerializationSet {
            message: msg,
            id: cell.repr_hash(),
            block_id: Some(block_id.clone()),
            transaction_id,
            status: MessageProcessingStatus::Finalized,
            boc,
            proof,
            transaction_now
        };
        let doc = ton_block_json::db_serialize_message("id", &set)?;
        Ok(DbRecord::Message(
            doc["id"].to_string(),
            format!("{:#}", serde_json::json!(doc))
        ))
    }

    fn prepare_out_message_record(
        out_msg: OutMsg, 
        block_root: &Cell, 
        block_id: UInt256,
        add_proof: bool,
    ) -> Result<DbRecord> {
        let transaction_id = out_msg.transaction_cell().map(|cell| cell.repr_hash());
        if let (Some(msg), Some(cell)) = (out_msg.read_message()?, out_msg.message_cell()?) {
            let boc = serialize_toc(&cell)?;
            let proof = if add_proof {
                Some(serialize_toc(&msg.prepare_proof(false, &block_root)?)?)
            } else {
                None
            };
            let set = ton_block_json::MessageSerializationSet {
                message: msg,
                id: cell.repr_hash(),
                block_id: Some(block_id.clone()),
                transaction_id,
                status: MessageProcessingStatus::Finalized,
                boc,
                proof,
                transaction_now: None // actual only for inbuound messages
            };
            let doc = ton_block_json::db_serialize_message("id", &set)?;
            Ok(DbRecord::Message(
                doc["id"].to_string(),
                format!("{:#}", serde_json::json!(doc))
            ))
        } else {
            Ok(DbRecord::Empty)
        }
    }

    fn prepare_transaction_record(
        transaction_slice: SliceData, 
        block_root: &Cell,
        block_id: UInt256,
        workchain_id: i32,
        add_proof: bool,
    ) -> Result<DbRecord> {
        let cell = transaction_slice.reference(0)?.clone();
        let boc = serialize_toc(&cell).unwrap();
        let transaction: Transaction = Transaction::construct_from(&mut cell.clone().into())?;
        let proof = if add_proof {
            Some(serialize_toc(&transaction.prepare_proof(&block_root)?)?)
        } else {
            None
        };
        let set = ton_block_json::TransactionSerializationSet {
            transaction,
            id: cell.repr_hash(),
            status: TransactionProcessingStatus::Finalized,
            block_id: Some(block_id.clone()),
            workchain_id,
            boc,
            proof,
        };
        let doc = ton_block_json::db_serialize_transaction("id", &set)?;
        Ok(DbRecord::Transaction(
            doc["id"].to_string(),
            format!("{:#}", serde_json::json!(doc))
        ))
    }

    fn prepare_account_record(account: Account) -> Result<DbRecord> {
        let boc = serialize_toc(&account.write_to_new_cell()?.into())?;
        let set = ton_block_json::AccountSerializationSet {
            account,
            proof: None,
            boc,
        };

        let doc = ton_block_json::db_serialize_account("id", &set)?;
        Ok(DbRecord::Account(
            doc["id"].to_string(),
            format!("{:#}", serde_json::json!(doc))
        ))
    }

    fn prepare_block_record(
        block: &Block,
        block_root: &Cell,
        block_boc: Vec<u8>,
    ) -> Result<DbRecord> {
        let set = ton_block_json::BlockSerializationSet {
            block: block.clone(),
            id: block_root.repr_hash(),
            status: BlockProcessingStatus::Finalized,
            boc: block_boc,
        };
        let doc = ton_block_json::db_serialize_block("id", &set)?;
        Ok(DbRecord::Block(
            doc["id"].to_string(),
            format!("{:#}", serde_json::json!(doc))
        ))
    }

    fn prepare_raw_block_record(
        block_root: &Cell,
        block_boc: Vec<u8>,
    ) -> Result<DbRecord> {
        Ok(DbRecord::RawBlock(
            block_root.repr_hash().as_slice().to_vec(),
            block_boc
        ))
    }

    fn prepare_block_proof_record(proof: &BlockProof) -> Result<DbRecord> {
        let doc = ton_block_json::db_serialize_block_proof("id", proof)?;
        Ok(DbRecord::BlockProof(
            doc["id"].to_string(),
            format!("{:#}", serde_json::json!(doc))
        ))
    }

    fn process_parsing_error(&self, block: &BlockStuff, e: failure::Error) {
        log::error!(
            "Error while parsing block (before put into kafka): {}   block: {}", 
            block.id(), e);
        let root = std::path::Path::new(&self.bad_blocks_storage);
        let name = block.id().to_string();
        let _ = std::fs::create_dir_all(root.clone());
        match std::fs::write(root.join(std::path::Path::new(&name)), block.data()) {
            Ok(_) => log::error!(
                "Bad block {}, saved into {}",
                block.id(),
                root.join(std::path::Path::new(&name)).to_str().unwrap_or_default()),
            Err(e) => log::error!(
                "Bad block {}, error while saving into {}: {}",
                block.id(),
                root.join(std::path::Path::new(&name)).to_str().unwrap_or_default(),
                e),
        };
    }


    pub async fn process_block_impl(
        &self, 
        block_stuff: &BlockStuff,
        block_proof: Option<&BlockProofStuff>,
        state: Option<&ShardStateStuff>,
        add_proof: bool,
     ) -> Result<()> {

        let process_block = self.write_block.enabled();
        let process_raw_block = self.write_raw_block.enabled();
        let process_message = self.write_message.enabled();
        let process_transaction = self.write_transaction.enabled();
        let process_account = self.write_account.enabled();
        let process_block_proof = self.write_block_proof.enabled();
        let block_id = block_stuff.id().clone();
        let block = block_stuff.block().clone();
        let proof = block_proof.map(|p| p.proof().clone());
        let block_root = block_stuff.root_cell().clone();
        let block_extra = block.read_extra()?;
        let block_boc1 = if process_block { Some(block_stuff.data().to_vec()) } else { None };
        let block_boc2 = if process_raw_block { Some(block_stuff.data().to_vec()) } else { None };
        let shard_accounts = state.map(|s| s.shard_state().read_accounts()).transpose()?;

        let now = std::time::Instant::now();

        let db_records = tokio::task::spawn_blocking(move || {

            let mut db_records = Vec::new();

            // Messages
            if process_message {
                let now = std::time::Instant::now();
                let mut msg_count = 0;
                block_extra.read_in_msg_descr()?.iterate_objects(|msg| {
                    msg_count += 1;
                    db_records.push(
                        Self::prepare_in_message_record(msg, &block_root, block_root.repr_hash(), add_proof)?
                    );
                    Ok(true)
                })?;
                log::trace!("TIME: in messages {} {}ms;   {}", msg_count, now.elapsed().as_millis(), block_id);
                let now = std::time::Instant::now();
                let mut msg_count = 0;
                block_extra.read_out_msg_descr()?.iterate_objects(|msg| {
                    match Self::prepare_out_message_record(msg, &block_root, block_root.repr_hash(), add_proof)? {
                        DbRecord::Empty => (),
                        r => {
                            msg_count += 1;
                            db_records.push(r);
                        }
                    }
                    Ok(true)
                })?;
                log::trace!("TIME: out messages {} {}ms;   {}", msg_count, now.elapsed().as_millis(), block_id);
            }
            // Transactions
            let mut changed_acc = HashSet::new();
            if process_transaction || process_account{
                let now = std::time::Instant::now();
                let mut tr_count = 0;
                let workchain_id = block.read_info()?.shard().workchain_id();

                block_extra.read_account_blocks()?.iterate_objects(|account_block: AccountBlock| {
                    let state_upd = account_block.read_state_update()?;
                    if process_account && state_upd.old_hash != state_upd.new_hash {
                        changed_acc.insert(account_block.account_id().clone());
                    }
                    if process_transaction {
                        account_block.transactions().iterate_slices(|_, transaction_slice| {
                            tr_count += 1;
                            db_records.push(
                                Self::prepare_transaction_record(
                                    transaction_slice, &block_root, block_root.repr_hash(), workchain_id, add_proof
                                )?
                            );
                            Ok(true)
                        })?;
                    }
                    Ok(true)
                })?;
                log::trace!("TIME: transactions {} {}ms;   {}", tr_count, now.elapsed().as_millis(), block_id);
            }

            // Accounts (changed only)
            if process_account {
                let now = std::time::Instant::now();
                if let Some(shard_accounts) = shard_accounts {
                    for acc_addr in changed_acc.iter() {
                        let acc = shard_accounts.account(acc_addr)?
                            .ok_or_else(|| 
                                NodeError::InvalidData(
                                    "Block and shard state mismatch: \
                                    state doesn't contain changed account".to_string()
                                )
                            )?;
                        let acc = acc.read_account()?;
                        db_records.push(Self::prepare_account_record(acc)?);
                    }
                }
                log::trace!("TIME: accounts {} {}ms;   {}", changed_acc.len(), now.elapsed().as_millis(), block_id);
            }

            // Block
            if process_block {
                let now = std::time::Instant::now();
                db_records.push(
                    Self::prepare_block_record(&block, &block_root, block_boc1.unwrap())?
                );
                log::trace!("TIME: block {}ms;   {}", now.elapsed().as_millis(), block_id);
            }

            // Block proof
            if process_block_proof {
                if let Some(proof) = proof {
                    let now = std::time::Instant::now();
                    db_records.push(
                        Self::prepare_block_proof_record(&proof)?
                    );
                    log::trace!("TIME: block proof {}ms;   {}", now.elapsed().as_millis(), block_id);
                }
            }

            // raw block
            if process_raw_block {
                let now = std::time::Instant::now();
                db_records.push(
                    Self::prepare_raw_block_record(&block_root, block_boc2.unwrap())?
                );
                log::trace!("TIME: raw block {}ms;   {}", now.elapsed().as_millis(), block_id);
            }

            Ok(db_records)
        }).await;

        log::trace!("TIME: prepare & build jsons {}ms;   {}", now.elapsed().as_millis(), block_stuff.id());

        let now = std::time::Instant::now();

        let db_records: Vec<DbRecord> = match db_records {
            Ok(Err(e)) => {
                self.process_parsing_error(block_stuff, e);
                return Ok(());
            }
            Err(e) => {
                self.process_parsing_error(block_stuff, e.into());
                return Ok(());
            }
            Ok(Ok(db_records)) => db_records
        };

        let mut send_tasks = vec!();
        for record in db_records {
            if let Some(send_task) = match record {
                DbRecord::Message(key, value) => Some(self.write_message.write_data(key, value)),
                DbRecord::Transaction(key, value) => Some(self.write_transaction.write_data(key, value)),
                DbRecord::Account(key, value) => Some(self.write_account.write_data(key, value)),
                DbRecord::Block(key, value) => Some(self.write_block.write_data(key, value)),
                DbRecord::RawBlock(key, value) => Some(self.write_raw_block.write_raw_data(key, value)),
                DbRecord::BlockProof(key, value) => Some(self.write_block_proof.write_data(key, value)),
                DbRecord::Empty => None
            } {
                send_tasks.push(send_task);
            }
        }

        log::trace!("TIME: must be zero {}ms;   {}", now.elapsed().as_millis(), block_stuff.id());

        let now = std::time::Instant::now();

        // Await while all the block stuff been written
        futures::future::join_all(send_tasks)
            .await
            .into_iter()
            .find(|r| r.is_err())
            .unwrap_or(Ok(()))?;

        log::trace!("TIME: sent to kafka {}ms;   {}", now.elapsed().as_millis(), block_stuff.id());

        Ok(())
    }
}

#[async_trait::async_trait]
impl<T: WriteData> ExternalDb for Processor<T> {
    async fn process_block(
        &self, 
        block_stuff: &BlockStuff,
        proof: Option<&BlockProofStuff>, 
        state: &ShardStateStuff
    ) -> Result<()> {
        self.process_block_impl(block_stuff, proof, Some(state), false).await
    }

    async fn process_full_state(&self, state: &ShardStateStuff) -> Result<()> {
        let now = std::time::Instant::now();
        log::trace!("process_full_state {} ...", state.block_id());

        if self.write_account.enabled() {
            let mut accounts = Vec::new();
            state.shard_state().read_accounts()?.iterate_objects(|acc: ShardAccount| {
                let acc = acc.read_account()?;
                let record = Self::prepare_account_record(acc)?;
                accounts.push(record);
                Ok(true)
            })?;

            futures::future::join_all(
                accounts.into_iter().map(|r| {
                    match r {
                        DbRecord::Account(key, value) => self.write_account.write_data(key, value),
                        _ => unreachable!(),
                    }
                })
            )
            .await
            .into_iter()
            .find(|r| r.is_err())
            .unwrap_or(Ok(()))?;
        }

        log::trace!("TIME: process_full_state {}ms;   {}", now.elapsed().as_millis(), state.block_id());
        Ok(())
    }
}
