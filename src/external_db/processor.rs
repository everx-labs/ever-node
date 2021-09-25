use std::{collections::hash_set::HashSet, sync::Arc};
use ton_block::{
    Account, InMsg, OutMsg, Deserializable, Serializable, MessageProcessingStatus, Transaction,
    TransactionProcessingStatus, BlockProcessingStatus, Block, BlockProof, HashmapAugType,
    AccountBlock, ShardAccount,
};
use ton_types::{
    cells_serialization::serialize_toc,
    types::UInt256,
    AccountId, Cell, Result, SliceData, HashmapType
};
use serde::Serialize;
use chrono::Utc;

use crate::{
    block::BlockStuff, block_proof::BlockProofStuff, engine::STATSD,
    engine_traits::{ChainRange, ExternalDb}, error::NodeError, external_db::WriteData,
    shard_state::ShardStateStuff
};

lazy_static::lazy_static!(
    static ref ACCOUNT_NONE_HASH: UInt256 = Account::default().serialize().unwrap().repr_hash();
);

enum DbRecord {
    Empty,
    Message(String, String),
    Transaction(String, String),
    Account(String, String),
    BlockProof(String, String),
    Block(String, String),
    RawBlock(Vec<u8>, Vec<u8>, u32)
}

#[derive(Clone, Debug, Serialize)]
struct ChainRangeMasterBlock {
    pub id: String,
    pub seq_no: u32,
}

#[derive(Clone, Debug, Serialize)]
struct ChainRangeData {
    pub master_block: ChainRangeMasterBlock,
    pub shard_blocks_ids: Vec<String>,
}

pub(super) struct Processor<T: WriteData> {
    write_block: T,
    write_raw_block: T,
    write_message: T,
    write_transaction: T,
    write_account: T,
    write_block_proof: T,
    write_chain_range: T,
    bad_blocks_storage: String,
    front_workchain_ids: Vec<i32>, // write only this workchain, or write all if None
}

impl<T: WriteData> Processor<T> {

    pub fn new(
        write_block: T,
        write_raw_block: T,
        write_message: T,
        write_transaction: T,
        write_account: T,
        write_block_proof: T,
        write_chain_range: T,
        bad_blocks_storage: String,
        front_workchain_ids: Vec<i32>,
    ) 
    -> Self {
        log::trace!("Processor::new workchains {:?}", front_workchain_ids);
        Processor {
            write_block,
            write_raw_block,
            write_message,
            write_transaction,
            write_account,
            write_block_proof,
            write_chain_range,
            bad_blocks_storage,
            front_workchain_ids,
        }
    }

    fn process_workchain(&self, workchain_id: i32) -> bool {
        self
            .front_workchain_ids
            .iter()
            .position(|id| id == &workchain_id)
            .is_some()
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
        let boc = serialize_toc(&account.serialize()?.into())?;
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

    fn prepare_deleted_account_record(account_id: AccountId, workchain_id: i32) -> Result<DbRecord> {
        let set = ton_block_json::DeletedAccountSerializationSet {
            account_id,
            workchain_id
        };

        let doc = ton_block_json::db_serialize_deleted_account("id", &set)?;
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
        mc_seq_no: u32,
    ) -> Result<DbRecord> {
        Ok(DbRecord::RawBlock(
            block_root.repr_hash().as_slice().to_vec(),
            block_boc,
            mc_seq_no,
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
        state: Option<&Arc<ShardStateStuff>>,
        mc_seq_no: u32,
        add_proof: bool,
     ) -> Result<()> {

        log::trace!("Processor block_stuff.id {}", block_stuff.id());
        if !self.process_workchain(block_stuff.shard().workchain_id()) {
            return Ok(())
        }
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
        let shard_accounts = state.map(|s| s.state().read_accounts()).transpose()?;

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
            let mut deleted_acc = HashSet::new();
            if process_transaction || process_account{
                let now = std::time::Instant::now();
                let mut tr_count = 0;
                let workchain_id = block.read_info()?.shard().workchain_id();

                block_extra.read_account_blocks()?.iterate_objects(|account_block: AccountBlock| {
                    let state_upd = account_block.read_state_update()?;
                    if process_account && state_upd.old_hash != state_upd.new_hash {
                        if state_upd.new_hash == *ACCOUNT_NONE_HASH {
                            deleted_acc.insert(account_block.account_id().clone());
                        } else {
                            changed_acc.insert(account_block.account_id().clone());
                        }
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
                    for acc_addr in deleted_acc {
                        db_records.push(Self::prepare_deleted_account_record(acc_addr, workchain_id)?);
                    }
                    log::trace!("TIME: accounts {} {}ms;   {}", changed_acc.len(), now.elapsed().as_millis(), block_id);
                    STATSD.timer("accounts_parsing_time", now.elapsed().as_micros() as f64 / 1000f64);
                    STATSD.histogram("parsed_accounts_count", changed_acc.len() as f64);
                }

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
                    Self::prepare_raw_block_record(&block_root, block_boc2.unwrap(), mc_seq_no)?
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
            match record {
                DbRecord::Message(key, value) => send_tasks.push(self.write_message.write_data(key, value, None)),
                DbRecord::Transaction(key, value) => send_tasks.push(self.write_transaction.write_data(key, value, None)),
                DbRecord::Account(key, value) => send_tasks.push(self.write_account.write_data(key, value, None)),
                DbRecord::Block(key, value) => send_tasks.push(self.write_block.write_data(key, value, None)),
                DbRecord::BlockProof(key, value) => send_tasks.push(self.write_block_proof.write_data(key, value, None)),
                DbRecord::RawBlock(key, value, mc_seq_no) =>  send_tasks.push(Box::pin(self.send_raw_block(key, value, mc_seq_no))),
                DbRecord::Empty => {}
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

    async fn send_raw_block(&self, key: Vec<u8>, value: Vec<u8>, mc_seq_no: u32) -> Result<()> {
        let attributes = [
            ("mc_seq_no", &mc_seq_no.to_be_bytes()[..]),
            ("raw_block_timestamp", &Utc::now().timestamp().to_be_bytes()[..])
        ];
        self.write_raw_block.write_raw_data(key, value, Some(&attributes)).await
    }
}

#[async_trait::async_trait]
impl<T: WriteData> ExternalDb for Processor<T> {
    async fn process_block(
        &self, 
        block_stuff: &BlockStuff,
        proof: Option<&BlockProofStuff>, 
        state: &Arc<ShardStateStuff>,
        mc_seq_no: u32,
    ) -> Result<()> {
        self.process_block_impl(block_stuff, proof, Some(state), mc_seq_no, false).await
    }

    async fn process_full_state(&self, state: &Arc<ShardStateStuff>) -> Result<()> {
        let now = std::time::Instant::now();
        log::trace!("process_full_state {} ...", state.block_id());

        if !self.process_workchain(state.block_id().shard().workchain_id()) {
            return Ok(())
        }

        if self.write_account.enabled() {
            let mut accounts = Vec::new();
            state.state().read_accounts()?.iterate_objects(|acc: ShardAccount| {
                let acc = acc.read_account()?;
                let record = Self::prepare_account_record(acc)?;
                accounts.push(record);
                Ok(true)
            })?;

            let accounts_len = accounts.len();
            futures::future::join_all(
                accounts.into_iter().map(|r| {
                    match r {
                        DbRecord::Account(key, value) => self.write_account.write_data(key, value, None),
                        _ => unreachable!(),
                    }
                })
            )
            .await
            .into_iter()
            .find(|r| r.is_err())
            .unwrap_or(Ok(()))?;

            STATSD.timer("full_state_parsing_time", now.elapsed().as_micros() as f64 / 1000f64);
            STATSD.histogram("full_state_accounts_count", accounts_len as f64);
        }

        log::trace!("TIME: process_full_state {}ms;   {}", now.elapsed().as_millis(), state.block_id());
        Ok(())
    }

    fn process_chain_range_enabled(&self) -> bool {
        self.write_chain_range.enabled()
    }

    async fn process_chain_range(&self, range: &ChainRange) -> Result<()> {
        if self.write_chain_range.enabled() {
            let master_block_id = range.master_block.root_hash().to_hex_string();
            let mut data = ChainRangeData {
                master_block: ChainRangeMasterBlock {
                    id: master_block_id.clone(),
                    seq_no: range.master_block.seq_no(),
                },
                shard_blocks_ids: Vec::new(),
            };

            for block in &range.shard_blocks {
                data.shard_blocks_ids.push(block.root_hash().to_hex_string());
            }

            self.write_chain_range.write_data(
                format!("\"{}\"", master_block_id),
                serde_json::to_string(&data)?,
                None
            ).await?;
        }

        Ok(())
    }
}
