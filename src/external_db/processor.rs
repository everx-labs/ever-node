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

use std::{
    collections::{hash_set::HashSet, BTreeMap, HashMap},
    convert::TryInto,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH}
};
use ton_block::{
    Account, AccountBlock, Block, BlockIdExt,
    BlockProcessingStatus, BlockProof, Deserializable,
    HashmapAugType, Message, MessageProcessingStatus,
    Serializable, ShardAccount, Transaction, TransactionProcessingStatus,
    MsgAddressInt, MsgAddrStd,
};
use ton_types::{
    cells_serialization::serialize_toc,
    types::UInt256,
    AccountId, Cell, Result, SliceData, HashmapType,
    fail, BuilderData,
};
use serde::Serialize;
use serde_json::{Map, Value};

use crate::{
    block::BlockStuff, block_proof::BlockProofStuff, engine::STATSD,
    engine_traits::{ChainRange, ExternalDb}, error::NodeError, external_db::WriteData,
    shard_state::ShardStateStuff
};

lazy_static::lazy_static!(
    static ref ACCOUNT_NONE_HASH: UInt256 = Account::default().serialize().unwrap().repr_hash();
    static ref MINTER_ADDRESS: MsgAddressInt = 
        MsgAddressInt::AddrStd(MsgAddrStd::with_address(None, -1, [0; 32].into()));
);

enum DbRecord {
    Empty,
    Message(String, String),
    Transaction(String, String),
    Account(String, String, Option<u32>),
    BlockProof(String, String, Option<u32>),
    Block(String, String),
    RawBlock([u8; 32], [u8; 32], Vec<u8>, u32, Option<u32>)
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
    max_account_bytes_size: Option<usize>,
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
        max_account_bytes_size: Option<usize>,
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
            max_account_bytes_size,
        }
    }

    fn process_workchain(&self, workchain_id: i32) -> bool {
        self
            .front_workchain_ids
            .iter()
            .position(|id| id == &workchain_id)
            .is_some()
    }

    fn calc_account_partition_key(sharding_depth: u32, mut partitioning_info: SliceData) -> Result<Option<u32>> {
        if sharding_depth > 0 {
            let partition_key = partitioning_info.get_next_u32()?;
            Ok(Some(partition_key >> (32 - sharding_depth)))
        } else {
            Ok(None)
        }
    }

    fn prepare_messages_from_transaction(
        transaction: &Transaction,
        block_id: UInt256,
        tr_chain_order: &str,
        block_root_for_proof: Option<&Cell>,
        messages: &mut HashMap<UInt256, Map<String, Value>>,
    ) -> Result<()> {
        if let Some(message_cell) = transaction.in_msg_cell() {
            let message = Message::construct_from_cell(message_cell.clone())?;
            let message_id = message_cell.repr_hash();
            let mut doc = 
                if message.is_inbound_external() || message.src_ref() == Some(&MINTER_ADDRESS) {
                    Self::prepare_message_json(
                        message_cell,
                        message,
                        block_root_for_proof,
                        block_id.clone(),
                        Some(transaction.now()),
                    )?
                } else {
                    messages.remove(&message_id)
                        .unwrap_or_else(|| {
                            let mut doc = Map::with_capacity(2);
                            doc.insert("id".into(), message_id.as_hex_string().into());
                            doc
                        })
                };
    
            doc.insert(
                "dst_chain_order".into(),
                format!("{}{}", tr_chain_order, ton_block_json::u64_to_string(0)).into());
    
            messages.insert(message_id, doc);
        };
    
        let mut index: u64 = 1;
        transaction.out_msgs.iterate_slices(|slice| {
            let message_cell = slice.reference(0)?;
            let message_id = message_cell.repr_hash();
            let message = Message::construct_from_cell(message_cell.clone())?;
            let mut doc = Self::prepare_message_json(
                message_cell,
                message,
                block_root_for_proof,
                block_id.clone(),
                None, // transaction_now affects ExtIn messages only
            )?;
    
            // messages are ordered by created_lt
            doc.insert(
                "src_chain_order".into(),
                format!("{}{}", tr_chain_order, ton_block_json::u64_to_string(index)).into());
    
            index += 1;
            messages.insert(message_id, doc);
            Ok(true)
        })?;
    
        Ok(())
    }

    fn prepare_message_json(
        message_cell: Cell,
        message: Message,
        block_root_for_proof: Option<&Cell>,
        block_id: UInt256,
        transaction_now: Option<u32>,
    ) -> Result<Map<String, Value>> {
        let boc = serialize_toc(&message_cell)?;
        let proof = block_root_for_proof
            .map(|cell| serialize_toc(&message.prepare_proof(true, cell)?))
            .transpose()?;
    
        let set = ton_block_json::MessageSerializationSet {
            message,
            id: message_cell.repr_hash(),
            block_id: Some(block_id.clone()),
            transaction_id: None, // it would be ambiguous for internal or replayed messages
            status: MessageProcessingStatus::Finalized,
            boc,
            proof,
            transaction_now, // affects ExtIn messages only
        };
        let doc = ton_block_json::db_serialize_message("id", &set)?;
        Ok(doc)
    }

    fn prepare_transaction_json(
        tr_cell: Cell, 
        transaction: Transaction,
        block_root: &Cell,
        block_id: UInt256,
        workchain_id: i32,
        add_proof: bool,
    ) -> Result<Map<String, Value>> {
        let boc = serialize_toc(&tr_cell).unwrap();
        let proof = if add_proof {
            Some(serialize_toc(&transaction.prepare_proof(&block_root)?)?)
        } else {
            None
        };
        let set = ton_block_json::TransactionSerializationSet {
            transaction,
            id: tr_cell.repr_hash(),
            status: TransactionProcessingStatus::Finalized,
            block_id: Some(block_id.clone()),
            workchain_id,
            boc,
            proof,
            ..Default::default()
        };
        let doc = ton_block_json::db_serialize_transaction("id", &set)?;
        Ok(doc)
    }

    fn prepare_account_record(
        account: Account,
        prev_account_state: Option<Account>,
        sharding_depth: u32,
        max_account_bytes_size: Option<usize>,
        last_trans_chain_order: Option<String>,
    ) -> Result<DbRecord> {
        if let Some(max_size) = max_account_bytes_size {
            let size = account.storage_info()
                .map(|si| si.used().bits() / 8)
                .unwrap_or_else(|| 0) as usize;
            if max_size < size {
                log::warn!(
                    "Too big account ({}, {} bytes), skipped",
                    account.get_addr().map(|a| a.to_string()).unwrap_or_else(|| "unknown".into()),
                    size
                );
                return Ok(DbRecord::Empty)
            }
        }
        
        let mut boc1 = None;
        if account.init_code_hash().is_some() {
            // new format
            let mut builder = BuilderData::new();
            account.write_original_format(&mut builder)?;
            boc1 = Some(serialize_toc(&builder.into_cell()?)?);
        }
        let boc = serialize_toc(&account.serialize()?.into())?;

        let account_id = match account.get_id() {
            Some(id) => id,
            None => fail!("Account without id in external db processor")
        };
        let set = ton_block_json::AccountSerializationSet {
            account,
            prev_account_state,
            proof: None,
            boc,
            boc1,
            ..Default::default()
        };
        
        let partition_key = Self::calc_account_partition_key(sharding_depth, account_id.clone())?;
        let mut doc = ton_block_json::db_serialize_account("id", &set)?;
        if let Some(last_trans_chain_order) = last_trans_chain_order {
            doc.insert("last_trans_chain_order".to_owned(), last_trans_chain_order.into());
        }
        Ok(DbRecord::Account(
            doc["id"].to_string(),
            format!("{:#}", serde_json::json!(doc)),
            partition_key,
        ))
    }

    fn prepare_deleted_account_record(
        account_id: AccountId,
        workchain_id: i32,
        sharding_depth: u32,
        prev_account_state: Option<Account>,
        last_trans_chain_order: Option<String>,
    ) -> Result<DbRecord> {
        let partition_key = Self::calc_account_partition_key(sharding_depth, account_id.clone())?;
        let set = ton_block_json::DeletedAccountSerializationSet {
            account_id,
            workchain_id,
            prev_account_state,
            ..Default::default()
        };

        let mut doc = ton_block_json::db_serialize_deleted_account("id", &set)?;
        if let Some(last_trans_chain_order) = last_trans_chain_order {
            doc.insert("last_trans_chain_order".to_owned(), last_trans_chain_order.into());
        }
        Ok(DbRecord::Account(
            doc["id"].to_string(),
            format!("{:#}", serde_json::json!(doc)),
            partition_key,
        ))
    }

    fn prepare_block_record(
        block: &Block,
        block_root: &Cell,
        boc: &[u8],
        file_hash: &UInt256,
        block_order: String,
    ) -> Result<DbRecord> {
        let set = ton_block_json::BlockSerializationSetFH {
            block,
            id: &block_root.repr_hash(),
            status: BlockProcessingStatus::Finalized,
            boc,
            file_hash: Some(file_hash),
        };
        let mut doc = ton_block_json::db_serialize_block("id", set)?;
        doc.insert("chain_order".to_owned(), block_order.into());
        Ok(DbRecord::Block(
            doc["id"].to_string(),
            format!("{:#}", serde_json::json!(doc))
        ))
    }

    fn get_block_partition_key(
        block_id: &BlockIdExt,
        sharding_depth: u32,
    ) -> Option<u32> {
        if sharding_depth > 0 {
            let partitioning_info = u64::from_be_bytes(block_id.root_hash.as_slice()[0..8].try_into().unwrap());
            Some((partitioning_info >> (64 - sharding_depth)) as u32)
        } else {
            None
        }
    }

    fn prepare_raw_block_record(
        block_id: &BlockIdExt,
        block_boc: Vec<u8>,
        mc_seq_no: u32,
        partition_key: Option<u32>,
    ) -> Result<DbRecord> {
        Ok(DbRecord::RawBlock(
            block_id.root_hash.as_array().clone(),
            block_id.file_hash.as_array().clone(),
            block_boc,
            mc_seq_no,
            partition_key,
        ))
    }

    fn prepare_block_proof_record(
        proof: &BlockProof,
        partition_key: Option<u32>,
        block_order: String,
    ) -> Result<DbRecord> {
        let mut doc = ton_block_json::db_serialize_block_proof("id", proof)?;
        doc.insert("chain_order".to_owned(), block_order.into());
        Ok(DbRecord::BlockProof(
            doc["id"].to_string(),
            format!("{:#}", serde_json::json!(doc)),
            partition_key,
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
        prev_states: Option<(&Arc<ShardStateStuff>, Option<&Arc<ShardStateStuff>>)>,
        mc_seq_no: u32,
        add_proof: bool,
     ) -> Result<()> {

        log::trace!("Processor block_stuff.id {}", block_stuff.id());
        if !self.process_workchain(block_stuff.id().shard().workchain_id()) {
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
        let block_boc = if process_block || process_raw_block { Some(block_stuff.data().to_vec()) } else { None };
        let block_order = ton_block_json::block_order(&block, mc_seq_no)?;
        let workchain_id = block.read_info()?.shard().workchain_id();
        let shard_accounts = state.map(|s| s.state().read_accounts()).transpose()?;
        let accounts_sharding_depth = self.write_account.sharding_depth();
        let max_account_bytes_size = self.max_account_bytes_size;
        let block_proofs_sharding_depth = self.write_block_proof.sharding_depth();
        let raw_blocks_sharding_depth = self.write_raw_block.sharding_depth();

        let (prev_shard_accounts1, prev_shard_accounts2, prev_shard1) = match prev_states {
            Some((prev1, Some(prev2))) => {
                (Some(prev1.state().read_accounts()?),  Some(prev2.state().read_accounts()?), prev1.block_id().shard().clone())
            },
            Some((prev, None)) => (Some(prev.state().read_accounts()?), None, prev.block_id().shard().clone()),
            None => (None, None, block_id.shard().clone())
        };

        let now = std::time::Instant::now();

        let db_records = tokio::task::spawn_blocking(move || {

            let mut db_records = Vec::new();

            
            // Accounts, transactions and messages
            if process_account || process_transaction || process_message {
                
                // Prepare sorted ton_block transactions and addresses of changed accounts
                let mut changed_acc = HashSet::new();
                let mut deleted_acc = HashSet::new();
                let mut acc_last_trans_chain_order = HashMap::new();
                let now = std::time::Instant::now();
                let mut tr_count = 0;
                let mut transactions = BTreeMap::new();
                block_extra.read_account_blocks()?.iterate_objects(|account_block: AccountBlock| {
                    // extract ids of changed accounts
                    if process_account {
                        let state_upd = account_block.read_state_update()?;
                        if state_upd.new_hash == *ACCOUNT_NONE_HASH {
                            deleted_acc.insert(account_block.account_id().clone());
                        } else {
                            changed_acc.insert(account_block.account_id().clone());
                        }
                    }

                    account_block.transactions().iterate_slices(|_, transaction_slice| {
                        // extract transactions
                        let cell = transaction_slice.reference(0)?;
                        let transaction = Transaction::construct_from(&mut cell.clone().into())?;
                        let ordering_key = (transaction.logical_time(), transaction.account_id().clone());
                        transactions.insert(ordering_key, (cell, transaction));
                        tr_count += 1;

                        Ok(true)
                    })?;
                    Ok(true)
                })?;
                log::trace!("TIME: preliminary prepare {} transactions {}ms;   {}", tr_count, now.elapsed().as_millis(), block_id);


                // Iterate ton_block transactions to:
                // - prepare messages and transactions for external db
                // - prepare last_trans_chain_order for accounts
                let now = std::time::Instant::now();
                let mut index = 0;
                let mut messages = Default::default();
                for (_, (cell, transaction)) in transactions.into_iter() {
                    let tr_chain_order = format!("{}{}", block_order, ton_block_json::u64_to_string(index as u64));
                    
                    if process_message {
                        Self::prepare_messages_from_transaction(
                            &transaction,
                            block_root.repr_hash(),
                            &tr_chain_order,
                            add_proof.then(|| &block_root),
                            &mut messages,
                        )?;
                    }

                    if process_account {
                        let account_id = transaction.account_id().clone();
                        acc_last_trans_chain_order.insert(account_id, tr_chain_order.clone());
                    }

                    if process_transaction {
                        let mut doc = Self::prepare_transaction_json(
                            cell, 
                            transaction,
                            &block_root,
                            block_root.repr_hash(),
                            workchain_id,
                            add_proof
                        )?;
                        doc.insert("chain_order".into(), tr_chain_order.into());
                        db_records.push(
                            DbRecord::Transaction(
                                doc["id"].to_string(),
                                format!("{:#}", serde_json::json!(doc))
                            ));
                    }
                    
                    index += 1;
                }
                let msg_count = messages.len(); // is 0 if not process_message
                for (_, message) in messages {
                    db_records.push(
                        DbRecord::Message(
                            message["id"].to_string(),
                            format!("{}", serde_json::json!(message)),
                        ));
                }
                log::trace!(
                    "TIME: prepare {} transactions and {} messages {}ms;   {}",
                    if process_transaction { tr_count } else { 0 },
                    msg_count,
                    now.elapsed().as_millis(),
                    block_id,
                );

                
                // Prepare accounts (changed and deleted)
                if process_account {
                    let now = std::time::Instant::now();
                    if let Some(shard_accounts) = shard_accounts {
                        let prev_shard_accounts1 = prev_shard_accounts1
                            .as_ref()
                            .ok_or_else(|| NodeError::InvalidData("No previous shard state provided".to_string()))?;

                        let get_prev_state = |address: SliceData| -> Result<Option<Account>> {
                            let prev_acc = if prev_shard1.contains_account(address.clone())? {
                                prev_shard_accounts1.account(&address)?
                            } else {
                                prev_shard_accounts2
                                    .as_ref()
                                    .ok_or_else(|| NodeError::InvalidData("No second previous shard state provided".to_string()))?
                                    .account(&address)?
                            };
                            prev_acc.map(|acc| acc.read_account()).transpose()
                        };

                        for account_id in changed_acc.iter() {
                            let acc = shard_accounts.account(account_id)?
                                .ok_or_else(|| 
                                    NodeError::InvalidData(
                                        "Block and shard state mismatch: \
                                        state doesn't contain changed account".to_string()
                                    )
                                )?;
                            let acc = acc.read_account()?;

                            let prev_acc = get_prev_state(account_id.clone())?;
                            db_records.push(Self::prepare_account_record(
                                acc,
                                prev_acc,
                                accounts_sharding_depth,
                                max_account_bytes_size,
                                acc_last_trans_chain_order.remove(account_id),
                            )?);
                        }

                        for account_id in deleted_acc {
                            let prev_acc = get_prev_state(account_id.clone())?;
                            let last_trans_chain_order = acc_last_trans_chain_order.remove(&account_id);
                            db_records.push(Self::prepare_deleted_account_record(
                                account_id,
                                workchain_id,
                                accounts_sharding_depth,
                                prev_acc,
                                last_trans_chain_order,
                            )?);
                        }
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
                    Self::prepare_block_record(
                        &block,
                        &block_root,
                        block_boc.as_deref().unwrap(),
                        &block_id.file_hash,
                        block_order.clone(),
                    )?
                );
                log::trace!("TIME: block {}ms;   {}", now.elapsed().as_millis(), block_id);
            }

            // Block proof
            if process_block_proof {
                if let Some(proof) = proof {
                    let now = std::time::Instant::now();
                    let partition_key = Self::get_block_partition_key(&block_id, block_proofs_sharding_depth);
                    db_records.push(
                        Self::prepare_block_proof_record(&proof, partition_key, block_order)?
                    );
                    log::trace!("TIME: block proof {}ms;   {}", now.elapsed().as_millis(), block_id);
                }
            }

            // raw block
            if process_raw_block {
                let now = std::time::Instant::now();
                let partition_key = Self::get_block_partition_key(&block_id, raw_blocks_sharding_depth);
                db_records.push(
                    Self::prepare_raw_block_record(&block_id, block_boc.unwrap(), mc_seq_no, partition_key)?
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
                DbRecord::Message(key, value) => send_tasks.push(self.write_message.write_data(key, value, None, None)),
                DbRecord::Transaction(key, value) => send_tasks.push(self.write_transaction.write_data(key, value, None, None)),
                DbRecord::Account(key, value, partition_key) => send_tasks.push(self.write_account.write_data(key, value, None, partition_key)),
                DbRecord::Block(key, value) => send_tasks.push(self.write_block.write_data(key, value, None, None)),
                DbRecord::BlockProof(key, value, partition_key) => send_tasks.push(self.write_block_proof.write_data(key, value, None, partition_key)),
                DbRecord::RawBlock(key, file_hash, value, mc_seq_no, partition_key) =>  send_tasks.push(Box::pin(self.send_raw_block(key, value, mc_seq_no, file_hash, partition_key))),
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

    async fn send_raw_block(&self, key: [u8; 32], value: Vec<u8>, mc_seq_no: u32, file_hash: [u8; 32], partition_key: Option<u32>) -> Result<()> {
        let raw_block_timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
        let attributes = [
            ("mc_seq_no", &mc_seq_no.to_be_bytes()[..]),
            ("file_hash", &file_hash[..]),
            ("raw_block_timestamp", &raw_block_timestamp.to_be_bytes()[..]),
        ];
        self.write_raw_block.write_raw_data(key.to_vec(), value, Some(&attributes), partition_key).await
    }
}

#[async_trait::async_trait]
impl<T: WriteData> ExternalDb for Processor<T> {
    async fn process_block(
        &self, 
        block_stuff: &BlockStuff,
        proof: Option<&BlockProofStuff>, 
        state: &Arc<ShardStateStuff>,
        prev_states: (&Arc<ShardStateStuff>, Option<&Arc<ShardStateStuff>>),
        mc_seq_no: u32,
    ) -> Result<()> {
        self.process_block_impl(block_stuff, proof, Some(state), Some(prev_states), mc_seq_no, false).await
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
                let record = Self::prepare_account_record(
                    acc, 
                    None,
                    self.write_account.sharding_depth(),
                    self.max_account_bytes_size,
                    None,
                )?;
                accounts.push(record);
                Ok(true)
            })?;

            let accounts_len = accounts.len();
            futures::future::join_all(
                accounts.into_iter().map(|r| {
                    match r {
                        DbRecord::Account(key, value, partition_key) => self.write_account.write_data(key, value, None, partition_key),
                        DbRecord::Empty => Box::pin(async{Ok(())}),
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
                None,
                None,
            ).await?;
        }

        Ok(())
    }
}
