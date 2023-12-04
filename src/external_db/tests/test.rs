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
    block::BlockStuff, block_proof::BlockProofStuff, collator_test_bundle::create_engine_allocated,
    engine_traits::ChainRange, shard_state::ShardStateStuff, external_db::processor::Writers
};
#[cfg(feature = "telemetry")]
use crate::collator_test_bundle::create_engine_telemetry;

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use ton_block::{HashmapAugType, OutMsg, AccountBlock};
use ton_types::HashmapType;

#[derive(Clone)]
struct TestWriter {
    pub records: Arc<AtomicUsize>,
    pub data: Arc<std::sync::RwLock<Vec<(String, String)>>>,
    pub raw_data: Arc<std::sync::RwLock<Vec<(Vec<u8>, Vec<u8>)>>>,
    pub enabled: bool,
    pub write_data: bool,
}

impl TestWriter {
    pub fn new(enabled: bool, write_data: bool) -> Self {
        Self {
            records: Arc::new(AtomicUsize::new(0)),
            enabled,
            write_data,
            data: Arc::new(std::sync::RwLock::new(Vec::new())),
            raw_data: Arc::new(std::sync::RwLock::new(Vec::new())),
        }
    }
}

#[async_trait::async_trait]
impl WriteData for TestWriter {
    fn enabled(&self) -> bool { self.enabled }
    fn sharding_depth(&self) -> u32 { 0 }
    async fn write_data(&self, _key: String, _data: String, _attributes: Option<&[(&str, &[u8])]>, _partition_key: Option<u32>) -> Result<()> {
        if !self.enabled() {
            panic!("Is not enabled!")
        }
        self.records.fetch_add(1, Ordering::SeqCst);
        if self.write_data {
            self.data.write().unwrap().push((_key, _data))
        }
        Ok(())
    }
    async fn write_raw_data(&self, _key: Vec<u8>, _data: Vec<u8>, _attributes: Option<&[(&str, &[u8])]>, _partition_key: Option<u32>) -> Result<()> {
        if !self.enabled() {
            panic!("Is not enabled!")
        }
        self.records.fetch_add(1, Ordering::SeqCst);
        if self.write_data {
            self.raw_data.write().unwrap().push((_key, _data))
        }
        Ok(())
    }
}

#[test]
fn test_external_db_processor_with_deleted_account() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let writers = runtime.block_on(
        test_external_db_processor_async(
            "src/external_db/tests/static/9CD0BCB4D40665E7928274894E98703E47CFC408048BA144CE913818692E7A15.boc",
            Some("src/external_db/tests/static/state_9CD0BCB4D40665E7928274894E98703E47CFC408048BA144CE913818692E7A15.boc"),
            None,
            true,
            true,
            123,
        )
    ).unwrap();
    for (_, data) in &*writers.write_account.data.read().unwrap() {
        let acc = serde_json::from_str::<serde_json::Value>(&data).unwrap();
        if acc["acc_type"].as_i64() == Some(3) {
            assert!(acc["last_trans_lt"].as_str().unwrap().len() > 0)
        }
    }
}

#[test]
fn test_external_db_processor_with_state() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(
        test_external_db_processor_async(
            "src/external_db/tests/static/705EE27D8C862EC367977465042211F9C72EB94D28040784F8424DEAB3AC642C.boc",
            Some("src/external_db/tests/static/state_705EE27D8C862EC367977465042211F9C72EB94D28040784F8424DEAB3AC642C.boc"),
            None,
            true,
            false,
            123,
        )
    ).unwrap();
}


#[test]
fn test_external_db_processor_with_proof() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(
        test_external_db_processor_async(
            "src/tests/static/test_master_block_proof/block__3082183",
            None,
            Some("src/tests/static/test_master_block_proof/proof__3082183"),
            true,
            false,
            3082183,
        )
    ).unwrap();
}

#[test]
fn test_external_db_processor_disabled() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(
        test_external_db_processor_async(
            "src/tests/static/test_master_block_proof/block__3082183",
            None,
            Some("src/tests/static/test_master_block_proof/proof__3082183"),
            false,
            false,
            3082183,
        )
    ).unwrap();
}

async fn test_external_db_processor_async(
    block_path: &str,
    state_path: Option<&str>,
    proof_path: Option<&str>,
    enabled: bool,
    write_data: bool,
    mc_seq_no: u32,
) -> Result<Writers<TestWriter>> {

    let writers = Writers {
        write_block: TestWriter::new(enabled, write_data),
        write_raw_block: TestWriter::new(enabled, write_data),
        write_message: TestWriter::new(enabled, write_data),
        write_transaction: TestWriter::new(enabled, write_data),
        write_account: TestWriter::new(enabled, write_data),
        write_block_proof: TestWriter::new(enabled, write_data),
        write_raw_block_proof: TestWriter::new(enabled, write_data),
        write_chain_range: TestWriter::new(enabled, write_data),
        write_remp_statuses: TestWriter::new(enabled, write_data),
        write_shard_hashes: TestWriter::new(enabled, write_data),
    };

    let p = Processor::new(
        writers.clone(),
        "target/test_external_db_processor_async/bad_block".to_owned(),
        vec![-1, 0],
        None,
        [1u8; 32],
    );

    let block = BlockStuff::read_block_from_file(block_path)?;

    let block_proof = proof_path.map(|pp| BlockProofStuff::read_from_file(
        block.id(),
        pp,
        false
    )).transpose()?;

    let ss = state_path.map(|sp| ShardStateStuff::read_from_file(
        block.id().clone(),
        sp,
        #[cfg(feature = "telemetry")]
        &create_engine_telemetry(),
        &create_engine_allocated()
    )).transpose()?;
    let block_extra = block.block()?.read_extra()?;

    let mut messages_count = 0_usize;
    block_extra.read_in_msg_descr()?.iterate_objects(|_| {
        messages_count += 1;
        Ok(true)
    })?;
    block_extra.read_out_msg_descr()?.iterate_objects(|out_msg: OutMsg| {
        if out_msg.message_cell()?.is_some() {
            messages_count += 1;
        }
        Ok(true)
    })?;

    let mut transactions_count = 0_usize;
    let mut accounts_count = 0_usize;
    block_extra.read_account_blocks()?.iterate_objects(|account_block: AccountBlock| {
        accounts_count += 1;
        account_block.transactions().iterate_slices(|_, _| {
            transactions_count += 1;
            Ok(true)
        })?;
        Ok(true)
    })?;


    let now = std::time::Instant::now();
    p.process_block_impl(
        &block,
        block_proof.as_ref(),
        ss.as_ref(),
        ss.as_ref().map(|ss| (ss, None)),
        mc_seq_no,
        false
    ).await?;
    println!("{}ms, messages: {}, transactions: {}, accounts: {}",
        now.elapsed().as_millis(), messages_count, transactions_count, accounts_count);

    if enabled {
        assert_eq!(1, writers.write_block.records.load(Ordering::Relaxed));
        assert_eq!(1, writers.write_raw_block.records.load(Ordering::Relaxed));
        assert_eq!(block_proof.is_some() as usize, writers.write_block_proof.records.load(Ordering::Relaxed));
        assert_eq!(block_proof.is_some() as usize, writers.write_raw_block_proof.records.load(Ordering::Relaxed));
        assert_eq!(messages_count, writers.write_message.records.load(Ordering::Relaxed));
        assert_eq!(transactions_count, writers.write_transaction.records.load(Ordering::Relaxed));
    } else {
        assert_eq!(0, writers.write_block.records.load(Ordering::Relaxed));
        assert_eq!(0, writers.write_raw_block.records.load(Ordering::Relaxed));
        assert_eq!(0, writers.write_block_proof.records.load(Ordering::Relaxed));
        assert_eq!(0, writers.write_message.records.load(Ordering::Relaxed));
        assert_eq!(0, writers.write_transaction.records.load(Ordering::Relaxed));
    }


    let range = ChainRange {
        master_block: block.id().clone(),
        shard_blocks: Vec::new(),
    };


    p.process_chain_range(&range).await?;

    assert_eq!(p.process_chain_range_enabled(), enabled);
    assert_eq!(enabled as usize, writers.write_chain_range.records.load(Ordering::Relaxed));

    p.process_shard_hashes(&[block.id().clone()]).await?;
    assert_eq!(enabled as usize, writers.write_shard_hashes.records.load(Ordering::Relaxed));

    if let Some(ss) = ss {
        assert_eq!(accounts_count, writers.write_shard_hashes.records.load(Ordering::Relaxed));

        p.process_full_state(&ss).await?;
        assert_eq!(
            accounts_count + ss.state()?.read_accounts()?.len()?,
            writers.write_account.records.load(Ordering::Relaxed)
        );
    }

    Ok(writers)
}

