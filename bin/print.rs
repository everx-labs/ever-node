/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
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

use clap::{Arg, App};
use serde_json::Map;
use ton_block::{
    BlockIdExt, Block, Deserializable, ShardStateUnsplit, McShardRecord, HashmapAugType
};
use ton_node::{
    collator_test_bundle::create_engine_allocated, 
    internal_db::{InternalDb, InternalDbConfig, LAST_APPLIED_MC_BLOCK}
};
#[cfg(feature = "telemetry")]
use ton_node::collator_test_bundle::create_engine_telemetry;
use ton_types::{error, Result};

fn print_block(block: &Block, brief: bool) -> Result<()> {
    if brief {
        println!("{}", ton_block_json::debug_block(block.clone())?);
    } else {
        println!("{}", ton_block_json::debug_block_full(block)?);
    }
    Ok(())
}

fn print_state(state: &ShardStateUnsplit, brief: bool) -> Result<()> {
    if brief {
        println!("{}", ton_block_json::debug_state(state.clone())?);
    } else {
        println!("{}", ton_block_json::debug_state_full(state.clone())?);
    }
    Ok(())
}

async fn print_db_block(db: &InternalDb, block_id: BlockIdExt, brief: bool) -> Result<()> {
    println!("loading block: {}", block_id);
    let handle = db.load_block_handle(&block_id)?.ok_or_else(
        || error!("Cannot load block {}", block_id)
    )?;
    let block = db.load_block_data(&handle).await?;
    print_block(block.block()?, brief)
}

async fn print_db_state(db: &InternalDb, block_id: BlockIdExt, brief: bool) -> Result<()> {
    println!("loading state: {}", block_id);
    let state = db.load_shard_state_dynamic(&block_id)?;
    print_state(state.state()?, brief)
}

async fn print_shards(db: &InternalDb, block_id: BlockIdExt) -> Result<()> {
    println!("loading state: {}", block_id);
    let state = db.load_shard_state_dynamic(&block_id)?;
    if let Ok(shards) = state.shards() {
        shards.iterate_shards(|shard, descr| {
            let descr = McShardRecord::from_shard_descr(shard, descr);
            println!("before_merge: {} {}", descr.descr.before_merge, descr.block_id());
            Ok(true)
        })?;
    }
    Ok(())
}

// full BlockIdExt or masterchain seq_no
fn get_block_id(db: &InternalDb, id: &str) -> Result<BlockIdExt> {
    if let Ok(id) = id.parse() {
        Ok(id)
    } else {
        let mc_seqno = id.parse()?;
        let handle = db.find_mc_block_by_seq_no(mc_seqno)?;
        Ok(handle.id().clone())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .arg(Arg::with_name("PATH")
            .short("p")
            .long("path")
            .help("path to DB")
            .takes_value(true)
            .default_value("node_db")
            .number_of_values(1))
        .arg(Arg::with_name("BLOCK")
            .short("b")
            .long("block")
            .help("print block")
            .takes_value(true)
            .number_of_values(1))
        .arg(Arg::with_name("STATE")
            .short("s")
            .long("state")
            .help("print state")
            .takes_value(true)
            .number_of_values(1))
        .arg(Arg::with_name("SHARDS")
            .short("r")
            .long("shards")
            .help("shard ids from master with seqno")
            .takes_value(true)
            .number_of_values(1))
        .arg(Arg::with_name("LAST_ACCOUNTS")
            .long("accounts")
            .takes_value(false)
            .help("print all accounts from all shards of workchains and masterchain for last applied state"))
        .arg(Arg::with_name("BOC")
            .short("c")
            .long("boc")
            .help("print containtment of bag of cells")
            .takes_value(true)
            .number_of_values(1))
        .arg(Arg::with_name("BRIEF")
            .short("i")
            .long("brief")
            .help("print brief info (block without messages and transactions, state without accounts) "))
        .get_matches();

    let brief = args.is_present("BRIEF");
    if let Some(path) = args.value_of("BOC") {
        let bytes = std::fs::read(path)?;
        if let Ok(block) = Block::construct_from_bytes(&bytes) {
            print_block(&block, brief)?;
        } else if let Ok(state) = ShardStateUnsplit::construct_from_bytes(&bytes) {
            print_state(&state, brief)?;
        }
    } else if let Some(db_dir) = args.value_of("PATH") {
        let db_config = InternalDbConfig { 
            db_directory: db_dir.to_string(), 
            ..Default::default()
        };
        let db = InternalDb::with_update(
            db_config,
            false,
            false,
            false,
            &|| Ok(()),
            None,
            #[cfg(feature = "telemetry")]
            create_engine_telemetry(),
            create_engine_allocated(),
        ).await?;
        if let Some(block_id) = args.value_of("BLOCK") {
            let block_id = get_block_id(&db, block_id)?;
            print_db_block(&db, block_id, brief).await?;
        }
        if let Some(block_id) = args.value_of("STATE") {
            let block_id = get_block_id(&db, block_id)?;
            print_db_state(&db, block_id, brief).await?;
        }
        if let Some(block_id) = args.value_of("SHARDS") {
            let block_id = get_block_id(&db, block_id)?;
            print_shards(&db, block_id).await?;
        }
        if args.is_present("LAST_ACCOUNTS") {
            let last_mc_id = db
                .load_full_node_state(LAST_APPLIED_MC_BLOCK)?
                .ok_or_else(|| error!("no info about last applied mc block"))?;
            println!("{{\"accounts\":[");
            let mut first = true; 
            let last_mc_state = db.load_shard_state_dynamic(&last_mc_id)?;
            let mut top_blocks = last_mc_state.top_blocks_all()?;
            top_blocks.push((*last_mc_id).clone());
            for block_id in &top_blocks {
                let state = db.load_shard_state_dynamic(&block_id)?;
                state.state()?.read_accounts()?.iterate_objects(|shard_account| {
                    let account = shard_account.read_account()?;
                    let addr = account.get_addr().unwrap();
                    let balance = account.balance().unwrap();
                    let mut acc = serde_json::json!({
                        "id": addr.to_string(),
                        "last_paid": account.storage_info().unwrap().last_paid(),
                        "last_trans_lt": account.last_tr_time().unwrap_or_default(),
                        "balance": balance.grams.as_u128(),
                    });
                    if !balance.other.is_empty() {
                        let mut other = Map::new();
                        balance.other.iterate_with_keys(|k: u32, v| {
                            other.insert(k.to_string(), v.value().to_string().into());
                            Ok(true)
                        })?;
                        if let Some(map) = acc.as_object_mut() {
                            map.insert("balance_other".to_string(), other.into());
                        }
                    };
                    if !first {
                        println!(",");
                    } else {
                        first = false;
                    }
                    print!("{:#}", acc);
                    Ok(true)
                })?;
            }
            println!("]}}");
        }
    }
    Ok(())
}
