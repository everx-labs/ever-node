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
    block::BlockStuff, block_proof::BlockProofStuff,
    collator_test_bundle::create_engine_allocated, engine_traits::{EngineAlloc, EngineOperations}, 
    internal_db::{
        BlockResult, InternalDb, InternalDbConfig, CURRENT_DB_VERSION, 
        restore::set_graceful_termination
    },
    shard_state::ShardStateStuff, test_helper::{are_shard_states_equal, WaitForHandle},
    types::top_block_descr::{TopBlockDescrId, TopBlockDescrStuff},
};
#[cfg(feature = "telemetry")]
use crate::{collator_test_bundle::create_engine_telemetry, engine_traits::EngineTelemetry};

use std::{future::{self, Future}, ops::Deref, pin::Pin, sync::Arc, time::Duration};
use storage::{
    block_handle_db::{BlockHandle, Callback}, shardstate_db_async::SsNotificationCallback
};
use ton_block::{
    BlockIdExt, ShardIdent, TopBlockDescr, BlockSignatures, ShardStateUnsplit, 
    Serializable
};
use ton_types::{error, fail, Result, sha256_digest_slices, UInt256};
use storage::types::BlockMeta;

include!("../../common/src/test.rs");

const DB_PATH: &str = "target/test";

async fn create_db(test_name: &str) -> Result<InternalDb> {
    let attempts = 5;
    for _ in 0..attempts {
        let result = InternalDb::with_update(
            InternalDbConfig {
                db_directory: format!("{}/{}", DB_PATH, test_name),
                ..Default::default()
            },
            false,
            false,
            false,
            &|| Ok(()),
            None,
            #[cfg(feature = "telemetry")]
            create_engine_telemetry(),
            create_engine_allocated(),
        ).await;
        match result {
            Err(e) => {
                println!("Can't open database ({}), retrying...", e);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            result => return result
        }
    }
    fail!("Can't open database in {} attempts", attempts);
}

async fn stop_db(db: &InternalDb) {
    db.stop_states_db().await;
    set_graceful_termination(&db.config.db_directory);
}

async fn clean_up(may_fail: bool, test_name: &str) {
    loop {
        if std::fs::remove_dir_all(format!("{}/{}", DB_PATH, test_name)).is_ok() {
            break
        } else if may_fail {
            break
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn prepare_block() -> Result<BlockStuff> {
    BlockStuff::read_block_from_file(
        "src/tests/static/F2BC2888EAE466FC172140A90949EDBC7091D81CCB4194A225C56C0F1D095097.boc"
    )
}

fn prepare_block_only() -> Result<Data<()>> {
    let ret = Data {
        block: prepare_block()?,
        extra: ()
    };
    Ok(ret)
}

fn prepare_block_proof() -> Result<Data<BlockProofStuff>> {
    let block = BlockStuff::read_block_from_file(
        "src/tests/static/test_master_block_proof/key_block__3082181"
    )?;
    let extra = BlockProofStuff::read_from_file(
        block.id(), 
        "src/tests/static/test_master_block_proof/key_proof__3082181", 
        false
    )?;
    let ret = Data {
        block,
        extra
    };
    Ok(ret)
}

fn prepare_block_with_next() -> Result<Data<BlockStuff>> {
    let ret = Data {
        block: prepare_block()?,
        extra: BlockStuff::read_block_from_file(
            "src/tests/static/368730B00DE6947DD549ABCF53730FC33FE96C8FAABDACBBB71E9D510525D639.boc"
        )?
    };
    Ok(ret)
}

fn prepare_block_with_prev() -> Result<Data<BlockStuff>> {
    let ret = Data {
        block: prepare_block()?,
        extra: BlockStuff::read_block_from_file(
            "src/tests/static/ABA8391BAF9DB3B48A6DAE91F8251DC031ACBCE22A80903C08B48BFDD5F6D7D0.boc"
        )?
    };
    Ok(ret)
}

fn prepare_ss(
    #[cfg(feature = "telemetry")]
    telemetry:&Arc<EngineTelemetry>,
    allocated: &Arc<EngineAlloc>
) -> Result<(BlockStuff, Arc<ShardStateStuff>)> {
    let block = BlockStuff::read_block_from_file("src/tests/static/b571525")?;                           
    let block_id = block.id().clone();
    let state = ShardStateStuff::read_from_file(
        block_id, 
        "src/tests/static/ss571525",
        #[cfg(feature = "telemetry")]
        telemetry,
        allocated
    )?;
    Ok((block, state))
}

struct Data<X> {
    block: BlockStuff,
    extra: X
}

struct Context<'a, X> {
    db: InternalDb,
    data: &'a Data<X>,
    wait: Arc<dyn Callback> 
}

struct Job<'a, X> {    
    ctx: Context<'a, X>,
    handle: Arc<BlockHandle>
}

type Pinned<'a, X> = Pin<Box<dyn Future<Output = Result<X>> + 'a>>;

async fn test_store<X>(      
    name: &str, 
    prep1: impl Fn() -> Result<Data<X>>, 
    prep2: impl for <'a> Fn(&'a Context<'a, X>) -> Pinned<'a, BlockResult>,
    store: impl for <'a> Fn(&'a Job<X>) -> Result<()>,
    check: impl for <'a> Fn(&'a Job<X>) -> Pinned<'a, ()>
) -> Result<()> {
    let data = prep1()?;
    let wait = WaitForHandle::with_max_count_and_delay(2, 50);
    for i in 0..2 {
        let ctx = Context {
            db: create_db(name).await?,
            data: &data,
            wait: wait.clone()
        };
        assert_eq!(ctx.db.load_db_version().unwrap(), CURRENT_DB_VERSION);
        let handle = if i == 0 {
            prep2(&ctx).await?.to_updated().ok_or_else(
                || error!("Bad result in store related to block {}", ctx.data.block.id())
            )?
        } else {
            ctx.db.load_block_handle(ctx.data.block.id())?.ok_or_else(
                || error!("Cannot load handle for block {}", ctx.data.block.id())
            )?
        };
        let job = Job {
            ctx, 
            handle
        };
        if i == 0 {
            store(&job)?;
        }
        check(&job).await?;
        stop_db(&job.ctx.db).await;
        if i == 0 {
            wait.wait().await;
        }
    }
    Ok(())
}

async fn test_store_with_clean<X>(
    name: &str, 
    prep1: impl Fn() -> Result<Data<X>>, 
    prep2: impl for <'a> Fn(&'a Context<'a, X>) -> Pinned<'a, BlockResult>,
    store: impl for <'a> Fn(&'a Job<X>) -> Result<()>,
    check: impl for <'a> Fn(&'a Job<X>) -> Pinned<'a, ()>
) {
    init_test_log();
    let r = test_store(name, prep1, prep2, store, check).await;
    clean_up(false, name).await;                             
    r.unwrap();
}

async fn test_store_flag_with_clean<X>(
    name: &str,
    prep1: impl Fn() -> Result<Data<X>>, 
    store: impl for <'a> Fn(&'a Job<X>) -> Result<()>,
    check: impl for <'a> Fn(&'a Job<X>) -> Pinned<'a, ()>
) {
    test_store_with_clean(
        name,
        prep1,
        |c| Box::pin(c.db.store_block_data(&c.data.block, Some(c.wait.clone()))),
        |j| store(j), 
        |j| Box::pin(check(j)) 
    ).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_block_data() {
    async fn check<'a>(job: &'a Job<'a, ()>) -> Result<()> {
        assert!(job.handle.has_data());
        let block = job.ctx.db.load_block_data(&job.handle).await?;
        assert_eq!(job.ctx.data.block, block);
        Ok(())
    }
    test_store_with_clean(
        "test_store_block_data",
        | | prepare_block_only(),
        |c| Box::pin(c.db.store_block_data(&c.data.block, Some(c.wait.clone()))),
        |_| Ok(()),
        |j| Box::pin(check(j)) 
    ).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_block_proof() {
    async fn check<'a>(job: &'a Job<'a, BlockProofStuff>) -> Result<()> {
        assert!(job.handle.has_proof());
        let proof = job.ctx.db.load_block_proof(&job.handle, false).await?;
        assert_eq!(job.ctx.data.extra, proof);
        Ok(())
    }
    test_store_with_clean(
        "test_store_block_data",
        | | prepare_block_proof(),
        |c| Box::pin(
            c.db.store_block_proof(
                &c.data.extra.id(), None, &c.data.extra, Some(c.wait.clone())
            )
        ),
        |_| Ok(()),
        |j| Box::pin(check(j)) 
    ).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_block_prev1() {
    test_store_flag_with_clean(
        "test_store_block_prev1",
        | | prepare_block_with_prev(),
        |j| j.ctx.db.store_block_prev1(&j.handle, j.ctx.data.extra.id(), Some(j.ctx.wait.clone())),
        |j| {
            assert!(j.handle.has_prev1());
            let prev1 = j.ctx.db.load_block_prev1(j.ctx.data.block.id()).unwrap();
            assert_eq!(j.ctx.data.extra.id(), &prev1);
            Box::pin(future::ready(Ok(())))
        }
    ).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_block_prev2() {
    test_store_flag_with_clean(
        "test_store_block_prev2",
        | | prepare_block_with_prev(),
        |j| j.ctx.db.store_block_prev2(&j.handle, j.ctx.data.extra.id(), Some(j.ctx.wait.clone())),
        |j| {
            assert!(j.handle.has_prev2());
            let prev2 = j.ctx.db.load_block_prev2(j.ctx.data.block.id()).unwrap().unwrap();
            assert_eq!(j.ctx.data.extra.id(), &prev2);
            Box::pin(future::ready(Ok(())))
        }
    ).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_block_next1() {
    test_store_flag_with_clean(
        "test_store_block_next1",
        | | prepare_block_with_next(),
        |j| j.ctx.db.store_block_next1(&j.handle, j.ctx.data.extra.id(), Some(j.ctx.wait.clone())),
        |j| {
            assert!(j.handle.has_next1());
            let next1 = j.ctx.db.load_block_next1(j.ctx.data.block.id()).unwrap();
            assert_eq!(j.ctx.data.extra.id(), &next1);
            Box::pin(future::ready(Ok(())))
        }
    ).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_block_next2() {
    test_store_flag_with_clean(
        "test_store_block_next2",
        | | prepare_block_with_next(),
        |j| j.ctx.db.store_block_next2(&j.handle, j.ctx.data.extra.id(), Some(j.ctx.wait.clone())),
        |j| {
            assert!(j.handle.has_next2());
            let next2 = j.ctx.db.load_block_next2(j.ctx.data.block.id()).unwrap().unwrap();
            assert_eq!(j.ctx.data.extra.id(), &next2);
            Box::pin(future::ready(Ok(())))
        }
    ).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_block_applied() {
    test_store_flag_with_clean(
        "test_store_block_applied",
        | | prepare_block_only(),
        |j| j.ctx.db.store_block_applied(&j.handle, Some(j.ctx.wait.clone())).map(|_| ()),
        |j| {
            assert!(j.handle.is_applied());
            Box::pin(future::ready(Ok(())))
        }
    ).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_ss_persistent() {
    clean_up(true, "test_store_ss_persistent").await;
    let r = test_store_ss_persistent_impl().await;
    clean_up(false, "test_store_ss_persistent").await;
    r.unwrap();
}

async fn test_store_ss_persistent_impl() -> Result<()> {
    let db = create_db("test_store_ss_persistent").await?;
    let (block, ss) = prepare_ss(
        #[cfg(feature = "telemetry")]
        &db.telemetry,
        &db.allocated
    )?;
    let wait = WaitForHandle::with_max_count(2);
    let block_handle = db.store_block_data(&block, Some(wait.clone())).await?
        .to_updated()
        .ok_or_else(
            || error!("Bad result in store block {} data", block.id())
        )?;
        // cells db is used while saving persistent state, 
        // so it is need to store state
        let cb = SsNotificationCallback::new();
        db.store_shard_state_dynamic(
            &block_handle, 
            &ss, 
            Some(wait.clone()), 
            Some(cb.clone()),
            false
        ).await?;
        cb.wait().await;
    db.store_shard_state_persistent(
        &block_handle, 
        ss.clone(), 
        Some(wait.clone()),
        Arc::new(|| false)
    ).await?;
    assert!(block_handle.has_persistent_state());
    let size = db.load_shard_state_persistent_size(ss.block_id()).await?;
    let data = db.load_shard_state_persistent_slice(ss.block_id(), 0, size).await?; 
    let ss2 = ShardStateStuff::deserialize_state(
        ss.block_id().clone(), 
        &data,
        #[cfg(feature = "telemetry")]
        &db.telemetry,
        &db.allocated
    )?;
    assert!(are_shard_states_equal(&ss, &ss2));
    wait.wait().await;
    stop_db(&db).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_top_shard_blocks_db() {
    let r = test_top_shard_blocks_db_impl().await;
    clean_up(false, "test_top_shard_blocks_db").await;
    r.unwrap();
}

async fn test_top_shard_blocks_db_impl() -> Result<()> {
    
    {
        let db = create_db("test_top_shard_blocks_db").await?;

        for seqno in 0..=100 {
            for shard in 0..=0xf {
                for cc_seqno in 44..=45 {
                    let id = BlockIdExt::with_params(
                        ShardIdent::with_tagged_prefix(0, shard << 60 | 0x0800000000000000)?,
                        seqno,
                        UInt256::default(),
                        UInt256::default(),
                    );

                    #[cfg(feature = "fast_finality")]
                    let group = crate::types::top_block_descr::TopBlockDescrGroup {
                        shard_ident: id.shard_id.clone(),
                        cc_seqno,
                    };

                    #[cfg(feature = "fast_finality")]
                    let tbds_id = TopBlockDescrId {
                        group,
                        seqno
                    };

                    #[cfg(not(feature = "fast_finality"))]
                    let tbds_id = TopBlockDescrId {
                        id: id.shard_id.clone(),
                        cc_seqno,
                    };

                    let tbd = TopBlockDescr::with_id_and_signatures(id.clone(), BlockSignatures::default());
                    let tbds = TopBlockDescrStuff::new(tbd, &id, true, seqno % 2 != 0)?;

                    db.save_top_shard_block(&tbds_id, &tbds)?;
                }
            }
        }
        // total 3232 tbd

        for seqno in 0..=100 {
            for shard in 0..=0xf {
                let shard_ident = ShardIdent::with_tagged_prefix(0, shard << 60 | 0x0800000000000000)?;

                #[cfg(feature = "fast_finality")]
                let group = crate::types::top_block_descr::TopBlockDescrGroup {
                    shard_ident,
                    cc_seqno: 44,
                };

                #[cfg(feature = "fast_finality")]
                let tbds_id = TopBlockDescrId {
                    group,
                    seqno,
                };

                #[cfg(not(feature = "fast_finality"))]
                let _ = seqno;

                #[cfg(not(feature = "fast_finality"))]
                let tbds_id = TopBlockDescrId {
                    id: shard_ident,
                    cc_seqno: 44,
                };

                db.remove_top_shard_block(&tbds_id)?;
            }
        }
        // deleted 1600 tbd

        #[cfg(feature = "fast_finality")]
        assert_eq!(db.load_all_top_shard_blocks_raw()?.len(), 1616);

        #[cfg(not(feature = "fast_finality"))]
        assert_eq!(db.load_all_top_shard_blocks_raw()?.len(), 16);

        stop_db(&db).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    {
        let db = create_db("test_top_shard_blocks_db").await?;

        #[cfg(feature = "fast_finality")]
        assert_eq!(db.load_all_top_shard_blocks_raw()?.len(), 1616);

        #[cfg(not(feature = "fast_finality"))]
        assert_eq!(db.load_all_top_shard_blocks_raw()?.len(), 16);

        for seqno in 0..=100 {
            for shard in 0..=0xf {
                let shard_ident = ShardIdent::with_tagged_prefix(0, shard << 60 | 0x0800000000000000)?;

                #[cfg(feature = "fast_finality")]
                let group = crate::types::top_block_descr::TopBlockDescrGroup {
                    shard_ident,
                    cc_seqno: 45,
                };

                #[cfg(feature = "fast_finality")]
                let tbds_id = TopBlockDescrId {
                    group,
                    seqno,
                };

                #[cfg(not(feature = "fast_finality"))]
                let _ = seqno;

                #[cfg(not(feature = "fast_finality"))]
                let tbds_id = TopBlockDescrId {
                    id: shard_ident,
                    cc_seqno: 45,
                };

                db.remove_top_shard_block(&tbds_id)?;
            }
        }
        // deleted all

        assert_eq!(db.load_all_top_shard_blocks_raw()?.len(), 0);
        stop_db(&db).await;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_full_node_state() {
    clean_up(true, "test_full_node_state").await;
    let r = test_full_node_state_impl().await;
    clean_up(false, "test_full_node_state").await;
    r.unwrap();
}

async fn test_full_node_state_impl() -> Result<()> {
    let id1 = BlockIdExt::with_params(
        ShardIdent::masterchain(),
        1,
        UInt256::rand(),
        UInt256::rand(),
    );
    let id2 = BlockIdExt::with_params(
        ShardIdent::masterchain(),
        1,
        UInt256::rand(),
        UInt256::rand(),
    );
    
    {
        let db = create_db("test_full_node_state").await?;

        db.save_full_node_state("test1", &id1)?;
        db.save_validator_state("test2", &id2)?;

        assert_eq!(*db.load_full_node_state("test1").unwrap().unwrap(), id1);
        assert_eq!(*db.load_validator_state("test2").unwrap().unwrap(), id2);

        db.drop_validator_state("test2")?;

        assert!(db.load_validator_state("test2").unwrap().is_none());
        stop_db(&db).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    {
        let db = create_db("test_full_node_state").await?;
        assert_eq!(*db.load_full_node_state("test1").unwrap().unwrap(), id1);
        assert!(db.load_validator_state("test2").unwrap().is_none());
        stop_db(&db).await;
    }

    Ok(())
}

const SHARDES: u64 = 32;
const THREADS: u64 = 5;
const MC_BLOCKS: u32 = 100;

fn gen_block_id_ext(shard: ShardIdent, seq_no: u32) -> BlockIdExt {
    let buf = sha256_digest_slices(
        &vec![
            &shard.workchain_id().to_be_bytes()[..],
            &shard.shard_prefix_with_tag().to_be_bytes()[..],
            &seq_no.to_be_bytes()[..]
        ]
    );
    BlockIdExt::with_params(
        shard, 
        seq_no,
        UInt256::from_slice(buf.as_slice()),
        UInt256::from_slice(buf.as_slice())
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn test_archives_multithread() {

    async fn wait_mc_block(db: &InternalDb, seq_no: u32) {
        let mc_id = gen_block_id_ext(ShardIdent::masterchain(), seq_no);
        let handle = db.load_block_handle(&mc_id).unwrap().unwrap();
        while !handle.is_applied() { 
            tokio::task::yield_now().await 
        }
    }

    fn shard_prefix(shard_byte: u64) -> ShardIdent {
        ShardIdent::with_tagged_prefix(
            0, 
            shard_byte << 56 | 0x0080_0000_0000_0000
        ).unwrap()
    }

    async fn store_block(
        db: &InternalDb, 
        id: &BlockIdExt,
        mc: Option<&BlockIdExt>
    ) -> Result<Arc<BlockHandle>> {
        let handle = db.load_block_handle(id)?;
        if let Some(handle) = handle {
            let mut is_link = false;
            if handle.has_data() && handle.has_proof_or_link(&mut is_link) {
                db.load_block_data_raw(&handle).await?;
                db.load_block_proof_raw(&handle, !id.shard().is_masterchain()).await?;
                return Ok(handle)
            }
        }
        let handle = db.store_block_data(
            &BlockStuff::fake_block(id.clone(), mc.map(|mc| mc.clone()), false)?,
            None
        ).await?._to_any();
        let handle = db.store_block_proof(
            id, Some(handle), &BlockProofStuff::fake(id)?, None
        ).await?.to_non_created().ok_or_else(
            || error!("Bad result in store block {} proof", id)
        )?;
        Ok(handle)
    }

    clean_up(true, "test_archives_multithread").await;
    let db = Arc::new(create_db("test_archives_multithread").await.unwrap());
    // initial key blocks
    // master
    let mc_id = gen_block_id_ext(ShardIdent::masterchain(), 0);
    let handle = store_block(db.as_ref(), &mc_id, None).await.unwrap();
    db.archive_block(&mc_id, None).await.unwrap();
    db.store_block_applied(&handle, None).unwrap();
    // shards
    for shard_byte in 0..SHARDES {
        let id = gen_block_id_ext(shard_prefix(shard_byte), 0);
        let handle = store_block(db.as_ref(), &id, Some(&mc_id)).await.unwrap();
        db.archive_block(&id, None).await.unwrap();
        db.store_block_applied(&handle, None).unwrap();
    }

    let semaphore = Arc::new(tokio::sync::Semaphore::new((2 * THREADS * SHARDES) as usize));

    let mut handles = vec!();
    let now = std::time::Instant::now();
    for mc_seq_no in 1..MC_BLOCKS {

        let mc_id = gen_block_id_ext(ShardIdent::masterchain(), mc_seq_no);
        // master chain
        for t in 0..THREADS {
            if t == THREADS / 2 {
                //println!("mc block {} t={}", mc_seq_no, t);
                let handle = store_block(db.as_ref(), &mc_id, None).await.unwrap();
                db.archive_block(&mc_id, None).await.unwrap();
                db.store_block_applied(&handle, None).unwrap();
            }
            let db = Arc::clone(&db);
            let mc_id = mc_id.clone();
            let h = tokio::spawn(
                async move {
                    store_block(db.as_ref(), &mc_id, None).await.unwrap();
                }
            );
            handles.push(h);
        }

        // shard chains
        for t in 0..THREADS {
            for shard_byte in 0..SHARDES {
                let db = Arc::clone(&db);
                let mc_id = mc_id.clone();
                let semaphore = semaphore.clone();
                let h = tokio::spawn(
                    async move {
                        let lock = semaphore.acquire_owned().await;
                        //println!("{} block {} t={}", shard_byte, mc_seq_no, t);
                        let id = gen_block_id_ext(shard_prefix(shard_byte), mc_seq_no);
                        let handle = store_block(db.as_ref(), &id, Some(&mc_id)).await.unwrap();
                        if t == 0 {
                            wait_mc_block(db.deref(), mc_seq_no - 1).await;
                            db.archive_block(&id, None).await.unwrap();
                            db.store_block_applied(&handle, None).unwrap();
                        }
                        //println!("{} block {} t={} DONE", shard_byte, mc_seq_no, t);
                        drop(lock);
                    }
                );
                handles.push(h);
            }
        }
    }

    futures::future::join_all(handles)
        .await
        .into_iter()
        .find(|r| r.is_err())
        .unwrap_or(Ok(()))
        .unwrap();
    println!("{}ms", now.elapsed().as_millis());
    stop_db(&db).await;
    drop(db);
    clean_up(false, "test_archives_multithread").await;

}

#[tokio::test(flavor = "multi_thread")]
async fn test_archives_singlethread() {

    fn shard_prefix(shard_byte: u64) -> ShardIdent {
        ShardIdent::with_tagged_prefix(0, shard_byte << 56 | 0x0080_0000_0000_0000).unwrap()
    }
    async fn store_block(
        db: &InternalDb, 
        id: &BlockIdExt,
        mc: Option<&BlockIdExt>
    ) -> Result<Arc<BlockHandle>> {
        let handle = db.load_block_handle(id)?;
        if let Some(handle) = handle {
            if handle.has_data() {
                db.load_block_data_raw(&handle).await?;
                return Ok(handle)
            }
        }
        let h = db.store_block_data(
            &BlockStuff::fake_block(id.clone(), mc.map(|mc| mc.clone()), false)?,
            None
        ).await?._to_any();
        Ok(h)
    }

    clean_up(true, "test_archives_singlethread").await;
    let db = Arc::new(create_db("test_archives_singlethread").await.unwrap());

    // initial key blocks
    // master
    let mc_id = gen_block_id_ext(ShardIdent::masterchain(), 0);
    let handle = db.store_block_data(
        &BlockStuff::fake_block(mc_id.clone(), None, true).unwrap(),
        None
    ).await.unwrap().to_updated().unwrap();
    db.archive_block(&mc_id, None).await.unwrap();
    db.store_block_applied(&handle, None).unwrap();
    // shardes
    for shard_byte in 0..SHARDES {
        let id = gen_block_id_ext(shard_prefix(shard_byte), 0);
        let handle = db.store_block_data(
            &BlockStuff::fake_block(id.clone(), Some(mc_id.clone()), true).unwrap(),
            None
        ).await.unwrap().to_updated().unwrap();
        db.archive_block(&id, None).await.unwrap();
        db.store_block_applied(&handle, None).unwrap();
    }

    let now = std::time::Instant::now();
    for mc_seq_no in 1..MC_BLOCKS {
        let mc_id = gen_block_id_ext(ShardIdent::masterchain(), mc_seq_no);

        // master chain
        for t in 0..THREADS {
            if t == THREADS / 2 {
                //println!("mc block {} t={}", mc_seq_no, t);
                let handle = store_block(db.as_ref(), &mc_id, None).await.unwrap();
                db.archive_block(&mc_id, None).await.unwrap();
                db.store_block_applied(&handle, None).unwrap();
            }
            store_block(db.as_ref(), &mc_id, None).await.unwrap();
        }

        // shard chains
        for t in 0..THREADS {
            for shard_byte in 0..SHARDES {
                let db = Arc::clone(&db);
                //println!("{} block {} t={}", shard_byte, mc_seq_no, t);
                let id = gen_block_id_ext(shard_prefix(shard_byte), mc_seq_no);
                let handle = store_block(db.as_ref(), &id, Some(&mc_id)).await.unwrap();
                if t == 0 {
                    db.archive_block(&id, None).await.unwrap();
                    db.store_block_applied(&handle, None).unwrap();
                }
                //println!("{} block {} t={} DONE", shard_byte, mc_seq_no, t);
            }
        }
    }

    println!("{}ms", now.elapsed().as_millis());
    stop_db(&db).await;
    drop(db);
    clean_up(false, "test_archives_singlethread").await;

}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_ss_lite() {
    init_test_log();
    let r = test_store_ss_lite_impl("test_store_ss_lite").await;
    clean_up(false, "test_store_ss_lite").await;
    r.unwrap();
}

async fn test_store_ss_lite_impl(path: &str) -> Result<()> {
    #[cfg(feature = "telemetry")]
    let telemetry = create_engine_telemetry();
    let allocated = create_engine_allocated();
    let block = prepare_block()?;
    let id = block.id().clone();
    let mut ss = ShardStateUnsplit::with_ident(id.shard().clone());
    ss.set_seq_no(id.seq_no());
    let ss1 = ShardStateStuff::from_state_root_cell(
        id,
        ss.serialize()?,
        #[cfg(feature = "telemetry")]
        &telemetry,
        &allocated
    )?;
    let wait = WaitForHandle::with_max_count(2);
    let db = create_db(path).await?;
    let block_handle = db.store_block_data(&block, Some(wait.clone())).await?
        .to_updated()
        .ok_or_else(
            || error!("Bad result in store block {} data", block.id())
        )?;
    let cb = SsNotificationCallback::new();
    let (ss1, _) = db.store_shard_state_dynamic(
        &block_handle, 
        &ss1, 
        Some(wait.clone()), 
        Some(cb.clone()), 
        false
    ).await?;
    cb.wait().await;
    let ss2 = db.load_shard_state_dynamic(block.id())?;
    assert!(are_shard_states_equal(&ss1, &ss2));
    wait.wait().await;
    stop_db(&db).await;
    Ok(())
}

struct SGcEngine;
impl crate::engine_traits::EngineOperations for SGcEngine {}

#[tokio::test(flavor = "multi_thread")]
async fn test_shard_state_persistent_gc() {
    init_test_log();
    let r = test_shard_state_persistent_gc_impl().await;
    clean_up(false, "test_shard_state_persistent_gc").await;
    r.unwrap();
}

async fn test_shard_state_persistent_gc_impl() -> Result<()> {
    let db = create_db("test_shard_state_persistent_gc").await?;
    let engine = SGcEngine;
    let key_blocks = [
        ("bdd5e5e8e024cab3a00f1d783095ee8936b4bf24afca1f8003b671c3f1ae6160", 1643599474),
        ("fc1dfe37245df2061d626ffaadc6e0e306940d178cc8e09c311dbb287160f6ac", 1643591284),
        ("ecf7e93afbcdbc6145e9c7a1ea5a715098751220a4dc72123142ba36980593b1", 1643533939),
        ("8b1f66bf7b7845db1758991ddefae2d4b225eb472ae83259b4697f4c2d5c16af", 1643525749),
        ("257277441b2710ae444889c947b5b144e78fdf625cf32264abfbaf515770ce01", 1643468403),
        ("ceb6457a051c2a8bbb10f5c08699a2e2e2e0474bbe872a78ff842ce8e3d16ae3", 1643460211),
        ("52edd3ffb6d3193fd22eb0a0180a779ef283a2a5b43d445811ede7a423f80816", 1643402869),
        ("5105031d9ee2ada49154d15e60892bdac028181e9145fbf57bc2609c4985c015", 1643394675),
        ("36a22c2239b41592e21b15c2479bfadeba4d241f9621517c71c6a5c80bdaeddd", 1643337330),
        ("773f6ab9d326aa11001f12170e6aaaebb595a50aa629be9898a94f5537318884", 1643329140),
        ("1ddd1c29573dc464f66d2770884dc389e229bc0f80a37c7f3a63ff4e41226eed", 1643271796),
        ("b4fba97ac4c5e257bbda825aa0bbd8930718e55922ac50123750c3f23ac9e929", 1643263607),
        ("b1e7e4b1d0badf57f13edd58535e07481b01e80a4b7d59931acee2a6af956a7b", 1643206259),
        ("4a0e9c8c3075f6e8eae6e10084c1c7f98e32c6e1f4501d0ee244f1472a3008cb", 1643198071),
        ("1b50e4b073d225accc3222268b4c0f7dfc5bad68d90140db8179ad512805629e", 1643140723),
        ("305336fe2170edbbea97e6469454f5165254507e25ad975cbc59abcba5b55e4a", 1643132535),
        ("e5ca64ce2aa2287b8899cbf4dbb18a2a85604b6f8ae1e7e6097871273dd46117", 1643075187),
        ("9cf97d2b416570ae16961dfa7d7af8b18a0bcda70b52176b9d2f4429b9e802bf", 1643066998),
        ("fc12f416c23308d8c5dcf5e739e53270454439da0fab9da70056ca78783e89a2", 1643009650),
        ("32abd3f8694d384ec7760b92bf224dc4a09b142db212918c3d9a9f79d697078c", 1643001463),
        ("b69cf0c1ffeb8ec4baccee3851baa11351db93c756a5ad156f8aacd1cc4ca3a3", 1642944116),
        ("2b5b58536183c3b3b3bb1ad55613ad675d6d58c1e6ce423db284044309ad041b", 1642935923),
        ("2b4d8d01897c7f2186809b7dad0c305a340c8a380807503e05128ff7dfb51be8", 1642878579),
        ("434a19f25cb0061a71d8bfcdecf43f5d074be8680e0cbdaa49c4f78079fe0748", 1642870388),
        ("f48506b0899af83f1784f62078be0051eb01809d4f86126b78a2972b3602cec2", 1642813043),
        ("041ef37aa6a70be6af10cc0ecd472b8d2b8f2d8bbfc19723f596f8cbe6cde6e0", 1642804850),
        ("b63231a40a2d685415b7a3ea4e3c426f6cbb775e148af9518a4ab9da0065eefd", 1642747509),
        ("01500aa403af9df9db6ce07f71a2cd95acb1a64d090b05bba977141a044c4b94", 1642739320),
        ("744754b0fd79b411dfb3ed18f3c6665eb1504b29006a0cd0b65da0208d529c92", 1642696765),
        ("7cbfa2970202e994d1ffae5382990209ab02255c064ac529c6e9eb3eb7ab9844", 1642683770),
        ("ff63d32f28e708712f2ea56df70fd7b9cc46b3916720c2a3d11e004f4bfccbf6", 1642681972),
        ("936e720e3100c9bb35a41da57622f96398daf92aa385e3af3b0e42ba68e47cc0", 1642673778),
        ("e33b0843d217f5e3ecdfd220cf5652ab774a168cc8f29eb53381d53ccdca7f79", 1642616436),
        ("f94e5cfe6bd3be44a6c9d6faadc4710f136a7bb270ac7123dfabd80a9412c2b3", 1642608242),
        ("f5d26eaab04ac9f2de4eb5a7f47500acb255147bec3fa966fdd865b169ecfcb6", 1642550900),
        ("1e7705b14f9cd256b21634305cbebb31ce51a21a34136af570440ee487e5685e", 1642542712),
        ("8f51226dc1e27d0b5e03575e218e84cd6415516c5fec366bb8571863bad6b2f5", 1642485364),
        ("02c5ec3307feef86d1594c36292a29c436b5c1c4ff222225e5fbdb03b81f9d88", 1642477171),
        ("6562f1a125993b825d93a3a138abd70846ff4560cc63704716a6ebbfe4ceca80", 1642419827),
        ("270df3f493e1b2a5bd5c58ebeb4e2cd0963ad384aa4210f09e408d27f708ab31", 1642411636),
        ("f99c150f50c202729e6f3464e1234b1282b287ad462d21ac6976b33477e0e21e", 1642354291),
        ("17f865386e3a97e1018024e1b7f228cc5d0fa6ce5a0da684530adf02b59a5cbf", 1642346099),
        ("1a49dfea4ae32f70488cf1093ae78393b6477e78e506115f8f2ef8711ca3a843", 1642288759),
        ("af88282e669b2bca60c9ba818b34849c328d54ff7d2c6950e7537d4967a30be8", 1642280564),
        ("65b9946edf6285cb23dc806498c5690cece72ce90576c57fba8b2851478d87ca", 1642223218),
        ("cf42362b92537b0ee6b5c321bf94e64edd2cdba39a5273207bcb6a2ea05e170a", 1642215026),
        ("4597b0318d5a3b4db31dc99b84182be80c7c3acb76799a44cf7c8622fb725c03", 1642157683),
        ("0901b183c97c1384712adfe8b77380b8c3bba69cfecb958d0ea75adbd9a8699e", 1642149490),
        ("c523fe1ff1a98879acb716b6a29deea6977a9ce797c2c0283fadbd03f54dbb22", 1642092181),
        ("a92bf479f2d78eceb0eb460d44a9e84e8c151e4d516b4d00b87a7f26d0369c9a", 1642083958),
        ("1a1d982c9e87fd0a61fc354d475804a963adc5e37feb565942a0bfa678c1de85", 1642026610),
        ("0bfbdaa620c938061a82cba80fa20b560ff61000a52cb4d6f23bc4d90deb4be7", 1642018419),
        ("1b5c2b94c695d2a41613c6e6e718a22e25f094d36c52e8496fdcb5287bb1a824", 1641961074),
        ("3006bb48845bf4ec6ac34dba48249132e5c5775ee5e2bb9aa3ed8902fdbed4cf", 1641952887),
        ("2d22d4cc00a560278967a55353a64c90dafaf7f1b7efb475c390a4079fd88848", 1641895540),
        ("f957f91e1a5d241de35dc920b8de67d296adddbc32700dbcf35f936f3c6e3f89", 1641887347),
        ("368f391788f898e9861eec55d79c369b51aa7822b6a8e5aab5f1c594e7e10bc2", 1641830003),
        ("eca30b15003ac6841194fd50f9d925160ef0c015be8c81c4d1d60d71d5c3ec62", 1641821812),
        ("02a37ab271b3ec50a28d34ca8e991ea44ed7a462dcff9cdaee05da93e1f262ce", 1641764468),
        ("80cfd751d2e741230c15e838b732011a5f4a6b41b29de0a5fb7e5fb526e38198", 1641756274),
        ("1dba05c332916b03ed5de20b1310043177362127f59ca2ef3fee2e58e8bfb9e7", 1641698930),
        ("f4952d44b8a2da4100a4ab6c414899c802748fbbfabe3a3265105cee43883d8a", 1641690742),
        ("87539f9c50344af78927a235db42111930f2905d5a0609f6dd72392d8ec8b042", 1641633395),
        ("0c52f637c4a2302e8ee65d88e4296e3cc0caac96af18d48559e1ef9124423414", 1641625206),
        ("5c4aa9ea3f3637f631038e2232defd96347872d1638e966681762cde0e42b98b", 1641567858),
        ("2901da4d2b93b8c769e30bc29c488e7ea404c1da301b0c5db4716b4e5da2cdd7", 1641559666),
        ("ddb135051f9ab42dc1ee64dea226be5d3ccc33e247b38ad8035c37a8a37fa757", 1641502323),
        ("a213da0d262d521a8b7241324b8b0ef81c730ea74bcf9000b8e3f9dd0f0cdcd0", 1641494132),
        ("6dd024bef29fc299e2632c319b618a9a08c63315d93ab784b55cd240fd5965fa", 1641436786),
        ("a4ef5b1d9db46761c0f6a266f05bfd7578db1298af607deed30f6b77e805e12e", 1641428595),
        ("53709a363ca2f61ab220f2d8047aa9e1d24a259f6fa2bb764588d5e061ddbdfe", 1641371256),
        ("1a809da147ecf6cb7a0da48666b52d31c62b836a972370eb00cc9cd25cb04496", 1641363059),
        ("e8cf0bc41ef54ed4f06b23838b34df94c970b3389d4c09e44250183338729b31", 1641305714),
        ("b326da4de837ba324232baf302d3a3f7d71d89f917aca659cb5fd89b49ad0d58", 1641297525),
        ("50d5d0baea232b09b9c9c16af06c931d55829dd51c22a39c9909794e055af1d1", 1641240179),
        ("795946a713c5e9122523608a190b9463b9f658796c92fa9db6cf04e50f64005a", 1641231987),
        ("eabe8e18c316e43513727d13d50446f55ba7dd1c519c87d4355b26a88557dc42", 1641174645),
        ("97a73f98e26a28fee6d7365f4a72bf79c736169879ef710d3e2da070c69cf4fc", 1641166457),
        ("ccf1d86f504e2cff6f348d21d7391795429b812a8d2b542ae5c68f5f33f68f61", 1641109106),
        ("946d32b8b4e05361f55d6993997b5e20e731af3601b8e4124959e8b4d5eb6a40", 1641100918),
        ("0255d37efa0bd15494dc17d24b29b89438d8b0d849539b2fb05a40f58f92f075", 1641043571),
        ("cad65f2bc5112a772084ad6e5312ca79051685524e9a4aaf78968f3d5efc6c74", 1641035384),
        ("1b3777628eb396ae96e7aa0e03d071781bb643bb0100522644d0da4f816f8d48", 1640978034),
        ("33396bf88117fbc2ce40e32a9ff21e4d5cfb2c85a251aada65409059d9e6ec80", 1640969848),
        ("fbfe45693226984fac77bbccdd7b0f464d4271e3421d07959bec7565723b2805", 1640912502),
        ("7b0812882ce9739c1094ee5e3308e12df2290f4f6aa5811969828eed09659ad8", 1640904310),
        ("4072a41db8bb7fbcc004d815aa29ccfbab91309ac51bfb88d54ad6915036fed4", 1640846963),
        ("d29e0965eced14e5c86dd5bc791a229764bbfe9f1ca8d6570d4203debf5d4968", 1640838772),
        ("0b82e40c516fb639bf993defeab555aa9429de5662d81bea05523cbef30e5e9f", 1640781430),
        ("d4e4ddac58bfd0334a19ba84ca39b92ad739e9e2f606e4869180e99f13b966ec", 1640773234),
        ("dfedb377a42d8ad9b36d95401f42a2734c8df59b2c5b1879fc40ffa7b0724431", 1640715891),
        ("c0fdd0362c871e954c303dc4f72631ac0a903dbe047891f06588b6672697be2c", 1640707700),
        ("790220fa58b3dc10f9ff4ebbb4f866a77096f595df14f48247c2ec3e5780e290", 1640650355),
        ("55b99424ba53b95b8cae019a45159141973478b032be15ba9cdb48d09a5e96e2", 1640642168),
        ("a4dc53669727db95cc0f337dc769f09e8fe9a7b1ca50a0909be08dc1c5426366", 1640584820),
        ("c8e6f7b35a8a48cfd5849493b14d28207973aa5029c6c53c7431229e8f02d6ca", 1640576628),
        ("40cc3b59693a3c27bae34617ba7fb0c991cefbb555e7496b0314cfb66e2dddd1", 1640519285),
        ("1d158f1fbb49e71f4d997947735595eaa743ae8c70e52bf783d38acf33d3c4d1", 1640511094),
        ("a3c85bda9772e5281b0bba9fb35cd45a15fecfc26be5e946ea50f737eda98665", 1640453747),
        ("3cc03878980134a0dbd7a832c0056afba1dd4f2f1fcdfcd9a7f4066dddeff6e8", 1640445564),
        ("78f07637bfe5bd25682647c6e209a5d083441fef1acc190fccef01ae368c926b", 1640388212),
        ("a118653161e685c238c55f0c39f5fa18a5b14f3569d2c7d0a21a6b51dd22a451", 1640380024),
        ("bbdea7a2f4907c96c2f8588f7d9b93e1b7279ef2fd24afe5d3da25946a3be533", 1640322675),
        ("8117f146a5850192b45310916244d1aca7d6badaa9149857bb2bfec6be83c093", 1640314482),
        ("2586ee594c935f6e91c26da613df8d5501393067de92a643c02c99c6880eb7f9", 1640257138),
        ("32476fd41cc5d184639ad2aa7eade144822ebc1239eaa2d4ff9ad96717ce45d2", 1640248958),
        ("6f34284b8d1288787010fae254c18cb6552c486282f32a86f8973a0da908671d", 1640191620),
        ("066b33b5ed1ba4994637e0edc5cfd665e6c35df9b88ff2ac3a9d199dda3c99b6", 1640183410),
        ("745d7cc6798389f960b5b9426e47bce83a3629ff17b00ece20b256b78a8d80ad", 1640126066),
        ("a94228a059d4c22c65218aba2ccf8b75bdcaf026ec83c7a49316d9856f16a61c", 1640117876),
        ("3d18156f9ed921777cc9af110729bc26d9403f20445c1cbf3fc77ee04f3c53d7", 1640060531),
        ("24a1233d21796ef172edf1636ad71723c8bc61f63a5e268a3fcd20f80ac4b3c9", 1640052339),
        ("70e39806d826bfdbe08b9154faadddbf6cf86eeaf9b5f95fd9980033b9d43718", 1639994994),
        ("d18c4b66da61b29baf83a5d0f40d7ac7ffeb2ff90b527872196882b103083c84", 1639986803),
        ("3d46f0d6e84d2c598dcd131afc88828e176a5d72ee9b177219e593ffe38a6dc0", 1639929458),
        ("7dc267658ab2d0b7c8d44af294cb26968eea4e79eb27812ab45d88144a71d817", 1639921267),
        ("695a8fcae7ee075c65b59dc7f471ef1c22511d6898cf3488edb14814b596ec71", 1639863924),
        ("ee30c95a19c595ad54c1e5e8655b50718d4c147932acbab82dd749436e734110", 1639855732),
        ("068cfad4d0e9056b0b77268c9e94491b884c279577dd94e4bfcbc9d08ba37b06", 1639798386),
        ("3801ecfb89c90cf71578c87308fe7137625d09ee6c3b8b7a4d60f8dc70cc24b5", 1639790196),
        ("fe626ee0188ab550207ac2848ef99032e17f8e455fbd5171836f7d7dc075dc77", 1639732852),
        ("11f6954df73a3a16e4618c0f8d16dabfc599d0602e0d471125bce1bf2d690815", 1639724663),
        ("cae1b60177e35a39c11c1698fc13dbb803574596dec952e593d6959c4ec34ede", 1639667317),
        ("bd1e28e96ffbae9dfa2fa3e53489dfdaecbc21872e2ed83bb0567fa5a0b71763", 1639659128),
        ("312dcdf2104adcc23e54c29cb49b4920e10219b44c716450b6b850e46b7ad340", 1639601778),
        ("d347fdcb568277918f2b7e8c49b7c3358c72748ef279bb32b21e8f88c6a6d727", 1639593587),
        ("9981f9b6c7b94772f3ff2a759d98e755cfae63c4a28e2742271344f719faf100", 1639536243),
        ("c9a4aac22989b5cc0355f06c316c9877364fcc70e46f0e9c4c4d02f3a806d9ba", 1639528059),
        ("add60b6d244dabf8799557f0b5ead565304732763016fe6283ee76e7cad4e234", 1639470706),
        ("2951c2c75b1ac5f677d097e9a7f58f58539d58f39732fde5c1b56ad5b5f879ed", 1639462518),
        ("3bd68a11ca4f5e35308da8546e5dea1782307f8c6ec9c3295c64fa1289b7cd5a", 1639405172),
        ("a5248d9a5872107a5acd84571b0224053af11c40fe8dc476fa29be6828e0fe81", 1639396988),
        ("945ac022d3ca557adadf60f807887c866c8c2d4f108d9b708add8801e03a7042", 1639339634),
        ("f9d8be41e21ef1d8d4131403dfe90e4f22e3d71c892b83a7a638ff38725660f9", 1639331443),
        ("4663aa4fef642a32dbf806cfecd7f08865124e654b7e81aaadf5547e03f93fc1", 1639274098),
        ("ee6166b4fb7a1ac10725c44c55a8bed5dbb6eba3c8df2bfd3c07b3f9595ecfb9", 1639265907),
        ("4a7fb8a957329c5b754fab2f021f0cb3a8ae7f711b495c95e4e9c8ee4c77c0b5", 1639208564),
        ("2d9170068f7a4c501f83a26d46686bf9517c270692005e490cd2cffccac875af", 1639200371),
        ("4fbb371b22f1f316425aeab80f014a8770f556fff399ceadcfa5e2fed2ab1852", 1639143026),
        ("ad9b47cf2a0f5599c7fe8910b1f9ed04ef8bc49d745f64eb616e57abd46cd9cd", 1639134836),
        ("d54a28bb2b10169048ccc03588ef542d7591dc536ca076fa04cf2cad1429dc0e", 1639077490),
        ("fab3ca5301cf6f8364b2423e311069f5fc3d6ca96072ef1f12576fe85c6ead05", 1639069301),
        ("42a5c84382c518ccf663d756604ad0ccf0905c86192c1eccd7d9102ee02ba9bb", 1639011954),
        ("ce58261f60f3a86328032010b985a2e9afb3b68821609bc861c3d4e3011aae55", 1639003766),
        ("3de4916b033640752712830d68a3d2874fa7c9d6929958ae6b9353afbe195314", 1638946421),
        ("a008c2640fe4a95bb51ac33daf84713c1c4f754671cfbf4d9bb68e3fb3b7e4e1", 1638938226),
        ("f95461bbec0d9c3ef1e4eeb69976a6362c00e3abf286a598ed26188abb1f334b", 1638880884),
        ("53cc62c2dde8c2a72fa252703adbe7649e025d66f2ecf9e647867da2a5783c17", 1638872694),
        ("7e32e9f43fd329529935cc8864cc1cb9d5004238ade0edadc3c1fe24ba3434f0", 1638815346),
        ("8c435cf8a938a094a76047db405bd36f70e617523225939964d5ed097e56a5d5", 1638807154),
        ("082a2a6470a82509c5f6bec3fb90ea7d435cd5ed221f27baea1655eae7d7c644", 1638780570),
        ("33366ef472ffaee26fe815c3bd0ad75abc418b768a424f976fde737ab80ea0de", 1638749813),
        ("1c7119f78e45023ee52d7ce820dbc3dda1f13a0c2f95bfea8c6711993dd1d52f", 1638741619),
        ("8a2804da1476edd2bdb7ef574fe575489f7a966dd3e8b8ecab3b803ff93f1410", 1638684275),
        ("c7ac9a781b058d9354e11d1d65999504c8ace3ef37dec2859a0fbcdd973fe393", 1638676082),
        ("fecca3d5018ee5f0b83264bcb555ae6e2109ed3cae1e2946f0d9d16f3b031beb", 1638618741),
        ("a9ed8bb944d8e98f6b5eabdfb06ed79a3013993d99f239c0e92c179494d1322f", 1638610550),
        ("8110addd97be0bc13bc86722fe34e610c9f252b43e72433a7c13018228e69f20", 1638553202),
        ("211c3e3f6e17a4901f8c5f515c0c828717f65952b4eba61a850f7435b7dde4cf", 1638545012),
        ("ae97574937069a1b41231e272fc5ab6c6b49907a94256b23ed94460061560e88", 1638487667),
        ("993c352f882315dce2beb2e53ac2cf42fb1f04145d752eb73cc2625b6939fd58", 1638479475),
        ("303a6c9aea4cccd9082e8d1f3ad091612e5d045633fc3af8dd05e8215b48dc02", 1638422133),
        ("0c201b73acd90581b4483ade2d59c1163a596411e0934b56f7af24b8190b321c", 1638413940),
        ("c550b422084e3dbb9c95e96e10166cdccc58260d0b03b28d4829aaf4909a43a2", 1638356594),
        ("f8868cd5d05d9fcfaf758170e7dc0d49ce0d20c1b9c1f83b7a469a306df9a7ee", 1638348405),
        ("ce960c63ab6b0eaa3d19e1c926185a00dfd2a43fcb14e40ce21d49cea4a40a19", 1638297986),
        ("5c1796be353802611ca2cd094b9e05dcb3f28e2f9711ec6e5a2923b4d14e9203", 1638291060),
        ("3d74d9395fca0ad772930147e0e745b17f4b78b9f7696a430ddce186b9016a09", 1638282866),
        ("1b96f0c89d848f964c5e4c084af318c5bade4b6271d15c8b41359f196e12f140", 1638225522),
        ("2dd29ecc55673375eb63722610107bb1ac4e1f76358c03709b6de0a72f4de822", 1638217332),
        ("4fa0ac9d4ae5d78dc92f353b61c0b28b76de1d0546c06be0d646cc32c7ffebf2", 1638159987),
        ("d0c3bec81426013b89bd58603ab7c7592f2287784bd5cd286f8d4a7328827477", 1638151794),
        ("43757f41c8607a49a3d1d9a916582c2ed604d68d662d5890e2b7b9e919171932", 1638094454),
        ("585be34848fcb61d6f2b6aa648e69f9dbdc21214160fb27499c3ea2f4006e9f0", 1638086262),
        ("00681c03df7065f8e6d6485e6ccfe889365ebb5f1c29d1863ad4624bab478e35", 1638028914),
        ("aed0ad6c557b7b840cdb4760e37e64f3477c8684067f1a610998be147e346e0b", 1638020724),
        ("6bff9f7f566039954210288a061a05abea3865059a947ba777dfd207d3dc2522", 1637963378),
        ("c9da59d8ee1ba3b935a031fdd9cf925cda867c920010cc819eba43b44a893e93", 1637955186),
        ("b8021e2e7463a8893b9b222711d8f65abf8884dac209aa4ad3d519fab2415381", 1637897842),
        ("e35cf534d12e3a5ccaee212190060c3c32076ed10122b2ac060d2b81f5a04cd2", 1637889652),
        ("9f2572dc42ba7a7b4170b705f786c5f6f04915862d5492636e6b769e6094b093", 1637832306),
        ("16c7d60816b5719ab0adb2beedf24651a144dc57b51745051fa73b14469e64a6", 1637824114),
        ("e16d25c0f9b5daef771a109f338dc68ba5af2f7fc431a57e59ce9c2ef13888ba", 1637766770),
        ("5d6cb32c89142b464080cbd7a34f76e3a5eaee218685a571a1fdb6308a75e93c", 1637758578),
        ("53d37c9905d770dcf9436ca1d810a6dccdf2c4be2647c0e4f91ffb9038bcf6de", 1637701236),
        ("23c0c010607aa329593b195ae345400cb707f6240329406d5b0eefa33f8508f7", 1637693042),
        ("0b42a2b3c13e848c00177daad33f417b22c45a5de47afdd5cf1d79d574ed3587", 1637635699),
        ("9d14b356705ca1727abc1be419af77a0b4c3367e3ffb13b33b85013cd12f26b8", 1637627515),
        ("de33b69f08242b9d0937e3dc7a7f3c468a026eabc8894a0280709061d5bb4a22", 1637570164),
        ("5f2cedc1577dd3a911bd816ac25dad6cae3a6a4205b469abc5f1391ae0a2f44c", 1637561970),
        ("590d75b88946959fac40b248349894516e1500adce160144ad41c672ebecc570", 1637504626),
        ("e4fd37d9687bf52d4d0280a10d988a0387ca005dabf1cea88fa13ee05a4c8c67", 1637496436),
        ("de6adaa25f5a9231a3be298fbd16e1b05c87cf4bc8431dd1fd625f68315cac3c", 1637439091),
        ("d9b10c54cc30586fcefc9ef945ae63e84c45a85520e5c5184b484e35e8fe2ce4", 1637430900),
        ("499d8524901b75048ea2a6df453cc3863660e2086de076c8af9b9ccaae6f5c9c", 1637373555),
        ("37fb4efe8cdef085f47601575530f41fb122f7f3a2b54b4b91274392023f076e", 1637365363),
        ("e3ed4793788b8b58fd2c54f7de41af194ec3fa5bb0f72710a9a8dfd57d864e12", 1637308020),
        ("6d73c1f10167bf8092b4644edb11e6328108af3fde6dc9ffa4734c1784a9d9c8", 1637299826),
        ("282f746db1723e89889fc15ac646204d87997bbfb41cbf6df960b66a1f8f5e71", 1637242482),
        ("652112b06d8d72f044e2a72fac52be9c49fa82a3cdb1456c79417d3bd7c68725", 1637234293),
        ("44da734ea957a13e668a409773b8e6939a8a70ff77025b82d75f01b83d7235ca", 1637176948),
        ("1f7af891c8e639ae151d564ed41bd42db88ac9ce413c4c24b3aaa4c5c3b7c011", 1637168763),
        ("d967e459c06a98da214cb26357f105b87b5e3a7ce485ff6eedb635a38c90b3e9", 1637111410),
        ("58fa6157254bd8ebe46ca18abb8afd70948b7c95ebbe4bcd56f44a888f1e3df1", 1637103219),
        ("c863840b3ccc917f829df4c691c6ced885d31893c40f25548b4820332b33f493", 1637045877),
        ("e0f2add24a8a16a22e95301d0d0ffcf64b96d7d331c78188f3825212ba1a11e4", 1637037682),
        ("f4c73f322c735cc682cfbbca40bcd867c2edb71269a8b214b9c2a1cb376a98fd", 1636980340),
        ("107ab69ac371a463d05b49f5c2f7b5090b0c6198a13d4afe53cf63633a993ad5", 1636972146),
        ("ed8091215145ac5fdc6c5e809ebdaabc647d7da3af21d86f1f63d16f1ed48be1", 1636914804),
        ("6646d90f997efafb0c95287e5c500f36859da35dcbcff4b9698e147ca21210aa", 1636906610),
        ("007c6d51c676dfea86550a1aa6329aa003d6667bc51e9cdc295de648f1b8c126", 1636849266),
        ("825f0f2075dc1fbdeb7555cc7ed6caa2bee1e624549014d8016dacbb85faaacf", 1636841078),
        ("393f6eac4cd94f541fff02230a43867361d7e8715dc80c431249482eb74e3e6f", 1636783734),
        ("8244073a09fd5e46711a1100522ce52718726264adf189eb812f94662f4e0f52", 1636775539),
        ("4d558d76d28a83c3a7a42ea9dd1aa10c8ecb7d72a84f0653bdedf2c9cbe2e36e", 1636718194),
        ("07c7cf5500222180090076a09f1dadfb53cbd5f63157f7c0229d5af86900bf84", 1636710011),
        ("9da1da66c413654eb2e854ac7624424a3be4a107b7ecad33baa85f5b5a8ae075", 1636652661),
        ("c83750ff83644261d93dd0777534b92f1b21468c4d9ba94d76f5ca7f9aa218c0", 1636644470),
        ("055e756d146dc6acd3a11f570c6442e1c24144d0f5dc423deddcf39067669198", 1636587124),
        ("4dc765243c961a6e4f09fa63dd9cabd308b73e75a76370a8c06c92f317d6e2d2", 1636578930),
        ("209ba9c9172a410e490b17ee8373058e1bc467ff72d594846b934ec0c8ddd4c1", 1636521586),
        ("ef83f74a5a7ceed05bf5bae83e1ba283286611bdf24f8d7c7e8d8be76b2706bd", 1636513397),
        ("ce81e5fdb9d037f6f62f77b9f34a28966c1b8195e7d9300bf63db36d3387cb82", 1636456052),
        ("328d24cd35f14793844695c071b756d47c639b550813ecd33072a48de000bb32", 1636447858),
        ("8a9b18f5086a358cb7f60b2f7d68e65af1750ab81b6a6a9bca1ec27f3c2f66ed", 1636390516),
        ("85b604f9ab2cc3a3a18265b5f497f0b92a77c3c9e39b0948c725d83c691f3a11", 1636384052),
        ("5dd66eea33de14fe94bbe6134648e92951d5d8550a026b61a072f32cea7becb9", 1636382322),
        ("b77d166798cc6297e0bc313e5e21ec8f23abfd0f3907fa6fcc7268dea5a58be9", 1636324981),
        ("9463c0c39ff7f79e69c040ae7c7dfc5150e5039a836a2a6f2476083eeabfcf65", 1636316786),
        ("d3edeb889828b8bce231d9284ca7b516d1a1896cf7ff55609a77d0f2156d7dd5", 1636259442),
        ("398db0653d9dc7eafc7692991be8286056f5307747edaca218e0e24dcbd49182", 1636251250),
        ("05222ec749b19ed869caf4b53563774428d13b3e7a19afa81b8c249c37961bd9", 1636193906),
        ("bab068d7fd582a1161e43fc269eb8728bf4c348d1eec47100d39d5d81ba69b63", 1636185716),
        ("47ffaba0cefbb2938515449c757c8c8598ba9f5ba67496960a9664d9a207433e", 1636128370),
        ("e636039fd341090a53e66b5595f027dea58fc94e3e5e95aa5bb16c849d8162c5", 1636120178),
        ("cc3831c9f409787efd91d0b6011fa55c6e9a57fe84f73fd566f100a0708040bf", 1636062835),
        ("1393f3781fd3343b2dc1826d66243f3d62d8fa8b6dc7563e28535fbd1f91d11d", 1636054643),
        ("8fa7dd4d2dfe6c73ed36e2ac322a377b0406170500e5ab46dc1b1fe3e51cdc99", 1635997300),
        ("cc548a8137bad5daacb69533d0cbb8a9b6afb29edb21bb68d431348fc1d112ef", 1635989109),
        ("2ccf03b4f3d3885aeb50968fc7814263262594358faf44ee412fa6da17117654", 1635931763),
        ("75a4f3485b197fda42be797ba3cda9059e729a797fc23134615e7615b163f5d0", 1635923571),
        ("52b0ab48f1a2c06cb3537e186ac830cac9811f70c75841d629a5fd4e31b20652", 1635866226),
        ("f0c839a8532478f356bc385e97a6d52aae62b40cc7d0e2ea5296a2392769880d", 1635858034),
        ("41bb54a186f3492e6e47f454dafd40ed0cb96becf838abee69e5bd0000093dd4", 1635800691),
        ("cc1fdd512a616cb9d6645504bb63f55cc577ea27f2014d991927ebf25ab4ed6e", 1635792501),
        ("5ad239c8fa4b7d72734e407e35f8ffa55f13fe731f6e454414311c06d773e0f2", 1635735155),
        ("8a2662c44417bc47c1b2d67d88e23e7051f07cd20817c85269ace4c1b3c843a3", 1635726962),
        ("4b794b80a37ab63bac3bedf45ba8d3435becb66a01aba4cd0e4bcb80ab899cfb", 1635669620),
        ("3eb1346b2cefa8d191bd49a417af86e09218ee84f947090ed219bbc3716f8ac3", 1635661426),
        ("11ca3311221300d5b4723161f84ea7b096c8d2b56b3f1763249582d0d3f3ec58", 1635604083),
        ("805e933c4f133ecaa183de60faeff45b8aadbce4f36752750213e0c55a829b63", 1635595893),
        ("b07c0a35b75592b4393ad722c11fa77e6efe9d491792be046bc892a33a814ecd", 1635538546),
        ("299bb21f1aca9a26accb49167fb06e1742fa9e896b03bac8a9f5cfb1656a18da", 1635530356),
        ("11a34d18c7edc5967896405f134aee929f3706d6479f807c0bf157496c9060af", 1635473011),
        ("7110358c077455ac60f75e0930251b07654a1ef802fd79e8cc7bfc9aa1bd256e", 1635464818),
        ("93235bed538141e3ef5b839b141f74f1786efc503b671183be6b8672f50d5c5b", 1635407475),
        ("abb334b5350eb43bc6fae5d41cfa0074eccc1a333658c2e17424cf40edb06b34", 1635399283),
        ("b7c63ba47a2c14454db6915a7f6838340343ca80385551e908de2cdfaea2aba5", 1635341940),
        ("f958f07ac1b1fbb5c9c49dae2a6ff6cba848d90c71804d65f810cc21f0e18394", 1635333748),
        ("6fc38a88790bf3587bcb2ad3c1e473a4cb9c03041b738f8cd73fb6b0fa9c2ea6", 1635276405),
        ("effe24ab41223093c8bd3b775668fe296f105a819260b54175ddfd8f2293da49", 1635271773),
        ("78415271eea45a2b87e466c9113367a42182c3c5600501bcaa923780f81f5b6e", 1635268212),
        ("75c8b4f7cbe00789c05d0efc1a5a827c921bc0b3613663dddeceb73aef2e4ab5", 1635210868),
        ("c0d0efd58c7ff65f92f1771a8400fe4c1d2412ca1a55aa816d59005e51dc75c2", 1635202676),
        ("6aeb225d615ba45e6b0e4201ff1295ea815eda9544e814fcb98b48bb4244a1a4", 1635145331),
        ("2b69bf3a121f589c66670bd52870dd37545bbad5c19e52622b88f58a081d76d5", 1635137139),
        ("daf4334ff05d6ad3a83e2519d581ce3179c3c5315b15fdb5a76db4c2d9f48716", 1635079796),
        ("438b3f20361c8546b71fec4ab0bfa62931466958718ae9ae3c95aff01036c89d", 1635071602),
        ("2c0842bea398f84acca2b321461743479499e59b8aa19438b3cfec211d7f3dd4", 1635014262),
        ("40720dde0c28e52ec1572aae376174a7add996d47aad3dba51fa41988d722e1f", 1635006068),
        ("346be71faf7b31b703d60a5f0960d3a36dae6763b8cd0827668ef371832f2ca2", 1634948722),
        ("e2b58758369cfb673a2756f23c1bacc7652344bbd595702d43e15fb150a3fe27", 1634940530),
        ("b637b102b1ffdebedf29daa05d9ac0786f207ef511cf63df970eaafe9d56f827", 1634883186),
        ("ef296f13090fd21c922826726b1aabda46ac6d3e89e6b094fce36f5ea7482bf8", 1634874996),
        ("0ce678699521df3d24dfbfdf6d2143beb84b8a28a7899fe5b989769f026819ba", 1634817650),
        ("e16ba6e53a29ea87704d4f9d3aee7e08fa10880db1f6b97a6cb28553a3d09317", 1634809458),
        ("888af6fb17c22e4e5d675ed04d08a29e2a59c9f05bb0d097b3ce73bf25d88656", 1634752115),
        ("62a2b37f1bd07756550b196351d5d9323ec1aff48f657085a930167a8479ffbf", 1634743924),
        ("e132638a7e6a5e677e743ecd1f85c5901eeb44ea93198336b628fc1a7a57511c", 1634686578),
        ("feaef4a280d65dc24baf3a959caacdb50a37545385d00fbb4a09163c185e01d6", 1634678387),
        ("b04e9d2c0f1f7e8dbbeab6d7df9e940bbc6cfb899321995325cdb48107f7635e", 1634621045),
        ("384cf56fe88f68cf23623d43048a163b80cece6057cafbcec0807b66fd6f6bc3", 1634612854),
        ("da0dfbd39780195889549ee4c8f1849bdde3cd25983348cef40f3d2e5ef35a77", 1634555513),
        ("50975e7ab918212d0994b1a4de1c3d662d0eb7b19f9cdad7ca2e9cfa54fc1bef", 1634547315),
        ("94178a51a2ea6aee9cc4032e59ef1a26180412ed7117fd424de4038fb2eb753f", 1634489970),
        ("dee53e4eff6ef4bbf94d5ab59b74dc272595c1ca65b599beb763b5736a511ffa", 1634481778),
        ("1bd232821f3f529868fe9046125ccc1acc310b3c61cea530148ea71763159c95", 1634424436),
        ("88325696c53c83496882510e9adfcd159b37ba5351fa8e9b015f8909e801853b", 1634416245),
        ("53626c128b92350e2d134ea6fe2001ad8f7cba579c2644b9ae28634ce1dab502", 1634358898),
        ("30c14020480b91ec01c6f7777f359df051eee794f58b2eaec0289caa1ed499b0", 1634350706),
        ("b2086a910fdaaac4f7b69734b43a09464bea2842e25d7f07018053fe37f1a32c", 1634293365),
        ("b5593d874ec5726677a9aa953a07b44f3849848f448456327b816dea086aeae4", 1634285173),
        ("ffd2dfc481213edc7844ea684bbd77df88f15cd860d8e6c18c579a3058aa516b", 1634227826),
        ("9e2e43b7198297f269360005849c1d9923f750a432d547fb59e607819c31b182", 1634219637),
        ("e58e5150037d9d81034502960bed3425defeb23453df0b51498c268805512f78", 1634162291),
        ("d5d71b5efbd69cbf17fdd8f3abf8d70bbf4ac19f860b782cfa176226acb7fb53", 1634154373),
        ("06d3604790b4704f2aa0945ff1c0f5389e9c6cac18f89ff6f78da2de391ea972", 1634059487),
        ("520022fb1d80a528c8691856107d8716f9982031ec99fdf094a318bd0059f54e", 1634051292),
        ("3e7bd5734913bb6a3a02d01536300c2705ce37c14aa4795210c06cf0b1007c27", 1633993949),
        ("9eee20c3a93ca93928d7dc4bbbe6570c492d09077f13ebf7b2f68f9e2e176433", 1633985762),
        ("45eee4d5c6bee315fe0ca3da623b898571da4702369fd35c0b38fc92638b5131", 1633928414),
        ("6be19dd31a2eacce41648d334ce78497fe9a55730976f94c6d6e982b1e8a9155", 1633920221),
        ("4eaf516189344b175c4bc634dbefd5f65886d7fcdd5c109112de2f197bd5918e", 1633862877),
        ("645e0f48e6f4914412887ce28e00612269258d1b22d6ded35912711dfdc21b5e", 1633854691),
        ("1ed81e7d4d63ac15c87eb2d733dfc9890c6ef55dc6197cf37dfcc4172e184303", 1633797342),
        ("bc27a257f22e9be83d5a8181803c52e0563f1ba7f0731542a5e7ec663146c914", 1633789150),
        ("7e9a7e2c68cde930ba30aecbc10231bbb088e564e0dcfd79c718261de98e2669", 1633731806),
        ("2bfe2ea707026c971ce03014acec785b89be784ccd95aa1d44a6a306f62ee466", 1633723614),
        ("e5a128494c839d8fa2b8247374d3f4ee075421e63b3189954098120d4d8b9563", 1633666269),
        ("42384f863c93fdce9d55200647350f1ab8367ec7111b049bdf9d8cab36a7fbef", 1633658077),
        ("5397d28fdf024dd4b2693ca5c3c61a7ecfd386c78c544533b53571aeaeee4189", 1633600733),
        ("8945ba7e062c00aac41dd149f4968db4d025bda5ffadde4ec82e1afcd3aea342", 1633592543),
        ("1e8046eb83b453c07798bc702a3ea2aec50287acd013d853a73ebf2f776e9398", 1633535198),
        ("0b2e59810421d6fe10edc78d87e4684cc0576d37dfdf0e6d6a1d9d7b8033484b", 1633527016),
        ("0b111354b169476bf2bec6e88b41ffe0c55280bc968f142f47d5181e3b3514ec", 1633469662),
        ("49aeeaaf66704e482a520e82fcb3c1a644ec1176204bb4d0bb23b0ab596f58ac", 1633461472),
        ("038c184f620f0747589d56310f1016f637f5a84cbc2d6b2144a1231b1e3f11a6", 1633404126),
        ("708ae52151104a4d57a009730ad1fe3ab95a37bb4299e50816d12dc1704af405", 1633395935),
        ("b0b75d28f03c43c39b55db00b44e63f35496825d48d2ed992fda0ca1ec18c320", 1633338591),
        ("c763a2c8e6c32b75f4ca39e825a9b75199cecf69650ffb004fedaf50a4d08afb", 1633330397),
        ("a9e76c8a7d3fdaa75f488f2dd0083129aa8938bb5aad68c75e77b751d589164a", 1633273053),
        ("7547d0bef579ce1edbdf77ff5e69d80da96b973e97a0b9890244bae3c7b75eb5", 1633264862),
        ("afcdd1ad80512bc0f211975e5f39ffb00865950ed10d3aeb7114088a95a3e219", 1633207517),
        ("1015bef7e8bdd35d66fdd50b319311045280bceb20e083a5225d18b504e94a36", 1633199326),
        ("75e51bf76c730e8c2f98f87a0a5a077f05d450dab153062776303957d7d988c0", 1633141982),
        ("a90eaa879a84cffd2908c1a57385fe7598244f664f1639aecc631d9488d4d389", 1633133791),
        ("b9b5e6ae46c22b8fb6d100ca93c1a9ad05b2bcee91a56bc1a76605a1aadb6146", 1633076446),
        ("df5634af7c90c7fdc07672bf2fb494fd4acc9b12c5d0d162b6f78c0b523aebc6", 1633068262),
        ("c730f582428d1d2b0dbf626b14b3dfbec730b34fe3e18df07d4dbb524e66cffc", 1633010911),
        ("5d60731d010d56133a71d10d01c53204791f99d74ee440daddbc96e4be99c13e", 1633002719),
        ("d313bfb58db919a204998a2b4a154d78a2dbe26e402f48e77cf280d39b8619a1", 1632945376),
        ("8f62ebe5895a4c76fe5047730f7aab3cd93c6e8e7291f5cd73df9e438e455ead", 1632937180),
        ("55ce9dbd454e981d970bff8212ab5ebaef8115c5d3302f9b81bb4e6e81655bcd", 1632879836),
        ("eeeb3a3ae04bfb4ac51655ab514fe0212524b83724580ff56c1636a8b3c858d2", 1632871644),
        ("d3e03d7088272670fc48d17abd7909e3f942bd6b59c760aec6f607611b9012f7", 1632814305),
        ("1550e34b49d49e4648c35adf874f2da01447db685808341b8b0120cee92c22f9", 1632806109),
        ("f856475872db523431c98c43fe030a6ab43a080013b29c167df6e137e2f2555b", 1632748765),
        ("d60cb56ffc002e839cf9564e8a56507e4991545fa3ddba03f0ed50e2ddb4369b", 1632740574),
        ("9b9bce433c6673e028a83c65ffa5b8aea3d2151e70a423e628dab22a599758e1", 1632683229),
        ("c89bdbd3a1f2d5f4876e41d8e62797a4621e2a5b55c609c69509d051f7181c5f", 1632675038),
        ("a8db674f64e4bca644c71dfba7fff4228c487c119fcdf5c13acbc5f87b6f1a55", 1632617693),
        ("da60f8f48d9646b65fe9eb1a13584b37754fbc5a412659e1dba561833a458d06", 1632609501),
        ("51c5b1f9b6c66b20fbdd539b8659c301575a9f7c70de2523954cd13e0693986b", 1632552158),
        ("3bf0eec71846891421266c74c6aadfd3be5ed7c2e1d6e8a2047e3e5de1c76f77", 1632543967),
        ("b91d9b97f63e0d51afa7027fb08f00176da1245b26871f770eb0ec184718788b", 1632486621),
        ("5ab72d77f17f6c092bdd8614d68dd9b7e413036a3d631d266e26031c36e0cd5a", 1632478431),
        ("fa485592004a87b101bb96388ea7228ddb32d99d4f3c6f301636d2e9168039fe", 1632421084),
        ("e0cb1ca937024d3b2ac63da86de6794c4b5d95707ed601e6a2f844c1dae5d91c", 1632412892),
        ("a7fcae8bc7c3096564b2f2ac1f29310cea27f5bfbae12dd901c28babe63a6de7", 1632355549),
        ("4c4ede4998654c34565947a58cf7e864c5120c59fa034914d7ce9236ffc3edcf", 1632347358),
        ("4d467d49a36d59710e12cb09d6cd93a93fdaa80e17768af8830c52bd4d72b1e6", 1632290012),
        ("920cd1c19e22c219ff0f4a8f51f1d02b35452154595a10ef4381a805f8ee5a53", 1632281822),
        ("6e8802669132e7bb0c6d21a61fb75280eb6df2177b62ecbfc792b0720318651d", 1632224480),
        ("0c22c02f8eba54e67a55fa551029af7e9f6d976bb4eac26e5e32ceac6ae2af6b", 1632216286),
        ("dab58229a09920c295caff2c9561028e92c84def41f0d1ccc239457fcc07189a", 1632158942),
        ("f08b67db8517af674e89bed76057736fc7a41cfc5141b3c52dfe25cd0a16c604", 1632150750),
        ("ebb4dcb7a979d641cd10ceb34f2e4ebae40eeb88f4bba24851b75246f0ff5740", 1632093406),
        ("627b2c12c302dc4a02af11dbcdde8853d27f51d61fa273342dc229963d3c25c3", 1632085218),
        ("36c8c915761750bcdd1d881f51bc2269aba6ce9d1f7761d8dbd347d026cb0904", 1632027869),
        ("0e536860ddccf0fde0de6eec7f6b22fef4918e6412aeb57f81cf115714a6c4a0", 1632019676),
        ("f02195490e33b4817192d7f6e2d4ea736c06e77e51a7f3f63262d31f45d47d4d", 1631962335),
        ("aafb63015b31d528fc9b65cf8e918b618ba329a30638fab4fbad6c7b9fc00715", 1631954141),
        ("20c96f093102b3ecfd58686100f3667f8759427b41f0a989aafb83507a92139e", 1631896798),
        ("7eb0adc374eb5c4e091ece2ca7927005687e531bc782d626f660eeca7fd90cd5", 1631888604),
        ("61c8cb9c86d7c920a9ef7fad7c6db8d7eb9254fda37bc1a90ce72f40e0168aae", 1631831264),
        ("e87f870898da7ef7b506a67331d6b61cd7d185459b510742e836b3b89f8e807c", 1631823070),
        ("79d12f0cbd1e16eaa16d470916f770ad51665aa26e828c011e3fc412cb9362d3", 1631765727),
        ("66cbc0ad0915f2f08acdf0ffe7889d3fc1e704219f4b532e4e7e0603e2b4c12b", 1631757534),
        ("742636d297649f15fb36c2eb9ada62b5ce577fccb16f9d63edef706afb23514c", 1631700189),
        ("f83ecdd0f727039cfc02b3ad379ae007130fa9e50204dbb88eaf202bc298fafa", 1631691997),
        ("c906f7b644ded3887d25d7a92a97704172040d747135d17c89d37d80e932bcd2", 1631634653),
        ("e4b0890be5748c5ed47582cfde5ecae272fd408cc2deb1308de7297c1a92adb4", 1631626460),
        ("bede1e167a69b604951d960b7a82a6745078afe529ef01f27943bddd2e30caec", 1631569116),
        ("b5bd465c3d11aec46ad9ec7ad26078f3417dae608c51a69e82c77e2cbf973e78", 1631560924),
        ("be4b4b924153ae91704501e39c459b95bbddb70837a76806ddf6bc952bd6a4aa", 1631503582),
        ("de564afc9f71d064cffe55504f6b16bcda635f3492e37cbd4fdc1c522b55b0a1", 1631495388),
        ("af44477ee3cce69dcd96793db120fd788fa6ccfa825fbfc894bf1ca7f87f863b", 1631438047),
        ("69b9bbe536d75753025f98a90614cddef5e46640f8b9273926a64b32315f2e89", 1631429853),
        ("cc6c9fbb6dc10b65d0fdd33cf8da9605e89ad242e504d756f987465feef3a79e", 1631372513),
        ("ed4c84fe29460a3492863f6759949fb9efa3ec07c9ee7950b6f5024a01a3dcd0", 1631364319),
        ("89e9bba2777fb9f26f4269af3cfb9de617c2dccdf246bfe9c901d1cf7d3ba3ff", 1631306975),
        ("21caa63b85cecda18cde7d72f91c7b034882b89616d48f9c0996fa510f8b7819", 1631298782),
        ("75c999e1b7723e356b548e47663b0543852adeab9c2338c091e18a23cc5d0376", 1631241438),
        ("f04f6f15c6b3decfc137fb99d8f8024df42308d1ce8ff264d833d3c5c67e98ac", 1631233244),
        ("9e9ba4e060782ee67b7fd1cf404a419d6b2946d19b9b91961abdb30d8de96053", 1631175900),
        ("9494ab33ace6b69f088b9ff926ed54139f71836715d12b7c90b525d3def5792d", 1631167711),
        ("6977d5f363dc01e4286896099b0f2d24b744be56321100080c5d3b0fa47dd2d0", 1631110365),
        ("256fd8223ddfc9ca7aad2c55df157821e62aa74a579bc6990cc8e829907259ef", 1631102172),
        ("e769ff37b4e6dc2feefb66a381f4e9ea51d185861397ab115a2487734354d986", 1631044830),
        ("776eec9c3a8cc80ad9ec12c19a2e31836fb851a58423545bb693406708d289c4", 1631036638),
        ("b114ee6175b609b029fe62186e38fae049d14bd37e7bffd37de34d0645543acc", 1630979294),
        ("c2a4faaba37b8806774699e4d412d4a7361cc6e196a3a8e3d107c2f4a03c7fd7", 1630971101),
        ("5c6d131feaf5601f96ed5337bbc377fdae640c57f36b2e48e074eb1a34b308a9", 1630913762),
        ("429586ab638187c415c1af03e2f39e742d100ce66cbc05b7065bc3e5e5594e44", 1630905565),
        ("60306762267f58f543796ae2aec02a6090d0031347622f31b2c1b7e9358c17a9", 1630848221),
        ("5f13d08053f551050bb388f79ee19ed3ffd5886579ef590bcebefb661b166ed4", 1630840029),
        ("b0f0915b22430211db0ae6b811b891c1e16bfa1acf1b00846217f950dcc6113e", 1630782686),
        ("90ff0a20075c61b90dd902cd38a37018923359aa22a8af1fa70af3080180e465", 1630774492),
        ("3717cb7a5ce392d410d59eb9244ef95adaf5a9f0eff902bb684da6de15e7fe7a", 1630717150),
        ("f9c72d466d13f5654fbe97f2930257ed1a864d4ac932baabf1b996ed7a886b52", 1630708959),
        ("e099538403266f37395fd6fa707a3c72997465cd8f9fe51c430b83e3697a3651", 1630651612),
        ("af929a14b5b9a656c69f2acf70798483662c2aa4da6fa7e2f9d66eadf8b9cb22", 1630643422),
        ("a34a70bae5a742d1e0fa6f6c2dd9e48be279aa6563fb714e3d7863c2b7742dfe", 1630586078),
        ("0fee576a6d265354d19a6b21ba76f463fcac36fb8b5a7ef3175da69dc897789e", 1630577884),
        ("b263c78075a5e4854c8c61ff6d71f86ec5bcdb04c6e96410ba293457a775ce90", 1630520542),
        ("7e1665b9d4fcc15b90aee73a881b45073c4b65500267c9a4976c51219a9b775f", 1630512349),
        ("d221b20cb82e5ce5a70e92922d6124d3f2b7f0741a5454e315053a2cb3eab3a9", 1630455014),
        ("0eef0a1a4bdfe3061eb25adccf40a3440a7a87a03703abef286002a716dd96d0", 1630446816),
        ("4db8b8a824c53ffeefb5d0006e26fbe22dd18692922280f1d3374cfcf24dd873", 1630389470),
        ("8cb2e528247cc238e11a4eae0916d5b25f47db719103d801e1daf9b23dc270d3", 1630381277),
        ("617d447468d4a4fab0e35fd3d698c56024dc5b639343a3330af39b416b5caa41", 1630323932),
        ("4d98da110a2656dd7ee9204b9084525a20645d2f69932d946a2fc4c05ca3e77b", 1630315743),
        ("aa39f99dd2150c844a9c65443038db54af6066ccb990dbf7b3b7acce3dd5f9c8", 1630258398),
        ("7c7da75f230e3f5eabc20192f620e5f61ffa2b0613a2a5931eb7526f327a3522", 1630250204),
        ("0b3ad1d2868ffac8128d4c84f009186d3595942c3440d96d9975ea9779dfae39", 1630192860),
        ("371bff9522a054e5bcee1b95b926554a4d6f9a96cfc596f6e735e9ab1ebdb672", 1630184670),
        ("ebe062450df74cc817bd5905f36dea66942f5ed0125a73300ffc1b7726332e97", 1630127324),
        ("9d5bfb868e1ffa9b77e46de08a9586781cb98a334f6868ff1b76e293c5d3424d", 1630119133),
        ("d018a75c1a22a5d5a7c55e5ce37d43bebaba8febfbb1682f235e73703002b5a1", 1630061790),
        ("0280bb5c8d3b645d544a68aa11ac5bbea4c2fbfd75089a1f136d25d99e00e447", 1630053600),
        ("d1299d4f514eb6de79a81d300a383ee04b6c552973491781b4114f02509bc61e", 1629996263),
        ("256b7e772fc79839b1d5b7033a15fd06c04fe5ef53e858291d5fe6902cc0df84", 1629988061),
        ("4d8887f18291bfb8388b823a8ef6334bb3f37c10c6bac0b44916362e6e3f01ec", 1629930718),
        ("f0aecda3be35e5f8a7d8054c21cfbc19c34abc19d9bd1cea0422d2a3eab405ef", 1629922525),
        ("8011fd945fb1e1fa28c93033fe5c94e22e928d95fa1e6da57f425920fcf3c89e", 1629865182),
        ("23b14c637d36398acd92ac92108235af8557f0fee977627db7e6950ef62375a2", 1629856989),
        ("f192090d26e39b3f13656a8e8fa0edb8206243e280cdb1bc0cd83c57f3d8c4fb", 1629799645),
        ("c9f59bb3c57c56ff149795ba65fd2fae801e525cde38786077d419e5516a1f02", 1629791456),
        ("0f66cf10a28a972c52abed680e4bf9aac9e2462b31b88469d043487a6cdb929c", 1629734108),
        ("3196cc1ec7a20e3a5012bede2552ab0dfb6a0e25ecf950372c4a4e2fe483581e", 1629725917),
        ("05fbbea7a1ec9f0d93986322abda3bc8fcc4b5b93e55b7ebe30539a20d70b12b", 1629668575),
        ("459c801c0c958334fec2b31730dd430f869825e2454da82b2a0e16e38f87f509", 1629660383),
        ("c0c87173f6d2d7ca6f8f3caa9baf953024bf3a8e19b0da0f5bab5bb87ee3cf61", 1629603038),
        ("fe479278ff5016ecf44990ac8f14f36cd15084330c07f7642cb1baab57fbf495", 1629594844),
        ("e29fd7c0fcbb56daea51512dd32deb0fb4465ad946342936f1bc07d9a0a0d47b", 1629537502),
        ("dc0f13a460d0cdc092dab9b06f3ca6de4192a618b65b158f984951a3605c59a2", 1629529310),
        ("701d65e37e31083e7cb1a304427921180b942db65508468a108872c707f41332", 1629471964),
        ("d551c1791e99fecb5f1bdc1b952c1d02263b35360f1d56653ae001d8b4e1948b", 1629463774),
        ("d8a6694d2881ee276a0c37484aea6c9f5d741e6371f90a18fc5dff7c444a4acd", 1629406429),
        ("ae4f78254f08a87ae7e29ca6fa5cc24a5f7f8e9fa7540b141164c78b230fec3d", 1629398238),
        ("1295b49724d09ed126fb9b85e26df6523a43a3a9fc3bbe4ba55f07e9b0213164", 1629340894),
        ("a76c8463fd9a6008a9eb09663edce5b2f5dcfe0e36555f8b9308e39e54c944eb", 1629332703),
        ("8afdf05d5c3efd1fdaea28e4f7c35f7ef2c613b16dbfd407da77d6bed641e070", 1629275357),
        ("2fc99515135873f8295f46497f6b1f81be8ba7249e9d6dc9947c582bb6eb1547", 1629267164),
        ("8885a708e9bc9296eaf90700234e89ad8b6165f4c8d0c22f81be618d3e01f315", 1629209821),
        ("a23469ef187a57e5993917052042b36aac4c78e3f5a3c4810f8223af287acfdb", 1629201632),
        ("8f106d2945cf45b1bb6b06f7caac6815bd795ed3a769ff7aec9828f6317c830e", 1629144287),
        ("3977c3567602f391527c000a710a64a48113cc0a4c93850eb30a065370e473f9", 1629136093),
        ("6926a0a5e81b11b16541bfdfe4bb7686e71250f0f393a31686a1d22da75962d5", 1629078748),
        ("f7156920732048579376e6c70829c65a07724d8a47c6844aa47b8a671f134f89", 1629070557),
        ("f9b52c4d556836b14a2bdd633cdf84c44a78032912b9d4852e9e249c6e5cbcb4", 1629013213),
        ("a618d21c7d69423fa73c6352a36694fcda69fdb6e466436e86089a5ddefe66da", 1629005020),
        ("2a82c58889aa206b56ad9f26632c47568e613da8660c76aa8be37081de374752", 1628947679),
        ("508ba8e6eb2f53782678e3e7ebf57b81e1d804ff666a26196f3a5cba696d169f", 1628939484),
        ("ec33822c64fcb279e00016675e87089161ecef3bef527f707d51042c6fc559a9", 1628882144),
        ("31f2a01ca1ccc12896587f6bbb1b8bace75c2177423132b5ed2de7ef2d035723", 1628873952),
        ("e58b85e1bc912e5d1dd047fd64e637e80b3aa1717bc0c3ef7c6831b8c5683346", 1628816606),
        ("b227c613fdf51b019f2b29f6b86b013e1b2ca758423302de5995000f04b5e743", 1628808413),
        ("a1c8d9263ceffce78e08acaaa0701452456f8f73ba3c0de3dc7b467da6cfde23", 1628751070),
        ("f0ace0c1b2d6fecb131e87a9cc31c9e2b3c9e6f64c110d0e472b0aef296aa6b1", 1628742878),
        ("3370484b20cd8ab9509895051fd1ad211fd0ef95eb1b552d13c49bb09168b814", 1628685532),
        ("f0c8a1ad58c505bd0bbaa6b51aa812e685f6bb665ad983ebd737e9c178980d8b", 1628677344),
        ("ed1ac47ed7e4dda5e2830d4e3962355c10c304ba7ae835d9d63a530a2106e78c", 1628619997),
        ("0809ad891d9de849a87f5338dcdd26b8f0fc691c47ccfe1f47a5ff7e11ce5779", 1628611804),
        ("9cc10d84d3f55c3fe6ec81e1d8da517a0f3649c3e7056fac2d30689a48fc6aea", 1628554461),
        ("492d58ec7dc3897988ee219cb9622bf8dd243e6fdd784cfa6ed0a56da90d68b4", 1628546269),
        ("de696c3c22b65002ea2f9727ce218eb0eb2b286958ccb3e1dc1d289ab87ef134", 1628488925),
        ("f12019dad9c750429ce8551670b9615dc64c9233956997be68ca91b4f6bee8eb", 1628480733),
        ("3216043cd166e27cf01ec465e7a43a629c1355a291d50fcc12486c434164969e", 1628423390),
        ("5290d95d278a57d5d2faa8c350d3f395ca427f5a270da13c4780b78adecbb790", 1628415196),
        ("723665e0b3ed95358fb4b5568b0600649ca90a86779e7b8523a82b8916c5d863", 1628357855),
        ("9daab185697f56d025d309d37ac49ec827a69a5a7b1d00b3a280ffe6bdbce19c", 1628349660),
        ("74a85b7a7b833d5e97f71bbd902f5f362bc9133e9f95f214377e4f24a1afd2f7", 1628292320),
        ("127788a2717c921ac865eca2c2161d7800f6179a58fd81a0367e10da3044f84f", 1628284127),
        ("859ddce030ce09408c4697817755bfcd4729d500a42afafadd9ba3bf5e53fb98", 1628226780),
        ("5721e5105cc4aa70e838233d27c01778f2a8529d7da0f091c4bdd6cfee3bc685", 1628218589),
        ("26107f5435e016e344442c3c334f515782b813fff0e2c02c3b864f63189b53d6", 1628161247),
        ("86bc9288c76084af9ba49b61c894b90c1ebf9bfc5bc32748c964de31e4db54c1", 1628153055),
        ("a774ef095ceba2704c47b80b7760935ba7a1084946ca55e54ec984a8116522c5", 1628095711),
        ("b0d2cd81da399fe25cb1506b5b04c559bd57fd5021b2081c9513fe0db9238efa", 1628087517),
        ("117141e957688fcc914bd1a3b19bdd0c9a58c1e5e7cd9b85fd5558f82e260a88", 1628030172),
        ("71cd71de2e4bdec8ffe21df6f3a85196e97ec635e6f8e5909ce550b7d6226da8", 1628021981),
        ("c1efa103034d519f86aab0a218f6e3271acc24d73e4eed5350d232b4b1aa7bf8", 1627964639),
        ("9aa1a70aa1e6ad1a2d31c8a59186aa55f73cc79ea86ed43f755ac7438d9daa3d", 1627956446),
        ("a8940ef5bef291429702a13106f2e885f35129aa4ae1c8af57b7058bcf802551", 1627899100),
        ("0d1791cc781fd889fbf53c42130ce25235960791ec89ae110311eb827656430a", 1627890910),
        ("6bf64dc313fee41c9d3457e6113ef8b99ebb3080e2bed1d170285256ed2f63b5", 1627833566),
        ("ec41fd7d92025645969443f279ce00c90ba58848eb4ec166aa6fcfe2eef6f4dc", 1627825375),
        ("fef4c4e1dabcb3e77c3616f29b014e3af4dbf17130aae6aedeba6efcdcbf41e5", 1627768029),
        ("da55461e6c0de6a6475c65aa8cd9751962406e793b77b3a0def94ed70fd05e7e", 1627759838),
        ("7cd0cfffb3d17a56259457a5b01594a75ffe16e76bb7798ef381a088c32fdb20", 1627702495),
        ("eefdbc662afaed662ce4f0638f028075f3afb1a40e666804b420e77c88bc2a0c", 1627694302),
        ("52c6fab5acd03e1a8d326d78abb124110977f2b19f01cef83c1750089155adbe", 1627636959),
        ("d9a74e4e96b8a3cae793da8351f64c1ca401692b916774d49ee83e25c8a6adaa", 1627628764),
        ("d93f747463df069ba8d0df9458b67bb5a67121fd7dee82d61819596691bd1f71", 1627571421),
        ("872cb46ed81f971a3d2a1d5a3e39f2863cdc94f50dedc33f4f80422d8564d4e6", 1627563228),
        ("c62be6e3c7d3e783c4a4c500fdfb4eb8d95e344197e22945b74b4679e7382a12", 1627505885),
        ("8582a522e3b79326f41f276b4494267819b899808a9526d576880caeccda2884", 1627497694),
        ("978344ed413b26a5ffbfae461b4d612dbc9f4cd7613f831a8bedd667c768c15b", 1627440349),
        ("34f251f91ffb0975a4bf00c4eaedd727a19a4d76c3633a7c9287a95d52ef8586", 1627432158),
        ("0912bde5d34477ea9e598ebe612b5a1986bda337069d463c018e43d96f14b614", 1627374815),
        ("0e7022d0479c2c4026c9bf670c674bc76702b6a08f63054cca5bff34f0e28004", 1627366623),
        ("a73a3268c041fe36597c6f5da1c64bd7987dc005d7631d60d1b1c4fc67bd1fc9", 1627309282),
        ("5a71f1ce539d68eeaa07c60922beb72ef6dda496d103196eeaac0939d2e0e19b", 1627301085),
        ("f889d302e563d0f23daeeae2089ce3cfb3d33e0aa93b7c1748bd5c28d888bef3", 1627243743),
        ("afa25f71ac01749e9b760de7783189876001216b20195bb927280df8f06d9356", 1627235548),
        ("d5b01706e8ad9a98a2a9523997f7a7db077dbf865b33c406b371c425154cd144", 1627178207),
        ("10c3a56e58362b46d43de3616abb07c923b23b4a01b3935a13e98bd41d7a3911", 1627170013),
        ("ec3f3eb81c58d581a147ddbde776953f0932856828f1c3b28344e91578192d92", 1627112670),
        ("6c0d33220508f00f8d68004ecca4961144537943fe43ce182c0cba072a25a9f5", 1627104478),
        ("7089aa999dea9329aaa83a6b50f27836c7a3ea5a99f164c9b8f34d697aa97117", 1627047134),
        ("1962cade3caf6ca5a11e1d2086ea242a73093a318b51952da4e6fe17182ce0de", 1627038942),
        ("1dce4b671edc281611a8802633e6a4a1a27dc9edde6739cbc09d17af478b4fc4", 1626981600),
        ("124ee0dafc24ffad17c8e2fed22023a1c6b5be05e2b8bc3c4c9d901d095f16a9", 1626973404),
        ("8624df1cc9eaaabe5a43e7793f2b989d5af49c4bd78253a402fa226c6f652871", 1626916060),
        ("72f377d4ed982b6098fa5d6583285d9f5f274c95285fc3b1811f2facd4514e60", 1626907870),
        ("a391b0aadbd63a443a408edf43300a5f546fa67f33cb753d803a666c9115f887", 1626850524),
        ("797493b26f406cfb71c4b89e96236a11fec62ddcfcd517187a328310717ef88a", 1626842334),
        ("5255e18714115cb744cd9fc2f25a4fb26a319b4ae5e5bbddf97b501165b3ae4f", 1626784988),
        ("abb70ea9bc83c433f64ad72a59c6d09ba8af84613c171e8f7c98fcb06eb2888b", 1626776797),
        ("9168efa750f6c323c3a2a474b6f305640027738aa0b1be020b9718fb75037906", 1626719452),
        ("bd80cc91e1ab43e1107dd692dab5e3d7238dca0b7e5a0126a0635a148490797a", 1626711261),
        ("589259f0272c7b0b15c1e5e8df738c598de9db814f25aee06cf36c7680794ef3", 1626653919),
        ("a255e007559c6de814083cf6abcf0754c71943dd33a2a2ed9c950a637cd19f71", 1626645731),
        ("3f48ddebf658838414f2e2295967015fafaaa9dab44b4613886bdc44e600ac5c", 1626588382),
        ("215c75bae3ea61abe3a55a382f4c6ccb2a62ef751c0c368d5dd13bc15873c2a2", 1626580189),
        ("acecb4ac1082c1c1ac1f729af629719ccc5097aed52c816198aedc23758612d3", 1626522844),
        ("b48a81c2ee767317be1a02154392690d03f6dd666131c9d4717192f58b19d5a2", 1626514654),
        ("5f88de3a683b463d4a6897e987f085a437dfacbb37124d14e44e92ca180da0b0", 1626457309),
        ("7ba33f8cacac8393dd6eaae341082e26b7fc099bd13baa92871695e87a00d880", 1626449117),
        ("faffa52b28ef3c648a8d86ba73ee295a16fbae9306602856bbccd8231d46a316", 1626391772),
        ("c0ee4913b9c5d821a33ec129a1c39ef75b7d8fda244fedb86a637acabc96f523", 1626383583),
        ("ad33377d8b5c763dd3c47346197bfd21b166417602c3b3398db4b5813f5e2713", 1626326236),
        ("d4b14d852063196c433688e3b4d3830fb45ed79ebc311aa07af0ae36dc1172dd", 1626318046),
        ("3824dccf6e392accb232eb1631ec9a166d534d0267eac64a58e17a6b69483e8c", 1626260702),
        ("4bf40ae82f7c40035339b71f22e07fd52d5cc4d911801f99901d934400c0a156", 1626252514),
        ("e2ba601811b7f43ebb7b417495b6afc6dcca216d00e5b14e8517893ae6bc3ca7", 1626195164),
        ("a2f1f5891c4943cc595a63fe129239ae83da74fb725782b1e0f133d25c6b7091", 1626186975),
        ("31b19eef98404da3c400955bd03bfe8090297699cab4707f19c7b444bbcf4af3", 1626129630),
        ("3911a9970aa8879f8824e9aeab289ea3f3e7f3b793a1ab684acbd6383030f1e0", 1626121436),
        ("65f6a3eedd73678920cd75c37cefc5a6b9abc317fac58f2a4724c35e0cbb37d0", 1626064095),
        ("7d8afd5bf9e88ef6bc7318708b3a7210b776233d53ce9d73d80ad7e2b0530980", 1626055903),
        ("576c335a60a33930d206cf1f45024a875dfc2e98e83a2260df6be6b2bf492459", 1625998556),
        ("108cfc1960217e15c51366722725dc8143921f9c68150a966bb70b28414142fb", 1625990366),
        ("0533af348fdab334c35179d95aaf9b6ff0027ca35b871c29f885427909c122c3", 1625933023),
        ("f2912ba9e2ce7c2e4e81389d3eb01387f0513b73fa339b4302f550cd4a2172c0", 1625924830),
        ("84004526b25f5fb52cbe1670afe9419bc6ca730118ba8546acfd427d6086c80d", 1625867486),
        ("ebf92de2f10f83beeecd675cb5673515ad8446aaa3e439174b933d62f1f6a32f", 1625859292),
        ("97b53be2604579e89bd0077a5dcfbea857792eb2ff09849d14321fc2c167f29e", 1625801951),
        ("fd55ec1b466c4eccdaa670f1ee20be17eaf4db8a51d305fb4036f4be6986abc6", 1625793767),
        ("0d3c60484993ff9d8444bf355b0c8ec687e3114bed4332550ec1f71d8c1970be", 1625736414),
        ("58ef64aea5718fcaa778372e2c336fbf35e7ecbd4342965da55611505c93a01c", 1625728228),
        ("752af6c8b113eda90acecba3efdb88ff084cf971d5ebf40863751f5e11cd161b", 1625670878),
        ("4a29ae3cc156bdfd626aac2883ad60fa940270a8b92978444e99c4f59c993b6a", 1625662685),
        ("9e4137d354055ce6bb231fffa9d611d511be04412d91e73b4366672bcf9e9a04", 1625605342),
        ("108600123e55cbaacdb0a6d9ac01eb4c20c9b08c79232f1b84f1795b4823543d", 1625597151),
        ("43de4ff7c775706bfaed896b805221a6df5518560dec82f929eaabc2885bd1c6", 1625539804),
        ("e16d55712ead2ee6b9b4551e156c7517107b610ea8ac9089b35a104ecdbf09ef", 1625531616),
        ("ea14cceb589daf59f7fd4149120fcd18a382fb52c6584a787b1ac905a1059d6a", 1625474271),
        ("7edb215a4f5f6d738dcde5e4a1875cd947fcd91052eec59a7d4ed828a6c03051", 1625466078),
        ("99f1b00d9840f59af4eab28a94bfe89dc5f32659b6050a7195175ab0569f4eed", 1625408733),
        ("c389cc5635477169b2bb1dc502fd5a41cc97e946d159094b24e206483a3dae84", 1625400540),
        ("7da9ca577dbeef5d1c6f9e14e365d716d2a2bc3087f0be7539f6943d8f209e32", 1625343198),
        ("fe589ebc2910a797c1a0c4d7e74e3574ca639601fb7ea74358fbe1821fd099e7", 1625335014),
        ("526480872dfaf29d4d00357eb413a1ea455cfb5fe7f9aee9d90c640fe90b8e7b", 1625277662),
        ("b6651c7a09b91d1415356a6198594185f298261696c1dc75062058f14f76c9c0", 1625269469),
        ("911713e3a0c10636233256f2b235fa7b5e66c90c5002c406c219d977f7ae5c6d", 1625212127),
        ("158b355ed0bd5f53019d4ca49ca04e1d9b6a21b6079c442a47b8156c02ac21d1", 1625203933),
        ("bf3d1e9e4b478877c666ae92a4ab3d657639067274d5a486e2b205db99f40c21", 1625146591),
        ("1f1e99aeaa49bb7174af67c589928b373bb536134cb7d35d3e43527289c84791", 1625138399),
        ("c8feb4cd0ac3df87503eac0e1fa4289f45570f40f34e6cbc7752c6b7ce8f1901", 1625081052),
        ("8ab842abbde63f04b565c2ef3d3113647468bedabce82dbb5c7262e8fd2d100a", 1625072863),
        ("4cf026f4381d572ff5a8ae4dc0fe5b7a17c028393616567788c3a19b23f63bd1", 1625015519),
        ("451f55b676b1696bd92d15c2732d72c44cc3e980a537f65b0425485ec48481f4", 1625007327),
        ("5af8b8e7d63ca7bd74ee4edfcddab46da6af1c2000ccd5cf315a29a00dfb7a1c", 1624949982),
        ("bf47e0b50019ba23cbd59ecced7fd9c4f234e60c3244390676bd86ba0427a99e", 1624941788),
        ("e87d0eb90072d68d3f191f8e8b670ee8993beb62251dd8dac8a122e7edd70d57", 1624884446),
        ("4a9ecca64eb2ce2f4400e92b5b8f0edd4ffa520f6e053a936fac5a066f78c800", 1624876261),
        ("40633e625b16c8b68d5f4188b7df917dc8805bf2836b24922a57147a8473e624", 1624818909),
        ("760fa2e8eb7e6adb0174b173dcbc68165da6c40e72d21484128e7d71dcccbc15", 1624810716),
        ("0c575cc0e66b341420904d7a43ce9318b26a1428de5a36d0bc2bcd08bb1a783b", 1624753374),
        ("e8a7d9f37ce94dabe2bbace73bf70942eda61d82332a1d2db2d1df5f4fda865e", 1624745181),
        ("6d4b8813a6e997754f3e568608ada5c50baa3099c0a2b58b80db327d4848d1cf", 1624687838),
        ("3136f800756f727d8cd3ba7ac12a88d95eb1fe7bcd6ed73d6e2531e86e4cd42b", 1624679645),
        ("2ac07f74fe8c586bad3a5a002d3cadb6e4246e09bdb6dbd1e46c60f86b409b0b", 1624622302),
        ("005f0343439bee357bf138d4d824472373b5592dfd74d2f3cb8ec92c51981615", 1624614110),
        ("b8e48d1de1033de3f98a9906e6d79cefecaa4e209e021a4642f132d32dab7591", 1624556764),
        ("86d92214aa51f085fe94144fd8b4c0186e94babbba885ecc508f5e86c9a457ed", 1624548572),
        ("62a37fa592040ad556cac026e01780ea160c8ae0d8bc7487c25385689e9ac7b5", 1624491230),
        ("a04503c91976a4516a59bbaa3d9c79d26ee120c0ab177d7b4e71c7ff435a99a8", 1624483037),
        ("fd62a1daf417bf3b9992677c170905cea07300fb79bf05bd9193a8025d8c31e6", 1624425693),
        ("93b63bf45ec0593ea866f6c1c0956fe8a1de97e865f260bf8698c24928b5693a", 1624417500),
        ("817f98a72d43750b102e6cc640316293d696c90d1def2dd0ee3a7395504a8d5a", 1624360158),
        ("faab0c0fef98691597c0b119464a4ecd169c74c295f26287c669cd56753b672a", 1624351966),
        ("288bb53e78895d76ef5c4c5b6581c8003c90206f32284c15697bfb03b33ad183", 1624294623),
        ("619152789ddd6b088e606ef56b6fda412ff8b1939c25cd9b5ab761f30d969960", 1624286431),
        ("b03708fba52f1512dbae14e93a225012e3af90e8d849c5df8c29ba5f15e1a8ab", 1624229086),
        ("46ec5b6efd0072a37a661e11d8fb8eccba31242d387e0c88e2d75ac2dd800d13", 1624220894),
        ("0fc454b8418c70ba49c786b09341e54b114fd70e7c031c2fb97c7a58cf265cfe", 1624163551),
        ("450021f14ad8cfc3f29015563f607730fb944d41d93471870776177a5d1a3768", 1624155358),
        ("f442d535fd4640866e908ae7349e6d31dec219aa7ad858defe2f11463bbe5067", 1624098012),
        ("5b241c0404e40b935ec4f9377787d50ac7816f7b363d361e92006b80030c2c36", 1624089822),
        ("aa98668bdae8d3a52ade4fe93c01149e4e286b04352cd03950abefb3c49e933c", 1624032478),
        ("d87c4d593319598f99f7ed5922559f7d98277e6c6d523028dec873837b3ff5fd", 1624024286),
        ("0900cfd108e9d3e3640a7b08eec942afa08bf9d28191c225c3ce4d2e07fd49ae", 1623966941),
        ("5d7e95fbbf6aabeaff4a4f0053a9d1ef1e1ee28dbb2bbe5b3d5e2505c1ed166b", 1623958751),
        ("348106fb479ef9f7931df37f69705421a59035df29f03912223f68bf31fe7bf4", 1623901408),
        ("c95a70ef3738bc286e6d9a93485d620ab93151d8e39236b70369bb01e9fab431", 1623893215),
        ("53bbeb91271c9bda1189174a02df81038000bed18db1210af58027fa66122daf", 1623835868),
        ("b7dffc92bf173a7f49399fa8f02cb25bbdd2185298d554522c85cf769ce33810", 1623827678),
        ("fe56506f7a4187e9fd9ea7efbaa61420bc37bcbdaf02cd96c2b7183c7a2823f2", 1623770332),
        ("2bb6ea7c47cf2bbd65b2f266479293900577c762c0a415f4b93909686bce5066", 1623762142),
        ("f4eeb1c5aa453b0a26eac19850785d1e1e422e0ab1d83b250d12760d6a77c13a", 1623704796),
        ("d172d7e986e861afe0d93b3c3febae5d2a4ed6bdcb63b8fae3cbed25f5037fc5", 1623696606),
        ("a26b7a326487ee0c5a36e5762eee481adfcb1335182f92df66b6bdb80cca15cc", 1623639262),
        ("ed97c66becab2a524528295cde0dbb0bfbdea18d4386e2cb03c346579a23ca38", 1623631072),
        ("167a67c6d88219e0724d670b66edeef18f911c0695d25fc1de6b5595661f7d75", 1623573727),
        ("58d4733449c06a00f7c23965b2c2fdcc0705bef5f08b42996cdd3ac18f5cd6b7", 1623565533),
        ("d3d5273042a335cd91d7ca11026d8a80767c07a85dee5d2ecbd1d7730b60a728", 1623508188),
        ("2be86ee7bd1f7dac5a4c0f26a25a5f011793743d74fc42e00a370f23f027a252", 1623499996),
        ("3c5fc0e687d59413a1b036a3a25ceef56510ae360140ab1c66e91a2e00dd331a", 1623442652),
        ("32f2ab0c7c68f3befcca8d0bbb03f7623d7d14fc9c7e6e81a446e5ce99c7e885", 1623434462),
        ("abe70dafb193b8ab747196126d75e1c562b723f3203b18259c13c4a5ac5fbb5c", 1623377117),
        ("b6c29cdf8e4946a53d7b037ec4a4b912f57582b09adfdb0b18e0ababd7621f65", 1623368926),
        ("63bb763a04fef9225441b8d5657476cbef768429885714feb7eefe268493ba83", 1623311580),
        ("caaa2a776c639720fef233fd2f8bbebf379cdcf3acacfabd1ebf8d564ec2b514", 1623303391),
        ("551b8831535393f6fd95be967be1257223c678799800d1c581b3973d46b0d906", 1623246046),
        ("500df89c818114713aa8797d82c76e2bf1e6ea818eba41e2ea4ef4a9b2478b26", 1623237853),
        ("37775c6673389e2bad7aabf881c9ee571d72802817c7cb409cbe10b717b54246", 1623180512),
        ("fb6e4347cf0929711f3d0ddb32f3b833edcd5086b4ffe1722c32e8dffb6602eb", 1623172317),
        ("08eaba66350ef3901073ec8b9e2f50f661817a57ac775837bd2bfbe867e512c5", 1623114974),
        ("e83d11770b6c8995f53be486b2204d764be339b4d090bd0957508e8656c48efa", 1623106781),
        ("f5be021880b86c1c7ec12d6faaae020ad22d2f6f29f9d26cbeacb18732542e41", 1623049438),
        ("b6e287d091a7a10785eae5324b2cfde1b549404aca2135f1b5f84d8895c0f746", 1623041244),
        ("9fad5e28d41ba38b9fe29bc329cddc3c51dc4d8199ca89658007c4807e9db24e", 1622983902),
        ("f003adf574f6d0fd5691824434980c3803013a57ffc4671c5cad71cca793308a", 1622975710),
        ("7af39fbef325b1b2e6d3b9be55db26c407e37fb66612730b5681256fb43b9c22", 1622918364),
        ("fc9bbbb0fbf55db8026da098437527a9539d6fb375fa9c813faeb211c1b65698", 1622910175),
        ("42902f350bda66ea2d3bef0eb3f73de1c4bc1603f1e1844be3fb358867231bb8", 1622852830),
        ("7254cd5f7ebe3745f6b48333572f4518d17860fa6657bf63c11c1d5a30bc1487", 1622844638),
        ("f1003610a7ab990b0c791054a8fd8755f48b1aa653a05fac1601a80c96b6996e", 1622787293),
        ("113d7707c9e892bbf75f3231a7c0c1ecc98c266c24a24da96d1bdde01b62d8a9", 1622779103),
        ("61a50f7a1da471b2e516eeedcd66cc56c1fb3db518f97947bd484aca0b6bf504", 1622721762),
        ("05e8df7039099230ce68adb23b0fd6bb140ecc3f9eb176c4fe306502cb8ea6ea", 1622713565),
        ("a9f5bd1585b014674813fb36d542dc9af8ef5052323ec5ffad92e8cc035c53cc", 1622656222),
        ("eb6bbf8721d8954e323552e6d814d786df9efc18a9f45b3512c46b3c60dd10d7", 1622648028),
        ("49b8a30f1dd29965c13b78808d52048a9b238252a98edd196169eecd0b79a313", 1622590685),
        ("bbd58c8ee06131667184cfd11c694aef5925058468ede484364d395a36ab91b5", 1622582495),
        ("852ab837ffed3e13fea6ac77110983bb566e0e3a28e2c66a2b332706408634e5", 1622525148),
        ("f279b7dab4913c7bec11a4124d3d13e09152ffbdc09acad04c6cd7f8f3d73c1f", 1622516958),
        ("1689fcb7c7ec05726e82af963ef11c626b022dcc8d76996065de46dbb3886d7d", 1622459613),
        ("93739173f38ca21288e905a9c6459facd3b69f8f8d0ffa0a1c602e56fa2df463", 1622451420),
        ("d7100dda182dbdb40dbde155d1112222a44e17b0cab9d52d51b95e67b153a5a5", 1622394078),
        ("12d9cfbc598d8ec65a8ed28d72dd34003324fc26669ea0e1ee9967e0004019de", 1622385887),
        ("c232647de1b684ee762fb3e174171bc5268191e321f7496c51b1d4c3d8d051d1", 1622328540),
        ("3802946a03f4f1be938291b048847c54484c3f56e2badd69bda7355de91b231d", 1622320348),
        ("c104313e59fe07c8258c2a0f69dc870cad35cb96bda88a14cff265432671fb9a", 1622263011),
        ("a3e5352e7efc70cb591bb17175196fc3618f2e99541ed563b02752f1a086e8eb", 1622254815),
        ("de83d894af28c99617c69ad2db935fa05e801b56ded14478f579ed8dc4179480", 1622197469),
        ("357626989d6c581577264c8d83e1ebd6e1ca26926d0c2dfab09e15cc514dc47c", 1622189278),
        ("4a4b16d1f65192d7c9ce2b891f794bd127e72aa4e796b4d7441b42cea988aaff", 1622131934),
        ("85f5da7b6a501c7170a00822004f21c8a9fa10d4489a1c052f5e1cbabe33a4ff", 1622123742),
        ("e69430893396d07faf1e16467ffaf2610456721c427b1ef0c91232cb80ab3b9b", 1622066399),
        ("f8d4dd173d8cccdd88e907aa077c37d8d1f88ebc62ecf6567bb641c5c103d97c", 1622058210),
        ("405d86b3e578ae5d3544ad3051ea686d2a3b701ba4a46b980eea75a03147c451", 1622000862),
        ("e37967de7d4b28ad9886b1cd023218a3cc8f5018f869a63b8c9672500dd8c087", 1621992670),
        ("9b0f0222b59133c308550a22df3e0434c6c509307c1f49cd2f3dbe05a7d1da42", 1621935324),
        ("28b0cfb0f95632f6d22be752ab3be8baabdc1b7a08cc6017c324a158d2353995", 1621927136),
        ("cc8ab3d1d7638bbc07c25548a8d7f04fc898e2f3723d775e0a0b1cd34447157b", 1621869788),
        ("84f9907abd3e740dc9f9d113949749d5d16dc92a7de817aedc7512b741a5416f", 1621861597),
        ("59a789c3fe231778e0b65983bd24573c0593c18529133927c22ec56889f51425", 1621804254),
        ("660b14e2da7de349a50c578fa0e7b233ed06f71d889356f44512902697e1336e", 1621796063),
        ("9bde895d6ead6ebccfe6a9f30b718136b6caf1423343fa954e96a97885742784", 1621738719),
        ("2b511ff588cc57d346510090a16f63a613189963f88db1e650bd84511c7ff2fe", 1621730525),
        ("ea6f62d3682a65e467f8c1d7b4dfa27851dea66ef978befd9ee774eff0ef3eff", 1621673182),
        ("f7d939c20e63e930c8e9f12db1dd43166dcb35bb8a727055534d607cf8a520f3", 1621664989),
        ("a724f9e1e8dd78143af6fdd09ee25c2d7ef17cb55169f7366342512461bbbaca", 1621607646),
        ("96c204e873abae342b167cb6bea6eef3c162972325efe63321ca7e97d2e4d927", 1621599454),
        ("9a68947cb5e8a14ff2f489183423aa7ccddcec5c379df0fbe59869a910ea47be", 1621542108),
        ("e7da94e8f97649401f5f255d9d98f446c04c1e5c47b481ad6541f677a64485bf", 1621533917),
        ("42ef6c978fedcb212cff024907b57ee6fd9611c0dbafd00ab05cd54b3aae81a5", 1621476572),
        ("6d8238cb0c4d4838d3eb497f7deba5bcbec67023a90f82b2ee9f1518f94ae6e4", 1621468381),
        ("64ee220fd5e1ea9011a837d102600dc2f02d4d85dc14b01bc58c3725134d7910", 1621411039),
        ("a78639c93c9a3a4f1841ed91db7a280d36a4c6eb1458cb5d73afc31f3d6bddd6", 1621402847),
        ("790cd2d6531bbccac7f1521a543f8ca0288069f53f6a98d8f659f8e07ac829ac", 1621345503),
        ("aa673da2e73a659bf919e6d6952df8b01e0e22018c5470076d34fb00b300d1a7", 1621337321),
        ("18b54095c832a364cd5c6f74aa3273a08273f2b020a4d1cc8236a11895e9f9b5", 1621279967),
        ("9147c036130a8757ef5a2d892fbf73e8e1992a5485169f9bcef4fd41ad04d065", 1621271773),
        ("af73293a6f7923e4998bbd0579e1646e506033010fa45a9900450cbe9f1afeac", 1621214430),
        ("71e929f86f76b2ca983ccbf079f6542ef60a63f60ebdb504588bb9d5e2c741d7", 1621206236),
        ("5f94a9beb25cfa0bc85c14fc248ca15c6d8d0f8f23efc95b5a4235b71d49e991", 1621148893),
        ("6886ad4d0959dc2f99d8ed55ba34aa13810eb448a63001a7a4c4a78ef0854504", 1621140702),
        ("879466ad332086f2b74049f0c3ac7b978d8d8ff3c7894bf248e9531648f9042f", 1621083359),
        ("3ad5c16054d78f027bb20fa1a0abd987c3bed70ba892a20ae0852a2d7035d8f6", 1621075165),
        ("4060dfd67ce9045076db0f7d8dca2e9418de7ec9cccf82d9c1ccaf6e1fa21ed7", 1621017821),
        ("503da88c92422549738c73b64d76d1ac2514ac0f80775a4f71c1c219312aa3e7", 1621009629),
        ("5aaac8d4c19a12204cb74f678c5eb53801f8419980374aa9d45907977b10c800", 1620952286),
        ("c81abca0e2d5c9037ed2ad0f3b45fe7fc30cfdf978ed333b2d652a25e3401522", 1620944093),
        ("a7df3cb4b496a190e8d0c11e0d4c20c81c80056209ad130018715618264b7103", 1620886750),
        ("dc014afcc454499149874e923128a266c218634d633074839f1cc28de1a43958", 1620878556),
        ("ebe59a5f7264a9a61152b7acb38299db09d1d953d30f436c0ac87fcacae156e3", 1620821216),
        ("664d9f60db6cdba0236c71c6f9704939a3cd6afd8b84012cf9703f7f27ec813a", 1620813021),
        ("c141a94a2f302cbb6f1dd546a8bdb54c330b2dff74b335fa99eee33783cd87cf", 1620755677),
        ("a25f60da31848b2b1daabef5da56797add4849349bac4a10cdec937f38906f4c", 1620747486),
        ("cd1e6014fdeb71098bfde51644e87b4c10990221fd9aa83f48e3717e84ed7005", 1620690144),
        ("6f34f9d24c8050d950675363e267e6e76c92754bf584e7d172c4040111b93e21", 1620681952),
        ("343d56200620382d461e791cfc06d4599d9787f03b50b2345784cb414812a6cd", 1620624606),
        ("9f9658511fcf7d71425a5b112a751635c614cab29558af89f68bfc325e71dda8", 1620616415),
        ("b01172e30349930faca571514c122f450a4c3473c0ce703f9220d62b877dfaa7", 1620559069),
        ("72d7d07f259a11c95294b49b980df644381f235bf548dfff34b0bb2153a3943f", 1620550878),
        ("abbfeaa15826f5dd3364ebc1cd3feaedc975d1d29fac2c0e4a2c80fcddf1b21d", 1620493535),
        ("fea315c0b1c876b30bce011d4439d840a27d6dc8f321af4d897633bd2f54c317", 1620485342),
        ("c83960b8fd2cdb1d4f2d122356a7d3bf9b1c21274d03615953a8566b1f6e6a85", 1620427998),
        ("cdb211c123c2255308ecf1b01da29db9e703921eb97c6272de13c5ccb9a9c735", 1620419808),
        ("aa5410dd0196b360a12303ac966d16e1c927f9f116f55936b7452ff785e625b1", 1620362463),
        ("a736699c636143556320fe590692f7bfa3fb6430148659cf96bffb342ddc3275", 1620354271),
        ("5e9864b8fd2537a53143f274af7190611f35c79d9eca62e7fb8eb43638ff7431", 1620296926),
        ("19b927207aa83f812445b47e179aee756210cd2aa5b869d088a13fccd4285056", 1620288732),
        ("e081d5ff85245fc42e096efce83ceb80fba913788b38ffbb42beec6c5d881a2d", 1620231389),
        ("32bb43db5d0e38385e126a9a47bce33796ceb47bf5733b5e49b4c956d3790549", 1620223198),
        ("937b99c78b36054b8e45c27032dfcbfdfa62c441e583819b0d87a51079c02c92", 1620165853),
        ("70641c21f9248776d72bf618a5a08618ba8c1df92c75a31ee676b190d3194ecb", 1620157663),
        ("829a61f5e024705eae791aababe42c819a7fd33cf3511441920a66314da92ec0", 1620100318),
        ("69d05477284945d5cae0007d44dbbf48cca0f5916478d65c2a429364454c5982", 1620092126),
        ("423e6cd5d39219c80493cab8209e8cd810847da73f9de982d4a033a3a8e13f7d", 1620034783),
        ("9f5945016c6e22ca5800e3d890dab7e900cec4fa98eabf0e9adbb665b1deb58b", 1620026590),
        ("6365178f085ac234ab43ed61e0bbe5ce434928e708f3df3bd5b8aebfa1c36d91", 1619969246),
        ("e1c78731262dd1df4e4ea5911c35d6f7347b3ca7abeb967da70eb1d6567a6256", 1619961052),
        ("d81c6dadff78e7b3f6ece672eca4fb5f814446f8a5be9516d582b59147681cc2", 1619903710),
        ("f7cd1cbddafcd4e35c1c8a6580a0edde09bf27fcf9e69919e26a488fc638ca0a", 1619895518),
        ("508190770cbcd202449e73e0edac578e3778bc5057cb03901053741ad1a36433", 1619838172),
        ("8d70de61e764618106c2fc652b5021b67f11e31adbfe5dd3d3fb8ffa921cbc9a", 1619829981),
        ("6b86df6a4f9e0811e19f6e1b4d1cb929c806087095a55fd401f30d62c1e37aa4", 1619772638),
        ("ecbe652cc84bb8690d3a9244270cfdbb799fdcca0d96f8918b72419186664f56", 1619764446),
        ("6549d1de5656340e3427223212639245681c6b482bc668b55b7ca701f43e3799", 1619707102),
        ("c2e076c675ec52fd40b9153c495b0bf38836e54cd1e4194fec07b47954098d5a", 1619698910),
        ("7e5a9923c44aefbb45be64f392ce936b8ac776a0ac292ae85856aa66dbb792eb", 1619641567),
        ("bbd83d288e276eacfb782aea22600f4d3fefb27dd41f9509654809a38e36539a", 1619633375),
        ("cfbcab1460da18780a0564fa0ffb92d8d3e8e1bdf77f4719a208e975a85652ce", 1619576029),
        ("fd2c6d06eb60005900d19dbae6a9eb77bfe478f52d26bcb50f00577d1f1c84cd", 1619567839),
        ("4a4dfaf4353f7e33e4806a2a704536ccdc7b3a9b671556b1762d3a4be228399a", 1619510494),
        ("cac763ad6c14d47b587ec6f1f57dfbe34e8c4f493ad5a0ebcc05e09988006d2a", 1619502303),
        ("d87f5f8df70a404bf71a3303e76738fb9aad9fa5146059b8a587cc4d35294b68", 1619444958),
        ("d3fe13efaa9c6554c0ab637a6e91e9be5cfca4c2d68cf48dab381ee524263aa9", 1619436764),
        ("ec3e341db6ad4de2a556b32d32846b644f4120b018f694bfc55f7a40895883eb", 1619379420),
        ("6170c78a00e4b21d71b7b04cc9cd6c8b18dea5da76977cf9bbd2eff1610472bb", 1619371230),
        ("892daf7b04ab1fdcbfbb89b633f80896367d78d36a1347161d4fc3c2a48fd048", 1619313887),
        ("34a112a47457c8d1419a210d37e6c35d26977f014d123f4075594f3c487016a1", 1619305693),
        ("5c6988c862fc862e62cf3eedf47ba07dc446370f973f2d84525c7b5a251615f5", 1619248350),
        ("c1eae272f1225f69dbbb5c72daaec7f6a506bae8d8e97ba1618f7a4ac25eb795", 1619240158),
        ("ed4da827b55a271d0a86de70886fd1cecb3aeb7f3c76eb08f3a39b6bc0951a12", 1619182815),
        ("74a83e06d07363e8e01703a08bcfd357237aad9496e9e1e417ec0a5b0d5714ab", 1619174620),
        ("6cb2e6769a284632b28494d346f79bc493210dbf2f0e2a9900d01eae5c37d5ff", 1619117278),
        ("c456d25ab50e567d26a84d1ef0f5f22b25c4a0dfb5840441e41cba7a0ecfcf62", 1619109087),
        ("36b1ca8e6cc22f5b926c5dec70b41d2dd47e79e029dc909305e2f64362f6701a", 1619051740),
        ("921f883be9133d648f57a314100cf49e51cdf06644e2c395c6f59a183b8a1086", 1619043550),
        ("d72315ac860a7be3e5fd5898ecba5a834465f1cc281197b05df4abedc9b29263", 1618986204),
        ("434b87e7ff73d2b3887f3795d1ea54510c8339c388d00a138a072bdccfa0cb30", 1618978015),
        ("e009a0fdce5b0ca4449e580ba8b5b5e69eb3bb2d735ef64a414ae0a400421c17", 1618920668),
        ("0b709995ab97a5f8adfc709606b88fafcfc19e87b3f8b8d4cd7ff1834d37c761", 1618912477),
        ("376545597a9d47986d78cf458ffb7e182af77166ad62306a27ec5229b33bf4b6", 1618855135),
        ("bea9cbc3a2cabcb3b1b695707ef232d6a383d7b1055aeebc046bd6b0bdadfed2", 1618846940),
        ("a41dccad68f52c15b175935edb3580e82e1c0ffac82151c36d18de95fa3b652b", 1618789599),
        ("41004a57550f822dff3cd11f4f3643f48fd7a57c1de541fa25d5d9a3c0582204", 1618781406),
        ("b9647d4d61855c84c0eb654321a72758f54fcd1b2a691788ad3320c220300a42", 1618724060),
        ("782c695e345080064cc06c30505fdb80f0ff05bd1ac2dd1f1a3fc1d9058788a7", 1618715868),
        ("68ff18049f50b58b4607b6fb987fe78eec6f94ef18edbb9e967d60e2e7dcfb5a", 1618658524),
        ("caf0cc72f0f370dc4a35319b38cd4ee2f4446c80a7d65a611cf6e0ebd96753f1", 1618650332),
        ("8e992d5a95c24c494934a05c48788260559a7a2ec1a426a53b9ce938908c3584", 1618592988),
        ("95c58f6a689ecadf8ac8dfd6e100e1840e879cc33c0ffd4f1c76d0fbdaf54096", 1618584797),
        ("c569963965740024a3c8a8ce7f7b7d3b8b16e1d70975e7756dc48ca20bea3f7c", 1618527454),
        ("59026ecb7928a46fe977d6752a63cd4fed87b1f4fee34ce0760b273e030668b7", 1618519262),
        ("c1c9998a34e961b6d0e382aff2fb926cc77b17a30b134fc839b41f49005998b0", 1618461917),
        ("9b1c249010365712c96ced55b3652068967c283ff667b6a184c3837185df845a", 1618453726),
        ("785d83f042e687e35dcf0eac4f239a14ed88edcb4fddaf0a68300691fae3d05a", 1618396380),
        ("5bac23676b5102ccfdf2804ac8e59f84fbb3699fd27008f0f8ffe91b9106de28", 1618388191),
        ("209948efc4ef7088bd52c2a27070ca2fdf2c04eef6d74cc7f5444396257587df", 1618330845),
        ("ea258ae994e60183200773b674252d533468bd5fdfb65168038bd5a6d50e9d51", 1618322653),
        ("b3a00c814c9085d2250247ddbb4f09755a03152a80f0b58523b8e6848fd760cd", 1618265310),
        ("d2820781e887941a6b873e10ec2d71851ca5b5cd91d229a611ac94552288377d", 1618257116),
        ("1efe656517818a224ba93771a038068ce79992da297754bf75b75eff4fe4843b", 1618199773),
        ("26590cb493a65df33a95a0d91a4eebd11a5bd2964e2da7c2141dee9afc399f94", 1618191580),
        ("9d4c36856c4c9d21a725d0771bd5df1eadcde74fd2bef94c77cb4e2343564d85", 1618134236),
        ("c40bde76430dda49e235b3e1fb07b3dbb903c7bc0082e033cd3fdd2153639247", 1618126047),
        ("8d4d17f47a06c1e42fcc444e5fb0408538b0f7941709da41e371376acb0818fd", 1618068702),
        ("9ded4af69eed311d788f4fa74315183e290fb16db624b56ad4a61c53baee83d7", 1618060513),
        ("372d7e94447fae455cb998ca9e4552643c182c8a66ec58735a6eca9dd52c9027", 1618003168),
        ("8b3fd394c03f915c90e8ca319e09ca98ff1be930691555503099e17acdd5eba3", 1617994974),
        ("1562a3164639bb71ca23e2a2cf30044fb17e2e035e0318fa72eb4b3804029e23", 1617937631),
        ("15dffc42678351596c87a1a0d2a43a5e670e4bfd357c835eb4d9093590a41658", 1617929439),
        ("4bd7eff1468f3c436a55478681c54efd4982c24e995a02fe5b386f6556146ca0", 1617872093),
        ("2f0a2eb051e4f52cf56a890fc64471acf9ccfacef8c7fe1f047180490ca4c346", 1617863900),
        ("e71aa054d83a7a15588e2cd6f3b99efb23347bc19468b5afdb9889d4d0942592", 1617806557),
        ("ccef3d4f00a0bcd28c7e134ae672cd99601f0e6cbe4084a3c699bd67b703fa51", 1617798366),
        ("a8c6de9d8d760c902fe63ee097744267664fcec6f04133ff8a654a445a7d976f", 1617741021),
        ("2161ceeb439243463bb7cea98f76ad8e967d145ecc94eb841160e62ce2950bf0", 1617732829),
        ("2a09c70718afe573356b3959ee83362c7fb4767fe5518f4979344218abad36c6", 1617675484),
        ("79da4e22f99204a8f7a5916c0ea843543ecb4d79c3e816b46a2cac7780bbf8f0", 1617667292),
        ("1f2dd815d1024ecf6b8252e094e8a6fc0539db2254c757122f47df36b3119bf0", 1617609948),
        ("647db0564283cd0895c0dfae6b0de27940883422fcb987261f7e037a7db9cf85", 1617601762),
        ("3cf62e77325adbf3354a3fc7fa4b9ebde1d333257ef1cb0f46233997f5735d86", 1617544412),
        ("4157c8602579ceef13b62eb19ee46776ae3219f9f6b69361d444ecb63a09dda5", 1617536220),
        ("72abe95b5ac772efb0553f6fac11f4deff07743d6f75a2e0f7285526c17faa56", 1617478878),
        ("358816c2bc6dbbb10ffda336b73c0acd3f16c16419e118057e702ca3fb9b0018", 1617470687),
        ("514978f6c0ea269af1aaa8737a2cab9b29f70afac9f6f4266ad3607d9fde1fe8", 1617413342),
        ("def31d0ff7cb6257ab8c734452447527371a413bf49f50cbf93cc0b035647300", 1617405152),
        ("3857589ef4909b24c81a45b6475614b61b9b6bd5b37aac5fe46611a7a599204e", 1617347806),
        ("7a19268bdf1d7371a466af1c4bd3e26a36c6cbe97aeb9e488fea23d3dbbdd435", 1617339615),
        ("535858c1d4c540057e2e38a63ebcc6e1d236d2594e2733cbfd032975ba821fd6", 1617282270),
        ("ee37187feb1aae4fa7ae05c7a68565edd6c605ed3ea3d7fcd62467e1931bf96c", 1617274078),
        ("371d94a5cb29548d19340079d0aeaf2b0725a39741975813d074f088a311d72f", 1617216732),
        ("28a40fbe00122ba1dbff380f6e1866de963d46e7a7ef55cad5552268ddcf2a7c", 1617208540),
        ("003b00e8171b6f859120c4eae09e2208d1d54160c52801456178ca1b5d39ff4c", 1617151200),
        ("8d46bb241c53d8f5a918a96e995a93fe96b3fa23dfc3bcc856997f761e66872b", 1617143014),
        ("7fae7ca2fa96488c43d15b719dc54754feac0d4beae06093f590805f76680752", 1617085661),
        ("fd5fded6557ffec68ff3df437e1b76202bae66eadecd6a286a21046f4d80e9c2", 1617077469),
        ("cc5a23ce6f1d109d536f77ff2ad19d63a0d4699fa061f41abd3163145ef0a4d5", 1617020127),
        ("c3fc0c911d483fcec832c33ec0cb2926dc6b4440e63e2adc8917eaa733175225", 1617011933),
        ("8bcab7e39d60438f74f57a847259ce9cfa8a1582aad0baa079ab4ec1ab9c2af8", 1616954591),
        ("7e66fc3d075669f4b8400bd1546bb872ee7ade965882b4f856b93404d14f3f17", 1616946397),
        ("dc35f75688c072e7dadb861a800ab8b30bf178fcf6698236e2789d9c88c120f0", 1616889055),
        ("4f2f109e5f61da966f4d3100d67055dd5828e138dc990d1f7a9438f1a6f6e470", 1616880861),
        ("abec16c9680b07b3e6214e893d2768c70250edecd5b08eb3cffb84de443db02c", 1616823519),
        ("0425da2acb93bb04ee614759dec171c594f9d30db3789d036677b3ccc5bb6c23", 1616815327),
        ("31181a1377a20a22b121238633bcf8f94f5edc2ab33aa0d58adc6fb3d62f1436", 1616757980),
        ("0377b38cc705767d5309e75c4cc0f68f45828530f9330f100664d01b10e8b100", 1616749789),
        ("da771ffa54c00e8c7783485e5cef0b8a81b72565a3f1c9da8326a8c2411eb51e", 1616692444),
        ("946e862306051b4915ba46042086173cce4067b15a9c0ae7fac0b887565bd3fe", 1616684260),
        ("7075557a2f13824e2c2c4de8f7120e2cebb6cb6fb8b398c613a4aa22d39a1d8c", 1616626911),
        ("173651bebdba8ecf1faf97fb9ecd17fb22ed023d77e3e1ec9f9bc79dfc8b922b", 1616618716),
        ("6954055f9ac9d89d692bf991fb0855df80f1810639101f070e2a24b014491bf3", 1616561372),
        ("6ed18b96b23b4792a4cc9e8fa0635b95dd3e51b114855f99bf57000a071cf0d5", 1616553181),
        ("23e39efd01fe5c4ac7b9dcb23d148a9e320669925ed88b90aa5d0d38fc8e53dd", 1616495838),
        ("55d765ec9fea749beec60f825b4836078e05a904553fb6c8e05b13f13484eee0", 1616487645),
        ("05e737a3c8bdc879b51d2520d01bcb63ebbd4cb4a0f90d132475aa5a7faf74d3", 1616430302),
        ("c35ca7297b89b1fc91152eef83149c1500a55dc13b993260304dd32ee4d31259", 1616422111),
        ("b245a39568c9557b542b92b078df8d13e9f907851b8536849352688aed2dd192", 1616364765),
        ("52c02710158c27c02292035e730e90f1f0bae4463fa6bff1d900b342691ceba4", 1616356573),
        ("8b297032bcbc875382f1d1b26a66a80a1dcf00d39989aa43335b453020a095f3", 1616299230),
        ("e6fd801dcc64d60cf4e490210ab235b33be67e8987c8e2c3f5d8d9eaaa6ad12c", 1616291038),
        ("c633eb8359728ca8b5bad4c3422f7a5f3f4e4f02b5be54745e6d4e189d7659a1", 1616233692),
        ("76b9d593d3d551d42c41984020078640fbe36764793ba3e41c5a9890d67396e2", 1616225502),
        ("7eadf59e4d0d335307ede3fa853911a2d53b7caac2e13bd120d38e4698029dfd", 1616168158),
        ("98611e0509d834ddf53568c37088b1d77093e11903ef472a62f92680b69b6a4f", 1616159966),
        ("b13a7079bdd60ea4cf7b6d21223a7be192263fad8191a28a69a17b065f2ed8f3", 1616102623),
        ("464289ecf767444daa4bd232e95f6f9e3709f858af90438a9bf78cabc21aca96", 1616094430),
        ("37e612eaf9c26877830f374b344a9325ce9bef812508478e82824637d4b5380d", 1616037084),
        ("df4e67c1a706d06f824ea7e88987b6a6ffff27bb4fd39faa5f4a5e81165ca242", 1616028893),
        ("3c6f659a0e78ead0f9a98d09ad0f3dccb3e3aa1493440111eba83cd775a32795", 1615971552),
        ("880e1b1cc2f8b2c3cb534bb0c95a19749b6735618ec7660692bb4a2940ab9ecf", 1615963357),
        ("dcce69ee51a27ccd7ef76c2ed500a006ee8938ef4889b1b96abfc82d171e06a4", 1615906014),
        ("4632239d1cac863f5ce35d1df8efd16090594f2ddb778bdd10b3ca0233f8c953", 1615897820),
        ("be3fcf26f1fae694eb19296f9ae8e6f1624b39d517ba92dfc0051efb9d2f09b8", 1615840482),
        ("2023f6cf61f472867f6b6b1c681a8c110e8d633b1b1a354e8f72068a6ba34d3a", 1615832284),
        ("4bd2a7a7cc22262f15f566fb0e29e1747e9861b05f6b49690eb0c55976876e54", 1615774943),
        ("06bb51b46052584423bc185ce4fdf054e8b986b53de96c34877e4d100451c5fa", 1615766750),
        ("e04b1cb10016d559a5bc75f4dfc6aa2eca6087365fe594e1ed6001f5779e38c9", 1615709408),
        ("74dd0110160e58aa79863abd359ce35df6458d1d1f2f1a8cb2c948922bb48b4d", 1615701213),
        ("634f5a66b3c52dab59fe77eb4a821233cfaa22b46b6857925dce57b4c4135791", 1615643868),
        ("9f11255decb70ebd7832f20ff84376521b93e01b66edb4e82b1eeea013880494", 1615635676),
        ("8a0add9b2c788583a5671c35978c5625c5170d57db464d2abca909a9fe179dee", 1615578334),
        ("b5cdda436101e21f5bddabfee9e0dd792bd92d493a393e4ff4685004c1f74d54", 1615570142),
        ("87374c5ea9f2017637e1d1635ac36b7aebdab22b9d5063938da474fdea1d5574", 1615512796),
        ("6554bd6196dfd35d54ba638fb67ee59d210def463c031ccf67c55248532e5ddc", 1615504606),
        ("89166d238f0780dfebb1975943910e544b903b377647dfea06bcc7cba25da0ed", 1615447262),
        ("1d6dce35869057a4c30724a14fb8154972983d244c0e9ec048261c0caec4238f", 1615439070),
        ("3a019353246cc1f13d4c027300e9db1d217ff595f9a4b89461c25192f0210987", 1615381724),
        ("30b8b0187f7ab05c59d3437a375316d859f38aa28c1c9a58c09efea8fb03a5f1", 1615373532),
        ("64886c73db9905f76d5f6f05bfd440dc48c266c1c8df3fa6cc4e1adb6c91322b", 1615316190),
        ("c2655fd772a6097878ab39c1494c0eada9f6b6ff7f9ecfc783e35c1c3e17121f", 1615308000),
        ("40bab5aaeaff66fb71d2ade5c0aecc5a4708b1af940192728ac69e704b3e0000", 1615250654),
        ("7f20bdc5cd05810133a013156cf2f938b056dabbf2673b54060180339bd3249d", 1615242460),
        ("6ddfd5a624f3120c9145c749945e9775c3d82dacb4357551bae3b7653d4c7cfb", 1615185119),
        ("3214d34575b15d0dad6bb853671b233d5d817f32abe5d1173a88836e9c2b0344", 1615176926),
        ("7a3fbe40b3de411414d8e3139314aeee3c3405dc085c8c65a877d2830469d6df", 1615119580),
        ("a873b8b6f77e1df25729590b5100cabb6f923ed829b66525413c2829f2a47e03", 1615111388),
        ("1f53fd1cfac438bfcae90c2f336e02e0f2d4fae280d131955501b5fa85201851", 1615054046),
        ("e30797c03e4d61902156ad2a5428b40884f0bcd223e7a66276f19df45a81a2ab", 1615045854),
        ("573f49cf026b6542087084024544239f347f4b16d584f2a1cf38e5bd7dc8f910", 1614988514),
        ("9cf866ff8a25f4335d8371674de902ff13ffead1915b8f4ddf5d2d0cd695862c", 1614980318),
        ("d816614ee794641722c9a101f990cc56462387eca40b204efeb8d90efe236379", 1614922974),
        ("0601ed80b96ffc35215185a70d898e2ef7ad884230cc988bcbb258551183b183", 1614914781),
        ("29353cacf5660d5c2faeca268ab8be0fe1909f0ddc6bb4acbd21dce8e337df04", 1614857439),
        ("26c57a4463c548cf3ffd7d9a6f4375c0bbb2037c37ede6b7faac1acc18281baa", 1614849244),
        ("f09548792ae8f99545e9e53695c5d97fdcf89afc18d2f200f5b5a855d369704c", 1614791901),
        ("58c2e163c2da25e55612d9f8ecc6097fb66ee481b0d4ade8137eac0df6f68ebd", 1614783709),
        ("37432c6876c30e80f8c5c9acf38b1247af3cd49339d6235eeb85f9fbb437127d", 1614726367),
        ("43e46081628f9e4637951bba2d132c98af5a06ce08e56d0ab98b48927a59df8b", 1614718174),
        ("3574194032380c6575a27b8590aec967e99882e214043aba3289cec65f587182", 1614660828),
        ("2b45156ac0b7eb075596f83e4bf5c52c0c312d28d44f71ee121b78a0802f8088", 1614652638),
        ("e8078cd64ee2cf50603bb6943226f76951c918f9ef5192e65eeeb750391d750f", 1614595294),
        ("39cd5e3cff63de889d8c0186562cf1e7481c81ecaaef90796b0835e52813c9b6", 1614587102),
        ("7a51ad1418dcfc81c9e0810de3405c1198371edb8e8117c00fce0b3d012580dc", 1614529761),
        ("8193d4a699c2098461ef5d68b5c6c70491f515323c4d53c7bfd4a41fe196aacf", 1614521564),
        ("96bac65b0fe979cee57a8fda8c4e2a3718d6a0bf5bd69ade24e4c1c164f545ad", 1614464221),
        ("6fd2328d3580156293d52259cd3b95092909be40fc745090d93c60747ba044d5", 1614456029),
        ("531ad8bf7f7bb4c2e3329ae81478b55fa39f6041eae3a82a0883236524e68269", 1614398688),
    ];

    let mut prev_time = key_blocks[0].1;
    for (id, time) in &key_blocks[1..] {
        if engine.is_persistent_state(*time, prev_time, crate::boot::PSS_PERIOD_BITS) {
            let id = BlockIdExt {
                root_hash: id.parse()?,
                ..Default::default()
            };
            let mut meta = BlockMeta::default();
            meta.gen_utime = *time;
            let handle = db.block_handle_storage.create_handle(id.clone(), meta, None)?.unwrap();
            db.store_shard_state_persistent_raw(
                &handle, 
                &vec![*time as u8; 1024], 
                None,
            ).await?;
        }
        prev_time = *time;
    }

    let calc_ttl = |t| {
        let ttl = engine.persistent_state_ttl(t, crate::boot::PSS_PERIOD_BITS);
        let expired = ttl <= 1638277771; // Tue Nov 30 2021 13:09:31
        (ttl, expired)
    };
    db.shard_state_persistent_gc(calc_ttl, &BlockIdExt::default()).await?;

    let calc_ttl = |t| {
        let ttl = engine.persistent_state_ttl(t, crate::boot::PSS_PERIOD_BITS);
        let expired = ttl <= 1643645379; // Mon Jan 31 2022 16:09:39
        (ttl, expired)
    };
    db.shard_state_persistent_gc(calc_ttl, &BlockIdExt::default()).await?;
    stop_db(&db).await;
    Ok(())
}

