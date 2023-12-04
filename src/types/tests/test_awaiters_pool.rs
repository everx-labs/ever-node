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

use super::*;
use crate::collator_test_bundle::create_engine_allocated;
#[cfg(feature = "telemetry")]
use crate::collator_test_bundle::create_engine_telemetry;

use std::{
    time::Duration,
    sync::{Arc, atomic::{AtomicU32, Ordering}}
};
use ton_types::fail;

async fn op(number: Arc<AtomicU32>) -> Result<u32> {
    futures_timer::Delay::new(Duration::from_millis(1000)).await;
    number.fetch_add(1, Ordering::SeqCst);
    Ok(number.load(Ordering::SeqCst))
}

const TASKS: u32 = 50;
const OPS: u32 = 10;

#[tokio::test]
async fn test_awaiters_pool() {
    let pool = Arc::new(
        AwaitersPool::new(
            "",
            #[cfg(feature = "telemetry")]
            create_engine_telemetry(),
            create_engine_allocated()
        )
    );
    let mut ns = vec!();
    let mut sums = vec!();
    let mut handles = vec!();

    for id in 0..OPS {
        let n = Arc::new(AtomicU32::new(id));
        let sum = Arc::new(AtomicU32::new(0));
        ns.push(n.clone());
        sums.push(sum.clone());

        for i in 0..TASKS {
            let n = n.clone();
            let pool = pool.clone();
            let sum = sum.clone();
            handles.push(
                tokio::spawn(async move {
                    futures_timer::Delay::new(Duration::from_millis((i % 5) as u64)).await;

                    if n.load(Ordering::SeqCst) != id + 1 {
                        if i % 2 == 0 {
                            if let Some(nn) = pool.do_or_wait(&id, None, op(n.clone())).await.unwrap() {
                                assert_eq!(nn, id + 1);
                            } else {
                                assert_eq!(n.load(Ordering::SeqCst), id + 1);
                            }
                        } else {
                            if let Some(nn) = pool.wait(&id, None, || Ok(false)).await.unwrap() {
                                assert_eq!(nn, id + 1);
                            } else {
                                assert_eq!(n.load(Ordering::SeqCst), id + 1);
                            }
                        }
                    }
                    sum.fetch_add(1, Ordering::SeqCst);
                })
            );
        }
    }
    futures::future::join_all(handles).await;

    for i in 0..OPS {
        assert_eq!(ns[i as usize].load(Ordering::SeqCst), i + 1);
        assert_eq!(sums[i as usize].load(Ordering::SeqCst), TASKS);
    }
}

async fn op_fail() -> Result<u32> {
    //futures_timer::Delay::new(Duration::from_millis(1)).await;
    fail!("test_awaiters_pool_fail!")
}

#[tokio::test]
async fn test_awaiters_pool_fail() {
    let pool = Arc::new(
        AwaitersPool::new(
            "",
            #[cfg(feature = "telemetry")]
            create_engine_telemetry(),
            create_engine_allocated()
        )
    );
    let mut handles = vec!();

    for _ in 0..10000 {
        let pool = pool.clone();
        handles.push(
            tokio::spawn(async move {
                for id in 1..10 {
                    loop {
                        match pool.do_or_wait(&id, None, op_fail()).await{
                            Ok(None) => println!("Ok(None)"),
                            Ok(Some(_)) => panic!("Ok(Some)"),
                            Err(e) => { 
                                assert_eq!(
                                    e.to_string().get(..24).unwrap(),
                                    "test_awaiters_pool_fail!"
                                );
                                break;
                            }
                        }
                    }
                }
            })
        );
    }

    futures::future::join_all(handles)
        .await
        .into_iter()
        .find(|r| r.is_err())
        .unwrap_or(Ok(()))
        .unwrap();
}

#[tokio::test]
async fn test_awaiters_pool_timeout() {
    let pool = Arc::new(
        AwaitersPool::new(
            "",
            #[cfg(feature = "telemetry")]
            create_engine_telemetry(),
            create_engine_allocated()
        )
    );
    let mut handles = vec!();

    {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let id = 1_u32;
            pool.do_or_wait(
                &id,
                None, 
                async move { 
                    futures_timer::Delay::new(Duration::from_millis(10)).await;
                    Ok(())
                }
            ).await.unwrap();
        }));
    }
    {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let id = 1_u32;
            assert!(
                pool.wait(&id, Some(5), || Ok(false)).await.is_err()
            );
        }));
    }
    {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let id = 1_u32;
            pool.wait(&id, Some(20), || Ok(false)).await.unwrap().unwrap();
        }));
    }

    futures::future::join_all(handles)
        .await
        .into_iter()
        .find(|r| r.is_err())
        .unwrap_or(Ok(()))
        .unwrap();
}

const WAIT_2_TASKS: usize = 100;

#[tokio::test(flavor = "multi_thread")]
async fn test_awaiters_pool_wait_2() {
    let mut handles = Vec::with_capacity(WAIT_2_TASKS * 2);
    let pool = Arc::new(
        AwaitersPool::new(
            "test_awaiters_pool_wait_2",
            #[cfg(feature = "telemetry")]
            create_engine_telemetry(),
            create_engine_allocated()
        )
    );
    let err = Arc::new(AtomicU32::new(0));
    let ok = Arc::new(AtomicU32::new(0));
    let already_done = Arc::new(AtomicU32::new(0));
    
    for id in 0..WAIT_2_TASKS {
        let done = Arc::new(AtomicBool::new(false));
        
        {
            let pool = pool.clone();
            let done = done.clone();
            handles.push(tokio::spawn(async move {
                futures_timer::Delay::new(Duration::from_millis(1)).await;
                pool.do_or_wait(
                    &id,
                    None,
                    async move {
                        futures_timer::Delay::new(Duration::from_millis(1)).await;
                        done.store(true, Ordering::Relaxed);
                        Ok(())
                    }).await.unwrap().unwrap();
            }));

        }

        for _ in 0..WAIT_2_TASKS {
            let err = err.clone();
            let ok = ok.clone();
            let done = done.clone();
            let already_done = already_done.clone();
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                loop {
                    if !done.load(Ordering::Relaxed) {
                        futures_timer::Delay::new(Duration::from_millis(1)).await;
                        match pool.wait(&id, Some(1000), || Ok(done.load(Ordering::Relaxed))).await {
                            Ok(None) => continue,
                            Ok(Some(_)) => break,
                            Err(_) => {
                                err.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                        }
                    } else {
                        already_done.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
                ok.fetch_add(1, Ordering::Relaxed);
            }));
        }
    }

    futures::future::join_all(handles).await;

    let ok = ok.load(Ordering::Relaxed);
    let err = err.load(Ordering::Relaxed);

    print!("ok {}  err {}  already_done {}", ok, err, already_done.load(Ordering::Relaxed));

    assert!(ok + err == (WAIT_2_TASKS * WAIT_2_TASKS) as u32);
    assert!(err == 0);
}