use super::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering}
    },
    time::Duration,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_mpmc_channel() -> Result<()> {
    let mpmc = Arc::new(MpmcChannel::new());
    let n = Arc::new(AtomicU64::new(0));
    let mut s_tasks = vec!();
    let mut r_tasks = vec!();

    for i in 0..3 {
        let mpmc = mpmc.clone();
        let n = n.clone();
        s_tasks.push(tokio::spawn(async move {
            for _ in 0..100_000 {
                let m = rand::random::<u64>() % 10_000;
                n.fetch_add(m, Ordering::Relaxed);
                mpmc.send(m).unwrap();
            }
            println!("sent #{}", i);
        }));
    }

    for i in 0..8 {
        let mpmc = mpmc.clone();
        let n = n.clone();
        r_tasks.push(tokio::spawn(async move {
            while let Ok(m) = mpmc.receive().await {
                n.fetch_sub(m, Ordering::Relaxed);
            }
            println!("received #{}", i);
        }));
    }

    futures::future::join_all(s_tasks).await;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    
    mpmc.stop();
    futures::future::join_all(r_tasks).await;

    assert_eq!(0, n.load(Ordering::Relaxed));

    Ok(())
}