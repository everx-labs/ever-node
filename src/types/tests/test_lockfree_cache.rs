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
use adnl::{declare_counted, common::Counter};

declare_counted!(
    struct Test {
        val: String
    }
);

#[tokio::test(flavor = "multi_thread")]
async fn test_lockfree_cache() {

    let counter = Arc::new(AtomicU64::new(0));
    let cache = TimeBasedCache::new(4, "test_lockfree_cache".to_string()); 

    assert!(cache.get(&1).is_none());
    assert!(cache.get(&2).is_none());
    assert!(cache.get(&3).is_none());

    assert!(cache.set(1, |prev| {
        assert!(prev.is_none());
        let ret = Test {
            val: "1".to_string(),
            counter: counter.clone().into() 
        };
        Some(Arc::new(ret))
    }).unwrap());
    assert!(cache.set(2, |prev| {
        assert!(prev.is_none());
        let ret = Test {
            val: "2".to_string(),
            counter: counter.clone().into() 
        };
        Some(Arc::new(ret))
    }).unwrap());
    assert!(cache.set(3, |prev| {
        assert!(prev.is_none());
        let ret = Test {
            val: "3".to_string(),
            counter: counter.clone().into() 
        };
        Some(Arc::new(ret))
    }).unwrap());

    assert!(cache.get(&1).is_some());
    assert!(cache.get(&2).is_some());
    assert!(cache.get(&3).is_some());

    assert!(!cache.set(2, |prev| {
        assert!(prev.is_some());
        None
    }).unwrap());

    futures_timer::Delay::new(Duration::from_secs(2)).await;

    assert!(cache.get(&1).is_some());

    futures_timer::Delay::new(Duration::from_secs(4)).await;

    assert!(cache.get(&1).is_some());
    assert!(cache.get(&2).is_none());
    assert!(cache.get(&3).is_none());

    futures_timer::Delay::new(Duration::from_secs(6)).await;
    assert!(cache.get(&1).is_none());
}