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

use ton_types::Result;
use adnl::common::{add_unbound_object_to_map_with_update, CountedObject};
use std::{
    time::{SystemTime, Duration, UNIX_EPOCH},
    sync::{Arc, atomic::{AtomicU64, Ordering}},
    hash::Hash,
    marker::Sync,
    fmt::Display,
};

pub struct TimeBasedCache<K, V: CountedObject> {
    map: Arc<lockfree::map::Map<K, (V, AtomicU64)>>,
}

impl<K, V> TimeBasedCache<K, V> where 
    K: 'static + Hash + Ord + Sync + Send + Display,
    V: 'static + Clone + Sync + Send + CountedObject 
{
    pub fn new(ttl_sec: u64, name: String) -> Self {
        let map = Arc::new(lockfree::map::Map::new());
        Self::gc(map.clone(), ttl_sec, name);
        Self{map}
    }

    pub fn get(&self, id: &K) -> Option<V> {
        let guard = self.map.get(id)?;
        let now = Self::now();
        guard.val().1.store(now, Ordering::Relaxed);
        Some(guard.val().0.clone())
    }

    pub fn set(&self, key: K, factory: impl Fn(Option<&V>) -> Option<V>) -> Result<bool> {
        add_unbound_object_to_map_with_update(
            &self.map, 
            key, 
            |prev| {
                let now = Self::now();
                if let Some((v, t)) = prev {
                    if let Some(new) = factory(Some(v)) {
                        Ok(Some((new, AtomicU64::new(now))))
                    } else {
                        t.store(now, Ordering::Relaxed);
                        Ok(None)
                    }
                } else {
                    if let Some(new) = factory(None) {
                        Ok(Some((new, AtomicU64::new(now))))
                    } else {
                        Ok(None)
                    }
                }
            }
        )
    }

    fn gc(map: Arc<lockfree::map::Map<K, (V, AtomicU64)>>, ttl: u64, name: String) {
        tokio::spawn(async move{
            loop {
                futures_timer::Delay::new(Duration::from_millis(ttl * 100)).await;
                let now = Self::now();
                let mut len = 0;
                for guard in map.iter() {
                    let time = guard.val().1.load(Ordering::Relaxed);
                    if now > time && now - time > ttl {
                        map.remove(guard.key());
                    } else {
                        len += 1;
                    }
                }
                log::trace!("{} capacity: {}", name, len);
            }
        });
    }

    fn now() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
    }
}


