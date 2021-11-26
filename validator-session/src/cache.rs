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

pub use super::*;
use crate::profiling::ResultStatusCounter;
pub use catchain::InstanceCounter;
use std::collections::HashMap;

/// Entry entry to be used by cache objects
pub type CacheEntry = Rc<dyn std::any::Any>;

/// Cache for session objects
pub trait SessionCache {
    /// Search cache entry in a cache
    fn get_cache_entry_by_hash(&self, hash: HashType, allow_temp: bool) -> Option<&CacheEntry>;

    /// Add new cache entry
    fn add_cache_entry(&mut self, hash: HashType, cache_entry: CacheEntry, pool: SessionPool);

    /// Increment reuse counter
    fn increment_reuse_counter(&mut self, pool: SessionPool);

    /// Clear temp pool
    fn clear_temp_memory(&mut self);
}

/// Trait for all cache objects
pub trait CacheObject<T: PoolObject + HashableObject> {
    /// Compare two objects
    fn compare(&self, value: &T) -> bool;

    /// Create cache entry clone
    fn wrap_cache_entry(value: &PoolPtr<T>) -> CacheEntry
    where
        T: 'static,
    {
        Rc::new(value.clone())
    }

    /// Extract value from cache entry
    fn unwrap_cache_entry(cache_entry: &CacheEntry) -> Option<&PoolPtr<T>>
    where
        T: 'static,
    {
        cache_entry.downcast_ref::<PoolPtr<T>>()
    }

    fn create_object(value: T, pool: SessionPool, cache: &mut dyn SessionCache) -> PoolPtr<T>
    where
        T: CacheObject<T> + PoolObject + 'static,
    {
        //search object in caches

        let hash = value.get_hash();
        let allow_temp = pool == SessionPool::Temp;

        if let Some(ref cache_entry) = &cache.get_cache_entry_by_hash(hash, allow_temp) {
            if let Some(ref ptr) = Self::unwrap_cache_entry(&cache_entry) {
                if ptr.compare(&value) {
                    assert!(
                        pool == (*ptr).get_pool() || (*ptr).get_pool() == SessionPool::Persistent
                    );

                    let result = (*ptr).clone();

                    cache.increment_reuse_counter(result.get_pool());

                    return result;
                }
            }
        }

        //create new object and register it in cache

        let mut object = value;

        object.set_pool(pool);

        let result = PoolPtr::<T>::new(object);
        let cache_entry = Self::wrap_cache_entry(&result);

        cache.add_cache_entry(hash, cache_entry, pool);

        result
    }

    fn create_temp_object(value: T, cache: &mut dyn SessionCache) -> PoolPtr<T>
    where
        T: CacheObject<T> + PoolObject + 'static,
    {
        Self::create_object(value, SessionPool::Temp, cache)
    }

    fn create_persistent_object(value: T, cache: &mut dyn SessionCache) -> PoolPtr<T>
    where
        T: CacheObject<T> + PoolObject + 'static,
    {
        let result = Self::create_object(value, SessionPool::Persistent, cache);

        assert!(result.get_pool() == SessionPool::Persistent);

        result
    }
}

/// Cached instance counter
pub struct CachedInstanceCounter {
    pool: SessionPool,                            //pool object belongs to
    total_instance_counter: InstanceCounter, //instance counter for all objects (temp + persistent)
    temp_instance_counter: InstanceCounter,  //instance counter for temp objects
    persistent_instance_counter: InstanceCounter, //instance counter for persistent objects
}

impl CachedInstanceCounter {
    pub fn new(receiver: &metrics_runtime::Receiver, key: &String) -> Self {
        let body = Self {
            pool: SessionPool::Temp,
            total_instance_counter: InstanceCounter::new(receiver, &format!("{}.total", key)),
            persistent_instance_counter: InstanceCounter::new(
                receiver,
                &format!("{}.persistent", key),
            ),
            temp_instance_counter: InstanceCounter::new(receiver, &format!("{}.temp", key)),
        };

        body
    }

    pub fn clone_as_temp(&self) -> Self {
        let body = Self {
            pool: SessionPool::Temp,
            total_instance_counter: self.total_instance_counter.clone(),
            persistent_instance_counter: self.persistent_instance_counter.clone_refs_only(),
            temp_instance_counter: self.temp_instance_counter.clone(),
        };

        body
    }

    pub fn get_pool(&self) -> SessionPool {
        self.pool
    }

    pub fn set_pool(&mut self, pool: SessionPool) {
        if pool == self.pool {
            return;
        }

        match self.pool {
            SessionPool::Persistent => {
                unreachable!();
            }
            SessionPool::Temp => {
                self.persistent_instance_counter = self.persistent_instance_counter.clone();
                self.temp_instance_counter.force_drop();
            }
        };

        self.pool = pool;
    }
}

impl Clone for CachedInstanceCounter {
    fn clone(&self) -> Self {
        let body = Self {
            pool: self.pool,
            total_instance_counter: self.total_instance_counter.clone(),
            persistent_instance_counter: if self.pool == SessionPool::Persistent {
                self.persistent_instance_counter.clone()
            } else {
                self.persistent_instance_counter.clone_refs_only()
            },
            temp_instance_counter: if self.pool == SessionPool::Temp {
                self.temp_instance_counter.clone()
            } else {
                self.temp_instance_counter.clone_refs_only()
            },
        };

        body
    }
}

/// FIFO cache
pub(crate) struct FifoCache<Key, Value> {
    items: HashMap<Key, (Value, std::time::SystemTime, InstanceCounter)>, //cache items
    items_to_remove: Vec<Key>,                                            //items to remove
    min_flush_size: usize,                                                //min cache size
    expiration_time: std::time::Duration, //expiration time for flushing
    reuse_counter: ResultStatusCounter,   //reuse counter for metrics
    instance_counter: InstanceCounter,    //cached instances counter
}

impl<Key, Value> FifoCache<Key, Value>
where
    Key: std::cmp::Eq + std::hash::Hash + Clone,
{
    pub fn new(name: String, metrics_receiver: &metrics_runtime::Receiver) -> Self {
        const DEFAULT_FIFO_CACHE_MIN_FLUSH_SIZE: usize = 1000;
        const DEFAULT_FIFO_CACHE_EXPIRATION_TIME: std::time::Duration =
            std::time::Duration::from_secs(60);

        Self::new_with_params(
            name,
            metrics_receiver,
            DEFAULT_FIFO_CACHE_MIN_FLUSH_SIZE,
            DEFAULT_FIFO_CACHE_EXPIRATION_TIME,
        )
    }

    pub fn new_with_params(
        name: String,
        metrics_receiver: &metrics_runtime::Receiver,
        min_flush_size: usize,
        expiration_time: std::time::Duration,
    ) -> Self {
        let reuse_counter =
            ResultStatusCounter::new(&metrics_receiver, &format!("{}_cache_reuse", name));
        let instance_counter =
            InstanceCounter::new(&metrics_receiver, &format!("{}_cache_items", name));

        Self {
            items: HashMap::new(),
            min_flush_size: min_flush_size,
            expiration_time: expiration_time,
            items_to_remove: Vec::with_capacity(min_flush_size),
            reuse_counter: reuse_counter,
            instance_counter: instance_counter,
        }
    }

    pub fn get(&mut self, key: &Key) -> Option<&Value> {
        self.reuse_counter.total_increment();

        if let Some(item) = self.items.get_mut(&key) {
            item.1 = std::time::SystemTime::now();

            self.reuse_counter.success();

            return Some(&item.0);
        }

        self.reuse_counter.failure();

        None
    }

    pub fn insert(&mut self, key: Key, value: Value) {
        self.items.insert(
            key,
            (
                value,
                std::time::SystemTime::now(),
                self.instance_counter.clone(),
            ),
        );
    }

    pub fn flush(&mut self) {
        self.items_to_remove.clear();
        self.items_to_remove.reserve(self.items.len());

        for (key, item) in self.items.iter() {
            if let Ok(last_accessed) = item.1.elapsed() {
                if last_accessed >= self.expiration_time {
                    self.items_to_remove.push((*key).clone());

                    if self.items.len() - self.items_to_remove.len() < self.min_flush_size {
                        break;
                    }
                }
            }
        }

        for key in &self.items_to_remove {
            self.items.remove(&key);
        }

        self.items_to_remove.clear();
    }
}
