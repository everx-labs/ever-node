pub use super::*;
pub use catchain::InstanceCounter;

/// Entry entry to be used by cache objects
pub type CacheEntry = Rc<dyn std::any::Any>;

/// Cache for session objects
pub trait SessionCache {
    /// Search cache entry in a cache
    fn get_cache_entry_by_hash(&self, hash: HashType, allow_temp: bool) -> Option<&CacheEntry>;

    /// Add new cache entry
    fn add_cache_entry(&mut self, hash: HashType, cache_entry: CacheEntry, pool: SessionPool);

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
        let allow_temp = match pool {
            SessionPool::Persistent => true,
            _ => false,
        };

        if let Some(ref cache_entry) = &cache.get_cache_entry_by_hash(hash, allow_temp) {
            if let Some(ref ptr) = Self::unwrap_cache_entry(&cache_entry) {
                if ptr.compare(&value) {
                    return (*ptr).clone();
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
        Self::create_object(value, SessionPool::Persistent, cache)
    }
}

/// Cached instance counter
#[derive(Clone)]
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

    pub fn clone(&self) -> Self {
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

    pub fn get_pool(&self) -> SessionPool {
        self.pool
    }

    pub fn set_pool(&mut self, pool: SessionPool) {
        if pool == self.pool {
            return;
        }

        match self.pool {
            SessionPool::Persistent => {
                self.temp_instance_counter = self.temp_instance_counter.clone();
                self.persistent_instance_counter.drop();
            }
            SessionPool::Temp => {
                self.persistent_instance_counter = self.persistent_instance_counter.clone();
                self.temp_instance_counter.drop();
            }
        };

        self.pool = pool;
    }
}
