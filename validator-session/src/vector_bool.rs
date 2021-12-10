pub use super::*;

/*
    Implementation details for BoolVector
*/

#[derive(Clone)]
pub(crate) struct BoolVectorImpl {
    pool: SessionPool,                       //pool of the object
    hash: HashType,                          //hash of the vector
    data: Vec<u32>,                          //vector data
    len: usize,                              //number of elements
    instance_counter: CachedInstanceCounter, //instance counter
}

/*
    Implementation for public Vector trait
*/

impl BoolVector for BoolVectorImpl {
    /*
        General purpose methods
    */

    fn get_impl(&self) -> &dyn Any {
        self
    }

    /*
        Accessors
    */

    fn len(&self) -> usize {
        self.len
    }

    fn at(&self, index: usize) -> bool {
        assert!(index <= self.len);

        self.get_bit(index)
    }

    /*
        Creating new modified vector
    */

    fn change(
        &self,
        desc: &mut dyn SessionDescription,
        index: usize,
        value: bool,
    ) -> PoolPtr<dyn BoolVector> {
        profiling::instrument!();

        assert!(index <= self.len);

        let mut data = self.data.clone();

        Self::set_bit_raw(&mut data, index, value);

        let result = BoolVectorImpl::with_raw_data(self.len, data, &self.instance_counter);

        BoolVectorImpl::create_temp_object(result, desc.get_cache())
    }

    /*
        Clone object to persistent pool
    */

    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn BoolVector> {
        profiling::instrument!();

        let self_cloned = Self::with_raw_data(self.len, self.data.clone(), &self.instance_counter);

        Self::create_persistent_object(self_cloned, cache)
    }
}

/*
    Implementation of merge
*/

impl VectorMerge<bool, PoolPtr<dyn BoolVector>> for PoolPtr<dyn BoolVector> {
    fn merge_impl(
        &self,
        _right: &PoolPtr<dyn BoolVector>,
        _desc: &mut dyn SessionDescription,
        _merge_all: bool,
        _merge_fn: &dyn Fn(&bool, &bool, &mut dyn SessionDescription) -> bool,
    ) -> PoolPtr<dyn BoolVector> {
        unreachable!();
    }

    fn merge(
        &self,
        right: &PoolPtr<dyn BoolVector>,
        desc: &mut dyn SessionDescription,
    ) -> PoolPtr<dyn BoolVector> {
        profiling::instrument!();

        let left = self;
        let left_count = left.len();
        let right_count = right.len();
        let left_data = &BoolVectorImpl::get_impl(&**left).data;
        let right_data = &BoolVectorImpl::get_impl(&**right).data;

        assert!(left_count == right_count);

        let size = left_data.len();
        let mut ret_left = true;
        let mut ret_right = true;

        for i in 0..size {
            if (left_data[i] & !right_data[i]) != 0 {
                ret_right = false;
            }
            if (right_data[i] & !left_data[i]) != 0 {
                ret_left = false;
            }
        }

        if ret_left {
            return left.clone();
        }

        if ret_right {
            return right.clone();
        }

        let mut result_data = left_data.clone();

        for i in 0..size {
            result_data[i] |= right_data[i];
        }

        BoolVectorImpl::create_temp_object(
            BoolVectorImpl::with_raw_data(
                left_count,
                result_data,
                bool::get_vector_instance_counter(desc),
            ),
            desc.get_cache(),
        )
    }
}

/*
    Comparison (address only because of the cache)
*/

impl std::cmp::PartialEq for dyn BoolVector {
    fn eq(&self, other: &Self) -> bool {
        //compare addresses only because each vector is unique in cache system

        (self as *const dyn BoolVector) == (other as *const dyn BoolVector)
    }
}

/*
    Implementation for public HashableObject & PoolObject & MovablePoolObject traits
*/

impl HashableObject for BoolVectorImpl {
    fn get_hash(&self) -> HashType {
        self.hash
    }
}

impl PoolObject for BoolVectorImpl {
    fn set_pool(&mut self, pool: SessionPool) {
        self.pool = pool;
        self.instance_counter.set_pool(pool);
    }

    fn get_pool(&self) -> SessionPool {
        self.pool
    }
}

impl MovablePoolObject<PoolPtr<dyn BoolVector>> for PoolPtr<dyn BoolVector> {
    fn move_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn BoolVector> {
        profiling::instrument!();

        if SessionPool::Persistent == self.get_pool() {
            return self.clone();
        }

        self.clone_to_persistent(cache)
    }
}

/*
    Implementation for public CacheObject trait
*/

impl CacheObject<BoolVectorImpl> for BoolVectorImpl {
    fn compare(&self, value: &Self) -> bool {
        if self.hash != value.hash {
            return false;
        }

        if self.len != value.len {
            return false;
        }

        self.data == value.data
    }
}

/*
    Implementation for public Display & Debug
*/

impl fmt::Display for BoolVectorImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;

        for i in 0..self.len {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", if self.get_bit(i) { "1" } else { "0" })?;
        }

        write!(f, "]")
    }
}

impl fmt::Debug for BoolVectorImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.pool == SessionPool::Temp {
            write!(f, "~").unwrap();
        }

        write!(f, "bool_vec(hash={:08x}, data=", self.hash)?;
        write!(f, "{}", self)
    }
}

/*
    Implementation internals of VectorImpl
*/

impl BoolVectorImpl {
    pub(crate) fn get_impl(value: &dyn BoolVector) -> &Self {
        value.get_impl().downcast_ref::<Self>().unwrap()
    }

    fn get_bit(&self, index: usize) -> bool {
        let mask: u32 = 1u32 << (index % 32);
        (self.data[index / 32] & mask) != 0
    }

    fn set_bit_raw(data: &mut Vec<u32>, index: usize, value: bool) {
        let mask: u32 = 1u32 << (index % 32);
        if value {
            data[index / 32] |= mask;
        } else {
            data[index / 32] &= !mask;
        }
    }

    fn with_data(data: Vec<bool>, instance_counter: &CachedInstanceCounter) -> Self {
        let elements_count = if data.len() % 32 == 0 {
            data.len()
        } else {
            data.len() - data.len() % 32 + 32
        };
        let mut buffer: Vec<u32> = vec![0; elements_count / 32];

        for (i, item) in data.iter().enumerate() {
            Self::set_bit_raw(&mut buffer, i, *item)
        }

        let hash = utils::compute_hash_from_buffer_u32(&buffer[..]);

        Self {
            pool: SessionPool::Temp,
            hash: hash,
            data: buffer,
            len: data.len(),
            instance_counter: instance_counter.clone_as_temp(),
        }
    }

    fn with_raw_data(
        len: usize,
        buffer: Vec<u32>,
        instance_counter: &CachedInstanceCounter,
    ) -> Self {
        let hash = utils::compute_hash_from_buffer_u32(&buffer[..]);

        Self {
            pool: SessionPool::Temp,
            hash: hash,
            data: buffer,
            len: len,
            instance_counter: instance_counter.clone_as_temp(),
        }
    }

    pub(crate) fn create(
        desc: &mut dyn SessionDescription,
        data: Vec<bool>,
    ) -> PoolPtr<dyn BoolVector> {
        profiling::instrument!();

        Self::create_temp_object(
            Self::with_data(data, bool::get_vector_instance_counter(desc)),
            desc.get_cache(),
        )
    }
}
