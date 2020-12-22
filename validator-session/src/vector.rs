pub use super::*;

/*
===================================================================================================
    Utilities
===================================================================================================
*/

fn compute_hash<T>(data: &Vec<T>) -> HashType
where
    T: HashableObject,
{
    let hashes: Vec<ton::int> = data.iter().map(|x| x.get_ton_hash()).collect();

    crate::utils::compute_hash(ton::hashable::Vector {
        value: hashes.into(),
    })
}

fn compare<T>(left: &Vec<T>, right: &Vec<T>) -> bool
where
    T: std::cmp::PartialEq,
{
    if left.len() != right.len() {
        return false;
    }

    for i in 0..left.len() as usize {
        if left[i] != right[i] {
            return false;
        }
    }

    true
}

/*
===================================================================================================
    Vector
===================================================================================================
*/

/*
    Implementation details for Vector
*/

#[derive(Clone)]
pub(crate) struct VectorImpl<T> {
    pool: SessionPool,                       //pool of the object
    hash: HashType,                          //hash of the vector
    data: Vec<T>,                            //vector data
    instance_counter: CachedInstanceCounter, //instance counter
}

/*
    Implementation for public Vector trait
*/

impl<T> Vector<T> for VectorImpl<T>
where
    T: Clone
        + HashableObject
        + TypeDesc
        + MovablePoolObject<T>
        + fmt::Debug
        + std::cmp::PartialEq
        + 'static,
{
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
        self.data.len()
    }

    fn at(&self, index: usize) -> &T {
        assert!(index <= self.data.len());

        &self.data[index]
    }

    fn iter(&self) -> std::slice::Iter<T> {
        self.data.iter()
    }

    /*
        Creating new modified vector
    */

    fn push(&self, desc: &mut dyn SessionDescription, value: T) -> PoolPtr<dyn Vector<T>> {
        let mut result = VectorImpl::<T>::new(self.data.len() + 1, &self.instance_counter);

        result.data.extend_from_slice(&self.data);
        result.data.push(value);
        result.recompute_hash();

        VectorImpl::create_temp_object(result, desc.get_cache())
    }

    fn change(
        &self,
        desc: &mut dyn SessionDescription,
        index: usize,
        value: T,
    ) -> PoolPtr<dyn Vector<T>> {
        assert!(index <= self.data.len());

        let mut result = VectorImpl::<T>::with_data(self.data.clone(), &self.instance_counter);

        result.data[index] = value;

        result.recompute_hash();

        VectorImpl::create_temp_object(result, desc.get_cache())
    }

    fn modify(
        &self,
        desc: &mut dyn SessionDescription,
        modifier: &Box<dyn Fn(&T) -> T>,
    ) -> PoolPtr<dyn Vector<T>> {
        let modified_vec = self
            .data
            .clone()
            .into_iter()
            .map(|x| modifier(&x))
            .collect();
        let result = VectorImpl::<T>::with_data(modified_vec, &self.instance_counter);

        VectorImpl::create_temp_object(result, desc.get_cache())
    }

    /*
        Clone object to persistent pool
    */

    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn Vector<T>> {
        let data_cloned = self
            .data
            .iter()
            .map(|x| x.move_to_persistent(cache))
            .collect();
        let self_cloned = Self::with_data(data_cloned, &self.instance_counter);

        Self::create_persistent_object(self_cloned, cache)
    }
}

/*
    Implementation for public Vector trait
*/

impl<T> VectorWrapper<T> for Option<PoolPtr<dyn Vector<T>>>
where
    T: Clone
        + HashableObject
        + MovablePoolObject<T>
        + TypeDesc
        + fmt::Debug
        + std::cmp::PartialEq
        + 'static,
{
    /*
        Accessors
    */

    fn len(&self) -> usize {
        match self {
            Some(vec) => vec.len(),
            _ => 0,
        }
    }

    fn at(&self, index: usize) -> &T {
        self.as_ref().unwrap().at(index)
    }

    /*
        Iterator
    */

    fn iter(&self) -> std::slice::Iter<T> {
        match &self {
            Some(ref src) => src.iter(),
            _ => (&[]).iter(),
        }
    }

    /*
        Modifying vector
    */

    fn push(&self, desc: &mut dyn SessionDescription, value: T) -> Option<PoolPtr<dyn Vector<T>>> {
        Some(match &self {
            Some(ref src) => VectorImpl::<T>::get_impl(&**src).push(desc, value),
            _ => VectorImpl::<T>::new(1, T::get_vector_instance_counter(desc)).push(desc, value),
        })
    }

    fn change(
        &self,
        desc: &mut dyn SessionDescription,
        index: usize,
        value: T,
    ) -> Option<PoolPtr<dyn Vector<T>>> {
        match &self {
            Some(ref src) => Some(VectorImpl::<T>::get_impl(&**src).change(desc, index, value)),
            _ => None,
        }
    }

    fn modify(
        &self,
        desc: &mut dyn SessionDescription,
        modifier: &Box<dyn Fn(&T) -> T>,
    ) -> Option<PoolPtr<dyn Vector<T>>> {
        match &self {
            Some(ref src) => Some(VectorImpl::<T>::get_impl(&**src).modify(desc, modifier)),
            _ => None,
        }
    }
}

/*
    Implementation of merge trait
*/

impl<T> VectorMerge<T, PoolPtr<dyn Vector<T>>> for PoolPtr<dyn Vector<T>>
where
    T: Clone
        + HashableObject
        + TypeDesc
        + std::cmp::PartialEq
        + MovablePoolObject<T>
        + fmt::Debug
        + 'static,
{
    fn merge_impl(
        &self,
        right: &PoolPtr<dyn Vector<T>>,
        desc: &mut dyn SessionDescription,
        merge_all: bool,
        merge_fn: &dyn Fn(&T, &T, &mut dyn SessionDescription) -> T,
    ) -> PoolPtr<dyn Vector<T>> {
        let left = self;
        let left_count = left.len();
        let right_count = right.len();
        let count = std::cmp::max(left_count, right_count);

        //check if the merge is really needed

        if !merge_all {
            let mut ret_left = true;
            let mut ret_right = true;

            for i in 0..count {
                if i >= left_count {
                    ret_left = false;
                    break;
                } else if i >= right_count {
                    ret_right = false;
                    break;
                } else if left.at(i) != right.at(i) {
                    ret_right = false;
                    ret_left = false;
                    break;
                }
            }

            if ret_left {
                return left.clone();
            }
            if ret_right {
                return right.clone();
            }
        }

        //merge all

        let mut items = Vec::with_capacity(count);

        for i in 0..count {
            let merged_item = if i >= left_count {
                if !merge_all {
                    right.at(i).clone()
                } else {
                    merge_fn(right.at(i), right.at(i), desc)
                }
            } else if i >= right_count {
                if !merge_all {
                    left.at(i).clone()
                } else {
                    merge_fn(left.at(i), left.at(i), desc)
                }
            } else {
                merge_fn(left.at(i), right.at(i), desc)
            };

            items.push(merged_item);
        }

        VectorImpl::<T>::create(desc, items)
    }
}

/*
    Comparison (address only because of the cache)
*/

impl<T> std::cmp::PartialEq for dyn Vector<T>
where
    T: 'static,
{
    fn eq(&self, other: &Self) -> bool {
        //compare addresses only because each vector is unique in cache system

        (self as *const dyn Vector<T>) == (other as *const dyn Vector<T>)
    }
}

/*
    Implementation for public HashableObject & PoolObject & MovablePoolObject traits
*/

impl<T> HashableObject for VectorImpl<T>
where
    T: 'static,
{
    fn get_hash(&self) -> HashType {
        self.hash
    }
}

impl<T> PoolObject for VectorImpl<T>
where
    T: 'static,
{
    fn set_pool(&mut self, pool: SessionPool) {
        self.pool = pool;
        self.instance_counter.set_pool(pool);
    }

    fn get_pool(&self) -> SessionPool {
        self.pool
    }
}

impl<T> MovablePoolObject<PoolPtr<dyn Vector<T>>> for PoolPtr<dyn Vector<T>>
where
    T: 'static,
    T: Clone + HashableObject + TypeDesc + MovablePoolObject<T> + fmt::Debug,
{
    fn move_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn Vector<T>> {
        if SessionPool::Persistent == self.get_pool() {
            return self.clone();
        }

        self.clone_to_persistent(cache)
    }
}

/*
    Implementation for public CacheObject trait
*/

impl<T> CacheObject<VectorImpl<T>> for VectorImpl<T>
where
    T: std::cmp::PartialEq + 'static,
{
    fn compare(&self, value: &Self) -> bool {
        if self.hash != value.hash {
            return false;
        }

        compare(&self.data, &value.data)
    }
}

/*
    Implementation for public Display & Debug
*/

impl<T> fmt::Display for VectorImpl<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &self.data)
    }
}

impl<T> fmt::Debug for VectorImpl<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.pool == SessionPool::Temp {
            write!(f, "~").unwrap();
        }

        f.debug_struct("vec")
            .field("hash", &format_args!("{:08x}", &self.hash))
            .field("data", &self.data)
            .finish()
    }
}

/*
    Implementation internals of VectorImpl
*/

impl<T> VectorImpl<T>
where
    T: Clone
        + HashableObject
        + MovablePoolObject<T>
        + TypeDesc
        + fmt::Debug
        + std::cmp::PartialEq
        + 'static,
{
    pub(crate) fn get_impl(value: &dyn Vector<T>) -> &Self {
        value.get_impl().downcast_ref::<Self>().unwrap()
    }

    fn new(capacity: usize, instance_counter: &CachedInstanceCounter) -> Self {
        let data = Vec::with_capacity(capacity);

        Self {
            pool: SessionPool::Temp,
            hash: compute_hash(&data),
            data: data,
            instance_counter: instance_counter.clone(),
        }
    }

    fn with_data(data: Vec<T>, instance_counter: &CachedInstanceCounter) -> Self {
        Self {
            pool: SessionPool::Temp,
            hash: compute_hash(&data),
            data: data,
            instance_counter: instance_counter.clone(),
        }
    }

    fn recompute_hash(&mut self) {
        self.hash = compute_hash(&self.data);
    }

    pub(crate) fn create(
        desc: &mut dyn SessionDescription,
        data: Vec<T>,
    ) -> PoolPtr<dyn Vector<T>> {
        VectorImpl::create_temp_object(
            Self::with_data(data, T::get_vector_instance_counter(desc)),
            desc.get_cache(),
        )
    }

    pub(crate) fn create_wrapper(
        desc: &mut dyn SessionDescription,
        data: Vec<T>,
    ) -> Option<PoolPtr<dyn Vector<T>>> {
        if data.len() == 0 {
            return None;
        }

        Some(Self::create(desc, data))
    }
}

/*
===================================================================================================
    SortedVector
===================================================================================================
*/

/*
    Implementation details for SortedVector
*/

#[derive(Clone)]
pub(crate) struct SortedVectorImpl<T> {
    pool: SessionPool,                       //pool of the object
    hash: HashType,                          //hash of the data
    data: Vec<T>,                            //vector data
    instance_counter: CachedInstanceCounter, //instance counter
}

/*
    Implementation for public Vector trait
*/

impl<T, Compare> SortedVector<T, Compare> for SortedVectorImpl<T>
where
    T: Clone
        + HashableObject
        + TypeDesc
        + MovablePoolObject<T>
        + std::cmp::PartialEq
        + fmt::Debug
        + 'static,
    Compare: SortingPredicate<T> + 'static,
{
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
        self.data.len()
    }

    fn at(&self, index: usize) -> &T {
        assert!(index <= self.data.len());

        &self.data[index]
    }

    fn iter(&self) -> std::slice::Iter<T> {
        self.data.iter()
    }

    /*
        Clone object to persistent pool
    */

    fn clone_to_persistent(
        &self,
        cache: &mut dyn SessionCache,
    ) -> PoolPtr<dyn SortedVector<T, Compare>> {
        let data_cloned = self
            .data
            .iter()
            .map(|x| x.move_to_persistent(cache))
            .collect();
        let self_cloned = Self::with_data(data_cloned, &self.instance_counter);

        Self::create_persistent_object(self_cloned, cache)
    }
}

/*
    Implementation for public SortedVectorWrapper trait
*/

impl<T, Compare> SortedVectorWrapper<T, Compare> for Option<PoolPtr<dyn SortedVector<T, Compare>>>
where
    T: Clone
        + HashableObject
        + TypeDesc
        + MovablePoolObject<T>
        + std::cmp::PartialEq
        + fmt::Debug
        + 'static,
    Compare: SortingPredicate<T> + 'static,
{
    /*
        Accessors
    */

    fn len(&self) -> usize {
        match self {
            Some(vec) => vec.len(),
            _ => 0,
        }
    }

    fn at(&self, index: usize) -> &T {
        self.as_ref().unwrap().at(index)
    }

    /*
        Iterator
    */

    fn iter(&self) -> std::slice::Iter<T> {
        match &self {
            Some(ref src) => src.iter(),
            _ => (&[]).iter(),
        }
    }

    /*
        Modifying vector
    */

    fn push(
        &self,
        desc: &mut dyn SessionDescription,
        value: T,
    ) -> Option<PoolPtr<dyn SortedVector<T, Compare>>> {
        match &self {
            Some(ref src) => {
                let data = &SortedVectorImpl::<T>::get_impl(&**src).data;

                let mut left: i32 = -1;
                let mut right: i32 = data.len() as i32;

                while right - left > 1 {
                    let middle = (right + left) as usize / 2;
                    let middle_value = &data[middle];

                    if Compare::less(&middle_value, &value) {
                        left = middle as i32;
                    } else if Compare::less(&value, &middle_value) {
                        right = middle as i32;
                    } else {
                        if middle_value == &value {
                            return self.clone();
                        }

                        let mut result = SortedVectorImpl::<T>::new(
                            data.len(),
                            T::get_vector_instance_counter(desc),
                        );

                        result.data.extend_from_slice(&data);

                        result.data[middle] = value;

                        result.recompute_hash();

                        return Some(SortedVectorImpl::<T>::create_temp_object(
                            result,
                            desc.get_cache(),
                        ));
                    }
                }

                let mut result = SortedVectorImpl::<T>::new(
                    data.len() + 1,
                    T::get_vector_instance_counter(desc),
                );

                result.data.extend_from_slice(&data[0..right as usize]);
                result.data.push(value);

                if data.len() > right as usize {
                    result
                        .data
                        .extend_from_slice(&data[right as usize..data.len()]);
                }

                result.recompute_hash();

                Some(SortedVectorImpl::<T>::create_temp_object(
                    result,
                    desc.get_cache(),
                ))
            }
            _ => {
                let mut result =
                    SortedVectorImpl::<T>::new(1, T::get_vector_instance_counter(desc));

                result.data.push(value);
                result.recompute_hash();

                Some(SortedVectorImpl::<T>::create_temp_object(
                    result,
                    desc.get_cache(),
                ))
            }
        }
    }
}

/*
    Implementation of merge trait
*/

impl<T, Compare> VectorMerge<T, PoolPtr<dyn SortedVector<T, Compare>>>
    for PoolPtr<dyn SortedVector<T, Compare>>
where
    T: Clone
        + HashableObject
        + TypeDesc
        + MovablePoolObject<T>
        + std::cmp::PartialEq
        + fmt::Debug
        + 'static,
    Compare: SortingPredicate<T> + 'static,
{
    fn merge_impl(
        &self,
        right: &PoolPtr<dyn SortedVector<T, Compare>>,
        desc: &mut dyn SessionDescription,
        _merge_all: bool,
        merge_fn: &dyn Fn(&T, &T, &mut dyn SessionDescription) -> T,
    ) -> PoolPtr<dyn SortedVector<T, Compare>> {
        let left = self;
        let left_count = left.len();
        let right_count = right.len();

        //check if the merge is really needed

        let mut ret_left = true;
        let mut ret_right = true;

        let mut left_index = 0;
        let mut right_index = 0;

        while left_index < left_count || right_index < right_count {
            if left_index == left_count {
                ret_left = false;
                break;
            } else if right_index == right_count {
                ret_right = false;
                break;
            } else {
                let left_item = left.at(left_index);
                let right_item = right.at(right_index);

                if Compare::less(left_item, right_item) {
                    ret_right = false;
                    left_index += 1;
                } else if Compare::less(right_item, left_item) {
                    ret_left = false;
                    right_index += 1;
                } else {
                    left_index += 1;
                    right_index += 1;

                    if left_item != right_item {
                        ret_left = false;
                        ret_right = false;
                        break;
                    }
                }
            }
        }

        if ret_left {
            return left.clone();
        }
        if ret_right {
            return right.clone();
        }

        //merge all

        let mut items = Vec::with_capacity(std::cmp::max(left_count, right_count));

        let mut left_index = 0;
        let mut right_index = 0;

        while left_index < left_count || right_index < right_count {
            if left_index == left_count {
                items.push(right.at(right_index).clone());

                right_index += 1;
            } else if right_index == right_count {
                items.push(left.at(left_index).clone());

                left_index += 1;
            } else {
                let left_item = left.at(left_index);
                let right_item = right.at(right_index);

                if Compare::less(left_item, right_item) {
                    items.push(left_item.clone());
                    left_index += 1;
                } else if Compare::less(right_item, left_item) {
                    items.push(right_item.clone());
                    right_index += 1;
                } else {
                    left_index += 1;
                    right_index += 1;

                    items.push(merge_fn(&left_item, &right_item, desc));
                }
            }
        }

        SortedVectorImpl::<T>::create(desc, items)
    }
}

/*
    Implementation for public HashableObject & PoolObject & MovablePoolObject public trait
*/

impl<T> HashableObject for SortedVectorImpl<T>
where
    T: 'static,
{
    fn get_hash(&self) -> HashType {
        self.hash
    }
}

impl<T> PoolObject for SortedVectorImpl<T>
where
    T: 'static,
{
    fn set_pool(&mut self, pool: SessionPool) {
        self.pool = pool;
        self.instance_counter.set_pool(pool);
    }

    fn get_pool(&self) -> SessionPool {
        self.pool
    }
}

impl<T, Compare> MovablePoolObject<PoolPtr<dyn SortedVector<T, Compare>>>
    for PoolPtr<dyn SortedVector<T, Compare>>
where
    T: 'static,
    T: Clone + HashableObject + TypeDesc + std::cmp::PartialEq + MovablePoolObject<T> + fmt::Debug,
    Compare: SortingPredicate<T> + 'static,
{
    fn move_to_persistent(
        &self,
        cache: &mut dyn SessionCache,
    ) -> PoolPtr<dyn SortedVector<T, Compare>> {
        if SessionPool::Persistent == self.get_pool() {
            return self.clone();
        }

        self.clone_to_persistent(cache)
    }
}

/*
    Comparison (address only because of the cache)
*/

impl<T, Compare> std::cmp::PartialEq for dyn SortedVector<T, Compare>
where
    T: 'static,
    Compare: 'static,
{
    fn eq(&self, other: &Self) -> bool {
        //compare addresses only because each vector is unique in cache system

        (self as *const dyn SortedVector<T, Compare>)
            == (other as *const dyn SortedVector<T, Compare>)
    }
}

/*
    Implementation for public CacheObject trait
*/

impl<T> CacheObject<SortedVectorImpl<T>> for SortedVectorImpl<T>
where
    T: std::cmp::PartialEq + 'static,
{
    fn compare(&self, value: &Self) -> bool {
        if self.hash != value.hash {
            return false;
        }

        compare(&self.data, &value.data)
    }
}

/*
    Implementation for public Display & Debug
*/

impl<T> fmt::Display for SortedVectorImpl<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &self.data)
    }
}

impl<T> fmt::Debug for SortedVectorImpl<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.pool == SessionPool::Temp {
            write!(f, "~").unwrap();
        }

        f.debug_struct("svec")
            .field("hash", &format_args!("{:08x}", &self.hash))
            .field("data", &self.data)
            .finish()
    }
}

/*
    Implementation internals of SortedVectorImpl
*/

impl<T> SortedVectorImpl<T>
where
    T: Clone
        + HashableObject
        + MovablePoolObject<T>
        + TypeDesc
        + std::cmp::PartialEq
        + fmt::Debug
        + 'static,
{
    pub(crate) fn get_impl<Compare>(value: &dyn SortedVector<T, Compare>) -> &Self
    where
        Compare: SortingPredicate<T> + 'static,
    {
        value.get_impl().downcast_ref::<Self>().unwrap()
    }

    fn new(capacity: usize, instance_counter: &CachedInstanceCounter) -> Self {
        let data = Vec::with_capacity(capacity);

        Self {
            pool: SessionPool::Temp,
            hash: compute_hash(&data),
            data: data,
            instance_counter: instance_counter.clone(),
        }
    }

    fn with_data(data: Vec<T>, instance_counter: &CachedInstanceCounter) -> Self {
        Self {
            pool: SessionPool::Temp,
            hash: compute_hash(&data),
            data: data,
            instance_counter: instance_counter.clone(),
        }
    }

    fn recompute_hash(&mut self) {
        self.hash = compute_hash(&self.data);
    }

    fn create<Compare>(
        desc: &mut dyn SessionDescription,
        data: Vec<T>,
    ) -> PoolPtr<dyn SortedVector<T, Compare>>
    where
        Compare: SortingPredicate<T> + 'static,
    {
        SortedVectorImpl::create_temp_object(
            Self::with_data(data, T::get_vector_instance_counter(desc)),
            desc.get_cache(),
        )
    }

    pub(crate) fn create_empty<Compare>(
        desc: &mut dyn SessionDescription,
    ) -> PoolPtr<dyn SortedVector<T, Compare>>
    where
        Compare: SortingPredicate<T> + 'static,
    {
        SortedVectorImpl::create_temp_object(
            Self::new(1, T::get_vector_instance_counter(desc)),
            desc.get_cache(),
        )
    }
}
