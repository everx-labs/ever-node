pub use super::*;
use crate::ton_api::ton::Hashable;
use crate::ton_api::IntoBoxed;
use std::collections::BTreeMap;

/*
    hash specialization
*/

pub(crate) fn compute_hash_from_bytes(data: &::ton_api::ton::bytes) -> HashType {
    crc32c::crc32c(&data.0[..])
}

pub(crate) fn compute_hash<T>(hashable: T) -> HashType
where
    T: IntoBoxed<Boxed = Hashable>,
{
    let serialized_object = catchain::utils::serialize_tl_boxed_object!(&hashable.into_boxed());

    compute_hash_from_bytes(&serialized_object)
}

impl HashableObject for u32 {
    fn get_hash(&self) -> HashType {
        compute_hash(ton::hashable::Int32 {
            value: *self as i32,
        })
    }
}

impl HashableObject for bool {
    fn get_hash(&self) -> HashType {
        compute_hash(ton::hashable::Bool {
            value: match *self {
                true => ::ton_api::ton::Bool::BoolTrue,
                _ => ::ton_api::ton::Bool::BoolFalse,
            },
        })
    }
}

impl HashableObject for BlockHash {
    fn get_hash(&self) -> HashType {
        compute_hash(ton::hashable::Int256 {
            value: self.clone().into(),
        })
    }
}

impl<T> HashableObject for Option<T>
where
    T: HashableObject + Sized + 'static,
{
    fn get_hash(&self) -> HashType {
        const ZERO_HASH: HashType = 0;

        match self {
            Some(value) => value.get_hash(),
            _ => ZERO_HASH,
        }
    }
}

impl<T> HashableObject for PoolPtr<T>
where
    T: HashableObject + ?Sized + 'static,
{
    fn get_hash(&self) -> HashType {
        self.as_ref().get_hash()
    }
}

/*
    move_to_persistent specialization
*/

impl<T> MovablePoolObject<Option<T>> for Option<T>
where
    T: MovablePoolObject<T>,
{
    fn move_to_persistent(&self, cache: &mut dyn SessionCache) -> Option<T> {
        match self {
            Some(obj) => Some(obj.move_to_persistent(cache)),
            _ => None,
        }
    }
}

impl MovablePoolObject<bool> for bool {
    fn move_to_persistent(&self, _cache: &mut dyn SessionCache) -> Self {
        *self
    }
}

impl MovablePoolObject<u32> for u32 {
    fn move_to_persistent(&self, _cache: &mut dyn SessionCache) -> Self {
        *self
    }
}

/*
    merge specializations
*/

impl Merge<bool> for bool {
    fn merge(&self, right: &Self, _desc: &mut dyn SessionDescription) -> Self {
        let left = self;

        left | right
    }
}

impl<T> Merge<Option<T>> for Option<T>
where
    T: Merge<T> + Clone + std::cmp::PartialEq,
{
    fn merge(&self, right: &Self, desc: &mut dyn SessionDescription) -> Self {
        let left = self;

        if left.is_none() {
            return right.clone();
        }

        if right.is_none() {
            return left.clone();
        }

        let left_ptr = left.as_ref().unwrap();
        let right_ptr = right.as_ref().unwrap();

        if left_ptr == right_ptr {
            return left.clone();
        }

        Some(left_ptr.merge(&right_ptr, desc))
    }
}

impl<T> VectorMerge<T, Option<PoolPtr<dyn Vector<T>>>> for Option<PoolPtr<dyn Vector<T>>>
where
    T: MovablePoolObject<T>
        + TypeDesc
        + HashableObject
        + std::cmp::PartialEq
        + Clone
        + fmt::Debug
        + 'static,
{
    fn merge_impl(
        &self,
        right: &Self,
        desc: &mut dyn SessionDescription,
        merge_all: bool,
        merge_fn: &dyn Fn(&T, &T, &mut dyn SessionDescription) -> T,
    ) -> Self {
        let left = self;

        if !merge_all {
            if left.is_none() {
                return right.clone();
            }

            if right.is_none() {
                return left.clone();
            }

            if left == right {
                return left.clone();
            }
        }

        let left_ptr = left.as_ref().unwrap();
        let right_ptr = right.as_ref().unwrap();

        Some(left_ptr.merge_impl(&right_ptr, desc, merge_all, merge_fn))
    }
}

impl<T, Compare> VectorMerge<T, Option<PoolPtr<dyn SortedVector<T, Compare>>>>
    for Option<PoolPtr<dyn SortedVector<T, Compare>>>
where
    T: MovablePoolObject<T>
        + TypeDesc
        + HashableObject
        + std::cmp::PartialEq
        + Clone
        + fmt::Debug
        + 'static,
    Compare: SortingPredicate<T> + 'static,
{
    fn merge_impl(
        &self,
        right: &Self,
        desc: &mut dyn SessionDescription,
        merge_all: bool,
        merge_fn: &dyn Fn(&T, &T, &mut dyn SessionDescription) -> T,
    ) -> Self {
        let left = self;

        if !merge_all {
            if left.is_none() {
                return right.clone();
            }

            if right.is_none() {
                return left.clone();
            }

            if left == right {
                return left.clone();
            }
        }

        let left_ptr = left.as_ref().unwrap();
        let right_ptr = right.as_ref().unwrap();

        Some(left_ptr.merge_impl(&right_ptr, desc, merge_all, merge_fn))
    }
}

/*
    type descs
*/

impl TypeDesc for u32 {
    fn get_vector_instance_counter(desc: &dyn SessionDescription) -> &CachedInstanceCounter {
        desc.get_integer_vectors_instance_counter()
    }
}

impl TypeDesc for bool {
    fn get_vector_instance_counter(desc: &dyn SessionDescription) -> &CachedInstanceCounter {
        desc.get_bool_vectors_instance_counter()
    }
}

impl TypeDesc for dyn BlockCandidateSignature {
    fn get_vector_instance_counter(desc: &dyn SessionDescription) -> &CachedInstanceCounter {
        desc.get_block_candidate_signature_vectors_instance_counter()
    }
}

impl TypeDesc for dyn BlockCandidate {
    fn get_vector_instance_counter(desc: &dyn SessionDescription) -> &CachedInstanceCounter {
        desc.get_block_candidate_vectors_instance_counter()
    }
}

impl TypeDesc for dyn VoteCandidate {
    fn get_vector_instance_counter(desc: &dyn SessionDescription) -> &CachedInstanceCounter {
        desc.get_vote_candidate_vectors_instance_counter()
    }
}

impl TypeDesc for dyn RoundAttemptState {
    fn get_vector_instance_counter(desc: &dyn SessionDescription) -> &CachedInstanceCounter {
        desc.get_round_attempt_vectors_instance_counter()
    }
}

impl TypeDesc for dyn OldRoundState {
    fn get_vector_instance_counter(desc: &dyn SessionDescription) -> &CachedInstanceCounter {
        desc.get_old_round_vectors_instance_counter()
    }
}

impl<T> TypeDesc for Option<T>
where
    T: TypeDesc,
{
    fn get_vector_instance_counter(desc: &dyn SessionDescription) -> &CachedInstanceCounter {
        T::get_vector_instance_counter(desc)
    }
}

impl<T> TypeDesc for PoolPtr<T>
where
    T: TypeDesc + ?Sized,
{
    fn get_vector_instance_counter(desc: &dyn SessionDescription) -> &CachedInstanceCounter {
        T::get_vector_instance_counter(desc)
    }
}

/*
    metrics
*/

pub(crate) fn dump_metric(
    key: &String,
    value: &metrics_runtime::Measurement,
    metrics: &BTreeMap<String, &metrics_runtime::Measurement>,
) {
    if !key.contains(".total") && !key.contains(".temp") && !key.contains(".persistent") {
        catchain::utils::dump_metric(key, value, metrics);
        return;
    }

    if !key.contains(".total.create") {
        return;
    }

    let basic_key = key.replace(".total.create", "");

    debug!(
        "...{}={} (persistent={}; temp={})",
        basic_key,
        catchain::utils::instance_counter_to_string(&(basic_key.clone() + ".total"), metrics),
        catchain::utils::instance_counter_to_string(&(basic_key.clone() + ".persistent"), metrics),
        catchain::utils::instance_counter_to_string(&(basic_key.clone() + ".temp"), metrics)
    );
}
