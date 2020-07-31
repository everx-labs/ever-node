pub use super::*;

/*
===================================================================================================
    BlockCandidateSignature
===================================================================================================
*/

/*
    Implementation details for BlockCandidateSignature
*/

#[derive(Clone)]
pub(crate) struct BlockCandidateSignatureImpl {
    pool: SessionPool,                       //pool of the object
    hash: HashType,                          //hash of the object
    signature: BlockSignature,               //signature
    instance_counter: CachedInstanceCounter, //instance counter
}

/*
    Implementation for public BlockCandidate public trait
*/

impl BlockCandidateSignature for BlockCandidateSignatureImpl {
    fn get_impl(&self) -> &dyn Any {
        self
    }

    fn get_signature(&self) -> &BlockSignature {
        &self.signature
    }

    fn clone_to_persistent(
        &self,
        cache: &mut dyn SessionCache,
    ) -> PoolPtr<dyn BlockCandidateSignature> {
        Self::create_persistent_object(self.clone(), cache)
    }
}

/*
    Implementation for Merge trait
*/

impl Merge<PoolPtr<dyn BlockCandidateSignature>> for PoolPtr<dyn BlockCandidateSignature> {
    fn merge(&self, right: &Self, _desc: &mut dyn SessionDescription) -> Self {
        let left = self;

        if left.get_signature().0 < right.get_signature().0 {
            left.clone()
        } else {
            right.clone()
        }
    }
}

/*
    Implementation for public HashableObject & PoolObject & MovablePoolObject public traits
*/

impl HashableObject for BlockCandidateSignatureImpl {
    fn get_hash(&self) -> HashType {
        self.hash
    }
}

impl PoolObject for BlockCandidateSignatureImpl {
    fn set_pool(&mut self, pool: SessionPool) {
        self.pool = pool;
        self.instance_counter.set_pool(pool);
    }

    fn get_pool(&self) -> SessionPool {
        self.pool
    }
}

impl MovablePoolObject<PoolPtr<dyn BlockCandidateSignature>>
    for PoolPtr<dyn BlockCandidateSignature>
{
    fn move_to_persistent(
        &self,
        cache: &mut dyn SessionCache,
    ) -> PoolPtr<dyn BlockCandidateSignature> {
        if SessionPool::Persistent == self.get_pool() {
            return self.clone();
        }

        self.clone_to_persistent(cache)
    }
}

/*
    Implementation for public CacheObject trait
*/

impl CacheObject<BlockCandidateSignatureImpl> for BlockCandidateSignatureImpl {
    fn compare(&self, value: &Self) -> bool {
        self.hash == value.hash && self.signature == value.signature
    }
}

/*
    Comparison (address only because of the cache)
*/

impl std::cmp::PartialEq for dyn BlockCandidateSignature {
    fn eq(&self, other: &Self) -> bool {
        //compare addresses only because each vector is unique in cache system

        (self as *const Self) == (other as *const Self)
    }
}

/*
    Implementation for public Display & Debug
*/

impl fmt::Display for BlockCandidateSignatureImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BlockCandidateSignature(hash={:?}, signature={:?})",
            &self.hash, &self.signature
        )
    }
}

impl fmt::Debug for BlockCandidateSignatureImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.pool == SessionPool::Temp {
            write!(f, "~").unwrap();
        }

        f.debug_struct("BlockCandidateSignature")
            .field("hash", &format_args!("{:08x}", &self.hash))
            .field("signature", &self.signature)
            .finish()
    }
}

/*
    Implementation internals of BlockCandidateSignatureImpl
*/

impl BlockCandidateSignatureImpl {
    /*
        Hash calculation
    */

    fn compute_hash(signature: &BlockSignature) -> HashType {
        crate::utils::compute_hash(ton::hashable::BlockSignature {
            signature: crate::utils::compute_hash_from_bytes(signature) as ton::int,
        })
    }

    /*
        Creation
    */

    fn new(signature: BlockSignature, instance_counter: &CachedInstanceCounter) -> Self {
        Self {
            hash: Self::compute_hash(&signature),
            signature: signature,
            pool: SessionPool::Temp,
            instance_counter: instance_counter.clone(),
        }
    }

    pub(crate) fn create(
        desc: &mut dyn SessionDescription,
        signature: BlockSignature,
    ) -> BlockCandidateSignaturePtr {
        let body = Self::new(
            signature,
            desc.get_block_candidate_signatures_instance_counter(),
        );

        Some(Self::create_temp_object(body, desc.get_cache()))
    }
}

/*
===================================================================================================
    BlockCandidate
===================================================================================================
*/

/*
    Implementation details for BlockCandidate
*/

#[derive(Clone)]
pub(crate) struct BlockCandidateImpl {
    pool: SessionPool,                             //pool of the object
    hash: HashType,                                //hash of the object
    block: SentBlockPtr,                           //reference to a proposed block
    approved_by: BlockCandidateSignatureVectorPtr, //list of approves for a candidate
    instance_counter: CachedInstanceCounter,       //instance counter
}

/*
    Implementation for public BlockCandidateImpl trait
*/

impl BlockCandidate for BlockCandidateImpl {
    /*
        General purpose methods & accessors
    */

    fn get_impl(&self) -> &dyn Any {
        self
    }

    fn get_id(&self) -> &BlockId {
        if let Some(ref block) = &self.block {
            return block.get_id();
        }

        &DEFAULT_BLOCKID
    }

    fn get_block(&self) -> &SentBlockPtr {
        &self.block
    }

    fn get_source_index(&self) -> u32 {
        if let Some(ref block) = &self.block {
            return block.get_source_index();
        }

        std::u32::MAX
    }

    /*
        Check approvals
    */

    fn check_block_is_approved_by(&self, source_index: u32) -> bool {
        self.approved_by.at(source_index as usize).is_some()
    }

    fn check_block_is_approved(&self, desc: &dyn SessionDescription) -> bool {
        let mut weight = 0;

        for i in 0..desc.get_total_nodes() {
            if self.approved_by.at(i as usize).is_some() {
                weight += desc.get_node_weight(i);

                if weight >= desc.get_cutoff_weight() {
                    return true;
                }
            }
        }

        false
    }

    fn get_approvers_list(&self) -> &BlockCandidateSignatureVectorPtr {
        &self.approved_by
    }

    /*
        Clone object to persistent pool
    */

    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn BlockCandidate> {
        let self_cloned = Self::new(
            self.block.move_to_persistent(cache),
            self.approved_by.move_to_persistent(cache),
            &self.instance_counter,
        );

        Self::create_persistent_object(self_cloned, cache)
    }
}

/*
    Implementation for Merge trait
*/

impl Merge<PoolPtr<dyn BlockCandidate>> for PoolPtr<dyn BlockCandidate> {
    fn merge(&self, right: &Self, desc: &mut dyn SessionDescription) -> Self {
        let left = self;

        assert!(left.get_id() == right.get_id());

        let approved_by = left
            .get_approvers_list()
            .merge(right.get_approvers_list(), desc);

        BlockCandidateImpl::create(desc, left.get_block().clone(), approved_by)
    }
}

/*
    Implementation for public HashableObject & PoolObject & MovablePoolObject public traits
*/

impl HashableObject for BlockCandidateImpl {
    fn get_hash(&self) -> HashType {
        self.hash
    }
}

impl PoolObject for BlockCandidateImpl {
    fn set_pool(&mut self, pool: SessionPool) {
        self.pool = pool;
        self.instance_counter.set_pool(pool);
    }

    fn get_pool(&self) -> SessionPool {
        self.pool
    }
}

impl MovablePoolObject<PoolPtr<dyn BlockCandidate>> for PoolPtr<dyn BlockCandidate> {
    fn move_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn BlockCandidate> {
        if SessionPool::Persistent == self.get_pool() {
            return self.clone();
        }

        self.clone_to_persistent(cache)
    }
}

/*
    Implementation for public BlockCandidateWrapper public traits
*/

impl BlockCandidateWrapper for BlockCandidatePtr {
    fn push(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        signature: BlockCandidateSignaturePtr,
    ) -> BlockCandidatePtr {
        let &self_impl = &get_impl(&**self);

        //if source has already sent signature for the proposed candidate - ignore new signature

        if let Some(ref _signature) = &self_impl.approved_by.at(src_idx as usize) {
            return self.clone();
        }

        //add signature to the candidate

        let approved_by = self_impl
            .approved_by
            .change(desc, src_idx as usize, signature);

        BlockCandidateImpl::create(desc, self_impl.block.clone(), approved_by)
    }
}

/*
    Comparison (address only because of the cache)
*/

impl std::cmp::PartialEq for dyn BlockCandidate {
    fn eq(&self, other: &Self) -> bool {
        //compare addresses only because each vector is unique in cache system

        (self as *const Self) == (other as *const Self)
    }
}

/*
    Implementation for public CacheObject trait
*/

impl CacheObject<BlockCandidateImpl> for BlockCandidateImpl {
    fn compare(&self, value: &Self) -> bool {
        self.block == value.block
            && &self.approved_by == &value.approved_by
            && self.hash == value.hash
    }
}

/*
    Implementation for public Display & Debug
*/

impl fmt::Display for BlockCandidateImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BlockCandidate(hash={:08x}, block={:?})",
            &self.hash, &self.block
        )
    }
}

impl fmt::Debug for BlockCandidateImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.pool == SessionPool::Temp {
            write!(f, "~").unwrap();
        }

        f.debug_struct("BlockCandidate")
            .field("hash", &format_args!("{:08x}", &self.hash))
            .field("block", &self.block)
            .field("approved_by", &self.approved_by)
            .finish()
    }
}

/*
    Implementation internals of BlockCandidateImpl
*/

/// Function which converts public trait to its implementation
fn get_impl(value: &dyn BlockCandidate) -> &BlockCandidateImpl {
    value
        .get_impl()
        .downcast_ref::<BlockCandidateImpl>()
        .unwrap()
}

impl BlockCandidateImpl {
    /*
        Hash calculation
    */

    fn compute_hash(
        block: &SentBlockPtr,
        approved_by: &BlockCandidateSignatureVectorPtr,
    ) -> HashType {
        crate::utils::compute_hash(ton::hashable::BlockCandidate {
            block: block.get_ton_hash(),
            approved: approved_by.get_ton_hash(),
        })
    }

    /*
        Candidate creation
    */

    fn new(
        block: SentBlockPtr,
        approved_by: BlockCandidateSignatureVectorPtr,
        instance_counter: &CachedInstanceCounter,
    ) -> Self {
        Self {
            hash: Self::compute_hash(&block, &approved_by),
            pool: SessionPool::Temp,
            block: block,
            approved_by: approved_by,
            instance_counter: instance_counter.clone(),
        }
    }

    pub(crate) fn create(
        desc: &mut dyn SessionDescription,
        block: SentBlockPtr,
        approved_by: BlockCandidateSignatureVectorPtr,
    ) -> BlockCandidatePtr {
        let body = Self::new(
            block,
            approved_by,
            desc.get_block_candidates_instance_counter(),
        );

        Self::create_temp_object(body, desc.get_cache())
    }

    pub(crate) fn create_unapproved(
        desc: &mut dyn SessionDescription,
        block: SentBlockPtr,
    ) -> BlockCandidatePtr {
        let nodes_count = desc.get_total_nodes() as usize;
        let approved_by = SessionFactory::create_vector(desc, vec![None; nodes_count]);

        Self::create(desc, block, approved_by)
    }
}
