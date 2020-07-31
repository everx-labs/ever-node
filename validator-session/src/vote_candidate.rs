pub use super::*;

/*
    Implementation details for VoteCandidate
*/

#[derive(Clone)]
pub(crate) struct VoteCandidateImpl {
    pool: SessionPool,                       //pool of the object
    hash: HashType,                          //hash of the object
    block: SentBlockPtr,                     //block for voting
    voted_by: BoolVectorPtr,                 //flags of votes from nodes
    instance_counter: CachedInstanceCounter, //instance counter
}

/*
    Implementation for public VoteCandidate traits
*/

impl VoteCandidate for VoteCandidateImpl {
    /*
        General purpose methods & accessors
    */

    fn get_impl(&self) -> &dyn Any {
        self
    }

    fn get_id(&self) -> &BlockId {
        self.block.get_id()
    }

    fn get_block(&self) -> &SentBlockPtr {
        &self.block
    }

    fn get_voters_list(&self) -> &BoolVectorPtr {
        &self.voted_by
    }

    fn get_source_index(&self) -> u32 {
        match &self.block {
            Some(block) => block.get_source_index(),
            _ => std::u32::MAX,
        }
    }

    /*
        Check votes
    */

    fn check_block_is_voted(&self, desc: &dyn SessionDescription) -> bool {
        let mut weight = 0;

        for i in 0..desc.get_total_nodes() {
            if *self.voted_by.at(i as usize) {
                weight += desc.get_node_weight(i);

                if weight >= desc.get_cutoff_weight() {
                    return true;
                }
            }
        }

        false
    }

    fn check_block_is_voted_by(&self, src_idx: u32) -> bool {
        *self.voted_by.at(src_idx as usize)
    }

    /*
        Clone object to persistent pool
    */

    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn VoteCandidate> {
        let self_cloned = Self::new(
            self.block.move_to_persistent(cache),
            self.voted_by.move_to_persistent(cache),
            &self.instance_counter,
        );

        Self::create_persistent_object(self_cloned, cache)
    }
}

/*
    Implementation for public VoteCandidateWrapper public traits
*/

impl VoteCandidateWrapper for VoteCandidatePtr {
    fn push(&self, desc: &mut dyn SessionDescription, src_idx: u32) -> VoteCandidatePtr {
        let &self_impl = &get_impl(&**self);

        //if vote from the node exists, do nothing

        if *self_impl.voted_by.at(src_idx as usize) {
            return self.clone();
        }

        //add vote

        let voted_by = self_impl.voted_by.change(desc, src_idx as usize, true);

        VoteCandidateImpl::create(desc, self_impl.block.clone(), voted_by)
    }
}

/*
    Implementation for Merge trait
*/

impl Merge<PoolPtr<dyn VoteCandidate>> for PoolPtr<dyn VoteCandidate> {
    fn merge(&self, right: &Self, desc: &mut dyn SessionDescription) -> Self {
        let left = self;

        assert!(left.get_id() == right.get_id());

        let voted_by = get_impl(&**left)
            .voted_by
            .merge(&get_impl(&**right).voted_by, desc);

        VoteCandidateImpl::create(desc, left.get_block().clone(), voted_by)
    }
}

/*
    Implementation for public HashableObject & PoolObject & MovablePoolObject public traits
*/

impl HashableObject for VoteCandidateImpl {
    fn get_hash(&self) -> HashType {
        self.hash
    }
}

impl PoolObject for VoteCandidateImpl {
    fn set_pool(&mut self, pool: SessionPool) {
        self.pool = pool;
        self.instance_counter.set_pool(pool);
    }

    fn get_pool(&self) -> SessionPool {
        self.pool
    }
}

impl MovablePoolObject<PoolPtr<dyn VoteCandidate>> for PoolPtr<dyn VoteCandidate> {
    fn move_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn VoteCandidate> {
        if SessionPool::Persistent == self.get_pool() {
            return self.clone();
        }

        self.clone_to_persistent(cache)
    }
}

/*
    Comparison (address only because of the cache)
*/

impl std::cmp::PartialEq for dyn VoteCandidate {
    fn eq(&self, other: &Self) -> bool {
        //compare addresses only because each vector is unique in cache system

        (self as *const Self) == (other as *const Self)
    }
}

/*
    Implementation for public CacheObject trait
*/

impl CacheObject<VoteCandidateImpl> for VoteCandidateImpl {
    fn compare(&self, value: &Self) -> bool {
        self.block == value.block && &self.voted_by == &value.voted_by && self.hash == value.hash
    }
}

/*
    Implementation for public Display & Debug
*/

impl fmt::Display for VoteCandidateImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Debug for VoteCandidateImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.pool == SessionPool::Temp {
            write!(f, "~").unwrap();
        }

        f.debug_struct("")
            .field("hash", &format_args!("{:08x}", &self.hash))
            .field("block", &self.block)
            .field("voted_by", &self.voted_by)
            .finish()
    }
}

/*
    Implementation internals of VoteCandidateImpl
*/

/// Function which converts public trait to its implementation
fn get_impl(value: &dyn VoteCandidate) -> &VoteCandidateImpl {
    value
        .get_impl()
        .downcast_ref::<VoteCandidateImpl>()
        .unwrap()
}

impl VoteCandidateImpl {
    /*
        Creation
    */

    fn compute_hash(block: &SentBlockPtr, voted_by: &BoolVectorPtr) -> HashType {
        crate::utils::compute_hash(ton::hashable::BlockVoteCandidate {
            block: block.get_ton_hash(),
            approved: voted_by.get_ton_hash(),
        })
    }

    fn new(
        block: SentBlockPtr,
        voted_by: BoolVectorPtr,
        instance_counter: &CachedInstanceCounter,
    ) -> Self {
        Self {
            pool: SessionPool::Temp,
            hash: Self::compute_hash(&block, &voted_by),
            block: block,
            voted_by: voted_by,
            instance_counter: instance_counter.clone(),
        }
    }

    fn create(
        desc: &mut dyn SessionDescription,
        block: SentBlockPtr,
        voted_by: BoolVectorPtr,
    ) -> VoteCandidatePtr {
        let body = Self::new(block, voted_by, desc.get_vote_candidates_instance_counter());

        Self::create_temp_object(body, desc.get_cache())
    }

    pub(crate) fn create_unvoted(
        desc: &mut dyn SessionDescription,
        block: SentBlockPtr,
    ) -> VoteCandidatePtr {
        let voted_by = vec![false; desc.get_total_nodes() as usize];
        let voted_by = SessionFactory::create_vector(desc, voted_by);

        Self::create(desc, block, voted_by)
    }
}
