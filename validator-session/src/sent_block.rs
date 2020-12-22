pub use super::*;

/*
    Implementation details for SentBlock
*/

#[derive(Clone)]
pub(crate) struct SentBlockImpl {
    pool: SessionPool,                       //pool of the object
    hash: HashType,                          //block hash
    candidate_id: BlockId,                   //candidate identifier
    source_id: u32,                          //source identifier
    root_hash: BlockHash,                    //root block hash
    file_hash: BlockHash,                    //block file hash
    collated_data_file_hash: BlockHash,      //collated data hash
    instance_counter: CachedInstanceCounter, //instance counter
}

/*
    Implementation for public SentBlock public trait
*/

impl SentBlock for SentBlockImpl {
    /*
        General purpose methods & accessors
    */

    fn get_impl(&self) -> &dyn Any {
        self
    }

    fn get_id(&self) -> &BlockId {
        &self.candidate_id
    }

    fn get_source_index(&self) -> u32 {
        self.source_id
    }

    fn get_root_hash(&self) -> &BlockHash {
        &self.root_hash
    }

    fn get_file_hash(&self) -> &BlockHash {
        &self.file_hash
    }

    fn get_collated_data_file_hash(&self) -> &BlockHash {
        &self.collated_data_file_hash
    }

    /*
        Clone object to pool
    */

    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn SentBlock> {
        Self::create_persistent_object(self.clone(), cache)
    }
}

/*
    Implementation for SentBlockWrapper
*/

impl SentBlockWrapper for SentBlockPtr {
    fn get_id(&self) -> &BlockId {
        match self {
            Some(block) => block.get_id(),
            None => &*SKIP_ROUND_CANDIDATE_BLOCKID,
        }
    }
}

/*
    Implementation for Merge trait
*/

impl Merge<Option<PoolPtr<dyn SentBlock>>> for Option<PoolPtr<dyn SentBlock>> {
    fn merge(&self, right: &Self, _desc: &mut dyn SessionDescription) -> Self {
        let left = self;

        if left.is_some() {
            return left.clone();
        }

        if right.is_some() {
            return right.clone();
        }

        None
    }
}

/*
    Implementation for public HashableObject & PoolObject & MovablePoolObject public traits
*/

impl HashableObject for SentBlockImpl {
    fn get_hash(&self) -> HashType {
        self.hash
    }
}

impl PoolObject for SentBlockImpl {
    fn set_pool(&mut self, pool: SessionPool) {
        self.pool = pool;
        self.instance_counter.set_pool(pool);
    }

    fn get_pool(&self) -> SessionPool {
        self.pool
    }
}

impl MovablePoolObject<PoolPtr<dyn SentBlock>> for PoolPtr<dyn SentBlock> {
    fn move_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn SentBlock> {
        if SessionPool::Persistent == self.get_pool() {
            return self.clone();
        }

        self.clone_to_persistent(cache)
    }
}

/*
    Comparison (address only because of the cache)
*/

impl std::cmp::PartialEq for dyn SentBlock {
    fn eq(&self, other: &Self) -> bool {
        //compare addresses only because each vector is unique in cache system

        (self as *const Self) == (other as *const Self)
    }
}

/*
    Implementation for public CacheObject trait
*/

impl CacheObject<SentBlockImpl> for SentBlockImpl {
    fn compare(&self, value: &Self) -> bool {
        self.hash == value.hash
            && self.candidate_id == value.candidate_id
            && self.source_id == value.source_id
            && self.root_hash == value.root_hash
            && self.file_hash == value.file_hash
            && self.collated_data_file_hash == value.collated_data_file_hash
    }
}

/*
    Implementation for public Display & Debug
*/

impl fmt::Display for SentBlockImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SentBlock(hash={:?}, source_id={}, file_hash={:?})",
            &self.hash, self.source_id, &self.file_hash
        )
    }
}

impl fmt::Debug for SentBlockImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.pool == SessionPool::Temp {
            write!(f, "~").unwrap();
        }

        f.debug_struct("SentBlock")
            .field("hash", &format_args!("{:08x}", &self.hash))
            .field("candidate_id", &self.candidate_id)
            .field("source_id", &self.source_id)
            .field("root_hash", &self.root_hash)
            .field("file_hash", &self.file_hash)
            .field("collated_data_file_hash", &self.collated_data_file_hash)
            .finish()
    }
}

/*
    Implementation internals of SentBlockImpl
*/

/// Function which converts public trait to its implementation
#[allow(dead_code)]
fn get_impl(value: &dyn SentBlock) -> &SentBlockImpl {
    value.get_impl().downcast_ref::<SentBlockImpl>().unwrap()
}

impl SentBlockImpl {
    /*
        Hash calculation
    */

    fn compute_hash(
        source_id: u32,
        root_hash: &BlockHash,
        file_hash: &BlockHash,
        collated_data_file_hash: &BlockHash,
        _candidate_id: &BlockId,
    ) -> HashType {
        crate::utils::compute_hash(ton::hashable::SentBlock {
            src: source_id as ton::int,
            root_hash: root_hash.get_ton_hash(),
            file_hash: file_hash.get_ton_hash(),
            collated_data_file_hash: collated_data_file_hash.get_ton_hash(),
        })
    }

    /*
        Object creation
    */

    fn new(
        source_id: u32,
        root_hash: BlockHash,
        file_hash: BlockHash,
        collated_data_file_hash: BlockHash,
        candidate_id: BlockId,
        instance_counter: &CachedInstanceCounter,
    ) -> Self {
        trace!(
          "...sent block {:?} has been created (source_id={}, root_hash={:?}, file_hash={:?}, collated_data_file_hash={:?})",
          candidate_id,
          source_id,
          root_hash,
          file_hash,
          collated_data_file_hash);

        Self {
            pool: SessionPool::Temp,
            source_id: source_id,
            root_hash: root_hash.clone(),
            file_hash: file_hash.clone(),
            collated_data_file_hash: collated_data_file_hash.clone(),
            candidate_id: candidate_id.clone(),
            hash: Self::compute_hash(
                source_id,
                &root_hash.clone(),
                &file_hash.clone(),
                &collated_data_file_hash.clone(),
                &candidate_id.clone(),
            ),
            instance_counter: instance_counter.clone(),
        }
    }

    pub(crate) fn create(
        desc: &mut dyn SessionDescription,
        source_id: u32,
        root_hash: BlockHash,
        file_hash: BlockHash,
        collated_data_file_hash: BlockHash,
    ) -> SentBlockPtr {
        let candidate_id =
            desc.candidate_id(source_id, &root_hash, &file_hash, &collated_data_file_hash);
        let body = Self::new(
            source_id,
            root_hash,
            file_hash,
            collated_data_file_hash,
            candidate_id,
            desc.get_sent_blocks_instance_counter(),
        );

        Some(Self::create_temp_object(body, desc.get_cache()))
    }

    pub(crate) fn create_empty(desc: &mut dyn SessionDescription) -> SentBlockPtr {
        Self::create(
            desc,
            0,
            BlockHash::default(),
            BlockHash::default(),
            BlockHash::default(),
        )
    }
}
