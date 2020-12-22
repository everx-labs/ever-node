pub use super::*;
use std::cmp;
use std::fmt;
use std::str::FromStr;
use utils::*;

/*
    Implementation details for ReceivedBlock
*/

pub(crate) struct ReceivedBlockImpl {
    self_cell: Weak<RefCell<ReceivedBlockImpl>>, //back reference to itself to be used in recursive calls
    state: ReceivedBlockState,                   //current block state
    height: BlockHeight,                         //height of the block
    incarnation: SessionId,                      //session ID for this block
    hash: BlockHash,                             //hash of the block
    data_hash: BlockHash,                        //hash of a payload
    source_id: usize, //receiver source which has generated & signed this block
    fork_id: usize,   //fork ID for this block inside current node
    prev: Option<ReceivedBlockPtr>, //previous block in a fork chain
    next: Option<Weak<RefCell<dyn ReceivedBlock>>>, //next block in a fork chain
    signature: BlockSignature, //signature of the block
    payload: BlockPayload, //block's payload (for validator session)
    block_deps: Vec<ReceivedBlockPtr>, //dependencies which have been used for this block creation
    rev_deps: Vec<Weak<RefCell<dyn ReceivedBlock>>>, //reverse dependecies (dependent blocks)
    forks_dep_heights: Vec<BlockHeight>, //heights of each fork which is used in prev & dependency blocks for this block
    pending_deps: usize, //number of pending dependencies for this block to be fully received
    in_db: bool,         //flag which showes that this block has been written to DB
    is_custom: bool, //flag which showes that this block should be processed by validator session
    _instance_counter: InstanceCounter, //received blocks instance counter
}

/// Functions which converts public ReceivedBlock to its implementation
fn get_impl(block: &dyn ReceivedBlock) -> &ReceivedBlockImpl {
    block
        .get_impl()
        .downcast_ref::<ReceivedBlockImpl>()
        .unwrap()
}

fn get_mut_impl(block: &mut dyn ReceivedBlock) -> &mut ReceivedBlockImpl {
    block
        .get_mut_impl()
        .downcast_mut::<ReceivedBlockImpl>()
        .unwrap()
}

/*
    Implementation for public ReceivedBlock trait
*/

impl ReceivedBlock for ReceivedBlockImpl {
    /*
        General purpose methods & accessors
    */

    /// Back reference to itself for recursive methods
    fn get_self(&self) -> ReceivedBlockPtr {
        self.self_cell.upgrade().unwrap()
    }

    fn get_impl(&self) -> &dyn Any {
        self
    }
    fn get_mut_impl(&mut self) -> &mut dyn Any {
        self
    }

    fn get_state(&self) -> ReceivedBlockState {
        self.state
    }

    fn is_initialized(&self) -> bool {
        match self.state {
            ReceivedBlockState::Initialized | ReceivedBlockState::Delivered => true,
            _ => false,
        }
    }

    fn in_db(&self) -> bool {
        self.in_db
    }

    fn is_delivered(&self) -> bool {
        self.state == ReceivedBlockState::Delivered
    }

    fn is_custom(&self) -> bool {
        self.is_custom
    }

    fn get_height(&self) -> BlockHeight {
        self.height
    }

    fn get_source_id(&self) -> usize {
        self.source_id
    }

    fn get_fork_id(&self) -> usize {
        self.fork_id
    }

    fn get_hash(&self) -> &BlockHash {
        &self.hash
    }

    fn get_signature(&self) -> &BlockSignature {
        &self.signature
    }

    fn get_payload(&self) -> &BlockPayload {
        &self.payload
    }

    /*
        Dependencies management
    */

    fn get_prev(&self) -> Option<ReceivedBlockPtr> {
        match &self.prev {
            None => None,
            Some(prev) => Some(Rc::clone(&prev)),
        }
    }

    fn get_prev_hash(&self) -> Option<BlockHash> {
        match self.get_prev() {
            None => None,
            Some(prev) => Some(prev.borrow().get_hash().clone()),
        }
    }

    fn get_next(&self) -> Option<ReceivedBlockPtr> {
        match &self.next {
            None => None,
            Some(next) => next.upgrade(),
        }
    }

    fn get_forks_dep_heights(&self) -> &Vec<BlockHeight> {
        &self.forks_dep_heights
    }

    fn get_dep_hashes(&self) -> Vec<BlockHash> {
        let mut hashes = Vec::with_capacity(self.block_deps.len());

        for it in &self.block_deps {
            hashes.push(it.borrow().get_hash().clone());
        }

        hashes
    }

    fn get_pending_deps(&self, max_deps_count: usize, dep_hashes: &mut Vec<BlockHash>) {
        if self.get_height() == 0
            || self.get_state() == ReceivedBlockState::Ill
            || self.is_delivered()
            || dep_hashes.len() == max_deps_count as usize
        {
            return;
        }

        if !self.is_initialized() {
            dep_hashes.push(self.get_hash().clone());
            return;
        }

        if let Some(prev) = self.get_prev() {
            prev.borrow().get_pending_deps(max_deps_count, dep_hashes);
        }

        for it in &self.block_deps {
            it.borrow().get_pending_deps(max_deps_count, dep_hashes);
        }
    }

    /*
        Block fork management
    */

    fn set_ill(&mut self, receiver: &mut dyn Receiver) {
        if self.state == ReceivedBlockState::Ill {
            return;
        }

        warn!("Ill block detected: {}", self.get_hash().to_hex_string());

        receiver
            .get_source(self.source_id)
            .borrow_mut()
            .mark_as_blamed(receiver);

        self.state = ReceivedBlockState::Ill;

        for block in &self.rev_deps {
            block.upgrade().unwrap().borrow_mut().set_ill(receiver);
        }
    }

    /*
        Block initialization
    */

    fn initialize(
        &mut self,
        block: &ton::Block,
        payload: BlockPayload,
        receiver: &mut dyn Receiver,
    ) -> Result<()> {
        if self.state != ReceivedBlockState::Null {
            return Ok(());
        }

        if log_enabled!(log::Level::Debug) {
            trace!(
                "...initialize block {:?} with payload of {} bytes",
                self.hash,
                payload.len()
            );
        }

        assert!(!payload.is_empty());
        self.payload = payload;

        let prev = receiver.create_block(&block.data.prev);
        self.prev = Some(prev.clone());

        for dep in &block.data.deps.0 {
            let dep_block = receiver.create_block(dep).clone();
            self.block_deps.push(dep_block);
        }

        self.signature = block.signature.clone();
        self.state = ReceivedBlockState::Initialized;

        if log_enabled!(log::Level::Debug) {
            trace!(
                "...check prev block and dependencies for {:?} to be initialized",
                self.hash
            );
        }

        if prev.borrow().get_state() == ReceivedBlockState::Ill {
            let message = format!(
                "...prev block {:?} for block {:?} is ill",
                self.hash,
                prev.borrow().get_hash()
            );

            warn!("{}", message);

            self.set_ill(receiver);

            return Err(err_msg(message));
        }

        for dep in &self.block_deps {
            if dep.borrow().get_state() == ReceivedBlockState::Ill {
                let message = format!(
                    "...dependency block {:?} for block {:?} is ill",
                    self.hash,
                    dep.borrow().get_hash()
                );

                warn!("{}", message);

                self.set_ill(receiver);

                return Err(err_msg(message));
            }
        }

        if log_enabled!(log::Level::Debug) {
            trace!("...compute forks dependency heights for {:?}", self.hash);
        }

        let mut pending_deps: usize = 0;

        if !prev.borrow().is_delivered() {
            pending_deps += 1;
        } else {
            get_mut_impl(self).update_forks_dependency_heights(get_impl(&*prev.borrow()));
        }
        if !prev.borrow().is_delivered() {
            get_mut_impl(&mut *prev.borrow_mut()).add_rev_dep(self);
        }

        let block_deps_for_iteration = self.block_deps.clone();

        for dep in &block_deps_for_iteration
        //TODO: problem with mutability of self, should be fixed later
        {
            if !dep.borrow().is_delivered() {
                pending_deps += 1;
            } else {
                get_mut_impl(self).update_forks_dependency_heights(get_impl(&*dep.borrow()));
            }
            if !dep.borrow().is_delivered() {
                get_mut_impl(&mut *dep.borrow_mut()).add_rev_dep(self);
            }
        }

        self.pending_deps = pending_deps;

        if log_enabled!(log::Level::Debug) {
            trace!(
                "...pending {} dependencies for {:?}",
                self.pending_deps,
                self.hash
            );
        }

        if self.pending_deps == 0 && self.in_db {
            get_mut_impl(self).schedule(receiver);
        }

        if log_enabled!(log::Level::Debug) {
            trace!(
                "...notify source #{} about new received block {:?}",
                self.source_id,
                self.hash
            );
        }

        receiver
            .get_source(self.source_id)
            .borrow_mut()
            .block_received(self.height);

        Ok(())
    }

    /*
        Block delivery management
    */

    fn process(&mut self, receiver: &mut dyn Receiver) {
        match self.get_state() {
            ReceivedBlockState::Null | ReceivedBlockState::Delivered | ReceivedBlockState::Ill => {
                return
            }
            _ => (),
        }

        assert!(self.get_state() == ReceivedBlockState::Initialized);
        assert!(self.pending_deps == 0);
        assert!(self.in_db);

        self.initialize_fork(receiver);
        self.pre_deliver(receiver);
        self.deliver(receiver);
    }

    fn written(&mut self, receiver: &mut dyn Receiver) {
        if self.in_db {
            return;
        }

        self.in_db = true;

        if log_enabled!(log::Level::Debug) {
            trace!(
                "...block {:?} has been written to DB, pending deps is {}",
                &self.hash,
                self.pending_deps
            );
        }

        if self.pending_deps == 0 {
            self.schedule(receiver);
        }
    }

    /*
        TL export
    */

    fn export_tl(&self) -> ton::Block {
        assert!(self.is_initialized());
        assert!(self.height > 0);

        let mut deps = Vec::new();

        for dep in &self.block_deps {
            deps.push(dep.borrow().export_tl_dep());
        }

        type TonBare = ::ton_api::ton::Bare;
        type TonDepVector = ::ton_api::ton::vector<TonBare, ton::BlockDep>;

        let block_data = ton::BlockData {
            prev: self.get_prev().unwrap().borrow().export_tl_dep(),
            deps: TonDepVector::from(deps),
        };

        let block = ton::Block {
            incarnation: self.incarnation.clone().into(),
            src: self.source_id as i32,
            height: self.height as i32,
            data: block_data,
            signature: self.signature.clone(),
        };

        block
    }

    fn export_tl_dep(&self) -> ton::BlockDep {
        let dep = ton::BlockDep {
            src: self.source_id as i32,
            data_hash: self.data_hash.clone().into(),
            height: self.height as i32,
            signature: self.signature.clone(),
        };

        dep
    }

    /*
        Debug methods
    */

    fn to_string(&self) -> String {
        let mut prev_hash: String = "NULL".to_owned();

        if let Some(prev_block) = &self.prev {
            prev_hash = format!("{}", prev_block.borrow().get_hash().to_hex_string());
        }

        let mut deps_string: String = "".to_owned();

        for dep_iter in &self.block_deps {
            let dep = dep_iter.borrow();

            if deps_string != "" {
                deps_string += ", "
            }

            deps_string += &format!(
                "{}@{}/{}",
                dep.get_height(),
                dep.get_source_id(),
                dep.get_hash().to_hex_string()
            );

            if !dep.is_delivered() {
                deps_string += "?";
            }
        }

        format!("ReceivedBlock(hash={}, state={:?}, source_id={}, height={}, fork={}, prev={}, deps=[{}], signature={})", self.hash.to_hex_string(), self.state, self.source_id, self.height, self.fork_id,
      prev_hash, deps_string, bytes_to_string(&self.signature))
    }
}

impl fmt::Display for ReceivedBlockImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ReceivedBlock(hash={:?}, source_id={}, height={})",
            &self.hash, self.source_id, self.height
        )
    }
}

/*
    Implementation of ReceiverBlockImpl
*/

impl ReceivedBlockImpl {
    /*
        Dependencies management
    */

    fn update_forks_dependency_heights(&mut self, block: &ReceivedBlockImpl) {
        let actual_forks_dep_heights = &block.forks_dep_heights;

        if actual_forks_dep_heights.len() > self.forks_dep_heights.len() {
            self.forks_dep_heights
                .resize(actual_forks_dep_heights.len(), 0);
        }

        for i in 0..actual_forks_dep_heights.len() {
            if self.forks_dep_heights[i] < actual_forks_dep_heights[i] {
                self.forks_dep_heights[i] = actual_forks_dep_heights[i];
            }
        }
    }

    fn add_rev_dep(&mut self, block: &ReceivedBlockImpl) {
        self.rev_deps.push(block.self_cell.clone());
    }

    /*
        Delivery management
    */

    fn schedule(&mut self, receiver: &mut dyn Receiver) {
        if log_enabled!(log::Level::Debug) {
            trace!("...schedule block {:?} for delivering", &self.hash);
        }

        receiver.run_block(self.get_self());
    }

    fn initialize_fork(&mut self, receiver: &mut dyn Receiver) {
        assert!(self.state == ReceivedBlockState::Initialized);
        assert!(self.fork_id == 0);

        if log_enabled!(log::Level::Debug) {
            trace!(
                "...initialize fork for block {:?} at height {} from source {}",
                &self.hash,
                self.height,
                self.source_id
            );
        }

        let source = receiver.get_source(self.source_id);

        if self.height == 1 {
            self.fork_id = source.borrow_mut().add_fork(receiver);
        } else {
            assert!(!self.prev.is_none());

            let prev = self.prev.as_ref().unwrap();

            if !prev.borrow().get_next().is_none() {
                self.fork_id = source.borrow_mut().add_fork(receiver);
            } else {
                get_mut_impl(&mut *prev.borrow_mut()).next = Some(self.self_cell.clone());
                self.fork_id = prev.borrow().get_fork_id();
            }
        }

        if self.forks_dep_heights.len() < self.fork_id + 1 {
            self.forks_dep_heights.resize(self.fork_id + 1, 0);
        }

        assert!(self.forks_dep_heights[self.fork_id] < self.height);

        self.forks_dep_heights[self.fork_id] = self.height;
    }

    fn pre_deliver(&mut self, receiver: &mut dyn Receiver) {
        if self.state == ReceivedBlockState::Ill {
            return;
        }

        if log_enabled!(log::Level::Debug) {
            trace!(
                "...check block {:?} dependencies before delivering (pre-deliver)",
                &self.hash
            );
        }

        assert!(self.state == ReceivedBlockState::Initialized);
        assert!(self.pending_deps == 0);
        assert!(self.in_db);

        let source = receiver.get_source(self.source_id);

        if let Some(prev) = self.prev.clone() {
            let mut prev_borrowed = prev.borrow_mut();
            let prev_forks_dep_heights = &get_mut_impl(&mut *prev_borrowed).forks_dep_heights;

            let block_deps_for_iteration = self.block_deps.clone();

            for dep_it in &block_deps_for_iteration
            //TODO: problem with mutability of self, should be fixed later
            {
                let dep = dep_it.borrow();
                let dep_source = receiver.get_source(dep.get_source_id());
                let dep_source = dep_source.borrow_mut();
                let dep_source_fork_ids = &dep_source.get_forks();

                if dep.get_fork_id() < prev_forks_dep_heights.len()
                    && dep.get_height() <= prev_forks_dep_heights[dep.get_fork_id()]
                {
                    warn!(concat!("Block {:?} has direct dependency {:?} with fork_id={} and height={} from source #{} and prev block {:?} ",
            "has newer indirect dependency with the same fork and height={}"),
            dep.get_hash(), dep.get_hash(), dep.get_fork_id(), dep.get_height(), dep_source.get_id(), prev.borrow().get_hash(),
            prev_forks_dep_heights[dep.get_fork_id()]);

                    self.set_ill(receiver);

                    return;
                }

                if !dep_source.is_blamed() {
                    continue;
                }

                for &dep_source_fork_id in *dep_source_fork_ids {
                    if dep_source_fork_id == dep.get_fork_id()
                        || prev_forks_dep_heights.len() <= dep_source_fork_id
                        || prev_forks_dep_heights[dep_source_fork_id] == 0
                    {
                        continue;
                    }

                    warn!(concat!("Block {:?} has direct dependency {:?} with fork_id={} and height={} from source #{} and prev block ",
            "{:?} has indirect dependency to another fork fork_id={} and height={} of the same source"),
            dep.get_hash(), dep.get_hash(), dep.get_fork_id(), dep.get_height(), dep_source.get_id(), prev.borrow().get_hash(),
            dep_source_fork_id, prev_forks_dep_heights[dep_source_fork_id]);

                    source
                        .borrow_mut()
                        .blame(self.fork_id, self.height, receiver);

                    self.set_ill(receiver);

                    return;
                }

                let dep_source_blamed_heights = dep_source.get_blamed_heights();
                let iterations_count = cmp::min(
                    dep_source_blamed_heights.len(),
                    prev_forks_dep_heights.len(),
                );

                for fork_id in 0..iterations_count {
                    if dep_source_blamed_heights[fork_id] == 0
                        || prev_forks_dep_heights[fork_id] < dep_source_blamed_heights[fork_id]
                    {
                        continue;
                    }

                    warn!(concat!("Block {:?} has direct dependency {:?} with fork_id={} and height={} from source #{} and prev block ",
            "{:?} has indirect dependency to fork fork_id={} and height={} which is known to blame this source"),
            dep.get_hash(), dep.get_hash(), dep.get_fork_id(), dep.get_height(), dep_source.get_id(), prev.borrow().get_hash(),
            fork_id, dep_source_blamed_heights[fork_id]);

                    source
                        .borrow_mut()
                        .blame(self.fork_id, self.height, receiver);

                    self.set_ill(receiver);

                    return;
                }
            }
        }

        use ton_api::ton::catchain::block::inner::Data;

        match ton_api::Deserializer::new(&mut self.payload.as_ref()).read_boxed::<Data>() {
            Ok(message) => match message {
                Data::Catchain_Block_Data_Fork(message) => {
                    let left = message.left.only();
                    let right = message.right.only();

                    if let Err(err) = receiver.validate_block_dependency(&left) {
                        warn!("Incorrect fork blame: left is ivalid: {:?}", err);
                        self.set_ill(receiver);
                        return;
                    }
                    if let Err(err) = receiver.validate_block_dependency(&right) {
                        warn!("Incorrect fork blame: right is ivalid: {:?}", err);
                        self.set_ill(receiver);
                        return;
                    }

                    if left.height != right.height
                        || left.src != right.src
                        || left.data_hash != right.data_hash
                    {
                        warn!("Incorrect fork blame: not a fork");
                        self.set_ill(receiver);
                        return;
                    }

                    let source = receiver.get_source(left.src as usize);
                    let serialized_fork_proof = serialize_tl_bare_object!(&left, &right);

                    source.borrow_mut().set_fork_proof(serialized_fork_proof);
                    source.borrow_mut().mark_as_blamed(receiver);
                }
                Data::Catchain_Block_Data_Nop | Data::Catchain_Block_Data_BadBlock(_) => { /*do nothing*/
                }
                _ => self.is_custom = true,
            },
            Err(_err) => self.is_custom = true,
        }

        /*
          auto X = fetch_tl_object<ton_api::catchain_block_inner_Data>(payload_.as_slice(), true);
          if (X.is_error()) {
            is_custom_ = true;
          } else {
            ton_api::downcast_call(*X.move_as_ok().get(), [Self = this](auto &obj) { Self->pre_deliver(obj); });
          }

        */
    }

    fn deliver(&mut self, receiver: &mut dyn Receiver) {
        if self.state == ReceivedBlockState::Ill {
            return;
        }

        if log_enabled!(log::Level::Debug) {
            trace!("...prepare block {:?} for delivering", &self.hash);
        }

        assert!(self.state == ReceivedBlockState::Initialized);
        assert!(self.pending_deps == 0);
        assert!(self.in_db);

        receiver.deliver_block(self);

        self.state = ReceivedBlockState::Delivered;

        if log_enabled!(log::Level::Debug) {
            trace!("...block {:?} has been delivered", self.get_hash());
        }

        for rev_dep_it in &self.rev_deps {
            if let Some(rev_dep) = rev_dep_it.upgrade() {
                get_mut_impl(&mut *rev_dep.borrow_mut()).dependency_delivered(self, receiver);
            }
        }

        self.rev_deps.clear();

        if log_enabled!(log::Level::Debug) {
            trace!(
                "...notify source #{} about new block {:?} which is ready to be delivered",
                self.get_source_id(),
                self.get_hash()
            );
        }

        receiver
            .get_source(self.source_id)
            .borrow_mut()
            .block_delivered(self.height);
    }

    fn dependency_delivered(&mut self, block: &ReceivedBlockImpl, receiver: &mut dyn Receiver) {
        if self.state == ReceivedBlockState::Ill {
            return;
        }

        assert!(block.get_state() != ReceivedBlockState::Ill);

        self.update_forks_dependency_heights(block);

        self.pending_deps -= 1;

        if self.pending_deps == 0 && self.in_db {
            self.schedule(receiver);
        }
    }

    /*
        Block pre validation
    */

    pub(crate) fn pre_validate_block_dependency(
        receiver: &dyn Receiver,
        block: &ton::BlockDep,
    ) -> Result<()> {
        if block.height < 0 {
            bail!("Invalid height {}", block.height);
        }

        if block.height > 0 {
            if block.src < 0 || block.src as usize >= receiver.get_sources_count() {
                bail!("Invalid source {}", block.src);
            }
        } else {
            if block.src < 0 || block.src as usize != receiver.get_sources_count() {
                bail!("Invalid source (first block) {}", block.src);
            }

            if UInt256::from(block.data_hash.0) != *receiver.get_incarnation()
                || block.signature.len() != 0
            {
                bail!("Invalid first block");
            }
        }

        Ok(())
    }

    pub(crate) fn pre_validate_block(
        receiver: &dyn Receiver,
        block: &ton::Block,
        payload: &BlockPayload,
    ) -> Result<()> {
        if UInt256::from(block.incarnation.0) != *receiver.get_incarnation() {
            bail!("Invalid session ID".to_string());
        }

        if block.height <= 0 {
            bail!("Invalid height {}", block.height);
        }

        if block.src < 0 || block.src as usize >= receiver.get_sources_count() {
            bail!("Invalid source {}", block.src);
        }

        if block.data.prev.src < 0 {
            bail!("Invalid prev block source {}", block.data.prev.src);
        }

        if block.data.deps.len() > receiver.get_options().max_deps as usize {
            bail!("Too many deps");
        }

        let prev_src = block.data.prev.src as usize;

        if block.height > 1 {
            if prev_src != block.src as usize {
                bail!("Invalid prev block source {}", block.data.prev.src);
            }
        } else {
            if prev_src != receiver.get_sources_count() {
                bail!("Invalid prev(first) block source {}", block.data.prev.src);
            }
        }

        if block.data.prev.height + 1 != block.height {
            bail!(
                "Invalid prev block height {} (our {})",
                block.data.prev.height,
                block.height
            );
        }

        use std::collections::HashSet;

        let mut used: HashSet<i32> = HashSet::new();

        used.insert(block.src);

        for dep in &block.data.deps.0 {
            if used.contains(&dep.src) {
                bail!("Two deps from the same source");
            }

            used.insert(dep.src);
        }

        (receiver.validate_block_dependency(&block.data.prev))?;

        for dep in &block.data.deps.0 {
            (receiver.validate_block_dependency(dep))?;
        }

        if payload.len() == 0 {
            bail!("Empty payload");
        }

        Ok(())
    }

    /*
        Block initialization & creation
    */

    fn new(instance_counter: &InstanceCounter) -> Self {
        ReceivedBlockImpl {
            self_cell: Weak::new(),
            state: ReceivedBlockState::Null,
            height: 0,
            incarnation: BlockHash::default(),
            data_hash: UInt256::from([0; 32]),
            hash: UInt256::from([0; 32]),
            source_id: 0,
            fork_id: 0,
            prev: None,
            next: None,
            signature: BlockSignature::default(),
            payload: BlockPayload::default(),
            block_deps: Vec::new(),
            rev_deps: Vec::new(),
            forks_dep_heights: Vec::new(),
            pending_deps: 0,
            in_db: false,
            is_custom: false,
            _instance_counter: instance_counter.clone(),
        }
    }

    fn wrap(block: &mut Rc<RefCell<ReceivedBlockImpl>>) -> Rc<RefCell<ReceivedBlockImpl>> {
        block.borrow_mut().self_cell = Rc::downgrade(block);
        block.clone()
    }

    pub fn create_from_string_dump(s: &String, receiver: &dyn Receiver) -> ReceivedBlockPtr {
        let mut body: ReceivedBlockImpl =
            ReceivedBlockImpl::new(receiver.get_received_blocks_instance_counter());

        for line in s.lines() {
            let v: Vec<&str> = line.split('=').collect();
            if v.len() == 2 {
                let id = v.get(0).unwrap().trim();
                let value = v.get(1).unwrap().trim();

                if id == "height" {
                    body.height = FromStr::from_str(value).unwrap();
                }
                if id == "fork_id" {
                    body.fork_id = FromStr::from_str(value).unwrap();
                }
                if id == "source_id" {
                    body.source_id = FromStr::from_str(value).unwrap();
                }
                if id == "hash" {
                    body.hash = parse_hex_as_int256(value);
                }
                if id == "data_hash" {
                    body.data_hash = parse_hex_as_int256(value);
                }
                if id == "signature" {
                    body.signature = parse_hex_as_bytes(value);
                }
                if id == "payload" {
                    body.payload = parse_hex_as_bytes(value);
                }
                if id == "deps" {
                    let dep_hash: BlockHash = parse_hex_as_int256(value);
                    let dep = match receiver.get_block_by_hash(&dep_hash) {
                        None => panic!("Block with hash {} not found", value),
                        Some(x) => x,
                    };
                    body.block_deps.push(dep);
                }
            }
        }

        ReceivedBlockImpl::wrap(&mut Rc::new(RefCell::new(body)))
    }

    pub(crate) fn create_root(
        source_id: usize,
        incarnation: &SessionId,
        instance_counter: &InstanceCounter,
    ) -> ReceivedBlockPtr {
        let mut body: ReceivedBlockImpl = ReceivedBlockImpl::new(instance_counter);
        let block_id = get_root_block_id(incarnation);

        body.source_id = source_id;
        body.data_hash = incarnation.clone();
        body.state = ReceivedBlockState::Delivered;
        body.hash = get_block_id_hash(&block_id);
        body.incarnation = incarnation.clone();

        if log_enabled!(log::Level::Debug) {
            trace!("...create root block: hash={:?}, source_id={}, height={}, data_hash={:?}, signature={:?}", body.hash, body.source_id, body.height, body.data_hash, body.signature);
        }

        ReceivedBlockImpl::wrap(&mut Rc::new(RefCell::new(body)))
    }

    pub(crate) fn create_with_payload(
        block: &ton::Block,
        payload: BlockPayload,
        receiver: &mut dyn Receiver,
    ) -> Result<ReceivedBlockPtr> {
        let mut body: ReceivedBlockImpl =
            ReceivedBlockImpl::new(receiver.get_received_blocks_instance_counter());
        let block_id = get_block_id(
            &receiver.get_incarnation(),
            receiver.get_source_public_key_hash(block.src as usize),
            block.height,
            &payload,
        );

        body.data_hash = get_hash(&payload);
        body.signature = block.signature.clone();
        body.hash = get_block_id_hash(&block_id);
        body.source_id = block.src as usize;
        body.height = block.height as BlockHeight;
        body.incarnation = receiver.get_incarnation().clone();

        if log_enabled!(log::Level::Debug) {
            trace!("...create new block with payload: hash={:?}, source_id={}, height={}, data_hash={:?}, signature={:?}", body.hash, body.source_id, body.height, body.data_hash, body.signature);
        }

        let new_block = ReceivedBlockImpl::wrap(&mut Rc::new(RefCell::new(body)));

        let source = receiver.get_source(block.src as usize);

        source
            .borrow_mut()
            .process_new_block(new_block.clone(), receiver);

        new_block
            .borrow_mut()
            .initialize(block, payload, receiver)?;

        Ok(new_block)
    }

    pub(crate) fn create(block: &ton::BlockDep, receiver: &mut dyn Receiver) -> ReceivedBlockPtr {
        let mut body: ReceivedBlockImpl =
            ReceivedBlockImpl::new(receiver.get_received_blocks_instance_counter());

        body.data_hash = UInt256::from(block.data_hash.0);
        body.signature = block.signature.clone();
        body.hash = get_block_dependency_hash(block, receiver);
        body.source_id = block.src as usize;
        body.height = block.height as BlockHeight;
        body.incarnation = receiver.get_incarnation().clone();

        if log_enabled!(log::Level::Debug) {
            trace!("...create new block dependency: hash={:?}, source_id={}, height={}, data_hash={:?}, signature={:?}", body.hash, body.source_id, body.height, body.data_hash, body.signature);
        }

        let new_block = ReceivedBlockImpl::wrap(&mut Rc::new(RefCell::new(body)));

        let source = receiver.get_source(block.src as usize);

        source
            .borrow_mut()
            .process_new_block(new_block.clone(), receiver);

        new_block
    }
}
