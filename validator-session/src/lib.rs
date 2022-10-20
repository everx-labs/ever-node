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

#[macro_use]
extern crate lazy_static;

extern crate catchain;
extern crate metrics_runtime;
extern crate rand;
extern crate sha2;
extern crate ton_api;
extern crate ton_types;

#[macro_use]
extern crate log;

#[macro_use]
extern crate failure;

//const TELEGRAM_NODE_COMPATIBILITY_HASHES_BUG: bool = false; //compatibility with Telegram Node: bug in hashes computation for attempt and round
const TELEGRAM_NODE_COMPATIBILITY_HASHES_BUG: bool = true; //compatibility with Telegram Node: bug in hashes computation for attempt and round

use std::any::Any;
use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Weak;
mod block_candidate;
mod cache;
mod old_round;
mod round;
mod round_attempt;
mod sent_block;
mod session;
mod session_description;
mod session_processor;
mod session_state;
pub mod slashing;
mod task_queue;
pub mod utils;
mod vector;
mod vector_bool;
mod vote_candidate;

pub use cache::*;
pub use catchain::ActivityNodePtr;
use catchain::CatchainPtr;
pub use catchain::CatchainReplayListener;
use task_queue::CallbackTaskQueuePtr;
use task_queue::CompletionHandlerProcessor;
use task_queue::TaskQueuePtr;

pub mod profiling {
    pub use catchain::profiling::*;
}

pub mod ton {
    pub use ton_api::ton::int;
    pub use ton_api::ton::rpc::validator_session::*;
    pub use ton_api::ton::validator_session::round::validator_session::*;
    pub use ton_api::ton::validator_session::*;

    pub mod blockid {
        pub use ton_api::ton::ton::blockid::*;
    }

    pub mod hashable {
        pub use ton_api::ton::hashable::hashable::*;
    }

    pub type Message = ::ton_api::ton::validator_session::round::Message;

    pub mod message {
        pub use ton_api::ton::validator_session::round::validator_session::message::message::*;
    }
}

/// Result for operations
pub type Result<T> = catchain::Result<T>;

/// Hash of the object
pub type HashType = u32;

/// Hash of the block
pub type BlockHash = ::catchain::BlockHash;

/// Signature of the block
pub type BlockSignature = ::catchain::BlockSignature;

/// Raw buffer
pub type RawBuffer = ::catchain::RawBuffer;

/// Block payload
pub type BlockPayloadPtr = ::catchain::BlockPayloadPtr;

/// Catchain node
pub type CatchainNode = ::catchain::CatchainNode;

/// Catchain session ID
pub type SessionId = ::catchain::SessionId;

/// Pointer to overlay API for the Catchain
pub type CatchainOverlayPtr = ::catchain::CatchainOverlayPtr;

/// Private key
pub type PrivateKey = ::catchain::PrivateKey;

/// Public key
pub type PublicKey = ::catchain::PublicKey;

/// Public key hash
pub type PublicKeyHash = ::catchain::PublicKeyHash;

/// Block ID
pub type BlockId = BlockHash;

lazy_static! {
  /// Block candidate identifier for skip round (optional case to identify candidate as empty)
  pub static ref SKIP_ROUND_CANDIDATE_BLOCKID : BlockId = ton_types::UInt256::default();

  /// Default block ID for internal use
  pub static ref DEFAULT_BLOCKID : BlockId = ton_types::UInt256::default();
}

/// Overlay manager
pub type CatchainOverlayManagerPtr = catchain::CatchainOverlayManagerPtr;

/// Log replay options
pub type LogReplayOptions = catchain::LogReplayOptions;

/// Log replay listener pointer
pub type SessionReplayListenerPtr = catchain::CatchainReplayListenerPtr;

/// Pool type
#[derive(Clone, Copy, PartialEq)]
pub enum SessionPool {
    /// Persistent storage
    Persistent,

    /// Temporary pool
    Temp,
}

/// Validator session node description
#[derive(Clone, Debug)]
pub struct SessionNode {
    /// ADNL node short ID
    pub adnl_id: PublicKeyHash,

    /// Node public key
    pub public_key: PublicKey,

    /// Weight of the validator
    pub weight: ValidatorWeight,
}

/// Trait to obtain hash of the object
pub trait HashableObject {
    /// Get object hash
    fn get_hash(&self) -> HashType;

    /// Get object hash in TON API format
    fn get_ton_hash(&self) -> ton::int {
        self.get_hash() as ton::int
    }
}

/// Pool control for the object
pub trait PoolObject {
    /// Set pool of the object
    fn set_pool(&mut self, pool: SessionPool);

    /// Get pool of the object
    fn get_pool(&self) -> SessionPool;
}

/// Movable pool object
pub trait MovablePoolObject<T> {
    /// Move object to pool
    fn move_to_persistent(&self, cache: &mut dyn SessionCache) -> T;
}

/// Pool pointer
pub type PoolPtr<T> = Rc<T>;

/// Pointer to SentBlock
pub type SentBlockPtr = Option<PoolPtr<dyn SentBlock>>;

/// Pointer to BlockCandidate
pub type BlockCandidatePtr = PoolPtr<dyn BlockCandidate>;

/// Pointer to BlockCandidateSignature
pub type BlockCandidateSignaturePtr = Option<PoolPtr<dyn BlockCandidateSignature>>;

/// Pointer to BoolVector
pub type BoolVectorPtr = PoolPtr<dyn BoolVector>;

/// Vector of block candidate signatures
pub type BlockCandidateSignatureVector = dyn Vector<BlockCandidateSignaturePtr>;

/// Pointer to BlockCandidateSignatureVector
pub type BlockCandidateSignatureVectorPtr = PoolPtr<BlockCandidateSignatureVector>;

/// Pointer to VoteCandidate
pub type VoteCandidatePtr = PoolPtr<dyn VoteCandidate>;

/// Vector of vote candidates
pub type VoteCandidateVector =
    dyn SortedVector<PoolPtr<dyn VoteCandidate>, VoteCandidateComparator>;

/// Pointer to VoteCandidateVector
pub type VoteCandidateVectorPtr = Option<PoolPtr<VoteCandidateVector>>;

/// Pointer to RoundAttemptState
pub type RoundAttemptStatePtr = PoolPtr<dyn RoundAttemptState>;

/// Pointer to RoundState
pub type RoundStatePtr = PoolPtr<dyn RoundState>;

/// Pointer to OldRoundState
pub type OldRoundStatePtr = PoolPtr<dyn OldRoundState>;

/// Pointer to SessionState
pub type SessionStatePtr = PoolPtr<dyn SessionState>;

/// Pointer to SessionProcessor
pub type SessionProcessorPtr = Rc<RefCell<dyn SessionProcessor>>;

/// Pointer to Session
pub type SessionPtr = Arc<dyn Session + Send>;

/// Pointer to SessionListener
pub type SessionListenerPtr = Weak<dyn SessionListener + Send + Sync>;

/// Validator's weight
pub type ValidatorWeight = catchain::ValidatorWeight;

/// Slashing validator statistics
pub type SlashingValidatorStat = slashing::ValidatorStat;

/// Slashing aggregated validator statistics
pub type SlashingAggregatedValidatorStat = slashing::AggregatedValidatorStat;

/// Slashed node
pub type SlashedNode = slashing::SlashedNode;

/// Validator session options
#[derive(Clone, Copy, Debug)]
pub struct SessionOptions {
    /// Catchain processing timeout
    pub catchain_idle_timeout: std::time::Duration,

    /// Maximum number of dependencies to merge
    pub catchain_max_deps: u32,

    /// Use Catchain in receive only mode (for debugging and log replay)
    pub catchain_skip_processed_blocks: bool,

    /// Number of block candidates per round
    pub round_candidates: u32,

    /// Delay before proposing new candidate
    pub next_candidate_delay: std::time::Duration,

    /// Duration of one round attempt
    pub round_attempt_duration: std::time::Duration,

    /// Maximum number of attempts per round
    pub max_round_attempts: u32,

    /// Maximum block size
    pub max_block_size: u32,

    /// Maximum size of collated data
    pub max_collated_data_size: u32,

    /// Allow new catchain IDs
    pub new_catchain_ids: bool,
}

/// Merge wrapper
pub trait Merge<Ptr> {
    /// Merge two state objects in one
    fn merge(&self, right: &Ptr, desc: &mut dyn SessionDescription) -> Ptr;
}

/// Vector merge wrapper
pub trait VectorMerge<T: std::cmp::PartialEq, Ptr: Clone> {
    /// Merge two state objects in one with default merger
    fn merge(&self, right: &Ptr, desc: &mut dyn SessionDescription) -> Ptr
    where
        T: Merge<T>,
    {
        self.merge_impl(right, desc, false, &|left: &T, right: &T, desc| {
            left.merge(right, desc)
        })
    }

    /// Merge two state objects in one
    fn merge_custom(
        &self,
        right: &Ptr,
        desc: &mut dyn SessionDescription,
        merge_fn: &dyn Fn(&T, &T, &mut dyn SessionDescription) -> T,
    ) -> Ptr {
        self.merge_impl(right, desc, false, merge_fn)
    }

    /// Merge two state objects in one
    fn merge_impl(
        &self,
        right: &Ptr,
        desc: &mut dyn SessionDescription,
        merge_all: bool,
        merge_fn: &dyn Fn(&T, &T, &mut dyn SessionDescription) -> T,
    ) -> Ptr;
}

/// Vector of elements
pub trait Vector<T: Clone + HashableObject + TypeDesc + MovablePoolObject<T> + fmt::Debug>:
    HashableObject + PoolObject + fmt::Display + fmt::Debug
{
    /// Size of vector
    fn len(&self) -> usize;

    /// Access to an item
    fn at(&self, index: usize) -> &T;

    /// Iterator
    fn iter(&self) -> std::slice::Iter<T>;

    /// Iterator (avoid rust bug with iter implementation in several traits)
    fn get_iter(&self) -> std::slice::Iter<T> {
        self.iter()
    }

    /// Push new element
    fn push(&self, desc: &mut dyn SessionDescription, value: T) -> PoolPtr<dyn Vector<T>>;

    /// Change element
    fn change(
        &self,
        desc: &mut dyn SessionDescription,
        index: usize,
        value: T,
    ) -> PoolPtr<dyn Vector<T>>;

    /// Modify whole vector
    fn modify(
        &self,
        desc: &mut dyn SessionDescription,
        modifier: &Box<dyn Fn(&T) -> T>,
    ) -> PoolPtr<dyn Vector<T>>;

    /// Clone object to persistent pool
    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn Vector<T>>;

    /// Get implementation details object
    fn get_impl(&self) -> &dyn Any;
}

/// Option based array trait
pub trait VectorWrapper<T: Clone + HashableObject + TypeDesc + MovablePoolObject<T> + fmt::Debug> {
    /// Size of vector
    fn len(&self) -> usize;

    /// Access to an item
    fn at(&self, index: usize) -> &T;

    /// Iterator
    fn iter(&self) -> std::slice::Iter<T>;

    /// Iterator (avoid rust bug with iter implementation in several traits)
    fn get_iter(&self) -> std::slice::Iter<T> {
        self.iter()
    }

    /// Push new element
    fn push(&self, desc: &mut dyn SessionDescription, value: T) -> Option<PoolPtr<dyn Vector<T>>>;

    /// Change element
    fn change(
        &self,
        desc: &mut dyn SessionDescription,
        index: usize,
        value: T,
    ) -> Option<PoolPtr<dyn Vector<T>>>;

    /// Modify whole vector
    fn modify(
        &self,
        desc: &mut dyn SessionDescription,
        modifier: &Box<dyn Fn(&T) -> T>,
    ) -> Option<PoolPtr<dyn Vector<T>>>;
}

/// Vector of bools
pub trait BoolVector: HashableObject + PoolObject + fmt::Display + fmt::Debug {
    /// Size of vector
    fn len(&self) -> usize;

    /// Access to an item
    fn at(&self, index: usize) -> bool;

    /// Change element
    fn change(
        &self,
        desc: &mut dyn SessionDescription,
        index: usize,
        value: bool,
    ) -> PoolPtr<dyn BoolVector>;

    /// Clone object to persistent pool
    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn BoolVector>;

    /// Get implementation details object
    fn get_impl(&self) -> &dyn Any;
}

/// Sorting predicate for vector items
pub trait SortingPredicate<T> {
    /// Should return true if first item is less than second item
    fn less(first: &T, second: &T) -> bool;
}

/// Sorted vector of elements
pub trait SortedVector<
    T: Clone + HashableObject + TypeDesc + MovablePoolObject<T> + fmt::Debug + std::cmp::PartialEq,
    Compare: SortingPredicate<T>,
>: HashableObject + PoolObject + fmt::Display + fmt::Debug
{
    /// Size of vector
    fn len(&self) -> usize;

    /// Access to an item
    fn at(&self, index: usize) -> &T;

    /// Iterator
    fn iter(&self) -> std::slice::Iter<T>;

    /// Iterator (avoid rust bug with iter implementation in several traits)
    fn get_iter(&self) -> std::slice::Iter<T> {
        self.iter()
    }

    /// Clone object to persistent pool
    fn clone_to_persistent(
        &self,
        cache: &mut dyn SessionCache,
    ) -> PoolPtr<dyn SortedVector<T, Compare>>;

    /// Get implementation details object
    fn get_impl(&self) -> &dyn Any;
}

/// Option based array trait
pub trait SortedVectorWrapper<
    T: Clone + HashableObject + TypeDesc + MovablePoolObject<T> + std::cmp::PartialEq + fmt::Debug,
    Compare: SortingPredicate<T>,
>
{
    /// Size of vector
    fn len(&self) -> usize;

    /// Access to an item
    fn at(&self, index: usize) -> &T;

    /// Iterator
    fn iter(&self) -> std::slice::Iter<T>;

    /// Iterator (avoid rust bug with iter implementation in several traits)
    fn get_iter(&self) -> std::slice::Iter<T> {
        self.iter()
    }

    /// Push new element
    fn push(
        &self,
        desc: &mut dyn SessionDescription,
        value: T,
    ) -> Option<PoolPtr<dyn SortedVector<T, Compare>>>;
}

/// Trait for type
pub trait TypeDesc {
    /// Returns instance counter for this type
    fn get_vector_instance_counter(desc: &dyn SessionDescription) -> &CachedInstanceCounter;
}

/// Block which has been sent to validator session
pub trait SentBlock: fmt::Display + fmt::Debug + PoolObject + HashableObject {
    /// Block identifier
    fn get_id(&self) -> &BlockId;

    /// Source validator index
    fn get_source_index(&self) -> u32;

    /// Hash of the root for this block
    fn get_root_hash(&self) -> &BlockHash;

    /// File data hash
    fn get_file_hash(&self) -> &BlockHash;

    /// Collated data file hash
    fn get_collated_data_file_hash(&self) -> &BlockHash;

    /// Timestamp of source block creation (when the block and all its dependencies are resolved)
    fn get_source_block_creation_time(&self) -> std::time::SystemTime;

    /// Timestamp of source block creation itself (without dependencies resolving)
    fn get_source_block_payload_creation_time(&self) -> std::time::SystemTime;

    /// Timestamp of sent block creation
    fn get_creation_time(&self) -> std::time::SystemTime;

    /// Clone object to persistent pool
    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn SentBlock>;

    /// Get implementation details object
    fn get_impl(&self) -> &dyn Any;
}

/// Block wrapper (for operations on top of Rc<SentBlock>)
pub trait SentBlockWrapper {
    /// Block identifier
    fn get_id(&self) -> &BlockId;
}

/// Signature of a block candidate
pub trait BlockCandidateSignature: fmt::Display + fmt::Debug + PoolObject + HashableObject {
    /// Block signature
    fn get_signature(&self) -> &BlockSignature;

    /// Clone object to persistent pool
    fn clone_to_persistent(
        &self,
        cache: &mut dyn SessionCache,
    ) -> PoolPtr<dyn BlockCandidateSignature>;

    /// Get implementation details object
    fn get_impl(&self) -> &dyn Any;
}

/// Block candidate
pub trait BlockCandidate: fmt::Display + fmt::Debug + PoolObject + HashableObject {
    /// Block ID
    fn get_id(&self) -> &BlockId;

    /// Get attached block
    fn get_block(&self) -> &SentBlockPtr;

    /// Source validator index
    fn get_source_index(&self) -> u32;

    /// Check if block is approved by specified validator
    fn check_block_is_approved_by(&self, source_index: u32) -> bool;

    /// Check if block is approved by cutoff weights
    fn check_block_is_approved(&self, desc: &dyn SessionDescription) -> bool;

    /// Get list of approvers
    fn get_approvers_list(&self) -> &BlockCandidateSignatureVectorPtr;

    /// Clone object to persistent pool
    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn BlockCandidate>;

    /// Get implementation details object
    fn get_impl(&self) -> &dyn Any;
}

/// Block candidate wrapper (for operations on top of Rc<BlockCandidate>)
pub trait BlockCandidateWrapper {
    /// Push signature of a new block candidate
    fn push(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        signature: BlockCandidateSignaturePtr,
    ) -> BlockCandidatePtr;
}

/// Block vote candidate
pub trait VoteCandidate: fmt::Display + fmt::Debug + PoolObject + HashableObject {
    /// Block ID
    fn get_id(&self) -> &BlockId;

    /// Get attached block
    fn get_block(&self) -> &SentBlockPtr;

    /// Source validator index
    fn get_source_index(&self) -> u32;

    /// Voters list
    fn get_voters_list(&self) -> &BoolVectorPtr;

    /// Check if block is voted
    fn check_block_is_voted(&self, desc: &dyn SessionDescription) -> bool;

    /// Check if block is voted by specific vlidator
    fn check_block_is_voted_by(&self, src_idx: u32) -> bool;

    /// Clone object to persistent pool
    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn VoteCandidate>;

    /// Get implementation details object
    fn get_impl(&self) -> &dyn Any;
}

/// Vote candidate wrapper (for operations on top of Rc<VoteCandidate>)
pub trait VoteCandidateWrapper {
    /// Push vote for candidate
    fn push(&self, desc: &mut dyn SessionDescription, src_idx: u32) -> VoteCandidatePtr;
}

/// Comparator for voting candidates
pub struct VoteCandidateComparator {}

impl SortingPredicate<PoolPtr<dyn VoteCandidate>> for VoteCandidateComparator {
    fn less(first: &PoolPtr<dyn VoteCandidate>, second: &PoolPtr<dyn VoteCandidate>) -> bool {
        first.get_id() < second.get_id()
    }
}

/// Attempt state
pub trait RoundAttemptState: fmt::Display + fmt::Debug + PoolObject + HashableObject {
    /// Sequence number of the attempt
    fn get_sequence_number(&self) -> u32;

    /// Votes
    fn get_votes(&self) -> &VoteCandidateVectorPtr;

    /// Precommits
    fn get_precommits(&self) -> &BoolVectorPtr;

    /// Get voted block
    fn get_voted_block(&self, desc: &dyn SessionDescription) -> Option<SentBlockPtr>;

    /// Get "vote-for" block
    fn get_vote_for_block(&self) -> &Option<SentBlockPtr>;

    /// Check if attempt is precommitted
    fn check_attempt_is_precommitted(&self, desc: &dyn SessionDescription) -> bool;

    /// Check vote is received from validator
    fn check_vote_received_from(&self, src_idx: u32) -> bool;

    /// Check precommit is received from validator
    fn check_precommit_received_from(&self, src_idx: u32) -> bool;

    /// Create action according to a current state
    fn create_action(
        &self,
        desc: &dyn SessionDescription,
        round: &dyn RoundState,
        src_idx: u32,
        attempt_id: u32,
    ) -> Option<ton::Message>;

    /// Clone object to persistent pool
    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn RoundAttemptState>;

    /// Get implementation details object
    fn get_impl(&self) -> &dyn Any;

    /// Dump state
    fn dump(&self, desc: &dyn SessionDescription) -> String;
}

/// Round attempt wrapper (for operations on top of Rc<RoundAttemptState>)
pub trait RoundAttemptStateWrapper {
    /// Apply action to a state
    fn apply_action(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
        message: &ton::Message,
        round_state: &dyn RoundState,
    ) -> RoundAttemptStatePtr;

    /// Consensus iteration actualization
    fn make_one(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
        round_state: &dyn RoundState,
    ) -> (RoundAttemptStatePtr, bool);
}

/// Round state
pub trait RoundState: fmt::Display + fmt::Debug + PoolObject + HashableObject {
    /// Round sequence number
    fn get_sequence_number(&self) -> u32;

    /// Get precommitted block
    fn get_precommitted_block(&self) -> Option<SentBlockPtr>;

    /// Does the round have precommitted block
    fn has_precommitted_block(&self) -> bool;

    /// First attempt for the specified validator
    fn get_first_attempt(&self, src_idx: u32) -> u32;

    /// Last precommit for the sspecified validator
    fn get_last_precommit(&self, src_idx: u32) -> u32;

    /// Get block by id
    fn get_block(&self, block_id: &BlockId) -> Option<BlockCandidatePtr>;

    /// Does the round have approved block
    fn has_approved_block(&self, desc: &dyn SessionDescription) -> bool;

    /// Validators which approved the block
    fn get_block_approvers(&self, desc: &dyn SessionDescription, block_id: &BlockId) -> Vec<u32>;

    /// Block approved by validator
    fn get_blocks_approved_by(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
    ) -> Vec<SentBlockPtr>;

    /// Check if block is signed
    fn check_block_is_signed(&self, desc: &dyn SessionDescription) -> bool;

    /// Check if block is signed by specific validator
    fn check_block_is_signed_by(&self, src_idx: u32) -> bool;

    /// Check if block is approved by specific validator
    fn check_block_is_approved_by(&self, src_idx: u32, block_id: &BlockId) -> bool;

    /// Check if block was sent by a specific validator
    fn check_block_is_sent_by(&self, src_idx: u32) -> bool;

    /// Does the round have voted block
    fn has_voted_block(&self, desc: &dyn SessionDescription) -> bool;

    /// Check if we need to generate vote for block
    fn check_need_generate_vote_for(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
        attempt: u32,
    ) -> bool;

    /// Generate vote for
    fn generate_vote_for(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
    ) -> ton::Message;

    /// Choose block to sign
    fn choose_block_to_sign(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
    ) -> Option<SentBlockPtr>;

    /// Choose blocks to approve
    fn choose_blocks_to_approve(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
    ) -> Vec<SentBlockPtr>;

    /// Choose block to vote
    fn choose_block_to_vote(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
        attempt: u32,
        vote_for: Option<SentBlockPtr>,
    ) -> Option<SentBlockPtr>;

    /// List of signatures
    fn get_signatures(&self) -> &BlockCandidateSignatureVectorPtr;

    /// Create action according to a current state
    fn create_action(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
    ) -> Option<ton::Message>;

    /// Clone object to persistent pool
    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn RoundState>;

    /// Get implementation details object
    fn get_impl(&self) -> &dyn Any;

    /// Dump state
    fn dump(&self, desc: &dyn SessionDescription) -> String;
}

/// Round wrapper (for operations on top of Rc<RoundState>)
pub trait RoundStateWrapper {
    /// Apply action to a state
    fn apply_action(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
        message: &ton::Message,
        block_creation_time: std::time::SystemTime,
        block_payload_creation_time: std::time::SystemTime,
    ) -> RoundStatePtr;

    /// Consensus iteration actualization
    fn make_one(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
    ) -> (RoundStatePtr, bool);
}

/// Old round state
pub trait OldRoundState: fmt::Display + fmt::Debug + PoolObject + HashableObject {
    /// Signed block
    fn get_block(&self) -> &SentBlockPtr;

    /// ID of signed block
    fn get_block_id(&self) -> &BlockId;

    /// Round sequence number
    fn get_sequence_number(&self) -> u32;

    /// Check if block is signed by specific validator
    fn check_block_is_signed_by(&self, src_idx: u32) -> bool;

    /// Check if block is approved by specific validator
    fn check_block_is_approved_by(&self, src_idx: u32) -> bool;

    /// Clone object to persistent pool
    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn OldRoundState>;

    /// Commit signatures
    fn get_signatures(&self) -> &BlockCandidateSignatureVectorPtr;

    /// Approval signatures
    fn get_approve_signatures(&self) -> &BlockCandidateSignatureVectorPtr;

    /// Merge round to old round
    fn merge_round(
        &self,
        round: &dyn RoundState,
        desc: &mut dyn SessionDescription,
    ) -> PoolPtr<dyn OldRoundState>;

    /// Get implementation details object
    fn get_impl(&self) -> &dyn Any;
}

/// Old round wrapper (for operations on top of Rc<OldRoundState>)
pub trait OldRoundStateWrapper {
    /// Apply action to a state
    fn apply_action(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
        message: &ton::Message,
    ) -> OldRoundStatePtr;
}

/// Session state
pub trait SessionState: fmt::Display + fmt::Debug + PoolObject + HashableObject {
    /// Current round sequence id
    fn get_current_round_sequence_number(&self) -> u32;

    /// Attempt global number (timestamp) for specified validator
    fn get_ts(&self, src_idx: u32) -> u32;

    /// Choose block to sign
    fn choose_block_to_sign(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
    ) -> Option<SentBlockPtr>;

    /// Committed block for specified round
    fn get_committed_block(
        &self,
        desc: &dyn SessionDescription,
        round_seqno: u32,
    ) -> Option<SentBlockPtr>;

    /// Get block by id
    fn get_block(&self, desc: &dyn SessionDescription, block_id: &BlockId) -> Option<SentBlockPtr>;

    /// Does the round have approved block
    fn has_approved_block(&self, desc: &dyn SessionDescription) -> bool;

    /// Block approved by validator
    fn get_blocks_approved_by(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
    ) -> Vec<SentBlockPtr>;

    /// Choose blocks to approve
    fn choose_blocks_to_approve(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
    ) -> Vec<SentBlockPtr>;

    /// Check if block is signed by specific validator
    fn check_block_is_signed_by(&self, src_idx: u32) -> bool;

    /// Check if block is approved by specific validator
    fn check_block_is_approved_by(&self, src_idx: u32, block_id: &BlockId) -> bool;

    /// Check if block was sent by a specific validator
    fn check_block_is_sent_by(&self, src_idx: u32) -> bool;

    /// Does the round have precommitted block
    fn has_precommitted_block(&self) -> bool;

    /// Does the round have voted block
    fn has_voted_block(&self, desc: &dyn SessionDescription) -> bool;

    /// Check if we need to generate vote for block
    fn check_need_generate_vote_for(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
        attempt: u32,
    ) -> bool;

    /// Generate vote for
    fn generate_vote_for(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
    ) -> ton::Message;

    /// Validators which approved the block
    fn get_block_approvers(&self, desc: &dyn SessionDescription, block_id: &BlockId) -> Vec<u32>;

    /// Get commited block signatures
    fn get_committed_block_signatures(
        &self,
        sequence_number: u32,
    ) -> Option<BlockCandidateSignatureVectorPtr>;

    /// Get commited block approve signatures
    fn get_committed_block_approve_signatures(
        &self,
        sequence_number: u32,
    ) -> Option<BlockCandidateSignatureVectorPtr>;

    /// Clone object to persistent pool
    fn clone_to_persistent(&self, cache: &mut dyn SessionCache) -> PoolPtr<dyn SessionState>;

    /// Apply action to a state
    fn apply_action(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
        message: &ton::Message,
        block_creation_time: std::time::SystemTime,
        block_payload_creation_time: std::time::SystemTime,
    ) -> SessionStatePtr;

    /// Create action according to a current state
    fn create_action(
        &self,
        desc: &dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
    ) -> Option<ton::Message>;

    /// Get implementation details object
    fn get_impl(&self) -> &dyn Any;

    /// Dump state
    fn dump(&self, desc: &dyn SessionDescription) -> String;
}

/// State wrapper (for operations on top of Rc<SessionState>)
pub trait SessionStateWrapper {
    /// Return actualized session (returns state and marker than session has been changed during actualization)
    fn make_all(
        &self,
        desc: &mut dyn SessionDescription,
        src_idx: u32,
        attempt_id: u32,
    ) -> SessionStatePtr;
}

/// Validator session description
pub trait SessionDescription: fmt::Display + fmt::Debug + cache::SessionCache {
    /// Source public key hash
    fn get_source_public_key_hash(&self, src_idx: u32) -> &PublicKeyHash;

    /// Source public key
    fn get_source_public_key(&self, src_idx: u32) -> &PublicKey;

    /// ADNL id
    fn get_source_adnl_id(&self, src_idx: u32) -> &PublicKeyHash;

    /// Get source index by public key hash
    fn get_source_index(&self, public_key_hash: &PublicKeyHash) -> u32;

    /// Validator weight
    fn get_node_weight(&self, src_idx: u32) -> ValidatorWeight;

    /// Nodes count
    fn get_total_nodes(&self) -> u32;

    /// Cutoff weight for decision making
    fn get_cutoff_weight(&self) -> ValidatorWeight;

    /// Total aggregated weight of validators
    fn get_total_weight(&self) -> ValidatorWeight;

    /// Get index of this validator
    fn get_self_idx(&self) -> u32;

    /// Node priority in a round
    fn get_node_priority(&self, src_idx: u32, round: u32) -> i32;

    /// Maximum priority of the node
    fn get_max_priority(&self) -> u32;

    /// Convert timestamp to unix-time (returns seconds only without fractional part)
    fn get_unixtime(&self, ts: u64) -> u32;

    /// Attempt sequence number
    fn get_attempt_sequence_number(&self, ts: u64) -> u32;

    /// Current timestamp in fixed point format 32.32 (bits [32;64) - seconds, [0..32) - seconds fraction)
    fn get_ts(&self) -> u64;

    /// Make candidate id
    fn candidate_id(
        &self,
        src_idx: u32,
        root_hash: &BlockHash,
        file_hash: &BlockHash,
        collated_data_hash: &BlockHash,
    ) -> BlockId;

    /// Check signature
    fn check_signature(
        &self,
        root_hash: &BlockHash,
        file_hash: &BlockHash,
        src_idx: u32,
        signature: &BlockSignature,
    ) -> Result<()>;

    /// Check signature of approval
    fn check_approve_signature(
        &self,
        root_hash: &BlockHash,
        file_hash: &BlockHash,
        src_idx: u32,
        signature: &BlockSignature,
    ) -> Result<()>;

    /// Get delay in seconds for specified priority
    fn get_delay(&self, priority: u32) -> std::time::Duration;

    /// Get delay to commit empty block
    fn get_empty_block_delay(&self) -> std::time::Duration;

    /// "Vote-for" validator index for specified attempt
    fn get_vote_for_author(&self, attempt: u32) -> u32;

    /// Validators public key hashes
    fn export_nodes(&self) -> Vec<PublicKeyHash>;

    /// Validators public keys
    fn export_full_nodes(&self) -> Vec<PublicKey>;

    /// Catchain nodes of the validators
    fn export_catchain_nodes(&self) -> Vec<CatchainNode>;

    /// Options
    fn opts(&self) -> &SessionOptions;

    /// Get cache
    fn get_cache(&mut self) -> &mut dyn SessionCache;

    /// Generate random usize value
    fn generate_random_usize(&mut self) -> usize;

    /// Get next attempt start time
    fn get_attempt_start_at(&self, attempt: u32) -> std::time::SystemTime;

    /// Set time for log replaying
    fn set_time(&mut self, time: std::time::SystemTime);

    /// Get time for log replaying (SystemTime::now() for realtime processing)
    fn get_time(&self) -> std::time::SystemTime;

    /// Check if time is in future
    fn is_in_future(&self, time: std::time::SystemTime) -> bool;

    /// Check if time is in past
    fn is_in_past(&self, time: std::time::SystemTime) -> bool;

    /// Receiver for metrics
    fn get_metrics_receiver(&self) -> &metrics_runtime::Receiver;

    /// Sent block instance counter
    fn get_sent_blocks_instance_counter(&self) -> &CachedInstanceCounter;

    /// Block candindate signature instance counter
    fn get_block_candidate_signatures_instance_counter(&self) -> &CachedInstanceCounter;

    /// Block candindate instance counter
    fn get_block_candidates_instance_counter(&self) -> &CachedInstanceCounter;

    /// Vote candindate instance counter
    fn get_vote_candidates_instance_counter(&self) -> &CachedInstanceCounter;

    /// Round attempt instance counter
    fn get_round_attempts_instance_counter(&self) -> &CachedInstanceCounter;

    /// Round instance counter
    fn get_rounds_instance_counter(&self) -> &CachedInstanceCounter;

    /// Old round instance counter
    fn get_old_rounds_instance_counter(&self) -> &CachedInstanceCounter;

    /// State instance counter
    fn get_session_states_instance_counter(&self) -> &CachedInstanceCounter;

    /// Integer vectors instance counter
    fn get_integer_vectors_instance_counter(&self) -> &CachedInstanceCounter;

    /// Bool vectors instance counter
    fn get_bool_vectors_instance_counter(&self) -> &CachedInstanceCounter;

    /// Block candidate vectors instance counter
    fn get_block_candidate_vectors_instance_counter(&self) -> &CachedInstanceCounter;

    /// Block candidate signature vectors instance counter
    fn get_block_candidate_signature_vectors_instance_counter(&self) -> &CachedInstanceCounter;

    /// Vote candidate vectors instance counter
    fn get_vote_candidate_vectors_instance_counter(&self) -> &CachedInstanceCounter;

    /// Round attempt vectors instance counter
    fn get_round_attempt_vectors_instance_counter(&self) -> &CachedInstanceCounter;

    /// Old round vectors instance counter
    fn get_old_round_vectors_instance_counter(&self) -> &CachedInstanceCounter;
}

/// Validator's block ID
#[derive(Debug)]
pub struct ValidatorBlockId {
    /// Root hash
    pub root_hash: BlockHash,

    /// File hash
    pub file_hash: BlockHash,
}

/// Validator's block candidate from validator
#[derive(Debug)]
pub struct ValidatorBlockCandidate {
    /// Public key of validator
    pub public_key: PublicKey,

    /// Block's identifier
    pub id: ValidatorBlockId,

    /// Collated file hash
    pub collated_file_hash: BlockHash,

    /// Block's data
    pub data: BlockPayloadPtr,

    /// Block's collated data
    pub collated_data: BlockPayloadPtr,
}

/// Pointer to a validator's block candidate from
pub type ValidatorBlockCandidatePtr = Arc<ValidatorBlockCandidate>;

/// Response for SessionListener.on_candidate
pub type ValidatorBlockCandidateDecisionCallback =
    Box<dyn FnOnce(Result<std::time::SystemTime>) + Send>;

/// Response for SessionListener.on_generate_slot
pub type ValidatorBlockCandidateCallback =
    Box<dyn FnOnce(Result<ValidatorBlockCandidatePtr>) + Send>;

/// Validator session callbacks API
pub trait SessionListener {
    /// New block candidate appears
    fn on_candidate(
        &self,
        round: u32,
        source: PublicKey,
        root_hash: BlockHash,
        data: BlockPayloadPtr,
        collated_data: BlockPayloadPtr,
        callback: ValidatorBlockCandidateDecisionCallback,
    );

    /// New block should be collated
    fn on_generate_slot(&self, round: u32, callback: ValidatorBlockCandidateCallback);

    /// New block is committed
    fn on_block_committed(
        &self,
        round: u32,
        source: PublicKey,
        root_hash: BlockHash,
        file_hash: BlockHash,
        data: BlockPayloadPtr,
        signatures: Vec<(PublicKeyHash, BlockPayloadPtr)>,
        approve_signatures: Vec<(PublicKeyHash, BlockPayloadPtr)>,
    );

    /// Block generation is skipped for the current round
    fn on_block_skipped(&self, round: u32);

    /// Ask validator to validate block candidate
    fn get_approved_candidate(
        &self,
        source: PublicKey,
        root_hash: BlockHash,
        file_hash: BlockHash,
        collated_data_hash: BlockHash,
        callback: ValidatorBlockCandidateCallback,
    );

    /// Slashing statistics event
    fn on_slashing_statistics(&self, round: u32, stat: SlashingValidatorStat);
}

/// Validator session processor
pub trait SessionProcessor: CompletionHandlerProcessor + fmt::Display {
    /// Session description
    fn get_description(&self) -> &dyn SessionDescription;

    /// Preprocess block
    fn preprocess_block(&mut self, block: catchain::BlockPtr);

    /// Process blocks
    fn process_blocks(&mut self, blocks: Vec<catchain::BlockPtr>);

    /// Notify about finished of blocks processing
    fn finished_catchain_processing(&mut self);

    /// Notify about catchain start
    fn catchain_started(&mut self);

    /// Notify about incoming broadcasts
    fn process_broadcast(&mut self, source_id: PublicKeyHash, data: BlockPayloadPtr);

    /// Notify about incoming query
    fn process_query(
        &mut self,
        source_id: PublicKeyHash,
        data: BlockPayloadPtr,
        callback: catchain::ExternalQueryResponseCallback,
    );

    /// Set timestamp for all further events
    fn set_time(&mut self, timestamp: std::time::SystemTime);

    /// Check & update session state
    fn check_all(&mut self);

    /// Set next awake time
    fn set_next_awake_time(&mut self, timestamp: std::time::SystemTime);

    /// Reset next awake time
    fn reset_next_awake_time(&mut self);

    /// Get next awake time
    fn get_next_awake_time(&self) -> std::time::SystemTime;

    /// Stop all further session processing
    fn stop(&mut self);

    /// Returns implementation specific details
    fn get_impl(&self) -> &dyn Any;

    /// Returns implementation specific details
    fn get_mut_impl(&mut self) -> &mut dyn Any;
}

/// Validator session (wrapper on top of SessionProcessor for multi-threaded use)
pub trait Session: fmt::Display + Send + Sync {
    /// Stop the session
    fn stop(&self);
}

/// Validator session factory
pub struct SessionFactory;

impl SessionFactory {
    /// Create vector from rust vector
    pub fn create_vector<T>(
        desc: &mut dyn SessionDescription,
        data: Vec<T>,
    ) -> PoolPtr<dyn Vector<T>>
    where
        T: Clone
            + MovablePoolObject<T>
            + HashableObject
            + TypeDesc
            + fmt::Debug
            + std::cmp::PartialEq
            + 'static,
    {
        vector::VectorImpl::<T>::create(desc, data)
    }

    /// Create bool vector from rust vector
    pub fn create_bool_vector(
        desc: &mut dyn SessionDescription,
        data: Vec<bool>,
    ) -> PoolPtr<dyn BoolVector> {
        vector_bool::BoolVectorImpl::create(desc, data)
    }

    /// Create vector wrapper from rust vector
    pub fn create_vector_wrapper<T>(
        desc: &mut dyn SessionDescription,
        data: Vec<T>,
    ) -> Option<PoolPtr<dyn Vector<T>>>
    where
        T: Clone
            + MovablePoolObject<T>
            + HashableObject
            + TypeDesc
            + fmt::Debug
            + std::cmp::PartialEq
            + 'static,
    {
        vector::VectorImpl::<T>::create_wrapper(desc, data)
    }

    /// Create empty sorted vector
    pub fn create_empty_sorted_vector<T, Compare>(
        desc: &mut dyn SessionDescription,
    ) -> PoolPtr<dyn SortedVector<T, Compare>>
    where
        T: Clone
            + std::cmp::PartialEq
            + MovablePoolObject<T>
            + HashableObject
            + fmt::Debug
            + TypeDesc
            + 'static,
        Compare: SortingPredicate<T> + 'static,
    {
        vector::SortedVectorImpl::<T>::create_empty(desc)
    }

    /// Create sent block
    pub fn create_sent_block(
        desc: &mut dyn SessionDescription,
        source_id: u32,
        root_hash: BlockHash,
        file_hash: BlockHash,
        collated_data_file_hash: BlockHash,
        block_creation_time: std::time::SystemTime,
        block_payload_creation_time: std::time::SystemTime,
    ) -> SentBlockPtr {
        sent_block::SentBlockImpl::create(
            desc,
            source_id,
            root_hash,
            file_hash,
            collated_data_file_hash,
            block_creation_time,
            block_payload_creation_time,
        )
    }

    /// Create empty sent block
    pub fn create_empty_sent_block(desc: &mut dyn SessionDescription) -> SentBlockPtr {
        sent_block::SentBlockImpl::create_empty(desc)
    }

    /// Create candidate signature
    pub fn create_block_candidate_signature(
        desc: &mut dyn SessionDescription,
        signature: BlockSignature,
    ) -> BlockCandidateSignaturePtr {
        block_candidate::BlockCandidateSignatureImpl::create(desc, signature)
    }

    /// Create block candidate with approvers
    pub fn create_block_candidate(
        desc: &mut dyn SessionDescription,
        block: SentBlockPtr,
        approved_by: BlockCandidateSignatureVectorPtr,
    ) -> BlockCandidatePtr {
        block_candidate::BlockCandidateImpl::create(desc, block, approved_by)
    }

    /// Create block candidate without approvers
    pub fn create_unapproved_block_candidate(
        desc: &mut dyn SessionDescription,
        block: SentBlockPtr,
    ) -> BlockCandidatePtr {
        block_candidate::BlockCandidateImpl::create_unapproved(desc, block)
    }

    /// Create vote candidate
    pub fn create_vote_candidate(
        desc: &mut dyn SessionDescription,
        block: SentBlockPtr,
    ) -> VoteCandidatePtr {
        vote_candidate::VoteCandidateImpl::create_unvoted(desc, block)
    }

    /// Create attempt
    pub fn create_attempt(
        desc: &mut dyn SessionDescription,
        sequence_number: u32,
    ) -> RoundAttemptStatePtr {
        round_attempt::RoundAttemptStateImpl::create_empty(desc, sequence_number)
    }

    /// Create current round
    pub fn create_round(desc: &mut dyn SessionDescription, sequence_number: u32) -> RoundStatePtr {
        round::RoundStateImpl::create_current_round(desc, sequence_number)
    }

    /// Create old round
    pub fn create_old_round(
        desc: &mut dyn SessionDescription,
        round: RoundStatePtr,
    ) -> OldRoundStatePtr {
        old_round::OldRoundStateImpl::create_from_round(desc, round)
    }

    /// Create state
    pub fn create_state(desc: &mut dyn SessionDescription) -> SessionStatePtr {
        session_state::SessionStateImpl::create_empty(desc)
    }

    /// Create session callbacks task queue
    pub fn create_callback_task_queue(
        metrics_receiver: Arc<metrics_runtime::Receiver>,
    ) -> CallbackTaskQueuePtr {
        session::SessionImpl::create_callback_task_queue(metrics_receiver)
    }

    /// Create session
    pub fn create_session(
        options: &SessionOptions,
        session_id: &SessionId,
        ids: &Vec<SessionNode>,
        local_key: &PrivateKey,
        db_path: String,
        db_suffix: String,
        allow_unsafe_self_blocks_resync: bool,
        overlay_manager: CatchainOverlayManagerPtr,
        listener: SessionListenerPtr,
    ) -> SessionPtr {
        session::SessionImpl::create(
            options,
            session_id,
            ids,
            local_key,
            db_path,
            db_suffix,
            allow_unsafe_self_blocks_resync,
            overlay_manager,
            listener,
        )
    }

    /// Create session replay
    pub fn create_session_replay(
        options: &SessionOptions,
        log_replay_options: &LogReplayOptions,
        session_listener: SessionListenerPtr,
        replay_listener: SessionReplayListenerPtr,
    ) -> Result<SessionPtr> {
        session::SessionImpl::create_replay(
            options,
            log_replay_options,
            session_listener,
            replay_listener,
        )
    }

    /// Create session processor
    pub fn create_session_processor(
        options: SessionOptions,
        session_id: SessionId,
        ids: Vec<SessionNode>,
        local_key: PrivateKey,
        listener: SessionListenerPtr,
        catchain: CatchainPtr,
        completion_task_queue: TaskQueuePtr,
        callbacks_task_queue: CallbackTaskQueuePtr,
        session_creation_time: std::time::SystemTime,
        metrics: Option<Arc<metrics_runtime::Receiver>>,
    ) -> SessionProcessorPtr {
        session_processor::SessionProcessorImpl::create(
            options,
            session_id,
            ids,
            local_key,
            listener,
            catchain,
            completion_task_queue,
            callbacks_task_queue,
            session_creation_time,
            metrics,
        )
    }
}
