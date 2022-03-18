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

extern crate base64;
extern crate chrono;
extern crate crossbeam;
extern crate metrics_core;
extern crate metrics_runtime;
extern crate rand;
extern crate regex;
extern crate sha2;
extern crate tokio;
extern crate ton_api;
extern crate ton_types;

#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;

/// Modules
mod activity_node;
mod block;
mod block_payload;
mod catchain;
mod database;
mod log_player;
pub mod profiling;
mod received_block;
mod receiver;
mod receiver_source;
pub mod utils;

use adnl::node::AdnlNode;
use ever_crypto::{KeyId, KeyOption};
use failure::err_msg;
pub use overlay::PrivateOverlayShortId;
pub use profiling::{InstanceCounter, ResultStatusCounter};
use std::{
    any::Any,
    cell::RefCell,
    fmt,
    path::Path,
    rc::{Rc, Weak},
    sync::Arc,
    time::SystemTime,
};
use ton_types::types::UInt256;

/// Public key
pub type PublicKey = Arc<dyn KeyOption>;

/// Public key hash
pub type PublicKeyHash = Arc<KeyId>;

/// Private key
pub type PrivateKey = Arc<dyn KeyOption>;

/// Result for operations
pub type Result<T> = ton_types::Result<T>;

pub type DatabasePtr = Arc<dyn Database>;

/// Overlay ID
pub type OverlayId = PublicKeyHash;

/// Overlay full ID
pub type OverlayFullId = SessionId;

/// Height of the block
pub type BlockHeight = i32;

/// Hash of the block
pub type BlockHash = UInt256;

/// Signature
pub type BlockSignature = ::ton_api::ton::bytes;

/// Raw data buffer
pub type RawBuffer = ::ton_api::ton::bytes;

/// Pointer to a BlockPayload
pub type BlockPayloadPtr = Arc<dyn BlockPayload>;

/// Block extra data identifier (is used by validator session to match blocks and states)
pub type BlockExtraId = u64;

/// Catchain session ID
pub type SessionId = UInt256;

/// Pointer to ReceivedBlock
pub type ReceivedBlockPtr = Rc<RefCell<dyn ReceivedBlock>>;

/// Pointer to Block
pub type BlockPtr = Arc<dyn Block>;

/// Pointer to ReceiverSource
pub type ReceiverSourcePtr = Rc<RefCell<dyn ReceiverSource>>;

/// Pointer to Receiver
pub type ReceiverPtr = Rc<RefCell<dyn Receiver>>;

/// Pointer to ReceiverListener
pub type ReceiverListenerPtr = Weak<RefCell<dyn ReceiverListener>>;

/// Pointer to receiver's task queue
pub type ReceiverTaskQueuePtr = Arc<dyn ReceiverTaskQueue + Send + Sync>;

/// Pointer to a Catchain
pub type CatchainPtr = Arc<dyn Catchain>;

/// Pointer to overlay API for the Catchain
pub type CatchainOverlayPtr = Arc<dyn CatchainOverlay + Send>;

/// Pointer to overlay listener API for the Catchain
pub type CatchainOverlayListenerPtr = std::sync::Weak<dyn CatchainOverlayListener + Send + Sync>;

/// Pointer to overlay log replay listener API for the Catchain
pub type CatchainOverlayLogReplayListenerPtr =
    std::sync::Weak<dyn CatchainOverlayLogReplayListener + Send + Sync>;

/// Pointer to overlay manager API for the Catchain
pub type CatchainOverlayManagerPtr = Arc<dyn CatchainOverlayManager + Send + Sync>;

/// Pointer to Catchain listener for validator session
pub type CatchainListenerPtr = std::sync::Weak<dyn CatchainListener + Send + Sync>;

/// Pointer to Catchain replaying listener
pub type CatchainReplayListenerPtr = std::sync::Weak<dyn CatchainReplayListener + Send + Sync>;

/// Pointer to ADNL Node
pub type AdnlNodePtr = Arc<AdnlNode>;

/// Pointer to LogPlayer
pub type LogPlayerPtr = Rc<dyn LogPlayer>;

/// Validator's weight
pub type ValidatorWeight = u64;

/// Activity node pointer
pub type ActivityNodePtr = Arc<dyn ActivityNode>;

pub mod ton {
    pub use ::ton_api::ton::catchain::*;
    pub use ::ton_api::ton::rpc::catchain::*;

    /// Catchain block ID
    pub type BlockId = block::Id;

    /// Catchain block dependency
    pub type BlockDep = block::dep::Dep;

    pub type BlockDepVec =
        ::ton_api::ton::vector<::ton_api::ton::Bare, ::ton_api::ton::catchain::block::dep::Dep>;

    /// Catchain block data (internal structure)
    pub type BlockData = block::data::Data;

    /// Catchain block payload
    pub type BlockInnerData = block::inner::Data;

    /// Catchain block
    pub type Block = block::Block;

    /// Catchain first block
    pub type FirstBlock = firstblock::Firstblock;

    /// Block data fork
    pub type BlockDataFork = block::inner::catchain::block::data::data::Fork;

    /// Event which will be received as a response for GetBlockRequest, GetBlocksRequest
    pub type BlockUpdateEvent = blockupdate::BlockUpdate;

    /// Sent when no forks are detected
    pub type GetDifferenceResponse = Difference;

    /// Sent when forks are detected
    pub type DifferenceFork = difference::DifferenceFork;

    /// Response for GetBlocksRequest, GetBlockHistoryRequest
    pub type CatchainSentResponse = Sent;

    /// Response for GetBlockRequest which is sent if the block is found
    pub type BlockResultResponse = BlockResult;

    /// This query is used by the catchain component to request an absent block from another validator
    pub type GetBlockRequest = GetBlock;

    /// This query is used to request several blocks from another validator
    pub type GetBlocksRequest = GetBlocks;

    /// This is the initial request sent by one validator to another one to receive absent blocks
    pub type GetDifferenceRequest = GetDifference;

    /// This query is used to obtain blocks used to build a block with a specified reverse height (number of blocks backwards to the specified block)
    pub type GetBlockHistoryRequest = GetBlockHistory;
}

/// Catchain receiver options
#[derive(Clone, Copy)]
pub struct Options {
    /// Timeout for catchain main loop procesing
    pub idle_timeout: std::time::Duration,

    /// Maximum number of dependencies for a block to merge
    pub max_deps: u32,

    /// Should internal database be used for debugging
    pub debug_disable_db: bool,

    /// Check blocks processed by ValidatorSession but don't use them in Catchain DAG (for debugging and log replay)
    pub skip_processed_blocks: bool,
}

/// Catchain log replay options
#[derive(Clone)]
pub struct LogReplayOptions {
    /// Path to the log file with data to be replayed
    pub log_file_name: String,

    /// Optional: preferred session ID (if None, the last session ID in log will be used)
    pub session_id: Option<String>,

    /// Optional: replay without delays
    pub replay_without_delays: bool,

    /// Catchain DB path
    pub db_path: String,

    /// Catchain DB suffix
    pub db_suffix: String,

    /// Flag which indicates that unsafe Catchain self node blocks resync mode is enabled
    pub allow_unsafe_self_blocks_resync: bool,
}

/// Catchain node description
#[derive(Clone)]
pub struct CatchainNode {
    /// ADNL node short ID
    pub adnl_id: PublicKeyHash,

    /// Node public key
    pub public_key: PublicKey,
}

/// State of the received block
#[derive(PartialEq, Copy, Clone, Debug)]
pub enum ReceivedBlockState {
    /// Block is not initialized
    Null,

    /// Block is a part of fork
    Ill,

    /// Block is initialized
    Initialized,

    /// Block is delivered
    Delivered,
}

/// Trait for block payload data
pub trait BlockPayload: fmt::Debug + Send + Sync {
    /// Get raw data buffer
    fn data(&self) -> &RawBuffer;

    /// Block creation time
    fn get_creation_time(&self) -> std::time::SystemTime;
}

/// Is used as a temporary storage during the block receiving from the catchain
pub trait ReceivedBlock: fmt::Display {
    /// State of block
    fn get_state(&self) -> ReceivedBlockState;

    /// Checks if the block has been initialized
    fn is_initialized(&self) -> bool;

    /// Checks if the block has been delivered
    fn is_delivered(&self) -> bool;

    /// Checks if the block is custom (and should be sent to validator session)
    fn is_custom(&self) -> bool;

    /// Checks if the block has been written to DB
    fn in_db(&self) -> bool;

    /// Height of the block
    fn get_height(&self) -> BlockHeight;

    /// Hash of the block
    fn get_hash(&self) -> &BlockHash;

    /// Block's signature
    fn get_signature(&self) -> &BlockSignature;

    /// Payload of the block
    fn get_payload(&self) -> &BlockPayloadPtr;

    /// Index of the receiver source
    fn get_source_id(&self) -> usize;

    /// Fork index
    fn get_fork_id(&self) -> usize;

    /// Previous block
    fn get_prev(&self) -> Option<ReceivedBlockPtr>;

    /// Previous block hash
    fn get_prev_hash(&self) -> Option<BlockHash>;

    /// Next block in a fork
    fn get_next(&self) -> Option<ReceivedBlockPtr>;

    /// Mapping from fork index to block dependency height
    /// 0 if the block does not have dependency from specified fork
    fn get_forks_dep_heights(&self) -> &Vec<BlockHeight>;

    /// List of dependency block hashes
    fn get_dep_hashes(&self) -> Vec<BlockHash>;

    /// Get several unresolved dependencies for this block
    fn get_pending_deps(
        &mut self,
        call_id: u64,
        max_deps_count: usize,
        dep_hashes: &mut Vec<BlockHash>,
    );

    /// Initialize block with a payload
    fn initialize(
        &mut self,
        block: &ton::Block,
        payload: BlockPayloadPtr,
        receiver: &mut dyn Receiver,
    ) -> Result<()>;

    /// Mark the block as ill
    fn set_ill(&mut self, receiver: &mut dyn Receiver);

    /// Process block
    fn process(&mut self, receiver: &mut dyn Receiver);

    /// Notify block is written to catchain DB
    fn written(&mut self, receiver: &mut dyn Receiver);

    /// Export TL block data
    fn export_tl(&self) -> ton::Block;

    /// Export TL block dependency data
    fn export_tl_dep(&self) -> ton::BlockDep;

    /// Check block can be sent to source according to throttling rules and save last sending time for a pair (block, source)
    fn mark_block_for_sending(&mut self, adnl_id: &PublicKeyHash) -> bool;

    /// Serialized block
    fn get_serialized_block_with_payload(&mut self) -> &BlockPayloadPtr;

    /// Implementation specific
    fn get_impl(&self) -> &dyn Any;

    /// Implementation specific
    fn get_mut_impl(&mut self) -> &mut dyn Any;

    /// Return self reference
    fn get_self(&self) -> ReceivedBlockPtr;

    /// Dump received block a string
    fn to_string(&self) -> String;
}

/// Source statistics
#[derive(Default, Debug)]
pub struct ReceiverSourceStatistics {
    /// Number of incoming received queries
    pub in_queries_count: usize,

    /// Number of outgoing sent queries
    pub out_queries_count: usize,

    /// Number of incoming received messages
    pub in_messages_count: usize,

    /// Number of outgoing sent messages
    pub out_messages_count: usize,

    /// Number of received broadcasts
    pub in_broadcasts_count: usize,
}

/// Source for received blocks
/// This trait contains validator's knowledge about other validator
pub trait ReceiverSource {
    /// Get source validator indentifier
    fn get_id(&self) -> usize;

    /// Hash of validator public key
    fn get_public_key_hash(&self) -> &PublicKeyHash;

    /// Public key of validator
    fn get_public_key(&self) -> &PublicKey;

    /// ADNL identifier
    fn get_adnl_id(&self) -> &PublicKeyHash;

    /// Received height (block is received, dependencies may be not)
    fn get_received_height(&self) -> BlockHeight;

    /// Delivered height (block is received with all dependencies)
    fn get_delivered_height(&self) -> BlockHeight;

    /// Check if we have unreceived blocks
    fn has_unreceived(&self) -> bool;

    /// Check if we have undelivered blocks
    fn has_undelivered(&self) -> bool;

    /// Get the block for specified height
    fn get_block(&self, height: BlockHeight) -> Option<ReceivedBlockPtr>;

    /// Number of forks
    fn get_forks_count(&self) -> usize;

    /// Get list of forks
    fn get_forks(&self) -> &Vec<usize>;

    /// Add new fork for this validator
    fn add_fork(&mut self, receiver: &mut dyn Receiver) -> usize;

    /// Is this validator blamed
    fn is_blamed(&self) -> bool;

    /// Blame the validator
    fn mark_as_blamed(&mut self, receiver: &mut dyn Receiver);

    /// Blame the validator and specify height and fork ID
    fn blame(&mut self, fork: usize, height: BlockHeight, receiver: &mut dyn Receiver);

    /// Get list of heights which have forks
    fn get_blamed_heights(&self) -> &Vec<BlockHeight>;

    /// Mark height for received block
    fn block_received(&mut self, height: BlockHeight);

    /// Mark height for delivered block
    fn block_delivered(&mut self, height: BlockHeight);

    /// Process new incoming block
    fn process_new_block(&mut self, block: ReceivedBlockPtr, receiver: &mut dyn Receiver);

    /// Is the fork proof found
    fn is_fork_found(&self) -> bool;

    /// Fork proof
    fn get_fork_proof(&self) -> &Option<ton::BlockDataFork>;

    /// Fork proof notification
    fn set_fork_proof(&mut self, slice: ton::BlockDataFork);

    /// Source statistics
    fn get_statistics(&self) -> &ReceiverSourceStatistics;

    /// Source statistics (mutable)
    fn get_mut_statistics(&mut self) -> &mut ReceiverSourceStatistics;

    /// Implementation specific
    fn get_impl(&self) -> &dyn Any;

    /// Implementation specific
    fn get_mut_impl(&mut self) -> &mut dyn Any;
}

/// Receiver which contains all receiver sources
pub trait Receiver {
    /// Get session ID
    fn get_incarnation(&self) -> &SessionId;

    /// Catchain options
    fn get_options(&self) -> &Options;

    /// Get number of sources
    fn get_sources_count(&self) -> usize;

    /// Get number of forks
    fn get_forks_count(&self) -> usize;

    /// Get receiver source by index
    fn get_source(&self, source_id: usize) -> ReceiverSourcePtr;

    /// Get receiver source public key hash by index
    fn get_source_public_key_hash(&self, source_id: usize) -> &PublicKeyHash;

    /// Run block
    fn run_block(&mut self, block: ReceivedBlockPtr);

    /// Mark block as delivered
    fn deliver_block(&mut self, block: &mut dyn ReceivedBlock);

    /// Add fork
    fn add_fork(&mut self) -> usize;

    /// Blame source
    fn blame(&mut self, source_id: usize);

    /// Add fork proof
    fn add_fork_proof(&mut self, fork_proof: &BlockPayloadPtr);

    /// Get block by it hash
    fn get_block_by_hash(&self, hash: &BlockHash) -> Option<ReceivedBlockPtr>;

    /// Validate block dependency
    fn validate_block_dependency(&self, block: &ton::BlockDep) -> Result<()>;

    /// Create new block
    fn create_block(&mut self, block: &ton::BlockDep) -> ReceivedBlockPtr;

    /// Create new block from a string dump
    fn create_block_from_string_dump(&self, dump: &String) -> ReceivedBlockPtr;

    fn parse_add_received_block(&mut self, s: &String);

    /// Adding new block
    fn add_block(&mut self, payload: BlockPayloadPtr, deps: Vec<BlockHash>);

    /// New block is received
    fn receive_block(
        &mut self,
        adnl_id: &PublicKeyHash,
        block: &ton::Block,
        payload: BlockPayloadPtr,
    ) -> Result<ReceivedBlockPtr>;

    /// New incoming message from overlay is received
    fn receive_message_from_overlay(
        &mut self,
        adnl_id: &PublicKeyHash,
        bytes: &mut &[u8],
    ) -> Result<ReceivedBlockPtr>;

    /// New incoming broadcast from overlay is received
    fn receive_broadcast_from_overlay(
        &mut self,
        source_key_hash: &PublicKeyHash,
        data: &BlockPayloadPtr,
    );

    /// New incoming query from overlay is received
    fn receive_query_from_overlay(
        &mut self,
        adnl_id: &PublicKeyHash,
        data: &BlockPayloadPtr,
        response_callback: ExternalQueryResponseCallback,
    );

    /// Do catchain processing iteration
    ///TODO: merge this code with check all
    fn process(&mut self);

    /// Receiver for metrics
    fn get_metrics_receiver(&self) -> &metrics_runtime::Receiver;

    /// Received blocks instance counter
    fn get_received_blocks_instance_counter(&self) -> &InstanceCounter;

    /// Implementation specific
    fn get_impl(&self) -> &dyn Any;

    /// Implementation specific
    fn get_mut_impl(&mut self) -> &mut dyn Any;

    /// Dump receiver state
    fn to_string(&self) -> String;

    /// Check & update state
    fn check_all(&mut self);

    /// Set next awake time
    fn set_next_awake_time(&mut self, timestamp: std::time::SystemTime);

    /// Get next awake time
    fn get_next_awake_time(&self) -> std::time::SystemTime;

    /// Destroy DB
    fn destroy_db(&mut self);
}

/// Catchain block
pub trait Block: fmt::Display + fmt::Debug + Send + Sync {
    /// Block creation time
    fn get_creation_time(&self) -> std::time::SystemTime;

    /// Get block extra data ID
    fn get_extra_id(&self) -> BlockExtraId;

    /// Payload
    fn get_payload(&self) -> &BlockPayloadPtr;

    /// Receiver source identifier
    fn get_source_id(&self) -> u32;

    /// Fork ID
    fn get_fork_id(&self) -> usize;

    /// Receiver source public hey hash
    fn get_source_public_key_hash(&self) -> &PublicKeyHash;

    /// Block hash
    fn get_hash(&self) -> &BlockHash;

    /// Block height
    fn get_height(&self) -> BlockHeight;

    /// Previous block
    fn get_prev(&self) -> Option<BlockPtr>;

    /// Get dependency blocks
    fn get_deps(&self) -> &Vec<BlockPtr>;

    /// Mapping from fork index to block dependency height
    /// 0 if the block does not have dependency from specified fork
    fn get_forks_dep_heights(&self) -> &Vec<BlockHeight>;

    /// Is this block is descendat of specified one
    fn is_descendant_of(&self, block: &dyn Block) -> bool;
}

/// Database for blocks saving
pub trait Database: Send + Sync {
    /// Return path to db
    fn get_db_path(&self) -> &Path;

    /// Has block written to DB
    fn is_block_in_db(&self, hash: &BlockHash) -> bool;

    /// Get block from DB
    fn get_block(&self, hash: &BlockHash) -> Result<RawBuffer>;

    /// Push block to database
    fn put_block(&self, hash: &BlockHash, data: RawBuffer);

    /// Erase block from database
    fn erase_block(&self, hash: &BlockHash);

    /// Destroy DB (after drop)
    fn destroy(&self);
}

/// Response for queries
pub type ExternalQueryResponseCallback = Box<dyn FnOnce(Result<BlockPayloadPtr>) + Send>;

/// Response for queries
pub type QueryResponseCallback = Box<dyn FnOnce(Result<BlockPayloadPtr>, &mut dyn Receiver)>;

/// Overlay inbound interface for Catchain (Overlay -> Catchain)
pub trait CatchainOverlayListener: Send + Sync {
    /// Incoming message processing
    fn on_message(&self, adnl_id: PublicKeyHash, data: &BlockPayloadPtr);

    /// Incoming broadcast processing
    fn on_broadcast(&self, source_key_hash: PublicKeyHash, data: &BlockPayloadPtr);

    /// Incoming query processing
    fn on_query(
        &self,
        adnl_id: PublicKeyHash,
        data: &BlockPayloadPtr,
        response_callback: ExternalQueryResponseCallback,
    );
}

/// Overlay listener interface to control time during the log replay
pub trait CatchainOverlayLogReplayListener: Send + Sync {
    /// Set timestamp for all further events
    fn on_time_changed(&self, timestamp: std::time::SystemTime);
}

/// Overlay outgoing interface for Catchain (Catchain -> Overlay)
pub trait CatchainOverlay: Send + Sync {
    /// Send message
    fn send_message(
        &self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        message: &BlockPayloadPtr,
    );

    /// Send message to multiple sources
    fn send_message_multicast(
        &self,
        receiver_ids: &[PublicKeyHash],
        sender_id: &PublicKeyHash,
        message: &BlockPayloadPtr,
    );

    /// Send query
    fn send_query(
        &self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        name: &str,
        timeout: std::time::Duration,
        message: &BlockPayloadPtr,
        response_callback: ExternalQueryResponseCallback,
    );

    /// Send query via RLDP (ADNL ID of the current node should be registered for the query)
    fn send_query_via_rldp(
        &self,
        dst_adnl_id: PublicKeyHash,
        name: String,
        response_callback: ExternalQueryResponseCallback,
        timeout: std::time::SystemTime,
        query: BlockPayloadPtr,
        max_answer_size: u64,
    );

    /// Send broadcast
    fn send_broadcast_fec_ex(
        &self,
        sender_id: &PublicKeyHash,
        send_as: &PublicKeyHash,
        payload: BlockPayloadPtr,
    );

    /// Implementation specific
    fn get_impl(&self) -> &dyn Any;
}

/// Overlay manager
pub trait CatchainOverlayManager {
    /// Create new overlay
    fn start_overlay(
        &self,
        local_id: &PublicKeyHash,
        overlay_short_id: &Arc<PrivateOverlayShortId>,
        nodes: &Vec<CatchainNode>,
        overlay_listener: CatchainOverlayListenerPtr,
        log_replay_listener: CatchainOverlayLogReplayListenerPtr,
    ) -> Result<CatchainOverlayPtr>;

    /// Stop existing overlay
    fn stop_overlay(
        &self,
        overlay_short_id: &Arc<PrivateOverlayShortId>,
        overlay: &CatchainOverlayPtr,
    );
}

/// Listener for Receiver callbacks
pub trait ReceiverListener {
    /// Any cast
    fn get_impl(&self) -> &dyn Any;

    /// Any cast (mutable)
    fn get_mut_impl(&mut self) -> &mut dyn Any;

    /// Notification about receiver started
    fn on_started(&mut self);

    /// New block receiving event
    fn on_new_block(
        &mut self,
        receiver: &mut dyn Receiver,
        source_id: usize,
        fork_id: usize,
        hash: BlockHash,
        height: BlockHeight,
        prev: BlockHash,
        deps: Vec<BlockHash>,
        forks_dep_heights: Vec<BlockHeight>,
        payload: &BlockPayloadPtr,
    );

    /// Incoming broadcast processing
    fn on_broadcast(
        &mut self,
        receiver: &mut dyn Receiver,
        source_key_hash: &PublicKeyHash,
        data: &BlockPayloadPtr,
    );

    /// Source blame event
    fn on_blame(&mut self, receiver: &mut dyn Receiver, source_id: usize);

    /// Custom query event
    fn on_custom_query(
        &mut self,
        receiver: &mut dyn Receiver,
        source_public_key_hash: &PublicKeyHash,
        data: &BlockPayloadPtr,
        response_callback: ExternalQueryResponseCallback,
    );

    /// Send message
    fn send_message(
        &mut self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        message: &BlockPayloadPtr,
        can_be_postponed: bool,
    );

    /// Send message to multiple sources
    fn send_message_multicast(
        &mut self,
        receiver_ids: &[PublicKeyHash],
        sender_id: &PublicKeyHash,
        message: &BlockPayloadPtr,
    );

    /// Send query
    fn send_query(
        &mut self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        name: &str,
        timeout: std::time::Duration,
        message: &BlockPayloadPtr,
        response_callback: QueryResponseCallback,
    );

    /// Task queue
    fn get_task_queue(&self) -> &ReceiverTaskQueuePtr;
}

/// Tasks queue for receiver
pub trait ReceiverTaskQueue: Send + Sync {
    /// Task execution
    fn post_closure(&self, handler: Box<dyn FnOnce(&mut dyn Receiver) + Send>);

    /// Utility task execution
    fn post_utility_closure(&self, handler: Box<dyn FnOnce() + Send>);
}

/// Listener for Catchain
pub trait CatchainListener {
    /// Preprocess block
    fn preprocess_block(&self, block: BlockPtr);

    /// Process blocks
    fn process_blocks(&self, blocks: Vec<BlockPtr>);

    /// Notify about finished of blocks processing
    fn finished_processing(&self);

    /// Notify about catchain start
    fn started(&self);

    /// Notify about incoming broadcasts
    fn process_broadcast(&self, source_id: PublicKeyHash, data: BlockPayloadPtr);

    /// Notify about incoming query
    fn process_query(
        &self,
        source_id: PublicKeyHash,
        data: BlockPayloadPtr,
        callback: ExternalQueryResponseCallback,
    );

    /// Set timestamp for all further events
    fn set_time(&self, timestamp: std::time::SystemTime);
}

/// Root class for Catchain processing
pub trait Catchain: Send + Sync {
    /// Request for a new block
    fn request_new_block(&self, time: SystemTime);

    /// Mark block as processed
    fn processed_block(
        &self,
        payload: BlockPayloadPtr,
        may_be_skipped: bool,
        enable_batching_mode: bool,
    );

    /// Send broadcast
    fn send_broadcast(&self, payload: BlockPayloadPtr);

    /// Stop the Catchain
    fn stop(&self, destroy_db: bool);

    /// Send query via RLDP
    fn send_query_via_rldp(
        &self,
        dst: PublicKeyHash,
        name: String,
        response_callback: ExternalQueryResponseCallback,
        timeout: std::time::SystemTime,
        query: BlockPayloadPtr,
        max_answer_size: u64,
    );
}

/// Catchain log player
pub trait LogPlayer {
    /// Get session ID
    fn get_session_id(&self) -> &SessionId;

    /// Get validator local ID
    fn get_local_id(&self) -> &PublicKeyHash;

    /// Get validator private key
    fn get_local_key(&self) -> &PrivateKey;

    /// Get list of nodes
    fn get_nodes(&self) -> &Vec<CatchainNode>;

    /// Get weights
    fn get_weights(&self) -> &Vec<ValidatorWeight>;

    /// Get overlay manager
    fn get_overlay_manager(
        &self,
        replay_listener: CatchainReplayListenerPtr,
    ) -> CatchainOverlayManagerPtr;
}

/// Listener for Catchain replaying
pub trait CatchainReplayListener {
    /// Start of replaying
    fn replay_started(&self);

    /// Finish of replaying
    fn replay_finished(&self);
}

/// Activity node for liveness tracking
pub trait ActivityNode: Send + Sync {
    /// Name of the object
    fn get_name(&self) -> String;

    /// Get creation time
    fn get_creation_time(&self) -> std::time::SystemTime;

    /// Get last activity notification time
    fn get_access_time(&self) -> std::time::SystemTime;

    /// Notify about activity
    fn tick(&self);
}

/// Catchain factory
pub struct CatchainFactory;

impl CatchainFactory {
    /// Create block payload
    pub fn create_block_payload(data: RawBuffer) -> BlockPayloadPtr {
        block_payload::BlockPayloadImpl::create(data)
    }

    /// Create empty payload
    pub fn create_empty_block_payload() -> BlockPayloadPtr {
        Self::create_block_payload(RawBuffer::default())
    }

    /// Create new received block from string dump
    pub fn create_received_block_from_string_dump(
        dump: &String,
        receiver: &dyn Receiver,
    ) -> ReceivedBlockPtr {
        received_block::ReceivedBlockImpl::create_from_string_dump(dump, receiver)
    }

    /// Create new root received block
    pub fn create_root_received_block(
        source_id: usize,
        incarnation: &SessionId,
        instance_counter: &InstanceCounter,
    ) -> ReceivedBlockPtr {
        received_block::ReceivedBlockImpl::create_root(source_id, incarnation, instance_counter)
    }

    /// Create new block
    pub fn create_block(
        source_id: usize,
        fork_id: usize,
        source_public_key_hash: PublicKeyHash,
        height: BlockHeight,
        hash: BlockHash,
        payload: BlockPayloadPtr,
        prev_block: Option<BlockPtr>,
        deps: Vec<BlockPtr>,
        forks_dep_heights: Vec<BlockHeight>,
        extra_id: BlockExtraId,
    ) -> BlockPtr {
        block::BlockImpl::create(
            source_id,
            fork_id,
            source_public_key_hash,
            height,
            hash,
            payload,
            prev_block,
            deps,
            forks_dep_heights,
            extra_id,
        )
    }

    pub fn create_block_from_string_dump(dump: &str, extra_id: BlockExtraId) -> BlockPtr {
        block::BlockImpl::create_from_string_dump(dump, extra_id)
    }

    /// Create receiver source
    pub fn create_receiver_source(
        source_id: usize,
        public_key: PublicKey,
        adnl_id: &PublicKeyHash,
    ) -> ReceiverSourcePtr {
        receiver_source::ReceiverSourceImpl::create(source_id, public_key, adnl_id)
    }

    /// Create dummy receiver for debugging
    pub fn create_dummy_receiver(path: String) -> Result<ReceiverPtr> {
        receiver::ReceiverImpl::create_dummy(path)
    }

    /// Create dummy listener for receiver
    pub fn create_dummy_receiver_listener() -> Rc<RefCell<dyn ReceiverListener>> {
        receiver::ReceiverImpl::create_dummy_listener()
    }

    /// Create receiver
    pub fn create_receiver(
        listener: ReceiverListenerPtr,
        incarnation: &SessionId,
        ids: &Vec<CatchainNode>,
        local_key: &PrivateKey,
        path: String,
        db_suffix: String,
        allow_unsafe_self_blocks_resync: bool,
        metrics: Option<Arc<metrics_runtime::Receiver>>,
    ) -> Result<ReceiverPtr> {
        receiver::ReceiverImpl::create(
            listener,
            incarnation,
            ids,
            local_key,
            path,
            db_suffix,
            allow_unsafe_self_blocks_resync,
            metrics,
        )
    }

    /// Create dummy overlay manager
    pub fn create_dummy_overlay_manager() -> CatchainOverlayManagerPtr {
        catchain::CatchainImpl::create_dummy_overlay_manager()
    }

    /// Create Catchain database
    pub fn create_database(
        path: String,
        name: String,
        metrics: &metrics_runtime::Receiver,
    ) -> Result<DatabasePtr> {
        database::DatabaseImpl::create(&path, &name, metrics)
    }

    /// Create Catchain root object
    pub fn create_catchain(
        options: &Options,
        session_id: &SessionId,
        ids: &Vec<CatchainNode>,
        local_key: &PrivateKey,
        path: String,
        db_suffix: String,
        allow_unsafe_self_blocks_resync: bool,
        overlay_manager: CatchainOverlayManagerPtr,
        listener: CatchainListenerPtr,
    ) -> CatchainPtr {
        catchain::CatchainImpl::create(
            options,
            session_id,
            ids,
            local_key,
            path,
            db_suffix,
            allow_unsafe_self_blocks_resync,
            overlay_manager,
            listener,
        )
    }

    /// Create log replay object
    pub fn create_log_player(log_replay_options: &LogReplayOptions) -> Result<LogPlayerPtr> {
        log_player::LogPlayerImpl::create_log_player(log_replay_options)
    }

    /// Enumerate all log replay objects
    pub fn create_log_players(log_replay_options: &LogReplayOptions) -> Vec<LogPlayerPtr> {
        log_player::LogPlayerImpl::create_log_players(log_replay_options)
    }

    /// Create Catchain root object with log replaying overlay
    pub fn create_catchain_replay(
        options: &Options,
        log_replay_options: &LogReplayOptions,
        catchain_listener: CatchainListenerPtr,
        replay_listener: CatchainReplayListenerPtr,
    ) -> Result<CatchainPtr> {
        log_player::LogPlayerImpl::create_catchain(
            options,
            log_replay_options,
            catchain_listener,
            replay_listener,
        )
    }

    /// Creaate activity node
    pub fn create_activity_node(name: String) -> ActivityNodePtr {
        activity_node::ActivityNodeManager::create_node(name)
    }
}
