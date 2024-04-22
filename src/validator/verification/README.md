# 1 Overview

This document describes Accelerated SMFT protocol which is designed to decrease EverScale blockchain finality time due to shard-chains consensus process change. This protocol removes BFT consensus from shard-validator sessions and proposes to use rarely changing collators to speed up collation process and soft-majority voting for consensus in shards. Master-chain consensus remains unchanged with responsibilities for forks resolution and shard-sessions configuration selection (collator, validators).

# 2 Requirements

The list below represents the requirements which Acelerated SMFT protocol should satisfy:
- multiple block candidate validations by several validator nodes (verificators)
- deterministic verificators choosing for further slashing algorithms implementations
- unpredictability of choice of the verificator by whole workchain except of verificator itself during the block candidate validation stage
- single node collations of block candidates during validation session
- validation session collator switch in case of collator malfunction

# 3 Specification

## 3.1 Primitives

### 3.1.1 BlockCandidateBroadcast

This broadcasts is sent by collator to all validators in workchain with collated data of new block candidate.

```rust
pub struct BlockCandidateBroadcast {
    pub id: crate::ton::ton_node::blockidext::BlockIdExt,
    pub data: crate::ton::bytes,
    pub collated_data: crate::ton::bytes,
    pub collated_data_file_hash: crate::ton::int256,
    pub created_by: crate::ton::int256,
    pub created_timestamp: crate::ton::long,
}
```

### 3.1.2 BlockCandidateStatus

This message is used to interact between validators to synchronize status of block-candidate processing. Block is identified by ```candidate_id``` field. Signature fields (```deliveries_signature```, ```rejections_signature```, ```approvals_signature``` and ```timeouts_signature```) contain corresponding BLS signatures for different stages of block processing.

```rust
pub struct BlockCandidateStatus {
    pub candidate_id: crate::ton::int256,
    pub deliveries_signature: crate::ton::bytes,
    pub approvals_signature: crate::ton::bytes,
    pub rejections_signature: crate::ton::bytes,
    pub timeouts_signature: crate::ton::bytes,
    pub merges_cnt: crate::ton::int,
    pub created_timestamp: crate::ton::long,
}
```

### 3.1.3 BlockCandidateArbitrage

This message is used to initiate arbitrage process in case of received rejections (NACK). Master-chain validator sends ```BlockCandiateArbitrage``` message to several workchain validators and waits for ```BlockCandidateStatus``` message back from them as a response.

```rust
pub struct BlockCandidateArbitrage {
    pub candidate_id: crate::ton::int256,
    pub created_timestamp: crate::ton::long,
}
```

### 3.1.3 ShardCollatorBlame

This message is sent by workchain validators to workchain to identify collator work malfunction. The message may be sent in case of collation timeout and in case of malicious block-candidate generation. This message is also used in consensus process between master-chain validators to vote for collator blame and rotation.

```rust
pub struct ShardCollatorBlame {
    pub validator_session: crate::ton::int256,
    pub collator_pub_key: crate::ton::int256,
    pub timeouts_blame_signature: crate::ton::bytes,
    pub correctness_blame_signature: crate::ton::bytes,
}
```

## 3.2 Collator Choosing

To speed up collation process and decrease latencies collation rotation period is increased up to validator session time. So during the validation session there is no collator change. Collator is choosen for shard-blocks interval which is specified in master-chain block. So it is always possible to find out collator which corresponds to each particular shard-block.

Collator change is done by master-chain validators in one of following cases:
- master-chain approved shard-collator validity interval is expired (regular case)
- master-chain approved shard-collator timeout blame
- master-chain approved shard-collator validity blame

**Regular Case**

Usually shard-collator is selected for some shard-blocks interval. This interval is written in master-chain block. So each node in network may deterministically detect a collator for each block in each shard.

Shard-collators change is a deterministic process which is initiated by master-chain collator and validated by master-chain validators. So master-chain collator proposes new collator for shard-chain using deterministic pseudo RNG scheme. This selection leads to new validator session creation with such collator as a lead in a shard.

**Shard-collator timeout blame**

Each workchain validator monitors shards health based on timeouts between shard blocks. If such timeout for particular shard is more than configurable timeout shreshold, workchain validator initiates timeout blame via ```ShardCollatorBlame``` message:
- workchain validator signs ```ShardCollatorBlame::timeouts_blame_signature``` with its BLS private key
- workchain validator sends ```ShardCollatorBlame``` message to all master-chain validators
- master-chain collator checks the blame and if ```ShardCollatorBlame::timeouts_blame_signature``` is more then 1/2 of all workchain voting weight, changes shard-collator for compromised shard in next master-chain block candidate; as a proof of change master-chain writes received ```ShardCollatorBlame``` to a new master block-candidate which allow further validation of shard collator rotation by other master-chain validators

**Shard-collator validity blame**

Validity blame may be initiated by master-chain validator in following cases:
- incoming shard-block header is malicious (for example, self-fork by workchain collator)
- shard-block was marked as malicios during the arbitrage process

Validity blame process is following:
- master-chain validator signs ```ShardCollatorBlame::validity_blame_signature``` with its BLS private key
- master-chain validator broadcasts ```ShardCollatorBlame``` message via private master-chain overlay
- master-chain collator checks received aggregated shard validator-sessions blames from other master-chain validators and in case of blame threshold (1/2 of master-chain total weight) changes shard-collator for compromised shard in next master-chain block candidate
- each master-chain validator can check ```ShardCollatorBlame::validity_blame_signature``` BLS signature during master-chain block-candidate validation to validate shard-collator switch
- validity blame signature with corresponding block information is stored in master-chain for further slashing
 
## 3.3 SMFT Protocol

## 3.3.1 Protocol Stages

SMFT protocol processes each new block-candidate from generation by shard-collator to inclusion to master-chain block. Basically protocol may be split to two big stages:
- Workchain Block Processing Stage
- Masterchain Block Processing Stage

## 3.3.2 Workchain Block Processing Stage

Block-candidate processing within workchain has following flow:
- **Block candidate collation**:
  - block-candidate is generated by active shard collator
  - shard collator broadcasts the block-candidate using  ```BlockCandidateBroadcast``` message among neighbors
- **Block candidate body delivery**. When node receives ```BlockCandidateBroadcast``` it has to:
  - update corresponding ```BlockCandidateStatus``` record by adding node's BLS signature  ```BlockCandidateStatus::deliveries_signature``` field to indicate the block has been successfully delivered; then initiate the ```BlockCandidateStatus``` status delivery among neighbours
  - check if the node has to verify this corresponding block-candidate based on private BLS node's key and block-candidate's hash
  - if the node has to verify block-candidate, it starts validation process with one of following results
    - validation passes: the node adds its signature to the corresponding ```BlockCandidateStatus::approvals_signature``` record in local database and sends updated status to all masterchain validators
    - validation fails: the node adds its signature to the corresponding ```BlockCandidateStatus::rejections_signature``` record in local database and sends updated status to all masterchain validators
    - validation timeout: the node adds its signature to the corresponding ```BlockCandidateStatus::timeouts_signature``` record in local database and sends updated status to all masterchain validators
  - **Block candidate status delivery**:
    - each workchain node aggregates block-candidate status information in internal database of ```BlockCandidateStatus``` records
    - when node receives ```BlockCandidateStatus``` message it has to:
      - check it correctness; cancel all further processing in case of malfunction message received
      - merge received status with corresponding ```BlockCandidateStatus``` in local node's database
      - in case of any BLS signatures change after merge save updated status and initiate its delivery among neighbours
      - if accumulated weight of all validators which received block (according to ```BlockCandidateStatus::deliveries_signature``` field) is more than threshold, the node sends updated status to all masterchain validators

## 3.3.3 Masterchain Block Processing Stage

Block-candidate processing within workchain has following flow:
- **BlockCandidateStatus Processing**:
  - when master-chain validator receives ```BlockCandidateStatus``` it checks it correctness
  - merges received ```BlockCandidateStatus``` message with corresponding record from local database and stores it
- **Shard-block Header Validation**:
  - each master-chain validator checks correctness of received shard-block header before adding it to master-chain
    - if block-check fails, whole validation session is marked blamed by this master-chain validator and validator session rotation process is initiated by sending ```ShardCollatorBlame``` messsage to other master-chain validators
    - if block-check passes:
      - if corresponding ```BlockCandidateStatus``` record is absent in a local master-chain node's database, wait for it during block-chain configurable timeout; otherwise:
        - if the accumulated weight of validators which received block-candidate according to ```BlockCandidateStatus::deliveries_signature``` is less than approval threshold, wait for ```BlockCandidateStatus``` update
        - if corresponding ```BlockCandidateStatus::rejections_signature``` is not empty, 
        start validation arbitrage process
        - if rejections wait timeout is expired but ```BlockCandidateStatus::deliveries_signature``` is more then threshold (1/2 of total workchain's weight) add shard-block header to master-chain
- **Validation Arbitrage Process**. The validation arbitrage process flow may be initiated by any master-chain validator to find consensus on particular shard-block correctness:
- master-chain validator sends to random workchain validators ```BlockCandidateArbitrage``` message; accumulated weight of such validators subset has to be not less than configurable threshold
- each workchain validator which received ```BlockCandidateArbitrage``` has to validate corresponding block-candidate and update ```BlockCandidateStatus::approvals_signature``` or ```BlockCandidateStatus::rejections_signature``` fields in corresponding status record in local database, then updated ```BlockCandidateStatus``` has to be sent back to master-chain validator
- master-chain validator merges received from workchain validators statuses until ```BlockCandidateStatus::approvals_signature``` or ```BlockCandidateStatus::rejections_signature``` becomes not less than configurable threshold
  - if according to the workchain nodes consensus block is rejected, the validator session is marked as blamed (so all futher blocks from it won't be accepted) and validator session rotation process is initiated by sending ```ShardCollatorBlame``` messsage to other master-chain validators
  - otherwise blame all validators from initial for arbitrage ```BlockCandidateStatus::rejections_signature``` field and continue interrupted flow process

# 4 Runtime metrics

All SMFT metrics are configured to be sent via metrics crate (metrics::gauge! macro) for further tracking. The main metrics are listed below. Note, some metrics are computer for specific workchain NNN. The placeholder NNN should be replaced with specific workchain number (for example, NNN=0).
- smft_block - number of blocks stored by SMFT internal database (.create - number of created, .drop - number of dropped, w/0 suffix - remaining count)
- smft_candidates - number of block candidates received by SMFT for further processing
- smft_mcNNN_overlay - number of nodes in MC overlay which are visible by this node
- smft_mcNNN_overlay_in_messages - number of messages which are received by this node in MC overlay
- smft_mcNNN_overlay_in_queries - number of queries which are received by this node in MC overlay
- smft_mcNNN_overlay_out_messages - number of messages which are sent by this node to MC overlay
- smft_mcNNN_overlay_out_queries - number of queries which are sent by this node to MC overlay (also change speed for this parameter is computed with suffix '.speed')
- smft_mc_overlays - number of overlays for communication with MC
- smft_wcNNN_block_candidate_processings - number of processed block candidates in workchain
- smft_wcNNN_block_candidate_verifications - numbfer of verifications initiated for block candidates in WC overlay (with linked metrics '.failure' - failed verifications, '.success' - approved verifications, '.total' - all verification requests)
- smft_wcNNN_block_status_merges - number of merges for block candidate statuses for this workchain
- smft_wcNNN_block_status_merges_count - histogram for merges count (with suffixes '.min', '.max', '.med')
- smft_wcNNN_block_status_processings - number of block status processings in workchain
- smft_wcNNN_block_status_received_in_mc - number of received block statuses in MC
- smft_wcNNN_block_status_sets - number of block status modifications
- smft_wcNNN_block_status_to_mc_sends - number of sendings from this node to MC overlay of block statuses
- smft_wcNNN_block_status_within_wc_sends - number of sendings of block statuses from this node to other workchain nodes
- smft_wcNNN_in_block_candidates - number of incoming block candidates (from other workchain nodes)
- smft_wcNNN_mc_approved - number of ACKs for blocks requested by MC (with derivative metric '.frequency')
- smft_wcNNN_mc_delivered - number of delivered and requested block statuses to MC
- smft_wcNNN_mc_rejected - number of NACKs for blocks requested by MC (with derivative metric '.frequency')
- smft_wcNNN_mc_requests - number of blocks requested by MC
- smft_wcNNN_mc_sends_per_block_candidate - average number of block statuses sends per one block
- smft_wcNNN_merges_per_block - average number of merges per block
- smft_wcNNN_new_block_candidates - number of new block candidates which are sent by this node
- smft_wcNNN_overlay_in_broadcasts - number of incoming broadcasts in workchain overlay (with derivative metric '.speed')
- smft_wcNNN_overlay_in_messages - number of incoming messages in workchain overlay (with derivative metric '.speed')
- smft_wcNNN_overlay_in_queries - number of incoming queries in workchain overlay (with derivative metric '.speed')
- smft_wcNNN_overlay_out_broadcasts - number of outgoing broadcasts to workchain overlay (with derivative metric '.speed')
- smft_wcNNN_overlay_out_messages - number of outgoing messages to workchain overlay (with derivative metric '.speed')
- smft_wcNNN_overlay_out_queries - number of outgoing queries to workchain overlay (with derivative metric '.speed')
- smft_wcNNN_overlay_send_message_to_neighbours_calls - number of block status sends to neighbour nodes
- smft_wcNNN_updates_per_mc_send - average number of block status updates per one block status send to MC
- smft_wc_overlays - number of workchain overlays
- smft_workchains - number of workchains
- smft_workchains_updates - number of workchain configuration update requests
- smft_wcNNN_block_mc_delay - histogram in milliseconds for delay between MC request and SMFT delivery response (with suffixes '.min', '.max', '.med')
- smft_wcNNN_stage1_block_candidate_delivered_in_wc - histogram in milliseconds for latency between block candidate collation and receive on workchain node (with suffixes '.min', '.max', '.med')
- smft_wcNNN_stage2_block_status_send_to_mc_latency - histogram in milliseconds for latency between block candidate collation and corresponding block status send from this node to MC validators (with suffixes '.min', '.max', '.med')
- smft_wcNNN_stage3_block_status_received_in_mc_latency - histogram in milliseconds for latency between block candidate collation and corresponding block status receive on MC node (with suffixes '.min', '.max', '.med')
