<p align="center">
  <a href="https://github.com/venom-blockchain/developer-program">
    <img src="https://raw.githubusercontent.com/venom-blockchain/developer-program/main/vf-dev-program.png" alt="Logo" width="366.8" height="146.4">
  </a>
</p>

# ever-node

Everscale/Venom node and validator

## Table of Contents

- [About](#about)
- [Getting Started](#getting-started)
- [Usage](#usage)
  - [Metrics](#metrics)
- [Contributing](#contributing)
- [License](#license)

## About

Implementation of Everscal/Venom node and validator in safe Rust.

## Getting Started

### Prerequisites

Rust complier v1.65+.

### Installing

```
git clone --recurse-submodules https://github.com/tonlabs/ever-node.git
cd ever-node
cargo build --release
```

## Usage

To get help about command line arguments, run

```
ton_node --help
```

### Metrics

#### Collator metrics

The main collator metrics shows the perfomance aspects of the collation process. All metrics are detailed down to hosts and shards with labels: "host" and "shard".

##### Collation times cumulative flow

**collator_elapsed_on_empty_collations** - shows how much time from the start of producing of a new block spent on empty collations. Empty collation is when there is no any message to process during the collation iteration. After the empty collation, the collator sleeps until 200 ms and retries to collate a new block. If there is any message to process it will produce a new block, otherwise it will wait the next 200 ms. After 800 ms of retries, it will produce an empty block and will go to collate the next one.
So the collator may have less than 1000 ms time to collate block if before there were some empty collation tries.

It is recommended to render _elapsed_on_empty_collations_ on a separate graph.

The following metrics are shown as a cumulative flow graph which allows to show them all clearly on one graph. All metrics relate to one collation process iteration and show the balance between different collation operations.

- **collator_elapsed_on_prepare_data** - time spent from the beginning of non-empty collation until the masterchain and shardchains state were loaded
- **collator_elapsed_on_initial_clean** - time spent until the initial out queue clean finished
- **collator_elapsed_on_internals_processed** - time spent until the inbound internals processing finished
- **collator_elapsed_on_remp_processed** - time spent until the inbound external messages processing via remp finished
- **collator_elapsed_on_externals_processed** - time spent until the inbound externals processing finished
- **collator_elapsed_on_new_processed** - time spent until the new internal messages processing finished
- **collator_elapsed_on_secondary_clean** - time spent until the secondary out queue cleaning finished
- **collator_elapsed_on_finalize** - time spent until the the collated block was finalized

##### Out messages queue metrics

Out messages queue is cleaned in two steps:

- inital clean with the ordered algorithm which mostly should delete all processed out internal messages from the queue
- secondary clean with the random access algorithm which executes when not all messages in queue were processed and no any out message was deleted

Collator reports these metrics for each step:

- **collator_clean_out_queue_partial** - if not all messages that could be deleted were processed. The ordered algorithm can stop when it reached the message with max possible processed lt. In this case, the result is not partial. But when it stops by the timeout the result will be partial. When the random algorithm doesn't process all messages for any reason the result will be partial.
- **collator_clean_out_queue_elapsed** - time spent on the cleaning
- **collator_clean_out_queue_processed** - how many out internal messages were processed during the clean
- **collator_clean_out_queue_deleted** - how many messages were deleted from the out queue

For each step metrics have additional label "step" which takes value "initial" or "secondary".

##### Messages processing stats

- **collator_processed_in_int_msgs_count** - the number of inbound internal messages processed
- **collator_dequeued_our_out_int_msgs_count** - the number of processed internal messages that were created previously in the current shard, so they were deleted from the shard's out queue after processing
- **collator_processed_remp_msgs_count** - the number of inbound external messages processed via remp
- **collator_processed_in_ext_msgs_count** - the number of processed inbound external messages (not via remp)
- **collator_created_new_msgs_count** - the number of new internal messages created during collation (both with destination to current shard and with destination to other shards)
- **collator_processed_new_msgs_count** - the number of created new messages that were processed immediately in the current collation
- **collator_reverted_transactions_count** - the number of transactions that were reverted when parallel collation was stopped due to a time limit

##### Collation stop info

The collation may be gracefully stopped before all messages are processed. There are two main reasons:

- by timeout
- by reaching the block limits

**collator_stopped_on_timeout** - the integer value that shows on what operation the collation timeout was reached:

- 0 = no timeout
- 1 = on new messages processing
- 2 = on externals processing
- 3 = on externals processing via remp
- 4 = on internals processing

**collator_stopped_on_block_limit** - first operations can be stopped when the Soft block limit is reached and the next ones can be stopped when the Medium block limit is reached. Eg first the internals processing can be stopped by the Soft limit then the collator will process externals and can be stopped again by the Medium limit. Every case of stopping by limits has an integer representation and the collator reports the sum of these values

```
collator_stopped_on_block_limit = stopped_on_soft_limit + stopped_on_remp_limit + stopped_on_medium_limit
```

Any possible combination of `stopped_on_soft_limit`, `stopped_on_remp_limit`, and `stopped_on_medium_limit` values will result in a unique sum value and we are able to recognize stop cases by this value.

`stopped_on_soft_limit` possible values:

- 0 = NotStopped
- 1 = Internals
- 2 = InitialClean

`stopped_on_remp_limit` possible values:

- 0 = NotStopped
- 3 = Remp

`stopped_on_medium_limit` possible values:

- 0 = NotStopped
- 4 = NewMessages
- 5 = Externals

**collator_not_all_msgs_processed** - one metric that shows what kind of messages were not fully processed during the collation because of graceful stop. Every case has unique value, metric value is the sum, and this sum is unique for any combination.

- +1 = new messages processed partially
- +2 = externals processed partially
- +4 = remp processed partially
- +8 = internals processed partially

Eg if new messages and externals processed partially, the metric value will be: `5 = 1 + 4`

## Contributing

Contribution to the project is expected to be done via pull requests submission.

## License

See the [LICENSE](LICENSE) file for details.

## Tags

`blockchain` `everscale` `rust` `venom-blockchain` `venom-developer-program` `venom-node` `venom-validator`
