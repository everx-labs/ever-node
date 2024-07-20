# Release Notes

All notable changes to this project will be documented in this file.

## Version 0.59.10

- SMFT fixes: 1) lifetime and sync time of smft status block are separated: status lives much more than synced to avoid empty blocks syncs after deletion; 2) messages size dump was added; 3) broadcast hops is set to 3

## Version 0.59.9

- CapUndeletableAccounts supported

## Version 0.59.8

- Log message about bad broadcast made "warn" instead of "error"

## Version 0.59.7

- Add broadcast candidate hops configuration

## Version 0.59.6

- Preparation for future updates

## Version 0.59.5

- Restore T-Node force_update behaviour (bugfix)

## Version 0.59.4

- Fixed AllowStateGcSmartResolver::allow_state_gc which caused memoty leak (no one shard state was deleted from cache)

## Version 0.59.3

- Removed extra logging in validate query

## Version 0.59.2

- Fixes for verificator

## Version 0.59.1

- Added possibility to proxy external messages from outer TCP conneciton to node using console

## Version 0.59.0

- Use modern crates anyhow and thiserror instead of failure

## Version 0.58.17

- Some issues logged as errors in remp client are now warnings
- Fixed top shard blocks resend - now it retries sending when master block was updated. It logged a error before

## Version 0.58.16

- SMFT stability updates (fixes, increase sync periods), extra per-block statistics

## Version 0.58.15

- Support TL vector interface changes

## Version 0.58.14

- Patch for REMP load reduction

## Version 0.58.13

- REMP performance and stability improvements

## Version 0.58.12

- Send external messages as an overlay broadcast when REMP capability is set, but REMP-client is disabled

## Version 0.58.11

- Adjust build with external DB

## Version 0.58.10

- Reduced strictness of block id fields checking. In case of a mismatch, logging is performed instead of returning an error 

## Version 0.58.9

- Attempt to load lost cell from storing cells cache by id

## Version 0.58.8

- Fix build warnings

## Version 0.58.7

- Refactor overlay consumer interface

## Version 0.58.6

- Send SMFT messages to random workchain nodes to improve delivery across workchain

## Version 0.58.5

- Support Mesh networks

## Version 0.58.4

- Added support for due payment fix
- Bump block version

## Version 0.58.3

- Minor fixes related to renaming

## Version 0.58.2

- Repo ever_types merged into repo ever_block

## Version 0.58.0

- The crate was renamed from `ton_node` to `ever-node`
- Supported renaming of other crates

## Version 0.57.0

- Shadow SMFT is prepared for first deployment

## Version 0.56.8

- `./configs/ton-global.config.json` renamed to `./configs/ton-global-config-sample.json` because
  it is not an actual mainnet config. You can get the actual mainnet config here: 
  https://github.com/tonlabs/main.ton.dev/blob/master/configs/ton-global.config.json.
- Removed `low_memory_mode` flag from node's config.json. It is always enabled now. 
  The false value of this flag is not actual anymore because the size of the shard states 
  has become too big to process it in memory.

## Version 0.56.7

- Logging was a bit refactored. Added neighbor's stat to telemetry.

## Version 0.56.6

- Added new parameter to node config `sync_by_archives` which allows to synchronize node by archives 
  instead of single blocks. It may be useful in some conditions, for example, long ping to other nodes.

## Version 0.56.5

- Estimate block size using pruned cells estimation

## Version 0.56.4

- Supported new multi-networks API of DHT
- United crate for protocols

## Version 0.56.3

- Fixed UNREGISTERED_CHAIN_MAX_LEN const for Venom blockchain

## Version 0.56.2

- Fixed bug in storing cells mechanism.

## Version 0.56.1

- Fix unstable sync (key blocks mismatch)

## Version 0.56.0

- Get rid of ton::bytes type

## Version 0.55.100

- Deleted messages from outbound queue which were left after split are taken into estimation of block size

## Version 0.55.99

- Improved mechanism of storing cells.

## Version 0.55.98

- Fixed mint of extra currencies. A bug in the validator was caused while minting the second and next currencies. The result was a validation failure.

## Version 0.55.97

- Improved alforithm for ping of bad peers

## Version 0.55.96

- Tools repo merged into node repo (see tools changelog in the end of this file)

## Version 0.55.95

- Fix script to run test network locally 

## Version 0.55.94

- Revert using all peers for requesting key block ids

## Version 0.55.93

- Fix compiler warnings
- Add REMP settings to default configs

## Version 0.55.92

- Added ability not to split out message queues during shard split

## Version 0.55.91

- Try to use old persistent states in cold boot if newest one is not ready yet

## Version 0.55.90

- Backport from public 

## Version 0.55.89

- Switch to rocksdb from crates.io

## Version 0.55.88

- Implement initial_sync_disabled node run parameter allowing node to sync from zero state  

## Version 0.55.87

- Decrease test memory usage
  
## Version 0.55.86

- Fixed compile warnings

## Version 0.55.85

- Updates in REMP protocol

## Version 0.55.84

- Optimizations on external messages processing

## Version 0.55.83

- Front node functionality isolation
- Fixed accept block when block data already saved

## Version 0.55.82

- Fixed persistent shard states GC
- Cells cache with improved performance  

## Version 0.55.81

- Added REMP broadcast message delayer
- Removed delay for direct REMP messages
- Added node tests

## Version 0.55.80

- Updates in REMP protocol

## Version 0.55.78

- More support for BLS TL structures

## Version 0.55.77

- Support BLS TL structures

## Version 0.55.76

- Catchain low-level receiver configuration options

## Version 0.55.75

- Added command line option `--process-conf-and-exit`.

## Version 0.55.74

- Run TVM control requests

## Version 0.55.73

- Remove deprecated level_mask_mut() call

## Version 0.55.72

- Fixed the blocks parser for newly synchronized nodes

## Version 0.55.71

- Move cleaning outdated Catchain caches on node startup into separate thread

## Version 0.55.70

- Cleaning outdated Catchain caches on node startup

## Version 0.55.69

- FIX: breaking compat in ever-block-json

## Version 0.55.68

- Correct cleanup of catchain DBs

## Version 0.55.67

- Added EngineTraits::load_and_pin_state method to work with states which may be deleted by GC.
- Fixed bug when ShardStatesKeeper tried to restore already deleted (by GC) shard state.

## Version 0.55.66

- Use tcmalloc without debug functionality (stability+)

## Version 0.55.65

- Sending blocks to Kafka was moved to separate worker

## Version 0.55.64

- Write `last_trans_lt` to account's json for deleted accounts.

## Version 0.55.63

- Fixed a possible hang-up while saving a shard state

## Version 0.55.62

- Refactor processing of external messages

## Version 0.55.61

- Support hardforking in node

## Version 0.55.60

- Remove time limit for boot

## Version 0.55.59

- Support hops check for old-fashioned broadcasts

## Version 0.55.58

- Fix build for front node

## Version 0.55.57

- Fixed loop to request peers from queue (add_overlay_peers)

## Version 0.55.56

- Save persistent state with BocWriterStack

## Version 0.55.55

- Update for fast finality

## Version 0.55.54

- Bugfixes for states cache optimisations

## Version 0.55.53

- Added control queries `GetAccountByBlock`, `GetShardAccountMeta`, `GetShardAccountMetaByBlock`, `GetAppliedShardsInfo`
- Produce applied shard hashes and raw block proofs to Kafka

## Version 0.55.52

- Bump cc crate version

## Version 0.55.51

- Supported ton_block library without fast-finality feature

## Version 0.55.50

- Speed up node shutdown

## Version 0.55.46

- Improved previous sessions calculation
- Improved sessions validator info logging

## Version 0.55.45

- Added parameter to default_config to limit maximal REMP queue length
- Added shard states cache modes ("states_cache_mode" param in config.json)
    - "Off" - states are saved synchronously and not cached.
    - "Moderate" - *recommended* - states are saved asiynchronously. Number of cached cells (in the state's BOCs) is minimal.
    - "Full" - states saved asynchronously. The number of cells in memory is continuously growing.
- Added "cache_cells_counters" param in config.json. If it is set to true, then cell counters are cached in memory.
  It speeds up cells' DB but takes more memory. This cache was always enabled before.
- Added limit for cells DB cache size ("cells_lru_size" param in config.json). Zero value means default limit - 1000000.
- Fixed stopping of node (in part of a cells GC)

## Version 0.55.44

- Limit for rocksdb log file: max 3 files, 100MB each

## Version 0.55.43

- Refactor boot procedure
- Simplify procedure to get next key blocks

## Version 0.55.42

- Fixed migration of DB from v4 to v5

## Version 0.55.41

- Adjusted the total WAL size limit for RocksDB

## Version 0.55.40

- Validation fixes for single-node sessions

## Version 0.55.39

- Fixed shard states GC stopping

## Version 0.55.38

- New cleaning algorithm for shard states cache. Now it lives less in case of a persistent state saving.
- Pretty logging for major workers' positions

## Version 0.55.37

- Skip validations for single node sessions
- Validator manager fix for single-node sessions
- Update session creation for single node sessions
- Fix for deep recursion on catchain blocks dropping
- Increase package version
- Now message REMP history before session start is properly collected

## Version 0.55.36

- Different fixes

## Version 0.55.33

- Fix of SystemTime::elapsed() unwrap call

## Version 0.55.32

- Prohibited big cells in ext messages

## Version 0.55.31

- Peristent state heuristics

## Version 0.55.30

- Remove ever-crypto crate

## Version 0.55.28

- Fast finality prototype

## Version 0.55.27

- Stabilization of fast finality feature

## Version 0.55.26

- Supported new version of BocWriter::with_params with fixed cell lifetime

## Version 0.55.25

- Filled gen_utime_ms for block and state

## Version 0.55.24

- Remp collation check for uid duplicates added

## Version: 0.55.22

- Limited the number of attempts in accept block routine (was infinite)

## Version 0.55.21

- Fixed setup of special settings for the "cells" column family in RocksDB

## Version 0.55.20

- Fix got GetStats during boot

## Version 0.55.18

- Optimistic consensus: supported key blocks
- Improve node requests logging

## Version 0.55.17

- Process lost collator

## Version 0.55.16

- Support for different formats of validator statistics

## Version 0.55.15

- Disable debug symbols in release

## Version 0.55.10

- Improve log messages about neighbours

## Version 0.55.9

- Stopping validation in validator_group after all blocks in range are generated
- Increase package version

## Version 0.55.8

- Preliminary changes for status merge
- Refactored REMP message forwarding criteria: now a forwarded message is collated if it has at least one non-reject status from previous sessions
- Test for status merges in REMP
- Fixes in check_message_duplicates: now same-block duplicate checks are properly performed
- log::error to log::trace for no validator_group
- Updated version number in Cargo.toml

## Version 0.55.7

- Merge shards MVP
- Add script to restart specific node
- Optimistic consensus: fixed cc rotating
- Update restart_nodes.sh
- Added 'validation_countdown_mode' option
- Printing valIdation_countdown_mode
- Changed default config for test_run_net - no countdown for zerostate
- Increase package version

## Version 0.55.6

- Disable timeout for catchain processing after restart
- Increase package version

## Version 0.55.5

- Added precalculation of out message queues for split
- Refactored limits and timeout in collator
- Skiped collator phases for after_split block
- Added processed_count_limit into clean_out_msg_queue
- Speeded up cleanup of huge output queue
- Added GlobalCapabilities::CapSuspendedList to supported_capabilities
- Supported new rocksdb interface

## Version 0.55.4

- Some changes for better test
- Added more statistic metrics
- Fix build errors
- Increase package version

## Version 0.55.3

- Fix boot wrong next key block sequence
- Fix boot from init block

## Version 0.55.2

- Optimistic consensus - part one

## Version 0.55.0

- No new validator lists for Remp; message rerouting in Remp fixed; less load when Remp caps is off
- Message duplicate checks using message unique ids (body hashes) in Remp

## Version 0.54.6

- Fixed compilation for build without telemetry feature

## Version: 0.54.5

- Stabilize internal DB tests
- Increase package version

## Version: 0.54.4

- Fixed bug in ShardStateKeeper: 'is_queue_update_for' had been called wrong way.

## Version: 0.54.3

- Prototype flexible build process
- Add ever-types to flexible build
- Increase package version
- Mokup package version for flex build

## Version: 0.54.2

- Open catchain tests
- Open archive tests
- Open storage tests
- Open node tests
- Increase node version

## Version: 0.54.1

- Fast validator session configuration
- Ready to use fast single session code
- SessionFactory::create_single_node_session has been added
- Move shardstate DB performance test to becnhes
- Fix script to run local test network
- Increase node version

## Version: 0.52.7

### Fixed

- Fixed bug in DB restore: LAST_ROTATION_MC_BLOCK pointer is not truncated.


## Version 0.52.6

### New

- Added ability to send the validator status to the console
- Fixed compilation after auto bumped rocks_db version

## Version: 0.52.5

### New

- Supported ever-types version 2.0

## Version: 0.52.4

### Fixed

- Fixed bug in external messages storage. It was not properly cleaned up when node collated blocks only in workchain or masterchain.

## Version: 0.52.3

### Fixed

- Fixed bug in archives GC. Now it checks if shard state related to block is allowed to be deleted.

## Version: 0.52.2

### New

- Made Jaeger tracing optional

## Version: 0.52.1

### New

- Bumped block version to 37
- Supported CapFeeInGasUnits capability

## Version: 0.52.0
### New

- New version of cells DB with separated values for cells counters. New version works faster.
  By-default old version is used (no need to refill DB). To migrate to new version it is need
  to run node with `--force-check-db` flag.
- Added new section in config.json.
  ```json
  "cells_db_config": {
      "states_db_queue_len": 1000,
      "max_pss_slowdown_mcs": 750,
      "prefill_cells_cunters": false
  }
  ```
  Description for flags see below.
- Added in-memory cache for all cells counters. It speeds up cells DB but takes more memory.
  Enabled by `prefill_cells_cunters` flag. By-default it is disabled.
- Abjusteble queue length for states DB. It is a throttling threshold - node slows down
  if the queue is full. Abjusted by `states_db_queue_len` flag in config.json. Default value is 1000.
- Slow down of persistent state saving. It reduces load on cells DB when it is overloaded.
  Maksimum slowdown time (pause before next cell saving) is set by `max_pss_slowdown_mcs`
  flag in config.json. Default value is 750 microseconds.
- Setup Bloom filters and increased caches in the cells DB. It gives significant performance boost
  on databases with a lot of cells (more than 10 millions).
- Fast database recovery. It works by optimistic scenario - doesn't check all cells.
  If cell lost while node works - it restarts and runs cells DB full recovery.

## Version: 0.50.23

### New

- Bumped block version to 31
- Removed unused code

## Version: 0.50.22

### New

- Fixed build warnings
- `crc32c` crate changed to common `crc`
- Added rust version to node info

# Tools Release Notes Archive

## Version 0.1.318

- Added ability to print all accounts short info

## Version 0.1.317

- Make keyid tool usable without extra features
- Added common submodule
  
## Version 0.1.315

- Supported pinned states in TestEngine

## Version 0.1.314

- Support hops check for old-fashioned broadcasts

## Version 0.1.313

- Supported ton_block library without fast-finality feature

## Version 0.1.312

- Support advanced node shutdown

## Version 0.1.311

- Fix build for keyid utility 

## Version 0.1.310

- Remove dependency on ever-crypto crate

## Version 0.1.307

- Support different formats of validator stat
- Support DHT policy on ADNL resolver

## Version 0.1.306

- Fix console output for getstats

## Version 0.1.305

- Supported changes in node

## Version: 0.1.304

- fixed console output for get_stats command

## Version 0.1.303

- Supported ever-types version 2.0

## Version: 0.1.299

- Fix for cell loading with checking

## Version: 0.1.282

### New

- Switched to Rust 2021 edition