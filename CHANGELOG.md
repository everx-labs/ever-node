# Release Notes

All notable changes to this project will be documented in this file.

## Version 0.55.35

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
