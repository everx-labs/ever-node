# Release Notes

All notable changes to this project will be documented in this file.

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
