# Release Notes

All notable changes to this project will be documented in this file.

## Version: 0.51.0

### New
 - Merge to master
 - Fix build after rocksdb release 0.19
 - Async states storage
 - Fixed state and handle mismatch in ShardStatesKeeper::worker
 - - used try_get instead get
 - - decreased copying using VisitedCell
 - - to be continued...
 - ...while saving persistent state


## Version: 0.50.23

### New

- Bumped block version to 31
- Removed unused code

## Version: 0.50.22

### New

- Fixed build warnings
- `crc32c` crate changed to common `crc`
- Added rust version to node info
