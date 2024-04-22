#!/bin/bash

NODES=$(pgrep ever-node | wc -l)
NODES=$((NODES - 1))
TEST_ROOT=$(pwd);
NODE_TARGET=$TEST_ROOT/../../target/release/

pkill -9 ever-node
bash ./remove_junk.sh "$NODE_TARGET" "$NODES" > /dev/null 2>&1
