#!/bin/bash

NODES=$(pgrep ton_node | wc -l)
NODES=$((NODES - 1))
TEST_ROOT=$(pwd);
NODE_TARGET=$TEST_ROOT/../../target/release/

pkill -9 ton_node
bash ./remove_junk.sh "$NODE_TARGET" "$NODES" > /dev/null 2>&1
