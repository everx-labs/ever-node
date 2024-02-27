#!/bin/bash

NODES=$(pgrep ton_node | wc -l)

echo "Stopping $NODES nodes..."

pkill ton_node
while pgrep -x "ton_node" > /dev/null
do
    sleep 1
done

echo "Rebuilding..."
if ! cargo build --release
then
    exit 1
fi

TEST_ROOT=$(pwd);
NODE_TARGET=$TEST_ROOT/../../target/release/

echo "Restarting $NODES nodes..."

cd $NODE_TARGET

for (( N=0; N < $NODES; N++ ))
do
    echo "  Starting node #$N..."
    ./ton_node --configs configs_$N -z . > "$TEST_ROOT/tmp/output_$N.log" 2>&1 &
done
