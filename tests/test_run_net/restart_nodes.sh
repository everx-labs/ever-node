#!/bin/bash

NODES=$(pgrep ever-node | wc -l)

echo "Stopping $NODES nodes..."

pkill ever-node
while pgrep -x "ever-node" > /dev/null
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
    ./ever-node --configs configs_$N -z . > "$TEST_ROOT/tmp/output_$N.log" 2>&1 &
done
