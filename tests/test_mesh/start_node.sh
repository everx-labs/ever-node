

TEST_ROOT=$(pwd);
NODE_TARGET=$TEST_ROOT/../../target/release/

echo "Starting node $1..."

cd $NODE_TARGET

./ton_node --configs configs_$1 -z . > "$TEST_ROOT/tmp/node_$1.output" 2>&1 &

