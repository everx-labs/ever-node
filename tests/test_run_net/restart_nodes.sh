echo "Rebuilding..."
if ! cargo build --release
then
    exit 1
fi

echo "Stopping nodes..."

pkill ton_node
while pgrep -x "ton_node" > /dev/null
do
    sleep 1
done

NODES=5
TEST_ROOT=$(pwd);
NODE_TARGET=$TEST_ROOT/../../target/release/

echo "Starting nodes..."

cd $NODE_TARGET

for (( N=0; N <= $NODES; N++ ))
do
    echo "  Starting node #$N..."
    ./ton_node --configs configs_$N -z . > "$TEST_ROOT/tmp/node_$N.output" 2>&1 &
done
