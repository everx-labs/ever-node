# The script should be run after `test_run_net.sh` beacause it uses TON global config 
# and needs runing net to sync with.

TEST_ROOT=$(pwd);
NODE_TARGET=$TEST_ROOT/../../target/release/

# NOWIP=$(curl ifconfig.me)
NOWIP="127.0.0.1"

echo "  Starting full node..."

cd $NODE_TARGET

rm -r -d $NODE_TARGET/configs_fullnode
rm -r -d $NODE_TARGET/full_node_db
mkdir $NODE_TARGET/configs_fullnode
sed "s/0.0.0.0/$NOWIP/g" $TEST_ROOT/default_config_fullnode.json > $NODE_TARGET/configs_fullnode/default_config.json
sed "s/NODE_NUM/fullnode/g" $TEST_ROOT/log_cfg.yml > $NODE_TARGET/configs_fullnode/log_cfg_fullnode.yml
cp $TEST_ROOT/ton-global.config.json $NODE_TARGET/configs_fullnode/ton-global.config.json

rm /shared/log_fullnode*
./ton_node --configs ./configs_fullnode > /shared/node_fullnode_output &


echo "Waiting 10mins for sync"
sleep 600

function find_new_shard_block {
    N=1
    while [ $N -lt 6 ]
    do
        AGO=$(tail -n 500 /shared/output_fullnode.log | grep -m 1 $1 | awk '{print $16}')
        if [ $AGO -lt 60 ]
        then
            echo "Applied block ($1) $AGO seconds ago - FOUND on fullnode!"
        else
            echo "ERROR: Can't find applied block ($1) newer 60 secs on fullnode!"
            exit 1
        fi
        (( N++ ))
    done
}

find_new_shard_block "0\:1000000000000000"
find_new_shard_block "0\:3000000000000000"
find_new_shard_block "0\:5000000000000000"
find_new_shard_block "0\:7000000000000000"
find_new_shard_block "0\:9000000000000000"
find_new_shard_block "0\:b000000000000000"
find_new_shard_block "0\:d000000000000000"
find_new_shard_block "0\:f000000000000000"

echo "TEST PASSED"