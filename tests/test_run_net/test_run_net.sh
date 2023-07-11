REMP_TEST=$1
NODES=6
# WORKCHAINS=true
CURRENT_BRANCH="$(git branch --show-current)"
echo "Current branch: $CURRENT_BRANCH"

apt update
apt install jq

if [ ! "$REMP_TEST" == "true" ]
then
    echo "No Remp testing: $REMP_TEST"
else
    echo "Remp testing in progress"
fi

if [ "$WORKCHAINS" == "true" ]
then
    echo "Workchains are enabled!"
else
    echo "Workchains are NOT enabled!"
fi

echo "Preparations..."

pkill -9 ton_node

TEST_ROOT=$(pwd);
NODE_TARGET=$TEST_ROOT/../../target/release/

./remove_junk.sh $NODE_TARGET $NODES > /dev/null 2>&1
echo "Building $(pwd)"

if ! cargo build --release --features "telemetry"
then
    exit 1
fi

cd ../../../
if ! [ -d "ever-node-tools-private" ]
then
    git clone "https://github.com/tonlabs/ever-node-tools"
    cd ever-node-tools-private
    git checkout "$CURRENT_BRANCH" || echo "Use default branch"
else
    cd ever-node-tools-private
fi
TOOLS_ROOT=$(pwd)

# cargo update
echo "Building $(pwd)"
if ! cargo build --release
then
    exit 1
fi
cd target/release/

cd $TEST_ROOT
NOWDATE=$(date +"%s")
# NOWIP=$(curl ifconfig.me)
NOWIP="127.0.0.1"
echo "  IP = $NOWIP"

declare -A VALIDATOR_PUB_KEY_HEX=();
declare -A VALIDATOR_PUB_KEY_BASE64=();

# Fake config just to start nodes
cat $TEST_ROOT/ton-global.config_1.json > $NODE_TARGET/ton-global.config.json
cat $TEST_ROOT/ton-global.config_2.json >> $NODE_TARGET/ton-global.config.json

rm -rd tmp > /dev/null 2>&1
mkdir tmp

# 0 is full node
for (( N=0; N <= $NODES; N++ ))
do
    cd $NODE_TARGET

    echo "Cleaning up #$N..."
    rm -r -d node_db_$N > /dev/null 2>&1
    rm -r -d configs_$N > /dev/null 2>&1
    rm /shared/output_$N.log > /dev/null 2>&1


    echo "Validator's #$N config generating..."

    pkill -9 ton_node

    $TOOLS_ROOT/target/release/keygen > $TEST_ROOT/tmp/genkey$N
    jq -c .public $TEST_ROOT/tmp/genkey$N > console_public_json
    rm config.json > /dev/null 2>&1
    rm default_config.json > /dev/null 2>&1
    rm console_config.json > /dev/null 2>&1
    sed "s/NODE_NUM/$N/g" $TEST_ROOT/log_cfg.yml > $NODE_TARGET/log_cfg_$N.yml

    if [ $N -ne 0 ]; then
        cp $TEST_ROOT/default_config.json default_config.json
    else 
        cp $TEST_ROOT/default_config_fullnode.json default_config.json
    fi

    sed "s/nodenumber/$N/g" default_config.json > default_config$N.json
    sed "s/0.0.0.0/$NOWIP/g" default_config$N.json > default_config.json
    PORT=$(( 3000 + $N ))
    sed "s/main_port/$PORT/g" default_config.json > default_config$N.json
    PORT=$(( 4920 + $N ))
    sed "s/control_port/$PORT/g" default_config$N.json > default_config.json
    
    if [ "$WORKCHAINS" == "true" ]
    then
        if [ $N -gt 10 ]
        then
            sed "s/workchain_id/\"workchain\": 1,/g" default_config.json > default_config$N.json
        elif [ $N -gt 5 ] || [ $N -eq 0 ]
        then
            sed "s/workchain_id/\"workchain\": 0,/g" default_config.json > default_config$N.json
        else
            sed "s/workchain_id/\"workchain\": -1,/g" default_config.json > default_config$N.json
        fi
    else
        sed "s/workchain_id//g" default_config.json > default_config$N.json
    fi

    cp default_config$N.json default_config.json
    cp $NODE_TARGET/default_config$N.json $TEST_ROOT/tmp/default_config$N.json

    rm tmp_output > /dev/null 2>&1
    ./ton_node --configs . --ckey "$(cat console_public_json)" > tmp_output &
    echo "  waiting for 20 secs"
    sleep 20
    if [ ! -f "console_config.json" ]; then
        echo "ERROR: console_config.json does not exist"
        exit 1
    fi


    cp console_config.json $TEST_ROOT/tmp/console$N.json
    cd $TOOLS_ROOT/target/release/
    jq ".client_key = $(jq .private $TEST_ROOT/tmp/genkey$N)" "$TEST_ROOT/tmp/console$N.json" > "$TEST_ROOT/tmp/console$N.tmp.json"
    jq ".config = $(cat $TEST_ROOT/tmp/console$N.tmp.json)" "$TEST_ROOT/console-template.json" > "$TEST_ROOT/tmp/console$N.json"
    rm $TEST_ROOT/tmp/console$N.tmp.json

    rm tmp_output_console > /dev/null 2>&1
    
    # 0 is full node
    if [ $N -ne 0 ]; then
        CONSOLE_OUTPUT=$(./console -C $TEST_ROOT/tmp/console$N.json -c newkey | cut -c 92-)
        ./console -C $TEST_ROOT/tmp/console$N.json -c "addpermkey ${CONSOLE_OUTPUT} ${NOWDATE} 1610000000" > tmp_output_console
        CONSOLE_OUTPUT=$(./console -C $TEST_ROOT/tmp/console$N.json -c "exportpub ${CONSOLE_OUTPUT}")
        # echo $CONSOLE_OUTPUT
        VALIDATOR_PUB_KEY_HEX[$N]=$(echo "${CONSOLE_OUTPUT}" | grep 'imported key:' | awk '{print $3}')
        # VALIDATOR_PUB_KEY_BASE64[$N]=$(echo "${CONSOLE_OUTPUT}" | grep 'imported key:' | awk '{print $4}')
        # echo "INFO: VALIDATOR_PUB_KEY_HEX[$N] = ${VALIDATOR_PUB_KEY_HEX[$N]}"
        # echo "INFO: VALIDATOR_PUB_KEY_BASE64[$N] = ${VALIDATOR_PUB_KEY_BASE64[$N]}"
    fi

    cp $NODE_TARGET/config.json $TEST_ROOT/tmp/config$N.json

    pkill -9 ton_node

done

echo "Zerostate generating..."

if [ "$WORKCHAINS" == "true" ]
then
    sed "s/nowdate/$NOWDATE/g" $TEST_ROOT/zero_state_blanc_1_2wcs.json > $TEST_ROOT/tmp/zero_state_tmp.json
else
    sed "s/nowdate/$NOWDATE/g" $TEST_ROOT/zero_state_blanc_1.json > $TEST_ROOT/tmp/zero_state_tmp.json
fi

WEIGHT=10
TOTAL_WEIGHT=$(( $NODES * 2 ))
sed "s/p34_total_weight/$NODES/g" $TEST_ROOT/tmp/zero_state_tmp.json > $TEST_ROOT/tmp/zero_state_tmp_2.json
sed "s/p34_total/$NODES/g" $TEST_ROOT/tmp/zero_state_tmp_2.json > $TEST_ROOT/tmp/zero_state.json

for (( N=1; N <= $NODES; N++ ))
do
    echo "  Validator #$N contract processing..."

    printf "{ \"public_key\": \"${VALIDATOR_PUB_KEY_HEX[$N]}\", \"weight\": \"$WEIGHT\"}" >> $TEST_ROOT/tmp/zero_state.json
    if [ ! $N -eq $NODES ]
    then
        printf ",\n" >> $TEST_ROOT/tmp/zero_state.json
    fi

done

cat $TEST_ROOT/zero_state_blanc_2.json >> $TEST_ROOT/tmp/zero_state.json

echo "  finish zerostate generating..."
./zerostate -i $TEST_ROOT/tmp/zero_state.json
rm $NODE_TARGET/*.boc > /dev/null 2>&1
cp *.boc $NODE_TARGET
rm *.boc > /dev/null 2>&1

echo "Global config generating..."

cd $TEST_ROOT
cat ton-global.config_1.json >> tmp/ton-global.config.json
cd $TOOLS_ROOT/target/release

for (( N=1; N <= $NODES; N++ ))
do
    echo "  Validator #$N DHT key processing..."

    # DHT key is the first one in config (tag 1)
    KEYTAG=$(grep "pvt_key" $TEST_ROOT/tmp/config$N.json | head -n1 | cut -c 23-66)

    PORT=$(( 3000 + $N ))
    ./gendht $NOWIP:$PORT $KEYTAG >> $TEST_ROOT/tmp/ton-global.config.json
    
    if [ ! $N -eq $NODES ]
    then
        echo "," >> $TEST_ROOT/tmp/ton-global.config.json
    fi

done

cat $TEST_ROOT/ton-global.config_2.json >> $TEST_ROOT/tmp/ton-global.config.json
jq ".validator.zero_state = $(jq .zero_state $TOOLS_ROOT/target/release/config.json)" "$TEST_ROOT/tmp/ton-global.config.json" > "$TEST_ROOT/tmp/ton-global.config.json.tmp"
# Looks like jq contains bug which converts big number wrong way, rolling back:
sed "s/-9223372036854776000/-9223372036854775808/g" $TEST_ROOT/tmp/ton-global.config.json.tmp > $TEST_ROOT/tmp/ton-global.config.json
cp $TEST_ROOT/tmp/ton-global.config.json $NODE_TARGET/ton-global.config.json

echo "Starting nodes..."

cd $NODE_TARGET

for (( N=0; N <= $NODES; N++ ))
do
    echo "  Starting node #$N..."

    rm -r -d $NODE_TARGET/configs_$N > /dev/null 2>&1
    mkdir $NODE_TARGET/configs_$N
    cp $TEST_ROOT/tmp/config$N.json $NODE_TARGET/configs_$N/config.json
    cp $TEST_ROOT/tmp/default_config$N.json $NODE_TARGET/configs_$N/default_config.json
    cp $TEST_ROOT/tmp/console$N.json $NODE_TARGET/configs_$N/console.json
    sed "s/NODE_NUM/$N/g" $TEST_ROOT/log_cfg.yml > $NODE_TARGET/configs_$N/log_cfg_$N.yml
    cp $TEST_ROOT/tmp/ton-global.config.json $NODE_TARGET/configs_$N/ton-global.config.json

    # rm /shared/output_$N.log
    ./ton_node --configs configs_$N -z . > "/shared/node_$N.output" 2>&1 &

done

date
echo "Waiting 10mins for 200th master block"
sleep 600

function find_block {
    for (( N=1; N <= $NODES; N++ ))
    do
        if cat "/shared/output_$N.log" | egrep -q "Applied(.*)$1"
        then
            echo "Applied block ($1) - FOUND on node #$N!"
        else
            echo "ERROR: Can't find applied block ($1) on node #$N!"
        fi
    done
}

find_block "-1\:8000000000000000, 200"

if [ ! "$REMP_TEST" == "true" ]
then
    echo "Waiting more for all shard's 200th blocks"
    sleep 600
    find_block "0\:2000000000000000, 200"
    find_block "0\:6000000000000000, 200"
    find_block "0\:a000000000000000, 200"
    find_block "0\:e000000000000000, 200"

    if [ "$WORKCHAINS" == "true" ]
    then
        find_block "1\:2000000000000000, 200"
        find_block "1\:6000000000000000, 200"
        find_block "1\:a000000000000000, 200"
	find_block "1\:e000000000000000, 200"
    fi
fi

# pkill ton_node


echo "TEST PASSED"
