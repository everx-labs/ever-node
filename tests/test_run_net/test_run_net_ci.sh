TEST_ROOT=
# max - 9 nodes
NODES=6
NODE_ROOT=
TOOLS_ROOT=
NODE_PATH=
TOOLS_PATH=
NODE_BIN=
NODE_BIN_NAME=ton_node

function getContainerEnv {
    if [ -e /ton-node/ton_node_no_kafka ]
    then
        NODE_ROOT=/tonlabs/ton-node
        if [ -e /tonlabs/ton-node-tools ]
        then
            TOOLS_ROOT=/tonlabs/ton-node-tools
            if [ -e /tonlabs/ton-node-tools/console ]
            then
                TOOLS_PATH=/tonlabs/ton-node-tools/
            fi
        fi
    fi
    FINAL_TIMEOUT=300
}

function prepare {
    cd $(dirname "$0") > /dev/null
    export TEST_ROOT=$(pwd)
    cd - > /dev/null

    echo "Preparations..."
    # check if we were started in container
    getContainerEnv
    # calc nodes src path
    if [ "$NODE_ROOT" == "" ] ; then 
        if [ -e "$TEST_ROOT/../../Cargo.toml" ] ; then 
            cd "$TEST_ROOT/../.." > /dev/null
            NODE_ROOT="$(pwd)"
            cd - > /dev/null
        else
            echo "Unable to find node src folder"
            exit 1
        fi
    fi
    # calc nodes targets path
    if [ "$NODE_PATH" == "" ] ; then
        if [ -e "$TEST_ROOT/target/release" ] ; then 
            cd "$TEST_ROOT/target/release" > /dev/null
            NODE_PATH="$(pwd)"
            cd - > /dev/null
        else
            cd "$NODE_ROOT" > /dev/null
            cargo build --release
            NODE_PATH="$NODE_ROOT/target/release"
            cd - > /dev/null
        fi
    fi
    # calc nodes bin path
    if [ "$NODE_BIN" == "" ] ; then
        if [ -e "$NODE_PATH/$NODE_BIN_NAME" ] ; then 
            NODE_BIN="$NODE_PATH/$NODE_BIN_NAME"
        else
            echo "Unable to find ton_node binary"
            exit 1
        fi
    fi
    # calc tools src path
    if [ "$TOOLS_ROOT" == "" ] ; then
        if [ -e "$NODE_ROOT/../ton-node-tools" ] ; then 
            cd "$NODE_ROOT/../ton-node-tools" > /dev/null
            TOOLS_ROOT="$(pwd)"
            git submodule init
            git submodule update
            cd - > /dev/null
        else
            git clone --recurse-submodules "git@github.com:tonlabs/ton-node-tools.git" "$NODE_ROOT/../ton-node-tools"
            cd "$NODE_ROOT/../ton-node-tools" > /dev/null
            TOOLS_ROOT="$(pwd)"
            cd - > /dev/null
        fi
    fi
    # calc tools targets path
    if [ "$TOOLS_PATH" == "" ] ; then
        if [ -e "$TOOLS_ROOT/target/release" ] ; then 
            cd "$TOOLS_ROOT/target/release" > /dev/null
            TOOLS_PATH="$(pwd)"
            cd - > /dev/null
        else
            if [ -e "$TOOLS_ROOT/console" ] ; then
                TOOLS_PATH="$TOOLS_ROOT"
            else
                cd "$TOOLS_ROOT" > /dev/null
                cargo build --release
                TOOLS_PATH="$TOOLS_ROOT/target/release"
                cd - > /dev/null
            fi
        fi
    fi
    # echo result
    echo "NODES $NODES"
    echo "TEST_ROOT $TEST_ROOT"
    echo "NODE_ROOT $NODE_ROOT"
    echo "TOOLS_ROOT $TOOLS_ROOT"
    echo "NODE_PATH $NODE_PATH"
    echo "TOOLS_PATH $TOOLS_PATH"
    echo "NODE_BIN $NODE_BIN"
    # install prerequesties
    apt update
    apt install -y jq
    # kill previous processes
    pkill -9 $NODE_BIN_NAME
    #remove junk
    cd $TEST_ROOT > /dev/null
    ./remove_junk.sh
    cd -
}

prepare

cd $TEST_ROOT
NOWDATE=$(date +"%s")
# NOWIP=$(curl ifconfig.me)
NOWIP="127.0.0.1"
echo "  IP = $NOWIP"

declare -A VALIDATOR_PUB_KEY_HEX=();
declare -A VALIDATOR_PUB_KEY_BASE64=();

# Fake config just to start nodes
cat $TEST_ROOT/ton-global.config_1.json > $NODE_PATH/ton-global.config.json
cat $TEST_ROOT/ton-global.config_2.json >> $NODE_PATH/ton-global.config.json

for (( N=1; N <= $NODES; N++ ))
do
    cd $NODE_PATH

    echo "Cleaning up #$N..."
    rm -rfd node_db_$N
    rm -rfd configs_$N


    echo "Validator's #$N config generating..."

    pkill -9 $NODE_BIN_NAME

    $TOOLS_PATH/keygen > $TEST_ROOT/genkey$N
    jq -c .public $TEST_ROOT/genkey$N > console_public_json
    rm config.json
    rm default_config.json
    rm console_config.json
    sed "s/NODE_NUM/$N/g" $TEST_ROOT/log_cfg.yml > $NODE_PATH/log_cfg_$N.yml

    cp $TEST_ROOT/default_config.json default_config.json
    sed "s/nodenumber/$N/g" default_config.json > default_config$N.json
    sed "s/0.0.0.0/$NOWIP/g" default_config$N.json > default_config.json
    PORT=$(( 3000 + $N ))
    sed "s/main_port/$PORT/g" default_config.json > default_config$N.json
    PORT=$(( 4920 + $N ))
    sed "s/control_port/$PORT/g" default_config$N.json > default_config.json
    cp $NODE_PATH/default_config.json $TEST_ROOT/default_config$N.json

    rm tmp_output
    $NODE_BIN --configs . --ckey "$(cat console_public_json)" > tmp_output &
    echo "  waiting for 20 secs"
    sleep 20
    if [ ! -f "console_config.json" ]; then
            echo "ERROR: console_config.json does not exist"
            exit 1
    fi


    cp console_config.json $TEST_ROOT/console$N.json
    cd $TOOLS_PATH/
    jq ".client_key = $(jq .private $TEST_ROOT/genkey$N)" "$TEST_ROOT/console$N.json" > "$TEST_ROOT/console$N.tmp.json"
    jq ".config = $(cat $TEST_ROOT/console$N.tmp.json)" "$TEST_ROOT/console-template.json" > "$TEST_ROOT/console$N.json"
    rm $TEST_ROOT/console$N.tmp.json
    CONSOLE_OUTPUT=$(./console -C $TEST_ROOT/console$N.json -c newkey | cut -c 92-)

    rm tmp_output_console
    ./console -C $TEST_ROOT/console$N.json -c "addpermkey ${CONSOLE_OUTPUT} ${NOWDATE} 1610000000" > tmp_output_console
    CONSOLE_OUTPUT=$(./console -C $TEST_ROOT/console$N.json -c "exportpub ${CONSOLE_OUTPUT}")
    # echo $CONSOLE_OUTPUT
    VALIDATOR_PUB_KEY_HEX[$N]=$(echo "${CONSOLE_OUTPUT}" | grep 'imported key:' | awk '{print $3}')
    # VALIDATOR_PUB_KEY_BASE64[$N]=$(echo "${CONSOLE_OUTPUT}" | grep 'imported key:' | awk '{print $4}')
    # echo "INFO: VALIDATOR_PUB_KEY_HEX[$N] = ${VALIDATOR_PUB_KEY_HEX[$N]}"
    # echo "INFO: VALIDATOR_PUB_KEY_BASE64[$N] = ${VALIDATOR_PUB_KEY_BASE64[$N]}"

    cp $NODE_PATH/config.json $TEST_ROOT/config$N.json

    pkill -9 $NODE_BIN_NAME

done

echo "Zerostate generating..."

rm $TEST_ROOT/zero_state.json
sed "s/nowdate/$NOWDATE/g" $TEST_ROOT/zero_state_blanc_1.json > $TEST_ROOT/zero_state_tmp.json
WEIGHT=10
TOTAL_WEIGHT=$(( $NODES * 2 ))
sed "s/p34_total_weight/$NODES/g" $TEST_ROOT/zero_state_tmp.json > $TEST_ROOT/zero_state_tmp_2.json
sed "s/p34_total/$NODES/g" $TEST_ROOT/zero_state_tmp_2.json > $TEST_ROOT/zero_state.json

for (( N=1; N <= $NODES; N++ ))
do
    echo "  Validator #$N contract processing..."

    printf "{ \"public_key\": \"${VALIDATOR_PUB_KEY_HEX[$N]}\", \"weight\": \"$WEIGHT\"}" >> $TEST_ROOT/zero_state.json
    if [ ! $N -eq $NODES ]
    then
        printf ",\n" >> $TEST_ROOT/zero_state.json
    fi

done

cat $TEST_ROOT/zero_state_blanc_2.json >> $TEST_ROOT/zero_state.json

echo "  finish zerostate generating..."
./zerostate -i $TEST_ROOT/zero_state.json
rm $NODE_PATH/*.boc
cp *.boc $NODE_PATH
rm *.boc

echo "Global config generating..."

cd $TEST_ROOT
rm ton-global.config.json
cat ton-global.config_1.json >> ton-global.config.json
cd $TOOLS_PATH

for (( N=1; N <= $NODES; N++ ))
do
    echo "  Validator #$N DHT key processing..."

    # DHT key is the first one in config (tag 1)
    KEYTAG=$(grep "pvt_key" $TEST_ROOT/config$N.json | head -n1 | cut -c 23-66)

    PORT=$(( 3000 + $N ))
    ./gendht $NOWIP:$PORT $KEYTAG >> $TEST_ROOT/ton-global.config.json
    
    if [ ! $N -eq $NODES ]
    then
        echo "," >> $TEST_ROOT/ton-global.config.json
    fi

done

cat $TEST_ROOT/ton-global.config_2.json >> $TEST_ROOT/ton-global.config.json
jq ".validator.zero_state = $(jq .zero_state $TOOLS_PATH/config.json)" "$TEST_ROOT/ton-global.config.json" > "$TEST_ROOT/ton-global.config.json.tmp"
# Looks like jq contains bug which converts big number wtong way, rolling back:
sed "s/-9223372036854776000/-9223372036854775808/g" $TEST_ROOT/ton-global.config.json.tmp > $TEST_ROOT/ton-global.config.json
cp $TEST_ROOT/ton-global.config.json $NODE_PATH/ton-global.config.json

echo "Starting nodes..."

cd $NODE_PATH

cargo build --release

for (( N=1; N <= $NODES; N++ ))
do
    echo "  Starting node #$N..."

    rm -r -d $NODE_PATH/configs_$N
    mkdir $NODE_PATH/configs_$N
    cp $TEST_ROOT/config$N.json $NODE_PATH/configs_$N/config.json
    cp $TEST_ROOT/default_config$N.json $NODE_PATH/configs_$N/default_config.json
    sed "s/NODE_NUM/$N/g" $TEST_ROOT/log_cfg.yml > $NODE_PATH/configs_$N/log_cfg_$N.yml
    cp $TEST_ROOT/ton-global.config.json $NODE_PATH/configs_$N/ton-global.config.json

    rm /shared/output_$N.log
    $NODE_BIN --configs configs_$N -z . > /shared/output_$N.log 2>&1 &

done

function find_block {
    LOOP_RES=0
    for (( N=1; N <= $NODES; N++ ))
    do
        if grep -R -q --include="output_$N*" "Applied block ($1" /shared/
        then
            if [ "$2" != "LOOP" ]
            then
                echo "Applied block ($1) - FOUND on node #$N!"
            else
                ((LOOP_RES++))
            fi
        else
            if [ "$2" != "LOOP" ]
            then
                echo "ERROR: Can't find applied block ($1) on node #$N!"
                PID=$(ps ax | grep configs_$N | grep -v grep | awk '{print $1}')
                gdb -p $PID -ex "thread apply all bt" -ex "detach" -ex "quit" > "output_"$N"_trace.log"
                exit 1
            fi
        fi
    done
    if [ "$2" == "LOOP" ]
    then
        echo $LOOP_RES
    fi
}

echo "Waiting for first master block 10 minutes"
sleep 600
find_block '-1:8000000000000000, 1' ''

echo "Waiting for 200th master block"

until [ "$(find_block '-1:8000000000000000, 200' 'LOOP')" == "$NODES" ]
do
    sleep 10
done
find_block "-1:8000000000000000, 200"

echo "Waiting more for all 8 shard's 200th blocks"

until [ "$(find_block '0\:1000000000000000, 200' 'LOOP')" == "$NODES" ]
do
    sleep 10
done
find_block "0\:1000000000000000, 200"

until [ "$(find_block '0\:3000000000000000, 200' 'LOOP')" == "$NODES" ]
do
    sleep 10
done
find_block "0\:3000000000000000, 200"

until [ "$(find_block '0\:5000000000000000, 200' 'LOOP')" == "$NODES" ]
do
    sleep 10
done
find_block "0\:5000000000000000, 200"

until [ "$(find_block '0\:7000000000000000, 200' 'LOOP')" == "$NODES" ]
do
    sleep 10
done
find_block "0\:7000000000000000, 200"

until [ "$(find_block '0\:9000000000000000, 200' 'LOOP')" == "$NODES" ]
do
    sleep 10
done
find_block "0\:9000000000000000, 200"

until [ "$(find_block '0\:b000000000000000, 200' 'LOOP')" == "$NODES" ]
do
    sleep 10
done
find_block "0\:b000000000000000, 200"

until [ "$(find_block '0\:d000000000000000, 200' 'LOOP')" == "$NODES" ]
do
    sleep 10
done
find_block "0\:d000000000000000, 200"

until [ "$(find_block '0\:f000000000000000, 200' 'LOOP')" == "$NODES" ]
do
    sleep 10
done
find_block "0\:f000000000000000, 200"

echo "TEST PASSED"