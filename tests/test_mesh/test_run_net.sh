NODES=5
WORKCHAINS=false
CURRENT_BRANCH="$(git branch --show-current)"
RUN_FULLNODE=false

set -e
handle_error() {
    echo "An error occurred on $0::$1"
    exit 1
}
trap 'handle_error $LINENO' ERR


GLOBAL_ID=$1
if [[ -z "$GLOBAL_ID" ]]
then
    GLOBAL_ID=42
fi

NODES_START_INDEX=$2
if [[ -z "$NODES_START_INDEX" ]]
then
    NODES_START_INDEX=0
fi

NO_CLEANUP=$3

echo "Current branch: $CURRENT_BRANCH"
echo "Global network id: $GLOBAL_ID"

# apt update
# apt install jq

if [ ! "$REMP_TEST" == "true" ]
then
    echo "No Remp testing: '$REMP_TEST'"
    REMP_TEST=false
else
    echo "Remp testing in progress. Make sure you have enabled a REMP capability in a zerostate."
fi

if [ "$WORKCHAINS" == "true" ]
then
    echo "Workchains are enabled!"
else
    echo "Workchains are NOT enabled!"
fi

# **************************************************************************************************
echo "Preparations..."

if [ ! "$NO_CLEANUP" == "no-cleanup" ]
then
    pkill -9 ton_node || true
fi

TEST_ROOT=$(pwd);
NODE_TARGET=$TEST_ROOT/../../target/release/

./remove_junk.sh $NODE_TARGET $NODES > /dev/null 2>&1 || true
echo "Building $(pwd)"

# cargo build --release

cd ../../../
if ! [ -d "ever-node-tools" ]
then
    git clone "https://github.com/tonlabs/ever-node-tools"
    cd ever-node-tools
    git submodule init
    git submodule up
date
fi
cd ever-node-tools
TOOLS_ROOT=$(pwd)

# cargo update
# echo "Building $(pwd)"
# cargo build --release
cd target/release/

cd $TEST_ROOT
NOWDATE=$(date +"%s")
# NOWIP=$(curl ifconfig.me)
NOWIP="127.0.0.1"
echo "  IP = $NOWIP"

declare -a VALIDATOR_PUB_KEY_HEX=();
declare -a VALIDATOR_PUB_KEY_BASE64=();

# Fake config just to start nodes
cat $TEST_ROOT/global_config_blank_1.json > $NODE_TARGET/global_config.json
cat $TEST_ROOT/global_config_blank_2.json >> $NODE_TARGET/global_config.json

if [ ! "$NO_CLEANUP" == "no-cleanup" ]
then
    mkdir tmp > /dev/null 2>&1 || true
    rm -rd tmp/* > /dev/null 2>&1 || true
    mkdir tmp/blocks/ > /dev/null 2>&1 || true
fi

# **************************************************************************************************
# Generating nodes keys & configs

# NODES_START_INDEX is full node
START=$NODES_START_INDEX
END=$(($NODES_START_INDEX + $NODES))
if [ "$RUN_FULLNODE" == "false" ]
then
    START=$(($NODES_START_INDEX + 1))
fi

for (( N=START; N <= $END; N++ ))
do
    cd $NODE_TARGET

    echo "Cleaning up #$N..."
    rm -r -d node_db_$N > /dev/null 2>&1 || true
    rm -r -d configs_$N > /dev/null 2>&1 || true

    echo "Validator's #$N config generating..."

    $TOOLS_ROOT/target/release/keygen > $TEST_ROOT/tmp/genkey$N
    jq -c .public $TEST_ROOT/tmp/genkey$N > console_public_json
    rm config.json > /dev/null 2>&1 || true
    rm default_config.json > /dev/null 2>&1 || true
    rm console_config.json > /dev/null 2>&1 || true

    sed "s/NODE_NUM/$N/g" $TEST_ROOT/log_cfg.yml > $NODE_TARGET/log_cfg_$N_tmp.yml
    TMP_DIR=$TEST_ROOT/tmp
    sed "s,LOGS_ROOT,$TMP_DIR,g" $NODE_TARGET/log_cfg_$N_tmp.yml > $NODE_TARGET/log_cfg_$N.yml

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
            WC="1"
        elif [ $N -gt 5 ] || [ $N -eq 0 ]
        then
            WC="0"
        else
            WC="-1"
        fi

        sed "s/workchain_id/\"workchain\": $WC,/g" default_config.json > default_config$N.json
    else
        sed "s/workchain_id//g" default_config.json > default_config$N.json
    fi

    cp default_config$N.json default_config.json
    cp $NODE_TARGET/default_config$N.json $TEST_ROOT/tmp/default_config$N.json

    rm tmp_output > /dev/null 2>&1 || true
    ./ton_node --configs . --ckey "$(cat console_public_json)" 2>&1 > tmp_output &
    NODE_PID=$!
    sleep 5
    if [ ! -f "console_config.json" ]; then
        echo "ERROR: console_config.json does not exist"
        exit 1
    fi

    cp console_config.json $TEST_ROOT/tmp/console$N.json
    cd $TOOLS_ROOT/target/release/
    jq ".client_key = $(jq .private $TEST_ROOT/tmp/genkey$N)" "$TEST_ROOT/tmp/console$N.json" > "$TEST_ROOT/tmp/console$N.tmp.json"
    jq ".config = $(cat $TEST_ROOT/tmp/console$N.tmp.json)" "$TEST_ROOT/console-template.json" > "$TEST_ROOT/tmp/console$N.json"
    rm $TEST_ROOT/tmp/console$N.tmp.json || true

    rm tmp_output_console > /dev/null 2>&1 || true

    # 0 is full node
    if [ $N -ne $NODES_START_INDEX ]; then
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

    # "-n" means kill only last run ton_node process
    kill -s 9 $NODE_PID

done

# **************************************************************************************************
echo "Zerostate generating..."

rm $TEST_ROOT/tmp/zero_state_tmp* 2>&1 || true

if [ "$WORKCHAINS" == "true" ]
then
    sed "s/nowdate/$NOWDATE/g" $TEST_ROOT/zero_state_blanc_1_2wcs.json > $TEST_ROOT/tmp/zero_state_tmp1.json
else
    sed "s/nowdate/$NOWDATE/g" $TEST_ROOT/zero_state_blanc_1.json > $TEST_ROOT/tmp/zero_state_tmp1.json
fi

sed "s/global_id_/$GLOBAL_ID/g" $TEST_ROOT/tmp/zero_state_tmp1.json > $TEST_ROOT/tmp/zero_state_tmp2.json

WEIGHT=10
TOTAL_WEIGHT=$(( $NODES * 2 ))
sed "s/p34_total_weight/$NODES/g" $TEST_ROOT/tmp/zero_state_tmp2.json > $TEST_ROOT/tmp/zero_state_tmp3.json
sed "s/p34_total/$NODES/g" $TEST_ROOT/tmp/zero_state_tmp3.json > $TEST_ROOT/tmp/zero_state_$GLOBAL_ID.json

for (( N=START; N <= $END; N++ ))
do
    echo "  Validator #$N contract processing..."

    printf "{ \"public_key\": \"${VALIDATOR_PUB_KEY_HEX[$N]}\", \"weight\": \"$WEIGHT\"}" >> $TEST_ROOT/tmp/zero_state_$GLOBAL_ID.json
    if [ ! $N -eq $END ]
    then
        printf ",\n" >> $TEST_ROOT/tmp/zero_state_$GLOBAL_ID.json
    fi

done

cat $TEST_ROOT/zero_state_blanc_2.json >> $TEST_ROOT/tmp/zero_state_$GLOBAL_ID.json

echo "Finish zerostate generating..."
if [ ! "$NO_CLEANUP" == "no-cleanup" ]
then
    rm $NODE_TARGET/*.boc > /dev/null 2>&1 || true
fi
rm *.boc > /dev/null 2>&1 || true
./zerostate -i $TEST_ROOT/tmp/zero_state_$GLOBAL_ID.json
cp *.boc $NODE_TARGET

# **************************************************************************************************
echo "Global config generating..."

cd $TEST_ROOT
cat global_config_blank_1.json >> tmp/global_config_$GLOBAL_ID.json
cd $TOOLS_ROOT/target/release

for (( N=START; N <= $END; N++ ))
do
    echo "  Validator #$N DHT key processing..."

    # DHT key is the first one in config (tag 1)
    KEYTAG=$(grep "pvt_key" $TEST_ROOT/tmp/config$N.json | head -n1 | cut -c 23-66)

    PORT=$(( 3000 + $N ))
    ./gendht $NOWIP:$PORT $KEYTAG >> $TEST_ROOT/tmp/global_config_$GLOBAL_ID.json
    
    if [ ! $N -eq $END ]
    then
        echo "," >> $TEST_ROOT/tmp/global_config_$GLOBAL_ID.json
    fi

done

cat $TEST_ROOT/global_config_blank_2.json >> $TEST_ROOT/tmp/global_config_$GLOBAL_ID.json
jq ".validator.zero_state = $(jq .zero_state $TOOLS_ROOT/target/release/config.json)" "$TEST_ROOT/tmp/global_config_$GLOBAL_ID.json" > "$TEST_ROOT/tmp/global_config_$GLOBAL_ID.json.tmp"
# Looks like jq contains bug which converts big number wrong way, rolling back:
sed "s/-9223372036854776000/-9223372036854775808/g" $TEST_ROOT/tmp/global_config_$GLOBAL_ID.json.tmp > $TEST_ROOT/tmp/global_config_$GLOBAL_ID.json


# **************************************************************************************************
echo "Starting nodes..."

cd $NODE_TARGET

for (( N=START; N <= $END; N++ ))
do
    echo "  Starting node #$N..."

    rm -r -d $NODE_TARGET/configs_$N > /dev/null 2>&1 || true
    mkdir $NODE_TARGET/configs_$N
    cp $TEST_ROOT/tmp/config$N.json $NODE_TARGET/configs_$N/config.json
    cp $TEST_ROOT/tmp/default_config$N.json $NODE_TARGET/configs_$N/default_config.json
    cp $TEST_ROOT/tmp/console$N.json $NODE_TARGET/configs_$N/console.json
    sed "s/NODE_NUM/$N/g" $TEST_ROOT/log_cfg.yml > $NODE_TARGET/configs_$N/log_cfg_$N.tmp.yml
    TMP_DIR=$TEST_ROOT/tmp
    sed "s,LOGS_ROOT,$TMP_DIR,g" $NODE_TARGET/configs_$N/log_cfg_$N.tmp.yml > $NODE_TARGET/configs_$N/log_cfg_$N.yml
    cp $TEST_ROOT/tmp/global_config_$GLOBAL_ID.json $NODE_TARGET/configs_$N/global_config.json

    ./ton_node --configs configs_$N -z . > "$TMP_DIR/node_$N.output" 2>&1 &

done

date
echo "Started"

exit 0
