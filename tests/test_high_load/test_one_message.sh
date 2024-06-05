source ../setup_test_paths.sh

RUN_TEST_ROOT=../test_run_net/
TEST_ROOT=$(pwd)
mkdir logs
export ROOT=$(pwd)/logs
export REMP_TEST=true
DELAY=20

cd $RUN_TEST_ROOT
if ! $RUN_TEST_ROOT/test_run_net.sh 
then
    exit 1
fi

cd ../../../
if ! [ -d "tonos-cli" ]
then
    git clone "git@github.com:tonlabs/tonos-cli.git"
fi
cd tonos-cli
cargo build --release
cd target/release
CLI=$(pwd)

cd $TEST_ROOT

echo "Press ENTER to start transactions"
read -r input

echo "Preparation (0/6) deploing giver"
$CLI/tonos-cli \
    message "-1:7777777777777777777777777777777777777777777777777777777777777777" \
    constructor '{
        "owners":["0xc86b504dbbcf2263c6d5985743f1b248eca31d9ae37d273102ddd2b6ccd95c8a"],
        "reqConfirms":1
    }' \
    --abi SetcodeMultisigWallet.abi.json \
    --sign deploy.keys.2.json \
    --output "$TEST_ROOT/msg0.boc" \
    --raw

echo $TOOLS_DIR/console -C ${CONSOLE_CONFIG_0} -c "sendmessage $TEST_ROOT/msg0.boc"
$TOOLS_DIR/console -C ${CONSOLE_CONFIG_0} -c "sendmessage $TEST_ROOT/msg0.boc"

while true; do
    echo "Enter amount of money to transfer: "
    read -r input
    echo "Preparation (1/6) sending money to Brazil contract"
    $CLI/tonos-cli \
        message "-1:7777777777777777777777777777777777777777777777777777777777777777" \
        submitTransaction "{
            \"dest\" : \"0:4a5ed5323d510dedb4cd8b6bf322b89b73eaec0b359ce46e3557c46499f2d64c\",
            \"value\":\"$input\",
            \"bounce\":false,
            \"allBalance\":false,
            \"payload\":\"\"
        }" \
        --abi SafeMultisigWallet.abi.json \
        --sign msig.keys.json \
        --output "$TEST_ROOT/msg1.boc" \
        --raw
    $TOOLS_DIR/console -C ${CONSOLE_CONFIG_0} -c "sendmessage $TEST_ROOT/msg1.boc"
done

pkill ton_node
