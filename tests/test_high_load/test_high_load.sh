TOOLS=../../target/release/
RUN_TEST_ROOT=../test_run_net/
TEST_ROOT=$(pwd)
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
$TOOLS/console -C $RUN_TEST_ROOT/console1.json -c "sendmessage $TEST_ROOT/msg0.boc"
echo "      waiting for $DELAY sec"
sleep $DELAY

echo "Preparation (1/6) sending money to Brazil contract"
$CLI/tonos-cli \
    message "-1:7777777777777777777777777777777777777777777777777777777777777777" \
    submitTransaction '{
        "dest" : "0:4a5ed5323d510dedb4cd8b6bf322b89b73eaec0b359ce46e3557c46499f2d64c",
        "value":"3000000000000000",
        "bounce":false,
        "allBalance":false,
        "payload":""
    }' \
    --abi SafeMultisigWallet.abi.json \
    --sign msig.keys.json \
    --output "$TEST_ROOT/msg1.boc" \
    --raw
$TOOLS/console -C $RUN_TEST_ROOT/console1.json -c "sendmessage $TEST_ROOT/msg1.boc"
echo "      waiting for $DELAY sec"
sleep $DELAY

echo "Preparation (2/6) deploing Brazil contract"
$CLI/tonos-cli \
    deploy_message \
    Brazil.tvc \
    --abi Brazil.abi.json \
    --sign deploy.keys.json {} \
    --raw \
    --output "$TEST_ROOT/msg2.boc"
$TOOLS/console -C $RUN_TEST_ROOT/console1.json -c "sendmessage $TEST_ROOT/msg2.boc"
echo "      waiting for $DELAY sec"
sleep $DELAY

echo "Preparation (3/6) setting code"
$CLI/tonos-cli \
    message '0:4a5ed5323d510dedb4cd8b6bf322b89b73eaec0b359ce46e3557c46499f2d64c' --abi Brazil.abi.json \
    setCode2 '{"c":"te6ccgECGgEABBUAAib/APSkICLAAZL0oOGK7VNYMPShBwEBCvSkIPShAgIDzsAGAwIBSAUEAFU7UTQ0//TP9MA1fpA+G74bNJ/+kDTf9cLf/hv+G34a/hqf/hh+Gb4Y/higAFs+ELIy//4Q88LP/hGzwsAyPhM+E4Czs74SvhL+E34T15AzxHKf87Lf8t/ye1UgAPNn4SpLbMOH4J28QghA7msoAuY5A+E+ktX/4b/hPwg+OMnD4b/hOyM+FiM6NBA5iWgAAAAAAAAAAAAAAAAABzxbPgc+Bz5EVvcFC+E3PC3/JcfsA3t74S8jPhQjOjQRQFXUqAAAAAAAAAAAAAAAAAAHPFs+Bz4HJc/sAgIBIAoIAbT/f40IYAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABPhpIe1E0CDXScIBjifT/9M/0wDV+kD4bvhs0n/6QNN/1wt/+G/4bfhr+Gp/+GH4Zvhj+GIJAbKOgOLTAAGfgQIA1xgg+QFY+EL5EPKo3tM/AY4e+EMhuSCfMCD4I4ED6KiCCBt3QKC53pL4Y+CANPI02NMfIcEDIoIQ/////byxk1vyPOAB8AH4R26TMPI83hECASATCwIBIA0MACm6YBoLL4QW6S8AXe0XD4avAEf/hngBD7oh5WFfhBboDgFsjoDe+Ebyc3H4ZvpA1w1/ldTR0NN/39Eg+G34ACH4a/hJ+G5wkyDBCpXwAaS1f+gwW/AEf/hnDwFi7UTQINdJwgGOJ9P/0z/TANX6QPhu+GzSf/pA03/XC3/4b/ht+Gv4an/4Yfhm+GP4YhABBo6A4hEB/vQFcPhqjQhgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAE+GuNCGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAT4bHD4bY0IYAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABPhucPhvcAESADSAQPQO8r3XC//4YnD4Y3D4Zn/4YXH4anD4bwIBIBcUARm7eoGUD4QW6S8AXe0YFQH+jnn4SpLbMOH4J28QghA7msoAuY5A+E+ktX/4b/hPwg+OMnD4b/hOyM+FiM6NBA5iWgAAAAAAAAAAAAAAAAABzxbPgc+Bz5EVvcFC+E3PC3/JcfsA3t74S8jPhQjOjQRQFXUqAAAAAAAAAAAAAAAAAAHPFs+Bz4HJc/sA2PAEfxYABPhnAgFiGRgAQ7TOBrf8ILdJeALvaLj8NThJkGCFSvgA0lq/9Bh4Aj/8M8AAotpwItDTA/pAMPhpqTgA3CHHACCcMCHTHyHAACCSbCHe344RcfAB8AX4SfhLxwWS8AHe8ATgIcEDIoIQ/////byxk1vyPOAB8AH4R26TMPI83g=="}' \
    --output "$TEST_ROOT/msg3.boc" \
    --raw
$TOOLS/console -C $RUN_TEST_ROOT/console1.json -c "sendmessage $TEST_ROOT/msg3.boc"
echo "      waiting for $DELAY sec"
sleep $DELAY

echo "Preparation (4/6) setting data"
$CLI/tonos-cli \
    message '0:4a5ed5323d510dedb4cd8b6bf322b89b73eaec0b359ce46e3557c46499f2d64c' --abi Brazil.abi.json \
    setData2 '{"c":"te6ccgEBAgEAKAABAcABAEPQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAg"}' \
    --output "$TEST_ROOT/msg4.boc" \
    --raw
$TOOLS/console -C $RUN_TEST_ROOT/console1.json -c "sendmessage $TEST_ROOT/msg4.boc"
echo "      waiting for $DELAY sec"
sleep $DELAY

echo "Preparation (5/6) building"
$CLI/tonos-cli \
    message '0:4a5ed5323d510dedb4cd8b6bf322b89b73eaec0b359ce46e3557c46499f2d64c' --abi Brazil.abi.json  \
    build '{}'  \
    --output "$TEST_ROOT/msg5.boc" \
    --raw
$TOOLS/console -C $RUN_TEST_ROOT/console1.json -c "sendmessage $TEST_ROOT/msg5.boc"
echo "      waiting for $DELAY sec"
sleep $DELAY

echo "Preparation (6/6) setting grant"
$CLI/tonos-cli \
    message '0:4a5ed5323d510dedb4cd8b6bf322b89b73eaec0b359ce46e3557c46499f2d64c' --abi Brazil.abi.json  \
    setdGrant '{"dgrant":3000000000}' \
    --output "$TEST_ROOT/msg6.boc" \
    --raw
$TOOLS/console -C $RUN_TEST_ROOT/console1.json -c "sendmessage $TEST_ROOT/msg6.boc"
echo "      waiting for $DELAY sec"
sleep $DELAY

echo "Starting load..."
COUNTER=20
N=0
MAX=30
DELAY=10
while [ $N -lt $(($MAX + 1)) ]
do
    echo "($N/$MAX) senging message"
    $CLI/tonos-cli \
        message '0:4a5ed5323d510dedb4cd8b6bf322b89b73eaec0b359ce46e3557c46499f2d64c' \
        --abi Brazil.abi.json \
        setC {\"counter\":$COUNTER} \
        --output "$TEST_ROOT/msg.boc" \
        --raw
    $TOOLS/console -C $RUN_TEST_ROOT/console1.json -c "sendmessage $TEST_ROOT/msg.boc"
    sleep $DELAY
    (( N++ ))
    COUNTER=$(($COUNTER + 20))
done

HILOAD_DURATION=30
echo "Working $HILOAD_DURATION mins under hiload"
N=0
while [ $N -lt $HILOAD_DURATION ]
do
    sleep 60
    (( N++ ))
    tail -n 1000 "/shared/output_1.log" | grep COLLATED
    echo "Working $HILOAD_DURATION mins under hiload ($N mins done)"
done

echo "Checking if the blockchain is alive"

function find_new_block_of_shard {
    N=1
    while [ $N -lt 6 ]
    do
        if tail -n 2000 "/shared/output_$N.log" | grep -q "Applied block ($1"
        then
            echo "New applied block of shard ($1) - FOUND on node #$N!"
        else
            echo "ERROR: Can't find new applied block of shard ($1) on node #$N!"
            exit 1
        fi
        (( N++ ))
    done
}

find_new_block_of_shard "0\:1000000000000000"
find_new_block_of_shard "0\:3000000000000000"
find_new_block_of_shard "0\:5000000000000000"
find_new_block_of_shard "0\:7000000000000000"
find_new_block_of_shard "0\:9000000000000000"
find_new_block_of_shard "0\:b000000000000000"
find_new_block_of_shard "0\:d000000000000000"
find_new_block_of_shard "0\:f000000000000000"

echo "TEST PASSED. Killing nodes"

pkill ton_node
