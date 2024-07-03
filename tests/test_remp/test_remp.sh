source "../setup_test_paths.sh"

contracts_count=20
messages_count=50
timeout_sec=0.5
prime_numbers=(
    1091 1093 1097 1103 1109 1117 1123 1129 1151 1153 1163 1171 1181 1187 1193 1201 1213 1217 1223
    1231 1237 1249 1259 1277 1279 1283 1289 1291 1297 1301 1303 1307 1319 1321 1327 1361 1367 1373
    1399 1409 1423 1427 1429 1433 1439 1447 1451 1453 1459 1471 1481 1483 1487 1489 1493 1499 1511
    1531 1543 1549 1553 1559 1567 1571 1579 1583 1597 1601 1607 1609 1613 1619 1621 1627 1637 1657
    1667 1669 1693 1697 1699 1709 1721 1723 1733 1741 1747 1753 1759 1777 1783 1787 1789 1801 1811
    1831 1847 1861 1867 1871 1873 1877 1879 1889 1901 1907 1913 1931 1933 1949 1951 1973 1979 1987
    1997 1999 2003 2011 2017 2027 2029 2039 2053 2063 2069 2081 2083 2087 2089 2099 2111 2113 2129
    2137 2141 2143 2153 2161 2179 2203 2207 2213 2221 2237 2239 2243 2251 2267 2269 2273 2281 2287
    2297 2309 2311 2333 2339 2341 2347 2351 2357 2371 2377 2381 2383 2389 2393 2399 2411 2417 2423
    2441 2447 2459 2467 2473 2477 2503 2521 2531 2539 2543 2549 2551 2557 2579 2591 2593 2609 2617
    2633 2647 2657 2659 2663 2671 2677 2683 2687 2689 2693 2699 2707 2711 2713 2719 2729 2731 2741
    2753 2767 2777 2789 2791 2797 2801 2803 2819 2833 2837 2843 2851 2857 2861 2879 2887 2897 2903
    2917 2927 2939 2953 2957 2963 2969 2971 2999 3001 3011 3019 3023 3037 3041 3049 3061 3067 3079
    3089 3109 3119 3121 3137 3163 3167 3169 3181 3187 3191 3203 3209 3217 3221 3229 3251 3253 3257
    3271 3299 3301 3307 3313 3319 3323 3329 3331 3343 3347 3359 3361 3371 3373 3389 3391 3407 3413
    3449 3457 3461 3463 3467 3469 3491 3499 3511 3517 3527 3529 3533 3539 3541 3547 3557 3559 3571
    3583 3593 3607 3613 3617 3623 3631 3637 3643 3659 3671 3673 3677 3691 3697 3701 3709 3719 3727
    3739 3761 3767 3769 3779 3793 3797 3803 3821 3823 3833 3847 3851 3853 3863 3877 3881 3889 3907
    3917 3919 3923 3929 3931 3943 3947 3967 3989 4001 4003 4007 4013 4019 4021 4027 4049 4051 4057
    4079 4091 4093 4099 4111 4127 4129 4133 4139 4153 4157 4159 4177 4201 4211 4217 4219 4229 4231
    4243 4253 4259 4261 4271 4273 4283 4289 4297 4327 4337 4339 4349 4357 4363 4373 4391 4397 4409
    4423 4441 4447 4451 4457 4463 4481 4483 4493 4507 4513 4517 4519 4523 4547 4549 4561 4567 4583
    4597 4603 4621 4637 4639 4643 4649 4651 4657 4663 4673 4679 4691 4703 4721 4723 4729 4733 4751
    4783 4787 4789 4793 4799 4801 4813 4817 4831 4861 4871 4877 4889 4903 4909 4919 4931 4933 4937
    4951 4957 4967 4969 4973 4987 4993 4999 5003 5009 5011 5021 5023 5039 5051 5059 5077 5081 5087
    5101 5107 5113 5119 5147 5153 5167 5171 5179 5189 5197 5209 5227 5231 5233 5237 5261 5273 5279
    5297 5303 5309 5323 5333 5347 5351 5381 5387 5393 5399 5407 5413 5417 5419 5431 5437 5441 5443
    5471 5477 5479 5483 5501 5503 5507 5519 5521 5527 5531 5557 5563 5569 5573 5581 5591 5623 5639
    5647 5651 5653 5657 5659 5669 5683 5689 5693 5701 5711 5717 5737 5741 5743 5749 5779 5783 5791
    5807 5813 5821 5827 5839 5843 5849 5851 5857 5861 5867 5869 5879 5881 5897 5903 5923 5927 5939
    5981 5987 6007 6011 6029 6037 6043 6047 6053 6067 6073 6079 6089 6091 6101 6113 6121 6131 6133
    6151 6163 6173 6197 6199 6203 6211 6217 6221 6229 6247 6257 6263 6269 6271 6277 6287 6299 6301
    6317 6323 6329 6337 6343 6353 6359 6361 6367 6373 6379 6389 6397 6421 6427 6449 6451 6469 6473
    6491 6521 6529 6547 6551 6553 6563 6569 6571 6577 6581 6599 6607 6619 6637 6653 6659 6661 6673
    6689 6691 6701 6703 6709 6719 6733 6737 6761 6763 6779 6781 6791 6793 6803 6823 6827 6829 6833
    6857 6863 6869 6871 6883 6899 6907 6911 6917 6947 6949 6959 6961 6967 6971 6977 6983 6991 6997
    7013 7019 7027 7039 7043 7057 7069 7079 7103 7109 7121 7127 7129 7151 7159 7177 7187 7193 7207
    7213 7219 7229 7237 7243 7247 7253 7283 7297 7307 7309 7321 7331 7333 7349 7351 7369 7393 7411
    7433 7451 7457 7459 7477 7481 7487 7489 7499 7507 7517 7523 7529 7537 7541 7547 7549 7559 7561
    7577 7583 7589 7591 7603 7607 7621 7639 7643 7649 7669 7673 7681 7687 7691 7699 7703 7717 7723
    7727 7741 7753 7757 7759 7789 7793 7817 7823 7829 7841 7853 7867 7873 7877 7879 7883 7901 7907
)
sold=${BASE_DIR}/TVM-Solidity-Compiler/target/release/sold
tvm_linker=${BASE_DIR}/TVM-linker/target/release/tvm_linker
lib="${BASE_DIR}/TVM-Solidity-Compiler/lib"
tonos_cli="${BASE_DIR}/tonos-cli/target/release/tonos-cli"
giver_keys=${TESTS_DIR}/test_high_load/msig.keys.json
giver_abi=${TESTS_DIR}/test_high_load/SafeMultisigWallet.abi.json
console=${TOOLS_DIR}/console
console_config=${CONSOLE_CONFIG_0}
contract_src="no_replay.sol"
abi_file="${contract_src%.sol}.abi.json"
node_log="/shared/output_0.log"

# $1 number of contract  $2 tvc_file
function pre_deploy {
    local n=$1
    local tvc_file=$2
    local msg_boc="msg.$n.boc"

    # Generate address, keys and seed phrase
    echo "Generating address, keys and seed phrase..."
    local keys_file="${contract_src%.sol}.$n.keys.json"
    if ! output="$($tonos_cli genaddr $tvc_file --abi $abi_file --genkey $keys_file)" ; then 
        echo "ERROR: $output"
        return 1
    fi
    local address=$(echo "$output" | grep "Raw address" | cut -c 14-)
    echo $address > "${n}__address"

    # Send money to contract's address
    #   prepare message
    echo "Preparing message to send money..."
    if ! output="$( $tonos_cli \
        message "-1:7777777777777777777777777777777777777777777777777777777777777777" \
        submitTransaction '{
            "dest" : "'$address'",
            "value":"10000000000",
            "bounce":false,
            "allBalance":false,
            "payload":""
        }' \
        --abi "$giver_abi" \
        --sign "$giver_keys" \
        --output "$msg_boc" \
        --raw )"
    then 
        echo "ERROR: $output"
        return 1
    fi
    local message_id=$( echo "$output" | grep "MessageId" | cut -c 12- )
    echo "Message id: " $message_id
    #   send it
    echo "Sending money to $address..."
    if ! output="$($console -C $console_config -c "sendmessage $msg_boc")" ; then 
        echo "ERROR: $output"
        return 1
    fi
    look_after_message $message_id &
    sleep 20

    # Ask contract's status to check have it got money
    for (( attempt=0; attempt < 10; attempt++ )); do
        echo "Ask contract's status to check have it got money..."
        if ! output="$( $console -C $console_config -c "getaccount $address" )" ; then 
            echo "ERROR: $output"
            return 1
        fi
        # echo "$output"
        if [ $(echo "$output" | grep "Uninit" | wc -l) != "1" ] ; then
            if [ $attempt -eq 9 ] ; then
                echo "ERROR can't find uninit account with money."
                return 1
            else
                echo "WARN can't find uninit account with money."
            fi
        else
            break
        fi
        sleep 5
    done

    echo "$address successfully got money to future deploy."
}

# $1 number of contract  $2 tvc_file  $3 address
function deploy {

    local n=$1
    local tvc_file=$2
    local address=$3
    local msg_boc="msg.$n.boc"
    local keys_file="${contract_src%.sol}.$n.keys.json"

    # Deploy contract
    #   prepare message
    echo "Preparing message to deploy contract..."
    if ! output="$( $tonos_cli \
        deploy_message "$tvc_file" \
        --abi "$abi_file" \
        --sign "$keys_file" {} \
        --raw \
        --output "$msg_boc" )"
    then 
        echo "ERROR: $output"
        return 1
    fi
    local message_id=$( echo "$output" | grep "MessageId" | cut -c 12- )
    echo "Message id: " $message_id
    #   send it
    echo "Deploying $address..."
    if ! output="$( $console -C $console_config -c "sendmessage $msg_boc" )" ; then 
        echo "ERROR: $output"
        return 1
    fi
    look_after_message $message_id &
    sleep 20

    # Ask contract's status to check have it been deployed
    echo "Ask contract's status to check have it been deployed..."
    for (( attempt=0; attempt < 10; attempt++ )); do
        echo "Ask contract's status to check have it got money..."
        if ! output="$( $console -C $console_config -c "getaccount $address" )" ; then 
            echo "ERROR: $output"
            return 1
        fi
        echo "$output"
        if [ $(echo "$output" | grep "Active" | wc -l) != "1" ] ; then
            if [ $attempt -eq 9 ] ; then
                echo "ERROR can't find uninit account with money."
                return 1
            else
                echo "WARN can't find uninit account with money."
            fi
        else
            break
        fi
        sleep 5
    done

    echo "$address successfully deployed."
}

# $1 message_id
function look_after_message {
    local message_id=$1

    for (( attempt=0; attempt < 25; attempt++ )); do
        sleep 10

        local output="$( cat $node_log | grep "New processing stage for external message $message_id" )"
        if [ $(echo "$output" | grep "TonNode_RempAccepted(RempAccepted { level: TonNode_RempMasterchain" | wc -l) != "1" ] ; then
            if [ $attempt -eq 24 ] ; then
                echo "ERROR message $message_id was not accepted yet! Current statuses: "
                echo "$output"
                return 1
            else
                echo "WARN message $message_id was not accepted yet! Current statuses: "
                echo "$output"
            fi
        else
            echo "Message $message_id was accepted! Statuses: "
            echo "$output"
            break
        fi
    done
}

# $1 address  $2 keys  $3 number of message
function send_message {
    local address=$1
    local keys_file=$2
    local message_number=$3
    local abi_file="${contract_src%.sol}.abi.json"
    local msg_boc="msg.$n.boc"
    local n=${prime_numbers[$message_number]}

    #   prepare message
    echo "Preparing message..."
    if ! output="$( $tonos_cli \
        message "$address" \
        addValue '{ "num": '$n'}' \
        --abi "$abi_file" \
        --sign "$keys_file" \
        --output "$msg_boc" \
        --raw )"
    then 
        echo "ERROR: $output"
        return 1
    fi
    local message_id=$( echo "$output" | grep "MessageId" | cut -c 12- )
    echo "Message id: " $message_id

    #   send it
    echo "Send to $address..."
    if ! output="$($console -C $console_config -c "sendmessage $msg_boc")" ; then 
        echo "ERROR: $output"
        return 1
    fi

    look_after_message $message_id &
}

# 1 address  $2 number of contract
function check_contract {
    local address=$1
    local n=$2

    # Calculate expected value
    local expacted_sum=0
    for (( n=0; n < messages_count; n++ )); do
        let "expacted_sum = expacted_sum + ${prime_numbers[$n]}"
    done

    # local call (get method)
    for (( attempt=0; attempt < 10; attempt++ )); do
        echo "Obtaining contract's boc..."
        if ! output="$( $console -C $console_config -c "getaccountstate $address $n.boc" )" ; then 
            echo "ERROR: $output"
            return 1
        fi
        echo "Calling contract's method locally..."
        if ! output="$( $tonos_cli \
            run $n.boc \
            --abi "$abi_file" \
            --boc \
            getValue '{ }' )"
        then 
            echo "ERROR $output"
            return 1
        fi
        # echo "$output"

        local hex=$(printf "%x" $expacted_sum)
        if [ $(echo "$output" | grep $hex | wc -l) != "1" ] ; then
            echo "$output"
            echo "expacted_sum: "$expacted_sum"   hex: "$hex
            if [ $attempt -eq 9 ] ; then
                echo "ERROR contract's value != expected one."
                return 1
            else 
                echo "WARN contract's value != expected one."
            fi
        else
            break
        fi
        sleep 10
    done
}

# $1 number of contract  $2 tvc_file
function one_contract_scenario {
    local n=$1
    local tvc_file=$2
    local keys_file="${contract_src%.sol}.$n.keys.json"
    local address=$(cat ${n}__address)

    if ! deploy $n $tvc_file $address ; then 
        return 1
    fi

    for (( mn=0; mn < messages_count; mn++ )); do
        # $1 address  $2 keys  $3 number of message
        if ! send_message $address $keys_file $mn ; then 
            return 1
        fi
        sleep $timeout_sec
    done

    echo "waiting..."
    sleep 30

    if ! check_contract $address $n ; then 
        return 1
    fi

    echo "$address succesfully finished their scenario!"

    exit 0
}

function cleanup {
    rm *.log
    rm *.boc
    rm *__address
    rm *.json
    rm *.code
    rm *.tvc
}

# 
# TEST REMP
# Pre deploy: send money to contracts (one by one)
# In parallel:
#  - deploy contract
#  - send messages - each message contains next prime number,
#                    contract stores sum of all got numbers.
#  - check contract state - check if all messages was delivered exactly ones. 
#                           We will check if contract's sum is equal to sum of sent numbers.

cleanup

if [ $messages_count -gt ${#prime_numbers[*]} ]; then
    echo "messages_count value is too big, max=${#prime_numbers[*]}"
    exit 1
fi

# Don't forget to deploy giver first!

# compile & link test contract
echo "Compile..."
if ! $sold $contract_src ; then exit 1 ; fi
#code_file="${contract_src%.sol}.tvc"

#echo "Linking..."
#if ! output="$($tvm_linker compile $code_file --lib $lib/stdlib_sol.tvm)" ; then 
#    echo "ERROR $output"
#    exit 1
#fi
#tvc_file=$(echo "$output" | grep "Saved contract to file" | cut -c 24-)
tvc_file="${contract_src%.sol}.tvc"

# 
echo "Obtaining blockchain's config..."
if ! output="$( $console -C $console_config -c "getblockchainconfig" )" ; then 
    echo "ERROR $output"
    exit 1
fi
echo $output | grep b5ee9c72 | xxd -r -p > config.boc

# 
echo "Send money to contracts (one by one)"
rm pre_deploy.log
for (( n=0; n < contracts_count; n++ )); do
    echo "Sending money to #$n..."
    echo "
    
    
    Sending money to #$n...
    
    " >> pre_deploy.log
    if ! pre_deploy $n $tvc_file >> pre_deploy.log ; then 
    # if ! output="$( pre_deploy $n $tvc_file )" ; then 
    #     echo "ERROR $output"
        exit 1
    else
        echo "Sent money to #$n"
    fi
done

# run test scenario
echo "Run test scenario..."
for (( n=0; n < contracts_count; n++ )); do
    one_contract_scenario $n $tvc_file > $n.log &
done

wait

# check log files of each contract
result=0
for (( n=0; n < contracts_count; n++ )); do
    if err="$( cat $n.log | grep ERROR )"; then
        echo "Contract #$n has errors in their test scenario"
        echo err
        echo
        result=1
    else
        echo "$n succesfully finished their scenario!"
    fi
done

if [ $result -eq 0 ]; then
    echo "Succesfully finished!"
fi

exit $result
