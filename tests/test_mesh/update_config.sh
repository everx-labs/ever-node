tonos_cli="../../../../tonos-cli/target/release/tonos-cli"
a_tool="../../../../a-tool/target/release/a-tool"
console="../../../ever-node-tools/target/release/console"
console_config=$1
config_param_index=$2
config_param_json=$3

set -e
handle_error() {
    echo "An error occurred on $0::$1"
    exit 1
}
trap 'handle_error $LINENO' ERR

config_smc_boc="config_smc.boc"

echo "Obtaining config contract's boc..."
if ! output="$( $console -C $console_config \
    -c "getaccountstate -1:5555555555555555555555555555555555555555555555555555555555555555 $config_smc_boc" )"
then 
    echo "ERROR: $output"
    exit 1
fi

echo "Preparing new config param boc..."
if ! output="$( $a_tool import \
        --output tmp/config_param.boc \
        $config_param_index \
        $config_param_json )"
then 
    echo "ERROR: $output"
    exit 1
fi

echo "Preparing message..."
msg_boc="tmp/msg.boc"
if ! output="$( $a_tool prepare \
        --output $msg_boc \
        --config_smc_boc $config_smc_boc \
        --priv-key config-master.pk \
        $config_param_index \
        tmp/config_param.boc )"
then 
    echo "ERROR: $output"
    exit 1
else
    echo "$output"
fi

echo "Sending message via console..."
$console -C $console_config -c "sendmessage $msg_boc"
