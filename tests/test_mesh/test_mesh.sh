
set -e
handle_error() {
    echo "An error occurred on $0::$1"
    exit 1
}
trap 'handle_error $LINENO' ERR

function prepare_config() {
    global_cofig=$1
    network_id=$2
    target_file=$3

    root_hash=$(jq .validator.zero_state.root_hash $global_cofig | sed 's/"//g' | base64 -d | xxd -c 256 -p) 
    sed "s/root_hash_value/$root_hash/g" config58_blank.json > tmp/config58.tmp.json

    file_hash=$(jq .validator.zero_state.file_hash $global_cofig | sed 's/"//g' | base64 -d | xxd -c 256 -p) 
    sed "s/file_hash_value/$file_hash/g" tmp/config58.tmp.json > tmp/config58.tmp2.json

    echo $global_cofig
    echo $network_id
    echo $target_file

    sed "s/network_id_value/$network_id/g" tmp/config58.tmp2.json > $target_file
}

# Start first network "23"
echo "
RUNNING NETWORK 23
"
./test_run_net.sh 23 0

# Start second network "48"
echo "
RUNNING NETWORK 48
"
./test_run_net.sh 48 10 no-cleanup

echo "
$(date +%H:%M:%S)  WAITING 10 min...
"
sleep 600

# register mesh networks - 48 in 23
echo "
REGISTER 48 in 23
"
prepare_config tmp/global_config_48.json 48 tmp/config58_48_in_23.json
./update_config.sh tmp/console1.json 58 tmp/config58_48_in_23.json

# register mesh networks - 23 in 48
echo "
REGISTER 23 in 48
"
prepare_config tmp/global_config_23.json 23 tmp/config58_23_in_48.json
./update_config.sh tmp/console11.json 58 tmp/config58_23_in_48.json

# copy global configs to the working directory of node (which is default path to find mesh global configs)
cp tmp/global_config_48.json ../../target/release/
cp tmp/global_config_23.json ../../target/release/


exit 0


echo "
$(date +%H:%M:%S)  WAITING 10 min...
"
sleep 600

# activate - 48 in 23
echo "
ACTIVATE 48 in 23
"
jq '.p58[0].is_active = true' tmp/config58_48_in_23.json > tmp/config58_48_in_23_.json
mv tmp/config58_48_in_23_.json tmp/config58_48_in_23.json
./update_config.sh tmp/console1.json 58 tmp/config58_48_in_23.json

# activate - 23 in 48
echo "
ACTIVATE 23 in 48
"
jq '.p58[0].is_active = true' tmp/config58_23_in_48.json > tmp/config58_23_in_48_.json
mv tmp/config58_48_in_23_.json tmp/config58_23_in_48.json
./update_config.sh tmp/console11.json 58 tmp/config58_23_in_48.json





