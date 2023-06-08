NODE_PATH=$1
NODES=$2
if [ "$NODES" == "" ] ; then
    NODES=20
fi

echo $NODE_PATH $NODES
   
if ! [ "$NODE_PATH" == "" ] ; then 
    rm -f $NODE_PATH/*.boc
    rm -f $NODE_PATH/config.json
    rm -f $NODE_PATH/console_config.json
    rm -f $NODE_PATH/console_public_json
    rm -f $NODE_PATH/default_config.json
    rm -f $NODE_PATH/tmp_output
    rm -f $NODE_PATH/ton-global.config.json
    rm -f $NODE_PATH/zero_state_tmp.json
    rm -f $NODE_PATH/zero_state_tmp_2.json 
fi

rm -f tmp_output
rm -f ton-global.config.json
rm -f ton-global.config.json.tmp
rm -f zero_state.json
rm -f zero_state_tmp.json
rm -f zero_state_tmp_2.json
rm -rdf tmp

for (( N=1; N <= $NODES; N++ ))
do
    rm -f /shared/output_$N.log
    if ! [ "$NODE_PATH" == "" ] ; then 
        rm -f $NODE_PATH/default_config$N.json
        rm -f $NODE_PATH/log_cfg_$N.yml
        rm -rdf $NODE_PATH/configs_$N
        rm -rdf $NODE_PATH/node_db_$N
    fi
    rm -f config$N.json
    rm -f console$N.json
    rm -f default_config$N.json
    rm -f genkey$N
done
