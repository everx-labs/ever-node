#!/bin/bash -eEx

#
# Copyright (C) 2019-2021 TON Labs. All Rights Reserved.
#
# Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
# this file except in compliance with the License.  You may obtain a copy of the
# License at:
#
# https://www.ton.dev/licenses
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific TON DEV software governing permissions and limitations
# under the License.
#

GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo "INFO: R-Node startup..."

echo "INFO: NETWORK_TYPE = ${NETWORK_TYPE}"
echo "INFO: DEPLOY_TYPE = ${DEPLOY_TYPE}"
echo "INFO: CONFIGS_PATH = ${CONFIGS_PATH}"
echo "INFO: \$1 = $1"
echo "INFO: \$2 = $2"

NODE_EXEC='/ton-node/ton_node_kafka'
if [ "${TON_NODE_ENABLE_KAFKA}" -ne 1 ]
then
    echo "Kafka disabled"
    NODE_EXEC='/ton-node/ton_node_no_kafka'
else 
    echo "Kafka enabled"
fi
if [ "${TON_NODE_ENABLE_COMPRESSION}" -eq 1 ]
then
    echo "Compression enabled"
    NODE_EXEC="${NODE_EXEC}_compression"
else
    echo "Compression disabled"
fi

function f_get_ton_global_config_json() {
    if [ "${NETWORK_TYPE}" = "customnet" ]; then
        if [ -f "${CONFIGS_PATH}/ton-global.config.json" ]; then
            echo "WARNING: ${CONFIGS_PATH}/ton-global.config.json will be used"
        else
            echo "WARNING: ${CONFIGS_PATH}/ton-global.config.json does not exist"
            exit 1
        fi
    else
        curl -sS "https://builder.tonlabs.io/nodes/${NETWORK_TYPE}/ton-global.config.json" -o "${CONFIGS_PATH}/ton-global.config.json"
    fi
}

function f_iscron() {
kill -0 "$(cat /var/run/crond.pid 2>/dev/null)" 2>/dev/null || crond
}

#f_iscron

f_get_ton_global_config_json


# main

if [ "${1}" = "bash" ];then
    tail -f /dev/null
else
    cd /ton-node
    exec $NODE_EXEC --configs "${CONFIGS_PATH}" ${TON_NODE_EXTRA_ARGS} >> /ton-node/logs/output.log 2>&1
fi

