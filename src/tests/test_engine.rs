/*
* Copyright (C) 2019-2023 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific TON DEV software governing permissions and
* limitations under the License.
*/

use crate::{                                
    config::TonNodeConfig, engine::{Engine, EngineFlags, run, Stopper}, 
    test_helper::{configure_ip, get_config, init_test}
};
use std::{fs::remove_dir_all, sync::Arc, time::Duration};
use ton_types::Result;

fn start_node(
    rt: &tokio::runtime::Runtime,
    stopper: Arc<Stopper>,
    config: TonNodeConfig
) -> Result<(Arc<Engine>, tokio::task::JoinHandle<()>)> {
    let validator_rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .expect("Can't create Validator tokio runtime")
        .handle().clone();
    let flags = EngineFlags {
        initial_sync_disabled: true,
        starting_block_disabled: false,
        force_check_db: false
    };
    rt.block_on(
        run(
            config, 
            None, 
            vec![], 
            validator_rt, 
            flags,
            stopper
        )
    )
}

#[ignore]
#[test]
fn test_node_restart() {

    const CONFIG_FROM_INITBLOCK: &str = "default_config_mainet_initblock.json";
    const CONFIG_FROM_ZEROSTATE: &str = "default_config_mainet.json";
    const DB_PATH: &str = "target/node_restart";

    for step in 1..=4 {

        remove_dir_all(DB_PATH).ok();

        // Steps: 
        //   1 - isolated node (no replies from network expected), start from zerostate
        //   2 - isolated node (no replies from network expected), start from initblock
        //   3 - connected node, start from zerostate
        //   4 - connected node, start from initblock
        let (config, ip) = match step {
            1 => (CONFIG_FROM_ZEROSTATE, "127.0.0.1:5191".to_string()),
            2 => (CONFIG_FROM_INITBLOCK, "127.0.0.1:5191".to_string()),
            3 => (CONFIG_FROM_ZEROSTATE, configure_ip("0.0.0.0:1", 4190)),
            _ => (CONFIG_FROM_INITBLOCK, configure_ip("0.0.0.0:1", 4190))
        };

        for i in 1..=2 {
            println!("Step {} Iteration #{}", step, i);
            let rt = init_test();
            let stopper = Arc::new(Stopper::new());
            let mut config = rt.block_on(get_config(&ip, None, config)).unwrap();
            config.set_internal_db_path(DB_PATH.to_string());
            let stopper_clone = stopper.clone();
            rt.spawn(
                async move {
                    tokio::time::sleep(Duration::from_millis(20000)).await;
                    stopper_clone.set_stop();
                    println!("Node stop signal sent");
                }
            );
            let stopper_clone = stopper.clone();
            match start_node(&rt, stopper, config) {
                Err(e) => {
                    if !stopper_clone.check_stop() {
                        panic!("Can't start node engine: {}", e)
                    } else {
                        println!("Node stopped (in start phase).");
                    }
                },
                Ok((engine, join_handle)) => rt.block_on(
                    async move {
                        join_handle.await.ok();
                        println!("Stopping node...");
                        engine.wait_stop().await;
                        println!("Node stopped (in run phase).");
                    }
                )
            }
        }

    }

}
