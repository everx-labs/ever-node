/*
* Copyright (C) 2019-2021 TON Labs. All Rights Reserved.
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

pub use super::*;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/*
    Constants
*/

const THREAD_NAME: &str = "ACTIVITY_NODES"; //activity nodes dumping thread
const ACCESS_WARN_THRESHOLD: Duration = Duration::from_millis(1000); //threshold for warnings about stalled acitivites

type NodeId = u64;

/*
    ActivityNode implementation
*/

struct ActivityNodeImpl {
    id: NodeId,                                //node identifier
    name: String,                              //node name
    creation_time: SystemTime,                 //creation time
    access_time: Arc<AtomicU64>,               //time of last tick
    manager: Weak<Mutex<ActivityNodeManager>>, //manager instance
}

impl ActivityNode for ActivityNodeImpl {
    /// Name of the object
    fn get_name(&self) -> String {
        self.name.clone()
    }

    /// Get creation time
    fn get_creation_time(&self) -> SystemTime {
        self.creation_time
    }

    /// Get last activity notification time
    fn get_access_time(&self) -> SystemTime {
        let timestamp = self.access_time.load(Ordering::Relaxed);

        UNIX_EPOCH + Duration::from_millis(timestamp)
    }

    /// Notify about activity
    fn tick(&self) {
        let now = SystemTime::now();
        let timestamp = match now.duration_since(UNIX_EPOCH) {
            Ok(elapsed) => elapsed.as_millis(),
            Err(err) => {
                error!("ActivityNode::tick: can't get system time: {}", err);
                panic!("ActivityNode::tick: can't get system time");
            }
        };

        self.access_time.store(timestamp as u64, Ordering::Release);
    }
}

impl Drop for ActivityNodeImpl {
    /// Drop the node
    fn drop(&mut self) {
        if let Some(manager) = self.manager.upgrade() {
            if let Ok(mut manager) = manager.lock() {
                manager.unregister_node(self.id);
            }
        }
    }
}

impl ActivityNodeImpl {
    /// Create new node
    fn create(name: String, manager_wrapper: Arc<Mutex<ActivityNodeManager>>) -> ActivityNodePtr {
        let manager = manager_wrapper.lock();
        let mut manager = manager.unwrap();

        let body = Self {
            manager: Arc::downgrade(&manager_wrapper),
            id: manager.generate_id(),
            name: name,
            creation_time: SystemTime::now(),
            access_time: Arc::new(AtomicU64::new(0)),
        };

        body.tick();

        let node = Arc::new(body);

        manager.register_node(node.id, node.clone());

        node
    }
}

/*
    ActivityNodeManager
*/

pub(crate) struct ActivityNodeManager {
    nodes: HashMap<NodeId, Weak<dyn ActivityNode>>, //map of active nodes
    current_node_id: NodeId,                        //current node identifier for generation
    thread_should_stop: Arc<AtomicBool>,            //dump thread should stop
    thread_started: Arc<AtomicBool>,                //dump thread is stopped
}

impl Drop for ActivityNodeManager {
    fn drop(&mut self) {
        self.stop();
    }
}

impl ActivityNodeManager {
    /// Get global instance of the manager
    fn global_instance() -> Arc<Mutex<ActivityNodeManager>> {
        lazy_static! {
            static ref INSTANCE: Arc<Mutex<ActivityNodeManager>> = ActivityNodeManager::create();
        }
        INSTANCE.clone()
    }

    /// Create new manager
    fn create() -> Arc<Mutex<ActivityNodeManager>> {
        let thread_started = Arc::new(AtomicBool::new(false));
        let thread_should_stop = Arc::new(AtomicBool::new(false));
        let body = ActivityNodeManager {
            nodes: HashMap::new(),
            current_node_id: 1,
            thread_should_stop: thread_should_stop.clone(),
            thread_started: thread_started.clone(),
        };

        let manager = Arc::new(Mutex::<ActivityNodeManager>::new(body));
        let manager_weak = Arc::downgrade(&manager);
        let thread_started_clone = thread_started.clone();

        let _dump_thread = std::thread::Builder::new()
            .name(THREAD_NAME.to_string())
            .spawn(move || {
                ActivityNodeManager::thread_main_loop(
                    thread_should_stop,
                    thread_started_clone,
                    manager_weak,
                );
            });

        loop {
            if thread_started.load(Ordering::Relaxed) {
                break;
            }

            info!("...waiting for Catchain activity node dumping thread start");

            const CHECKING_INTERVAL: std::time::Duration = std::time::Duration::from_millis(300);

            std::thread::sleep(CHECKING_INTERVAL);
        }

        manager
    }

    /// Stopping activity nodes manager
    fn stop(&self) {
        self.thread_should_stop.store(true, Ordering::Release);

        loop {
            if !self.thread_started.load(Ordering::Relaxed) {
                break;
            }

            info!("...waiting for Catchain activity node dumping thread stop");

            const CHECKING_INTERVAL: std::time::Duration = std::time::Duration::from_millis(300);

            std::thread::sleep(CHECKING_INTERVAL);
        }
    }

    /// Generate node ID
    fn generate_id(&mut self) -> NodeId {
        const MAX_ID: NodeId = std::u64::MAX;

        assert!(self.current_node_id < MAX_ID);

        let result = self.current_node_id;

        self.current_node_id += 1;

        result
    }

    /// Register node
    fn register_node(&mut self, id: NodeId, node: ActivityNodePtr) {
        self.nodes.insert(id, Arc::downgrade(&node));
    }

    /// Unregister node
    fn unregister_node(&mut self, id: NodeId) {
        self.nodes.remove(&id);
    }

    /// Dumping thread main loop
    fn thread_main_loop(
        should_stop: Arc<AtomicBool>,
        started: Arc<AtomicBool>,
        manager: Weak<Mutex<ActivityNodeManager>>,
    ) {
        started.store(true, Ordering::Release);

        info!("Catchain activity nodes dumping thread is started");

        loop {
            if should_stop.load(Ordering::Relaxed) {
                break;
            }

            let nodes = if let Some(manager) = manager.upgrade() {
                if let Ok(manager) = manager.lock() {
                    manager.nodes.clone()
                } else {
                    HashMap::new()
                }
            } else {
                HashMap::new()
            };

            ActivityNodeManager::dump(nodes);

            const DUMP_PERIOD: Duration = Duration::from_millis(5000);

            std::thread::sleep(DUMP_PERIOD);
        }

        info!("Catchain activity nodes dumping thread is finished");

        started.store(false, Ordering::Release);
    }

    /// Create new activity node
    pub(crate) fn create_node(name: String) -> ActivityNodePtr {
        ActivityNodeImpl::create(name, ActivityNodeManager::global_instance())
    }

    /// Dump active nodes
    fn dump(nodes: HashMap<NodeId, Weak<dyn ActivityNode>>) {
        if nodes.len() == 0 {
            info!("No Catchain activity nodes have been found");
            return;
        }

        let mut result = format!("Activity nodes dump ({} nodes are found):", nodes.len());

        for (_, node) in nodes.iter() {
            if let Some(node) = node.upgrade() {
                let creation_time = node.get_creation_time();
                let access_time = node.get_access_time();

                result = format!("{}\n  - {}: created {:.3}s ago, accessed {:.3}s ago (creation_time={}, access_time={})",
                result, node.get_name(), creation_time.elapsed().unwrap().as_secs_f64(), access_time.elapsed().unwrap().as_secs_f64(), utils::time_to_timestamp_string(&creation_time),
                utils::time_to_timestamp_string(&access_time));
            }
        }

        debug!("{}", result);

        for (_, node) in nodes.iter() {
            if let Some(node) = node.upgrade() {
                let creation_time = node.get_creation_time();
                let access_time = node.get_access_time();

                if access_time.elapsed().unwrap() < ACCESS_WARN_THRESHOLD {
                    continue;
                }

                warn!("Activity node '{}' has not been accessed during the last {:.3}s; last accessed at {} (created {:.3}s ago at {})",
                node.get_name(), access_time.elapsed().unwrap().as_secs_f64(), utils::time_to_timestamp_string(&access_time), creation_time.elapsed().unwrap().as_secs_f64(), utils::time_to_timestamp_string(&creation_time));
            }
        }
    }
}
