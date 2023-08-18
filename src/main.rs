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

mod block;
mod block_proof;
mod boot;
mod collator_test_bundle;
mod config;
mod engine;
mod engine_traits;
mod engine_operations;
mod error;
mod ext_messages;
#[cfg(feature = "external_db")]
mod external_db;
mod full_node;
mod internal_db;
mod macros;
mod network;
mod rng;
mod shard_state;
mod sync;
mod types;
mod validating_utils;
mod validator;
mod shard_states_keeper;

mod shard_blocks;

#[cfg(feature = "tracing")]
mod jaeger;

#[cfg(not(feature = "tracing"))]
mod jaeger {
    pub fn init_jaeger(){}
    #[cfg(feature = "external_db")]
    pub fn message_from_kafka_received(_kf_key: &[u8]) {}
    pub fn broadcast_sended(_msg_id: String) {}
}

use crate::{
    config::TonNodeConfig, engine_traits::ExternalDb, engine::{Engine, STATSD, Stopper}, 
    jaeger::init_jaeger, internal_db::restore::set_graceful_termination,
    validating_utils::supported_version
};

use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::os::raw::c_void;
#[cfg(feature = "trace_alloc")]
use std::{
    alloc::{GlobalAlloc, System, Layout}, sync::atomic::{AtomicBool, AtomicU64, Ordering}, 
    thread, time::Duration
};
#[cfg(feature = "trace_alloc_detail")]
use std::{
    fs::File, io::Write, mem::{self, MaybeUninit}, sync::atomic::{AtomicIsize, AtomicUsize}
};
#[cfg(feature = "external_db")]
use ton_types::error;
use ton_types::Result;

#[cfg(target_os = "linux")]
#[link(name = "tcmalloc", kind = "dylib")]
extern "C" {
    pub fn tc_memalign(alignment: usize, size: usize) -> *mut c_void;
    pub fn tc_free(ptr: *mut c_void);
}

#[cfg(target_os = "linux")]
fn check_tcmalloc() {
    unsafe {
        let ptr = tc_memalign(10, 10);
        tc_free(ptr);
    }
}

#[cfg(feature = "trace_alloc")]
struct TracingAllocator {
    count: AtomicU64,
    allocated: AtomicU64,
    overhead: AtomicU64
}

#[cfg(feature = "trace_alloc_detail")]
struct AllocDetail {
    start: AtomicUsize,
    size: AtomicIsize,
}

#[cfg(feature = "trace_alloc_detail")]
const SIZE_TRACEBUF: usize = 20000000;

#[cfg(feature = "trace_alloc_detail")]
lazy_static::lazy_static!{
    static ref TRACEBUF: [AllocDetail; SIZE_TRACEBUF] = {
        let mut data: [MaybeUninit<AllocDetail>; SIZE_TRACEBUF] = unsafe {
            MaybeUninit::uninit().assume_init()
        };
        for elem in &mut data[..] {
            elem.write(
                AllocDetail {
                    start: AtomicUsize::new(0),
                    size: AtomicIsize::new(0)
                }
            );
        }
        unsafe { mem::transmute::<_, [AllocDetail; SIZE_TRACEBUF]>(data) }
    };
    static ref TRACEBUF_HEAD: AtomicUsize = AtomicUsize::new(0);
    static ref TRACEBUF_TAIL: AtomicUsize = AtomicUsize::new(0);
}

#[cfg(feature = "trace_alloc")]
thread_local!(
    static NOCALC: AtomicBool = AtomicBool::new(false)
);

#[cfg(feature = "trace_alloc")]
unsafe impl GlobalAlloc for TracingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        self.check_alloc(ret, layout.size());
        ret
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc_zeroed(layout);
        self.check_alloc(ret, layout.size());
        ret
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        self.check_dealloc(ptr, layout.size());
        let ret = System.realloc(ptr, layout, new_size);
        self.check_alloc(ret, new_size);
        ret
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.check_dealloc(ptr, layout.size());
        System.dealloc(ptr, layout);
    }
}

#[cfg(feature = "trace_alloc")]
impl TracingAllocator {

    fn check_alloc(&self, _ptr: *mut u8, size: usize) {
        if NOCALC.with(
            |f| f.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_ok()
        ) {
            self.allocated.fetch_add(size as u64, Ordering::Relaxed);
            #[cfg(feature = "trace_alloc_detail")]
            Self::post_trace(_ptr as usize, size as isize);
            self.count.fetch_add(1, Ordering::Relaxed);
            NOCALC.with(|f| f.store(false, Ordering::Relaxed));
        } else {
            self.overhead.fetch_add(size as u64, Ordering::Relaxed);
        }
    }

    fn check_dealloc(&self, _ptr: *mut u8, size: usize) {
        if NOCALC.with(
            |f| f.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_ok()
        ) {
            self.allocated.fetch_sub(size as u64, Ordering::Relaxed);
            #[cfg(feature = "trace_alloc_detail")]
            Self::post_trace(_ptr as usize, -(size as isize));
            self.count.fetch_sub(1, Ordering::Relaxed);
            NOCALC.with(|f| f.store(false, Ordering::Relaxed));
        } else {
            self.overhead.fetch_sub(size as u64, Ordering::Relaxed);
        }
    }

    #[cfg(feature = "trace_alloc_detail")]
    fn post_trace(start: usize, size: isize) {
        loop {
            let this = TRACEBUF_HEAD.load(Ordering::Relaxed);
            let next = if this == SIZE_TRACEBUF - 1 {
                0
            } else {
                this + 1
            };
            if next == TRACEBUF_TAIL.load(Ordering::Acquire) {
                thread::yield_now();
                continue
            }
            if TRACEBUF_HEAD.compare_exchange(
                this, next, Ordering::Relaxed, Ordering::Relaxed
            ).is_err() {
                thread::yield_now();
                continue;
            }
            if start == 0 {
                panic!("ZEROADDR_WRITE")
            }
            TRACEBUF[this].start.store(start, Ordering::Relaxed);
            TRACEBUF[this].size.store(size, Ordering::Release);
            break
        }
    }

}

#[cfg(feature = "trace_alloc")]
#[global_allocator]
static GLOBAL: TracingAllocator = TracingAllocator { 
    count: AtomicU64::new(0),
    allocated: AtomicU64::new(0),
    overhead: AtomicU64::new(0)
};

fn init_logger(log_config_path: Option<String>) {

    if let Some(path) = log_config_path {
        if let Err(err) = log4rs::init_file(path, Default::default()) {
            println!("Error while initializing log by {}: {}", err, err);
        } else {
            return;
        }
    }

    let level = log::LevelFilter::Info; 
    let stdout = log4rs::append::console::ConsoleAppender::builder()
        .target(log4rs::append::console::Target::Stdout)
        .build();

    let config = log4rs::config::Config::builder()
        .appender(
            log4rs::config::Appender::builder()
                .filter(Box::new(log4rs::filter::threshold::ThresholdFilter::new(level)))
                .build("stdout", Box::new(stdout)),
        )
        .build(
            log4rs::config::Root::builder()
                .appender("stdout")
                .build(log::LevelFilter::Info),
        )
        .unwrap();

    let result = log4rs::init_config(config);
    if let Err(e) = result {
        println!("Error init log: {}", e);
    }
}

static NOT_SET_LABEL: &str = "Not set";

fn get_version() -> String {
    format!(
        "Execute {:?}\n\
        BLOCK_VERSION: {:?}\n\
        COMMIT_ID: {:?}\n\
        BUILD_DATE: {:?}\n\
        COMMIT_DATE: {:?}\n\
        GIT_BRANCH: {:?}\n\
        RUST_VERSION:{}\n",
        std::option_env!("CARGO_PKG_VERSION").unwrap_or(NOT_SET_LABEL),
        supported_version(), 
        std::option_env!("BUILD_GIT_COMMIT").unwrap_or(NOT_SET_LABEL),
        std::option_env!("BUILD_TIME").unwrap_or(NOT_SET_LABEL),
        std::option_env!("BUILD_GIT_DATE").unwrap_or(NOT_SET_LABEL),
        std::option_env!("BUILD_GIT_BRANCH").unwrap_or(NOT_SET_LABEL),
        std::option_env!("BUILD_RUST_VERSION").unwrap_or(NOT_SET_LABEL),
    )
}

fn get_build_info() -> String {
    let mut info = String::new();
    info += &format!(
        "TON Node, version {}\n\
        Rust: {}\n\
        TON NODE git commit:         {}\n\
        ADNL git commit:             {}\n\
        DHT git commit:              {}\n\
        OVERLAY git commit:          {}\n\
        RLDP git commit:             {}\n\
        TON_BLOCK git commit:        {}\n\
        TON_BLOCK_JSON git commit:   {}\n\
        TON_EXECUTOR git commit:     {}\n\
        TON_TL git commit:           {}\n\
        TON_TYPES git commit:        {}\n\
        TON_VM git commit:           {}\n",
        std::option_env!("CARGO_PKG_VERSION").unwrap_or(NOT_SET_LABEL),
        std::option_env!("BUILD_RUST_VERSION").unwrap_or(NOT_SET_LABEL),
        std::option_env!("BUILD_GIT_COMMIT").unwrap_or(NOT_SET_LABEL),
        adnl::build_commit().unwrap_or(NOT_SET_LABEL),
        dht::build_commit().unwrap_or(NOT_SET_LABEL),
        overlay::build_commit().unwrap_or(NOT_SET_LABEL),
        rldp::build_commit().unwrap_or(NOT_SET_LABEL),
        ton_block::build_commit().unwrap_or(NOT_SET_LABEL),
        ton_block_json::build_commit().unwrap_or(NOT_SET_LABEL),
        ton_executor::build_commit().unwrap_or(NOT_SET_LABEL),
        ton_api::build_commit().unwrap_or(NOT_SET_LABEL),
        ton_types::build_commit().unwrap_or(NOT_SET_LABEL),
        ton_vm::build_commit().unwrap_or(NOT_SET_LABEL),
    );
    #[cfg(feature = "slashing")] {
        info += &format!(
            "TON_ABI git commit:     {}\n",
            ton_abi::build_commit().unwrap_or(NOT_SET_LABEL)
        );
    }
    info
}

#[cfg(feature = "external_db")]
fn start_external_db(config: &TonNodeConfig) -> Result<Vec<Arc<dyn ExternalDb>>> {
    Ok(vec!(
        external_db::create_external_db(
            config.external_db_config().ok_or_else(|| error!("Can't load external database config!"))?,
            config.front_workchain_ids()
        )?
    ))
}

#[cfg(not(feature = "external_db"))]
fn start_external_db(_config: &TonNodeConfig) -> Result<Vec<Arc<dyn ExternalDb>>> {
    Ok(vec!())
}

async fn start_engine(
    config: TonNodeConfig, 
    zerostate_path: Option<&str>, 
    validator_runtime: tokio::runtime::Handle, 
    initial_sync_disabled: bool,
    force_check_db: bool,
    stopper: Arc<Stopper>
) -> Result<(Arc<Engine>, tokio::task::JoinHandle<()>)> {
    let external_db = start_external_db(&config)?;
    crate::engine::run(
        config, 
        zerostate_path, 
        external_db, 
        validator_runtime, 
        initial_sync_disabled,
        force_check_db,
        stopper,
    ).await
}

const CONFIG_NAME: &str = "config.json";
const DEFAULT_CONFIG_NAME: &str = "default_config.json";

fn main() {

    #[cfg(target_os = "linux")]
    check_tcmalloc();

    println!("{}", get_build_info());
    let version = get_version();
    println!("{}", version);

    let app = clap::App::new("TON node")
        .arg(clap::Arg::with_name("zerostate")
            .short("z")
            .long("zerostate")
            .value_name("zerostate"))
        .arg(clap::Arg::with_name("config")
            .short("c")
            .long("configs")
            .value_name("config")
            .default_value("./"))
        .arg(clap::Arg::with_name("console_key")
            .short("k")
            .long("ckey")
            .value_name("console_key")
            .help("use console key in json format"))
        .arg(clap::Arg::with_name("initial_sync_disabled")
            .short("i")
            .long("initial-sync-disabled")
            .help("use this flag to sync from zero_state"))
        .arg(clap::Arg::with_name("force_check_db")
            .short("f")
            .long("force-check-db")
            .help("start check & restore db process forcedly with refilling cells database"));

    let matches = app.get_matches();

    let initial_sync_disabled = matches.is_present("initial_sync_disabled");
    let force_check_db = matches.is_present("force_check_db");

    let config_dir_path = match matches.value_of("config") {
        Some(config) => {
            config
        }
        None => {
            println!("Can't load config: config dir is not set!");
            return;
        }
    };

    let console_key = matches.value_of("console_key").map(|console_key| console_key.to_string());

    let zerostate_path = matches.value_of("zerostate");
    let config = match TonNodeConfig::from_file(
        config_dir_path, 
        CONFIG_NAME, 
        None,
        DEFAULT_CONFIG_NAME,
        console_key
    ) {
        Err(e) => {
            println!("Can't load config: {:?}", e);
            return;
        },
        Ok(c) => c
    };

    init_logger(config.log_config_path());
    log::info!("{}", version);
    
    lazy_static::initialize(&STATSD);
    
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .expect("Can't create Engine tokio runtime");
    let validator_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .expect("Can't create Validator tokio runtime");

    init_jaeger();

    #[cfg(feature = "trace_alloc_detail")]
    thread::spawn(
        || {
            let mut file = File::create("trace.bin").unwrap();
            loop {
                let this = TRACEBUF_TAIL.load(Ordering::Relaxed);
                let next = if this == SIZE_TRACEBUF - 1 {
                    0
                } else {
                    this + 1
                };
                if this == TRACEBUF_HEAD.load(Ordering::Relaxed) {
                    thread::yield_now();
                    continue
                }
                let size = TRACEBUF[this].size.load(Ordering::Acquire);
                if size == 0 {
                    thread::yield_now();
                    continue
                }
                let start = TRACEBUF[this].start.load(Ordering::Relaxed);
                if start == 0 {
                    panic!("ZEROADDR_READ")
                }
                file.write_all(&start.to_le_bytes()).ok();
                file.write_all(&size.to_le_bytes()).ok();
                TRACEBUF[this].size.store(0, Ordering::Release);
                TRACEBUF_TAIL.store(next, Ordering::Release);
            }
        }
    );

    #[cfg(feature = "trace_alloc")]
    thread::spawn(
        || {
            loop {
                thread::sleep(Duration::from_millis(30000));
                let count = GLOBAL.count.load(Ordering::Relaxed);
                let allocated = GLOBAL.allocated.load(Ordering::Relaxed);
                let overhead = GLOBAL.overhead.load(Ordering::Relaxed);
                log::info!(
                    "Allocated {} + {} = {} bytes, {} objects", 
                    allocated, overhead, allocated + overhead, count
                ); 
            }
        }
    );

    let stopper = Arc::new(Stopper::new());
    let stopper1 = stopper.clone();
    ctrlc::set_handler(move || {
        log::warn!("Got SIGINT, starting node's safe stopping...");
        stopper1.set_stop();
    }).expect("Error setting termination signals handler");

    let validator_rt_handle = validator_runtime.handle().clone();
    let db_dir = config.internal_db_path().to_string();
    runtime.block_on(async move {
        match start_engine(
            config, 
            zerostate_path, 
            validator_rt_handle,
            initial_sync_disabled,
            force_check_db,
            stopper.clone(),
        ).await {
            Err(e) => {
                if stopper.check_stop() {
                    log::warn!("Node stopped ({})", e);
                    set_graceful_termination(&db_dir);
                } else {
                    log::error!("Can't start node's Engine: {:?}", e);
                }
            }
            Ok((engine, join_handle)) => {
                join_handle.await.ok();

                log::warn!("Still safe stopping node...");
                engine.wait_stop().await;
                log::warn!("Node stopped");
                set_graceful_termination(&db_dir);
            }
        }
    });
}
