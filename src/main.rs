pub mod block;
pub mod block_proof;
pub mod boot;
pub mod config;
pub mod db;
pub mod engine;
pub mod engine_traits;
pub mod engine_operations;
pub mod error;
pub mod full_node;
pub mod macros;
pub mod network;
pub mod shard_state;
pub mod types;

#[cfg(feature = "tracing")]
pub mod jaeger;

#[cfg(not(feature = "tracing"))]
pub mod jaeger {
    pub fn init_jaeger(){}
    pub fn message_from_kafka_received(_kf_key: &[u8]) {}
    pub fn broadcast_sended(_msg_id: String) {}
}

extern crate lazy_static;

#[cfg(feature = "external_db")]
mod external_db;

use crate::{config::TonNodeConfig, engine_traits::ExternalDb, engine::STATSD, jaeger::init_jaeger};
use clap;

#[cfg(feature = "external_db")]
use ton_types::error;
use ton_types::Result;
use std::sync::Arc;

fn init_logger(log_config_path: Option<&str>) {

    if let Some(path) = log_config_path {
        if let Err(err) = log4rs::init_file(path, Default::default()) {
            println!("Error while initializing log by {}: {}", err, err);
        } else {
            return;
        }
    }

    let level = log::LevelFilter::Trace; 
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
                .build(log::LevelFilter::Trace),
        )
        .unwrap();

    let result = log4rs::init_config(config);
    if let Err(e) = result {
        println!("Error init log: {}", e);
    }
}

fn log_version() {
    log::info!(
        "Execute {:?}\nCOMMIT_ID: {:?}\nBUILD_DATE: {:?}\nCOMMIT_DATE: {:?}\nGIT_BRANCH: {:?}\n", // RUST_VERSION:{}\n
        std::option_env!("CARGO_PKG_VERSION"),
        std::option_env!("BUILD_GIT_COMMIT"),
        std::option_env!("BUILD_TIME"),
        std::option_env!("BUILD_GIT_DATE"),
        std::option_env!("BUILD_GIT_BRANCH"),
        //std::env!("BUILD_RUST_VERSION") // TODO
    );
}

#[cfg(feature = "external_db")]
fn start_external_db(config: &TonNodeConfig) -> Result<Vec<Arc<dyn ExternalDb>>> {
    Ok(vec!(
        external_db::create_external_db(
            config.external_db_config().ok_or_else(|| error!("Can't load external database config!"))?
        )?
    ))
}

#[cfg(not(feature = "external_db"))]
fn start_external_db(_config: &TonNodeConfig) -> Result<Vec<Arc<dyn ExternalDb>>> {
    Ok(vec!())
}





async fn start_engine(config: TonNodeConfig) -> Result<()> {
    let external_db = start_external_db(&config)?;
    crate::engine::run(config, external_db).await?;
    Ok(())
}

fn main() {
    let app = clap::App::new("TON node")
        .arg(clap::Arg::with_name("config")
            .short("c")
            .long("--configs")
            .value_name("config")
            .default_value("./"));

    let matches = app.get_matches();

    let config_dir_path = match matches.value_of("config") {
        Some(config) => {
            config
        },
        None => {
            println!("Can't load config: config dir is not set!");
            return;
        }
    };

    let config = match TonNodeConfig::from_file(config_dir_path, "config.json", "default_config.json") {
        Err(e) => {
            println!("Can't load config: {:?}", e);
            return;
        },
        Ok(c) => c
    };

    init_logger(config.log_config_path().as_ref().map(|s| s.as_str()));
    log_version();
    
    lazy_static::initialize(&STATSD);
    
    let mut runtime = tokio::runtime::Builder::new()
        .threaded_scheduler()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .expect("Can't create tokio runtime");

    init_jaeger();
    
    runtime.block_on(async move {
        if let Err(e) = start_engine(config).await {
            log::error!("Can't start node's Engine: {:?}", e);
        }
    });
}
