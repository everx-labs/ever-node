use clap::{Arg, App};
use std::{
    sync::Arc,
};
use ton_node::{
    collator_test_bundle::create_engine_allocated, 
    internal_db::{InternalDb, InternalDbConfig, CURRENT_DB_VERSION},
};
#[cfg(feature = "telemetry")]
use ton_node::collator_test_bundle::create_engine_telemetry;
use ton_types::Result;

pub async fn open_db(db_dir: &str) -> Result<Arc<InternalDb>> {
    let db_config = InternalDbConfig { 
        cells_gc_interval_sec: 0, 
        db_directory: db_dir.to_string() 
    };
    #[cfg(feature = "telemetry")]
    let telemetry = create_engine_telemetry();
    let allocated = create_engine_allocated();
    let db = Arc::new(
        InternalDb::with_update(
            db_config, 
            false,
            false,
            false,
            &|| Ok(()),
            None,
            #[cfg(feature = "telemetry")]
            telemetry.clone(),
            allocated.clone(),
        ).await?
    );
    Ok(db)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .arg(Arg::with_name("PATH")
            .short("p")
            .long("path")
            .help("path to DB")
            .takes_value(true)
            .required(true)
            .default_value("node_db")
            .number_of_values(1))
        .get_matches();

    // TODO
    // print db options (last block etc.)
    // load block <block id> <file name>
    // print block <block id>
    // print last mc block
    // load account [block id] <acc id> <file name>
    // print account [block id] <acc id>
    // print config [block id]
    // load state <block id> <file name>

    let db_dir = args.value_of("PATH").expect("path to database must be set");

    let db = open_db(&db_dir).await?;

    println!("Current DB version: {}", db.resolve_db_version()?);
    println!("Supported DB version: {}", CURRENT_DB_VERSION);

    Ok(())
}
