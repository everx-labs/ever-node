/*
* Copyright 2018-2022 TON DEV SOLUTIONS LTD.
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

use std::env;
use std::process::Command;
use std::fs;
use std::io::{prelude::*};
use toml::{Value as Toml};

// =========================================================

fn main() -> Result<(), String> {
    //let car_man_dir = std::option_env!("CARGO_MANIFEST_DIR").unwrap_or("Not set").to_string();
    
    let build_mode: &str;
    if Ok("release".to_owned()) == std::env::var("PROFILE") {
        build_mode = "RELEASE";
    } else 
    {
        build_mode = "DEBUG";
    } 

    execute_command(Command::new("cargo").arg("update"))?;

    let cargo_toml = get_toml("Cargo.toml");
    let top_pkg_name = parse_package_name(&cargo_toml);
    let cargo_lock = get_toml("Cargo.lock");
    let cargo_evx_deps_list = parse_deps(&cargo_lock, top_pkg_name)?;


    let mut gc_abi: String = String::new();
    let mut gc_adnl: String = String::new();
    let mut gc_block_json: String = String::new();
    let mut gc_block: String = String::new();
    let mut gc_crypto: String = String::new();
    let mut gc_dht: String = String::new();
    let mut gc_executor: String = String::new();
    let mut gc_overlay: String = String::new();
    let mut gc_rldp: String = String::new();
    let mut gc_tl: String = String::new();
    let mut gc_types: String = String::new();
    let mut gc_vm: String = String::new();
    let mut gc_lockfree: String = String::new();
   
    for curr_dep in cargo_evx_deps_list {
        let dep_hash: String = curr_dep.clone().split("#").last().unwrap().to_string();
        if curr_dep.contains("labs-abi")        {gc_abi          =  dep_hash} else 
        if curr_dep.contains("labs-adnl")       {gc_adnl         =  dep_hash;} else 
        if curr_dep.contains("labs-block-json") {gc_block_json   =  dep_hash;} else 
        if curr_dep.contains("labs-block")      {gc_block        =  dep_hash;} else 
        if curr_dep.contains("labs-crypto")     {gc_crypto       =  dep_hash;} else 
        if curr_dep.contains("labs-dht")        {gc_dht          =  dep_hash;} else 
        if curr_dep.contains("labs-executor")   {gc_executor     =  dep_hash;} else 
        if curr_dep.contains("labs-overlay")    {gc_overlay      =  dep_hash;} else 
        if curr_dep.contains("labs-rldp")       {gc_rldp         =  dep_hash;} else 
        if curr_dep.contains("labs-tl")         {gc_tl           =  dep_hash;} else 
        if curr_dep.contains("labs-types")      {gc_types        =  dep_hash;} else 
        if curr_dep.contains("labs-vm")         {gc_vm           =  dep_hash;} else 
        if curr_dep.contains("lockfree")        {gc_lockfree     =  dep_hash;} 
    }

    // =========================================================

    let git_branch_remote = get_value("git", &["name-rev", "--name-only", "HEAD"]);
    let gb: Vec<&str> = git_branch_remote.split('/').collect::<Vec<&str>>();
    let git_branch = gb.last().unwrap();

    let git_commit =    get_value("git",    &["rev-parse", "HEAD"]);
    let commit_date =   get_value("git",    &["log", "-1", "--date=iso", "--pretty=format:%cd"]);
    let build_time =    get_value("date",   &["+%Y-%m-%d %T %z"]);
    let rust_version =  get_value("rustc",  &["--version"]);

    // =========================================================

    println!("cargo:rustc-env=BUILD_RUST_VERSION={}",   rust_version);
    println!("cargo:rustc-env=BUILD_TIME={}",           build_time);
    println!("cargo:rustc-env=BUILD_MODE={}",           build_mode);
    println!("cargo:rustc-env=BUILD_GIT_COMMIT={}",     git_commit);
    println!("cargo:rustc-env=BUILD_GIT_BRANCH={}",     git_branch);
    println!("cargo:rustc-env=BUILD_GIT_DATE={}",       commit_date);

    println!("cargo:rustc-env=GC_ABI={}",           gc_abi);
    println!("cargo:rustc-env=GC_ADNL={}",          gc_adnl);
    println!("cargo:rustc-env=GC_BLOCK_JSON={}",    gc_block_json);
    println!("cargo:rustc-env=GC_BLOCK={}",         gc_block);
    println!("cargo:rustc-env=GC_CRYPTO={}",        gc_crypto);
    println!("cargo:rustc-env=GC_DHT={}",           gc_dht);
    println!("cargo:rustc-env=GC_EXECUTOR={}",      gc_executor);
    println!("cargo:rustc-env=GC_OVERLAY={}",       gc_overlay);
    println!("cargo:rustc-env=GC_RLDP={}",          gc_rldp);
    println!("cargo:rustc-env=GC_TL={}",            gc_tl);
    println!("cargo:rustc-env=GC_TYPES={}",         gc_types);
    println!("cargo:rustc-env=GC_VM={}",            gc_vm);
    println!("cargo:rustc-env=GC_LOCKFREE={}",      gc_lockfree);

    Ok(())
}

// #########################################################################################################
fn get_value(cmd: &str, args: &[&str]) -> String {
    if let Ok(result) = Command::new(cmd).args(args).output() {
        if let Ok(result) = String::from_utf8(result.stdout) {
            return result
        }
    }
    "Unknown".to_string()
}
fn execute_command(command: &mut Command) -> Result<(), String> {
    let mut child = command.envs(env::vars()).spawn()
        .map_err(|_| "failed to execute process".to_string())?;

    let exit_status = child.wait().expect("failed to run command");

    if !exit_status.success() {
        match exit_status.code() {
            Some(code) => Err(format!("Exited with status code: {}", code)),
            None => Err(format!("Process terminated by signal")),
        }
    } else {
        Ok(())
    }
}

fn get_toml(file_path: &str) -> Toml {
    let mut toml_file = fs::File::open(file_path).unwrap();
    let mut toml_string = String::new();
    toml_file.read_to_string(&mut toml_string).unwrap();
    toml_string.parse().expect("failed to parse toml")
}

fn parse_package_name(toml: &Toml) -> &str {
    match toml {
        &Toml::Table(ref table) => {
            match table.get("package") {
                Some(&Toml::Table(ref table)) => {
                    match table.get("name") {
                        Some(&Toml::String(ref name)) => name,
                        _ => panic!("failed to parse name"),
                    }
                }
                _ => panic!("failed to parse package"),
            }
        }
        _ => panic!("failed to parse Cargo.toml: incorrect format"),
    }
}

fn parse_deps(toml: &Toml, top_pkg_name: &str) -> Result<Vec<String>, String> {
    match cargo_lock_find_package(toml, top_pkg_name)? {
        &Toml::Table(ref pkg) => {
            if let Some(&Toml::Array(ref deps_toml_array)) = pkg.get("dependencies") {
                deps_toml_array.iter()
                    .map(|value| {
                        if let Some(crate_name) = value.as_str() {
                                crate_name_version(toml, crate_name)
                        } else {
                            Err("Empty dependency".to_string())
                        }
                    })
                    .collect()
            } else {
                Err("error parsing dependencies table".to_string())
            }
        }
        _ => Err("error parsing dependencies table".to_string()),
    }
}

fn cargo_lock_find_package<'a>(toml: &'a Toml, pkg_name: &str) -> Result<&'a Toml, String> {
    match toml.get("package") {
        Some(&Toml::Array(ref pkgs)) => {
            pkgs.iter()
                .find(|pkg| {
                    pkg.get("name")
                    .map_or(false, |name| name.as_str().unwrap_or("") == pkg_name)
                })
                    .map_or(Err(format!("failed to find package {}", pkg_name)), |x| Ok(x))
        }
        _ => Err("failed to find packages in Cargo.lock".to_string()),
    }
}

fn crate_name_version(toml: &Toml, crate_name: &str) -> Result<String, String> {
    println!("Crate name: {}", crate_name.to_string());
    if crate_name.contains(" ") {
        let mut value_parts = crate_name.split(" ");
        let crate_name = value_parts.next()
            .map_or(Err(format!("failed to parse name from dependency string {:?}",    crate_name)), |x| Ok(x),
        )?;
        let crate_version = value_parts.next()
            .map_or(Err(format!("failed to parse version from dependency string {:?}", crate_name)), |x| Ok(x),
        )?;
        
        let value_pkg = cargo_lock_find_package(toml, crate_name)?;

        let crate_source_str = value_pkg.get("source")            
        .map_or("", |hash| hash.as_str().unwrap_or(""));
        
        //let crate_hash = crate_source_str.split("#").last().unwrap();
        
        println!("{}:{}:{}\n", crate_source_str, crate_name, crate_version);

        Ok(format!("{}:{}:{}", crate_source_str, crate_name, crate_version))
    } else {
        let value_pkg = cargo_lock_find_package(toml, crate_name)?;
        let crate_version = value_pkg.get("version")
            .map_or(Err(format!("Version not found for {}",     crate_name)), |x| Ok(x),)?.as_str()
            .map_or(Err(format!("Invalid version field for {}", crate_name)), |x| Ok(x))?;

        let value_pkg = cargo_lock_find_package(toml, crate_name)?;

        let crate_source_str = value_pkg.get("source")
            .map_or("", |hash| hash.as_str().unwrap_or(""));

        //let crate_hash = crate_source_str.split("#").last().unwrap();

        println!("{}:{}:{}\n", crate_source_str, crate_name, crate_version);

        Ok(format!("{}:{}:{}", crate_source_str, crate_name, crate_version))
    }
}
