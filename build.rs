/*
 * Copyright 2018-2021 TON DEV SOLUTIONS LTD.
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

use rustc_version;
use std::process::Command;
use std::fs::File;
use std::io::{BufReader, BufRead};

fn main() {
    // --                                                       // CARGO_PKG_VERSION
    let mut node_blk_ver= String::from("Unknown");      // NODE_BLK_VER
    let     r_ver:String;                                       // RUST_VERSION
    let mut build_time = String::from("Unknown");       // BUILD_TIME
    let mut git_branch = String::from("Unknown");       // BUILD_GIT_BRANCH
    let mut git_commit = String::from("Unknown");       // BUILD_GIT_COMMIT_ID
    let mut commit_date = String::from("Unknown");      // BUILD_GIT_COMMIT_DATE
    
    // ------------------------------------------------------------------------
    // Rust version
    r_ver = rustc_version::version().unwrap().to_string();

    // ------------------------------------------------------------------------
    // Node block supported version: 
    let sup_blk_file = "src/validating_utils.rs";
    let func_str = "pub fn supported_version";
    let sup_blk_ver = get_supported_blk_version(sup_blk_file, func_str);
    if sup_blk_ver > 0 {
        node_blk_ver = sup_blk_ver.to_string();
    } 

    // ------------------------------------------------------------------------
    // Node build time:
    let b_time = Command::new("date").args(&["+%F %T %z"]).output();
    if let Ok(b_time) = b_time {
        build_time = String::from_utf8(b_time.stdout).unwrap_or_else(|_| "Unknown".to_string());
    }

    // ------------------------------------------------------------------------
    // Git branch: 
    let branch = Command::new("git")
        .args(&["rev-parse", "--abbrev-ref", "HEAD"])
        .output();

    if let Ok(branch) = branch {
        git_branch = String::from_utf8(branch.stdout).unwrap_or_else(|_| "Unknown".to_string());
    }

    // ------------------------------------------------------------------------
    // Git commit ID: 
    let last = Command::new("git").args(&["rev-parse", "HEAD"]).output();
    if let Ok(last) = last {
        git_commit = String::from_utf8(last.stdout).unwrap_or_else(|_| "Unknown".to_string());
    }

    // ------------------------------------------------------------------------
    // Git commit ID: 
    let time = Command::new("git").args(&["log", "-1", "--pretty=format:%ci"]).output();
    if let Ok(time) = time {
        commit_date = String::from_utf8(time.stdout).unwrap_or_else(|_| "Unknown".to_string());
    }

    // =========================================================================
    // set env vars
    println!("cargo:rustc-env=NODE_BLK_VER={}", node_blk_ver);
    println!("cargo:rustc-env=BUILD_TIME={}", build_time);
    println!("cargo:rustc-env=RUST_VERSION={}", r_ver);
    println!("cargo:rustc-env=BUILD_GIT_BRANCH={}", git_branch);
    println!("cargo:rustc-env=BUILD_GIT_COMMIT_ID={}", git_commit);
    println!("cargo:rustc-env=BUILD_GIT_COMMIT_DATE={}", commit_date);
}

fn get_supported_blk_version(path:&str, func_str:&str)-> u32 {
    let mut sup_blk_ver:u32 = 0;
    let file = File::open(path).unwrap();
    let content = BufReader::new(file);
    let lines: Vec<String> = content
        .lines()
        .map(|line| line.expect("Something went wrong"))
        .collect();
    for (current, next) in lines.iter().zip(lines.iter().skip(1)) {
        if current.contains(func_str){
            sup_blk_ver = next.trim().parse::<u32>().unwrap();
            break;
        }
    }
   return sup_blk_ver;
}
