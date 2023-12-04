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

use crate::archives::package_id::PackageType;
use super::PackageId;
use std::path::Path;
use ton_block::UnixTime32;

#[test]
fn test_construction() {
    assert_eq!(PackageId::for_block(0xFFFF_FFF0), PackageId::with_values(0xFFFF_FFF0, PackageType::Blocks));
    assert_eq!(PackageId::for_key_block(0xFFFF_FFF0), PackageId::with_values((0xFFFF_FFF0) as u32, PackageType::KeyBlocks));

    assert_eq!(PackageId::for_temp(&UnixTime32::new(5000)), PackageId::with_values(3600, PackageType::Temp));
    assert_eq!(PackageId::for_temp(&UnixTime32::new(20_000)), PackageId::with_values(18_000, PackageType::Temp));

    // At least one of checks must success (other can fail because of time changing):
    assert!(PackageId::for_temp_now() == PackageId::for_temp(&UnixTime32::now())
        || PackageId::for_temp_now() == PackageId::for_temp(&UnixTime32::now()));
}

#[test]
fn test_paths() {
    let id = PackageId::for_block(123);
    assert_eq!(id.name(), Path::new("archive.00123"));
    assert_eq!(id.path(), Path::new("archive/packages/arch0000/"));

    let id = PackageId::for_block(123_456);
    assert_eq!(id.name(), Path::new("archive.123456"));
    assert_eq!(id.path(), Path::new("archive/packages/arch0001/"));

    let id = PackageId::for_block(6_123_456);
    assert_eq!(id.name(), Path::new("archive.6123456"));
    assert_eq!(id.path(), Path::new("archive/packages/arch0061/"));

    let id = PackageId::for_key_block(123);
    assert_eq!(id.name(), Path::new("key.archive.000123"));
    assert_eq!(id.path(), Path::new("archive/packages/key000/"));

    let id = PackageId::for_key_block(223_456);
    assert_eq!(id.name(), Path::new("key.archive.223456"));
    assert_eq!(id.path(), Path::new("archive/packages/key000/"));

    let id = PackageId::for_key_block(12_223_456);
    assert_eq!(id.name(), Path::new("key.archive.12223456"));
    assert_eq!(id.path(), Path::new("archive/packages/key001/"));

    let temp = PackageId::for_temp(&UnixTime32::new(20_000));
    assert_eq!(temp.name(), Path::new("temp.archive.18000"));
    assert_eq!(temp.path(), Path::new("files/packages/"));

    assert_eq!(temp.full_path(Path::new("/tmp/"), "pack"),
               Path::new("/tmp/files/packages/temp.archive.18000.pack"));
}
