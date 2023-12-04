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

use crate::archives::{
    package::{Package, PKG_HEADER_SIZE, read_package_from_file}, package_entry::PackageEntry,
    package_entry_id::{GetFileName, PackageEntryId}, package_id::PackageId
};
use std::{fs, path::{Path, PathBuf}, str::FromStr};
use ton_block::BlockIdExt;
use ton_types::{Result, UInt256};

const DB_PATH: &str = "../target/test";

const ARCHIVE_00001_GOLD_PATH: &str = "src/archives/tests/testdata/archive.00001.pack.gold";
const KEY_ARCHIVE_00000_PATH: &str = "src/archives/tests/testdata/key.archive.000000.pack";

#[tokio::test]
async fn test_creation() -> Result<()> {

    const DB_NAME: &str = "test_package_creation"; 

    let dir = Path::new(DB_PATH).join(DB_NAME);
    fs::remove_dir_all(dir.as_path()).ok();
    let package_id = PackageId::for_block(1);
    let filename = package_id.full_path(dir.as_path(), "pack");

    fs::create_dir_all(filename.parent().unwrap())?;
    let package = Package::open(filename, false, true).await?;

    let block_id = BlockIdExt::default();
    let entry_id = PackageEntryId::<_, &UInt256, &UInt256>::Block(&block_id);
    let entry = PackageEntry::with_data(entry_id.filename(), vec![1, 2, 3]);
    package.append_entry(&entry, |_offset, _size| Ok(())).await?;

    let entry_id = PackageEntryId::<_, &UInt256, &UInt256>::Proof(&block_id);
    let entry = PackageEntry::with_data(entry_id.filename(), vec![3, 2, 1]);
    package.append_entry(&entry, |_offset, _size| Ok(())).await?;

    assert_eq!(
        tokio::fs::read(package.path()).await.unwrap(), 
        tokio::fs::read(ARCHIVE_00001_GOLD_PATH).await.unwrap()
    );

    fs::remove_dir_all(dir.as_path()).unwrap();
    Ok(())

}

#[tokio::test]
async fn test_reading() -> Result<()> {

    let path = PathBuf::from_str(ARCHIVE_00001_GOLD_PATH)?;
    let package = Package::open(path, true, false).await?;
    assert_eq!(package.size(), 342 - PKG_HEADER_SIZE as u64);
    let entry = package.read_entry(0).await?;

    assert_eq!(entry.filename(), concat!("block_(0,8000000000000000,0)",
        ":0000000000000000000000000000000000000000000000000000000000000000",
        ":0000000000000000000000000000000000000000000000000000000000000000"));
    assert_eq!(entry.data(), &vec![1u8, 2, 3]);
    Ok(())

}

#[tokio::test]
async fn test_pkg_reader() -> Result<()> {

    const FILES: [(&str, usize); 15] = [
        ("proof_(-1,8000000000000000,9555):7E5C43F016E4C3B5C9A0BC73F4EC648876606646B6F5F450F225373499D925DC:0359939B6284F670E529B252E3A9F2547392B84746B1440AE6B17B7E28371BEE", 5319),
        ("proof_(-1,8000000000000000,12301):BF407B2F4DC67D4096A43E1045789C9A7C84F7521FBD26E2A339B1BAA0C174D1:58604A1715832BAB22D775FBDB5D086B4F67C61151C73C9F93D5C13039F64742", 5335),
        ("proof_(-1,8000000000000000,29186):DF052033165A78F256A06AE9B85F5FA80C30834CC32FF031AE71095A6AC09E27:E22252406B74995350AD73C43A5CAF04449456546AF6B502A4C3FE33A9D05B0B", 10885),
        ("proof_(-1,8000000000000000,31541):D749D0348ED761C5C00540FA86C17F0E63F77710CA5B8B15F80C8F3B312ECDD9:E0F01DECC9DB51CFA331316ADB2B01262733CF45FA843196FCFD7BDC56B2AA23", 9765),
        ("proof_(-1,8000000000000000,46568):07FE7257ECF5525616F07906175C5D5B669FF5B66EE350E2887499E095959185:17730AE4088DFA2D3D2A265FD291F83A6F6CF60F16454D4633FC26B07B10E705", 19239),
        ("proof_(-1,8000000000000000,48696):7CBB3355466440D531058BCDED3F57E0A405590E394C42E3494A820B818EFD69:C56361C9C3B2274A7B51B140C06C2C1BC65A3899956775B79E5CD43834B6AEE1", 16237),
        ("proof_(-1,8000000000000000,63329):51BC9E4E8849ADA0A8DCD5FFAB27D1DBB95DD06FA7356390E86EAD77ED190C27:148DAD6F844242EE70F60C368928FE1E0735DA9EA67AE263F5CE1BC1369F8507", 27922),
        ("proof_(-1,8000000000000000,65364):471401A99725A869712CB8FCF3E8736C3DB9282A549251837716F8210283F662:A2252E733B6FEDDD53C5DF361E10690FED7C76253C79348C00064366B8AF3B11", 23892),
        ("proof_(-1,8000000000000000,78875):3B7762DCD71FBB4DDDA385318F1E9718D1EA740D58B7ADFF3453F2EC489B6451:23F03E3FE94148BAE2C50792BF8D7086E74C9B6ACE3F0F6C2D464DD9D84CF052", 31353),
        ("proof_(-1,8000000000000000,78875):3B7762DCD71FBB4DDDA385318F1E9718D1EA740D58B7ADFF3453F2EC489B6451:23F03E3FE94148BAE2C50792BF8D7086E74C9B6ACE3F0F6C2D464DD9D84CF052", 31777),
        ("proof_(-1,8000000000000000,80723):BEB3A4081099E9AB2DF83A4F5C3D3137D9E2C3846FC72D005D4EB7B40D01DCBC:97FFAEFA8BD166CE0F20C98B4BD069C2D6BCDA72A1502B37671D806F1F6A7768", 24974),
        ("proof_(-1,8000000000000000,95662):F023BAC6BEB0F617CDFCC44F6FAA8F45D440B4168AFDFB9C9E63BD3E9694583B:EE978651BD08D8CF3734569D9B102B5D7A5BA2C8770C1D37194F617D83C99549", 33650),
        ("proof_(-1,8000000000000000,97655):3E7C288E2FEAA904A9C8324A45D57911007FA59FF555601AC9BDD3D21E5332CB:B3C367F7397710FBDF03F49AFA47B5DE454EFBCA10B19DE0D14B663D025A452C", 24721),
        ("proof_(-1,8000000000000000,112390):B3ADC6B0EC166F00322B92703F950129AF62ADD16893D5B67115A854116482C7:60D4ABF8C22C6E714F0F22BA2A968B17D6354F777DDEBD2DED554BF7760AB681", 32799),
        ("proof_(-1,8000000000000000,114493):08C29CB4020ED067F669E22CA009C5DD251796D66C64EE77E9F17E20E78CC16C:799AC961D302EC87A58BE42E31AE9C5381C4F129D5899BD5EAB41860D6A8F331", 25338),
    ];

    let mut reader = read_package_from_file(PathBuf::from_str(KEY_ARCHIVE_00000_PATH)?).await?;
    let mut i = 0;
    while let Some(entry) = reader.next().await? {
        assert_eq!(entry.filename(), FILES[i].0);
        assert_eq!(entry.data().len(), FILES[i].1);

        i += 1;
    }

    Ok(())

}
