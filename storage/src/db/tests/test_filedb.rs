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

use crate::{
    db::{
        filedb::FileDb, tests::utils::{expect_key_not_found_error, expect_error},
    },
    error::StorageError
};
use std::{ops::{Deref, DerefMut}, path::Path};
use ton_types::Result;

const KEY0: &[u8] = b"key0key0key0key0";
const KEY1: &[u8] = b"key1key1key1key1";

struct AutoDestroyableDb {
    db: FileDb,
}

impl AutoDestroyableDb {
    pub fn new(path: &str) -> Self {
        Self {
            db: FileDb::with_path(path),
        }
    }
}

impl Deref for AutoDestroyableDb {
    type Target = FileDb;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl DerefMut for AutoDestroyableDb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.db
    }
}

impl Drop for AutoDestroyableDb {
    fn drop(&mut self) {
        if self.path().is_dir() {
            std::fs::remove_dir_all(&self.db.path())
                .expect("Failed to destroy DB");
        }
    }
}

#[test]
fn test_make_path() {
    let db = FileDb::with_path("/test");
    let key = vec![
        0x12, 0x34, 0x56, 0x78,
        0xAA, 0xBB, 0xCC, 0xDD,
        0xEE, 0xFF, 0xFF, 0xEE,
        0x87, 0x65, 0x43, 0x21,
    ];
    let path = db.make_path(key.as_slice());

    assert_eq!(path, Path::new("/test/1234/5678aabbccddeeffffee87654321"));
}

#[tokio::test]
async fn test_get() -> Result<()> {
    let db = AutoDestroyableDb::new("filedb_test_get");

    expect_key_not_found_error(db.read_whole_file(&KEY0).await, KEY0);
    db.write_whole_file(&KEY0, &[0]).await?;
    assert_eq!(&db.read_whole_file(&KEY0).await?, &[0]);
    expect_key_not_found_error(db.read_whole_file(&KEY1).await, KEY1);

    Ok(())
}

#[tokio::test]
async fn test_put() -> Result<()> {
    let db = AutoDestroyableDb::new("filedb_test_put");

    db.write_whole_file(&KEY0, &[0]).await?;
    assert_eq!(&db.read_whole_file(&KEY0).await?, &[0]);
    expect_key_not_found_error(db.read_whole_file(&KEY1).await, KEY1);
    db.write_whole_file(&KEY1, &[1]).await?;
    assert_eq!(&db.read_whole_file(&KEY1).await?, &[1]);

    Ok(())
}

#[tokio::test]
async fn test_delete() -> Result<()> {
    let db = AutoDestroyableDb::new("filedb_test_delete");

    db.delete_file(&KEY0).await?;

    expect_key_not_found_error(db.read_whole_file(&KEY0).await, KEY0);
    db.write_whole_file(&KEY0, &[0]).await?;
    assert_eq!(&db.read_whole_file(&KEY0).await?, &[0]);
    expect_key_not_found_error(db.read_whole_file(&KEY1).await, KEY1);
    db.write_whole_file(&KEY1, &[1]).await?;
    assert_eq!(&db.read_whole_file(&KEY1).await?, &[1]);

    db.delete_file(&KEY0).await?;
    expect_key_not_found_error(db.read_whole_file(&KEY0).await, KEY0);
    assert_eq!(&db.read_whole_file(&KEY1).await?, &[1]);
    db.delete_file(&KEY1).await?;
    expect_key_not_found_error(db.read_whole_file(&KEY1).await, KEY1);

    Ok(())
}

#[tokio::test]
async fn test_destroy() -> Result<()> {
    let mut db = AutoDestroyableDb::new("filedb_test_destroy");
    assert!(!db.path().is_dir());

    db.destroy().await?;

    db.write_whole_file(&KEY0, &[0]).await?;
    assert!(db.path().is_dir());

    db.destroy().await?;

    assert!(!db.path().is_dir());

    Ok(())
}

#[tokio::test]
async fn test_get_size() -> Result<()> {
    let db = AutoDestroyableDb::new("filedb_test_get_size");

    expect_key_not_found_error(db.get_file_size(&KEY0).await, KEY0);

    db.write_whole_file(&KEY0, &[0, 1, 2, 3]).await?;
    assert_eq!(db.get_file_size(&KEY0).await?, 4);

    db.write_whole_file(&KEY1, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).await?;
    assert_eq!(db.get_file_size(&KEY1).await?, 10);

    db.write_whole_file(&KEY0, &[]).await?;
    assert_eq!(db.get_file_size(&KEY0).await?, 0);

    Ok(())
}

#[tokio::test]
async fn test_contains() -> Result<()> {
    let db = AutoDestroyableDb::new("filedb_test_contains");

    assert!(!db.contains(&KEY0).await?);

    db.write_whole_file(&KEY0, &[0]).await?;
    assert!(db.contains(&KEY0).await?);

    assert!(!db.contains(&KEY1).await?);

    db.write_whole_file(&KEY1, &[1]).await?;
    assert!(db.contains(&KEY1).await?);

    Ok(())
}

#[tokio::test]
async fn test_get_slice() -> Result<()> {
    let db = AutoDestroyableDb::new("filedb_test_get_slice");

    expect_key_not_found_error(db.read_file_part(&KEY0, 0, 5).await, KEY0);

    db.write_whole_file(&KEY0, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).await?;
    assert_eq!(&db.read_file_part(&KEY0, 0, 1).await?, &[0]);
    assert_eq!(&db.read_file_part(&KEY0, 0, 10).await?, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    assert_eq!(&db.read_file_part(&KEY0, 5, 5).await?, &[5, 6, 7, 8, 9]);
    assert_eq!(&db.read_file_part(&KEY0, 5, 2).await?, &[5, 6]);

    expect_error(db.read_file_part(&KEY0, 7, 5).await, StorageError::OutOfRange);
    expect_error(db.read_file_part(&KEY0, 7, 4).await, StorageError::OutOfRange);
    expect_error(db.read_file_part(&KEY0, 11, 1).await, StorageError::OutOfRange);

    Ok(())
}

#[tokio::test]
async fn test_for_each_key() -> Result<()> {
    let db = AutoDestroyableDb::new("filedb_test_for_each_key");

    for i in 1..=100 {
        let key = vec!(i; 32);
        let val = vec!(i; 1024);
        db.write_whole_file(&key, &val).await?;
    }

    let mut sum = 0_u32;

    db.for_each_key(&mut |key: &[u8]| {
        sum += key[0] as u32;
        Ok(true)
    })?;

    assert_eq!(sum, 5050);

    Ok(())
}