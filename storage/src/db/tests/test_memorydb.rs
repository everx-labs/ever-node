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
        memorydb::MemoryDb, 
        tests::utils::{
            expect_error, expect_key_not_found_error, KEY0, KEY1, test_for_dropped_error
        },
        traits::{
            Kvc, KvcReadable, KvcSnapshotable, KvcTransaction, KvcTransactional, KvcWriteable
        }
    },
    error::StorageError
};
use ton_types::Result;

#[test]
fn test_get_put_delete() -> Result<()> {
    let db = MemoryDb::new();

    db.delete(&KEY0)?;

    expect_key_not_found_error(db.get(&KEY0), KEY0);
    db.put(&KEY0, &[0])?;
    assert_eq!(db.get(&KEY0)?.as_ref(), &[0]);
    expect_key_not_found_error(db.get(&KEY1), KEY1);
    db.put(&KEY1, &[1])?;
    assert_eq!(db.get(&KEY1)?.as_ref(), &[1]);

    db.delete(&KEY0)?;
    expect_key_not_found_error(db.get(&KEY0), KEY0);
    assert_eq!(db.get(&KEY1)?.as_ref(), &[1]);
    db.delete(&KEY1)?;
    expect_key_not_found_error(db.get(&KEY1), KEY1);

    Ok(())
}

#[test]
fn test_transactions() -> Result<()> {
    let db = MemoryDb::new();

    assert!(db.is_empty()?);
    {
        let mut transaction = db.begin_transaction()?;
        transaction.put(&KEY0, &[0])?;
        transaction.put(&KEY1, &[1])?;
        assert!(db.is_empty()?);
        // Transaction should be rolled back on drop
    }

    assert!(db.is_empty()?);
    {
        let mut transaction = db.begin_transaction()?;
        transaction.put(&KEY0, &[0])?;
        transaction.put(&KEY1, &[1])?;
        assert!(db.is_empty()?);
        transaction.commit()?;
    }
    assert_eq!(db.len()?, 2);

    Ok(())
}

#[test]
fn test_destroy() -> Result<()> {
    let mut db = MemoryDb::new();
    assert!(db.is_empty()?);
    db.put(&KEY0, &[0])?;
    assert!(!db.is_empty()?);
    {
        let _transaction: Box<dyn KvcTransaction<&str>> = db.begin_transaction()?;
        expect_error(db.destroy(), StorageError::HasActiveTransactions);
    }
    assert!(!db.is_empty()?);
    assert!(db.destroy().is_ok());

    test_for_dropped_error(db.get(&KEY0));
    test_for_dropped_error(db.put(&KEY0, &[0]));
    test_for_dropped_error(db.delete(&KEY0));
    test_for_dropped_error(db.contains(&KEY0));
    test_for_dropped_error(db.len());
    test_for_dropped_error(db.is_empty());

    {
        let mut transaction = db.begin_transaction()?;
        transaction.put(&KEY0, &[0])?;
        test_for_dropped_error(transaction.commit());
    }

    Ok(())
}

#[test]
fn test_snapshots() -> Result<()> {
    let db = MemoryDb::new();

    {
        db.put(&KEY0, &[0])?;
        let snapshot = db.snapshot()?;
        db.put(&KEY1, &[1])?;
        assert_eq!(snapshot.get(&KEY0)?.as_ref(), &[0]);
        assert!(snapshot.get(&KEY1).is_err());
        assert_eq!(db.get(&KEY0)?.as_ref(), &[0]);
        assert_eq!(db.get(&KEY1)?.as_ref(), &[1]);
    }

    Ok(())
}

#[test]
fn test_foreach() -> Result<()> {
    let db = MemoryDb::new();

    let vec = vec![(KEY0, 0), (KEY1, 1)];
    for (key, value) in vec.iter() {
        db.put(key, &[*value])?;
    }

    let mut index = 0;
    let result = KvcReadable::<&[u8]>::for_each(&db, &mut |key, value| {
        let (key0, value0) = vec.get(index).unwrap();
        index += 1;
        assert_eq!(key, *key0);
        assert_eq!(value, &[*value0]);

        Ok(true)
    })?;

    assert!(result);

    let snapshot = db.snapshot()?;
    index = 0;
    let result = KvcReadable::<&[u8]>::for_each(snapshot.as_ref(), &mut |key, value| {
        let (key0, value0) = vec.get(index).unwrap();
        index += 1;
        assert_eq!(key, *key0);
        assert_eq!(value, &[*value0]);

        Ok(true)
    })?;

    assert!(result);

    Ok(())
}

#[test]
fn test_get_slice() -> Result<()> {
    let db = MemoryDb::new();

    expect_key_not_found_error(db.get_slice(&KEY0, 0, 5), KEY0);

    db.put(&KEY0, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])?;
    assert_eq!(db.get_slice(&KEY0, 0, 1)?.as_ref(), &[0]);
    assert_eq!(db.get_slice(&KEY0, 0, 10)?.as_ref(), &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    assert_eq!(db.get_slice(&KEY0, 5, 5)?.as_ref(), &[5, 6, 7, 8, 9]);
    assert_eq!(db.get_slice(&KEY0, 5, 2)?.as_ref(), &[5, 6]);

    expect_error(db.get_slice(&KEY0, 7, 5), StorageError::OutOfRange);
    expect_error(db.get_slice(&KEY0, 7, 4), StorageError::OutOfRange);
    expect_error(db.get_slice(&KEY0, 11, 1), StorageError::OutOfRange);

    Ok(())
}