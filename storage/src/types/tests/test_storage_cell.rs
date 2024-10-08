use std::ops::Deref;

use ever_block::{create_big_cell, create_cell};

use crate::{db::rocksdb::{RocksDb, destroy_rocks_db}, StorageAlloc};
#[cfg(feature = "telemetry")]
use crate::StorageTelemetry;
use super::*;

const DB_PATH: &str = "../target/test";

async fn init_boc_db(db_name: &str) -> Result<Arc<DynamicBocDb>> {
    destroy_rocks_db(DB_PATH, db_name).await?;
    let db = RocksDb::with_path(DB_PATH, db_name)?;
    Ok(Arc::new(DynamicBocDb::with_db(
        db.clone(),
        "cell_db",
        DB_PATH,
        1_000_000,
        #[cfg(feature = "telemetry")]
        Arc::new(StorageTelemetry::default()),
        Arc::new(StorageAlloc::default()),
    )?))
}

#[tokio::test]
async fn test_storage_cell_serde() -> Result<()> {

    let boc_db = init_boc_db("test_storage_cell_serde").await?;

    let c1 = create_cell(vec!(), &[1, 2, 45, 76, 200])?;
    let c2 = create_cell(vec!(), &[10, 200, 45, 7, 20])?;
    let c3 = create_cell(vec!(c1.clone(), c2.clone()), &[1, 2, 45, 76, 200])?;

    let s1 = StorageCell::with_cell(c1.cell_impl().deref(), &boc_db, false, false)?;
    let s2 = StorageCell::with_cell(c2.cell_impl().deref(), &boc_db, false, false)?;
    let s3 = StorageCell::with_cell(c3.cell_impl().deref(), &boc_db, false, false)?;

    let d1 = StorageCell::serialize(&s1)?;
    let d2 = StorageCell::serialize(&s2)?;
    let d3 = StorageCell::serialize(&s3)?;

    assert!(s1 == StorageCell::deserialize(&boc_db, &c1.repr_hash(), &d1, false)?);
    assert!(s2 == StorageCell::deserialize(&boc_db, &c2.repr_hash(), &d2, false)?);
    assert!(s3 == StorageCell::deserialize(&boc_db, &c3.repr_hash(), &d3, false)?);

    Ok(())
}

fn crate_and_save_cell(boc_db: &Arc<DynamicBocDb>, data: &[u8]) -> Result<UInt256> {
    let cell = create_cell(vec!(), data)?;
    boc_db.save_boc(cell.clone(), true, &|| Ok(()), &mut None, false)?;
    Ok(cell.repr_hash())
}

#[tokio::test]
async fn test_storage_cell_migration_to_v6_1() -> Result<()> {

    let boc_db = init_boc_db("test_storage_cell_migration_to_v6_1").await?;

    let data = [1, 2, 3, 0x84];
    let hashes = [
        UInt256::from_slice(&[0x7a, 0x86, 0x6c, 0x58, 0xfb, 0x11, 0xab, 0xf4, 0x6c, 0xca, 0xf0, 0x2f, 0xdf, 0x02, 0x9c, 0x89, 0xfc, 0x24, 0xf6, 0x4d, 0x76, 0x07, 0x4a, 0x58, 0x0e, 0x0c, 0xab, 0x7d, 0x5c, 0xf5, 0x92, 0x5b,]),
        UInt256::from_slice(&[0x8a, 0x86, 0x6c, 0x58, 0xfb, 0x11, 0xab, 0xf4, 0x6c, 0xca, 0xf0, 0x2f, 0xdf, 0x02, 0x9c, 0x89, 0x00, 0x24, 0xf6, 0x4d, 0x76, 0x07, 0x4a, 0x58, 0x0e, 0x0c, 0xab, 0x7d, 0x5c, 0x11, 0x92, 0x5b,]),
        UInt256::default(),
        UInt256::default(),
    ];
    let cell_data = CellData::with_params(
        CellType::Ordinary,
        &data, // data
        1, // level mask
        2, // refs count
        false, // store hashes
        Some(hashes.clone()),
        Some([3, 2, 0, 0])
    )?;

    let mut v5_data = Vec::new();
    cell_data.serialize(&mut v5_data)?;
    let ref1 = crate_and_save_cell(&boc_db, &[1, 2, 3, 0x80])?;
    v5_data.extend_from_slice(ref1.as_slice()); // reference 1
    let ref2 = crate_and_save_cell(&boc_db, &[1, 1, 43, 0x80])?;
    v5_data.extend_from_slice(ref2.as_slice()); // reference 2
    let tree_bits_count = 329847_u64;
    v5_data.extend_from_slice(tree_bits_count.to_le_bytes().as_ref()); // tree bits/cells count
    let tree_cell_count = 3297_u64;
    v5_data.extend_from_slice(tree_cell_count.to_le_bytes().as_ref());

    let v6_data = StorageCell::migrate_to_v6(&v5_data)?;

    println!("v6 len: {}, v5 len {}", v6_data.len(), v5_data.len());

    let cell = StorageCell::deserialize(&boc_db, &hashes[1], &v6_data, false)?;

    assert!(cell.cell_type() == CellType::Ordinary);
    assert!(cell.data() == &data);
    assert!(cell.level() == 1);
    assert!(cell.references_count() == 2);
    assert!(cell.store_hashes() == false);
    assert!(cell.hash(0) == hashes[0]);
    assert!(cell.hash(1) == hashes[1]);
    assert!(cell.depth(0) == 3);
    assert!(cell.depth(1) == 2);
    assert!(cell.tree_bits_count() == tree_bits_count);
    assert!(cell.tree_cell_count() == tree_cell_count);
    assert!(cell.reference_repr_hash(0)? == ref1);
    assert!(cell.reference_repr_hash(1)? == ref2);

    let d1 = StorageCell::serialize(&cell)?;
    assert!(cell == StorageCell::deserialize(&boc_db, &hashes[1], &d1, false)?);

    Ok(())
}

#[tokio::test]
async fn test_storage_cell_migration_to_v6_2() -> Result<()> {

    let boc_db = init_boc_db("test_storage_cell_migration_to_v6_2").await?;

    let data = [
        0x1,
        0x1, 
        0x7a, 0x86, 0x6c, 0x58, 0xfb, 0x11, 0xab, 0xf4, 0x6c, 0xca, 0xf0, 0x2f, 0xdf, 0x02, 0x9c, 0x89,
        0xfc, 0x24, 0xf6, 0x4d, 0x76, 0x07, 0x4a, 0x58, 0x0e, 0x0c, 0xab, 0x7d, 0x5c, 0xf5, 0x92, 0x5b,
        0x1, 0x0,
        0x80,
    ];

    let mut cell_data = CellData::with_params(
        CellType::PrunedBranch,
        &data,
        1,
        0,
        false,
        None,
        None
    )?;

    let repr_hash = UInt256::rand();
    cell_data.set_hash_depth(0, repr_hash.as_slice(), 0)?;

    let mut v5_data = Vec::new();
    cell_data.serialize(&mut v5_data)?;

    let tree_bits_count = 2_u64;
    v5_data.extend_from_slice(tree_bits_count.to_le_bytes().as_ref()); // tree bits/cells count
    let tree_cell_count = 1_u64;
    v5_data.extend_from_slice(tree_cell_count.to_le_bytes().as_ref());

    let v6_data = StorageCell::migrate_to_v6(&v5_data)?;

    println!("v6 len: {}, v5 len {}", v6_data.len(), v5_data.len());

    let cell = StorageCell::deserialize(&boc_db, &repr_hash, &v6_data, false)?;

    assert!(cell.cell_type() == CellType::PrunedBranch);
    assert!(cell.data() == &data[..data.len() - 1]);
    assert!(cell.level() == 1);
    assert!(cell.references_count() == 0);
    assert!(cell.store_hashes() == false);
    assert!(cell.hash(MAX_LEVEL) == repr_hash);
    assert!(cell.hash(0) == UInt256::from_slice(&data[2..34]));
    assert!(cell.depth(MAX_LEVEL) == 0);
    assert!(cell.depth(0) == 0x0100);
    assert!(cell.tree_bits_count() == tree_bits_count);
    assert!(cell.tree_cell_count() == tree_cell_count);

    let d1 = StorageCell::serialize(&cell)?;
    assert!(cell == StorageCell::deserialize(&boc_db, &repr_hash, &d1, false)?);

    Ok(())
}

#[tokio::test]
async fn test_storage_cell_migration_to_v6_3() -> Result<()> {

    let boc_db = init_boc_db("test_storage_cell_migration_to_v6_3").await?;

    let data = [
        1, // type
        0b011_u8, // levelmask
        // 2 hashes
        0x7a, 0x86, 0x6c, 0x58, 0xfb, 0x11, 0xab, 0xf4, 0x6c, 0xca, 0xf0, 0x2f, 0xdf, 0x02, 0x9c, 0x89,
        0xfc, 0x24, 0xf6, 0x4d, 0x76, 0x07, 0x4a, 0x58, 0x0e, 0x0c, 0xab, 0x7d, 0x5c, 0xf5, 0x92, 0x5b,
        0x8a, 0x86, 0x6c, 0x98, 0xfb, 0x11, 0x0b, 0xf4, 0x6c, 0xca, 0xf0, 0x2f, 0xdf, 0x02, 0x9c, 0x89,
        0xfc, 0x24, 0xf6, 0x4d, 0x76, 0x07, 0x4a, 0x58, 0x0e, 0x0c, 0xab, 0x7d, 0x5c, 0xf5, 0x92, 0x5b,
        // 2 depths
        0, 1,
        0, 1,
        0x80
    ];

    let mut cell_data = CellData::with_params(
        CellType::PrunedBranch,
        &data,
        0b011,
        0,
        false,
        None,
        None
    )?;

    let repr_hash = UInt256::rand();
    cell_data.set_hash_depth(0, repr_hash.as_slice(), 0)?;

    let mut v5_data = Vec::new();
    cell_data.serialize(&mut v5_data)?;

    let tree_bits_count = 2_u64;
    v5_data.extend_from_slice(tree_bits_count.to_le_bytes().as_ref()); // tree bits/cells count
    let tree_cell_count = 1_u64;
    v5_data.extend_from_slice(tree_cell_count.to_le_bytes().as_ref());

    let v6_data = StorageCell::migrate_to_v6(&v5_data)?;

    println!("v6 len: {}, v5 len {}", v6_data.len(), v5_data.len());

    let cell = StorageCell::deserialize(&boc_db, &repr_hash, &v6_data, false)?;

    assert!(cell.cell_type() == CellType::PrunedBranch);
    assert!(cell.data() == &data[..data.len() - 1]);
    assert!(cell.level() == 2);
    assert!(cell.references_count() == 0);
    assert!(cell.store_hashes() == false);
    assert!(cell.hash(MAX_LEVEL) == repr_hash);
    assert!(cell.hash(0) == UInt256::from_slice(&data[2..34]));
    assert!(cell.hash(1) == UInt256::from_slice(&data[34..66]));
    assert!(cell.depth(MAX_LEVEL) == 0);
    assert!(cell.depth(0) == 1);
    assert!(cell.depth(1) == 1);
    assert!(cell.tree_bits_count() == tree_bits_count);
    assert!(cell.tree_cell_count() == tree_cell_count);

    let d1 = StorageCell::serialize(&cell)?;
    assert!(cell == StorageCell::deserialize(&boc_db, &repr_hash, &d1, false)?);

    Ok(())
}

#[tokio::test]
async fn test_storage_cell_migration_to_v6_4() -> Result<()> {

    let boc_db = init_boc_db("test_storage_cell_migration_to_v6_4").await?;

    let data = [1, 2, 3, 0x84];
    let hashes = [UInt256::rand(), UInt256::rand(), UInt256::rand(), UInt256::default()];
    let depths = [3, 2, 0, 0];

    let cell_data = CellData::with_params(
        CellType::Ordinary,
        &data, // data
        0b101, // level mask
        2, // refs count
        true, // store hashes
        Some(hashes.clone()),
        Some(depths.clone())
    )?;

    let mut v5_data = Vec::new();
    cell_data.serialize(&mut v5_data)?;

    let ref1 = crate_and_save_cell(&boc_db, &[1, 0x80])?;
    v5_data.extend_from_slice(ref1.as_slice()); // reference 1
    let ref2 = crate_and_save_cell(&boc_db, &[1, 26, 3, 5, 6, 3, 0x80])?;
    v5_data.extend_from_slice(ref2.as_slice()); // reference 2

    let tree_bits_count = 32847_u64;
    v5_data.extend_from_slice(tree_bits_count.to_le_bytes().as_ref()); // tree bits/cells count
    let tree_cell_count = 83294_u64;
    v5_data.extend_from_slice(tree_cell_count.to_le_bytes().as_ref());

    let v6_data = StorageCell::migrate_to_v6(&v5_data)?;

    println!("v6 len: {}, v5 len {}", v6_data.len(), v5_data.len());

    let cell = StorageCell::deserialize(&boc_db, &hashes[2], &v6_data, false)?;

    assert!(cell.cell_type() == CellType::Ordinary);
    assert!(cell.data() == data);
    assert!(cell.level() == 2);
    assert!(cell.level_mask().mask() == 0b101);
    assert!(cell.references_count() == 2);
    assert!(cell.reference_repr_hash(0)? == ref1);
    assert!(cell.reference_repr_hash(1)? == ref2);
    assert!(cell.store_hashes());
    assert!(cell.hash(MAX_LEVEL) == hashes[2]);
    assert!(cell.hash(0) == hashes[0]);
    assert!(cell.hash(1) == hashes[1]);
    assert!(cell.depth(MAX_LEVEL) == depths[2]);
    assert!(cell.depth(0) == depths[0]);
    assert!(cell.depth(1) == depths[1]);
    assert!(cell.tree_bits_count() == tree_bits_count);
    assert!(cell.tree_cell_count() == tree_cell_count);

    let d1 = StorageCell::serialize(&cell)?;
    assert!(cell == StorageCell::deserialize(&boc_db, &hashes[2], &d1, false)?);

    Ok(())
}

#[tokio::test]
async fn test_storage_cell_migration_to_v6_5() -> Result<()> {

    let boc_db = init_boc_db("test_storage_cell_migration_to_v6_5").await?;

    let data = [1, 2, 3, 0x84];
    let hashes = [UInt256::rand(), UInt256::default(), UInt256::default(), UInt256::default()];
    let depths = [0xf121, 0, 0, 0];

    let cell_data = CellData::with_params(
        CellType::Ordinary,
        &[1, 2, 3, 0x84], // data
        0, // level mask
        4, // refs count
        true, // store hashes
        Some(hashes.clone()),
        Some(depths.clone())
    )?;

    let mut v5_data = Vec::new();
    cell_data.serialize(&mut v5_data)?;

    let mut refs = vec!();
    for i in 0..4 {
        let r = crate_and_save_cell(&boc_db, &[1, i, 3, 0x80])?;
        v5_data.extend_from_slice(r.as_slice());
        refs.push(r);
    }

    let tree_bits_count = 32847_u64;
    v5_data.extend_from_slice(tree_bits_count.to_le_bytes().as_ref()); // tree bits/cells count
    let tree_cell_count = 83294_u64;
    v5_data.extend_from_slice(tree_cell_count.to_le_bytes().as_ref());

    let v6_data = StorageCell::migrate_to_v6(&v5_data)?;

    println!("v6 len: {}, v5 len {}", v6_data.len(), v5_data.len());

    let cell = StorageCell::deserialize(&boc_db, &hashes[0], &v6_data, false)?;

    assert!(cell.cell_type() == CellType::Ordinary);
    assert!(cell.data() == data);
    assert!(cell.level() == 0);
    assert!(cell.level_mask().mask() == 0);
    assert!(cell.references_count() == 4);
    for i in 0..4 {
        assert!(cell.reference_repr_hash(i)? == refs[i]);
    }
    assert!(cell.store_hashes());
    assert!(cell.hash(MAX_LEVEL) == hashes[0]);
    assert!(cell.depth(MAX_LEVEL) == depths[0]);
    assert!(cell.tree_bits_count() == tree_bits_count);
    assert!(cell.tree_cell_count() == tree_cell_count);

    let d1 = StorageCell::serialize(&cell)?;
    assert!(cell == StorageCell::deserialize(&boc_db, &hashes[0], &d1, false)?);

    Ok(())
}

#[tokio::test]
async fn test_storage_cell_migration_to_v6_6() -> Result<()> {

    let boc_db = init_boc_db("test_storage_cell_migration_to_v6_6").await?;

    let data = [1, 2, 3, 0x84];
    let hashes = [UInt256::rand(), UInt256::default(), UInt256::default(), UInt256::default()];
    let depths = [0xf121, 0, 0, 0];

    let cell_data = CellData::with_params(
        CellType::Ordinary,
        &[1, 2, 3, 0x84], // data
        0, // level mask
        4, // refs count
        true, // store hashes
        Some(hashes.clone()),
        Some(depths.clone())
    )?;

    let mut v5_data = Vec::new();
    cell_data.serialize(&mut v5_data)?;

    let mut refs = vec!();
    for i in 0..4 {
        let r = crate_and_save_cell(&boc_db, &[1, i, 3, 0x80])?;
        v5_data.extend_from_slice(r.as_slice());
        refs.push(r);
    }

    let tree_bits_count = 32847_u64;
    v5_data.extend_from_slice(tree_bits_count.to_le_bytes().as_ref()); // tree bits/cells count
    let tree_cell_count = 83294_u64;
    v5_data.extend_from_slice(tree_cell_count.to_le_bytes().as_ref());

    let v6_data = StorageCell::migrate_to_v6(&v5_data)?;

    println!("v6 len: {}, v5 len {}", v6_data.len(), v5_data.len());

    let cell = StorageCell::deserialize(&boc_db, &hashes[0], &v6_data, false)?;

    assert!(cell.cell_type() == CellType::Ordinary);
    assert!(cell.data() == data);
    assert!(cell.level() == 0);
    assert!(cell.level_mask().mask() == 0);
    assert!(cell.references_count() == 4);
    for i in 0..4 {
        assert!(cell.reference_repr_hash(i)? == refs[i]);
    }
    assert!(cell.store_hashes());
    assert!(cell.hash(MAX_LEVEL) == hashes[0]);
    assert!(cell.depth(MAX_LEVEL) == depths[0]);
    assert!(cell.tree_bits_count() == tree_bits_count);
    assert!(cell.tree_cell_count() == tree_cell_count);

    let d1 = StorageCell::serialize(&cell)?;
    assert!(cell == StorageCell::deserialize(&boc_db, &hashes[0], &d1, false)?);

    Ok(())
}

#[tokio::test]
async fn test_big_storage_cell() -> Result<()> {

    fn test(len: usize, boc_db: &Arc<DynamicBocDb>) -> Result<()> {
        let mut data = vec![0; len];
        rand::Rng::try_fill(&mut rand::thread_rng(), &mut data[..])?;
        let cell = create_big_cell(&data)?;
        let sc = StorageCell::with_cell(cell.cell_impl().deref(), boc_db, false, false)?;
        let d = StorageCell::serialize(&sc)?;
        let sc2 = StorageCell::deserialize(boc_db, &cell.repr_hash(), &d, false)?;
        assert!(sc == sc2);
        Ok(())
    }

    let boc_db = init_boc_db("test_big_storage_cell").await?;

    test(0, &boc_db)?;
    test(100, &boc_db)?;
    test(1024, &boc_db)?;
    test(1024 * 1024, &boc_db)?;
    test(1024 * 1024 * 2, &boc_db)?;
    test(0xffffff, &boc_db)?;

    Ok(())
}
