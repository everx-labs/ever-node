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
    tests::utils::{FILE_HASH, get_test_block_id, get_test_shard_ident, ROOT_HASH},
    traits::Serializable, types::BlockMeta
};
use std::{cell::RefCell, sync::atomic::Ordering};
use ton_block::{BlockIdExt, ShardIdent};
use ton_types::Result;

static SHARD_IDENT_SERIALIZED: [u8; 12] = [
    // workchain_id = -1 (4 bytes)
    0xFF, 0xFF, 0xFF, 0xFF,
    // prefix = 0x8000_0000_0000_0000 (8 bytes)
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,
];

static SEQ_NO_SERIALIZED: [u8; 4] = [
    // seq_no = 1830539 (4 bytes)
    0x8B, 0xEE, 0x1B, 0x00,
];

#[test]
fn test_shard_ident_serialization() -> Result<()> {
    let shard_ident = get_test_shard_ident();
    let data = shard_ident.to_vec()?;
    assert_eq!(data, SHARD_IDENT_SERIALIZED);

    let new_shard_ident = ShardIdent::from_slice(data.as_slice())?;
    assert_eq!(new_shard_ident, shard_ident);

    Ok(())
}

#[test]
fn test_block_id_ext_serialization() -> Result<()> {
    let block_id_ext = get_test_block_id();
    let data = block_id_ext.to_vec()?;
    assert_eq!(data, [
        &SHARD_IDENT_SERIALIZED[..],
        &SEQ_NO_SERIALIZED[..],
        &ROOT_HASH[..],
        &FILE_HASH[..]
    ].concat());

    let new_block_id_ext = BlockIdExt::from_slice(data.as_slice())?;
    assert_eq!(new_block_id_ext, block_id_ext);

    Ok(())
}

#[test]
fn test_block_meta_serialization() -> Result<()> {

    let meta = BlockMeta::with_data(0x00_00_56_78, 0x90_AB_CD_EF, 0x11_22_33_44_55_66_77_88, 0x01_02_03_04, 0x01_01_00_00);
    let expected = RefCell::new(
        vec![
            0x04, 0x03, 0x02, 0x01,
            0x78, 0x56, 0x00, 0x00,
            0xEF, 0xCD, 0xAB, 0x90,
            0x88, 0x77, 0x66, 0x55,
            0x44, 0x33, 0x22, 0x11,
            0x00, 0x00, 0x01, 0x01,
        ]
    );

    meta.test_counter.store(0x10203040, Ordering::Relaxed);
    let mut counter_stored = vec![0x40, 0x30, 0x20, 0x10];
    expected.borrow_mut().append(&mut counter_stored);

    let data = meta.to_vec()?;
    assert_eq!(data, *expected.borrow());

    let new_meta = BlockMeta::from_slice(data.as_slice())?;
    assert_eq!(new_meta.flags(), meta.flags());
    assert_eq!(new_meta.gen_utime, meta.gen_utime);
    assert_eq!(new_meta.gen_lt, meta.gen_lt);
    assert_eq!(new_meta.masterchain_ref_seq_no(), meta.masterchain_ref_seq_no());

    #[cfg(test)]
    assert_eq!(new_meta.test_counter.load(Ordering::SeqCst), meta.test_counter.load(Ordering::SeqCst));
    Ok(())

}