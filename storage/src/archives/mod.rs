use crate::types::BlockHandle;

mod package_index_db;

pub mod archive_manager;
pub mod package;
pub mod package_entry_id;
pub mod package_entry;

mod package_status_db;
mod package_status_key;
mod file_maps;
mod package_offsets_db;
mod package_info;
mod archive_slice;
mod package_entry_meta_db;
mod package_entry_meta;
mod package_id;

pub const ARCHIVE_SIZE: u32 = 100_000;
pub const ARCHIVE_SLICE_SIZE: u32 = 20_000;
pub const ARCHIVE_PACKAGE_SIZE: u32 = 100;

pub const KEY_ARCHIVE_SIZE: u32 = 10_000_000;
pub const KEY_ARCHIVE_SLICE_SIZE: u32 = 2_000_000;
pub const KEY_ARCHIVE_PACKAGE_SIZE: u32 = 200_000;

fn get_mc_seq_no_opt(block_handle: Option<&BlockHandle>) -> u32 {
    if let Some(handle) = block_handle {
        get_mc_seq_no(handle)
    } else {
        0
    }
}

fn get_mc_seq_no(handle: &BlockHandle) -> u32 {
    if handle.id().shard().is_masterchain() {
        handle.id().seq_no()
    } else {
        handle.masterchain_ref_seq_no()
    }
}

