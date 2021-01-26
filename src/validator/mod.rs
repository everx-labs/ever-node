pub mod validate_query;
mod fabric;
mod log_parser;
pub mod accept_block;
mod validator_group;
pub mod validator_utils;
pub mod validator_manager;
pub mod validator_session_listener;
mod candidate_db;
pub mod collator;
pub mod collator_sync;

use ton_types::UInt256;
use ton_block::BlockIdExt;

#[derive(Clone, Default, Debug)]
pub struct BlockCandidate {
    pub block_id: BlockIdExt,
    pub data: Vec<u8>,
    pub collated_data: Vec<u8>,
    pub collated_file_hash: UInt256,
    pub created_by: UInt256,
}

#[derive(Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct CollatorSettings {
    pub want_split: Option<bool>,
    pub want_merge: Option<bool>,
    pub max_collate_threads: Option<usize>,
}

impl CollatorSettings {
    pub fn want_merge() -> Self {
        let mut settings = Self::default();
        settings.want_merge = Some(true);
        settings
    }
    pub fn want_split() -> Self {
        let mut settings = Self::default();
        settings.want_split = Some(true);
        settings
    }
}

