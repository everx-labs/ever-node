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

pub mod validate_query;
mod fabric;
mod log_parser;
pub mod accept_block;
mod validator_group;
pub mod validator_utils;
pub mod validator_manager;
pub mod validator_session_listener;
pub mod candidate_db;
pub mod collator;
pub mod out_msg_queue;
mod mutex_wrapper;
#[cfg(feature = "telemetry")]
pub mod telemetry;
#[cfg(feature = "slashing")]
mod slashing;

use std::sync::Arc;
use ton_types::{Result, UInt256, error};
use ton_block::{
    BlkMasterInfo, BlockIdExt, ConfigParams, CurrencyCollection,
    ExtBlkRef, McStateExtra, Libraries,
};
use crate::shard_state::ShardStateStuff;

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
    pub is_fake: bool,
}

impl CollatorSettings {
    #[allow(dead_code)]
    pub fn fake() -> Self {
        Self {
            is_fake: true,
            ..Self::default()
        }
    }
}

pub struct McData {
    mc_state_extra: McStateExtra,
    prev_key_block_seqno: u32,
    prev_key_block: Option<BlockIdExt>,
    state: Arc<ShardStateStuff>

    // TODO put here what you need from masterchain state and block and init in `unpack_last_mc_state`
}

impl McData {
    pub fn new(mc_state: Arc<ShardStateStuff>) -> Result<Self> {

        let mc_state_extra = mc_state.state().read_custom()?
            .ok_or_else(|| error!("Can't read custom field from mc state"))?;

        // prev key block
        let (prev_key_block_seqno, prev_key_block) = if mc_state_extra.after_key_block {
            (mc_state.block_id().seq_no(), Some(mc_state.block_id().clone()))
        } else if let Some(block_ref) = mc_state_extra.last_key_block.clone() {
            (block_ref.seq_no, Some(block_ref.master_block_id().1))
        } else {
            (0, None)
        };
        Ok(Self{
            mc_state_extra,
            prev_key_block,
            prev_key_block_seqno,
            state: mc_state
        })
    }

    pub fn config(&self) -> &ConfigParams { self.mc_state_extra.config() }
    pub fn mc_state_extra(&self) -> &McStateExtra { &self.mc_state_extra }
    pub fn prev_key_block_seqno(&self) -> u32 { self.prev_key_block_seqno }
    pub fn prev_key_block(&self) -> Option<&BlockIdExt> { self.prev_key_block.as_ref() }
    pub fn state(&self) -> &Arc<ShardStateStuff> { &self.state }
    pub fn vert_seq_no(&self) -> u32 { self.state().state().vert_seq_no() }
    pub fn get_lt_align(&self) -> u64 { 1000000 }
    pub fn global_balance(&self) -> &CurrencyCollection { &self.mc_state_extra.global_balance }
    pub fn libraries(&self) -> &Libraries { self.state.state().libraries() }
    pub fn master_ref(&self) -> BlkMasterInfo {
        let end_lt = self.state.state().gen_lt();
        let master = ExtBlkRef {
            end_lt,
            seq_no: self.state.state().seq_no(),
            root_hash: self.state.block_id().root_hash().clone(),
            file_hash: self.state.block_id().file_hash().clone(),
        };
        BlkMasterInfo { master }
    }
}

/* UNUSED
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
*/
