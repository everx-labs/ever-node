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

use ton_api::ton::ton_node::blockidext::BlockIdExt;

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LtDbEntry {
    block_id_ext: BlockIdExt,
    lt: u64,
    unix_time: u32,
}

impl LtDbEntry {
    pub const fn with_values(block_id_ext: BlockIdExt, lt: u64, unix_time: u32) -> Self {
        Self { block_id_ext, lt, unix_time }
    }

    pub const fn block_id_ext(&self) -> &BlockIdExt {
        &self.block_id_ext
    }

    pub const fn lt(&self) -> u64 {
        self.lt
    }

    pub const fn unix_time(&self) -> u32 {
        self.unix_time
    }
}
