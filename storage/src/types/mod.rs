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


mod block_id;
mod block_meta;
mod cell_id;
mod db_slice;
mod reference;
mod shard_ident_key;
mod status_key;
mod storage_cell;

pub use block_id::*;
pub use block_meta::*;
pub use cell_id::*;
pub use db_slice::*;
pub use reference::*;
pub use shard_ident_key::*;
pub use status_key::*;
pub use storage_cell::*;

/*
/// Usually >= 1; 0 used to indicate the initial state, i.e. "zerostate"
pub type BlockSeqNo = i32;
pub type BlockVertSeqNo = u32;
pub type WorkchainId = i32;
pub type ShardId = i64;
*/
