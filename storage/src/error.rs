/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific EVERX DEV software governing permissions and
* limitations under the License.
*/

use ever_block::BlockIdExt;

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum StorageError {
    /// Key not found
    #[error("Key not found: {0}({1})")]
    KeyNotFound(&'static str, String),

    /// Reading out of buffer range
    #[error("Reading out of buffer range")]
    OutOfRange,

    #[error("Attempt to load state {0} which is already allowed to GC")]
    StateIsAllowedToGc(BlockIdExt),
}
