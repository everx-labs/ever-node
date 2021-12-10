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

use crate::{db::traits::DbKey, traits::Serializable};
use std::io::Write;
use ton_block::ShardIdent;
use ton_types::Result;

pub struct LtDbKey(Vec<u8>);

impl LtDbKey {
    pub fn with_values(shard_id: &ShardIdent, index: u32) -> Result<Self> {
        let mut key = shard_id.to_vec()?;
        key.write_all(&index.to_le_bytes())?;

        Ok(Self(key))
    }
}

impl DbKey for LtDbKey {
    fn key_name(&self) -> &'static str {
        "LtDbKey"
    }

    fn key(&self) -> &[u8] {
        self.0.as_slice()
    }
}
