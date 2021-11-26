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
use ton_block::ShardIdent;
use ton_types::Result;

pub struct ShardIdentKey(Vec<u8>);

impl ShardIdentKey {
    pub fn new(shard_ident: &ShardIdent) -> Result<Self> {
        let mut key = Vec::with_capacity(std::mem::size_of::<ShardIdent>());
        shard_ident.serialize(&mut key)?;

        Ok(Self(key))
    }
}

impl DbKey for ShardIdentKey {
    fn key_name(&self) -> &'static str {
        "ShardIdentKey"
    }

    fn as_string(&self) -> String {
        ShardIdent::from_slice(self.key())
            .map(|shard_ident| format!("{}", shard_ident))
            .unwrap_or_else(|_err| hex::encode(self.key()))
    }

    fn key(&self) -> &[u8] {
        self.0.as_slice()
    }
}
