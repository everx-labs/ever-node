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

use ton_types::types::UInt256;

/// Trait for database key
pub trait DbKey {
    fn key_name(&self) -> &'static str;

    fn as_string(&self) -> String {
        hex::encode(self.key())
    }

    fn key(&self) -> &[u8];
}

impl DbKey for &[u8] {
    fn key_name(&self) -> &'static str {
        "&[u8]"
    }

    fn key(&self) -> &[u8] {
        self
    }
}

impl DbKey for &str {
    fn key_name(&self) -> &'static str {
        "&str"
    }

    fn as_string(&self) -> String {
        String::from_utf8_lossy(self.key()).to_string()
    }

    fn key(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl DbKey for UInt256 {
    fn key_name(&self) -> &'static str {
        "UInt256"
    }

    fn key(&self) -> &[u8] {
        self.as_slice()
    }
}

pub struct U32Key {
    key: [u8; 4],
}

impl U32Key {
    pub fn with_value(value: u32) -> Self {
        Self { key: value.to_le_bytes() }
    }
}

impl From<u32> for U32Key {
    fn from(value: u32) -> Self {
        Self::with_value(value)
    }
}

impl DbKey for U32Key {
    fn key_name(&self) -> &'static str {
        "U32Key"
    }

    fn as_string(&self) -> String {
        u32::from_le_bytes(self.key).to_string()
    }

    fn key(&self) -> &[u8] {
        &self.key
    }
}

impl DbKey for Vec<u8> {
    fn key_name(&self) -> &'static str {
        "&[u8]"
    }

    fn key(&self) -> &[u8] {
        self
    }
}
