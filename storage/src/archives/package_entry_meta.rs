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


#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct PackageEntryMeta {
    entry_size: u64,
    version: u32,
}

impl PackageEntryMeta {
    pub const fn with_data(entry_size: u64, version: u32) -> Self {
        Self { entry_size, version }
    }

    pub const fn entry_size(&self) -> u64 {
        self.entry_size
    }

    pub const fn version(&self) -> u32 {
        self.version
    }
}
