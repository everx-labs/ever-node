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

use crate::db::traits::DbKey;
use strum_macros::AsRefStr;

#[derive(Debug, AsRefStr)]
pub enum PackageStatusKey {
    SlicedMode,
    SliceSize,
    NonSlicedSize,
    TotalSlices,
}

impl DbKey for PackageStatusKey {
    fn key_name(&self) -> &'static str {
        "PackageStatusKey"
    }

    fn as_string(&self) -> String {
        self.as_ref().to_string()
    }

    fn key(&self) -> &[u8] {
        self.as_ref().as_bytes()
    }
}
