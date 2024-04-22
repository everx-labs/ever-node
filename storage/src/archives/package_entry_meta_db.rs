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

use crate::{
    db_impl_cbor, archives::package_entry_meta::PackageEntryMeta, 
    db::traits::{KvcWriteable, U32Key}
};

db_impl_cbor!(PackageEntryMetaDb, KvcWriteable, U32Key, PackageEntryMeta);
