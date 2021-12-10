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

#[derive(Debug, PartialEq, failure::Fail)]
pub enum StorageError {
    /// Key not found  
    #[fail(display = "Key not found: {}({})", 0, 1)]
    KeyNotFound(&'static str, String),

/*
    /// Reference not loaded
    #[fail(display = "Reference not loaded. Need to load reference.")]
    ReferenceNotLoaded,
*/

    /// Database is dropped
    #[fail(display = "Database is dropped")]
    DbIsDropped,

    /// One or more active transactions exist
    #[fail(display = "Operation is not permitted while one or more active transactions exist")]
    HasActiveTransactions,

    /// Reading out of buffer range
    #[fail(display = "Reading out of buffer range")]
    OutOfRange,
}
