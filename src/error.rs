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

#[derive(Debug, failure::Fail)]
pub enum NodeError {
    #[fail(display = "Invalid argument: {}", 0)]
    InvalidArg(String),
    #[fail(display = "Invalid data: {}", 0)]
    InvalidData(String),
    #[fail(display = "Invalid operation: {}", 0)]
    InvalidOperation(String),
    #[fail(display = "{}", 0)]
    ValidatorReject(String),
    #[fail(display = "{}", 0)]
    ValidatorSoftReject(String),
    #[cfg(feature = "external_db")]
    #[fail(display = "{}", 0)]
    #[allow(dead_code)]
    Other(String),
}
