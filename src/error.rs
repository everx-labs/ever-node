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

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("Invalid argument: {0}")]
    InvalidArg(String),
    #[error("Invalid data: {0}")]
    InvalidData(String),
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
    #[error("{0}")]
    ValidatorReject(String),
    #[error("{0}")]
    ValidatorSoftReject(String),
    #[cfg(feature = "external_db")]
    #[error("{0}")]
    #[allow(dead_code)]
    Other(String),
}
