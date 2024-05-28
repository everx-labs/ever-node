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

use futures::Future;

pub mod accounts;
pub mod awaiters_pool;
pub mod top_block_descr;
pub mod limits;
pub mod messages;
pub mod lockfree_cache;
pub mod shard_blocks_observer;
pub mod mpmc_channel;


pub fn spawn_cancelable<F>(
    cancellation_token: tokio_util::sync::CancellationToken, 
    task: F
) where
    F: Future<Output = ()> + Send + 'static
{
    tokio::spawn(
        async move {
            tokio::select! {
                _ = task => {},
                _ = cancellation_token.cancelled() => {}
            }
        }
    );
}
