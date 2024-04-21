/*
* Copyright (C) 2019-2023 EverX. All Rights Reserved.
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

#[cfg(feature = "telemetry")]
use std::time::Instant;
use std::sync::Arc;
use tokio::sync::OwnedMutexGuard;
#[cfg(feature = "telemetry")]
use adnl::telemetry::Metric;

pub struct MutexWrapper<T: Sized> {
    mutex: Arc<tokio::sync::Mutex<T>>,
    id: String,
    #[cfg(feature = "telemetry")]
    mutex_awaiting_metric: Option<Arc<Metric>>
}

impl <T: Sized> MutexWrapper<T> {
    pub fn new (t: T, id: String) -> Self {
        MutexWrapper{ 
            mutex: Arc::new(tokio::sync::Mutex::new(t)),
            id,
            #[cfg(feature = "telemetry")]
            mutex_awaiting_metric: None,
        }
    }

    pub fn with_metric (
        t: T,
        id: String,
        #[cfg(feature = "telemetry")]
        mutex_awaiting_metric: Arc<Metric>
    ) -> Self {
        MutexWrapper{ 
            mutex: Arc::new(tokio::sync::Mutex::new(t)),
            id,
            #[cfg(feature = "telemetry")]
            mutex_awaiting_metric: Some(mutex_awaiting_metric),
        }
    }

    pub async fn execute_sync <Res,F>(&self, f: F) -> Res
        where
            F: FnOnce(&mut T) -> Res,
    {
        log::trace!(target: "validator", "Lock {} started acquire", self.id);

        let mut guard: OwnedMutexGuard<T>;
        #[cfg(feature = "telemetry")] {
            if let Some(metric) = &self.mutex_awaiting_metric {
                let started = Instant::now();
                guard = self.mutex.clone().lock_owned().await;
                metric.update(started.elapsed().as_micros() as u64);
            } else {
                guard = self.mutex.clone().lock_owned().await;
            }
        }
        #[cfg(not(feature = "telemetry"))] {
            guard = self.mutex.clone().lock_owned().await;
        }

        let guard_ref: &mut T = &mut guard;
        log::trace!(target: "validator", "Lock {} acquired", self.id);
        let res = f (guard_ref);
        log::trace!(target: "validator", "Lock {} released", self.id);
        res
    }
}
