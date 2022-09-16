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

pub use super::*;

use tokio::time::sleep;
use std::time::Duration;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use log::warn;

/*
    Constants
*/

const HANG_CHECKER_WARN_DUMP_PERIOD: Duration = Duration::from_millis(2000); //latency warning dump period

/*
===================================================================================================
    HangChecker
===================================================================================================
*/

pub struct HangCheck {
    is_running: Arc<AtomicBool>, //is code still running
}

impl HangCheck {
    pub fn new(runtime: tokio::runtime::Handle, name: String, warn_delay: std::time::Duration) -> Self {
        let is_running = Arc::new(AtomicBool::new(true));
        let is_running_clone = is_running.clone();
        let start_time = std::time::SystemTime::now();
        let warn_time = start_time + warn_delay;

        runtime.spawn(async move {
            if let Ok(delay) = warn_time.duration_since(std::time::SystemTime::now()) {
                sleep(delay).await;
            }

            loop {
                if !is_running.load(Ordering::Relaxed) {
                    break;
                }

                let processing_delay = match start_time.elapsed() {
                    Ok(elapsed) => elapsed,
                    Err(_err) => std::time::Duration::default(),
                };

                warn!(target: "verificator", "{} is hanging for {:.3}s", name, processing_delay.as_secs_f64());

                sleep(HANG_CHECKER_WARN_DUMP_PERIOD).await;
            }
        });

        Self {
            is_running: is_running_clone,
        }
    }
}

impl Drop for HangCheck {
    fn drop(&mut self) {
        self.is_running.store(false, Ordering::Release);
    }
}
