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

use std::sync::atomic::{AtomicU32, AtomicBool, Ordering};
use ton_types::{Result, fail};

pub struct MpmcChannel<T> {
    queue: lockfree::queue::Queue<T>,
    sync: lockfree::queue::Queue<tokio::sync::oneshot::Sender<()>>,
    stop: AtomicBool,
    in_channel: AtomicU32,
}

impl<T> Default for MpmcChannel<T> {
    fn default() -> Self {
        MpmcChannel::new()
    }
}

impl<T> MpmcChannel<T> {
    pub fn new() -> Self {
        Self {
            queue: lockfree::queue::Queue::new(),
            sync: lockfree::queue::Queue::new(),
            stop: AtomicBool::new(false),
            in_channel: AtomicU32::new(0),
        }
    }

    pub fn send(&self, item: T) -> Result<()> {
        self.in_channel.fetch_add(1, Ordering::Relaxed);
        self.queue.push(item);
        if let Some(sender) = self.sync.pop() {
            if let Err(_) = sender.send(()) {
                fail!("sender.send returns error");
            }
        }
        Ok(())
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        while let Some(sender) = self.sync.pop() {
            sender.send(()).ok();
        }
    }

    #[cfg(feature = "telemetry")]
    pub fn len(&self) -> u32 {
        self.in_channel.load(Ordering::Relaxed)
    }

    pub async fn receive(&self) -> Result<T> {
        loop {
            if self.stop.load(Ordering::Relaxed) {
                fail!("Stopped");
            }

            if let Some(item) = self.queue.pop() {
                self.in_channel.fetch_sub(1, Ordering::Relaxed);
                return Ok(item)
            }

            let (sender, receiver) = tokio::sync::oneshot::channel();
            self.sync.push(sender);
            let _ = receiver.await;
        }
    }
}