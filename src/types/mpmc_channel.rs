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
