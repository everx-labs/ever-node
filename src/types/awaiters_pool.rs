use std::{
    sync::{Arc, atomic::AtomicBool, atomic::Ordering}, fmt::Display, hash::Hash, cmp::Ord
};
use ton_types::{Result, fail};


struct OperationAwaiters<R> {
    pub is_started: AtomicBool,
    pub is_finished: AtomicBool,
    pub awaiters: lockfree::queue::Queue<Arc<tokio::sync::Barrier>>,
    pub results: lockfree::queue::Queue<Result<R>>,
}

impl<R: Clone> OperationAwaiters<R> {
    fn new(is_started: bool) -> Arc<Self> {
        Arc::new(Self {
            is_started: AtomicBool::new(is_started),
            is_finished: AtomicBool::new(false),
            awaiters: lockfree::queue::Queue::new(),
            results: lockfree::queue::Queue::new(),
        })
    }
}

pub struct AwaitersPool<I, R> {
    ops_awaiters: lockfree::map::Map<I, Arc<OperationAwaiters<R>>>,
}

impl<I, R> AwaitersPool<I, R> where
    I: Ord + Hash + Clone + Display,
    R: Clone,
{
    pub fn new() -> Self {
        Self {
            ops_awaiters: lockfree::map::Map::new(),
        }
    }

    pub async fn do_or_wait(&self, id: &I, operation: impl futures::Future<Output = Result<R>>) -> Result<Option<R>> {
        loop {
            if let Some(op_awaiters) = self.ops_awaiters.get(id) {
                if !op_awaiters.1.is_started.swap(true, Ordering::SeqCst) {
                    return Ok(Some(self.do_operation(id, operation, &op_awaiters.1).await?))
                } else {
                    return self.wait_operation(id, &op_awaiters.1).await
                }
            } else {
                let new_awaiters = OperationAwaiters::new(true);
                if self.try_insert_awaiters(id.clone(), new_awaiters.clone()) {
                    return Ok(Some(self.do_operation(id, operation, &new_awaiters).await?))
                }
            }
        }
    }

    pub async fn wait(&self, id: &I) -> Result<Option<R>> {
        loop {
            if let Some(op_awaiters) = self.ops_awaiters.get(id) {
                return self.wait_operation(id, &op_awaiters.1).await
            } else {
                let new_awaiters = OperationAwaiters::new(false);
                if self.try_insert_awaiters(id.clone(), new_awaiters.clone()) {
                    return self.wait_operation(id, &new_awaiters).await
                }
            }
        }
    }

    fn try_insert_awaiters(&self, id: I, new_awaiters: Arc<OperationAwaiters<R>>) -> bool {
        // This is so-called "interactive insertion"
        let result = self.ops_awaiters.insert_with(id, |_key, prev_gen_val, updated_pair | {
            if updated_pair.is_some() {
                // someone already added the value into map
                // so discard this insertion attempt
                lockfree::map::Preview::Discard
            } else if prev_gen_val.is_some() {
                // it is value we inserted just now
                lockfree::map::Preview::Keep
            } else {
                // there is not the value in the map - try to add.
                // If other thread adding value the same time - the closure will be recalled
                lockfree::map::Preview::New(new_awaiters.clone())
            }
        });
        match result {
            lockfree::map::Insertion::Created => {
                // block info we loaded now was added - use it
                return true
            },
            lockfree::map::Insertion::Updated(_) => {
                // unreachable situation - all updates must be discarded
                unreachable!("AwaitersPool: unreachable Insertion::Updated")
            },
            lockfree::map::Insertion::Failed(_) => {
                // other thread's awaiters was added - retry
                return false
            }
        }
    }

    async fn wait_operation(&self, id: &I, op_awaiters: &OperationAwaiters<R>) -> Result<Option<R>> {
        if op_awaiters.is_finished.load(Ordering::SeqCst) {
            log::trace!("awaiters pool: wait_operation: is_finished {}", id);
            return Ok(None)
        }
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        op_awaiters.awaiters.push(barrier.clone());
        log::trace!("awaiters pool: wait_operation: waiting... {}", id);
        barrier.wait().await;
        op_awaiters.results.pop().transpose()
    }

    async fn do_operation(&self, id: &I, operation: impl futures::Future<Output = Result<R>>, op_awaiters: &OperationAwaiters<R>) -> Result<R> {
        log::trace!("awaiters pool: do_operation: doing... {}", id);
        let result = operation.await;
        op_awaiters.is_finished.store(true, Ordering::SeqCst);
        while let Some(barrier) = op_awaiters.awaiters.pop() {
            op_awaiters.results.push(match result {
                Ok(ref r) => Ok(r.clone()),
                Err(e) => fail!("{}", e), // failure::Error doesn't impl Clone, so it is impossible to clone full result
            });
            barrier.wait().await;
        }
        self.ops_awaiters.remove(id).expect("AwaitersPool: ops_awaiters.remove returns None");
        log::trace!("awaiters pool: do_operation: done {}", id);
        result
    }
}

