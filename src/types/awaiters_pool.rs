use std::{
    sync::{Arc, atomic::AtomicBool, atomic::Ordering}, fmt::Display, hash::Hash, cmp::Ord
};
use ton_types::{Result, error};
use adnl::common::add_object_to_map;


struct OperationAwaiters<R> {
    pub is_started: AtomicBool,
    pub tx: tokio::sync::watch::Sender<Option<std::result::Result<R, String>>>,
    pub rx: tokio::sync::watch::Receiver<Option<std::result::Result<R, String>>>,
}

impl<R: Clone> OperationAwaiters<R> {
    fn new(is_started: bool) -> Arc<Self> {
        let (tx, rx) = tokio::sync::watch::channel(None);
        Arc::new(Self {
            is_started: AtomicBool::new(is_started),
            tx,
            rx
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

    pub async fn do_or_wait(
        &self,
        id: &I,
        operation: impl futures::Future<Output = Result<R>>
    ) -> Result<Option<R>> {
        loop {
            if let Some(op_awaiters) = self.ops_awaiters.get(id) {
                if !op_awaiters.1.is_started.swap(true, Ordering::SeqCst) {
                    return Some(self.do_operation(id, operation, &op_awaiters.1).await).transpose()
                } else {
                    return self.wait_operation(id, &op_awaiters.1).await
                }
            } else {
                let new_awaiters = OperationAwaiters::new(true);
                if add_object_to_map(&self.ops_awaiters, id.clone(), || Ok(new_awaiters.clone()))? {
                    return Some(self.do_operation(id, operation, &new_awaiters).await).transpose()
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
                if add_object_to_map(&self.ops_awaiters, id.clone(), || Ok(new_awaiters.clone()))? {
                    return self.wait_operation(id, &new_awaiters).await
                }
            }
        }
    }

    async fn wait_operation(&self, id: &I, op_awaiters: &OperationAwaiters<R>) -> Result<Option<R>> {
        let mut rx = op_awaiters.rx.clone();
        loop {
            log::trace!("awaiters pool: wait_operation: waiting... {}", id);
            let result = rx.recv().await;
            let r = match result {
                None => return Ok(None),
                Some(Some(Ok(r))) => Ok(Some(r)),
                Some(Some(Err(e))) => Err(error!("{}", e)),
                Some(None) => continue
            };
            log::trace!("awaiters pool: wait_operation: done {}", id);
            break r;
        }
    }

    async fn do_operation(
        &self,
        id: &I,
        operation: impl futures::Future<Output = Result<R>>,
        op_awaiters: &OperationAwaiters<R>
    ) -> Result<R> {
        log::trace!("awaiters pool: do_operation: doing... {}", id);
        let result = operation.await;
        log::trace!("awaiters pool: do_operation: done {}", id);

        self.ops_awaiters.remove(id);

        let r = match result {
            Ok(ref r) => Ok(r.clone()),
            Err(ref e) => Err(format!("{}", e)), // failure::Error doesn't impl Clone, 
                                                 // so it is impossible to clone full result
        };
        let _ = op_awaiters.tx.broadcast(Some(r));
        result
    }
}

