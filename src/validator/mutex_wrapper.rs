use std::sync::Arc;
use tokio::sync::OwnedMutexGuard;

pub struct MutexWrapper<T: Sized> {
    mutex: Arc<tokio::sync::Mutex<T>>,
    id: String
}

impl <T: Sized> MutexWrapper<T> {
    pub fn new (t: T, id: String) -> Self {
        MutexWrapper{ mutex: Arc::new(tokio::sync::Mutex::new(t)), id }
    }

    pub async fn execute_sync <Res,F>(&self, f: F) -> Res
        where
            F: FnOnce(&mut T) -> Res,
    {
        log::trace!(target: "validator", "Lock {} started acquire", self.id);
        let mut guard: OwnedMutexGuard<T> = self.mutex.clone().lock_owned().await;
        let guard_ref: &mut T = &mut guard;
        log::trace!(target: "validator", "Lock {} acquired", self.id);
        let res = f (guard_ref);
        log::trace!(target: "validator", "Lock {} released", self.id);
        res
    }
}
