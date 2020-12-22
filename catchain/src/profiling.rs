pub use super::*;

use metrics_runtime;

#[derive(Clone)]
pub struct InstanceCounter {
    create_counter: metrics_runtime::data::Counter,
    drop_counter: metrics_runtime::data::Counter,
    need_drop: bool,
}

impl InstanceCounter {
    pub fn new(receiver: &metrics_runtime::Receiver, key: &String) -> Self {
        let body = Self {
            create_counter: receiver.sink().counter(format!("{}.create", &key)),
            drop_counter: receiver.sink().counter(format!("{}.drop", &key)),
            need_drop: false,
        };

        body
    }

    pub fn clone(&self) -> Self {
        let mut body = self.clone_refs_only();

        body.need_drop = true;
        body.create_counter.increment();

        body
    }

    pub fn clone_refs_only(&self) -> Self {
        let body = Self {
            create_counter: self.create_counter.clone(),
            drop_counter: self.drop_counter.clone(),
            need_drop: false,
        };

        body
    }

    pub fn drop(&mut self) {
        if self.need_drop {
            self.drop_counter.increment();
            self.need_drop = false;
        }
    }
}
