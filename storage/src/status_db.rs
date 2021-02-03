use std::borrow::Borrow;

use ton_types::Result;

use crate::db_impl_base;
use crate::db::traits::KvcWriteable;
use crate::traits::Serializable;
use crate::types::StatusKey;

db_impl_base!(StatusDb, KvcWriteable, StatusKey);

impl StatusDb {
    pub fn try_get_value<T: Serializable>(&self, key: &StatusKey) -> Result<Option<T>> {
        Ok(if let Some(db_slice) = self.try_get(key)? {
            Some(T::from_slice(db_slice.as_ref())?)
        } else {
            None
        })
    }

    pub fn get_value<T: Serializable>(&self, key: &StatusKey) -> Result<T> {
        T::from_slice(self.get(key)?.as_ref())
    }

    pub fn put_value<T: Serializable>(&self, key: &StatusKey, value: impl Borrow<T>) -> Result<()> {
        self.put(key, value.borrow().to_vec()?.as_slice())
    }
}
