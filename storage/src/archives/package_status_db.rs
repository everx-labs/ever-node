use crate::{
    db_impl_base, archives::package_status_key::PackageStatusKey, 
    db::traits::KvcTransactional, traits::Serializable
};
use std::borrow::Borrow;
use ton_types::Result;

db_impl_base!(PackageStatusDb, KvcTransactional, PackageStatusKey);

impl PackageStatusDb {
    pub fn try_get_value<T: Serializable>(&self, key: &PackageStatusKey) -> Result<Option<T>> {
        Ok(if let Some(db_slice) = self.try_get(key)? {
            Some(T::from_slice(db_slice.as_ref())?)
        } else {
            None
        })
    }

    pub fn get_value<T: Serializable>(&self, key: &PackageStatusKey) -> Result<T> {
        T::from_slice(self.get(key)?.as_ref())
    }

    pub fn put_value<T: Serializable>(&self, key: &PackageStatusKey, value: impl Borrow<T>) -> Result<()> {
        self.put(key, value.borrow().to_vec()?.as_slice())
    }
}
