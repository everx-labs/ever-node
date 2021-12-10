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
