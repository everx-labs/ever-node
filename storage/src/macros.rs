/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific EVERX DEV software governing permissions and
* limitations under the License.
*/

#[macro_export]
macro_rules! db_impl_base {
    ($type: ident, $key_type: ty) => {
        #[derive(Debug)]
        pub struct $type {
            db: $crate::db::rocksdb::RocksDbTable<$key_type>,
        }

        impl $type {
            /// Constructs new instance using RocksDB with given path
            pub fn with_db(
                db: std::sync::Arc<$crate::db::rocksdb::RocksDb>,
                family: impl ToString,
                create_if_not_exist: bool,
            ) -> ever_block::Result<Self> {
                Ok(Self {
                    db: db.table(family, create_if_not_exist)?
                })
            }
        }

        impl std::ops::Deref for $type {
            type Target = $crate::db::rocksdb::RocksDbTable<$key_type>;

            fn deref(&self) -> &Self::Target {
                &self.db
            }
        }

        impl std::ops::DerefMut for $type {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.db
            }
        }
    }
}

#[macro_export]
macro_rules! db_impl_single {
    ($type: ident, $key_type: ty) => {
        #[derive(Debug)]
        pub struct $type {
            db: std::sync::Arc<$crate::db::rocksdb::RocksDb>,
        }

        impl $type{
            /// Constructs new instance using RocksDB with given path
            pub fn with_path(
                path: impl AsRef<std::path::Path>,
                name: &str
            ) -> ever_block::Result<Self> {
                Ok(Self {
                    db: $crate::db::rocksdb::RocksDb::with_path(path, name)?
                })
            }

            /// Destroys instance of RocksDB with given path
            pub fn destroy(&mut self) -> ever_block::Result<bool> {
                let path = self.db.path().to_path_buf();
                if let Some(db) = std::sync::Arc::get_mut(&mut self.db) {
                    if let Err(err) = db.destroy() {
                        ever_block::fail!(
                            "Database {:?} destroying error: {}",
                            path,
                            err
                        )
                    } else {
                        Ok(true)
                    }
                } else {
                    ever_block::fail!("operation pending in db {:?}", path)
                }
            }

            pub fn path(&self) -> &std::path::Path {
                self.db.path()
            }

            pub fn db(&self) -> &std::sync::Arc<$crate::db::rocksdb::RocksDb> {
                &self.db
            }
        }

        impl std::ops::Deref for $type {
            type Target = RocksDb;

            fn deref(&self) -> &Self::Target {
                self.db.deref()
            }
        }
    }
}

#[macro_export]
macro_rules! db_impl_cbor {
    ($type: ident, $key_type: ty, $value_type: ty) => {
        $crate::db_impl_base!($type, $key_type);

        impl $type {
            #[allow(dead_code)]
            pub fn value(&self, db_slice: impl AsRef<[u8]>) -> std::result::Result<$value_type, serde_cbor::Error> {
                serde_cbor::from_slice(db_slice.as_ref())
            }

            #[allow(dead_code)]
            pub fn try_get_value(&self, key: &$key_type) -> ever_block::Result<Option<$value_type>> {
                if let Some(db_slice) = self.try_get(key)? {
                    return Ok(Some(serde_cbor::from_slice(db_slice.as_ref())?));
                }

                Ok(None)
            }

            #[allow(dead_code)]
            pub fn get_value(&self, key: &$key_type) -> ever_block::Result<$value_type> {
                Ok(serde_cbor::from_slice(self.get(key)?.as_ref())?)
            }

            pub fn put_value(&self, key: &$key_type, value: impl std::borrow::Borrow<$value_type>) -> ever_block::Result<()> {
                self.put(key, &serde_cbor::to_vec(value.borrow())?)
            }
        }
    }
}

#[macro_export]
macro_rules! db_impl_serializable {
    ($type: ident, $key_type: ty, $value_type: ty) => {
        $crate::db_impl_base!($type, $key_type);

        impl $type {
            pub fn try_get_value(&self, key: &$key_type) -> ever_block::Result<Option<$value_type>> {
                if let Some(db_slice) = self.db.try_get(key)? {
                    return Ok(Some(<$value_type>::from_slice(db_slice.as_ref())?));
                }

                Ok(None)
            }

            pub fn get_value(&self, key: &$key_type) -> ever_block::Result<$value_type> {
                <$value_type>::from_slice(self.db.get(key)?.as_ref())
            }

            pub fn put_value(&self, key: &$key_type, value: impl std::borrow::Borrow<$value_type>) -> ever_block::Result<()> {
                self.db.put(key, &value.borrow().to_vec()?)
            }
        }
    }
}
