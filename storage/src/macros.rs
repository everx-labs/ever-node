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

#[macro_export]
macro_rules! db_impl_base {
    ($type: ident, $trait: ident, $key_type: ty) => {
        #[derive(Debug)]
        pub struct $type {
            db: Box<dyn $crate::db::traits::$trait<$key_type> + Send + Sync>,
        }

        impl $type{
            /// Constructs new instance using in-memory key-value collection
            #[allow(dead_code)]
            pub fn in_memory() -> Self {
                Self {
                    db: Box::new($crate::db::memorydb::MemoryDb::new())
                }
            }

            /// Constructs new instance using RocksDB with given path
            #[allow(dead_code)]
            pub fn with_db(
                db: std::sync::Arc<$crate::db::rocksdb::RocksDb>, 
                family: impl ToString
            ) -> ton_types::Result<Self> {
                let ret = Self {
                    db: Box::new(db.table(family)?)
                };
                Ok(ret)
            }

            // /// Constructs new instance using RocksDB with given path
            // #[allow(dead_code)]
            // pub fn with_path(
            //     path: &str, 
            //     name: &str
            // ) -> ton_types::Result<Self> {
            //     let db = $crate::db::rocksdb::RocksDb::with_path(path, name);
            //     let ret = Self {
            //         db: Box::new(db)
            //     };
            //     Ok(ret)
            // }

            // /// Destroys instance of RocksDB with given path
            // #[allow(dead_code)]
            // pub fn destroy_db(path: impl AsRef<std::path::Path>) -> ton_types::Result<bool> {
            //     $crate::db::rocksdb::RocksDb::destroy_db(path)
            // }
        }

        impl std::ops::Deref for $type {
            type Target = dyn $trait<$key_type> + Send + Sync;

            fn deref(&self) -> &Self::Target {
                self.db.deref()
            }
        }

        impl std::ops::DerefMut for $type {
            fn deref_mut(&mut self) -> &mut Self::Target {
                self.db.deref_mut()
            }
        }
    }
}

#[macro_export]
macro_rules! db_impl_single {
    ($type: ident, $trait: ident, $key_type: ty) => {
        #[derive(Debug)]
        pub struct $type {
            db: std::sync::Arc<$crate::db::rocksdb::RocksDb>,
        }

        impl $type{
            /// Constructs new instance using RocksDB with given path
            #[allow(dead_code)]
            pub fn with_path(
                path: &str, 
                name: &str
            ) -> ton_types::Result<Self> {
                let db = $crate::db::rocksdb::RocksDb::with_path(path, name);
                let ret = Self {
                    db
                };
                Ok(ret)
            }

            /// Destroys instance of RocksDB with given path
            #[allow(dead_code)]
            pub fn destroy(&mut self) -> ton_types::Result<bool> {
                let path = self.db.path().to_path_buf();
                if let Some(db) = std::sync::Arc::get_mut(&mut self.db) {
                    if let Err(err) = $crate::db::traits::Kvc::destroy(db) {
                        ton_types::fail!(
                            "Database {:?} destroying error: {}",
                            path,
                            err
                        )
                    } else {
                        Ok(true)
                    }
                } else {
                    ton_types::fail!("operation pending in db {:?}", path)
                }
            }

            pub fn path(&self) -> &std::path::Path {
                self.db.path()
            }
        }

        impl std::ops::Deref for $type {
            type Target = dyn $trait<$key_type> + Send + Sync;

            fn deref(&self) -> &Self::Target {
                self.db.deref()
            }
        }
    }
}

#[macro_export]
macro_rules! db_impl_cbor {
    ($type: ident, $trait: ident, $key_type: ty, $value_type: ty) => {
        $crate::db_impl_base!($type, $trait, $key_type);

        impl $type {
            #[allow(dead_code)]
            pub fn value(&self, db_slice: impl AsRef<[u8]>) -> std::result::Result<$value_type, serde_cbor::Error> {
                serde_cbor::from_slice(db_slice.as_ref())
            }

            #[allow(dead_code)]
            pub fn try_get_value(&self, key: &$key_type) -> ton_types::Result<Option<$value_type>> {
                if let Some(db_slice) = self.try_get(key)? {
                    return Ok(Some(serde_cbor::from_slice(db_slice.as_ref())?));
                }

                Ok(None)
            }

            #[allow(dead_code)]
            pub fn get_value(&self, key: &$key_type) -> ton_types::Result<$value_type> {
                Ok(serde_cbor::from_slice(self.get(key)?.as_ref())?)
            }

            #[allow(dead_code)]
            pub fn put_value(&self, key: &$key_type, value: impl std::borrow::Borrow<$value_type>) -> ton_types::Result<()> {
                self.put(key, &serde_cbor::to_vec(value.borrow())?)
            }
        }
    }
}

#[macro_export]
macro_rules! db_impl_serializable {
    ($type: ident, $trait: ident, $key_type: ty, $value_type: ty) => {
        $crate::db_impl_base!($type, $trait, $key_type);

        impl $type {
            #[allow(dead_code)]
            pub fn try_get_value(&self, key: &$key_type) -> ton_types::Result<Option<$value_type>> {
                if let Some(db_slice) = self.try_get(key)? {
                    return Ok(Some(<$value_type>::from_slice(db_slice.as_ref())?));
                }

                Ok(None)
            }

            #[allow(dead_code)]
            pub fn get_value(&self, key: &$key_type) -> ton_types::Result<$value_type> {
                <$value_type>::from_slice(self.get(key)?.as_ref())
            }

            #[allow(dead_code)]
            pub fn put_value(&self, key: &$key_type, value: impl std::borrow::Borrow<$value_type>) -> ton_types::Result<()> {
                self.put(key, &value.borrow().to_vec()?)
            }
        }
    }
}
