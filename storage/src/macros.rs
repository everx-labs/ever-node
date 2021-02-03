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
            pub fn with_path<P: AsRef<std::path::Path>>(path: P) -> Self {
                Self {
                    db: Box::new($crate::db::rocksdb::RocksDb::with_path(path))
                }
            }
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
macro_rules! db_impl_cbor {
    ($type: ident, $trait: ident, $key_type: ty, $value_type: ty) => {
        $crate::db_impl_base!($type, $trait, $key_type);

        impl $type {
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
                Ok(<$value_type>::from_slice(self.get(key)?.as_ref())?)
            }

            #[allow(dead_code)]
            pub fn put_value(&self, key: &$key_type, value: impl std::borrow::Borrow<$value_type>) -> ton_types::Result<()> {
                self.put(key, &value.borrow().to_vec()?)
            }
        }
    }
}
