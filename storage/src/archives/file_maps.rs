use crate::archives::{
    archive_slice::ArchiveSlice, package_id::{PackageId, PackageType},
    package_index_db::{PackageIndexDb, PackageIndexEntry}
};
use std::{path::{Path, PathBuf}, sync::Arc};
use ton_types::Result;

#[derive(Debug)]
pub struct FileDescription {
    id: PackageId,
    deleted: bool,
    archive_slice: Arc<ArchiveSlice>,
}

impl FileDescription {
    pub fn with_data(id: PackageId, archive_slice: Arc<ArchiveSlice>, deleted: bool) -> Self {
        Self { id, deleted, archive_slice }
    }

    pub const fn id(&self) -> &PackageId {
        &self.id
    }

    pub const fn deleted(&self) -> bool {
        self.deleted
    }

    pub const fn archive_slice(&self) -> &Arc<ArchiveSlice> {
        &self.archive_slice
    }
}

#[derive(Debug)]
pub struct FileMapEntry {
    key: u32,
    value: Arc<FileDescription>,
}

#[derive(Debug)]
pub struct FileMap {
    storage: PackageIndexDb,
    elements: tokio::sync::RwLock<Vec<FileMapEntry>>,
}

impl FileMap {
    pub async fn new(db_root_path: &Arc<PathBuf>, path: impl AsRef<Path>, package_type: PackageType) -> Result<Self> {
        let storage = PackageIndexDb::with_path(path);
        let mut index_pairs = Vec::new();

        storage.for_each_deserialized(|key, value| {
            index_pairs.push((key, value));

            Ok(true)
        })?;

        index_pairs.sort_by_key(|pair| pair.0);

        let mut elements = Vec::new();
        for (key, value) in index_pairs {
            let archive_slice = Arc::new(ArchiveSlice::with_data(
                Arc::clone(db_root_path),
                key,
                package_type,
                value.finalized()
            ).await?);
            let value = Arc::new(FileDescription::with_data(
                PackageId::with_values(key, package_type),
                archive_slice,
                value.deleted()
            ));
            elements.push(FileMapEntry { key, value });
        }

        Ok(Self {
            storage,
            elements: tokio::sync::RwLock::new(elements),
        })
    }

    pub async fn put(&self, package_id: u32, file_description: Arc<FileDescription>) -> Result<()> {
        let entry = FileMapEntry { key: package_id, value: file_description };
        let mut guard = self.elements.write().await;
        match guard.binary_search_by(|entry| entry.key.cmp(&package_id)) {
            Ok(index) => guard[index] = entry,
            Err(index) => guard.insert(index, entry),
        }
        self.storage.put_value(&package_id.into(), PackageIndexEntry::new())?;

        Ok(())
    }

    pub async fn get(&self, package_id: u32) -> Option<Arc<FileDescription>> {
        let guard = self.elements.read().await;
        guard.binary_search_by(|entry| entry.key.cmp(&package_id))
            .map(|index| Arc::clone(&guard[index].value))
            .ok()
    }

    pub async fn get_closest(&self, mc_seq_no: u32) -> Option<Arc<FileDescription>> {
        let guard = self.elements.read().await;
        log::debug!(target: "storage", "Searching for file description (elements count = {})", guard.len());
        match guard.binary_search_by(|entry| entry.key.cmp(&mc_seq_no)) {
            Ok(index) => Some(Arc::clone(&guard[index].value)),
            Err(index) => {
                if index == 0 {
                    return None;
                }
                Some(Arc::clone(&guard[index - 1].value))
            },
        }
    }
}

pub struct FileMaps {
    files: FileMap,
    // key_files: FileMap,
    // temp_files: FileMap,
}

impl FileMaps {
    pub async fn new(db_root_path: &Arc<PathBuf>) -> Result<Self> {
        let path = db_root_path.join("file_maps");
        Ok(Self {
            files: FileMap::new(db_root_path, path.join("files"), PackageType::Blocks).await?,
            // key_files: FileMap::new(db_root_path, path.join("key_files"), PackageType::KeyBlocks).await?,
            // temp_files: FileMap::new(db_root_path, path.join("temp_files"), PackageType::Temp).await?,
        })
    }

    pub fn files(&self) -> &FileMap {
        &self.files
    }

    pub fn get(&self, package_type: PackageType) -> &FileMap {
        match package_type {
            // PackageType::KeyBlocks => &self.key_files,
            // PackageType::Temp => &self.temp_files,
            PackageType::Blocks => &self.files,
            _ => unimplemented!("{:?}", package_type)
        }
    }
}
