use crate::archives::{
    archive_slice::ArchiveSlice, package_id::{PackageId, PackageType},
    package_index_db::{PackageIndexDb, PackageIndexEntry}
};
use std::{path::{Path, PathBuf}, sync::Arc};
use ton_types::{Result, error};
use ton_block::BlockIdExt;

use super::ARCHIVE_SLICE_SIZE;

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

    async fn destroy(&mut self) -> Result<()> {
        Arc::get_mut(&mut self.archive_slice)
            .ok_or_else(|| error!("Unable to get mutable reference to offsets_db"))?
            .destroy().await
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

    async fn get_marked_entries(&self, front_for_gc_master_block_id: &BlockIdExt) -> Vec<u32> {
        let elements = self.elements.read().await;
        let mut marked_packages = Vec::new();

        for i in 0..elements.len() {
            let next_id = if i == elements.len() - 1 {
                elements[i].value.archive_slice.archive_id() + ARCHIVE_SLICE_SIZE
            } else {
                elements[i + 1].value.archive_slice.archive_id()
            };

            if elements[i].value.archive_slice.package_type() == PackageType::Blocks &&
               next_id <= front_for_gc_master_block_id.seq_no() {
                marked_packages.push(elements[i].key.clone());
            }
        }
        marked_packages
    }

    pub async fn gc(&self, front_for_gc_master_block_id: &BlockIdExt) -> Result<()> {
        log::info!(target: "storage", "file_maps gc started.");
        let mut marked_packages = self.get_marked_entries(front_for_gc_master_block_id).await;

        while let Some(key) = marked_packages.pop() {
            let mut guard = self.elements.write().await;
            let position = guard.iter().position(|item| item.key == key)
                .ok_or_else(|| error!("slice not found!"))?;
            let mut removed_entry = guard.remove(position);
            match Arc::get_mut(&mut removed_entry.value) {
                Some(file_description) => {
                    if let Err(e) = file_description.destroy().await {
                        log::warn!(target: "storage", "destroy file_description is error: {:?}", e);
                    }
                },
                None => { 
                    log::warn!(target: "storage", "Unable to get mutable reference to file_description"); 
                }
            }
        }
        log::info!(target: "storage", "file_maps gc finished.");
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
    key_files: FileMap,
    // temp_files: FileMap,
}

impl FileMaps {
    pub async fn new(db_root_path: &Arc<PathBuf>) -> Result<Self> {
        let path = db_root_path.join("file_maps");
        Ok(Self {
            files: FileMap::new(db_root_path, path.join("files"), PackageType::Blocks).await?,
            key_files: FileMap::new(db_root_path, path.join("key_files"), PackageType::KeyBlocks).await?,
            // temp_files: FileMap::new(db_root_path, path.join("temp_files"), PackageType::Temp).await?,
        })
    }

    pub fn files(&self) -> &FileMap {
        &self.files
    }

    pub fn key_files(&self) -> &FileMap {
        &self.key_files
    }

    pub fn get(&self, package_type: PackageType) -> &FileMap {
        match package_type {
            PackageType::KeyBlocks => &self.key_files,
            //PackageType::Temp => &self.temp_files,
            PackageType::Blocks => &self.files,
            _ => unimplemented!("{:?}", package_type)
        }
    }
}
