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
    StorageAlloc, 
    archives::{
        archive_slice::ArchiveSlice, package_id::{PackageId, PackageType},
        package_index_db::{PackageIndexDb, PackageIndexEntry}
    },
    db::rocksdb::RocksDb
};
#[cfg(feature = "telemetry")]
use crate::StorageTelemetry;
use adnl::{declare_counted, common::{CountedObject, Counter}};
use std::{path::PathBuf, sync::Arc};
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;
use ton_types::{Result, error};
use ton_block::BlockIdExt;

use super::ARCHIVE_SLICE_SIZE;

//#[derive(Debug)]
pub struct FileDescription {
    id: PackageId,
    deleted: bool,
    archive_slice: ArchiveSlice,
}

impl FileDescription {
    pub fn with_data(id: PackageId, archive_slice: ArchiveSlice, deleted: bool) -> Self {
        Self { id, deleted, archive_slice }
    }

    pub const fn id(&self) -> &PackageId {
        &self.id
    }

    pub const fn deleted(&self) -> bool {
        self.deleted
    }

    pub const fn archive_slice(&self) -> &ArchiveSlice {
        &self.archive_slice
    }

    async fn destroy(&mut self) -> Result<()> {
        self.archive_slice.destroy().await
    }

    async fn trunc<F: Fn(&BlockIdExt) -> bool>(&mut self, block_id: &BlockIdExt, delete_condition: &F) -> Result<()> {
        self.archive_slice.trunc(block_id, delete_condition).await
    }
}

//#[derive(Debug)]
declare_counted!(
    pub struct FileMapEntry {
        key: u32,
        value: Arc<FileDescription>
    }
);

//#[derive(Debug)]
pub struct FileMap {
    storage: PackageIndexDb,
    elements: tokio::sync::RwLock<Vec<FileMapEntry>>, // new FileMapEntry every key_block
}

impl FileMap {

    pub async fn new(
        db: Arc<RocksDb>,
        db_root_path: &Arc<PathBuf>,
        path: impl ToString,
        package_type: PackageType,
        #[cfg(feature = "telemetry")]
        telemetry: &Arc<StorageTelemetry>,
        allocated: &Arc<StorageAlloc>
    ) -> Result<Self> {
 
        let storage = PackageIndexDb::with_db(db.clone(), path)?;
        let mut index_pairs = Vec::new();

        storage.for_each_deserialized(|key, value| {
            index_pairs.push((key, value));
            Ok(true)
        })?;

        index_pairs.sort_by_key(|pair| pair.0);

        let mut elements = Vec::new();
        for (key, value) in index_pairs {
            let archive_slice = match ArchiveSlice::with_data(
                db.clone(),
                Arc::clone(db_root_path),
                key,
                package_type,
                value.finalized(),
                #[cfg(feature = "telemetry")]
                telemetry.clone(),
                allocated.clone()
            ).await {
                Ok(s) => s,
                Err(e) => {
                    log::warn!(target: "storage", "Can't read archive slice {}: {}", key, e);
                    continue;
                }
            };
            let value = Arc::new(FileDescription::with_data(
                PackageId::with_values(key, package_type),
                archive_slice,
                value.deleted()
            ));
            elements.push(
                FileMapEntry { 
                    key,  
                    value,
                    counter: allocated.file_entries.clone().into() 
                }
            );
            #[cfg(feature = "telemetry")]
            telemetry.file_entries.update(
                allocated.file_entries.load(Ordering::Relaxed)
            )
        }

        Ok(Self {
            storage,
            elements: tokio::sync::RwLock::new(elements),
        })
    }

    pub async fn put(
        &self, 
        mc_seq_no: u32, 
        file_description: Arc<FileDescription>,
        #[cfg(feature = "telemetry")]
        telemetry: &Arc<StorageTelemetry>,
        allocated: &Arc<StorageAlloc>
    ) -> Result<()> {
        let entry = FileMapEntry { 
            key: mc_seq_no, 
            value: file_description ,
            counter: allocated.file_entries.clone().into() 
        };
        #[cfg(feature = "telemetry")]
        telemetry.file_entries.update(
            allocated.file_entries.load(Ordering::Relaxed)
        );
        let mut guard = self.elements.write().await;
        match guard.binary_search_by(|entry| entry.key.cmp(&mc_seq_no)) {
            Ok(index) => guard[index] = entry,
            Err(index) => guard.insert(index, entry),
        }
        self.storage.put_value(&mc_seq_no.into(), PackageIndexEntry::new())?;
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

            if elements[i].value.archive_slice.package_type() == PackageType::Blocks
            && next_id <= front_for_gc_master_block_id.seq_no() {
                marked_packages.push(elements[i].key);
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

    pub async fn get(&self, mc_seq_no: u32) -> Option<Arc<FileDescription>> {
        let guard = self.elements.read().await;
        log::debug!(target: "storage", "Searching for file description (elements count = {})", guard.len());
        match guard.binary_search_by(|entry| entry.key.cmp(&mc_seq_no)) {
            Ok(index) => Some(Arc::clone(&guard[index].value)),
            Err(_) => None
        }
    }

    pub async fn get_closest(&self, mc_seq_no: u32) -> Option<Arc<FileDescription>> {
        let guard = self.elements.read().await;
        log::debug!(target: "storage", "Searching for file description (elements count = {})", guard.len());
        let index = match guard.binary_search_by(|entry| entry.key.cmp(&mc_seq_no)) {
            Ok(index) => index,
            Err(0) => return None,
            Err(index) => index - 1
        };
        Some(Arc::clone(&guard[index].value))
    }

    pub async fn get_closest_id(&self, mc_seq_no: u32) -> Option<u32> {
        self.get_closest(mc_seq_no).await.map(|fd| fd.id().id())
    }

    pub async fn get_closest_archive_id(&self, mc_seq_no: u32) -> Option<u64> {
        match self.get_closest(mc_seq_no).await {
            Some(fd) => fd.archive_slice().get_archive_id(mc_seq_no).await,
            None => None
        }
    }

    pub async fn trunc<F: Fn(&BlockIdExt) -> bool>(&self, block_id: &BlockIdExt, delete_condition: &F) -> Result<()> {
        let mut guard = self.elements.write().await;
        log::debug!(target: "storage", "Searching for file description (elements count = {})", guard.len());
        // TODO: may be iterate from end
        let index = match guard.binary_search_by(|entry| entry.key.cmp(&block_id.seq_no)) {
            Ok(index) => index,
            Err(0) => return Ok(()),
            Err(index) => index - 1
        };
        if guard.len() > index + 1 {
            for mut entry in guard.drain(index + 1..) {
                if let Some(entry) = Arc::get_mut(&mut entry.value) {
                    if let Err(e) = entry.destroy().await {
                        log::warn!(target: "storage", "Can't destroy entry {}: {}", index, e);
                    }
                } else {
                    log::warn!(target: "storage", "Can't get_mut entry {}", index);
                }
                self.storage.delete(&entry.key.into())?;
            }
        }
        debug_assert_eq!(guard.len(), index + 1);
        let entry = guard.last_mut().ok_or_else(|| error!("internal error during trunc {}", index))?;
        let fd = Arc::get_mut(&mut entry.value).ok_or_else(|| error!("unable to get FileDescription as mutable"))?;
        // clear finalized flag
        self.storage.put_value(&entry.key.into(), PackageIndexEntry::new())?;
        fd.trunc(block_id, delete_condition).await
    }
}

pub struct FileMaps {
    files: FileMap,
    key_files: FileMap,
    // temp_files: FileMap,
}

impl FileMaps {

    pub async fn new(
        db: Arc<RocksDb>,
        db_root_path: &Arc<PathBuf>,
        #[cfg(feature = "telemetry")]
        telemetry: &Arc<StorageTelemetry>,
        allocated: &Arc<StorageAlloc>
    ) -> Result<Self> {
        Ok(Self {
            files: FileMap::new(
                db.clone(),
                db_root_path, 
                "files",
                PackageType::Blocks,
                #[cfg(feature = "telemetry")]
                telemetry,
                allocated
            ).await?,
            key_files: FileMap::new(
                db.clone(),
                db_root_path, 
                "key_files",
                PackageType::KeyBlocks,
                #[cfg(feature = "telemetry")]
                telemetry,
                allocated
            ).await?,
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

    pub async fn trunc<F: Fn(&BlockIdExt) -> bool>(&self, block_id: &BlockIdExt, delete_condition: &F) -> Result<()> {
        self.files.trunc(block_id, delete_condition).await?;
        self.key_files.trunc(block_id, delete_condition).await
    }
}
