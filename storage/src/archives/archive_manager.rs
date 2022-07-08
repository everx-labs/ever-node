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
        archive_slice::ArchiveSlice, file_maps::{FileDescription, FileMaps}, get_mc_seq_no,
        package_entry::PackageEntry, package_entry_id::{GetFileNameShort, PackageEntryId}, 
        package_id::PackageId, ARCHIVE_SLICE_SIZE, KEY_ARCHIVE_PACKAGE_SIZE
    },
    block_handle_db::BlockHandle, db::rocksdb::RocksDb
};
#[cfg(feature = "telemetry")]
use crate::StorageTelemetry;
use std::{borrow::Borrow, hash::Hash, io::ErrorKind, path::PathBuf, sync::Arc};
use tokio::io::AsyncWriteExt;
use ton_block::*;
use ton_types::{error, fail, Result, UInt256};


pub struct ArchiveManager {
    db: Arc<RocksDb>,
    db_root_path: Arc<PathBuf>,
    unapplied_dir: PathBuf,
    file_maps: FileMaps,
    #[cfg(feature = "telemetry")]
    telemetry: Arc<StorageTelemetry>,
    allocated: Arc<StorageAlloc>
}

impl ArchiveManager {

    pub async fn with_data(
        db: Arc<RocksDb>,
        db_root_path: Arc<PathBuf>,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<StorageTelemetry>,
        allocated: Arc<StorageAlloc>
    ) -> Result<Self> {
        let file_maps = FileMaps::new(
            db.clone(),
            &db_root_path,
            #[cfg(feature = "telemetry")]
            &telemetry,
            &allocated
        ).await?;
        let unapplied_dir = db_root_path.join("archive").join("unapplied");
        tokio::fs::create_dir_all(unapplied_dir.as_path()).await?;
        Ok(Self {
            db,
            db_root_path,
            unapplied_dir,
            file_maps,
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated
        })
    }

    pub const fn db_root_path(&self) -> &Arc<PathBuf> {
        &self.db_root_path
    }

    pub async fn add_file<B, U256, PK>(&self, entry_id: &PackageEntryId<B, U256, PK>, data: Vec<u8>) -> Result<()>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<UInt256> + Hash
    {
        log::debug!(target: "storage", "Saving unapplied file: {}", entry_id);

        if data.is_empty() {
            fail!("Added file's ({}) data can't have zero length", entry_id);
        }

        let filename = self.unapplied_dir.join(entry_id.filename_short());
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&filename).await
            .map_err(|err| error!("{} : {}", err, filename.display()))?;
        file.write_all(&data).await?;
        file.flush().await?;

        Ok(())
    }

    pub fn check_file<B, U256, PK>(
        &self,
        handle: &BlockHandle,
        entry_id: &PackageEntryId<B, U256, PK>
    ) -> bool
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<UInt256> + Hash
    {
        if handle.is_archived() {
            true
        } else {
            self.unapplied_dir.join(entry_id.filename_short()).exists()
        }
    }

    pub async fn get_file<B, U256, PK>(
        &self,
        handle: &BlockHandle,
        entry_id: &PackageEntryId<B, U256, PK>
    ) -> Result<Vec<u8>>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<UInt256> + Hash
    {
        if handle.is_archived() {
            let file = self.get_package_entry(
                handle, 
                entry_id, 
                handle.is_key_block()?
            ).await?
            .ok_or_else(|| error!("{} is archived, but archive was not found", entry_id))?;
            return Ok(file.take_data())
        }

        let _lock = match &entry_id {
            PackageEntryId::Block(_) => handle.block_file_lock().read().await,
            PackageEntryId::Proof(_) => handle.proof_file_lock().read().await,
            PackageEntryId::ProofLink(_) => handle.proof_file_lock().read().await,
            _ => fail!("Unsupported package entry")
        };
        match self.read_temp_file(entry_id).await {
            Ok((_filename, data)) => Ok(data),
            Err(e) => {
                match &entry_id {
                    PackageEntryId::Block(_) => handle.reset_data(),
                    PackageEntryId::Proof(_) => handle.reset_proof(),
                    PackageEntryId::ProofLink(_) => handle.reset_proof_link(),
                    _ => ()
                }
                log::error!(target: "storage", "Error getting file: {}", e);
                Err(e)
            }
        }
    }

    pub async fn move_to_archive(
        &self,
        handle: &BlockHandle,
        mut on_success: impl FnMut() -> Result<()>,
    ) -> Result<()> {

        if !handle.set_moving_to_archive() {
            return Ok(());
        }

        let proof_inited = handle.has_proof();
        let prooflink_inited = handle.has_proof_link();
        let data_inited = handle.has_data();

        if !data_inited || !(proof_inited || prooflink_inited) {
            log::error!(
                target: "storage",
                "Block {} is not moved to archive: data are not stored (data = {}, proof = {}, prooflink = {})",
                handle.id(),
                data_inited,
                proof_inited,
                prooflink_inited
            );
        }

        let proof_filename = if proof_inited {
            let _lock = handle.proof_file_lock().write().await;
            self.move_file_to_archives(
                handle, 
                &PackageEntryId::<&BlockIdExt, &UInt256, &UInt256>::Proof(handle.id())
            ).await?
        } else if prooflink_inited {
            let _lock = handle.proof_file_lock().write().await;
            self.move_file_to_archives(
                handle, 
                &PackageEntryId::<&BlockIdExt, &UInt256, &UInt256>::ProofLink(handle.id())
            ).await?
        } else {
            None
        };
        let block_filename = if data_inited {
            let _lock = handle.block_file_lock().write().await;
            self.move_file_to_archives(
                handle, 
                &PackageEntryId::<&BlockIdExt, &UInt256, &UInt256>::Block(handle.id())
            ).await?
        } else {
            None
        };

        on_success()?;

        if let Some(filename) = proof_filename {
            let _lock = handle.proof_file_lock().write().await;
            tokio::fs::remove_file(filename).await?;
        }
        if let Some(filename) = block_filename {
            let _lock = handle.block_file_lock().write().await;
            tokio::fs::remove_file(filename).await?;
        }
        Ok(())

    }

    pub async fn get_archive_id(&self, mc_seq_no: u32) -> Option<u64> {
        if let Some(id) = self.file_maps.files().get_closest_archive_id(mc_seq_no).await {
            Some(id)
        } else {
            self.file_maps.key_files().get_closest_archive_id(mc_seq_no).await
        }
    }

    pub async fn get_archive_slice(&self, archive_id: u64, offset: u64, limit: u32) -> Result<Vec<u8>> {
        let fd = match self.get_file_desc(&PackageId::for_block(archive_id as u32), false).await? {
            Some(file_desc) => file_desc,
            None => {
                match self.get_file_desc(&PackageId::for_key_block(archive_id as u32 / KEY_ARCHIVE_PACKAGE_SIZE), false).await? {
                    Some(key_file_desc) => key_file_desc,
                    None => fail!("Archive not found"),
                }
            },
        };
            
        fd.archive_slice().get_slice(archive_id, offset, limit).await
    }

    pub async fn gc(&self, front_for_gc_master_block_id: &BlockIdExt) {
        if let Err(e) = self.file_maps.files().gc(front_for_gc_master_block_id).await {
            log::info!(target: "storage", "archive_manager gc is error: {:?}", e);
        }
    }

    async fn get_package_entry<B, U256, PK>(
        &self, 
        handle: &BlockHandle, 
        entry_id: &PackageEntryId<B, U256, PK>,
        is_key: bool
    ) -> Result<Option<PackageEntry>>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<UInt256> + Hash
    {
        let package_id = self.get_package_id(get_mc_seq_no(handle), is_key).await?;
        let fd = self.get_file_desc(&package_id, false).await?
            .ok_or_else(|| error!("file descriptor was not found for {:?}", package_id))?;
        let pi = fd.archive_slice().get_file(Some(handle), entry_id).await?
            .ok_or_else(|| error!("file was not read for {:?}", fd.id()))?;
        Ok(Some(pi))
    }

    async fn move_file_to_archives<B, U256, PK>(
        &self, 
        handle: &BlockHandle, 
        entry_id: &PackageEntryId<B, U256, PK>
    ) -> Result<Option<PathBuf>>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<UInt256> + Hash
    {
        log::debug!(target: "storage", "Moving entry to archive: {}", entry_id.filename_short());
        let (filename, data) = {
            match self.read_temp_file(entry_id).await {
                Err(e) => {
                    // this case if file is already moved
                    if self.get_package_entry(handle, entry_id, false).await?.is_none() {
                        return Err(e)
                    }
                    if handle.is_key_block()? && self.get_package_entry(handle, entry_id, true).await?.is_none() {
                        return Err(e)
                    }
                    return Ok(None)
                }
                Ok(read) => read
            }
        };

        let data = self.move_file_to_archive(data, handle, entry_id, false).await?; 

        if handle.is_key_block()? {
            self.move_file_to_archive(data, handle, entry_id, true).await?; 
        }

        Ok(Some(filename))
    }

    async fn move_file_to_archive<B, U256, PK>(
        &self,
        data: Vec<u8>,
        handle: &BlockHandle,
        entry_id: &PackageEntryId<B, U256, PK>,
        key_archive: bool,
    ) -> Result<Vec<u8>>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<UInt256> + Hash
    {
        let mc_seq_no = get_mc_seq_no(handle);
        let is_key = handle.is_key_block()?;

        let package_id = self.get_package_id_force(mc_seq_no, key_archive, is_key).await;
        log::debug!(target: "storage", "PackageId for ({},{},{}) (mc_seq_no = {}, key block = {:?}) is {:?}, path: {:?}",
            handle.id().shard().workchain_id(),
            handle.id().shard().shard_prefix_as_str_with_tag(),
            handle.id().seq_no(),
            mc_seq_no,
            is_key,
            package_id,
            package_id.full_path(self.db_root_path.as_ref(), "pack"),
        );

        let fd = self.get_file_desc(&package_id, true).await?
            .ok_or_else(|| error!("Expected some value for {:?}", package_id))?;

        fd.archive_slice().add_file(Some(handle), entry_id, data).await
    }

    async fn read_temp_file<B, U256, PK>(&self, entry_id: &PackageEntryId<B, U256, PK>) -> Result<(PathBuf, Vec<u8>)>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<UInt256> + Hash
    {
        let temp_filename = self.unapplied_dir.join(entry_id.filename_short());
        let data = tokio::fs::read(&temp_filename).await
            .map_err(|error| {
                if error.kind() == ErrorKind::NotFound {
                    error!("File not found in archive: {:?}, {}", temp_filename, error)
                } else {
                    error!("Error reading file: {:?}, {}", temp_filename, error)
                }
            })?;
        if data.is_empty() {
            fail!(
                "Read temp file {} ({}) is corrupted! It cannot have zero length!", 
                temp_filename.as_os_str().to_str().unwrap_or("bad path"), 
                entry_id
            )
        }
        Ok((temp_filename, data))
    }

    async fn get_file_desc(&self, id: &PackageId, force_create: bool) -> Result<Option<Arc<FileDescription>>> {
        // TODO: Rewrite logics in order to handle multithreaded adding of packages
        if let Some(fd) = self.file_maps.get(id.package_type()).get(id.id()).await {
            if fd.deleted() {
                return Ok(None)
            }
            return Ok(Some(fd))
        }
        if force_create {
            Ok(Some(self.add_file_desc(id).await?))
        } else {
            Ok(None)
        }
    }

    async fn add_file_desc(&self, id: &PackageId) -> Result<Arc<FileDescription>> {
        // TODO: Rewrite logics in order to handle multithreaded adding of packages
        let file_map = self.file_maps.get(id.package_type());
        assert!(file_map.get(id.id()).await.is_none());
        let dir = self.db_root_path.join(id.path());
        tokio::fs::create_dir_all(&dir).await?;
        let archive_slice = ArchiveSlice::new_empty(
            self.db.clone(),
            Arc::clone(&self.db_root_path),
            id.id(),
            id.package_type(),
            #[cfg(feature = "telemetry")]
            self.telemetry.clone(),
            self.allocated.clone()
        ).await?;
        let fd = Arc::new(FileDescription::with_data(
            id.clone(),
            archive_slice,
            false
        ));
        file_map.put(
            id.id(), 
            Arc::clone(&fd),
            #[cfg(feature = "telemetry")]
            &self.telemetry,
            &self.allocated
        ).await?;
        Ok(fd)
    }

    async fn get_package_id(&self, mc_seq_no: u32, is_key: bool) -> Result<PackageId> {
        if is_key {
            Ok(PackageId::for_key_block(mc_seq_no / KEY_ARCHIVE_PACKAGE_SIZE))
        } else {
            let package_id = self.file_maps.files().get_closest_id(mc_seq_no).await.ok_or_else(|| {
                log::error!(target: "storage", "Package not found for seq_no: {}", mc_seq_no);
                error!("Package not found for seq_no: {}", mc_seq_no)
            })?;
            Ok(PackageId::for_block(package_id))
        }
    }

    async fn get_package_id_force(&self, mc_seq_no: u32, key_archive: bool, is_key: bool) -> PackageId {
        if is_key {
            if key_archive {
                PackageId::for_key_block(mc_seq_no / KEY_ARCHIVE_PACKAGE_SIZE)
            } else {
                PackageId::for_block(mc_seq_no)
            }
        } else {
            let mut package_id = mc_seq_no - (mc_seq_no % ARCHIVE_SLICE_SIZE);
            if let Some(found_package_id) = self.file_maps.files().get_closest_id(mc_seq_no).await {
                if package_id < found_package_id {
                    package_id = found_package_id;
                }
            }
            PackageId::for_block(package_id)
        }
    }

    pub async fn trunc<F: Fn(&BlockIdExt) -> bool>(&self, block_id: &BlockIdExt, delete_condition: &F) -> Result<()> {
        self.file_maps.trunc(block_id, delete_condition).await
    }

}
