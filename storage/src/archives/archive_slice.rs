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
        get_mc_seq_no_opt, ARCHIVE_PACKAGE_SIZE, KEY_ARCHIVE_PACKAGE_SIZE,
        package::{Package, read_package_from},
        package_entry::PackageEntry, package_entry_id::{GetFileName, PackageEntryId},
        package_entry_meta::PackageEntryMeta, package_entry_meta_db::PackageEntryMetaDb,
        package_id::{PackageId, PackageType}, package_info::PackageInfo,
        package_offsets_db::PackageOffsetsDb, package_status_db::PackageStatusDb, 
        package_status_key::PackageStatusKey
    },
    block_handle_db::BlockHandle, db::rocksdb::RocksDb, traits::Serializable
};
#[cfg(feature = "telemetry")]
use crate::StorageTelemetry;
use std::{borrow::Borrow, hash::Hash, io::SeekFrom, path::PathBuf, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use ton_block::BlockIdExt;
use ton_types::{error, fail, Result, UInt256};


const DEFAULT_PKG_VERSION: u32 = 1;

//#[derive(Debug)]
pub struct ArchiveSlice {
    archive_id: u32,
    packages: tokio::sync::RwLock<Vec<Arc<PackageInfo>>>,
    db_root_path: Arc<PathBuf>,
    sliced_mode: bool,
    slice_size: u32,
    package_type: PackageType,
    finalized: bool,
    index_db: PackageEntryMetaDb,
    offsets_db: PackageOffsetsDb,
    package_status_db: PackageStatusDb,
    #[cfg(feature = "telemetry")]
    telemetry: Arc<StorageTelemetry>,
    allocated: Arc<StorageAlloc>
}

impl ArchiveSlice {

    async fn new(
        db: Arc<RocksDb>,
        db_root_path: Arc<PathBuf>,
        archive_id: u32,
        package_type: PackageType,
        finalized: bool,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<StorageTelemetry>,
        allocated: Arc<StorageAlloc>
    ) -> Result<Self> {

        std::fs::create_dir_all(db_root_path.join("archive/packages"))
            .map_err(|err| error!("Cannot create directory: {:?} : {}", db_root_path, err))?;

        let (prefix, slice_size, sliced_mode) =  if package_type == PackageType::KeyBlocks {
            ("key_", KEY_ARCHIVE_PACKAGE_SIZE, false)
        } else {
            ("", ARCHIVE_PACKAGE_SIZE, true)
        };
        let index_db = PackageEntryMetaDb::with_db(
            db.clone(), 
            format!("entry_meta_{}db_{}", prefix, archive_id)
        )?;
        let offsets_db = PackageOffsetsDb::with_db(
            db.clone(), 
            format!("offsets_{}db_{}", prefix, archive_id)
        )?;
        let package_status_db = PackageStatusDb::with_db(
            db, 
            format!("status_{}db_{}", prefix, archive_id)
        )?;

        Ok(Self {
            archive_id,
            packages: tokio::sync::RwLock::new(Vec::new()),
            db_root_path,
            sliced_mode,
            slice_size,
            package_type,
            finalized,
            index_db,
            offsets_db,
            package_status_db,
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated,
        })

    }

    pub async fn new_empty(
        db: Arc<RocksDb>,
        db_root_path: Arc<PathBuf>,
        archive_id: u32,
        package_type: PackageType,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<StorageTelemetry>,
        allocated: Arc<StorageAlloc>
    ) -> Result<Self> {
        let archive_slice = Self::new(
            db,
            db_root_path,
            archive_id,
            package_type,
            false,
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated,
            ).await?;
        archive_slice.init().await
    }

    async fn init(self) -> Result<Self> {
        if self.sliced_mode {
            let mut transaction = self.package_status_db.begin_transaction()?;

            transaction.put(&PackageStatusKey::SlicedMode, true.to_vec()?.as_slice());
            transaction.put(&PackageStatusKey::TotalSlices, 1u32.to_vec()?.as_slice());
            transaction.put(&PackageStatusKey::SliceSize, self.slice_size.to_vec()?.as_slice());

            let meta = PackageEntryMeta::with_data(0, DEFAULT_PKG_VERSION);
            self.index_db.put_value(&0.into(), &meta)?;
            transaction.commit()?;

            assert_eq!(self.index_db.len().unwrap(), 1);
            assert_eq!(self.package_status_db.len().unwrap(), 3);

        } else {
            let mut transaction = self.package_status_db.begin_transaction()?;

            transaction.put(&PackageStatusKey::SlicedMode, false.to_vec()?.as_slice());
            transaction.put(&PackageStatusKey::NonSlicedSize, 0u64.to_vec()?.as_slice());

            transaction.commit()?;

            assert_eq!(self.index_db.len().unwrap(), 0);
            assert_eq!(self.package_status_db.len().unwrap(), 2);
        }
        self.packages.write().await
            .push(self.new_package(0, self.archive_id, 0, DEFAULT_PKG_VERSION).await?);
        Ok(self)
    }

    pub async fn with_data(
        db: Arc<RocksDb>,
        db_root_path: Arc<PathBuf>,
        archive_id: u32,
        package_type: PackageType,
        finalized: bool,
        #[cfg(feature = "telemetry")]
        telemetry: Arc<StorageTelemetry>,
        allocated: Arc<StorageAlloc>,
    ) -> Result<Self> {
        let archive_slice = Self::new(
            db,
            db_root_path,
            archive_id,
            package_type,
            finalized,
            #[cfg(feature = "telemetry")]
            telemetry,
            allocated,
            ).await?;
        archive_slice.load().await
    }

    async fn load(mut self) -> Result<Self> {
        self.sliced_mode = self.package_status_db.try_get_value::<bool>(&PackageStatusKey::SlicedMode)?
            .ok_or_else(|| error!("cannot read sliced_mode"))?;
        if self.sliced_mode {
            let total_slices = self.package_status_db.get_value::<u32>(&PackageStatusKey::TotalSlices)?;
            self.slice_size = self.package_status_db.get_value::<u32>(&PackageStatusKey::SliceSize)?;
            log::debug!(target: "storage", "Read package status for the sliced mode. total_slices: {}, slice_size: {}", total_slices, self.slice_size);
            assert!(self.slice_size > 0);

            let mut packages = Vec::new();
            'c: for i in 0..total_slices {
                let meta = self.index_db.get_value(&i.into())?;
                log::info!(target: "storage", "Read slice #{} metadata: {:?}", i, meta);

                match self.new_package(i, self.archive_id + self.slice_size * i, 
                    meta.entry_size(), meta.version()).await 
                {
                    Ok(p) => packages.push(p),
                    Err(e) => {
                        log::warn!("Error while read slice #{}: {}. Stopped slices reading", i, e);
                        break 'c;
                    }
                }
            }
            self.packages = tokio::sync::RwLock::new(packages);
        } else {
            let size = self.package_status_db.get_value::<u64>(&PackageStatusKey::NonSlicedSize)?;
            self.packages.write().await
                .push(self.new_package(0, self.archive_id, size, 0).await?);
        }
        Ok(self)
    }

    pub fn package_type(&self) -> PackageType {
        self.package_type
    }

    pub async fn destroy(&mut self) -> Result<()> {
        for pi in self.packages.write().await.drain(..) {
            // TODO: check existance
            pi.package().destroy().await?;
        }

        self.index_db.destroy()?;
        self.offsets_db.destroy()?;
        self.package_status_db.destroy()?;

        Ok(())
    }

    pub fn archive_id(&self) -> u32 {
        self.archive_id
    }

    fn get_index_opt(&self, mc_seq_no: u32) -> Option<u32> {
        if self.package_type == PackageType::KeyBlocks {
            (mc_seq_no / KEY_ARCHIVE_PACKAGE_SIZE).checked_sub(self.archive_id).map(|value| value / self.slice_size)
        } else {
            mc_seq_no.checked_sub(self.archive_id).map(|value| value / self.slice_size)
        }
    }

    fn get_index(&self, mc_seq_no: u32) -> Result<u32> {
        self.get_index_opt(mc_seq_no)
            .ok_or_else(|| error!("wrong mc_seq_no {} < {}", mc_seq_no, self.archive_id))
    }

    pub async fn get_archive_id(&self, mc_seq_no: u32) -> Option<u64> {
        if !self.sliced_mode {
            return Some(self.archive_id as u64);
        }

        if let Some(idx) = self.get_index_opt(mc_seq_no) {
            let package_count = self.packages.read().await.len() as u32;
            if idx < package_count {
                let mc_seq_no = self.archive_id + self.slice_size * idx;
                return Some(((mc_seq_no as u64) << 32) | (self.archive_id as u64));
            }
        }

        None
    }

    pub async fn add_file<B, U256, PK>(&self, block_handle: Option<&BlockHandle>, entry_id: &PackageEntryId<B, U256, PK>, data: Vec<u8>) -> Result<Vec<u8>>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<UInt256> + Hash
    {
        let offset_key = entry_id.into();
        if self.offsets_db.contains(&offset_key)? {
            // afrer DB's truncation it is possible to have some remains in offsets_db
            log::warn!(target: "storage", 
                "Entry {} was already presented in offsets_db, it will be rewrite", entry_id);
        }

        let package_info = self.choose_package(get_mc_seq_no_opt(block_handle), true).await?;

        let entry = PackageEntry::with_data(entry_id.filename(), data);

        let idx = if self.sliced_mode {
            package_info.idx()
        } else {
            //assert_ne!(package_info.idx(), 0);
            u32::max_value()
        };

        package_info.package().append_entry(&entry,
            |offset, size| {
                let meta = PackageEntryMeta::with_data(size, package_info.version());
                log::debug!(target: "storage", "Writing package entry metadata for slice #{}: {:?}, offset: {}", idx, meta, offset);
                self.index_db.put_value(&idx.into(), meta)?;
                self.offsets_db.put_value(&offset_key, offset)
            }
        ).await?;

        Ok(entry.take_data())
    }

    pub async fn get_file<B, U256, PK>(
        &self, 
        block_handle: Option<&BlockHandle>, 
        entry_id: &PackageEntryId<B, U256, PK>
    ) -> Result<Option<PackageEntry>>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<UInt256> + Hash
    {
        let offset_key = entry_id.into();
        let offset = match self.offsets_db.try_get_value(&offset_key)? {
            Some(offset) => offset,
            None => return Ok(None)
        };

        let package_info = self.choose_package(get_mc_seq_no_opt(block_handle), false).await?;

        log::debug!(
            target: "storage",
            "Reading package entry: {:?}, offset: {}",
            package_info.package().get_path(),
            offset
        );
        let entry = package_info.package().read_entry(offset).await?;
        if entry.data().is_empty() {
            fail!("Read entry ({}) is corrupted! It can't have zero length!", entry_id);
        }
        Ok(Some(entry))
    }

    pub async fn get_slice(&self, archive_id: u64, offset: u64, limit: u32) -> Result<Vec<u8>> {
        if archive_id as u32 != self.archive_id {
            fail!("Bad archive ID (archive_id = {}, expected {})!", archive_id as u32, self.archive_id);
        }

        let mc_seq_no = (archive_id >> 32) as u32;
        let package_info = self.choose_package(mc_seq_no, false).await?;
        let mut file = tokio::fs::File::open(package_info.package().path()).await?;
        let mut buffer = vec![0; limit as usize];
        file.seek(SeekFrom::Start(offset)).await?;
        let mut buf_offset = 0;
        let mut actual_read = 0;
        loop {
            let read = file.read(&mut buffer[buf_offset..]).await?;
            if read == 0 {
                break;
            }
            actual_read += read;
            buf_offset += read;
        }
        buffer.resize(actual_read, 0);

        Ok(buffer)
    }

    async fn new_package(&self, idx: u32, seq_no: u32, size: u64, version: u32) -> Result<Arc<PackageInfo>> {
        log::debug!(target: "storage", "Adding package, seq_no: {}, size: {} bytes, version: {}", seq_no, size, version);
        let package_id = PackageId::with_values(seq_no, self.package_type);
        let path = package_id.full_path(self.db_root_path.as_path(), "pack");
        std::fs::create_dir_all(path.parent().unwrap())
            .map_err(|err| error!("Cannot create directory: {:?} : {}", path.parent(), err))?;

        let package = match Package::open(path.clone(), false, true).await {
            Ok(p) => p,
            Err(e) => {
                match tokio::fs::remove_file(path.as_path()).await {
                    Ok(_) => log::warn!(
                        "Failed to open or create archive \"{}\": {}. Archive file was cleaned up.",
                        path.display(), e
                    ),
                    Err(e2) => log::warn!(
                        "Failed to open or create archive \"{}\": {}. Archive file wasn't cleaned up: {}.",
                        path.display(), e, e2
                    ),
                }
                fail!("Failed to open or create archive \"{}\": {}.", path.display(), e);
            }
        };

        if !self.finalized && version >= DEFAULT_PKG_VERSION {
            package.truncate(size).await?;
        }

        let pi = Arc::new(PackageInfo::with_data(
            package_id,
            package,
            idx,
            version,
            #[cfg(feature = "telemetry")]
            &self.telemetry,
            &self.allocated
        ));

        Ok(pi)
    }

    async fn choose_package(&self, mc_seq_no: u32, force_create: bool) -> Result<Arc<PackageInfo>> {
        if self.package_type != PackageType::Blocks || !self.sliced_mode {
            return Ok(Arc::clone(&self.packages.read().await[0]));
        }

        let idx = self.get_index(mc_seq_no)?;
        {
            let mut write_guard = self.packages.write().await;
            let package_count = write_guard.len();
            if (idx as usize) < package_count {
                Ok(Arc::clone(&write_guard[idx as usize]))
            } else {
                if !force_create {
                    fail!("mc_seq_no is too big");
                }

                if idx as usize != package_count {
                    fail!(
                        "Packages must not be skipped! mc_seq_no = {}, archive_id = {}, idx = {}, expected = {}",
                        mc_seq_no,
                        self.archive_id,
                        idx,
                        package_count
                    )
                }

                if (mc_seq_no - self.archive_id) % self.slice_size != 0 {
                    fail!("Blocks must not be skipped! mc_seq_no = {}, expected = {}",
                    mc_seq_no,
                    mc_seq_no - (mc_seq_no - self.archive_id) / self.slice_size
                    );
                }

                let pi = self.new_package(idx, mc_seq_no, 0, DEFAULT_PKG_VERSION).await?;

                let index_entry = PackageEntryMeta::with_data(0, DEFAULT_PKG_VERSION);
                self.index_db.put_value(&idx.into(), &index_entry)?;
                self.package_status_db.put_value(&PackageStatusKey::TotalSlices, idx + 1)?;
                write_guard.push(Arc::clone(&pi));

                Ok(pi)
            }
        }
    }

    /// truncs slice starting from master block_id
    pub async fn trunc<F: Fn(&BlockIdExt) -> bool>(&mut self, block_id: &BlockIdExt, delete_condition: &F) -> Result<()> {
        log::info!(target: "storage", "truncating by mc_seq_no: {}, sliced_mode: {}", block_id.seq_no(), self.sliced_mode);
        let entry_id = &PackageEntryId::<&BlockIdExt, &UInt256, &UInt256>::Proof(block_id);
        let offset_key = entry_id.into();
        let offset = match self.offsets_db.try_get_value(&offset_key)? {
            Some(offset) => offset,
            None => return Ok(())
        };

        let index = self.get_index_opt(block_id.seq_no())
            .ok_or_else(|| error!("slice is corrupted sliced_mode: {}, {} < {}", self.sliced_mode, block_id.seq_no(), self.archive_id))?;

        let mut guard = self.packages.write().await;
        if guard.len() > index as usize + 1 {
            for ref mut package_info in guard.drain(index as usize + 1..) {
                Arc::get_mut(package_info)
                    .ok_or_else(|| error!("slice incorrect {}", index))?
                    .destroy().await?;
            }
        }

        if self.sliced_mode {
            self.package_status_db.put_value(&PackageStatusKey::TotalSlices, index + 1)?;
        } else {
            self.package_status_db.put_value(&PackageStatusKey::NonSlicedSize, offset)?;
        }

        let package_info = guard.last_mut()
            .ok_or_else(|| error!("slice incorrect {}", index))?;

        if !self.sliced_mode {
            package_info.package().truncate(offset).await?;
        } else {
            // Delete unneeded entries from package by repack.
            // 1) read all items from package and write it (with condition) into "new" package
            //      write new offsets and indexes into correspond dbs by the way
            // 2) rename new package
            // while old package is not deleted repack might be replay many times (it doesn't use offsets db),
            // but read form package will fail (offsets db points new package)

            let old_package = package_info.package();
            let new_name = old_package.get_path() + ".new";
            log::trace!(target: "storage", "repack package {}", old_package.get_path());
            let _ = tokio::fs::remove_file(&new_name);
            let new_package = Package::open(new_name.into(), false, true).await?;
            let mut old_reader = read_package_from(old_package.open_file().await?).await?;
            while let Some(entry) = old_reader.next().await? {
                let entry_id = PackageEntryId::from_filename(entry.filename())?;
                let id = match &entry_id {
                    PackageEntryId::Block(id) => id,
                    PackageEntryId::Proof(id) => id,
                    PackageEntryId::ProofLink(id) => id,
                    _ => fail!("Unsupported entry: {:?}", entry_id),
                };
                if delete_condition(id) {
                    log::trace!(target: "storage", "repack package: delete {}", entry_id);

                    let idx = package_info.idx();
                    let key = idx.into();
                    if let Err(e) = self.index_db.delete(&key) {
                        log::warn!("Can't delete {} from index db (slice: {}): {}", idx, self.archive_id, e);
                    }
                    let offset_key = (&entry_id).into();
                    if let Err(e) = self.offsets_db.delete(&offset_key) {
                        log::warn!("Can't delete {} from offsets db (slice: {}): {}", entry_id, self.archive_id, e);
                    }
                } else {
                    log::trace!(target: "storage", "repack package: repack {}", entry_id);

                    new_package.append_entry(
                        &entry,
                        |offset, size| {
                            let meta = PackageEntryMeta::with_data(size, package_info.version());
                            let idx = package_info.idx();
                            log::debug!(
                                target: "storage", 
                                "Writing package entry metadata for slice #{}: {:?}, offset: {}",
                                idx, meta, offset
                            );
                            self.index_db.put_value(&idx.into(), meta)?;
                            let offset_key = (&entry_id).into();
                            self.offsets_db.put_value(&offset_key, offset)
                        }
                    ).await?;
                }
            }
            tokio::fs::rename(new_package.get_path(), old_package.get_path()).await?;

            let name = old_package.get_path().into();
            let pi = Arc::get_mut(package_info)
                .ok_or_else(|| error!("can't get mut ptr of package_info"))?;
            *pi.package_mut() = Package::open(name, false, true).await?;

            log::trace!(target: "storage", "package repacked {}", pi.package().path().display());
        }

        Ok(())
    }

}
