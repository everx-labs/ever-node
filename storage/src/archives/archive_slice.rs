use crate::{
    archives::{
        get_mc_seq_no_opt, archive_manager::SLICE_SIZE, package::Package,
        package_entry::PackageEntry, package_entry_id::{GetFileName, PackageEntryId},
        package_entry_meta::PackageEntryMeta, package_entry_meta_db::PackageEntryMetaDb,
        package_id::{PackageId, PackageType}, package_info::PackageInfo,
        package_offsets_db::PackageOffsetsDb, package_status_db::PackageStatusDb, 
        package_status_key::PackageStatusKey
    },
    traits::Serializable, types::BlockHandle
};
use std::{borrow::Borrow, hash::Hash, io::SeekFrom, path::PathBuf, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use ton_api::ton::PublicKey;
use ton_block::BlockIdExt;
use ton_types::{error, fail, Result, UInt256};


const DEFAULT_PKG_VERSION: u32 = 1;

#[derive(Debug)]
pub struct ArchiveSlice {
    archive_id: u32,
    packages: tokio::sync::RwLock<Vec<Arc<PackageInfo>>>,
    db_root_path: Arc<PathBuf>,
    index_path: PathBuf,
    sliced_mode: bool,
    slice_size: u32,
    package_type: PackageType,
    finalized: bool,
    index_db: Arc<PackageEntryMetaDb>,
    offsets_db: Arc<PackageOffsetsDb>,
    package_status_db: Arc<PackageStatusDb>,
}

impl ArchiveSlice {
    pub async fn with_data(
        db_root_path: Arc<PathBuf>,
        archive_id: u32,
        package_type: PackageType,
        finalized: bool,
    ) -> Result<Self> {
        let package_id = PackageId::with_values(archive_id, package_type);
        let index_path = package_id.full_path(db_root_path.as_ref(), "index");

        let index_db = Arc::new(PackageEntryMetaDb::with_path(index_path.join("entry_meta_db")));
        let offsets_db = Arc::new(PackageOffsetsDb::with_path(index_path.join("offsets_db")));
        let package_status_db = Arc::new(PackageStatusDb::with_path(index_path.join("status_db")));

        let mut archive_slice = Self {
            archive_id,
            packages: tokio::sync::RwLock::new(Vec::new()),
            db_root_path,
            index_path,
            sliced_mode: false,
            slice_size: SLICE_SIZE,
            package_type,
            finalized,
            index_db: Arc::clone(&index_db),
            offsets_db,
            package_status_db: Arc::clone(&package_status_db),
        };

        if let Some(sliced_mode) = package_status_db.try_get_value::<bool>(&PackageStatusKey::SlicedMode)? {
            archive_slice.sliced_mode = sliced_mode;
            if sliced_mode {
                let total_slices = package_status_db.get_value::<u32>(&PackageStatusKey::TotalSlices)?;
                archive_slice.slice_size = package_status_db.get_value::<u32>(&PackageStatusKey::SliceSize)?;
                log::debug!(target: "storage", "Read package status for the sliced mode. total_slices: {}, slice_size: {}", total_slices, archive_slice.slice_size);
                assert!(archive_slice.slice_size > 0);

                let mut packages = Vec::new();
                for i in 0..total_slices {
                    let meta = index_db.get_value(&i.into())?;
                    log::debug!(target: "storage", "Read slice #{} metadata: {:?}", i, meta);

                    packages.push(archive_slice.new_package(i, archive_id + archive_slice.slice_size * i, meta.entry_size(), meta.version()).await?);
                }
                archive_slice.packages = tokio::sync::RwLock::new(packages);
            } else {
                let size = package_status_db.get_value::<u64>(&PackageStatusKey::NonSlicedSize)?;
                archive_slice.packages.write().await
                    .push(archive_slice.new_package(0, archive_id, size, 0).await?);
            }
        } else {
            if package_type == PackageType::Blocks {
                archive_slice.sliced_mode = true;

                {
                    let transaction = package_status_db.begin_transaction()?;

                    transaction.put(&PackageStatusKey::SlicedMode, true.to_vec()?.as_slice());
                    transaction.put(&PackageStatusKey::TotalSlices, 1u32.to_vec()?.as_slice());
                    transaction.put(&PackageStatusKey::SliceSize, archive_slice.slice_size.to_vec()?.as_slice());

                    let meta = PackageEntryMeta::with_data(0, DEFAULT_PKG_VERSION);
                    index_db.put_value(&0.into(), &meta)?;
                    transaction.commit()?;
                }

                archive_slice.packages.write().await
                    .push(archive_slice.new_package(0, archive_id, 0, DEFAULT_PKG_VERSION).await?);
            } else {
                {
                    let transaction = package_status_db.begin_transaction()?;

                    transaction.put(&PackageStatusKey::SlicedMode, false.to_vec()?.as_slice());
                    transaction.put(&PackageStatusKey::NonSlicedSize, 0u64.to_vec()?.as_slice());

                    transaction.commit()?;
                }

                archive_slice.packages.write().await
                    .push(archive_slice.new_package(0, archive_id, 0, 0).await?);
            }
        }

        Ok(archive_slice)
    }

    #[allow(dead_code)]
    pub async fn destroy(mut self) -> Result<()> {
        for pi in self.packages.write().await.drain(..) {
            let path = Arc::clone(pi.package().path());
            drop(pi);
            tokio::fs::remove_file(&*path).await?;
        }

        Arc::get_mut(&mut self.index_db)
            .ok_or_else(|| error!("Unable to get mutable reference to index_db"))?
            .destroy()?;
        Arc::get_mut(&mut self.offsets_db)
            .ok_or_else(|| error!("Unable to get mutable reference to offsets_db"))?
            .destroy()?;
        Arc::get_mut(&mut self.package_status_db)
            .ok_or_else(|| error!("Unable to get mutable reference to package_status_db"))?
            .destroy()?;

        tokio::fs::remove_dir_all(self.index_path).await?;

        Ok(())
    }

    pub async fn get_archive_id(&self, mc_seq_no: u32) -> Option<u64> {
        if !self.sliced_mode {
            return Some(self.archive_id as u64);
        }

        if mc_seq_no >= self.archive_id {
            let idx = (mc_seq_no - self.archive_id) / self.slice_size;
            let package_count = self.packages.read().await.len() as u32;
            if idx < package_count {
                let package_id = self.archive_id + self.slice_size * idx;
                return Some(((package_id as u64) << 32) | (self.archive_id as u64));
            }
        }

        None
    }

    pub async fn add_file<B, U256, PK>(&self, block_handle: Option<&BlockHandle>, entry_id: &PackageEntryId<B, U256, PK>, data: Vec<u8>) -> Result<()>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<PublicKey> + Hash
    {
        let offset_key = entry_id.into();
        if self.offsets_db.contains(&offset_key)? {
            return Ok(());
        }

        let package_info = self.choose_package(get_mc_seq_no_opt(block_handle), true).await?;

        let entry = PackageEntry::with_data(entry_id.filename(), data);

        let idx = if self.sliced_mode {
            package_info.idx()
        } else {
            assert_ne!(package_info.idx(), 0);
            u32::max_value()
        };

        package_info.package().append_entry(&entry,
            |offset, size| {
                let meta = PackageEntryMeta::with_data(size, package_info.version());
                log::debug!(target: "storage", "Writing package entry metadata for slice #{}: {:?}, offset: {}", idx, meta, offset);
                self.index_db.put_value(&idx.into(), meta)?;
                self.offsets_db.put_value(&offset_key, offset)
            }
        ).await
    }

    pub async fn get_file<B, U256, PK>(
        &self, 
        block_handle: Option<&BlockHandle>, 
        entry_id: &PackageEntryId<B, U256, PK>
    ) -> Result<PackageEntry>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<PublicKey> + Hash
    {
        let offset_key = entry_id.into();
        let offset = self.offsets_db.try_get_value(&offset_key)?
            .ok_or_else(|| error!("File is not in archive: {}", entry_id))?;

        let package_info = self.choose_package(get_mc_seq_no_opt(block_handle), false).await?;

        log::debug!(
            target: "storage",
            "Reading package entry: {:?}, offset: {}",
            package_info.package().path(),
            offset
        );
        let entry = package_info.package().read_entry(offset).await?;
        if entry.data().len() == 0 {
            fail!("Read entry ({}) is corrupted! It can't have zero length!", entry_id);
        }
        Ok(entry)
    }

    pub async fn get_slice(&self, archive_id: u64, offset: u64, limit: u32) -> Result<Vec<u8>> {
        if archive_id as u32 != self.archive_id {
            fail!("Bad archive ID (archive_id = {}, expected {})!", archive_id as u32, self.archive_id);
        }

        let package_id = (archive_id >> 32) as u32;
        let package_info = self.choose_package(package_id, false).await?;
        let mut file = tokio::fs::File::open(&**package_info.package().path()).await?;
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
        let path = Arc::new(package_id.full_path(self.db_root_path.as_ref(), "pack"));

        let package = Package::open(Arc::clone(&path), false, true).await
            .map_err(|err| error!("Failed to open or create archive \"{}\": {}", path.to_string_lossy(), err))?;

        if !self.finalized && version >= DEFAULT_PKG_VERSION {
            package.truncate(size).await?;
        }

        let pi = Arc::new(PackageInfo::with_data(
            package_id,
            package,
            idx,
            version
        ));

        Ok(pi)
    }

    async fn choose_package(&self, mc_seq_no: u32, force: bool) -> Result<Arc<PackageInfo>> {
        if self.package_type != PackageType::Blocks || !self.sliced_mode {
            return Ok(Arc::clone(&self.packages.read().await[0]));
        }

        if mc_seq_no < self.archive_id {
            fail!("mc_seq_no is too small");
        }

        let idx = (mc_seq_no - self.archive_id) / self.slice_size;
        {
            let mut write_guard = self.packages.write().await;
            let package_count = write_guard.len();
            if (idx as usize) < package_count {
                Ok(Arc::clone(&write_guard[idx as usize]))
            } else {
                if !force {
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
}
