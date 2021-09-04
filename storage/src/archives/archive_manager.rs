use crate::{
    archives::{
        archive_slice::ArchiveSlice, file_maps::{FileDescription, FileMaps}, get_mc_seq_no,
        package_entry_id::{GetFileNameShort, PackageEntryId}, package_id::PackageId,
        ARCHIVE_SLICE_SIZE, KEY_ARCHIVE_PACKAGE_SIZE
    },
    types::BlockHandle
};
use std::{borrow::Borrow, hash::Hash, io::ErrorKind, path::PathBuf, sync::Arc};
use tokio::io::AsyncWriteExt;
use ton_api::ton::PublicKey;
use ton_block::BlockIdExt;
use ton_types::{error, fail, Result, UInt256};


pub struct ArchiveManager {
    db_root_path: Arc<PathBuf>,
    unapplied_dir: Arc<PathBuf>,
    file_maps: FileMaps,
}

impl ArchiveManager {
    pub async fn with_data(
        db_root_path: Arc<PathBuf>,
    ) -> Result<Self> {
        let file_maps = FileMaps::new(&db_root_path).await?;
        let unapplied_dir = Arc::new(db_root_path.join("archive").join("unapplied"));
        tokio::fs::create_dir_all(&*unapplied_dir).await?;

        Ok(Self {
            db_root_path,
            unapplied_dir,
            file_maps,
        })
    }

    pub const fn db_root_path(&self) -> &Arc<PathBuf> {
        &self.db_root_path
    }

    pub const fn unapplied_dir(&self) -> &Arc<PathBuf> {
        &self.unapplied_dir
    }

    pub async fn add_file<B, U256, PK>(&self, entry_id: &PackageEntryId<B, U256, PK>, data: Vec<u8>) -> Result<()>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<PublicKey> + Hash
    {
        log::debug!(target: "storage", "Saving unapplied file: {}", entry_id);

        if data.len() == 0 {
            fail!("Added file's ({}) data can't have zero length", entry_id);
        }

        let filename = self.unapplied_dir.join(entry_id.filename_short());
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&filename).await?;
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
        PK: Borrow<PublicKey> + Hash
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
        PK: Borrow<PublicKey> + Hash
    {
            
        if handle.is_archived() {
            let is_key = handle.is_key_block()?;
            let package_id = self.get_package_id(get_mc_seq_no(handle), is_key).await?;
            if let Some(ref fd) = self.get_file_desc(package_id, false).await? {
                return Ok(fd.archive_slice()
                    .get_file(Some(handle), entry_id).await?
                    .take_data());
            }
        }

        let _lock = match &entry_id {
            PackageEntryId::Block(_) => handle.block_file_lock().read().await,
            PackageEntryId::Proof(_) => handle.proof_file_lock().read().await,
            PackageEntryId::ProofLink(_) => handle.proof_file_lock().read().await,
            _ => fail!("Unsupported package entry")
        };
        self.read_temp_file(entry_id).await.map(|(_filename, data)| data)

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
            Some(
                self.move_file_to_archives(
                    handle, 
                    &PackageEntryId::<&BlockIdExt, &UInt256, &PublicKey>::Proof(handle.id())
                ).await?
            )
        } else if prooflink_inited {
            let _lock = handle.proof_file_lock().write().await;
            Some(
                self.move_file_to_archives(
                    handle, 
                    &PackageEntryId::<&BlockIdExt, &UInt256, &PublicKey>::ProofLink(handle.id())
                ).await?
            )
        } else {
            None
        };
        let block_filename = if data_inited {
            let _lock = handle.block_file_lock().write().await;
            Some(
                self.move_file_to_archives(
                    handle, 
                    &PackageEntryId::<&BlockIdExt, &UInt256, &PublicKey>::Block(handle.id())
                ).await?
            )
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
        if let Some(fd) = self.file_maps.files().get_closest(mc_seq_no).await {
            fd.archive_slice().get_archive_id(mc_seq_no).await
        } else if let Some(key_fd) = self.file_maps.key_files().get_closest(mc_seq_no).await { 
            key_fd.archive_slice().get_archive_id(mc_seq_no).await
        } else {
            None
        }
    }

    pub async fn get_archive_slice(&self, archive_id: u64, offset: u64, limit: u32) -> Result<Vec<u8>> {
        let fd = match self.get_file_desc(PackageId::for_block(archive_id as u32), false).await? {
            Some(file_desc) => file_desc,
            None => {
                match self.get_file_desc(PackageId::for_key_block(archive_id as u32 / KEY_ARCHIVE_PACKAGE_SIZE), false).await? {
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

    async fn move_file_to_archives<B, U256, PK>(&self, handle: &BlockHandle, entry_id: &PackageEntryId<B, U256, PK>) -> Result<PathBuf>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<PublicKey> + Hash
    {
        log::debug!(target: "storage", "Moving entry to archive: {}", entry_id.filename_short());
        let (filename, data) = {
            self.read_temp_file(entry_id).await?
        };

        let data = self.move_file_to_archive(data, handle, entry_id, false).await?; 

        if handle.is_key_block()? {
            self.move_file_to_archive(data, handle, entry_id, true).await?; 
        }

        Ok(filename)
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
        PK: Borrow<PublicKey> + Hash
    {
        let mc_seq_no = get_mc_seq_no(handle);
        let key_block = handle.is_key_block()?;

        let package_id = self.get_package_id_force(mc_seq_no, key_archive, key_block).await;
        log::debug!(target: "storage", "PackageId for ({},{},{}) (mc_seq_no = {}, key block = {:?}) is {:?}, path: {:?}",
            handle.id().shard().workchain_id(),
            handle.id().shard().shard_prefix_as_str_with_tag(),
            handle.id().seq_no(),
            mc_seq_no,
            key_block,
            package_id,
            package_id.full_path(self.db_root_path.as_ref(), "pack"),
        );

        let fd = self.get_file_desc(package_id, true).await?
            .ok_or_else(|| error!("Expected some value"))?;

        fd.archive_slice().add_file(Some(handle), entry_id, data).await
    }

    async fn read_temp_file<B, U256, PK>(&self, entry_id: &PackageEntryId<B, U256, PK>) -> Result<(PathBuf, Vec<u8>)>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<PublicKey> + Hash
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
        if data.len() == 0 {
            fail!(
                "Read temp file {} ({}) is corrupted! It cannot have zero length!", 
                temp_filename.as_os_str().to_str().unwrap_or("bad path"), 
                entry_id
            )
        }

        Ok((temp_filename, data))
    }

    async fn get_file_desc(&self, id: PackageId, force: bool) -> Result<Option<Arc<FileDescription>>> {
        // TODO: Rewrite logics in order to handle multithreaded adding of packages
        if let Some(fd) = self.file_maps.get(id.package_type()).get(id.id()).await {
            if fd.deleted() {
                return Ok(None);
            }
            return Ok(Some(fd));
        }

        if force {
            Ok(Some(self.add_file_desc(id).await?))
        } else {
            Ok(None)
        }
    }

    async fn add_file_desc(&self, id: PackageId) -> Result<Arc<FileDescription>> {
        // TODO: Rewrite logics in order to handle multithreaded adding of packages
        let file_map = self.file_maps.get(id.package_type());
        assert!(file_map.get(id.id()).await.is_none());

        let dir = self.db_root_path.join(id.path());
        tokio::fs::create_dir_all(&dir).await?;

        let archive_slice = Arc::new(
            ArchiveSlice::with_data(
                Arc::clone(&self.db_root_path),
                id.id(),
                id.package_type(),
                false,
            ).await?
        );

        let fd = Arc::new(FileDescription::with_data(
            id.clone(),
            archive_slice,
            false
        ));

        file_map.put(id.id(), Arc::clone(&fd)).await?;

        Ok(fd)
    }

    async fn get_package_id(&self, seq_no: u32, is_key: bool) -> Result<PackageId> {
        if is_key {
            Ok(PackageId::for_key_block(seq_no / KEY_ARCHIVE_PACKAGE_SIZE))
        } else {
            let id = self.file_maps.files().get_closest(seq_no).await
                .ok_or_else(|| {
                    log::error!(target: "storage", "Package not found for seq_no: {}", seq_no);
                    error!("Package not found for seq_no: {}", seq_no)
                })?
                .id()
                .clone();

            Ok(id)
        }
    }

    async fn get_package_id_force(&self, mc_seq_no: u32, key_archive: bool, key_block: bool) -> PackageId {
        if key_block {
            if key_archive {
                PackageId::for_key_block(mc_seq_no / KEY_ARCHIVE_PACKAGE_SIZE)
            } else {
                PackageId::for_block(mc_seq_no)
            }
        } else {
            let mut package_id = PackageId::for_block(mc_seq_no - (mc_seq_no % ARCHIVE_SLICE_SIZE as u32));
            if let Some(fd) = self.file_maps.files().get_closest(mc_seq_no).await {
                let found_package_id = fd.id();
                if package_id < *found_package_id {
                    package_id = found_package_id.clone();
                }
            }
            package_id
        }
    }
}
