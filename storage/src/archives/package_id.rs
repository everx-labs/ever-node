use crate::archives::archive_manager::KEY_ARCHIVE_SIZE;
use std::{cmp::Ordering, ffi::OsString, path::{Path, PathBuf}};
use ton_block::UnixTime32;


#[derive(Debug, Copy, Clone, Hash, PartialOrd, Ord, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PackageType {
    Blocks,
    KeyBlocks,
    Temp
}

#[derive(Debug, Clone, Hash, Ord, Eq, serde::Serialize, serde::Deserialize)]
pub struct PackageId {
    id: u32,
    package_type: PackageType,
}

impl PackageId {
    pub const fn with_values(id: u32, package_type: PackageType) -> Self {
        Self { id, package_type }
    }

    #[allow(dead_code)]
    pub const fn empty(package_type: PackageType) -> Self {
        Self::with_values(u32::max_value(), package_type)
    }

    pub const fn for_block(mc_seq_no: u32) -> Self {
        Self::with_values(mc_seq_no, PackageType::Blocks)
    }

    #[allow(dead_code)]
    pub const fn for_key_block(mc_seq_no: u32) -> Self {
        Self::with_values(mc_seq_no % KEY_ARCHIVE_SIZE as u32, PackageType::KeyBlocks)
    }

    pub const fn for_temp(ts: &UnixTime32) -> Self {
        Self::with_values(ts.0 - ts.0 % 3_600, PackageType::Temp)
    }

    #[allow(dead_code)]
    pub fn for_temp_now() -> Self {
        Self::for_temp(&UnixTime32::now())
    }

    pub const fn id(&self) -> u32 {
        self.id
    }

    pub const fn package_type(&self) -> PackageType {
        self.package_type
    }

    #[allow(dead_code)]
    pub const fn is_empty(&self) -> bool {
        self.id == u32::max_value()
    }

    pub fn path(&self) -> PathBuf {
        match self.package_type {
            PackageType::Temp => "files/packages/".into(),
            PackageType::KeyBlocks => format!("archive/packages/key{id:03}/", id = self.id / 1_000_000).into(),
            PackageType::Blocks => format!("archive/packages/arch{id:04}/", id = self.id / 100_000).into(),
        }
    }

    pub fn name(&self) -> PathBuf {
        match self.package_type {
            PackageType::Temp => format!("temp.archive.{id}", id = self.id).into(),
            PackageType::KeyBlocks => format!("key.archive.{id:06}", id = self.id).into(),
            PackageType::Blocks => format!("archive.{id:05}", id = self.id).into(),
        }
    }

    pub fn full_path(&self, db_root: impl AsRef<Path>, extension: &str) -> PathBuf {
        let mut gen_extension = OsString::new();
        if let Some(ext) = self.name().extension() {
            gen_extension.push(ext);
            gen_extension.push(".");
        }
        gen_extension.push(extension);

        db_root.as_ref()
            .join(self.path())
            .join(self.name())
            .with_extension(gen_extension)
    }
}

impl PartialEq for PackageId {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl PartialOrd for PackageId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.id().partial_cmp(&other.id())
    }
}
