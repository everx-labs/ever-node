
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct PackageEntryMeta {
    entry_size: u64,
    version: u32,
}

impl PackageEntryMeta {
    pub const fn with_data(entry_size: u64, version: u32) -> Self {
        Self { entry_size, version }
    }

    pub const fn entry_size(&self) -> u64 {
        self.entry_size
    }

    pub const fn version(&self) -> u32 {
        self.version
    }
}
