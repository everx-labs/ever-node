use crate::archives::{package::Package, package_id::PackageId};

#[derive(Debug)]
pub struct PackageInfo {
    package_id: PackageId,
    package: Package,
    idx: u32,
    version: u32,
}

impl PackageInfo {
    pub const fn with_data(package_id: PackageId, package: Package, idx: u32, version: u32) -> Self {
        Self { package_id, package, idx, version }
    }

    #[allow(dead_code)]
    pub const fn package_id(&self) -> &PackageId {
        &self.package_id
    }

    pub const fn package(&self) -> &Package {
        &self.package
    }

    pub const fn idx(&self) -> u32 {
        self.idx
    }

    pub const fn version(&self) -> u32 {
        self.version
    }
}
