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

use crate::{StorageAlloc, archives::{package::Package, package_id::PackageId}};
#[cfg(feature = "telemetry")]
use crate::StorageTelemetry;
use adnl::{declare_counted, common::{CountedObject, Counter}};
use std::sync::Arc;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;
use ton_types::Result;

//#[derive(Debug)]
declare_counted!(
    pub struct PackageInfo {
        package_id: PackageId,
        package: Package,
        idx: u32,
        version: u32
    }
);

impl PackageInfo {

    pub fn with_data(
        package_id: PackageId, 
        package: Package, 
        idx: u32, 
        version: u32,
        #[cfg(feature = "telemetry")]
        telemetry: &Arc<StorageTelemetry>,
        allocated: &Arc<StorageAlloc>     
    ) -> Self {
        let ret = Self { 
            package_id, 
            package, 
            idx, 
            version,
            counter: allocated.packages.clone().into() 
        };
        #[cfg(feature = "telemetry")]
        telemetry.packages.update(
            allocated.packages.load(Ordering::Relaxed)
        );
        ret
    }

    #[allow(dead_code)]
    pub const fn package_id(&self) -> &PackageId {
        &self.package_id
    }

    pub const fn package(&self) -> &Package {
        &self.package
    }

    pub fn package_mut(&mut self) -> &mut Package {
        &mut self.package
    }

    pub const fn idx(&self) -> u32 {
        self.idx
    }

    pub const fn version(&self) -> u32 {
        self.version
    }

    pub async fn destroy(&mut self) -> Result<()> {
        self.package.destroy().await
    }
}
