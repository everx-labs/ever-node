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

use crate::traits::Serializable;
use std::io::{Read, Write};
use ton_types::{ByteOrderRead, fail, Result};

pub(crate) const PKG_ENTRY_HEADER_SIZE: usize = 8;
const PKG_ENTRY_HEADER_MAGIC: u16 = 0x1E8B;

#[derive(Debug)]
pub struct PackageEntryHeader {
    filename_size: u16,
    data_size: u32,
}

impl PackageEntryHeader {
    pub const fn with_data(filename_size: u16, data_size: u32) -> Self {
        Self { filename_size, data_size }
    }

    pub const fn calc_entry_size(&self) -> u64 {
        PKG_ENTRY_HEADER_SIZE as u64
            + self.filename_size as u64
            + self.data_size as u64
    }
}

impl Serializable for PackageEntryHeader {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&PKG_ENTRY_HEADER_MAGIC.to_le_bytes())?;
        writer.write_all(&self.filename_size.to_le_bytes())?;
        writer.write_all(&self.data_size.to_le_bytes())?;

        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self> where Self: Sized {
        let magic = reader.read_le_u16()?;

        if magic != PKG_ENTRY_HEADER_MAGIC {
            fail!("Bad entry magic: 0x{:X}", magic)
        }

        let filename_size = reader.read_le_u16()?;
        let data_size = reader.read_le_u32()?;

        Ok(Self::with_data(filename_size, data_size))
    }
}

pub struct PackageEntry {
    filename: String,
    data: Vec<u8>,
}

impl PackageEntry {
    pub const fn with_data(filename: String, data: Vec<u8>) -> Self {
        Self { filename, data }
    }

    pub(super) async fn read_from<R: tokio::io::AsyncReadExt + Unpin>(
        reader: &mut R
    ) -> Result<Option<Self>> {
        let mut buf = [0; PKG_ENTRY_HEADER_SIZE];
        match reader.read_exact(&mut buf).await {
            Ok(count) => assert_eq!(count, buf.len()),
            Err(error) => return if error.kind() == tokio::io::ErrorKind::UnexpectedEof {
                Ok(None)
            } else {
                Err(error.into())
            }
        }
        let entry_header = PackageEntryHeader::from_slice(&buf)?;

        let mut buf = vec![0; entry_header.filename_size as usize];
        reader.read_exact(&mut buf).await?;
        let filename = String::from_utf8(buf)?;

        log::trace!(target: "storage", "Reading package entry: {}, size: {}", filename, entry_header.data_size);

        let mut data = vec![0; entry_header.data_size as usize];
        reader.read_exact(&mut data).await?;

        Ok(Some(Self::with_data(filename, data)))
    }

    pub(super) async fn write_to<W: tokio::io::AsyncWriteExt + Unpin>(
        &self, 
        writer: &mut W
    ) -> Result<u64> {
        let entry_header = PackageEntryHeader::with_data(
            self.filename.as_bytes().len() as u16,
            self.data.len() as u32
        );

        writer.write_all(&entry_header.to_vec()?).await?;
        writer.write_all(self.filename.as_bytes()).await?;
        writer.write_all(&self.data).await?;
        writer.flush().await?;

        Ok(entry_header.calc_entry_size())
    }

    pub const fn filename(&self) -> &String {
        &self.filename
    }

    pub const fn data(&self) -> &Vec<u8> {
        &self.data
    }

    pub fn take_data(self) -> Vec<u8> {
        self.data
    }
}
