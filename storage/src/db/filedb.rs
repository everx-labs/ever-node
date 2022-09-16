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
    db::traits::DbKey,
    error::StorageError
};
use std::{io::{ErrorKind, SeekFrom, Write, Read, Seek}, path::{Path, PathBuf}};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use ton_types::{error, Result};

#[derive(Debug)]
pub struct FileDb {
    path: PathBuf,
}

static PATH_CHUNK_MAX_LEN: usize = 4;
static PATH_MAX_DEPTH: usize = 2;

impl FileDb {
    pub fn with_path<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub async fn destroy(&mut self) -> Result<bool> {
        match tokio::fs::metadata(&self.path).await {
            Ok(meta) if meta.is_dir() => tokio::fs::remove_dir_all(&self.path).await?,
            _ => ()
        }
        Ok(true)
    }

    pub fn get_write_object(&self, key: &(dyn DbKey + Send + Sync)) -> Result<impl Write> {
        let path = self.make_path(key.key());
        let dir = path.parent().ok_or_else(|| error!("Unable to get parent path"))?;
        std::fs::create_dir_all(dir)?;
        let file = std::fs::File::create(path)?;
        Ok(file)
    }

    pub async fn get_read_object(&self, key: &(dyn DbKey + Send + Sync)) -> Result<impl Read + Seek> {
        let path = self.make_path(key.key());
        let file = std::fs::File::open(path)?;
        Ok(file)
    }

    pub async fn write_whole_file(&self, key: &(dyn DbKey + Send + Sync), data: &[u8]) -> Result<()> {
        let path = self.make_path(key.key());
        let dir = path.parent()
            .ok_or_else(|| error!("Unable to get parent path"))?;
        tokio::fs::create_dir_all(dir).await?;
        tokio::fs::write(path, data).await?;

        Ok(())
    }

    pub async fn read_whole_file(&self, key: &(dyn DbKey + Send + Sync)) -> Result<Vec<u8>> {
       self.read_file_part(key, 0, self.get_file_size(key).await?).await
    }

    pub async fn read_file_part(&self, key: &(dyn DbKey + Send + Sync), offset: u64, size: u64) -> Result<Vec<u8>> {
        let path = self.make_path(key.key());
        let mut file = tokio::fs::File::open(path).await
            .map_err(|err| Self::transform_io_error(err, key.key()))?;
        file.seek(SeekFrom::Start(offset)).await?;
        let mut result = vec![0; size as usize];
        file.read_exact(&mut result).await
            .map_err(|err| Self::transform_io_error(err, key.key()))?;

        Ok(result)
    }

    pub async fn get_file_size(&self, key: &(dyn DbKey + Send + Sync)) -> Result<u64> {
        let path = self.make_path(key.key());
        let metadata = tokio::fs::metadata(path).await
            .map_err(|err| Self::transform_io_error(err, key.key()))?;

        Ok(metadata.len())
    }

    pub async fn contains(&self, key: &(dyn DbKey + Send + Sync)) -> Result<bool> {
        let path = self.make_path(key.key());
        Ok(path.is_file() && path.exists())
    }

    pub fn for_each_key(&self, predicate: &mut dyn FnMut(&[u8]) -> Result<bool>) -> Result<bool> {
        self.for_each_key_worker(&self.path, &[], 1, predicate)
    }

    pub async fn delete_file(&self, key: &(dyn DbKey + Send + Sync)) -> Result<()> {
        let path = self.make_path(key.key());
        if let Err(err) = tokio::fs::remove_file(&path).await {
            if err.kind() != ErrorKind::NotFound {
                return Err(err.into());
            }
        }

        // Cleanup upper-level empty directories
        let mut dir = path.as_path();
        loop {
            dir = dir.parent()
                .ok_or_else(|| error!("Unable to get parent path"))?;
            if self.path().starts_with(dir) || !Self::is_dir_empty(&dir).await {
                break;
            } else {
                let _ = tokio::fs::remove_dir(&dir).await;// If can't remove empty dir, do nothing.
            }
        }

        Ok(())
    }

    pub(crate) fn make_path(&self, key: &[u8]) -> PathBuf {
        let mut key_str = hex::encode(key);
        let mut result = self.path.clone();
        let mut depth = 1;
        while depth < PATH_MAX_DEPTH && !key_str.is_empty() {
            let remaining = key_str.split_off(std::cmp::min(key_str.len(), PATH_CHUNK_MAX_LEN));
            result = result.join(key_str);
            key_str = remaining;
            depth += 1;
        }
        if !key_str.is_empty() {
            return result.join(key_str);
        }
        result
    }

    fn transform_io_error(err: std::io::Error, key: &[u8]) -> failure::Error {
        match err.kind() {
            ErrorKind::NotFound => StorageError::KeyNotFound("&[u8]", hex::encode(key)).into(),
            ErrorKind::UnexpectedEof => StorageError::OutOfRange.into(),
            _ => err.into()
        }
    }

    async fn is_dir_empty<P: AsRef<Path>>(path: P) -> bool {
        if let Ok(mut read_dir) = tokio::fs::read_dir(path).await {
            if let Ok(val) = read_dir.next_entry().await {
                return val.is_none();
            }
        }
        false
    }

    fn for_each_key_worker<P: AsRef<Path>>(
        &self,
        path: P,
        key: &[u8],
        depth: usize,
        predicate: &mut dyn FnMut(&[u8]) -> Result<bool>
    ) -> Result<bool> {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let file_name = file_name.to_str().ok_or_else(|| error!("Can't decode filename"))?;
            let mut key = key.to_vec();
            key.append(&mut hex::decode(&file_name)?);
            let result = if depth == PATH_MAX_DEPTH {
                predicate(&key)?
            } else {
                self.for_each_key_worker(entry.path(), &key, depth + 1, predicate)?
            };
            if !result {
                return Ok(false)
            }
        }
        Ok(true)
    }
}