// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Object store that represents the HDFS File System.

use std::collections::{BTreeSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::{stream::BoxStream, StreamExt};
use hdfs::err::HdfsErr::FileNotFound;
use hdfs::hdfs::{get_hdfs_by_full_path, FileStatus, HdfsErr, HdfsFs};
use hdfs::walkdir::HdfsWalkDir;
use object_store::path;
use object_store::path::Path;
use object_store::{Error, GetResult, ListResult, ObjectMeta, ObjectStore, Result};

/// scheme for HDFS File System
pub static HDFS_SCHEME: &str = "hdfs";
/// scheme for HDFS Federation File System
pub static VIEWFS_SCHEME: &str = "viewfs";

#[derive(Debug)]
/// Hadoop File System as Object Store.
pub struct HadoopFileSystem {
    hdfs: Arc<HdfsFs>,
}

impl Default for HadoopFileSystem {
    fn default() -> Self {
        Self {
            hdfs: get_hdfs_by_full_path("default").expect("Fail to get default HdfsFs"),
        }
    }
}

impl HadoopFileSystem {
    /// Get HDFS from the full path, like hdfs://localhost:8020/xxx/xxx
    pub fn new(full_path: &str) -> Option<Self> {
        get_hdfs_by_full_path(full_path)
            .map(|hdfs| Some(Self { hdfs }))
            .unwrap_or(None)
    }

    /// Return filesystem path of the given location
    fn path_to_filesystem(location: &Path) -> String {
        format!("/{}", location.as_ref())
    }

    pub fn get_path_root(&self) -> String {
        self.hdfs.url().to_owned()
    }

    pub fn get_path(&self, full_path: &str) -> Path {
        get_path(full_path, self.hdfs.url())
    }

    pub fn get_hdfs_host(&self) -> String {
        let hdfs_url = self.hdfs.url();
        if hdfs_url.starts_with(HDFS_SCHEME) {
            hdfs_url[7..].to_owned()
        } else if hdfs_url.starts_with(VIEWFS_SCHEME) {
            hdfs_url[9..].to_owned()
        } else {
            "".to_owned()
        }
    }
}

impl Display for HadoopFileSystem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HadoopFileSystem")
    }
}

#[async_trait]
impl ObjectStore for HadoopFileSystem {
    // Current implementation is very simple due to missing configs,
    // like whether able to overwrite, whether able to create parent directories, etc
    async fn put(&self, location: &Path, bytes: Bytes) -> object_store::Result<()> {
        let hdfs = self.hdfs.clone();
        let location = HadoopFileSystem::path_to_filesystem(location);

        maybe_spawn_blocking(move || {
            let file = match hdfs.create(&location) {
                Ok(f) => f,
                Err(e) => {
                    return Err(to_error(e));
                }
            };

            file.write(bytes.as_ref()).map_err(to_error)?;

            Ok(())
        })
        .await
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        let hdfs = self.hdfs.clone();
        let location = HadoopFileSystem::path_to_filesystem(location);

        let blob: Bytes = maybe_spawn_blocking(move || {
            let file = hdfs.open(&location).map_err(to_error)?;

            let file_status = file.get_file_status().map_err(to_error)?;

            let to_read = file_status.len();
            let mut buf = vec![0; to_read];
            let read = file.read(buf.as_mut_slice()).map_err(to_error)?;
            assert_eq!(
                to_read as i32, read,
                "Read path {} with expected size {} and actual size {}",
                &location, to_read, read
            );

            Ok(buf.into())
        })
        .await?;

        Ok(GetResult::Stream(
            futures::stream::once(async move { Ok(blob) }).boxed(),
        ))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<Bytes> {
        let hdfs = self.hdfs.clone();
        let location = HadoopFileSystem::path_to_filesystem(location);

        maybe_spawn_blocking(move || {
            let file = hdfs.open(&location).map_err(to_error)?;

            let to_read = range.end - range.start;
            let mut buf = vec![0; to_read];
            let read = file
                .read_with_pos(range.start as i64, buf.as_mut_slice())
                .map_err(to_error)?;
            assert_eq!(
                to_read as i32, read,
                "Read path {} from {} with expected size {} and actual size {}",
                &location, range.start, to_read, read
            );

            Ok(buf.into())
        })
        .await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let hdfs = self.hdfs.clone();
        let hdfs_root = self.hdfs.url().to_owned();
        let location = HadoopFileSystem::path_to_filesystem(location);

        maybe_spawn_blocking(move || {
            let file_status = hdfs.get_file_status(&location).map_err(to_error)?;
            Ok(convert_metadata(file_status, &hdfs_root))
        })
        .await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let hdfs = self.hdfs.clone();
        let location = HadoopFileSystem::path_to_filesystem(location);

        maybe_spawn_blocking(move || {
            hdfs.delete(&location, false).map_err(to_error)?;

            Ok(())
        })
        .await
    }

    /// List all of the leaf files under the prefix path.
    /// It will recursively search leaf files whose depth is larger than 1
    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<BoxStream<'_, object_store::Result<ObjectMeta>>> {
        let prefix = prefix.expect("Prefix for hdfs should not be None");
        let hdfs = self.hdfs.clone();
        let hdfs_root = self.hdfs.url().to_owned();
        let walkdir =
            HdfsWalkDir::new_with_hdfs(HadoopFileSystem::path_to_filesystem(prefix), hdfs)
                .min_depth(1);

        let s =
            walkdir.into_iter().flat_map(move |result_dir_entry| {
                match convert_walkdir_result(result_dir_entry) {
                    Err(e) => Some(Err(e)),
                    Ok(None) => None,
                    Ok(entry @ Some(_)) => entry
                        .filter(|dir_entry| dir_entry.is_file())
                        .map(|entry| Ok(convert_metadata(entry, &hdfs_root))),
                }
            });

        // If no tokio context, return iterator directly as no
        // need to perform chunked spawn_blocking reads
        if tokio::runtime::Handle::try_current().is_err() {
            return Ok(futures::stream::iter(s).boxed());
        }

        // Otherwise list in batches of CHUNK_SIZE
        const CHUNK_SIZE: usize = 1024;

        let buffer = VecDeque::with_capacity(CHUNK_SIZE);
        let stream = futures::stream::try_unfold((s, buffer), |(mut s, mut buffer)| async move {
            if buffer.is_empty() {
                (s, buffer) = tokio::task::spawn_blocking(move || {
                    for _ in 0..CHUNK_SIZE {
                        match s.next() {
                            Some(r) => buffer.push_back(r),
                            None => break,
                        }
                    }
                    (s, buffer)
                })
                .await?;
            }

            match buffer.pop_front() {
                Some(Err(e)) => Err(e),
                Some(Ok(meta)) => Ok(Some((meta, (s, buffer)))),
                None => Ok(None),
            }
        });

        Ok(stream.boxed())
    }

    /// List files and directories directly under the prefix path.
    /// It will not recursively search leaf files whose depth is larger than 1
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix = prefix.expect("Prefix for hdfs should not be None");
        let hdfs = self.hdfs.clone();
        let hdfs_root = self.hdfs.url().to_owned();
        let walkdir =
            HdfsWalkDir::new_with_hdfs(HadoopFileSystem::path_to_filesystem(prefix), hdfs)
                .min_depth(1)
                .max_depth(1);

        let prefix = prefix.clone();
        maybe_spawn_blocking(move || {
            let mut common_prefixes = BTreeSet::new();
            let mut objects = Vec::new();

            for entry_res in walkdir.into_iter().map(convert_walkdir_result) {
                if let Some(entry) = entry_res? {
                    let is_directory = entry.is_directory();
                    let entry_location = get_path(entry.name(), &hdfs_root);

                    let mut parts = match entry_location.prefix_match(&prefix) {
                        Some(parts) => parts,
                        None => continue,
                    };

                    let common_prefix = match parts.next() {
                        Some(p) => p,
                        None => continue,
                    };

                    drop(parts);

                    if is_directory {
                        common_prefixes.insert(prefix.child(common_prefix));
                    } else {
                        objects.push(convert_metadata(entry, &hdfs_root));
                    }
                }
            }

            Ok(ListResult {
                next_token: None,
                common_prefixes: common_prefixes.into_iter().collect(),
                objects,
            })
        })
        .await
    }

    /// Copy an object from one path to another.
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let hdfs = self.hdfs.clone();
        let from = HadoopFileSystem::path_to_filesystem(from);
        let to = HadoopFileSystem::path_to_filesystem(to);

        maybe_spawn_blocking(move || {
            // We need to make sure the source exist
            if !hdfs.exist(&from) {
                return Err(Error::NotFound {
                    path: from.clone(),
                    source: Box::new(HdfsErr::FileNotFound(from)),
                });
            }
            // Delete destination if exists
            if hdfs.exist(&to) {
                hdfs.delete(&to, false).map_err(to_error)?;
            }

            hdfs::util::HdfsUtil::copy(hdfs.as_ref(), &from, hdfs.as_ref(), &to)
                .map_err(to_error)?;

            Ok(())
        })
        .await
    }

    /// It's only allowed for the same HDFS
    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let hdfs = self.hdfs.clone();
        let from = HadoopFileSystem::path_to_filesystem(from);
        let to = HadoopFileSystem::path_to_filesystem(to);

        maybe_spawn_blocking(move || {
            hdfs.rename(&from, &to).map_err(to_error)?;

            Ok(())
        })
        .await
    }

    /// Copy an object from one path to another, only if destination is empty.
    /// Will return an error if the destination already has an object.
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let hdfs = self.hdfs.clone();
        let from = HadoopFileSystem::path_to_filesystem(from);
        let to = HadoopFileSystem::path_to_filesystem(to);

        maybe_spawn_blocking(move || {
            if hdfs.exist(&to) {
                return Err(Error::AlreadyExists {
                    path: from,
                    source: Box::new(HdfsErr::FileAlreadyExists(to)),
                });
            }

            hdfs::util::HdfsUtil::copy(hdfs.as_ref(), &from, hdfs.as_ref(), &to)
                .map_err(to_error)?;

            Ok(())
        })
        .await
    }
}

/// Create Path without prefix
pub fn get_path(full_path: &str, prefix: &str) -> Path {
    let partial_path = &full_path[prefix.len()..];
    Path::from(partial_path)
}

/// Convert HDFS file status to ObjectMeta
pub fn convert_metadata(file: FileStatus, prefix: &str) -> ObjectMeta {
    ObjectMeta {
        location: get_path(file.name(), prefix),
        last_modified: DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(file.last_modified(), 0),
            Utc,
        ),
        size: file.len(),
    }
}

/// Convert walkdir results and converts not-found errors into `None`.
fn convert_walkdir_result(
    res: std::result::Result<FileStatus, HdfsErr>,
) -> Result<Option<FileStatus>> {
    match res {
        Ok(entry) => Ok(Some(entry)),
        Err(walkdir_err) => match walkdir_err {
            FileNotFound(_) => Ok(None),
            _ => Err(to_error(HdfsErr::Generic(
                "Fail to walk hdfs directory".to_owned(),
            ))),
        },
    }
}

/// Takes a function and spawns it to a tokio blocking pool if available
pub async fn maybe_spawn_blocking<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    #[cfg(feature = "try_spawn_blocking")]
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => runtime.spawn_blocking(f).await?,
        Err(_) => f(),
    }

    #[cfg(not(feature = "try_spawn_blocking"))]
    f()
}

fn to_error(err: HdfsErr) -> Error {
    match err {
        HdfsErr::FileNotFound(path) => Error::NotFound {
            path: path.clone(),
            source: Box::new(HdfsErr::FileNotFound(path)),
        },
        HdfsErr::FileAlreadyExists(path) => Error::AlreadyExists {
            path: path.clone(),
            source: Box::new(HdfsErr::FileAlreadyExists(path)),
        },
        HdfsErr::InvalidUrl(path) => Error::InvalidPath {
            source: path::Error::InvalidPath {
                path: PathBuf::from(path),
            },
        },
        HdfsErr::CannotConnectToNameNode(namenode_uri) => Error::Generic {
            store: "HadoopFileSystem",
            source: Box::new(HdfsErr::CannotConnectToNameNode(namenode_uri)),
        },
        HdfsErr::Generic(err_str) => Error::Generic {
            store: "HadoopFileSystem",
            source: Box::new(HdfsErr::Generic(err_str)),
        },
    }
}
