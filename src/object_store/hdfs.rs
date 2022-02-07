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
use std::fmt::Debug;
use std::io::Read;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{NaiveDateTime, Utc};
use futures::AsyncRead;
use futures::{stream, StreamExt};
use hdfs::hdfs::{FileStatus, HdfsErr, HdfsFile, HdfsFs};

use datafusion::datasource::object_store::{
    FileMeta, FileMetaStream, ListEntryStream, ObjectReader, ObjectReaderStream, ObjectStore,
    SizedFile,
};
use datafusion::datasource::PartitionedFile;
use datafusion::error::{DataFusionError, Result};

/// scheme for HDFS File System
pub static HDFS_SCHEME: &str = "hdfs";
/// scheme for HDFS Federation File System
pub static VIEWFS_SCHEME: &str = "viewfs";

/// Hadoop File.
#[derive(Clone, Debug)]
pub struct HadoopFile {
    inner: HdfsFile,
}

#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for HadoopFile {}

unsafe impl Sync for HadoopFile {}

impl Read for HadoopFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner
            .read(buf)
            .map(|read_len| read_len as usize)
            .map_err(to_error)
    }
}

/// Hadoop File System as Object Store.
#[derive(Clone, Debug)]
pub struct HadoopFileSystem {
    inner: HdfsFs,
}

impl HadoopFileSystem {
    /// Get HdfsFs configured by default configuration files
    pub fn new() -> Result<Self> {
        HdfsFs::default()
            .map(|fs| HadoopFileSystem { inner: fs })
            .map_err(|e| DataFusionError::IoError(to_error(e)))
    }

    /// Get HdfsFs by uri, only one global instance will be created for the same scheme
    pub fn new_with_uri(uri: &str) -> Result<Self> {
        HdfsFs::new(uri)
            .map(|fs| HadoopFileSystem { inner: fs })
            .map_err(|e| DataFusionError::IoError(to_error(e)))
    }

    /// Wrap the known HdfsFs. Only used for testing
    pub fn wrap(inner: HdfsFs) -> Self {
        HadoopFileSystem { inner }
    }

    /// Open a HdfsFile with specified path
    pub fn open(&self, path: &str) -> Result<HadoopFile> {
        self.inner
            .open(path)
            .map(|file| HadoopFile { inner: file })
            .map_err(|e| DataFusionError::IoError(to_error(e)))
    }

    /// Find out all leaf files directly under a directory
    fn find_leaf_files_in_dir(&self, path: String) -> Result<Vec<FileMeta>> {
        let mut result: Vec<FileMeta> = Vec::new();

        let children = self.inner.list_status(path.as_str()).map_err(to_error)?;
        let mut files = Vec::new();
        let mut dirs = Vec::new();

        for c in children {
            let c_path = c.name();
            match c.is_file() {
                true => files.push(get_meta(c_path.to_owned(), c)),
                false => dirs.push(c_path.to_owned()),
            }
        }

        result.append(&mut files);

        for dir in dirs {
            let mut dir_files = self.find_leaf_files_in_dir(dir.to_owned())?;
            result.append(&mut dir_files);
        }

        Ok(result)
    }
}

#[async_trait]
impl ObjectStore for HadoopFileSystem {
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream> {
        let files = self.find_leaf_files_in_dir(prefix.to_string())?;
        get_files_in_dir(files).await
    }

    async fn list_dir(&self, _prefix: &str, _delimiter: Option<String>) -> Result<ListEntryStream> {
        todo!()
    }

    fn file_reader(&self, file: SizedFile) -> Result<Arc<dyn ObjectReader>> {
        Ok(Arc::new(HadoopFileReader::new(
            Arc::new(self.clone()),
            file,
        )?))
    }
}

async fn get_files_in_dir(files: Vec<FileMeta>) -> Result<FileMetaStream> {
    Ok(Box::pin(stream::iter(files).map(Ok)))
}

fn get_meta(path: String, file_status: FileStatus) -> FileMeta {
    FileMeta {
        sized_file: SizedFile {
            path,
            size: file_status.len() as u64,
        },
        last_modified: Some(chrono::DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(file_status.last_modified(), 0),
            Utc,
        )),
    }
}

/// Create a stream of `ObjectReader` by converting each file in the `files` vector
/// into instances of `HadoopFileReader`
pub fn hadoop_object_reader_stream(
    fs: Arc<HadoopFileSystem>,
    files: Vec<String>,
) -> ObjectReaderStream {
    Box::pin(futures::stream::iter(files).map(move |f| Ok(hadoop_object_reader(fs.clone(), f))))
}

/// Helper method to convert a file location to a `LocalFileReader`
pub fn hadoop_object_reader(fs: Arc<HadoopFileSystem>, file: String) -> Arc<dyn ObjectReader> {
    fs.file_reader(
        hadoop_unpartitioned_file(fs.clone(), file)
            .file_meta
            .sized_file,
    )
    .expect("File not found")
}

/// Helper method to fetch the file size and date at given path and create a `FileMeta`
pub fn hadoop_unpartitioned_file(fs: Arc<HadoopFileSystem>, file: String) -> PartitionedFile {
    let file_status = fs.inner.get_file_status(&file).ok().unwrap();
    PartitionedFile {
        file_meta: get_meta(file, file_status),
        partition_values: vec![],
    }
}

struct HadoopFileReader {
    fs: Arc<HadoopFileSystem>,
    file: SizedFile,
}

impl HadoopFileReader {
    fn new(fs: Arc<HadoopFileSystem>, file: SizedFile) -> Result<Self> {
        Ok(Self { fs, file })
    }
}

#[async_trait]
impl ObjectReader for HadoopFileReader {
    async fn chunk_reader(&self, _start: u64, _length: usize) -> Result<Box<dyn AsyncRead>> {
        todo!("implement once async file readers are available (arrow-rs#78, arrow-rs#111)")
    }

    fn sync_chunk_reader(&self, start: u64, length: usize) -> Result<Box<dyn Read + Send + Sync>> {
        let file = self.fs.open(&self.file.path)?;
        file.inner.seek(start);
        Ok(Box::new(file.take(length as u64)))
    }

    fn length(&self) -> u64 {
        self.file.size
    }
}

fn to_error(err: HdfsErr) -> std::io::Error {
    match err {
        HdfsErr::FileNotFound(err_str) => {
            std::io::Error::new(std::io::ErrorKind::NotFound, err_str.as_str())
        }
        HdfsErr::FileAlreadyExists(err_str) => {
            std::io::Error::new(std::io::ErrorKind::AlreadyExists, err_str.as_str())
        }
        HdfsErr::CannotConnectToNameNode(err_str) => {
            std::io::Error::new(std::io::ErrorKind::NotConnected, err_str.as_str())
        }
        HdfsErr::InvalidUrl(err_str) => {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, err_str.as_str())
        }
        HdfsErr::Unknown => std::io::Error::new(std::io::ErrorKind::Other, "Unknown"),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::assert_batches_eq;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::file_format::FileFormat;
    use datafusion::physical_plan::file_format::PhysicalPlanConfig;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::ExecutionContext;
    use hdfs::minidfs;
    use hdfs::util::HdfsUtil;
    use std::future::Future;
    use std::pin::Pin;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn read_small_batches_from_hdfs() -> Result<()> {
        run_hdfs_test("alltypes_plain.parquet".to_string(), |fs, filename_hdfs| {
            Box::pin(async move {
                let projection = None;
                let exec =
                    get_hdfs_exec(Arc::new(fs), filename_hdfs.as_str(), &projection, 2, None)
                        .await?;
                let stream = exec.execute(0).await?;

                let tt_batches = stream
                    .map(|batch| {
                        let batch = batch.unwrap();
                        assert_eq!(11, batch.num_columns());
                        assert_eq!(2, batch.num_rows());
                    })
                    .fold(0, |acc, _| async move { acc + 1i32 })
                    .await;

                assert_eq!(tt_batches, 4 /* 8/2 */);

                // test metadata
                assert_eq!(exec.statistics().num_rows, Some(8));
                assert_eq!(exec.statistics().total_byte_size, Some(671));

                Ok(())
            })
        })
        .await
    }

    #[tokio::test]
    async fn parquet_query() {
        run_with_register_alltypes_parquet(|mut ctx| {
            Box::pin(async move {
                // NOTE that string_col is actually a binary column and does not have the UTF8 logical type
                // so we need an explicit cast
                let sql = "SELECT id, CAST(string_col AS varchar) FROM alltypes_plain";
                let actual = ctx.sql(sql).await?.collect().await?;
                let expected = vec![
                    "+----+-----------------------------------------+",
                    "| id | CAST(alltypes_plain.string_col AS Utf8) |",
                    "+----+-----------------------------------------+",
                    "| 4  | 0                                       |",
                    "| 5  | 1                                       |",
                    "| 6  | 0                                       |",
                    "| 7  | 1                                       |",
                    "| 2  | 0                                       |",
                    "| 3  | 1                                       |",
                    "| 0  | 0                                       |",
                    "| 1  | 1                                       |",
                    "+----+-----------------------------------------+",
                ];

                assert_batches_eq!(expected, &actual);

                Ok(())
            })
        })
        .await
        .unwrap()
    }

    async fn get_hdfs_exec(
        fs: Arc<HadoopFileSystem>,
        file_name: &str,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filename = file_name.to_string();
        let format = ParquetFormat::default();
        let file_schema = format
            .infer_schema(hadoop_object_reader_stream(
                fs.clone(),
                vec![filename.clone()],
            ))
            .await
            .expect("Schema inference");
        let statistics = format
            .infer_stats(hadoop_object_reader(fs.clone(), filename.clone()))
            .await
            .expect("Stats inference");
        let file_groups = vec![vec![hadoop_unpartitioned_file(
            fs.clone(),
            filename.clone(),
        )]];
        let exec = format
            .create_physical_plan(
                PhysicalPlanConfig {
                    object_store: fs.clone(),
                    file_schema,
                    file_groups,
                    statistics,
                    projection: projection.clone(),
                    batch_size,
                    limit,
                    table_partition_cols: vec![],
                },
                &[],
            )
            .await?;
        Ok(exec)
    }

    /// Run test after related data prepared
    pub async fn run_hdfs_test<F>(filename: String, test: F) -> Result<()>
    where
        F: FnOnce(
            HadoopFileSystem,
            String,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>,
    {
        let (hdfs, tmp_dir, dst_file) = setup_with_hdfs_data(&filename);

        let result = test(hdfs, dst_file).await;

        teardown(&tmp_dir);

        result
    }

    /// Prepare hdfs parquet file by copying local parquet file to hdfs
    fn setup_with_hdfs_data(filename: &str) -> (HadoopFileSystem, String, String) {
        let uuid = Uuid::new_v4().to_string();
        let tmp_dir = format!("/{}", uuid);

        let dfs = minidfs::get_dfs();
        let fs = dfs.get_hdfs().ok().unwrap();
        assert!(fs.mkdir(&tmp_dir).is_ok());

        // Source
        let testdata = datafusion::test_util::parquet_test_data();
        let src_path = format!("{}/{}", testdata, filename);

        // Destination
        let dst_path = format!("{}/{}", tmp_dir, filename);

        // Copy to hdfs
        assert!(HdfsUtil::copy_file_to_hdfs(dfs.clone(), &src_path, &dst_path).is_ok());

        (
            HadoopFileSystem::wrap(fs),
            tmp_dir,
            format!("{}{}", dfs.namenode_addr(), dst_path),
        )
    }

    /// Cleanup testing files in hdfs
    fn teardown(tmp_dir: &str) {
        let dfs = minidfs::get_dfs();
        let fs = dfs.get_hdfs().ok().unwrap();
        assert!(fs.delete(&tmp_dir, true).is_ok());
    }

    /// Run query after table registered with parquet file on hdfs
    pub async fn run_with_register_alltypes_parquet<F>(test_query: F) -> Result<()>
    where
        F: FnOnce(ExecutionContext) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>
            + Send
            + 'static,
    {
        run_hdfs_test("alltypes_plain.parquet".to_string(), |fs, filename_hdfs| {
            Box::pin(async move {
                let mut ctx = ExecutionContext::new();
                ctx.register_object_store(HDFS_SCHEME, Arc::new(fs));
                let table_name = "alltypes_plain";
                println!(
                    "Register table {} with parquet file {}",
                    table_name, filename_hdfs
                );
                ctx.register_parquet(table_name, &filename_hdfs).await?;

                test_query(ctx).await
            })
        })
        .await
    }
}
