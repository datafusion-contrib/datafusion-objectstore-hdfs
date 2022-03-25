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
use chrono::{DateTime, NaiveDateTime, Utc};
use datafusion_data_access::{
    object_store::{
        FileMetaStream, ListEntryStream, ObjectReader, ObjectReaderStream, ObjectStore,
    },
    FileMeta, Result, SizedFile,
};
use futures::AsyncRead;
use futures::{stream, StreamExt};
use hdfs::hdfs::{get_hdfs_by_full_path, FileStatus, HdfsErr, HdfsFile, HdfsFs};

/// scheme for HDFS File System
pub static HDFS_SCHEME: &str = "hdfs";
/// scheme for HDFS Federation File System
pub static VIEWFS_SCHEME: &str = "viewfs";

#[derive(Debug)]
/// Hadoop File System as Object Store.
pub struct HadoopFileSystem;

#[async_trait]
impl ObjectStore for HadoopFileSystem {
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream> {
        let hdfs = get_hdfs_by_full_path(prefix).map_err(to_error)?;
        let mut leaf_files = vec![];
        find_leaf_files_in_dir(hdfs, prefix, &mut leaf_files)?;
        Ok(Box::pin(stream::iter(leaf_files).map(Ok)))
    }

    async fn list_dir(&self, _prefix: &str, _delimiter: Option<String>) -> Result<ListEntryStream> {
        todo!()
    }

    fn file_reader(&self, file: SizedFile) -> Result<Arc<dyn ObjectReader>> {
        Ok(Arc::new(HadoopFile::new(file)?))
    }
}

#[derive(Clone, Debug)]
struct HadoopFile {
    file: Arc<HdfsFile>,
    file_meta: SizedFile,
}

impl HadoopFile {
    fn new(file_meta: SizedFile) -> Result<Self> {
        let fs = get_hdfs_by_full_path(&file_meta.path).map_err(to_error)?;
        let file = Arc::new(fs.open(&file_meta.path).map_err(to_error)?);
        Ok(Self { file, file_meta })
    }
}

#[async_trait]
impl ObjectReader for HadoopFile {
    async fn chunk_reader(&self, _start: u64, _length: usize) -> Result<Box<dyn AsyncRead>> {
        todo!("implement once async file readers are available (arrow-rs#78, arrow-rs#111)")
    }

    fn sync_chunk_reader(&self, start: u64, length: usize) -> Result<Box<dyn Read + Send + Sync>> {
        let reader = HadoopFileReader {
            file: self.file.clone(),
        };
        reader.file.seek(start);
        Ok(Box::new(reader.take(length as u64)))
    }

    fn length(&self) -> u64 {
        self.file_meta.size
    }
}

struct HadoopFileReader {
    file: Arc<HdfsFile>,
}

impl Read for HadoopFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file
            .read(buf)
            .map(|read_len| read_len as usize)
            .map_err(to_error)
    }
}

/// Find out all leaf files under a directory
fn find_leaf_files_in_dir(
    hdfs: Arc<HdfsFs>,
    path: &str,
    leaf_files: &mut Vec<FileMeta>,
) -> Result<()> {
    for file in hdfs.list_status(path).map_err(to_error)? {
        if file.is_directory() {
            find_leaf_files_in_dir(hdfs.clone(), file.name(), leaf_files)?;
        } else if file.is_file() {
            leaf_files.push(get_meta(file.name().to_owned(), file));
        }
    }
    Ok(())
}

fn get_meta(path: String, file_status: FileStatus) -> FileMeta {
    FileMeta {
        sized_file: SizedFile {
            path,
            size: file_status.len() as u64,
        },
        last_modified: Some(DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(file_status.last_modified(), 0),
            Utc,
        )),
    }
}

/// Create a stream of `ObjectReader` by converting each file in the `files` vector
/// into instances of `HadoopFileReader`
pub fn hadoop_object_reader_stream(files: Vec<String>) -> ObjectReaderStream {
    Box::pin(futures::stream::iter(files).map(move |f| Ok(hadoop_object_reader(f))))
}

/// Helper method to convert a file location to a `LocalFileReader`
pub fn hadoop_object_reader(file: String) -> Arc<dyn ObjectReader> {
    HadoopFileSystem
        .file_reader(hadoop_unpartitioned_file(file).sized_file)
        .expect("File not found")
}

/// Helper method to fetch the file size and date at given path and create a `FileMeta`
pub fn hadoop_unpartitioned_file(file: String) -> FileMeta {
    let fs = get_hdfs_by_full_path(&file).expect("HdfsFs not found");
    let file_status = fs.get_file_status(&file).expect("File status not found");
    get_meta(file, file_status)
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
    use std::future::Future;
    use std::pin::Pin;

    use datafusion::assert_batches_eq;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::file_format::FileFormat;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::error::Result;
    use datafusion::physical_plan::file_format::FileScanConfig;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use hdfs::minidfs;
    use hdfs::util::HdfsUtil;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn read_small_batches_from_hdfs() -> Result<()> {
        run_hdfs_test("alltypes_plain.parquet".to_string(), |filename_hdfs| {
            Box::pin(async move {
                let session_context =
                    SessionContext::with_config(SessionConfig::new().with_batch_size(2));
                let projection = None;
                let exec = get_hdfs_exec(filename_hdfs.as_str(), &projection, None).await?;
                let stream = exec.execute(0, session_context.task_ctx()).await?;

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
        file_name: &str,
        projection: &Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filename = file_name.to_string();
        let format = ParquetFormat::default();
        let file_schema = format
            .infer_schema(hadoop_object_reader_stream(vec![filename.clone()]))
            .await
            .expect("Schema inference");
        let statistics = format
            .infer_stats(hadoop_object_reader(filename.clone()))
            .await
            .expect("Stats inference");
        let file_groups = vec![vec![unpartitioned_file(filename.clone())]];
        let exec = format
            .create_physical_plan(
                FileScanConfig {
                    object_store: Arc::new(HadoopFileSystem {}),
                    file_schema,
                    file_groups,
                    statistics,
                    projection: projection.clone(),
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
        F: FnOnce(String) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>,
    {
        let (tmp_dir, dst_file) = setup_with_hdfs_data(&filename);

        let result = test(dst_file).await;

        teardown(&tmp_dir);

        result
    }

    /// Prepare hdfs parquet file by copying local parquet file to hdfs
    fn setup_with_hdfs_data(filename: &str) -> (String, String) {
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

        (tmp_dir, format!("{}{}", dfs.namenode_addr(), dst_path))
    }

    /// Cleanup testing files in hdfs
    fn teardown(tmp_dir: &str) {
        let dfs = minidfs::get_dfs();
        let fs = dfs.get_hdfs().ok().unwrap();
        assert!(fs.delete(tmp_dir, true).is_ok());
    }

    /// Run query after table registered with parquet file on hdfs
    pub async fn run_with_register_alltypes_parquet<F>(test_query: F) -> Result<()>
    where
        F: FnOnce(SessionContext) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>
            + Send
            + 'static,
    {
        run_hdfs_test("alltypes_plain.parquet".to_string(), |hdfs_file_uri| {
            Box::pin(async move {
                let mut ctx = SessionContext::new();
                ctx.runtime_env()
                    .register_object_store("hdfs", Arc::new(HadoopFileSystem {}));
                let table_name = "alltypes_plain";
                println!(
                    "Register table {} with parquet file {}",
                    table_name, hdfs_file_uri
                );
                ctx.register_parquet(table_name, &hdfs_file_uri).await?;

                test_query(ctx).await
            })
        })
        .await
    }

    /// Helper method to fetch the file size and date at given path and create a `FileMeta`
    pub fn unpartitioned_file(file: String) -> PartitionedFile {
        PartitionedFile {
            file_meta: hadoop_unpartitioned_file(file),
            partition_values: vec![],
        }
    }
}
