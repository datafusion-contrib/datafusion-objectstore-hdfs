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

//! utility for setup local hdfs testing environment

use datafusion::common::DataFusionError;
use datafusion::error::Result;
use hdfs::minidfs;
use hdfs::util::HdfsUtil;
use std::env;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use uuid::Uuid;

/// Run test after related data prepared
pub async fn run_hdfs_test<F>(filename: String, test: F) -> Result<()>
where
    F: FnOnce(String) -> Pin<Box<dyn Future<Output = Result<()>> + 'static>>,
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
    let testdata = parquet_test_data();
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

/// Returns the parquet test data directory, which is by default
/// stored in a git submodule rooted at
/// `parquet-testing/data`.
///
/// panics when the directory can not be found.
pub fn parquet_test_data() -> String {
    match get_data_dir("../parquet-testing/data") {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!("failed to get parquet data dir: {}", err),
    }
}

/// Returns a directory path for finding test data.
///
/// submodule_dir: path relative to CARGO_MANIFEST_DIR
///
///  Returns:
/// The submodule_data directory relative to CARGO_MANIFEST_PATH
fn get_data_dir(submodule_data: &str) -> Result<PathBuf> {
    // env "CARGO_MANIFEST_DIR" is "the directory containing the manifest of your package",
    // set by `cargo run` or `cargo test`, see:
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html
    let dir = env!("CARGO_MANIFEST_DIR");

    let pb = PathBuf::from(dir).join(submodule_data);
    if pb.is_dir() {
        Ok(pb)
    } else {
        Err(DataFusionError::External(
            format!(
                "the pre-defined data dir `{}` not found\n\
             HINT: try running `git submodule update --init`",
                pb.display(),
            )
            .into(),
        ))
    }
}
