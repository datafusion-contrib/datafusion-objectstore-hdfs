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

use std::future::Future;
use std::pin::Pin;

use ballista::prelude::*;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::ParquetReadOptions;

use datafusion_objectstore_hdfs_testing::util::run_hdfs_test;

/// This example demonstrates executing a simple query against an Arrow data source (CSV) and
/// fetching results, using SQL
#[tokio::main]
async fn main() -> Result<()> {
    run_with_register_alltypes_parquet(|ctx| {
        Box::pin(async move {
            {
                // NOTE that string_col is actually a binary column and does not have the UTF8 logical type
                // so we need an explicit cast
                let sql = "SELECT id, CAST(string_col AS varchar) FROM alltypes_plain";

                // execute the query
                let df = ctx.sql(sql).await?;

                // print the results
                df.show().await?;
            }

            {
                let sql = "SELECT count(*) FROM alltypes_plain";

                // execute the query
                let df = ctx.sql(sql).await?;

                // print the results
                df.show().await?;
            }

            Ok(())
        })
    })
    .await?;

    Ok(())
}

/// Run query after table registered with parquet file on hdfs
pub async fn run_with_register_alltypes_parquet<F>(test_query: F) -> Result<()>
where
    F: FnOnce(BallistaContext) -> Pin<Box<dyn Future<Output = Result<()>> + 'static>>
        + Send
        + 'static,
{
    run_hdfs_test("alltypes_plain.parquet".to_string(), |filename_hdfs| {
        Box::pin(async move {
            let config = BallistaConfig::builder()
                .set("ballista.shuffle.partitions", "4")
                .build()
                .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
            let ctx = BallistaContext::standalone(&config, 4)
                .await
                .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;

            let table_name = "alltypes_plain";
            println!(
                "Register table {} with parquet file {}",
                table_name, filename_hdfs
            );
            ctx.register_parquet(table_name, &filename_hdfs, ParquetReadOptions::default())
                .await?;

            test_query(ctx).await
        })
    })
    .await
}
