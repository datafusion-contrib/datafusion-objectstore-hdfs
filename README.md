# datafusion-objectstore-hdfs

HDFS as a remote ObjectStore for [Datafusion](https://github.com/apache/arrow-datafusion).

## Querying files on HDFS with DataFusion

This crate introduces ``HadoopFileSystem`` as a remote ObjectStore which provides the ability of querying on HDFS files. 

For the HDFS access, We leverage the library [fs-hdfs](https://github.com/datafusion-contrib/fs-hdfs). Basically, the library only provides Rust FFI APIs for the ``libhdfs`` which can be compiled by a set of C files provided by the [official Hadoop Community](https://github.com/apache/hadoop).

## Prerequisites
Since the ``libhdfs`` is also just a C interface wrapper and the real implementation for the HDFS access is a set of Java jars, in order to make this crate work, we need to prepare the Hadoop client jars and the JRE environment.

### Prepare JAVA

1. Install Java.

2. Specify and export ``JAVA_HOME``.

### Prepare Hadoop client

1. To get a Hadoop distribution, download a recent stable release from one of the [Apache Download Mirrors](http://www.apache.org/dyn/closer.cgi/hadoop/common). Currently, we support Hadoop-2 and Hadoop-3.

2. Unpack the downloaded Hadoop distribution. For example, the folder is /opt/hadoop. Then prepare some environment variables:
```shell
export HADOOP_HOME=/opt/hadoop

export PATH=$PATH:$HADOOP_HOME/bin
```

### Prepare JRE environment

1. Firstly, we need to add library path for the jvm related dependencies. An example for MacOS,
```shell
export DYLD_LIBRARY_PATH=$JAVA_HOME/jre/lib/server
```

2. Since our compiled libhdfs is JNI native implementation, it requires the proper CLASSPATH to load the Hadoop related jars. An example,
```shell
export CLASSPATH=$CLASSPATH:`hadoop classpath --glob`
```

## Examples
Suppose there's a hdfs directory,
```rust
let hdfs_file_uri = "hdfs://localhost:8020/testing/tpch_1g/parquet/line_item";
```
in which there're a list of parquet files. Then we can query on these parquet files as follows:
```rust
let ctx = SessionContext::new();
ctx.runtime_env().register_object_store("hdfs", "", Arc::new(HadoopFileSystem));
let table_name = "line_item";
println!(
    "Register table {} with parquet file {}",
    table_name, hdfs_file_uri
);
ctx.register_parquet(table_name, &hdfs_file_uri, ParquetReadOptions::default()).await?;

let sql = "SELECT count(*) FROM line_item";
let result = ctx.sql(sql).await?.collect().await?;
```

## Testing
1. First clone the test data repository:
```shell
git submodule update --init --recursive
```

2. Run testing
```shell
cargo test
```
During the testing, a HDFS cluster will be mocked and started automatically.

3. Run testing for with enabling feature hdfs3
```shell
cargo build --no-default-features --features datafusion-objectstore-hdfs/hdfs3,datafusion-objectstore-hdfs-testing/hdfs3,datafusion-hdfs-examples/hdfs3

cargo test --no-default-features --features datafusion-objectstore-hdfs/hdfs3,datafusion-objectstore-hdfs-testing/hdfs3,datafusion-hdfs-examples/hdfs3
```

Run the ballista-sql test by
```shell
cargo run --bin ballista-sql --no-default-features --features hdfs3
```