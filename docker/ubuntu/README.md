# Guidance

## Build Image

> docker build -t datafusion-objectstore-hdfs-ubuntu:1.0 -f ./Dockerfile .

## Run Container

Run container
> docker run --name datafusion-objectstore-hdfs -it datafusion-objectstore-hdfs-ubuntu:1.0 bash

> docker start datafusion-objectstore-hdfs

> docker exec -it datafusion-objectstore-hdfs bash

## Test

> cd ~/datafusion-objectstore-hdfs

> git submodule update --init --recursive

> cargo test