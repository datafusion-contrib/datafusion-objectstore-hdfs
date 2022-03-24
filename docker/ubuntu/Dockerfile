FROM ubuntu:20.04

MAINTAINER Yanghong Zhong <nju_yaho@apache.org>

WORKDIR /tmp

# install tools
RUN set -x \
    && apt-get update \
    && apt install -y lsb-release wget curl

# install llvm
RUN set -x \
    && apt install -y software-properties-common apt-transport-https \
    && cd /tmp \
    && wget https://apt.llvm.org/llvm.sh \
    && chmod +x llvm.sh \
    && ./llvm.sh 12

ARG APACHE_HOME=/apache

# install jdk
RUN set -x \
    && apt-get install -y openjdk-8-jdk \
    && java -version

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

RUN set -x \
    && mkdir -p $APACHE_HOME \
    && ln -s $JAVA_HOME /apache/java

# install hadoop clients
ARG HADOOP_VERSION=2.10.1
ENV HADOOP_HOME=$APACHE_HOME/hadoop
RUN set -x \
    && wget https://dlcdn.apache.org/hadoop/common/stable2/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzvf hadoop-${HADOOP_VERSION}.tar.gz -C $APACHE_HOME \
    && rm -f hadoop-${HADOOP_VERSION}.tar.gz \
    && ln -s $APACHE_HOME/hadoop-${HADOOP_VERSION} $HADOOP_HOME

# install Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HOME/.cargo/bin

RUN echo "export CLASSPATH=`hadoop classpath --glob`:$CLASSPATH" >> $HOME/.bashrc

ENV LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server/:$LD_LIBRARY_PATH

# install gcc
RUN set -x \
    && apt-get install -y gcc \
    && gcc --version

# install git
RUN set -x \
    && apt-get install -y git \
    && git version

# Download source code
RUN set -x \
    && cd ~ \
    && git clone https://github.com/datafusion-contrib/datafusion-objectstore-hdfs.git

