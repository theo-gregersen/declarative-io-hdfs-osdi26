#!/bin/bash

sudo apt -y install openjdk-8-jdk
sudo apt -y install openjdk-11-jdk
sudo apt -y install sysstat
sudo apt -y install cgroup-tools
sudo apt-get -y install maven
sudo apt-get -y install build-essential autoconf automake libtool cmake zlib1g-dev pkg-config libssl-dev libsasl2-dev
#sudo apt-get -y install snappy libsnappy-dev
sudo apt-get -y install libsnappy-dev
sudo apt-get -y install bzip2 libbz2-dev
sudo apt-get -y install libjansson-dev
sudo apt-get -y install fuse libfuse-dev
sudo apt-get -y install zstd

curl -L -s -S https://github.com/protocolbuffers/protobuf/releases/download/v3.7.1/protobuf-java-3.7.1.tar.gz -o protobuf-3.7.1.tar.gz
mkdir protobuf-3.7-src
tar xzf protobuf-3.7.1.tar.gz --strip-components 1 -C protobuf-3.7-src && cd protobuf-3.7-src
cd protobuf-3.7-src
./configure
make -j$(nproc)
sudo make install

# install isa-l
cd /tmp \
  && sudo apt-get -y install nasm \
  && wget https://github.com/01org/isa-l/archive/v2.25.0.tar.gz \
  && tar zxf v2.25.0.tar.gz \
  && cd isa-l-2.25.0/ \
  && ./autogen.sh \
  && ./configure \
  && make -j4 \
  && sudo make install

# might need
sudo apt-get -y install libgmock-dev
sudo ldconfig

# Create a cgroup for constraining hdfs memory usage
# Can delete with cgdelete memory:hdfs
sudo cgcreate -t root:root -a root:root -g memory:/hdfs

# Install and setup dfs-perf, will need to be built with mvn later
cd .. && git clone git@github.com:PasaLab/dfs-perf.git
cd dfs-perf && sed -i 's/<hadoop.version>2.3.0<\/hadoop.version>/<hadoop.version>3.4.0<\/hadoop.version>/' pom.xml
cd ../scripts
