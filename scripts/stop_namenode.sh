#!/bin/bash

${HADOOP_HOME}/bin/hdfs --daemon stop namenode
sudo rm -rf /tmp/hadoop-*

