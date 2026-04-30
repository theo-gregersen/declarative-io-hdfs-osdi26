#!/bin/bash

${HADOOP_HOME}/bin/hdfs namenode -format
${HADOOP_HOME}/bin/hdfs --daemon start namenode

