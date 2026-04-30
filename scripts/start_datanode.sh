#!/bin/bash

MEM_LIMIT=$((10*1024*1024*1024))
sudo cgset -r memory.high=${MEM_LIMIT} hdfs
sudo cgset -r memory.max=${MEM_LIMIT} hdfs

sudo cgexec -g memory:hdfs ${HADOOP_HOME}/bin/hdfs --daemon start datanode

# Note: if you try to redirect write into cgroup.procs it will give a permission error because
# the redirect operator isn't run with sudo even if the sudo echo is. If you put the command
# in a bash script and run the script with sudo it will work.

