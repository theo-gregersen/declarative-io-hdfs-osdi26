#!/bin/bash

${HADOOP_HOME}/bin/hdfs --daemon stop datanode

# Fallback - might need to directly kill the process if using cgroups because of permissions.
# E.g., jps doesn't see the process (or /bin/hdfs) but sudo jps does.
sudo kill `sudo jps | awk '/DataNode/{print $1}'`

