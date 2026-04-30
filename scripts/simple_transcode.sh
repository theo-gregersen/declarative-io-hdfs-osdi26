#!/bin/bash

# <file path> <new path>

BYTES=`${HADOOP_HOME}/bin/hdfs dfs -ls $1 | awk '{print $5}'`
MS=`date +%s%N`
MS=$((${MS}/1000000))
DEADLINE=$(($MS+1000*60*100))

echo "Transcoding $1 to $2: ${BYTES} bytes"

#${PROJ_SCRIPTS}/declare "TRANSCODE" $1 0 $BYTES $DEADLINE

#${HADOOP_HOME}/bin/hdfs dfs -cp $1 $2

${HADOOP_HOME}/bin/hadoop distcp -overwrite -skipcrccheck $1 $2 && \
${HADOOP_HOME}/bin/hdfs dfs -rm $1

echo "Done transcoding $1"

