#!/bin/bash

IS_DECLARATIVE=1 # Set to 1 if you are running the declarative version of HDFS, else 0
NN= # Namenode address
NUM_DN=8 # Number of datanodes at start (experiment will add another towards the beginning)
DN_BALANCER= # Datanode for balancer process
DN_DFS_PERF= # Datanode for dfs-perf process
DN_TRANSCODING= # Datanode for transcoding processes
TRANSCODE_SRC=/tmp/dfs-perf-workspace/simple-read-write
TRANSCODE_DST=/.transcoded/tmp/dfs-perf-workspace/simple-read-write
EXP_DURATION=292600
# Note that starting a datanode on a node that already had a datanode might raise an error unless you
# wipe the previous one's data/metadata in the file system
DN_TO_ADD_1= # Datanode to add partway through the experiment
DN_TO_ADD_2= # Another datanode to add partway through the experiment
DN_TO_DROP_1= # Datanode to drop partway through the experiment
DN_TO_DROP_2= # Another datanode to drop partway through the experiment

# Setup
ssh "${DN_DFS_PERF}" "source ${PROJ_SCRIPTS}/env.sh; cd ${PROJ_SCRIPTS}; ${PROJ_SCRIPTS}/build_env.sh"
ssh "${DN_BALANCER}" "source ${PROJ_SCRIPTS}/env.sh; cd ${PROJ_SCRIPTS}; ${PROJ_SCRIPTS}/build_env.sh"

${PROJ_SCRIPTS}/start_cluster.sh
${HADOOP_HOME}/bin/hdfs ec -enablePolicy -policy RS-3-2-1024k
${HADOOP_HOME}/bin/hdfs dfs -mkdir -p ${TRANSCODE_DST}
${HADOOP_HOME}/bin/hdfs ec -setPolicy -policy RS-3-2-1024k -path ${TRANSCODE_DST}

for i in {0..3}; do
	${HADOOP_HOME}/bin/hdfs dfs -mkdir -p ${TRANSCODE_DST}/${i}
	${HADOOP_HOME}/bin/hdfs dfs -mkdir -p ${TRANSCODE_SRC}/${i}
done
for i in {1..3}; do
	${HADOOP_HOME}/bin/hdfs ec -setPolicy -policy RS-3-2-1024k -path ${TRANSCODE_SRC}/${i}
done

echo "Starting to fill the filesystem at `date`"
ssh "${DN_DFS_PERF}" "source ${PROJ_SCRIPTS}/env.sh; ${DFS_PERF_HOME}/bin/dfs-perf SimpleWrite"
ssh "${DN_DFS_PERF}" "source ${PROJ_SCRIPTS}/env.sh; ${DFS_PERF_HOME}/bin/dfs-perf-collect SimpleWrite"
echo "Done filling the filesystem at `date`"
${HADOOP_HOME}/bin/hdfs dfsadmin -report

# Workload
echo "Starting to run maintenance workload"
${PROJ_SCRIPTS}/start_io_recording.sh

ssh "${DN_BALANCER}" "source ${PROJ_SCRIPTS}/env.sh; ${HADOOP_HOME}/bin/hdfs balancer -asService -threshold 25 > balancer-logs 2>&1 &"

# Datanode installation and failure events
(sleep 500 && ssh "${DN_TO_ADD_1}" "${PROJ_SCRIPTS}/start_datanode.sh") > /dev/null 2>&1 &
(sleep 73150 && ssh "${DN_TO_DROP_1}" "${PROJ_SCRIPTS}/stop_datanode.sh") > /dev/null 2>&1 &
(sleep 145800 && ssh "${DN_TO_ADD_2}" "${PROJ_SCRIPTS}/start_datanode.sh") > /dev/null 2>&1 &
(sleep 218450 && ssh "${DN_TO_DROP_2}" "${PROJ_SCRIPTS}/stop_datanode.sh") > /dev/null 2>&1 &

# Transcoding
if (( IS_DECLARATIVE == 1 )); then
  ssh "${DN_TRANSCODING}" "source ${PROJ_SCRIPTS}/env.sh; ${PROJ_SCRIPTS}/declarative_transcoding.sh 500 ${TRANSCODE_SRC}/0 ${TRANSCODE_DST}/0 1610612736 2371980140820 29 > transcode-logs 2>&1 &"
  ssh "${DN_TRANSCODING}" "source ${PROJ_SCRIPTS}/env.sh; ${PROJ_SCRIPTS}/declarative_transcoding.sh 44667 ${TRANSCODE_SRC}/1 ${TRANSCODE_DST}/1 1610612736 2371980140820 29 >> transcode-logs 2>&1 &"
  ssh "${DN_TRANSCODING}" "source ${PROJ_SCRIPTS}/env.sh; ${PROJ_SCRIPTS}/declarative_transcoding.sh 88833 ${TRANSCODE_SRC}/2 ${TRANSCODE_DST}/2 1610612736 2371980140820 29 >> transcode-logs 2>&1 &"
  ssh "${DN_TRANSCODING}" "source ${PROJ_SCRIPTS}/env.sh; ${PROJ_SCRIPTS}/declarative_transcoding.sh 133000 ${TRANSCODE_DST}/0 ${TRANSCODE_SRC}/0 1610612736 2371980140820 29 >> transcode-logs 2>&1 &"
  ssh "${DN_TRANSCODING}" "source ${PROJ_SCRIPTS}/env.sh; ${PROJ_SCRIPTS}/declarative_transcoding.sh 177166 ${TRANSCODE_DST}/1 ${TRANSCODE_SRC}/1 1610612736 2371980140820 29 >> transcode-logs 2>&1 &"
  ssh "${DN_TRANSCODING}" "source ${PROJ_SCRIPTS}/env.sh; ${PROJ_SCRIPTS}/declarative_transcoding.sh 221333 ${TRANSCODE_DST}/2 ${TRANSCODE_SRC}/2 1610612736 2371980140820 29 >> transcode-logs 2>&1 &"
  ssh "${DN_TRANSCODING}" "source ${PROJ_SCRIPTS}/env.sh; ${PROJ_SCRIPTS}/declarative_transcoding.sh 265499 ${TRANSCODE_SRC}/0 ${TRANSCODE_DST}/0 1610612736 2371980140820 29 >> transcode-logs 2>&1 &"
else
  ssh "${DN_TRANSCODING}" "source ${PROJ_SCRIPTS}/env.sh; ${PROJ_SCRIPTS}/imperative_transcoding.sh 500 ${TRANSCODE_SRC}/0 ${TRANSCODE_DST}/0 1610612736 2371980140820 29 > transcode-logs 2>&1 &"
  ssh "${DN_TRANSCODING}" "source ${PROJ_SCRIPTS}/env.sh; ${PROJ_SCRIPTS}/imperative_transcoding.sh 44667 ${TRANSCODE_SRC}/1 ${TRANSCODE_DST}/1 1610612736 2371980140820 29 >> transcode-logs 2>&1 &"
  ssh "${DN_TRANSCODING}" "source ${PROJ_SCRIPTS}/env.sh; ${PROJ_SCRIPTS}/imperative_transcoding.sh 88833 ${TRANSCODE_SRC}/2 ${TRANSCODE_DST}/2 1610612736 2371980140820 29 >> transcode-logs 2>&1 &"
  ssh "${DN_TRANSCODING}" "source ${PROJ_SCRIPTS}/env.sh; ${PROJ_SCRIPTS}/imperative_transcoding.sh 133000 ${TRANSCODE_DST}/0 ${TRANSCODE_SRC}/0 1610612736 2371980140820 29 >> transcode-logs 2>&1 &"
  ssh "${DN_TRANSCODING}" "source ${PROJ_SCRIPTS}/env.sh; ${PROJ_SCRIPTS}/imperative_transcoding.sh 177166 ${TRANSCODE_DST}/1 ${TRANSCODE_SRC}/1 1610612736 2371980140820 29 >> transcode-logs 2>&1 &"
  ssh "${DN_TRANSCODING}" "source ${PROJ_SCRIPTS}/env.sh; ${PROJ_SCRIPTS}/imperative_transcoding.sh 221333 ${TRANSCODE_DST}/2 ${TRANSCODE_SRC}/2 1610612736 2371980140820 29 >> transcode-logs 2>&1 &"
  ssh "${DN_TRANSCODING}" "source ${PROJ_SCRIPTS}/env.sh; ${PROJ_SCRIPTS}/imperative_transcoding.sh 265499 ${TRANSCODE_SRC}/0 ${TRANSCODE_DST}/0 1610612736 2371980140820 29 >> transcode-logs 2>&1 &"
fi

sleep ${EXP_DURATION}

${PROJ_SCRIPTS}/stop_io_recording.sh

# Shut down
echo "Stopping experiment"
${HADOOP_HOME}/bin/hdfs dfsadmin -report
${PROJ_SCRIPTS}/stop_cluster.sh
ssh "${DN_BALANCER}" "source ${PROJ_SCRIPTS}/env.sh; ${PROJ_SCRIPTS}/stop_balancer.sh"
ssh "${DN_TO_ADD_2}" "source ${PROJ_SCRIPTS}/env.sh; ${PROJ_SCRIPTS}/stop_datanode.sh"
if (( IS_DECLARATIVE == 1 )); then
  ssh "${DN_TRANSCODING}" "pkill -f declarative_transcoding.sh"
else
  ssh "${DN_TRANSCODING}" "pkill -f imperative_transcoding.sh"
fi
