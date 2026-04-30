# Scripts

Our code was built, tested, and run on Ubuntu 22 (64 bit) machines.
The scripts in this folder are intended for Unix systems and bash.

## Building
- `setup.sh` will install all the pre-requisites for building and running the system.
- You'll also need to install [dfs-perf](https://pasa-bigdata.nju.edu.cn/dfs-perf/index.html).
    - There are multiple ways to do so (build from source, install via tar, etc.); here are [their instructions](https://pasa-bigdata.nju.edu.cn/dfs-perf/Running-DFS-Perf-on-a-Cluster.html).
    - We use dfs-perf to fill the filesystem with data.
- `env.sh` has several environment variables that need to be set.
When building HDFS, the Java version and home need to be Java 8. When running HDFS, it should be Java 11.
- `hdfs_build_cmd.sh` has the command used to build HDFS from our source directories (either 
the declarative or baseline versions in this repository).
Run this from the root source directory.
    - Sometimes you need to hardcode the Java home variable in `hadoop-common-project/hadoop-common/HadoopJNI.cmake`.
- The `declare` and `declare-transcode-grouped` binaries were built from `hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfspp/examples/cc/declarative-io/`

## Recording IO and utilization
- `record_iostat.sh` is the command we used to track disk IO.
- `record_top.sh` is a program we used to track node hardware utilization.

## Running experiments
- Before starting, make sure that the configuration files are properly set. The key ones are:
    - `$HADOOP_HOME/etc/hadoop/core-site.xml` is where the namenode address must be defined.
    - `$HADOOP_HOME/etc/hadoop/hdfs-site.xml` is where HDFS configuration variables can be changed from default.
    You'll want to change the value of `dfs.datanode.data.dir` to point to an appropriate location for the datanode
    to store data.
    If you need separate configurations for different datanodes then you can set the environment variable
    `HADOOP_CONF_DIR` to point to a different configuration folder.
    - `$DFS_PERF_HOME/conf/testsuite/SimpleWrite.xml` is where the dfs-perf write parameters are configured.
    - `$DFS_PERF_HOME/conf/dfs-perf-env.sh` has other dfs-perf variables that must be set (e.g. namenode address).
        - We set `DFS_PERF_THREADS_NUM=2` and use 4 slves.
        - See [their instructions](https://pasa-bigdata.nju.edu.cn/dfs-perf/Running-DFS-Perf-on-HDFS.html) for other configuration requirements.
    - Our `hdfs-site.xml` and `SimpleWrite.xml` configuration files are in the `configs` folder for copying.
- `experiment.sh` is the high-level experiment script used for the paper.
To use this to run our experiment on your own cluster, you'll have to set some of the variables at the top of the
experiment script and potentially alter some lines to reflect your cluster setup (e.g. our nodes could access the
script files and HDFS dist via NFS).
You will also need to create the following scripts in this folder, tailored to how your cluster operates.
    - `start_cluster.sh` should start a namenode process on one node and N datanode processes
    on N other nodes.
    For our experiment, we have one namenode and 8 datanodes (the experiment script will add and drop additional
    datanodes over time, peaking at a cluster size of 10 total nodes).
    - `start_io_recording.sh` should run `record_iostat.sh` and `record_top.sh` on each node and save the outputs.
    - `stop_cluster.sh` should shutdown the cluster using `stop_namenode.sh` and `stop_datanode.sh`
    on the appropriate nodes.
    If you want to start the cluster again after, this script should wipe any namenode and datanode data and metadata
    stored in the node filesystems.
    - `stop_io_recording.sh` should stop the recording processes on each node.
- Modify any of the scripts to change what standard output and standard error you want to see.
- The namenode and datanodes log metrics from the IO Planner and maintenance tasks over time. These
logs can be found in the `$HADOOP_HOME/logs` folder.
- The balancer and transcoding output metrics as well (location is set in the `experiment.sh` via piping).
    
