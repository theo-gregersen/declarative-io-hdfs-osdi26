# HDFS with declarative IO

## Overview:

This repository contains source code and scripts for running HDFS with declarative IO.
- `hdfs-declarative/` contains source code for the declarative version of HDFS.
- `hdfs-baseline/` contains source code for the baseline version of HDFS (with modifications to support
our transcoding operations and some slight smoothing to maintenance task timing).
- `scripts/` contains scripts and instructions for building and running experiments on a cluster.

## Source code: key HDFS files and modifications for declarative IO

All declarative IO implementation is in the `hdfs-declarative/hadoop-hdfs-project` folder.

### IO Planner
- `IOPlanner.java` - core logic for IO Planner and declarative IO scheduling and dispatching.
- `NameNode.java` - hosts the IO Planner, new methods for declarations which interface with the planner.
    - See also `NameNodeRpcServer.java`
- `LocatedBlock.java` - overriding equals and hashcode for IO Planner comparisons (these weren't being used elsewhere).
    - Also commented out a precondition assertion in `PBHelperClient.java` since we slightly misuse LocatedBlock.
- `DFSConfigKeys.java` - defaults and names for configuration values for declarative IO (user can set values in `hdfs-site.xml`).
    - See also `hdfs-default.xml`
- `CacheManager.java` - added a path for the IO Planner to explicitly cache and uncache blocks.
    - See also `CacheReplicationMonitor.java`, `FsDatasetCache.java`

### HDFS internal maintenance task updates
- `DatanodeProtocolClientSideTranslatorPB.java` - adding some declaration information on heartbeats.
    - See also `DatanodeProtocolServerSideTranslatorPB.java`, `DatanodeProtocol.proto`, `NamenodeProtocol.proto`
- `DataNodeMetrics.java` - some new metrics for tracking maintenance task logical work.
- `FSNamesystem.java` - modifications to heartbeat logic for declarative internal maintenance tasks, and also some
specific indirection logic for transcoding.

#### Scrubbing
- `BPServiceActor.java` - makes scrubbing declarations and parses out blocks to scrub from heartbeat responses.
- `DataNode.java` - collects blocks to be declared for scrubbing so that the BPServiceActor can declare them.
- `BlockScanner.java` - activates volume scanners when directed to scrub a block that was declared.
- `VolumeScanner.java` - new pathway to scrub a declared set of blocks rather than imperatively looping.

#### Rebalancing
- `NamenodeProtocolServerSideTranslatorPB.java` - new messages for rebalancing declarations.
    - See also `NamenodeProtocolTranslatorPB.java`, `NameNodeConnector.java`, `DatanodeProtocol.java`,
    `NamenodeProtocol.java`, `PBHelper.java`
- `Balancer.java` - modified balancing flow to be declarative rather than imperative.
- `Dispatcher.java` - modified dispatching process to target blocks scheduled declaratively.

#### Reconstruction
- `DatanodeManager.java` - update how reconstruction work is assigned to datanodes in order to be declarative.

### C++ external declaration API
- `HdfsClientConfigKeys.java` - new keys for transcoding.
- `ClientProtocol.java` - new methods for declarations (declareIOs, declareIOsAsync, pollDeclaration, etc.).
    - See also `ClientNamenodeProtocolTranslatorPB.java`, `ClientNamenodeProtocolServerSideTranslatorPB.java`,
    `RouterClientProtocol.java`, `NameNodeRpcServer.java`
- `ClientNamenodeProtocol.java` - protobuf definitions for declarations.
- `hdfspp.h` - C++ definitions for declarations.
    - See also `filesystem.h`, `filesystem.cc`, `namenode_operations.h`, `namenode_operations.cc`
- See `hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfspp/examples/cc/declarative-io/` for
some examples of file segment declarations.

### Transcoding
- `FSNamesystem.java` - update key interface methods in file system to check special transcoding directory logic.
- `declare-transcode-grouped.cc` - code for a binary that does declarative transcoding of file groups.

