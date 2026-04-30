package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos;

import java.util.concurrent.ConcurrentHashMap;

public class BalancerHeartbeatResponse {
    private ConcurrentHashMap<String, NamenodeProtocolProtos.DeclareIOBlocksRequestProto> queue;

    public BalancerHeartbeatResponse(ConcurrentHashMap<String, NamenodeProtocolProtos.DeclareIOBlocksRequestProto> queue) {
        this.queue = queue;
    }

    public ConcurrentHashMap<String, NamenodeProtocolProtos.DeclareIOBlocksRequestProto> getQueue() {return queue;}

    public boolean isQueueEmpty() {return queue.isEmpty();}
}
