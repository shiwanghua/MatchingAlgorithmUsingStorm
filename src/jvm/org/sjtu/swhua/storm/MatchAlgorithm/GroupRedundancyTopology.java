package org.sjtu.swhua.storm.MatchAlgorithm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.*;
import org.sjtu.swhua.storm.MatchAlgorithm.bolt.GroupRedundancyMatchBolt;
import org.sjtu.swhua.storm.MatchAlgorithm.bolt.GroupRedundancyMergerBolt;
import org.sjtu.swhua.storm.MatchAlgorithm.bolt.MultiPartitionMergerBolt;
import org.sjtu.swhua.storm.MatchAlgorithm.bolt.ReinMPMatchBolt;
import org.sjtu.swhua.storm.MatchAlgorithm.spout.EventSpout;
import org.sjtu.swhua.storm.MatchAlgorithm.spout.SubscriptionSpout;

import java.util.BitSet;

//storm local target/storm-2021-11-21.jar  org.sjtu.swhua.storm.MatchAlgorithm.GroupRedundancyTopology
// storm jar target/storm-2021-11-21.jar  org.sjtu.swhua.storm.MatchAlgorithm.GroupRedundancyTopology
public class GroupRedundancyTopology {
    public static void main(String[] args) throws Exception {

        int numExecutorInASpout = TypeConstant.numExecutorPerSpout;
        int numExecutorInAMatchBolt = TypeConstant.numExecutorPerMatchBolt;
        int numWorkers=TypeConstant.numWorkers;
        int numAckers=TypeConstant.numAckers;
        int maxTaskParallelism=TypeConstant.maxTaskParallelism;
        int parallelismDegree = TypeConstant.parallelismDegree;
        int redundancy = TypeConstant.redundancy;
        int dataDistributionType = TypeConstant.TYPE;
        int numMatchGroup = TypeConstant.numMatchGroup;
        int boltID = 0;
        String boltNameHead="ReinGRMBoltG";

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("SubSpout", new SubscriptionSpout(dataDistributionType), numExecutorInASpout);
//        builder.setSpout("SubscriptionSpout_emitDirect",new SubscriptionSpout_emitDirect(dataDistributionType,utils.getNumVisualSubSet(),utils.getVSSIDtoExecutorID()),numExecutorInASpout);
        builder.setSpout("EventSpout", new EventSpout(dataDistributionType), numExecutorInASpout);

        for(int groupID = 0;groupID<numMatchGroup;groupID++)
            for (int executorID=0; executorID < redundancy; executorID++)
                builder.setBolt(boltNameHead+groupID+"B" + String.valueOf(executorID), new GroupRedundancyMatchBolt(groupID, boltID++,executorID), numExecutorInAMatchBolt).allGrouping("SubSpout").allGrouping("EventSpout");

        boltID++;
        builder.setBolt("GRMergerBolt"+boltID, new GroupRedundancyMergerBolt(boltID), 1).allGrouping("ReinGRMBoltG0B0").allGrouping("ReinGRMBoltG0B1").allGrouping("ReinGRMBoltG0B2").allGrouping("ReinGRMBoltG1B0").allGrouping("ReinGRMBoltG1B1").allGrouping("ReinGRMBoltG1B2");
//        BoltDeclarer boltDeclarer=builder.setBolt("GRMergerBolt"+boltID, new GroupRedundancyMergerBolt(boltID), 1);
//        for(int groupID=0;groupID<numMatchGroup;groupID++)
//            for (int executorID=0; executorID < redundancy; executorID++)
//                boltDeclarer.allGrouping(boltNameHead+groupID+"B" + String.valueOf(executorID));

        Config conf = new Config();
        Config.setFallBackOnJavaSerialization(conf, false); // Don't use java's serialization.
        conf.registerSerialization(OutputToFile.class);
        conf.registerSerialization(Pair.class);
        conf.registerSerialization(Event.class);
        conf.registerSerialization(Subscription.class);
        conf.registerSerialization(Double.class);
        conf.registerSerialization(BitSet.class);
        conf.registerSerialization(long[].class);

        conf.setDebug(false); // True will print all sub, event and match data.
        conf.setNumWorkers(numWorkers);
        conf.setMaxTaskParallelism(maxTaskParallelism);
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, numAckers);            // 设置acker的数量, default: 1
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 90);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 100000);            //设置一个spout task上面最多有多少个没有处理的tuple（没有ack/failed）回复，以防止tuple队列爆掉
        conf.put(Config.TOPOLOGY_EXECUTOR_OVERFLOW_LIMIT, 100000);      // If number of items in task’s overflowQ exceeds this, new messages coming from other workers to this task will be dropped This prevents OutOfMemoryException that can occur in rare scenarios in the presence of BackPressure.
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 100000); // 262144*2 8192*32
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 6000);      // A per topology config that specifies the maximum amount of memory a worker can use for that specific topology
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 4048);                 // The default heap memory size in MB per worker, used in the jvm -Xmx opts for launching the worker
//        conf.put(Config.NIMBUS_SUPERVISOR_USERS,);
        conf.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 2000);         // The total amount of memory (in MiB) a supervisor is allowed to give to its workers.
        conf.put(Config.SUPERVISOR_QUEUE_SIZE, 100000);
//        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,8192); // 无法识别
//        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE,32);// 接收线程缓存消息的大小 // 无法识别
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 100000);         // 进程中向外发送消息的缓存大小 The size of the Disruptor transfer queue for each worker.
        // conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS,"-Xmx%HEAP-MEM%m -XX:+PrintGCDetails -Xloggc:artifacts/gc.log  -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=1M -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=artifacts/heapdump");
        conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 2000); // The maximum amount of memory an instance of a spout/bolt will take on heap. This enables the scheduler to allocate slots on machines with enough available memory.
        conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 2000);
        conf.put(Config.TOPOLOGY_ACKER_RESOURCES_ONHEAP_MEMORY_MB, 400);
        conf.put(Config.TOPOLOGY_ACKER_RESOURCES_OFFHEAP_MEMORY_MB, 400);
        // -XX:+PrintGCDateStamps is omitted, because it will lead to a log: "[INFO] Unrecognized VM option 'PrintGCDateStamps'"
        String topoName = "ClusterMatchTopology";
        if (args != null && args.length > 0) {
            topoName = args[0];
        }

//        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
//        System.out.println("StormSubmitter over.");
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(topoName, conf, builder.createTopology());
        Utils.sleep(7200000);
        localCluster.killTopology(topoName);
        localCluster.shutdown();
    }
}
