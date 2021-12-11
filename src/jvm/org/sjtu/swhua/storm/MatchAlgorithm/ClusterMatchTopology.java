package org.sjtu.swhua.storm.MatchAlgorithm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.*;
import org.sjtu.swhua.storm.MatchAlgorithm.bolt.MultiPartitionMergerBolt;
import org.sjtu.swhua.storm.MatchAlgorithm.bolt.ReinMPMatchBolt;
import org.sjtu.swhua.storm.MatchAlgorithm.bolt.TamaMPMatchBolt;
import org.sjtu.swhua.storm.MatchAlgorithm.spout.EventSpout;
import org.sjtu.swhua.storm.MatchAlgorithm.spout.SubscriptionSpout;
import org.sjtu.swhua.storm.MatchAlgorithm.spout.SubscriptionSpout_emitDirect;
import java.util.BitSet;

//storm local target/storm-2021-11-21.jar  org.sjtu.swhua.storm.MatchAlgorithm.ClusterMatchTopology
// storm jar target/storm-2021-11-21.jar  org.sjtu.swhua.storm.MatchAlgorithm.ClusterMatchTopology
public class ClusterMatchTopology {
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
        int boltId = 0;
        int groupID = 0;

        MyUtils utils = new MyUtils(parallelismDegree, redundancy);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("SubSpout", new SubscriptionSpout(dataDistributionType), numExecutorInASpout);
//        builder.setSpout("SubscriptionSpout_emitDirect",new SubscriptionSpout_emitDirect(dataDistributionType,utils.getNumVisualSubSet(),utils.getVSSIDtoExecutorID()),numExecutorInASpout);
        builder.setSpout("EventSpout", new EventSpout(dataDistributionType), numExecutorInASpout);

        for (; boltId < parallelismDegree; boltId++) {
//            builder.setBolt("TamaMPMBolt"+String.valueOf(boltId), new TamaMPMatchBolt(groupID,boltId, numExecutorInAMatchBolt, redundancy, utils.getNumVisualSubSet(), utils.getVSSIDtoExecutorID()), numExecutorInAMatchBolt).directGrouping("SubscriptionSpout_emitDirect").allGrouping("EventSpout");
            builder.setBolt("ReinMPMBolt" + String.valueOf(boltId), new ReinMPMatchBolt(groupID, boltId, numExecutorInAMatchBolt, redundancy, utils.getNumVisualSubSet(), utils.getVSSIDtoExecutorID()), numExecutorInAMatchBolt).allGrouping("SubSpout").allGrouping("EventSpout");
        }
//        builder.setBolt("ReinMPMBolt"+String.valueOf(boltId), new ReinMPMatchBolt(groupID,boltId, numExecutorInAMatchBolt, redundancy, utils.getNumVisualSubSet(), utils.getVSSIDtoExecutorID()), numExecutorInAMatchBolt).allGrouping("SubSpout");
//        builder.setBolt("TamaMPMBolt0", new TamaMPMatchBolt(groupID,boltId++, numExecutorInAMatchBolt, redundancy, utils.getNumVisualSubSet(), utils.getVSSIDtoExecutorID()), numExecutorInAMatchBolt).allGrouping("SubSpout").allGrouping("EventSpout");
//        builder.setBolt("MPMergerBolt0", new MultiPartitionMergerBolt(parallelismDegree, redundancy, utils.getExecutorCombination()), 1).allGrouping("TamaMPMBolt0").allGrouping("TamaMPMBolt1").allGrouping("TamaMPMBolt2").allGrouping("TamaMPMBolt3").allGrouping("TamaMPMBolt4").allGrouping("TamaMPMBolt5");
        builder.setBolt("MPMergerBolt0", new MultiPartitionMergerBolt(parallelismDegree, redundancy, utils.getExecutorCombination()), 1).allGrouping("ReinMPMBolt0").allGrouping("ReinMPMBolt1").allGrouping("ReinMPMBolt2").allGrouping("ReinMPMBolt3").allGrouping("ReinMPMBolt4").allGrouping("ReinMPMBolt5");//.allGrouping("ReinMPMBolt6").allGrouping("ReinMPMBolt7").allGrouping("ReinMPMBolt8").allGrouping("ReinMPMBolt9").allGrouping("ReinMPMBolt10").allGrouping("ReinMPMBolt11").allGrouping("ReinMPMBolt12").allGrouping("ReinMPMBolt13").allGrouping("ReinMPMBolt14");

//        builder.setBolt("ReinMPMBolt0", new ReinMPMatchBolt(groupID,boltId++,numExecutorInAMatchBolt, redundancy, utils.getNumVisualSubSet(), utils.getVSSIDtoExecutorID()), numExecutorInAMatchBolt).allGrouping("SubSpout").allGrouping("EventSpout");//.setNumTasks(2);
//        builder.setBolt("ReinMPMBolt1", new ReinMPMatchBolt(groupID,boltId++,numExecutorInAMatchBolt, redundancy, utils.getNumVisualSubSet(), utils.getVSSIDtoExecutorID()), numExecutorInAMatchBolt).allGrouping("SubSpout").allGrouping("EventSpout");//.setNumTasks(2);
//        builder.setBolt("MPMergerBolt0", new MultiPartitionMergerBolt(numExecutorInAMatchBolt, redundancy, utils.getExecutorCombination()), 1).allGrouping("ReinMPMBolt0");
//        builder.setBolt("MPMergerBolt1", new MultiPartitionMergerBolt(numExecutorInAMatchBolt, redundancy, utils.getExecutorCombination()), 1).allGrouping("ReinMPMBolt1");

//        builder.setBolt("MPMBolt1", new MultiPartitionMatchBolt(groupID,0,numExecutorInAMatchBolt, redundancy, utils.getNumVisualSubSet(), utils.getVSSIDtoExecutorID()),numExecutorInAMatchBolt).allGrouping("SubSpout").allGrouping("EventSpout");
//        builder.setBolt("MergerBolt1",new MultiPartitionMergerBolt(numExecutorInAMatchBolt,redundancy,utils.getExecutorCombination()),1).allGrouping("MPMBolt1");

        Config conf = new Config();
        Config.setFallBackOnJavaSerialization(conf, false); // Don't use java's serialization.
        conf.registerSerialization(OutputToFile.class);
        conf.registerSerialization(Pair.class);
        conf.registerSerialization(Event.class);
        conf.registerSerialization(Subscription.class);
        conf.registerSerialization(Double.class);
        conf.registerSerialization(BitSet.class);
        conf.registerSerialization(long[].class);
//        conf.registerSerialization(Rein.class);

        conf.setDebug(false); // True will print all sub, event and match data.
        conf.setNumWorkers(numWorkers);
        conf.setMaxTaskParallelism(maxTaskParallelism);
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, numAckers);                 // 设置acker的数量, default: 1
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 120);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1000);            //设置一个spout task上面最多有多少个没有处理的tuple（没有ack/failed）回复，以防止tuple队列爆掉
        conf.put(Config.TOPOLOGY_EXECUTOR_OVERFLOW_LIMIT, 1000);      // If number of items in task’s overflowQ exceeds this, new messages coming from other workers to this task will be dropped This prevents OutOfMemoryException that can occur in rare scenarios in the presence of BackPressure.
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1000); // 262144*2 8192*32
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 6000);      // A per topology config that specifies the maximum amount of memory a worker can use for that specific topology
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 5000);                 // The default heap memory size in MB per worker, used in the jvm -Xmx opts for launching the worker
//        conf.put(Config.NIMBUS_SUPERVISOR_USERS,);
        conf.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1024);         // The total amount of memory (in MiB) a supervisor is allowed to give to its workers.
        conf.put(Config.SUPERVISOR_QUEUE_SIZE, 1000);
//        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,8192); // 无法识别
//        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE,32);// 接收线程缓存消息的大小 // 无法识别
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 1024);         // 进程中向外发送消息的缓存大小 The size of the Disruptor transfer queue for each worker.
        // conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS,"-Xmx%HEAP-MEM%m -XX:+PrintGCDetails -Xloggc:artifacts/gc.log  -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=1M -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=artifacts/heapdump");
        conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 1500); // The maximum amount of memory an instance of a spout/bolt will take on heap. This enables the scheduler to allocate slots on machines with enough available memory.
        conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 1500);
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
