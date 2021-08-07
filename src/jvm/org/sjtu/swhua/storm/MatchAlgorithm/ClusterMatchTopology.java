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

//storm local ./storm-2021-5-4.jar  org.sjtu.swhua.storm.MatchAlgorithm.ClusterMatchTopology
public class ClusterMatchTopology {
    public static void main(String[] args) throws Exception {

        int numExecutorInASpout = TypeConstant.numExecutorPerSpout;
        int numExecutorInAMatchBolt = TypeConstant.numExecutorPerMatchBolt;
        int parallelismDegree = TypeConstant.parallelismDegree;
        int redundancy = TypeConstant.redundancy;
        int dataDistributionType = TypeConstant.TYPE;
        int numMatchGroup = TypeConstant.numMatchGroup;
        int boltId = 0;
        int groupID=0;

        MyUtils utils = new MyUtils(parallelismDegree, redundancy);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("SubSpout", new SubscriptionSpout(dataDistributionType), numExecutorInASpout);
//        builder.setSpout("SubscriptionSpout_emitDirect",new SubscriptionSpout_emitDirect(dataDistributionType,utils.getNumVisualSubSet(),utils.getVSSIDtoExecutorID()),numExecutorInASpout);
//        builder.setSpout("EventSpout", new EventSpout(dataDistributionType, numMatchGroup), numExecutorInASpout);

        for (; boltId <parallelismDegree;boltId++){
//            builder.setBolt("TamaMPMBolt"+String.valueOf(boltId), new TamaMPMatchBolt(groupID,boltId, numExecutorInAMatchBolt, redundancy, utils.getNumVisualSubSet(), utils.getVSSIDtoExecutorID()), numExecutorInAMatchBolt).directGrouping("SubscriptionSpout_emitDirect").allGrouping("EventSpout");
            builder.setBolt("ReinMPMBolt"+String.valueOf(boltId), new ReinMPMatchBolt(groupID,boltId, numExecutorInAMatchBolt, redundancy, utils.getNumVisualSubSet(), utils.getVSSIDtoExecutorID()), numExecutorInAMatchBolt).allGrouping("SubSpout");
        }
//        builder.setBolt("ReinMPMBolt"+String.valueOf(boltId), new ReinMPMatchBolt(groupID,boltId, numExecutorInAMatchBolt, redundancy, utils.getNumVisualSubSet(), utils.getVSSIDtoExecutorID()), numExecutorInAMatchBolt).allGrouping("SubSpout");
//        builder.setBolt("TamaMPMBolt0", new TamaMPMatchBolt(groupID,boltId++, numExecutorInAMatchBolt, redundancy, utils.getNumVisualSubSet(), utils.getVSSIDtoExecutorID()), numExecutorInAMatchBolt).allGrouping("SubSpout").allGrouping("EventSpout");
//        builder.setBolt("MPMergerBolt0", new MultiPartitionMergerBolt(parallelismDegree, redundancy, utils.getExecutorCombination()), 1).allGrouping("TamaMPMBolt0").allGrouping("TamaMPMBolt1").allGrouping("TamaMPMBolt2").allGrouping("TamaMPMBolt3").allGrouping("TamaMPMBolt4").allGrouping("TamaMPMBolt5");
        builder.setBolt("MPMergerBolt0", new MultiPartitionMergerBolt(parallelismDegree, redundancy, utils.getExecutorCombination()), 1).allGrouping("ReinMPMBolt0").allGrouping("ReinMPMBolt1").allGrouping("ReinMPMBolt2").allGrouping("ReinMPMBolt3").allGrouping("ReinMPMBolt4").allGrouping("ReinMPMBolt5");

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
//        conf.registerSerialization(Rein.class);

        conf.setDebug(false); // True will print all sub, event and match data.
        conf.setNumWorkers(6);
        conf.setMaxTaskParallelism(16);
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 10);// 设置acker的数量, default: 1
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 90);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1000);//设置一个spout task上面最多有多少个没有处理的tuple（没有ack/failed）回复，以防止tuple队列爆掉
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 262144*2); // 8192*32
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 9192); // A per topology config that specifies the maximum amount of memory a worker can use for that specific topology
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 4096); // The default heap memory size in MB per worker, used in the jvm -Xmx opts for launching the worker
//        conf.put(Config.NIMBUS_SUPERVISOR_USERS,);
        conf.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1024); // The total amount of memory (in MiB) a supervisor is allowed to give to its workers.
//        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,8192); // 无法识别
//        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE,32);// 接收线程缓存消息的大小 // 无法识别
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 1024); // 进程中向外发送消息的缓存大小 The size of the Disruptor transfer queue for each worker.
        // conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS,"-Xmx%HEAP-MEM%m -XX:+PrintGCDetails -Xloggc:artifacts/gc.log  -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=1M -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=artifacts/heapdump");

        // -XX:+PrintGCDateStamps is omitted, because it will lead to a log: "[INFO] Unrecognized VM option 'PrintGCDateStamps'"
        String topoName = "SimpleMatchTopology";
        if (args != null && args.length > 0) {
            topoName = args[0];
        }

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(topoName, conf, builder.createTopology());
//        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
        Utils.sleep(7200000);
        localCluster.killTopology(topoName);
        localCluster.shutdown();
    }
}
