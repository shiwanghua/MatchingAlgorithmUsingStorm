package org.sjtu.swhua.storm.MatchAlgorithm;

import clojure.lang.Compiler;
import kafka.admin.ConsumerGroupCommand;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.*;
import org.sjtu.swhua.storm.MatchAlgorithm.bolt.*;
import org.sjtu.swhua.storm.MatchAlgorithm.spout.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

//storm local ./storm-2021-5-4.jar  org.sjtu.swhua.storm.MatchAlgorithm.SimpleMatchTopology
public class SimpleMatchTopology {
    public static void main(String[] args) throws Exception {

        int numExecutorInASpout=TypeConstant.numExecutorPerSpout;
        int numExecutorInAMatchBolt = TypeConstant.numExecutorPerMatchBolt;
        int redundancy = TypeConstant.redundancy;
        int type=TypeConstant.TYPE;
        int numMatchBolt=TypeConstant.numMatchBolt;
        int boltId=0;

        MyUtils utils = new MyUtils(numExecutorInAMatchBolt, redundancy);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("SubSpout", new SubscriptionSpout(type), numExecutorInASpout);
        builder.setSpout("EventSpout", new EventSpout(type,numMatchBolt), numExecutorInASpout);

//        builder.setBolt("TamaMPMBolt0",new TamaMPMatchBolt(boltId++,numExecutorInAMatchBolt, redundancy, utils.getNumVisualSubSet(), utils.getVSSIDtoExecutorID()), numExecutorInAMatchBolt).allGrouping("SubSpout").allGrouping("EventSpout");
//        builder.setBolt("MPMergerBolt0", new MultiPartitionMergerBolt(numExecutorInAMatchBolt, redundancy, utils.getExecutorCombination()), 1).allGrouping("TamaMPMBolt0");

        builder.setBolt("ReinMPMBolt0", new ReinMPMatchBolt(boltId++,numExecutorInAMatchBolt, redundancy, utils.getNumVisualSubSet(), utils.getVSSIDtoExecutorID()), numExecutorInAMatchBolt).allGrouping("SubSpout").allGrouping("EventSpout");//.setNumTasks(2);
//        builder.setBolt("ReinMPMBolt1", new ReinMPMatchBolt(boltId++,numExecutorInAMatchBolt, redundancy, utils.getNumVisualSubSet(), utils.getVSSIDtoExecutorID()), numExecutorInAMatchBolt).allGrouping("SubSpout").allGrouping("EventSpout");//.setNumTasks(2);
        builder.setBolt("MPMergerBolt0", new MultiPartitionMergerBolt(numExecutorInAMatchBolt, redundancy, utils.getExecutorCombination()), 1).allGrouping("ReinMPMBolt0");
//        builder.setBolt("MPMergerBolt1", new MultiPartitionMergerBolt(numExecutorInAMatchBolt, redundancy, utils.getExecutorCombination()), 1).allGrouping("ReinMPMBolt1");

//        builder.setBolt("MPMBolt1", new MultiPartitionMatchBolt(0,numExecutorInAMatchBolt, redundancy, utils.getNumVisualSubSet(), utils.getVSSIDtoExecutorID()),numExecutorInAMatchBolt).allGrouping("SubSpout");
//        builder.setBolt("MergerBolt1",new MultiPartitionMergerBolt(numExecutorInAMatchBolt,redundancy,utils.getExecutorCombination()),1).allGrouping("MPMBolt1");

//        builder.setBolt("TDMBolt0", new ThreadDivisionMatchBolt(numExecutorInAMatchBolt),6).allGrouping("SubSpout").allGrouping("EventSpout");//.setNumTasks(2);
//        builder.setBolt("MergerBolt1",new MergerBolt(numExecutorInAMatchBolt),1).allGrouping("TDMBolt0");

//        builder.setBolt("TDMBolt1", new ThreadDivisionMatchBolt(),1).allGrouping("SubSpout").allGrouping("EventSpout");
//        builder.setBolt("TDMBolt2", new ThreadDivisionMatchBolt(),1).allGrouping("SubSpout").allGrouping("EventSpout");
//        builder.setBolt("TDMBolt3", new ThreadDivisionMatchBolt(),1).allGrouping("SubSpout").allGrouping("EventSpout");
//        builder.setBolt("SMBolt", new SimpleMatchBolt("SimpleMatchBolt1"), 1).shuffleGrouping("SubSpout").allGrouping("EventSpout");//
//        builder.setBolt("SMBolt", new SimpleMatchBolt("SimpleMatchBolt2"), 4).allGrouping("SubSpout").shuffleGrouping("EventSpout");

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
        conf.setMaxTaskParallelism(12);
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 10);// 设置acker的数量, default: 1
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 90);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1000);//设置一个spout task上面最多有多少个没有处理的tuple（没有ack/failed）回复，以防止tuple队列爆掉
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,262144); // 8192*32
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB,2048); // A per topology config that specifies the maximum amount of memory a worker can use for that specific topology
        conf.put(Config.WORKER_HEAP_MEMORY_MB,2048); // The default heap memory size in MB per worker, used in the jvm -Xmx opts for launching the worker
//        conf.put(Config.NIMBUS_SUPERVISOR_USERS,);
        conf.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB,512); // The total amount of memory (in MiB) a supervisor is allowed to give to its workers.
//        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,8192); // 无法识别
//        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE,32);// 接收线程缓存消息的大小 // 无法识别
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,256); // 进程中向外发送消息的缓存大小 The size of the Disruptor transfer queue for each worker.
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
