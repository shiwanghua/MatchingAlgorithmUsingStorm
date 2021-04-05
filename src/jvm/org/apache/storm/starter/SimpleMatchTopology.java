package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.starter.DataStructure.Event;
import org.apache.storm.starter.DataStructure.OutputToFile;
import org.apache.storm.starter.DataStructure.Pair;
import org.apache.storm.starter.DataStructure.Subscription;
import org.apache.storm.starter.bolt.*;
import org.apache.storm.starter.spout.EventSpout;
import org.apache.storm.starter.spout.SubscriptionSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

//storm jar ./storm-2021-02-22.jar org.apache.storm.starter.SimpleMatchTopology
public class SimpleMatchTopology {
    public static void main(String[] args) throws Exception {

        Integer numExecutorInAMatchBolt=6;
        Integer redundancy=3;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("SubSpout", new SubscriptionSpout(), 1);
        builder.setSpout("EventSpout", new EventSpout(), 1);
//        builder.setBolt("ReinMPMBolt0", new ReinMPMatchBolt(numExecutorInAMatchBolt,redundancy),numExecutorInAMatchBolt).allGrouping("SubSpout").allGrouping("EventSpout");//.setNumTasks(2);
//        builder.setBolt("MPMBolt1", new MultiPartitionMatchBolt(numExecutorInAMatchBolt,redundancy),numExecutorInAMatchBolt).allGrouping("SubSpout").allGrouping("EventSpout");
//        builder.setBolt("MergerBolt0",new MergerBolt(numExecutorInAMatchBolt),1).allGrouping("ReinMPMBolt0");
//        builder.setBolt("MergerBolt1",new MergerBolt(numExecutorInAMatchBolt),1).allGrouping("MPMBolt1");
        builder.setBolt("TDMBolt0", new ThreadDivisionMatchBolt(numExecutorInAMatchBolt),4).allGrouping("SubSpout").allGrouping("EventSpout");//.setNumTasks(2);
        builder.setBolt("MergerBolt1",new MergerBolt(numExecutorInAMatchBolt),1).allGrouping("TDMBolt0");
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

        conf.setDebug(false);
        conf.setNumWorkers(9);
        conf.setMaxTaskParallelism(12);
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 12);// 设置acker的数量, default: 1
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,180);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 800);//设置一个spout task上面最多有多少个没有处理的tuple（没有ack/failed）回复，以防止tuple队列爆掉
        // conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS,"-Xmx%HEAP-MEM%m -XX:+PrintGCDetails -Xloggc:artifacts/gc.log  -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=1M -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=artifacts/heapdump");

        // -XX:+PrintGCDateStamps is omitted, because it will lead to a log: "[INFO] Unrecognized VM option 'PrintGCDateStamps'"
        String topoName = "SimpleMatchTopology";
        if (args != null && args.length > 0) {
            topoName = args[0];
        }

        LocalCluster localCluster=new LocalCluster();
        localCluster.submitTopology(topoName,conf,builder.createTopology());
//        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
        Utils.sleep(720000);
        localCluster.killTopology(topoName);
        localCluster.shutdown();
    }
}
