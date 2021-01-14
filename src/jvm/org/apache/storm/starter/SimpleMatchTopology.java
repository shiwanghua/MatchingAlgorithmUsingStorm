package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.starter.DataStructure.Event;
import org.apache.storm.starter.DataStructure.OutputToFile;
import org.apache.storm.starter.DataStructure.Pair;
import org.apache.storm.starter.DataStructure.Subscription;
import org.apache.storm.starter.bolt.SimpleMatchBolt;
import org.apache.storm.starter.spout.EventSpout;
import org.apache.storm.starter.spout.SubscriptionSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class SimpleMatchTopology {
    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("SubSpout", new SubscriptionSpout("SubSpout1"), 1);
        builder.setSpout("EventSpout", new EventSpout("EventSpout1"), 1);
        builder.setBolt("SMBolt", new SimpleMatchBolt("SimpleMatchBolt1"), 4).allGrouping("SubSpout").shuffleGrouping("EventSpout");

        Config conf = new Config();
        Config.setFallBackOnJavaSerialization(conf, false); // Don't use java's serialization.
        conf.registerSerialization(OutputToFile.class);
        conf.registerSerialization(Pair.class);
        conf.registerSerialization(Event.class);
        conf.registerSerialization(Subscription.class);

        conf.setDebug(false);
        conf.setNumWorkers(4);
        conf.setMaxTaskParallelism(3);
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 20);// 设置acker的数量, default: 1
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 100);//设置一个spout task上面最多有多少个没有处理的tuple（没有ack/failed）回复，以防止tuple队列爆掉
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS,"-Xmx%HEAP-MEM%m -XX:+PrintGCDetails -Xloggc:artifacts/gc.log  -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=1M -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=artifacts/heapdump");
        // -XX:+PrintGCDateStamps is omitted, because it will lead to a log: "[INFO] Unrecognized VM option 'PrintGCDateStamps'"
        String topoName = "SimpleMatchTopology";
        if (args != null && args.length > 0) {
            topoName = args[0];
        }


//        LocalCluster localCluster=new LocalCluster();
//        localCluster.submitTopology(topoName,conf,builder.createTopology());
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
    }
}
