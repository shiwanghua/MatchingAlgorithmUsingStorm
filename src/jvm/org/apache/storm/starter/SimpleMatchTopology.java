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

        builder.setSpout("SubSpout", new SubscriptionSpout(), 1);
        builder.setSpout("EventSpout", new EventSpout(), 1);
        builder.setBolt("bolt", new SimpleMatchBolt("1"), 1).allGrouping("SubSpout").shuffleGrouping("EventSpout");

        Config conf = new Config();
        Config.setFallBackOnJavaSerialization(conf, false); // Don't use java's serialization.
        conf.registerSerialization(OutputToFile.class);
        conf.registerSerialization(Pair.class);
        conf.registerSerialization(Event.class);
        conf.registerSerialization(Subscription.class);

        conf.setDebug(false);
        conf.setNumWorkers(3);
        conf.setMaxTaskParallelism(1);

        String topoName = "SimpleMatchTopology";
        if (args != null && args.length > 0) {
            topoName = args[0];
        }

        LocalCluster localCluster=new LocalCluster();
        localCluster.submitTopology(topoName,conf,builder.createTopology());
//        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
    }
}
