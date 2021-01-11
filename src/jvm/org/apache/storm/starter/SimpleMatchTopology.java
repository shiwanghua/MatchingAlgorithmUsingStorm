package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.starter.DataStructure.Event;
import org.apache.storm.starter.DataStructure.OutputToFile;
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
        builder.setBolt("bolt", new SimpleMatchBolt(), 1).shuffleGrouping("EventSpout").allGrouping("SubSpout");

        Config conf = new Config();
        conf.registerSerialization(OutputToFile.class);
        conf.registerSerialization(Event.class);
        conf.registerSerialization(Subscription.class);
        conf.setDebug(true);
        String topoName = "SimpleMatchTopology";
        if (args != null && args.length > 0) {
            topoName = args[0];
        }
        conf.setNumWorkers(2);
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
    }
}
