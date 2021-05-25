package org.sjtu.swhua.storm.MatchAlgorithm;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
//import org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

//参考如下
//https://community.hortonworks.com/articles/87597/how-to-write-topology-with-the-new-kafka-spout-cli.html
//https://github.com/apache/storm/blob/master/examples/storm-kafka-client-examples/src/main/java/org/apache/storm/kafka/spout/KafkaSpoutTopologyMainNamedTopics.java#L52


// KafkaSpout  spout读取主题的消息发给bolt消费
public class Test2 {
     static class MyboltO extends BaseRichBolt {
        private static final long serialVersionUID = 1L;
        OutputCollector collector = null;

        public MyboltO(){}

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        public void execute(Tuple input) {
            //这里把消息大一出来，在对应的woker下面的日志可以找到打印的内容
            String out = input.getString(1);
            System.out.println("mybolt0:"+out);
            //collector.ack(input);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        //该类将传入的kafka记录转换为storm的tuple
        ByTopicRecordTranslator<String, String> brt =
                new ByTopicRecordTranslator<>((r) -> new Values(r.value(), r.topic()), new Fields("values", "test7"));
        //设置要消费的topic即test7
        brt.forTopic("event", (r) -> new Values(r.value(), r.topic()), new Fields("values", "test7"));
        //类似之前的SpoutConfig
        KafkaSpoutConfig<String, String> ksc = KafkaSpoutConfig
                //bootstrapServers 以及topic(event)
                .builder("swhua:9092", "event")
                //设置group.id
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "test2") // 消费者名称
//                //设置开始消费的气势位置
//                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.LATEST)
                //设置提交消费边界的时长间隔
                .setOffsetCommitPeriodMs(10_000)
                //Translator
                .setRecordTranslator(brt)
                .build();

        builder.setSpout("kafkaspout", new KafkaSpout<>(ksc), 2);
        builder.setBolt("mybolt1", new MyboltO(), 4).shuffleGrouping("kafkaspout");

        Config config = new Config();
        config.setNumWorkers(2);
        config.setNumAckers(0);
        try {
            LocalCluster cu  = new LocalCluster();
            cu.submitTopology("test", config, builder.createTopology());
//            StormSubmitter.submitTopology("storm-kafka-clients", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}

