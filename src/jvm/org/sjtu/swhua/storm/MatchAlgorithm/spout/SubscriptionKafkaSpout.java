package org.sjtu.swhua.storm.MatchAlgorithm.spout;

import java.io.IOException;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.*;
import com.alibaba.fastjson.JSON;


public class SubscriptionKafkaSpout extends BaseRichSpout {
    //    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionSpout.class);
    private SpoutOutputCollector collector;
    private TopologyContext subSpoutTopologyContext;
    private int subID;
    private int numSubPacket;
    private final int subSetSize;

    private OutputToFile output;
    private StringBuilder log;
    private StringBuilder errorLog;
    private String spoutName;

    private final String topicName;
    Properties props;
    @SuppressWarnings("resource")
    private KafkaConsumer<String, Object> subscriptionConsumer;

    private HashMap<Integer, ArrayList<Subscription>> tupleUnacked;  // backup data

    public SubscriptionKafkaSpout() {
        subSetSize = TypeConstant.subSetSize;
        topicName = "subscription"; // 常量需在构造函数里赋值
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        subID = 1;
        numSubPacket = 0;
        subSpoutTopologyContext = topologyContext;
        spoutName = "Kafka-" + subSpoutTopologyContext.getThisComponentId();
        collector = spoutOutputCollector;
        output = new OutputToFile();
        tupleUnacked = new HashMap<>();
        try {
            log = new StringBuilder(spoutName);
            log.append(" ThreadNum: " + Thread.currentThread().getName() + "\n" + spoutName + ":");
            List<Integer> taskIds = subSpoutTopologyContext.getComponentTasks(spoutName);
            Iterator taskIdsIter = taskIds.iterator();
            while (taskIdsIter.hasNext())
                log.append(" " + String.valueOf(taskIdsIter.next()));
            log.append("\nThisTaskId: ");
            log.append(subSpoutTopologyContext.getThisTaskId() + "\n\n");  // Get the current thread number
            output.otherInfo(log.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Kafka consumer configuration settings
        props = new Properties();
        props.put("bootstrap.servers", "swhua:9092");
        props.put("group.id", "SubscriptionsKafkaSpout");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.sjtu.swhua.storm.MatchAlgorithm.serialization.KafkaDeserializer");
        subscriptionConsumer = new KafkaConsumer<String, Object>(props);
        //Kafka Consumer subscribes list of topics here.
        subscriptionConsumer.subscribe(Arrays.asList(topicName));
    }

    @Override
    public void nextTuple() {
//        Utils.sleep(5);
        if (subID >= subSetSize) {
//            collector.emit(new Values(TypeConstant.Null_Operation, null));
            return;
        }

//        while (true) {
        ArrayList<Subscription> subscriptions;
        ConsumerRecords<String, Object> records = subscriptionConsumer.poll(100); // 拿多少出来？　所有?
        for (ConsumerRecord<String, Object> record : records) {
            // print the offset,key and value for the consumer records.
//            System.out.printf("offset = %d, key = %s, value = %s\n",
//                    record.offset(), record.key(), record.value());
            subscriptions = new ArrayList<>();
            subscriptions.addAll(JSON.parseArray(record.value().toString(), Subscription.class));
            subID = subscriptions.get(subscriptions.size() - 1).getSubID();
            numSubPacket++;
            try {
                log = new StringBuilder(spoutName);
                log.append(": SubID ");
                log.append(subID);
                log.append(" in SubPacket ");
                log.append(numSubPacket);
                log.append(" from KafkaSubscriptionPacket ");
                log.append(record.key());
                log.append(" is sent.\n");
                output.writeToLogFile(log.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
            tupleUnacked.put(numSubPacket, subscriptions);
            collector.emit(new Values(TypeConstant.Insert_Subscription, numSubPacket, subscriptions), numSubPacket);
        }
//        }
    }

    @Override
    public void ack(Object packetID) {
//        LOG.debug("Got ACK for msgId : ");
//        log = new StringBuilder(spoutName);
//        log.append(": SubTuple ");
//        log.append(id);
//        log.append(" is acked.\n");
//        try {
//            output.writeToLogFile(log.toString());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        tupleUnacked.remove((int) packetID);
    }

    @Override
    public void fail(Object packetID) {
        errorLog = new StringBuilder(spoutName);
        errorLog.append(": SubTuple ");
        errorLog.append(packetID);
        errorLog.append(" is failed.\n");
        try {
            output.errorLog(errorLog.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        collector.emit(new Values(TypeConstant.Insert_Subscription, numSubPacket, tupleUnacked.get(packetID)), numSubPacket);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Type", "PacketID", "SubscriptionPacket"));
    }
}

