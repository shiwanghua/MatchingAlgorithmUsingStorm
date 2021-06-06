package org.sjtu.swhua.storm.MatchAlgorithm.spout;

import java.io.IOException;
import java.util.*;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.*;
import com.alibaba.fastjson.JSON;

// kafka consumer
public class EventKafkaSpout extends BaseRichSpout {
    //    private static final Logger LOG = LoggerFactory.getLogger(EventSpout.class);
    private SpoutOutputCollector collector;
    private TopologyContext eventSpoutTopologyContext;
    private int eventID;
    private int numEventPacket;
    private int numMatchBolt;
    private int nextMatchBoltID;

    private OutputToFile output;
    private StringBuilder log;
    private StringBuilder errorLog;
    private String spoutName;

    @SuppressWarnings("resource")
    private final String topicName = "event";
    private Properties props;
    private KafkaConsumer<String, Object> eventConsumer;

    private HashMap<Integer, ArrayList<Event>> tupleUnacked;


    public EventKafkaSpout(Integer num_match_bolt) {
        numMatchBolt = num_match_bolt;
    }

    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        eventID = 1;
        numEventPacket = 0;  // messageID、packetID
        nextMatchBoltID = -1;
        eventSpoutTopologyContext = topologyContext;
        spoutName = "Kafka-" + eventSpoutTopologyContext.getThisComponentId();
        collector = spoutOutputCollector;
        output = new OutputToFile();
        log = new StringBuilder();
        tupleUnacked = new HashMap<>();

        try {
            log = new StringBuilder(spoutName);
            log.append(" ThreadNum: " + Thread.currentThread().getName() + "\n" + spoutName + ":");
            List<Integer> taskIds = eventSpoutTopologyContext.getComponentTasks(spoutName);
            Iterator taskIdsIter = taskIds.iterator();
            while (taskIdsIter.hasNext())
                log.append(" " + String.valueOf(taskIdsIter.next()));
            log.append("\nThisTaskId: ");
            log.append(eventSpoutTopologyContext.getThisTaskId());  // Get the current thread number
            log.append("\n\n");
            output.otherInfo(log.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }

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
        eventConsumer = new KafkaConsumer<String, Object>(props);
        eventConsumer.subscribe(Arrays.asList(topicName));
    }

    @Override
    public void ack(Object packetID) {
//        LOG.debug("Got ACK for msgId : ");
//        log=new StringBuilder(spoutName);
//        log.append(": EventTuple ");
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
        errorLog.append(": EventTuple ");
        errorLog.append(packetID);
        errorLog.append(" is failed and re-emitted.\n");
        try {
            output.errorLog(errorLog.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        collector.emit(new Values(TypeConstant.Insert_Subscription, numEventPacket, tupleUnacked.get(packetID)), numEventPacket);
    }

    @Override
    public void nextTuple() {
//        if (eventID >= 1000) return;
//        if (eventID % 1000 == 0)
//            Utils.sleep(2000);

        ArrayList<Event> events;
//        while (true) {
        ConsumerRecords<String, Object> records = eventConsumer.poll(100);
        for (ConsumerRecord<String, Object> record : records) {
            // print the offset,key and value for the consumer records.
//            System.out.printf("offset = %d, key = %s, value = %s\n",
//                    record.offset(), record.key(), record.value());
            events = new ArrayList<>();
            events.addAll(JSON.parseArray(record.value().toString(), Event.class));
            eventID = events.get(events.size() - 1).getEventID();
            numEventPacket++;

            try {
                log = new StringBuilder(spoutName);
                log.append(": EventID ");
                log.append(eventID);
                log.append(" in EventPacket ");
                log.append(numEventPacket);
                log.append(" from KafkaEventPacket ");
                log.append(record.key());
                log.append(" is sent.\n");
                output.writeToLogFile(log.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
            nextMatchBoltID = (nextMatchBoltID + 1) % numMatchBolt;
            tupleUnacked.put(numEventPacket, events);
            collector.emit(new Values(nextMatchBoltID, TypeConstant.Event_Match_Subscription, numEventPacket, events), numEventPacket);
        }
//        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MatchBoltID", "Type", "PacketID", "EventPacket"));
    }
}
