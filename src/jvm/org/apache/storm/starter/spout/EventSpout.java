package org.apache.storm.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.starter.DataStructure.Event;
import org.apache.storm.starter.DataStructure.OutputToFile;
import org.apache.storm.starter.DataStructure.TypeConstant;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.IOException;
import java.util.*;

public class EventSpout extends BaseRichSpout {
    //    private static final Logger LOG = LoggerFactory.getLogger(EventSpout.class);
    SpoutOutputCollector collector;
    private Random valueGenerator;
    private Integer eventID;
    private Integer numEventPacket;
    private Integer numMatchBolt;
    private Integer nextMatchBoltID;
    final int maxNumEvent;            //  Maximum number of event emitted per time
    final int maxNumAttribute;        //  Maxinum number of attributes in a event
    final int numAttributeType;       //  Type number of attributes
    private int[] randomPermutation;  //  To get the attribute name
    private OutputToFile output;
    private StringBuilder log;
    private StringBuilder errorLog;
    private String spoutName;
    TopologyContext eventSpoutTopologyContext;

    public EventSpout(Integer num_match_bolt) {
        maxNumEvent = TypeConstant.maxNumEventPerPacket;
        maxNumAttribute = TypeConstant.maxNumAttributePerEvent;
        numAttributeType=TypeConstant.numAttributeType;
        numMatchBolt=num_match_bolt;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        valueGenerator = new Random();
        eventID = 1;
        numEventPacket = 0;  // messageID
        nextMatchBoltID=-1;
        randomPermutation = new int[numAttributeType];
        for (int i = 0; i < numAttributeType; i++)
            randomPermutation[i] = i;

        eventSpoutTopologyContext = topologyContext;
        spoutName = eventSpoutTopologyContext.getThisComponentId();
        collector = spoutOutputCollector;
        output = new OutputToFile();
        log = new StringBuilder();

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
    }

    @Override
    public void ack(Object id) {
//        LOG.debug("Got ACK for msgId : ");
        log=new StringBuilder(spoutName);
        log.append(": EventTuple ");
        log.append(id);
        log.append(" is acked.\n");
        try {
            output.writeToLogFile(log.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void fail(Object id) {
        errorLog=new StringBuilder(spoutName);
        errorLog.append(": EventTuple ");
        errorLog.append(id);
        errorLog.append(" is failed.\n");
        try {
            output.errorLog(errorLog.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
//        Utils.sleep(100);
        int numEvent = (int) (Math.random() * maxNumEvent + 1); // Generate the number of subscriptions in this tuple: 1~maxNumEvent
        ArrayList<Event> events = new ArrayList<>(numEvent);

        for (int i = 0; i < numEvent; i++) {
            int numAttribute = new Random().nextInt(maxNumAttribute + 1); // Generate the number of attribute in this subscription: 0~maxNumAttribute

            Double eventValue;
            //String attributeName = "attributeName";

            for (int j = 0; j < numAttribute; j++) { // Use the first #numAttribute values of randomArray to create the attribute name
                int index = valueGenerator.nextInt(numAttributeType - j) + j;
                int temp = randomPermutation[j];
                randomPermutation[j] = randomPermutation[index];
                randomPermutation[index] = temp;
            }

            HashMap<Integer, Double> mapIDToValue = new HashMap<>();
            for (int j = 0; j < numAttribute; j++) {
                eventValue = valueGenerator.nextDouble();
//                mapNameToValue.put(attributeName + String.valueOf(randomPermutation[j]), eventValue);
                mapIDToValue.put(randomPermutation[j], eventValue);
            }
            try {
                events.add(new Event(eventID, numAttribute, mapIDToValue));
                eventID += 1;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // for test
//        Utils.sleep(5000);
//        try {
//
//            HashMap<String, Double> m1 = new HashMap<>();  // Match sub0,sub1,sub3
//            m1.put("name1", 0.06);
//            m1.put("name2", 0.14);
//            m1.put("name3", 0.25);
//            m1.put("name4", 0.35);
//            events.add(new Event(1, 4, m1));
//
//            HashMap<String, Double> m2 = new HashMap<>();   // Match null
//            m2.put("name3", 0.21);
//            m2.put("name4", 0.5);
//            events.add(new Event(2, 2, m2));
//
//            HashMap<String, Double> m3 = new HashMap<>(); // Match sub2
//            m3.put("name1", 0.46);
//            m3.put("name2", 0.54);
//            events.add(new Event(3, 2, m3));
//
//            HashMap<String, Double> m4 = new HashMap<>(); // Match sub2, sub3
//            m4.put("name1", 0.48);
//            m4.put("name2", 0.56);
//            m4.put("name3", 0.85);
//            events.add(new Event(4, 3, m4));
//
//            HashMap<String, Double> m5 = new HashMap<>(); // Match sub4
//            m5.put("name2", 0.18);
//            m5.put("name4", 0.75);
//            events.add(new Event(5, 2, m5));
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        numEventPacket++;
        try {
            log = new StringBuilder(spoutName);
            log.append(": EventID ");
            log.append(eventID);
            log.append(" in EventPacket ");
            log.append(numEventPacket);
            log.append(" is sent.\n");
            output.writeToLogFile(log.toString());
//            output.writeToLogFile(spoutName+": EventID "+String.valueOf(eventID)+" in EventPacket "+String.valueOf(++numEventPacket) +" is sent.\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        nextMatchBoltID=(nextMatchBoltID+1)%numMatchBolt;
        collector.emit(new Values(nextMatchBoltID,TypeConstant.Event_Match_Subscription, numEventPacket, events), numEventPacket);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MatchBoltID","Type", "PacketID", "EventPacket"));
    }
}