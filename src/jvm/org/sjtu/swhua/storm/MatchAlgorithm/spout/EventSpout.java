package org.sjtu.swhua.storm.MatchAlgorithm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.*;
//import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Event;
//import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.OutputToFile;
//import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Subscription;
//import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.TypeConstant;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.*;

public class EventSpout extends BaseRichSpout {
    //    private static final Logger LOG = LoggerFactory.getLogger(EventSpout.class);
    private SpoutOutputCollector collector;
    private int eventID;
    private int numEventPacket;
    private int numMatchBolt;
    private int nextMatchBoltID;
    private final int type;
    private final int maxNumEvent;            //  Maximum number of event emitted per time
    private final int maxNumAttribute_Simple; //  Maxinum number of attributes in a event
    private final int minNumAttribute_Rein;
    private final int maxNumAttribute_Tama;
    private final int numAttributeType;       //  Type number of attributes
    private int[] randomPermutation;  //  To get the attribute name
    private Random valueGenerator;
    private OutputToFile output;
    private StringBuilder log;
    private StringBuilder errorLog;
    private String spoutName;
    private TopologyContext eventSpoutTopologyContext;
    private HashMap<Integer, ArrayList<Event>> tupleUnacked;  // backup data

    private final long beginTime; //

    public EventSpout(int Type, int num_match_bolt) {
        type = Type;
        numMatchBolt = num_match_bolt;
        maxNumEvent = TypeConstant.maxNumEventPerPacket;
        maxNumAttribute_Simple = TypeConstant.maxNumAttributePerEvent_Simple;
        minNumAttribute_Rein = TypeConstant.minNumAttributePerEvent_Rein;
        maxNumAttribute_Tama = TypeConstant.maxNumAttributePerEvent_Tama;
        numAttributeType = TypeConstant.numAttributeType;
        beginTime = System.nanoTime()+60000000000L;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        eventID = 1;
        numEventPacket = 0;  // messageID、packetID
        nextMatchBoltID = -1;
        randomPermutation = new int[numAttributeType];
        for (int i = 0; i < numAttributeType; i++)
            randomPermutation[i] = i;
        valueGenerator = new Random();
        eventSpoutTopologyContext = topologyContext;
        spoutName = eventSpoutTopologyContext.getThisComponentId();
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
    }

    @Override
    public void ack(Object packetID) {
//        LOG.debug("Got ACK for msgId : ");
        log = new StringBuilder(spoutName);
        log.append(": EventTuple ");
        log.append((int) packetID);
        log.append(" is acked.\n");
        try {
            output.writeToLogFile(log.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        tupleUnacked.remove((int) packetID);
    }

    @Override
    public void fail(Object packetID) {
        errorLog = new StringBuilder(spoutName);
        errorLog.append(": EventTuple ");
        errorLog.append((int) packetID);
        errorLog.append(" is failed and re-emitted.\n");
        try {
            output.errorLog(errorLog.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 只能处理一组match-bolt的情况，即0号bolt组
        collector.emit(new Values(0, TypeConstant.Event_Match_Subscription, (int) packetID, tupleUnacked.get(packetID)), packetID);// 最后一个参数是numEventPacket时会一直死循环地处理最后这个超时的事件，换汤不换药
    }

    // 只发同一个事件
//    @Override
//    public void nextTuple() {
//        int numEvent = 1;
//        ArrayList<Event> events = new ArrayList<>(numEvent);
//
//        for (int i = 0; i < numEvent; i++) {
//            Double eventValue;
//            HashMap<Integer, Double> mapIDToValue = new HashMap<>();
//            for (int j = 0; j < 10; j++) {
//                eventValue = 0.5;//valueGenerator.nextDouble();
////                mapNameToValue.put(attributeName + String.valueOf(randomPermutation[j]), eventValue);
//                mapIDToValue.put(j, eventValue);
//            }
//            try {
//                events.add(new Event(eventID, 10, mapIDToValue));
//                eventID += 1;
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
////        numEventPacket++;
////        try {
////            log = new StringBuilder(spoutName);
////            log.append(": EventID ");
////            log.append(eventID);
////            log.append(" in EventPacket ");
////            log.append(numEventPacket);
////            log.append(" is sent.\n");
////            output.writeToLogFile(log.toString());
//////            output.writeToLogFile(spoutName+": EventID "+String.valueOf(eventID)+" in EventPacket "+String.valueOf(++numEventPacket) +" is sent.\n");
////        } catch (IOException e) {
////            e.printStackTrace();
////        }
////        nextMatchBoltID = (nextMatchBoltID + 1) % numMatchBolt;
////        tupleUnacked.put(1, events);
//        collector.emit(new Values(0, TypeConstant.Event_Match_Subscription, 1, events), 1);
//    }

    @Override
    public void nextTuple() {
        if(System.nanoTime()<beginTime)
        {
            log = new StringBuilder(spoutName);
            log.append(": Waiting sub inserted. \n");
            try {
                output.writeToLogFile(log.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }
//        if(eventID>=11000) return;
//        if (eventID % 1000 == 0)
//            Utils.sleep(2000);
        int numEvent = (int) (Math.random() * maxNumEvent + 1); // Generate the number of subscriptions in this tuple: 1~maxNumEvent
        ArrayList<Event> events = new ArrayList<>(numEvent);

        for (int i = 0; i < numEvent; i++) {
            int numAttribute; // Generate the number of attribute in this event
            switch (type) {
                case TypeConstant.SIMPLE:
                    numAttribute = new Random().nextInt(maxNumAttribute_Simple + 1); // 0~maxNumAttribute_Simple
                    break;
                case TypeConstant.REIN:
                    numAttribute = minNumAttribute_Rein + new Random().nextInt(numAttributeType - minNumAttribute_Rein + 1); // minNumAttribute_Rein~numAttributeType
                    break;
                case TypeConstant.TAMA:
                    numAttribute = new Random().nextInt(maxNumAttribute_Tama + 1); // 0~maxNumAttribute_Tama
                    break;
                default:
                    numAttribute=0;
                    System.out.println("Error: algorithm type.\n");
            }
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
//        try {
//            log = new StringBuilder(spoutName);
//            log.append(": EventID ");
//            log.append(eventID);
//            log.append(" in EventPacket ");
//            log.append(numEventPacket);
//            log.append(" is sent.\n");
//            output.writeToLogFile(log.toString());
////            output.writeToLogFile(spoutName+": EventID "+String.valueOf(eventID)+" in EventPacket "+String.valueOf(++numEventPacket) +" is sent.\n");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        nextMatchBoltID = (nextMatchBoltID + 1) % numMatchBolt;
        tupleUnacked.put(numEventPacket, events);
        collector.emit(new Values(nextMatchBoltID, TypeConstant.Event_Match_Subscription, numEventPacket, events), numEventPacket);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MatchBoltID", "Type", "PacketID", "EventPacket"));
    }
}
