package org.apache.storm.starter.bolt;

//import com.esotericsoftware.kryo.Kryo;

import org.apache.storm.starter.DataStructure.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SimpleMatchBolt extends BaseRichBolt {
    private OutputToFile output;
    private HashMap<Integer, Subscription> mapIDtoSub = null;
    private OutputCollector collector = null;

    private Integer numSubPacket;
    private Integer numEventPacket;
    private String boltName;

    public SimpleMatchBolt(String boltName) {
        this.boltName = boltName;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        output = new OutputToFile();
        mapIDtoSub = new HashMap<>();
        numSubPacket = 0;
        numEventPacket = 0;
    }

    @Override
    public void execute(Tuple tuple) {
// olution A: catch Exception to find whether the tuple is a subPacket or a eventPacket
//        try {
//            ArrayList<Subscription> subPacket=(ArrayList<Subscription>) tuple.getValueByField("SubscriptionPacket");
//        }
//        catch (IllegalArgumentException e) {
//        }
//        try {
//         ArrayList<Event> eventPacket = (ArrayList<Event>) tuple.getValueByField("EventPacket");
//        }
//        catch (IllegalArgumentException e){
//        }

        // Solution B: get the operation type to find what the tuple is
        int type = (int) tuple.getValue(0);
        try {
            switch (type) {
                case TypeConstant.Insert_Subscription: {
                    int subID;
                    numSubPacket++;
                    output.writeToLogFile(boltName + ": SubPacket" + String.valueOf(numSubPacket) + " is received.\n");

                    ArrayList<Subscription> subPacket = (ArrayList<Subscription>) tuple.getValueByField("SubscriptionPacket");
                    for (int i = 0; i < subPacket.size(); i++) {
                        subID = subPacket.get(i).getSubID();
                        mapIDtoSub.put(subID, subPacket.get(i));
//                        System.out.println("\n\n\nSubscription " + String.valueOf(subID) + " is inserted." + "\n\n\n");
                        output.writeToLogFile(boltName + ": Subscription " + String.valueOf(subID) + " is inserted.\n");
                    }
                    collector.ack(tuple);
                    break;
                }
                case TypeConstant.Insert_Attribute_Subscription: {
                    break;
                }
                case TypeConstant.Update_Attribute_Subscription: {
                    break;
                }
                case TypeConstant.Delete_Attribute_Subscription: {
                    break;
                }
                case TypeConstant.Delete_Subscription: {
                    break;
                }
                case TypeConstant.Event_Match_Subscription: {
                    numEventPacket++;
                    output.writeToLogFile(boltName + ": EventPacket" + String.valueOf(numEventPacket) + " is received.\n");
                    ArrayList<Event> eventPacket = (ArrayList<Event>) tuple.getValueByField("EventPacket");

                    for (int i = 0; i < eventPacket.size(); i++) {
                        int eventID = eventPacket.get(i).getEventID();
                        int matchNum = 0;
                        String matchResult = boltName+" - EventID: " + String.valueOf(eventID) + "; SubNum:" + String.valueOf(mapIDtoSub.size()) + "; SubID:";

                        if (mapIDtoSub.size() == 0) {
                            output.writeToLogFile(boltName+": Event " + String.valueOf(eventID) + " matching task is done.\n");
                            output.saveMatchResult(matchResult + " ; MatchedSubNum: 0.\n");
                            continue;
                        }
//                        System.out.println("\n\n\n" + String.valueOf(eventID) + " begins to match." + "\n\n\n");

                        HashMap<String, Double> eventAttributeNameToValue = eventPacket.get(i).getMap();
                        Iterator<HashMap.Entry<Integer, Subscription>> subIterator = mapIDtoSub.entrySet().iterator();

                        while (subIterator.hasNext()) {
                            HashMap.Entry<Integer, Subscription> subEntry = subIterator.next();
                            Integer subID = subEntry.getKey();
                            Iterator<HashMap.Entry<String, Pair<Double, Double>>> subAttributeIterator = subEntry.getValue().getMap().entrySet().iterator();

                            Boolean matched = true;
                            while (subAttributeIterator.hasNext()) {
                                HashMap.Entry<String, Pair<Double, Double>> subAttributeEntry = subAttributeIterator.next();
                                String subAttributeName = subAttributeEntry.getKey();
                                if (!eventAttributeNameToValue.containsKey(subAttributeName)) {
                                    matched = false;
                                    break;
                                }

                                Double low = subAttributeEntry.getValue().getFirst();
                                Double high = subAttributeEntry.getValue().getSecond();
                                Double eventValue = eventAttributeNameToValue.get(subAttributeName);
                                if (eventValue < low || eventValue > high) {
                                    matched = false;
                                    break;
                                }
                            }
                            if (matched) {   // Save this subID to MatchResult
                                matchNum++;
                                matchResult += " " + String.valueOf(subID);
                            }
                        }
                        output.writeToLogFile(boltName+": Event " + String.valueOf(eventID) + " matching task is done.\n");
                        output.saveMatchResult(matchResult + "; MatchedSubNum: " + String.valueOf(matchNum) + ".\n");
                    }
                    collector.ack(tuple);
                    break;
                }
                default:
                    collector.fail(tuple);
                    output.writeToLogFile(boltName+": Wrong operation type is detected.\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("MatchResult"));
    }
}
