package org.apache.storm.starter.bolt;

//import com.esotericsoftware.kryo.Kryo;

import org.apache.storm.starter.DataStructure.Event;
import org.apache.storm.starter.DataStructure.OutputToFile;
import org.apache.storm.starter.DataStructure.Subscription;
import org.apache.storm.starter.DataStructure.TypeConstant;
import org.apache.storm.streams.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SimpleMatchBolt extends BaseBasicBolt {
    private OutputToFile out;
    OutputCollector collector;
    private HashMap<Integer, Subscription> mapIDtoSub;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // olution A: catch Exception to find whether the tuple is a subPacket or a eventPacket
//        try {
//            ArrayList<Subscription> subPacket=(ArrayList<Subscription>) tuple.getValueByField("SubscriptionPacket");
//
//        }
//        catch (IllegalArgumentException e) {
//
//        }
//        try {
//         ArrayList<Event> eventPacket = (ArrayList<Event>) tuple.getValueByField("EventPacket");
//        }
//        catch (IllegalArgumentException e){
//
//        }

        // Solution B: get the operation type to find what the tuple is
        int type = (int) tuple.getValue(0);
        try {
            switch (type) {
                case TypeConstant.Insert_Subscription: {
                    int subID;
                    ArrayList<Subscription> subPacket = (ArrayList<Subscription>) tuple.getValueByField("SubscriptionPacket");
                    for (int i = 0; i < subPacket.size(); i++) {
                        subID = subPacket.get(i).getSubID();
                        mapIDtoSub.put(subID, subPacket.get(i));
                        out.writeToFile("Subscription "+String.valueOf(subID)+"is inserted.\n");
                    }
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
                    ArrayList<Event> eventPacket = (ArrayList<Event>) tuple.getValueByField("EventPacket");

                    for(int i=0;i<eventPacket.size();i++){
                        int eventID=eventPacket.get(i).getEventID();
                        HashMap<String, Double> eventAttributeNameToValue=eventPacket.get(i).attributeNameToValue;
                        Iterator<HashMap.Entry<Integer, Subscription>> subIterator = mapIDtoSub.entrySet ().iterator();

                        while (subIterator.hasNext()) {
                            HashMap.Entry<Integer, Subscription> subEntry = subIterator.next();
                            Integer subID = subEntry.getKey();
                            Iterator<HashMap.Entry<String,Pair<Double,Double>>> subAttributeIterator = subEntry.getValue().attributeNameToPair.entrySet().iterator();

                            Boolean matched=true;
                            while(subAttributeIterator.hasNext()){
                                HashMap.Entry<String, Pair<Double,Double>> subAttributeEntry = subAttributeIterator.next();
                                String subAttributeName = subAttributeEntry.getKey();
                                if(!eventAttributeNameToValue.containsKey(subAttributeName)) {
                                    matched=false;
                                    break;
                                }

                                Double low = subAttributeEntry.getValue().getFirst();
                                Double high =subAttributeEntry.getValue().getSecond();
                                Double eventValue = eventAttributeNameToValue.get(subAttributeName);
                                if(eventValue<low||eventValue>high){
                                    matched=false;
                                    break;
                                }
                            }
                            if(matched){   // Save this subID to MatchResult

                            }
                        }
                    }
                    break;
                }
                default:
                    out.writeToFile("Wrong operation type is detected.\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MatchResult"));
    }
}
