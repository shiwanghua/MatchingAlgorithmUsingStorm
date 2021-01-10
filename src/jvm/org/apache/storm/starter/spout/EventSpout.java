package org.apache.storm.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.starter.DataStructure.Event;
import org.apache.storm.starter.DataStructure.Subscription;
import org.apache.storm.starter.DataStructure.TypeConstant;
import org.apache.storm.streams.Pair;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class EventSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(EventSpout.class);
    SpoutOutputCollector collector;
    private Random valueGenerator;
    private Integer eventID = 0;
    final int maxNumEvent = 20;         //  Maximum number of event emitted per time
    final int maxNumAttribute = 30;     //  Maxinum number of attributes in a event
    private int[] randomArray = new int[maxNumAttribute]; // To get the attribute name

    public EventSpout() {
        valueGenerator = new Random();
        for (int i = 0; i < maxNumAttribute; i++)
            randomArray[i] = i;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        int numEvent = (int) Math.random() * maxNumEvent + 1; // Generate the number of subscriptions in this tuple: 1~maxNumEvent
        ArrayList<Event> sub = new ArrayList<>(numEvent);
//        for(int i=0;i<numEvent;i++){
//            int numAttribute = new Random().nextInt(maxNumAttribute+1); // Generate the number of attribute in this subscription: 0~maxNumAttribute
//
//            Double eventValue;
//            String attributeName="attributeName";
//
//            for(int j=0;j<numAttribute;j++){ // Use the first #numAttribute values of randomArray to create the attribute name
//                int index = valueGenerator.nextInt(maxNumAttribute-j)+j;
//                int temp = randomArray[j];
//                randomArray[j]=randomArray[index];
//                randomArray[index]=temp;
//            }
//
//            HashMap<String, Double> mapNameToValue=new HashMap<>();
//            for(int j = 0; j<numAttribute; j++){
//                eventValue=valueGenerator.nextDouble();
//                mapNameToValue.put(attributeName+String.valueOf(randomArray[j]), eventValue);
//            }
//            try {
//                sub.add(new Event(eventID,numAttribute,mapNameToValue));
//                eventID+=1;
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }

        // for test
        Utils.sleep(1000);
        try {

            HashMap<String, Double> m1 = new HashMap<>();  // Match sub0,sub1,sub3
            m1.put("name1", 0.06);
            m1.put("name2", 0.14);
            m1.put("name3", 0.25);
            m1.put("name4", 0.35);
            sub.add(new Event(eventID++, 3, m1));

            HashMap<String, Double> m2 = new HashMap<>();   // Match null
            m2.put("name3", 0.21);
            m2.put("name4", 5.0);
            sub.add(new Event(eventID++, 2, m2));

            HashMap<String, Double> m3 = new HashMap<>(); // Match sub2
            m3.put("name1", 0.46);
            m3.put("name2", 0.54);
            sub.add(new Event(eventID++, 2, m3));

            HashMap<String, Double> m4 = new HashMap<>(); // Match sub2, sub3
            m4.put("name1", 0.48);
            m4.put("name2", 0.56);
            m4.put("name3", 8.5);
            sub.add(new Event(eventID++, 2, m4));

            HashMap<String, Double> m5 = new HashMap<>(); // Match sub4
            m5.put("name2", 0.18);
            m5.put("name4", 7.5);
            sub.add(new Event(eventID++, 2, m5));

        } catch (IOException e) {
            e.printStackTrace();
        }
        collector.emit(new Values(TypeConstant.Event_Match_Subscription, sub));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Type","EventPacket"));
    }
}
