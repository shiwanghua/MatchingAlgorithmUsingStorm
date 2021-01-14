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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class EventSpout extends BaseRichSpout {
    //    private static final Logger LOG = LoggerFactory.getLogger(EventSpout.class);
    SpoutOutputCollector collector;
    private Random valueGenerator;
    private Integer eventID;
    private Integer numEventPacket;
    final int maxNumEvent;            //  Maximum number of event emitted per time
    final int maxNumAttribute;        //  Maxinum number of attributes in a event
    private int[] randomArray = null; // To get the attribute name
    private OutputToFile output;
    private String spoutName;

    public EventSpout(String spoutName) {
        valueGenerator = new Random();
        eventID = 1;
        numEventPacket=0;  // messageID
        maxNumEvent = 20;
        maxNumAttribute = 30;
        randomArray = new int[maxNumAttribute];
        for (int i = 0; i < maxNumAttribute; i++)
            randomArray[i] = i;
        this.spoutName=spoutName;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        output=new OutputToFile();
    }

    @Override
    public void nextTuple() {
//        Utils.sleep(100);
        int numEvent = (int) (Math.random() * maxNumEvent + 1); // Generate the number of subscriptions in this tuple: 1~maxNumEvent
        ArrayList<Event> events = new ArrayList<>(numEvent);

        for(int i=0;i<numEvent;i++){
            int numAttribute = new Random().nextInt(maxNumAttribute+1); // Generate the number of attribute in this subscription: 0~maxNumAttribute

            Double eventValue;
            String attributeName="attributeName";

            for(int j=0;j<numAttribute;j++){ // Use the first #numAttribute values of randomArray to create the attribute name
                int index = valueGenerator.nextInt(maxNumAttribute-j)+j;
                int temp = randomArray[j];
                randomArray[j]=randomArray[index];
                randomArray[index]=temp;
            }

            HashMap<String, Double> mapNameToValue=new HashMap<>();
            for(int j = 0; j<numAttribute; j++){
                eventValue=valueGenerator.nextDouble();
                mapNameToValue.put(attributeName+String.valueOf(randomArray[j]), eventValue);
            }
            try {
                events.add(new Event(eventID,numAttribute,mapNameToValue));
                eventID+=1;
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
        try {
            output.writeToLogFile(spoutName+": Event"+String.valueOf(eventID)+" is sent.\n");
            output.writeToLogFile(spoutName + ": EventPacket" + String.valueOf(++numEventPacket) + " is sent.\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        collector.emit(new Values(TypeConstant.Event_Match_Subscription, events),++numEventPacket);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Type", "EventPacket"));
    }
}
