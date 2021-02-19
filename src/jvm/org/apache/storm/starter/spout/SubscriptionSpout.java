package org.apache.storm.starter.spout;

import org.apache.jasper.tagplugins.jstl.core.Out;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.starter.DataStructure.OutputToFile;
import org.apache.storm.starter.DataStructure.Pair;
import org.apache.storm.starter.DataStructure.Subscription;
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

public class SubscriptionSpout extends BaseRichSpout {
    //    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionSpout.class);
    SpoutOutputCollector collector;
    private Integer subID;
    private Integer numSubPacket;
    final int maxNumSubscription;           //  Maximum number of subscription emitted per time
    final int maxNumAttribute;              //  Maxinum number of attributes in a subscription
    final Integer subSetSize;
    private Random valueGenerator;          //  Generate the interval value and index of attribute name
    private int[] randomArray;              //  To get the attribute name
    private OutputToFile output;
    private StringBuilder log;
    private String spoutName;

    public SubscriptionSpout(String spoutName) {
        subID = 1;
        numSubPacket=0;
        maxNumSubscription = 100;
        maxNumAttribute = 30;
        subSetSize=100000;
        valueGenerator = new Random();
        randomArray = new int[maxNumAttribute];
        for (int i = 0; i < maxNumAttribute; i++)
            randomArray[i] = i;
        this.spoutName=spoutName;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        output=new OutputToFile();
        log=new StringBuilder();
    }

    @Override
    public void ack(Object id) {
//        LOG.debug("Got ACK for msgId : ");
    }

    @Override
    public void fail(Object id) {
        
    }

    @Override
    public void nextTuple() {
//        Utils.sleep(50);
        if(subID>=subSetSize){
//            collector.emit(new Values(TypeConstant.Null_Operation, null));
            return;
        }

        int numSub = (int)(Math.random() * maxNumSubscription + 1); // Generate the number of subscriptions in this tuple: 1~maxNumSubscription
        ArrayList<Subscription> sub = new ArrayList<>(numSub);
        for(int i=0;i<numSub;i++){
            int numAttribute = new Random().nextInt(maxNumAttribute+1); // Generate the number of attribute in this subscription: 0~maxNumAttribute

            for(int j=0;j<numAttribute;j++){ // Use the first #numAttribute values of randomArray to create the attribute name
                int index = valueGenerator.nextInt(maxNumAttribute-j)+j;
                int temp = randomArray[j];
                randomArray[j]=randomArray[index];
                randomArray[index]=temp;
            }

            Double low,high;
            String attributeName="attributeName";
            HashMap<String, Pair<Double, Double>> mapNameToPair =new HashMap<>();

            for(int j = 0; j<numAttribute; j++){
                low=valueGenerator.nextDouble();
                high = low + (1.0 - low )*valueGenerator.nextDouble();
                mapNameToPair.put(attributeName+String.valueOf(randomArray[j]), Pair.of(low,high));
            }
            try {
                sub.add(new Subscription(subID,numAttribute,mapNameToPair));
                subID+=1;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // for test
//        try {
//
//            HashMap<String, Pair<Double, Double>> m0 = new HashMap<>();
//            m0.put("name1", Pair.of(0.0, 0.1));
//            m0.put("name2", Pair.of(0.1, 0.2));
//            sub.add(new Subscription(0, 2, m0));
//
//            HashMap<String, Pair<Double, Double>> m1 = new HashMap<>();
//            m1.put("name3", Pair.of(0.2, 0.3));
//            m1.put("name4", Pair.of(0.3, 0.4));
//            sub.add(new Subscription(1, 2, m1));
//
//            HashMap<String, Pair<Double, Double>> m2 = new HashMap<>();
//            m2.put("name1", Pair.of(0.4, 0.5));
//            m2.put("name2", Pair.of(0.5, 0.6));
//            sub.add(new Subscription(2, 2, m2));
//
//            HashMap<String, Pair<Double, Double>> m3 = new HashMap<>();
//            m3.put("name1", Pair.of(0.05, 0.95));
//            m3.put("name3", Pair.of(0.09, 0.86));
//            sub.add(new Subscription(3, 2, m3));
//
//            HashMap<String, Pair<Double, Double>> m4 = new HashMap<>();
//            m4.put("name2", Pair.of(0.15, 0.55));
//            m4.put("name4", Pair.of(0.35, 0.94));
//            sub.add(new Subscription(4, 2, m4));
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        numSubPacket++;
        try {
            log=new StringBuilder(spoutName);
            log.append(": SubID ");
            log.append(subID);
            log.append(" in SubPacket ");
            log.append(numSubPacket);
            log.append(" is sent.\n");
            output.writeToLogFile(log.toString());
//            output.writeToLogFile(spoutName+": SubID "+String.valueOf(subID)+" in SubPacket " + String.valueOf(numSubPacket) + " is sent.\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        collector.emit(new Values(TypeConstant.Insert_Subscription, sub),numSubPacket);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Type", "SubscriptionPacket"));
    }
}