package org.apache.storm.starter.spout;

//import org.apache.jasper.tagplugins.jstl.core.Out;

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
//import org.apache.storm.utils.Utils;

import java.io.IOException;
import java.util.*;

public class SubscriptionSpout extends BaseRichSpout {
    //    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionSpout.class);
    SpoutOutputCollector collector;
    TopologyContext subSpoutTopologyContext;
    private Integer subID;
    private Integer numSubPacket;
    final int maxNumSubscription;           //  Maximum number of subscription emitted per time
    final int maxNumAttribute;              //  Maxinum number of attributes in a subscription
    final int numAttributeType;             //  Type number of attributes
    final Integer subSetSize;
    private Random valueGenerator;          //  Generate the interval value and index of attribute name
    private int[] randomPermutation;              //  To get the attribute name
    private OutputToFile output;
    private StringBuilder log;
    private StringBuilder errorLog;
    private String spoutName;

    public SubscriptionSpout() {
        maxNumSubscription = TypeConstant.maxNumSubscriptionPerPacket;
        maxNumAttribute = TypeConstant.maxNumAttributePerSubscription;
        numAttributeType = TypeConstant.numAttributeType;
        subSetSize = TypeConstant.subSetSize;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        subID = 1;
        numSubPacket = 0;
        valueGenerator = new Random();
        randomPermutation = new int[numAttributeType];
        for (int i = 0; i < numAttributeType; i++)
            randomPermutation[i] = i;
        subSpoutTopologyContext = topologyContext;
        spoutName=subSpoutTopologyContext.getThisComponentId();
        collector = spoutOutputCollector;
        output = new OutputToFile();
        try {
            log = new StringBuilder(spoutName);
            log.append(" ThreadNum: " + Thread.currentThread().getName() + "\n" + spoutName + ":");
            List<Integer> taskIds = subSpoutTopologyContext.getComponentTasks(spoutName);
            Iterator taskIdsIter = taskIds.iterator();
            while (taskIdsIter.hasNext())
                log.append(" " + String.valueOf(taskIdsIter.next()));
            log.append("\nThisTaskId: ");
            log.append(subSpoutTopologyContext.getThisTaskId()+"\n\n");  // Get the current thread number
            output.otherInfo(log.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object id) {
//        LOG.debug("Got ACK for msgId : ");
        log=new StringBuilder(spoutName);
        log.append(": SubTuple ");
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
        errorLog.append(": SubTuple ");
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
//        Utils.sleep(50);
        if (subID >= subSetSize) {
//            collector.emit(new Values(TypeConstant.Null_Operation, null));
            return;
        }

        int numSub = (int) (Math.random() * maxNumSubscription + 1); // Generate the number of subscriptions in this tuple: 1~maxNumSubscription
        ArrayList<Subscription> sub = new ArrayList<>(numSub);
        for (int i = 0; i < numSub; i++) {
            int numAttribute = new Random().nextInt(maxNumAttribute + 1); // Generate the number of attribute in this subscription: 0~maxNumAttribute

            for (int j = 0; j < numAttribute; j++) { // Use the first #numAttribute values of randomArray to create the attribute name
                int index = valueGenerator.nextInt(numAttributeType - j) + j;
                int temp = randomPermutation[j];
                randomPermutation[j] = randomPermutation[index];
                randomPermutation[index] = temp;
            }

            Double low, high;
            String attributeName = "attributeName";
            HashMap<String, Pair<Double, Double>> mapNameToPair = new HashMap<>();

            for (int j = 0; j < numAttribute; j++) {
                low = valueGenerator.nextDouble();
                high = low + (1.0 - low) * valueGenerator.nextDouble();
                mapNameToPair.put(attributeName + String.valueOf(randomPermutation[j]), Pair.of(low, high));
            }
            try {
                sub.add(new Subscription(subID, numAttribute, mapNameToPair));
                subID += 1;
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
            log = new StringBuilder(spoutName);
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

//        collector.emit(new Values(TypeConstant.Insert_Subscription, sub),numSubPacket);
        collector.emit(new Values(TypeConstant.Insert_Subscription, numSubPacket, sub), numSubPacket);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Type", "PacketID", "SubscriptionPacket"));
    }
}