package org.sjtu.swhua.storm.MatchAlgorithm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.OutputToFile;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Pair;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Subscription;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.TypeConstant;

import java.io.IOException;
import java.util.*;

// 用于发订阅和事件的spout, 还没写好
public class SubEventSpout extends BaseRichSpout {
    //    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionSpout.class);
    private SpoutOutputCollector collector;
    private TopologyContext spoutTopologyContext;
    private int subID;
    private int numPacket;
    private final int type;
    private final int maxNumSubscription;           //  Maximum number of subscription emitted per time
    private final int maxNumSubAttribute;              //  Maxinum number of attributes in a subscription
    private final int numAttributeType;             //  Type number of attributes
    private final int subSetSize;
    private Random valueGenerator;          //  Generate the interval value and index of attribute name
    private int[] randomPermutation;              //  To get the attribute name
    private OutputToFile output;
    private StringBuilder log;
    private StringBuilder errorLog;
    private String spoutName;
    private HashMap<Integer, ArrayList<Subscription>> tupleUnacked;  // backup data

    private final double maxIntervalWidth_Simple;
    private final double minIntervalWidth_Rein;
    private final double minIntervalWidth_Tama;


    public SubEventSpout(int Type) { // 1 代表简单匹配模式，2 代表 Rein 模式，3 代表 Tama 模式
        type = Type;

        maxNumSubscription = TypeConstant.maxNumSubscriptionPerPacket;
        maxNumSubAttribute = TypeConstant.maxNumAttributePerSubscription;
        numAttributeType = TypeConstant.numAttributeType;
        subSetSize = TypeConstant.subSetSize;

        maxIntervalWidth_Simple = TypeConstant.maxIntervalWidth_Simple;
        minIntervalWidth_Rein = TypeConstant.minIntervalWidth_Rein;
        minIntervalWidth_Tama = TypeConstant.minIntervalWidth_Tama;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        subID = 1;
        numPacket = 0;
        valueGenerator = new Random();
        randomPermutation = new int[numAttributeType];
        for (int i = 0; i < numAttributeType; i++)
            randomPermutation[i] = i;
        spoutTopologyContext = topologyContext;
        spoutName = spoutTopologyContext.getThisComponentId();
        collector = spoutOutputCollector;
        output = new OutputToFile();
        tupleUnacked = new HashMap<>();
        try {
            log = new StringBuilder(spoutName);
            log.append(" ThreadNum: " + Thread.currentThread().getName() + "\n" + "    TaskID:");
            List<Integer> taskIds = spoutTopologyContext.getComponentTasks(spoutName);
            Iterator taskIdsIter = taskIds.iterator();
            while (taskIdsIter.hasNext())
                log.append(" " + String.valueOf(taskIdsIter.next()));
            log.append("\n    ThisTaskId: ");
            log.append(spoutTopologyContext.getThisTaskId() + "\n\n");  // Get the current thread number
            output.otherInfo(log.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close(){
        log = new StringBuilder(spoutName);
        log.append(": All subscriptions have been created and sent. \n");
        try {
            output.otherInfo(log.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object packetID) {
//        LOG.debug("Got ACK for msgId : ");
        log = new StringBuilder(spoutName);
        log.append(": SubTuple ");
        log.append((int) packetID);
        log.append(" is acked.\n");
        try {
            output.writeToLogFile(log.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
//        dataUnacked.put((int)packetID,null);
        tupleUnacked.remove((int) packetID);
    }

    @Override
    public void fail(Object packetID) {
        errorLog = new StringBuilder(spoutName);
        errorLog.append(": SubTuple ");
        errorLog.append(packetID);
        errorLog.append(" is failed and re-emitted.\n");
        try {
            output.errorLog(errorLog.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        collector.emit(new Values(TypeConstant.Insert_Subscription, (int) packetID, tupleUnacked.get(packetID)), numPacket);
    }

    @Override
    public void nextTuple() {
//        Utils.sleep(5);
        if (subID >= subSetSize) {
//            collector.emit(new Values(TypeConstant.Null_Operation, null));
            log = new StringBuilder(spoutName);
            log.append(": All subscriptions are created and sent. \n");
            try {
                output.writeToLogFile(log.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
            close();
            return;
        }

        int numSub = (int) (Math.random() * maxNumSubscription + 1); // Generate the number of subscriptions in this tuple: 1~maxNumSubscription
        ArrayList<Subscription> sub = new ArrayList<>(numSub);
        for (int i = 0; i < numSub; i++) {
            int numAttribute = new Random().nextInt(maxNumSubAttribute + 1); // Generate the number of attribute in this subscription: 0~maxNumAttribute

            for (int j = 0; j < numAttribute; j++) { // Use the first #numAttribute values of randomArray to create the attribute name
                int index = valueGenerator.nextInt(numAttributeType - j) + j;
                int temp = randomPermutation[j];
                randomPermutation[j] = randomPermutation[index];
                randomPermutation[index] = temp;
            }

            Double low, high;

//            String attributeName = "attributeName";
            HashMap<Integer, Pair<Double, Double>> mapNameToPair = new HashMap<>();

            for (int j = 0; j < numAttribute; j++) {
                switch (type) {
                    case TypeConstant.SIMPLE:
                        low = valueGenerator.nextDouble();
                        high = low + Math.min(1.0 - low, maxIntervalWidth_Simple) * valueGenerator.nextDouble();
                        break;
                    case TypeConstant.REIN:
                        low = (1.0 - minIntervalWidth_Rein) * valueGenerator.nextDouble();
                        high = low + minIntervalWidth_Rein + (1.0 - low - minIntervalWidth_Rein) * valueGenerator.nextDouble();
                        break;
                    case TypeConstant.TAMA:
                        low = (1.0 - minIntervalWidth_Tama) * valueGenerator.nextDouble();
                        high = low + minIntervalWidth_Tama + (1.0 - low - minIntervalWidth_Tama) * valueGenerator.nextDouble();
                        break;
                    default:
                        low = 0.0;
                        high = 1.0;
                        System.out.println("Error: algorithm type.\n");
                }
                mapNameToPair.put(randomPermutation[j], Pair.of(low, high));
            }
            try {
                sub.add(new Subscription(subID, mapNameToPair));
                subID += 1;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // for test
//        try {
//
//            HashMap<Integer, Pair<Double, Double>> m0 = new HashMap<>();
//            m0.put(1, Pair.of(0.0, 0.1));
//            m0.put(2, Pair.of(0.1, 0.2));
//            sub.add(new Subscription(0, 2, m0));
//
//            HashMap<Integer, Pair<Double, Double>> m1 = new HashMap<>();
//            m1.put(3, Pair.of(0.2, 0.3));
//            m1.put(4, Pair.of(0.3, 0.4));
//            sub.add(new Subscription(1, 2, m1));
//
//            HashMap<Integer, Pair<Double, Double>> m2 = new HashMap<>();
//            m2.put(5, Pair.of(0.4, 0.5));
//            m2.put(2, Pair.of(0.5, 0.6));
//            sub.add(new Subscription(2, 2, m2));
//
//            HashMap<Integer, Pair<Double, Double>> m3 = new HashMap<>();
//            m3.put(1, Pair.of(0.05, 0.95));
//            m3.put(3, Pair.of(0.09, 0.86));
//            sub.add(new Subscription(3, 2, m3));
//
//            HashMap<Integer, Pair<Double, Double>> m4 = new HashMap<>();
//            m4.put(2, Pair.of(0.15, 0.55));
//            m4.put(4, Pair.of(0.35, 0.94));
//            sub.add(new Subscription(4, 2, m4));
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        numPacket++;
//        try {
//            log = new StringBuilder(spoutName);
//            log.append(": SubID ");
//            log.append(subID);
//            log.append(" in SubPacket ");
//            log.append(numSubPacket);
//            log.append(" is sent.\n");
//            output.writeToLogFile(log.toString());
////            output.writeToLogFile(spoutName+": SubID "+String.valueOf(subID)+" in SubPacket " + String.valueOf(numSubPacket) + " is sent.\n");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

//        collector.emit(new Values(TypeConstant.Insert_Subscription, sub),numSubPacket);
        tupleUnacked.put(numPacket, sub);
        collector.emit(new Values(TypeConstant.Insert_Subscription, numPacket, sub), numPacket);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Type", "PacketID", "SubscriptionPacket"));
    }
}
