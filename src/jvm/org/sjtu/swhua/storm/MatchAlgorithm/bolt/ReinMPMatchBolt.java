package org.sjtu.swhua.storm.MatchAlgorithm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.*;

import java.io.IOException;
import java.util.*;

public class ReinMPMatchBolt extends BaseRichBolt {
    private OutputToFile output;
    private OutputCollector collector;
    private TopologyContext boltContext;
    private Rein rein;
    private ArrayList<String> VSSIDtoExecutorID;

    private StringBuilder log;
//    private StringBuilder matchResult;

    private String boltName;
    private int boltID;
    private int numSubPacket;
    private int numEventPacket;
    private int numSubInserted;
    private int numSubInsertedLast;
    private int numEventMatched;
    private int numEventMatchedLast;
    final private int numVisualSubSet;
    final private int numExecutor;
    private int executorID;
    static private int executorIDAllocator;
    //    static private IDAllocator executorIDAllocator;
    final private int redundancy;
    private long runTime;
    private long speedTime;  // The time to calculate and record speed
    final private long beginTime;
    final private long intervalTime; // The interval between two calculations of speed

    public ReinMPMatchBolt(int boltid, int num_executor, int redundancy_degree, int num_visual_subSet, ArrayList<String> VSSID_to_ExecutorID) {   // only execute one time for all executors!
        beginTime = System.nanoTime();
        boltID = boltid;
        intervalTime = TypeConstant.intervalTime;//60000000000L;  // 1 minute
        executorIDAllocator = 0;
        //executorIDAllocator=new IDAllocator();
        numSubPacket = 0;
        numEventPacket = 0;
        numSubInserted = 1;
        numSubInsertedLast = 1;
        numEventMatched = 1;
        numEventMatchedLast = 1;
        runTime = 1;
        numExecutor = num_executor;
        redundancy = redundancy_degree;
        numVisualSubSet = num_visual_subSet;
        VSSIDtoExecutorID = VSSID_to_ExecutorID; // 不需要每个线程都获取整个 map, 只要拿到属于自己的那一列就好了，这里是为了简便实现和防止出错

        log = new StringBuilder();
//        matchResult = new StringBuilder();
    }

    private synchronized void allocateID() {
        executorID = executorIDAllocator++;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) { // execute one time for every executor!

        boltContext = topologyContext;
        collector = outputCollector;
        boltName = boltContext.getThisComponentId();
        //allocateID();  // boltIDAllocator need to keep synchronized
        executorID = MyUtils.allocateID(boltName);
        rein = new Rein();
        output = new OutputToFile();

        if (executorID == 0) {
            log = new StringBuilder(boltName);
            log.append(" boltID: ");
            log.append(boltID);
            log.append("\nnumExecutor = ");
            log.append(numExecutor);
            log.append("\nredundancy = ");
            log.append(redundancy);
            log.append("\nnumVisualSubSet = ");
            log.append(numVisualSubSet);
            log.append("\nMap Table:\nID  ExecutorID");
            int size = VSSIDtoExecutorID.size();
            for (int i = 0; i < size; i++) {
                log.append(String.format("\n%02d: ", i));
                log.append(VSSIDtoExecutorID.get(i));
            }
            log.append("\n\n");
            try {
                output.otherInfo(log.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            log = new StringBuilder(boltName);
            log.append(" boltID: " + String.valueOf(boltID) + " ThreadNum: " + Thread.currentThread().getName() + "\n" + boltName + ":");
            List<Integer> taskIds = boltContext.getComponentTasks(boltContext.getThisComponentId());
            Iterator taskIdsIter = taskIds.iterator();
            int taskID;
            while (taskIdsIter.hasNext()) {
                taskID = (Integer) taskIdsIter.next();
                log.append(" ");
                log.append(taskID);
            }
            log.append("\nThisTaskId: ");
            log.append(executorID);   // boltContext.getThisTaskId();
            log.append("\n\n");
            output.otherInfo(log.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        speedTime = System.nanoTime() + intervalTime;
    }

    @Override
    public void execute(Tuple tuple) {

        int type = tuple.getIntegerByField("Type");
        try {
            switch (type) {
                case TypeConstant.Insert_Subscription: {

                    //Integer subPacketID = tuple.getIntegerByField("PacketID");

                    int subID;
                    numSubPacket++;
//                    log = new StringBuilder(boltName);
//                    log.append(" boltID: ");
//                    log.append(boltID);
//                    log.append(". Thread ");
//                    log.append(executorID);
//                    log.append(": SubPacket ");
//                    log.append(numSubPacket);
//                    log.append(" is received.\n");
//                    output.writeToLogFile(log.toString());

                    ArrayList<Subscription> subPacket = (ArrayList<Subscription>) tuple.getValueByField("SubscriptionPacket");
                    if(subPacket==null){
                        log = new StringBuilder(boltName);
                        log.append(" boltID: ");
                        log.append(boltID);
                        log.append(". Thread ");
                        log.append(executorID);
                        log.append(": subPacket ");
                        log.append(tuple.getValueByField("PacketID"));
                        log.append(" is null.\n");
                        output.writeToLogFile(log.toString());
                        collector.ack(tuple);
                        break;
                    }
                    int size = subPacket.size();
                    for (int i = 0; i < size; i++) {
                        subID = subPacket.get(i).getSubID();
                        if (VSSIDtoExecutorID.get(subID % numVisualSubSet).charAt(executorID) == '0')
                            continue;
                        if (rein.insert(subPacket.get(i))) // no need to add if already exists
                        {
                            numSubInserted++;
//                       System.out.println("\n\n\n"+numSubInserted+"\n\n\n");
                            log = new StringBuilder(boltName);
                            log.append(" boltID: ");
                            log.append(boltID);
                            log.append(". Thread ");
                            log.append(executorID);
                            log.append(": Sub ");
                            log.append(subID);
                            log.append(" is inserted.\n");
                            output.writeToLogFile(log.toString());
                        }
                        else{
                            log = new StringBuilder(boltName);
                            log.append(" boltID: ");
                            log.append(boltID);
                            log.append(". Thread ");
                            log.append(executorID);
                            log.append(": Sub ");
                            log.append(subID);
                            log.append(" is already inserted.\n");
                            output.writeToLogFile(log.toString());
                        }
                    }
                    collector.ack(tuple);
//                    insertSubTime += System.nanoTime() - startTime;
                    break;
                }
                case TypeConstant.Insert_Attribute_Subscription: {
                    System.out.println("Insert attributes to a subscription.\n");
                    break;
                }
                case TypeConstant.Update_Attribute_Subscription: {
                    System.out.println("Update attributes of a subscription.\n");
                    break;
                }
                case TypeConstant.Delete_Subscription: {
                    System.out.println("Delete subscriptions.\n");
                    break;
                }
                case TypeConstant.Delete_Attribute_Subscription: {
                    System.out.println("Delete attributes of a subscription.\n");
                    break;
                }
                case TypeConstant.Event_Match_Subscription: {
                    if (tuple.getIntegerByField("MatchBoltID").equals(boltID)) {
                        numEventPacket++;
                        log = new StringBuilder(boltName);
                        log.append(" boltID: ");
                        log.append(boltID);
                        log.append(". Thread ");
                        log.append(executorID);
                        log.append(": EventPacket ");
                        log.append(numEventPacket);
                        log.append(" is received.\n");
                        output.writeToLogFile(log.toString());
                        ArrayList<Event> eventPacket = (ArrayList<Event>) tuple.getValueByField("EventPacket");
                        // 偶尔会出现为空的情况，而且是运行一段时间后产生，还与订阅集大小有关，改大一点可能就没错误了，很随机
                        if(eventPacket==null){
                            log = new StringBuilder(boltName);
                            log.append(" boltID: ");
                            log.append(boltID);
                            log.append(". Thread ");
                            log.append(executorID);
                            log.append(": EventPacket ");
                            log.append(tuple.getValueByField("PacketID"));
                            log.append(" is null.\n");
                            output.writeToLogFile(log.toString());
                            collector.ack(tuple);
                            break;
                        }
                        int size = eventPacket.size(), eventID;
                        for (int i = 0; i < size; i++) {
                            ArrayList<Integer> matchedSubIDList = rein.match(eventPacket.get(i));
                            eventID = eventPacket.get(i).getEventID();
//                            log = new StringBuilder(boltName);
//                            log.append(" boltID: ");
//                            log.append(boltID);
//                            log.append(". Thread ");
//                            log.append(executorID);
//                            log.append(": EventID ");
//                            log.append(eventID);
//                            log.append(" matching task is done.\n");
//                            output.writeToLogFile(log.toString());
                            collector.emit(new Values(executorID, eventID, matchedSubIDList));
                        }
                        numEventMatched += eventPacket.size();
                    }
                    collector.ack(tuple);
                    break;
                }
                default:
                    collector.fail(tuple);
                    log = new StringBuilder(boltName);
                    log.append(" boltID: ");
                    log.append(boltID);
                    log.append(". Thread ");
                    log.append(executorID);
                    log.append(": Wrong operation type is detected.\n");
                    output.writeToLogFile(log.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (System.nanoTime() > speedTime) {
            runTime = System.nanoTime() - beginTime;
            StringBuilder speedReport = new StringBuilder(boltName);
            speedReport.append(" boltID: ");
            speedReport.append(boltID);
            speedReport.append(". Thread ");
            speedReport.append(executorID);
            speedReport.append(" - RunTime: ");
            speedReport.append(runTime / intervalTime);
            speedReport.append("min. numSubInserted: ");
            speedReport.append(numSubInserted); //mapIDtoSub.size()
            speedReport.append("; InsertSpeed: ");
            speedReport.append(intervalTime / (numSubInserted - numSubInsertedLast + 1) / 1000);  // us/per 加一避免除以0
            numSubInsertedLast = numSubInserted;
            speedReport.append(". numEventMatched: ");
            speedReport.append(numEventMatched);
            speedReport.append("; MatchSpeed: ");
//            speedReport.append(runTime / numEventMatched / 1000); // us/per
            speedReport.append(intervalTime / (numEventMatched - numEventMatchedLast + 1) / 1000);
            numEventMatchedLast = numEventMatched;
            speedReport.append(".\n");
            try {
                output.recordSpeed(speedReport.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
            speedTime = System.nanoTime() + intervalTime;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("executorID", "eventID", "subIDs"));
    }

    public Integer getNumExecutor() {
        return numExecutor;
//        return boltIDAllocator;   //  this variable may not be the last executor number.
    }


    class Rein {
        private int numBucket, numSub, numAttributeType;
        private double bucketSpan;
        private ArrayList<Integer> mapToSubID;
        private HashMap<Integer, Boolean> exist;
        private ArrayList<ArrayList<LinkedList<Pair<Integer, Double>>>> infBuckets; // Attribute ID -> bucket id -> a bucket list -> (subID,subVlue)
        private ArrayList<ArrayList<LinkedList<Pair<Integer, Double>>>> supBuckets;
        private boolean[][][] leftBits;
        private boolean[][][] rightBits;

        public Rein() {
            numSub = 0;
            numBucket = TypeConstant.numBucket;
            numAttributeType = TypeConstant.numAttributeType;
            bucketSpan = 1.0 / numBucket;
            mapToSubID = new ArrayList<>();
            exist = new HashMap<>();
            infBuckets = new ArrayList<>();
            supBuckets = new ArrayList<>();
            for (int i = 0; i < TypeConstant.numAttributeType; i++) {
                infBuckets.add(new ArrayList<>());
                supBuckets.add(new ArrayList<>());
                for (int j = 0; j < numBucket; j++) {
                    infBuckets.get(i).add(new LinkedList<>());
                    supBuckets.get(i).add(new LinkedList<>());
                }
            }
            leftBits=new boolean[numAttributeType][3][TypeConstant.subSetSize];  // [0.0,0.25] [0.0,0.5] [0.0,0.75] high
            rightBits=new boolean[numAttributeType][3][TypeConstant.subSetSize]; // [0.25,1.0] [0.5,1.0] [0.75,1.0] low
        }

        public boolean insert(Subscription sub) {
            int subID = sub.getSubID();
            if (exist.getOrDefault(subID, false) == true)
                return false;
            int subAttributeID,lowBucketID,highBucketID;
            double low, high;
            HashMap.Entry<Integer, Pair<Double, Double>> subAttributeEntry;
            Iterator<HashMap.Entry<Integer, Pair<Double, Double>>> subAttributeIterator = sub.getMap().entrySet().iterator();
            while (subAttributeIterator.hasNext()) {
                subAttributeEntry = subAttributeIterator.next();
                subAttributeID = subAttributeEntry.getKey();
                low = subAttributeEntry.getValue().getFirst();
                high = subAttributeEntry.getValue().getSecond();
                lowBucketID=(int) (low / bucketSpan);
                highBucketID=(int) (high / bucketSpan);
                infBuckets.get(subAttributeID).get(lowBucketID).add(Pair.of(numSub, low));
                supBuckets.get(subAttributeID).get(highBucketID).add(Pair.of(numSub, high));
                if(lowBucketID>=0.75*(numBucket-1)){
                    rightBits[subAttributeID][0][numSub]=true;
                    rightBits[subAttributeID][1][numSub]=true;
                    rightBits[subAttributeID][2][numSub]=true;
                }
                else if(lowBucketID>=0.5*(numBucket-1)){
                    rightBits[subAttributeID][0][numSub]=true;
                    rightBits[subAttributeID][1][numSub]=true;
                }
                else if(lowBucketID>=0.25*numBucket)
                    rightBits[subAttributeID][0][numSub]=true;

                if(highBucketID<=0.25*numBucket){
                    leftBits[subAttributeID][0][numSub]=true;
                    leftBits[subAttributeID][1][numSub]=true;
                    leftBits[subAttributeID][2][numSub]=true;
                }
                else if(highBucketID<=0.5*(numBucket-1)){
                    leftBits[subAttributeID][1][numSub]=true;
                    leftBits[subAttributeID][2][numSub]=true;
                }
                else if(highBucketID<=0.75*numBucket)
                    leftBits[subAttributeID][2][numSub]=true;

            }
            mapToSubID.add(subID);  //  add this map to ensure the size of bits array int match() is right, since each executor will not get a successive subscription set
            exist.put(subID, true);
            numSub++;   //  after Deletion operation, numSub!=numSubInserted, so variable 'numSubInserted' is needed.
            return true;
        }

        public ArrayList<Integer> match(Event e) {

            boolean[] bits = new boolean[numSub];
//        Integer eventAttributeID;
            Double attributeValue;
            int eventBucketID;
            double lowBorder=numBucket,highBorder=0;

            // Solution: each event has all attribute types i.e.: e.getMap().size()==numAttributeType
//        HashMap.Entry<Integer, Double> eventAttributeEntry;
//        Iterator<HashMap.Entry<Integer, Double>> eventAttributeIterator = e.getMap().entrySet().iterator();
//        while (eventAttributeIterator.hasNext()) {
//            eventAttributeEntry = eventAttributeIterator.next();
//            eventAttributeID = eventAttributeEntry.getKey();
//            attributeValue = eventAttributeEntry.getValue();
//            bucketID = (int) (attributeValue / bucketSpan);
//
//            for (Pair<Integer, Double> subIDValue : infBuckets.get(eventAttributeID).get(bucketID))
//                if (subIDValue.value2 > attributeValue)
//                    bits[subIDValue.value1] = true;
//            for (int i = bucketID + 1; i < numBucket; i++)
//                for (Pair<Integer, Double> subIDValue : infBuckets.get(eventAttributeID).get(i))
//                    bits[subIDValue.value1] = true;
//
//            for (Pair<Integer, Double> subIDValue : supBuckets.get(eventAttributeID).get(bucketID))
//                if (subIDValue.value2 < attributeValue)
//                    bits[subIDValue.value1] = true;
//            for (int i = 0; i < bucketID; i++)
//                for (Pair<Integer, Double> subIDValue : infBuckets.get(eventAttributeID).get(i))
//                    bits[subIDValue.value1] = true;
//        }

//        HashMap<Integer,Double> attributeIDToValue=e.getMap();
            for (int i = 0; i < numAttributeType; i++) {   // i: attributeID
                attributeValue = e.getAttributeValue(i);
                if (attributeValue == null) {  // all sub containing this attribute should be marked, only either sup or inf is enough.
                    for (int j = 0; j < numBucket; j++) {  // j: BucketID
                        for (Iterator<Pair<Integer, Double>> pairIterator = infBuckets.get(i).get(j).iterator(); pairIterator.hasNext(); ) {
                            bits[pairIterator.next().getFirst()] = true;
                        } // LinkedList
                    } // Bucket ArrayList
                } else {
                    eventBucketID = (int) (attributeValue / bucketSpan);

                    if(eventBucketID<0.25*numBucket){
                        lowBorder=0.25*numBucket;
                        for(int j=0;j<numSub;j++)
                            bits[j]=bits[j]|rightBits[i][0][j];
                    }
                    else if(eventBucketID<0.5*(numBucket-1)){
                        lowBorder=0.5*(numBucket-1);
                        for(int j=0;j<numSub;j++)
                            bits[j]=bits[j]|rightBits[i][1][j];
                    }
                    else if(eventBucketID<0.75*(numBucket-1)){
                        lowBorder=0.75*(numBucket-1);
                        for(int j=0;j<numSub;j++)
                            bits[j]=bits[j]|rightBits[i][2][j];
                    }

                    if(eventBucketID>0.75*numBucket){
                        highBorder=0.75*numBucket;
                        for(int j=0;j<numSub;j++)
                            bits[j]=bits[j]|leftBits[i][2][j];
                    }
                    else if(eventBucketID>0.5*(numBucket-1)){
                        highBorder=0.5*(numBucket-1);
                        for(int j=0;j<numSub;j++)
                            bits[j]=bits[j]|leftBits[i][1][j];
                    }
                    else if (eventBucketID>0.25*numBucket){
                        highBorder=0.25*numBucket;
                        for(int j=0;j<numSub;j++)
                            bits[j]=bits[j]|leftBits[i][0][j];
                    }

                    for (Pair<Integer, Double> subIDValue : infBuckets.get(i).get(eventBucketID))
                        if (subIDValue.value2 > attributeValue)
                            bits[subIDValue.value1] = true;
                    for (int bi = eventBucketID + 1; bi < lowBorder; bi++)
                        for (Pair<Integer, Double> subIDValue : infBuckets.get(i).get(bi))
                            bits[subIDValue.value1] = true;

                    for (Pair<Integer, Double> subIDValue : supBuckets.get(i).get(eventBucketID))
                        if (subIDValue.value2 < attributeValue)
                            bits[subIDValue.value1] = true;
                    for (int bi = eventBucketID; bi > highBorder ; bi--)
                        for (Pair<Integer, Double> subIDValue : infBuckets.get(i).get(bi))
                            bits[subIDValue.value1] = true;
                }
            }

            ArrayList<Integer> matchResult = new ArrayList<>();
            for (int i = 0; i < numSub; i++)
                if (!bits[i])
                    matchResult.add(mapToSubID.get(i));
            return matchResult;
        }

        public int getNumSub() {
            return numSub;
        }
    }
}
