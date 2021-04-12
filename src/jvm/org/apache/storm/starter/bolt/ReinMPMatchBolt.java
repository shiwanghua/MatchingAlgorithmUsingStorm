package org.apache.storm.starter.bolt;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.starter.DataStructure.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

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
    private Integer numSubPacket;
    private Integer numEventPacket;
    private Integer numSubInserted;
    private Integer numEventMatched;
    final private Integer numVisualSubSet;
    final private Integer numExecutor;
    private Integer executorID;
    static private Integer executorIDAllocator;
    //    static private IDAllocator executorIDAllocator;
    final private Integer redundancy;
    private long runTime;
    private long speedTime;  // The time to calculate and record speed
    final private long beginTime;
    final private long intervalTime; // The interval between two calculations of speed

    public ReinMPMatchBolt(Integer num_executor, Integer redundancy_degree, Integer num_visual_subSet, ArrayList<String>VSSID_to_ExecutorID) {   // only execute one time for all executors!
        beginTime = System.nanoTime();
        intervalTime = 60000000000L;  // 1 minute
        executorIDAllocator = 0;
        //executorIDAllocator=new IDAllocator();
        numSubPacket = 0;
        numEventPacket = 0;
        numSubInserted = 1;
        numEventMatched = 1;
        runTime = 1;
        numExecutor = num_executor;
        redundancy = redundancy_degree;
        numVisualSubSet = num_visual_subSet;
        VSSIDtoExecutorID = VSSID_to_ExecutorID;

        log = new StringBuilder();
//        matchResult = new StringBuilder();
    }

    private synchronized void allocateID() {
        executorID = executorIDAllocator++;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) { // execute one time for every executor!

        speedTime = System.nanoTime() + intervalTime;
        boltContext = topologyContext;
        collector = outputCollector;
        boltName = boltContext.getThisComponentId();
        //  executorID=executorIDAllocator.allocateID();
        allocateID();  // boltIDAllocator need to keep synchronized
        rein = new Rein();
        output = new OutputToFile();

        if (executorID == 0) {
            log = new StringBuilder(boltName);
            log.append("MultiPartitionMatchBolt \nnumExecutor = ");
            log.append(numExecutor);
            log.append("\nredundancy = ");
            log.append(redundancy);
            log.append("\nnumVisualSubSet = ");
            log.append(numVisualSubSet);
            log.append("\nMap Table:\nID  ExecutorID");
            int size=VSSIDtoExecutorID.size();
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
            log.append(" ThreadNum: " + Thread.currentThread().getName() + "\n" + boltName + ":");
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
    }

    @Override
    public void execute(Tuple tuple) {

        int type = tuple.getInteger(0);
        try {
            switch (type) {
                case TypeConstant.Insert_Subscription: {

                    Integer subPacketID = tuple.getInteger(1);

                    int subID;
                    numSubPacket++;
                    log = new StringBuilder(boltName);
                    log.append(" Thread ");
                    log.append(executorID);
                    log.append(": SubPacket ");
                    log.append(numSubPacket);
                    log.append(" is received.\n");
                    output.writeToLogFile(log.toString());

                    ArrayList<Subscription> subPacket = (ArrayList<Subscription>) tuple.getValueByField("SubscriptionPacket");
                    int size=subPacket.size();
                    for (int i = 0; i < size; i++) {
                        subID = subPacket.get(i).getSubID();
                        if (VSSIDtoExecutorID.get(subID % numVisualSubSet).charAt(executorID) == '0')
                            continue;
                        if (rein.insert(subPacket.get(i))) // no need to add if already exists
                            numSubInserted++;
                        log = new StringBuilder(boltName);
                        log.append(" Thread ");
                        log.append(executorID);
                        log.append(": Sub ");
                        log.append(subID);
                        log.append(" is inserted.\n");
                        output.writeToLogFile(log.toString());
                    }
                    collector.ack(tuple);
//                    insertSubTime += System.nanoTime() - startTime;
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
                    log = new StringBuilder(boltName);
                    log.append(" Thread ");
                    log.append(executorID);
                    log.append(": EventPacket ");
                    log.append(numEventPacket);
                    log.append(" is received.\n");
                    output.writeToLogFile(log.toString());
                    ArrayList<Event> eventPacket = (ArrayList<Event>) tuple.getValueByField("EventPacket");
                    int size = eventPacket.size(),eventID;
                    for (int i = 0; i < size; i++) {
                        ArrayList<Integer> matchedSubIDList = rein.match(eventPacket.get(i));
                        eventID=eventPacket.get(i).getEventID();
                        log = new StringBuilder(boltName);
                        log.append(" Thread ");
                        log.append(executorID);
                        log.append(": EventID ");
                        log.append(eventID);
                        log.append(" matching task is done.\n");
                        output.writeToLogFile(log.toString());
                        collector.emit(new Values(executorID, eventID, matchedSubIDList));
                    }
                    collector.ack(tuple);
                    numEventMatched += eventPacket.size();
                    break;
                }
                default:
                    collector.fail(tuple);
                    log = new StringBuilder(boltName);
                    log.append(" Thread ");
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
            speedReport.append(" Thread ");
            speedReport.append(executorID);
            speedReport.append(" - RunTime: ");
            speedReport.append(runTime / intervalTime);
            speedReport.append("min. numSubInserted: ");
            speedReport.append(numSubInserted); //mapIDtoSub.size()
            speedReport.append("; InsertSpeed: ");
            speedReport.append(runTime / numSubInserted / 1000);  // us/per
            speedReport.append(". numEventMatched: ");
            speedReport.append(numEventMatched);
            speedReport.append("; MatchSpeed: ");
            speedReport.append(runTime / numEventMatched / 1000); // us/per
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
        private Integer numBucket, numSub, numAttributeType;
        private double bucketSpan;
        private ArrayList<Integer> mapToSubID;
        private HashMap<Integer, Boolean> exist;
        private ArrayList<ArrayList<LinkedList<Pair<Integer, Double>>>> infBuckets; // Attribute ID -> bucket id -> a bucket list -> (subID,subVlue)
        private ArrayList<ArrayList<LinkedList<Pair<Integer, Double>>>> supBuckets;

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
        }

        public boolean insert(Subscription sub) {
            Integer subID = sub.getSubID();
            if (exist.get(subID)!=null&&exist.get(subID)==true)
                return false;

            Integer subAttributeID;
            double low, high;
            HashMap.Entry<Integer, Pair<Double, Double>> subAttributeEntry;
            Iterator<HashMap.Entry<Integer, Pair<Double, Double>>> subAttributeIterator = sub.getMap().entrySet().iterator();
            while (subAttributeIterator.hasNext()) {
                subAttributeEntry = subAttributeIterator.next();
                subAttributeID = subAttributeEntry.getKey();
                low = subAttributeEntry.getValue().getFirst();
                high = subAttributeEntry.getValue().getSecond();

                infBuckets.get(subAttributeID).get((int) (low / bucketSpan)).add(Pair.of(numSub, low));
                supBuckets.get(subAttributeID).get((int) (high / bucketSpan)).add(Pair.of(numSub, high));
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
            int bucketID;

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
                    bucketID=(int)(attributeValue/bucketSpan);
                    for (Pair<Integer, Double> subIDValue : infBuckets.get(i).get(bucketID))
                        if (subIDValue.value2 > attributeValue)
                            bits[subIDValue.value1] = true;
                    for (int bi = bucketID + 1; bi < numBucket; bi++)
                        for (Pair<Integer, Double> subIDValue : infBuckets.get(i).get(bi))
                            bits[subIDValue.value1] = true;

                    for (Pair<Integer, Double> subIDValue : supBuckets.get(i).get(bucketID))
                        if (subIDValue.value2 < attributeValue)
                            bits[subIDValue.value1] = true;
                    for (int bi = 0; bi < bucketID; bi++)
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

        public Integer getNumSub() {
            return numSub;
        }
    }
}
