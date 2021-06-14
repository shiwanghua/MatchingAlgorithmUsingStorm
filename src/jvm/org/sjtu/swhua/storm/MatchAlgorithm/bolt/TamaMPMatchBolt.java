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

public class TamaMPMatchBolt extends BaseRichBolt {
    private OutputToFile output;
    private OutputCollector collector;
    private TopologyContext boltContext;
    private Tama tama;
    private ArrayList<String> VSSIDtoExecutorID;

    private StringBuilder log;
//    private StringBuilder matchResult;

    private String boltName;
    private int boltID;
    //    private int numSubPacket;
//    private int numEventPacket;
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

    public TamaMPMatchBolt(int boltid, int num_executor, int redundancy_degree, int num_visual_subSet, ArrayList<String> VSSID_to_ExecutorID) {   // only execute one time for all executors!
        beginTime = System.nanoTime();
        boltID = boltid;
        intervalTime = TypeConstant.intervalTime;//1000000000L;//60000000000L;  // 1 minute
        executorIDAllocator = 0;
        //executorIDAllocator=new IDAllocator();
//        numSubPacket = 0;
//        numEventPacket = 0;
        numSubInserted = 1;
        numSubInsertedLast = 1;
        numEventMatched = 1;
        numEventMatchedLast = 0;
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

        boltContext = topologyContext;
        collector = outputCollector;
        boltName = boltContext.getThisComponentId();
        //allocateID();  // boltIDAllocator need to keep synchronized
        executorID = MyUtils.allocateID(boltName);
        tama = new Tama();
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
//                    numSubPacket++;
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
                    int size = subPacket.size();
                    for (int i = 0; i < size; i++) {
                        subID = subPacket.get(i).getSubID();
                        if (VSSIDtoExecutorID.get(subID % numVisualSubSet).charAt(executorID) == '0')
                            continue;
                        if (tama.insert(subPacket.get(i))) // no need to add if already exists
                            numSubInserted++;
//                        log = new StringBuilder(boltName);
//                        log.append(" boltID: ");
//                        log.append(boltID);
//                        log.append(". Thread ");
//                        log.append(executorID);
//                        log.append(": Sub ");
//                        log.append(subID);
//                        log.append(" is inserted.\n");
//                        output.writeToLogFile(log.toString());
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
                    if (tuple.getIntegerByField("MatchBoltID").equals(boltID)) {
//                        numEventPacket++;
//                        log = new StringBuilder(boltName);
//                        log.append(" boltID: ");
//                        log.append(boltID);
//                        log.append(". Thread ");
//                        log.append(executorID);
//                        log.append(": EventPacket ");
//                        log.append(numEventPacket);
//                        log.append(" is received.\n");
//                        output.writeToLogFile(log.toString());
                        ArrayList<Event> eventPacket = (ArrayList<Event>) tuple.getValueByField("EventPacket");
                        int size = eventPacket.size(), eventID;
                        for (int i = 0; i < size; i++) {
                            ArrayList<Integer> matchedSubIDList = tama.match(eventPacket.get(i));
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
            speedReport.append("s. numSubInserted: ");
            speedReport.append(numSubInserted); //mapIDtoSub.size()
            speedReport.append("; InsertSpeed: ");
            speedReport.append(intervalTime / (numSubInserted - numSubInsertedLast + 1) / 1000);  // us/per 加一避免除以0
            numSubInsertedLast = numSubInserted;
            speedReport.append(". numEventMatched: ");
            speedReport.append(numEventMatched);
            speedReport.append("; MatchSpeed: ");
//            speedReport.append(runTime / numEventMatched / 1000); // us/per
            speedReport.append(intervalTime / (numEventMatched - numEventMatchedLast) / 1000);
            numEventMatchedLast = numEventMatched - 1; // 防止某分钟内一个事件都没匹配，从而除以0
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


    // cell 从０开始编号，level从１开始编号
    class Tama {
        private int numSub, numAttributeType, numLevel, numCell;
        private int[] counter, lchild, rchild;
        private double[] mid;
        private ArrayList<ArrayList<ArrayList<Integer>>> table;
        private HashMap<Integer, Integer> mapSubIDtoNumAttribute;
        private ArrayList<Integer> mapToSubID;

        public Tama() {
            numSub = 0;
            numCell = 0;
            numAttributeType = TypeConstant.numAttributeType;
            numLevel = TypeConstant.numLevel;
            counter = new int[TypeConstant.subSetSize];
            lchild = new int[1 << numLevel];
            rchild = new int[1 << numLevel];
            mid = new double[1 << numLevel];
            mapSubIDtoNumAttribute = new HashMap<>();
            mapToSubID = new ArrayList<>();
            table = new ArrayList<>();
            for (int i = 0; i < numAttributeType; i++) {
                table.add(new ArrayList<>());
                for (int j = 0; j < (1 << numLevel); j++) { // (1 << numLevel) - 1
                    table.get(i).add(new ArrayList<>());
                }
            }
            initiate(1, 0, 0.0, 1.0);
        }

        private void initiate(int level, int cellID, double l, double r) {
//            log=new StringBuilder(level);
//            log.append(" ");
//            log.append(cellID);
//            log.append(" ");
//            log.append(l);
//            log.append(" ");
//            log.append(r);
//            log.append(".\n");
//            try {
//                output.writeToLogFile(log.toString());
//            } catch (IOException e) {
//                e.printStackTrace();
//            }

            if (level == numLevel)
                return;
            mid[cellID] = (l + r) / 2;
//            if (l < r) {
            lchild[cellID] = ++numCell;
            initiate(level + 1, numCell, l, mid[cellID]);
            rchild[cellID] = ++numCell;
            initiate(level + 1, numCell, mid[cellID], r);
//            }
        }

        public boolean insert(Subscription sub) {
            int subID = sub.getSubID();
            if (mapSubIDtoNumAttribute.getOrDefault(subID, 0) > 0)
                return false;

            HashMap.Entry<Integer, Pair<Double, Double>> subAttributeEntry;
            Iterator<HashMap.Entry<Integer, Pair<Double, Double>>> subAttributeIterator = sub.getMap().entrySet().iterator();
            while (subAttributeIterator.hasNext()) {
                subAttributeEntry = subAttributeIterator.next();
                insert(1, 0, 0.0, 1.0, numSub, subAttributeEntry.getKey(), subAttributeEntry.getValue().getFirst(), subAttributeEntry.getValue().getSecond());
            }
            mapSubIDtoNumAttribute.put(numSub, sub.getAttibuteNum());
            mapToSubID.add(subID);  //  add this map to ensure the size of bits array int match() is right, since each executor will not get a successive subscription set
            numSub++;   //  after Deletion operation, numSub!=numSubInserted, so variable 'numSubInserted' is needed.
            return true;
        }

        private void insert(int level, int cellID, double left, double right, int subID, int attributeID, double low, double high) {
            if (level == numLevel || (low <= left && high >= right)) {
                table.get(attributeID).get(cellID).add(subID);
                return;
            }
            if (high <= mid[cellID])
                insert(level + 1, lchild[cellID], left, mid[cellID], subID, attributeID, low, high);
            else if (low > mid[cellID])
                insert(level + 1, rchild[cellID], mid[cellID], right, subID, attributeID, low, high);
            else {
                insert(level + 1, lchild[cellID], left, mid[cellID], subID, attributeID, low, high);
                insert(level + 1, rchild[cellID], mid[cellID], right, subID, attributeID, low, high);
            }
        }

        public ArrayList<Integer> match(Event e) {

            for (HashMap.Entry<Integer, Integer> entry : mapSubIDtoNumAttribute.entrySet())
                counter[entry.getKey()] = entry.getValue();
            int eventAttributeID;
            double attributeValue;
            HashMap.Entry<Integer, Double> eventAttributeEntry;
            Iterator<HashMap.Entry<Integer, Double>> eventAttributeIterator = e.getAttributeIDToValue().entrySet().iterator();
            while (eventAttributeIterator.hasNext()) {
                eventAttributeEntry = eventAttributeIterator.next();
                eventAttributeID = eventAttributeEntry.getKey();
                attributeValue = eventAttributeEntry.getValue();
                match(1, 0, eventAttributeID, 0.0, 1.0, attributeValue);
            }

            ArrayList<Integer> matchResult = new ArrayList<>();
            for (int i = 0; i < numSub; i++)
                if (counter[i] == 0)
                    matchResult.add(mapToSubID.get(i));
            return matchResult;
        }

        private void match(int level, int cellID, int attributeID, double left, double right, double value) {
            for (int i = 0; i < table.get(attributeID).get(cellID).size(); i++)
                --counter[table.get(attributeID).get(cellID).get(i)];
            if (left >= right || level == numLevel)
                return;
            else if (value <= mid[cellID])
                match(level + 1, lchild[cellID], attributeID, left, mid[cellID], value);
            else
                match(level + 1, rchild[cellID], attributeID, mid[cellID], right, value);
        }

        public int getNumSub() {
            return numSub;
        }
    }
}
