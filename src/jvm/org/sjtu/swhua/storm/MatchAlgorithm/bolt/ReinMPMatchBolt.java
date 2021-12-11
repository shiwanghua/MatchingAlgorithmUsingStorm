package org.sjtu.swhua.storm.MatchAlgorithm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.*;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Matcher.*;

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
    private String signature;
    private int groupID;
    private int boltID;
    private int numSubPacket;
    private int numEventPacket;
    private int numSubInserted;
    private int numSubInsertedLast;
    private int numEventMatched;
    private int numEventMatchedLast;
    final private int numVisualSubSet;
    final private int numExecutor;  // 大于１时表示一个bolt就是一个并行算子组，等于１时表示一个bolt就是一个匹配器
    private int executorID;
    //    static private int executorIDAllocator;
//        static private IDAllocator executorIDAllocator;
    final private int redundancy; // 这个可以删了
    private long runTime;
    private long speedTime;  // The time to calculate and record speed
    final private long beginTime;
    final private long intervalTime; // The interval between two calculations of speed

    public ReinMPMatchBolt(int groupid, int boltid, int num_executor, int redundancy_degree, int num_visual_subSet, ArrayList<String> VSSID_to_ExecutorID) {   // only execute one time for all executors!
        beginTime = System.nanoTime();
        groupID = groupid;
        boltID = boltid;
        intervalTime = TypeConstant.intervalTime;//60000000000L;  // 1 minute
//        executorIDAllocator = 0;
        //executorIDAllocator=new IDAllocator();
        numSubPacket = 0;
        numEventPacket = 0;
        numSubInserted = 0;
        numSubInsertedLast = 0;
        numEventMatched = 0;
        numEventMatchedLast = 0;
        runTime = 0;
        numExecutor = num_executor;
        redundancy = redundancy_degree;
        numVisualSubSet = num_visual_subSet;
        VSSIDtoExecutorID = VSSID_to_ExecutorID; // 不需要每个线程都获取整个 map, 只要拿到属于自己的那一列就好了，这里是为了简便实现和防止出错

        log = new StringBuilder();
//        matchResult = new StringBuilder();
    }

//    private synchronized void allocateID() {
//        executorID = executorIDAllocator++;
//    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) { // execute one time for every executor!

        boltContext = topologyContext;
        collector = outputCollector;
        boltName = boltContext.getThisComponentId();
        //allocateID();  // boltIDAllocator need to keep synchronized
        if (numExecutor > 1) // 本地运行时
            executorID = MyUtils.allocateID(boltName);
        else
            executorID = boltID;  // 一个bolt就是一个匹配器, boltID就是匹配器ID
        signature = boltName + ", GroupID=" + groupID + ", BoltID=" + boltID + ", ExecutorID=" + executorID;
        rein = new Rein();
        output = new OutputToFile();

        if (executorID == 0) {
            log = new StringBuilder("ReinMPMatchBolt " + signature);
            log.append("\n    ThreadName: " + Thread.currentThread().getName() + "\n    TaskID: ");
            List<Integer> taskIds = boltContext.getComponentTasks(boltContext.getThisComponentId());
            Iterator taskIdsIter = taskIds.iterator();
            int taskID;
            while (taskIdsIter.hasNext()) {
                taskID = (Integer) taskIdsIter.next();
                log.append(" ");
                log.append(taskID);
            }
            log.append("\n\n    numExecutor = ");
            log.append(numExecutor);
            log.append("\n    redundancy = ");
            log.append(redundancy);
            log.append("\n    numVisualSubSet = ");
            log.append(numVisualSubSet);
            log.append("\n    Map Table:\n    ID  ExecutorID");
            for (int i = 0; i < VSSIDtoExecutorID.size(); i++) {
                log.append(String.format("\n    %02d: ", i));
                log.append(VSSIDtoExecutorID.get(i));
            }
            log.append("\n\n");
            try {
                output.otherInfo(log.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else
            try {
                log = new StringBuilder("ReinMPMatchBolt " + signature);
                log.append("\n    ThreadName: " + Thread.currentThread().getName() + "\n    TaskID: ");
                List<Integer> taskIds = boltContext.getComponentTasks(boltContext.getThisComponentId());
                Iterator taskIdsIter = taskIds.iterator();
                int taskID;
                while (taskIdsIter.hasNext()) {
                    taskID = (Integer) taskIdsIter.next();
                    log.append(" ");
                    log.append(taskID);
                }
                //            log.append("\nThisTaskId: ");
                //            log.append(executorID);   // boltContext.getThisTaskId();
                log.append("\n\n");
                output.otherInfo(log.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        speedTime = System.nanoTime() + intervalTime;
    }

    @Override
    public void execute(Tuple tuple) {

        // 测速
//        collector.ack(tuple);
      /*  ArrayList<Subscription> subPacket = (ArrayList<Subscription>) tuple.getValueByField("SubscriptionPacket");
        if (VSSIDtoExecutorID.get((subPacket.get(0).getSubID()) % numVisualSubSet).charAt(executorID) != '0')
            numSubInserted++;
        else{
             //用emitDirect发送时，收到的订阅应该都是属于这个匹配器的
            log = new StringBuilder(signature);
            log.append(": subPacket ");
            log.append(tuple.getValueByField("PacketID"));
            log.append(", subID "+subPacket.get(0).getSubID()+" is not correctly emitted.\n");
            try {
                output.errorLog(log.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/
//        numEventMatched++;
//        if (System.nanoTime() > speedTime) {
//            runTime = System.nanoTime() - beginTime;
//            StringBuilder speedReport = new StringBuilder(signature);
//            speedReport.append(" - RunTime: ");
//            speedReport.append(runTime / intervalTime);
//            speedReport.append("min. numSubInserted: ");
//            speedReport.append(numSubInserted); //mapIDtoSub.size()
//            speedReport.append("; InsertSpeed: ");
//            speedReport.append(intervalTime / (numSubInserted - numSubInsertedLast + 1) / 1000);  // us/per 加一避免除以0
//            numSubInsertedLast = numSubInserted;
//            speedReport.append(". numEventMatched: ");
//            speedReport.append(numEventMatched);
//            speedReport.append("; MatchSpeed: ");
////            speedReport.append(runTime / numEventMatched / 1000); // us/per
//            speedReport.append(intervalTime / (numEventMatched - numEventMatchedLast + 1) / 1000);
//            numEventMatchedLast = numEventMatched;
//            speedReport.append(".\n");
//            try {
//                output.recordSpeed(speedReport.toString());
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            speedTime = System.nanoTime() + intervalTime;
//        }
//        return;

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
                    if (subPacket == null) {
                        log = new StringBuilder(signature);
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
                        if (VSSIDtoExecutorID.get(subID % numVisualSubSet).charAt(executorID) == '0') {
                            // 用emitDirect发送时，收到的订阅应该都是属于这个匹配器的
//                            log = new StringBuilder(signature);
//                            log.append(": subPacket ");
//                            log.append(tuple.getValueByField("PacketID"));
//                            log.append(", subID "+subID+" is not correctly emitted.\n");
//                            output.errorLog(log.toString());
                            continue;
                        }
                        log = new StringBuilder(signature);
                        log.append(": Sub ");
                        log.append(subID);
                        if (rein.insert(subPacket.get(i))) // no need to add if already exists
                        {
                            numSubInserted++;
//                       System.out.println("\n\n\n"+numSubInserted+"\n\n\n");
                            log.append(" is inserted.\n");
                        } else {
                            log.append(" is already inserted.\n");
                        }
                        output.writeToLogFile(log.toString());
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
                    if (tuple.getIntegerByField("MatchGroupID").equals(groupID)) {
                        numEventPacket++;
                        log = new StringBuilder(signature);
                        log.append(": EventPacket ");
                        log.append(numEventPacket);
                        log.append(" is received.\n");
                        output.writeToLogFile(log.toString());
                        ArrayList<Event> eventPacket = (ArrayList<Event>) tuple.getValueByField("EventPacket");
                        // 偶尔会出现为空的情况，而且是运行一段时间后产生，还与订阅集大小有关，改大一点可能就没错误了，很随机
                        if (eventPacket == null) {
                            log = new StringBuilder(signature);
                            log.append(": EventPacket ");
                            log.append(tuple.getValueByField("PacketID"));
                            log.append(" is null.\n");
                            output.writeToLogFile(log.toString());
                            collector.ack(tuple);
                            break;
                        }
                        int size = eventPacket.size(), eventID;
                        for (int i = 0; i < size; i++) {
//                            BitSet matchingBitset = rein.match(eventPacket.get(i));
//                            eventID = eventPacket.get(i).getEventID();
//                            System.out.println("\nEvent"+String.valueOf(eventID)+" from executor "+executorID+": "
//                                    +matchingBitset.size()+" "+numSubInserted+" "+rein.getNumSub()+"\n");
//                            log = new StringBuilder(signature);
//                            log.append(": EventID ");
//                            log.append(eventID);
//                            log.append(" matching task is done.\n");
//                            output.writeToLogFile(log.toString());
                            collector.emit(new Values(executorID, eventPacket.get(i).getEventID(), rein.match(eventPacket.get(i))));
                        }
                        numEventMatched += eventPacket.size();
                    }
                    collector.ack(tuple);
                    break;
                }
                default:
                    collector.fail(tuple);
                    log = new StringBuilder(signature);
                    log.append(": Wrong operation type is detected.\n");
                    output.writeToLogFile(log.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (System.nanoTime() > speedTime) {
            runTime = System.nanoTime() - beginTime;
            StringBuilder speedReport = new StringBuilder(signature);
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
        outputFieldsDeclarer.declare(new Fields("executorID", "eventID", "subIDBitset"));
    }

    public Integer getNumExecutor() {
        return numExecutor;
//        return boltIDAllocator;   //  this variable may not be the last executor number.
    }


}
