package org.sjtu.swhua.storm.MatchAlgorithm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.*;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Matcher.Rein;

import java.io.IOException;
import java.util.*;

public class GroupRedundancyMatchBolt extends BaseRichBolt {
    private OutputToFile output;
    private OutputCollector collector;
    private TopologyContext boltContext;
    private Rein rein;
    private ArrayList<String> VSSIDtoExecutorID;

    private StringBuilder log;

    private String boltName;
    private String signature;
    private Integer groupID;
    private Integer boltID;
    private int numSubPacket;
    private int numEventPacket;
    private int numSubInserted;
    private int numSubInsertedLast;
    private int numEventMatched;
    private int numEventMatchedLast;
    //    final private int numExecutor;
    private int executorID;

    private long runTime;
    private long speedTime;  // The time to calculate and record speed
    final private long beginTime;
    final private long intervalTime; // The interval between two calculations of speed

    public GroupRedundancyMatchBolt(int groupid, int boltid, int executorId) {
        beginTime = System.nanoTime();
        intervalTime = TypeConstant.intervalTime;//60000000000L;  // 1 minute
        groupID = groupid;
        boltID = boltid;
        executorID = executorId;
        numSubPacket = 0;
        numEventPacket = 0;
        numSubInserted = 0;
        numSubInsertedLast = 0;
        numEventMatched = 0;
        numEventMatchedLast = 0;
        runTime = 0;
        log = new StringBuilder();
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) { // execute one time for every executor!
        boltContext = topologyContext;
        collector = outputCollector;
        boltName = boltContext.getThisComponentId();
        signature = boltName + ", GroupID=" + groupID + ", BoltID=" + boltID + ", executorID=" + executorID;
        rein = new Rein();
        output = new OutputToFile();
        try {
            log = new StringBuilder("GroupRedundancyMatchBolt " + signature);
            log.append("\n    ThreadName: " + Thread.currentThread().getName() + "\n    TaskID: ");
            List<Integer> taskIds = boltContext.getComponentTasks(boltContext.getThisComponentId());
            Iterator taskIdsIter = taskIds.iterator();
            int taskID;
            while (taskIdsIter.hasNext()) {
                taskID = (Integer) taskIdsIter.next();
                log.append(" ");
                log.append(taskID);
            }
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
                    System.out.println("Delete a subscription.\n");
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
                            eventID = eventPacket.get(i).getEventID();
                            log = new StringBuilder(signature);
                            log.append(": Begin to match eventID ");
                            log.append(eventID+"\n");
                            output.writeToLogFile(log.toString());
                            BitSet matchingBitset = rein.match(eventPacket.get(i));
                            System.out.println("\nEvent"+String.valueOf(eventID)+" from group "+groupID+", executor "+executorID+": "
                                    +matchingBitset.size()+" "+numSubInserted+" "+rein.getNumSub()+"\n");
                            log = new StringBuilder(signature);
                            log.append(": EventID ");
                            log.append(eventID);
                            log.append(" matching task is done.\n");
                            output.writeToLogFile(log.toString());
                            collector.emit(new Values(eventID, matchingBitset));
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
            speedReport.append(numSubInserted);
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
        outputFieldsDeclarer.declare(new Fields("eventID", "subIDBitset"));
    }
}
