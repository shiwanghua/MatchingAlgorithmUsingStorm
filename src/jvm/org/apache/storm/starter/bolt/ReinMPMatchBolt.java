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
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ReinMPMatchBolt extends BaseRichBolt {
    private OutputToFile output;
    private OutputCollector collector;
    private TopologyContext boltContext;
    private Rein rein;
    static private ArrayList<String> VSSIDtoExecutorID;

    private StringBuilder log;
    private StringBuilder matchResult;

    private String boltName;
    private Integer numSubPacket;
    private Integer numEventPacket;
    private Integer numSubInserted;
    private Integer numEventMatched;
    final private Integer numVisualSubSet;
    final private Integer numExecutor;
    private Integer executorID;
    static private IDAllocator executorIDAllocator;
    static private Integer redundancy;
    private long runTime;
    private long speedTime;  // The time to calculate and record speed
    final private long beginTime;
    final private long intervalTime; // The interval between two calculations of speed

    public ReinMPMatchBolt(Integer num_executor,Integer redundancy_degree) {   // only execute one time for all executors!
        beginTime = System.nanoTime();
        intervalTime = 60000000000L;  // 1 minute
        executorIDAllocator=new IDAllocator();
        numSubPacket = 0;
        numEventPacket = 0;
        numSubInserted = 1;
        numEventMatched = 1;
        runTime = 1;
        numExecutor=num_executor;
        redundancy=redundancy_degree;

        log = new StringBuilder();
        matchResult = new StringBuilder();

        // calculate the number of visual subset
        int n=1,m=1,nm=1;
        for(int i=2; i<=numExecutor; i++) {
            n *= i;
            if (i == redundancy)
                m = n;
            if (i == (numExecutor - redundancy))
                nm = n;
        }
        numVisualSubSet=n/m/nm;
        mpv=new HashMap<>();
        VSSIDtoExecutorID=SubsetCodeGeneration(redundancy,numExecutor);
        mpv=null;  //  Now is not needed.
    }

//    public synchronized void allocateID(){
//        executorID = executorIDAllocator.allocateID();//boltContext.getThisTaskId(); // Get the current thread number
//    }

    static private HashMap<Pair<Integer, Integer>, ArrayList<String>> mpv;

    //  从K位里生成含k个1的字符串的集合
    ArrayList<String> SubsetCodeGeneration(int k, int K) {
        if (k == 0) return new ArrayList<String>(){{add(StringUtils.repeat("0", K));}};
        if (mpv.containsKey(Pair.of(k, K))) return mpv.get(Pair.of(k, K));
        ArrayList<String> strSet=new ArrayList<>();
        for (int i = k; i <= K; i++)  //  只有前 i 位有1且第 i 位必须是1
        {
            String highStr = StringUtils.repeat("0",K - i) + "1";
            //  从前i-1位里生成含k-1个1的字符串的集合
            ArrayList<String> lowPart = SubsetCodeGeneration(k - 1, i - 1);
            mpv.put(Pair.of(k - 1, i - 1),lowPart);
            for (int j=0;j<lowPart.size();j++)
                strSet.add(highStr + lowPart.get(j));
        }
        return strSet;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) { // execute one time for every executor!

        speedTime = System.nanoTime() + intervalTime;
        boltContext = topologyContext;
        collector = outputCollector;
        boltName = boltContext.getThisComponentId();
        executorID=executorIDAllocator.allocateID();
//        allocateID();  // boltIDAllocator need to keep synchronized
        rein=new Rein();
        output = new OutputToFile();

        if(executorID==0){
            log=new StringBuilder(boltName);
            log.append("MultiPartitionMatchBolt \nnumExecutor = ");
            log.append(numExecutor);
            log.append("\nredundancy = ");
            log.append(redundancy);
            log.append("\nnumVisualSubSet = ");
            log.append(numVisualSubSet);
            log.append("\nMap Table:\nID  ExecutorID");
            for(int i=0;i<VSSIDtoExecutorID.size();i++){
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

        int type=tuple.getInteger(0);
        try {
            switch (type) {
                case TypeConstant.Insert_Subscription: {

                    Integer subPacketID=tuple.getInteger(1);

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
                    for (int i = 0; i < subPacket.size(); i++) {
                        subID = subPacket.get(i).getSubID();
                        if (VSSIDtoExecutorID.get(subID % numVisualSubSet).charAt(executorID)=='0')
                            continue;
                        rein.insert(subPacket.get(i));
//                        mapIDtoSub.put(subID, subPacket.get(i));
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
                    for (int i = 0; i < eventPacket.size(); i++) {
                        ArrayList<Integer> matchedSubIDList = rein.match(eventPacket.get(i));
                        log = new StringBuilder(boltName);
                        log.append(" Thread ");
                        log.append(executorID);
                        log.append(": EventID ");
                        log.append(eventPacket.get(i).getEventID());
                        log.append(" matching task is done.\n");
                        output.writeToLogFile(log.toString());
                        collector.emit(new Values(executorID, eventPacket.get(i).getEventID(), matchedSubIDList));
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
        outputFieldsDeclarer.declare(new Fields("executorID", "eventID","subIDs"));
    }

    public Integer getNumExecutor(){
        return numExecutor;
//        return boltIDAllocator;   //  this variable may not be the last executor number.
    }
}
