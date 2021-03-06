package org.apache.storm.starter.bolt;

import org.apache.storm.starter.DataStructure.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;


import java.io.IOException;
import java.util.*;
//import java.util.Map;

public class MergerBolt extends BaseRichBolt {
    private OutputToFile output;
    private OutputCollector collector;
    private TopologyContext boltContext;
    private StringBuilder log;
    private StringBuilder matchResultBuilder;
    private String boltName;
    private Integer executorID;
    static private Integer numMatchExecutor;
    private Integer numEventMatched;
    private HashMap<Integer, HashSet<Integer>> matchResultMap;
    private HashMap<Integer, HashSet<Integer>> matchResultNum;

    public MergerBolt() {
        numMatchExecutor = ThreadDivisionMatchBolt.getNumExecutor();
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        boltContext = topologyContext;
        boltName = boltContext.getThisComponentId();
        executorID = boltContext.getThisTaskId();
        collector = outputCollector;
        output = new OutputToFile();
        log = new StringBuilder();
        matchResultBuilder = new StringBuilder();
        matchResultMap = new HashMap<>();
        numEventMatched = 0;

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
            log.append(executorID);
            log.append("; NumberOfMatchExecutor: ");
            log.append(numMatchExecutor);
            log.append("\n\n");
            output.otherInfo(log.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
//        collector.ack(tuple);
//        return;
        Integer eventID = tuple.getInteger(1);
        ArrayList<Integer> subIDs = (ArrayList<Integer>) tuple.getValueByField("subIDs");
        if (!matchResultNum.containsKey(eventID)) {
            matchResultNum.put(eventID, new HashSet<>());
            matchResultMap.put(eventID, new HashSet<>());
        }
        HashSet<Integer> resultSet = matchResultMap.get(eventID);
        for (int i = 0; i < subIDs.size(); i++)
            resultSet.add(subIDs.get(i));
        matchResultNum.get(eventID).add(tuple.getInteger(0));
        if (matchResultNum.get(eventID).size() == numMatchExecutor) {
            matchResultBuilder = new StringBuilder(boltName);
            matchResultBuilder.append(" Thread ");
            matchResultBuilder.append(executorID);
            matchResultBuilder.append(" - EventID: ");
            matchResultBuilder.append(eventID);
            matchResultBuilder.append("; MatchedSubNum: ");
            matchResultBuilder.append(resultSet.size());
            matchResultBuilder.append("; SubID:");

            Iterator<Integer> setIterator = resultSet.iterator();
            while (setIterator.hasNext()) {
                matchResultBuilder.append(" ");
                matchResultBuilder.append(setIterator.next());
            }
            matchResultBuilder.append(".\n");
            try {
                output.saveMatchResult(matchResultBuilder.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }

            matchResultNum.remove(eventID);
            matchResultMap.remove(eventID);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("MatchResult"));
    }
}
