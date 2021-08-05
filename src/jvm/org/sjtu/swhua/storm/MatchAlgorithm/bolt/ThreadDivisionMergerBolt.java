package org.sjtu.swhua.storm.MatchAlgorithm.bolt;

import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.OutputToFile;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.*;


public class ThreadDivisionMergerBolt extends BaseRichBolt {
    private OutputToFile output;
    private OutputCollector collector;
    private TopologyContext boltContext;
    private StringBuilder log;
    private StringBuilder matchResultBuilder;
    private String boltName;
    private Integer executorID;
    private Integer numMatchExecutor;
    private Integer redundancy;
    private Integer numEventMatched;
    private long runTime;
    private long speedTime;  // The time to calculate and record speed
    final private long beginTime;
    final private long intervalTime; // The interval between two calculations of speed

    private HashMap<Integer, HashSet<Integer>> matchResultMap;
    private HashMap<Integer, HashSet<Integer>> matchResultNum;
    private Boolean[] executorCombination;

    public ThreadDivisionMergerBolt(Integer num_executor, Integer redundancy_degree, Boolean[] executor_combination) {
        numMatchExecutor = num_executor; // receive a eventID from this number of matchBolts then the event is fully matched
        redundancy = redundancy_degree;
        executorCombination = executor_combination;
        beginTime = System.nanoTime();
        intervalTime = 60000000000L;  // 1 minute
//        numMatchExecutor = ThreadDivisionMatchBolt.getNumExecutor(); // This function may not return the final right number. MergerBolt may be initialized before matchBolt!
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
        matchResultNum = new HashMap<>();
        numEventMatched = 1;
        runTime = 1;
        speedTime = System.nanoTime() + intervalTime;

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
            log.append("\n    ThisTaskId: ");
            log.append(executorID);
            log.append(";\n    NumberOfMatchExecutor: ");
            log.append(numMatchExecutor); // need to be checked carefully
            log.append("\n\n    Complete Executor Combination:\n");
            for (int i = 0; i < executorCombination.length; i++) {
                if (executorCombination[i] == true) {
                    log.append(i);
                    log.append(" ");
                }
            }
            output.otherInfo(log.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Integer eventID = tuple.getInteger(1);
        if (!matchResultMap.containsKey(eventID)) {
            matchResultNum.put(eventID, new HashSet<>());
            matchResultMap.put(eventID, new HashSet<>());
        }
//        else if (matchResultNum.get(eventID).size() == redundancy) {
//            matchResultNum.remove(eventID);
//            collector.ack(tuple);
//            return;
//        }
        ArrayList<Integer> subIDs = (ArrayList<Integer>) tuple.getValueByField("subIDs");

        HashSet<Integer> resultSet = matchResultMap.get(eventID);  // This is an reference.
        for (int i = 0; i < subIDs.size(); i++)
            resultSet.add(subIDs.get(i));
        matchResultNum.get(eventID).add(tuple.getInteger(0));
        if (matchResultNum.get(eventID).size() == redundancy) {
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
            numEventMatched++;
            matchResultMap.put(eventID, null);
            matchResultMap.remove(eventID);
        }
        collector.ack(tuple);

        if (System.nanoTime() > speedTime) {
            runTime = System.nanoTime() - beginTime;
            StringBuilder speedReport = new StringBuilder(boltName);
            speedReport.append(" Thread ");
            speedReport.append(executorID);
            speedReport.append(" - RunTime: ");
            speedReport.append(runTime / intervalTime);
            speedReport.append("min. numEventMatched: ");
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
//        outputFieldsDeclarer.declare(new Fields("MatchResult"));
    }
}
