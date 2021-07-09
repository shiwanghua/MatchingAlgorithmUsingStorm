package org.sjtu.swhua.storm.MatchAlgorithm.bolt;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.OutputToFile;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.TypeConstant;
import org.sjtu.swhua.storm.MatchAlgorithm.KafkaProducer.SubscriptionProducer;


import java.io.IOException;
import java.util.*;
import java.lang.Math;
//import java.util.Map;

public class MultiPartitionMergerBolt extends BaseRichBolt {
    private OutputToFile output;
    private OutputCollector collector;
    private TopologyContext boltContext;
    private StringBuilder log;
    private StringBuilder matchResultBuilder;
    private String boltName;
    private Integer executorID;
    private Integer numMatchExecutor;
    //    private Integer redundancy;
    private Integer numEventMatched;
    private Integer numEventMatchedLast;
    private long runTime;
    private long speedTime;  // The time to calculate and record speed
    final private long beginTime;
    final private long intervalTime; // The interval between two calculations of speed

    private HashMap<Integer, HashSet<Integer>> matchResultMap;
    //private HashMap<Integer, HashSet<Integer>> matchResultNum;
    private HashMap<Integer, Integer> recordStatus;
    private Boolean[] executorCombination;

    @SuppressWarnings("resource")
    final private String topicName = "match_result";
    Properties props;
    KafkaProducer<String, Object> resultProducer;

    private int receive_max_event_id = 0;

    public MultiPartitionMergerBolt(Integer num_executor, Integer redundancy_degree, Boolean[] executor_combination) {
        numMatchExecutor = num_executor; // receive a eventID from this number of matchBolts then the event is fully matched
//        redundancy = redundancy_degree;
        executorCombination = executor_combination;
        beginTime = System.nanoTime();
        intervalTime = TypeConstant.intervalTime;  // 1 minute
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
        //matchResultNum = new HashMap<>();
        recordStatus = new HashMap<>();
        numEventMatched = 1;
        numEventMatchedLast = 0;
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
            log.append("\nThisTaskId: ");
            log.append(executorID);
            log.append(";\nNumberOfMatchExecutor: ");
            log.append(numMatchExecutor); // need to be checked carefully
            log.append("\nComplete Executor Combination:\n");
            int count = 0;
            for (int i = 0; i < executorCombination.length; i++) {
                if (executorCombination[i] == true) {
                    log.append(i);
                    log.append(" ");
                    count++;
                }
            }
            log.append(" TotalNum: ");
            log.append(count);
            log.append("\n\n");
            output.otherInfo(log.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }


//        // create instance for properties to access producer configs
//        props = new Properties();
//        //Assign localhost id
//        props.put("bootstrap.servers", "swhua:9092");
//        //Set acknowledgements for producer requests.
//        props.put("acks", "all");
//        //If the request fails, the producer can automatically retry
//        props.put("retries", 1);
//        props.put("metadata.fetch.timeout.ms", 30000);
//        //Specify buffer size in config
//        props.put("batch.size", 16384);
//        //Reduce the no of requests less than 0
//        props.put("linger.ms", 1);
//        //The buffer.memory controls the total amount of memory available to the producer for buffering.
//        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.sjtu.swhua.storm.MatchAlgorithm.serialization.KafkaSerializer");
//        resultProducer = new KafkaProducer<String, Object>(props);
    }

    @Override
    public void execute(Tuple tuple) {
        Integer eventID = tuple.getIntegerByField("eventID");
        // 每个事件会有 redundancy-1 个进来
        // -1代表得到了完整匹配集，0代表还没收到过这个事件的部分匹配结果
        if (recordStatus.getOrDefault(eventID, 0) == -1) {
            log = new StringBuilder(boltName);
            log.append(" Thread ");
            log.append(executorID);
            log.append(" - EventID ");
            log.append(eventID);
            log.append(" is already processed.\n");
            try {
                output.writeToLogFile(log.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            receive_max_event_id = Math.max(eventID, receive_max_event_id);
            if (!recordStatus.containsKey(eventID)) {
                // matchResultNum.put(eventID, new HashSet<>());
                matchResultMap.put(eventID, new HashSet<>());
                recordStatus.put(eventID, 0);
            }
            HashSet<Integer> resultSet = matchResultMap.get(eventID);  // This is an reference !
            ArrayList<Integer> subIDs = (ArrayList<Integer>) tuple.getValueByField("subIDs");
//            for (int i = 0; i < subIDs.size(); i++)
//                resultSet.add(subIDs.get(i));
            resultSet.addAll(subIDs);
            // matchResultNum.get(eventID).add(tuple.getInteger(0));
            Integer nextState = recordStatus.get(eventID) | (1 << tuple.getIntegerByField("executorID"));
            //if (matchResultNum.get(eventID).size() == redundancy) {
            if (executorCombination[nextState]) {
                matchResultBuilder = new StringBuilder(boltName);
                matchResultBuilder.append(" Thread ");
                matchResultBuilder.append(executorID);
                matchResultBuilder.append(" - EventID: ");
                matchResultBuilder.append(eventID);
                matchResultBuilder.append("; MatchedSubNum: ");
                matchResultBuilder.append(resultSet.size());
//            matchResultBuilder.append("; SubID:");
//            Iterator<Integer> setIterator = resultSet.iterator();
//            while (setIterator.hasNext()) {
//                matchResultBuilder.append(" ");
//                matchResultBuilder.append(setIterator.next());
//            }
                matchResultBuilder.append("; receive_max_event_id: ");
                matchResultBuilder.append(receive_max_event_id);
                matchResultBuilder.append(".\n");
                try {
                    output.saveMatchResult(matchResultBuilder.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
//            resultProducer.send(new ProducerRecord<String, Object>(topicName, Integer.toString(eventID), resultSet), new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata metadata, Exception exception) {
//                    if (metadata != null) {
//                        System.out.println("第" + String.valueOf(eventID) + "个事件匹配完成：metadata.checksum: " + metadata.checksum()
//                                + " metadata.offset: " + metadata.offset() + " metadata.partition: " + metadata.partition() + " metadata.topic: " + metadata.topic());
//                    }
//                    if (exception != null) {
//                        System.out.println("第" + String.valueOf(eventID)  + "个事件匹配发送到主题时产生异常：" + exception.getMessage());
//                    }
//                }
//            });
                numEventMatched++;
//            matchResultMap.put(eventID, null);
                matchResultMap.remove(eventID);
//            recordStatus.remove(eventID);
                recordStatus.put(eventID, -1); // 表示已经完成
            } else
                recordStatus.put(eventID, nextState);
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
//        outputFieldsDeclarer.declare(new Fields("MatchResult"));
    }
}
