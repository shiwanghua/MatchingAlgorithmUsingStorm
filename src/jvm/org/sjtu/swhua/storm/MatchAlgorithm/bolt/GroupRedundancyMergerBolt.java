package org.sjtu.swhua.storm.MatchAlgorithm.bolt;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.OutputToFile;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.TypeConstant;

import java.io.IOException;
import java.util.*;

public class GroupRedundancyMergerBolt extends BaseRichBolt {
    private OutputToFile output;
    private OutputCollector collector;
    private TopologyContext boltContext;
    private StringBuilder log;
    private StringBuilder matchResultBuilder;
    private String boltName;
    private String signature;
    private Integer boltID;
    private Integer executorID;
    //    private Integer numMatchExecutor;
    private Integer numEventMatched;
    private Integer numEventMatchedLast;
    private final Integer subSetSize;
    private long runTime;
    private long speedTime;  // The time to calculate and record speed
    final private long beginTime;
    final private long intervalTime; // The interval between two calculations of speed
    private int receive_max_event_id = 0;
    private HashMap<Integer, Boolean> recordStatus;

    @SuppressWarnings("resource")
    final private String topicName = "match_result";
    Properties props;
    KafkaProducer<String, Object> resultProducer;

    public GroupRedundancyMergerBolt(Integer boltid) {
        boltID = boltid;
        beginTime = System.nanoTime();
        intervalTime = TypeConstant.intervalTime;  // 1 minute
        subSetSize=TypeConstant.subSetSize;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        boltContext = topologyContext;
        collector = outputCollector;
        boltName = boltContext.getThisComponentId();
        executorID = boltContext.getThisTaskId();
        signature = boltName + ", BoltID=" + boltID + ", executorID=" + executorID;
        output = new OutputToFile();
        log = new StringBuilder();
        matchResultBuilder = new StringBuilder();
        recordStatus = new HashMap<>();
        numEventMatched = 0;
        numEventMatchedLast = 0;
        runTime = 0;

        try {
            log = new StringBuilder(signature);
            log.append("\n    ThreadName: " + Thread.currentThread().getName() + "\n" + "    TaskID:");
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
            log.append("\n\n");
            output.otherInfo(log.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        speedTime = System.nanoTime() + intervalTime;
    }

    @Override
    public void execute(Tuple tuple) {
        Integer eventID = tuple.getIntegerByField("eventID");
//        System.out.println("\nEventID= "+eventID+" receive_max_event_id="+receive_max_event_id+".\n");
        // true代表得到了完整匹配集，false代表还没收到过这个事件的匹配结果
        // 每个事件会有 redundancy-1 个进来
        if (recordStatus.getOrDefault(eventID, false) == true) {
            log = new StringBuilder(signature);
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
            BitSet subIDBitset = (BitSet) tuple.getValueByField("subIDBitset");
//            System.out.println("\nEventID= "+eventID+" receive_max_event_id="+receive_max_event_id+".\n");
            recordStatus.put(eventID, true);
            matchResultBuilder = new StringBuilder(signature);
            matchResultBuilder.append(" - EventID: ");
            matchResultBuilder.append(eventID);
            matchResultBuilder.append("; MatchedSubNum: ");
            matchResultBuilder.append(subSetSize-subIDBitset.stream().count());
            matchResultBuilder.append("; SubID:");
//            for (int i = subIDBitset.nextClearBit(0); i >= 0&&i<=subSetSize; subIDBitset.nextClearBit(i + 1)) {
//                matchResultBuilder.append(" ");
//                matchResultBuilder.append(i);
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
        } // Add a partial result

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
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
