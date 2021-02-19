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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
//import java.util.Map;

public class MergerBolt extends BaseRichBolt {
    private OutputToFile output;
    private OutputCollector collector;
    private TopologyContext boltContext;
    private StringBuilder log;
    private StringBuilder matchResult;
    private String boltName;
    private Integer numExecutor;
    private Integer numEventMatched;
    private HashMap<Integer, ArrayList<Integer>> mapSubIDtoExecutorID;

    public MergerBolt(String boltName, Integer numExecutor) {
        numEventMatched=0;
        this.boltName = boltName;
        this.numExecutor=numExecutor;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.boltContext=topologyContext;
        this.collector = outputCollector;
        output = new OutputToFile();
        log=new StringBuilder();
        matchResult=new StringBuilder();
        mapSubIDtoExecutorID=new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        Integer eventID = (Integer) tuple.getValueByField("eventID");
        ArrayList<Integer> subIDs = (ArrayList<Integer>) tuple.getValueByField("subIDs");

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("MatchResult"));
    }
}
