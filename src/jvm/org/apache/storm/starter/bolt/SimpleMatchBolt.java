package org.apache.storm.starter.bolt;

//import com.esotericsoftware.kryo.Kryo;
import org.apache.storm.starter.DataStrcture.Event;
import org.apache.storm.starter.DataStrcture.Subscription;
import org.apache.storm.starter.DataStrcture.TypeConstant;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;

public class SimpleMatchBolt extends BaseBasicBolt {

    OutputCollector collector;
    private HashMap<Integer,Subscription> mapIDtoSub;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // olution A: catch Exception to find whether the tuple is a subPacket or a eventPacket
//        try {
//            ArrayList<Subscription> subPacket=(ArrayList<Subscription>) tuple.getValueByField("SubscriptionPacket");
//
//        }
//        catch (IllegalArgumentException e) {
//
//        }
//        try {
//         ArrayList<Event> eventPacket = (ArrayList<Event>) tuple.getValueByField("EventPacket");
//        }
//        catch (IllegalArgumentException e){
//
//        }

        // Solution B: get the operation type to find what the tuple is
        int type = (int)tuple.getValue(0);
        switch(type){
            case TypeConstant.Insert_Subscription:

            case TypeConstant.Insert_Attribute_Subscription:

            case TypeConstant.Update_Attribute_Subscription:

            case TypeConstant.Delete_Attribute_Subscription:

            case TypeConstant.Delete_Subscription:

            default:
                System.out.println("Wrong");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MatchResult"));
    }
}
