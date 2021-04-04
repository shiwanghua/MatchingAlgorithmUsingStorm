package org.apache.storm.starter;

//import com.esotericsoftware.kryo.kryo5.util.Null;
import org.apache.storm.starter.DataStructure.Pair;
import org.apache.storm.starter.DataStructure.Subscription;
import org.json.simple.JSONObject;

import java.lang.instrument.Instrumentation;
import java.util.*;


public class Test {
    public static void main(String[] args) throws Exception {
        HashMap<String, ArrayList<LinkedList<Pair<Integer, Double>>>> supBuckets=new HashMap<>();
        supBuckets.put("123",new ArrayList<>(new ArrayList<>(new LinkedList<>())));
        supBuckets.get("123").add(new LinkedList<>());
        System.out.println(supBuckets.get("123").size());
        System.out.println(supBuckets.get("123").get(0));
        return;
    }
}
