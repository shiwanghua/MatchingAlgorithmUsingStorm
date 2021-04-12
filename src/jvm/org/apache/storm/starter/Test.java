package org.apache.storm.starter;

//import com.esotericsoftware.kryo.kryo5.util.Null;

import org.apache.storm.starter.DataStructure.Pair;
import org.apache.storm.starter.DataStructure.Subscription;
import org.json.simple.JSONObject;

import java.lang.instrument.Instrumentation;
import java.util.*;


public class Test {
    public static void main(String[] args) throws Exception {
        HashMap<Integer, Boolean> m = new HashMap<>();
//        Scanner  scan_input = new Scanner(System.in);
//        int  q = scan_input.nextInt();

        HashMap<Integer, Integer> matchResultNum=new HashMap<>();
        matchResultNum.put(1,2);
        System.out.println(matchResultNum.get(1));
        matchResultNum.put(1,null);
        System.out.println(matchResultNum.get(1));
        matchResultNum.remove(1);
        System.out.println(matchResultNum.get(1));
        return;
    }
}
