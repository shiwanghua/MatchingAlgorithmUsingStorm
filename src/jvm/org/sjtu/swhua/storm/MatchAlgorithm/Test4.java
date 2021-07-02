package org.sjtu.swhua.storm.MatchAlgorithm;

import org.checkerframework.checker.units.qual.A;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Event;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.MyUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Test4 {
    public static void main(final String[] args) {

        int redundancy=3;
        final long intervalTime = 1000000000L;
        for(int numExecutor=6;numExecutor<=20;numExecutor++){

            long beginTime = System.nanoTime();
            MyUtils myUtils=new MyUtils(numExecutor,redundancy);
            long runTime = System.nanoTime() - beginTime;
            int numVSS1=myUtils.numVisualSubSet1;
            int numZeros=myUtils.getNumZerosOfCompleteNumbers();
            int numState=(int) Math.pow(2, numExecutor);
            int numVSS2=myUtils.numVisualSubSet2;
            int numCN=myUtils.getNumCompleteNumbers();
            int numVSS3=myUtils.numVisualSubSet3;
            System.out.println("numExecutor = "+numExecutor);
            System.out.println("numVSS1 = " + numVSS1);
            System.out.println("numVSS2 = " + numVSS2+", ratio = "+numCN+"/"+numState+" = "+(double)numCN/numState);
            System.out.println("numVSS3 = " + numVSS3+", ratio = "+numZeros+"/"+numState*numExecutor+" = "+(double)numZeros/numState/numExecutor);
            System.out.println("Run Time = "+(double)runTime/intervalTime+" s　= "+(double)runTime/intervalTime/60+" min\n\n");

        }




//        HashMap<Integer, HashSet<Integer>> m=new HashMap<>();
//        HashSet<Integer> v=new HashSet<>();
//        v.add(1);
//        m.put(1,v);
//
//        HashSet<Integer> g=m.get(1);
//        g.add(2);
//        g.add(3);
//        System.out.println(m.get(1));
//
//        HashSet<Integer> g2=new HashSet<>();
//        g2.addAll(m.get(1));
//        g2.add(4);
//        g2.add(5);
//        System.out.println(m.get(1));
//
//        HashSet<Integer> g3=new HashSet<>();
//        g3=m.get(1);
//        g3.add(6);
//        g3.add(7);
//        System.out.println(m.get(1));
//
//        HashSet<Integer> g4=new HashSet<>(m.get(1));
//        g4.add(8);
//        g4.add(9);
//        System.out.println(m.get(1));
    }

}
