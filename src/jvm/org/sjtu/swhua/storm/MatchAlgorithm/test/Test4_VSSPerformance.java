package org.sjtu.swhua.storm.MatchAlgorithm.test;

import org.checkerframework.checker.units.qual.A;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Event;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.MyUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Test4_VSSPerformance {
    public static void main(final String[] args) {

        int redundancy = 2;
        final long intervalTime = 1000000000L;
        for (int numExecutor = 1; numExecutor <= 20; numExecutor++) {

            long beginTime = System.nanoTime();
            MyUtils myUtils = new MyUtils(numExecutor, redundancy);
            long runTime = System.nanoTime() - beginTime;

            int numState = (int) Math.pow(2, numExecutor);
            int numVSS1 = myUtils.numVisualSubSet1;
            int numVSS2 = myUtils.numVisualSubSet2;
            int numCN = myUtils.getNumCompleteNumbers();
            HashMap<Integer, ArrayList<Integer>> mapVSS2 = myUtils.completeNumberToVSS2Num;
            int numVSS3 = myUtils.numVisualSubSet3;
            int numZeros = myUtils.getNumZerosOfCompleteNumbers();
            HashMap<Integer, ArrayList<Integer>> mapVSS3 = myUtils.zeroNumberToVSS3Num;
            int numVSS4 = myUtils.numVisualSubSet4;
            int numZeros2 = myUtils.getNumZeros2OfCompleteNumbers();
            HashMap<Integer, ArrayList<Integer>> mapVSS4 = myUtils.zeroNumber2ToVSS4Num;

            System.out.println("numExecutor = " + numExecutor);

            System.out.println("numVSS1 = " + numVSS1);

            System.out.println("numVSS2 = " + numVSS2 + ", ratio = " + numCN + "/" + numState + " = " + (double) numCN / numState);
            for (HashMap.Entry<Integer, ArrayList<Integer>> entry : mapVSS2.entrySet()) {
                System.out.print("    numCN=" + entry.getKey() + ": ");
                for (int i = 0; i < entry.getValue().size(); i++) {
                    System.out.print(entry.getValue().get(i) + " ");
                }
                System.out.println();
            }
            System.out.println();

            System.out.println("numVSS3 = " + numVSS3 + ", ratio = " + numZeros + "/" + numState * numExecutor + " = " + (double) numZeros / numState / numExecutor);
            for (HashMap.Entry<Integer, ArrayList<Integer>> entry : mapVSS3.entrySet()) {
                System.out.print("    numZN=" + entry.getKey() + ": ");
                for (int i = 0; i < entry.getValue().size(); i++) {
                    System.out.print(entry.getValue().get(i) + " ");
                }
                System.out.println();
            }
            System.out.println();

//            System.out.println("numVSS4 = " + numVSS4 + ", ratio = " + numZeros2 + "/" + numState * numExecutor * numExecutor + " = " + (double) numZeros / numState / numExecutor / numExecutor);
//            for (HashMap.Entry<Integer, ArrayList<Integer>> entry : mapVSS4.entrySet()) {
//                System.out.print("    numZ2N=" + entry.getKey() + ": ");
//                for (int i = 0; i < entry.getValue().size(); i++) {
//                    System.out.print(entry.getValue().get(i) + " ");
//                }
//                System.out.println();
//            }
//            System.out.println();

            System.out.println("Run Time = " + (double) runTime / intervalTime + " s　= " + (double) runTime / intervalTime / 60 + " min\n\n");

        }


//        HashMap<Integer, HashSet<Integer>> m=new HashMap<>();
//        m.getOrDefault(0,new HashSet<>()).add(111);
//        System.out.println(m.get(0));
//
//        HashSet<Integer> h= m.getOrDefault(1,new HashSet<>());
//        h.add(222);
//        System.out.println(m.get(1));

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
