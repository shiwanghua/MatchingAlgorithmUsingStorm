package org.sjtu.swhua.storm.MatchAlgorithm.test;

import java.util.BitSet;

public class Test5 {

    public static void main(final String[] args) {
        BitSet b1=new BitSet(10);
        BitSet b2=new BitSet(12);
        b1.set(1);
        System.out.println(b1);
        b2.set(8,20);
        System.out.println(b2);
        System.out.println(b2.size());
        b1.or(b2);

        System.out.println(b1.stream().count());
        System.out.println(Runtime.getRuntime().availableProcessors());
    }
}
