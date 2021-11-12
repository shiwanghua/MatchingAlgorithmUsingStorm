package org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Matcher;

import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Event;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Pair;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Subscription;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.TypeConstant;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// 并行化Rein
public class pRein {

    private int numBucket, numSub, numAttributeType;
    private double bucketSpan;
    private ArrayList<Integer> mapToSubID;
    private HashMap<Integer, Boolean> exist;
    private ArrayList<ArrayList<LinkedList<Pair<Integer, Double>>>> infBuckets; // Attribute ID -> bucket id -> a bucket list -> (subID,subVlue)
    private ArrayList<ArrayList<LinkedList<Pair<Integer, Double>>>> supBuckets;
    private boolean[][][] leftBits;
    private boolean[][][] rightBits;

    private int threadNum;
    private int maxThreadNum;
    private ExecutorService executorService;
    private int[] attributeNum;
    private int[] attributeThread; // 记录哪个线程负责这个属性
    private int[] workloadThread;  // 记录每个线程的负载情况(NumPredicates)

    public pRein() {
        numSub = 0;
        numBucket = TypeConstant.numBucket;
        numAttributeType = TypeConstant.numAttributeType;
        bucketSpan = 1.0 / numBucket;
        mapToSubID = new ArrayList<>();
        exist = new HashMap<>();
        infBuckets = new ArrayList<>();
        supBuckets = new ArrayList<>();
        for (int i = 0; i < TypeConstant.numAttributeType; i++) {
            infBuckets.add(new ArrayList<>());
            supBuckets.add(new ArrayList<>());
            for (int j = 0; j < numBucket; j++) {
                infBuckets.get(i).add(new LinkedList<>());
                supBuckets.get(i).add(new LinkedList<>());
            }
        }
        leftBits = new boolean[numAttributeType][3][TypeConstant.subSetSize];  // [0.0,0.25] [0.0,0.5] [0.0,0.75] high
        rightBits = new boolean[numAttributeType][3][TypeConstant.subSetSize]; // [0.25,1.0] [0.5,1.0] [0.75,1.0] low

        threadNum=4;
        maxThreadNum=Math.min(Runtime.getRuntime().availableProcessors(),numAttributeType);
        executorService=new ThreadPoolExecutor(1,maxThreadNum,1L, TimeUnit.SECONDS,new SynchronousQueue<>());
        attributeNum=new int[numAttributeType];
        attributeThread=new int[numAttributeType];
        workloadThread=new int[maxThreadNum];
    }

    public boolean insert(Subscription sub) {
        int subID = sub.getSubID();
        if (exist.getOrDefault(subID, false) == true)
            return false;
        int subAttributeID, lowBucketID, highBucketID;
        double low, high;
        HashMap.Entry<Integer, Pair<Double, Double>> subAttributeEntry;
        Iterator<HashMap.Entry<Integer, Pair<Double, Double>>> subAttributeIterator = sub.getMap().entrySet().iterator();
        while (subAttributeIterator.hasNext()) {
            subAttributeEntry = subAttributeIterator.next();
            subAttributeID = subAttributeEntry.getKey();
            low = subAttributeEntry.getValue().getFirst();
            high = subAttributeEntry.getValue().getSecond();
            lowBucketID = (int) (low / bucketSpan);
            highBucketID = Math.min((int) (high / bucketSpan),numBucket-1); // high==1时会越界
            infBuckets.get(subAttributeID).get(lowBucketID).add(Pair.of(numSub, low));
            supBuckets.get(subAttributeID).get(highBucketID).add(Pair.of(numSub, high));
            // 从右往左判断插入位置
            if (lowBucketID >= 0.75 * (numBucket - 1)) {  // 9, >=6, 678; 10, >=7, 789; 11, >=8, 8910;
                rightBits[subAttributeID][0][numSub] = true;
                rightBits[subAttributeID][1][numSub] = true;
                rightBits[subAttributeID][2][numSub] = true;
            } else if (lowBucketID >= 0.5 * (numBucket - 1)) { // 9, >=4, 45678;
                rightBits[subAttributeID][0][numSub] = true;
                rightBits[subAttributeID][1][numSub] = true;
            } else if (lowBucketID >= 0.25 * numBucket)
                rightBits[subAttributeID][0][numSub] = true;

            // 从左往右判断插入位置
            if (highBucketID <= 0.25 * numBucket) {
                leftBits[subAttributeID][0][numSub] = true;
                leftBits[subAttributeID][1][numSub] = true;
                leftBits[subAttributeID][2][numSub] = true;
            } else if (highBucketID <= 0.5 * (numBucket - 1)) {
                leftBits[subAttributeID][1][numSub] = true;
                leftBits[subAttributeID][2][numSub] = true;
            } else if (highBucketID <= 0.75 * numBucket)
                leftBits[subAttributeID][2][numSub] = true;
            attributeNum[subAttributeID]++;
        }
        mapToSubID.add(subID);  //  add this map to ensure the size of bits array int match() is right, since each executor will not get a successive subscription set
        exist.put(subID, true);
        numSub++;   //  after Deletion operation, numSub!=numSubInserted, so variable 'numSubInserted' is needed.
        return true;
    }

    public ArrayList<Integer> match(Event e) {

        boolean[] bits = new boolean[numSub];
//        Integer eventAttributeID;
        Double attributeValue;
        int eventBucketID;
        double lowBorder = numBucket, highBorder = 0;

        // Solution: each event has all attribute types i.e.: e.getMap().size()==numAttributeType
//        HashMap.Entry<Integer, Double> eventAttributeEntry;
//        Iterator<HashMap.Entry<Integer, Double>> eventAttributeIterator = e.getMap().entrySet().iterator();
//        while (eventAttributeIterator.hasNext()) {
//            eventAttributeEntry = eventAttributeIterator.next();
//            eventAttributeID = eventAttributeEntry.getKey();
//            attributeValue = eventAttributeEntry.getValue();
//            bucketID = (int) (attributeValue / bucketSpan);
//
//            for (Pair<Integer, Double> subIDValue : infBuckets.get(eventAttributeID).get(bucketID))
//                if (subIDValue.value2 > attributeValue)
//                    bits[subIDValue.value1] = true;
//            for (int i = bucketID + 1; i < numBucket; i++)
//                for (Pair<Integer, Double> subIDValue : infBuckets.get(eventAttributeID).get(i))
//                    bits[subIDValue.value1] = true;
//
//            for (Pair<Integer, Double> subIDValue : supBuckets.get(eventAttributeID).get(bucketID))
//                if (subIDValue.value2 < attributeValue)
//                    bits[subIDValue.value1] = true;
//            for (int i = 0; i < bucketID; i++)
//                for (Pair<Integer, Double> subIDValue : infBuckets.get(eventAttributeID).get(i))
//                    bits[subIDValue.value1] = true;
//        }

//        HashMap<Integer,Double> attributeIDToValue=e.getMap();
        for (int i = 0; i < numAttributeType; i++) {   // i: attributeID
            attributeValue = e.getAttributeValue(i);
            if (attributeValue == null) {  // all sub containing this attribute should be marked, only either sup or inf is enough.
                for (int j = 0; j < numSub; j++)
                    bits[j] = bits[j] | rightBits[i][0][j];
                for (int j = 0; j < 0.25 * numBucket; j++) {  // j: BucketID
                    for (Iterator<Pair<Integer, Double>> pairIterator = infBuckets.get(i).get(j).iterator(); pairIterator.hasNext(); ) {
                        bits[pairIterator.next().getFirst()] = true;
                    } // LinkedList
                } // Bucket ArrayList
            } else {
                eventBucketID = (int) (attributeValue / bucketSpan);

                // 从左往右判断cell
                if (eventBucketID < 0.25 * numBucket) {
                    lowBorder = 0.25 * numBucket;
                    for (int j = 0; j < numSub; j++)
                        bits[j] = bits[j] | rightBits[i][0][j];
                } else if (eventBucketID < 0.5 * (numBucket - 1)) {
                    lowBorder = 0.5 * (numBucket - 1);
                    for (int j = 0; j < numSub; j++)
                        bits[j] = bits[j] | rightBits[i][1][j];
                } else if (eventBucketID < 0.75 * (numBucket - 1)) {
                    lowBorder = 0.75 * (numBucket - 1);
                    for (int j = 0; j < numSub; j++)
                        bits[j] = bits[j] | rightBits[i][2][j];
                }

                // 从右往左判断cell
                if (eventBucketID > 0.75 * numBucket) {
                    highBorder = 0.75 * numBucket;
                    for (int j = 0; j < numSub; j++)
                        bits[j] = bits[j] | leftBits[i][2][j];
                } else if (eventBucketID > 0.5 * (numBucket - 1)) {
                    highBorder = 0.5 * (numBucket - 1);
                    for (int j = 0; j < numSub; j++)
                        bits[j] = bits[j] | leftBits[i][1][j];
                } else if (eventBucketID > 0.25 * numBucket) {
                    highBorder = 0.25 * numBucket;
                    for (int j = 0; j < numSub; j++)
                        bits[j] = bits[j] | leftBits[i][0][j];
                }

                // low buckets
                for (Pair<Integer, Double> subIDValue : infBuckets.get(i).get(eventBucketID))
                    if (subIDValue.value2 > attributeValue)
                        bits[subIDValue.value1] = true;
                for (int bi = eventBucketID + 1; bi < lowBorder; bi++)
                    for (Pair<Integer, Double> subIDValue : infBuckets.get(i).get(bi))
                        bits[subIDValue.value1] = true;

                // high buckets
                for (Pair<Integer, Double> subIDValue : supBuckets.get(i).get(eventBucketID))
                    if (subIDValue.value2 < attributeValue)
                        bits[subIDValue.value1] = true;
                for (int bi = eventBucketID-1; bi > highBorder; bi--)
                    for (Pair<Integer, Double> subIDValue : supBuckets.get(i).get(bi))
                        bits[subIDValue.value1] = true;
            }
        }

        ArrayList<Integer> matchResult = new ArrayList<>();
        for (int i = 0; i < numSub; i++)
            if (!bits[i])
                matchResult.add(mapToSubID.get(i));
        return matchResult;
    }

    public int getNumSub() {
        return numSub;
    }
}
