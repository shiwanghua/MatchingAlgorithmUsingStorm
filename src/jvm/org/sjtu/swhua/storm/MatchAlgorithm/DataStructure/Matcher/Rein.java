package org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Matcher;

import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Event;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Pair;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Subscription;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.TypeConstant;

import java.util.*;

// old Rein
public class Rein {
    private int numBucket, numSub, numAttributeType, subSetSize;
    private double bucketSpan;
    private ArrayList<Integer> mapToSubID;
    private HashMap<Integer, Boolean> exist;
    private ArrayList<ArrayList<LinkedList<Pair<Integer, Double>>>> infBuckets; // Attribute ID -> bucket id -> a bucket list -> (subID,subVlue)
    private ArrayList<ArrayList<LinkedList<Pair<Integer, Double>>>> supBuckets;

    public Rein() {
        numSub = 0; // number of inserted sub
        numBucket = TypeConstant.numBucket;
        numAttributeType = TypeConstant.numAttributeType;
        subSetSize = TypeConstant.subSetSize;
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
    }

    public boolean insert(Subscription sub) {
        int subID = sub.getSubID();
        if (exist.getOrDefault(subID, false) == true)
            return false;
        int subAttributeID;
        double low, high;
        HashMap.Entry<Integer, Pair<Double, Double>> subAttributeEntry;
        Iterator<HashMap.Entry<Integer, Pair<Double, Double>>> subAttributeIterator = sub.getMap().entrySet().iterator();
        while (subAttributeIterator.hasNext()) {
            subAttributeEntry = subAttributeIterator.next();
            subAttributeID = subAttributeEntry.getKey();
            low = subAttributeEntry.getValue().getFirst();
            high = subAttributeEntry.getValue().getSecond();

            infBuckets.get(subAttributeID).get((int) (low / bucketSpan)).add(Pair.of(numSub, low));
            supBuckets.get(subAttributeID).get(Math.min((int) (high / bucketSpan), numBucket - 1)).add(Pair.of(numSub, high));
        }
        mapToSubID.add(subID);  //  add this map to ensure the size of bits array int match() is right, since each executor will not get a successive subscription set
        exist.put(subID, true);
        numSub++;   //  after Deletion operation, numSub!=numSubInserted, so variable 'numSubInserted' is needed.
        return true;
    }

    public BitSet match(Event e) {

//        boolean[] bits = new boolean[numSub];
        BitSet gb = new BitSet(numSub);
//        System.out.println("Rein"+e.getEventID()+" "+gb.size()+" "+numSub+"\n");

//        Integer eventAttributeID;
        Double attributeValue;
        int bucketID;
//        HashMap<Integer,Double> attributeIDToValue=e.getMap();
        for (int i = 0; i < numAttributeType; i++) {   // i: attributeID
            attributeValue = e.getAttributeValue(i);
            if (attributeValue == null) {  // all sub containing this attribute should be marked, only either sup or inf is enough.
                for (int j = 0; j < numBucket; j++) {  // j: BucketID
                    for (Iterator<Pair<Integer, Double>> pairIterator = infBuckets.get(i).get(j).iterator(); pairIterator.hasNext(); ) {
                        //bits[pairIterator.next().getFirst()] = true;
                        gb.set(pairIterator.next().getFirst());
                    } // LinkedList
                } // Bucket ArrayList
            } else {
                bucketID = (int) (attributeValue / bucketSpan);
                for (Pair<Integer, Double> subIDValue : infBuckets.get(i).get(bucketID))
                    if (subIDValue.value2 > attributeValue)
//                        bits[subIDValue.value1] = true;
                        gb.set(subIDValue.value1);
                for (int bi = bucketID + 1; bi < numBucket; bi++)
                    for (Pair<Integer, Double> subIDValue : infBuckets.get(i).get(bi))
                        gb.set(subIDValue.value1);

                for (Pair<Integer, Double> subIDValue : supBuckets.get(i).get(bucketID))
                    if (subIDValue.value2 < attributeValue)
                        gb.set(subIDValue.value1);
                for (int bi = 0; bi < bucketID; bi++)
                    for (Pair<Integer, Double> subIDValue : supBuckets.get(i).get(bi))
                        gb.set(subIDValue.value1);
            }
        }

//        ArrayList<Integer> matchResult = new ArrayList<>();
//        for (int i = 0; i < numSub; i++)
//            if (!gb.get(i))
//                matchResult.add(mapToSubID.get(i));
        BitSet ans = new BitSet(subSetSize);
        ans.set(0, subSetSize); // number of 1 > number of 0 in most cases, so set some 1 to 0
        for (int i = gb.nextClearBit(0); i < numSub; i = gb.nextClearBit(i + 1)) { // && i >= 0
            ans.clear(mapToSubID.get(i));
//            System.out.println("ans.size= " + ans.size() + ", ans.count= " + ans.stream().count()
//                    + ", mtsi.size= " + mapToSubID.size() + ", mtsi.count= " + mapToSubID.stream().count()
//                    + ", gB.size= " + gb.size() + ", gB.count= " + gb.stream().count() + ", i= " + i+"\n");
        }
        return ans;
    }

    public int getNumSub() {
        return numSub;
    }
}