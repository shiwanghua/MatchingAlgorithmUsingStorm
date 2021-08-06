package org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Matcher;

import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Event;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Pair;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Subscription;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.TypeConstant;

import java.util.*;

// old Rein
public class Rein {
    private int numBucket, numSub, numAttributeType;
    private double bucketSpan;
    private ArrayList<Integer> mapToSubID;
    private HashMap<Integer, Boolean> exist;
    private ArrayList<ArrayList<LinkedList<Pair<Integer, Double>>>> infBuckets; // Attribute ID -> bucket id -> a bucket list -> (subID,subVlue)
    private ArrayList<ArrayList<LinkedList<Pair<Integer, Double>>>> supBuckets;

    public Rein() {
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

    public ArrayList<Integer> match(Event e) {

        boolean[] bits = new boolean[numSub];
//        Integer eventAttributeID;
        Double attributeValue;
        int bucketID;
//        HashMap<Integer,Double> attributeIDToValue=e.getMap();
        for (int i = 0; i < numAttributeType; i++) {   // i: attributeID
            attributeValue = e.getAttributeValue(i);
            if (attributeValue == null) {  // all sub containing this attribute should be marked, only either sup or inf is enough.
                for (int j = 0; j < numBucket; j++) {  // j: BucketID
                    for (Iterator<Pair<Integer, Double>> pairIterator = infBuckets.get(i).get(j).iterator(); pairIterator.hasNext(); ) {
                        bits[pairIterator.next().getFirst()] = true;
                    } // LinkedList
                } // Bucket ArrayList
            } else {
                bucketID = (int) (attributeValue / bucketSpan);
                for (Pair<Integer, Double> subIDValue : infBuckets.get(i).get(bucketID))
                    if (subIDValue.value2 > attributeValue)
                        bits[subIDValue.value1] = true;
                for (int bi = bucketID + 1; bi < numBucket; bi++)
                    for (Pair<Integer, Double> subIDValue : infBuckets.get(i).get(bi))
                        bits[subIDValue.value1] = true;

                for (Pair<Integer, Double> subIDValue : supBuckets.get(i).get(bucketID))
                    if (subIDValue.value2 < attributeValue)
                        bits[subIDValue.value1] = true;
                for (int bi = 0; bi < bucketID; bi++)
                    for (Pair<Integer, Double> subIDValue : infBuckets.get(i).get(bi))
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