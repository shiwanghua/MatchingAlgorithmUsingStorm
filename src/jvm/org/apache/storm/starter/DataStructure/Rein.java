package org.apache.storm.starter.DataStructure;

import java.util.*;

public class Rein {
    private Integer numBucket, numSub;
    private double bucketSpan;
    private ArrayList<Integer> mapToSubID;
    private HashMap<String, ArrayList<LinkedList<Pair<Integer, Double>>>> infBuckets; // Attribute name -> bucket id -> a bucket list -> (subID,subVlue)
    private HashMap<String, ArrayList<LinkedList<Pair<Integer, Double>>>> supBuckets;

    public Rein() {
        numSub = 0;
        numBucket = TypeConstant.numBucket;
        bucketSpan = 1.0 / numBucket;
        mapToSubID=new ArrayList<>();
        infBuckets = new HashMap<>();
        supBuckets = new HashMap<>();
        for (int i = 0; i < TypeConstant.numAttributeType; i++) {
            infBuckets.put("attributeName" + String.valueOf(i), new ArrayList<>());
            supBuckets.put("attributeName" + String.valueOf(i), new ArrayList<>());
            for (int j = 0; j < numBucket; j++) {
                infBuckets.get("attributeName"+ String.valueOf(i)).add(new LinkedList<>());
                supBuckets.get("attributeName"+ String.valueOf(i)).add(new LinkedList<>());
            }
        }
    }

    public boolean insert(Subscription sub) {
        Integer subID = sub.getSubID();
        String subAttributeName;
        double low, high;
        HashMap.Entry<String, Pair<Double, Double>> subAttributeEntry;
        Iterator<HashMap.Entry<String, Pair<Double, Double>>> subAttributeIterator = sub.getMap().entrySet().iterator();
        while (subAttributeIterator.hasNext()) {
            subAttributeEntry = subAttributeIterator.next();
            subAttributeName = subAttributeEntry.getKey();
            low = subAttributeEntry.getValue().getFirst();
            high = subAttributeEntry.getValue().getSecond();

            infBuckets.get(subAttributeName).get((int) (low / bucketSpan)).add(Pair.of(numSub, low));
            supBuckets.get(subAttributeName).get((int) (high / bucketSpan)).add(Pair.of(numSub, high));
        }
        mapToSubID.add(subID);
        numSub++;
        return true;
    }

    public ArrayList<Integer> match(Event e) {
        ArrayList<Integer> matchResult = new ArrayList<>();
        boolean[] bits = new boolean[numSub];
        String eventAttributeName;
        double attributeValue;
        int bucketID;
        HashMap.Entry<String, Double> eventAttributeEntry;
        Iterator<HashMap.Entry<String, Double>> eventAttributeIterator = e.getMap().entrySet().iterator();
        while (eventAttributeIterator.hasNext()) {
            eventAttributeEntry = eventAttributeIterator.next();
            eventAttributeName = eventAttributeEntry.getKey();
            attributeValue = eventAttributeEntry.getValue();
            bucketID = (int) (eventAttributeEntry.getValue() / bucketSpan);

            for (Pair<Integer, Double> subIDValue : infBuckets.get(eventAttributeName).get(bucketID))
                if (subIDValue.value2 > attributeValue)
                    bits[subIDValue.value1] = true;
            for (int i = bucketID + 1; i < numBucket; i++)
                for (Pair<Integer, Double> subIDValue : infBuckets.get(eventAttributeName).get(i))
                    bits[subIDValue.value1] = true;

            for (Pair<Integer, Double> subIDValue : supBuckets.get(eventAttributeName).get(bucketID))
                if (subIDValue.value2 < attributeValue)
                    bits[subIDValue.value1] = true;
            for (int i = 0; i < bucketID; i++)
                for (Pair<Integer, Double> subIDValue : infBuckets.get(eventAttributeName).get(i))
                    bits[subIDValue.value1] = true;
        }

        for (int i = 0; i < numSub; i++)
            if (!bits[i])
                matchResult.add(mapToSubID.get(i));
        return matchResult;
    }

    public Integer getNumSub() {
        return numSub;
    }
}
