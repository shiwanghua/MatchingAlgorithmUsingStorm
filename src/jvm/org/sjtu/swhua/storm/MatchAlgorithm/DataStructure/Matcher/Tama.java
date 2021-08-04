package org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Matcher;

import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

// cell 从０开始编号，level从１开始编号
public class Tama {
    private int numSub, numAttributeType, numLevel, numCell;
    private int[] counter, lchild, rchild;
    private double[] mid;
    private ArrayList<ArrayList<ArrayList<Integer>>> table;
    private HashMap<Integer, Integer> mapSubIDtoNumAttribute;  // Can't be ArrayList, need to check whether it exists in Rein
    private ArrayList<Integer> mapToSubID;
    private StringBuilder log;
    private OutputToFile output;
    public Tama() {
        numSub = 0;
        numCell = 0;
        numAttributeType = TypeConstant.numAttributeType;
        numLevel = TypeConstant.numLevel;
        counter = new int[TypeConstant.subSetSize*TypeConstant.redundancy/TypeConstant.parallelismDegree+1];
        lchild = new int[1 << numLevel];
        rchild = new int[1 << numLevel];
        mid = new double[1 << numLevel];
        output=new OutputToFile();
        mapSubIDtoNumAttribute = new HashMap<>();
        mapToSubID = new ArrayList<>();
        table = new ArrayList<>();
        for (int i = 0; i < numAttributeType; i++) {
            table.add(new ArrayList<>());
            for (int j = 0; j < (1 << numLevel); j++) { // (1 << numLevel) - 1
                table.get(i).add(new ArrayList<>());
            }
        }
        initiate(1, 0, 0.0, 1.0);
//        for (int i = 0; i < 1 << numLevel; i++) {
//            System.out.println("ID="+String.valueOf(i)+", lchild="+String.valueOf(lchild[i])+", rchild="+String.valueOf(rchild[i])+", mid="+mid[i]);
//        }
    }

    private void initiate(int level, int cellID, double l, double r) {
//        log = new StringBuilder(level);
//            log.append(" ");
//            log.append(cellID);
//            log.append(" ");
//            log.append(l);
//            log.append(" ");
//            log.append(r);
//            log.append(".\n");
//            try {
//                output.otherInfo(log.toString());
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        mid[cellID] = (l + r) / 2;
        if (level == numLevel)
            return;
//            if (l < r) {
        lchild[cellID] = ++numCell;
        initiate(level + 1, numCell, l, mid[cellID]);
        rchild[cellID] = ++numCell;
        initiate(level + 1, numCell, mid[cellID], r);
//            }
    }

    public boolean insert(Subscription sub) {
        int subID = sub.getSubID();
        if (mapSubIDtoNumAttribute.getOrDefault(subID, 0) > 0) // Check whether it has been in Rein, so mapSubIDtoNumAttribute can't be a ArrayList
            return false;

        HashMap.Entry<Integer, Pair<Double, Double>> subAttributeEntry;
        Iterator<HashMap.Entry<Integer, Pair<Double, Double>>> subAttributeIterator = sub.getMap().entrySet().iterator();
        while (subAttributeIterator.hasNext()) {
            subAttributeEntry = subAttributeIterator.next();
            insert(1, 0, 0.0, 1.0, numSub, subAttributeEntry.getKey(), subAttributeEntry.getValue().getFirst(), subAttributeEntry.getValue().getSecond());
        }
        mapSubIDtoNumAttribute.put(subID, sub.getAttibuteNum());  // Here is subID not numSub
        mapToSubID.add(subID);  //  add this map to ensure the size of bits array int match() is right, since each executor will not get a successive subscription set
        numSub++;   //  after Deletion operation, numSub!=numSubInserted, so variable 'numSubInserted' is needed.
        return true;
    }

    private void insert(int level, int cellID, double left, double right, int subID, int attributeID, double low, double high) {
        if (level == numLevel || (low <= left && high >= right)) {
            table.get(attributeID).get(cellID).add(subID);
            return;
        }
        if (high <= mid[cellID])
            insert(level + 1, lchild[cellID], left, mid[cellID], subID, attributeID, low, high);
        else if (low > mid[cellID])
            insert(level + 1, rchild[cellID], mid[cellID], right, subID, attributeID, low, high);
        else {
            insert(level + 1, lchild[cellID], left, mid[cellID], subID, attributeID, low, high);
            insert(level + 1, rchild[cellID], mid[cellID], right, subID, attributeID, low, high);
        }
    }

    public ArrayList<Integer> match(Event e) {

        for (HashMap.Entry<Integer, Integer> entry : mapSubIDtoNumAttribute.entrySet())
            counter[entry.getKey()] = entry.getValue();
        int eventAttributeID;
        double attributeValue;
        HashMap.Entry<Integer, Double> eventAttributeEntry;
        Iterator<HashMap.Entry<Integer, Double>> eventAttributeIterator = e.getAttributeIDToValue().entrySet().iterator();
        while (eventAttributeIterator.hasNext()) {
            eventAttributeEntry = eventAttributeIterator.next();
            eventAttributeID = eventAttributeEntry.getKey();
            attributeValue = eventAttributeEntry.getValue();
            match(1, 0, eventAttributeID, 0.0, 1.0, attributeValue);
        }

        ArrayList<Integer> matchResult = new ArrayList<>();
        for (int i = 0; i < numSub; i++)
            if (counter[i] == 0)
                matchResult.add(mapToSubID.get(i));
        return matchResult;
    }

    private void match(int level, int cellID, int attributeID, double left, double right, double value) {
        for (int i = 0; i < table.get(attributeID).get(cellID).size(); i++)
            --counter[table.get(attributeID).get(cellID).get(i)];
        if (left >= right || level == numLevel)
            return;
        else if (value <= mid[cellID])
            match(level + 1, lchild[cellID], attributeID, left, mid[cellID], value);
        else
            match(level + 1, rchild[cellID], attributeID, mid[cellID], right, value);
    }

    public int getNumSub() {
        return numSub;
    }
}
