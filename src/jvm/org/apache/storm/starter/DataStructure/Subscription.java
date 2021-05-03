package org.apache.storm.starter.DataStructure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Subscription {

    private int maxNumAttributes;
    private int subID;
    private OutputToFile out;
    private HashMap<Integer, Pair<Double, Double>> attributeIDToPair;

    public Subscription() {
        maxNumAttributes = 10;
        subID = -1;
        out = new OutputToFile();
        attributeIDToPair = new HashMap<>();
    }

    public Subscription(final int ID, int num_attributes, ArrayList<Integer> attributeID, ArrayList<Pair<Double, Double>> pairs) throws IOException {
        subID = ID;
        maxNumAttributes = num_attributes;
        out = new OutputToFile();
        if (num_attributes < pairs.size()) {
            out.writeToLogFile("The number of pair is larger than the number of attributes, subscription construct failed.\n");
            return;
        }
        if (attributeID.size() != pairs.size()) {
            out.writeToLogFile("The number of pair is not equal to the number of attributeName(ArrayList), subscription construct failed.\n");
            return;
        }

        int size=attributeID.size();
        for (int i = 0; i < size; i++) {
            if (attributeIDToPair.containsKey(attributeID.get(i))) {
                out.writeToLogFile("Attribte Name duplicate, subscription construct failed.\n");
                //return;
            }
            if (pairs.get(i).getFirst() > pairs.get(i).getSecond()) {
                out.writeToLogFile("Wrong interval pair, subscription construct failed.\n");
                return;
            }
            attributeIDToPair.put(attributeID.get(i), pairs.get(i));
        }
    }

    public Subscription(final int ID, HashMap<Integer, Pair<Double, Double>> mapIDToPair) throws IOException {
        subID = ID;
        maxNumAttributes = mapIDToPair.size();
        out = new OutputToFile();
//        for (Pair<Double, Double> value : mapNameToPair.values()) {
//            if (value.getFirst() > value.getSecond()) {
//                out.writeToLogFile("Wrong interval pair, subscription construct failed.");
//                return;
//            }
//        }
        attributeIDToPair = mapIDToPair;
    }

    public Pair<Double, Double> getPair(Integer attributeID) {
        return attributeIDToPair.get(attributeID);
    }

    public int getSubID() {
        return subID;
    }

    public HashMap<Integer, Pair<Double, Double>> getMap(){return attributeIDToPair;}

    public Boolean insertAttribute(Integer attributeID, Pair<Double, Double> p) throws IOException {
        if (p.getFirst() > p.getSecond()) {
            out.writeToLogFile("Wrong interval pair, subscription insert failed.\n");
            return false;
        }
        if (attributeIDToPair.containsKey(attributeID)) {
            out.writeToLogFile("Already exists such a attribute name, subscription insert failed.\n");
            return false;
        }
        if (maxNumAttributes == attributeIDToPair.size()) {
            out.writeToLogFile("Number of attributes is full, subscription insert failed.\n");
            return false;
        }
        attributeIDToPair.put(attributeID, p);
        return true;
    }

    public Boolean deleteAttribute(Integer attributeID) throws IOException {
        if (attributeIDToPair.containsKey(attributeID)) {
            attributeIDToPair.remove(attributeID);
            return true;
        }
        out.writeToLogFile("No such an attribute name, subscription delete failed.\n");
        return false;
    }

    public Boolean updateAttribute(Integer attributeID, Pair<Double, Double> p) throws IOException {
        if (p.getFirst() > p.getSecond()) {
            out.writeToLogFile("Wrong interval pair, subscription update failed.\n");
            return false;
        }
        if (attributeIDToPair.containsKey(attributeID)) {
            attributeIDToPair.put(attributeID, p);
            return true;
        }
        out.writeToLogFile("No such a attribute name, subscription update failed.\n");
        return false;
    }
}