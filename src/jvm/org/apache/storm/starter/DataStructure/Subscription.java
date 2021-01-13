package org.apache.storm.starter.DataStructure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Subscription {

    private int numAttributes;
    private Integer subID;
    private OutputToFile out;
    private HashMap<String, Pair<Double, Double>> attributeNameToPair;

    public Subscription() {
        numAttributes = 0;
        subID = -1;
        out = new OutputToFile();
        attributeNameToPair = new HashMap<>();
    }

    public Subscription(final Integer ID, int num_attributes, ArrayList<String> attributeName, ArrayList<Pair<Double, Double>> pairs) throws IOException {
        subID = ID;
        numAttributes = num_attributes;
        if (num_attributes < pairs.size()) {
            out.writeToLogFile("The number of pair is larger than the number of attributes, subscription construct failed.\n");
            return;
        }
        if (attributeName.size() < pairs.size()) {
            out.writeToLogFile("The number of pair is not equal to the number of attributeName(ArrayList), subscription construct failed.\n");
            return;
        }
        for (int i = 0; i < attributeName.size(); i++) {
            if (attributeNameToPair.containsKey(attributeName.get(i))) {
                out.writeToLogFile("Attribte Name duplicate, subscription construct failed.\n");
                //return;
            }
            if (pairs.get(i).getFirst() > pairs.get(i).getSecond()) {
                out.writeToLogFile("Wrong interval pair, subscription construct failed.\n");
                return;
            }
            attributeNameToPair.put(attributeName.get(i), pairs.get(i));
        }
    }

    public Subscription(final Integer ID, int num_attributes, HashMap<String, Pair<Double, Double>> mapNameToPair) throws IOException {
        subID = ID;
        numAttributes = num_attributes;

        for (Pair<Double, Double> value : mapNameToPair.values()) {
            if (value.getFirst() > value.getSecond()) {
                out.writeToLogFile("Wrong interval pair, subscription construct failed.");
                return;
            }
        }
        attributeNameToPair = mapNameToPair;
    }

    public Pair<Double, Double> getPair(String attributeName) {
        return attributeNameToPair.get(attributeName);
    }

    public int getSubID() {
        return subID;
    }

    public HashMap<String, Pair<Double, Double>> getMap(){return attributeNameToPair;}

    public Boolean insertAttribute(String attributeName, Pair<Double, Double> p) throws IOException {
        if (p.getFirst() > p.getSecond()) {
            out.writeToLogFile("Wrong interval pair, subscription insert failed.\n");
            return false;
        }
        if (attributeNameToPair.containsKey(attributeName)) {
            out.writeToLogFile("Already exists such a attribute name, subscription insert failed.\n");
            return false;
        }
        if (numAttributes == attributeNameToPair.size()) {
            out.writeToLogFile("Number of attributes is full, subscription insert failed.\n");
            return false;
        }
        attributeNameToPair.put(attributeName, p);
        return true;
    }

    public Boolean deleteAttribute(String attributeName) throws IOException {
        if (attributeNameToPair.containsKey(attributeName)) {
            attributeNameToPair.remove(attributeName);
            return true;
        }
        out.writeToLogFile("No such an attribute name, subscription delete failed.\n");
        return false;
    }

    public Boolean updateAttribute(String attributeName, Pair<Double, Double> p) throws IOException {
        if (p.getFirst() > p.getSecond()) {
            out.writeToLogFile("Wrong interval pair, subscription update failed.\n");
            return false;
        }
        if (attributeNameToPair.containsKey(attributeName)) {
            attributeNameToPair.put(attributeName, p);
            return true;
        }
        out.writeToLogFile("No such a attribute name, subscription update failed.\n");
        return false;
    }
}
