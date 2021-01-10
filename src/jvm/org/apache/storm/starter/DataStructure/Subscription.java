package org.apache.storm.starter.DataStructure;

import org.apache.storm.streams.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Subscription {

    private int numAttributes = 0;
    private Integer subID=-1;
    private OutputToFile out;
    public HashMap<String, Pair<Double, Double>> attributeNameToPair;

    public Subscription(final Integer ID, int num_attributes, ArrayList<String> attributeName, ArrayList<Pair<Double, Double>> pairs) throws IOException {
        subID=ID;
        numAttributes = num_attributes;
        if (num_attributes < pairs.size()) {
            out.writeToFile("The number of pair is larger than the number of attributes, subscription construct failed.\n");
            return;
        }
        if (attributeName.size() < pairs.size()) {
            out.writeToFile("The number of pair is not equal to the number of attributeName(ArrayList), subscription construct failed.\n");
            return;
        }
        for (int i = 0; i < attributeName.size(); i++) {
            if (attributeNameToPair.containsKey(attributeName.get(i))) {
                out.writeToFile("Attribte Name duplicate, subscription construct failed.\n");
                //return;
            }
            if(pairs.get(i).getFirst()>pairs.get(i).getSecond()) {
                out.writeToFile("Wrong interval pair, subscription construct failed.\n");
                return;
            }
            attributeNameToPair.put(attributeName.get(i), pairs.get(i));
        }
    }

    public Subscription(final Integer ID, int num_attributes, HashMap<String, Pair<Double, Double>> mapNameToPair) throws IOException {
        subID=ID;
        numAttributes = num_attributes;

        for(Pair<Double, Double> value : mapNameToPair.values()){
            if(value.getFirst()>value.getSecond())
            {
                out.writeToFile("Wrong interval pair, subscription construct failed.");
                return;
            }
        }
        attributeNameToPair=mapNameToPair;
    }

    public Pair<Double, Double> getPair(String attributeName) {
        return attributeNameToPair.get(attributeName);
    }

    public int getSubID(){
        return subID;
    }

    public Boolean insertAttribute(String attributeName,Pair<Double,Double> p)throws IOException{
        if(p.getFirst()>p.getSecond()) {
            out.writeToFile("Wrong interval pair, subscription insert failed.\n");
            return false;
        }
        if(attributeNameToPair.containsKey(attributeName)){
            out.writeToFile("Already exists such a attribute name, subscription insert failed.\n");
            return false;
        }
        if(numAttributes == attributeNameToPair.size()){
            out.writeToFile("Number of attributes is full, subscription insert failed.\n");
            return false;
        }
        attributeNameToPair.put(attributeName,p);
        return true;
    }

    public Boolean deleteAttribute(String attributeName) throws IOException {
        if (attributeNameToPair.containsKey(attributeName)) {
            attributeNameToPair.remove(attributeName);
            return true;
        }
        out.writeToFile("No such an attribute name, subscription delete failed.\n");
        return false;
    }

    public Boolean updateAttribute(String attributeName,Pair<Double,Double> p)throws IOException {
        if(p.getFirst()>p.getSecond()) {
            out.writeToFile("Wrong interval pair, subscription update failed.\n");
            return false;
        }
        if(attributeNameToPair.containsKey(attributeName)){
            attributeNameToPair.put(attributeName,p);
            return true;
        }
        out.writeToFile("No such a attribute name, subscription update failed.\n");
        return false;
    }
}
