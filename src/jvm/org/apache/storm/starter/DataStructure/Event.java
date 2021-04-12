package org.apache.storm.starter.DataStructure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Event {
    private int maxNumAttributes;
    private Integer eventID;
    private OutputToFile output;
    //private HashMap<String, Double> attributeNameToValue;
    private HashMap<Integer, Double> attributeIDToValue;

    public Event() {
        maxNumAttributes = 0;
        eventID = -1;
        output = new OutputToFile();
        attributeIDToValue = new HashMap<>();
    }

    public Event(final Integer ID, int num_attributes, ArrayList<Integer> attributeID, ArrayList<Double> values) throws IOException {
        eventID = ID;
        maxNumAttributes = num_attributes;
        output = new OutputToFile();
        if (attributeID.size() < values.size()) {
            output.writeToLogFile("The number of value is larger than the number of attributes, event construct failed.\n");
            return;
        }

        Integer size=attributeID.size();
        for (int i = 0; i < size; i++) {
            if (attributeIDToValue.containsKey(attributeID.get(i))) {
                output.writeToLogFile("Attribte name duplicate, event construct failed.\n");
                //return;
            }
            attributeIDToValue.put(attributeID.get(i), values.get(i));
        }
    }

    public Event(final Integer ID, int num_attributes, HashMap<Integer, Double> mapIDToValue) throws IOException {
        eventID = ID;
        output = new OutputToFile();
        if (num_attributes < mapIDToValue.size()) {
            output.writeToLogFile("The number of values is larger than the max number of attributes, event construct failed.\n");
            return;
        }
        maxNumAttributes = num_attributes;
        attributeIDToValue = mapIDToValue;
    }

    public Double getAttributeValue(Integer attributeID) {
        return attributeIDToValue.get(attributeID);
    }

    public Integer getEventID() {
        return eventID;
    }

    public HashMap<Integer, Double> getMap() {
        return attributeIDToValue;
    }

    public Boolean insertAttribute(Integer attributeID, Double d) throws IOException {
        if (attributeIDToValue.containsKey(attributeID)) {
            output.writeToLogFile("Already exists such a attribute name, event insert failed.\n");
            return false;
        }
        if (maxNumAttributes == attributeIDToValue.size()) {
            output.writeToLogFile("Number of attributes is full, event insert failed.\n");
            return false;
        }
        attributeIDToValue.put(attributeID, d);
        return true;
    }

    public Boolean deleteAttribute(String attributeName) throws IOException {
        if (attributeIDToValue.containsKey(attributeName)) {
            attributeIDToValue.remove(attributeName);
            return true;
        }
        output.writeToLogFile("No such an attribute name, event delete failed.\n");
        return false;
    }

    public Boolean updateAttribute(Integer attributeID, Double d) throws IOException {
        if (attributeIDToValue.containsKey(attributeID)) {
            attributeIDToValue.put(attributeID, d);
            return true;
        }
        output.writeToLogFile("No such a attribute name, event update failed.\n");
        return false;
    }
}