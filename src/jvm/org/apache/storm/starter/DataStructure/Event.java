package org.apache.storm.starter.DataStructure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Event {
    private int maxNumAttributes;
    private Integer eventID;
    private OutputToFile output;
    private HashMap<String, Double> attributeNameToValue;

    public Event(){
        maxNumAttributes = 0;
        eventID=-1;
        output=new OutputToFile();
        attributeNameToValue=new HashMap<>();
    }

    public Event(final Integer ID, int num_attributes, ArrayList<String> attributeName, ArrayList<Double> values) throws IOException {
        eventID = ID;
        maxNumAttributes = num_attributes;
        output=new OutputToFile();
        if (attributeName.size() < values.size()) {
            output.writeToLogFile("The number of value is larger than the number of attributes, event construct failed.\n");
            return;
        }

        for (int i = 0; i < attributeName.size(); i++) {
            if (attributeNameToValue.containsKey(attributeName.get(i))) {
                output.writeToLogFile("Attribte name duplicate, event construct failed.\n");
                //return;
            }
            attributeNameToValue.put(attributeName.get(i), values.get(i));
        }
    }

    public Event(final Integer ID, int num_attributes, HashMap<String, Double> mapNameToValue) throws IOException {
        eventID = ID;
        output=new OutputToFile();
        if (num_attributes < mapNameToValue.size()) {
            output.writeToLogFile("The number of values is larger than the max number of attributes, event construct failed.\n");
            return;
        }
        maxNumAttributes = num_attributes;
        attributeNameToValue = mapNameToValue;
    }

    public Double getAttributeValue(String attributeName) {
        return attributeNameToValue.get(attributeName);
    }

    public Integer getEventID() {
        return eventID;
    }

    public HashMap<String, Double> getMap(){return attributeNameToValue;}

    public Boolean insertAttribute(String attributeName, Double d) throws IOException {
        if (attributeNameToValue.containsKey(attributeName)) {
            output.writeToLogFile("Already exists such a attribute name, event insert failed.\n");
            return false;
        }
        if (maxNumAttributes == attributeNameToValue.size()) {
            output.writeToLogFile("Number of attributes is full, event insert failed.\n");
            return false;
        }
        attributeNameToValue.put(attributeName, d);
        return true;
    }

    public Boolean deleteAttribute(String attributeName) throws IOException {
        if (attributeNameToValue.containsKey(attributeName)) {
            attributeNameToValue.remove(attributeName);
            return true;
        }
        output.writeToLogFile("No such an attribute name, event delete failed.\n");
        return false;
    }

    public Boolean updateAttribute(String attributeName, Double d) throws IOException {
        if (attributeNameToValue.containsKey(attributeName)) {
            attributeNameToValue.put(attributeName, d);
            return true;
        }
        output.writeToLogFile("No such a attribute name, event update failed.\n");
        return false;
    }
}
