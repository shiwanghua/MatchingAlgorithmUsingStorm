package org.sjtu.swhua.storm.MatchAlgorithm.KafkaProducer;

import org.apache.kafka.clients.producer.*;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.*;

import java.io.IOException;
import java.util.*;

public class EventProducer {

    private int eventID;
    private int numEventPacket;
    final int maxNumEvent;            //  Maximum number of event emitted per time
    final int maxNumAttribute;        //  Maxinum number of attributes in a event
    final int numAttributeType;       //  Type number of attributes
    private int[] randomPermutation;  //  To get the attribute name
    private Random valueGenerator;

    public EventProducer() {
//        maxNumEvent = TypeConstant.maxNumEventPerPacket;
//        maxNumAttribute = TypeConstant.maxNumAttributePerEvent;
        maxNumEvent = 1500;
        maxNumAttribute = 10;
        numAttributeType = TypeConstant.numAttributeType;

        eventID = 1;
        numEventPacket = 0;  // messageID、packetID
        randomPermutation = new int[numAttributeType];
        for (int i = 0; i < numAttributeType; i++)
            randomPermutation[i] = i;
        valueGenerator = new Random();
    }

    public ArrayList<Event> produceEventPacket() {
        int numEvent = (int) (Math.random() * maxNumEvent + 1); // Generate the number of subscriptions in this tuple: 1~maxNumEvent
//        int numEvent = maxNumEvent;
        int numAttribute;
        ArrayList<Event> events = new ArrayList<>(numEvent);
        for (int i = 0; i < numEvent; i++) {
            numAttribute = new Random().nextInt(maxNumAttribute + 1); // Generate the number of attribute in this subscription: 0~maxNumAttribute
//            numAttribute=numAttributeType;
            for (int j = 0; j < numAttribute; j++) { // Use the first #numAttribute values of randomArray to create the attribute name
                int index = valueGenerator.nextInt(numAttributeType - j) + j;
                int temp = randomPermutation[j];
                randomPermutation[j] = randomPermutation[index];
                randomPermutation[index] = temp;
            }

            HashMap<Integer, Double> mapIDToValue = new HashMap<>();
            for (int j = 0; j < numAttribute; j++)
                mapIDToValue.put(randomPermutation[j], valueGenerator.nextDouble());
//                mapIDToValue.put(randomPermutation[j], 0.2);
            try {
                events.add(new Event(eventID, numAttribute, mapIDToValue));
                eventID += 1;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return events;
    }

    public static void main(String[] args) throws Exception {
        //Assign topicName to string variable
        String topicName = "event";
        // create instance for properties to access producer configs
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", "swhua:9092");
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        props.put("metadata.fetch.timeout.ms", 30000);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.sjtu.swhua.storm.MatchAlgorithm.serialization.KafkaSerializer");

        EventProducer eventProducer = new EventProducer();
        Producer<String, Object> producer = new KafkaProducer<String, Object>(props);
//        long startTimes = System.currentTimeMillis();

//        for(int i = 0; i < 5; i++){
//            HashMap<Integer, Double> mapIDToValue=new HashMap<>();
//            mapIDToValue.put(-1,0.0);
//            mapIDToValue.put(-2,0.15);
//            Event e1=new Event(1,2,mapIDToValue);
//            mapIDToValue=new HashMap<>();
//            mapIDToValue.put(3,0.5);
//            mapIDToValue.put(6,0.6);
//            mapIDToValue.put(7,0.7);
//            Event e2=new Event(2,3,mapIDToValue);
//            List<Event> asList = Arrays.asList(e1,e2);
//	    	  producer.send(new ProducerRecord<String, Object>(topicName,Integer.toString(i),asList));
//	          producer.send(new ProducerRecord<String, Object>(topicName, Integer.toString(i), perSon));
        producer.send(new ProducerRecord<String, Object>(topicName, Integer.toString(eventProducer.numEventPacket), eventProducer.produceEventPacket()), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (metadata != null) {
                    System.out.println("第" + String.valueOf(eventProducer.numEventPacket) + "个事件包消息发送成功：metadata.checksum: " + metadata.checksum()
                            + " metadata.offset: " + metadata.offset() + " metadata.partition: " + metadata.partition() + " metadata.topic: " + metadata.topic());
                }
                if (exception != null) {
                    System.out.println("第" + String.valueOf(eventProducer.numEventPacket) + "个事件包消息发送异常：" + exception.getMessage());
                }
            }
        });
        eventProducer.numEventPacket++;
//        }
        producer.close();
    }
}
