package org.sjtu.swhua.storm.MatchAlgorithm.KafkaProducer;

import org.apache.kafka.clients.producer.*;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Event;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class EventProducer {
    public static void main(String[] args) throws Exception{
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

        Producer<String, Object> producer = new KafkaProducer<String, Object>(props);
        long startTimes = System.currentTimeMillis();

        for(int i = 0; i < 5; i++){

            final int index = i;

            HashMap<Integer, Double> mapIDToValue=new HashMap<>();
            mapIDToValue.put(-1,0.0);
            mapIDToValue.put(-2,0.15);
            Event e1=new Event(1,2,mapIDToValue);
            mapIDToValue=new HashMap<>();
            mapIDToValue.put(3,0.5);
            mapIDToValue.put(6,0.6);
            mapIDToValue.put(7,0.7);
            Event e2=new Event(2,3,mapIDToValue);

            List<Event> asList = Arrays.asList(e1,e2);
//	    	  producer.send(new ProducerRecord<String, Object>(topicName,Integer.toString(i),asList));
//	          producer.send(new ProducerRecord<String, Object>(topicName, Integer.toString(i), perSon));
            producer.send(new ProducerRecord<String, Object>(topicName, Integer.toString(i), asList), new Callback() {

                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (metadata != null) {
                        System.out.println("第"+(1+index)+"个事件发送成功：metadata.checksum: "+metadata.checksum()
                                +" metadata.offset: "+metadata.offset()+" metadata.partition: "+metadata.partition()+" metadata.topic: "+metadata.topic());
                    }
                    if (exception != null) {
                        System.out.println(index+"异常："+exception.getMessage());
                    }
                }
            });
        }
        producer.close();
    }
}
