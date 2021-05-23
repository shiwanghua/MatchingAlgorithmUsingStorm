package org.sjtu.swhua.storm.MatchAlgorithm.KafkaProducer;

import org.apache.kafka.clients.producer.*;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Event;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Pair;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Subscription;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class SubscriptionProducer {
    public static void main(String[] args) throws Exception{
        //Assign topicName to string variable
        String topicName = "subscription";
        // create instance for properties to access producer configs
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");
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
//        long startTimes = System.currentTimeMillis();

        for(int i = 0; i < 5; i++){

            final int index = i;

            HashMap<Integer, Pair<Double, Double>> attributeIDToPair=new HashMap<>();
            attributeIDToPair.put(i*10+1,Pair.of(0.0,0.1));
            attributeIDToPair.put(i*10+2,Pair.of(0.1,0.2));
            Subscription s1=new Subscription(10,attributeIDToPair);
            attributeIDToPair=new HashMap<>();
            attributeIDToPair.put(i*10+3,Pair.of(0.2,0.3));
            attributeIDToPair.put(4,Pair.of(0.3,0.4));
            attributeIDToPair.put(5,Pair.of(0.4,0.5));
            Subscription s2=new Subscription(22,attributeIDToPair);

            List<Subscription> asList = Arrays.asList(s1,s2);
            producer.send(new ProducerRecord<String, Object>(topicName, Integer.toString(+1), asList), new Callback() {
                // 每发一个消息就调用一次
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
