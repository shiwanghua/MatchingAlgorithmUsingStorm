package org.sjtu.swhua.storm.MatchAlgorithm.spout;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Event;
import com.alibaba.fastjson.JSON;

// kafka consumer
public class EventKafkaSpout {
    public static void main(String[] args) throws Exception {
        //Kafka consumer configuration settings
        String topicName = "event";
        Properties props = new Properties();

        props.put("bootstrap.servers", "swhua:9092");
        props.put("group.id", "SubscriptionsKafkaSpout");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.sjtu.swhua.storm.MatchAlgorithm.serialization.KafkaDeserializer");
        @SuppressWarnings("resource")
        KafkaConsumer<String, Object> consumer = new KafkaConsumer<String, Object>(props);

        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Arrays.asList(topicName));

        while (true) {
            ConsumerRecords<String, Object> records = consumer.poll(100);
            for (ConsumerRecord<String, Object> record : records) {
                // print the offset,key and value for the consumer records.
                System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
//                System.out.println(record.toString());
//                List<Event> e = (List<Event>) record.value();
//                Event ee=JSONObject.parseObject(record.value().toString(), Event.class);
                List<Event> e = JSON.parseArray(record.value().toString(), Event.class);
                System.out.println("eventID="+e.get(1).getEventID());
                System.out.println("attributeValue="+e.get(1).getNumAttribute());
                System.out.println("e[attr[-2]] = "+e.get(0).getAttributeValue(-2));
            }
        }

    }
}
