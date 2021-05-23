package org.sjtu.swhua.storm.MatchAlgorithm.spout;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Event;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.Subscription;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class SubscriptionKafkaSpout {
    public static void main(String[] args) throws Exception {
        //Kafka consumer configuration settings
        String topicName = "subscription";
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
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
                List<Subscription> e = JSON.parseArray(record.value().toString(), Subscription.class);
                System.out.println(record.value().toString());
                System.out.println("subID="+e.get(0).getSubID());
                System.out.println("s[attr[3]] = "+e.get(1).getPair(3));
            }
        }
    }
}
