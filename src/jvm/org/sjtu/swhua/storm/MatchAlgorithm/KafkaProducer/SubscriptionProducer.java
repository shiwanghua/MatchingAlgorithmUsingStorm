package org.sjtu.swhua.storm.MatchAlgorithm.KafkaProducer;

import org.apache.kafka.clients.producer.*;
import org.sjtu.swhua.storm.MatchAlgorithm.DataStructure.*;

import java.io.IOException;
import java.util.*;

public class SubscriptionProducer {
    private int subID;
    private int numSubPacket;               //  Number of message
    final int maxNumSubscription;           //  Maximum number of subscription emitted per time
    final int maxNumAttribute;              //  Maxinum number of attributes in a subscription
    final int numAttributeType;             //  Type number of attributes
//    final int subSetSize;
    private Random valueGenerator;          //  Generate the interval value and index of attribute name
    private int[] randomPermutation;              //  To get the attribute name
//    private OutputToFile output;
//    private StringBuilder log;

    public SubscriptionProducer() {
//        maxNumSubscription = TypeConstant.maxNumSubscriptionPerPacket;
//        maxNumAttribute = TypeConstant.maxNumAttributePerSubscription;
        maxNumSubscription = 1500;
        maxNumAttribute = 10;
        numAttributeType = TypeConstant.numAttributeType;
//        subSetSize = TypeConstant.subSetSize;

        subID = 1;
        numSubPacket = 1;
        valueGenerator = new Random();
        randomPermutation = new int[numAttributeType];
        for (int i = 0; i < numAttributeType; i++)
            randomPermutation[i] = i;
//        output = new OutputToFile();
    }

    public ArrayList<Subscription> produceSubscriptionPacket() {
        int numSub = (int) (Math.random() * maxNumSubscription + 1); // Generate the number of subscriptions in this tuple: 1~maxNumSubscription
//        int numSub = maxNumSubscription; // 不能一次发3000,可以发1500个
        int numAttribute;
        ArrayList<Subscription> sub = new ArrayList<>(numSub);
        for (int i = 0; i < numSub; i++) {
            numAttribute = new Random().nextInt(maxNumAttribute + 1); // Generate the number of attribute in this subscription: 0~maxNumAttribute
            int index, temp;
            for (int j = 0; j < numAttribute; j++) { // Use the first #numAttribute values of randomArray to create the attribute name
                index = valueGenerator.nextInt(numAttributeType - j) + j;
                temp = randomPermutation[j];
                randomPermutation[j] = randomPermutation[index];
                randomPermutation[index] = temp;
            }

            Double low, high;
//            String attributeName = "attributeName";
            HashMap<Integer, Pair<Double, Double>> mapNameToPair = new HashMap<>();

            for (int j = 0; j < numAttribute; j++) {
                low = valueGenerator.nextDouble();
                high = low + (1.0 - low) * valueGenerator.nextDouble();
                mapNameToPair.put(randomPermutation[j], Pair.of(low, high));
            }
            try {
                sub.add(new Subscription(subID, mapNameToPair));
                subID += 1;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sub;
    }

    public static void main(String[] args) throws Exception {
        //Assign topicName to string variable
        String topicName = "subscription";
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
//        props.put("max.request.size",1000000000);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.sjtu.swhua.storm.MatchAlgorithm.serialization.KafkaSerializer");

        SubscriptionProducer subProducer = new SubscriptionProducer();
        Producer<String, Object> producer = new KafkaProducer<String, Object>(props);
//        long startTimes = System.currentTimeMillis();

//        for (int i = 0; i < 5; i++) {
//            HashMap<Integer, Pair<Double, Double>> attributeIDToPair = new HashMap<>();
//            attributeIDToPair.put(i * 10 + 1, Pair.of(0.0, 0.1));
//            attributeIDToPair.put(i * 10 + 2, Pair.of(0.1, 0.2));
//            Subscription s1 = new Subscription(10, attributeIDToPair);
//            attributeIDToPair = new HashMap<>();
//            attributeIDToPair.put(i * 10 + 3, Pair.of(0.2, 0.3));
//            attributeIDToPair.put(4, Pair.of(0.3, 0.4));
//            attributeIDToPair.put(5, Pair.of(0.4, 0.5));
//            Subscription s2 = new Subscription(22, attributeIDToPair);
//            List<Subscription> asList = Arrays.asList(s1, s2);

        producer.send(new ProducerRecord<String, Object>(topicName, Integer.toString(subProducer.numSubPacket), subProducer.produceSubscriptionPacket()), new Callback() {
            // 每发一个消息就调用一次
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (metadata != null) {
                    System.out.println("第" + String.valueOf(subProducer.numSubPacket) + "个订阅包消息发送成功：metadata.checksum: " + metadata.checksum()
                            + " metadata.offset: " + metadata.offset() + " metadata.partition: " + metadata.partition() + " metadata.topic: " + metadata.topic());
                }
                if (exception != null) {
                    System.out.println("第" + String.valueOf(subProducer.numSubPacket) + "个订阅包消息发送异常：" + exception.getMessage());
                }
            }
        });
        subProducer.numSubPacket++;
//        }
        producer.close();
    }
}
