package org.sjtu.swhua.storm.MatchAlgorithm.serialization;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KafkaDeserializer implements Deserializer<Object>{
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        System.out.println("KafkaDeserializer configure: "+configs.toString()+"  "+isKey);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return JSON.parseObject(data,Object.class);
    }

    @Override
    public void close() {
        System.out.println("KafkaDeserializer close().");
    }
}
