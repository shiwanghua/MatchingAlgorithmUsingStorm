package org.sjtu.swhua.storm.MatchAlgorithm.serialization;


import com.alibaba.fastjson.JSON;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

public class KafkaSerializer implements Serializer<Object> {
    @Override
    public void configure(Map configs, boolean isKey) {
        System.out.println("KafkaSerializer configure: "+configs.toString()+"  "+isKey);
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return JSON.toJSONBytes(data);
    }

    @Override
    public void close() {
        System.out.println("KafkaSerializer close().");
    }

}
