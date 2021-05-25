# MatchAlgorithmUsingStorm
Use Storm to implement sub/pub matching algorithms in clustered environments.
### script
* kafka_2.13-2.7.0/bin/zookeeper-server-start.sh config/zookeeper.properties
* kafka_2.13-2.7.0/bin/kafka-server-start.sh config/server.properties
* kafka_2.13-2.7.0/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic event
* kafka_2.13-2.7.0/bin/kafka-run-class.sh  kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic subscription --time -1 --partitions 0
* kafka_2.13-2.7.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --property print.key=true --topic subscription --from-beginning
* kafka_2.13-2.7.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic matchResult --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer


### Step1 SimpleRealization

### Step2 MultiplePartition

### Step3 Rein/Tama


