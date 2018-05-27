package com.github.zyz.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zyz
 * 18/5/24 21:15
 *
 * kafka 082 版本 消费demo
 */
public class ConsumerExample {


    public static void main(String[] args) throws InterruptedException {
        args = new String[]{"127.0.0.1:2181/kafka0.8.2.2", "test", "group1", "consumer1"};
//        args = new String[]{"node1:2181,node2:2181,node3:2181/kafka0.8.2.2", "test", "group1", "consumer1"};
        String zk = args[0];
        String topic = args[1];
        String groupid = args[2];
        String consumerid = args[3];


        Properties prop = new Properties();
        prop.put("zookeeper.connect",zk);
        prop.put("group.id",groupid);
        prop.put("zookeeper.session.timeout.ms", "10000");
        prop.put("zookeeper.sync.time.ms", "10000");
        prop.put("auto.offset.reset", "smallest"); //smallest
        prop.put("auto.commit.enable", "true");
        prop.put("auto.commit.interval.ms", "5000");
        ConsumerConfig consumerConfig = new ConsumerConfig(prop);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);

        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
//            String message =
//                    String.format("Consumer ID:%s, Topic:%s, GroupID:%s, PartitionID:%s, Offset:%s, Message Key:%s, Message Payload: %s",
//                            consumerid,
//                            messageAndMetadata.topic(), groupid, messageAndMetadata.partition(),
//                            messageAndMetadata.offset(), new String(messageAndMetadata.key()), new String(messageAndMetadata.message()));
//            System.err.println(message);
        }

    }
}
