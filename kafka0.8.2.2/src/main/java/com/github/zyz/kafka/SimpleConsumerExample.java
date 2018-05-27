package com.github.zyz.kafka;


import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;

/**
 * @author zyz
 * @since 2018/5/18
 */
public class SimpleConsumerExample {

    public static void main(String[] args) {

        String topic = "test";
        FetchRequest fetchRequest = new FetchRequestBuilder()
                .addFetch(topic, 0, 0, 10000).clientId("testClient").build();

        SimpleConsumer simpleConsumer = new SimpleConsumer("127.0.0.1",9092,10000,10000,"testClient");
        FetchResponse response = simpleConsumer.fetch(fetchRequest);
        System.out.println(response.hasError());
        System.out.println(response.errorCode(topic,0));
        ByteBufferMessageSet messageAndOffsets = response.messageSet(topic, 0);
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            System.out.println("offset : " + messageAndOffset.offset());
            Message message = messageAndOffset.message();
            ByteBuffer key = message.key();
            byte [] keyBytes = new byte[key.limit()];
            key.get(keyBytes);
            ByteBuffer payload = message.payload();
            byte [] valueBytes = new byte[payload.limit()];
            payload.get(valueBytes);

            System.out.println("key : " + new String(keyBytes));
            System.out.println("value : " + new String(valueBytes));

        }

    }
}
