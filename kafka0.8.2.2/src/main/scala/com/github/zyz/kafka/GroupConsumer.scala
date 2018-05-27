package com.github.zyz.kafka

import java.util.Properties

import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}

import scala.collection.Map

/**
  * @author zyz
  * @since 2018/5/270
  *
  * 一条消息只会被一个组下的任意消费者消费，单播
  *
  * 一条消息会被不同组下的消费者消息，多播
  */
object GroupConsumer extends App {


  val topic =  args(0)
  val prop = new Properties
  prop.put("zookeeper.connect", "127.0.0.1:2181/kafka0.8.2.2")
  prop.put("group.id", args(1))
  prop.put("zookeeper.session.timeout.ms", "10000")
  prop.put("zookeeper.sync.time.ms", "10000")
  prop.put("auto.offset.reset", "smallest") //smallest
  prop.put("auto.commit.enable", "true")
  prop.put("auto.commit.interval.ms", "5000")

  val consumerConfig = new ConsumerConfig(prop)

  val consumerConnector = Consumer.create(consumerConfig)

  val map = scala.collection.immutable.Map[String,Int](topic -> 1)


  private val streams: Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] = consumerConnector.createMessageStreams(Map[String,Int](topic->1))

  streams(topic).head.foreach {v =>
    println("offset : " +  v.offset)
    println("partition : " + v.partition)
    //    println("key : " + new String(v.key()))
    println("value : " + new String(v.message()))
  }


}