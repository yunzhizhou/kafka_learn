package com.github.zyz.kafka

import com.github.zyz.kafka.config.Config
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer

/**
  * @author zyz
  * @since 2016/11/18
  */
object TimeConsumer extends App {

  val standaloneBrokerList: String = Config.standaloneBrokerList()
  val hostAndPort = standaloneBrokerList.split(":")
  val topic = "test"
  val clientId = "timeClient"

  //  val time = System.currentTimeMillis() - 3*24*60*60*1000
  val time =  OffsetRequest.LatestTime

  val debugSoTime = 60 * 60 * 1000
  val simpleConsumer = new SimpleConsumer(hostAndPort(0),hostAndPort(1).toInt, debugSoTime, 100 * 1024, clientId)

  val brokerList = ClientUtils.parseBrokerList(standaloneBrokerList)
  val topicMetadataResponse = ClientUtils.fetchTopicMetadata(Set(topic),brokerList,clientId, debugSoTime)

  val topicAndPartition = TopicAndPartition(topic,0)
  val offsetRequest = OffsetRequest(requestInfo = Map(topicAndPartition -> PartitionOffsetRequestInfo(time,1)))

  val offsetResponse = simpleConsumer.getOffsetsBefore(offsetRequest)

  val partitionErrorAndOffsets = offsetResponse.partitionErrorAndOffsets(topicAndPartition)

  val offset = partitionErrorAndOffsets.error match {
    case 0 => partitionErrorAndOffsets.offsets
    case _ => throw new RuntimeException()
  }

  println(offset)

}