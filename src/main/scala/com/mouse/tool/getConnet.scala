package com.mouse.tool

import java.sql.Connection

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import redis.clients.jedis.Jedis

/**
  * @author 咖啡不加糖
  */
case class getConnet() {
  /**
    * 得到Redis的连接
    */
  def getRedisConnet():Jedis = {
    // Redis configurations
    val maxTotal = 20
    val maxIdle = 10
    val minIdle = 1
    val redisHost = "47.98.119.122"
    val redisPort = 6379
    val redisTimeout = 30000
    InitRedis.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle).getResource
  }

  def getMySQLConnet():Connection={
    // 访问本地MySQL服务器，通过3306端口访问mysql数据库
    val url = "jdbc:mysql://localhost:3306/cgjr?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    val driver = "com.mysql.jdbc.Driver"  //驱动名称
    val username = "root" //用户名
    val password = "root"  //密码
    InitMySQL.getMySQLStatement(driver,url,username,password)
  }

  /**
    * 从redis里获取Topic的offset值
    */
  def getKafkaOffsets(topicName: String, partitions: Int): Map[TopicPartition, Long] = {
    //从Redis获取上一次存的Offset
    val jedis = getConnet().getRedisConnet()
    val fromOffsets = collection.mutable.HashMap.empty[TopicPartition, Long]
    for (partition <- 0 to partitions - 1) {
      val topic_partition_key = topicName + "_" + partition
      val lastSavedOffset = jedis.get(topic_partition_key)
      val lastOffset = if (lastSavedOffset == null) 0L else lastSavedOffset.toLong
      fromOffsets += (new TopicPartition(topicName, partition) -> lastOffset)
    }
    jedis.close()

    fromOffsets.toMap
  }

  /**
    * 得到Kafka连接
    */
  def getKafkaParams(): Map[String, Object] ={
    val bootstrapServers = "hadoop1:9092,hadoop2:9092,hadoop3:9092"
    val groupId = "kafka-test-group"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "auto.offset.reset" -> "none"
    )
    kafkaParams
  }


}
