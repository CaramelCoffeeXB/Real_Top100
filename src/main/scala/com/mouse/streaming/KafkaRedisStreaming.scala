package com.mouse.streaming

import java.sql.{Connection, Statement}
import java.text.SimpleDateFormat
import java.util.Date

import com.mouse.tool.{IsLive, getConnet}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}


/**
  * @author 咖啡不加糖
  */
object KafkaRedisStreaming {

  def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("ScalaKafkaStream").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(3))
        val topicName = "Test"
        // 这里指定Topic的Partition的总数
        val fromOffsets = getConnet().getKafkaOffsets(topicName, 3)
        // 初始化KafkaDS
        val kafkaTopicDS = KafkaUtils.createDirectStream(
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList,getConnet().getKafkaParams(), fromOffsets)
        )
        //transform在Driver端执行
        val filteredKafkaRDD = kafkaTopicDS.transform(kafkaRDD=>{
            val redis = getConnet().getRedisConnet()
            //得到以是奖励名单数据
            val rewardBroadcast=sc.broadcast(redis.smembers("奖励名单"))
            //得到数据是否是去重数据,set值由 账号+时间戳 组成
            val disticBroadcast=sc.broadcast(redis.smembers("去重数据"))
              val filterRDD = kafkaRDD.filter(record=>{
            //该值的时间戳
            val time: Long = record.timestamp()
            //获取values中的账号,在字符串数组第一的位置
            val datas: Array[String] = record.value().split("\t")
            val account = datas(0)
            //*检查kafka里面核心数据是否符合条件
            var checkLog=true
            if(account==null || datas(1).toLong<=0){
              checkLog=false
            }
            checkLog && !rewardBroadcast.value.contains(account) && !disticBroadcast.value.contains(account+"_"+time)
          })
          redis.close()
          filterRDD
        })

        // 处理符合条件的数据
        filteredKafkaRDD.foreachRDD(rdd => {
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          // 如果rdd有数据
          if (!rdd.isEmpty()) {
            val recordRDD: Array[ConsumerRecord[String, String]] = rdd.collect() //将数据拉取到Driver端
            val mysqlConnet: Connection = getConnet().getMySQLConnet()
            val mysqlStatement: Statement = mysqlConnet.createStatement() //连接mysql
            val jedis = getConnet().getRedisConnet() //连接Redis
            val p = jedis.pipelined() //开启管道
            p.multi() //开启事务
            p.del("去重数据","")
            p.sadd("去重数据", "")

            /**
              * 二步：
              * 1.将kafka数据解析出来，与redis中的数据进行判断，
              * 如果与当前值累加后超过10万金币，则删除该账户,
              * 并将账户连同当前时间插入mysql中，并把该账户放入过滤名单中
              * 2.当前批次数据全部存入redis中进行校验去重
              */
            recordRDD.foreach { record =>
              val data: Array[String] = record.value().split("\t")
              val account = data(0)
              //0坐标 账户
              val coin = data(1).toLong
              //1坐标 金币
              val currentCoin = jedis.hget("每一个用户累计金币数", account).toLong
              if ((coin + currentCoin) >= 100000) {
                //如果累加后大于10万金币，累计金币名单内删除该账户，该账户拉入过滤名单，将账户+时间戳保存到mysql
                p.hdel("每一个用户累计金币数", account) //删除该账号的累计值
                if(jedis.scard("奖励名单") < 100){ //如果奖励名单里超过100位，那么不添加新的数据
                  p.sadd("奖励名单", account) //添加该账号到过滤名单中
                }
                val date = new Date(record.timestamp())
                val time: String = new SimpleDateFormat("yyyy-MM-dd").format(date)
                mysqlStatement.execute("insert into 奖励名单表 values(" + account + "," + System.currentTimeMillis() + "," + time + ")") //添加到mysql
              }
              //不满足情况，则将该账户金币更新
              p.hset("每一个用户累计金币数", account, (coin + currentCoin).toString)

              /**
                * 更新此批次的数据到redis,为下一批数据去重做准备
                * 把当前账号和kafka发送消息时间戳组成数据
                */
              p.sadd("去重数据", account + "_" + record.timestamp())
            }
            //更新Offset
            offsetRanges.foreach { offsetRange =>
              println("partition : " + offsetRange.partition + " fromOffset:  " + offsetRange.fromOffset + " untilOffset: " + offsetRange.untilOffset)
              val topic_partition_key = offsetRange.topic + "_" + offsetRange.partition
              p.set(topic_partition_key, offsetRange.untilOffset + "")
            }
            mysqlConnet.commit() //提交mysql事务
            mysqlConnet.close() //关闭资源
            p.exec() //提交事务
            p.sync //关闭pipeline
            jedis.close()
          }
        })
        ssc.start()
        // 优雅停止
        IsLive(ssc).stopByMarkKey()
        ssc.awaitTermination()
  }



}
