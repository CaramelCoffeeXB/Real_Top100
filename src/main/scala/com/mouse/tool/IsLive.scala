package com.mouse.tool

import org.apache.spark.streaming.StreamingContext

/**
  * @author 咖啡不加糖
  */
case class IsLive(ssc: StreamingContext) {
  private val stop_flag = "stop_flag"
  /**
    * 优雅停止
    */
  def stopByMarkKey(): Unit = {
    val intervalMills = 6 * 1000 // sparkStreaming每3秒拉去一次，检测代码每隔6秒扫描一次消息是否存在
    var isStop = false
    while (!isStop) {
      isStop = ssc.awaitTerminationOrTimeout(intervalMills)
      if (!isStop && isExists(stop_flag)) {
        Thread.sleep(2000)
        ssc.stop(true, true)
      }
    }
  }

  /**
    * 判断Key是否存在
    */
  def isExists(key: String): Boolean = {
    val jedis = getConnet().getRedisConnet()
    val flag = jedis.exists(key)
    jedis.close()
    flag
  }

}
