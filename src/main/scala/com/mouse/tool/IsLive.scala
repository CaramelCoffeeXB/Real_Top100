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
    val intervalMills = 6 * 1000
    var isStop = false
    while (!isStop) {
      //等待执行停止。执行过程中发生的任何异常都会在此线程中抛出，如果执行停止了返回true，
      //线程等待超时长，当超过timeout时间后，会监测ExecutorService是否已经关闭，若关闭则返回true，否则返回false。
      isStop = ssc.awaitTerminationOrTimeout(intervalMills)
      if (!isStop && isExists(stop_flag)) {
        Thread.sleep(2000)
        //第一个true：停止相关的SparkContext。无论这个流媒体上下文是否已经启动，底层的SparkContext都将被停止。
        //第二个true：则通过等待所有接收到的数据的处理完成，从而优雅地停止。
        ssc.stop(true, true)
      }
    }
  }

  /**
    * 判断Redis中的Key是否存在，存在返回为true
    */
  def isExists(key: String): Boolean = {
    val jedis = getConnet().getRedisConnet()
    val flag = jedis.exists(key)
    jedis.close()
    flag
  }

}
