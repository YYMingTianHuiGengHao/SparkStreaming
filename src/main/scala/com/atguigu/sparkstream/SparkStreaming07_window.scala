package com.atguigu.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yymstart
 * @create 2020-10-31 21:35
 */
object SparkStreaming07_window {
  def main(args: Array[String]): Unit = {
     val sparkconf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")
     val scc = new StreamingContext(sparkconf,Seconds(3))

    val lines = scc.socketTextStream("hadoop102",9999)
    //切割交换
    val wordToOneDStream = lines.flatMap(_.split(" "))
      .map((_, 1))
    val wordToOneByWindow: DStream[(String, Int)] = wordToOneDStream.window(Seconds(12),Seconds(6))
    val wordToCountDStream: DStream[(String, Int)] = wordToOneByWindow.reduceByKey(_+_)
    wordToCountDStream.print()
    scc.start()
    scc.awaitTermination()
  }
}
