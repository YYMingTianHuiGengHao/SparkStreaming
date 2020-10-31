package com.atguigu.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yymstart
 * @create 2020-10-31 10:35
 */
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    val scc = new StreamingContext(sparkConf,Seconds(3))
    val lineDStream: ReceiverInputDStream[String] = scc.socketTextStream("hadoop102",9999)
    val wordToOneDStream: DStream[(String, Int)] = lineDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    wordToOneDStream.print()
      
  }
}
