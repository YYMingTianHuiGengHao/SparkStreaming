package com.atguigu.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author yymstart
 * @create 2020-10-31 13:11
 */
object SparkStreaming02_RDDStream {
  def main(args: Array[String]): Unit = {
    val sparkconf: SparkConf = new SparkConf().setAppName("sparkstream").setMaster("local[*]")
    val scc = new StreamingContext(sparkconf,Seconds(4))

    val rddQuene: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()
    val inputDStream = scc.queueStream(rddQuene,oneAtATime = false)

    //处理队列中的rdd数据
    val sumDStream: DStream[Int] = inputDStream.reduce(_+_)

    //打印结果
    sumDStream.print()
    //启动任务
    scc.start()

    //循环并向rdd中放入rdd
    for(i<-1 to 5){
      rddQuene += scc.sparkContext.makeRDD(1 to 5)
      Thread.sleep(2000)
    }
    scc.awaitTermination()

  }
}
