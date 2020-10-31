package com.atguigu.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yymstart
 * @create 2020-10-31 21:57
 */
object SparkStreaming08_reduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    val scc = new StreamingContext(sparkConf,Seconds(3))

    //保存到检查点数据
    scc.checkpoint("./ck")
    //通过监控端口号创建DStream,读进来的数据为一行
    val lines = scc.socketTextStream("hadoop102",9999)
    val wordToOne = lines.flatMap(_.split(" "))
    val word: DStream[(String, Int)] = wordToOne.map((_,1))
    val wordCounts = word.reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),Seconds(12),Seconds(6))
    wordCounts.print()

    //启动
    scc.start()
    scc.awaitTermination()

  }
}
