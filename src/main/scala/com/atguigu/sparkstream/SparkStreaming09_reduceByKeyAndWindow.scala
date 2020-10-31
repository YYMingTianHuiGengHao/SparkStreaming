package com.atguigu.sparkstream

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yymstart
 * @create 2020-10-31 22:07
 */
class SparkStreaming09_reduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {

      // 1 初始化SparkStreamingContext
      val conf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
      val ssc = new StreamingContext(conf, Seconds(3))

    //通过监控端口创建dstream,读进来的数据为一行行
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //切割
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //统计
    val word: DStream[(String, Int)] = words.map((_,1))

    val wordToSumDStream: DStream[(String, Int)] = word.reduceByKeyAndWindow(
      (x: Int, y: Int) => (x + y),
      (x: Int, y: Int) => x - y,
      Seconds(12),
      Seconds(6),
      new HashPartitioner(2),
      (x: (String, Int)) => x._2 > 0
    )
    wordToSumDStream.print()


    // 6 启动=》阻塞
      ssc.start()
      ssc.awaitTermination()
    }

}
