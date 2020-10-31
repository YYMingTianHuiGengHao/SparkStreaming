package com.atguigu.sparkstream

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author yymstart
 * @create 2020-10-31 11:29
 */
object SparkStreaming03_CustomerReceiver {
  def main(args: Array[String]): Unit = {
    def main(args: Array[String]): Unit = {

      //1.初始化Spark配置信息
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")

      //2.初始化SparkStreamingContext
      val ssc = new StreamingContext(sparkConf, Seconds(5))

      //3.创建自定义receiver的Streaming
      val lineDStream = ssc.receiverStream(new CustomerReceiver("hadoop102", 9999))

      //4.将每一行数据做切分，形成一个个单词
      val wordDStream = lineDStream.flatMap(_.split(" "))

      //5.将单词映射成元组（word,1）
      val wordToOneDStream = wordDStream.map((_, 1))

      //6.将相同的单词次数做统计
      val wordToSumDStream = wordToOneDStream.reduceByKey(_ + _)

      //7.打印
      wordToSumDStream.print()

      //8.启动SparkStreamingContext
      ssc.start()
      ssc.awaitTermination()
    }
  }

  class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    // 最初启动的时候，调用该方法，作用为：读数据并将数据发送给Spark
    override def onStart(): Unit = {

      new Thread("Socket Receiver") {
        override def run() {
          receive()
        }
      }.start()
    }

    // 读数据并将数据发送给Spark
    def receive(): Unit = {

      // 创建一个Socket
      var socket: Socket = new Socket(host, port)

      // 创建一个BufferedReader用于读取端口传来的数据
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))

      // 读取数据
      var input: String = reader.readLine()

      //当receiver没有关闭并且输入数据不为空，则循环发送数据给Spark
      while (!isStopped() && input != null) {
        store(input)
        input = reader.readLine()
      }

      // 如果循环结束，则关闭资源
      reader.close()
      socket.close()

      //重启接收任务
      restart("restart")
    }

    override def onStop(): Unit = {}
  }

}