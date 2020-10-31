package com.atguigu.sparkstream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author yymstart
 * @create 2020-10-31 15:00
 */
object SparkStreaming04_DirectAuto {
  def main(args: Array[String]): Unit = {
    val sparkconf: SparkConf = new SparkConf().setAppName("sparkstream").setMaster("local[*]")

    val scc: StreamingContext = new StreamingContext(sparkconf,Seconds(3))

    //定义kafka参数:kafka集群地址,消费者组名称,key序列化,value序列化
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "hadoop102:9092,hadoop103:9093,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguiguGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    kafkaPara
    //读取kafka数据创建DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      scc,
      LocationStrategies.PreferConsistent, //优先位置
      ConsumerStrategies.Subscribe[String, String](Set("testTopic"), kafkaPara) //消费策略(订阅多个主题,配置参数)
    )
    //5.将每天信息的kv取出
    val valueDStream: DStream[String] = kafkaDStream.map(record=>record.value())

    //计算wordCount
    valueDStream.flatMap(_.split(" "))
        .map((_,1))
        .reduceByKey(_+_)
        .print()

    //开启任务
    scc.start()
    scc.awaitTermination()
  }
}
