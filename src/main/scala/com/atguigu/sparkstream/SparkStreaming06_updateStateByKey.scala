package com.atguigu.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yymstart
 * @create 2020-10-31 21:13
 */
object SparkStreaming06_updateStateByKey {
  //定义更新状态方法,参数seq为当前批次单词的次数,state为已往批次单词次数
  val updateFunc=(seq:Seq[Int],state:Option[Int])=>{
    //当前批次数据累加
    val currentCount=seq.sum
    //历史批次数据累加
    val previousCount=state.getOrElse(0)
    //总的数据累积
    Some(currentCount+previousCount)
  }
  def createSCC():StreamingContext={
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    ssc.checkpoint("/.ck")
    //获取一行数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val word = words.map((_,1))
    val stateDStream: DStream[(String, Int)] = word.updateStateByKey[Int](updateFunc)
    stateDStream.print()
    ssc
  }

  def main(args: Array[String]): Unit = {
    val scc: StreamingContext = StreamingContext.getActiveOrCreate("./ck", ()=>createSCC())

    //开启任务
    scc.start()
    scc.awaitTermination()
  }
}