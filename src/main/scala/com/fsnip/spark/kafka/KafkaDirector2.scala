package com.fsnip.spark.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 9:31 2019/8/1
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object KafkaDirector2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("KafkaDirector2")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("file:///e:/test")

    val topics = Set("mydemo2")
    val kafkaParams = Map("metadata.broker.list" -> "sinan3:9092")
//    val data = KafkaUtils.createDirectStream[String, String](ssc, kafkaParams, topics)



    ssc.start()
    ssc.awaitTermination()
  }

}
