package com.fsnip.spark.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 17:12 2019/7/31
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object KafkaDirector {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KafkaReceiver")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("file:///e:/test")


//    val Array(brokers, topics) = args
//    val topic = topics.split(",").toSet
    val topic = Set("mydemo2")
    val kafkaParams = Map("metadata.broker.list" -> "sinan3:9092", "group.id" -> "Kafka_Direct")
    val data = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topic, kafkaParams)
    )
    val lines = data.map(_.value())
    val wordCounts = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    wordCounts.print()


    ssc.start()
    ssc.awaitTermination()

  }

}
