package com.fsnip.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 14:24 2019/1/18
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

//    读取本地文件计算
//    val lines = sc.textFile("file:///f:\\word.txt")

    val lines = sc.textFile("/user/test2/word.txt")

    val wordCount =lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    wordCount.foreach(println(_))

//    val spark = SparkSession.builder().appName("WordCount").master("local[*]").getOrCreate()
//    val lines = spark.read.text("/test/word.txt").
//    val wordCount = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
//    wordCount.foreach(println(_))

    sc.stop()
  }

}
