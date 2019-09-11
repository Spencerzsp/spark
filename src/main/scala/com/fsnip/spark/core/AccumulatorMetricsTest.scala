package com.fsnip.spark.core

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 16:26 2019/8/21
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val sc = spark.sparkContext
    val acc = sc.longAccumulator("longAccum")

    val rdd = sc.parallelize(Array(1,2,3,4,5,6,7,8,9), 2)
    val sum = rdd.filter(n => {
      if(n % 2 != 0)
        acc.add(1L)
      n % 2 == 0
    }).reduce(_ + _)

    println("sum: " + sum)
    println("accum: " + acc.value)
  }
}
