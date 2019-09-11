package com.fsnip.spark.ml.myml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 9:57 2019/7/19
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object ML_Regression {
  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .appName("TextClassify")
      .master("local[*]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val sc = spark.sparkContext

  }

}
