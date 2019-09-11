package com.fsnip.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 17:24 2019/7/15
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object VectorAssemblerExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("IndexToStringExample")
      .master("local[*]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val data = Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0),
      (0, 20, 1.0, Vectors.dense(1.0, 10.0, 0.5), 2.0)
    )
    val df = spark.createDataFrame(data).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    val assembler = new VectorAssembler()
      .setInputCols(Array("hour", "mobile", "userFeatures"))
      .setOutputCol("features")

    val output = assembler.transform(df)
    output.select("features", "clicked").take(2).foreach(println(_))
    output.select("features", "clicked").show(2,truncate = false)
//    println(output.select("features", "clicked").take(2))
  }


}
