package com.fsnip.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.DCT
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 16:25 2019/7/15
  * @ Description：离散余弦
  * @ Modified By：
  * @ Version:     
  */
object DiscreteCosineExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DiscreteCosineExample")
      .master("local[2]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0, 3.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(14.0, -2.0, -5.0, 1.0)
    )

    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val dct = new DCT()
      .setInputCol("features")
      .setOutputCol("featuresDCT")
      .setInverse(false)

    val dctDF = dct.transform(df)
    dctDF.select("featuresDCT").take(3).foreach(println(_))
    dctDF.select("featuresDCT").show(3, truncate = false)
  }

}
