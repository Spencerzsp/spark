package com.fsnip.spark.ml

import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 10:35 2019/1/23
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object FeatureHasherTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Word2Vec").master("local[*]").getOrCreate()

    val dataset = spark.createDataFrame(Seq(

      (2.2, true, "1", "foo"),
      (3.3, false, "2", "bar"),
      (4.4, false, "3", "baz"),
      (5.5, false, "4", "foo")

    )).toDF("real", "bool", "stringNum", "string")

    val hasher = new FeatureHasher()
      .setInputCols("real", "bool", "stringNum", "string")
      .setOutputCol("features")

    val featurized = hasher.transform(dataset)
    featurized.select("real", "bool", "stringNum", "string", "features").show(truncate = false)
    featurized.show(false)
  }
}
