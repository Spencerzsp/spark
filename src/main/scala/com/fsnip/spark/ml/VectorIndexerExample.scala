package com.fsnip.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 16:55 2019/7/15
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object VectorIndexerExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("IndexToStringExample")
      .master("local[*]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val data = spark.read.format("libsvm")
      .load("file:///F:\\spark-2.3.0-bin-hadoop2.7\\data\\mllib\\sample_libsvm_data.txt")
    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10)

    val indexerModel = indexer.fit(data)
//    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    val indexedData = indexerModel.transform(data)
    indexedData.show(3, truncate = false)
//    indexedData.take(3).foreach(println(_))

  }

}
