package com.fsnip.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 16:41 2019/7/15
  * @ Description：与StringIndexer对应，IndexToString将指标标签映射回原始字符串标签。
  * 一个常用的场景是先通过StringIndexer产生指标标签，然后使用指标标签进行训练，
  * 最后再对预测结果使用IndexToString来获取其原始的标签字符串。
  * @ Modified By：
  * @ Version:     
  */
object IndexToStringExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("IndexToStringExample")
      .master("local[*]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val data = Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )
    val df = spark.createDataFrame(data).toDF("id", "category")
    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    val converter = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")

    val converted = converter.transform(indexed)
    converted.select("id", "originalCategory").show(truncate = false)
  }
}
