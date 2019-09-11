package com.fsnip.spark.ml

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 15:56 2019/5/22
  * @ Description：StringIndexer将字符串标签编码为标签指标
  * @ Modified By：
  * @ Version:     
  */
object StringIndexerExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DCTExample")
      .master("local[*]")
      .getOrCreate()

    val df = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")

//    单词出现的次数由高到低，对应indexer从0.0增加
    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df).transform(df)

    indexed.show(false)
//    +---+--------+-------------+
//    |id |category|categoryIndex|
//    +---+--------+-------------+
//    |0  |a       |0.0          |
//    |1  |b       |2.0          |
//    |2  |c       |1.0          |
//    |3  |a       |0.0          |
//    |4  |a       |0.0          |
//    |5  |c       |1.0          |
//    +---+--------+-------------+
  }

}
