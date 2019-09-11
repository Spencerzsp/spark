package com.fsnip.spark.ml

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 11:10 2019/1/23
  * @ Description：停用词为在文档中频繁出现，但未承载太多意义的词语，他们不应该被包含在算法输入中。
  *               StopWordsRemover的输入为一系列字符串（如分词器输出），输出中删除了所有停用词。停用词表由stopWords参数提供。
  * @ Modified By：
  * @ Version:     
  */
object StopWordRemoverTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("StopWordRemover").master("local[*]").getOrCreate()

    val dataset = spark.createDataFrame(Seq(

      (0, Seq("I", "saw", "the", "red", "baloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))

    )).toDF("id", "raw")

    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    remover.transform(dataset).show(truncate = false)
//    +---+----------------------------+--------------------+
//    |id |raw                         |filtered            |
//    +---+----------------------------+--------------------+
//    |0  |[I, saw, the, red, baloon]  |[saw, red, baloon]  |
//    |1  |[Mary, had, a, little, lamb]|[Mary, little, lamb]|
//    +---+----------------------------+--------------------+

  }
}
