package com.fsnip.spark.ml

import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 10:23 2019/1/23
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object TokenizerTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Word2Vec").master("local[*]").getOrCreate()

    val sentenceDataFrame = spark.createDataFrame(Seq(

      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")

    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")

    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W+")

    val tokenized = tokenizer.transform(sentenceDataFrame)
    tokenized.select("words", "label").take(3).foreach(println(_))

    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
    regexTokenized.select("words", "label").take(3).foreach(println(_))
//    regexTokenized.select("words", "label").show(truncate = false)
  }


}
