package com.fsnip.spark.ml

import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 14:21 2019/5/22
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object NGramExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("NGramExample")
      .master("local[*]")
      .getOrCreate()

    val wordDataFrame = spark.createDataFrame(
      Seq(
        (0, Array("Hi", "I", "heard", "about", "Spark")),
        (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
        (2, Array("Logistic", "regression", "models", "are", "neat"))
      )
    ).toDF("id", "words")

    val ngram = new NGram()
      .setN(2)
      .setInputCol("words")
      .setOutputCol("ngrams")

    val model = ngram.transform(wordDataFrame)
    model.select("ngrams").show(false)
  }

}
