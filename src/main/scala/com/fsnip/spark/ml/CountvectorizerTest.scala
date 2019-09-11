package com.fsnip.spark.ml

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 10:07 2019/1/23
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object CountvectorizerTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("CountvectorizerTest").master("local[*]").getOrCreate()

    val df = spark.createDataFrame(Seq(

      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))

    )).toDF("id", "words")

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df)

    val cvm = new CountVectorizerModel(Array("a", "b" ,"c"))
      .setInputCol("words")
      .setOutputCol("features")

    cvModel.transform(df).select("id", "words", "features").show(truncate = false)

  }

}
