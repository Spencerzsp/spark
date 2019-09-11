package com.fsnip.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 9:31 2019/1/23
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object TFIDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TF-IDF").master("local[*]").getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val sentenceData = spark.createDataFrame(Seq(

      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat")

    )).toDF("label", "sentence")

//    首先用分词器将句子转换为单个word
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

//      将word转换为特征向量，多个特征值组成特征向量
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(10)
    val featurizedData = hashingTF.transform(wordsData)

//    使用IDF重新调整特征向量
//    IDF is an Estimator which is fit on a dataset and produces an IDFModel
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show(truncate = false)
  }

}
